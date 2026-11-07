//! A Store with `wait_replicas` refuses a write that too few replicas have, even one that it finds
//! done, and its writes wait for their replicas in parallel.

use prost::Message;
use std::{num::NonZeroUsize, time::Duration};
use temporalio_streams::{
    Reason,
    proto::{ClientEpoch, ClientProducer, Producer, StreamRecord, StreamRef, producer::Kind},
    store::{Batch, Create, CreateOutcome, Store, Withdrawal},
};
use temporalio_streams_redis::{
    RedisStore, RedisStoreOptions, Topology, WaitOptions,
    testing::{SlotRange, cluster_url, standalone_url},
};

const PREFIX: &str = "streams-wait";

/// Held by each case for its whole run. The cases flush the scripts, pause the replicas and block
/// in `WAIT`, so one case would disturb the next.
static SERVER: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// A `WAIT` that must fail gets a short timeout, and one that must pass gets a long one.
const SHORT: Duration = Duration::from_millis(50);
const LONG: Duration = Duration::from_secs(5);

async fn store(topology: &Topology, wait: Option<WaitOptions>) -> RedisStore {
    let mut options = RedisStoreOptions::from(topology.clone());
    options.key_prefix = PREFIX.parse().unwrap();
    options.response_timeout = 2 * LONG;
    options.wait = wait;
    RedisStore::connect(options).await.unwrap()
}

/// A `WAIT` for `replicas` replicas, which must be at least one.
fn wait_for(replicas: usize, timeout: Duration) -> Option<WaitOptions> {
    Some(WaitOptions {
        replicas: NonZeroUsize::new(replicas).unwrap(),
        timeout,
    })
}

fn cluster_at(url: &str) -> Topology {
    Topology::Cluster {
        first: url.to_string(),
        rest: Vec::new(),
    }
}

async fn admin(url: &str) -> redis::aio::MultiplexedConnection {
    redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}

/// The URL of each replica of a standalone server.
async fn replicas(url: &str) -> Vec<String> {
    let info: String = redis::cmd("INFO")
        .arg("replication")
        .query_async(&mut admin(url).await)
        .await
        .unwrap();
    info.lines()
        .filter(|line| line.starts_with("slave"))
        .filter_map(|line| {
            let field = |name: &str| {
                line.split([':', ','])
                    .find_map(|part| part.strip_prefix(name)?.strip_prefix('='))
            };
            Some(format!("redis://{}:{}/0", field("ip")?, field("port")?))
        })
        .collect()
}

/// The URL of each replica of the cluster.
async fn cluster_replicas(url: &str) -> Vec<String> {
    let client = redis::cluster::ClusterClient::new(vec![url]).unwrap();
    let mut conn = client.get_async_connection().await.unwrap();
    let ranges: Vec<SlotRange> = redis::cmd("CLUSTER")
        .arg("SLOTS")
        .query_async(&mut conn)
        .await
        .unwrap();
    let mut urls: Vec<String> = ranges
        .iter()
        .flat_map(|range| &range.replicas)
        .map(|node| {
            // An empty or NULL host is local in the test cluster.
            let host = node.host.as_deref().filter(|host| !host.is_empty());
            let host = host.unwrap_or("127.0.0.1");
            format!("redis://{host}:{}/0", node.port)
        })
        .collect();
    urls.sort();
    urls.dedup();
    urls
}

/// Stops each replica from applying the primary's writes until the guard drops. The pause has a
/// long bound, so it can't end before the case does.
async fn pause(replicas: &[String]) -> Paused {
    for replica in replicas {
        let _: () = redis::cmd("CLIENT")
            .arg("PAUSE")
            .arg(600_000)
            .arg("WRITE")
            .query_async(&mut admin(replica).await)
            .await
            .unwrap();
    }
    Paused(replicas.to_vec())
}

/// Paused replicas. The drop unpauses them, also when the case panics, so the next case finds them
/// live.
struct Paused(Vec<String>);

impl Drop for Paused {
    fn drop(&mut self) {
        // A drop can't wait on a future, so it uses blocking connections.
        let unpause = |replica: &str| -> redis::RedisResult<()> {
            let mut conn = redis::Client::open(replica)?.get_connection()?;
            redis::cmd("CLIENT").arg("UNPAUSE").query(&mut conn)
        };
        let unpaused: Vec<_> = self.0.iter().map(|replica| unpause(replica)).collect();
        // A second panic would abort the test binary, so the drop checks only on a clean path.
        if !std::thread::panicking() {
            for unpaused in unpaused {
                unpaused.unwrap();
            }
        }
    }
}

fn unique(name: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{name}-{nanos}")
}

fn client() -> Producer {
    Producer {
        id: "client:wait".to_string(),
        kind: Some(Kind::Client(ClientProducer {
            epoch: Some(ClientEpoch { attempt: 1 }),
        })),
    }
}

#[tokio::test]
#[ignore = "needs Redis"]
async fn a_write_that_too_few_replicas_have_fails_and_its_retry_finds_it() {
    let _lock = SERVER.lock().await;
    too_few_replicas(Topology::Standalone(standalone_url())).await;
}

#[tokio::test]
#[ignore = "needs a Redis cluster"]
async fn a_write_that_too_few_replicas_have_fails_on_a_redis_cluster() {
    let _lock = SERVER.lock().await;
    too_few_replicas(cluster_at(&cluster_url())).await;
}

async fn too_few_replicas(topology: Topology) {
    let handle = StreamRef {
        id: format!("s-{}", std::process::id()),
        ..Default::default()
    };
    let namespace = unique("ns");
    let create = |store: &RedisStore| {
        let (store, namespace, handle) = (store.clone(), namespace.clone(), handle.clone());
        async move {
            // Each create is a retry of the first one, so it gets the Stream as created.
            let create = Create {
                request_id: "too-few",
                ..Create::default()
            };
            store.create(&namespace, &handle, &create).await
        }
    };
    // No test server has this many replicas.
    let waits = store(&topology, wait_for(16, SHORT)).await;
    // So the write reloads its script between the script call and its `WAIT`.
    let flush = redis::cmd("SCRIPT").arg("FLUSH").clone();
    match &topology {
        Topology::Cluster { .. } => {
            let client =
                redis::cluster::ClusterClient::new(topology.servers().map(String::as_str)).unwrap();
            let mut conn = client.get_async_connection().await.unwrap();
            let _: () = flush.query_async(&mut conn).await.unwrap();
        }
        Topology::Standalone(url) => {
            let _: () = flush.query_async(&mut admin(url).await).await.unwrap();
        }
    }
    let refused = create(&waits).await.unwrap_err();
    assert_eq!(refused.reason, Reason::StorageUnavailable, "{refused}");
    let plain = store(&topology, None).await;
    assert_eq!(
        create(&plain).await.unwrap().outcome,
        CreateOutcome::Created
    );
    if let Topology::Standalone(url) = &topology {
        let count = replicas(url).await.len();
        assert!(count > 0, "the server at STREAMS_REDIS_URL needs a replica");
        let enough = store(&topology, wait_for(count, LONG)).await;
        assert_eq!(
            create(&enough).await.unwrap().outcome,
            CreateOutcome::Created
        );
    }
}

/// A `WAIT` on a node with no write of its own returns at once, so only a `WAIT` on the write's
/// primary fails while the replicas pause.
#[tokio::test]
#[ignore = "needs a Redis cluster"]
async fn a_cluster_write_waits_for_the_replica_of_its_own_primary() {
    let _lock = SERVER.lock().await;
    let url = cluster_url();
    let replicas = cluster_replicas(&url).await;
    assert!(
        !replicas.is_empty(),
        "the cluster at STREAMS_REDIS_CLUSTER_URL needs replicas"
    );
    let namespace = unique("ns");
    let create = |store: RedisStore, id: &'static str| {
        let namespace = namespace.clone();
        async move {
            let handle = StreamRef {
                id: id.to_string(),
                ..Default::default()
            };
            store.create(&namespace, &handle, &Create::default()).await
        }
    };
    let short = store(&cluster_at(&url), wait_for(1, SHORT)).await;
    let paused = pause(&replicas).await;
    let refused = create(short, "paused").await;
    drop(paused);
    assert_eq!(refused.unwrap_err().reason, Reason::StorageUnavailable);
    let acked = create(store(&cluster_at(&url), wait_for(1, LONG)).await, "acked").await;
    assert_eq!(acked.unwrap().outcome, CreateOutcome::Created);
}

/// A create, resend or close that finds its write done waits for the replicas to have that write.
#[tokio::test]
#[ignore = "needs Redis"]
async fn a_call_that_finds_its_write_waits_for_the_replicas() {
    let _lock = SERVER.lock().await;
    let url = standalone_url();
    let replicas = replicas(&url).await;
    assert!(
        !replicas.is_empty(),
        "the server at STREAMS_REDIS_URL needs a replica"
    );
    let (namespace, handle) = (
        unique("ns"),
        StreamRef {
            id: "s".to_string(),
            ..Default::default()
        },
    );
    let standalone = Topology::Standalone(url.clone());
    let plain = store(&standalone, None).await;
    // A Store per call, since a call's own write puts its connection's offset past the replica's.
    let mut waits = vec![];
    for _ in 0..3 {
        waits.push(store(&standalone, wait_for(replicas.len(), SHORT)).await);
    }
    let none = Withdrawal::default;
    let producer = client();
    let records = [StreamRecord::default().encode_to_vec()];
    let batch = Batch {
        producer: &producer,
        sequence: 1,
        records: &records,
        digest: [0; 16],
        withdrawal: none(),
    };
    let other = StreamRef {
        id: "other".to_string(),
        ..Default::default()
    };
    plain
        .create(&namespace, &handle, &Create::default())
        .await
        .unwrap();
    let paused = pause(&replicas).await;
    plain.append(&namespace, &handle, &batch).await.unwrap();
    let resent = waits[0]
        .append(&namespace, &handle, &batch)
        .await
        .map(|_| ());
    plain.close(&namespace, &handle, &producer).await.unwrap();
    let closed = waits[1]
        .close(&namespace, &handle, &producer)
        .await
        .map(|_| ());
    // The second create is a retry of the first, so it finds its write done.
    let retry_of_other = Create {
        request_id: "other",
        ..Create::default()
    };
    plain
        .create(&namespace, &other, &retry_of_other)
        .await
        .unwrap();
    let found = waits[2]
        .create(&namespace, &other, &retry_of_other)
        .await
        .map(|_| ());
    drop(paused);
    for result in [resent, closed, found] {
        assert_eq!(result.unwrap_err().reason, Reason::StorageUnavailable);
    }
    let enough = store(&standalone, wait_for(replicas.len(), LONG)).await;
    let resent = enough.append(&namespace, &handle, &batch).await.unwrap();
    assert!(resent.resend);
    let closed = enough.close(&namespace, &handle, &producer).await.unwrap();
    assert!(closed.already);
}

/// Writes with `WAIT` don't queue behind each other's `WAIT`. Standalone only, since a cluster
/// spreads the Streams over its nodes.
#[tokio::test]
#[ignore = "needs Redis"]
async fn writes_wait_for_their_replicas_at_once() {
    let _lock = SERVER.lock().await;
    let url = standalone_url();
    // No test server has this many replicas, so each `WAIT` blocks until the case unblocks it.
    let mut options = RedisStoreOptions::new(&url);
    options.key_prefix = PREFIX.parse().unwrap();
    options.response_timeout = Duration::from_secs(60);
    options.wait = Some(WaitOptions {
        replicas: NonZeroUsize::new(16).unwrap(),
        timeout: Duration::from_secs(30),
    });
    let waits = RedisStore::connect(options).await.unwrap();
    let namespace = unique("ns");
    // A write that finds no script runs its `WAIT` twice, so load the script first.
    store(&Topology::Standalone(url.clone()), None)
        .await
        .create(
            &namespace,
            &StreamRef {
                id: "loads".to_string(),
                ..Default::default()
            },
            &Create::default(),
        )
        .await
        .unwrap();
    let mut writes = tokio::task::JoinSet::new();
    for i in 0..4 {
        let (waits, namespace) = (waits.clone(), namespace.clone());
        writes.spawn(async move {
            let handle = StreamRef {
                id: format!("s{i}"),
                ..Default::default()
            };
            waits.create(&namespace, &handle, &Create::default()).await
        });
    }
    // Writes that share a connection would run their `WAIT`s one after the other, so the server
    // would never show all four at once.
    let mut admin = admin(&url).await;
    let blocked = loop {
        let blocked = waiting_clients(&mut admin).await;
        if blocked.len() == 4 {
            break blocked;
        }
        assert!(
            writes.try_join_next().is_none(),
            "a write ended before all four waited at once"
        );
        tokio::task::yield_now().await;
    };
    for id in blocked {
        let unblocked: i64 = redis::cmd("CLIENT")
            .arg("UNBLOCK")
            .arg(&id)
            .query_async(&mut admin)
            .await
            .unwrap();
        assert_eq!(unblocked, 1, "client {id} was no longer blocked");
    }
    for result in writes.join_all().await {
        assert_eq!(result.unwrap_err().reason, Reason::StorageUnavailable);
    }
}

/// The ids of the clients that block in `WAIT` now. `cmd` is only a client's last command, so a
/// client counts only while its flags have `b`, for blocked.
async fn waiting_clients(admin: &mut redis::aio::MultiplexedConnection) -> Vec<String> {
    let clients: String = redis::cmd("CLIENT")
        .arg("LIST")
        .query_async(admin)
        .await
        .unwrap();
    clients
        .lines()
        .filter_map(|client| {
            let field = |name: &str| {
                client
                    .split(' ')
                    .find_map(|field| field.strip_prefix(name)?.strip_prefix('='))
            };
            let blocked = field("flags")?.contains('b') && field("cmd")? == "wait";
            blocked.then(|| field("id").map(str::to_string)).flatten()
        })
        .collect()
}
