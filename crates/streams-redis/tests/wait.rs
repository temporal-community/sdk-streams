//! A Store with `wait_replicas` refuses a write that too few replicas have, even one that it finds
//! done, and its writes wait for their replicas in parallel.

use prost::Message;
use std::{num::NonZeroUsize, time::Duration};
use temporalio_streams::{
    Reason,
    proto::{Producer, ProducerEpoch, StreamHandle, StreamProducerKind, StreamRecord},
    store::{Batch, CreateOutcome, Owner, Store, Withdrawal},
};
use temporalio_streams_redis::{
    RedisStore, RedisStoreOptions, WaitOptions,
    testing::{cluster_url, standalone_url},
};

const PREFIX: &str = "streams-wait";

/// Held by each case for its whole run. The cases flush the scripts, pause the replicas and block
/// in `WAIT`, so one case would disturb the next.
static SERVER: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// A `WAIT` that must fail gets a short timeout, and one that must pass gets a long one.
const SHORT: Duration = Duration::from_millis(50);
const LONG: Duration = Duration::from_secs(1);

async fn store(url: &str, cluster: bool, wait_replicas: usize, timeout: Duration) -> RedisStore {
    let mut options = RedisStoreOptions::new(url);
    options.cluster = cluster;
    options.key_prefix = PREFIX.parse().unwrap();
    options.wait =
        NonZeroUsize::new(wait_replicas).map(|replicas| WaitOptions { replicas, timeout });
    RedisStore::connect(options).await.unwrap()
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
    let ranges: Vec<Vec<redis::Value>> = redis::cmd("CLUSTER")
        .arg("SLOTS")
        .query_async(&mut conn)
        .await
        .unwrap();
    // Each range is its start, its end, its primary, and then its replicas.
    let mut urls: Vec<String> = ranges
        .iter()
        .flat_map(|range| range.iter().skip(3))
        .filter_map(|node| {
            let redis::Value::Array(node) = node else {
                return None;
            };
            let host: String = redis::from_redis_value(node.first()?.clone()).ok()?;
            let port: u16 = redis::from_redis_value(node.get(1)?.clone()).ok()?;
            Some(format!("redis://{host}:{port}/0"))
        })
        .collect();
    urls.sort();
    urls.dedup();
    urls
}

/// Stops each replica from applying the primary's writes, until [resume] or 5 s.
async fn pause(replicas: &[String]) {
    for replica in replicas {
        let _: () = redis::cmd("CLIENT")
            .arg("PAUSE")
            .arg(5000)
            .arg("WRITE")
            .query_async(&mut admin(replica).await)
            .await
            .unwrap();
    }
}

async fn resume(replicas: &[String]) {
    for replica in replicas {
        let _: () = redis::cmd("CLIENT")
            .arg("UNPAUSE")
            .query_async(&mut admin(replica).await)
            .await
            .unwrap();
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
        kind: StreamProducerKind::Client as i32,
        id: "client:wait".to_string(),
        epoch: Some(ProducerEpoch {
            attempt: 1,
            ..Default::default()
        }),
        run_id: String::new(),
    }
}

#[tokio::test]
#[ignore = "needs Redis"]
async fn a_write_that_too_few_replicas_have_fails_and_its_retry_finds_it() {
    let _lock = SERVER.lock().await;
    too_few_replicas(&standalone_url(), false).await;
}

#[tokio::test]
#[ignore = "needs a Redis cluster"]
async fn a_write_that_too_few_replicas_have_fails_on_a_redis_cluster() {
    let _lock = SERVER.lock().await;
    too_few_replicas(&cluster_url(), true).await;
}

async fn too_few_replicas(url: &str, cluster: bool) {
    let handle = StreamHandle {
        id: format!("s-{}", std::process::id()),
        ..Default::default()
    };
    let namespace = unique("ns");
    let create = |store: &RedisStore| {
        let (store, namespace, handle) = (store.clone(), namespace.clone(), handle.clone());
        async move {
            store
                .create(
                    &namespace,
                    &handle,
                    None,
                    Owner::Client,
                    &Withdrawal::default(),
                )
                .await
        }
    };
    // No test server has this many replicas.
    let waits = store(url, cluster, 16, SHORT).await;
    // So the write reloads its script between the script call and its `WAIT`.
    let flush = redis::cmd("SCRIPT").arg("FLUSH").clone();
    if cluster {
        let client = redis::cluster::ClusterClient::new(vec![url]).unwrap();
        let mut conn = client.get_async_connection().await.unwrap();
        let _: () = flush.query_async(&mut conn).await.unwrap();
    } else {
        let _: () = flush.query_async(&mut admin(url).await).await.unwrap();
    }
    let refused = create(&waits).await.unwrap_err();
    assert_eq!(refused.reason, Reason::Storage, "{refused}");
    let plain = store(url, cluster, 0, SHORT).await;
    assert_eq!(create(&plain).await.unwrap().outcome, CreateOutcome::Exists);
    if !cluster {
        let count = replicas(url).await.len();
        assert!(count > 0, "the server at STREAMS_REDIS_URL needs a replica");
        let enough = store(url, cluster, count, LONG).await;
        assert_eq!(
            create(&enough).await.unwrap().outcome,
            CreateOutcome::Exists
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
            let handle = StreamHandle {
                id: id.to_string(),
                ..Default::default()
            };
            store
                .create(
                    &namespace,
                    &handle,
                    None,
                    Owner::Client,
                    &Withdrawal::default(),
                )
                .await
        }
    };
    let paused = store(&url, true, 1, SHORT).await;
    pause(&replicas).await;
    let refused = create(paused, "paused").await;
    resume(&replicas).await;
    assert_eq!(refused.unwrap_err().reason, Reason::Storage);
    let acked = create(store(&url, true, 1, LONG).await, "acked").await;
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
        StreamHandle {
            id: "s".to_string(),
            ..Default::default()
        },
    );
    let plain = store(&url, false, 0, SHORT).await;
    // A Store per call, since a call's own write puts its connection's offset past the replica's.
    let mut waits = vec![];
    for _ in 0..3 {
        waits.push(store(&url, false, replicas.len(), SHORT).await);
    }
    let none = Withdrawal::default;
    let producer = client();
    let records = [StreamRecord::default().encode_to_vec()];
    let batch = Batch {
        producer: &producer,
        sequence: 1,
        records: &records,
        digest: "d",
        withdrawal: none(),
    };
    let other = StreamHandle {
        id: "other".to_string(),
        ..Default::default()
    };
    plain
        .create(&namespace, &handle, None, Owner::Client, &none())
        .await
        .unwrap();
    pause(&replicas).await;
    plain.append(&namespace, &handle, &batch).await.unwrap();
    let resent = waits[0]
        .append(&namespace, &handle, &batch)
        .await
        .map(|_| ());
    plain
        .close(&namespace, &handle, &producer, b"")
        .await
        .unwrap();
    let closed = waits[1]
        .close(&namespace, &handle, &producer, b"")
        .await
        .map(|_| ());
    plain
        .create(&namespace, &other, None, Owner::Client, &none())
        .await
        .unwrap();
    let found = waits[2]
        .create(&namespace, &other, None, Owner::Client, &none())
        .await
        .map(|_| ());
    resume(&replicas).await;
    for result in [resent, closed, found] {
        assert_eq!(result.unwrap_err().reason, Reason::Storage);
    }
    let enough = store(&url, false, replicas.len(), LONG).await;
    let resent = enough.append(&namespace, &handle, &batch).await.unwrap();
    assert!(resent.resend);
    let closed = enough
        .close(&namespace, &handle, &producer, b"")
        .await
        .unwrap();
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
    store(&url, false, 0, SHORT)
        .await
        .create(
            &namespace,
            &StreamHandle {
                id: "loads".to_string(),
                ..Default::default()
            },
            None,
            Owner::Client,
            &Withdrawal::default(),
        )
        .await
        .unwrap();
    let mut writes = tokio::task::JoinSet::new();
    for i in 0..4 {
        let (waits, namespace) = (waits.clone(), namespace.clone());
        writes.spawn(async move {
            let handle = StreamHandle {
                id: format!("s{i}"),
                ..Default::default()
            };
            waits
                .create(
                    &namespace,
                    &handle,
                    None,
                    Owner::Client,
                    &Withdrawal::default(),
                )
                .await
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
        assert_eq!(result.unwrap_err().reason, Reason::Storage);
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
