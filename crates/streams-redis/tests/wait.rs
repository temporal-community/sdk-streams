//! A Store with `wait_replicas` refuses a write that too few replicas have, even one that it finds
//! done, and its writes wait for their replicas in parallel.

mod support;

use std::time::Duration;
use support::server_url;
use temporalio_streams::{
    Reason, StreamError,
    proto::{Producer, ProducerEpoch, StreamHandle, StreamProducerKind},
    store::{Batch, CreateOutcome, Owner, Withdrawal},
};
use temporalio_streams_redis::{RedisStore, RedisStoreOptions};

const PREFIX: &str = "streams-wait";

/// Held by each case for its whole run. The cases flush the scripts, pause the replicas and time
/// their `WAIT`s, so one case would slow down the next.
static SERVER: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// A `WAIT` that must fail gets a short timeout, and one that must pass gets a long one.
const SHORT: Duration = Duration::from_millis(50);
const LONG: Duration = Duration::from_secs(1);

async fn store(url: &str, cluster: bool, wait_replicas: usize, timeout: Duration) -> RedisStore {
    let mut options = RedisStoreOptions::new(url);
    options.cluster = cluster;
    options.key_prefix = PREFIX.to_string();
    options.wait_replicas = wait_replicas;
    options.wait_timeout = timeout;
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
async fn a_write_that_too_few_replicas_have_fails_and_its_retry_finds_it() {
    let _lock = SERVER.lock().await;
    for (variable, cluster) in [
        ("STREAMS_REDIS_URL", false),
        ("STREAMS_REDIS_CLUSTER_URL", true),
    ] {
        let Some(url) = server_url(variable) else {
            continue;
        };
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
        let waits = store(&url, cluster, 16, SHORT).await;
        // So the write reloads its script between the script call and its `WAIT`.
        let flush = redis::cmd("SCRIPT").arg("FLUSH").clone();
        if cluster {
            let client = redis::cluster::ClusterClient::new(vec![url.as_str()]).unwrap();
            let mut conn = client.get_async_connection().await.unwrap();
            let _: () = flush.query_async(&mut conn).await.unwrap();
        } else {
            let _: () = flush.query_async(&mut admin(&url).await).await.unwrap();
        }
        let refused = StreamError::from(create(&waits).await.unwrap_err());
        assert_eq!(refused.reason, Reason::Storage, "{refused}");
        let plain = store(&url, cluster, 0, SHORT).await;
        assert_eq!(create(&plain).await.unwrap().outcome, CreateOutcome::Exists);
        if !cluster {
            let count = replicas(&url).await.len();
            if count > 0 {
                let enough = store(&url, cluster, count, LONG).await;
                assert_eq!(
                    create(&enough).await.unwrap().outcome,
                    CreateOutcome::Exists
                );
            }
        }
    }
}

/// A create, resend or close that finds its write done waits for the replicas to have that write.
#[tokio::test]
async fn a_call_that_finds_its_write_waits_for_the_replicas() {
    let _lock = SERVER.lock().await;
    let Some(url) = server_url("STREAMS_REDIS_URL") else {
        return;
    };
    let replicas = replicas(&url).await;
    if replicas.is_empty() {
        let required = std::env::var("STREAMS_REQUIRE").unwrap_or_default();
        assert!(
            !required.split(',').any(|service| service.trim() == "redis"),
            "STREAMS_REQUIRE names redis, but the server at STREAMS_REDIS_URL has no replica"
        );
        eprintln!("give the server at STREAMS_REDIS_URL a replica to run this case");
        return;
    }
    let (namespace, handle) = (
        unique("ns"),
        StreamHandle {
            id: "s".to_string(),
            ..Default::default()
        },
    );
    let plain = store(&url, false, 0, SHORT).await;
    // A Store per call, since a call's own write moves its connection's offset past the replica's.
    let mut waits = vec![];
    for _ in 0..3 {
        waits.push(store(&url, false, replicas.len(), SHORT).await);
    }
    let none = Withdrawal::default;
    let producer = client();
    let records = [b"x".to_vec()];
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
    plain.append(&namespace, &handle.id, &batch).await.unwrap();
    let resent = waits[0]
        .append(&namespace, &handle.id, &batch)
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
        assert_eq!(
            StreamError::from(result.unwrap_err()).reason,
            Reason::Storage
        );
    }
    let enough = store(&url, false, replicas.len(), LONG).await;
    let resent = enough.append(&namespace, &handle.id, &batch).await.unwrap();
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
async fn writes_wait_for_their_replicas_at_once() {
    let _lock = SERVER.lock().await;
    let Some(url) = server_url("STREAMS_REDIS_URL") else {
        return;
    };
    // No test server has this many replicas, so each `WAIT` blocks until the case unblocks it.
    let mut options = RedisStoreOptions::new(&url);
    options.key_prefix = PREFIX.to_string();
    options.wait_replicas = 16;
    options.response_timeout = Duration::from_secs(60);
    options.wait_timeout = Duration::from_secs(30);
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
        let _: i64 = redis::cmd("CLIENT")
            .arg("UNBLOCK")
            .arg(id)
            .query_async(&mut admin)
            .await
            .unwrap();
    }
    for result in writes.join_all().await {
        assert_eq!(
            StreamError::from(result.unwrap_err()).reason,
            Reason::Storage
        );
    }
}

/// The ids of the clients that block in `WAIT`.
async fn waiting_clients(admin: &mut redis::aio::MultiplexedConnection) -> Vec<String> {
    let clients: String = redis::cmd("CLIENT")
        .arg("LIST")
        .query_async(admin)
        .await
        .unwrap();
    clients
        .lines()
        .filter(|client| client.split(' ').any(|field| field == "cmd=wait"))
        .filter_map(|client| {
            client
                .split(' ')
                .find_map(|field| field.strip_prefix("id="))
        })
        .map(str::to_string)
        .collect()
}
