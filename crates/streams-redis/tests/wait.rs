//! A Store with `wait_replicas` refuses a write that too few replicas have, even one that it finds
//! done, and its writes wait for their replicas in parallel.

mod support;

use std::time::{Duration, Instant};
use support::server_url;
use temporalio_streams::{
    Reason,
    proto::{Producer, ProducerEpoch, StreamProducerKind, StreamRef},
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
        let stream = StreamRef {
            id: format!("s-{}", std::process::id()),
            ..Default::default()
        };
        let namespace = unique("ns");
        let create = |store: &RedisStore| {
            let (store, namespace, stream) = (store.clone(), namespace.clone(), stream.clone());
            async move {
                store
                    .create(
                        &namespace,
                        &stream,
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
        let refused = create(&waits).await.unwrap_err();
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
    let (namespace, stream) = (
        unique("ns"),
        StreamRef {
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
    let other = StreamRef {
        id: "other".to_string(),
        ..Default::default()
    };
    plain
        .create(&namespace, &stream, None, Owner::Client, &none())
        .await
        .unwrap();
    pause(&replicas).await;
    plain.append(&namespace, &stream.id, &batch).await.unwrap();
    let resent = waits[0]
        .append(&namespace, &stream.id, &batch)
        .await
        .map(|_| ());
    plain
        .close(&namespace, &stream, &producer, b"")
        .await
        .unwrap();
    let closed = waits[1]
        .close(&namespace, &stream, &producer, b"")
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
    let resent = enough.append(&namespace, &stream.id, &batch).await.unwrap();
    assert!(resent.resend);
    let closed = enough
        .close(&namespace, &stream, &producer, b"")
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
    // No test server has this many replicas, so each `WAIT` takes its whole timeout.
    let timeout = Duration::from_millis(500);
    let waits = store(&url, false, 16, timeout).await;
    let namespace = unique("ns");
    // A write that finds no script waits once before the reload and once after, so load it first.
    store(&url, false, 0, SHORT)
        .await
        .create(
            &namespace,
            &StreamRef {
                id: "loads".to_string(),
                ..Default::default()
            },
            None,
            Owner::Client,
            &Withdrawal::default(),
        )
        .await
        .unwrap();
    let start = Instant::now();
    let mut writes = tokio::task::JoinSet::new();
    for i in 0..4 {
        let (waits, namespace) = (waits.clone(), namespace.clone());
        writes.spawn(async move {
            let stream = StreamRef {
                id: format!("s{i}"),
                ..Default::default()
            };
            waits
                .create(
                    &namespace,
                    &stream,
                    None,
                    Owner::Client,
                    &Withdrawal::default(),
                )
                .await
        });
    }
    for result in writes.join_all().await {
        assert_eq!(result.unwrap_err().reason, Reason::Storage);
    }
    // One after the other, the four writes take four timeouts.
    assert!(start.elapsed() < timeout * 3, "{:?}", start.elapsed());
}
