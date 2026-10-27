//! The blocking reads of a Store whose pool has one connection per node. Standalone Redis only,
//! since the pool is per node and a cluster spreads the Streams over its nodes.

mod support;

use std::time::Duration;
use support::server_url;
use temporalio_streams::ids::StreamKeys;
use temporalio_streams_redis::{RedisStore, RedisStoreOptions};
use tokio::time::Instant;

const PREFIX: &str = "streams-blocking";

async fn store(url: &str) -> RedisStore {
    let mut options = RedisStoreOptions::new(url);
    options.key_prefix = PREFIX.to_string();
    options.blocking_reads_per_node = 1;
    RedisStore::connect(options).await.unwrap()
}

fn namespace() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("ns-{nanos}")
}

async fn add_entry(url: &str, namespace: &str, stream: &str) {
    let mut conn = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let _: String = redis::cmd("XADD")
        .arg(StreamKeys::new(PREFIX, namespace, stream).log())
        .arg("*")
        .arg("x")
        .arg("y")
        .query_async(&mut conn)
        .await
        .unwrap();
}

#[tokio::test]
async fn a_read_with_no_free_connection_ends_before_its_deadline() {
    let Some(url) = server_url("STREAMS_REDIS_URL") else {
        return;
    };
    let (store, namespace) = (store(&url).await, namespace());
    let first = {
        let (store, namespace) = (store.clone(), namespace.clone());
        let deadline = Instant::now() + Duration::from_millis(1800);
        tokio::spawn(async move { store.wait_after(&namespace, "a", 0, deadline).await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    let start = Instant::now();
    let woke = store
        .wait_after(&namespace, "b", 0, start + Duration::from_secs(3))
        .await
        .unwrap();
    assert!(!woke);
    let waited = start.elapsed();
    assert!(
        waited > Duration::from_millis(800) && waited < Duration::from_millis(1500),
        "{waited:?}"
    );
    assert!(!first.await.unwrap().unwrap());
}

#[tokio::test]
async fn a_cancelled_read_holds_up_no_later_read() {
    let Some(url) = server_url("STREAMS_REDIS_URL") else {
        return;
    };
    let (store, namespace) = (store(&url).await, namespace());
    let cancelled = store.wait_after(&namespace, "a", 0, Instant::now() + Duration::from_secs(5));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), cancelled)
            .await
            .is_err()
    );
    let start = Instant::now();
    let read = store.wait_after(&namespace, "b", 0, start + Duration::from_secs(3));
    let append = async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        add_entry(&url, &namespace, "b").await;
    };
    let (woke, ()) = tokio::join!(read, append);
    assert!(woke.unwrap());
    assert!(
        start.elapsed() < Duration::from_secs(1),
        "{:?}",
        start.elapsed()
    );
}
