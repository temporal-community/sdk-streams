//! What the Store answers to a newer format, a lost reply, a server that isn't up yet, and a
//! wrong setup.

use crate::support::{
    Proxy, Redis, append, close, create, describe, free_port, keys, read, reason, runtime, snapshot,
};
use std::{future::Future, time::Duration};
use temporalio_streams::{
    AnyReason, Reason, StreamError,
    proto::{AppendRecordsRequest, CreateStreamRequest, CreateStreamResponse, StreamErrorReason},
};
use temporalio_streams_redis::RedisStoreOptions;
use tokio::time::Instant;

#[tokio::test(flavor = "multi_thread")]
async fn a_newer_format_refuses_every_call() {
    let redis = Redis::start(&[]);
    let mut admin = redis.admin().await;
    let store = runtime(RedisStoreOptions::new(redis.url()));
    store.create_stream(create("s")).await.unwrap();
    store.append_records(append("s", &[b"x"])).await.unwrap();
    let [meta, ..] = keys("temporal-streams", "s");
    let mut newer = redis::cmd("HSET");
    let _: i64 = (newer
        .arg(&meta)
        .arg("v")
        .arg(2)
        .query_async(&mut admin)
        .await)
        .unwrap();
    let before = snapshot(&mut admin, "temporal-streams", "s").await;
    // The Stream may hold what this Store can't read, so no call answers from it.
    let refusal = StreamErrorReason::FormatTooNew;
    assert_eq!(reason(store.create_stream(create("s")).await), refusal);
    let appended = store.append_records(append("s", &[b"y"])).await;
    assert_eq!(reason(appended), refusal);
    assert_eq!(reason(store.read_records(read("s", &[])).await), refusal);
    assert_eq!(reason(store.describe_stream(describe("s")).await), refusal);
    assert_eq!(reason(store.close_stream(close("s")).await), refusal);
    assert_eq!(snapshot(&mut admin, "temporal-streams", "s").await, before);
}

/// Returns the first answer of `call` that is no outage, as a caller that retries gets it.
async fn after_outage<T, R, F>(call: impl Fn() -> F) -> Result<T, StreamError<Reason<R>>>
where
    F: Future<Output = Result<T, StreamError<Reason<R>>>>,
{
    let started = Instant::now();
    loop {
        match call().await {
            Err(error) if matches!(error.reason, Reason::Any(AnyReason::StoreUnavailable)) => {
                assert!(
                    started.elapsed() < Duration::from_secs(20),
                    "{}",
                    error.message
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            answer => return answer,
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lost_reply_is_an_outage_and_its_retry_writes_nothing() {
    let redis = Redis::start(&[]);
    let proxy = Proxy::start(redis.port).await;
    let store = runtime(RedisStoreOptions::new(proxy.url()));
    let unavailable = StreamErrorReason::StoreUnavailable;
    // Redis has each script after these calls, so the next call of one runs it at its first try.
    store.create_stream(create("warm")).await.unwrap();
    store.append_records(append("warm", &[b"x"])).await.unwrap();
    store.close_stream(close("warm")).await.unwrap();

    let request = CreateStreamRequest {
        request_id: "create-1".to_string(),
        ..create("s")
    };
    proxy.lose_next_reply();
    assert_eq!(
        reason(store.create_stream(request.clone()).await),
        unavailable
    );
    let created = after_outage(|| store.create_stream(request.clone())).await;
    assert_eq!(created, Ok(CreateStreamResponse { created: true }));

    let request = AppendRecordsRequest {
        request_id: "append-1".to_string(),
        ..append("s", &[b"a", b"b"])
    };
    proxy.lose_next_reply();
    assert_eq!(
        reason(store.append_records(request.clone()).await),
        unavailable
    );
    let appended = after_outage(|| store.append_records(request.clone()))
        .await
        .unwrap();
    let page = store.read_records(read("s", &[])).await.unwrap();
    let offsets: Vec<_> = page.records.iter().map(|record| &record.offset).collect();
    assert_eq!(offsets, [&appended.first_offset, &appended.last_offset]);

    proxy.lose_next_reply();
    assert_eq!(reason(store.close_stream(close("s")).await), unavailable);
    after_outage(|| store.close_stream(close("s")))
        .await
        .unwrap();
    let page = store
        .read_records(read("s", &appended.last_offset))
        .await
        .unwrap();
    assert!(page.done && page.records.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_store_that_starts_before_redis_connects_later() {
    let port = free_port();
    let store = runtime(RedisStoreOptions::new(format!("redis://127.0.0.1:{port}")));
    let unavailable = StreamErrorReason::StoreUnavailable;
    assert_eq!(reason(store.create_stream(create("s")).await), unavailable);
    let _redis = Redis::start_on(port, &[]);
    let created = store.create_stream(create("s")).await;
    assert_eq!(created, Ok(CreateStreamResponse { created: true }));
}

/// Returns the message of an outage, or panics for an error with another reason.
fn outage<R>(error: StreamError<Reason<R>>) -> String {
    let unavailable = matches!(error.reason, Reason::Any(AnyReason::StoreUnavailable));
    assert!(unavailable, "{}", error.message);
    error.message
}

#[tokio::test(flavor = "multi_thread")]
async fn a_wrong_setup_is_an_outage_that_says_its_cause() {
    let redis = Redis::start(&["--requirepass", "secret"]);
    let url = |user: &str| format!("redis://{user}@127.0.0.1:{}", redis.port);
    let mut admin = crate::support::admin(&url(":secret")).await;
    let _: () = redis::cmd("ACL")
        .arg("SETUSER")
        .arg("no-xadd")
        .arg("on")
        .arg(">pw")
        .arg("~*")
        .arg("+@all")
        .arg("-xadd")
        .query_async(&mut admin)
        .await
        .unwrap();
    let no_password = runtime(RedisStoreOptions::new(redis.url()));
    let message = outage(no_password.create_stream(create("s")).await.unwrap_err());
    assert!(
        message.to_lowercase().contains("authentication"),
        "{message}"
    );

    let wrong_password = runtime(RedisStoreOptions::new(url(":wrong")));
    let message = outage(wrong_password.create_stream(create("s")).await.unwrap_err());
    assert!(
        message.to_lowercase().contains("authentication"),
        "{message}"
    );

    let no_xadd = runtime(RedisStoreOptions::new(url("no-xadd:pw")));
    no_xadd.create_stream(create("s")).await.unwrap();
    let message = outage(
        no_xadd
            .append_records(append("s", &[b"x"]))
            .await
            .unwrap_err(),
    );
    let message = message.to_lowercase();
    assert!(
        message.contains("user") && message.contains("command"),
        "{message}"
    );
}
