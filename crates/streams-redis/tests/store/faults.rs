//! What the Store answers to a newer format, a lost reply, a server that isn't up yet, and a
//! wrong setup.

use crate::support::{
    NoRequestId, Proxy, Redis, append, create, describe, free_port, keys, made, read, reason,
    runtime, snapshot, standalone,
};
use std::{future::Future, time::Duration};
use temporalio_streams::{AnyReason, StreamError, proto::StreamErrorReason};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::Instant,
};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Redis"]
async fn a_newer_format_refuses_every_call() {
    let redis = Redis::start(&[]);
    let mut admin = redis.admin().await;
    let store = runtime(standalone(redis.url()));
    store.create_no_id(create("s")).await.unwrap();
    store.append_no_id(append("s", &[b"x"])).await.unwrap();
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
    assert_eq!(reason(store.create_no_id(create("s")).await), refusal);
    let appended = store.append_no_id(append("s", &[b"y"])).await;
    assert_eq!(reason(appended), refusal);
    assert_eq!(reason(store.read_records(read("s", &[])).await), refusal);
    assert_eq!(reason(store.describe_stream(describe("s")).await), refusal);
    assert_eq!(snapshot(&mut admin, "temporal-streams", "s").await, before);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Redis"]
async fn an_append_to_a_stream_with_a_broken_retention_writes_nothing() {
    let redis = Redis::start(&[]);
    let mut admin = redis.admin().await;
    let store = runtime(standalone(redis.url()));
    let [meta, ..] = keys("temporal-streams", "s");
    for broken in [None, Some("seven days")] {
        let _: i64 = redis::cmd("DEL")
            .arg(&keys("temporal-streams", "s"))
            .query_async(&mut admin)
            .await
            .unwrap();
        store.create_no_id(create("s")).await.unwrap();
        store.append_no_id(append("s", &[b"x"])).await.unwrap();
        let mut change = match broken {
            None => redis::cmd("HDEL"),
            Some(_) => redis::cmd("HSET"),
        };
        change.arg(&meta).arg("ret").arg(broken);
        let _: i64 = change.query_async(&mut admin).await.unwrap();
        let before = snapshot(&mut admin, "temporal-streams", "s").await;
        let appended = store.append_no_id(append("s", &[b"y"])).await;
        assert_eq!(
            reason(appended),
            StreamErrorReason::StoreCorrupt,
            "{broken:?}"
        );
        let after = snapshot(&mut admin, "temporal-streams", "s").await;
        assert_eq!(after, before, "{broken:?}");
    }
}

/// Returns the first answer of `call` that is no outage, as a caller that retries gets it.
pub(crate) async fn after_outage<T, R, F>(call: impl Fn() -> F) -> Result<T, StreamError<R>>
where
    R: From<AnyReason> + PartialEq,
    F: Future<Output = Result<T, StreamError<R>>>,
{
    let started = Instant::now();
    loop {
        match call().await {
            Err(error) if error.reason == R::from(AnyReason::StoreUnavailable) => {
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
#[ignore = "needs Redis"]
async fn a_lost_reply_is_an_outage_and_its_retry_writes_nothing() {
    let redis = Redis::start(&[]);
    let proxy = Proxy::start(redis.port).await;
    let store = runtime(standalone(proxy.url()));
    let unavailable = StreamErrorReason::StoreUnavailable;
    // Redis has each script after these calls, so the next call of one runs it at its first try.
    store.create_no_id(create("warm")).await.unwrap();
    store.append_no_id(append("warm", &[b"x"])).await.unwrap();

    let (request, request_id) = (create("s"), "create-1".to_string());
    proxy.lose_next_reply();
    assert_eq!(
        reason(
            store
                .create_stream(request.clone(), request_id.clone())
                .await
        ),
        unavailable
    );
    let created = after_outage(|| store.create_stream(request.clone(), request_id.clone())).await;
    assert_eq!(created, Ok(made(&request)));

    let (request, request_id) = (append("s", &[b"a", b"b"]), "append-1".to_string());
    proxy.lose_next_reply();
    assert_eq!(
        reason(
            store
                .append_records(request.clone(), request_id.clone())
                .await
        ),
        unavailable
    );
    let appended = after_outage(|| store.append_records(request.clone(), request_id.clone()))
        .await
        .unwrap();
    let page = store.read_records(read("s", &[])).await.unwrap();
    let offsets: Vec<_> = page.records.iter().map(|record| &record.offset).collect();
    assert_eq!(offsets, [&appended.first_offset, &appended.last_offset]);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Redis"]
async fn a_store_that_starts_before_redis_connects_later() {
    let port = free_port();
    let store = runtime(standalone(format!("redis://127.0.0.1:{port}")));
    let unavailable = StreamErrorReason::StoreUnavailable;
    assert_eq!(reason(store.create_no_id(create("s")).await), unavailable);
    let _redis = Redis::start_on(port, &[]);
    let created = store.create_no_id(create("s")).await;
    assert_eq!(created, Ok(made(&create("s"))));
}

/// Returns the message of an outage, or panics for an error with another reason.
fn outage<R: From<AnyReason> + PartialEq>(error: StreamError<R>) -> String {
    let unavailable = error.reason == R::from(AnyReason::StoreUnavailable);
    assert!(unavailable, "{}", error.message);
    error.message
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Redis"]
async fn a_wrong_setup_is_an_outage_that_says_its_cause() {
    let redis = Redis::start(&[]);
    let url = |user: &str| format!("redis://{user}@127.0.0.1:{}", redis.port);
    let mut admin = redis.admin().await;
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
    // The server gets its password here, since the test helper asks a new server for its id.
    let mut password = redis::cmd("CONFIG");
    password.arg("SET").arg("requirepass").arg("secret");
    let _: () = password.query_async(&mut admin).await.unwrap();
    let no_password = runtime(standalone(redis.url()));
    let message = outage(no_password.create_no_id(create("s")).await.unwrap_err());
    assert!(
        message.to_lowercase().contains("authentication"),
        "{message}"
    );

    let wrong_password = runtime(standalone(url(":wrong")));
    let message = outage(wrong_password.create_no_id(create("s")).await.unwrap_err());
    assert!(
        message.to_lowercase().contains("authentication"),
        "{message}"
    );

    let no_xadd = runtime(standalone(url("no-xadd:pw")));
    no_xadd.create_no_id(create("s")).await.unwrap();
    let message = outage(
        no_xadd
            .append_no_id(append("s", &[b"x"]))
            .await
            .unwrap_err(),
    );
    let message = message.to_lowercase();
    assert!(
        message.contains("user") && message.contains("command"),
        "{message}"
    );

    // A URL that points at another service, which answers in its own protocol.
    let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let not_redis = format!("redis://{}", http.local_addr().unwrap());
    tokio::spawn(async move {
        while let Ok((mut client, _)) = http.accept().await {
            let mut request = [0; 1024];
            let _ = client.read(&mut request).await;
            let _ = client.write_all(b"HTTP/1.1 400 Bad Request\r\n\r\n").await;
            // A close with unread bytes would reset the connection before the Store reads.
            while let Ok(1..) = client.read(&mut request).await {}
        }
    });
    let not_redis = runtime(standalone(not_redis));
    let message = outage(not_redis.create_no_id(create("s")).await.unwrap_err());
    assert!(message.contains("another service"), "{message}");
}
