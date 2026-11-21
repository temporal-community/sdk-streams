//! Reads that wait for a Record.

use crate::support::{
    Redis, admin, append, client, close, create, read, record, runtime, standalone,
};
use redis::aio::MultiplexedConnection;
use std::{sync::Arc, time::Duration};
use temporalio_streams::{
    StreamRuntime,
    proto::{ReadRecordsRequest, ReadRecordsResponse, StreamRecord},
};
use tokio::time::Instant;

/// Returns a read that waits for up to `seconds`.
pub(crate) fn waiting(stream_id: &str, from_offset: &[u8], seconds: i64) -> ReadRecordsRequest {
    let mut request = read(stream_id, from_offset);
    request.wait = Some(Default::default());
    request.wait.as_mut().unwrap().seconds = seconds;
    request
}

/// Returns the number of clients that block in `XREAD` on the server.
async fn blocked_reads(admin: &mut MultiplexedConnection) -> usize {
    let clients: String = redis::cmd("CLIENT")
        .arg("LIST")
        .query_async(admin)
        .await
        .unwrap();
    let blocked = |client: &&str| client.contains(" cmd=xread ") && client.contains(" flags=b");
    clients.lines().filter(blocked).count()
}

/// Waits until `count` clients block in `XREAD`, so the next write of the test finds them there.
pub(crate) async fn until_blocked(url: &str, count: usize) {
    let mut admin = admin(url).await;
    let started = Instant::now();
    while blocked_reads(&mut admin).await != count {
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "no read blocked"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Returns a page with one Record of the test's client.
pub(crate) fn page(offset: &[u8], data: &[u8]) -> ReadRecordsResponse {
    ReadRecordsResponse {
        records: vec![StreamRecord {
            producer: Some(client("client:test")),
            body: record(data).body,
            offset: offset.to_vec(),
        }],
        last_offset: offset.to_vec(),
        done: false,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_waiting_read_wakes_on_an_append_and_on_a_close() {
    let redis = Redis::start(&[]);
    let store = Arc::new(runtime(standalone(redis.url())));
    store.create_stream(create("s")).await.unwrap();
    let spawn = |request| {
        let store = store.clone();
        tokio::spawn(async move { store.read_records(request).await })
    };

    let read = spawn(waiting("s", &[], 60));
    until_blocked(&redis.url(), 1).await;
    let first = store.append_records(append("s", &[b"a"])).await.unwrap();
    assert_eq!(read.await.unwrap(), Ok(page(&first.first_offset, b"a")));

    // An append that comes at any moment of a read wakes it, also before the read blocks.
    let mut from = first.last_offset;
    for round in 0..20 {
        let read = spawn(waiting("s", &from, 60));
        if round % 2 == 0 {
            tokio::task::yield_now().await;
        }
        let appended = store.append_records(append("s", &[b"b"])).await.unwrap();
        assert_eq!(read.await.unwrap(), Ok(page(&appended.first_offset, b"b")));
        from = appended.last_offset;
    }

    let read = spawn(waiting("s", &from, 60));
    until_blocked(&redis.url(), 1).await;
    store.close_stream(close("s")).await.unwrap();
    let done = ReadRecordsResponse {
        records: vec![],
        last_offset: from,
        done: true,
    };
    assert_eq!(read.await.unwrap(), Ok(done));

    // With nothing to wake it, a read ends at its deadline with an empty page.
    store.create_stream(create("quiet")).await.unwrap();
    let mut request = read_for_millis("quiet", 300);
    let started = Instant::now();
    let empty = store.read_records(request.clone()).await.unwrap();
    assert_eq!(empty, ReadRecordsResponse::default());
    assert!(started.elapsed() >= Duration::from_millis(300));
    // A read with no `wait` answers immediately, so a caller can poll.
    request.wait = None;
    assert_eq!(
        store.read_records(request).await,
        Ok(ReadRecordsResponse::default())
    );
}

fn read_for_millis(stream_id: &str, millis: i32) -> ReadRecordsRequest {
    let mut request = read(stream_id, &[]);
    request.wait = Some(Default::default());
    request.wait.as_mut().unwrap().nanos = millis * 1_000_000;
    request
}

#[tokio::test(flavor = "multi_thread")]
async fn reads_over_the_connection_limit_still_get_their_records() {
    let redis = Redis::start(&[]);
    let mut options = standalone(redis.url());
    options.max_blocking_connections = 1;
    let store: Arc<StreamRuntime> = Arc::new(runtime(options));
    store.create_stream(create("s")).await.unwrap();
    let spawn = |request| {
        let store = store.clone();
        tokio::spawn(async move { store.read_records(request).await })
    };

    let readers = [spawn(waiting("s", &[], 60)), spawn(waiting("s", &[], 60))];
    until_blocked(&redis.url(), 1).await;
    let appended = store.append_records(append("s", &[b"a"])).await.unwrap();
    for reader in readers {
        assert_eq!(
            reader.await.unwrap(),
            Ok(page(&appended.first_offset, b"a"))
        );
    }

    // A read that is dropped closes its connection, since its `XREAD` still blocks on the server.
    // The place is free for the next read.
    let dropped = spawn(waiting("s", &appended.last_offset, 60));
    until_blocked(&redis.url(), 1).await;
    dropped.abort();
    until_blocked(&redis.url(), 0).await;
    let read = spawn(waiting("s", &appended.last_offset, 60));
    until_blocked(&redis.url(), 1).await;
    let next = store.append_records(append("s", &[b"b"])).await.unwrap();
    assert_eq!(read.await.unwrap(), Ok(page(&next.first_offset, b"b")));
}

/// Returns the bytes that the server has sent to its clients so far.
async fn bytes_sent(admin: &mut MultiplexedConnection) -> usize {
    let stats: String = redis::cmd("INFO")
        .arg("stats")
        .query_async(admin)
        .await
        .unwrap();
    let sent = stats
        .lines()
        .find_map(|line| line.strip_prefix("total_net_output_bytes:"));
    sent.unwrap().parse().unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_with_a_byte_limit_takes_little_more_from_redis() {
    let redis = Redis::start(&[]);
    let mut admin = redis.admin().await;
    let store = runtime(standalone(redis.url()));
    store.create_stream(create("s")).await.unwrap();
    // 300 Records of 2 KiB, which is 30 times the limit of the read.
    let body = [7; 2048];
    for _ in 0..3 {
        let batch = append("s", &[&body[..]; 100]);
        store.append_records(batch).await.unwrap();
    }
    let limit = 20 * 1024;
    let mut request = read("s", &[]);
    request.max_bytes = limit as i64;
    let before = bytes_sent(&mut admin).await;
    let page = store.read_records(request).await.unwrap();
    let taken = bytes_sent(&mut admin).await - before;
    let size = prost::encoding::message::encoded_len(1, &page.records[0]);
    assert_eq!(page.records.len(), limit / size);
    // The script stops at the limit too, so Redis doesn't send the rest of the Stream for one
    // page. The count has the answers to the test's own two commands, which are small.
    assert!(
        taken < 2 * limit,
        "Redis sent {taken} bytes for a page of {limit}"
    );
}
