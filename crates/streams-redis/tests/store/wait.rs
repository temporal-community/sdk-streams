//! Writes that wait for their replicas.

use crate::{
    cluster::slot_of,
    faults::after_outage,
    reads::until_blocked_in,
    support::{Cluster, Redis, append, close, create, keys, read, reason, runtime, standalone},
};
use std::{num::NonZeroUsize, sync::Arc, time::Duration};
use temporalio_streams::{
    AnyReason, Reason, StreamError, StreamRuntime,
    proto::{
        AppendRecordsRequest, CreateStreamRequest, CreateStreamResponse, StreamErrorReason,
        StreamIdConflictPolicy,
    },
};
use temporalio_streams_redis::{RedisStoreOptions, WaitForReplicas};

/// Returns a Store whose writes wait for one replica, for up to `timeout`.
fn waits(mut options: RedisStoreOptions, timeout: Duration) -> StreamRuntime {
    let mut wait = WaitForReplicas::new(NonZeroUsize::MIN);
    wait.timeout = timeout;
    options.wait = Some(wait);
    // Redis ends a `WAIT` at its next timer tick, which can be 100 ms after the timeout.
    options.response_timeout = timeout + Duration::from_secs(5);
    runtime(options)
}

/// Panics unless the write failed because its one replica doesn't have it.
fn assert_not_replicated<T, R>(written: Result<T, StreamError<Reason<R>>>) {
    let Err(error) = written else {
        panic!("the write didn't wait for its replica");
    };
    let outage = matches!(error.reason, Reason::Any(AnyReason::StoreUnavailable));
    let cause = error.message.starts_with("0 of 1 replicas have the write");
    assert!(outage && cause, "{}", error.message);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Redis"]
async fn a_write_waits_for_its_replicas() {
    let redis = Redis::start(&[]);
    let replica = redis.start_replica();
    let plain = runtime(standalone(redis.url()));
    // Each Store has its own connections. A `WAIT` covers the last write of its connection, so
    // a call on the connection of an earlier write would wait for that write anyway.
    let short = Duration::from_millis(100);
    let [first, second, third, fourth, fifth] =
        [(); 5].map(|()| waits(standalone(redis.url()), short));
    first.create_stream(create("s")).await.unwrap();
    first.append_records(append("s", &[b"a"])).await.unwrap();

    replica.pause();
    let batch = AppendRecordsRequest {
        request_id: "append-1".to_string(),
        ..append("s", &[b"b"])
    };
    assert_not_replicated(first.append_records(batch.clone()).await);
    // A call that finds its write done answers only once the replicas have that write.
    assert_not_replicated(second.append_records(batch.clone()).await);
    plain.close_stream(close("s")).await.unwrap();
    assert_not_replicated(third.close_stream(close("s")).await);
    let other = CreateStreamRequest {
        request_id: "create-1".to_string(),
        ..create("other")
    };
    plain.create_stream(other.clone()).await.unwrap();
    assert_not_replicated(fourth.create_stream(other.clone()).await);
    let found = CreateStreamRequest {
        id_conflict_policy: StreamIdConflictPolicy::UseExisting as i32,
        ..create("other")
    };
    assert_not_replicated(fifth.create_stream(found.clone()).await);

    replica.resume();
    let appended = after_outage(|| second.append_records(batch.clone()))
        .await
        .unwrap();
    after_outage(|| third.close_stream(close("s")))
        .await
        .unwrap();
    let created = after_outage(|| fourth.create_stream(other.clone())).await;
    assert_eq!(created, Ok(CreateStreamResponse { created: true }));
    let found = after_outage(|| fifth.create_stream(found.clone())).await;
    assert_eq!(found, Ok(CreateStreamResponse { created: false }));
    let all = plain.read_records(read("s", &[])).await.unwrap();
    let bodies = all
        .records
        .iter()
        .map(|record| record.body.clone().unwrap().data);
    assert_eq!(bodies.collect::<Vec<_>>(), [b"a", b"b"]);
    assert_eq!(all.last_offset, appended.last_offset);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Redis"]
async fn writes_wait_for_their_replicas_at_the_same_time() {
    let redis = Redis::start(&[]);
    let replica = redis.start_replica();
    let store = Arc::new(waits(standalone(redis.url()), Duration::from_secs(60)));
    store.create_stream(create("s")).await.unwrap();
    replica.pause();
    let writes = [b"a", b"b", b"c", b"d"].map(|data| {
        let store = store.clone();
        tokio::spawn(async move { store.append_records(append("s", &[data])).await })
    });
    // Writes on one connection would block in `WAIT` one after the other.
    until_blocked_in(&redis.url(), "wait", 4).await;
    replica.resume();
    for write in writes {
        write.await.unwrap().unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Redis"]
async fn a_write_that_waits_gets_a_usable_connection() {
    let redis = Redis::start(&[]);
    let replica = redis.start_replica();
    let store = Arc::new(waits(standalone(redis.url()), Duration::from_secs(60)));
    store.create_stream(create("s")).await.unwrap();
    // The server closes the connection of the create, which is idle in the pool, as its idle
    // timeout does. The Store sends no write a second time, so the next write is an outage.
    let mut kill = redis::cmd("CLIENT");
    kill.arg("KILL").arg("TYPE").arg("normal");
    let _: i64 = kill.query_async(&mut redis.admin().await).await.unwrap();
    let batch = AppendRecordsRequest {
        request_id: "append-1".to_string(),
        ..append("s", &[b"a"])
    };
    let unavailable = StreamErrorReason::StoreUnavailable;
    assert_eq!(
        reason(store.append_records(batch.clone()).await),
        unavailable
    );
    // The retry of the caller runs on a new connection.
    store.append_records(batch).await.unwrap();

    // A write that is dropped closes its connection, since its `WAIT` still blocks on the
    // server. So the next write doesn't wait behind it.
    replica.pause();
    let dropped = store.clone();
    let dropped = tokio::spawn(async move { dropped.append_records(append("s", &[b"b"])).await });
    until_blocked_in(&redis.url(), "wait", 1).await;
    dropped.abort();
    until_blocked_in(&redis.url(), "wait", 0).await;
    replica.resume();
    store.append_records(append("s", &[b"c"])).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Redis"]
async fn a_cluster_write_waits_for_the_replica_of_its_own_node() {
    let cluster = Cluster::start_with_replicas().await;
    let store = waits(cluster.options(), Duration::from_millis(100));
    // The Store connects here, while every node of the cluster answers.
    store.create_stream(create("first")).await.unwrap();
    // A Stream of the first node, and one of another node.
    let mut streams = [None, None];
    for id in 0.. {
        let [meta, ..] = keys("temporal-streams", &id.to_string());
        let node = cluster.node_of(slot_of(&cluster.nodes[0], &meta).await);
        let on_first = usize::from(node.port != cluster.nodes[0].port);
        streams[on_first].get_or_insert(CreateStreamRequest {
            request_id: "create-1".to_string(),
            ..create(&id.to_string())
        });
        if streams.iter().all(Option::is_some) {
            break;
        }
    }
    let [Some(first), Some(other)] = streams else {
        unreachable!("the loop ends with both Streams");
    };
    cluster.replicas[0].pause();
    assert_not_replicated(store.create_stream(first.clone()).await);
    // A `WAIT` on any other node would also answer for the first Stream.
    let created = Ok(CreateStreamResponse { created: true });
    assert_eq!(store.create_stream(other).await, created);
    cluster.replicas[0].resume();
    let retried = after_outage(|| store.create_stream(first.clone())).await;
    assert_eq!(retried, created);
}
