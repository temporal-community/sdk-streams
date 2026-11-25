//! Writes that wait for their replicas.

use crate::{
    cluster::slot_of,
    faults::after_outage,
    reads::until_blocked_in,
    support::{
        Cluster, NoRequestId, Redis, append, create, keys, made, read, reason, runtime, standalone,
        stream_ref,
    },
};
use std::{num::NonZeroUsize, sync::Arc, time::Duration};
use temporalio_streams::{
    AnyReason, StreamError, StreamRuntime,
    proto::{
        StreamErrorReason, StreamIdConflictPolicy,
        sdk::{CreateStreamRequest, CreateStreamResponse},
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
fn assert_not_replicated<T, R>(written: Result<T, StreamError<R>>)
where
    R: From<AnyReason> + PartialEq,
{
    let Err(error) = written else {
        panic!("the write didn't wait for its replica");
    };
    let outage = error.reason == R::from(AnyReason::StoreUnavailable);
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
    let [first, second, third, fourth] = [(); 4].map(|()| waits(standalone(redis.url()), short));
    first.create_no_id(create("s")).await.unwrap();
    first.append_no_id(append("s", &[b"a"])).await.unwrap();

    replica.pause();
    let (batch, batch_id) = (append("s", &[b"b"]), "append-1".to_string());
    assert_not_replicated(first.append_records(batch.clone(), batch_id.clone()).await);
    // A call that finds its write done answers only once the replicas have that write.
    assert_not_replicated(second.append_records(batch.clone(), batch_id.clone()).await);
    let (other, other_id) = (create("other"), "create-1".to_string());
    plain
        .create_stream(other.clone(), other_id.clone())
        .await
        .unwrap();
    assert_not_replicated(third.create_stream(other.clone(), other_id.clone()).await);
    let found = CreateStreamRequest {
        id_conflict_policy: StreamIdConflictPolicy::UseExisting as i32,
        ..create("other")
    };
    assert_not_replicated(fourth.create_no_id(found.clone()).await);

    replica.resume();
    let appended = after_outage(|| second.append_records(batch.clone(), batch_id.clone()))
        .await
        .unwrap();
    let created = after_outage(|| third.create_stream(other.clone(), other_id.clone())).await;
    assert_eq!(created, Ok(made(&other)));
    let found = after_outage(|| fourth.create_no_id(found.clone())).await;
    let existing = CreateStreamResponse {
        r#ref: stream_ref("other"),
        created: false,
    };
    assert_eq!(found, Ok(existing));
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
    store.create_no_id(create("s")).await.unwrap();
    replica.pause();
    let writes = [b"a", b"b", b"c", b"d"].map(|data| {
        let store = store.clone();
        tokio::spawn(async move { store.append_no_id(append("s", &[data])).await })
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
    store.create_no_id(create("s")).await.unwrap();
    // The server closes the connection of the create, which is idle in the pool, as its idle
    // timeout does. The Store sends no write a second time, so the next write is an outage.
    let mut kill = redis::cmd("CLIENT");
    kill.arg("KILL").arg("TYPE").arg("normal");
    let _: i64 = kill.query_async(&mut redis.admin().await).await.unwrap();
    let (batch, batch_id) = (append("s", &[b"a"]), "append-1".to_string());
    let unavailable = StreamErrorReason::StoreUnavailable;
    assert_eq!(
        reason(store.append_records(batch.clone(), batch_id.clone()).await),
        unavailable
    );
    // The retry of the caller runs on a new connection.
    store.append_records(batch, batch_id).await.unwrap();

    // A write that is dropped closes its connection, since its `WAIT` still blocks on the
    // server. So the next write doesn't wait behind it.
    replica.pause();
    let dropped = store.clone();
    let dropped = tokio::spawn(async move { dropped.append_no_id(append("s", &[b"b"])).await });
    until_blocked_in(&redis.url(), "wait", 1).await;
    dropped.abort();
    until_blocked_in(&redis.url(), "wait", 0).await;
    replica.resume();
    store.append_no_id(append("s", &[b"c"])).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Redis"]
async fn a_cluster_write_waits_for_the_replica_of_its_own_node() {
    let cluster = Cluster::start_with_replicas().await;
    let store = waits(cluster.options(), Duration::from_millis(100));
    // The Store connects here, while every node of the cluster answers.
    store.create_no_id(create("first")).await.unwrap();
    // A Stream of the first node, and one of another node.
    let mut streams = [None, None];
    for id in 0.. {
        let [meta, ..] = keys("temporal-streams", &id.to_string());
        let node = cluster.node_of(slot_of(&cluster.nodes[0], &meta).await);
        let on_first = usize::from(node.port != cluster.nodes[0].port);
        streams[on_first].get_or_insert(create(&id.to_string()));
        if streams.iter().all(Option::is_some) {
            break;
        }
    }
    let [Some(first), Some(other)] = streams else {
        unreachable!("the loop ends with both Streams");
    };
    let request_id = "create-1".to_string();
    cluster.replicas[0].pause();
    assert_not_replicated(store.create_stream(first.clone(), request_id.clone()).await);
    // A `WAIT` on any other node would also answer for the first Stream.
    let created = store.create_stream(other.clone(), request_id.clone()).await;
    assert_eq!(created, Ok(made(&other)));
    cluster.replicas[0].resume();
    let retried = after_outage(|| store.create_stream(first.clone(), request_id.clone())).await;
    assert_eq!(retried, Ok(made(&first)));
}
