//! The cases that only a Redis Cluster has. They are the node of a read that waits, a call that
//! the client sends twice, a slot that moves, and options for the wrong kind of servers.

use crate::{
    reads::{page, until_blocked, waiting},
    support::{
        Cluster, Redis, Rng, append, close, create, describe, keys, read, reason, runtime, seeds,
        standalone,
    },
};
use std::{sync::Arc, time::Duration};
use temporalio_streams::{
    AnyReason, Reason,
    proto::{StreamErrorReason, StreamStatus},
};
use temporalio_streams_redis::{RedisServers, RedisStoreOptions};

const PREFIX: &str = "temporal-streams";

async fn slot_of(node: &Redis, key: &str) -> u16 {
    let mut keyslot = redis::cmd("CLUSTER");
    keyslot.arg("KEYSLOT").arg(key);
    keyslot.query_async(&mut node.admin().await).await.unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_waiting_read_blocks_on_the_node_of_its_stream() {
    let cluster = Cluster::start().await;
    let store = Arc::new(runtime(cluster.options()));
    // The characters of a hash tag and of the Store's own escapes, and one that isn't ASCII.
    let characters = ['a', '7', '{', '}', '%', ':', '-', 'é'];
    for seed in seeds(1) {
        let mut rng = Rng::new(seed);
        for round in 0..20 {
            let text: String = (0..rng.below(12)).map(|_| rng.pick(&characters)).collect();
            let id = format!("{text}{round}");
            store.create_stream(create(&id)).await.unwrap();
            let request = waiting(&id, &[], 60);
            let reader = store.clone();
            let read = tokio::spawn(async move { reader.read_records(request).await });
            // The four keys are in one slot, or the scripts of the Stream would fail. A read
            // that computes another slot never blocks on this node.
            let [_, log, ..] = keys(PREFIX, &id);
            let node = cluster.node_of(slot_of(&cluster.nodes[0], &log).await);
            until_blocked(&node.url(), 1).await;
            let appended = store.append_records(append(&id, &[b"x"])).await.unwrap();
            let got = read.await.unwrap();
            assert_eq!(
                got,
                Ok(page(&appended.first_offset, b"x")),
                "seed {seed}, {id:?}"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cluster_resend_of_one_call_writes_once() {
    let cluster = Cluster::start().await;
    let store = runtime(cluster.options());
    store.create_stream(create("s")).await.unwrap();
    // The node has the append script after this call, so the next append runs at its first try.
    store.append_records(append("s", &[b"a"])).await.unwrap();
    // The cluster connection sends a call again when its connection drops. This batch has no
    // `request_id`, so only its request key tells Redis that the second one is the same call.
    cluster.lose_next_reply();
    let appended = store.append_records(append("s", &[b"b"])).await.unwrap();
    assert!(cluster.lost_the_reply());
    let all = store.read_records(read("s", &[])).await.unwrap();
    let bodies = all
        .records
        .iter()
        .map(|record| record.body.clone().unwrap().data);
    assert_eq!(bodies.collect::<Vec<_>>(), [b"a", b"b"]);
    assert_eq!(all.last_offset, appended.last_offset);
}

async fn cluster_command(node: &Redis, args: &[&str]) {
    let mut command = redis::cmd("CLUSTER");
    let _: () = command
        .arg(args)
        .query_async(&mut node.admin().await)
        .await
        .unwrap();
}

/// Returns the keys that a node has, whatever slot they are in.
async fn keys_on(node: &Redis) -> Vec<String> {
    let mut keys: Vec<String> = redis::cmd("KEYS")
        .arg("*")
        .query_async(&mut node.admin().await)
        .await
        .unwrap();
    keys.sort();
    keys
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stream_is_served_while_its_slot_migrates() {
    let cluster = Cluster::start().await;
    let mut options = cluster.options();
    // The cluster connection tries a call again until this time is up.
    options.response_timeout = Duration::from_secs(1);
    let store = Arc::new(runtime(options));
    let unavailable = StreamErrorReason::StoreUnavailable;
    store.create_stream(create("s")).await.unwrap();
    store.append_records(append("s", &[b"a"])).await.unwrap();

    let [meta, log, producers, closed] = keys(PREFIX, "s");
    let slot = slot_of(&cluster.nodes[0], &meta).await;
    let source = cluster.node_of(slot);
    let target = cluster
        .nodes
        .iter()
        .find(|node| node.port != source.port)
        .unwrap();
    let id_of = async |node: &Redis| -> String {
        let mut my_id = redis::cmd("CLUSTER");
        my_id
            .arg("MYID")
            .query_async(&mut node.admin().await)
            .await
            .unwrap()
    };
    let (source_id, target_id) = (id_of(source).await, id_of(target).await);
    let slot = slot.to_string();
    cluster_command(target, &["SETSLOT", &slot, "IMPORTING", &source_id]).await;
    cluster_command(source, &["SETSLOT", &slot, "MIGRATING", &target_id]).await;

    // A close makes the `closed` key, so its keys exist only in part while the slot moves, on
    // either node. Redis refuses to run it, and nothing of it is written.
    let refused_close = async || {
        let before = (keys_on(source).await, keys_on(target).await);
        assert_eq!(reason(store.close_stream(close("s")).await), unavailable);
        let open = store.describe_stream(describe("s")).await.unwrap();
        assert_eq!(open.status, StreamStatus::Open as i32);
        assert_eq!((keys_on(source).await, keys_on(target).await), before);
        assert!(!before.0.contains(&closed) && !before.1.contains(&closed));
    };

    // The source still has every key of the Stream, so it serves it.
    store.append_records(append("s", &[b"b"])).await.unwrap();
    refused_close().await;

    let _: () = redis::cmd("MIGRATE")
        .arg("127.0.0.1")
        .arg(target.port)
        .arg("")
        .arg(0)
        .arg(5000)
        .arg("KEYS")
        .arg(&[&meta, &log, &producers])
        .query_async(&mut source.admin().await)
        .await
        .unwrap();
    // The target has every key, and the source redirects each call to it.
    let third = store.append_records(append("s", &[b"c"])).await.unwrap();
    let all = store.read_records(read("s", &[])).await.unwrap();
    assert_eq!(all.records.len(), 3);
    assert_eq!(all.last_offset, third.last_offset);
    // A read that waits can't block while the slot moves, so it polls and still gets a Record.
    let request = waiting("s", &third.last_offset, 60);
    let reader = store.clone();
    let waiting_read = tokio::spawn(async move { reader.read_records(request).await });
    let fourth = store.append_records(append("s", &[b"d"])).await.unwrap();
    assert_eq!(
        waiting_read.await.unwrap(),
        Ok(page(&fourth.first_offset, b"d"))
    );
    refused_close().await;

    for node in &cluster.nodes {
        cluster_command(node, &["SETSLOT", &slot, "NODE", &target_id]).await;
    }
    crate::faults::after_outage(|| store.close_stream(close("s")))
        .await
        .unwrap();
    let end = store
        .read_records(read("s", &fourth.last_offset))
        .await
        .unwrap();
    assert!(end.done && end.records.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn the_wrong_kind_of_servers_is_an_outage_that_says_its_cause() {
    let cluster = Cluster::start().await;
    let node = &cluster.nodes[0];
    // A Stream of another node, which this node redirects to.
    let mut id = 0;
    loop {
        let [meta, ..] = keys(PREFIX, &id.to_string());
        if cluster.node_of(slot_of(node, &meta).await).port != node.port {
            break;
        }
        id += 1;
    }
    let one_server = runtime(standalone(node.url()));
    let error = one_server
        .create_stream(create(&id.to_string()))
        .await
        .unwrap_err();
    assert_eq!(error.reason, Reason::Any(AnyReason::StoreUnavailable));
    assert!(
        error.message.contains("needs `RedisServers::Cluster`"),
        "{error}"
    );

    let redis = Redis::start(&[]);
    let options = RedisStoreOptions::new(RedisServers::Cluster(vec![redis.url()]));
    let error = runtime(options)
        .create_stream(create("s"))
        .await
        .unwrap_err();
    assert_eq!(error.reason, Reason::Any(AnyReason::StoreUnavailable));
    assert!(
        error.message.contains("cluster support disabled"),
        "{error}"
    );
}
