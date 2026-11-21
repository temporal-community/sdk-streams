//! The access rules of `docs/redis.md`. A Redis user with exactly the keys and the commands of
//! its role runs every call of that role.

use crate::{
    cluster::Migration,
    reads::{page, until_blocked, waiting},
    support::{Cluster, Redis, append, close, create, describe, keys, read, reason, runtime},
};
use redis::aio::ConnectionLike;
use std::{collections::BTreeSet, num::NonZeroUsize, sync::Arc};
use temporalio_streams::{
    StreamRuntime,
    proto::{StreamErrorReason, StreamStatus},
};
use temporalio_streams_redis::{RedisServers, RedisStoreOptions, WaitForReplicas};

const DOCS: &str = include_str!("../../docs/redis.md");
const PREFIX: &str = "acl";

/// How the access rule of each role starts in the docs.
const PRODUCER: &str = "A Producer creates";
const READER: &str = "A Reader reads";
const CLUSTER: &str = "On a Redis Cluster, each user";

/// Returns the commands that one access rule of the docs lists, each with its `+`. A rule is a
/// list item, and `start` is how it starts. The commands are those of its first sentence that
/// has any, or with `also`, those of its sentence that has that word.
fn documented(start: &str, also: Option<&str>) -> Vec<String> {
    let rule = DOCS.split("\n- ").find(|rule| rule.starts_with(start));
    let rule = rule
        .unwrap()
        .split("\n\n")
        .next()
        .unwrap()
        .replace('\n', " ");
    let mut sentences = rule.split(". ");
    let sentence = match also {
        None => sentences.find(|sentence| sentence.contains('+')),
        Some(word) => sentences.find(|sentence| sentence.contains(word)),
    };
    let words = sentence.unwrap().split_whitespace();
    let words = words.map(|word| word.trim_matches(['`', '.']));
    let commands = words.filter(|word| word.starts_with('+'));
    commands.map(str::to_string).collect()
}

/// Returns the Redis commands that a script calls, as the access rules write them.
fn called(script: &str) -> BTreeSet<String> {
    let calls = script.split("redis.call('").skip(1);
    let commands = calls.map(|call| format!("+{}", call.split('\'').next().unwrap()));
    commands.map(|command| command.to_lowercase()).collect()
}

#[test]
fn each_script_calls_the_commands_that_the_docs_list() {
    let state = include_str!("../../lua/state.lua");
    let writes = [
        state,
        include_str!("../../lua/write.lua"),
        include_str!("../../lua/create.lua"),
        include_str!("../../lua/append.lua"),
        include_str!("../../lua/close.lua"),
    ];
    let reads = [
        state,
        include_str!("../../lua/read.lua"),
        include_str!("../../lua/describe.lua"),
    ];
    // The Store sends these itself, to run a script and to load one that Redis lacks.
    let sent = |command: &String| command.starts_with("+evalsha") || command == "+script|load";
    for (role, scripts) in [(PRODUCER, &writes[..]), (READER, &reads[..])] {
        let listed = documented(role, None).into_iter().filter(|c| !sent(c));
        let listed: BTreeSet<String> = listed.collect();
        let called: BTreeSet<String> = scripts.iter().flat_map(|script| called(script)).collect();
        assert_eq!(listed, called, "{role}");
    }
}

/// Makes a Redis user with the password `pw`, a key pattern and the commands of the given rules.
async fn user(admin: &mut impl ConnectionLike, name: &str, keys: &str, rules: &[&Vec<String>]) {
    let mut acl = redis::cmd("ACL");
    acl.arg("SETUSER")
        .arg(name)
        .arg("reset")
        .arg("on")
        .arg(">pw");
    acl.arg(format!("{keys}{PREFIX}:*"));
    for commands in rules {
        acl.arg(commands);
    }
    let _: () = acl.query_async(admin).await.unwrap();
}

fn store(servers: RedisServers, wait: Option<WaitForReplicas>) -> Arc<StreamRuntime> {
    let mut options = RedisStoreOptions::new(servers);
    options.key_prefix = PREFIX.parse().unwrap();
    options.wait = wait;
    Arc::new(runtime(options))
}

/// Runs every call of the two roles on a Stream, each with the Store of its role.
async fn run_roles(producer: &StreamRuntime, reader: &Arc<StreamRuntime>, blocks_on: &str) {
    producer.create_stream(create("s")).await.unwrap();
    let request = waiting("s", &[], 60);
    let waiting_reader = reader.clone();
    let read_ahead = tokio::spawn(async move { waiting_reader.read_records(request).await });
    until_blocked(blocks_on, 1).await;
    let first = producer.append_records(append("s", &[b"a"])).await.unwrap();
    assert_eq!(
        read_ahead.await.unwrap(),
        Ok(page(&first.first_offset, b"a"))
    );
    let described = reader.describe_stream(describe("s")).await.unwrap();
    assert_eq!(described.last_offset, first.last_offset);
    producer.close_stream(close("s")).await.unwrap();
    let end = reader
        .read_records(read("s", &first.last_offset))
        .await
        .unwrap();
    assert!(end.done);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Redis"]
async fn a_user_with_the_documented_access_runs_its_role() {
    let redis = Redis::start(&[]);
    let _replica = redis.start_replica();
    let mut admin = redis.admin().await;
    let url = |user: &str| format!("redis://{user}:pw@127.0.0.1:{}", redis.port);
    let store =
        |user: &str, wait| store(RedisServers::Standalone(url(user).parse().unwrap()), wait);
    let (writes, reads) = (documented(PRODUCER, None), documented(READER, None));
    let waits = documented(PRODUCER, Some("wait"));
    let blocks = documented(READER, Some("waits"));
    user(&mut admin, "producer", "~", &[&writes, &waits]).await;
    user(&mut admin, "reader", "%R~", &[&reads, &blocks]).await;
    let producer = store("producer", Some(WaitForReplicas::new(NonZeroUsize::MIN)));
    run_roles(&producer, &store("reader", None), &redis.url()).await;

    // Read access to the keys doesn't let a user write, whatever its commands are.
    user(&mut admin, "read-only", "%R~", &[&writes]).await;
    let read_only = store("read-only", None);
    let unavailable = StreamErrorReason::StoreUnavailable;
    assert_eq!(
        reason(read_only.create_stream(create("other")).await),
        unavailable
    );
    let [meta, ..] = keys(PREFIX, "other");
    let exists: bool = redis::cmd("EXISTS")
        .arg(meta)
        .query_async(&mut admin)
        .await
        .unwrap();
    assert!(!exists);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Redis"]
async fn a_user_with_the_documented_access_runs_its_role_on_a_cluster() {
    let cluster = Cluster::start().await;
    let on_cluster = documented(CLUSTER, None);
    let (writes, reads) = (documented(PRODUCER, None), documented(READER, None));
    let blocks = documented(READER, Some("waits"));
    // Each node has its own users.
    for node in &cluster.nodes {
        let mut admin = node.admin().await;
        user(&mut admin, "producer", "~", &[&writes, &on_cluster]).await;
        user(&mut admin, "reader", "%R~", &[&reads, &blocks, &on_cluster]).await;
    }
    let store = |user: &str| {
        let url = format!("redis://{user}:pw@127.0.0.1:{}", cluster.seed_port());
        let (first, rest) = (url.parse().unwrap(), Vec::new());
        store(RedisServers::Cluster { first, rest }, None)
    };
    let (producer, reader) = (store("producer"), store("reader"));
    let [_, log, ..] = keys(PREFIX, "s");
    let node = cluster.node_of(crate::cluster::slot_of(&cluster.nodes[0], &log).await);
    run_roles(&producer, &reader, &node.url()).await;

    // While a slot migrates, the node that gives it away sends a call to the other node, and the
    // call needs one more command there.
    producer.create_stream(create("moves")).await.unwrap();
    let first = producer
        .append_records(append("moves", &[b"a"]))
        .await
        .unwrap();
    let [moved_meta, moved_log, moved_producers, _] = keys(PREFIX, "moves");
    let migration = Migration::start(&cluster, &moved_meta).await;
    migration
        .move_keys(&[&moved_meta, &moved_log, &moved_producers])
        .await;
    let second = producer
        .append_records(append("moves", &[b"b"]))
        .await
        .unwrap();
    let after = reader.read_records(read("moves", &first.last_offset)).await;
    assert_eq!(after, Ok(page(&second.first_offset, b"b")));
    let described = reader.describe_stream(describe("moves")).await.unwrap();
    assert_eq!(described.status, StreamStatus::Open as i32);
    migration.finish().await;
}
