//! The Redis users of `docs/redis.md`, with exactly the key access and commands it lists per role,
//! run every Store call of that role, on a standalone server and on a cluster whose slot migrates.

use prost::Message;
use std::{
    num::NonZeroUsize,
    ops::RangeInclusive,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use temporalio_common::protos::temporal::api::common::v1::Payload;
use temporalio_streams::{
    Reason,
    digest::batch_digest,
    ids::{StreamKeys, client_producer_id},
    offset::offset_of_entry_id,
    proto::{
        AppendRecord, Producer, ProducerEpoch, StreamData, StreamHandle, StreamProducerKind,
        StreamRecord, stream_record::Item,
    },
    store::{Batch, Owner, Store, Withdrawal},
};
use temporalio_streams_redis::{
    RedisStore, RedisStoreError, RedisStoreOptions, Topology, WaitOptions,
    testing::{SlotNode, SlotRange, cluster_url, standalone_url},
};

const PREFIX: &str = "streams-acl";

/// The key access and the commands that `docs/redis.md` lists for a Producer and for a Reader.
const PRODUCER: (&str, &str) = (
    "~",
    "+evalsha +script|load +time +hget +hset +hdel +exists +pexpireat +xadd +xtrim +xrevrange \
     +zadd +zrangebyscore +zrem +wait",
);
const READER: (&str, &str) = (
    "%R~",
    "+evalsha_ro +script|load +time +hmget +xrange +xread +xrevrange",
);
/// The commands that `docs/redis.md` adds for each role on a cluster.
const CLUSTER: &str = "+cluster|slots +asking +ping";

async fn admin(url: &str) -> redis::aio::MultiplexedConnection {
    redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}

/// The `ACL SETUSER` of a user with a role's key access and commands on the keys of `namespace`.
fn set_user(name: &str, namespace: &str, (keys, commands): (&str, &str)) -> redis::Cmd {
    let mut acl = redis::cmd("ACL");
    acl.arg("SETUSER")
        .arg(name)
        .arg("reset")
        .arg("on")
        .arg(">pw")
        .arg(format!("{keys}{PREFIX}:{{{namespace}:*"));
    for command in commands.split_whitespace() {
        acl.arg(command);
    }
    acl
}

/// The URL that logs in to `url` as the user `name`.
fn login(url: &str, name: &str) -> String {
    let rest = url.split_once("://").map_or(url, |(_, rest)| rest);
    format!("redis://{name}:pw@{rest}")
}

/// A user with a role's key access and commands on the keys of `namespace`, and the URL that logs
/// in as it.
async fn user(url: &str, name: &str, namespace: &str, role: (&str, &str)) -> String {
    let _: () = set_user(name, namespace, role)
        .query_async(&mut admin(url).await)
        .await
        .unwrap();
    login(url, name)
}

async fn store(url: String) -> Box<dyn Store> {
    store_with(RedisStoreOptions::new(url)).await
}

async fn store_with(mut options: RedisStoreOptions) -> Box<dyn Store> {
    options.key_prefix = PREFIX.parse().unwrap();
    Box::new(RedisStore::connect(options).await.unwrap())
}

fn client() -> Producer {
    client_named("p")
}

fn unique() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

/// The encoded Records of a one-Record batch of `client()`, and the batch digest.
fn one_record() -> (Vec<Vec<u8>>, String) {
    let records = vec![AppendRecord {
        body: Some(Payload {
            data: b"x".to_vec(),
            ..Default::default()
        }),
    }];
    let stored = StreamRecord {
        item: Some(Item::Data(StreamData {
            producer: Some(client()),
            body: records[0].body.clone(),
        })),
        ..Default::default()
    };
    (vec![stored.encode_to_vec()], batch_digest(&records))
}

fn client_named(process: &str) -> Producer {
    Producer {
        kind: StreamProducerKind::Client as i32,
        id: client_producer_id("me", process),
        epoch: Some(ProducerEpoch {
            attempt: 1,
            ..Default::default()
        }),
        run_id: String::new(),
    }
}

#[tokio::test]
#[ignore = "needs Redis"]
async fn the_documented_commands_run_every_call_of_each_role() {
    let url = standalone_url();
    let unique = unique();
    let namespace = format!("ns-{unique}");
    let (producer_name, reader_name) = (format!("prod-{unique}"), format!("read-{unique}"));
    let read_only_name = format!("ro-{unique}");
    let handle = StreamHandle {
        id: "s".to_string(),
        ..Default::default()
    };
    // With no cached script, each user loads the scripts it runs.
    let _: () = redis::cmd("SCRIPT")
        .arg("FLUSH")
        .query_async(&mut admin(&url).await)
        .await
        .unwrap();

    let producer = store(user(&url, &producer_name, &namespace, PRODUCER).await).await;
    producer
        .create(
            &namespace,
            &handle,
            None,
            Owner::Client,
            &Withdrawal::default(),
        )
        .await
        .unwrap();
    let (stored, digest) = one_record();
    let batch = Batch {
        producer: &client(),
        sequence: 1,
        records: &stored,
        digest: &digest,
        withdrawal: Withdrawal::default(),
    };
    let appended = producer.append(&namespace, &handle, &batch).await.unwrap();
    let (first, _) = appended.offsets.unwrap();

    let reader = store(user(&url, &reader_name, &namespace, READER).await).await;
    let (state, entries) = reader.read(&namespace, "s", 0, 10).await.unwrap();
    assert_eq!((state.newest, entries.len()), (first, 1));
    assert_eq!(
        reader
            .read_back(&namespace, "s", first, 10)
            .await
            .unwrap()
            .len(),
        1
    );
    // The log has an entry after 0, so the Reader's blocking read wakes at once.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    assert!(
        reader
            .wait_after(&namespace, "s", 0, deadline)
            .await
            .unwrap()
    );
    // A user with read access to the keys can't write them, even with the Producer's commands.
    let read_only = (READER.0, PRODUCER.1);
    let read_only = store(user(&url, &read_only_name, &namespace, read_only).await).await;
    let refused = read_only
        .append(
            &namespace,
            &handle,
            &Batch {
                sequence: 2,
                ..batch.clone()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(refused.reason, Reason::Invalid, "{refused}");
    assert!(!refused.message.contains("evalsha"), "{refused}");

    producer
        .close(&namespace, &handle, &client(), b"")
        .await
        .unwrap();

    // An append trims the Records older than the Retention, and prunes the Producers with no
    // append for one Retention. The scripts read the server's `TIME`, so raw writes age an entry
    // and a Producer, and the case doesn't wait.
    let short = StreamHandle {
        id: "short".to_string(),
        ..Default::default()
    };
    let retention = Duration::from_secs(60);
    // A Workflow Stream has the safety TTL, so it doesn't expire with the Retention.
    producer
        .create(
            &namespace,
            &short,
            Some(retention),
            Owner::Workflow,
            &Withdrawal::default(),
        )
        .await
        .unwrap();
    let keys = StreamKeys::new(PREFIX, &namespace, "short");
    let mut raw = admin(&url).await;
    let (seconds, _): (u64, u64) = redis::cmd("TIME").query_async(&mut raw).await.unwrap();
    let old_ms = (seconds - 2 * retention.as_secs()) * 1000;
    let old = format!("{old_ms}-1");
    let fence = format!(
        "e|{}|{}",
        StreamProducerKind::Client as i32,
        client_named("old").id
    );
    let _: () = redis::pipe()
        .cmd("XADD")
        .arg(keys.log())
        .arg(&old)
        .arg("x")
        .arg("y")
        .ignore()
        .cmd("ZADD")
        .arg(keys.producers())
        .arg(old_ms)
        .arg(&fence[2..])
        .ignore()
        .cmd("HSET")
        .arg(keys.meta())
        .arg(&fence)
        .arg("x")
        .ignore()
        .query_async(&mut raw)
        .await
        .unwrap();
    producer.append(&namespace, &short, &batch).await.unwrap();
    let (state, entries) = reader.read(&namespace, "short", 0, 10).await.unwrap();
    assert_eq!(
        (state.trimmed, entries.len()),
        (offset_of_entry_id(&old).unwrap(), 1)
    );
    let fenced: bool = redis::cmd("HEXISTS")
        .arg(keys.meta())
        .arg(&fence)
        .query_async(&mut raw)
        .await
        .unwrap();
    assert!(!fenced);

    // With `+wait`, too few replicas is a Store failure, not a refusal.
    let mut options =
        RedisStoreOptions::new(user(&url, &producer_name, &namespace, PRODUCER).await);
    options.wait = Some(WaitOptions {
        replicas: NonZeroUsize::new(16).unwrap(),
        timeout: Duration::from_millis(50),
    });
    let waits = store_with(options).await;
    let failed = waits
        .create(
            &namespace,
            &StreamHandle {
                id: "waits".to_string(),
                ..Default::default()
            },
            None,
            Owner::Client,
            &Withdrawal::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(failed.reason, Reason::Storage, "{failed}");
    // Without `+wait`, each write would run its script and then fail, so connect refuses it.
    let no_wait = (PRODUCER.0, PRODUCER.1.trim_end_matches(" +wait"));
    let mut options = RedisStoreOptions::new(user(&url, &producer_name, &namespace, no_wait).await);
    options.key_prefix = PREFIX.parse().unwrap();
    options.wait = Some(WaitOptions {
        replicas: NonZeroUsize::MIN,
        timeout: Duration::from_millis(50),
    });
    let refused = RedisStore::connect(options).await.unwrap_err();
    assert!(matches!(refused, RedisStoreError::Invalid(_)), "{refused}");

    let mut conn = admin(&url).await;
    for name in [producer_name, reader_name, read_only_name] {
        let _: () = redis::cmd("ACL")
            .arg("DELUSER")
            .arg(name)
            .query_async(&mut conn)
            .await
            .unwrap();
    }
}

/// A cluster primary and its slot ranges.
struct Primary {
    node: SlotNode,
    slots: Vec<RangeInclusive<u16>>,
}

impl Primary {
    /// An empty host is the node that answered, which is local in the test cluster.
    fn host(&self) -> &str {
        match self.node.host.as_str() {
            "" => "127.0.0.1",
            host => host,
        }
    }

    fn url(&self) -> String {
        format!("redis://{}:{}/0", self.host(), self.node.port)
    }

    fn owns(&self, slot: u16) -> bool {
        self.slots.iter().any(|range| range.contains(&slot))
    }

    async fn run(&self, cmd: &redis::Cmd) -> redis::Value {
        cmd.query_async(&mut admin(&self.url()).await)
            .await
            .unwrap()
    }
}

async fn primaries(conn: &mut redis::cluster_async::ClusterConnection) -> Vec<Primary> {
    let ranges: Vec<SlotRange> = redis::cmd("CLUSTER")
        .arg("SLOTS")
        .query_async(conn)
        .await
        .unwrap();
    let mut primaries: Vec<Primary> = vec![];
    for range in ranges {
        match primaries
            .iter_mut()
            .find(|primary| primary.node.id == range.primary.id)
        {
            Some(primary) => primary.slots.push(range.slots),
            None => primaries.push(Primary {
                node: range.primary,
                slots: vec![range.slots],
            }),
        }
    }
    primaries
}

/// A slot that migrates from `source` to `target`. The drop undoes the migration, also when the
/// case panics, so the next case finds the slot stable.
struct Migration {
    slot: u16,
    source: String,
    target: String,
    keys: [String; 3],
}

impl Drop for Migration {
    fn drop(&mut self) {
        // A drop can't wait on a future, so it uses blocking connections.
        let run = |url: &str, pipe: &redis::Pipeline| -> redis::RedisResult<()> {
            pipe.query(&mut redis::Client::open(url)?.get_connection()?)
        };
        // The Stream's keys are only on the target, so deleting them there undoes the migration.
        let mut drop_keys = redis::pipe();
        for key in &self.keys {
            drop_keys
                .cmd("ASKING")
                .ignore()
                .cmd("DEL")
                .arg(key)
                .ignore();
        }
        let dropped = run(&self.target, &drop_keys);
        let mut stable = redis::pipe();
        stable
            .cmd("CLUSTER")
            .arg("SETSLOT")
            .arg(self.slot)
            .arg("STABLE")
            .ignore();
        let stabled = [&self.target, &self.source].map(|url| run(url, &stable));
        // A second panic would abort the test binary, so the drop checks only on a clean path.
        if !std::thread::panicking() {
            dropped.unwrap();
            for stabled in stabled {
                stabled.unwrap();
            }
        }
    }
}

/// While a slot migrates, the source answers `ASK` for the Stream's keys that it gave away, so the
/// Store's client sends `ASKING` to the target.
#[tokio::test]
#[ignore = "needs a Redis cluster"]
async fn the_documented_commands_serve_a_cluster_while_a_slot_moves() {
    let url = cluster_url();
    let unique = unique();
    let namespace = format!("ns-{unique}");
    let mut cluster = redis::cluster::ClusterClient::new(vec![url.as_str()])
        .unwrap()
        .get_async_connection()
        .await
        .unwrap();
    let nodes = primaries(&mut cluster).await;
    assert!(nodes.len() > 1, "a slot move needs two primaries");
    // A Stream whose slot has no other keys, so the migration takes only the Stream's keys.
    let (stream, slot, source, target) = 'pick: {
        for i in 0.. {
            let id = format!("s{i}");
            let keys = StreamKeys::new(PREFIX, &namespace, &id);
            let slot: u16 = redis::cmd("CLUSTER")
                .arg("KEYSLOT")
                .arg(keys.meta())
                .query_async(&mut cluster)
                .await
                .unwrap();
            let source = nodes.iter().find(|node| node.owns(slot)).unwrap();
            let count = source
                .run(redis::cmd("CLUSTER").arg("COUNTKEYSINSLOT").arg(slot))
                .await;
            if count == redis::Value::Int(0) {
                let target = nodes
                    .iter()
                    .find(|node| node.node.id != source.node.id)
                    .unwrap();
                break 'pick (id, slot, source, target);
            }
        }
        unreachable!()
    };
    let keys = StreamKeys::new(PREFIX, &namespace, &stream);
    let handle = StreamHandle {
        id: stream,
        ..Default::default()
    };
    let (producer_name, reader_name) = (format!("prod-{unique}"), format!("read-{unique}"));
    let producer_commands = format!("{} {CLUSTER}", PRODUCER.1);
    let reader_commands = format!("{} {CLUSTER}", READER.1);
    // The client sends `ACL SETUSER` to each node, since each node keeps its own users.
    for cmd in [
        set_user(&producer_name, &namespace, (PRODUCER.0, &producer_commands)),
        set_user(&reader_name, &namespace, (READER.0, &reader_commands)),
    ] {
        let _: () = cmd.query_async(&mut cluster).await.unwrap();
    }
    let connect = |name: &str| {
        let mut options = RedisStoreOptions::from(Topology::Cluster {
            first: login(&url, name),
            rest: Vec::new(),
        });
        options.key_prefix = PREFIX.parse().unwrap();
        RedisStore::connect(options)
    };
    let producer = connect(&producer_name).await.unwrap();
    let reader = connect(&reader_name).await.unwrap();
    let ((stored, digest), me) = (one_record(), client());
    let batch = |sequence| Batch {
        producer: &me,
        sequence,
        records: &stored,
        digest: &digest,
        withdrawal: Withdrawal::default(),
    };
    let none = Withdrawal::default();
    producer
        .create(&namespace, &handle, None, Owner::Client, &none)
        .await
        .unwrap();
    producer
        .append(&namespace, &handle, &batch(1))
        .await
        .unwrap();

    // The migration that `redis-cli --cluster reshard` runs, stopped after the Stream's keys moved.
    let set_slot = |state: &str, other: &Primary| {
        let mut cmd = redis::cmd("CLUSTER");
        cmd.arg("SETSLOT").arg(slot).arg(state).arg(&other.node.id);
        cmd
    };
    let migration = Migration {
        slot,
        source: source.url(),
        target: target.url(),
        keys: [keys.meta(), keys.log(), keys.producers()],
    };
    target.run(&set_slot("IMPORTING", source)).await;
    source.run(&set_slot("MIGRATING", target)).await;
    let mut migrate = redis::cmd("MIGRATE");
    migrate
        .arg(target.host())
        .arg(target.node.port)
        .arg("")
        .arg(0)
        .arg(5000)
        .arg("KEYS")
        .arg(keys.meta())
        .arg(keys.log())
        .arg(keys.producers());
    source.run(&migrate).await;

    let appended = producer.append(&namespace, &handle, &batch(2)).await;
    let read = reader.read(&namespace, &handle.id, 0, 10).await;
    let read_back = reader.read_back(&namespace, &handle.id, i64::MAX, 10).await;
    let deadline = tokio::time::Instant::now() + Duration::from_millis(300);
    let waited = reader.wait_after(&namespace, &handle.id, 0, deadline).await;
    let closed = producer
        .close(&namespace, &handle, &me, b"")
        .await
        .map(|_| ());

    drop(migration);
    for name in [&producer_name, &reader_name] {
        let _: () = redis::cmd("ACL")
            .arg("DELUSER")
            .arg(name)
            .query_async(&mut cluster)
            .await
            .unwrap();
    }
    let appended = appended.unwrap().offsets.unwrap();
    let (state, entries) = read.unwrap();
    assert_eq!((state.newest, entries.len()), (appended.1, 2));
    assert_eq!(read_back.unwrap().len(), 2);
    // A blocking read ends with no wake while the slot migrates, and the service reads again.
    assert!(!waited.unwrap());
    closed.unwrap();
}
