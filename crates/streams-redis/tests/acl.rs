//! The Redis users of `docs/redis.md`, with exactly the key access and commands it lists per role,
//! run every Store call of that role, on a standalone server and on a cluster whose slot moves.

mod support;

use prost::Message;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use support::server_url;
use temporalio_common::protos::temporal::api::common::v1::Payload;
use temporalio_streams::{
    Reason,
    digest::batch_digest,
    ids::{StreamKeys, client_producer_id},
    proto::{
        AppendRecord, Producer, ProducerEpoch, StreamData, StreamProducerKind, StreamRecord,
        StreamRef, stream_record::Item,
    },
    store::{Batch, Owner, Store, Withdrawal},
};
use temporalio_streams_redis::{RedisStore, RedisStoreOptions};

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
    options.key_prefix = PREFIX.to_string();
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
async fn the_documented_commands_run_every_call_of_each_role() {
    let Some(url) = server_url("STREAMS_REDIS_URL") else {
        return;
    };
    let unique = unique();
    let namespace = format!("ns-{unique}");
    let (producer_name, reader_name) = (format!("prod-{unique}"), format!("read-{unique}"));
    let stream = StreamRef {
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
            &stream,
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
    let appended = producer.append(&namespace, &stream, &batch).await.unwrap();
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
    let deadline = tokio::time::Instant::now() + Duration::from_millis(100);
    assert!(
        !reader
            .wait_after(&namespace, "s", first, deadline)
            .await
            .unwrap()
    );
    // The Reader's user has only read access to the keys, so its write fails for good.
    let refused = reader
        .append(
            &namespace,
            &stream,
            &Batch {
                sequence: 2,
                ..batch.clone()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(refused.reason, Reason::Invalid, "{refused}");

    producer
        .close(&namespace, &stream, &client(), b"")
        .await
        .unwrap();

    // An append after the Retention trims the old Records and prunes the old Producers. An open
    // Workflow Stream has the safety TTL, so it doesn't expire with the Retention.
    let short = StreamRef {
        id: "short".to_string(),
        ..Default::default()
    };
    let retention = Duration::from_secs(1);
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
    let append_as = |process: &'static str| {
        let (producer, namespace, short) = (&producer, &namespace, &short);
        let digest = batch.digest;
        let stored = &stored;
        async move {
            let client = client_named(process);
            let batch = Batch {
                producer: &client,
                sequence: 1,
                records: stored,
                digest,
                withdrawal: Withdrawal::default(),
            };
            producer.append(namespace, short, &batch).await.unwrap()
        }
    };
    let old = append_as("old").await.offsets.unwrap().1;
    tokio::time::sleep(retention / 2).await;
    // A write within the Retention keeps the log key.
    append_as("recent").await;
    tokio::time::sleep(retention * 7 / 10).await;
    append_as("new").await;
    let (state, entries) = reader.read(&namespace, "short", 0, 10).await.unwrap();
    assert_eq!((state.trimmed, entries.len()), (old, 2));
    let fence = format!(
        "e|{}|{}",
        StreamProducerKind::Client as i32,
        client_named("old").id
    );
    let fenced: bool = redis::cmd("HEXISTS")
        .arg(StreamKeys::new(PREFIX, &namespace, "short").meta())
        .arg(fence)
        .query_async(&mut admin(&url).await)
        .await
        .unwrap();
    assert!(!fenced);

    // With `+wait`, too few replicas is a Store failure, not a refusal.
    let mut options =
        RedisStoreOptions::new(user(&url, &producer_name, &namespace, PRODUCER).await);
    options.wait_replicas = 16;
    options.wait_timeout = Duration::from_millis(50);
    let waits = store_with(options).await;
    let failed = waits
        .create(
            &namespace,
            &StreamRef {
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

    let mut conn = admin(&url).await;
    for name in [producer_name, reader_name] {
        let _: () = redis::cmd("ACL")
            .arg("DELUSER")
            .arg(name)
            .query_async(&mut conn)
            .await
            .unwrap();
    }
}

/// A cluster primary, from `CLUSTER SLOTS`.
struct Primary {
    host: String,
    port: i64,
    id: String,
    slots: Vec<(i64, i64)>,
}

impl Primary {
    fn url(&self) -> String {
        format!("redis://{}:{}/0", self.host, self.port)
    }

    fn owns(&self, slot: i64) -> bool {
        self.slots
            .iter()
            .any(|&(start, end)| start <= slot && slot <= end)
    }

    async fn run(&self, cmd: &redis::Cmd) -> redis::Value {
        cmd.query_async(&mut admin(&self.url()).await)
            .await
            .unwrap()
    }
}

async fn primaries(conn: &mut redis::cluster_async::ClusterConnection) -> Vec<Primary> {
    let reply: redis::Value = redis::cmd("CLUSTER")
        .arg("SLOTS")
        .query_async(conn)
        .await
        .unwrap();
    let redis::Value::Array(ranges) = reply else {
        panic!("CLUSTER SLOTS gave {reply:?}");
    };
    let mut primaries: Vec<Primary> = vec![];
    for range in ranges {
        let redis::Value::Array(parts) = range else {
            continue;
        };
        let (redis::Value::Int(start), redis::Value::Int(end), redis::Value::Array(node)) =
            (&parts[0], &parts[1], &parts[2])
        else {
            continue;
        };
        let [
            redis::Value::BulkString(host),
            redis::Value::Int(port),
            redis::Value::BulkString(id),
        ] = &node[..3]
        else {
            panic!("CLUSTER SLOTS gave {node:?}");
        };
        let id = String::from_utf8_lossy(id).into_owned();
        match primaries.iter_mut().find(|primary| primary.id == id) {
            Some(primary) => primary.slots.push((*start, *end)),
            None => primaries.push(Primary {
                host: match String::from_utf8_lossy(host).into_owned() {
                    host if host.is_empty() => "127.0.0.1".to_string(),
                    host => host,
                },
                port: *port,
                id,
                slots: vec![(*start, *end)],
            }),
        }
    }
    primaries
}

/// While a slot moves, the source answers `ASK` for the Stream's keys that it gave away, so the
/// Store's client sends `ASKING` to the target.
#[tokio::test]
async fn the_documented_commands_serve_a_cluster_while_a_slot_moves() {
    let Some(url) = server_url("STREAMS_REDIS_CLUSTER_URL") else {
        return;
    };
    let unique = unique();
    let namespace = format!("ns-{unique}");
    let mut cluster = redis::cluster::ClusterClient::new(vec![url.as_str()])
        .unwrap()
        .get_async_connection()
        .await
        .unwrap();
    let nodes = primaries(&mut cluster).await;
    assert!(nodes.len() > 1, "a slot move needs two primaries");
    // A Stream whose slot has no other keys, so the move takes only the Stream's keys.
    let (stream, slot, source, target) = 'pick: {
        for i in 0.. {
            let id = format!("s{i}");
            let keys = StreamKeys::new(PREFIX, &namespace, &id);
            let slot: i64 = redis::cmd("CLUSTER")
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
                let target = nodes.iter().find(|node| node.id != source.id).unwrap();
                break 'pick (id, slot, source, target);
            }
        }
        unreachable!()
    };
    let keys = StreamKeys::new(PREFIX, &namespace, &stream);
    let stream = StreamRef {
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
        let mut options = RedisStoreOptions::new(login(&url, name));
        options.cluster = true;
        options.key_prefix = PREFIX.to_string();
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
        .create(&namespace, &stream, None, Owner::Client, &none)
        .await
        .unwrap();
    producer
        .append(&namespace, &stream.id, &batch(1))
        .await
        .unwrap();

    // The move as `redis-cli --cluster reshard` makes it, stopped after the Stream's keys moved.
    let set_slot = |state: &str, other: &Primary| {
        let mut cmd = redis::cmd("CLUSTER");
        cmd.arg("SETSLOT").arg(slot).arg(state).arg(&other.id);
        cmd
    };
    target.run(&set_slot("IMPORTING", source)).await;
    source.run(&set_slot("MIGRATING", target)).await;
    let mut migrate = redis::cmd("MIGRATE");
    migrate
        .arg(&target.host)
        .arg(target.port)
        .arg("")
        .arg(0)
        .arg(5000)
        .arg("KEYS")
        .arg(keys.meta())
        .arg(keys.log())
        .arg(keys.producers());
    source.run(&migrate).await;

    let appended = producer.append(&namespace, &stream.id, &batch(2)).await;
    let read = reader.read(&namespace, &stream.id, 0, 10).await;
    let read_back = reader.read_back(&namespace, &stream.id, i64::MAX, 10).await;
    let deadline = tokio::time::Instant::now() + Duration::from_millis(300);
    let waited = reader.wait_after(&namespace, &stream.id, 0, deadline).await;
    let closed = producer
        .close(&namespace, &stream, &me, b"")
        .await
        .map(|_| ());

    // The Stream's keys are only on the target, so dropping them there undoes the move. A failed
    // drop must not keep the slot moving.
    let mut drop_keys = redis::pipe();
    for key in [keys.meta(), keys.log(), keys.producers()] {
        drop_keys
            .cmd("ASKING")
            .ignore()
            .cmd("DEL")
            .arg(key)
            .ignore();
    }
    let dropped: redis::RedisResult<()> =
        drop_keys.query_async(&mut admin(&target.url()).await).await;
    for node in [target, source] {
        node.run(redis::cmd("CLUSTER").arg("SETSLOT").arg(slot).arg("STABLE"))
            .await;
    }
    for name in [&producer_name, &reader_name] {
        let _: () = redis::cmd("ACL")
            .arg("DELUSER")
            .arg(name)
            .query_async(&mut cluster)
            .await
            .unwrap();
    }
    dropped.unwrap();

    let appended = appended.unwrap().offsets.unwrap();
    let (state, entries) = read.unwrap();
    assert_eq!((state.newest, entries.len()), (appended.1, 2));
    assert_eq!(read_back.unwrap().len(), 2);
    // A blocking read ends with no wake while the slot moves, and the service reads again.
    assert!(!waited.unwrap());
    closed.unwrap();
}
