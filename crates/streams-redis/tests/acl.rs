//! The Redis users of `docs/redis.md`, with exactly the key access and commands it lists per role,
//! run every Store call of that role, on a standalone server and on a cluster whose slot migrates.

use prost::Message;
use std::{
    num::NonZeroUsize,
    ops::RangeInclusive,
    sync::LazyLock,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use temporalio_common::protos::temporal::api::common::v1::Payload;
use temporalio_streams::{
    Reason,
    digest::batch_digest,
    ids::StreamKeys,
    proto::{AppendRecord, ClientProducer, Producer, StreamRecord, StreamRef, producer::Kind},
    store::{Batch, Create, Store},
};
use temporalio_streams_redis::{
    RedisStore, RedisStoreError, RedisStoreOptions, Topology, WaitOptions,
    testing::{SlotNode, SlotRange, cluster_url, documented_commands, standalone_url},
};

const PREFIX: &str = "streams-acl";

/// The key access of each role, with the commands that `docs/redis.md` lists for it. The docs are
/// the only list, so a script that calls a new command fails here until the docs list it.
static PRODUCER: LazyLock<(&str, String)> = LazyLock::new(|| ("~", documented("A Producer")));
static READER: LazyLock<(&str, String)> = LazyLock::new(|| ("%R~", documented("A Reader")));
/// The commands that `docs/redis.md` adds for each role on a cluster.
static CLUSTER: LazyLock<String> = LazyLock::new(|| documented("On a cluster, each role"));

fn documented(bullet: &str) -> String {
    let commands: Vec<_> = documented_commands(bullet)
        .iter()
        .map(|command| format!("+{command}"))
        .collect();
    commands.join(" ")
}

/// A role as `set_user` takes it.
fn role<'a>(role: &'a (&'static str, String)) -> (&'static str, &'a str) {
    (role.0, &role.1)
}

async fn admin(url: &str) -> redis::aio::MultiplexedConnection {
    redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}

/// The `ACL SETUSER` of a user with a role's key access and commands on the keys of the prefix.
fn set_user(name: &str, (keys, commands): (&str, &str)) -> redis::Cmd {
    let mut acl = redis::cmd("ACL");
    acl.arg("SETUSER")
        .arg(name)
        .arg("reset")
        .arg("on")
        .arg(">pw")
        .arg(format!("{keys}{PREFIX}:*"));
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

/// A user with a role's key access and commands on the keys of the prefix, and the URL that logs
/// in as it.
async fn user(url: &str, name: &str, role: (&str, &str)) -> String {
    let _: () = set_user(name, role)
        .query_async(&mut admin(url).await)
        .await
        .unwrap();
    login(url, name)
}

async fn store(url: String) -> Box<dyn Store> {
    store_with(RedisStoreOptions::new(url).unwrap()).await
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
fn one_record() -> (Vec<Vec<u8>>, [u8; 16]) {
    let records = vec![AppendRecord {
        body: Some(Payload {
            data: b"x".to_vec(),
            ..Default::default()
        }),
    }];
    let stored = StreamRecord {
        producer: Some(client()),
        body: records[0].body.clone(),
        ..Default::default()
    };
    (vec![stored.encode_to_vec()], batch_digest(&records))
}

fn client_named(process: &str) -> Producer {
    Producer {
        id: format!("client:me:{process}"),
        kind: Some(Kind::Client(ClientProducer {})),
    }
}

#[tokio::test]
#[ignore = "needs Redis"]
async fn the_documented_commands_run_every_call_of_each_role() {
    let url = standalone_url();
    let unique = unique();
    let (producer_name, reader_name) = (format!("prod-{unique}"), format!("read-{unique}"));
    let read_only_name = format!("ro-{unique}");
    // A key holds no namespace, so each run takes Stream ids of its own.
    let stream_ref = StreamRef {
        id: format!("s-{unique}"),
        ..Default::default()
    };
    // With no cached script, each user loads the scripts it runs.
    let _: () = redis::cmd("SCRIPT")
        .arg("FLUSH")
        .query_async(&mut admin(&url).await)
        .await
        .unwrap();

    let producer = store(user(&url, &producer_name, role(&PRODUCER)).await).await;
    producer
        .create(&stream_ref, &Create::default())
        .await
        .unwrap();
    let (stored, digest) = one_record();
    let batch = Batch {
        producer: &client(),
        sequence: 1,
        records: &stored,
        digest,
    };
    let appended = producer.append(&stream_ref, &batch).await.unwrap();
    let (first, _) = appended.offsets.unwrap();

    let reader = store(user(&url, &reader_name, role(&READER)).await).await;
    let (state, entries) = reader.read(&stream_ref, 0, 10).await.unwrap();
    assert_eq!((state.newest, entries.len()), (first, 1));
    // The log has an entry after 0, so the Reader's blocking read wakes at once.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    assert!(reader.wait_after(&stream_ref, 0, deadline).await.unwrap());
    // A user with read access to the keys can't write them, even with the Producer's commands.
    let read_only = (READER.0, PRODUCER.1.as_str());
    let read_only = store(user(&url, &read_only_name, read_only).await).await;
    let refused = read_only
        .append(
            &stream_ref,
            &Batch {
                sequence: 2,
                ..batch.clone()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(refused.reason, Reason::Invalid, "{refused}");
    assert!(!refused.message.contains("evalsha"), "{refused}");

    producer.close(&stream_ref, &client()).await.unwrap();

    // With `+wait`, too few replicas is a Store failure, not a refusal.
    let mut options =
        RedisStoreOptions::new(user(&url, &producer_name, role(&PRODUCER)).await).unwrap();
    options.wait = Some(WaitOptions {
        replicas: NonZeroUsize::new(16).unwrap(),
        timeout: Duration::from_millis(50),
    });
    let waits = store_with(options).await;
    let failed = waits
        .create(
            &StreamRef {
                id: format!("waits-{unique}"),
                ..Default::default()
            },
            &Create::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(failed.reason, Reason::StorageUnavailable, "{failed}");
    // Without `+wait`, each write would run its script and then fail, so connect refuses it.
    let no_wait = PRODUCER.1.replace(" +wait", "");
    let no_wait = (PRODUCER.0, no_wait.as_str());
    let mut options = RedisStoreOptions::new(user(&url, &producer_name, no_wait).await).unwrap();
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
    /// An empty host is the node that answered, and a NULL one is the endpoint that the test asked.
    /// Both are local in the test cluster.
    fn host(&self) -> &str {
        match self.node.host.as_deref() {
            None | Some("") => "127.0.0.1",
            Some(host) => host,
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
            let id = format!("s{unique}-{i}");
            let keys = StreamKeys::new(PREFIX, &id);
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
    let keys = StreamKeys::new(PREFIX, &stream);
    let stream_ref = StreamRef {
        id: stream,
        ..Default::default()
    };
    let (producer_name, reader_name) = (format!("prod-{unique}"), format!("read-{unique}"));
    let producer_commands = format!("{} {}", PRODUCER.1, *CLUSTER);
    let reader_commands = format!("{} {}", READER.1, *CLUSTER);
    // The client sends `ACL SETUSER` to each node, since each node keeps its own users.
    for cmd in [
        set_user(&producer_name, (PRODUCER.0, &producer_commands)),
        set_user(&reader_name, (READER.0, &reader_commands)),
    ] {
        let _: () = cmd.query_async(&mut cluster).await.unwrap();
    }
    let connect = |name: &str| {
        let mut options = RedisStoreOptions::from(Topology::Cluster {
            first: login(&url, name).parse().unwrap(),
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
        digest,
    };
    producer
        .create(&stream_ref, &Create::default())
        .await
        .unwrap();
    producer.append(&stream_ref, &batch(1)).await.unwrap();

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
        keys: [keys.meta(), keys.log(), keys.closed()],
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
        .arg(keys.log());
    source.run(&migrate).await;

    let appended = producer.append(&stream_ref, &batch(2)).await;
    let read = reader.read(&stream_ref, 0, 10).await;
    let deadline = tokio::time::Instant::now() + Duration::from_millis(300);
    let waited = reader.wait_after(&stream_ref, 0, deadline).await;
    let closed = producer.close(&stream_ref, &me).await.map(|_| ());

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
    // A blocking read ends with no wake while the slot migrates, and the service reads again.
    assert!(!waited.unwrap());
    closed.unwrap();
}

/// A standalone Store at a cluster node, with a user that can't run `INFO`, gets past `connect`.
/// Then each key of another node gets `MOVED`, which is a config error and not a passing state, so
/// no caller retries it for good.
#[tokio::test]
#[ignore = "needs a Redis cluster"]
async fn a_standalone_store_at_a_cluster_node_fails_with_a_config_error() {
    let url = cluster_url();
    let unique = unique();
    let mut cluster = redis::cluster::ClusterClient::new(vec![url.as_str()])
        .unwrap()
        .get_async_connection()
        .await
        .unwrap();
    let nodes = primaries(&mut cluster).await;
    assert!(nodes.len() > 1, "the case needs a key on another primary");
    let mut stream = None;
    for i in 0.. {
        let id = format!("s{unique}-{i}");
        let slot: u16 = redis::cmd("CLUSTER")
            .arg("KEYSLOT")
            .arg(StreamKeys::new(PREFIX, &id).meta())
            .query_async(&mut cluster)
            .await
            .unwrap();
        if !nodes[0].owns(slot) {
            stream = Some(id);
            break;
        }
    }
    let stream_ref = StreamRef {
        id: stream.unwrap(),
        ..Default::default()
    };
    // Both roles, as the docs list them, and so no `+info`.
    let name = format!("both-{unique}");
    let commands = format!("{} {}", PRODUCER.1, READER.1);
    let _: () = set_user(&name, (PRODUCER.0, &commands))
        .query_async(&mut cluster)
        .await
        .unwrap();
    let store = store(login(&nodes[0].url(), &name)).await;
    let created = store.create(&stream_ref, &Create::default()).await;
    let read = store.read(&stream_ref, 0, 10).await;
    let deadline = tokio::time::Instant::now() + Duration::from_millis(300);
    let waited = store.wait_after(&stream_ref, 0, deadline).await;
    let _: () = redis::cmd("ACL")
        .arg("DELUSER")
        .arg(&name)
        .query_async(&mut cluster)
        .await
        .unwrap();
    for error in [created.unwrap_err(), read.unwrap_err(), waited.unwrap_err()] {
        assert_eq!(error.reason, Reason::Invalid, "{error}");
        assert!(error.message.contains("set `cluster`"), "{error}");
    }
}
