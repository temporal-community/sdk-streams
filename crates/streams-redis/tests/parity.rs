//! The Redis Store and the memory Store must give the same answers. Each seed runs the same random
//! creates, appends, closes and reads on both, and compares the answers. It also checks that a
//! refused write leaves the Stream's Redis keys as they were, since Redis doesn't roll a script
//! back.
//!
//! A failure prints its seed and its last calls. `STREAMS_PARITY_SEED=<seed>` runs that seed
//! alone, and `STREAMS_PARITY_SEEDS=<count>` runs more seeds.

use prost::Message;
use std::{
    collections::{BTreeMap, HashMap},
    fmt::Write as _,
    sync::Arc,
    time::Duration,
};
use temporalio_common::protos::temporal::api::common::v1::Payload;
use temporalio_streams::{
    Reason, StreamResult,
    digest::batch_digest,
    ids::{
        StreamKeys, activity_producer_id, client_producer_id, local_activity_producer_id,
        workflow_producer_id,
    },
    memory::MemoryStore,
    proto::{
        ActivityEpoch, ActivityProducer, AppendRecord, ClientProducer, LocalActivityEpoch,
        LocalActivityProducer, Producer, StreamRecord, StreamRef, WorkflowProducer,
        WorkflowTaskEpoch, producer::Kind,
    },
    store::{Batch, ConflictPolicy, Create, CreateOutcome, Entry, Store},
};
use temporalio_streams_redis::{
    RedisStore, RedisStoreOptions, Topology, Url, WaitOptions,
    testing::{cluster_url, standalone_url},
};

const PREFIX: &str = "streams-parity";
const DEFAULT_SEEDS: u64 = 48;
const STEPS: usize = 80;
const IN_FLIGHT: usize = 32;
/// Long enough that no entry or Producer expires during a seed.
const RETENTION: Duration = Duration::from_secs(3600);
const MAX_SEQUENCE: i64 = (1 << 53) - 1;
const STREAMS: [&str; 2] = ["s0", "s1"];
const TEXTS: [&str; 5] = ["a", "b", "c", "dd", "eee"];

#[tokio::test]
#[ignore = "needs Redis"]
async fn the_redis_store_answers_as_the_memory_store_on_redis() {
    let mut options =
        RedisStoreOptions::from(Topology::Standalone(standalone_url().parse().unwrap()));
    // The `WAIT` pipeline has its own path for script loads and errors.
    options.wait = Some(WaitOptions {
        replicas: std::num::NonZeroUsize::MIN,
        timeout: Duration::from_secs(5),
    });
    options.response_timeout = Duration::from_secs(10);
    run(options).await;
}

#[tokio::test]
#[ignore = "needs a Redis cluster"]
async fn the_redis_store_answers_as_the_memory_store_on_a_redis_cluster() {
    run(RedisStoreOptions::from(Topology::Cluster {
        first: cluster_url().parse().unwrap(),
        rest: Vec::new(),
    }))
    .await;
}

async fn run(mut options: RedisStoreOptions) {
    options.key_prefix = PREFIX.parse().unwrap();
    options.retention = RETENTION;
    options.last_write_ttl = Some(RETENTION);
    let raw = Raw::open(&options.topology).await;
    let redis = Arc::new(RedisStore::connect(options).await.unwrap());
    let seeds: Vec<u64> = match std::env::var("STREAMS_PARITY_SEED") {
        Ok(seed) => vec![seed.parse().unwrap()],
        Err(_) => {
            let count = std::env::var("STREAMS_PARITY_SEEDS")
                .map_or(DEFAULT_SEEDS, |count| count.parse().unwrap());
            (1..=count).collect()
        }
    };
    let run = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    // A bound on the seeds at once, so that a large run doesn't time out on a busy server.
    let mut tasks = tokio::task::JoinSet::new();
    let mut failures = Vec::new();
    for seed in seeds {
        if tasks.len() == IN_FLIGHT {
            failures.extend(tasks.join_next().await.unwrap().unwrap());
        }
        let seed = Seed::new(seed, format!("{run}-{seed}"), redis.clone(), raw.clone());
        tasks.spawn(seed.run());
    }
    while let Some(failure) = tasks.join_next().await {
        failures.extend(failure.unwrap());
    }
    assert!(failures.is_empty(), "{}", failures.join("\n\n"));
}

/// A small xorshift generator, so that a seed gives the same calls on every host.
struct Rng(u64);

impl Rng {
    fn below(&mut self, bound: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        usize::try_from(self.0 % bound as u64).unwrap()
    }

    fn pick<T: Copy>(&mut self, items: &[T]) -> T {
        items[self.below(items.len())]
    }
}

/// The Producers of the calls. Some share an id with another epoch, so that the fence of a newer
/// epoch runs. The run `r1`'s Local Activities come from three Workflow Tasks.
fn producers() -> Vec<Producer> {
    let client = |id: String| Producer {
        id,
        kind: Some(Kind::Client(ClientProducer {})),
    };
    let activity = |run: &str, attempt| Producer {
        id: activity_producer_id(run, "a", "s"),
        kind: Some(Kind::WorkflowActivity(ActivityProducer {
            run_id: run.to_string(),
            epoch: Some(ActivityEpoch {
                attempt_scheduled_time: Some(prost_types::Timestamp {
                    seconds: 100 + i64::from(attempt),
                    nanos: 0,
                }),
                attempt,
            }),
        })),
    };
    let local_activity = |run: &str, started: i64, attempt| Producer {
        id: local_activity_producer_id(run, &format!("call-{started}")),
        kind: Some(Kind::LocalActivity(LocalActivityProducer {
            run_id: run.to_string(),
            epoch: Some(LocalActivityEpoch {
                task: Some(task(started)),
                attempt,
            }),
        })),
    };
    let workflow = Producer {
        id: workflow_producer_id("r1"),
        kind: Some(Kind::Workflow(WorkflowProducer {
            run_id: "r1".to_string(),
            epoch: Some(WorkflowTaskEpoch::default()),
        })),
    };
    vec![
        client(client_producer_id("me", "p1")),
        client(client_producer_id("other", "p2")),
        activity("r1", 1),
        activity("r1", 2),
        activity("r0", 1),
        local_activity("r1", 3, 1),
        local_activity("r1", 3, 2),
        local_activity("r1", 5, 1),
        local_activity("r1", 7, 1),
        local_activity("r0", 3, 1),
        workflow,
    ]
}

/// The Workflow Task of a run that starts at the event `started`.
fn task(started: i64) -> WorkflowTaskEpoch {
    WorkflowTaskEpoch {
        started_event_id: started,
        attempt: 1,
        started_time: Some(prost_types::Timestamp {
            seconds: started,
            nanos: 0,
        }),
    }
}

/// The ref of each Stream of the seed `run`. A key holds no namespace, so the seeds that share a
/// server take Stream ids of their own.
fn stream_ref(run: &str, stream: usize) -> StreamRef {
    StreamRef {
        id: format!("{run}-{}", STREAMS[stream]),
        ..Default::default()
    }
}

#[derive(Debug, Clone)]
enum Call {
    Create {
        stream: usize,
        conflict: ConflictPolicy,
        retention: Option<Duration>,
        request_id: &'static str,
    },
    Append {
        stream: usize,
        producer: usize,
        sequence: i64,
        texts: Vec<&'static str>,
    },
    Close {
        stream: usize,
        producer: usize,
    },
    /// A read from the entry at `from`, or from the start for none. A `from` past the log reads
    /// after the newest entry.
    Read {
        stream: usize,
        from: Option<usize>,
        count: usize,
    },
}

impl Call {
    fn stream(&self) -> usize {
        match self {
            Call::Create { stream, .. }
            | Call::Append { stream, .. }
            | Call::Close { stream, .. }
            | Call::Read { stream, .. } => *stream,
        }
    }

    fn writes(&self) -> bool {
        matches!(
            self,
            Call::Create { .. } | Call::Append { .. } | Call::Close { .. }
        )
    }
}

/// An entry, with its offset as its place in the log. Each Store makes its own offsets.
type Item = (Place, Option<Vec<u8>>);

/// The place of an offset in the Store's log, or the offset that isn't in it.
#[derive(Debug, PartialEq)]
enum Place {
    At(usize),
    Missing,
}

/// An answer of a Store, with each offset as its place in the log.
#[derive(Debug, PartialEq)]
enum Answer {
    Created {
        outcome: CreateOutcome,
        retention: Duration,
    },
    Appended {
        places: Option<(Place, Place)>,
        resend: bool,
    },
    Closed {
        already: bool,
    },
    Read {
        exists: bool,
        version: u32,
        closed: bool,
        newest: Option<Place>,
        trimmed: Option<Place>,
        entries: Vec<Item>,
    },
    Refused(Reason),
}

/// One Store of a seed, with the offsets of each Stream's log after the last call.
struct Side {
    store: Arc<dyn Store>,
    logs: [Vec<i64>; 2],
}

struct Seed {
    seed: u64,
    rng: Rng,
    /// The part of each Stream id that is the seed's own.
    run: String,
    producers: Vec<Producer>,
    memory: Side,
    redis: Side,
    raw: Raw,
    /// The last sequence and texts that each Producer wrote to each Stream, so that the calls
    /// resend and diverge.
    last: HashMap<(usize, usize), (i64, Vec<&'static str>)>,
    calls: Vec<String>,
    /// The error of each refused call, for the failure text.
    errors: Vec<String>,
}

impl Seed {
    fn new(seed: u64, run: String, redis: Arc<RedisStore>, raw: Raw) -> Self {
        Self {
            seed,
            // Zero is a fixed point of xorshift.
            rng: Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1),
            run,
            producers: producers(),
            memory: Side {
                store: Arc::new(MemoryStore::new(RETENTION, Some(RETENTION))),
                logs: Default::default(),
            },
            redis: Side {
                store: redis,
                logs: Default::default(),
            },
            raw,
            last: HashMap::new(),
            calls: Vec::new(),
            errors: Vec::new(),
        }
    }

    /// The failure of the seed, if any.
    async fn run(mut self) -> Option<String> {
        for step in 0..STEPS {
            let call = self.next_call();
            self.calls.push(format!("{step}: {call:?}"));
            self.errors.clear();
            let keys = StreamKeys::new(PREFIX, &stream_ref(&self.run, call.stream()).id);
            let before = match call.writes() {
                true => Some(self.raw.snapshot(&keys).await),
                false => None,
            };
            let memory = self.answer(Which::Memory, &call).await;
            let redis = self.answer(Which::Redis, &call).await;
            if let (Some(before), Answer::Refused(reason)) = (before, &redis) {
                let after = self.raw.snapshot(&keys).await;
                if after != before {
                    let what = format!(
                        "a write refused with {reason:?} changed the Stream's Redis keys\n\
                         before {before:?}\nafter  {after:?}"
                    );
                    return Some(self.failure(&what));
                }
            }
            if memory != redis {
                return Some(self.failure(&format!("memory {memory:?}\nredis  {redis:?}")));
            }
            self.learn(&call, &memory);
        }
        None
    }

    fn failure(&self, what: &str) -> String {
        let mut text = format!("seed {} failed. {what}\n", self.seed);
        for error in &self.errors {
            let _ = writeln!(text, "{error}");
        }
        text.push_str("The last calls:\n");
        for call in self.calls.iter().rev().take(25).rev() {
            let _ = writeln!(text, "  {call}");
        }
        text
    }

    fn next_call(&mut self) -> Call {
        let stream = self.rng.below(STREAMS.len());
        let len = self.memory.logs[stream].len();
        match self.rng.below(20) {
            0..=2 => Call::Create {
                stream,
                conflict: self
                    .rng
                    .pick(&[ConflictPolicy::Fail, ConflictPolicy::UseExisting]),
                retention: self.rng.pick(&[None, Some(RETENTION), Some(RETENTION / 2)]),
                request_id: self.rng.pick(&["", "r1", "r2"]),
            },
            3 => Call::Close {
                stream,
                producer: self.rng.below(self.producers.len()),
            },
            4..=6 => {
                let from = self.rng.below(len + 2);
                Call::Read {
                    stream,
                    from: self.rng.pick(&[None, Some(from)]),
                    count: self.rng.pick(&[0, 1, 2, 5, 100]),
                }
            }
            _ => {
                let producer = self.rng.below(self.producers.len());
                let (last, last_texts) = self
                    .last
                    .get(&(stream, producer))
                    .cloned()
                    .unwrap_or((0, Vec::new()));
                let count = self.rng.below(4);
                let mut texts: Vec<_> = (0..count).map(|_| self.rng.pick(&TEXTS)).collect();
                let sequence = match self.rng.below(10) {
                    0..=1 => 0,
                    2..=5 => last + 1,
                    6 => {
                        texts = last_texts;
                        last
                    }
                    7 => last,
                    8 => last - 1,
                    _ => self.rng.pick(&[1, MAX_SEQUENCE, MAX_SEQUENCE + 1, -1]),
                };
                Call::Append {
                    stream,
                    producer,
                    sequence,
                    texts,
                }
            }
        }
    }

    /// Keeps what the next calls need, from the memory Store's answer, which matched.
    fn learn(&mut self, call: &Call, answer: &Answer) {
        if let (
            Call::Append {
                stream,
                producer,
                sequence,
                texts,
                ..
            },
            Answer::Appended { resend: false, .. },
        ) = (call, answer)
        {
            let key = (*stream, *producer);
            let sequence = match *sequence {
                0 => self.last.get(&key).map_or(1, |(last, _)| last + 1),
                sequence => sequence,
            };
            self.last.insert(key, (sequence, texts.clone()));
        }
    }

    async fn answer(&mut self, which: Which, call: &Call) -> Answer {
        let run = self.run.clone();
        let side = match which {
            Which::Memory => &mut self.memory,
            Which::Redis => &mut self.redis,
        };
        let stream = call.stream();
        let offset_at = |logs: &[Vec<i64>; 2], place: usize| {
            let log = &logs[stream];
            log.get(place)
                .copied()
                .unwrap_or_else(|| log.last().map_or(1, |newest| newest + 1))
        };
        let answer = match call {
            Call::Create {
                conflict,
                retention,
                request_id,
                ..
            } => {
                let create = Create {
                    retention: *retention,
                    conflict: *conflict,
                    request_id,
                };
                let created = side.store.create(&stream_ref(&run, stream), &create).await;
                created.map(|created| Answer::Created {
                    outcome: created.outcome,
                    retention: created.retention,
                })
            }
            Call::Append {
                producer,
                sequence,
                texts,
                ..
            } => {
                let producer = &self.producers[*producer];
                let (records, digest) = batch(producer, texts);
                let appended = side
                    .store
                    .append(
                        &stream_ref(&run, stream),
                        &Batch {
                            producer,
                            sequence: *sequence,
                            records: &records,
                            digest,
                        },
                    )
                    .await;
                let logs = listing(side.store.as_ref(), &stream_ref(&run, stream)).await;
                side.logs[stream] = logs;
                appended.map(|appended| Answer::Appended {
                    places: appended.offsets.map(|(first, last)| {
                        (place(side, stream, first), place(side, stream, last))
                    }),
                    resend: appended.resend,
                })
            }
            Call::Close { producer, .. } => {
                let producer = &self.producers[*producer];
                let closed = side.store.close(&stream_ref(&run, stream), producer).await;
                closed.map(|closed| Answer::Closed {
                    already: closed.already,
                })
            }
            Call::Read { from, count, .. } => {
                let from = from.map_or(0, |from| offset_at(&side.logs, from));
                let read = side
                    .store
                    .read(&stream_ref(&run, stream), from, *count)
                    .await;
                read.map(|(state, entries)| Answer::Read {
                    exists: state.exists,
                    version: state.version,
                    closed: state.closed,
                    newest: (state.newest != 0).then(|| place(side, stream, state.newest)),
                    trimmed: (state.trimmed != 0).then(|| place(side, stream, state.trimmed)),
                    entries: items(side, stream, entries),
                })
            }
        };
        answer.unwrap_or_else(|error| {
            self.errors.push(format!("{which:?} {error}"));
            Answer::Refused(error.reason)
        })
    }
}

#[derive(Clone, Copy, Debug)]
enum Which {
    Memory,
    Redis,
}

/// The offsets of a Stream's whole log.
async fn listing(store: &dyn Store, stream: &StreamRef) -> Vec<i64> {
    let read: StreamResult<_> = store.read(stream, 0, 100_000).await;
    read.map_or_else(
        |_| Vec::new(),
        |(_, entries)| entries.iter().map(|entry| entry.offset).collect(),
    )
}

fn place(side: &Side, stream: usize, offset: i64) -> Place {
    side.logs[stream]
        .iter()
        .position(|logged| *logged == offset)
        .map_or(Place::Missing, Place::At)
}

fn items(side: &Side, stream: usize, entries: Vec<Entry>) -> Vec<Item> {
    entries
        .into_iter()
        .map(|entry| (place(side, stream, entry.offset), entry.record))
        .collect()
}

/// The Records of a batch as the runtime gives them to a Store, and the batch's digest.
fn batch(producer: &Producer, texts: &[&str]) -> (Vec<Vec<u8>>, [u8; 16]) {
    let bodies: Vec<_> = texts
        .iter()
        .map(|text| Payload {
            data: text.as_bytes().to_vec(),
            ..Default::default()
        })
        .collect();
    let records = bodies
        .iter()
        .map(|body| {
            StreamRecord {
                producer: Some(producer.clone()),
                body: Some(body.clone()),
                ..Default::default()
            }
            .encode_to_vec()
        })
        .collect();
    let appended: Vec<_> = bodies
        .into_iter()
        .map(|body| AppendRecord { body: Some(body) })
        .collect();
    (records, batch_digest(&appended))
}

/// What a write can change in a Stream's keys. A hash with long values is a hash table, whose
/// `DUMP` order changes as Redis rehashes it on a read, so the meta hash is compared by its fields.
#[derive(Debug, PartialEq)]
struct Snapshot {
    meta: BTreeMap<Vec<u8>, Vec<u8>>,
    /// The expiry of each key, and the `DUMP` of the log and `:closed`.
    rest: Vec<redis::Value>,
}

/// Plain commands on the Store's keys. A cluster connection sends each one to its key's node.
#[derive(Clone)]
enum Raw {
    Standalone(redis::aio::ConnectionManager),
    Cluster(redis::cluster_async::ClusterConnection),
}

impl Raw {
    async fn open(topology: &Topology<Url>) -> Self {
        match topology {
            Topology::Standalone(url) => {
                let client = redis::Client::open(url.as_str()).unwrap();
                Raw::Standalone(client.get_connection_manager().await.unwrap())
            }
            Topology::Cluster { .. } => {
                let seeds = topology.servers().map(Url::as_str);
                let client = redis::cluster::ClusterClient::new(seeds).unwrap();
                Raw::Cluster(client.get_async_connection().await.unwrap())
            }
        }
    }

    /// The Stream's keys as a write would change them. A key that doesn't exist gives no fields,
    /// nil and -2.
    async fn snapshot(&self, keys: &StreamKeys) -> Snapshot {
        let mut pipe = redis::pipe();
        pipe.cmd("HGETALL").arg(keys.meta());
        pipe.cmd("PEXPIRETIME").arg(keys.meta());
        for key in [keys.log(), keys.closed()] {
            pipe.cmd("DUMP").arg(&key).cmd("PEXPIRETIME").arg(&key);
        }
        let (meta, rest): (BTreeMap<Vec<u8>, Vec<u8>>, Vec<redis::Value>) = {
            let mut values: Vec<redis::Value> = self.pipe(&pipe).await;
            let meta = redis::from_redis_value(values.remove(0)).unwrap();
            (meta, values)
        };
        Snapshot { meta, rest }
    }

    async fn pipe(&self, pipe: &redis::Pipeline) -> Vec<redis::Value> {
        match self.clone() {
            Raw::Standalone(mut conn) => pipe.query_async(&mut conn).await.unwrap(),
            Raw::Cluster(mut conn) => pipe.query_async(&mut conn).await.unwrap(),
        }
    }
}
