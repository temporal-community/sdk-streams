//! The rules of a Stream, over generated calls. The test keeps what it sent and what was
//! answered, states the rule of each call on that, and compares the whole answer.

use crate::support::{Cluster, Redis, Rng, keys, seeds, snapshot, standalone};
use prost::encoding::message::encoded_len;
use redis::aio::ConnectionLike;
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};
use temporalio_common::protos::temporal::api::common::v1::Payload;
use temporalio_streams::{
    StreamRuntime, StreamStore,
    attempt::{ATTEMPT_KEY_LEN, attempt_key},
    proto::{
        ActivityAttempt, AppendRecord, AppendRecordsRequest, AppendRecordsResponse, ClientProducer,
        CloseStreamRequest, CloseStreamResponse, CreateStreamRequest, CreateStreamResponse,
        DescribeStreamRequest, DescribeStreamResponse, LocalActivityAttempt, LocalActivityProducer,
        Producer, ReadRecordsRequest, ReadRecordsResponse, StandaloneActivityProducer,
        StreamErrorReason, StreamIdConflictPolicy, StreamRecord, StreamStatus,
        WorkflowActivityProducer, WorkflowProducer, WorkflowTaskAttempt, producer::Kind,
    },
    record::StoreLimits,
};
use temporalio_streams_redis::{Fence, RedisStore, RedisStoreOptions, WaitForReplicas};

/// The batches that the Store remembers for a Producer. It's small, so that a seed passes its
/// edge often.
const WINDOW: usize = 3;
const STEPS: usize = 120;
const STREAMS: [&str; 2] = ["a}{%é", "b"];

#[tokio::test(flavor = "multi_thread")]
async fn a_store_follows_the_rules_for_any_calls() {
    let redis = Redis::start(&[]);
    run_seeds(40, || standalone(redis.url()), redis.admin().await).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_store_follows_the_rules_on_a_cluster() {
    let cluster = Cluster::start().await;
    run_seeds(10, || cluster.options(), cluster.admin().await).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_store_follows_the_rules_when_it_waits_for_a_replica() {
    let redis = Redis::start(&[]);
    let _replica = redis.start_replica();
    let options = || {
        let mut options = standalone(redis.url());
        options.wait = Some(WaitForReplicas::new(1));
        options
    };
    run_seeds(5, options, redis.admin().await).await;
}

/// Runs the seeds of the generated calls on Stores with `options`, with the fence off and on.
/// `admin` reads the keys of a Stream and deletes its log.
async fn run_seeds<C: ConnectionLike + Clone>(
    count: u64,
    options: impl Fn() -> RedisStoreOptions,
    admin: C,
) {
    for fence in [Fence::Off, Fence::On] {
        for seed in seeds(count) {
            let mut options = options();
            options.key_prefix = format!("rules-{seed}-{fence:?}");
            options.fence = fence;
            options.resend_window = WINDOW;
            Run::new(options, admin.clone(), seed).run().await;
        }
    }
}

type AttemptKey = [u8; ATTEMPT_KEY_LEN];

/// A batch that the Store accepted, as it remembers one.
struct Batch {
    attempt: AttemptKey,
    request_id: String,
    answer: AppendRecordsResponse,
}

/// What the Store has for one Producer of a Stream.
#[derive(Default)]
struct Writer {
    newest_attempt: Option<AttemptKey>,
    batches: VecDeque<Batch>,
}

/// What the test knows of one Stream.
#[derive(Default)]
struct Known {
    /// The request id of the create that made the Stream.
    created_by: Option<String>,
    closed: bool,
    /// The Records of the accepted batches, in the order of their offsets.
    records: Vec<StreamRecord>,
    /// The offset up to which the log lost its Records, or empty.
    lost: Vec<u8>,
    writers: HashMap<WriterId, Writer>,
}

impl Known {
    fn newest(&self) -> Vec<u8> {
        self.records.last().map_or(Vec::new(), |r| r.offset.clone())
    }
}

#[derive(Debug)]
enum Call {
    Create(CreateStreamRequest),
    Append(AppendRecordsRequest),
    Close(CloseStreamRequest),
    Read(ReadRecordsRequest),
    Describe(DescribeStreamRequest),
    /// Deletes the log of the Stream, as an eviction does.
    LoseLog(String),
}

#[derive(Debug, PartialEq)]
enum Answer {
    Created(CreateStreamResponse),
    Appended(AppendRecordsResponse),
    Closed(CloseStreamResponse),
    Page(ReadRecordsResponse),
    Described(DescribeStreamResponse),
    Refused(StreamErrorReason),
}

/// What the rules say about a call.
enum Expected {
    /// The answer, of a call that changes no key.
    Unchanged(Answer),
    /// The answer, of a call that writes.
    Written(Answer),
    /// A batch that the Store writes, at offsets that only the Store knows.
    NewBatch,
}

/// The run of one seed, with its Store, the Streams that the test knows, and the generator.
struct Run<C> {
    seed: u64,
    fence: Fence,
    prefix: String,
    /// The Store with no runtime in front, to see what the runtime changes of its answers.
    store: Arc<RedisStore>,
    runtime: StreamRuntime,
    admin: C,
    rng: Rng,
    known: HashMap<String, Known>,
    /// The appends that the test sent, the newest last, for the resends.
    sent: Vec<AppendRecordsRequest>,
}

impl<C: ConnectionLike> Run<C> {
    fn new(options: RedisStoreOptions, admin: C, seed: u64) -> Self {
        let (fence, prefix) = (options.fence, options.key_prefix.clone());
        let store = Arc::new(RedisStore::new(options).unwrap());
        Self {
            seed,
            fence,
            prefix,
            runtime: StreamRuntime::new(store.clone(), StoreLimits::default()),
            store,
            admin,
            rng: Rng::new(seed),
            known: STREAMS.map(|id| (id.to_string(), Known::default())).into(),
            sent: Vec::new(),
        }
    }

    async fn run(mut self) {
        for step in 0..STEPS {
            let call = self.next_call();
            let context = format!("seed {}, step {step}, {call:?}", self.seed);
            let Some(stream_id) = self.apply_fault(&call).await else {
                continue;
            };
            let expected = self.expected(&call);
            let before = snapshot(&mut self.admin, &self.prefix, &stream_id).await;
            let answer = self.send(&call).await;
            match expected {
                Expected::Unchanged(wanted) => {
                    assert_eq!(answer, wanted, "{context}");
                    let after = snapshot(&mut self.admin, &self.prefix, &stream_id).await;
                    assert_eq!(after, before, "a key changed. {context}");
                }
                Expected::Written(wanted) => assert_eq!(answer, wanted, "{context}"),
                Expected::NewBatch => self.accept(&call, answer, &context),
            }
        }
        for stream_id in STREAMS {
            self.read_all(stream_id).await;
        }
    }

    fn next_call(&mut self) -> Call {
        let stream_id = self.rng.pick(&STREAMS).to_string();
        match self.rng.below(100) {
            0..10 => Call::Create(CreateStreamRequest {
                stream_id,
                id_conflict_policy: self.rng.below(3) as i32,
                request_id: self.rng.pick(&["", "c1", "c2"]).to_string(),
            }),
            10..57 => Call::Append(self.next_append(stream_id)),
            57..59 => Call::Close(CloseStreamRequest {
                stream_id,
                producer: Some(producer(&mut self.rng)),
            }),
            59..66 => Call::Describe(DescribeStreamRequest { stream_id }),
            66..68 => Call::LoseLog(stream_id),
            _ => {
                let from_offset = self.next_from(&stream_id);
                let after = self.known[&stream_id].records.iter();
                let after = after.filter(|record| record.offset > from_offset);
                let max_bytes = match self.rng.below(10) {
                    0..2 => 1 + self.rng.below(300),
                    // The size of the next Records, so the limit is where a Record ends, or one
                    // byte off it.
                    2..5 => {
                        let sizes = after.map(|record| encoded_len(1, record));
                        let end: usize = sizes.take(1 + self.rng.below(3)).sum();
                        (end + self.rng.below(3)).saturating_sub(1)
                    }
                    _ => 0,
                };
                Call::Read(ReadRecordsRequest {
                    from_offset,
                    max_records: self.rng.below(6) as i32,
                    max_bytes: max_bytes as i64,
                    wait: None,
                    stream_id,
                })
            }
        }
    }

    /// Returns a new batch, or one that has the Producer and the `request_id` of an earlier one.
    /// Its Records are new either way, since a resend doesn't compare them.
    fn next_append(&mut self, stream_id: String) -> AppendRecordsRequest {
        let records = (0..1 + self.rng.below(3)).map(|_| AppendRecord {
            body: Some(Payload {
                metadata: (0..self.rng.below(3))
                    .map(|key| (format!("k{key}"), self.rng.bytes(4)))
                    .collect(),
                data: self.rng.bytes(20),
                ..Default::default()
            }),
        });
        let records = records.collect();
        let recent = &self.sent[self.sent.len().saturating_sub(8)..];
        let request = match recent.is_empty() || self.rng.chance(60) {
            true => AppendRecordsRequest {
                stream_id,
                producer: Some(producer(&mut self.rng)),
                request_id: match self.rng.chance(30) {
                    true => String::new(),
                    false => format!("r{}", self.sent.len()),
                },
                records,
            },
            false => {
                let mut again = self.rng.pick(recent);
                // The same Producer and `request_id` from another attempt is no resend.
                if self.rng.chance(15) {
                    let (writer, _) = writer_of(again.producer.as_ref().unwrap());
                    again.producer = Some(loop {
                        let other = producer(&mut self.rng);
                        if writer_of(&other).0 == writer {
                            break other;
                        }
                    });
                }
                AppendRecordsRequest { records, ..again }
            }
        };
        self.sent.push(request.clone());
        request
    }

    fn next_from(&mut self, stream_id: &str) -> Vec<u8> {
        let known = &self.known[stream_id];
        let newest = known.newest();
        // An offset of this Store is the `<ms>` and the `<seq>` of an entry id, 8 bytes each.
        let after = |offset: &[u8], seq: u64| [&offset[..8], &seq.to_be_bytes()[..]].concat();
        match self.rng.below(100) {
            0..30 => Vec::new(),
            30..70 if !known.records.is_empty() => {
                let at = self.rng.below(known.records.len());
                known.records[at].offset.clone()
            }
            70..80 => newest,
            // Above the newest Record, by one entry or by far.
            80..84 if !newest.is_empty() => {
                let seq = u64::from_be_bytes(newest[8..].try_into().unwrap());
                after(&newest, seq + 1)
            }
            84..88 => vec![0xff; 16],
            // Between two Records, at an id that no entry has.
            88..95 if !known.records.is_empty() => {
                let at = self.rng.below(known.records.len());
                after(&known.records[at].offset, u64::MAX)
            }
            95..97 => vec![7; 5],
            _ => Vec::new(),
        }
    }

    /// Makes the fault of a call that is one, and returns the Stream of any other call.
    async fn apply_fault(&mut self, call: &Call) -> Option<String> {
        let stream_id = match call {
            Call::Create(request) => &request.stream_id,
            Call::Append(request) => &request.stream_id,
            Call::Close(request) => &request.stream_id,
            Call::Read(request) => &request.stream_id,
            Call::Describe(request) => &request.stream_id,
            Call::LoseLog(stream_id) => {
                let known = self.known.get_mut(stream_id).unwrap();
                let [_, log, ..] = keys(&self.prefix, stream_id);
                let _: i64 = redis::cmd("DEL")
                    .arg(log)
                    .query_async(&mut self.admin)
                    .await
                    .unwrap();
                known.lost = known.newest();
                return None;
            }
        };
        Some(stream_id.clone())
    }

    /// States the rules of a call on what the test knows, and records what the call changes.
    fn expected(&mut self, call: &Call) -> Expected {
        let refused = |reason| Expected::Unchanged(Answer::Refused(reason));
        let fences = self.fence == Fence::On;
        match call {
            Call::Create(request) => {
                let known = self.known.get_mut(&request.stream_id).unwrap();
                let created = |created| Answer::Created(CreateStreamResponse { created });
                let Some(created_by) = &known.created_by else {
                    known.created_by = Some(request.request_id.clone());
                    return Expected::Written(created(true));
                };
                let use_existing = StreamIdConflictPolicy::UseExisting as i32;
                if known.closed {
                    refused(StreamErrorReason::Closed)
                } else if !request.request_id.is_empty() && request.request_id == *created_by {
                    Expected::Unchanged(created(true))
                } else if request.id_conflict_policy != use_existing {
                    refused(StreamErrorReason::AlreadyExists)
                } else {
                    Expected::Unchanged(created(false))
                }
            }
            Call::Append(request) => {
                let known = &self.known[&request.stream_id];
                if known.created_by.is_none() {
                    return refused(StreamErrorReason::NotFound);
                }
                let (writer, attempt) = writer_of(request.producer.as_ref().unwrap());
                let writer = known.writers.get(&writer);
                let resent = writer.into_iter().flat_map(|w| &w.batches).find(|batch| {
                    !request.request_id.is_empty()
                        && batch.request_id == request.request_id
                        && batch.attempt == attempt
                });
                if let Some(batch) = resent {
                    return Expected::Unchanged(Answer::Appended(batch.answer.clone()));
                }
                let newest_attempt = writer.and_then(|w| w.newest_attempt);
                if fences && newest_attempt.is_some_and(|newest| attempt < newest) {
                    return refused(StreamErrorReason::Fenced);
                }
                if known.closed {
                    return refused(StreamErrorReason::Closed);
                }
                Expected::NewBatch
            }
            Call::Close(request) => {
                let known = self.known.get_mut(&request.stream_id).unwrap();
                if known.created_by.is_none() {
                    return refused(StreamErrorReason::NotFound);
                }
                let closed = Answer::Closed(CloseStreamResponse {});
                if known.closed {
                    return Expected::Unchanged(closed);
                }
                let (writer, attempt) = writer_of(request.producer.as_ref().unwrap());
                let newest_attempt = known.writers.get(&writer).and_then(|w| w.newest_attempt);
                if fences && newest_attempt.is_some_and(|newest| attempt < newest) {
                    return refused(StreamErrorReason::Fenced);
                }
                known.closed = true;
                Expected::Written(closed)
            }
            Call::Describe(request) => {
                let known = &self.known[&request.stream_id];
                if known.created_by.is_none() {
                    return refused(StreamErrorReason::NotFound);
                }
                let status = match known.closed {
                    true => StreamStatus::Closed,
                    false => StreamStatus::Open,
                };
                Expected::Unchanged(Answer::Described(DescribeStreamResponse {
                    status: status as i32,
                    last_offset: known.newest(),
                }))
            }
            Call::Read(request) => {
                let known = &self.known[&request.stream_id];
                let from = &request.from_offset;
                if !from.is_empty() && from.len() != 16 {
                    return refused(StreamErrorReason::InvalidArgument);
                }
                if known.created_by.is_none() {
                    return refused(StreamErrorReason::NotFound);
                }
                if *from > known.newest() {
                    return refused(StreamErrorReason::OffsetTooHigh);
                }
                if *from < known.lost {
                    return refused(StreamErrorReason::Expired);
                }
                Expected::Unchanged(Answer::Page(page(known, request)))
            }
            Call::LoseLog(_) => unreachable!("a fault has no answer"),
        }
    }

    async fn send(&self, call: &Call) -> Answer {
        fn answer<T, R: Into<StreamErrorReason>>(
            result: Result<T, temporalio_streams::StreamError<R>>,
            ok: fn(T) -> Answer,
        ) -> Answer {
            result.map_or_else(|error| Answer::Refused(error.reason.into()), ok)
        }
        let runtime = &self.runtime;
        match call {
            Call::Create(request) => answer(
                runtime.create_stream(request.clone()).await,
                Answer::Created,
            ),
            Call::Append(request) => answer(
                runtime.append_records(request.clone()).await,
                Answer::Appended,
            ),
            Call::Close(request) => {
                answer(runtime.close_stream(request.clone()).await, Answer::Closed)
            }
            Call::Read(request) => {
                let page = runtime.read_records(request.clone()).await;
                // The Store keeps to both limits of a read, so the runtime has nothing to cut.
                let bare = self.store.read_records(request.clone()).await;
                assert_eq!(bare, page, "seed {}, {request:?}", self.seed);
                answer(page, Answer::Page)
            }
            Call::Describe(request) => answer(
                runtime.describe_stream(request.clone()).await,
                Answer::Described,
            ),
            Call::LoseLog(_) => unreachable!("a fault is no call of the Store"),
        }
    }

    /// Checks the answer of a batch that the Store had to write, and records the batch.
    fn accept(&mut self, call: &Call, answer: Answer, context: &str) {
        let (Call::Append(request), Answer::Appended(appended)) = (call, &answer) else {
            panic!("the Store didn't write the batch: {answer:?}. {context}");
        };
        let known = self.known.get_mut(&request.stream_id).unwrap();
        let producer = request.producer.as_ref().unwrap();
        let first = &appended.first_offset;
        assert!(first.len() == 16 && *first > known.newest(), "{context}");
        // The ids of a batch have one `<ms>` and go up by one in `<seq>`.
        let first_seq = u64::from_be_bytes(first[8..].try_into().unwrap());
        for (seq, record) in (first_seq..).zip(&request.records) {
            known.records.push(StreamRecord {
                producer: Some(producer.clone()),
                body: record.body.clone(),
                offset: [&first[..8], &seq.to_be_bytes()[..]].concat(),
            });
        }
        assert_eq!(appended.last_offset, known.newest(), "{context}");
        let (writer, attempt) = writer_of(producer);
        let writer = known.writers.entry(writer).or_default();
        writer.newest_attempt = writer.newest_attempt.max(Some(attempt));
        writer.batches.push_front(Batch {
            attempt,
            request_id: request.request_id.clone(),
            answer: appended.clone(),
        });
        writer.batches.truncate(WINDOW);
    }

    /// Reads a Stream to its end, in pages of random sizes, and compares the Records with the
    /// ones of the accepted batches.
    async fn read_all(&mut self, stream_id: &str) {
        let known = &self.known[stream_id];
        if known.created_by.is_none() {
            return;
        }
        let mut from = known.lost.clone();
        let mut records = Vec::new();
        loop {
            let request = ReadRecordsRequest {
                stream_id: stream_id.to_string(),
                from_offset: from,
                max_records: self.rng.below(5) as i32,
                ..Default::default()
            };
            let page = self.runtime.read_records(request).await.unwrap();
            let ended = page.done || page.records.is_empty();
            records.extend(page.records);
            from = page.last_offset;
            if ended {
                assert_eq!(page.done, known.closed, "seed {}, {stream_id}", self.seed);
                break;
            }
        }
        let kept = known.records.iter().filter(|r| r.offset > known.lost);
        let kept: Vec<_> = kept.cloned().collect();
        assert_eq!(records, kept, "seed {}, {stream_id}", self.seed);
    }
}

/// Returns the page that a read gets, with the Records after its `from_offset` up to its limits.
/// The runtime counts a Record as the page encodes it, and it always keeps the first one.
fn page(known: &Known, request: &ReadRecordsRequest) -> ReadRecordsResponse {
    let after = known
        .records
        .iter()
        .filter(|r| r.offset > request.from_offset);
    let max_records = match request.max_records {
        0 => usize::MAX,
        max => max as usize,
    };
    let mut bytes = 0;
    let mut records = Vec::new();
    for record in after.take(max_records) {
        bytes += encoded_len(1, record);
        if request.max_bytes > 0 && bytes > request.max_bytes as usize && !records.is_empty() {
            break;
        }
        records.push(record.clone());
    }
    let last_offset = match records.last() {
        Some(record) => record.offset.clone(),
        None => request.from_offset.clone(),
    };
    ReadRecordsResponse {
        done: known.closed && last_offset == known.newest(),
        last_offset,
        records,
    }
}

/// What identifies a Producer, as the proto field number of its kind and the ids of that kind.
type WriterId = (u8, Vec<String>);

/// Returns what identifies the Producer of a write in a Stream, and the key of its attempt.
fn writer_of(producer: &Producer) -> (WriterId, AttemptKey) {
    let (kind, ids) = match producer.kind.as_ref().unwrap() {
        Kind::Workflow(of) => (1, vec![&of.namespace, &of.workflow_id, &of.run_id]),
        Kind::LocalActivity(of) => {
            let ids = vec![&of.namespace, &of.workflow_id, &of.run_id, &of.activity_id];
            (2, ids)
        }
        Kind::WorkflowActivity(of) => {
            let ids = vec![&of.namespace, &of.workflow_id, &of.run_id, &of.activity_id];
            (3, ids)
        }
        Kind::StandaloneActivity(of) => (4, vec![&of.namespace, &of.activity_id, &of.run_id]),
        Kind::Client(of) => (5, vec![&of.namespace, &of.id]),
    };
    let ids = ids.into_iter().cloned().collect();
    ((kind, ids), attempt_key(producer.kind.as_ref()))
}

/// Returns a Producer of one of the five kinds, with one of two run ids and a random attempt.
/// The two kinds of Activity that a Workflow runs have equal ids, and so have a Workflow and a
/// Standalone Activity, so only the kind tells their Producers apart. The numbers of an attempt
/// have bytes on both sides of 0x80 and of a byte's end, where a wrong byte order shows.
fn producer(rng: &mut Rng) -> Producer {
    let namespace = "ns".to_string();
    let (workflow_id, activity_id) = ("w".to_string(), "a".to_string());
    let run_id = format!("r{}", rng.below(2));
    let number = |rng: &mut Rng| rng.pick(&[1, 255, 256, i32::MAX]);
    let task = |rng: &mut Rng| WorkflowTaskAttempt {
        started_event_id: number(rng) as i64,
        attempt: rng.pick(&[1, 2]),
        started_time: None,
    };
    let activity = |rng: &mut Rng| {
        let mut attempt = ActivityAttempt {
            attempt_scheduled_time: Some(Default::default()),
            attempt: rng.pick(&[1, 2]),
        };
        attempt.attempt_scheduled_time.as_mut().unwrap().seconds = number(rng) as i64;
        Some(attempt)
    };
    let kind = match rng.below(5) {
        0 => Kind::Workflow(WorkflowProducer {
            namespace,
            workflow_id,
            run_id,
            attempt: Some(task(rng)),
        }),
        1 => Kind::LocalActivity(LocalActivityProducer {
            namespace,
            workflow_id,
            run_id,
            activity_id,
            attempt: Some(LocalActivityAttempt {
                task: Some(task(rng)),
                attempt: rng.pick(&[1, 256]),
            }),
        }),
        2 => Kind::WorkflowActivity(WorkflowActivityProducer {
            namespace,
            workflow_id,
            run_id,
            activity_id,
            attempt: activity(rng),
        }),
        3 => Kind::StandaloneActivity(StandaloneActivityProducer {
            namespace,
            activity_id: workflow_id,
            run_id,
            attempt: activity(rng),
        }),
        _ => Kind::Client(ClientProducer {
            namespace,
            id: run_id,
        }),
    };
    Producer { kind: Some(kind) }
}
