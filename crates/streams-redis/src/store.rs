//! The Redis Store. One Lua script runs each create, append and close, so the fence, dedupe and
//! close checks are atomic with the write. `docs/redis.md` has the layout of the stored data.

mod conn;
mod records;
mod replies;
mod scripts;

use crate::{KeyPrefix, RedisStoreError, RedisStoreOptions, RedisStoreResult};
use conn::{BlockingPool, Endpoint, Lease, Leased, Origin, SharedConnection, WaitPool};
use prost::Message;
use records::{Body, LogEntry, with_producers, without_producer};
use redis::{
    Cmd, ErrorKind, FromRedisValue, InfoDict, Pipeline, RedisError, RedisResult, RedisWrite,
    Script, ServerErrorKind, ToRedisArgs, Value,
};
use replies::{AppendReply, CloseReply, CreateReply, ReadReply};
use std::{collections::HashMap, num::NonZeroUsize, sync::Arc, time::Duration};
use temporalio_streams::{
    StreamResult,
    epoch::{TASK_KEY_LEN, epoch_key, task_key, task_of},
    ids::{StreamKeys, encode_part},
    offset::entry_id_of_offset,
    proto::{Producer, StreamHandle, StreamProducerKind, WorkflowTaskEpoch},
    store::{
        Appended, Batch, Capabilities, Closed, Created, Entry, FORMAT_VERSION, MAX_TTL, Owner,
        Store, StreamState, TOMBSTONE, TaskFacts, Withdrawal, owner_chain,
    },
};
use tokio::time::Instant;

/// The replicas that each write waits for, for how long, and the connections of those writes.
struct Wait {
    replicas: NonZeroUsize,
    timeout: Duration,
    pool: WaitPool,
}

/// The Redis Store.
#[derive(Clone)]
pub struct RedisStore {
    shared: SharedConnection,
    wait: Option<Arc<Wait>>,
    blocking: Arc<BlockingPool>,
    prefix: KeyPrefix,
    retention: Duration,
    safety_ttl: Duration,
}

impl std::fmt::Debug for RedisStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisStore")
            .field("prefix", &self.prefix)
            .field("retention", &self.retention)
            .field("safety_ttl", &self.safety_ttl)
            .finish_non_exhaustive()
    }
}

impl RedisStore {
    /// Connects to Redis and checks that it is Redis 7.0 or later.
    pub async fn connect(options: RedisStoreOptions) -> RedisStoreResult<Self> {
        let endpoint = Arc::new(check(&options)?);
        let shared = endpoint.open().await?;
        check_server(&shared).await?;
        if options.wait.is_some() {
            check_wait(&shared).await?;
        }
        let cluster = match &shared {
            SharedConnection::Cluster(cluster) => Some(cluster.clone()),
            SharedConnection::Single(_) => None,
        };
        let wait = match options.wait {
            None => None,
            Some(wait) => Some(Arc::new(Wait {
                replicas: wait.replicas,
                timeout: wait.timeout,
                pool: WaitPool::new(endpoint.open().await?, endpoint.clone(), cluster.clone()),
            })),
        };
        let blocking = BlockingPool::new(endpoint, options.blocking_reads_per_node, cluster);
        Ok(Self {
            shared,
            wait,
            blocking: Arc::new(blocking),
            prefix: options.key_prefix,
            retention: options.retention,
            safety_ttl: options.safety_ttl,
        })
    }

    fn keys(&self, namespace: &str, stream_id: &str) -> StreamKeys {
        StreamKeys::new(self.prefix.as_str(), namespace, stream_id)
    }

    /// Runs a write script, and then `WAIT` when the options set replicas. Both are in one
    /// pipeline on one connection, so the `WAIT` covers the script's writes.
    async fn write<T: FromRedisValue>(&self, call: &ScriptCall) -> RedisStoreResult<T> {
        let reply = match &self.wait {
            None => call.run(&mut self.shared.clone()).await?,
            Some(wait) => {
                let mut lease = match wait.pool.take().await? {
                    Leased::Busy => {
                        return Err(RedisStoreError::storage(
                            "Every connection for writes with WAIT stayed busy for the response \
                             timeout",
                        ));
                    }
                    // A write runs once, since the server may have run it before its connection
                    // closed.
                    Leased::Idle(lease) => Lease::from(lease),
                    Leased::New(lease) => lease,
                };
                let reply = Self::write_and_wait(wait, call, lease.conn()).await;
                lease.settle(reply)??
            }
        };
        Ok(redis::from_redis_value(reply).map_err(RedisError::from)?)
    }

    /// The Producer of the batch of `entry`, when the batch starts before `entry`.
    async fn batch_producer(
        &self,
        keys: &StreamKeys,
        entry: &LogEntry,
    ) -> RedisStoreResult<Option<Producer>> {
        let Body::Later { first, .. } = &entry.body else {
            return Ok(None);
        };
        let first = first.to_string();
        let entries: Vec<LogEntry> = redis::cmd("XRANGE")
            .arg(keys.log())
            .arg(&first)
            .arg(&first)
            .query_async(&mut self.shared.clone())
            .await?;
        Ok(entries.into_iter().next().and_then(LogEntry::producer))
    }

    /// The outer error comes from the connection, or from a script load that Redis refused. The
    /// inner one is the server's answer to the write.
    async fn write_and_wait(
        wait: &Wait,
        call: &ScriptCall,
        conn: &mut SharedConnection,
    ) -> RedisResult<RedisStoreResult<Value>> {
        let mut pipe = redis::pipe();
        pipe.ignore_errors()
            .add_command(call.cmd.clone())
            .cmd("WAIT")
            .arg(wait.replicas)
            .arg(wait.timeout.as_millis());
        let mut replies = write_replies(&pipe, conn).await?;
        // The script didn't run, so the pipeline runs again after the load.
        if let [Value::ServerError(error), _] = &replies
            && error.kind() == Some(ServerErrorKind::NoScript)
        {
            call.script.load_async(conn).await?;
            replies = write_replies(&pipe, conn).await?;
        }
        let [reply, acked] = replies;
        Ok(reply
            .extract_error()
            .map_err(RedisStoreError::from)
            .and_then(|reply| acknowledged(wait, acked).map(|()| reply)))
    }
}

// The trait returns Stream errors, so the runtime and lang get the same reasons on every backend.
// A Redis error converts through the crate's error, since this crate can't add that `From`.
#[async_trait::async_trait]
impl Store for RedisStore {
    // Every rule is in the Lua scripts.
    fn capabilities(&self) -> Capabilities {
        Capabilities::ALL
    }

    /// Creates a Stream, or finds it. A retry run's create withdraws its previous run. A
    /// Workflow-owned Stream stores its handle's run chain, which later creates and closes
    /// must match.
    async fn create(
        &self,
        namespace: &str,
        handle: &StreamHandle,
        retention: Option<Duration>,
        owner: Owner,
        withdrawal: &Withdrawal<'_>,
    ) -> StreamResult<Created> {
        let keys = self.keys(namespace, &handle.id);
        let retention = retention.filter(|r| !r.is_zero()).unwrap_or(self.retention);
        let (owner, chain) = match owner {
            Owner::Workflow => ("wf", owner_chain(handle)),
            Owner::Client => ("client", String::new()),
        };
        let mut call = ScriptCall::write(&scripts::CREATE_SCRIPT, &keys);
        call.cmd
            .arg(FORMAT_VERSION)
            .arg(retention.as_millis())
            .arg(owner)
            .arg(self.safety_ttl.as_millis())
            .arg(TOMBSTONE.as_millis())
            .arg(encode_part(withdrawal.withdraw_run))
            .arg(withdrawal.superseded)
            .arg(chain)
            .arg(self.wait.is_some());
        let CreateReply(created) = self.write(&call).await?;
        Ok(created)
    }

    async fn append(
        &self,
        namespace: &str,
        handle: &StreamHandle,
        batch: &Batch<'_>,
    ) -> StreamResult<Appended> {
        let keys = self.keys(namespace, &handle.id);
        let producer = batch.producer;
        let epoch = producer.epoch.unwrap_or_default();
        let withdrawal = &batch.withdrawal;
        let mut call = ScriptCall::write(&scripts::APPEND_SCRIPT, &keys);
        call.cmd
            .arg(FORMAT_VERSION)
            .arg(producer.kind)
            .arg(&producer.id)
            .arg(encode_part(&producer.run_id))
            .arg(&epoch_key(&epoch)[..])
            .arg(batch.sequence)
            .arg(encode_part(withdrawal.withdraw_run))
            .arg(withdrawal.superseded)
            .arg(self.safety_ttl.as_millis())
            .arg(TOMBSTONE.as_millis())
            .arg(TaskArgs::of(batch))
            .arg(&batch.digest[..])
            .arg(self.wait.is_some())
            .arg(producer.encode_to_vec());
        for record in batch.records {
            call.cmd.arg(without_producer(record, producer)?);
        }
        let AppendReply(appended) = self.write(&call).await?;
        Ok(appended)
    }

    /// Closes a Stream with an encoded close Record, if the Producer's kind and the handle's run
    /// chain are the owner's.
    async fn close(
        &self,
        namespace: &str,
        handle: &StreamHandle,
        producer: &Producer,
        record: &[u8],
    ) -> StreamResult<Closed> {
        let keys = self.keys(namespace, &handle.id);
        let epoch = producer.epoch.unwrap_or_default();
        let mut call = ScriptCall::write(&scripts::CLOSE_SCRIPT, &keys);
        call.cmd
            .arg(FORMAT_VERSION)
            .arg(producer.kind)
            .arg(&producer.id)
            .arg(encode_part(&producer.run_id))
            .arg(&epoch_key(&epoch)[..])
            .arg(TOMBSTONE.as_millis())
            .arg(record)
            .arg(owner_chain(handle))
            .arg(self.wait.is_some());
        let CloseReply(closed) = self.write(&call).await?;
        Ok(closed)
    }

    /// Reads the Stream's state and up to `count` entries from `from`, inclusive. Zero reads
    /// from the start of the log.
    async fn read(
        &self,
        namespace: &str,
        stream_id: &str,
        from: i64,
        count: usize,
    ) -> StreamResult<(StreamState, Vec<Entry>)> {
        let keys = self.keys(namespace, stream_id);
        let start = if from > 0 {
            entry_id_of_offset(from)
        } else {
            "-".to_string()
        };
        let mut call = ScriptCall::read(&keys);
        call.cmd.arg(start).arg(count);
        let reply = call
            .run(&mut self.shared.clone())
            .await
            .map_err(RedisStoreError::from)?;
        let ReadReply {
            state,
            entries,
            earlier,
        } = redis::from_redis_value(reply)
            .map_err(|error| RedisStoreError::from(RedisError::from(error)))?;
        Ok((state, with_producers(entries, earlier)?))
    }

    /// Reads up to `count` entries at or before `before`, newest first, with `XREVRANGE`.
    async fn read_back(
        &self,
        namespace: &str,
        stream_id: &str,
        before: i64,
        count: usize,
    ) -> StreamResult<Vec<Entry>> {
        let keys = self.keys(namespace, stream_id);
        let mut entries: Vec<LogEntry> = redis::cmd("XREVRANGE")
            .arg(keys.log())
            .arg(entry_id_of_offset(before))
            .arg("-")
            .arg("COUNT")
            .arg(count)
            .query_async(&mut self.shared.clone())
            .await
            .map_err(RedisStoreError::from)?;
        entries.reverse();
        // The oldest entry's batch can start below the page, so its Producer comes from there.
        let earlier = match entries.first() {
            Some(oldest) => self.batch_producer(&keys, oldest).await?,
            None => None,
        };
        let mut entries = with_producers(entries, earlier)?;
        entries.reverse();
        Ok(entries)
    }

    /// The wait uses a dedicated connection, so it doesn't block the short calls.
    async fn wait_after(
        &self,
        namespace: &str,
        stream_id: &str,
        after: i64,
        deadline: Instant,
    ) -> StreamResult<bool> {
        let keys = self.keys(namespace, stream_id);
        let after = if after > 0 {
            entry_id_of_offset(after)
        } else {
            "0-0".to_string()
        };
        Ok(self
            .blocking
            .wait_after(&keys.log(), &after, deadline)
            .await
            .map_err(RedisStoreError::from)?)
    }
}

/// A call of a script with the Stream's keys. It keeps its script, so a reply of `NOSCRIPT` loads
/// the same one.
struct ScriptCall {
    script: &'static Script,
    cmd: Cmd,
}

impl ScriptCall {
    /// A write script call, with the meta, the log and `:producers`.
    fn write(script: &'static Script, keys: &StreamKeys) -> Self {
        let mut cmd = redis::cmd("EVALSHA");
        cmd.arg(script.get_hash())
            .arg(3)
            .arg(keys.meta())
            .arg(keys.log())
            .arg(keys.producers());
        Self { script, cmd }
    }

    /// The read script call. With `EVALSHA_RO`, a Reader's user needs only read access to the
    /// keys.
    fn read(keys: &StreamKeys) -> Self {
        let script = &*scripts::READ_SCRIPT;
        let mut cmd = redis::cmd("EVALSHA_RO");
        cmd.arg(script.get_hash())
            .arg(2)
            .arg(keys.meta())
            .arg(keys.log());
        Self { script, cmd }
    }

    /// Runs the call, and loads the script when Redis lacks it.
    async fn run(&self, conn: &mut SharedConnection) -> RedisResult<Value> {
        match self.cmd.query_async(conn).await {
            Err(error) if error.kind() == ErrorKind::Server(ServerErrorKind::NoScript) => {
                self.script.load_async(conn).await?;
                self.cmd.query_async(conn).await
            }
            result => result,
        }
    }
}

/// Refuses a write that fewer than `wait.replicas` replicas have.
fn acknowledged(wait: &Wait, acked: Value) -> RedisStoreResult<()> {
    let acked: usize = redis::from_redis_value(acked.extract_error()?)
        .map_err(|error| bad_reply("WAIT", error))?;
    if acked < wait.replicas.get() {
        return Err(RedisStoreError::storage(format!(
            "{acked} of {} replicas had the write after {:?}. A failover can lose it, and a retry \
             with the same sequence writes it once",
            wait.replicas, wait.timeout
        )));
    }
    Ok(())
}

/// The replies of a write pipeline, the script's and the `WAIT`'s.
async fn write_replies(pipe: &Pipeline, conn: &mut SharedConnection) -> RedisResult<[Value; 2]> {
    let replies: Vec<Value> = pipe.query_async(conn).await?;
    <[Value; 2]>::try_from(replies).map_err(|replies| {
        RedisError::from((
            ErrorKind::Parse,
            "Redis gave a write pipeline the wrong number of answers",
            replies.len().to_string(),
        ))
    })
}

/// What an append batch gives the script about its Workflow Task.
enum TaskArgs<'a> {
    /// The batch isn't of a Local Activity of a run, so the script tracks no task.
    Untracked,
    /// A Local Activity's task, with no task facts, as from an older Core.
    Task([u8; TASK_KEY_LEN]),
    /// A Local Activity's task, with the run's task facts.
    Facts {
        task: [u8; TASK_KEY_LEN],
        facts: TaskFacts<'a>,
    },
}

impl<'a> TaskArgs<'a> {
    fn of(batch: &Batch<'a>) -> Self {
        let producer = batch.producer;
        let kind = StreamProducerKind::LocalActivity as i32;
        if producer.kind != kind || producer.run_id.is_empty() {
            return Self::Untracked;
        }
        let task = task_key(&task_of(&producer.epoch.unwrap_or_default()));
        match batch.withdrawal.tasks {
            None => Self::Task(task),
            Some(facts) => Self::Facts { task, facts },
        }
    }
}

/// The four task arguments of `append.lua`. These are the task key, the completed task key, the
/// failed tasks' started event ids, 8 bytes each, and the task Superseded facts. With no task facts,
/// the completed task is empty, and before the run's first completed task it is the zero key.
impl ToRedisArgs for TaskArgs<'_> {
    fn write_redis_args<W: ?Sized + RedisWrite>(&self, out: &mut W) {
        let (task, facts) = match self {
            Self::Untracked => (&[][..], None),
            Self::Task(task) => (&task[..], None),
            Self::Facts { task, facts } => (&task[..], Some(facts)),
        };
        out.write_arg(task);
        let Some(facts) = facts else {
            for _ in 0..3 {
                out.write_arg(b"");
            }
            return;
        };
        let zero = WorkflowTaskEpoch::default();
        out.write_arg(&task_key(facts.completed.unwrap_or(&zero)));
        let failed: Vec<u8> = facts
            .failed_tasks
            .iter()
            .flat_map(|id| id.to_be_bytes())
            .collect();
        out.write_arg(&failed);
        out.write_arg(facts.superseded);
    }
}

fn bad_reply(call: &str, reply: impl std::fmt::Debug) -> RedisStoreError {
    RedisStoreError::storage(format!("Redis gave the {call} script the answer {reply:?}"))
}

/// Refuses options that can't work, before any connection, and parses the URLs.
pub(crate) fn check(options: &RedisStoreOptions) -> RedisStoreResult<Endpoint> {
    check_options(options)?;
    Endpoint::try_from(options)
        .map_err(|error| RedisStoreError::invalid(format!("The Redis URLs are wrong: {error}")))
}

fn check_options(options: &RedisStoreOptions) -> RedisStoreResult<()> {
    // The scripts take whole milliseconds, so a shorter TTL would be zero there.
    let ttls = [options.retention, options.safety_ttl];
    if ttls
        .iter()
        .any(|ttl| ttl.as_millis() == 0 || *ttl > MAX_TTL)
    {
        return Err(RedisStoreError::invalid(format!(
            "The Retention and the safety TTL must be at least 1 ms and at most {} s",
            MAX_TTL.as_secs()
        )));
    }
    if options.response_timeout.is_zero() {
        return Err(RedisStoreError::invalid(
            "The response timeout must be above zero",
        ));
    }
    // A `WAIT` with a zero timeout waits for the replicas forever. The script and the round trip
    // need the other half of the response timeout, or too few replicas looks like a lost answer.
    if let Some(wait) = &options.wait
        && (wait.timeout.as_millis() == 0 || wait.timeout > options.response_timeout / 2)
    {
        return Err(RedisStoreError::invalid(
            "The wait timeout must be at least 1 ms and at most half the response timeout",
        ));
    }
    Ok(())
}

/// Refuses a user that can't run `WAIT`. Its write would run the script, fail the `WAIT`, and
/// report a write that happened as a lasting failure.
async fn check_wait(shared: &SharedConnection) -> RedisStoreResult<()> {
    let wait: RedisResult<Value> = redis::cmd("WAIT")
        .arg(0)
        .arg(1)
        .query_async(&mut shared.clone())
        .await;
    match wait {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == ErrorKind::Server(ServerErrorKind::NoPerm) => Err(
            RedisStoreError::invalid("A Store with `wait` needs a Redis user with `+wait`"),
        ),
        Err(error) => Err(error.into()),
    }
}

/// Refuses a server older than Redis 7.0, and warns about one that may evict Stream keys.
async fn check_server(shared: &SharedConnection) -> RedisStoreResult<()> {
    let mut info = redis::cmd("INFO");
    info.arg("server");
    match shared.on_each_primary::<InfoDict>(info).await {
        Ok(replies) => {
            for (origin, info) in replies {
                check_version(&origin, &info)?;
            }
        }
        // A user with only the Store's commands may not have INFO. Then a Redis before 7.0 fails at
        // the first read, on the flags of the read script.
        Err(error) if error.kind() == ErrorKind::Server(ServerErrorKind::NoPerm) => {
            tracing::debug!("Could not read the Redis version: {error}");
        }
        Err(error) => return Err(error.into()),
    }
    if let SharedConnection::Single(_) = shared {
        check_not_cluster(shared).await?;
    }
    let mut policy = redis::cmd("CONFIG");
    policy.arg("GET").arg("maxmemory-policy");
    let policies = shared
        .on_each_primary::<HashMap<String, String>>(policy)
        .await;
    let could_not_read = |why: &dyn std::fmt::Display| {
        tracing::warn!(
            "Could not read maxmemory-policy: {why}. The Streams need noeviction, since eviction \
             loses Records and their fence."
        )
    };
    match policies {
        Ok(policies) => {
            for (origin, mut config) in policies {
                match config.remove("maxmemory-policy").as_deref() {
                    Some("noeviction") => {}
                    Some(policy) => tracing::warn!(
                        node = %origin,
                        policy,
                        "Redis may evict Stream keys under memory pressure, which loses Records \
                         and their fence. Use maxmemory-policy noeviction for Streams."
                    ),
                    None => could_not_read(&format!("{origin} gave no value")),
                }
            }
        }
        // Managed services often refuse CONFIG, and some default to a `volatile-*` policy.
        Err(error) if error.code().is_some() => could_not_read(&error),
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

/// Refuses a cluster node for a standalone Store. Its keys of other slots would answer with
/// redirects on each call.
async fn check_not_cluster(shared: &SharedConnection) -> RedisStoreResult<()> {
    let reply: RedisResult<InfoDict> = redis::cmd("INFO")
        .arg("cluster")
        .query_async(&mut shared.clone())
        .await;
    match reply {
        Ok(info) if info.get::<i64>("cluster_enabled") == Some(1) => Err(RedisStoreError::invalid(
            "The server is a Redis Cluster node, so set `cluster`",
        )),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == ErrorKind::Server(ServerErrorKind::NoPerm) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn check_version(node: &Origin, info: &InfoDict) -> RedisStoreResult<()> {
    let version: String = info
        .get("redis_version")
        .ok_or_else(|| RedisStoreError::invalid(format!("{node} reports no Redis version")))?;
    let major: u32 = version
        .split('.')
        .next()
        .and_then(|major| major.parse().ok())
        .ok_or_else(|| {
            RedisStoreError::invalid(format!("{node} reports the Redis version {version:?}"))
        })?;
    if major < 7 {
        return Err(RedisStoreError::invalid(format!(
            "The Redis Store needs Redis 7.0 or later, but {node} reports {version}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use temporalio_streams::{Reason, StreamError, store::DEFAULT_SAFETY_TTL};

    #[test]
    fn a_server_before_redis_7_is_refused() {
        let check = |info: &str| check_version(&Origin::Server, &InfoDict::new(info));
        check("# Server\r\nredis_version:7.0.15\r\n").unwrap();
        check("redis_version:8.10.1").unwrap();
        assert!(check("redis_version:6.2.14").is_err());
        assert!(check("").is_err());
    }

    #[test]
    fn a_wait_timeout_must_leave_half_the_response_timeout() {
        let check = |timeout: Duration| {
            let mut options = RedisStoreOptions::new("redis://127.0.0.1:1");
            options.wait = Some(crate::WaitOptions {
                replicas: NonZeroUsize::MIN,
                timeout,
            });
            check_options(&options)
        };
        for timeout in [Duration::ZERO, Duration::from_millis(2501)] {
            assert!(
                matches!(check(timeout), Err(RedisStoreError::Invalid(_))),
                "{timeout:?}"
            );
        }
        check(Duration::from_millis(2500)).unwrap();
    }

    #[test]
    fn the_ttls_and_the_response_timeout_must_be_at_least_1_ms() {
        let check = |change: fn(&mut RedisStoreOptions)| {
            let mut options = RedisStoreOptions::new("redis://127.0.0.1:1");
            change(&mut options);
            check_options(&options)
        };
        let changes: [fn(&mut RedisStoreOptions); 3] = [
            |options| options.retention = Duration::from_micros(500),
            |options| options.safety_ttl = Duration::from_micros(500),
            |options| options.response_timeout = Duration::ZERO,
        ];
        for change in changes {
            assert!(matches!(check(change), Err(RedisStoreError::Invalid(_))));
        }
    }

    #[test]
    fn retention_hides_old_records_on_read() {
        let state = StreamState {
            exists: true,
            trimmed: 5,
            retention: Duration::from_millis(1000),
            now_ms: 3000,
            ..Default::default()
        };
        assert_eq!(
            state.retained_from(),
            2000 << temporalio_streams::offset::SEQ_BITS
        );
        let state = StreamState {
            trimmed: 9000 << temporalio_streams::offset::SEQ_BITS,
            ..state
        };
        assert_eq!(
            state.retained_from(),
            (9000 << temporalio_streams::offset::SEQ_BITS) + 1
        );
    }

    #[tokio::test]
    #[ignore = "needs Redis"]
    async fn bad_task_facts_are_refused_before_the_first_write() {
        let mut options = RedisStoreOptions::new(crate::testing::standalone_url());
        options.key_prefix = "streams-unit".parse().unwrap();
        let store = RedisStore::connect(options).await.unwrap();
        let namespace = format!("ns-{}", std::process::id());
        // The script is the format that every SDK calls, so these are arguments that no Rust type
        // can build. They are a task key of another length, a completed task key of another
        // length, and failed ids that aren't 8 bytes each.
        let zero = task_key(&Default::default());
        let cases: [(&[u8], &[u8], &[u8]); 3] = [
            (b"zz", &zero, b""),
            (&zero, b"zz", b""),
            (&zero, &zero, b"abc"),
        ];
        for (i, (task, completed, failed)) in cases.into_iter().enumerate() {
            let handle = StreamHandle {
                id: format!("s{i}"),
                ..Default::default()
            };
            let none = Withdrawal::default();
            store
                .create(&namespace, &handle, None, Owner::Client, &none)
                .await
                .unwrap();
            let keys = store.keys(&namespace, &handle.id);
            let mut shared = store.shared.clone();
            let _: () = redis::cmd("HSET")
                .arg(keys.meta())
                .arg("run|r0")
                .arg("1")
                .query_async(&mut shared)
                .await
                .unwrap();
            // A Local Activity of run r1 that withdraws r0, with one malformed task fact.
            let mut call = ScriptCall::write(&scripts::APPEND_SCRIPT, &keys);
            call.cmd
                .arg(FORMAT_VERSION)
                .arg(StreamProducerKind::LocalActivity as i32)
                .arg("la")
                .arg("r1")
                .arg(&epoch_key(&Default::default())[..])
                .arg(1)
                .arg("r0")
                .arg(b"")
                .arg(DEFAULT_SAFETY_TTL.as_millis())
                .arg(TOMBSTONE.as_millis())
                .arg(task)
                .arg(completed)
                .arg(failed)
                .arg(b"")
                .arg(&[0; 16][..])
                .arg("")
                .arg(b"")
                .arg(b"r");
            let error = call.run(&mut shared).await.unwrap_err();
            let error = StreamError::from(RedisStoreError::from(error));
            assert_eq!(error.reason, Reason::Invalid, "{error}");
            let fence: Option<String> = redis::cmd("HGET")
                .arg(keys.meta())
                .arg("wr|r0")
                .query_async(&mut shared)
                .await
                .unwrap();
            assert_eq!(fence, None);
        }
    }

    #[tokio::test]
    #[ignore = "needs Redis"]
    async fn a_read_inside_a_batch_gets_the_producer_of_the_batch() {
        use temporalio_streams::proto::{
            StreamData, StreamProducerKind, StreamRecord, stream_record::Item,
        };
        let mut options = RedisStoreOptions::new(crate::testing::standalone_url());
        options.key_prefix = "streams-unit".parse().unwrap();
        let store = RedisStore::connect(options).await.unwrap();
        let (namespace, handle) = (
            format!("ns-{}", std::process::id()),
            StreamHandle {
                id: "batch".to_string(),
                ..Default::default()
            },
        );
        store
            .create(
                &namespace,
                &handle,
                None,
                Owner::Client,
                &Withdrawal::default(),
            )
            .await
            .unwrap();
        let producer = Producer {
            kind: StreamProducerKind::Client as i32,
            id: "client:unit".to_string(),
            ..Default::default()
        };
        let record = StreamRecord {
            item: Some(Item::Data(StreamData {
                producer: Some(producer.clone()),
                ..Default::default()
            })),
            ..Default::default()
        }
        .encode_to_vec();
        let records = vec![record.clone(); 5];
        let batch = Batch {
            producer: &producer,
            sequence: 1,
            records: &records,
            digest: [0; 16],
            withdrawal: Withdrawal::default(),
        };
        let (first, last) = store
            .append(&namespace, &handle, &batch)
            .await
            .unwrap()
            .offsets
            .unwrap();
        let (_, forward) = store.read(&namespace, "batch", first + 2, 2).await.unwrap();
        let back = store
            .read_back(&namespace, "batch", last - 1, 2)
            .await
            .unwrap();
        for entry in forward.iter().chain(&back) {
            assert_eq!(entry.record.as_deref(), Some(record.as_slice()));
        }
        assert_eq!((forward.len(), back.len()), (2, 2));
    }
}
