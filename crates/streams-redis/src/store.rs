//! The Redis Store. One Lua script runs each create, append and close, so the fence, dedupe and
//! close checks are atomic with the write. `docs/redis.md` has the layout of the stored data.

mod conn;
mod scripts;

use crate::{KeyPrefix, RedisStoreError, RedisStoreOptions, RedisStoreResult};
use conn::{BlockingPool, Endpoint, SharedConnection, WaitPool};
use redis::{Cmd, ErrorKind, RedisResult, Script, ServerErrorKind, Value};
use std::{num::NonZeroUsize, sync::Arc, time::Duration};
use temporalio_streams::{
    StreamResult,
    epoch::{epoch_key, task_key, task_of},
    ids::{StreamKeys, encode_part},
    offset::{entry_id_of_offset, offset_of_entry_id},
    proto::{Producer, StreamHandle, StreamProducerKind},
    store::{
        Appended, Batch, Capabilities, Closed, CreateOutcome, Created, Entry, FORMAT_VERSION,
        MAX_TTL, Owner, Store, StreamState, TOMBSTONE, Withdrawal, owner_chain,
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
    async fn write(&self, script: &Script, cmd: &Cmd) -> RedisStoreResult<Value> {
        let Some(wait) = &self.wait else {
            return run_script(script, cmd, &mut self.shared.clone())
                .await
                .map_err(RedisStoreError::from);
        };
        let Some(mut lease) = wait.pool.take().await? else {
            return Err(RedisStoreError::storage(
                "Every connection for writes with WAIT stayed busy for the response timeout",
            ));
        };
        let reply = Self::write_and_wait(wait, script, cmd, lease.conn()).await;
        lease.settle(reply)?
    }

    /// Whether each write runs a `WAIT` after it. The write scripts take it as their last argument.
    fn waits(&self) -> bool {
        self.wait.is_some()
    }

    /// The outer error comes from the connection, or from a script load that Redis refused. The
    /// inner one is the server's answer to the write.
    async fn write_and_wait(
        wait: &Wait,
        script: &Script,
        cmd: &Cmd,
        conn: &mut SharedConnection,
    ) -> RedisResult<RedisStoreResult<Value>> {
        let mut pipe = redis::pipe();
        pipe.ignore_errors()
            .add_command(cmd.clone())
            .cmd("WAIT")
            .arg(wait.replicas)
            .arg(wait.timeout.as_millis());
        let mut loaded = false;
        loop {
            let replies: Vec<Value> = pipe.query_async(conn).await?;
            let Ok([reply, acked]) = <[Value; 2]>::try_from(replies) else {
                return Ok(Err(RedisStoreError::storage("Redis gave WAIT no answer")));
            };
            match reply.extract_error() {
                Err(error) if !loaded && no_script(&error) => {
                    script.load_async(conn).await?;
                    loaded = true;
                }
                Err(error) => return Ok(Err(error.into())),
                Ok(reply) => return Ok(acknowledged(wait, acked).map(|()| reply)),
            }
        }
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
        let mut cmd = script_cmd("EVALSHA", &scripts::CREATE_SCRIPT, &keys);
        cmd.arg(FORMAT_VERSION)
            .arg(retention.as_millis())
            .arg(owner)
            .arg(self.safety_ttl.as_millis())
            .arg(TOMBSTONE.as_millis())
            .arg(encode_part(withdrawal.withdraw_run))
            .arg(withdrawal.superseded)
            .arg(chain)
            .arg(self.waits());
        let reply = self.write(&scripts::CREATE_SCRIPT, &cmd).await?;
        let [outcome, stored, flag] = strings::<3>(reply, "create")?;
        let outcome = match outcome.as_str() {
            "created" => CreateOutcome::Created,
            "exists" => CreateOutcome::Exists,
            other => return Err(bad_reply("create", other).into()),
        };
        let stored: u64 = stored.parse().map_err(|_| bad_reply("create", &stored))?;
        Ok(Created {
            outcome,
            retention: Duration::from_millis(stored),
            flagged: flagged(&flag, "create")?,
        })
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
        let mut cmd = script_cmd("EVALSHA", &scripts::APPEND_SCRIPT, &keys);
        cmd.arg(FORMAT_VERSION)
            .arg(producer.kind)
            .arg(&producer.id)
            .arg(encode_part(&producer.run_id))
            .arg(epoch_key(&epoch))
            .arg(batch.sequence)
            .arg(encode_part(withdrawal.withdraw_run))
            .arg(withdrawal.superseded)
            .arg(self.safety_ttl.as_millis())
            .arg(TOMBSTONE.as_millis())
            .arg(local_activity_task(producer))
            .arg(completed_arg(withdrawal))
            .arg(failed_list(withdrawal))
            .arg(withdrawal.tasks.map_or(&[][..], |tasks| tasks.superseded))
            .arg(batch.digest)
            .arg(self.waits());
        for record in batch.records {
            cmd.arg(record.as_slice());
        }
        let reply = self.write(&scripts::APPEND_SCRIPT, &cmd).await?;
        let [outcome, first, last, flag] = strings::<4>(reply, "append")?;
        let resend = match outcome.as_str() {
            "ok" => false,
            "resend" => true,
            other => return Err(bad_reply("append", other).into()),
        };
        let offsets = match (first.as_str(), last.as_str()) {
            ("", _) | (_, "") => None,
            (first, last) => Some((offset_of_entry_id(first)?, offset_of_entry_id(last)?)),
        };
        Ok(Appended {
            offsets,
            resend,
            flagged: flagged(&flag, "append")?,
        })
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
        let mut cmd = script_cmd("EVALSHA", &scripts::CLOSE_SCRIPT, &keys);
        cmd.arg(FORMAT_VERSION)
            .arg(producer.kind)
            .arg(&producer.id)
            .arg(encode_part(&producer.run_id))
            .arg(epoch_key(&epoch))
            .arg(TOMBSTONE.as_millis())
            .arg(record)
            .arg(owner_chain(handle))
            .arg(self.waits());
        let reply = self.write(&scripts::CLOSE_SCRIPT, &cmd).await?;
        let [outcome, id] = strings::<2>(reply, "close")?;
        let already = match outcome.as_str() {
            "closed" => false,
            "already" => true,
            other => return Err(bad_reply("close", other).into()),
        };
        Ok(Closed {
            offset: offset_of_entry_id(&id)?,
            already,
        })
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
        // With `EVALSHA_RO`, a Reader's user needs only read access to the keys.
        let mut cmd = redis::cmd("EVALSHA_RO");
        cmd.arg(scripts::READ_SCRIPT.get_hash())
            .arg(2)
            .arg(keys.meta())
            .arg(keys.log())
            .arg(start)
            .arg(count);
        let reply = run_script(&scripts::READ_SCRIPT, &cmd, &mut self.shared.clone())
            .await
            .map_err(RedisStoreError::from)?;
        Ok(read_reply(reply)?)
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
        let reply: Value = redis::cmd("XREVRANGE")
            .arg(keys.log())
            .arg(entry_id_of_offset(before))
            .arg("-")
            .arg("COUNT")
            .arg(count)
            .query_async(&mut self.shared.clone())
            .await
            .map_err(RedisStoreError::from)?;
        let Value::Array(entries) = reply else {
            return Err(bad_reply("read back", reply).into());
        };
        Ok(entries
            .into_iter()
            .map(entry)
            .collect::<RedisStoreResult<_>>()?)
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

/// A call of `script` with the Stream's keys, as `EVALSHA` or `EVALSHA_RO`.
fn script_cmd(name: &str, script: &Script, keys: &StreamKeys) -> Cmd {
    let mut cmd = redis::cmd(name);
    cmd.arg(script.get_hash())
        .arg(3)
        .arg(keys.meta())
        .arg(keys.log())
        .arg(keys.producers());
    cmd
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

fn no_script(error: &redis::RedisError) -> bool {
    error.kind() == ErrorKind::Server(ServerErrorKind::NoScript)
}

/// Runs a script call, and loads the script when Redis lacks it.
async fn run_script(script: &Script, cmd: &Cmd, conn: &mut SharedConnection) -> RedisResult<Value> {
    match cmd.query_async(conn).await {
        Err(error) if no_script(&error) => {
            script.load_async(conn).await?;
            cmd.query_async(conn).await
        }
        result => result,
    }
}

/// The task key of a Local Activity's dispatching Workflow Task, empty for any other kind.
fn local_activity_task(producer: &Producer) -> String {
    if producer.kind != StreamProducerKind::LocalActivity as i32 {
        return String::new();
    }
    task_key(&task_of(&producer.epoch.unwrap_or_default()))
}

/// The run's last completed task key, `-` when the batch gives no task facts.
fn completed_arg(withdrawal: &Withdrawal<'_>) -> String {
    match &withdrawal.tasks {
        None => "-".to_string(),
        Some(tasks) => tasks.completed.map(task_key).unwrap_or_default(),
    }
}

fn failed_list(withdrawal: &Withdrawal<'_>) -> String {
    let failed = withdrawal.tasks.map_or(&[][..], |tasks| tasks.failed_tasks);
    failed
        .iter()
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join(" ")
}

/// The flag of a script answer. Lua has no bool, and a nil would end the answer's array, so the
/// scripts give `1` or an empty string.
fn flagged(flag: &str, call: &str) -> RedisStoreResult<bool> {
    match flag {
        "1" => Ok(true),
        "" => Ok(false),
        other => Err(bad_reply(call, other)),
    }
}

fn bad_reply(call: &str, reply: impl std::fmt::Debug) -> RedisStoreError {
    RedisStoreError::storage(format!("Redis gave the {call} script the answer {reply:?}"))
}

fn text(value: &Value) -> Option<String> {
    match value {
        Value::BulkString(bytes) => String::from_utf8(bytes.clone()).ok(),
        Value::SimpleString(text) => Some(text.clone()),
        Value::Int(number) => Some(number.to_string()),
        Value::Nil => Some(String::new()),
        _ => None,
    }
}

fn strings<const N: usize>(reply: Value, call: &str) -> RedisStoreResult<[String; N]> {
    let Value::Array(items) = &reply else {
        return Err(bad_reply(call, &reply));
    };
    let texts: Option<Vec<String>> = items.iter().map(text).collect();
    texts
        .and_then(|texts| texts.try_into().ok())
        .ok_or_else(|| bad_reply(call, &reply))
}

fn read_reply(reply: Value) -> RedisStoreResult<(StreamState, Vec<Entry>)> {
    let Value::Array(parts) = reply else {
        return Err(bad_reply("read", reply));
    };
    let [
        Value::Array(meta),
        Value::Array(time),
        Value::Array(entries),
    ] = <[Value; 3]>::try_from(parts).map_err(|parts| bad_reply("read", parts))?
    else {
        return Err(bad_reply("read", "parts that are not arrays"));
    };
    // A field that the Stream doesn't have comes back as nil, and `text` gives it as empty.
    let meta: Vec<Option<String>> = meta
        .iter()
        .map(|value| text(value).filter(|text| !text.is_empty()))
        .collect();
    let field = |i: usize| meta.get(i).cloned().flatten();
    let offset = |i: usize| -> RedisStoreResult<i64> {
        field(i).map_or(Ok(0), |id| Ok(offset_of_entry_id(&id)?))
    };
    let number = |i: usize| -> RedisStoreResult<i64> {
        field(i).map_or(Ok(0), |text| {
            text.parse().map_err(|_| bad_reply("read", text))
        })
    };
    let time: Vec<i64> = time
        .iter()
        .map(|value| text(value).and_then(|text| text.parse().ok()))
        .collect::<Option<_>>()
        .ok_or_else(|| bad_reply("read", &time))?;
    let [seconds, micros] = <[i64; 2]>::try_from(time).map_err(|time| bad_reply("read", time))?;
    let state = StreamState {
        exists: field(0).is_some(),
        version: u32::try_from(number(0)?).map_err(|error| bad_reply("read", error))?,
        closed_at: match field(1) {
            Some(_) => Some(offset(2)?),
            None => None,
        },
        newest: offset(3)?,
        trimmed: offset(4)?,
        retention: Duration::from_millis(
            u64::try_from(number(5)?).map_err(|error| bad_reply("read", error))?,
        ),
        expires_ms: number(6)?,
        now_ms: seconds * 1000 + micros / 1000,
    };
    let entries = entries
        .into_iter()
        .map(entry)
        .collect::<RedisStoreResult<Vec<_>>>()?;
    Ok((state, entries))
}

fn entry(value: Value) -> RedisStoreResult<Entry> {
    let Value::Array(pair) = value else {
        return Err(bad_reply("read", value));
    };
    let [id, Value::Array(fields)] =
        <[Value; 2]>::try_from(pair).map_err(|pair| bad_reply("read", pair))?
    else {
        return Err(bad_reply("read", "an entry with no field array"));
    };
    let id = text(&id).ok_or_else(|| bad_reply("read", &id))?;
    let mut entry = Entry {
        offset: offset_of_entry_id(&id)?,
        record: None,
        superseded: None,
    };
    for pair in fields.chunks(2) {
        let (Some(Value::BulkString(name)), Some(Value::BulkString(value))) =
            (pair.first(), pair.get(1))
        else {
            continue;
        };
        match name.as_slice() {
            b"r" => entry.record = Some(value.clone()),
            b"s" => entry.superseded = Some(value.clone()),
            _ => {}
        }
    }
    Ok(entry)
}

/// Refuses options that can't work, before any connection, and parses the URLs.
pub(crate) fn check(options: &RedisStoreOptions) -> RedisStoreResult<Endpoint> {
    check_options(options)?;
    Endpoint::parse(&options.urls, options.cluster, options.response_timeout)
        .map_err(|error| RedisStoreError::invalid(format!("The Redis URLs are wrong: {error}")))
}

fn check_options(options: &RedisStoreOptions) -> RedisStoreResult<()> {
    if options.urls.is_empty() {
        return Err(RedisStoreError::invalid("The Redis Store needs a URL"));
    }
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
    match shared.on_each_primary(info).await {
        Ok(replies) => {
            for (node, reply) in replies {
                let text: String =
                    redis::from_redis_value(reply).map_err(|error| bad_reply("INFO", error))?;
                check_version(node.as_deref().unwrap_or("the server"), &text)?;
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
    match shared.on_each_primary(policy).await {
        Ok(replies) => {
            for (node, reply) in replies {
                if let Some(policy) = eviction_policy(reply).filter(|p| p != "noeviction") {
                    tracing::warn!(
                        node = node.as_deref().unwrap_or("the server"),
                        policy,
                        "Redis may evict Stream keys under memory pressure, which loses Records \
                         and their fence. Use maxmemory-policy noeviction for Streams."
                    );
                    break;
                }
            }
        }
        // Managed services often refuse CONFIG, and some default to a `volatile-*` policy.
        Err(error) => tracing::warn!(
            "Could not read maxmemory-policy: {error}. The Streams need noeviction, since \
             eviction loses Records and their fence."
        ),
    }
    Ok(())
}

/// Refuses a cluster node for a standalone Store. Its keys of other slots would answer with
/// redirects on each call.
async fn check_not_cluster(shared: &SharedConnection) -> RedisStoreResult<()> {
    let reply: RedisResult<String> = redis::cmd("INFO")
        .arg("cluster")
        .query_async(&mut shared.clone())
        .await;
    match reply {
        Ok(info) if info.lines().any(|line| line.trim() == "cluster_enabled:1") => Err(
            RedisStoreError::invalid("The server is a Redis Cluster node, so set `cluster`"),
        ),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == ErrorKind::Server(ServerErrorKind::NoPerm) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn check_version(node: &str, info: &str) -> RedisStoreResult<()> {
    let version = info
        .lines()
        .find_map(|line| line.trim().strip_prefix("redis_version:"))
        .unwrap_or("0");
    let major: u32 = version
        .split('.')
        .next()
        .and_then(|major| major.parse().ok())
        .unwrap_or(0);
    if major < 7 {
        return Err(RedisStoreError::invalid(format!(
            "The Redis Store needs Redis 7.0 or later, but {node} reports {version}"
        )));
    }
    Ok(())
}

fn eviction_policy(reply: Value) -> Option<String> {
    let flat: Vec<String> = match reply {
        Value::Map(pairs) => pairs
            .into_iter()
            .flat_map(|(name, value)| [text(&name), text(&value)])
            .collect::<Option<_>>()?,
        Value::Array(items) => items.iter().map(text).collect::<Option<_>>()?,
        _ => return None,
    };
    flat.chunks(2)
        .find(|pair| pair.first().map(String::as_str) == Some("maxmemory-policy"))
        .and_then(|pair| pair.get(1).cloned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use temporalio_streams::store::DEFAULT_SAFETY_TTL;

    #[test]
    fn a_server_before_redis_7_is_refused() {
        check_version("a", "# Server\r\nredis_version:7.0.15\r\n").unwrap();
        check_version("a", "redis_version:8.10.1").unwrap();
        assert!(check_version("a", "redis_version:6.2.14").is_err());
        assert!(check_version("a", "").is_err());
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
        let task = task_key(&Default::default());
        // A task key that isn't an epoch key, a failed list that isn't event ids, and a completed
        // task key that isn't an epoch key.
        let cases = [
            ("zz", "", "1"),
            (task.as_str(), "", "x"),
            (task.as_str(), "zz", "1"),
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
            let mut cmd = script_cmd("EVALSHA", &scripts::APPEND_SCRIPT, &keys);
            cmd.arg(FORMAT_VERSION)
                .arg(StreamProducerKind::LocalActivity as i32)
                .arg("la")
                .arg("r1")
                .arg(epoch_key(&Default::default()))
                .arg(1)
                .arg("r0")
                .arg(b"")
                .arg(DEFAULT_SAFETY_TTL.as_millis())
                .arg(TOMBSTONE.as_millis())
                .arg(task)
                .arg(completed)
                .arg(failed)
                .arg(b"")
                .arg("d")
                .arg("")
                .arg(b"r");
            let error = run_script(&scripts::APPEND_SCRIPT, &cmd, &mut shared)
                .await
                .unwrap_err();
            assert_eq!(error.code(), Some("STREAM_INVALID"), "{error}");
            let fence: Option<String> = redis::cmd("HGET")
                .arg(keys.meta())
                .arg("wr|r0")
                .query_async(&mut shared)
                .await
                .unwrap();
            assert_eq!(fence, None);
        }
    }
}
