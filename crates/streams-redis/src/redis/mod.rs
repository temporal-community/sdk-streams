//! The Redis Store. One Lua script runs each create, append and close, so the fence, dedupe and
//! close checks are atomic with the write. `docs/redis.md` has the layout of the stored data.

mod conn;
mod errors;
mod scripts;

use conn::{BlockingPool, Shared, WaitPool};
use errors::Mapped;
use redis::{Cmd, ErrorKind, RedisResult, Script, ServerErrorKind, Value};
use std::{sync::Arc, time::Duration};
use temporalio_streams::{
    StreamError, StreamResult,
    epoch::epoch_key,
    ids::{StreamKeys, encode_part},
    offset::{entry_id_of_offset, offset_of_entry_id},
    proto::{Producer, StreamRef},
    store::{
        Appended, Batch, Capabilities, Closed, CreateOutcome, Created, DEFAULT_RETENTION,
        DEFAULT_SAFETY_TTL, Entry, FORMAT_VERSION, MAX_TTL, Owner, Store, StreamState, TOMBSTONE,
        Withdrawal, owner_chain,
    },
};
use tokio::time::Instant;

/// The Redis connection options and the Stream TTLs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisStoreOptions {
    /// One `redis://` or `rediss://` URL, or the seed nodes of a cluster. Credentials and TLS
    /// come from the URL.
    pub urls: Vec<String>,
    /// Whether `urls` are the seeds of a Redis Cluster.
    pub cluster: bool,
    /// The prefix of every key, so that Streams can share a Redis with other data. It has only
    /// letters, digits, `-`, `.`, `_` and `~`.
    pub key_prefix: String,
    /// The Retention of a Stream whose create sets none.
    pub retention: Duration,
    /// How long an open Workflow-owned Stream stays after its last write.
    pub safety_ttl: Duration,
    /// How many blocking reads may wait on one node at once. Each has a connection of its own.
    pub blocking_reads_per_node: usize,
    /// How long a short call waits for its answer.
    pub response_timeout: Duration,
    /// How many replicas must have each create, append and close before it answers, with `WAIT`.
    /// Zero sends none. A write that fewer replicas have in `wait_timeout` is a Store failure.
    pub wait_replicas: usize,
    /// How long a write waits for its replicas. It must be at most half of `response_timeout`.
    pub wait_timeout: Duration,
}

impl RedisStoreOptions {
    /// The defaults for one Redis at `url`.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            urls: vec![url.into()],
            cluster: false,
            key_prefix: temporalio_streams::ids::DEFAULT_KEY_PREFIX.to_string(),
            retention: DEFAULT_RETENTION,
            safety_ttl: DEFAULT_SAFETY_TTL,
            blocking_reads_per_node: 256,
            response_timeout: Duration::from_secs(5),
            wait_replicas: 0,
            wait_timeout: Duration::from_secs(1),
        }
    }
}

/// The replicas that each write waits for, for how long, and the connections of those writes.
struct Wait {
    replicas: usize,
    timeout: Duration,
    pool: WaitPool,
}

/// The Redis Store.
#[derive(Clone)]
pub struct RedisStore {
    shared: Shared,
    wait: Option<Arc<Wait>>,
    blocking: Arc<BlockingPool>,
    prefix: String,
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

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

impl RedisStore {
    /// Connects to Redis and checks that it is Redis 7.0 or later.
    pub async fn connect(options: RedisStoreOptions) -> StreamResult<Self> {
        check_options(&options)?;
        let connect = || Shared::connect(&options.urls, options.cluster, options.response_timeout);
        let shared = connect().await.mapped()?;
        check_server(&shared).await?;
        let wait = match options.wait_replicas {
            0 => None,
            replicas => Some(Arc::new(Wait {
                replicas,
                timeout: options.wait_timeout,
                pool: WaitPool::new(
                    connect().await.mapped()?,
                    &options.urls,
                    options.cluster,
                    options.response_timeout,
                ),
            })),
        };
        let blocking = BlockingPool::new(
            &options.urls,
            options.blocking_reads_per_node,
            options.cluster,
            options.response_timeout,
        )
        .mapped()?;
        Ok(Self {
            shared,
            wait,
            blocking: Arc::new(blocking),
            prefix: options.key_prefix,
            retention: options.retention,
            safety_ttl: options.safety_ttl,
        })
    }

    /// The Retention of a Stream whose create sets none.
    pub fn default_retention(&self) -> Duration {
        self.retention
    }

    fn keys(&self, namespace: &str, stream_id: &str) -> StreamKeys {
        StreamKeys::new(&self.prefix, namespace, stream_id)
    }

    /// Creates a Stream, or finds it. A Workflow-owned Stream stores its ref's run chain, which
    /// later creates and closes must name.
    pub async fn create(
        &self,
        namespace: &str,
        stream: &StreamRef,
        retention: Option<Duration>,
        owner: Owner,
        _withdrawal: &Withdrawal<'_>,
    ) -> StreamResult<Created> {
        let keys = self.keys(namespace, &stream.id);
        let retention = retention.filter(|r| !r.is_zero()).unwrap_or(self.retention);
        let (owner, chain) = match owner {
            Owner::Workflow => ("wf", owner_chain(stream)),
            Owner::Client => ("client", String::new()),
        };
        let mut cmd = script_cmd("EVALSHA", &scripts::CREATE_SCRIPT, &keys);
        cmd.arg(FORMAT_VERSION)
            .arg(millis(retention))
            .arg(owner)
            .arg(millis(self.safety_ttl))
            .arg(millis(TOMBSTONE))
            .arg(chain)
            .arg(self.wait_flag());
        let reply = self.write(&scripts::CREATE_SCRIPT, &cmd).await?;
        let [outcome, stored] = strings::<2>(reply, "create")?;
        let outcome = match outcome.as_str() {
            "created" => CreateOutcome::Created,
            "exists" => CreateOutcome::Exists,
            other => return Err(bad_reply("create", other)),
        };
        let stored: u64 = stored.parse().map_err(|_| bad_reply("create", &stored))?;
        Ok(Created {
            outcome,
            retention: Duration::from_millis(stored),
            flagged: false,
        })
    }

    /// Appends one batch.
    pub async fn append(
        &self,
        namespace: &str,
        stream_id: &str,
        batch: &Batch<'_>,
    ) -> StreamResult<Appended> {
        let keys = self.keys(namespace, stream_id);
        let producer = batch.producer;
        let epoch = producer.epoch.unwrap_or_default();
        let withdrawal = &batch.withdrawal;
        let mut cmd = script_cmd("EVALSHA", &scripts::APPEND_SCRIPT, &keys);
        cmd.arg(FORMAT_VERSION)
            .arg(producer.kind)
            .arg(&producer.id)
            .arg(epoch_key(&epoch))
            .arg(batch.sequence)
            .arg(withdrawal.supersession)
            .arg(millis(self.safety_ttl))
            .arg(millis(TOMBSTONE))
            .arg(batch.digest)
            .arg(self.wait_flag());
        for record in batch.records {
            cmd.arg(record.as_slice());
        }
        let reply = self.write(&scripts::APPEND_SCRIPT, &cmd).await?;
        let [outcome, first, last, flag] = strings::<4>(reply, "append")?;
        let offsets = match (first.as_str(), last.as_str()) {
            ("", _) | (_, "") => None,
            (first, last) => Some((offset_of_entry_id(first)?, offset_of_entry_id(last)?)),
        };
        Ok(Appended {
            offsets,
            resend: outcome == "resend",
            flagged: flag == "1",
        })
    }

    /// Closes a Stream with an encoded close Record, if the Producer's kind and the ref's run
    /// chain are the owner's.
    pub async fn close(
        &self,
        namespace: &str,
        stream: &StreamRef,
        producer: &Producer,
        record: &[u8],
    ) -> StreamResult<Closed> {
        let keys = self.keys(namespace, &stream.id);
        let epoch = producer.epoch.unwrap_or_default();
        let mut cmd = script_cmd("EVALSHA", &scripts::CLOSE_SCRIPT, &keys);
        cmd.arg(FORMAT_VERSION)
            .arg(producer.kind)
            .arg(&producer.id)
            .arg(epoch_key(&epoch))
            .arg(millis(TOMBSTONE))
            .arg(record)
            .arg(owner_chain(stream))
            .arg(self.wait_flag());
        let reply = self.write(&scripts::CLOSE_SCRIPT, &cmd).await?;
        let [outcome, id] = strings::<2>(reply, "close")?;
        Ok(Closed {
            offset: offset_of_entry_id(&id)?,
            already: outcome == "already",
        })
    }

    /// Reads the Stream's state and up to `count` entries from `from`, inclusive. Zero reads
    /// from the start of the log.
    pub async fn read(
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
            .mapped()?;
        read_reply(reply)
    }

    /// Runs a write script, and then `WAIT` when the options name replicas. Both are in one
    /// pipeline on one connection, so the `WAIT` covers the script's writes.
    async fn write(&self, script: &Script, cmd: &Cmd) -> StreamResult<Value> {
        let Some(wait) = &self.wait else {
            return run_script(script, cmd, &mut self.shared.clone())
                .await
                .mapped();
        };
        let Some(mut lease) = wait.pool.take().await.mapped()? else {
            return Err(StreamError::storage(
                "Every connection for writes with WAIT stayed busy for the response timeout",
            ));
        };
        let reply = Self::write_and_wait(wait, script, cmd, &mut lease.conn).await;
        // After a transport error, the `WAIT` can still block the connection, so the pool drops it.
        if reply.is_ok() {
            lease.release();
        }
        reply.mapped()?
    }

    /// The script argument that tells a write whether a `WAIT` follows it.
    fn wait_flag(&self) -> &'static str {
        if self.wait.is_some() { "1" } else { "" }
    }

    /// The outer error is a transport error, and the inner one is the server's answer.
    async fn write_and_wait(
        wait: &Wait,
        script: &Script,
        cmd: &Cmd,
        conn: &mut Shared,
    ) -> RedisResult<StreamResult<Value>> {
        let mut pipe = redis::pipe();
        pipe.ignore_errors()
            .add_command(cmd.clone())
            .cmd("WAIT")
            .arg(wait.replicas)
            .arg(millis(wait.timeout));
        let mut loaded = false;
        loop {
            let replies: Vec<Value> = pipe.query_async(conn).await?;
            let Ok([reply, acked]) = <[Value; 2]>::try_from(replies) else {
                return Ok(Err(StreamError::storage("Redis gave WAIT no answer")));
            };
            match reply.extract_error() {
                Err(error) if !loaded && no_script(&error) => {
                    script.load_async(conn).await?;
                    loaded = true;
                }
                Err(error) => return Ok(Err(errors::stream_error(&error))),
                Ok(reply) => return Ok(acknowledged(wait, acked).map(|()| reply)),
            }
        }
    }

    /// Reads up to `count` entries at or before `before`, newest first, with `XREVRANGE`.
    pub async fn read_back(
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
            .mapped()?;
        let Value::Array(entries) = reply else {
            return Err(bad_reply("read back", reply));
        };
        entries.into_iter().map(entry).collect()
    }

    /// Waits until the log has an entry after `after`, or `deadline` passes. Returns whether an
    /// entry came. The wait uses a dedicated connection.
    pub async fn wait_after(
        &self,
        namespace: &str,
        stream_id: &str,
        after: i64,
        deadline: Instant,
    ) -> StreamResult<bool> {
        let keys = self.keys(namespace, stream_id);
        if deadline <= Instant::now() {
            return Ok(false);
        }
        let after = if after > 0 {
            entry_id_of_offset(after)
        } else {
            "0-0".to_string()
        };
        let xread = |wait: Duration| {
            let mut xread = redis::cmd("XREAD");
            xread
                .arg("COUNT")
                .arg(1)
                .arg("BLOCK")
                .arg(millis(wait).max(1))
                .arg("STREAMS")
                .arg(keys.log())
                .arg(&after);
            xread
        };
        let reply = self
            .blocking
            .query(&keys.log(), deadline, xread)
            .await
            .mapped()?;
        Ok(
            matches!(reply, Some(Value::Array(ref keys)) if !keys.is_empty())
                || matches!(reply, Some(Value::Map(ref keys)) if !keys.is_empty()),
        )
    }
}

#[async_trait::async_trait]
impl Store for RedisStore {
    // Every rule but the run fence is in the Lua scripts.
    fn capabilities(&self) -> Capabilities {
        Capabilities::ALL.without(Capabilities::RUN_FENCE)
    }

    fn default_retention(&self) -> Duration {
        self.retention
    }

    async fn create(
        &self,
        namespace: &str,
        stream: &StreamRef,
        retention: Option<Duration>,
        owner: Owner,
        withdrawal: &Withdrawal<'_>,
    ) -> StreamResult<Created> {
        RedisStore::create(self, namespace, stream, retention, owner, withdrawal).await
    }

    async fn append(
        &self,
        namespace: &str,
        stream: &StreamRef,
        batch: &Batch<'_>,
    ) -> StreamResult<Appended> {
        RedisStore::append(self, namespace, &stream.id, batch).await
    }

    async fn close(
        &self,
        namespace: &str,
        stream: &StreamRef,
        producer: &Producer,
        record: &[u8],
    ) -> StreamResult<Closed> {
        RedisStore::close(self, namespace, stream, producer, record).await
    }

    async fn read(
        &self,
        namespace: &str,
        stream_id: &str,
        from: i64,
        count: usize,
    ) -> StreamResult<(StreamState, Vec<Entry>)> {
        RedisStore::read(self, namespace, stream_id, from, count).await
    }

    async fn wait_after(
        &self,
        namespace: &str,
        stream_id: &str,
        after: i64,
        deadline: Instant,
    ) -> StreamResult<bool> {
        RedisStore::wait_after(self, namespace, stream_id, after, deadline).await
    }

    async fn read_back(
        &self,
        namespace: &str,
        stream_id: &str,
        before: i64,
        count: usize,
    ) -> StreamResult<Vec<Entry>> {
        RedisStore::read_back(self, namespace, stream_id, before, count).await
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

/// Refuses a write that fewer replicas than `wait` names have.
fn acknowledged(wait: &Wait, acked: Value) -> StreamResult<()> {
    let acked: usize = redis::from_redis_value(acked.extract_error().mapped()?)
        .map_err(|error| bad_reply("WAIT", error))?;
    if acked < wait.replicas {
        return Err(StreamError::storage(format!(
            "{acked} of {} replicas had the write after {:?}. A failover can lose it, and a retry \
             writes it once",
            wait.replicas, wait.timeout
        )));
    }
    Ok(())
}

fn no_script(error: &redis::RedisError) -> bool {
    error.kind() == ErrorKind::Server(ServerErrorKind::NoScript)
}

/// Runs a script call, and loads the script when Redis lacks it.
async fn run_script(script: &Script, cmd: &Cmd, conn: &mut Shared) -> RedisResult<Value> {
    match cmd.query_async(conn).await {
        Err(error) if no_script(&error) => {
            script.load_async(conn).await?;
            cmd.query_async(conn).await
        }
        result => result,
    }
}

fn bad_reply(call: &str, reply: impl std::fmt::Debug) -> StreamError {
    StreamError::storage(format!("Redis gave the {call} script the answer {reply:?}"))
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

fn strings<const N: usize>(reply: Value, call: &str) -> StreamResult<[String; N]> {
    let Value::Array(items) = &reply else {
        return Err(bad_reply(call, &reply));
    };
    let texts: Option<Vec<String>> = items.iter().map(text).collect();
    texts
        .and_then(|texts| texts.try_into().ok())
        .ok_or_else(|| bad_reply(call, &reply))
}

fn read_reply(reply: Value) -> StreamResult<(StreamState, Vec<Entry>)> {
    let Value::Array(mut parts) = reply else {
        return Err(bad_reply("read", reply));
    };
    if parts.len() != 3 {
        return Err(bad_reply("read", parts));
    }
    let entries = parts.pop().expect("three parts");
    let time = parts.pop().expect("three parts");
    let meta = parts.pop().expect("three parts");
    let Value::Array(meta) = meta else {
        return Err(bad_reply("read", meta));
    };
    let meta: Vec<Option<String>> = meta
        .iter()
        .map(|value| text(value).filter(|text| !text.is_empty()))
        .collect();
    let field = |i: usize| meta.get(i).cloned().flatten();
    let offset =
        |i: usize| -> StreamResult<i64> { field(i).map_or(Ok(0), |id| offset_of_entry_id(&id)) };
    let number = |i: usize| {
        field(i)
            .and_then(|text| text.parse::<i64>().ok())
            .unwrap_or(0)
    };
    let [seconds, micros] = match time {
        Value::Array(time) => {
            let time: Vec<i64> = time
                .iter()
                .filter_map(|value| text(value)?.parse().ok())
                .collect();
            <[i64; 2]>::try_from(time).map_err(|time| bad_reply("read", time))?
        }
        other => return Err(bad_reply("read", other)),
    };
    let state = StreamState {
        exists: field(0).is_some(),
        version: number(0) as u32,
        closed_at: if field(1).is_some() {
            Some(offset(2)?)
        } else {
            None
        },
        newest: offset(3)?,
        trimmed: offset(4)?,
        retention: Duration::from_millis(number(5).max(0) as u64),
        expires_ms: number(6),
        now_ms: seconds * 1000 + micros / 1000,
    };
    let Value::Array(entries) = entries else {
        return Err(bad_reply("read", entries));
    };
    let entries = entries
        .into_iter()
        .map(entry)
        .collect::<StreamResult<Vec<_>>>()?;
    Ok((state, entries))
}

fn entry(value: Value) -> StreamResult<Entry> {
    let Value::Array(mut pair) = value else {
        return Err(bad_reply("read", value));
    };
    if pair.len() != 2 {
        return Err(bad_reply("read", pair));
    }
    let fields = pair.pop().expect("two parts");
    let id = text(&pair.pop().expect("two parts")).unwrap_or_default();
    let mut entry = Entry {
        offset: offset_of_entry_id(&id)?,
        record: None,
        supersession: None,
    };
    let Value::Array(fields) = fields else {
        return Err(bad_reply("read", fields));
    };
    for pair in fields.chunks(2) {
        let (Some(Value::BulkString(name)), Some(Value::BulkString(value))) =
            (pair.first(), pair.get(1))
        else {
            continue;
        };
        match name.as_slice() {
            b"r" => entry.record = Some(value.clone()),
            b"s" => entry.supersession = Some(value.clone()),
            _ => {}
        }
    }
    Ok(entry)
}

/// Refuses options that can't work, before any connection.
fn check_options(options: &RedisStoreOptions) -> StreamResult<()> {
    if options.urls.is_empty() {
        return Err(StreamError::invalid("The Redis Store needs a URL"));
    }
    if options.key_prefix.is_empty() {
        return Err(StreamError::invalid("The key prefix must not be empty"));
    }
    // The keys hold the prefix percent-encoded, but the ACL key patterns of `docs/redis.md` name
    // it as written, so a prefix that encoding changes could widen or miss a pattern.
    if encode_part(&options.key_prefix) != options.key_prefix {
        return Err(StreamError::invalid(
            "The key prefix must have only letters, digits, '-', '.', '_' and '~'",
        ));
    }
    let ttls = [options.retention, options.safety_ttl];
    if ttls.iter().any(|ttl| ttl.is_zero() || *ttl > MAX_TTL) {
        return Err(StreamError::invalid(format!(
            "The Retention and the safety TTL must be above zero and at most {} s",
            MAX_TTL.as_secs()
        )));
    }
    // A `WAIT` with a zero timeout waits for the replicas forever. The script and the round trip
    // need the other half of the response timeout, or too few replicas looks like a lost answer.
    if options.wait_replicas > 0
        && (options.wait_timeout.as_millis() == 0
            || options.wait_timeout > options.response_timeout / 2)
    {
        return Err(StreamError::invalid(
            "The wait timeout must be at least 1 ms and at most half the response timeout",
        ));
    }
    Ok(())
}

/// Refuses a server older than Redis 7.0, and warns about one that may evict Stream keys.
async fn check_server(shared: &Shared) -> StreamResult<()> {
    let mut info = redis::cmd("INFO");
    info.arg("server");
    match shared.on_each_primary(info).await {
        Ok(replies) => {
            for (node, reply) in replies {
                let text: String =
                    redis::from_redis_value(reply).map_err(|error| bad_reply("INFO", error))?;
                check_version(&node, &text)?;
            }
        }
        // A user with only the Store's commands may not have INFO. Then a Redis before 7.0 fails at
        // the first read, on the flags of the read script.
        Err(error) if error.code() == Some("NOPERM") => {
            tracing::debug!("Could not read the Redis version: {error}");
        }
        Err(error) => return Err(errors::stream_error(&error)),
    }
    let mut policy = redis::cmd("CONFIG");
    policy.arg("GET").arg("maxmemory-policy");
    match shared.on_each_primary(policy).await {
        Ok(replies) => {
            for (node, reply) in replies {
                if let Some(policy) = eviction_policy(reply).filter(|p| p != "noeviction") {
                    tracing::warn!(
                        node,
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

fn check_version(node: &str, info: &str) -> StreamResult<()> {
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
        return Err(StreamError::invalid(format!(
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
    use temporalio_streams::Reason;

    #[test]
    fn a_server_before_redis_7_is_refused() {
        check_version("a", "# Server\r\nredis_version:7.0.15\r\n").unwrap();
        check_version("a", "redis_version:8.10.1").unwrap();
        assert!(check_version("a", "redis_version:6.2.14").is_err());
        assert!(check_version("a", "").is_err());
    }

    #[test]
    fn a_key_prefix_has_only_the_characters_that_encoding_keeps() {
        let check = |prefix: &str| {
            let mut options = RedisStoreOptions::new("redis://127.0.0.1:1");
            options.key_prefix = prefix.to_string();
            check_options(&options)
        };
        for prefix in ["app:streams", "app*", "app{x}", "app}", "a?", "a[b]", "a b"] {
            assert_eq!(
                check(prefix).unwrap_err().reason,
                Reason::Invalid,
                "{prefix}"
            );
        }
        check("temporal-streams").unwrap();
        check("App.streams_2~x").unwrap();
    }

    #[test]
    fn a_wait_timeout_must_leave_half_the_response_timeout() {
        let check = |timeout: Duration| {
            let mut options = RedisStoreOptions::new("redis://127.0.0.1:1");
            options.wait_replicas = 1;
            options.wait_timeout = timeout;
            check_options(&options)
        };
        for timeout in [Duration::ZERO, Duration::from_millis(2501)] {
            assert_eq!(
                check(timeout).unwrap_err().reason,
                Reason::Invalid,
                "{timeout:?}"
            );
        }
        check(Duration::from_millis(2500)).unwrap();
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
}
