//! `XREAD` and `WAIT` block later commands, so each uses a separate pooled connection.

use crate::{
    RedisServers,
    error::{RedisStoreError, RedisStoreResult},
    keys::StreamKeys,
    offset::EntryId,
    options::{Settings, Wait},
    scripts::ScriptCall,
};
use parking_lot::Mutex;
use redis::{
    AsyncConnectionConfig, Client, Cmd, ConnectionAddr, ConnectionInfo, ErrorKind, FromRedisValue,
    ParsingError, Pipeline, RedisError, RedisFuture, RedisResult, Value,
    aio::{ConnectionLike, ConnectionManager, ConnectionManagerConfig, MultiplexedConnection},
    cluster::ClusterClient,
    cluster_async::ClusterConnection,
    from_redis_value,
};
use std::{collections::HashMap, future::Future, num::NonZeroUsize, sync::Arc, time::Duration};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Instant,
};

// Reads above the connection limit must still observe new Records.
const BUSY_POLL: Duration = Duration::from_secs(1);

// A short pause avoids a busy loop during slot migration or connection recovery.
const RETRY_PAUSE: Duration = Duration::from_millis(100);

pub(crate) struct Pool<C> {
    permits: Arc<Semaphore>,
    idle: Arc<Mutex<Vec<C>>>,
}

enum Leased<C> {
    Idle(IdleLease<C>),
    Free(Permit<C>),
    Busy,
}

// Redis may have committed a write before its connection failed.
pub(crate) enum CallKind {
    Write,
    Read,
}

pub(crate) trait PooledCall<C> {
    type Answer;
    const KIND: CallKind;

    fn run(&self, conn: &mut C) -> impl Future<Output = RedisResult<Self::Answer>> + Send;
}

pub(crate) enum Called<T> {
    Answered(T),
    Busy,
}

impl<C> Pool<C> {
    pub(crate) fn new(size: NonZeroUsize) -> Self {
        Self {
            // A connection limit is an upper bound, so larger limits can use the supported maximum.
            permits: Arc::new(Semaphore::new(size.get().min(Semaphore::MAX_PERMITS))),
            idle: Arc::default(),
        }
    }

    async fn lease(&self, give_up: impl Future<Output = ()>) -> RedisResult<Leased<C>> {
        let permit = tokio::select! {
            biased;
            permit = self.permits.clone().acquire_owned() => permit,
            () = give_up => return Ok(Leased::Busy),
        };
        // A closed semaphore is a Store fault, not pool saturation.
        let permit = permit
            .map_err(|_| RedisError::from((ErrorKind::Client, "The connection pool is closed")))?;
        let permit = Permit {
            idle: self.idle.clone(),
            _permit: permit,
        };
        let idle = self.idle.lock().pop();
        Ok(match idle {
            Some(conn) => Leased::Idle(IdleLease(Lease { conn, permit })),
            None => Leased::Free(permit),
        })
    }

    // Retry stale connections only for reads. A failed write may already be committed.
    pub(crate) async fn call<P, O>(
        &self,
        give_up: impl Future<Output = ()>,
        open: impl FnOnce() -> O,
        call: &P,
    ) -> RedisResult<Called<P::Answer>>
    where
        P: PooledCall<C>,
        O: Future<Output = RedisResult<C>>,
    {
        let permit = match self.lease(give_up).await? {
            Leased::Busy => return Ok(Called::Busy),
            Leased::Free(permit) => permit,
            Leased::Idle(mut lease) => {
                let result = call.run(&mut lease.0.conn).await;
                match (lease.settle(&result), P::KIND) {
                    (Settled::Stale(permit), CallKind::Read) => permit,
                    (Settled::Stale(_) | Settled::Done, _) => return result.map(Called::Answered),
                }
            }
        };
        let mut lease = Lease {
            conn: open().await?,
            permit,
        };
        let result = call.run(&mut lease.conn).await;
        lease.settle(&result);
        result.map(Called::Answered)
    }
}

struct Permit<C> {
    idle: Arc<Mutex<Vec<C>>>,
    _permit: OwnedSemaphorePermit,
}

// A dropped call may leave Redis blocked. Only `settle` can return its connection to the pool.
struct Lease<C> {
    conn: C,
    permit: Permit<C>,
}

impl<C> Lease<C> {
    // A transport error leaves the connection state unknown.
    fn settle<T>(self, result: &RedisResult<T>) {
        if !matches!(result, Err(error) if error.code().is_none()) {
            // The connection is back before its place gets free, so the next lease finds it.
            self.permit.idle.lock().push(self.conn);
        }
    }
}

// Redis may close an idle connection before its next call.
struct IdleLease<C>(Lease<C>);

enum Settled<C> {
    Done,
    Stale(Permit<C>),
}

impl<C> IdleLease<C> {
    fn settle<T>(self, result: &RedisResult<T>) -> Settled<C> {
        match result {
            // A timeout only shows that the server is slow, so the idle connections may be fine.
            Err(error)
                if !error.is_timeout()
                    && (error.is_io_error() || error.is_connection_dropped()) =>
            {
                self.0.permit.idle.lock().clear();
                Settled::Stale(self.0.permit)
            }
            _ => {
                self.0.settle(result);
                Settled::Done
            }
        }
    }
}

#[derive(Clone)]
pub(crate) enum Shared {
    Single(ConnectionManager),
    Cluster(ClusterConnection),
}

impl ConnectionLike for Shared {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        match self {
            Shared::Single(conn) => conn.req_packed_command(cmd),
            Shared::Cluster(conn) => conn.req_packed_command(cmd),
        }
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        pipeline: &'a Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        match self {
            Shared::Single(conn) => conn.req_packed_commands(pipeline, offset, count),
            Shared::Cluster(conn) => conn.req_packed_commands(pipeline, offset, count),
        }
    }

    fn get_db(&self) -> i64 {
        match self {
            Shared::Single(conn) => conn.get_db(),
            Shared::Cluster(conn) => conn.get_db(),
        }
    }
}

#[derive(Debug, PartialEq)]
struct SlotRange {
    first: u16,
    last: u16,
    // An absent host means the host that answered `CLUSTER SLOTS`.
    host: Option<String>,
    port: u16,
}

impl FromRedisValue for SlotRange {
    fn from_redis_value(value: Value) -> Result<Self, ParsingError> {
        let range: Vec<Value> = from_redis_value(value)?;
        let [first, last, primary, ..] = range.as_slice() else {
            return Err("A range of CLUSTER SLOTS has too few fields"
                .to_string()
                .into());
        };
        let primary: Vec<Value> = FromRedisValue::from_redis_value_ref(primary)?;
        let [host, port, ..] = primary.as_slice() else {
            return Err("A node of CLUSTER SLOTS has too few fields"
                .to_string()
                .into());
        };
        let host: Option<String> = FromRedisValue::from_redis_value_ref(host)?;
        Ok(Self {
            first: FromRedisValue::from_redis_value_ref(first)?,
            last: FromRedisValue::from_redis_value_ref(last)?,
            host: host.filter(|host| !host.is_empty()),
            port: FromRedisValue::from_redis_value_ref(port)?,
        })
    }
}

async fn connect(servers: &RedisServers, response_timeout: Duration) -> RedisResult<Shared> {
    match servers {
        RedisServers::Standalone(server) => {
            // Other Store calls wait for this connection, so fail fast and let them retry.
            let config = ConnectionManagerConfig::new()
                .set_response_timeout(Some(response_timeout))
                .set_number_of_retries(0);
            let client = Client::open(server.info().clone())?;
            let conn = client.get_connection_manager_with_config(config).await?;
            Ok(Shared::Single(conn))
        }
        RedisServers::Cluster { first, rest } => {
            let nodes = std::iter::once(first).chain(rest);
            // Bound retry delays so migration errors return to the caller.
            let pause = u64::try_from(response_timeout.as_millis() / 20).unwrap_or(u64::MAX);
            let client = ClusterClient::builder(nodes.map(|node| node.info().clone()))
                .response_timeout(response_timeout)
                .retries(16)
                .min_retry_wait(pause / 2)
                .max_retry_wait(pause)
                .build()?;
            Ok(Shared::Cluster(client.get_async_connection().await?))
        }
    }
}

struct WriteAndWait<'a> {
    call: &'a ScriptCall,
    wait: Wait,
}

impl PooledCall<Shared> for WriteAndWait<'_> {
    type Answer = (Value, usize);
    const KIND: CallKind = CallKind::Write;

    async fn run(&self, conn: &mut Shared) -> RedisResult<Self::Answer> {
        self.call.run_and_wait(conn, self.wait).await
    }
}

struct Xread<'a> {
    keys: &'a StreamKeys,
    after: EntryId,
    deadline: Option<Instant>,
}

impl PooledCall<MultiplexedConnection> for Xread<'_> {
    type Answer = Value;
    const KIND: CallKind = CallKind::Read;

    async fn run(&self, conn: &mut MultiplexedConnection) -> RedisResult<Value> {
        let block = match self.deadline {
            None => 0,
            Some(deadline) => {
                let left = deadline.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    return Ok(Value::Nil);
                }
                // Redis treats zero as an unlimited wait.
                u64::try_from(left.as_millis()).unwrap_or(u64::MAX).max(1)
            }
        };
        redis::cmd("XREAD")
            .arg("BLOCK")
            .arg(block)
            .arg("COUNT")
            .arg(1)
            .arg("STREAMS")
            .arg(self.keys.log())
            .arg(self.after)
            .query_async(conn)
            .await
    }
}

pub(crate) struct Connections {
    shared: Shared,
    servers: RedisServers,
    writers: Pool<Shared>,
    slots: Mutex<Vec<SlotRange>>,
    readers: Mutex<HashMap<String, Arc<Pool<MultiplexedConnection>>>>,
    max_blocking_connections: NonZeroUsize,
    response_timeout: Duration,
}

impl Connections {
    pub(crate) async fn open(settings: &Settings) -> RedisResult<Self> {
        Ok(Self {
            shared: connect(&settings.servers, settings.response_timeout).await?,
            servers: settings.servers.clone(),
            writers: Pool::new(settings.max_blocking_connections),
            slots: Mutex::default(),
            readers: Mutex::default(),
            max_blocking_connections: settings.max_blocking_connections,
            response_timeout: settings.response_timeout,
        })
    }

    pub(crate) fn shared(&self) -> Shared {
        self.shared.clone()
    }

    pub(crate) async fn write_and_wait<T: FromRedisValue>(
        &self,
        call: &ScriptCall,
        wait: Wait,
    ) -> RedisStoreResult<(T, usize)> {
        let give_up = tokio::time::sleep(self.response_timeout);
        let open = || connect(&self.servers, self.response_timeout);
        let write = WriteAndWait { call, wait };
        match self.writers.call(give_up, open, &write).await? {
            Called::Answered((answer, acknowledged)) => {
                Ok((from_redis_value(answer)?, acknowledged))
            }
            Called::Busy => Err(RedisStoreError::Unavailable(
                "No connection for a write that waits for its replicas got free in time"
                    .to_string(),
            )),
        }
    }

    // Transient wake-up failures must return to the read script for a fresh Stream state.
    pub(crate) async fn wait_for_entry(
        &self,
        keys: &StreamKeys,
        after: EntryId,
        deadline: Option<Instant>,
    ) -> RedisStoreResult<()> {
        match self.block(keys, after, deadline).await {
            Err(error) if error.can_pass() => {
                // The node of the slot may be another one after the failure.
                self.slots.lock().clear();
                tokio::select! {
                    () = tokio::time::sleep(RETRY_PAUSE) => {}
                    () = until(deadline) => {}
                }
                Ok(())
            }
            result => result,
        }
    }

    async fn block(
        &self,
        keys: &StreamKeys,
        after: EntryId,
        deadline: Option<Instant>,
    ) -> RedisStoreResult<()> {
        let node = tokio::select! {
            node = self.node(keys) => node?,
            () = until(deadline) => return Ok(()),
        };
        let pool = self.pool(node.addr().to_string());
        let open = || async {
            // The read bounds its own wait, so the connection has no response timeout.
            let config = AsyncConnectionConfig::new().set_response_timeout(None);
            let client = Client::open(node)?;
            client
                .get_multiplexed_async_connection_with_config(&config)
                .await
        };
        let give_up = async {
            tokio::select! {
                () = tokio::time::sleep(BUSY_POLL) => {}
                () = until(deadline) => {}
            }
        };
        let xread = Xread {
            keys,
            after,
            deadline,
        };
        let called = pool.call(give_up, open, &xread);
        // A server that stopped without closing the connection never replies.
        let end = deadline.and_then(|deadline| deadline.checked_add(self.response_timeout));
        tokio::select! {
            called = called => called.map(drop)?,
            () = until(end) => {}
        }
        Ok(())
    }

    fn pool(&self, node: String) -> Arc<Pool<MultiplexedConnection>> {
        let new = || Arc::new(Pool::new(self.max_blocking_connections));
        self.readers.lock().entry(node).or_insert_with(new).clone()
    }

    async fn node(&self, keys: &StreamKeys) -> RedisStoreResult<ConnectionInfo> {
        let (first, rest) = match &self.servers {
            RedisServers::Standalone(server) => return Ok(server.info().clone()),
            RedisServers::Cluster { first, rest } => (first, rest),
        };
        let slot = keys.slot();
        let serves = |range: &&SlotRange| range.first <= slot && slot <= range.last;
        let node = |range: &SlotRange| first.node(range.host.clone(), range.port);
        if let Some(node) = self.slots.lock().iter().find(serves).map(node) {
            return Ok(node);
        }
        let mut command = redis::cmd("CLUSTER");
        command.arg("SLOTS");
        let mut ranges: Vec<SlotRange> = command.query_async(&mut self.shared()).await?;
        if ranges.iter().find(serves).is_some_and(|r| r.host.is_some()) {
            // Only explicit endpoints can be cached from a reply with an unknown responder.
            ranges.retain(|r| r.host.is_some());
        } else if ranges.iter().any(|r| r.host.is_none()) {
            // An omitted endpoint is relative to the seed that received the command.
            let mut source = first;
            let mut others = rest.iter();
            let config = AsyncConnectionConfig::new()
                .set_connection_timeout(Some(self.response_timeout))
                .set_response_timeout(Some(self.response_timeout));
            loop {
                let reply = async {
                    let client = Client::open(source.info().clone())?;
                    let mut conn = client
                        .get_multiplexed_async_connection_with_config(&config)
                        .await?;
                    command.query_async(&mut conn).await
                }
                .await;
                match reply {
                    Ok(reply) => {
                        ranges = reply;
                        let host = match source.info().addr() {
                            ConnectionAddr::Tcp(host, _) | ConnectionAddr::TcpTls { host, .. } => {
                                host
                            }
                            _ => unreachable!("cluster seeds require a host"),
                        };
                        for range in &mut ranges {
                            range.host.get_or_insert_with(|| host.clone());
                        }
                        break;
                    }
                    Err(error) => source = others.next().ok_or(error)?,
                }
            }
        }
        let node = ranges.iter().find(serves).map(node);
        *self.slots.lock() = ranges;
        node.ok_or_else(|| {
            let message = "No node of the cluster serves the slot of the Stream";
            RedisStoreError::Unavailable(message.to_string())
        })
    }
}

async fn until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        future::{pending, ready},
        io,
    };

    // Numeric connections keep pool tests independent of Redis.
    struct Fake<F>(F, Mutex<Vec<u8>>);

    impl<F: Fn(u8) -> RedisResult<u8> + Sync> PooledCall<u8> for Fake<F> {
        type Answer = u8;
        const KIND: CallKind = CallKind::Read;

        async fn run(&self, conn: &mut u8) -> RedisResult<u8> {
            self.1.lock().push(*conn);
            self.0(*conn)
        }
    }

    fn fake<F>(answer: F) -> Fake<F> {
        Fake(answer, Mutex::default())
    }

    #[tokio::test]
    async fn a_pool_keeps_a_connection_only_after_an_answer() {
        let pool = Pool::new(NonZeroUsize::MIN);
        let open = |conn: u8| move || async move { Ok(conn) };
        let conn_of = async |open| match pool.call(pending(), open, &fake(Ok)).await.unwrap() {
            Called::Answered(conn) => conn,
            Called::Busy => panic!("the pool has a free place"),
        };
        // The pool has one place, so a second call is busy while the first one has it.
        let Leased::Free(first) = pool.lease(ready(())).await.unwrap() else {
            panic!("a new pool has no connection");
        };
        assert!(matches!(pool.lease(ready(())).await, Ok(Leased::Busy)));
        drop(first);
        assert_eq!(conn_of(open(1)).await, 1);
        // An error with a code is an answer of Redis, so the connection goes back too.
        let refused = redis::parse_redis_value(b"-OOM out of memory\r\n").unwrap();
        let refused = refused.extract_error().unwrap_err();
        let refuses = fake(|_| Err(refused.clone()));
        assert!(pool.call(pending(), open(2), &refuses).await.is_err());
        assert_eq!(conn_of(open(3)).await, 1);
        // A call that is dropped may have left its command on the server, and a call with no
        // answer too, so their connections are gone.
        let Leased::Idle(lease) = pool.lease(ready(())).await.unwrap() else {
            panic!("the pool has an idle connection");
        };
        drop(lease);
        assert_eq!(conn_of(open(4)).await, 4);
        let timeout = RedisError::from(io::Error::from(io::ErrorKind::TimedOut));
        let times_out = fake(|_| Err(timeout.clone()));
        assert!(pool.call(pending(), open(5), &times_out).await.is_err());
        assert_eq!(conn_of(open(6)).await, 6);
        // A read on a connection that the server closed while it was idle gets one more try on a
        // new one, and the pool drops the other idle ones too.
        pool.idle.lock().extend([7, 8]);
        let closed = RedisError::from(io::Error::from(io::ErrorKind::BrokenPipe));
        let closed_until_9 = fake(|conn| match conn {
            9 => Ok(conn),
            _ => Err(closed.clone()),
        });
        let called = pool.call(pending(), open(9), &closed_until_9).await;
        assert!(matches!(called, Ok(Called::Answered(9))));
        assert_eq!(*closed_until_9.1.lock(), [8, 9]);
        assert_eq!(*pool.idle.lock(), [9]);
    }

    #[test]
    fn a_pool_accepts_the_largest_connection_limit() {
        let pool = Pool::<()>::new(NonZeroUsize::MAX);
        assert_eq!(pool.permits.available_permits(), Semaphore::MAX_PERMITS);
    }

    #[test]
    fn cluster_slots_gives_each_range_its_primary() {
        // Three ranges with a host, an empty host and no host, and a replica after the first
        // primary.
        let reply = redis::parse_redis_value(
            b"*3\r\n*4\r\n:0\r\n:5460\r\n*3\r\n$8\r\n10.0.0.2\r\n:7001\r\n$1\r\na\r\n\
              *3\r\n$8\r\n10.0.0.5\r\n:7004\r\n$1\r\nb\r\n\
              *3\r\n:5461\r\n:10922\r\n*3\r\n$0\r\n\r\n:7002\r\n$1\r\nc\r\n\
              *3\r\n:10923\r\n:16383\r\n*3\r\n$-1\r\n:7003\r\n$1\r\nd\r\n",
        );
        let range = |first, last, host: Option<&str>, port| SlotRange {
            first,
            last,
            host: host.map(str::to_string),
            port,
        };
        let ranges: Vec<SlotRange> = from_redis_value(reply.unwrap()).unwrap();
        let expected = [
            range(0, 5460, Some("10.0.0.2"), 7001),
            range(5461, 10922, None, 7002),
            range(10923, 16383, None, 7003),
        ];
        assert_eq!(ranges, expected);
        let short = redis::parse_redis_value(b"*1\r\n*2\r\n:0\r\n:1\r\n").unwrap();
        assert!(from_redis_value::<Vec<SlotRange>>(short).is_err());
    }
}
