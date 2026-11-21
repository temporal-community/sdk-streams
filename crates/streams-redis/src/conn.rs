//! The connections of the Store. A blocked `XREAD` stops every later command on its connection.
//! So each read that waits takes a connection from a pool, and the calls that don't block share
//! one.

use crate::{
    RedisUrl, error::RedisStoreResult, keys::StreamKeys, offset::EntryId, options::Settings,
};
use parking_lot::Mutex;
use redis::{
    AsyncConnectionConfig, Client, Cmd, ErrorKind, RedisError, RedisResult, Value,
    aio::{ConnectionManager, ConnectionManagerConfig, MultiplexedConnection},
};
use std::{future::Future, num::NonZeroUsize, sync::Arc, time::Duration};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Instant,
};

/// How long a read waits for a free connection before it runs its script again. So the reads over
/// the maximum poll at this rate.
const BUSY_POLL: Duration = Duration::from_secs(1);

/// How long a read pauses after an `XREAD` that failed in a way that can pass, such as a dropped
/// connection. Then it runs its script again, which fails the read if Redis is still down.
const RETRY_PAUSE: Duration = Duration::from_millis(100);

/// A bounded set of connections, each for one call at a time.
pub(crate) struct Pool<C> {
    permits: Arc<Semaphore>,
    idle: Arc<Mutex<Vec<C>>>,
}

/// What [`Pool::lease`] gives.
enum Leased<C> {
    /// A connection that was idle in the pool.
    Idle(IdleLease<C>),
    /// The right to open a connection, since none was idle.
    Free(Permit<C>),
    /// Every connection stayed busy until the caller gave up.
    Busy,
}

/// A call that a [`Pool`] runs on one of its connections.
pub(crate) trait PooledCall<C> {
    type Answer;

    fn run(&self, conn: &mut C) -> impl Future<Output = RedisResult<Self::Answer>> + Send;
}

/// How a call on a connection of a [`Pool`] ended.
pub(crate) enum Called<T> {
    Answered(T),
    /// Every connection stayed busy until the caller gave up, so the call didn't run.
    Busy,
}

impl<C> Pool<C> {
    pub(crate) fn new(size: NonZeroUsize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(size.get())),
            idle: Arc::default(),
        }
    }

    /// Takes an idle connection, or the right to open one. `give_up` ends the wait for a free
    /// place, so the pool reads no clock.
    async fn lease(&self, give_up: impl Future<Output = ()>) -> RedisResult<Leased<C>> {
        let permit = tokio::select! {
            // A free place wins when `give_up` is ready too.
            biased;
            permit = self.permits.clone().acquire_owned() => permit,
            () = give_up => return Ok(Leased::Busy),
        };
        // Nothing closes the semaphore, so a failed acquire is a bug of the Store. It must not
        // look like a busy pool.
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

    /// Runs `call` on a connection of the pool, which `open` makes when none is idle. A
    /// connection that was idle, and that the server has closed, gets one more try on a new
    /// one. So `call` must be safe to run twice.
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
                match lease.settle(&result) {
                    Settled::Done => return result.map(Called::Answered),
                    Settled::Stale(permit) => permit,
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

/// The right to one connection of a [`Pool`], with the idle list that the connection goes back
/// to.
struct Permit<C> {
    idle: Arc<Mutex<Vec<C>>>,
    _permit: OwnedSemaphorePermit,
}

/// A connection of a [`Pool`], for one call. A dropped call doesn't stop its command on the
/// server, and the next call would wait behind it. So a drop closes the connection, and only
/// [`Lease::settle`] gives it back.
struct Lease<C> {
    conn: C,
    permit: Permit<C>,
}

impl<C> Lease<C> {
    /// Gives the connection back to the pool after an answer of Redis, which is a value or an
    /// error with a code. Any other failure closes it.
    fn settle<T>(self, result: &RedisResult<T>) {
        if !matches!(result, Err(error) if error.code().is_none()) {
            // The connection is back before its place gets free, so the next lease finds it.
            self.permit.idle.lock().push(self.conn);
        }
    }
}

/// A connection that was idle in a [`Pool`]. The server may have closed it since its last call,
/// so its call can end with the right to a new connection.
struct IdleLease<C>(Lease<C>);

/// How a call on an [`IdleLease`] ended.
enum Settled<C> {
    /// Redis answered, or the call failed on a connection that was alive.
    Done,
    /// The server had closed the connection, and likely the other idle ones too, so the pool
    /// dropped them all.
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

impl PooledCall<MultiplexedConnection> for Cmd {
    type Answer = Value;

    async fn run(&self, conn: &mut MultiplexedConnection) -> RedisResult<Value> {
        self.query_async(conn).await
    }
}

/// The connections of one Store.
pub(crate) struct Connections {
    /// The connection of the calls that don't block, which reconnects by itself.
    shared: ConnectionManager,
    url: RedisUrl,
    /// The connections of the reads that wait.
    readers: Pool<MultiplexedConnection>,
    response_timeout: Duration,
}

impl Connections {
    pub(crate) async fn open(settings: &Settings) -> RedisResult<Self> {
        // A connect that fails must fail its call fast, since the other calls of a Store with no
        // connection wait behind it. The next call connects again.
        let config = ConnectionManagerConfig::new()
            .set_response_timeout(Some(settings.response_timeout))
            .set_number_of_retries(0);
        let client = Client::open(settings.url.info().clone())?;
        Ok(Self {
            shared: client.get_connection_manager_with_config(config).await?,
            url: settings.url.clone(),
            readers: Pool::new(settings.max_blocking_connections),
            response_timeout: settings.response_timeout,
        })
    }

    /// Returns the connection of the calls that don't block.
    pub(crate) fn shared(&self) -> ConnectionManager {
        self.shared.clone()
    }

    /// Waits until the Stream has an entry after `after`, until it closes, or until `deadline`.
    /// A read with no deadline waits with no end. The wait can also end earlier, and the caller
    /// then runs its script again. It fails only for an error that a retry gets again, such as
    /// a missing ACL permission.
    pub(crate) async fn wait_for_entry(
        &self,
        keys: &StreamKeys,
        after: EntryId,
        deadline: Option<Instant>,
    ) -> RedisStoreResult<()> {
        match self.block(keys, after, deadline).await {
            Err(error) if error.can_pass() => {
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
        let open = || async {
            // The read bounds its own wait, so the connection has no response timeout.
            let config = AsyncConnectionConfig::new().set_response_timeout(None);
            let client = Client::open(self.url.info().clone())?;
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
        let xread = xread(keys, after, deadline);
        let called = self.readers.call(give_up, open, &xread);
        // A server that stopped without closing the connection never replies.
        let end = deadline.and_then(|deadline| deadline.checked_add(self.response_timeout));
        tokio::select! {
            called = called => called.map(drop)?,
            () = until(end) => {}
        }
        Ok(())
    }
}

/// Waits until `deadline`, or with no end when there is none.
async fn until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

/// Returns the `XREAD` that blocks until the log has an entry after `after`, until the Stream
/// closes, or until `deadline`.
fn xread(keys: &StreamKeys, after: EntryId, deadline: Option<Instant>) -> Cmd {
    // `BLOCK 0` waits with no end, as a read with no deadline does. A read at its deadline
    // blocks for 1 ms.
    let block = deadline.map_or(0, |deadline| {
        let left = deadline.saturating_duration_since(Instant::now());
        u64::try_from(left.as_millis()).unwrap_or(u64::MAX).max(1)
    });
    let mut xread = redis::cmd("XREAD");
    xread
        .arg("BLOCK")
        .arg(block)
        .arg("COUNT")
        .arg(1)
        .arg("STREAMS")
        .arg(keys.log())
        .arg(keys.closed())
        .arg(after)
        // The key of a close has an entry only once the Stream is closed.
        .arg(EntryId::ZERO);
    xread
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        future::{pending, ready},
        io,
    };

    /// A call on a connection that is a number. It answers with what `answer` gives for the
    /// connection, and keeps the connections that it ran on.
    struct Fake<F>(F, Mutex<Vec<u8>>);

    impl<F: Fn(u8) -> RedisResult<u8> + Sync> PooledCall<u8> for Fake<F> {
        type Answer = u8;

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
        // A connection that the server closed while it was idle gets one more try on a new one,
        // and the pool drops the other idle ones too.
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
}
