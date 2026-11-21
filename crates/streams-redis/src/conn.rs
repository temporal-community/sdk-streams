//! The connections of the Store. A blocked `XREAD` stops every later command on its connection,
//! so each read that waits takes a connection from a pool, and the calls that don't block share
//! one.

use crate::{keys::StreamKeys, offset::EntryId, options::Settings};
use parking_lot::Mutex;
use redis::{
    AsyncConnectionConfig, Client, ErrorKind, RedisError, RedisResult, RetryMethod, Value,
    aio::{ConnectionManager, ConnectionManagerConfig, MultiplexedConnection},
};
use std::{future::Future, sync::Arc, time::Duration};
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

/// A bounded set of connections, each for one call at a time. It opens a connection when a call
/// finds none idle.
pub(crate) struct Pool<C> {
    permits: Arc<Semaphore>,
    idle: Arc<Mutex<Vec<C>>>,
}

/// A connection of a [`Pool`], for one call. A lease that is dropped closes its connection,
/// because its command may still run on the server, and the next call would wait behind it.
pub(crate) struct Lease<C> {
    pub(crate) conn: C,
    /// Whether the connection was idle in the pool, where the server may have closed it.
    reused: bool,
    idle: Arc<Mutex<Vec<C>>>,
    permit: OwnedSemaphorePermit,
}

impl<C> Pool<C> {
    pub(crate) fn new(size: usize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(size)),
            idle: Arc::default(),
        }
    }

    /// Waits for a free place, and returns an idle connection or the one that `open` gives.
    pub(crate) async fn lease<F>(&self, open: impl FnOnce() -> F) -> RedisResult<Lease<C>>
    where
        F: Future<Output = RedisResult<C>>,
    {
        let permit = self.permits.clone().acquire_owned().await;
        // Nothing closes the semaphore, so this error is a bug and no busy pool.
        let permit = permit
            .map_err(|_| RedisError::from((ErrorKind::Client, "The connection pool is closed")))?;
        let idle = self.idle.lock().pop();
        let (conn, reused) = match idle {
            Some(conn) => (conn, true),
            None => (open().await?, false),
        };
        Ok(Lease {
            conn,
            reused,
            idle: self.idle.clone(),
            permit,
        })
    }
}

impl<C> Lease<C> {
    /// Returns the connection to the pool, after a call that got its whole reply.
    pub(crate) fn give_back(self) {
        self.idle.lock().push(self.conn);
        // The connection is back before its place gets free, so the next lease finds it.
        drop(self.permit);
    }

    /// Closes the connection after a call that failed on it. A server that closed a connection
    /// which was idle has likely closed the other idle ones too, so the pool drops them all.
    fn close(self) {
        if self.reused {
            self.idle.lock().clear();
        }
    }
}

/// The connections of one Store.
pub(crate) struct Connections {
    shared: ConnectionManager,
    client: Client,
    readers: Pool<MultiplexedConnection>,
    response_timeout: Duration,
}

impl Connections {
    pub(crate) async fn open(settings: &Settings) -> RedisResult<Self> {
        let client = Client::open(settings.server.clone())?;
        let config =
            ConnectionManagerConfig::new().set_response_timeout(Some(settings.response_timeout));
        Ok(Self {
            shared: client.get_connection_manager_with_config(config).await?,
            client,
            readers: Pool::new(settings.max_blocking_connections),
            response_timeout: settings.response_timeout,
        })
    }

    /// Returns the connection of the calls that don't block. It reconnects by itself.
    pub(crate) fn shared(&self) -> ConnectionManager {
        self.shared.clone()
    }

    /// Waits until the Stream has an entry after `after`, until it closes, or until `deadline`.
    /// It can also return earlier, and the caller then runs its script again: when no
    /// connection is free, and after a failure that can pass. It fails only for an error that a
    /// retry gets again, such as a missing ACL permission.
    pub(crate) async fn wait_for_entry(
        &self,
        keys: &StreamKeys,
        after: EntryId,
        deadline: Instant,
    ) -> RedisResult<()> {
        let give_up = deadline.min(Instant::now() + BUSY_POLL);
        let open = || async {
            // The read bounds its own wait, so the connection has no response timeout.
            let config = AsyncConnectionConfig::new().set_response_timeout(None);
            (self.client)
                .get_multiplexed_async_connection_with_config(&config)
                .await
        };
        let mut lease = match tokio::time::timeout_at(give_up, self.readers.lease(open)).await {
            Err(_) => return Ok(()),
            Ok(Err(error)) => return pause_if_passing(error, deadline).await,
            Ok(Ok(lease)) => lease,
        };
        let left = deadline
            .saturating_duration_since(Instant::now())
            .as_millis();
        let mut xread = redis::cmd("XREAD");
        // `BLOCK 0` waits with no end, so a read at its deadline blocks for 1 ms.
        xread
            .arg("BLOCK")
            .arg(u64::try_from(left).unwrap_or(u64::MAX).max(1))
            .arg("COUNT")
            .arg(1)
            .arg("STREAMS")
            .arg(keys.log())
            .arg(keys.closed())
            .arg(after.to_string())
            // The key of a close has an entry only once the Stream is closed.
            .arg("0-0");
        // A server that stopped without closing the connection never replies.
        let end = deadline + self.response_timeout;
        match tokio::time::timeout_at(end, xread.query_async::<Value>(&mut lease.conn)).await {
            Err(_) => Ok(()),
            Ok(Ok(_)) => {
                lease.give_back();
                Ok(())
            }
            Ok(Err(error)) if matches!(error.retry_method(), RetryMethod::NoRetry) => {
                lease.give_back();
                Err(error)
            }
            Ok(Err(error)) => {
                lease.close();
                pause_if_passing(error, deadline).await
            }
        }
    }
}

/// Pauses after a failure that can pass, and returns the error of one that can't.
async fn pause_if_passing(error: RedisError, deadline: Instant) -> RedisResult<()> {
    if matches!(error.retry_method(), RetryMethod::NoRetry) {
        return Err(error);
    }
    tokio::time::sleep_until(deadline.min(Instant::now() + RETRY_PAUSE)).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_lease_keeps_its_connection_unless_the_call_ended() {
        let pool = Pool::new(1);
        let open = |conn: u8| move || async move { Ok(conn) };
        let first = pool.lease(open(1)).await.unwrap();
        // The pool has one place, so a second call waits while the first one has it.
        let busy = tokio::time::timeout(Duration::from_millis(10), pool.lease(open(2))).await;
        assert!(busy.is_err());
        first.give_back();
        let again = pool.lease(open(3)).await.unwrap();
        assert_eq!((again.conn, again.reused), (1, true));
        // A dropped call may have left its command on the server, so its connection is gone.
        drop(again);
        let fresh = pool.lease(open(4)).await.unwrap();
        assert_eq!((fresh.conn, fresh.reused), (4, false));
        // When a connection fails after it was idle, the pool drops the other idle ones too.
        pool.idle.lock().extend([5, 6]);
        fresh.give_back();
        pool.lease(open(7)).await.unwrap().close();
        assert_eq!(pool.lease(open(8)).await.unwrap().conn, 8);
    }
}
