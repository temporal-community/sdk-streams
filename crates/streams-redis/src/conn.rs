//! The connections of the Store. A blocked `XREAD` stops every later command on its connection,
//! so each read that waits takes a connection from a pool, and the calls that don't block share
//! one.

use crate::{
    keys::StreamKeys,
    offset::EntryId,
    options::{Servers, Settings},
};
use parking_lot::Mutex;
use redis::{
    AsyncConnectionConfig, Client, Cmd, ConnectionAddr, ConnectionInfo, ErrorKind, Pipeline,
    RedisError, RedisFuture, RedisResult, RetryMethod, Value,
    aio::{ConnectionLike, ConnectionManager, ConnectionManagerConfig, MultiplexedConnection},
    cluster::ClusterClient,
    cluster_async::ClusterConnection,
    from_redis_value,
};
use std::{collections::HashMap, future::Future, sync::Arc, time::Duration};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Instant,
};

/// How long a read waits for a free connection before it runs its script again. So the reads over
/// the maximum poll at this rate.
const BUSY_POLL: Duration = Duration::from_secs(1);

/// How long a read pauses after an `XREAD` that failed in a way that can pass, such as a dropped
/// connection or a slot that moves. Then it runs its script again, which fails the read if Redis
/// is still down.
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

/// The connection of the calls that don't block, to one server or to a Redis Cluster. Both
/// reconnect by themselves, and the one of a cluster follows a redirect to another node.
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

/// A range of slots of a Redis Cluster, and the node that serves it.
#[derive(Debug, PartialEq)]
struct SlotRange {
    first: u16,
    last: u16,
    host: String,
    port: u16,
}

/// Reads the reply of `CLUSTER SLOTS`. It has an array for each range, with its first slot, its
/// last slot, and then its nodes, the primary first. A node is an array of its host, its port
/// and more.
///
/// Redis gives no host, or an empty one, for a node that the client reaches at the host that it
/// asked. The Store asked through its cluster connection, so it takes `seed_host`, the host of
/// its first URL.
fn slot_ranges(reply: Vec<Vec<Value>>, seed_host: &str) -> RedisResult<Vec<SlotRange>> {
    let short = || RedisError::from((ErrorKind::Parse, "CLUSTER SLOTS gave too few fields"));
    let ranges = reply.into_iter().map(|range| {
        let mut fields = range.into_iter();
        let mut next = || fields.next().ok_or_else(short);
        let (first, last) = (from_redis_value(next()?)?, from_redis_value(next()?)?);
        let primary: Vec<Value> = from_redis_value(next()?)?;
        let mut fields = primary.into_iter();
        let mut next = || fields.next().ok_or_else(short);
        let host: Option<String> = from_redis_value(next()?)?;
        Ok(SlotRange {
            first,
            last,
            host: match host {
                Some(host) if !host.is_empty() => host,
                _ => seed_host.to_string(),
            },
            port: from_redis_value(next()?)?,
        })
    });
    ranges.collect()
}

/// The connections of one Store.
pub(crate) struct Connections {
    shared: Shared,
    /// The first URL of the options, whose credentials and TLS every node gets.
    seed: ConnectionInfo,
    /// What the Store knows of the slots of a cluster. A read that finds it empty asks again.
    slots: Mutex<Vec<SlotRange>>,
    /// The connections of the reads that wait, for each node.
    readers: Mutex<HashMap<String, Arc<Pool<MultiplexedConnection>>>>,
    max_blocking_connections: usize,
    response_timeout: Duration,
}

impl Connections {
    pub(crate) async fn open(settings: &Settings) -> RedisResult<Self> {
        let (shared, seed) = match &settings.servers {
            Servers::Standalone(server) => {
                let config = ConnectionManagerConfig::new()
                    .set_response_timeout(Some(settings.response_timeout));
                let client = Client::open(server.clone())?;
                let conn = client.get_connection_manager_with_config(config).await?;
                (Shared::Single(conn), server)
            }
            Servers::Cluster(nodes) => {
                let client = ClusterClient::builder(nodes.clone())
                    .response_timeout(settings.response_timeout)
                    .build()?;
                let conn = client.get_async_connection().await?;
                (Shared::Cluster(conn), &nodes[0])
            }
        };
        Ok(Self {
            shared,
            seed: seed.clone(),
            slots: Mutex::default(),
            readers: Mutex::default(),
            max_blocking_connections: settings.max_blocking_connections,
            response_timeout: settings.response_timeout,
        })
    }

    /// Returns the connection of the calls that don't block.
    pub(crate) fn shared(&self) -> Shared {
        self.shared.clone()
    }

    /// Waits until the Stream has an entry after `after`, until it closes, or until `deadline`.
    /// It can also return earlier, when no connection is free and after a failure that can pass,
    /// such as a dropped connection or a slot that moves. The caller then runs its script again.
    /// It fails only for an error that a retry gets again, such as a missing ACL permission.
    pub(crate) async fn wait_for_entry(
        &self,
        keys: &StreamKeys,
        after: EntryId,
        deadline: Instant,
    ) -> RedisResult<()> {
        match self.block(keys, after, deadline).await {
            Err(error) if !matches!(error.retry_method(), RetryMethod::NoRetry) => {
                // The node of the slot may be another one after the failure.
                self.slots.lock().clear();
                tokio::time::sleep_until(deadline.min(Instant::now() + RETRY_PAUSE)).await;
                Ok(())
            }
            result => result,
        }
    }

    async fn block(&self, keys: &StreamKeys, after: EntryId, deadline: Instant) -> RedisResult<()> {
        let node = self.node(keys).await?;
        let pool = self.pool(node.addr().to_string());
        let open = || async {
            // The read bounds its own wait, so the connection has no response timeout.
            let config = AsyncConnectionConfig::new().set_response_timeout(None);
            let client = Client::open(node)?;
            client
                .get_multiplexed_async_connection_with_config(&config)
                .await
        };
        let give_up = deadline.min(Instant::now() + BUSY_POLL);
        let Ok(lease) = tokio::time::timeout_at(give_up, pool.lease(open)).await else {
            return Ok(());
        };
        let mut lease = lease?;
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
                Err(error)
            }
        }
    }

    /// Returns the connections of the reads that wait on one node.
    fn pool(&self, node: String) -> Arc<Pool<MultiplexedConnection>> {
        let new = || Arc::new(Pool::new(self.max_blocking_connections));
        self.readers.lock().entry(node).or_insert_with(new).clone()
    }

    /// Returns the node that a read of the Stream blocks on. It's the one server, or the node of
    /// a cluster that serves the slot of the Stream's keys.
    async fn node(&self, keys: &StreamKeys) -> RedisResult<ConnectionInfo> {
        let Shared::Cluster(cluster) = &self.shared else {
            return Ok(self.seed.clone());
        };
        let (seed_host, tls) = match self.seed.addr() {
            ConnectionAddr::Tcp(host, _) => (host, None),
            ConnectionAddr::TcpTls {
                host,
                insecure,
                tls_params,
                ..
            } => (host, Some((*insecure, tls_params.clone()))),
            _ => return Err(unusable("The URL of a cluster node must have a host")),
        };
        let slot = keys.slot();
        let serves = |range: &&SlotRange| range.first <= slot && slot <= range.last;
        let address = |range: &SlotRange| (range.host.clone(), range.port);
        let known = self.slots.lock().iter().find(serves).map(address);
        let (host, port) = match known {
            Some(node) => node,
            None => {
                let mut slots = redis::cmd("CLUSTER");
                let reply = slots.arg("SLOTS").query_async(&mut cluster.clone()).await?;
                let ranges = slot_ranges(reply, seed_host)?;
                let node = ranges.iter().find(serves).map(address);
                *self.slots.lock() = ranges;
                node.ok_or_else(|| unusable("No node of the cluster serves the slot"))?
            }
        };
        Ok(self.seed.clone().set_addr(match tls {
            None => ConnectionAddr::Tcp(host, port),
            Some((insecure, tls_params)) => ConnectionAddr::TcpTls {
                host,
                port,
                insecure,
                tls_params,
            },
        }))
    }
}

/// Returns the error of a cluster that the read can't block on for the moment.
fn unusable(what: &'static str) -> RedisError {
    RedisError::from(std::io::Error::other(what))
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

    #[test]
    fn cluster_slots_gives_each_range_its_node() {
        // Three ranges with a host, an empty host and no host, and a replica after the first
        // primary.
        let reply = redis::parse_redis_value(
            b"*3\r\n*4\r\n:0\r\n:5460\r\n*3\r\n$8\r\n10.0.0.2\r\n:7001\r\n$1\r\na\r\n\
              *3\r\n$8\r\n10.0.0.5\r\n:7004\r\n$1\r\nb\r\n\
              *3\r\n:5461\r\n:10922\r\n*3\r\n$0\r\n\r\n:7002\r\n$1\r\nc\r\n\
              *3\r\n:10923\r\n:16383\r\n*3\r\n$-1\r\n:7003\r\n$1\r\nd\r\n",
        );
        let range = |first, last, host: &str, port| SlotRange {
            first,
            last,
            host: host.to_string(),
            port,
        };
        let ranges = slot_ranges(from_redis_value(reply.unwrap()).unwrap(), "seed.example");
        let expected = [
            range(0, 5460, "10.0.0.2", 7001),
            range(5461, 10922, "seed.example", 7002),
            range(10923, 16383, "seed.example", 7003),
        ];
        assert_eq!(ranges.unwrap(), expected);
        let short = redis::parse_redis_value(b"*1\r\n*2\r\n:0\r\n:1\r\n").unwrap();
        assert!(slot_ranges(from_redis_value(short).unwrap(), "seed.example").is_err());
    }
}
