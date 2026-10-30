//! The connections of the Redis Store. All short calls share one multiplexed connection, and each
//! blocking read, and each write with `WAIT`, uses a dedicated one.
//!
//! redis-rs sends all commands of a multiplexed connection on one socket, in order. A blocking
//! `XREAD` there would block every append after it. So a blocking read uses a dedicated connection
//! from a bounded pool per node. A cancelled read drops its connection, since the server can still
//! reply to the blocked command on it. Redis reads no more commands from a connection while its
//! `WAIT` blocks, so each write with `WAIT` also takes a connection of its own.

use redis::{
    AsyncConnectionConfig, Client, Cmd, ConnectionAddr, ConnectionInfo, ErrorKind,
    IntoConnectionInfo, Pipeline, RedisError, RedisFuture, RedisResult, ServerErrorKind, Value,
    aio::{ConnectionLike, ConnectionManager, ConnectionManagerConfig, MultiplexedConnection},
    cluster::ClusterClient,
    cluster_async::ClusterConnection,
    cluster_routing::{MultipleNodeRoutingInfo, RoutingInfo},
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, SemaphorePermit},
    time::Instant,
};

/// How long a blocking read waits for a free connection of its node. Then it returns with no wake,
/// and the caller reads the Stream again, so a read past the pool's size polls about once a second.
const PERMIT_WAIT: Duration = Duration::from_secs(1);

/// How long a blocking read waits before it returns with no wake while the cluster moves its slot.
/// The caller then reads the Stream again, so such a read polls at this rate until the move ends.
const RESHARD_WAIT: Duration = Duration::from_millis(100);

/// The most writes with `WAIT` that one Store runs at once, so its connections per node stay few.
const WAIT_CONNECTIONS: usize = 8;

/// The connection that all short calls share.
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

impl Shared {
    pub(crate) async fn connect(
        urls: &[String],
        cluster: bool,
        response_timeout: Duration,
    ) -> RedisResult<Self> {
        if cluster {
            let client = ClusterClient::builder(urls.iter().map(String::as_str))
                .response_timeout(response_timeout)
                .build()?;
            return Ok(Shared::Cluster(client.get_async_connection().await?));
        }
        let client = Client::open(urls[0].as_str())?;
        let config = ConnectionManagerConfig::new().set_response_timeout(Some(response_timeout));
        Ok(Shared::Single(
            client.get_connection_manager_with_config(config).await?,
        ))
    }

    /// Runs `cmd` on the server, or on every primary of a cluster, and returns each reply with the
    /// node that sent it. Each primary holds its own share of the keys and its own settings.
    pub(crate) async fn on_each_primary(&self, cmd: Cmd) -> RedisResult<Vec<(String, Value)>> {
        match self.clone() {
            Shared::Single(mut conn) => {
                Ok(vec![(String::new(), cmd.query_async(&mut conn).await?)])
            }
            Shared::Cluster(mut conn) => {
                let routing = RoutingInfo::MultiNode((MultipleNodeRoutingInfo::AllMasters, None));
                // The client gives each node's refusal as that node's reply, not as an error.
                match conn.route_command(cmd, routing).await? {
                    Value::Map(replies) => replies
                        .into_iter()
                        .map(|(node, reply)| {
                            let node = match node {
                                Value::BulkString(bytes) => {
                                    String::from_utf8_lossy(&bytes).into_owned()
                                }
                                Value::SimpleString(text) => text,
                                other => format!("{other:?}"),
                            };
                            Ok((node, reply.extract_error()?))
                        })
                        .collect(),
                    other => Ok(vec![(String::new(), other.extract_error()?)]),
                }
            }
        }
    }
}

/// The connections of the writes with `WAIT`, opened as the writes need them.
pub(crate) struct WaitPool {
    urls: Vec<String>,
    cluster: bool,
    response_timeout: Duration,
    permits: Semaphore,
    idle: Mutex<Vec<Shared>>,
}

/// A connection of the [WaitPool], returned to it only when the write on it got its answers.
pub(crate) struct WaitLease<'a> {
    pool: &'a WaitPool,
    pub(crate) conn: Shared,
    _permit: SemaphorePermit<'a>,
}

impl WaitLease<'_> {
    pub(crate) fn release(self) {
        self.pool.idle.lock().unwrap().push(self.conn);
    }
}

impl WaitPool {
    /// A pool that starts with `first`, which tells a bad URL at connect.
    pub(crate) fn new(
        first: Shared,
        urls: &[String],
        cluster: bool,
        response_timeout: Duration,
    ) -> Self {
        Self {
            urls: urls.to_vec(),
            cluster,
            response_timeout,
            permits: Semaphore::new(WAIT_CONNECTIONS),
            idle: Mutex::new(vec![first]),
        }
    }

    /// Takes an idle connection, or opens one. Returns `None` when every connection stays busy
    /// for the response timeout.
    pub(crate) async fn take(&self) -> RedisResult<Option<WaitLease<'_>>> {
        let permit = self.permits.acquire();
        let Ok(Ok(permit)) = tokio::time::timeout(self.response_timeout, permit).await else {
            return Ok(None);
        };
        let idle = self.idle.lock().unwrap().pop();
        let conn = match idle {
            Some(conn) => conn,
            None => Shared::connect(&self.urls, self.cluster, self.response_timeout).await?,
        };
        Ok(Some(WaitLease {
            pool: self,
            conn,
            _permit: permit,
        }))
    }
}

/// The host and port of a Redis node.
type Node = (String, u16);

struct NodePool {
    info: ConnectionInfo,
    permits: Arc<Semaphore>,
    idle: Mutex<Vec<MultiplexedConnection>>,
}

/// Dedicated connections for blocking reads, a bounded number per node.
pub(crate) struct BlockingPool {
    /// The standalone server, or each seed of a cluster. The first one gives every node its
    /// credentials and TLS.
    seeds: Vec<ConnectionInfo>,
    per_node: usize,
    cluster: bool,
    /// How long a blocked read waits for the server's reply after the read's deadline. A server
    /// that stopped without a reset never replies, and the read must still end.
    response_timeout: Duration,
    nodes: Mutex<HashMap<Node, Arc<NodePool>>>,
    /// The node that serves each slot, learned from redirects. A cluster moves slots rarely, and
    /// a stale entry costs one redirect.
    slots: Mutex<HashMap<u16, Node>>,
}

/// A dedicated connection, returned to its pool only when the call on it finished.
struct Lease {
    pool: Arc<NodePool>,
    conn: Option<MultiplexedConnection>,
    /// Taken idle from the pool, so the server may have dropped it since its last use.
    reused: bool,
    _permit: OwnedSemaphorePermit,
}

impl Lease {
    fn release(mut self) {
        if let Some(conn) = self.conn.take() {
            self.pool.idle.lock().unwrap().push(conn);
        }
    }
}

impl BlockingPool {
    pub(crate) fn new(
        urls: &[String],
        per_node: usize,
        cluster: bool,
        response_timeout: Duration,
    ) -> RedisResult<Self> {
        let count = if cluster { urls.len() } else { 1 };
        let seeds = urls
            .iter()
            .take(count)
            .map(|url| url.as_str().into_connection_info())
            .collect::<RedisResult<Vec<_>>>()?;
        if seeds.is_empty() {
            return Err(RedisError::from((
                ErrorKind::InvalidClientConfig,
                "no Redis URL",
            )));
        }
        Ok(Self {
            seeds,
            per_node: per_node.max(1),
            cluster,
            response_timeout,
            nodes: Mutex::default(),
            slots: Mutex::default(),
        })
    }

    /// Runs the command that `cmd` builds for the time left to `deadline`, on a dedicated
    /// connection to the node that holds `key`.
    ///
    /// Returns `None` when no connection became free before `deadline` or within [PERMIT_WAIT], or
    /// when the cluster moves the key's slot. The caller treats it as a wait in which nothing came.
    pub(crate) async fn query(
        &self,
        key: &str,
        deadline: Instant,
        cmd: impl Fn(Duration) -> Cmd,
    ) -> RedisResult<Option<Value>> {
        let slot = self.cluster.then(|| slot(key));
        let cached = slot.and_then(|slot| self.slots.lock().unwrap().get(&slot).cloned());
        // A seed that is down may have a replica that took over its slots, which another seed
        // knows.
        let mut seeds = self.seeds.iter().map(address);
        let mut node = match cached {
            Some(node) => node,
            None => seeds.next().expect("a pool has a seed"),
        };
        // One redirect is expected when the slot was unknown or moved. A second means the
        // cluster is resharding.
        let mut redirected = false;
        let mut fresh = false;
        loop {
            // Each node that is down costs a connect timeout, so the seeds can outlast the read.
            if deadline <= Instant::now() {
                return Ok(None);
            }
            let mut lease = match self.lease(&node, deadline, fresh).await {
                Ok(Some(lease)) => lease,
                Ok(None) => return Ok(None),
                Err(error) if unreachable(&error) => {
                    self.forget(slot, &node);
                    node = seeds.by_ref().find(|seed| *seed != node).ok_or(error)?;
                    fresh = false;
                    continue;
                }
                Err(error) => return Err(error),
            };
            // The lease can wait up to [PERMIT_WAIT], so the block time comes from the deadline.
            let wait = deadline.saturating_duration_since(Instant::now());
            if wait.is_zero() {
                lease.release();
                return Ok(None);
            }
            let conn = lease.conn.as_mut().expect("a lease holds its connection");
            let answer = tokio::time::timeout_at(
                deadline + self.response_timeout,
                cmd(wait).query_async::<Value>(conn),
            )
            .await;
            // Dropping the lease drops the connection, which may still get the late answer.
            let Ok(result) = answer else {
                return Err(RedisError::from((
                    ErrorKind::Io,
                    "Redis did not answer a blocked read by its deadline",
                )));
            };
            match result {
                Ok(value) => {
                    lease.release();
                    return Ok(Some(value));
                }
                // A server restart closes every idle connection. So one retry on a new connection
                // tells a dead connection from a dead server.
                Err(error)
                    if lease.reused
                        && !fresh
                        && (error.is_io_error() || error.is_connection_dropped()) =>
                {
                    fresh = true;
                }
                Err(error) if unreachable(&error) => {
                    self.forget(slot, &node);
                    node = seeds.by_ref().find(|seed| *seed != node).ok_or(error)?;
                    fresh = false;
                }
                Err(error) => match (slot, redirect(&error)) {
                    (Some(slot), Some(target)) if !redirected => {
                        lease.release();
                        self.slots.lock().unwrap().insert(slot, target.clone());
                        node = target;
                        redirected = true;
                    }
                    _ if slot.is_some() && resharding(&error) => {
                        lease.release();
                        tokio::time::sleep_until(deadline.min(Instant::now() + RESHARD_WAIT)).await;
                        return Ok(None);
                    }
                    _ => return Err(error),
                },
            }
        }
    }

    /// Drops a slot's cached node. A failover can change a node's address for good, and otherwise
    /// only a redirect would update the entry.
    fn forget(&self, slot: Option<u16>, node: &Node) {
        if let Some(slot) = slot {
            let mut slots = self.slots.lock().unwrap();
            if slots.get(&slot) == Some(node) {
                slots.remove(&slot);
            }
        }
    }

    async fn lease(
        &self,
        node: &Node,
        deadline: Instant,
        fresh: bool,
    ) -> RedisResult<Option<Lease>> {
        let pool = self.node(node);
        let until = deadline.min(Instant::now() + PERMIT_WAIT);
        let Ok(Ok(permit)) =
            tokio::time::timeout_at(until, pool.permits.clone().acquire_owned()).await
        else {
            return Ok(None);
        };
        let idle = if fresh {
            None
        } else {
            pool.idle.lock().unwrap().pop()
        };
        let reused = idle.is_some();
        let conn = match idle {
            Some(conn) => conn,
            None => {
                // The read bounds the wait itself, so the connection sets no response timeout.
                let config = AsyncConnectionConfig::new().set_response_timeout(None);
                Client::open(pool.info.clone())?
                    .get_multiplexed_async_connection_with_config(&config)
                    .await?
            }
        };
        Ok(Some(Lease {
            pool,
            conn: Some(conn),
            reused,
            _permit: permit,
        }))
    }

    fn node(&self, node: &Node) -> Arc<NodePool> {
        self.nodes
            .lock()
            .unwrap()
            .entry(node.clone())
            .or_insert_with(|| {
                Arc::new(NodePool {
                    info: at(&self.seeds[0], node),
                    permits: Arc::new(Semaphore::new(self.per_node)),
                    idle: Mutex::default(),
                })
            })
            .clone()
    }
}

fn address(info: &ConnectionInfo) -> Node {
    match info.addr() {
        ConnectionAddr::Tcp(host, port) | ConnectionAddr::TcpTls { host, port, .. } => {
            (host.clone(), *port)
        }
        ConnectionAddr::Unix(path) => (path.display().to_string(), 0),
        _ => (String::new(), 0),
    }
}

/// The seed's connection settings, with its credentials and TLS, for another node.
fn at(seed: &ConnectionInfo, (host, port): &Node) -> ConnectionInfo {
    let addr = match seed.addr() {
        ConnectionAddr::TcpTls {
            insecure,
            tls_params,
            ..
        } => ConnectionAddr::TcpTls {
            host: host.clone(),
            port: *port,
            insecure: *insecure,
            tls_params: tls_params.clone(),
        },
        ConnectionAddr::Tcp(..) => ConnectionAddr::Tcp(host.clone(), *port),
        other => other.clone(),
    };
    seed.clone().set_addr(addr)
}

fn unreachable(error: &RedisError) -> bool {
    error.is_io_error() || error.is_connection_refusal() || error.is_connection_dropped()
}

fn redirect(error: &RedisError) -> Option<Node> {
    if error.kind() != ErrorKind::Server(ServerErrorKind::Moved) {
        return None;
    }
    error
        .redirect_node()
        .and_then(|(address, _)| address.rsplit_once(':'))
        .and_then(|(host, port)| Some((host.to_string(), port.parse().ok()?)))
}

/// Whether the cluster is moving the slot of the command's key. An `ASK` target serves the key
/// only for a command sent after `ASKING`, and the slot can move again before the next read.
fn resharding(error: &RedisError) -> bool {
    matches!(
        error.kind(),
        ErrorKind::Server(ServerErrorKind::Ask | ServerErrorKind::Moved)
    )
}

/// The cluster slot of a key, which is the CRC16 of its hash tag, or of the whole key with no tag.
pub(crate) fn slot(key: &str) -> u16 {
    let bytes = key.as_bytes();
    let tagged = bytes.iter().position(|&b| b == b'{').and_then(|open| {
        let close = bytes[open + 1..].iter().position(|&b| b == b'}')?;
        (close > 0).then(|| &bytes[open + 1..open + 1 + close])
    });
    crc16::State::<crc16::XMODEM>::calculate(tagged.unwrap_or(bytes)) % 16384
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_slot_is_taken_over_the_hash_tag() {
        // Values from `CLUSTER KEYSLOT`.
        assert_eq!(slot("foo"), 12182);
        assert_eq!(slot("{user1000}.following"), slot("{user1000}.followers"));
        assert_eq!(slot("{user1000}.following"), slot("user1000"));
        // An empty tag hashes the whole key.
        assert_ne!(slot("foo{}{bar}"), slot("bar"));
        assert_eq!(slot("p:{ns:wf:run}:t:a"), slot("p:{ns:wf:run}:chain"));
    }

    #[test]
    fn only_a_moved_slot_is_remembered() {
        let reply = |text: &str| {
            redis::parse_redis_value(format!("-{text}\r\n").as_bytes())
                .unwrap()
                .extract_error()
                .unwrap_err()
        };
        assert_eq!(
            redirect(&reply("MOVED 3999 127.0.0.1:7292")),
            Some(("127.0.0.1".to_string(), 7292))
        );
        // An `ASK` target holds the slot only for one command sent after `ASKING`.
        assert_eq!(redirect(&reply("ASK 3999 127.0.0.1:7292")), None);
    }

    #[tokio::test]
    async fn a_cached_slot_on_a_dead_node_goes_back_to_the_seed() {
        let Some(url) = crate::server::server_url("STREAMS_REDIS_URL") else {
            return;
        };
        // A standalone server answers every slot, so the seed serves the read once the dead
        // node is dropped.
        let pool = BlockingPool::new(&[url], 1, true, Duration::from_secs(1)).unwrap();
        let key = "test:{dead-node}:log";
        pool.slots
            .lock()
            .unwrap()
            .insert(slot(key), ("127.0.0.1".to_string(), dead_port()));
        let deadline = Instant::now() + Duration::from_secs(2);
        let answer = pool.query(key, deadline, |_| redis::cmd("PING")).await;
        assert_eq!(
            answer.unwrap(),
            Some(Value::SimpleString("PONG".to_string()))
        );
        assert!(pool.slots.lock().unwrap().is_empty());
    }

    /// A port that nothing listens on.
    fn dead_port() -> u16 {
        let dead = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        dead.local_addr().unwrap().port()
    }

    /// The URL of a server that gives every command the answer that `reply` builds from its
    /// address.
    fn fake_server(reply: fn(std::net::SocketAddr) -> String) -> String {
        use std::io::Write;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            for mut stream in listener.incoming().flatten() {
                let reply = reply(address);
                std::thread::spawn(move || {
                    let mut reader = stream.try_clone().unwrap();
                    let mut parser = redis::Parser::new();
                    while parser.parse_value(&mut reader).is_ok()
                        && stream.write_all(reply.as_bytes()).is_ok()
                    {}
                });
            }
        });
        format!("redis://{address}")
    }

    #[tokio::test]
    async fn a_seed_that_is_down_passes_the_read_to_the_next_seed() {
        let live = fake_server(|_| "+PONG\r\n".to_string());
        let dead = format!("redis://127.0.0.1:{}", dead_port());
        let pool = BlockingPool::new(&[dead, live], 1, true, Duration::from_secs(1)).unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        let answer = pool
            .query("{s}:log", deadline, |_| redis::cmd("PING"))
            .await;
        assert_eq!(
            answer.unwrap(),
            Some(Value::SimpleString("PONG".to_string()))
        );
    }

    #[tokio::test]
    async fn a_slot_in_motion_ends_the_read_with_no_wake() {
        let ask = fake_server(|at| format!("-ASK 1 {at}\r\n"));
        let moved = fake_server(|at| format!("-MOVED 1 {at}\r\n"));
        for url in [ask, moved] {
            let pool = BlockingPool::new(&[url], 1, true, Duration::from_secs(1)).unwrap();
            let start = Instant::now();
            let answer = pool.query("{s}:log", start + Duration::from_secs(5), |_| {
                redis::cmd("XREAD")
            });
            assert_eq!(answer.await.unwrap(), None);
            assert!(start.elapsed() < Duration::from_secs(1));
        }
    }

    #[tokio::test]
    async fn the_block_time_starts_when_the_read_has_a_connection() {
        let url = fake_server(|_| "*-1\r\n".to_string());
        let pool = Arc::new(BlockingPool::new(&[url], 1, false, Duration::from_secs(1)).unwrap());
        let start = Instant::now();
        let held = pool
            .lease(
                &address(&pool.seeds[0]),
                start + Duration::from_secs(1),
                false,
            )
            .await
            .unwrap()
            .unwrap();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            held.release();
        });
        let blocked = Mutex::new(Duration::ZERO);
        let deadline = start + Duration::from_millis(500);
        let answer = pool.query("s", deadline, |wait| {
            *blocked.lock().unwrap() = wait;
            redis::cmd("XREAD")
        });
        assert_eq!(answer.await.unwrap(), Some(Value::Nil));
        assert!(*blocked.lock().unwrap() <= Duration::from_millis(200));
    }

    #[test]
    fn another_node_keeps_the_seed_credentials() {
        let seed = "redis://user:secret@127.0.0.1:7291/0"
            .into_connection_info()
            .unwrap();
        let other = at(&seed, &("10.0.0.2".to_string(), 7292));
        assert_eq!(address(&other), ("10.0.0.2".to_string(), 7292));
        let settings = |info: &ConnectionInfo| {
            let redis = info.redis_settings();
            (
                redis.username().map(str::to_string),
                redis.password().map(str::to_string),
                redis.db(),
            )
        };
        assert_eq!(settings(&other), settings(&seed));
        assert_eq!(settings(&other).1.as_deref(), Some("secret"));
    }
}
