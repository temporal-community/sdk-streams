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
    future::Future,
    str::FromStr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
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

/// Where the Store's Redis is, parsed once from the URLs of the options.
pub(crate) struct Endpoint {
    /// The standalone server, or each seed of a cluster. The first one gives every node its
    /// credentials and TLS.
    seeds: Vec<ConnectionInfo>,
    cluster: bool,
    response_timeout: Duration,
}

impl Endpoint {
    pub(crate) fn parse(
        urls: &[String],
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
            cluster,
            response_timeout,
        })
    }

    /// Opens a connection for short calls, which the client reconnects when it drops.
    pub(crate) async fn open(&self) -> RedisResult<SharedConnection> {
        if self.cluster {
            let client = ClusterClient::builder(self.seeds.clone())
                .response_timeout(self.response_timeout)
                .build()?;
            return Ok(SharedConnection::Cluster(
                client.get_async_connection().await?,
            ));
        }
        let client = Client::open(self.seeds[0].clone())?;
        let config =
            ConnectionManagerConfig::new().set_response_timeout(Some(self.response_timeout));
        Ok(SharedConnection::Single(
            client.get_connection_manager_with_config(config).await?,
        ))
    }
}

/// The connection that all short calls share.
#[derive(Clone)]
pub(crate) enum SharedConnection {
    Single(ConnectionManager),
    Cluster(ClusterConnection),
}

impl ConnectionLike for SharedConnection {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        match self {
            SharedConnection::Single(conn) => conn.req_packed_command(cmd),
            SharedConnection::Cluster(conn) => conn.req_packed_command(cmd),
        }
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        pipeline: &'a Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        match self {
            SharedConnection::Single(conn) => conn.req_packed_commands(pipeline, offset, count),
            SharedConnection::Cluster(conn) => conn.req_packed_commands(pipeline, offset, count),
        }
    }

    fn get_db(&self) -> i64 {
        match self {
            SharedConnection::Single(conn) => conn.get_db(),
            SharedConnection::Cluster(conn) => conn.get_db(),
        }
    }
}

impl SharedConnection {
    /// Runs `cmd` on the server, or on every primary of a cluster, and returns each reply with the
    /// node that sent it. Each primary holds its own share of the keys and its own settings.
    pub(crate) async fn on_each_primary(&self, cmd: Cmd) -> RedisResult<Vec<(String, Value)>> {
        match self.clone() {
            SharedConnection::Single(mut conn) => {
                Ok(vec![(String::new(), cmd.query_async(&mut conn).await?)])
            }
            SharedConnection::Cluster(mut conn) => {
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

/// A bounded set of dedicated connections, opened as the calls need them.
struct Pool<C> {
    permits: Arc<Semaphore>,
    idle: Arc<Mutex<Vec<C>>>,
}

impl<C> Pool<C> {
    fn new(size: usize, idle: Vec<C>) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(size.max(1))),
            idle: Arc::new(Mutex::new(idle)),
        }
    }

    /// Takes an idle connection, or opens one with `open`. A `fresh` lease always opens one.
    /// Returns `None` when no connection becomes free by `until`.
    async fn lease<F>(
        &self,
        until: Instant,
        fresh: bool,
        open: impl FnOnce() -> F,
    ) -> RedisResult<Option<Lease<C>>>
    where
        F: Future<Output = RedisResult<C>>,
    {
        let permit = self.permits.clone().acquire_owned();
        let Ok(Ok(permit)) = tokio::time::timeout_at(until, permit).await else {
            return Ok(None);
        };
        let idle = if fresh {
            None
        } else {
            self.idle.lock().unwrap().pop()
        };
        let reused = idle.is_some();
        let conn = match idle {
            Some(conn) => conn,
            None => open().await?,
        };
        Ok(Some(Lease {
            conn: Some(conn),
            reused,
            keep: false,
            idle: self.idle.clone(),
            _permit: permit,
        }))
    }
}

/// A connection taken from a [Pool]. It goes back to the pool when the lease drops, but only if
/// the call on it got its answer. A cancelled or failed call can still get a late reply on the
/// connection, and the next call would read that reply as its own.
pub(crate) struct Lease<C> {
    conn: Option<C>,
    /// Taken idle from the pool, so the server may have dropped it since its last use.
    reused: bool,
    keep: bool,
    idle: Arc<Mutex<Vec<C>>>,
    _permit: OwnedSemaphorePermit,
}

impl<C> Lease<C> {
    pub(crate) fn conn(&mut self) -> &mut C {
        self.conn
            .as_mut()
            .expect("a lease holds its connection until it drops")
    }

    /// Says that the call on the connection got its answer, so the next call can use it.
    pub(crate) fn keep(&mut self) {
        self.keep = true;
    }
}

impl<C> Drop for Lease<C> {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take().filter(|_| self.keep) {
            self.idle.lock().unwrap().push(conn);
        }
    }
}

/// The connections of the writes with `WAIT`.
pub(crate) struct WaitPool {
    endpoint: Arc<Endpoint>,
    pool: Pool<SharedConnection>,
}

impl WaitPool {
    /// A pool that starts with `first`, which tells a bad URL at connect.
    pub(crate) fn new(first: SharedConnection, endpoint: Arc<Endpoint>) -> Self {
        Self {
            endpoint,
            pool: Pool::new(WAIT_CONNECTIONS, vec![first]),
        }
    }

    /// Takes an idle connection, or opens one. Returns `None` when every connection stays busy
    /// for the response timeout.
    pub(crate) async fn take(&self) -> RedisResult<Option<Lease<SharedConnection>>> {
        let until = Instant::now() + self.endpoint.response_timeout;
        self.pool.lease(until, false, || self.endpoint.open()).await
    }
}

/// The host and port of a Redis node.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Node {
    host: String,
    port: u16,
}

impl Node {
    fn of(info: &ConnectionInfo) -> Self {
        let (host, port) = match info.addr() {
            ConnectionAddr::Tcp(host, port) | ConnectionAddr::TcpTls { host, port, .. } => {
                (host.clone(), *port)
            }
            ConnectionAddr::Unix(path) => (path.display().to_string(), 0),
            _ => (String::new(), 0),
        };
        Self { host, port }
    }

    /// The seed's connection settings, with its credentials and TLS, for this node.
    fn info(&self, seed: &ConnectionInfo) -> ConnectionInfo {
        let addr = match seed.addr() {
            ConnectionAddr::TcpTls {
                insecure,
                tls_params,
                ..
            } => ConnectionAddr::TcpTls {
                host: self.host.clone(),
                port: self.port,
                insecure: *insecure,
                tls_params: tls_params.clone(),
            },
            ConnectionAddr::Tcp(..) => ConnectionAddr::Tcp(self.host.clone(), self.port),
            other => other.clone(),
        };
        seed.clone().set_addr(addr)
    }
}

/// Parses `host:port`, as Redis writes a node in a redirect.
impl FromStr for Node {
    type Err = RedisError;

    fn from_str(address: &str) -> RedisResult<Self> {
        let bad = || RedisError::from((ErrorKind::InvalidClientConfig, "not a host:port"));
        let (host, port) = address.rsplit_once(':').ok_or_else(bad)?;
        Ok(Self {
            host: host.to_string(),
            port: port.parse().map_err(|_| bad())?,
        })
    }
}

/// The node that serves each slot, learned from redirects. A cluster moves slots rarely, and a
/// stale entry costs one redirect.
#[derive(Default)]
struct SlotCache(Mutex<HashMap<u16, Node>>);

impl SlotCache {
    fn get(&self, slot: Option<u16>) -> Option<Node> {
        self.0.lock().unwrap().get(&slot?).cloned()
    }

    fn learn(&self, slot: u16, node: Node) {
        self.0.lock().unwrap().insert(slot, node);
    }

    /// Drops a slot's entry while it still names `node`. A failover can change a node's address
    /// for good, and otherwise only a redirect would update the entry.
    fn forget(&self, slot: Option<u16>, node: &Node) {
        if let Some(slot) = slot {
            let mut slots = self.0.lock().unwrap();
            if slots.get(&slot) == Some(node) {
                slots.remove(&slot);
            }
        }
    }
}

/// What a blocking read knows when a try fails.
#[derive(Clone, Copy, Debug, Default)]
struct Attempt {
    cluster: bool,
    /// The connection came idle from the pool.
    reused: bool,
    /// The try was already on a new connection.
    fresh: bool,
    /// The read already followed one redirect.
    redirected: bool,
}

/// What a blocking read does after a failed try.
#[derive(Debug, PartialEq, Eq)]
enum Step {
    /// Try the node again on a new connection.
    Fresh,
    /// The node is down, so try the next seed.
    NextSeed,
    /// Follow the cluster to the node that now serves the slot.
    Redirect(Node),
    /// The cluster is moving the slot, so end the read with no wake.
    NoWake,
    /// Give the error to the caller.
    Fail,
}

/// The retry policy of a blocking read. It has no I/O, so a table test covers it.
fn classify(error: &RedisError, attempt: Attempt) -> Step {
    // A server restart closes every idle connection. So one retry on a new connection tells a
    // dead connection from a dead server.
    if attempt.reused && !attempt.fresh && (error.is_io_error() || error.is_connection_dropped()) {
        return Step::Fresh;
    }
    if unreachable(error) {
        return Step::NextSeed;
    }
    if !attempt.cluster {
        return Step::Fail;
    }
    // One redirect is expected when the slot was unknown or moved. A second means the cluster is
    // resharding.
    match redirect(error) {
        Some(target) if !attempt.redirected => Step::Redirect(target),
        _ if resharding(error) => Step::NoWake,
        _ => Step::Fail,
    }
}

/// Dedicated connections for blocking reads, a bounded number per node.
pub(crate) struct BlockingPool {
    endpoint: Arc<Endpoint>,
    per_node: usize,
    nodes: Mutex<HashMap<Node, Arc<NodePool>>>,
    slots: SlotCache,
}

struct NodePool {
    info: ConnectionInfo,
    pool: Pool<MultiplexedConnection>,
}

impl BlockingPool {
    pub(crate) fn new(endpoint: Arc<Endpoint>, per_node: usize) -> Self {
        Self {
            endpoint,
            per_node,
            nodes: Mutex::default(),
            slots: SlotCache::default(),
        }
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
        let slot = self.endpoint.cluster.then(|| slot(key));
        // A seed that is down may have a replica that took over its slots, which another seed
        // knows.
        let mut seeds = self.endpoint.seeds.iter().map(Node::of);
        let mut node = match self.slots.get(slot) {
            Some(node) => node,
            None => seeds.next().expect("an endpoint has a seed"),
        };
        let mut attempt = Attempt {
            cluster: slot.is_some(),
            ..Attempt::default()
        };
        loop {
            // Each node that is down costs a connect timeout, so the seeds can outlast the read.
            if deadline <= Instant::now() {
                return Ok(None);
            }
            let error = match self.lease(&node, deadline, attempt.fresh).await {
                Ok(None) => return Ok(None),
                Ok(Some(mut lease)) => {
                    // The lease can wait up to [PERMIT_WAIT], so the block time comes from the
                    // deadline.
                    let wait = deadline.saturating_duration_since(Instant::now());
                    if wait.is_zero() {
                        lease.keep();
                        return Ok(None);
                    }
                    attempt.reused = lease.reused;
                    let answer = tokio::time::timeout_at(
                        deadline + self.endpoint.response_timeout,
                        cmd(wait).query_async::<Value>(lease.conn()),
                    )
                    .await;
                    // A server that stopped without a reset never replies, and the read must still
                    // end. The lease then drops the connection, which may still get the answer.
                    let Ok(result) = answer else {
                        return Err(RedisError::from((
                            ErrorKind::Io,
                            "Redis did not answer a blocked read by its deadline",
                        )));
                    };
                    match result {
                        Ok(value) => {
                            lease.keep();
                            return Ok(Some(value));
                        }
                        Err(error) => {
                            // A server error is a whole reply, so the connection is still in step.
                            if error.code().is_some() {
                                lease.keep();
                            }
                            error
                        }
                    }
                }
                Err(error) => {
                    attempt.reused = false;
                    error
                }
            };
            match classify(&error, attempt) {
                Step::Fresh => attempt.fresh = true,
                Step::NextSeed => {
                    self.slots.forget(slot, &node);
                    node = seeds.by_ref().find(|seed| *seed != node).ok_or(error)?;
                    attempt.fresh = false;
                }
                Step::Redirect(target) => {
                    if let Some(slot) = slot {
                        self.slots.learn(slot, target.clone());
                    }
                    node = target;
                    attempt.redirected = true;
                }
                Step::NoWake => {
                    tokio::time::sleep_until(deadline.min(Instant::now() + RESHARD_WAIT)).await;
                    return Ok(None);
                }
                Step::Fail => return Err(error),
            }
        }
    }

    async fn lease(
        &self,
        node: &Node,
        deadline: Instant,
        fresh: bool,
    ) -> RedisResult<Option<Lease<MultiplexedConnection>>> {
        let node = self.node(node);
        let until = deadline.min(Instant::now() + PERMIT_WAIT);
        let open = || async {
            // The read bounds the wait itself, so the connection sets no response timeout.
            let config = AsyncConnectionConfig::new().set_response_timeout(None);
            Client::open(node.info.clone())?
                .get_multiplexed_async_connection_with_config(&config)
                .await
        };
        node.pool.lease(until, fresh, open).await
    }

    fn node(&self, node: &Node) -> Arc<NodePool> {
        self.nodes
            .lock()
            .unwrap()
            .entry(node.clone())
            .or_insert_with(|| {
                Arc::new(NodePool {
                    info: node.info(&self.endpoint.seeds[0]),
                    pool: Pool::new(self.per_node, Vec::new()),
                })
            })
            .clone()
    }
}

fn unreachable(error: &RedisError) -> bool {
    error.is_io_error() || error.is_connection_refusal() || error.is_connection_dropped()
}

fn redirect(error: &RedisError) -> Option<Node> {
    if error.kind() != ErrorKind::Server(ServerErrorKind::Moved) {
        return None;
    }
    error.redirect_node()?.0.parse().ok()
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

    fn node(host: &str, port: u16) -> Node {
        Node {
            host: host.to_string(),
            port,
        }
    }

    fn reply(text: &str) -> RedisError {
        redis::parse_redis_value(format!("-{text}\r\n").as_bytes())
            .unwrap()
            .extract_error()
            .unwrap_err()
    }

    fn io(kind: std::io::ErrorKind) -> RedisError {
        RedisError::from(std::io::Error::new(kind, "test"))
    }

    fn endpoint(urls: &[&str], cluster: bool) -> Arc<Endpoint> {
        let urls: Vec<String> = urls.iter().map(|url| url.to_string()).collect();
        Arc::new(Endpoint::parse(&urls, cluster, Duration::from_secs(1)).unwrap())
    }

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
        assert_eq!(
            redirect(&reply("MOVED 3999 127.0.0.1:7292")),
            Some(node("127.0.0.1", 7292))
        );
        // An `ASK` target holds the slot only for one command sent after `ASKING`.
        assert_eq!(redirect(&reply("ASK 3999 127.0.0.1:7292")), None);
        assert!("127.0.0.1".parse::<Node>().is_err());
        assert!("127.0.0.1:port".parse::<Node>().is_err());
    }

    #[test]
    fn the_retry_policy_of_a_blocking_read() {
        let cluster = Attempt {
            cluster: true,
            ..Attempt::default()
        };
        let reused = Attempt {
            reused: true,
            ..cluster
        };
        let cases = [
            // A reused connection that the server closed gets one retry on a new connection.
            (io(std::io::ErrorKind::BrokenPipe), reused, Step::Fresh),
            // On a new connection, the same error means the node is down.
            (
                io(std::io::ErrorKind::BrokenPipe),
                Attempt {
                    fresh: true,
                    ..reused
                },
                Step::NextSeed,
            ),
            (
                io(std::io::ErrorKind::ConnectionRefused),
                cluster,
                Step::NextSeed,
            ),
            (
                reply("MOVED 1 10.0.0.2:7292"),
                cluster,
                Step::Redirect(node("10.0.0.2", 7292)),
            ),
            // A second redirect, or an `ASK`, means the slot is moving.
            (
                reply("MOVED 1 10.0.0.2:7292"),
                Attempt {
                    redirected: true,
                    ..cluster
                },
                Step::NoWake,
            ),
            (reply("ASK 1 10.0.0.2:7292"), cluster, Step::NoWake),
            // A standalone server never redirects, so a redirect there is an error.
            (
                reply("MOVED 1 10.0.0.2:7292"),
                Attempt::default(),
                Step::Fail,
            ),
            (reply("WRONGTYPE not a stream"), cluster, Step::Fail),
        ];
        for (error, attempt, step) in cases {
            assert_eq!(classify(&error, attempt), step, "{error} {attempt:?}");
        }
    }

    #[test]
    fn a_slot_entry_goes_only_while_it_names_the_dead_node() {
        let cache = SlotCache::default();
        cache.learn(1, node("10.0.0.2", 7292));
        cache.forget(Some(1), &node("10.0.0.3", 7293));
        assert_eq!(cache.get(Some(1)), Some(node("10.0.0.2", 7292)));
        cache.forget(Some(1), &node("10.0.0.2", 7292));
        assert_eq!(cache.get(Some(1)), None);
        assert_eq!(cache.get(None), None);
    }

    #[test]
    fn an_endpoint_needs_a_url() {
        assert!(Endpoint::parse(&[], false, Duration::from_secs(1)).is_err());
        assert!(Endpoint::parse(&[], true, Duration::from_secs(1)).is_err());
    }

    #[tokio::test]
    async fn a_cached_slot_on_a_dead_node_goes_back_to_the_seed() {
        let Some(url) = crate::server::server_url("STREAMS_REDIS_URL") else {
            return;
        };
        // A standalone server answers every slot, so the seed serves the read once the dead
        // node is dropped.
        let pool = BlockingPool::new(endpoint(&[&url], true), 1);
        let key = "test:{dead-node}:log";
        pool.slots.learn(slot(key), node("127.0.0.1", dead_port()));
        let deadline = Instant::now() + Duration::from_secs(2);
        let answer = pool.query(key, deadline, |_| redis::cmd("PING")).await;
        assert_eq!(
            answer.unwrap(),
            Some(Value::SimpleString("PONG".to_string()))
        );
        assert_eq!(pool.slots.get(Some(slot(key))), None);
    }

    /// A port that nothing listens on.
    fn dead_port() -> u16 {
        let dead = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        dead.local_addr().unwrap().port()
    }

    /// The URL of a server that gives every command a nil array.
    fn nil_server() -> String {
        use std::io::Write;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            for mut stream in listener.incoming().flatten() {
                std::thread::spawn(move || {
                    let mut reader = stream.try_clone().unwrap();
                    let mut parser = redis::Parser::new();
                    while parser.parse_value(&mut reader).is_ok()
                        && stream.write_all(b"*-1\r\n").is_ok()
                    {}
                });
            }
        });
        format!("redis://{address}")
    }

    #[tokio::test]
    async fn the_block_time_starts_when_the_read_has_a_connection() {
        let pool = Arc::new(BlockingPool::new(endpoint(&[&nil_server()], false), 1));
        let start = Instant::now();
        let seed = Node::of(&pool.endpoint.seeds[0]);
        let mut held = pool
            .lease(&seed, start + Duration::from_secs(1), false)
            .await
            .unwrap()
            .unwrap();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            held.keep();
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

    #[tokio::test]
    async fn a_lease_returns_its_connection_only_when_kept() {
        let pool: Pool<u8> = Pool::new(1, Vec::new());
        let until = || Instant::now() + Duration::from_secs(1);
        let mut kept = pool
            .lease(until(), false, || async { Ok(1) })
            .await
            .unwrap()
            .unwrap();
        kept.keep();
        drop(kept);
        let dropped = pool
            .lease(until(), false, || async { Ok(2) })
            .await
            .unwrap()
            .unwrap();
        assert!(dropped.reused);
        drop(dropped);
        // The lease that wasn't kept took its connection with it.
        let next = pool
            .lease(until(), false, || async { Ok(3) })
            .await
            .unwrap()
            .unwrap();
        assert!(!next.reused);
    }

    #[test]
    fn another_node_keeps_the_seed_credentials() {
        let seed = "redis://user:secret@127.0.0.1:7291/0"
            .into_connection_info()
            .unwrap();
        let other = node("10.0.0.2", 7292).info(&seed);
        assert_eq!(Node::of(&other), node("10.0.0.2", 7292));
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
