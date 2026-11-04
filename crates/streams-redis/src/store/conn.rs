//! The connections of the Redis Store. A blocked `XREAD` or `WAIT` stops all later commands on its
//! connection, so each one gets a dedicated connection, and the short calls share one.

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
    num::NonZeroUsize,
    ops::RangeInclusive,
    path::PathBuf,
    str::FromStr,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::Duration,
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Instant,
};

/// How long a blocking read waits for a free connection. Then it returns with no wake, so a read
/// over the pool's size polls about once a second.
const PERMIT_WAIT: Duration = Duration::from_secs(1);

/// How long a blocking read waits, with no wake, while the cluster migrates its slot. So such a
/// read polls at this rate until the migration ends.
const RESHARD_WAIT: Duration = Duration::from_millis(100);

/// The most writes with `WAIT` that one Store runs at once, so its connections per node stay few.
const WAIT_CONNECTIONS: NonZeroUsize = match NonZeroUsize::new(8) {
    Some(count) => count,
    None => panic!("the count is above zero"),
};

/// Where the Store's Redis is, parsed once from the URLs of the options.
pub(crate) struct Endpoint {
    topology: Topology,
    response_timeout: Duration,
}

/// One server, or the seeds of a cluster. Each kind has a first URL, since every node gets its
/// credentials and TLS.
enum Topology {
    Standalone(Seed),
    Cluster { first: Seed, rest: Vec<Seed> },
}

/// A URL of the options, and the node at that URL.
struct Seed {
    info: ConnectionInfo,
    node: Node,
}

impl Seed {
    fn parse(url: &str) -> RedisResult<Self> {
        let info = url.into_connection_info()?;
        let node = Node::of(&info)?;
        Ok(Self { info, node })
    }
}

impl Endpoint {
    pub(crate) fn parse(
        urls: &[String],
        cluster: bool,
        response_timeout: Duration,
    ) -> RedisResult<Self> {
        let mut seeds = urls.iter().map(|url| Seed::parse(url));
        let Some(first) = seeds.next().transpose()? else {
            return Err(RedisError::from((
                ErrorKind::InvalidClientConfig,
                "no Redis URL",
            )));
        };
        let rest = seeds.collect::<RedisResult<Vec<_>>>()?;
        let topology = match (cluster, rest.is_empty()) {
            (true, _) => Topology::Cluster { first, rest },
            (false, true) => Topology::Standalone(first),
            // A standalone Store connects to one server, so the other URLs would have no effect.
            (false, false) => {
                return Err(RedisError::from((
                    ErrorKind::InvalidClientConfig,
                    "a standalone Redis takes one URL, and a cluster's seeds need `cluster`",
                )));
            }
        };
        Ok(Self {
            topology,
            response_timeout,
        })
    }

    fn cluster(&self) -> bool {
        matches!(self.topology, Topology::Cluster { .. })
    }

    /// The server, or the cluster seed whose credentials and TLS every node gets.
    fn first(&self) -> &Seed {
        match &self.topology {
            Topology::Standalone(first) | Topology::Cluster { first, .. } => first,
        }
    }

    /// The server, or each seed of the cluster, with the first one first.
    fn seeds(&self) -> impl Iterator<Item = &Seed> {
        let rest = match &self.topology {
            Topology::Standalone(_) => &[][..],
            Topology::Cluster { rest, .. } => rest.as_slice(),
        };
        std::iter::once(self.first()).chain(rest)
    }

    /// Opens a connection for short calls, which the client reconnects when it drops.
    pub(crate) async fn open(&self) -> RedisResult<SharedConnection> {
        self.open_with(&[]).await
    }

    /// Opens a connection for short calls. A cluster client also gets the `live` nodes as seeds,
    /// since after a failover every configured seed can be down.
    async fn open_with(&self, live: &[Node]) -> RedisResult<SharedConnection> {
        match &self.topology {
            Topology::Cluster { first, .. } => {
                let live = live.iter().map(|node| node.info(&first.info));
                let seeds = self.seeds().map(|seed| seed.info.clone()).chain(live);
                let client = ClusterClient::builder(seeds)
                    .response_timeout(self.response_timeout)
                    .build()?;
                Ok(SharedConnection::Cluster(
                    client.get_async_connection().await?,
                ))
            }
            Topology::Standalone(server) => {
                let client = Client::open(server.info.clone())?;
                let config = ConnectionManagerConfig::new()
                    .set_response_timeout(Some(self.response_timeout));
                Ok(SharedConnection::Single(
                    client.get_connection_manager_with_config(config).await?,
                ))
            }
        }
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
    /// Runs `cmd` on the server, or on each primary of a cluster, since each primary has its own
    /// keys and settings. A cluster's reply comes with the node that sent it.
    pub(crate) async fn on_each_primary(
        &self,
        cmd: Cmd,
    ) -> RedisResult<Vec<(Option<String>, Value)>> {
        match self.clone() {
            SharedConnection::Single(mut conn) => {
                Ok(vec![(None, cmd.query_async(&mut conn).await?)])
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
                            Ok((Some(node), reply.extract_error()?))
                        })
                        .collect(),
                    other => Ok(vec![(None, other.extract_error()?)]),
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
    fn new(size: NonZeroUsize, idle: Vec<C>) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(size.get())),
            idle: Arc::new(Mutex::new(idle)),
        }
    }

    /// Takes an idle connection, or opens one. Returns `None` if `give_up` ends first, so the
    /// caller sets the wait and the pool reads no clock.
    async fn lease<F>(
        &self,
        give_up: impl Future<Output = ()>,
        open: impl FnOnce() -> F,
    ) -> RedisResult<Option<Lease<C>>>
    where
        F: Future<Output = RedisResult<C>>,
    {
        let permit = tokio::select! {
            // Take a free connection even when `give_up` is also ready.
            biased;
            permit = self.permits.clone().acquire_owned() => permit,
            () = give_up => return Ok(None),
        };
        let Ok(permit) = permit else {
            return Ok(None);
        };
        let idle = lock(&self.idle).pop();
        let reused = idle.is_some();
        let conn = match idle {
            Some(conn) => conn,
            None => open().await?,
        };
        Ok(Some(Lease {
            conn,
            reused,
            idle: self.idle.clone(),
            _permit: permit,
        }))
    }
}

/// A connection from a [Pool]. A dropped call doesn't stop its command on the server, and the next
/// call would wait behind it. So a drop closes the connection, and only [Lease::settle] returns it.
pub(crate) struct Lease<C> {
    conn: C,
    /// Taken idle from the pool, so the server may have closed it since its last call.
    reused: bool,
    idle: Arc<Mutex<Vec<C>>>,
    _permit: OwnedSemaphorePermit,
}

impl<C> Lease<C> {
    pub(crate) fn conn(&mut self) -> &mut C {
        &mut self.conn
    }

    /// Returns the connection to the pool after a whole reply, which is an answer or a server
    /// error. A wire error closes it.
    pub(crate) fn settle<T>(self, result: RedisResult<T>) -> RedisResult<T> {
        let Lease {
            conn,
            reused,
            idle,
            _permit,
        } = self;
        match &result {
            Err(error) if error.code().is_none() => {
                // A server restart also closed the other idle connections.
                if reused && stale(error) {
                    lock(&idle).clear();
                }
            }
            // Push it back before the permit drops, so the next lease finds it.
            _ => lock(&idle).push(conn),
        }
        result
    }
}

/// Each change under these locks is a single push, pop or insert, so a poisoned lock still guards
/// valid data.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Whether the connection itself failed, as when the server closed it. A timeout only shows that
/// the server is slow, so the idle connections may still be good.
fn stale(error: &RedisError) -> bool {
    !error.is_timeout() && (error.is_io_error() || error.is_connection_dropped())
}

/// The connections of the writes with `WAIT`.
pub(crate) struct WaitPool {
    endpoint: Arc<Endpoint>,
    /// The shared connection of a cluster, which knows the live nodes.
    cluster: Option<ClusterConnection>,
    pool: Pool<SharedConnection>,
}

impl WaitPool {
    /// The pool starts with `first`, so a bad URL fails at connect.
    pub(crate) fn new(
        first: SharedConnection,
        endpoint: Arc<Endpoint>,
        cluster: Option<ClusterConnection>,
    ) -> Self {
        Self {
            endpoint,
            cluster,
            pool: Pool::new(WAIT_CONNECTIONS, vec![first]),
        }
    }

    /// Takes an idle connection, or opens one. Returns `None` when every connection stays busy
    /// for the response timeout.
    pub(crate) async fn take(&self) -> RedisResult<Option<Lease<SharedConnection>>> {
        let give_up = tokio::time::sleep(self.endpoint.response_timeout);
        let open = || async {
            let live: Vec<Node> = match &self.cluster {
                Some(cluster) => primaries(cluster)
                    .await
                    .into_iter()
                    .map(|(_, node)| node)
                    .collect(),
                None => Vec::new(),
            };
            self.endpoint.open_with(&live).await
        };
        self.pool.lease(give_up, open).await
    }
}

/// The primary of each slot range, from a live node of the cluster. An empty list when the
/// cluster doesn't answer, so the caller falls back to the seeds.
async fn primaries(cluster: &ClusterConnection) -> Vec<(RangeInclusive<u16>, Node)> {
    let reply = redis::cmd("CLUSTER")
        .arg("SLOTS")
        .query_async::<Value>(&mut cluster.clone())
        .await;
    reply.map(primaries_of).unwrap_or_default()
}

fn primaries_of(reply: Value) -> Vec<(RangeInclusive<u16>, Node)> {
    let Value::Array(ranges) = reply else {
        return Vec::new();
    };
    let range = |range: Value| {
        let Value::Array(range) = range else {
            return None;
        };
        let [
            Value::Int(start),
            Value::Int(end),
            Value::Array(primary),
            ..,
        ] = range.as_slice()
        else {
            return None;
        };
        let [host, Value::Int(port), ..] = primary.as_slice() else {
            return None;
        };
        let host = match host {
            Value::BulkString(host) => String::from_utf8(host.clone()).ok()?,
            Value::SimpleString(host) => host.clone(),
            _ => return None,
        };
        // An empty host is the node that answered, and `?` is unknown, so neither one is usable.
        if host.is_empty() || host == "?" {
            return None;
        }
        let node = Node::Tcp {
            host,
            port: u16::try_from(*port).ok()?,
        };
        Some((
            u16::try_from(*start).ok()?..=u16::try_from(*end).ok()?,
            node,
        ))
    };
    ranges.into_iter().filter_map(range).collect()
}

/// A Redis node from a seed or a redirect. A standalone server can also be a local socket.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Node {
    Tcp { host: String, port: u16 },
    Unix(PathBuf),
}

impl Node {
    fn of(info: &ConnectionInfo) -> RedisResult<Self> {
        match info.addr() {
            ConnectionAddr::Tcp(host, port) | ConnectionAddr::TcpTls { host, port, .. } => {
                Ok(Self::Tcp {
                    host: host.clone(),
                    port: *port,
                })
            }
            ConnectionAddr::Unix(path) => Ok(Self::Unix(path.clone())),
            other => Err(RedisError::from((
                ErrorKind::InvalidClientConfig,
                "the Store can't connect to this kind of address",
                format!("{other:?}"),
            ))),
        }
    }

    /// The seed's connection settings, with its credentials and TLS, for this node.
    fn info(&self, seed: &ConnectionInfo) -> ConnectionInfo {
        let addr = match (self, seed.addr()) {
            (
                Self::Tcp { host, port },
                ConnectionAddr::TcpTls {
                    insecure,
                    tls_params,
                    ..
                },
            ) => ConnectionAddr::TcpTls {
                host: host.clone(),
                port: *port,
                insecure: *insecure,
                tls_params: tls_params.clone(),
            },
            (Self::Tcp { host, port }, _) => ConnectionAddr::Tcp(host.clone(), *port),
            (Self::Unix(path), _) => ConnectionAddr::Unix(path.clone()),
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
        Ok(Self::Tcp {
            host: host.to_string(),
            port: port.parse().map_err(|_| bad())?,
        })
    }
}

/// The node that serves each slot, from redirects. A cluster migrates slots rarely, and a stale
/// entry costs one redirect.
#[derive(Default)]
struct SlotCache(Mutex<HashMap<u16, Node>>);

impl SlotCache {
    fn get(&self, slot: Option<u16>) -> Option<Node> {
        lock(&self.0).get(&slot?).cloned()
    }

    fn learn(&self, slot: u16, node: Node) {
        lock(&self.0).insert(slot, node);
    }

    /// Removes the slot's entry if it still points at `node`. A failover can change a node's
    /// address permanently, and only a redirect would update the entry.
    fn forget(&self, slot: Option<u16>, node: &Node) {
        if let Some(slot) = slot {
            let mut slots = lock(&self.0);
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
    /// The read already used one redirect.
    redirected: bool,
}

/// What a blocking read does after a failed try.
#[derive(Debug, PartialEq, Eq)]
enum Step {
    /// Try the node again. The pool closed its idle connections with the dead one, so the try
    /// opens a new connection.
    Retry,
    /// The node is down, so try the next seed.
    NextSeed,
    /// Try the node that the redirect gives.
    Redirect(Node),
    /// The cluster is migrating the slot, so end the read with no wake.
    NoWake,
    /// Give the error to the caller.
    Fail,
}

/// The retry policy of a blocking read. It has no I/O, so a table test covers it.
fn classify(error: &RedisError, attempt: Attempt) -> Step {
    // A server restart closes every idle connection. So a retry on a new connection tells a
    // dead connection from a dead server.
    if attempt.reused && stale(error) {
        return Step::Retry;
    }
    if unreachable(error) {
        return Step::NextSeed;
    }
    if !attempt.cluster {
        return Step::Fail;
    }
    // One redirect is normal when the cache had no entry or a stale one. A second means the cluster
    // is migrating the slot.
    match redirect(error) {
        Some(target) if !attempt.redirected => Step::Redirect(target),
        _ if resharding(error) => Step::NoWake,
        _ => Step::Fail,
    }
}

/// Dedicated connections for blocking reads, a bounded number per node.
pub(crate) struct BlockingPool {
    endpoint: Arc<Endpoint>,
    /// The shared connection of a cluster, which knows each slot's live primary.
    cluster: Option<ClusterConnection>,
    per_node: NonZeroUsize,
    nodes: Mutex<HashMap<Node, Arc<NodePool>>>,
    slots: SlotCache,
}

struct NodePool {
    info: ConnectionInfo,
    pool: Pool<MultiplexedConnection>,
}

impl BlockingPool {
    pub(crate) fn new(
        endpoint: Arc<Endpoint>,
        per_node: NonZeroUsize,
        cluster: Option<ClusterConnection>,
    ) -> Self {
        Self {
            endpoint,
            cluster,
            per_node,
            nodes: Mutex::default(),
            slots: SlotCache::default(),
        }
    }

    /// Waits for an entry after the entry id `after` in the log at `key`. With no free connection,
    /// or during a slot migration, it returns `false` early, and the caller reads the Stream again.
    pub(crate) async fn wait_after(
        &self,
        key: &str,
        after: &str,
        deadline: Instant,
    ) -> RedisResult<bool> {
        let xread = |wait: Duration| {
            let mut xread = redis::cmd("XREAD");
            xread
                .arg("COUNT")
                .arg(1)
                .arg("BLOCK")
                // `BLOCK 0` waits with no end, so a read at its deadline blocks for 1 ms.
                .arg(wait.as_millis().max(1))
                .arg("STREAMS")
                .arg(key)
                .arg(after);
            xread
        };
        let reply = self.query(key, deadline, xread).await?;
        Ok(
            matches!(reply, Some(Value::Array(ref keys)) if !keys.is_empty())
                || matches!(reply, Some(Value::Map(ref keys)) if !keys.is_empty()),
        )
    }

    /// Runs the command that `cmd` builds for the time left to `deadline`, which can be zero.
    /// Returns `None` if no connection was free in time, or if the key's slot is migrating.
    async fn query(
        &self,
        key: &str,
        deadline: Instant,
        cmd: impl Fn(Duration) -> Cmd,
    ) -> RedisResult<Option<Value>> {
        let slot = self.endpoint.cluster().then(|| slot(key));
        let mut node = match (self.slots.get(slot), slot) {
            (Some(node), _) => Some(node),
            (None, Some(slot)) => self.live_primary(slot).await,
            (None, None) => None,
        }
        .unwrap_or_else(|| self.endpoint.first().node.clone());
        // When a node is down, try each other node once. After a failover the live topology has
        // the slot's new primary, and a seed that is down may have a replica that took over.
        let mut fallbacks: Option<std::vec::IntoIter<Node>> = None;
        let mut attempt = Attempt {
            cluster: slot.is_some(),
            ..Attempt::default()
        };
        loop {
            // Each node that is down costs a connect timeout, so the seed list can take longer than
            // the read.
            if deadline <= Instant::now() {
                return Ok(None);
            }
            let error = match self.lease(&node, deadline).await {
                Ok(None) => return Ok(None),
                Ok(Some(mut lease)) => {
                    attempt.reused = lease.reused;
                    // The lease can wait up to [PERMIT_WAIT], so the block time comes from the
                    // deadline.
                    let wait = deadline.saturating_duration_since(Instant::now());
                    let answer = tokio::time::timeout_at(
                        deadline + self.endpoint.response_timeout,
                        cmd(wait).query_async::<Value>(lease.conn()),
                    )
                    .await;
                    // A server that stopped without a reset never replies, and the read must still
                    // end. The lease then drops, which closes the connection.
                    let Ok(result) = answer else {
                        return Err(RedisError::from((
                            ErrorKind::Io,
                            "Redis did not answer a blocked read by its deadline",
                        )));
                    };
                    match lease.settle(result) {
                        Ok(value) => return Ok(Some(value)),
                        Err(error) => error,
                    }
                }
                Err(error) => {
                    attempt.reused = false;
                    error
                }
            };
            match classify(&error, attempt) {
                Step::Retry => {}
                Step::NextSeed => {
                    self.slots.forget(slot, &node);
                    if fallbacks.is_none() {
                        let live = match slot {
                            Some(slot) => self.live_primary(slot).await,
                            None => None,
                        };
                        let seeds = self.endpoint.seeds().map(|seed| seed.node.clone());
                        fallbacks = Some(
                            live.into_iter()
                                .chain(seeds)
                                .collect::<Vec<_>>()
                                .into_iter(),
                        );
                    }
                    let mut nodes = fallbacks.iter_mut().flatten();
                    node = nodes.find(|other| *other != node).ok_or(error)?;
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

    /// The primary that serves `slot` now, as the shared connection of a cluster sees it.
    async fn live_primary(&self, slot: u16) -> Option<Node> {
        let primaries = primaries(self.cluster.as_ref()?).await;
        let (_, node) = primaries
            .into_iter()
            .find(|(range, _)| range.contains(&slot))?;
        self.slots.learn(slot, node.clone());
        Some(node)
    }

    async fn lease(
        &self,
        node: &Node,
        deadline: Instant,
    ) -> RedisResult<Option<Lease<MultiplexedConnection>>> {
        let node = self.node(node);
        let give_up = tokio::time::sleep_until(deadline.min(Instant::now() + PERMIT_WAIT));
        let open = || async {
            // The read bounds the wait itself, so the connection sets no response timeout.
            let config = AsyncConnectionConfig::new().set_response_timeout(None);
            Client::open(node.info.clone())?
                .get_multiplexed_async_connection_with_config(&config)
                .await
        };
        node.pool.lease(give_up, open).await
    }

    fn node(&self, node: &Node) -> Arc<NodePool> {
        lock(&self.nodes)
            .entry(node.clone())
            .or_insert_with(|| {
                Arc::new(NodePool {
                    info: node.info(&self.endpoint.first().info),
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

/// Whether the cluster is migrating the slot of the command's key. An `ASK` target serves the key
/// only after `ASKING`, and the slot can migrate again before the next read.
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
        Node::Tcp {
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
        // An `ASK` target serves the slot only for one command after `ASKING`.
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
            // A reused connection that the server closed gets a retry on a new connection.
            (io(std::io::ErrorKind::BrokenPipe), reused, Step::Retry),
            // On a new connection, the same error means the node is down.
            (io(std::io::ErrorKind::BrokenPipe), cluster, Step::NextSeed),
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
    fn an_endpoint_needs_a_url_and_a_standalone_one_takes_only_one() {
        let parse = |urls: &[&str], cluster| {
            let urls: Vec<String> = urls.iter().map(|url| url.to_string()).collect();
            Endpoint::parse(&urls, cluster, Duration::from_secs(1))
        };
        assert!(parse(&[], false).is_err());
        assert!(parse(&[], true).is_err());
        let two = ["redis://127.0.0.1:1", "redis://127.0.0.1:2"];
        assert!(parse(&two, false).is_err());
        assert_eq!(parse(&two, true).unwrap().seeds().count(), 2);
    }

    #[tokio::test]
    #[ignore = "needs Redis"]
    async fn a_cached_slot_on_a_dead_node_goes_back_to_the_seed() {
        let url = crate::testing::standalone_url();
        // A standalone server answers every slot, so the seed serves the read once the dead
        // node is dropped.
        let pool = BlockingPool::new(endpoint(&[&url], true), NonZeroUsize::MIN, None);
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

    #[test]
    fn the_cluster_slots_reply_gives_each_range_its_primary() {
        let reply = redis::parse_redis_value(
            b"*2\r\n*3\r\n:0\r\n:5460\r\n*3\r\n$9\r\n127.0.0.1\r\n:7001\r\n$2\r\nid\r\n\
              *3\r\n:5461\r\n:16383\r\n*2\r\n$0\r\n\r\n:7002\r\n",
        )
        .unwrap();
        // The second range has no usable host.
        assert_eq!(
            primaries_of(reply),
            vec![(0..=5460, node("127.0.0.1", 7001))]
        );
    }

    #[tokio::test]
    #[ignore = "needs a Redis cluster"]
    async fn a_cluster_read_and_a_wait_connection_work_when_every_seed_is_down() {
        let live = endpoint(&[&crate::testing::cluster_url()], true);
        let SharedConnection::Cluster(cluster) = live.open().await.unwrap() else {
            panic!("a cluster endpoint opens a cluster connection");
        };
        let dead = endpoint(&[&format!("redis://127.0.0.1:{}", dead_port())], true);
        let key = "test:{dead-seeds}:log";
        let _: String = redis::cmd("XADD")
            .arg(key)
            .arg("*")
            .arg("x")
            .arg("y")
            .query_async(&mut cluster.clone())
            .await
            .unwrap();
        let reads = BlockingPool::new(dead.clone(), NonZeroUsize::MIN, Some(cluster.clone()));
        let deadline = Instant::now() + Duration::from_secs(5);
        assert!(reads.wait_after(key, "0-0", deadline).await.unwrap());
        // The second lease finds no idle connection, so it opens one.
        let first = SharedConnection::Cluster(cluster.clone());
        let waits = WaitPool::new(first, dead, Some(cluster));
        let (one, two) = tokio::join!(waits.take(), waits.take());
        assert!(one.unwrap().is_some() && two.unwrap().is_some());
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

    // The paused clock advances only on `advance`, or to the next timer when all tasks wait. The
    // connections open before the pause, since the clock would jump past a connect.
    #[tokio::test]
    async fn the_block_time_starts_when_the_read_has_a_connection() {
        let pool = BlockingPool::new(endpoint(&[&nil_server()], false), NonZeroUsize::MIN, None);
        let seed = pool.endpoint.first().node.clone();
        let held = pool
            .lease(&seed, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap()
            .unwrap();
        tokio::time::pause();
        let deadline = Instant::now() + Duration::from_millis(500);
        let blocked = Mutex::new(None);
        let read = pool.query("s", deadline, |wait| {
            *blocked.lock().unwrap() = Some(wait);
            redis::cmd("XREAD")
        });
        let free = async {
            tokio::time::advance(Duration::from_millis(300)).await;
            held.settle(Ok(())).unwrap();
        };
        // The paused clock can pass the read's deadline before the answer comes, so only the
        // block time counts.
        let _ = tokio::join!(read, free);
        assert_eq!(*blocked.lock().unwrap(), Some(Duration::from_millis(200)));
    }

    #[tokio::test]
    async fn a_read_with_no_free_connection_ends_after_the_permit_wait() {
        let pool = BlockingPool::new(endpoint(&[&nil_server()], false), NonZeroUsize::MIN, None);
        let seed = pool.endpoint.first().node.clone();
        let deadline = || Instant::now() + Duration::from_secs(10);
        let _held = pool.lease(&seed, deadline()).await.unwrap().unwrap();
        tokio::time::pause();
        let start = Instant::now();
        let answer = pool.query("s", deadline(), |_| redis::cmd("XREAD")).await;
        assert_eq!(answer.unwrap(), None);
        let waited = start.elapsed();
        assert!(
            waited >= PERMIT_WAIT && waited < Duration::from_secs(10),
            "{waited:?}"
        );
    }

    async fn lease(pool: &Pool<u8>, open: u8) -> Lease<u8> {
        pool.lease(std::future::pending(), || async move { Ok(open) })
            .await
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn a_lease_gives_its_connection_back_only_after_a_whole_reply() {
        let pool = Pool::new(NonZeroUsize::MIN, Vec::new());
        // A server error is a whole reply.
        let first = lease(&pool, 1).await;
        assert!(first.settle::<()>(Err(reply("ERR no"))).is_err());
        let again = lease(&pool, 2).await;
        assert_eq!((again.conn, again.reused), (1, true));
        assert!(again.settle(Ok(())).is_ok());
        // After a wire error the connection closes.
        let lost = lease(&pool, 3).await;
        assert!(
            lost.settle::<()>(Err(io(std::io::ErrorKind::TimedOut)))
                .is_err()
        );
        let next = lease(&pool, 4).await;
        assert_eq!((next.conn, next.reused), (4, false));
        // A lease that drops, as a cancelled call does, closes its connection too.
        drop(next);
        assert_eq!(lease(&pool, 5).await.conn, 5);
    }

    #[tokio::test]
    async fn a_dead_reused_connection_closes_the_idle_ones_but_a_slow_one_does_not() {
        let pool = Pool::new(NonZeroUsize::new(3).unwrap(), vec![1, 2, 3]);
        let slow = lease(&pool, 0).await;
        assert!(
            slow.settle::<()>(Err(io(std::io::ErrorKind::TimedOut)))
                .is_err()
        );
        let dead = lease(&pool, 0).await;
        assert_eq!((dead.conn, dead.reused), (2, true));
        assert!(
            dead.settle::<()>(Err(io(std::io::ErrorKind::BrokenPipe)))
                .is_err()
        );
        let next = lease(&pool, 4).await;
        assert_eq!((next.conn, next.reused), (4, false));
    }

    #[tokio::test]
    async fn a_lease_gives_up_only_when_no_connection_is_free() {
        let pool = Pool::new(NonZeroUsize::MIN, Vec::new());
        let give_up = || std::future::ready(());
        let open = || async { Ok(1) };
        let held = pool.lease(give_up(), open).await.unwrap();
        assert!(held.is_some());
        assert!(pool.lease(give_up(), open).await.unwrap().is_none());
    }

    #[test]
    fn another_node_keeps_the_seed_credentials() {
        let seed = "redis://user:secret@127.0.0.1:7291/0"
            .into_connection_info()
            .unwrap();
        let other = node("10.0.0.2", 7292).info(&seed);
        assert_eq!(Node::of(&other).unwrap(), node("10.0.0.2", 7292));
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
