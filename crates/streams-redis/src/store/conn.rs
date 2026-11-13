//! The connections of the Redis Store. A blocked `XREAD` or `WAIT` stops all later commands on its
//! connection, so each one gets a dedicated connection, and the short calls share one.

use crate::{
    RedisStoreOptions, Topology,
    slots::{SlotNode, SlotRange},
};
use parking_lot::Mutex;
use redis::{
    AsyncConnectionConfig, Client, Cmd, ConnectionAddr, ConnectionInfo, ErrorKind, FromRedisValue,
    IntoConnectionInfo, Pipeline, RedisError, RedisFuture, RedisResult, ServerErrorKind, Value,
    aio::{ConnectionLike, ConnectionManager, ConnectionManagerConfig, MultiplexedConnection},
    cluster::ClusterClient,
    cluster_async::ClusterConnection,
    cluster_routing::{MultipleNodeRoutingInfo, RoutingInfo},
    streams::{StreamReadOptions, StreamReadReply},
};
use std::{
    collections::HashMap, fmt, future::Future, num::NonZeroUsize, ops::RangeInclusive,
    path::PathBuf, str::FromStr, sync::Arc, time::Duration,
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Instant,
};
use url::Url;

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
    topology: Topology<Seed>,
    response_timeout: Duration,
}

/// A URL of the options, and the node at that URL.
struct Seed {
    info: ConnectionInfo,
    node: Node,
}

impl TryFrom<&Url> for Seed {
    type Error = RedisError;

    fn try_from(url: &Url) -> RedisResult<Self> {
        let info = url.clone().into_connection_info()?;
        let node = Node::try_from(&info)?;
        Ok(Self { info, node })
    }
}

/// The endpoint of the options' URLs and timeout.
impl TryFrom<&RedisStoreOptions> for Endpoint {
    type Error = RedisError;

    fn try_from(options: &RedisStoreOptions) -> RedisResult<Self> {
        Ok(Self {
            topology: options.topology.try_map(|url| Seed::try_from(url))?,
            response_timeout: options.response_timeout,
        })
    }
}

impl Endpoint {
    /// How a blocking read of `key` finds its first node.
    fn route(&self, key: &str) -> Route {
        match self.topology {
            Topology::Standalone(_) => Route::Standalone,
            Topology::Cluster { .. } => Route::Slot(slot(key)),
        }
    }

    /// The server, or the cluster seed whose credentials and TLS every node gets.
    fn first(&self) -> &Seed {
        match &self.topology {
            Topology::Standalone(first) | Topology::Cluster { first, .. } => first,
        }
    }

    /// The server, or each seed of the cluster, with the first one first.
    fn seeds(&self) -> impl Iterator<Item = &Seed> {
        self.topology.servers()
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

// A `MOVED` means another thing on each topology. The cluster client follows redirects, and one
// that it gives up on is a passing state. A standalone server that redirects is a cluster node,
// so the options are wrong.
impl ConnectionLike for SharedConnection {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        match self {
            SharedConnection::Single(conn) => Box::pin(async move {
                let value = conn.req_packed_command(cmd).await.map_err(misrouted)?;
                not_moved(&value)?;
                Ok(value)
            }),
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
            SharedConnection::Single(conn) => Box::pin(async move {
                let values = conn
                    .req_packed_commands(pipeline, offset, count)
                    .await
                    .map_err(misrouted)?;
                values.iter().try_for_each(not_moved)?;
                Ok(values)
            }),
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

/// A standalone server's reply that is a `MOVED` becomes a config error.
fn not_moved(value: &Value) -> RedisResult<()> {
    match value {
        Value::ServerError(error) if error.kind() == Some(ServerErrorKind::Moved) => {
            Err(misrouted(error.clone().into()))
        }
        _ => Ok(()),
    }
}

/// A `MOVED` error of a standalone server as a config error. Other errors stay as they are.
fn misrouted(error: RedisError) -> RedisError {
    if error.kind() != ErrorKind::Server(ServerErrorKind::Moved) {
        return error;
    }
    RedisError::from((
        ErrorKind::InvalidClientConfig,
        "A Redis Cluster node answered, so set `cluster`",
        error.to_string(),
    ))
}

/// The server that sent a reply. Each primary of a cluster has its own keys and settings.
#[derive(Clone, Debug)]
pub(crate) enum Origin {
    Server,
    Primary(String),
}

impl fmt::Display for Origin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Origin::Server => f.write_str("the server"),
            Origin::Primary(node) => f.write_str(node),
        }
    }
}

impl SharedConnection {
    /// Runs `cmd` on the server, or on each primary of a cluster, and decodes each reply.
    pub(crate) async fn on_each_primary<T: FromRedisValue>(
        &self,
        cmd: Cmd,
    ) -> RedisResult<Vec<(Origin, T)>> {
        match self.clone() {
            SharedConnection::Single(mut conn) => {
                Ok(vec![(Origin::Server, cmd.query_async(&mut conn).await?)])
            }
            SharedConnection::Cluster(mut conn) => {
                let routing = RoutingInfo::MultiNode((MultipleNodeRoutingInfo::AllMasters, None));
                // The client gives each node's refusal as that node's reply, not as an error.
                match conn.route_command(cmd, routing).await? {
                    Value::Map(replies) => replies
                        .into_iter()
                        .map(|(node, reply)| {
                            let node = String::from_redis_value(node)?;
                            Ok((
                                Origin::Primary(node),
                                T::from_redis_value(reply.extract_error()?)?,
                            ))
                        })
                        .collect(),
                    // A reply that the client didn't split by node.
                    other => Ok(vec![(
                        Origin::Server,
                        T::from_redis_value(other.extract_error()?)?,
                    )]),
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

/// What [Pool::lease] gives.
pub(crate) enum Leased<C> {
    /// A connection that was idle in the pool.
    Idle(IdleLease<C>),
    /// A connection that the pool opened for this lease.
    New(Lease<C>),
    /// Every connection stayed busy until the caller gave up.
    Busy,
}

impl<C> Pool<C> {
    fn new(size: NonZeroUsize, idle: Vec<C>) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(size.get())),
            idle: Arc::new(Mutex::new(idle)),
        }
    }

    /// Takes an idle connection, or opens one. The caller's `give_up` ends the wait for a free
    /// connection, so the pool reads no clock.
    async fn lease<F>(
        &self,
        give_up: impl Future<Output = ()>,
        open: impl FnOnce() -> F,
    ) -> RedisResult<Leased<C>>
    where
        F: Future<Output = RedisResult<C>>,
    {
        let permit = tokio::select! {
            // Take a free connection even when `give_up` is also ready.
            biased;
            permit = self.permits.clone().acquire_owned() => permit,
            () = give_up => return Ok(Leased::Busy),
        };
        // Nothing closes the semaphore, so a failed acquire is a bug, not a busy pool.
        let permit = permit
            .map_err(|_| RedisError::from((ErrorKind::Client, "the connection pool is closed")))?;
        let permit = Permit {
            idle: self.idle.clone(),
            _permit: permit,
        };
        let idle = self.idle.lock().pop();
        Ok(match idle {
            Some(conn) => Leased::Idle(IdleLease(Held { conn, permit })),
            None => Leased::New(permit.open(open()).await?),
        })
    }
}

/// The right to one connection of a [Pool], with the idle list that the connection goes back to.
pub(crate) struct Permit<C> {
    idle: Arc<Mutex<Vec<C>>>,
    _permit: OwnedSemaphorePermit,
}

impl<C> Permit<C> {
    /// Opens a new connection with this permit.
    pub(crate) async fn open(
        self,
        open: impl Future<Output = RedisResult<C>>,
    ) -> RedisResult<Lease<C>> {
        Ok(Lease(Held {
            conn: open.await?,
            permit: self,
        }))
    }
}

/// A connection and its permit.
struct Held<C> {
    conn: C,
    permit: Permit<C>,
}

/// A connection from a [Pool]. A dropped call doesn't stop its command on the server, and the next
/// call would wait behind it. So a drop closes the connection, and only [Lease::settle] returns it.
pub(crate) struct Lease<C>(Held<C>);

impl<C> Lease<C> {
    pub(crate) fn conn(&mut self) -> &mut C {
        &mut self.0.conn
    }

    /// Returns the connection to the pool after a whole reply, which is an answer or a server
    /// error. A wire error closes it.
    pub(crate) fn settle<T>(self, result: RedisResult<T>) -> RedisResult<T> {
        let Held { conn, permit } = self.0;
        if !matches!(&result, Err(error) if error.code().is_none()) {
            // Push it back before the permit drops, so the next lease finds it.
            permit.idle.lock().push(conn);
        }
        result
    }
}

/// A connection that was idle in a [Pool]. The server may have closed it since its last call, so
/// its call can end with a permit for a new connection.
pub(crate) struct IdleLease<C>(Held<C>);

/// How a call on an [IdleLease] ended.
pub(crate) enum Settled<T, C> {
    /// The call's result, as [Lease::settle] gives it.
    Done(RedisResult<T>),
    /// The server closed the connection, and the other idle ones with it. The pool dropped them.
    /// A call that can run again opens a new connection with the permit, and that try tells a
    /// dead connection from a dead server. A call that must run at most once returns the error.
    Stale {
        error: RedisError,
        permit: Permit<C>,
    },
}

impl<C> IdleLease<C> {
    pub(crate) fn conn(&mut self) -> &mut C {
        &mut self.0.conn
    }

    pub(crate) fn settle<T>(self, result: RedisResult<T>) -> Settled<T, C> {
        match result {
            Err(error) if stale(&error) => {
                let Held { conn, permit } = self.0;
                drop(conn);
                permit.idle.lock().clear();
                Settled::Stale { error, permit }
            }
            result => Settled::Done(Lease(self.0).settle(result)),
        }
    }
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

    /// Takes an idle connection, or opens one. Gives [Leased::Busy] when every connection stays
    /// busy for the response timeout.
    pub(crate) async fn take(&self) -> RedisResult<Leased<SharedConnection>> {
        let give_up = tokio::time::sleep(self.endpoint.response_timeout);
        let open = || async {
            let live: Vec<Node> = match &self.cluster {
                Some(cluster) => primaries(cluster)
                    .await?
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

/// The primary of each slot range, from a live node of the cluster. A cluster that doesn't
/// answer gives none, so the caller falls back to the seeds. A refusal, such as from a user with no
/// `+cluster|slots`, is an error.
async fn primaries(cluster: &ClusterConnection) -> RedisResult<Vec<(RangeInclusive<u16>, Node)>> {
    let ranges = redis::cmd("CLUSTER")
        .arg("SLOTS")
        .query_async::<Vec<SlotRange>>(&mut cluster.clone())
        .await;
    match ranges {
        Ok(ranges) => Ok(ranges
            .into_iter()
            .filter_map(|range| Some((range.slots, Node::reachable(&range.primary)?)))
            .collect()),
        Err(error) if unreachable(&error) || error.is_timeout() => {
            tracing::debug!("The cluster gave no slot ranges: {error}");
            Ok(Vec::new())
        }
        Err(error) => Err(error),
    }
}

/// A Redis node from a seed or a redirect. A standalone server can also be a local socket.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Node {
    Tcp { host: String, port: u16 },
    Unix(PathBuf),
}

/// The node at the address of a URL.
impl TryFrom<&ConnectionInfo> for Node {
    type Error = RedisError;

    fn try_from(info: &ConnectionInfo) -> RedisResult<Self> {
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
}

impl Node {
    /// The node of a slot range. A NULL host is the endpoint that the caller asked, and an empty
    /// one is the node that answered. The cluster client doesn't say which node it asked, so
    /// neither is reachable, and nor is the unknown `?`. The caller still has the seeds.
    fn reachable(node: &SlotNode) -> Option<Self> {
        let host = node.host.as_deref()?;
        (!host.is_empty() && host != "?").then(|| Self::Tcp {
            host: host.to_string(),
            port: node.port,
        })
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
    fn get(&self, slot: u16) -> Option<Node> {
        self.0.lock().get(&slot).cloned()
    }

    fn learn(&self, slot: u16, node: Node) {
        self.0.lock().insert(slot, node);
    }

    /// Removes the slot's entry if it still points at `node`. A failover can change a node's
    /// address permanently, and only a redirect would update the entry.
    fn forget(&self, slot: u16, node: &Node) {
        let mut slots = self.0.lock();
        if slots.get(&slot) == Some(node) {
            slots.remove(&slot);
        }
    }
}

/// How a blocking read found the node of its try.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Route {
    /// The one server of a standalone Store.
    Standalone,
    /// A cluster node for the slot, from the slot cache, the live topology or the seeds.
    Slot(u16),
    /// The cluster node that a redirect gave for the slot.
    Redirected(u16),
}

impl Route {
    fn slot(self) -> Option<u16> {
        match self {
            Route::Standalone => None,
            Route::Slot(slot) | Route::Redirected(slot) => Some(slot),
        }
    }
}

/// What a blocking read does after a failed try.
#[derive(Debug, PartialEq, Eq)]
enum Step {
    /// The node is down, so try the next seed.
    NextSeed,
    /// Try the node that the redirect gives.
    Redirect(Node),
    /// The cluster is migrating the slot, so end the read with no wake.
    NoWake,
    /// Give the error to the caller.
    Fail,
}

/// The retry policy of a blocking read on `from`. It has no I/O, so a table test covers it.
fn classify(error: &RedisError, route: Route, from: &Node) -> Step {
    if unreachable(error) {
        return Step::NextSeed;
    }
    match (route, error.kind()) {
        // A standalone server never redirects, so a redirect there is an error.
        (Route::Standalone, _) => Step::Fail,
        // One redirect is normal when the cache had no entry or a stale one. A second means the
        // cluster is migrating the slot.
        (Route::Slot(_), ErrorKind::Server(ServerErrorKind::Moved)) => {
            match moved_to(error, from) {
                Ok(target) => Step::Redirect(target),
                Err(_) => Step::Fail,
            }
        }
        // An `ASK` target serves the key only after `ASKING`, and the slot can migrate again
        // before the next read.
        (_, ErrorKind::Server(ServerErrorKind::Ask | ServerErrorKind::Moved)) => Step::NoWake,
        _ => Step::Fail,
    }
}

/// A read that blocks until the log at `key` has an entry after `after`, the Stream closes, or
/// `deadline` passes.
struct BlockedRead<'a> {
    key: &'a str,
    /// The Stream's `:closed` key, which gets an entry when the Stream closes.
    closed: &'a str,
    after: &'a str,
    deadline: Instant,
}

impl BlockedRead<'_> {
    /// The `XREAD` for the time left to the deadline. A lease can wait up to [PERMIT_WAIT], so the
    /// block time comes from the deadline once the read has its connection.
    fn xread(&self) -> Cmd {
        let wait = self.deadline.saturating_duration_since(Instant::now());
        // `BLOCK 0` waits with no end, so a read at its deadline blocks for 1 ms.
        let block = usize::try_from(wait.as_millis().max(1)).unwrap_or(usize::MAX);
        let options = StreamReadOptions::default().count(1).block(block);
        // Any entry of the `:closed` key wakes the read, so a read of a closed Stream ends at once.
        Cmd::xread_options(&[self.key, self.closed], &[self.after, "0-0"], &options)
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

impl NodePool {
    /// Takes a connection. It waits for a free one up to [PERMIT_WAIT], or up to the deadline.
    async fn lease(&self, deadline: Instant) -> RedisResult<Leased<MultiplexedConnection>> {
        let give_up = tokio::time::sleep_until(deadline.min(Instant::now() + PERMIT_WAIT));
        self.pool.lease(give_up, || self.open()).await
    }

    async fn open(&self) -> RedisResult<MultiplexedConnection> {
        // The read bounds the wait itself, so the connection sets no response timeout.
        let config = AsyncConnectionConfig::new().set_response_timeout(None);
        Client::open(self.info.clone())?
            .get_multiplexed_async_connection_with_config(&config)
            .await
    }
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

    /// Waits for an entry after the entry id `after` in the log at `key`, or for the Stream's close
    /// on its `closed` key. Gives `false` when the read ends with neither, also early with no free
    /// connection or during a slot migration. Then the caller reads the Stream again.
    //
    // The pool works on raw keys, not on the Stream key layout, so the WAIT writes and the blocking
    // reads share it. It only reads the keys, so it borrows them.
    pub(crate) async fn wait_after(
        &self,
        key: &str,
        closed: &str,
        after: &str,
        deadline: Instant,
    ) -> RedisResult<bool> {
        let read = BlockedRead {
            key,
            closed,
            after,
            deadline,
        };
        let mut route = self.endpoint.route(key);
        let mut node = match route.slot() {
            Some(slot) => match self.slots.get(slot) {
                Some(node) => Some(node),
                None => self.live_primary(slot).await?,
            },
            None => None,
        }
        .unwrap_or_else(|| self.endpoint.first().node.clone());
        // When a node is down, try each other node once. After a failover the live topology has
        // the slot's new primary, and a seed that is down may have a replica that took over.
        let mut fallbacks: Option<std::vec::IntoIter<Node>> = None;
        loop {
            // Each node that is down costs a connect timeout, so the seed list can take longer than
            // the read.
            if deadline <= Instant::now() {
                return Ok(false);
            }
            let error = match self.try_node(&node, &read).await {
                Ok(woke) => return Ok(woke),
                Err(error) => error,
            };
            match classify(&error, route, &node) {
                Step::NextSeed => {
                    if let Some(slot) = route.slot() {
                        self.slots.forget(slot, &node);
                    }
                    if fallbacks.is_none() {
                        let live = match route.slot() {
                            Some(slot) => self.live_primary(slot).await?,
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
                    if let Route::Slot(slot) = route {
                        self.slots.learn(slot, target.clone());
                        route = Route::Redirected(slot);
                    }
                    node = target;
                }
                Step::NoWake => {
                    tokio::time::sleep_until(deadline.min(Instant::now() + RESHARD_WAIT)).await;
                    return Ok(false);
                }
                Step::Fail if matches!(route, Route::Standalone) => return Err(misrouted(error)),
                Step::Fail => return Err(error),
            }
        }
    }

    /// One try of the read on `node`. A connection from the idle list that the server closed gets
    /// one more try on a new connection.
    async fn try_node(&self, node: &Node, read: &BlockedRead<'_>) -> RedisResult<bool> {
        let node = self.node(node);
        let mut lease = match node.lease(read.deadline).await? {
            Leased::Busy => return Ok(false),
            Leased::New(lease) => lease,
            Leased::Idle(mut lease) => {
                let answer = self.run(lease.conn(), read).await?;
                match lease.settle(answer) {
                    Settled::Done(answer) => return answer,
                    Settled::Stale { permit, .. } => permit.open(node.open()).await?,
                }
            }
        };
        let answer = self.run(lease.conn(), read).await?;
        lease.settle(answer)
    }

    /// Runs the read on `conn`. The outer error is a read that Redis didn't answer in time. Then
    /// the lease drops, which closes the connection.
    async fn run(
        &self,
        conn: &mut MultiplexedConnection,
        read: &BlockedRead<'_>,
    ) -> RedisResult<RedisResult<bool>> {
        // A server that stopped without a reset never replies, and the read must still end. A read
        // that ends with no entry gives nil.
        let reply = tokio::time::timeout_at(
            read.deadline + self.endpoint.response_timeout,
            read.xread().query_async::<Option<StreamReadReply>>(conn),
        )
        .await
        .map_err(|_| {
            RedisError::from((
                ErrorKind::Io,
                "Redis did not answer a blocked read by its deadline",
            ))
        })?;
        Ok(reply.map(|reply| reply.is_some_and(|reply| !reply.keys.is_empty())))
    }

    /// The primary that serves `slot` now, as the shared connection of a cluster sees it.
    async fn live_primary(&self, slot: u16) -> RedisResult<Option<Node>> {
        let Some(cluster) = &self.cluster else {
            return Ok(None);
        };
        let node = primaries(cluster)
            .await?
            .into_iter()
            .find_map(|(range, node)| range.contains(&slot).then_some(node));
        if let Some(node) = &node {
            self.slots.learn(slot, node.clone());
        }
        Ok(node)
    }

    fn node(&self, node: &Node) -> Arc<NodePool> {
        self.nodes
            .lock()
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

/// The node of a `MOVED` redirect that `from` sent. A cluster with unknown endpoints gives an
/// empty host, which means the host of `from`. A `?` host is a hostname that the cluster doesn't
/// know, so no connect can reach it.
fn moved_to(error: &RedisError, from: &Node) -> RedisResult<Node> {
    let bad = |what| RedisError::from((ErrorKind::Parse, what));
    let (address, _) = error
        .redirect_node()
        .ok_or_else(|| bad("a redirect with no node"))?;
    match address.parse()? {
        Node::Tcp { host, port } if host.is_empty() => match from {
            Node::Tcp { host, .. } => Ok(Node::Tcp {
                host: host.clone(),
                port,
            }),
            Node::Unix(_) => Err(bad("a redirect with no host from a local socket")),
        },
        Node::Tcp { host, .. } if host == "?" => Err(bad("a redirect to an unknown hostname")),
        node => Ok(node),
    }
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

    fn standalone_at(url: &str) -> Topology<Url> {
        Topology::Standalone(url.parse().unwrap())
    }

    fn cluster_at(url: &str) -> Topology<Url> {
        Topology::Cluster {
            first: url.parse().unwrap(),
            rest: Vec::new(),
        }
    }

    fn endpoint(topology: Topology<Url>) -> Arc<Endpoint> {
        let options = RedisStoreOptions {
            response_timeout: Duration::from_secs(1),
            ..RedisStoreOptions::from(topology)
        };
        Arc::new(Endpoint::try_from(&options).unwrap())
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

    // A script names only its Stream's keys, so a cluster refuses it with `CROSSSLOT` when one
    // key falls outside the hash tag. The ids take the characters that a tag or a key uses.
    #[test]
    fn the_keys_of_a_stream_share_one_slot_for_any_id() {
        use temporalio_streams::ids::StreamKeys;
        let chars: Vec<char> = "ab{}:|%~ -_./\\*?\u{e9}\u{1f600}".chars().collect();
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        let mut text = |len: u64| -> String {
            (0..len)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    chars[usize::try_from(state % chars.len() as u64).unwrap()]
                })
                .collect()
        };
        for round in 0..2_000 {
            let stream = text(round % 13 + 1);
            let keys = StreamKeys::new("streams", &stream);
            let slots = [keys.meta(), keys.log(), keys.closed()].map(|key| {
                let tag = key
                    .split_once('{')
                    .and_then(|(_, rest)| rest.split_once('}'));
                assert!(tag.is_some_and(|(tag, _)| !tag.is_empty()), "{key}");
                slot(&key)
            });
            assert!(slots.iter().all(|s| *s == slots[0]), "{stream:?}");
        }
    }

    #[test]
    fn the_retry_policy_of_a_blocking_read() {
        let cluster = Route::Slot(1);
        let cases = [
            // The pool already tried a new connection, so a connection error means the node is
            // down.
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
                Route::Redirected(1),
                Step::NoWake,
            ),
            (reply("ASK 1 10.0.0.2:7292"), cluster, Step::NoWake),
            // A cluster with unknown endpoints redirects with no host, so the host is the one of
            // the node that answered.
            (
                reply("MOVED 1 :7292"),
                cluster,
                Step::Redirect(node("10.0.0.1", 7292)),
            ),
            // A redirect to a node that doesn't parse or has no known host is an error, not a
            // migration.
            (reply("MOVED 1 10.0.0.2"), cluster, Step::Fail),
            (reply("MOVED 1 ?:7292"), cluster, Step::Fail),
            // A standalone server never redirects, so a redirect there is an error.
            (
                reply("MOVED 1 10.0.0.2:7292"),
                Route::Standalone,
                Step::Fail,
            ),
            (reply("WRONGTYPE not a stream"), cluster, Step::Fail),
        ];
        let from = node("10.0.0.1", 7291);
        for (error, route, step) in cases {
            assert_eq!(classify(&error, route, &from), step, "{error} {route:?}");
        }
    }

    #[test]
    fn a_redirect_from_a_standalone_server_is_a_config_error() {
        use temporalio_streams::{Reason, StreamError};
        let moved = redis::parse_redis_value(b"-MOVED 3999 127.0.0.1:7292\r\n").unwrap();
        let error = StreamError::from(crate::RedisStoreError::from(not_moved(&moved).unwrap_err()));
        assert_eq!(error.reason, Reason::Invalid);
        assert!(error.message.contains("set `cluster`"), "{error}");
        assert!(not_moved(&Value::Okay).is_ok());
        let other = misrouted(reply("WRONGTYPE not a stream"));
        assert_eq!(other.code(), Some("WRONGTYPE"));
    }

    #[tokio::test]
    #[ignore = "needs Redis"]
    async fn a_cached_slot_on_a_dead_node_goes_back_to_the_seed() {
        let url = crate::testing::standalone_url();
        // A standalone server answers every slot, so the seed serves the read once the dead
        // node is dropped.
        let pool = BlockingPool::new(endpoint(cluster_at(&url)), NonZeroUsize::MIN, None);
        let key = "test:{dead-node}:log";
        let mut conn = Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let _: String = redis::cmd("XADD")
            .arg(key)
            .arg("*")
            .arg("x")
            .arg("y")
            .query_async(&mut conn)
            .await
            .unwrap();
        pool.slots.learn(slot(key), node("127.0.0.1", dead_port()));
        let deadline = Instant::now() + Duration::from_secs(2);
        let closed = "test:{dead-node}:closed";
        assert!(pool.wait_after(key, closed, "0-0", deadline).await.unwrap());
        assert_eq!(pool.slots.get(slot(key)), None);
    }

    #[test]
    fn the_cluster_slots_reply_gives_each_range_its_primary() {
        let reply = redis::parse_redis_value(
            b"*3\r\n*4\r\n:0\r\n:5460\r\n*3\r\n$9\r\n127.0.0.1\r\n:7001\r\n$1\r\na\r\n\
              *3\r\n$9\r\n127.0.0.1\r\n:7004\r\n$1\r\nb\r\n\
              *3\r\n:5461\r\n:10922\r\n*3\r\n$0\r\n\r\n:7002\r\n$1\r\nc\r\n\
              *3\r\n:10923\r\n:16383\r\n*3\r\n$-1\r\n:7003\r\n$1\r\nd\r\n",
        )
        .unwrap();
        let ranges: Vec<SlotRange> = redis::from_redis_value(reply).unwrap();
        assert_eq!(ranges[0].slots, 0..=5460);
        assert_eq!(ranges[0].replicas[0].port, 7004);
        let reachable: Vec<_> = ranges
            .iter()
            .map(|range| Node::reachable(&range.primary))
            .collect();
        // The second range has an empty host, and the third a NULL one.
        assert_eq!(ranges[2].primary.host, None);
        assert_eq!(reachable, [Some(node("127.0.0.1", 7001)), None, None]);
        let short = redis::parse_redis_value(b"*1\r\n*2\r\n:0\r\n:1\r\n").unwrap();
        assert!(redis::from_redis_value::<Vec<SlotRange>>(short).is_err());
    }

    #[tokio::test]
    #[ignore = "needs a Redis cluster"]
    async fn a_cluster_read_and_a_wait_connection_work_when_every_seed_is_down() {
        let live = endpoint(cluster_at(&crate::testing::cluster_url()));
        let SharedConnection::Cluster(cluster) = live.open().await.unwrap() else {
            panic!("a cluster endpoint opens a cluster connection");
        };
        let dead = endpoint(cluster_at(&format!("redis://127.0.0.1:{}", dead_port())));
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
        let closed = "test:{dead-seeds}:closed";
        assert!(
            reads
                .wait_after(key, closed, "0-0", deadline)
                .await
                .unwrap()
        );
        // The second lease finds no idle connection, so it opens one.
        let first = SharedConnection::Cluster(cluster.clone());
        let waits = WaitPool::new(first, dead, Some(cluster));
        let (one, two) = tokio::join!(waits.take(), waits.take());
        for leased in [one.unwrap(), two.unwrap()] {
            assert!(!matches!(leased, Leased::Busy));
        }
    }

    /// A port that nothing listens on.
    fn dead_port() -> u16 {
        let dead = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        dead.local_addr().unwrap().port()
    }

    /// The URL of a server that gives every command a nil array, and the commands that it gets.
    fn nil_server() -> (String, std::sync::mpsc::Receiver<Vec<String>>) {
        use std::io::Write;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (sent, commands) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for mut stream in listener.incoming().flatten() {
                let sent = sent.clone();
                std::thread::spawn(move || {
                    let mut reader = stream.try_clone().unwrap();
                    let mut parser = redis::Parser::new();
                    while let Ok(command) = parser.parse_value(&mut reader) {
                        let _ = sent.send(redis::from_redis_value(command).unwrap());
                        if stream.write_all(b"*-1\r\n").is_err() {
                            break;
                        }
                    }
                });
            }
        });
        (format!("redis://{address}"), commands)
    }

    // The paused clock advances only on `advance`, or to the next timer when all tasks wait. The
    // connections open before the pause, since the clock would jump past a connect.
    #[tokio::test]
    async fn the_block_time_starts_when_the_read_has_a_connection() {
        let (url, commands) = nil_server();
        let pool = BlockingPool::new(endpoint(standalone_at(&url)), NonZeroUsize::MIN, None);
        let seed = pool.endpoint.first().node.clone();
        let held = new(pool
            .node(&seed)
            .lease(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap());
        tokio::time::pause();
        let deadline = Instant::now() + Duration::from_millis(500);
        let read = pool.wait_after("s", "s:closed", "0-0", deadline);
        let free = async {
            tokio::time::advance(Duration::from_millis(300)).await;
            held.settle(Ok(())).unwrap();
        };
        // The paused clock can pass the read's deadline before the answer comes, so only the
        // block time that the server gets counts.
        let _ = tokio::join!(read, free);
        let xread = commands
            .iter()
            .find(|command| command.first().is_some_and(|name| name == "XREAD"))
            .unwrap();
        let block = xread.iter().position(|arg| arg == "BLOCK").unwrap();
        assert_eq!(xread[block + 1], "200");
    }

    #[tokio::test]
    async fn a_read_with_no_free_connection_ends_after_the_permit_wait() {
        let (url, _) = nil_server();
        let pool = BlockingPool::new(endpoint(standalone_at(&url)), NonZeroUsize::MIN, None);
        let seed = pool.endpoint.first().node.clone();
        let deadline = || Instant::now() + Duration::from_secs(10);
        let _held = new(pool.node(&seed).lease(deadline()).await.unwrap());
        tokio::time::pause();
        let start = Instant::now();
        assert!(
            !pool
                .wait_after("s", "s:closed", "0-0", deadline())
                .await
                .unwrap()
        );
        // The paused clock moves only to the read's give-up, which the timer rounds up to the
        // next ms.
        let waited = start.elapsed();
        assert!(
            (PERMIT_WAIT..=PERMIT_WAIT + Duration::from_millis(1)).contains(&waited),
            "{waited:?}"
        );
    }

    async fn lease(pool: &Pool<u8>, open: u8) -> Leased<u8> {
        pool.lease(std::future::pending(), || async move { Ok(open) })
            .await
            .unwrap()
    }

    fn new<C>(leased: Leased<C>) -> Lease<C> {
        match leased {
            Leased::New(lease) => lease,
            _ => panic!("the pool had an idle connection"),
        }
    }

    fn idle<C>(leased: Leased<C>) -> IdleLease<C> {
        match leased {
            Leased::Idle(lease) => lease,
            _ => panic!("the pool had no idle connection"),
        }
    }

    #[tokio::test]
    async fn a_lease_gives_its_connection_back_only_after_a_whole_reply() {
        let pool = Pool::new(NonZeroUsize::MIN, Vec::new());
        // A server error is a whole reply.
        let first = new(lease(&pool, 1).await);
        assert!(first.settle::<()>(Err(reply("ERR no"))).is_err());
        let mut again = idle(lease(&pool, 2).await);
        assert_eq!(*again.conn(), 1);
        assert!(matches!(again.settle(Ok(())), Settled::Done(Ok(()))));
        // After a wire error the connection closes.
        let lost = idle(lease(&pool, 3).await);
        let timeout = io(std::io::ErrorKind::TimedOut);
        assert!(matches!(
            lost.settle::<()>(Err(timeout)),
            Settled::Done(Err(_))
        ));
        let mut next = new(lease(&pool, 4).await);
        assert_eq!(*next.conn(), 4);
        // A lease that drops, as a cancelled call does, closes its connection too.
        drop(next);
        assert_eq!(*new(lease(&pool, 5).await).conn(), 5);
    }

    #[tokio::test]
    async fn a_dead_idle_connection_closes_the_idle_ones_but_a_slow_one_does_not() {
        let pool = Pool::new(NonZeroUsize::new(3).unwrap(), vec![1, 2, 3]);
        let slow = idle(lease(&pool, 0).await);
        let timeout = io(std::io::ErrorKind::TimedOut);
        assert!(matches!(
            slow.settle::<()>(Err(timeout)),
            Settled::Done(Err(_))
        ));
        let mut dead = idle(lease(&pool, 0).await);
        assert_eq!(*dead.conn(), 2);
        let Settled::Stale { error, permit } =
            dead.settle::<()>(Err(io(std::io::ErrorKind::BrokenPipe)))
        else {
            panic!("a dead idle connection gives its permit back");
        };
        assert!(error.is_io_error());
        let mut retry = permit.open(async { Ok(4) }).await.unwrap();
        assert_eq!(*retry.conn(), 4);
        // The pool dropped connection 1 with the dead one.
        drop(retry);
        assert_eq!(*new(lease(&pool, 5).await).conn(), 5);
    }

    #[tokio::test]
    async fn a_lease_gives_up_only_when_no_connection_is_free() {
        let pool = Pool::new(NonZeroUsize::MIN, Vec::new());
        let give_up = || std::future::ready(());
        let open = || async { Ok(1) };
        let held = pool.lease(give_up(), open).await.unwrap();
        assert!(matches!(held, Leased::New(_)));
        let busy = pool.lease(give_up(), open).await.unwrap();
        assert!(matches!(busy, Leased::Busy));
    }

    #[test]
    fn another_node_keeps_the_seed_credentials() {
        let seed = "redis://user:secret@127.0.0.1:7291/0"
            .into_connection_info()
            .unwrap();
        let other = node("10.0.0.2", 7292).info(&seed);
        assert_eq!(Node::try_from(&other).unwrap(), node("10.0.0.2", 7292));
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
