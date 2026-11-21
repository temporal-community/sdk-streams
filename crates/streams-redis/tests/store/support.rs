//! The Redis servers that the tests start, the calls of a Store, and a seeded generator.

use redis::{
    aio::{ConnectionLike, MultiplexedConnection},
    cluster_async::ClusterConnection,
};
use std::{
    collections::BTreeMap,
    net::TcpStream,
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
    time::{Duration, Instant},
};
use temporalio_common::protos::temporal::api::common::v1::Payload;
use temporalio_streams::{
    StreamError, StreamRuntime,
    proto::{
        AppendRecord, AppendRecordsRequest, ClientProducer, CloseStreamRequest,
        CreateStreamRequest, DescribeStreamRequest, Producer, ReadRecordsRequest,
        StreamErrorReason, StreamIdConflictPolicy, producer::Kind,
    },
    record::StoreLimits,
};
use temporalio_streams_redis::{RedisServers, RedisStore, RedisStoreOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// A `redis-server` process of one test. The drop stops it, also when the test panics.
pub(crate) struct Redis {
    pub(crate) port: u16,
    child: Child,
    dir: PathBuf,
}

/// Returns a port that nothing listens on. It's below the ports that a system gives to a socket
/// by itself, so no socket of a test takes it before its server starts.
pub(crate) fn free_port() -> u16 {
    static GIVEN: AtomicU32 = AtomicU32::new(0);
    loop {
        // The process id gives two test runs of one host their own ports.
        let at = std::process::id() + GIVEN.fetch_add(1, Ordering::Relaxed);
        let port = 20_000 + u16::try_from(at % 10_000).unwrap();
        // The check is a connect. A server that another test starts at that moment would inherit
        // a listener of the test, and keep the port.
        if TcpStream::connect(("127.0.0.1", port)).is_err() {
            return port;
        }
    }
}

impl Redis {
    /// Starts a server on a free port, with `args` after the test's own.
    pub(crate) fn start(args: &[&str]) -> Self {
        Self::start_on(free_port(), args)
    }

    pub(crate) fn start_on(port: u16, args: &[&str]) -> Self {
        // Other software of the host uses these ports, and a test must never talk to it.
        assert!(![6379, 6399, 7299].contains(&port));
        let dir = std::env::temp_dir().join(format!("streams-redis-{}-{port}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let binary = std::env::var("REDIS_SERVER").unwrap_or("redis-server".to_string());
        let child = Command::new(binary)
            .args(["--port", &port.to_string(), "--bind", "127.0.0.1"])
            .args(["--save", "", "--appendonly", "no", "--dir"])
            .arg(&dir)
            // A server waits this long for more replicas before it sends its data to the first.
            .args(["--repl-diskless-sync-delay", "0"])
            .args(args)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("the tests need `redis-server` in the PATH, or its path in REDIS_SERVER");
        let mut redis = Self { port, child, dir };
        let started = Instant::now();
        while redis.ping().is_err() {
            let ended = redis.child.try_wait().unwrap();
            assert!(
                ended.is_none(),
                "redis-server ended at its start: {ended:?}"
            );
            assert!(
                started.elapsed() < Duration::from_secs(20),
                "Redis didn't start"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        redis
    }

    /// Any answer shows that the server takes connections. One with a password answers `NOAUTH`.
    fn ping(&self) -> Result<(), redis::RedisError> {
        let mut conn = redis::Client::open(self.url())?.get_connection()?;
        match redis::cmd("PING").query::<redis::Value>(&mut conn) {
            Err(error) if error.is_io_error() => Err(error),
            _ => Ok(()),
        }
    }

    pub(crate) fn url(&self) -> String {
        format!("redis://127.0.0.1:{}", self.port)
    }

    /// Returns a connection for the plain commands of a test.
    pub(crate) async fn admin(&self) -> MultiplexedConnection {
        admin(&self.url()).await
    }

    /// Starts a replica of this server, and returns it once the server has it online.
    pub(crate) fn start_replica(&self) -> Redis {
        let replica = Redis::start(&["--replicaof", "127.0.0.1", &self.port.to_string()]);
        self.until_replicated();
        replica
    }

    /// Waits until a replica has every write of this server. The server shows a replica as
    /// online while the replica still loads its first copy, and a write then waits for it.
    fn until_replicated(&self) {
        let mut conn = redis::Client::open(self.url())
            .unwrap()
            .get_connection()
            .unwrap();
        // A write that needs no key, so that it also runs on any node of a cluster.
        let mut probe = redis::pipe();
        probe.cmd("FUNCTION").arg("FLUSH").ignore();
        probe.cmd("WAIT").arg(1).arg(1000);
        let started = Instant::now();
        while probe.query::<(usize,)>(&mut conn).unwrap().0 < 1 {
            assert!(
                started.elapsed() < Duration::from_secs(30),
                "no replica has the writes of its server"
            );
        }
    }

    /// Stops the process without ending it, so a replica acknowledges nothing until `resume`.
    pub(crate) fn pause(&self) {
        self.signal("-STOP");
    }

    pub(crate) fn resume(&self) {
        self.signal("-CONT");
    }

    fn signal(&self, signal: &str) {
        let pid = self.child.id().to_string();
        assert!(
            Command::new("kill")
                .args([signal, &pid])
                .status()
                .unwrap()
                .success()
        );
    }
}

impl Drop for Redis {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

pub(crate) async fn admin(url: &str) -> MultiplexedConnection {
    let client = redis::Client::open(url).unwrap();
    client.get_multiplexed_async_connection().await.unwrap()
}

/// Returns the default options of a Store for the server at `url`.
pub(crate) fn standalone(url: impl AsRef<str>) -> RedisStoreOptions {
    RedisStoreOptions::new(RedisServers::Standalone(url.as_ref().parse().unwrap()))
}

/// Returns the Store of `options` behind the runtime, as every caller uses it.
pub(crate) fn runtime(options: RedisStoreOptions) -> StreamRuntime {
    let store = RedisStore::new(options).unwrap();
    StreamRuntime::new(Arc::new(store), StoreLimits::default())
}

/// Returns the four keys of a Stream, as `docs/redis.md` gives them.
pub(crate) fn keys(prefix: &str, stream_id: &str) -> [String; 4] {
    let id = stream_id.replace('%', "%25").replace('}', "%7D");
    ["meta", "log", "producers", "closed"].map(|part| format!("{prefix}:{{{id}}}:{part}"))
}

/// What the keys of a Stream hold and when they expire, so a test can show that a call changed
/// nothing. A hash is in the order of its fields, since Redis keeps no order of them.
#[derive(Debug, PartialEq)]
pub(crate) struct Snapshot {
    meta: BTreeMap<Vec<u8>, Vec<u8>>,
    producers: BTreeMap<Vec<u8>, Vec<u8>>,
    log: redis::Value,
    closed: redis::Value,
    expiries: [i64; 4],
}

pub(crate) async fn snapshot(
    admin: &mut impl ConnectionLike,
    prefix: &str,
    stream_id: &str,
) -> Snapshot {
    let [meta, log, producers, closed] = keys(prefix, stream_id);
    let mut pipe = redis::pipe();
    pipe.cmd("HGETALL")
        .arg(&meta)
        .cmd("HGETALL")
        .arg(&producers);
    for key in [&log, &closed] {
        pipe.cmd("XRANGE").arg(key).arg("-").arg("+");
    }
    for key in [&meta, &log, &producers, &closed] {
        pipe.cmd("PEXPIRETIME").arg(key);
    }
    let (meta, producers, log, closed, a, b, c, d) = pipe.query_async(admin).await.unwrap();
    Snapshot {
        meta,
        producers,
        log,
        closed,
        expiries: [a, b, c, d],
    }
}

/// Returns the reason of a failed call, or panics for a call that succeeded.
pub(crate) fn reason<T: std::fmt::Debug, R: Into<StreamErrorReason>>(
    result: Result<T, StreamError<R>>,
) -> StreamErrorReason {
    result.map_err(|error| error.reason.into()).unwrap_err()
}

pub(crate) fn client(id: &str) -> Producer {
    Producer {
        kind: Some(Kind::Client(ClientProducer {
            namespace: "default".to_string(),
            id: id.to_string(),
        })),
    }
}

pub(crate) fn record(data: &[u8]) -> AppendRecord {
    AppendRecord {
        body: Some(Payload {
            data: data.to_vec(),
            ..Default::default()
        }),
    }
}

pub(crate) fn create(stream_id: &str) -> CreateStreamRequest {
    CreateStreamRequest {
        stream_id: stream_id.to_string(),
        id_conflict_policy: StreamIdConflictPolicy::Fail as i32,
        request_id: String::new(),
    }
}

/// Returns an append of `records` by a client, with a `request_id` that no other call has.
pub(crate) fn append(stream_id: &str, records: &[&[u8]]) -> AppendRecordsRequest {
    AppendRecordsRequest {
        stream_id: stream_id.to_string(),
        producer: Some(client("client:test")),
        request_id: String::new(),
        records: records.iter().map(|data| record(data)).collect(),
    }
}

pub(crate) fn close(stream_id: &str) -> CloseStreamRequest {
    CloseStreamRequest {
        stream_id: stream_id.to_string(),
        producer: Some(client("client:test")),
    }
}

pub(crate) fn read(stream_id: &str, from_offset: &[u8]) -> ReadRecordsRequest {
    ReadRecordsRequest {
        stream_id: stream_id.to_string(),
        from_offset: from_offset.to_vec(),
        ..Default::default()
    }
}

pub(crate) fn describe(stream_id: &str) -> DescribeStreamRequest {
    DescribeStreamRequest {
        stream_id: stream_id.to_string(),
    }
}

/// A small xorshift generator, so a seed gives the same inputs on every host.
pub(crate) struct Rng(u64);

impl Rng {
    pub(crate) fn new(seed: u64) -> Self {
        // The multiply spreads small seeds over the state, which must not be zero.
        Self(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1)
    }

    pub(crate) fn below(&mut self, bound: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % bound as u64) as usize
    }

    pub(crate) fn chance(&mut self, percent: usize) -> bool {
        self.below(100) < percent
    }

    pub(crate) fn pick<T: Clone>(&mut self, items: &[T]) -> T {
        items[self.below(items.len())].clone()
    }

    pub(crate) fn bytes(&mut self, max: usize) -> Vec<u8> {
        (0..self.below(max + 1))
            .map(|_| self.below(256) as u8)
            .collect()
    }
}

/// Returns the seeds of a generated test, or only the one of `STREAMS_SEED`.
pub(crate) fn seeds(count: u64) -> Vec<u64> {
    match std::env::var("STREAMS_SEED") {
        Ok(seed) => vec![seed.parse().unwrap()],
        Err(_) => (1..=count).collect(),
    }
}

/// A TCP proxy between a Store and a Redis server. It can lose the reply of a script after Redis
/// ran it, which is the fault of a connection that drops at the wrong moment.
pub(crate) struct Proxy {
    pub(crate) port: u16,
    lose: Arc<AtomicBool>,
    accept: tokio::task::JoinHandle<()>,
}

impl Proxy {
    pub(crate) async fn start(target: u16) -> Self {
        Self::start_with(target, Arc::default()).await
    }

    /// Starts a proxy that loses a reply when `lose` is set, which several proxies can share.
    async fn start_with(target: u16, lose: Arc<AtomicBool>) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let armed = lose.clone();
        let accept = tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                tokio::spawn(relay(client, target, armed.clone()));
            }
        });
        Self { port, lose, accept }
    }

    pub(crate) fn url(&self) -> String {
        format!("redis://127.0.0.1:{}", self.port)
    }

    /// Makes the proxy close the connection of the next script call in place of its reply.
    pub(crate) fn lose_next_reply(&self) {
        self.lose.store(true, Ordering::SeqCst);
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.accept.abort();
    }
}

async fn relay(client: tokio::net::TcpStream, target: u16, lose: Arc<AtomicBool>) {
    let Ok(server) = tokio::net::TcpStream::connect(("127.0.0.1", target)).await else {
        return;
    };
    let (mut from_client, mut to_client) = client.into_split();
    let (mut from_server, mut to_server) = server.into_split();
    // Whether this connection loses the next bytes that the server sends.
    let cut = AtomicBool::new(false);
    let requests = async {
        let mut chunk = [0; 16 * 1024];
        while let Ok(read @ 1..) = from_client.read(&mut chunk).await {
            let script = chunk[..read].windows(7).any(|bytes| bytes == b"EVALSHA");
            if script && lose.swap(false, Ordering::SeqCst) {
                cut.store(true, Ordering::SeqCst);
            }
            if to_server.write_all(&chunk[..read]).await.is_err() {
                break;
            }
        }
    };
    let replies = async {
        let mut chunk = [0; 16 * 1024];
        while let Ok(read @ 1..) = from_server.read(&mut chunk).await {
            // The reply shows that Redis ran the script. Both sockets close when this ends.
            if cut.load(Ordering::SeqCst) || to_client.write_all(&chunk[..read]).await.is_err() {
                break;
            }
        }
    };
    tokio::select! {
        () = requests => {}
        () = replies => {}
    }
}

/// A Redis Cluster of three primaries. Each node announces the port of a proxy of the test, so a
/// client reaches every node through one, and a test can lose a reply of any node.
pub(crate) struct Cluster {
    pub(crate) nodes: Vec<Redis>,
    /// One replica of each node, in the order of the nodes, or none.
    pub(crate) replicas: Vec<Redis>,
    proxies: Vec<Proxy>,
}

/// The slots of a node of the test cluster, of which the last node has two more.
const NODE_SLOTS: u16 = 5461;

impl Cluster {
    pub(crate) async fn start() -> Self {
        let lose = Arc::<AtomicBool>::default();
        let mut cluster = Self {
            nodes: Vec::new(),
            replicas: Vec::new(),
            proxies: Vec::new(),
        };
        for at in 0..3 {
            let (port, bus) = (free_port(), free_port().to_string());
            let proxy = Proxy::start_with(port, lose.clone()).await;
            let announced = proxy.port.to_string();
            let node = Redis::start_on(
                port,
                &[
                    "--cluster-enabled",
                    "yes",
                    "--cluster-port",
                    &bus,
                    // The other nodes and the clients learn these two from the node.
                    "--cluster-announce-port",
                    &announced,
                    "--cluster-announce-bus-port",
                    &bus,
                ],
            );
            let first = at * NODE_SLOTS;
            let last = if at == 2 {
                16383
            } else {
                first + NODE_SLOTS - 1
            };
            let mut admin = node.admin().await;
            let mut slots = redis::cmd("CLUSTER");
            slots.arg("ADDSLOTSRANGE").arg(first).arg(last);
            let _: () = slots.query_async(&mut admin).await.unwrap();
            if let Some(known) = cluster.nodes.first() {
                let mut meet = redis::cmd("CLUSTER");
                meet.arg("MEET").arg("127.0.0.1").arg(port).arg(&bus);
                let _: () = meet.query_async(&mut known.admin().await).await.unwrap();
            }
            cluster.nodes.push(node);
            cluster.proxies.push(proxy);
        }
        let started = Instant::now();
        while !cluster.ready().await {
            assert!(
                started.elapsed() < Duration::from_secs(60),
                "the cluster didn't form"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        cluster
    }

    /// Starts the cluster, and gives each node a replica.
    pub(crate) async fn start_with_replicas() -> Self {
        let mut cluster = Self::start().await;
        for node in &cluster.nodes {
            let (port, bus) = (free_port(), free_port().to_string());
            let cluster_args = ["--cluster-enabled", "yes", "--cluster-port", &bus];
            let replica = Redis::start_on(port, &cluster_args);
            let mut meet = redis::cmd("CLUSTER");
            meet.arg("MEET").arg("127.0.0.1").arg(port).arg(&bus);
            let _: () = meet.query_async(&mut node.admin().await).await.unwrap();
            let mut my_id = redis::cmd("CLUSTER");
            let id: String =
                (my_id.arg("MYID").query_async(&mut node.admin().await).await).unwrap();
            // The replica knows the node only once the two have met.
            let mut admin = replica.admin().await;
            let started = Instant::now();
            let mut replicate = redis::cmd("CLUSTER");
            replicate.arg("REPLICATE").arg(&id);
            while replicate.query_async::<()>(&mut admin).await.is_err() {
                assert!(
                    started.elapsed() < Duration::from_secs(60),
                    "the replica didn't join"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            node.until_replicated();
            cluster.replicas.push(replica);
        }
        cluster
    }

    /// Returns whether every node serves the whole cluster and announces the proxies.
    async fn ready(&self) -> bool {
        for node in &self.nodes {
            let mut admin = node.admin().await;
            let info: String = redis::cmd("CLUSTER")
                .arg("INFO")
                .query_async(&mut admin)
                .await
                .unwrap();
            let nodes: String = redis::cmd("CLUSTER")
                .arg("NODES")
                .query_async(&mut admin)
                .await
                .unwrap();
            let announced = |proxy: &Proxy| nodes.contains(&format!(":{}@", proxy.port));
            let whole =
                info.contains("cluster_state:ok") && info.contains("cluster_slots_ok:16384");
            if !whole || !self.proxies.iter().all(announced) {
                return false;
            }
        }
        true
    }

    /// Returns the default options of a Store for the cluster, by the URL of one node.
    pub(crate) fn options(&self) -> RedisStoreOptions {
        RedisStoreOptions::new(RedisServers::Cluster {
            first: self.proxies[0].url().parse().unwrap(),
            rest: Vec::new(),
        })
    }

    /// Returns the port of the node that a Store of the test gets as its one URL.
    pub(crate) fn seed_port(&self) -> u16 {
        self.proxies[0].port
    }

    /// Returns a connection that sends each plain command of a test to the node of its key.
    pub(crate) async fn admin(&self) -> ClusterConnection {
        let client = redis::cluster::ClusterClient::new(vec![self.proxies[0].url()]).unwrap();
        client.get_async_connection().await.unwrap()
    }

    /// Returns the node that serves a slot.
    pub(crate) fn node_of(&self, slot: u16) -> &Redis {
        &self.nodes[usize::from(slot / NODE_SLOTS).min(2)]
    }

    /// Makes the proxy of the next script call close its connection in place of the reply.
    pub(crate) fn lose_next_reply(&self) {
        self.proxies[0].lose_next_reply();
    }

    /// Returns whether a proxy lost the reply that `lose_next_reply` asked for.
    pub(crate) fn lost_the_reply(&self) -> bool {
        !self.proxies[0].lose.load(Ordering::SeqCst)
    }
}
