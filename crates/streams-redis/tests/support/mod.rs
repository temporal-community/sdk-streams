//! The Redis backends of the shared suites, on a standalone server and on a cluster. The
//! standalone server's replica also runs them with `WAIT`.

use prost::Message;
use std::{num::NonZeroUsize, sync::Arc, time::Duration};
use temporalio_streams::{
    StreamRuntime,
    ids::StreamKeys,
    store::Store,
    testing::{Needs, RawKeys, TestBackend},
};
use temporalio_streams_redis::{
    RedisStore, RedisStoreOptions, RedisStreamRuntime, Topology, Url, WaitOptions,
    proto::RedisStreamConfig,
    testing::{cluster_url, standalone_url},
};

/// The key prefix of the tests, so that they never touch other data on the server.
const PREFIX: &str = "streams-test";

/// The backends of the standalone server, with `WAIT` and with none.
pub(crate) fn standalone() -> Vec<Arc<dyn TestBackend>> {
    let url = standalone_url();
    assert!(
        has_replica(&url),
        "the server at STREAMS_REDIS_URL needs a replica"
    );
    backends(Topology::Standalone(url.parse().unwrap()))
}

/// The backends of the cluster, with `WAIT` on each primary's replica and with none.
pub(crate) fn cluster() -> Vec<Arc<dyn TestBackend>> {
    backends(Topology::Cluster {
        first: cluster_url().parse().unwrap(),
        rest: Vec::new(),
    })
}

// Each write of the first backend goes through the `WAIT` pipeline, refusals too.
fn backends(topology: Topology<Url>) -> Vec<Arc<dyn TestBackend>> {
    let waits = RedisBackend {
        topology: topology.clone(),
        wait: Some(WaitOptions {
            replicas: NonZeroUsize::MIN,
            timeout: WAIT_TIMEOUT,
        }),
    };
    let plain = RedisBackend {
        topology,
        wait: None,
    };
    vec![Arc::new(waits), Arc::new(plain)]
}

/// Whether a standalone server has a replica that is online.
pub(crate) fn has_replica(url: &str) -> bool {
    let mut conn = redis::Client::open(url).unwrap().get_connection().unwrap();
    let info: redis::InfoDict = redis::cmd("INFO")
        .arg("replication")
        .query(&mut conn)
        .unwrap();
    info.get::<u64>("connected_slaves")
        .is_some_and(|count| count > 0)
}

/// The Redis of a backend, and its `WAIT`, if any.
pub(crate) struct RedisBackend {
    topology: Topology<Url>,
    wait: Option<WaitOptions>,
}

/// Long enough for a replica on a busy test host. The response timeout is twice this.
const WAIT_TIMEOUT: Duration = Duration::from_secs(5);

impl RedisBackend {
    fn options(&self) -> RedisStoreOptions {
        let mut options = RedisStoreOptions::from(self.topology.clone());
        options.key_prefix = PREFIX.parse().unwrap();
        options.response_timeout = 2 * WAIT_TIMEOUT;
        options.wait = self.wait;
        options
    }
}

#[async_trait::async_trait]
impl TestBackend for RedisBackend {
    fn gives(&self) -> Needs {
        Needs::ALL
    }

    async fn connect(&self) -> (Arc<dyn Store>, Arc<dyn RawKeys>) {
        let store = Arc::new(RedisStore::connect(self.options()).await.unwrap());
        let raw = Arc::new(Raw::open(&self.topology).await);
        (store, raw)
    }

    // Through the config bytes, as a bridge builds it.
    fn runtime(&self, backend: &str) -> Arc<StreamRuntime> {
        let seconds = |duration: Duration| prost_types::Duration {
            seconds: i64::try_from(duration.as_secs()).unwrap(),
            nanos: 0,
        };
        let config = RedisStreamConfig {
            urls: self.topology.servers().map(|url| url.to_string()).collect(),
            cluster: matches!(self.topology, Topology::Cluster { .. }),
            key_prefix: PREFIX.to_string(),
            backend: backend.to_string(),
            wait_replicas: self
                .wait
                .map_or(0, |wait| i32::try_from(wait.replicas.get()).unwrap()),
            wait_timeout: Some(seconds(WAIT_TIMEOUT)),
            response_timeout: Some(seconds(2 * WAIT_TIMEOUT)),
            // The writer cases leave megabytes of Records, which a local server keeps until the
            // Stream expires. An open Stream expires the safety TTL after its last write.
            retention: Some(seconds(Duration::from_secs(300))),
            safety_ttl: Some(seconds(Duration::from_secs(300))),
            ..Default::default()
        };
        RedisStreamRuntime::from_config_bytes(&config.encode_to_vec(), None).unwrap()
    }
}

/// The keys under the Store's calls, which the cases read and change with plain commands. A cluster
/// connection sends each command to the node of its key.
#[derive(Clone)]
enum Raw {
    Standalone(redis::aio::ConnectionManager),
    Cluster(redis::cluster_async::ClusterConnection),
}

impl Raw {
    async fn open(topology: &Topology<Url>) -> Self {
        match topology {
            Topology::Standalone(url) => {
                let client = redis::Client::open(url.as_str()).unwrap();
                Raw::Standalone(client.get_connection_manager().await.unwrap())
            }
            Topology::Cluster { .. } => {
                let client =
                    redis::cluster::ClusterClient::new(topology.servers().map(Url::as_str))
                        .unwrap();
                Raw::Cluster(client.get_async_connection().await.unwrap())
            }
        }
    }

    async fn query<T: redis::FromRedisValue>(&self, cmd: &redis::Cmd) -> T {
        match self.clone() {
            Raw::Standalone(mut conn) => cmd.query_async(&mut conn).await.unwrap(),
            Raw::Cluster(mut conn) => cmd.query_async(&mut conn).await.unwrap(),
        }
    }
}

#[async_trait::async_trait]
impl RawKeys for Raw {
    async fn set_meta(&self, namespace: &str, stream: &str, field: &[u8], value: &[u8]) {
        let keys = StreamKeys::new(PREFIX, namespace, stream);
        self.query::<()>(redis::cmd("HSET").arg(keys.meta()).arg(field).arg(value))
            .await;
    }

    async fn meta(&self, namespace: &str, stream: &str, field: &[u8]) -> Option<Vec<u8>> {
        let keys = StreamKeys::new(PREFIX, namespace, stream);
        self.query(redis::cmd("HGET").arg(keys.meta()).arg(field))
            .await
    }

    async fn ttls(&self, namespace: &str, stream: &str) -> (i64, i64) {
        let keys = StreamKeys::new(PREFIX, namespace, stream);
        let log = self.query(redis::cmd("PTTL").arg(keys.log())).await;
        let meta = self.query(redis::cmd("PTTL").arg(keys.meta())).await;
        (log, meta)
    }

    async fn add_raw_entry(&self, namespace: &str, stream: &str, id: &str) {
        let keys = StreamKeys::new(PREFIX, namespace, stream);
        self.query::<String>(redis::cmd("XADD").arg(keys.log()).arg(id).arg("x").arg("y"))
            .await;
    }

    async fn log_len(&self, namespace: &str, stream: &str) -> usize {
        let keys = StreamKeys::new(PREFIX, namespace, stream);
        self.query(redis::cmd("XLEN").arg(keys.log())).await
    }

    async fn drop_log(&self, namespace: &str, stream: &str) {
        let keys = StreamKeys::new(PREFIX, namespace, stream);
        self.query::<()>(redis::cmd("DEL").arg(keys.log())).await;
    }
}
