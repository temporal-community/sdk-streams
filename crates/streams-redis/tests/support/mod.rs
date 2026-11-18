//! The Redis Stores of the shared suites, on a standalone server and on a cluster. The
//! standalone server's replica also runs them with `WAIT`.

use prost::Message;
use std::{num::NonZeroUsize, sync::Arc, time::Duration};
use temporalio_streams::StreamRuntime;
use temporalio_streams_log::{
    ids::StreamKeys,
    store::Store,
    testing::{Needs, RawKeys, RuntimeConfig, TestStore},
};
use temporalio_streams_redis::{
    RedisServers, RedisStore, RedisStoreOptions, RedisStreamRuntime, Url, WaitOptions,
    proto::RedisStreamConfig,
    testing::{cluster_url, standalone_url},
};

/// The key prefix of the tests, so that they never touch other data on the server.
const PREFIX: &str = "streams-test";

/// The Stores of the standalone server, with `WAIT` and with none.
pub(crate) fn standalone() -> Vec<Arc<dyn TestStore>> {
    let url = standalone_url();
    assert!(
        has_replica(&url),
        "the server at STREAMS_REDIS_URL needs a replica"
    );
    test_stores(RedisServers::Standalone(url.parse().unwrap()))
}

/// The Stores of the cluster, with `WAIT` on each primary's replica and with none.
pub(crate) fn cluster() -> Vec<Arc<dyn TestStore>> {
    test_stores(RedisServers::Cluster {
        first: cluster_url().parse().unwrap(),
        rest: Vec::new(),
    })
}

// Each write of the first Store goes through the `WAIT` pipeline, refusals too.
fn test_stores(servers: RedisServers<Url>) -> Vec<Arc<dyn TestStore>> {
    let waits = RedisTestStore {
        servers: servers.clone(),
        wait: Some(WaitOptions {
            replicas: NonZeroUsize::MIN,
            timeout: WAIT_TIMEOUT,
        }),
    };
    let plain = RedisTestStore {
        servers,
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

/// The Redis of a Store under test, and its `WAIT`, if any.
pub(crate) struct RedisTestStore {
    servers: RedisServers<Url>,
    wait: Option<WaitOptions>,
}

/// Long enough for a replica on a busy test host. The response timeout is twice this.
const WAIT_TIMEOUT: Duration = Duration::from_secs(5);

impl RedisTestStore {
    fn options(&self) -> RedisStoreOptions {
        let mut options = RedisStoreOptions::from(self.servers.clone());
        options.key_prefix = PREFIX.parse().unwrap();
        options.response_timeout = 2 * WAIT_TIMEOUT;
        options.wait = self.wait;
        options
    }
}

#[async_trait::async_trait]
impl TestStore for RedisTestStore {
    fn gives(&self) -> Needs {
        Needs::ALL
    }

    async fn connect(&self) -> (Arc<dyn Store>, Arc<dyn RawKeys>) {
        let store = Arc::new(RedisStore::connect(self.options()).await.unwrap());
        let raw = Arc::new(Raw::open(&self.servers).await);
        (store, raw)
    }

    // Through the config bytes, as a bridge builds it.
    fn runtime(&self, runtime: &RuntimeConfig) -> Arc<StreamRuntime> {
        let seconds = |duration: Duration| prost_types::Duration {
            seconds: i64::try_from(duration.as_secs()).unwrap(),
            nanos: i32::try_from(duration.subsec_nanos()).unwrap(),
        };
        let config = RedisStreamConfig {
            urls: self.servers.iter().map(|url| url.to_string()).collect(),
            cluster: matches!(self.servers, RedisServers::Cluster { .. }),
            key_prefix: PREFIX.to_string(),
            store_name: runtime.store_name.clone(),
            wait_replicas: self
                .wait
                .map_or(0, |wait| i32::try_from(wait.replicas.get()).unwrap()),
            wait_timeout: Some(seconds(WAIT_TIMEOUT)),
            response_timeout: Some(seconds(2 * WAIT_TIMEOUT)),
            // A case that sets no TTL still gets ones that end, so a local server doesn't keep its
            // Streams for good.
            retention: Some(seconds(
                runtime.retention.unwrap_or(Duration::from_secs(300)),
            )),
            producer_idle_ttl: Some(seconds(
                runtime
                    .producer_idle_ttl
                    .unwrap_or(Duration::from_secs(300)),
            )),
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
    async fn open(servers: &RedisServers<Url>) -> Self {
        match servers {
            RedisServers::Standalone(url) => {
                let client = redis::Client::open(url.as_str()).unwrap();
                Raw::Standalone(client.get_connection_manager().await.unwrap())
            }
            RedisServers::Cluster { .. } => {
                let client =
                    redis::cluster::ClusterClient::new(servers.iter().map(Url::as_str)).unwrap();
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
    async fn set_meta(&self, stream: &str, field: &[u8], value: &[u8]) {
        let keys = StreamKeys::new(PREFIX, stream);
        self.query::<()>(redis::cmd("HSET").arg(keys.meta()).arg(field).arg(value))
            .await;
    }

    async fn meta(&self, stream: &str, field: &[u8]) -> Option<Vec<u8>> {
        let keys = StreamKeys::new(PREFIX, stream);
        self.query(redis::cmd("HGET").arg(keys.meta()).arg(field))
            .await
    }

    async fn ttls(&self, stream: &str) -> (i64, i64) {
        let keys = StreamKeys::new(PREFIX, stream);
        let log = self.query(redis::cmd("PTTL").arg(keys.log())).await;
        let meta = self.query(redis::cmd("PTTL").arg(keys.meta())).await;
        (log, meta)
    }

    async fn add_raw_entry(&self, stream: &str, id: &str) {
        let keys = StreamKeys::new(PREFIX, stream);
        self.query::<String>(redis::cmd("XADD").arg(keys.log()).arg(id).arg("x").arg("y"))
            .await;
    }

    async fn log_len(&self, stream: &str) -> usize {
        let keys = StreamKeys::new(PREFIX, stream);
        self.query(redis::cmd("XLEN").arg(keys.log())).await
    }

    async fn drop_log(&self, stream: &str) {
        let keys = StreamKeys::new(PREFIX, stream);
        self.query::<()>(redis::cmd("DEL").arg(keys.log())).await;
    }
}
