//! The Redis backends of the shared suites, on a standalone server and on a cluster. The
//! standalone server's replica also runs them with `WAIT`.

#![allow(dead_code)]

use prost::Message;
use std::{num::NonZeroUsize, sync::Arc, time::Duration};
use temporalio_streams::{
    StreamRuntime,
    ids::StreamKeys,
    store::Store,
    testing::{Needs, RawKeys, TestBackend},
};
use temporalio_streams_redis::{
    RedisStore, RedisStoreOptions, RedisStreamRuntime, WaitOptions,
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
    // Each write then goes through the `WAIT` pipeline, refusals too.
    let waits = RedisBackend {
        url: url.clone(),
        cluster: false,
        wait_replicas: 1,
    };
    let plain = RedisBackend {
        url,
        cluster: false,
        wait_replicas: 0,
    };
    vec![Arc::new(waits), Arc::new(plain)]
}

/// The backends of the cluster, with `WAIT` on each primary's replica and with none.
pub(crate) fn cluster() -> Vec<Arc<dyn TestBackend>> {
    let url = cluster_url();
    let waits = RedisBackend {
        url: url.clone(),
        cluster: true,
        wait_replicas: 1,
    };
    let plain = RedisBackend {
        url,
        cluster: true,
        wait_replicas: 0,
    };
    vec![Arc::new(waits), Arc::new(plain)]
}

/// Whether a standalone server has a replica that is online.
pub(crate) fn has_replica(url: &str) -> bool {
    let mut conn = redis::Client::open(url).unwrap().get_connection().unwrap();
    let info: String = redis::cmd("INFO")
        .arg("replication")
        .query(&mut conn)
        .unwrap();
    info.lines()
        .find_map(|line| line.trim().strip_prefix("connected_slaves:"))
        .is_some_and(|count| count != "0")
}

/// One Redis server, standalone or the seed of a cluster.
pub(crate) struct RedisBackend {
    url: String,
    cluster: bool,
    wait_replicas: usize,
}

/// Long enough for a replica on a busy test host.
const WAIT_TIMEOUT: Duration = Duration::from_secs(1);

impl RedisBackend {
    fn options(&self) -> RedisStoreOptions {
        let mut options = RedisStoreOptions::new(self.url.clone());
        options.cluster = self.cluster;
        options.key_prefix = PREFIX.parse().unwrap();
        options.wait = NonZeroUsize::new(self.wait_replicas).map(|replicas| WaitOptions {
            replicas,
            timeout: WAIT_TIMEOUT,
        });
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
        let raw = Arc::new(Raw::open(&self.url, self.cluster).await);
        (store, raw)
    }

    // Through the config bytes, as a bridge builds it.
    fn runtime(&self, backend: &str) -> Arc<StreamRuntime> {
        let config = RedisStreamConfig {
            urls: vec![self.url.clone()],
            cluster: self.cluster,
            key_prefix: PREFIX.to_string(),
            backend: backend.to_string(),
            wait_replicas: self.wait_replicas as i32,
            wait_timeout: Some(prost_types::Duration {
                seconds: WAIT_TIMEOUT.as_secs() as i64,
                nanos: 0,
            }),
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
    async fn open(url: &str, cluster: bool) -> Self {
        if cluster {
            let client = redis::cluster::ClusterClient::new(vec![url]).unwrap();
            Raw::Cluster(client.get_async_connection().await.unwrap())
        } else {
            let client = redis::Client::open(url).unwrap();
            Raw::Standalone(client.get_connection_manager().await.unwrap())
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
    async fn set_meta(&self, namespace: &str, stream: &str, field: &str, value: &str) {
        let keys = StreamKeys::new(PREFIX, namespace, stream);
        self.query::<()>(redis::cmd("HSET").arg(keys.meta()).arg(field).arg(value))
            .await;
    }

    async fn meta(&self, namespace: &str, stream: &str, field: &str) -> Option<String> {
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
}
