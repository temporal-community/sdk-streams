//! The Redis backends of the shared suites, one for a standalone server and one for a cluster,
//! each when its URL is set. A standalone server with a replica also runs them with `WAIT`.

#![allow(dead_code)]

mod server;

pub(crate) use server::server_url;

use prost::Message;
use redis::AsyncCommands;
use std::{sync::Arc, time::Duration};
use temporalio_streams::{
    StreamRuntime,
    ids::StreamKeys,
    store::Store,
    testing::{Needs, RawKeys, TestBackend},
};
use temporalio_streams_redis::{
    RedisStore, RedisStoreOptions, RedisStreamRuntime, proto::RedisStreamConfig,
};

/// The key prefix of the tests, so that they never touch other data on the server.
const PREFIX: &str = "streams-test";

/// The backends of each Redis server that is set, standalone first.
pub(crate) fn backends() -> Vec<Arc<dyn TestBackend>> {
    let mut backends: Vec<Arc<dyn TestBackend>> = vec![];
    for (variable, cluster) in [
        ("STREAMS_REDIS_URL", false),
        ("STREAMS_REDIS_CLUSTER_URL", true),
    ] {
        let Some(url) = server_url(variable) else {
            continue;
        };
        // Each write then goes through the `WAIT` pipeline, refusals too.
        if !cluster && has_replica(&url) {
            backends.push(Arc::new(RedisBackend {
                url: url.clone(),
                cluster,
                wait_replicas: 1,
            }));
        }
        backends.push(Arc::new(RedisBackend {
            url,
            cluster,
            wait_replicas: 0,
        }));
    }
    backends
}

/// Whether a standalone server has a replica that is online.
fn has_replica(url: &str) -> bool {
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
        options.key_prefix = PREFIX.to_string();
        options.wait_replicas = self.wait_replicas;
        options.wait_timeout = WAIT_TIMEOUT;
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
        let raw = Arc::new(Raw {
            url: self.url.clone(),
            cluster: self.cluster,
        });
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

/// The keys under the Store's calls, which the cases read and change with plain commands.
struct Raw {
    url: String,
    cluster: bool,
}

impl Raw {
    /// A plain connection to the node of the Stream's slot.
    async fn conn(&self, keys: &StreamKeys) -> redis::aio::ConnectionManager {
        let url = if self.cluster {
            let client = redis::cluster::ClusterClient::new(vec![self.url.as_str()]).unwrap();
            let mut conn = client.get_async_connection().await.unwrap();
            let slot: u16 = redis::cmd("CLUSTER")
                .arg("KEYSLOT")
                .arg(keys.meta())
                .query_async::<i64>(&mut conn)
                .await
                .unwrap() as u16;
            let shards: redis::Value = redis::cmd("CLUSTER")
                .arg("SLOTS")
                .query_async(&mut conn)
                .await
                .unwrap();
            node_of(&shards, slot).expect("a node serves the slot")
        } else {
            self.url.clone()
        };
        redis::Client::open(url)
            .unwrap()
            .get_connection_manager()
            .await
            .unwrap()
    }
}

#[async_trait::async_trait]
impl RawKeys for Raw {
    async fn set_meta(&self, namespace: &str, stream: &str, field: &str, value: &str) {
        let keys = StreamKeys::new(PREFIX, namespace, stream);
        let mut conn = self.conn(&keys).await;
        let _: () = conn.hset(keys.meta(), field, value).await.unwrap();
    }

    async fn meta(&self, namespace: &str, stream: &str, field: &str) -> Option<String> {
        let keys = StreamKeys::new(PREFIX, namespace, stream);
        let mut conn = self.conn(&keys).await;
        conn.hget(keys.meta(), field).await.unwrap()
    }

    async fn ttls(&self, namespace: &str, stream: &str) -> (i64, i64) {
        let keys = StreamKeys::new(PREFIX, namespace, stream);
        let mut conn = self.conn(&keys).await;
        let log: i64 = conn.pttl(keys.log()).await.unwrap();
        let meta: i64 = conn.pttl(keys.meta()).await.unwrap();
        (log, meta)
    }

    async fn add_raw_entry(&self, namespace: &str, stream: &str, id: &str) {
        let keys = StreamKeys::new(PREFIX, namespace, stream);
        let mut conn = self.conn(&keys).await;
        let _: String = redis::cmd("XADD")
            .arg(keys.log())
            .arg(id)
            .arg("x")
            .arg("y")
            .query_async(&mut conn)
            .await
            .unwrap();
    }

    async fn log_len(&self, namespace: &str, stream: &str) -> usize {
        let keys = StreamKeys::new(PREFIX, namespace, stream);
        let mut conn = self.conn(&keys).await;
        conn.xlen(keys.log()).await.unwrap()
    }
}

fn node_of(slots: &redis::Value, slot: u16) -> Option<String> {
    let redis::Value::Array(ranges) = slots else {
        return None;
    };
    for range in ranges {
        let redis::Value::Array(parts) = range else {
            continue;
        };
        let number = |value: &redis::Value| match value {
            redis::Value::Int(n) => Some(*n),
            _ => None,
        };
        let (Some(start), Some(end)) = (number(&parts[0]), number(&parts[1])) else {
            continue;
        };
        if (start..=end).contains(&(slot as i64)) {
            let redis::Value::Array(node) = &parts[2] else {
                continue;
            };
            let host = match &node[0] {
                redis::Value::BulkString(bytes) => String::from_utf8(bytes.clone()).ok()?,
                _ => return None,
            };
            let port = number(&node[1])?;
            let host = if host.is_empty() {
                "127.0.0.1".to_string()
            } else {
                host
            };
            return Some(format!("redis://{host}:{port}/0"));
        }
    }
    None
}
