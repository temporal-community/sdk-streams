//! The Stream runtime on Redis.

use crate::{
    RedisStore, RedisStoreError, RedisStoreOptions, RedisStoreResult, proto::RedisStreamConfig,
};
use prost::Message;
use std::{sync::Arc, time::Duration};
use temporalio_common::telemetry::metrics::TemporalMeter;
use temporalio_streams::{StoreFactory, StreamResult, StreamRuntime, store::Store};

/// The name of the Redis runtime, for logs and for a bridge that selects a runtime by name.
pub const REDIS_RUNTIME_NAME: &str = "temporalio-streams-redis";

/// The options of a Stream runtime on Redis.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeOptions {
    /// How to connect to Redis.
    pub store: RedisStoreOptions,
    /// The backend name that lang registered the runtime under, for the refs it builds.
    pub backend: String,
}

impl RuntimeOptions {
    /// The options from the backend's config message.
    pub fn from_config(config: RedisStreamConfig) -> RedisStoreResult<Self> {
        let Some(seed) = config.urls.first() else {
            return Err(RedisStoreError::invalid(
                "The Redis Stream backend needs a URL",
            ));
        };
        let mut store = RedisStoreOptions::new(seed.clone());
        store.urls = config.urls;
        store.cluster = config.cluster;
        if !config.key_prefix.is_empty() {
            store.key_prefix = config.key_prefix;
        }
        if let Some(retention) = duration(config.retention) {
            store.retention = retention;
        }
        if let Some(safety_ttl) = duration(config.safety_ttl) {
            store.safety_ttl = safety_ttl;
        }
        if config.blocking_reads_per_node > 0 {
            store.blocking_reads_per_node = config.blocking_reads_per_node as usize;
        }
        if let Some(timeout) = duration(config.response_timeout) {
            store.response_timeout = timeout;
        }
        if config.wait_replicas > 0 {
            store.wait_replicas = config.wait_replicas as usize;
        }
        if let Some(timeout) = duration(config.wait_timeout) {
            store.wait_timeout = timeout;
        }
        Ok(Self {
            store,
            backend: config.backend,
        })
    }
}

fn duration(value: Option<prost_types::Duration>) -> Option<Duration> {
    let value = value?;
    let duration = Duration::new(value.seconds.max(0) as u64, value.nanos.max(0) as u32);
    (!duration.is_zero()).then_some(duration)
}

/// Connects to Redis on the runtime's first call that needs Redis.
struct Connect(RedisStoreOptions);

#[async_trait::async_trait]
impl StoreFactory for Connect {
    async fn connect(&self) -> StreamResult<Arc<dyn Store>> {
        Ok(Arc::new(RedisStore::connect(self.0.clone()).await?))
    }
}

/// Builds the Stream runtime on Redis.
///
/// The runtime connects on the first call that needs Redis. So a bridge can build it with no async
/// runtime, and a Workflow that never publishes never needs Redis.
#[derive(Debug)]
pub struct RedisStreamRuntime;

impl RedisStreamRuntime {
    /// Builds the runtime on Redis. It records metrics on `meter`, lang's runtime meter, if set.
    pub fn create(options: RuntimeOptions, meter: Option<TemporalMeter>) -> Arc<StreamRuntime> {
        StreamRuntime::new(
            REDIS_RUNTIME_NAME,
            options.backend,
            Arc::new(Connect(options.store)),
            meter,
        )
    }

    /// Builds the runtime from an encoded [RedisStreamConfig].
    pub fn from_config_bytes(
        config: &[u8],
        meter: Option<TemporalMeter>,
    ) -> RedisStoreResult<Arc<StreamRuntime>> {
        let config = RedisStreamConfig::decode(config)
            .map_err(|error| RedisStoreError::invalid(format!("The config is bad: {error}")))?;
        Ok(Self::create(RuntimeOptions::from_config(config)?, meter))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_config_fills_the_defaults() {
        let options = RuntimeOptions::from_config(RedisStreamConfig {
            urls: vec!["redis://x:1".to_string()],
            backend: "redis-a".to_string(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(options.backend, "redis-a");
        assert_eq!(options.store, RedisStoreOptions::new("redis://x:1"));
        assert!(RuntimeOptions::from_config(RedisStreamConfig::default()).is_err());
        let waits = RuntimeOptions::from_config(RedisStreamConfig {
            urls: vec!["redis://x:1".to_string()],
            wait_replicas: 2,
            wait_timeout: Some(prost_types::Duration {
                seconds: 0,
                nanos: 250_000_000,
            }),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            (waits.store.wait_replicas, waits.store.wait_timeout),
            (2, Duration::from_millis(250))
        );
    }
}
