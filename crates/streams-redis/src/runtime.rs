//! The Stream runtime on Redis.

use crate::{
    RedisStore, RedisStoreError, RedisStoreOptions, RedisStoreResult, proto::RedisStreamConfig,
};
use prost::Message;
use std::{sync::Arc, time::Duration};
use temporalio_common::telemetry::metrics::TemporalMeter;
use temporalio_streams::{StoreFactory, StreamResult, StreamRuntime, store::Store};

/// The name that [StreamRuntime::name] gives for a Redis runtime.
pub const REDIS_RUNTIME_NAME: &str = "temporalio-streams-redis";

/// The options of a Stream runtime on Redis.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct RuntimeOptions {
    /// How to connect to Redis.
    pub store: RedisStoreOptions,
    /// The backend name that lang registered the runtime under, for the handles it builds.
    pub backend: String,
}

/// The options from the backend's config message. An unset or zero field keeps its default.
impl TryFrom<RedisStreamConfig> for RuntimeOptions {
    type Error = RedisStoreError;

    fn try_from(config: RedisStreamConfig) -> RedisStoreResult<Self> {
        if config.urls.is_empty() {
            return Err(RedisStoreError::invalid(
                "The Redis Stream backend needs a URL",
            ));
        }
        let defaults = RedisStoreOptions::default();
        let store = RedisStoreOptions {
            urls: config.urls,
            cluster: config.cluster,
            key_prefix: Some(config.key_prefix)
                .filter(|prefix| !prefix.is_empty())
                .unwrap_or(defaults.key_prefix),
            retention: duration("retention", config.retention)?.unwrap_or(defaults.retention),
            safety_ttl: duration("safety_ttl", config.safety_ttl)?.unwrap_or(defaults.safety_ttl),
            blocking_reads_per_node: count(
                "blocking_reads_per_node",
                config.blocking_reads_per_node,
            )?
            .unwrap_or(defaults.blocking_reads_per_node),
            response_timeout: duration("response_timeout", config.response_timeout)?
                .unwrap_or(defaults.response_timeout),
            wait_replicas: count("wait_replicas", config.wait_replicas)?
                .unwrap_or(defaults.wait_replicas),
            wait_timeout: duration("wait_timeout", config.wait_timeout)?
                .unwrap_or(defaults.wait_timeout),
        };
        Ok(Self {
            store,
            backend: config.backend,
        })
    }
}

// Proto3 can't tell an unset number from zero, so zero keeps the default. A negative value is
// a bad config.
fn duration(
    field: &str,
    value: Option<prost_types::Duration>,
) -> RedisStoreResult<Option<Duration>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let duration = Duration::try_from(value)
        .map_err(|_| RedisStoreError::invalid(format!("The config's {field} is negative")))?;
    Ok((!duration.is_zero()).then_some(duration))
}

fn count(field: &str, value: i32) -> RedisStoreResult<Option<usize>> {
    let count = usize::try_from(value)
        .map_err(|_| RedisStoreError::invalid(format!("The config's {field} is negative")))?;
    Ok((count > 0).then_some(count))
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
        Ok(Self::create(RuntimeOptions::try_from(config)?, meter))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_config_fills_the_defaults() {
        let options = RuntimeOptions::try_from(RedisStreamConfig {
            urls: vec!["redis://x:1".to_string()],
            backend: "redis-a".to_string(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(options.backend, "redis-a");
        assert_eq!(options.store, RedisStoreOptions::new("redis://x:1"));
        assert!(RuntimeOptions::try_from(RedisStreamConfig::default()).is_err());
        let waits = RuntimeOptions::try_from(RedisStreamConfig {
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
        let negative = RuntimeOptions::try_from(RedisStreamConfig {
            urls: vec!["redis://x:1".to_string()],
            retention: Some(prost_types::Duration {
                seconds: -1,
                nanos: 0,
            }),
            ..Default::default()
        });
        assert!(matches!(negative, Err(RedisStoreError::Invalid(_))));
    }
}
