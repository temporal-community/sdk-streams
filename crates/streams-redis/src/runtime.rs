//! The Stream runtime on Redis.

use crate::{
    KeyPrefix, RedisStore, RedisStoreError, RedisStoreOptions, RedisStoreResult, Topology,
    WaitOptions, proto::RedisStreamConfig,
};
use prost::Message;
use std::{num::NonZeroUsize, sync::Arc, time::Duration};
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
        let mut urls = config.urls.into_iter();
        let first = urls
            .next()
            .ok_or_else(|| RedisStoreError::invalid("The Redis Stream backend needs a URL"))?;
        let first = crate::options::parse_url(&first)?;
        let topology = match (config.cluster, urls.len()) {
            (true, _) => Topology::Cluster {
                first,
                rest: urls
                    .map(|url| crate::options::parse_url(&url))
                    .collect::<RedisStoreResult<_>>()?,
            },
            (false, 0) => Topology::Standalone(first),
            // A standalone Store connects to one server, so the other URLs would have no effect.
            (false, _) => {
                return Err(RedisStoreError::invalid(
                    "A standalone Redis takes one URL, and a cluster's seeds need `cluster`",
                ));
            }
        };
        let defaults = RedisStoreOptions::from(topology);
        // A bad timeout is refused even with no replicas, since the config is wrong either way.
        let wait_timeout = duration("wait_timeout", config.wait_timeout)?;
        let wait = count("wait_replicas", config.wait_replicas)?.map(|replicas| WaitOptions {
            replicas,
            timeout: wait_timeout.unwrap_or(WaitOptions::DEFAULT_TIMEOUT),
        });
        let key_prefix = match config.key_prefix.as_str() {
            "" => defaults.key_prefix,
            _ => KeyPrefix::try_from(config.key_prefix)?,
        };
        let store = RedisStoreOptions {
            key_prefix,
            retention: duration("retention", config.retention)?.unwrap_or(defaults.retention),
            safety_ttl: duration("safety_ttl", config.safety_ttl)?.unwrap_or(defaults.safety_ttl),
            blocking_reads_per_node: count(
                "blocking_reads_per_node",
                config.blocking_reads_per_node,
            )?
            .unwrap_or(defaults.blocking_reads_per_node),
            response_timeout: duration("response_timeout", config.response_timeout)?
                .unwrap_or(defaults.response_timeout),
            wait,
            ..defaults
        };
        // A bridge builds the runtime at startup and connects later, so a bad config fails here.
        crate::store::check(&store)?;
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

fn count(field: &str, value: i32) -> RedisStoreResult<Option<NonZeroUsize>> {
    let count = usize::try_from(value)
        .map_err(|_| RedisStoreError::invalid(format!("The config's {field} is negative")))?;
    Ok(NonZeroUsize::new(count))
}

/// Connects to Redis on the runtime's first call that needs Redis.
struct Connect(RedisStoreOptions);

#[async_trait::async_trait]
impl StoreFactory for Connect {
    async fn connect(&self) -> StreamResult<Arc<dyn Store>> {
        Ok(Arc::new(RedisStore::connect(self.0.clone()).await?))
    }
}

/// Builds the Stream runtime on Redis. It connects on the first call that needs Redis, so a bridge
/// can build it with no async runtime, and a Workflow that never publishes needs no Redis.
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
    fn a_bad_config_fails_when_the_runtime_builds() {
        let url = || vec!["redis://x:1".to_string()];
        let negative = Some(prost_types::Duration {
            seconds: -1,
            nanos: 0,
        });
        for bad in [
            RedisStreamConfig::default(),
            RedisStreamConfig {
                urls: vec!["redis://x:1".to_string(), "redis://x:2".to_string()],
                ..Default::default()
            },
            RedisStreamConfig {
                urls: url(),
                retention: negative,
                ..Default::default()
            },
            RedisStreamConfig {
                urls: url(),
                wait_timeout: negative,
                ..Default::default()
            },
        ] {
            assert!(
                matches!(
                    RuntimeOptions::try_from(bad.clone()),
                    Err(RedisStoreError::Invalid(_))
                ),
                "{bad:?}"
            );
        }
    }
}
