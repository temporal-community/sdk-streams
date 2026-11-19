//! The Stream runtime on Redis.

use crate::{
    KeyPrefix, RedisServers, RedisStore, RedisStoreError, RedisStoreOptions, RedisStoreResult,
    WaitOptions, proto::RedisStreamConfig,
};
use prost::Message;
use std::{num::NonZeroUsize, sync::Arc, time::Duration};
use temporalio_common::telemetry::metrics::TemporalMeter;
use temporalio_streams::{StreamResult, StreamRuntime, record::BatchLimits};
use temporalio_streams_log::{
    store::{Capabilities, Fence, LazyStore},
    stream_runtime,
};

/// The options of a Stream runtime on Redis.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct RuntimeOptions {
    /// How to connect to Redis.
    pub store: RedisStoreOptions,
    /// The Store name that lang registered the runtime under, for the refs it builds.
    pub store_name: String,
}

/// The options from the Store's config message. An unset or zero field keeps its default.
impl TryFrom<RedisStreamConfig> for RuntimeOptions {
    type Error = RedisStoreError;

    fn try_from(config: RedisStreamConfig) -> RedisStoreResult<Self> {
        let mut urls = config.urls.into_iter();
        let first = urls
            .next()
            .ok_or_else(|| RedisStoreError::invalid("The Redis Stream Store needs a URL"))?;
        let first = crate::options::parse_url(&first)?;
        let servers = match (config.cluster, urls.len()) {
            (true, _) => RedisServers::Cluster {
                first,
                rest: urls
                    .map(|url| crate::options::parse_url(&url))
                    .collect::<RedisStoreResult<_>>()?,
            },
            (false, 0) => RedisServers::Standalone(first),
            // A standalone Store connects to one server, so the other URLs would have no effect.
            (false, _) => {
                return Err(RedisStoreError::invalid(
                    "A standalone Redis takes one URL, and a cluster's seeds need `cluster`",
                ));
            }
        };
        let defaults = RedisStoreOptions::from(servers);
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
        let limits = batch_limits(
            config.max_record_bytes,
            config.max_batch_bytes,
            config.max_batch_records,
        )?;
        let store = RedisStoreOptions {
            key_prefix,
            retention: duration("retention", config.retention)?.unwrap_or(defaults.retention),
            producer_idle_ttl: duration("producer_idle_ttl", config.producer_idle_ttl)?,
            blocking_reads_per_node: count(
                "blocking_reads_per_node",
                config.blocking_reads_per_node,
            )?
            .unwrap_or(defaults.blocking_reads_per_node),
            response_timeout: duration("response_timeout", config.response_timeout)?
                .unwrap_or(defaults.response_timeout),
            wait,
            limits,
            fence: match config.fence_off {
                true => Fence::Off,
                false => Fence::On,
            },
            ..defaults
        };
        // A bridge builds the runtime at startup and connects later, so a bad config fails here.
        crate::store::check(&store)?;
        Ok(Self {
            store,
            store_name: config.store_name,
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

/// The batch limits of the config. Zero keeps the default, and a setter refuses a limit above it.
fn batch_limits(
    max_record_bytes: i64,
    max_batch_bytes: i64,
    max_batch_records: i32,
) -> RedisStoreResult<BatchLimits> {
    type Set = fn(BatchLimits, usize) -> StreamResult<BatchLimits>;
    let settings: [(&str, i64, Set); 3] = [
        (
            "max_record_bytes",
            max_record_bytes,
            BatchLimits::with_max_record_bytes,
        ),
        (
            "max_batch_bytes",
            max_batch_bytes,
            BatchLimits::with_max_batch_bytes,
        ),
        (
            "max_batch_records",
            i64::from(max_batch_records),
            BatchLimits::with_max_batch_records,
        ),
    ];
    settings
        .into_iter()
        .try_fold(BatchLimits::default(), |limits, (field, value, set)| {
            let value = u64::try_from(value).map_err(|_| {
                RedisStoreError::invalid(format!("The config's {field} is negative"))
            })?;
            if value == 0 {
                return Ok(limits);
            }
            // A value too large for `usize` is still above every limit, so the setter refuses it.
            let value = usize::try_from(value).unwrap_or(usize::MAX);
            set(limits, value).map_err(|error| {
                RedisStoreError::invalid(format!("The config's {}", error.message))
            })
        })
}

/// Builds the Stream runtime on Redis. It connects on the first call that needs Redis, so a bridge
/// can build it with no async runtime.
#[derive(Debug)]
pub struct RedisStreamRuntime;

impl RedisStreamRuntime {
    /// Builds the runtime on Redis. It records metrics on `meter`, lang's runtime meter, if set.
    pub fn create(options: RuntimeOptions, meter: Option<TemporalMeter>) -> Arc<StreamRuntime> {
        let limits = options.store.limits;
        let store = options.store;
        let lazy = LazyStore::new(Capabilities::ALL, move || {
            RedisStore::connect(store.clone())
        });
        stream_runtime(options.store_name, Arc::new(lazy), limits, meter)
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
    use temporalio_common::protos::temporal::api::common::v1::Payload;
    use temporalio_streams::{
        Reason,
        proto::{
            AppendRecord, AppendRecordsRequest, ClientProducer, Producer, StreamRef, producer::Kind,
        },
    };

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

    fn config_with_url() -> RedisStreamConfig {
        RedisStreamConfig {
            urls: vec!["redis://x:1".to_string()],
            ..Default::default()
        }
    }

    // The runtime refuses the batch before the Store, so the test needs no Redis server.
    #[tokio::test]
    async fn a_batch_over_the_limits_of_the_config_is_too_large() {
        let config = RedisStreamConfig {
            max_batch_records: 2,
            ..config_with_url()
        };
        let runtime = RedisStreamRuntime::create(RuntimeOptions::try_from(config).unwrap(), None);
        let append = AppendRecordsRequest {
            r#ref: Some(StreamRef {
                id: "s".to_string(),
                ..Default::default()
            }),
            producer: Some(Producer {
                id: "client:me:p".to_string(),
                kind: Some(Kind::Client(ClientProducer {})),
            }),
            sequence: 1,
            records: vec![
                AppendRecord {
                    body: Some(Payload::default()),
                };
                3
            ],
            ..Default::default()
        };
        let error = runtime.append_records(append).await.unwrap_err();
        assert_eq!(error.reason, Reason::TooLarge);
    }

    #[test]
    fn a_limit_of_the_config_is_at_most_the_default() {
        type Set = fn(&mut RedisStreamConfig, i64);
        type Get = fn(&BatchLimits) -> usize;
        let default = BatchLimits::default();
        let fields: [(&str, usize, Set, Get); 3] = [
            (
                "max_record_bytes",
                default.max_record_bytes(),
                |config, value| config.max_record_bytes = value,
                BatchLimits::max_record_bytes,
            ),
            (
                "max_batch_bytes",
                default.max_batch_bytes(),
                |config, value| config.max_batch_bytes = value,
                BatchLimits::max_batch_bytes,
            ),
            (
                "max_batch_records",
                default.max_batch_records(),
                |config, value| config.max_batch_records = i32::try_from(value).unwrap(),
                BatchLimits::max_batch_records,
            ),
        ];
        let options = |set: Set, value: i64| {
            let mut config = config_with_url();
            set(&mut config, value);
            RuntimeOptions::try_from(config)
        };
        assert_eq!(
            RuntimeOptions::try_from(config_with_url())
                .unwrap()
                .store
                .limits,
            default
        );
        for (field, most, set, get) in fields {
            let most = i64::try_from(most).unwrap();
            let over = format!(
                "The config's {field} must be from 1 to {most}, not {}",
                most + 1
            );
            let negative = format!("The config's {field} is negative");
            for (value, expected) in [(most + 1, over), (-1, negative)] {
                let Err(RedisStoreError::Invalid(message)) = options(set, value) else {
                    panic!("{field} {value} is not refused");
                };
                assert_eq!(message, expected);
            }
            for value in [1, most] {
                let limits = options(set, value).unwrap().store.limits;
                assert_eq!(get(&limits), usize::try_from(value).unwrap(), "{field}");
            }
            assert_eq!(options(set, 0).unwrap().store.limits, default, "{field}");
        }
    }
}
