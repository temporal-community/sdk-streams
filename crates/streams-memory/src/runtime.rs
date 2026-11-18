//! The Stream runtime on the memory Store.

use crate::MemoryStore;
use std::{sync::Arc, time::Duration};
use temporalio_common::telemetry::metrics::TemporalMeter;
use temporalio_streams::StreamRuntime;
use temporalio_streams_log::{store::DEFAULT_RETENTION, stream_runtime};

/// The settings of the memory Store runtime. A field left `None` takes its default.
#[derive(Clone, Debug, Default)]
pub struct MemoryStreamOptions {
    /// The Retention of each Stream. `None` is [DEFAULT_RETENTION].
    pub retention: Option<Duration>,
    /// How long an open Stream stays after the last write of any Producer. `None` keeps an open
    /// Stream until its close.
    pub producer_idle_ttl: Option<Duration>,
}

/// The Stream runtime on the memory Store, for tests. It serves the same calls with the same
/// rules as every other Store, and it keeps its Streams in this process only.
#[derive(Debug)]
pub struct MemoryStreamRuntime;

impl MemoryStreamRuntime {
    /// Builds a runtime on a new memory Store. A zero duration in `options` is the same as `None`.
    /// The type only holds this constructor, so it returns the runtime and not `Self`.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        store_name: impl Into<String>,
        options: MemoryStreamOptions,
        meter: Option<TemporalMeter>,
    ) -> Arc<StreamRuntime> {
        let positive = |value: Option<Duration>| value.filter(|value| !value.is_zero());
        let store = Arc::new(MemoryStore::new(
            positive(options.retention).unwrap_or(DEFAULT_RETENTION),
            positive(options.producer_idle_ttl),
        ));
        stream_runtime(store_name, store, meter)
    }
}
