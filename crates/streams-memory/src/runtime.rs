//! The Stream runtime on the memory Store.

use crate::MemoryStore;
use std::{sync::Arc, time::Duration};
use temporalio_common::telemetry::metrics::TemporalMeter;
use temporalio_streams::{StreamRuntime, record::BatchLimits};
use temporalio_streams_log::{
    store::{DEFAULT_RETENTION, Fence},
    stream_runtime,
};

/// The settings of the memory Store runtime. A field left `None` takes its default.
#[derive(Clone, Debug, Default)]
pub struct MemoryStreamOptions {
    /// The Retention of each Stream. `None` is [DEFAULT_RETENTION].
    pub retention: Option<Duration>,
    /// How long an open Stream stays after the last write of any Producer. `None` keeps an open
    /// Stream until its close.
    pub producer_idle_ttl: Option<Duration>,
    /// The limits of one batch. The default is the largest batch that any Store may take.
    pub limits: BatchLimits,
    /// What the Store does with an append or a close of a Producer with a lower epoch than its
    /// newest one. The default refuses it as fenced.
    pub fence: Fence,
}

/// The Stream runtime on the memory Store, for tests. It serves the same calls with the same
/// rules as every other Store, and it keeps its Streams in this process only.
#[derive(Debug)]
pub struct MemoryStreamRuntime;

impl MemoryStreamRuntime {
    /// Builds a runtime on a new memory Store. A zero duration in `options` is the same as `None`.
    pub fn create(
        store_name: impl Into<String>,
        options: MemoryStreamOptions,
        meter: Option<TemporalMeter>,
    ) -> Arc<StreamRuntime> {
        let positive = |value: Option<Duration>| value.filter(|value| !value.is_zero());
        let store = Arc::new(
            MemoryStore::new(
                positive(options.retention).unwrap_or(DEFAULT_RETENTION),
                positive(options.producer_idle_ttl),
            )
            .with_fence(options.fence),
        );
        stream_runtime(store_name, store, options.limits, meter)
    }
}
