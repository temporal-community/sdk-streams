//! The memory Store in the shared suites of `temporalio-streams-log`.

use crate::{MemoryStore, MemoryStreamOptions, MemoryStreamRuntime};
use std::sync::Arc;
use temporalio_streams::StreamRuntime;
use temporalio_streams_log::{
    store::{DEFAULT_RETENTION, Store},
    testing::{Needs, RawKeys, RuntimeConfig, TestStore},
};

/// The memory Store, with a new Store for each case.
#[derive(Debug, Default)]
pub struct MemoryTestStore;

#[async_trait::async_trait]
impl TestStore for MemoryTestStore {
    fn gives(&self) -> Needs {
        Needs::ALL
    }

    async fn connect(&self) -> (Arc<dyn Store>, Arc<dyn RawKeys>) {
        let store = Arc::new(MemoryStore::new(DEFAULT_RETENTION, None));
        (store.clone(), store)
    }

    fn runtime(&self, config: &RuntimeConfig) -> Arc<StreamRuntime> {
        let options = MemoryStreamOptions {
            retention: config.retention,
            producer_idle_ttl: config.producer_idle_ttl,
        };
        MemoryStreamRuntime::new(config.store_name.clone(), options, None)
    }
}

#[async_trait::async_trait]
impl RawKeys for MemoryStore {
    async fn set_meta(&self, stream: &str, field: &[u8], value: &[u8]) {
        self.test_set_meta(stream, field, value)
    }

    async fn meta(&self, stream: &str, field: &[u8]) -> Option<Vec<u8>> {
        self.test_meta(stream, field)
    }

    async fn ttls(&self, stream: &str) -> (i64, i64) {
        self.test_ttls(stream)
    }

    async fn add_raw_entry(&self, stream: &str, id: &str) {
        self.test_add_entry(stream, id)
    }

    async fn log_len(&self, stream: &str) -> usize {
        self.test_log_len(stream)
    }

    async fn drop_log(&self, stream: &str) {
        self.test_drop_log(stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use temporalio_streams_log::testing::{run, service, store};

    #[tokio::test]
    async fn the_store_suite_passes_on_the_memory_store() {
        run(vec![Arc::new(MemoryTestStore)], store::CASES).await;
    }

    #[tokio::test]
    async fn the_service_suite_passes_on_the_memory_store() {
        run(vec![Arc::new(MemoryTestStore)], service::CASES).await;
    }
}
