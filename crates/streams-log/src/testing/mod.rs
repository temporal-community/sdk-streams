//! The shared suites of the Stream rules. Each Store crate runs them on its own Stores, so that
//! every Store keeps the same rules.

mod support;

pub mod fields;
pub mod service;
pub mod store;

pub use support::Needs;

use crate::store::Store;
use futures_util::{FutureExt, future::join_all};
use std::{future::Future, panic::AssertUnwindSafe, pin::Pin, sync::Arc, time::Duration};
use temporalio_streams::StreamRuntime;

/// The state under a Store's calls, which some cases read or change.
#[async_trait::async_trait]
pub trait RawKeys: Send + Sync {
    /// Sets one meta field of a Stream. Field names and values are bytes, as Redis keeps them.
    async fn set_meta(&self, stream: &str, field: &[u8], value: &[u8]);
    /// One meta field of a Stream.
    async fn meta(&self, stream: &str, field: &[u8]) -> Option<Vec<u8>>;
    /// The ms left on the log and on the meta, as `PTTL` gives them.
    async fn ttls(&self, stream: &str) -> (i64, i64);
    /// Adds a log entry with an explicit id, as `XADD <id>`.
    async fn add_raw_entry(&self, stream: &str, id: &str);
    /// The number of log entries.
    async fn log_len(&self, stream: &str) -> usize;
    /// Deletes the log, as an eviction would.
    async fn drop_log(&self, stream: &str);
}

/// The settings of a runtime under test. A field left `None` keeps the Store's own value.
#[derive(Clone, Debug, Default)]
pub struct RuntimeConfig {
    /// The Store name that the runtime sets on each ref it builds.
    pub store_name: String,
    /// The Retention of each Stream that the runtime creates.
    pub retention: Option<Duration>,
    /// How long an open Stream stays after the last write of any Producer.
    pub producer_idle_ttl: Option<Duration>,
}

/// A Store that the suites run on, with the server under it.
#[async_trait::async_trait]
pub trait TestStore: Send + Sync {
    /// What the Store gives. A case that needs more doesn't run on it.
    fn gives(&self) -> Needs;
    /// A Store for one case, and the state under it.
    async fn connect(&self) -> (Arc<dyn Store>, Arc<dyn RawKeys>);
    /// A runtime on the Store with `config`.
    fn runtime(&self, config: &RuntimeConfig) -> Arc<StreamRuntime>;
}

/// One case of a suite.
pub struct Case {
    /// The name of the case, for the report.
    pub name: &'static str,
    /// Runs the case on each Store under test that gives what it needs.
    pub run: fn() -> Pin<Box<dyn Future<Output = ()>>>,
}

macro_rules! case {
    ($name:ident) => {
        $crate::testing::Case {
            name: stringify!($name),
            run: || Box::pin($name()),
        }
    };
}
pub(crate) use case;

tokio::task_local! {
    static TEST_STORES: Vec<Arc<dyn TestStore>>;
}

/// The Stores of the running suite.
pub(crate) fn test_stores() -> Vec<Arc<dyn TestStore>> {
    TEST_STORES.with(Clone::clone)
}

/// Runs `cases` on `stores`, all together, and then fails with the names of the cases that
/// failed. A case that fails prints its panic as it goes.
pub async fn run(stores: Vec<Arc<dyn TestStore>>, cases: &[Case]) {
    let outcomes = TEST_STORES
        .scope(
            stores,
            join_all(cases.iter().map(|case| async move {
                let passed = AssertUnwindSafe((case.run)()).catch_unwind().await.is_ok();
                (case.name, passed)
            })),
        )
        .await;
    let failed: Vec<_> = outcomes
        .into_iter()
        .filter(|(_, passed)| !passed)
        .map(|(name, _)| name)
        .collect();
    assert!(failed.is_empty(), "these cases failed: {failed:?}");
}
