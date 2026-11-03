#![warn(missing_docs)]
// A panic in a lang bridge's worker takes the process down, so the library says why it can't fail.
#![warn(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_possible_truncation
)]

//! The Redis Store of the Temporal Stream runtime. It is experimental.
//!
//! The crate serves the Stream service of `temporalio-streams` on the customer's Redis. One Lua
//! script runs each create, append and close, so the checks are atomic with the write. Lang's
//! bridge builds the runtime with [RedisStreamRuntime] and calls it as `temporalio-streams`
//! describes.

mod error;
mod options;
mod protos;
mod runtime;
mod store;
#[cfg(any(test, feature = "test-support"))]
pub mod testing;

pub use error::{RedisStoreError, RedisStoreResult};
pub use options::{KeyPrefix, RedisStoreOptions, WaitOptions};
pub use runtime::{REDIS_RUNTIME_NAME, RedisStreamRuntime, RuntimeOptions};
pub use store::RedisStore;

/// The config that lang builds for the Redis runtime.
pub mod proto {
    pub use crate::protos::temporal::sdk::streams::v1::RedisStreamConfig;
}
