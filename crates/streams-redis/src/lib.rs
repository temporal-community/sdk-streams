#![warn(missing_docs)]
// A panic in a lang bridge's worker ends the process, so the library returns errors instead.
#![warn(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_possible_truncation
)]

//! The experimental Redis Store of the Temporal Stream runtime. Lang's bridge builds it with
//! [RedisStreamRuntime]. One Lua script runs each create, append and close, so its checks are
//! atomic with its write.

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
