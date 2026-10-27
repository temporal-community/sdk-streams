#![warn(missing_docs)]

//! The Redis Store of the Temporal Stream runtime. It is experimental.
//!
//! The crate serves the Stream service of `temporalio-streams` on the customer's Redis. One Lua
//! script runs each create, append and close, so the checks are atomic with the write. Lang's
//! bridge builds the runtime with [RedisStreamRuntime] and calls it as `temporalio-streams`
//! describes.

mod protos;
mod redis;
mod runtime;
#[cfg(test)]
mod server {
    include!("../tests/support/server.rs");
}

pub use redis::{RedisStore, RedisStoreOptions};
pub use runtime::{REDIS_RUNTIME_NAME, RedisStreamRuntime, RuntimeOptions};

/// The config that lang builds for the Redis runtime.
pub mod proto {
    pub use crate::protos::temporal::sdk::streams::v1::RedisStreamConfig;
}
