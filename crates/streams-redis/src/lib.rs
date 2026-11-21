#![warn(missing_docs)]
// A panic in a lang bridge's worker ends the process, so the library returns errors instead.
#![warn(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_possible_truncation
)]

//! The experimental Redis Store of Temporal Streams. [`RedisStore`] implements the
//! [`StreamStore`](temporalio_streams::StreamStore) trait of `temporalio-streams` on Redis, and
//! `docs/redis.md` describes what it stores.

mod conn;
mod error;
mod keys;
mod offset;
mod options;
mod producer;
mod scripts;
mod store;

pub use error::RedisStoreError;
pub use options::{Fence, KeyPrefix, RedisStoreOptions, RedisUrl};
pub use store::RedisStore;
