#![warn(missing_docs)]
// A panic in a lang bridge's worker ends the process, so the library returns errors instead.
#![warn(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_possible_truncation
)]

//! Experimental. A Stream Store that keeps its Streams in the memory of one process, for tests.
//! It is a [LogStore](temporalio_streams_log::store::LogStore) of `temporalio-streams-log`. It
//! keeps the key layout of the Redis Store and applies the same rules in the same order.

mod memory;
mod runtime;
#[cfg(any(test, feature = "test-support"))]
pub mod testing;

pub use memory::MemoryStore;
pub use runtime::{MemoryStreamOptions, MemoryStreamRuntime};
