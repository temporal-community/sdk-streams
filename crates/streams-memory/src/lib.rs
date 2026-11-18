#![warn(missing_docs)]

//! Experimental. A Stream Store that keeps its Streams in the memory of one process, for tests.
//! It is a [Store](temporalio_streams_log::store::Store) of `temporalio-streams-log`. It
//! keeps the key layout of the Redis Store and applies the same rules in the same order.

mod memory;
mod runtime;
#[cfg(any(test, feature = "test-support"))]
pub mod testing;

pub use memory::MemoryStore;
pub use runtime::{MemoryStreamOptions, MemoryStreamRuntime};
