#![warn(missing_docs)]

//! Experimental. The rules of the Stream service for a Store that keeps each Stream as a log.
//!
//! A [store::LogStore] has a few log calls: create, append, read, wait and close. [stream_store]
//! turns one into a [StreamStore](temporalio_streams::StreamStore), which answers the five
//! operations of the Stream service. It applies the rules in one place, so every Store that is
//! built on it keeps the same ones. [stream_runtime] gives a
//! [StreamRuntime](temporalio_streams::StreamRuntime) on that Store.

pub mod digest;
pub mod ids;
pub mod offset;
pub mod producer_field;
mod read;
mod runtime;
pub mod store;
#[cfg(any(test, feature = "test-support"))]
pub mod testing;

pub use runtime::{stream_runtime, stream_store};
