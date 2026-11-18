#![warn(missing_docs)]

//! Experimental. The shared part of the Stores that keep each Stream as a log.
//!
//! A [store::LogStore] has a few log calls: create, append, read, wait and close. [stream_store]
//! turns one into a [StreamStore](temporalio_streams::StreamStore), which answers the five
//! operations of the Stream service. [stream_runtime] gives a
//! [StreamRuntime](temporalio_streams::StreamRuntime) on that Store. [dispatch::call] is the byte
//! form of the five operations, for a binding in another language. It takes an operation name and
//! the encoded request, and gives the encoded response.
//!
//! The crate does what is the same for every log: the form of an offset, the digest of a batch,
//! the pages and the deadline of a read, and the check that a Stream has expired. A `LogStore`
//! does what must happen in one step with its write: it gives each entry its id, and it applies
//! the fence of a Producer's epochs, the dedupe of a batch that comes again, and Retention.

pub mod digest;
pub mod dispatch;
pub mod ids;
pub mod offset;
pub mod producer_field;
mod read;
mod runtime;
pub mod store;
#[cfg(any(test, feature = "test-support"))]
pub mod testing;

pub use runtime::{stream_runtime, stream_store};
