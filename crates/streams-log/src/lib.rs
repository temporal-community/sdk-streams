#![warn(missing_docs)]
// A panic in a lang bridge's worker ends the process, so the library returns errors instead.
#![warn(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_possible_truncation
)]

//! Experimental. The shared part of the Stores that keep each Stream as a log.
//!
//! A [store::LogStore] has a few log calls: create, append, read, wait and close. [stream_runtime]
//! gives a [StreamRuntime](temporalio_streams::StreamRuntime) on a [store::LogStore].
//! [dispatch::call] is the byte form of the five operations, for a binding in another language. It
//! takes an operation name and the encoded request, and gives the encoded response.
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
// The suites are test code, and a failed unwrap there is a failed case.
#[cfg(feature = "test-support")]
#[allow(clippy::unwrap_used, clippy::expect_used)]
pub mod testing;

pub use runtime::stream_runtime;
