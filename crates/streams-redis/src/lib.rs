#![warn(missing_docs)]
// A panic in a lang bridge's worker ends the process, so the library returns errors instead.
#![warn(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_possible_truncation
)]

//! The experimental Redis Store of Temporal Streams.
