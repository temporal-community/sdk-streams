//! The errors of the Redis Store. Each one becomes a Stream error at the `Store` trait, with the
//! reason that lang turns into an error class, so a caller gets the same classes on every backend.

use redis::RedisError;
use temporalio_streams::{Reason, StreamError};

/// An error of the Redis Store or of its runtime.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RedisStoreError {
    /// Redis failed the call, or a Stream script refused it.
    #[error(transparent)]
    Redis(#[from] RedisError),
    /// The options are wrong or Redis is too old, so a retry can't fix it.
    #[error("{0}")]
    Invalid(String),
    /// Redis gave an answer that the Store doesn't expect, or too few replicas acknowledged a
    /// write. A retry may fix it.
    #[error("{0}")]
    Storage(String),
    /// A rule of the Stream runtime refused a value that Redis keeps.
    #[error(transparent)]
    Stream(#[from] StreamError),
}

/// The result of a Redis Store call.
pub type RedisStoreResult<T> = Result<T, RedisStoreError>;

impl RedisStoreError {
    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        Self::Invalid(message.into())
    }

    pub(crate) fn storage(message: impl Into<String>) -> Self {
        Self::Storage(message.into())
    }
}

impl From<RedisStoreError> for StreamError {
    fn from(error: RedisStoreError) -> Self {
        match error {
            RedisStoreError::Redis(error) => from_redis(&error),
            RedisStoreError::Invalid(message) => StreamError::invalid(message),
            RedisStoreError::Storage(message) => StreamError::storage(message),
            RedisStoreError::Stream(error) => error,
        }
    }
}

/// The Stream error of a Redis error. Scripts name their refusals with a `STREAM_` code. A refusal
/// that a retry can't fix is invalid, and any other failure is a Store failure that may be retried.
fn from_redis(error: &RedisError) -> StreamError {
    let detail = error.detail().unwrap_or_default().to_string();
    let reason = match error.code() {
        Some("STREAM_NOT_FOUND") => Reason::NotFound,
        Some("STREAM_EXPIRED") => Reason::Expired,
        Some("STREAM_CLOSED") => Reason::Closed,
        Some("STREAM_FENCED") => Reason::Fenced,
        Some("STREAM_SEQUENCE") => Reason::Sequence,
        Some("STREAM_SEQ_DIVERGENT") => Reason::SeqDivergent,
        Some("STREAM_NOT_OWNER") => Reason::NotOwner,
        Some("STREAM_FORMAT_TOO_NEW") => Reason::FormatTooNew,
        Some("STREAM_INVALID") => Reason::Invalid,
        Some("STREAM_STORAGE") => Reason::Storage,
        Some(code @ ("NOPERM" | "WRONGTYPE" | "NOSCRIPT" | "NOAUTH" | "WRONGPASS")) => {
            return StreamError::invalid(format!(
                "Redis refused the call for good: {code} {detail}"
            ));
        }
        Some("ERR") if detail.contains("user_script") => {
            return StreamError::invalid(format!("A Stream script failed: {detail}"));
        }
        // The code as the server wrote it, so the message has the code the Redis docs use.
        Some(code) => {
            return StreamError::storage(format!("Redis refused the call: {code} {detail}"));
        }
        None => return StreamError::storage(format!("Redis failed: {error}")),
    };
    StreamError::new(reason, detail)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn server(code: &str, detail: &str) -> StreamError {
        // Parse it as a wire reply, so that the code and the detail split as in a real reply.
        let error = redis::parse_redis_value(format!("-{code} {detail}\r\n").as_bytes())
            .unwrap()
            .extract_error()
            .unwrap_err();
        RedisStoreError::from(error).into()
    }

    #[test]
    fn a_script_refusal_keeps_its_reason_a_lasting_refusal_is_invalid_and_the_rest_is_storage() {
        for (code, reason) in [
            ("STREAM_NOT_FOUND", Reason::NotFound),
            ("STREAM_EXPIRED", Reason::Expired),
            ("STREAM_CLOSED", Reason::Closed),
            ("STREAM_FENCED", Reason::Fenced),
            ("STREAM_SEQUENCE", Reason::Sequence),
            ("STREAM_SEQ_DIVERGENT", Reason::SeqDivergent),
            ("STREAM_NOT_OWNER", Reason::NotOwner),
            ("STREAM_FORMAT_TOO_NEW", Reason::FormatTooNew),
            ("STREAM_INVALID", Reason::Invalid),
        ] {
            let error = server(code, "why it refused");
            assert_eq!(
                (error.reason, error.message.as_str()),
                (reason, "why it refused")
            );
        }
        for code in ["READONLY", "OOM", "LOADING", "BUSY", "TRYAGAIN"] {
            let error = server(code, "x");
            assert_eq!(error.reason, Reason::Storage, "{code}");
            assert!(error.message.contains(code), "{error}");
        }
        for (code, detail) in [
            (
                "NOPERM",
                "this user has no permissions to run the 'evalsha' command",
            ),
            (
                "WRONGTYPE",
                "Operation against a key holding the wrong kind of value",
            ),
            (
                "ERR",
                "value is not an integer script: 86f9, on @user_script:59.",
            ),
        ] {
            let error = server(code, detail);
            assert_eq!(error.reason, Reason::Invalid, "{code}");
            assert!(error.message.contains(detail), "{error}");
        }
        let other = server("ERR", "max number of clients reached");
        assert_eq!(other.reason, Reason::Storage);
    }
}
