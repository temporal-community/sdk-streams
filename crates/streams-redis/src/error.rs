//! The errors of the Redis Store. Each one becomes a Stream error at the `Store` trait, with the
//! reason that lang turns into an error class, so a caller gets the same classes on every Store.

use redis::{ErrorKind, RedisError, ServerErrorKind};
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
    /// Redis or its replicas didn't take the call in time, so a retry may fix it.
    #[error("{0}")]
    Unavailable(String),
    /// Redis keeps a value that isn't valid, or gave an answer that the Store can't decode. A
    /// retry can't fix it.
    #[error("{0}")]
    Corrupt(String),
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

    pub(crate) fn unavailable(message: impl Into<String>) -> Self {
        Self::Unavailable(message.into())
    }

    pub(crate) fn corrupt(message: impl Into<String>) -> Self {
        Self::Corrupt(message.into())
    }
}

impl From<RedisStoreError> for StreamError {
    fn from(error: RedisStoreError) -> Self {
        match error {
            RedisStoreError::Redis(error) => from_redis(&error),
            RedisStoreError::Invalid(message) => StreamError::invalid(message),
            RedisStoreError::Unavailable(message) => StreamError::storage_unavailable(message),
            RedisStoreError::Corrupt(message) => StreamError::storage_corrupt(message),
            RedisStoreError::Stream(error) => error,
        }
    }
}

/// The Stream error of a Redis error. Scripts mark their refusals with a `STREAM_` code. A refusal
/// that a retry can't fix is invalid, a key of another type is corrupt, and any other failure is
/// unavailable, so a retry may fix it.
fn from_redis(error: &RedisError) -> StreamError {
    let detail = error.detail().unwrap_or_default().to_string();
    // A bad URL or bad credentials stay wrong on a retry. redis-rs gives some server codes as
    // kinds, and the rest stay strings below.
    match error.kind() {
        ErrorKind::InvalidClientConfig | ErrorKind::AuthenticationFailed => {
            return StreamError::invalid(format!("The Redis config is wrong: {error}"));
        }
        ErrorKind::Server(ServerErrorKind::NoPerm) => {
            return StreamError::invalid(format!(
                "Redis refused the call for good: NOPERM {detail}"
            ));
        }
        ErrorKind::Server(ServerErrorKind::ResponseError) if detail.contains("user_script") => {
            return StreamError::invalid(format!("A Stream script failed: {detail}"));
        }
        ErrorKind::Server(ServerErrorKind::ResponseError) if detail.contains("unknown command") => {
            return StreamError::invalid(format!("Redis is too old for the Store: {detail}"));
        }
        ErrorKind::Server(ServerErrorKind::ResponseError)
            if detail.contains("cluster support disabled") =>
        {
            return StreamError::invalid(format!(
                "The server isn't a Redis Cluster, so don't set `cluster`: {detail}"
            ));
        }
        _ => {}
    }
    let reason = match error.code() {
        Some("STREAM_NOT_FOUND") => Reason::NotFound,
        Some("STREAM_EXPIRED") => Reason::Expired,
        Some("STREAM_CLOSED") => Reason::Closed,
        Some("STREAM_FENCED") => Reason::Fenced,
        Some("STREAM_SEQUENCE_TOO_LOW") => Reason::SequenceTooLow,
        Some("STREAM_ALREADY_EXISTS") => Reason::AlreadyExists,
        Some("STREAM_SEQ_DIVERGENT") => Reason::SeqDivergent,
        Some("STREAM_NOT_OWNER") => Reason::NotOwner,
        Some("STREAM_FORMAT_TOO_NEW") => Reason::FormatTooNew,
        Some("STREAM_INVALID") => Reason::Invalid,
        Some("STREAM_STORAGE_UNAVAILABLE") => Reason::StorageUnavailable,
        Some("STREAM_STORAGE_CORRUPT") => Reason::StorageCorrupt,
        Some("WRONGTYPE") => {
            return StreamError::storage_corrupt(format!(
                "A key of the Stream has another type: {detail}"
            ));
        }
        Some(code @ ("NOAUTH" | "WRONGPASS")) => {
            return StreamError::invalid(format!(
                "Redis refused the call for good: {code} {detail}"
            ));
        }
        // The code as the server wrote it, so the message has the code the Redis docs use.
        Some(code) => {
            return StreamError::storage_unavailable(format!(
                "Redis refused the call: {code} {detail}"
            ));
        }
        None => return StreamError::storage_unavailable(format!("Redis failed: {error}")),
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
    fn a_script_refusal_keeps_its_reason_and_a_server_refusal_gets_its_class() {
        for (code, reason) in [
            ("STREAM_NOT_FOUND", Reason::NotFound),
            ("STREAM_EXPIRED", Reason::Expired),
            ("STREAM_CLOSED", Reason::Closed),
            ("STREAM_FENCED", Reason::Fenced),
            ("STREAM_SEQUENCE_TOO_LOW", Reason::SequenceTooLow),
            ("STREAM_ALREADY_EXISTS", Reason::AlreadyExists),
            ("STREAM_SEQ_DIVERGENT", Reason::SeqDivergent),
            ("STREAM_NOT_OWNER", Reason::NotOwner),
            ("STREAM_FORMAT_TOO_NEW", Reason::FormatTooNew),
            ("STREAM_INVALID", Reason::Invalid),
            ("STREAM_STORAGE_UNAVAILABLE", Reason::StorageUnavailable),
            ("STREAM_STORAGE_CORRUPT", Reason::StorageCorrupt),
        ] {
            let error = server(code, "why it refused");
            assert_eq!(
                (error.reason, error.message.as_str()),
                (reason, "why it refused")
            );
        }
        // A script that a flush or a failover dropped again after its reload is a passing state,
        // and so is a redirect that the cluster client gave up on.
        for code in [
            "READONLY", "OOM", "LOADING", "BUSY", "TRYAGAIN", "NOSCRIPT", "MOVED",
        ] {
            let error = server(code, "x");
            assert_eq!(error.reason, Reason::StorageUnavailable, "{code}");
            assert!(error.message.contains(code), "{error}");
        }
        for (code, detail) in [
            (
                "NOPERM",
                "this user has no permissions to run the 'evalsha' command",
            ),
            (
                "ERR",
                "value is not an integer script: 86f9, on @user_script:59.",
            ),
            (
                "ERR",
                "unknown command 'EVALSHA_RO', with args beginning with:",
            ),
            ("ERR", "This instance has cluster support disabled"),
        ] {
            let error = server(code, detail);
            assert_eq!(error.reason, Reason::Invalid, "{code}");
            assert!(error.message.contains(detail), "{error}");
        }
        let other = server("ERR", "max number of clients reached");
        assert_eq!(other.reason, Reason::StorageUnavailable);
        let wrong_type = server(
            "WRONGTYPE",
            "Operation against a key holding the wrong kind",
        );
        assert_eq!(wrong_type.reason, Reason::StorageCorrupt);
        let url = RedisError::from((ErrorKind::InvalidClientConfig, "Redis URL did not parse"));
        assert_eq!(
            StreamError::from(RedisStoreError::from(url)).reason,
            Reason::Invalid
        );
    }
}
