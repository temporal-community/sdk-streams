//! Turns Redis client errors into Stream errors, so no Redis error reaches lang.

use redis::RedisError;
use temporalio_streams::{Reason, StreamError};

/// The Stream error of a Redis error. Scripts name their refusals with a `STREAM_` code. A refusal
/// that a retry can't fix is invalid, and any other failure is a Store failure that may be retried.
pub(crate) fn stream_error(error: &RedisError) -> StreamError {
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

/// Maps a Redis result.
pub(crate) trait Mapped<T> {
    fn mapped(self) -> Result<T, StreamError>;
}

impl<T> Mapped<T> for Result<T, RedisError> {
    fn mapped(self) -> Result<T, StreamError> {
        self.map_err(|error| stream_error(&error))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn server(code: &str, detail: &str) -> RedisError {
        // Parse it as a wire reply, so that the code and the detail split as in a real reply.
        redis::parse_redis_value(format!("-{code} {detail}\r\n").as_bytes())
            .unwrap()
            .extract_error()
            .unwrap_err()
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
            let error = stream_error(&server(code, "why it refused"));
            assert_eq!(
                (error.reason, error.message.as_str()),
                (reason, "why it refused")
            );
        }
        for code in ["READONLY", "OOM", "LOADING", "BUSY", "TRYAGAIN"] {
            let error = stream_error(&server(code, "x"));
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
            let error = stream_error(&server(code, detail));
            assert_eq!(error.reason, Reason::Invalid, "{code}");
            assert!(error.message.contains(detail), "{error}");
        }
        let other = stream_error(&server("ERR", "max number of clients reached"));
        assert_eq!(other.reason, Reason::Storage);
    }
}
