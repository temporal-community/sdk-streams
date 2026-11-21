//! The errors of the Redis Store. At the `StreamStore` trait each one becomes the error of its
//! operation, with the reason that a caller acts on.

use redis::{ErrorKind, ParsingError, RedisError, ServerErrorKind};
use temporalio_streams::{Reason, StreamError, WireReason, proto::StreamErrorReason};

/// An error of the Redis Store.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RedisStoreError {
    /// An option or a request has a value that the Store can't use.
    #[error("{0}")]
    Invalid(String),
    /// Redis failed a call, or a script of the Store refused it.
    #[error(transparent)]
    Redis(#[from] RedisError),
    /// Redis answered a script of the Store with a value that the Store can't convert.
    #[error("The Store can't read an answer of Redis: {0}")]
    UnreadableAnswer(#[from] ParsingError),
    /// Redis or its replicas didn't take a call in time, so a later try may succeed.
    #[error("{0}")]
    Unavailable(String),
}

pub(crate) type RedisStoreResult<T> = Result<T, RedisStoreError>;

impl RedisStoreError {
    /// Returns whether a later try of the same call may succeed with no change by an operator.
    pub(crate) fn can_pass(&self) -> bool {
        match self {
            Self::Invalid(_) | Self::UnreadableAnswer(_) => false,
            Self::Redis(error) => !matches!(error.retry_method(), redis::RetryMethod::NoRetry),
            Self::Unavailable(_) => true,
        }
    }
}

/// Returns the error of an operation for an error of the Store.
///
/// - A script refuses a call with the name of a [`StreamErrorReason`] as its code, and the error
///   has that reason and the script's message. A reason that the operation can't return means
///   that the script is not one of this Store, so it's `STORE_CORRUPT`.
/// - A key of another type, a script that fails at one of its steps and an answer that the
///   Store can't convert are `STORE_CORRUPT`, since a retry reads the same value.
/// - Any other failure of Redis is `STORE_UNAVAILABLE` with the text of Redis. That includes a
///   wrong password, a missing ACL permission, a server that is too old and a server that is no
///   Redis. The request is fine, and the call succeeds once an operator fixes the setup, as it
///   does after an outage.
impl<R: WireReason> From<RedisStoreError> for StreamError<Reason<R>> {
    fn from(error: RedisStoreError) -> Self {
        let error = match error {
            RedisStoreError::Invalid(message) => return Self::invalid_argument(message),
            RedisStoreError::Unavailable(message) => return Self::store_unavailable(message),
            RedisStoreError::UnreadableAnswer(_) => return Self::store_corrupt(error.to_string()),
            RedisStoreError::Redis(error) => error,
        };
        let detail = error.detail().unwrap_or_default();
        if let Some(reason) = error.code().and_then(StreamErrorReason::from_str_name) {
            return Self::from_wire(i32::from(reason), detail);
        }
        match error.kind() {
            // A failure inside a script keeps its code, such as `OOM`. So a script that stops with
            // the plain `ERR` failed at one of its own steps, unless Redis stopped it.
            ErrorKind::Server(ServerErrorKind::ResponseError)
                if detail.contains("@user_script") && !stopped_by_redis(detail) =>
            {
                Self::store_corrupt(format!(
                    "A script of the Store failed on what the Stream holds: {detail}"
                ))
            }
            ErrorKind::Extension if error.code() == Some("WRONGTYPE") => Self::store_corrupt(
                format!("A key of the Stream holds a value of another type: {detail}"),
            ),
            _ => Self::store_unavailable(outage(&error)),
        }
    }
}

/// Returns whether Redis stopped a script for a reason that isn't in what the Stream holds. That
/// is a Redis user that lacks a permission, whose text changed after Redis 7.0, and an operator
/// who sent `SCRIPT KILL`. Redis has no code for either.
fn stopped_by_redis(detail: &str) -> bool {
    [
        "ACL failure in script",
        "The user executing the script",
        "Script killed by user",
    ]
    .iter()
    .any(|text| detail.contains(text))
}

/// Returns the message of a Redis failure that is no answer of a script.
fn outage(error: &RedisError) -> String {
    match error.kind() {
        // Redis 7.0 added `EVALSHA_RO` and the flags of a script, which the Store's reads need.
        ErrorKind::Server(ServerErrorKind::ResponseError)
            if error
                .detail()
                .is_some_and(|detail| detail.contains("unknown command")) =>
        {
            format!("Redis doesn't know a command of the Store, which needs Redis 7.0: {error}")
        }
        // The Store converts the answer of a script itself, so the library failed on bytes that
        // are no reply of Redis.
        ErrorKind::Parse => format!(
            "The server didn't answer as Redis does, so its URL may be of another service: {error}"
        ),
        // The cluster connection goes to the node of a redirect, so only a Store for one server
        // gets one.
        ErrorKind::Server(ServerErrorKind::Moved | ServerErrorKind::Ask) => format!(
            "A node of a Redis Cluster redirected the call, so the Store needs \
             `RedisServers::Cluster`: {error}"
        ),
        // The message has the code as Redis writes it, and not the library's name for it.
        _ => match (error.code(), error.detail()) {
            (Some(code), Some(detail)) => format!("Redis failed: {code} {detail}"),
            _ => format!("Redis failed: {error}"),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use temporalio_streams::{AnyReason, AppendError, AppendReason, DescribeError};

    /// Parses the error from its wire form, so its code and detail split as in a real reply.
    fn reply(text: &str) -> RedisStoreError {
        redis::parse_redis_value(format!("-{text}\r\n").as_bytes())
            .unwrap()
            .extract_error()
            .unwrap_err()
            .into()
    }

    #[test]
    fn a_redis_failure_gets_the_reason_of_its_class() {
        let closed = || reply("STREAM_ERROR_REASON_CLOSED The Stream is closed");
        assert_eq!(
            AppendError::from(closed()),
            AppendError::operation(AppendReason::Closed, "The Stream is closed")
        );
        // A describe can't fail with `CLOSED`, so the answer isn't one of the describe script.
        let error = DescribeError::from(closed());
        assert_eq!(error.reason, Reason::Any(AnyReason::StoreCorrupt));
        // A retry of these reads the same value again, so they must not look like an outage.
        let unreadable = redis::from_redis_value::<i64>(redis::Value::Okay).unwrap_err();
        let corrupt: [RedisStoreError; 3] = [
            reply("WRONGTYPE Operation against a key holding the wrong kind of value"),
            reply(
                "ERR user_script:128: attempt to perform arithmetic on field 'ret' (a boolean \
                 value) script: 4b32, on @user_script:128.",
            ),
            unreadable.into(),
        ];
        for error in corrupt {
            let text = error.to_string();
            let error = AppendError::from(error);
            assert_eq!(error.reason, Reason::Any(AnyReason::StoreCorrupt), "{text}");
        }
        // The first four pass by themselves, and the others need an operator. Workflow code
        // waits for either, so none of them may be a reason that fails the call for good.
        let outages = [
            "READONLY You can't write against a read only replica.",
            "LOADING Redis is loading the dataset in memory",
            "OOM command not allowed when used memory > 'maxmemory'.",
            "TRYAGAIN Multiple keys request during rehashing of slot",
            "NOPERM this user has no permissions to run the 'xadd' command",
            "WRONGPASS invalid username-password pair or user is disabled.",
            "ERR ACL failure in script: User u has no permissions to run the 'xadd' command \
             script: 4b32, on @user_script:169.",
            "ERR The user executing the script can't run this command or subcommand script: \
             4b32, on @user_script:169.",
            "ERR Script killed by user with SCRIPT KILL... script: 4b32, on @user_script:2.",
            "OOM command not allowed when used memory > 'maxmemory'. script: 4b32, on \
             @user_script:169.",
            "MOVED 866 127.0.0.1:7001",
        ];
        for text in outages {
            let error = AppendError::from(reply(text));
            let unavailable = Reason::Any(AnyReason::StoreUnavailable);
            assert_eq!(error.reason, unavailable, "{text}");
            let (_, detail) = text.split_once(' ').unwrap();
            assert!(error.message.contains(detail), "{error}");
        }
        // Bytes that are no reply of Redis, as another service at the URL sends them.
        let http = redis::parse_redis_value(b"HTTP/1.1 400 Bad Request\r\n\r\n").unwrap_err();
        let not_redis = AppendError::from(RedisStoreError::from(http));
        assert_eq!(not_redis.reason, Reason::Any(AnyReason::StoreUnavailable));
        assert!(not_redis.message.contains("another service"), "{not_redis}");
        let old = AppendError::from(reply("ERR unknown command 'EVALSHA_RO', with args"));
        assert_eq!(old.reason, Reason::Any(AnyReason::StoreUnavailable));
        assert!(old.message.contains("needs Redis 7.0"), "{old}");
    }
}
