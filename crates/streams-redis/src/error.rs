//! Error reasons determine whether a caller retries or stops.

use redis::{ErrorKind, ParsingError, RedisError, ServerErrorKind};
use temporalio_streams::{StreamError, WireReason, proto::StreamErrorReason};

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

// Retrying cannot repair stored data. Setup failures can recover after an operator corrects them.
impl<R: WireReason> From<RedisStoreError> for StreamError<R> {
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
            // Plain script errors need a cause check because Redis gives them no specific code.
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

// Redis has no distinct code for ACL failures or `SCRIPT KILL` inside a script.
fn stopped_by_redis(detail: &str) -> bool {
    [
        "ACL failure in script",
        "The user executing the script",
        "Script killed by user",
    ]
    .iter()
    .any(|text| detail.contains(text))
}

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
        // Script results are decoded separately, so this is a protocol failure.
        ErrorKind::Parse => format!(
            "The server didn't answer as Redis does, so its URL may be of another service: {error}"
        ),
        // Cluster connections handle redirects, so this indicates standalone configuration.
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
    use temporalio_streams::{AnyReason, AppendError, AppendReason, DescribeError, DescribeReason};

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
        let fenced = || reply("STREAM_ERROR_REASON_FENCED A later attempt wrote");
        assert_eq!(
            AppendError::from(fenced()),
            AppendError::operation(AppendReason::Fenced, "A later attempt wrote")
        );
        // A describe can't fail with `FENCED`, so the answer isn't one of the describe script.
        let error = DescribeError::from(fenced());
        assert_eq!(error.reason, DescribeReason::Other(AnyReason::StoreCorrupt));
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
            assert_eq!(
                error.reason,
                AppendReason::Other(AnyReason::StoreCorrupt),
                "{text}"
            );
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
            let unavailable = AppendReason::Other(AnyReason::StoreUnavailable);
            assert_eq!(error.reason, unavailable, "{text}");
            let (_, detail) = text.split_once(' ').unwrap();
            assert!(error.message.contains(detail), "{error}");
        }
        // Bytes that are no reply of Redis, as another service at the URL sends them.
        let http = redis::parse_redis_value(b"HTTP/1.1 400 Bad Request\r\n\r\n").unwrap_err();
        let not_redis = AppendError::from(RedisStoreError::from(http));
        assert_eq!(
            not_redis.reason,
            AppendReason::Other(AnyReason::StoreUnavailable)
        );
        assert!(not_redis.message.contains("another service"), "{not_redis}");
        let old = AppendError::from(reply("ERR unknown command 'EVALSHA_RO', with args"));
        assert_eq!(old.reason, AppendReason::Other(AnyReason::StoreUnavailable));
        assert!(old.message.contains("needs Redis 7.0"), "{old}");
    }
}
