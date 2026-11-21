//! The reason of each failed Redis call.

use redis::RedisError;
use temporalio_streams::{Reason, StreamError, WireReason, proto::StreamErrorReason};

/// Returns the error of an operation for a failed Redis call.
///
/// - A script refuses a call with the name of a [`StreamErrorReason`] as its code, and the error
///   has that reason and the script's message. A reason that the operation can't return means
///   that the script is not one of this Store, so it's `STORE_CORRUPT`.
/// - `WRONGTYPE` means that a key of the Stream holds a value of another type, and a script that
///   stops at an error met a value that it can't handle. A retry reads the same value, so both
///   are `STORE_CORRUPT`.
/// - Anything else is `STORE_UNAVAILABLE` with the text of Redis, also a wrong password, a
///   missing ACL permission and a server that is too old. The request is fine, and the call
///   succeeds once an operator fixes the setup, as it does after an outage.
pub(crate) fn from_redis<R: WireReason>(error: &RedisError) -> StreamError<Reason<R>> {
    let code = error.code().unwrap_or_default();
    let detail = error.detail().unwrap_or_default();
    if let Some(reason) = StreamErrorReason::from_str_name(code) {
        return StreamError::from_wire(reason as i32, detail);
    }
    if code == "WRONGTYPE" {
        return StreamError::store_corrupt(format!(
            "A key of the Stream holds a value of another type: {detail}"
        ));
    }
    // A failure that has its own code, such as `OOM` or `READONLY`, keeps that code when a
    // script meets it. So a script that stops with the plain `ERR` failed at one of its steps.
    if code == "ERR" && detail.contains("@user_script") && !acl_failure(detail) {
        return StreamError::store_corrupt(format!(
            "A script of the Store failed on what the Stream holds: {detail}"
        ));
    }
    StreamError::store_unavailable(outage(error))
}

/// Returns whether a script stopped because its Redis user lacks a permission. Redis has no code
/// for it, and its text changed after Redis 7.0.
fn acl_failure(detail: &str) -> bool {
    detail.contains("ACL failure in script") || detail.contains("The user executing the script")
}

/// Returns the message of a Redis failure that is no answer of a script.
pub(crate) fn outage(error: &RedisError) -> String {
    // Redis 7.0 added `EVALSHA_RO` and the flags of a script, which the Store's reads need.
    if error
        .detail()
        .is_some_and(|detail| detail.contains("unknown command"))
    {
        return format!(
            "Redis doesn't know a command of the Store, which needs Redis 7.0: {error}"
        );
    }
    format!("Redis failed: {error}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use temporalio_streams::{AnyReason, AppendError, AppendReason, DescribeError};

    /// Parses the error from its wire form, so its code and detail split as in a real reply.
    fn reply(text: &str) -> RedisError {
        redis::parse_redis_value(format!("-{text}\r\n").as_bytes())
            .unwrap()
            .extract_error()
            .unwrap_err()
    }

    #[test]
    fn a_redis_failure_gets_the_reason_of_its_class() {
        let closed = reply("STREAM_ERROR_REASON_CLOSED The Stream is closed");
        assert_eq!(
            from_redis(&closed),
            AppendError::operation(AppendReason::Closed, "The Stream is closed")
        );
        // A describe can't fail with `CLOSED`, so the answer isn't one of the describe script.
        let error: DescribeError = from_redis(&closed);
        assert_eq!(error.reason, Reason::Any(AnyReason::StoreCorrupt));
        // A retry of these two reads the same value again, so they must not look like an outage.
        let corrupt = [
            "WRONGTYPE Operation against a key holding the wrong kind of value",
            "ERR user_script:128: attempt to perform arithmetic on field 'ret' (a boolean value) \
             script: 4b32, on @user_script:128.",
        ];
        for text in corrupt {
            let error: AppendError = from_redis(&reply(text));
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
            "OOM command not allowed when used memory > 'maxmemory'. script: 4b32, on \
             @user_script:169.",
        ];
        for text in outages {
            let error: AppendError = from_redis(&reply(text));
            assert_eq!(
                error.reason,
                Reason::Any(AnyReason::StoreUnavailable),
                "{text}"
            );
            let (_, detail) = text.split_once(' ').unwrap();
            assert!(error.message.contains(detail), "{error}");
        }
        let old: AppendError = from_redis(&reply("ERR unknown command 'EVALSHA_RO', with args"));
        assert_eq!(old.reason, Reason::Any(AnyReason::StoreUnavailable));
        assert!(old.message.contains("needs Redis 7.0"), "{old}");
    }
}
