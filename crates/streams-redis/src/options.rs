//! The options of the Redis Store.

use redis::{ConnectionInfo, IntoConnectionInfo};
use std::time::Duration;
use temporalio_streams::AnyError;

/// Whether the Store refuses a write from an earlier attempt of a Producer once a later attempt of
/// it has written to the Stream.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Fence {
    /// The Store accepts the write. Every Record has its Producer and attempt, so a reader can
    /// still tell the attempts apart.
    #[default]
    Off,
    /// The Store refuses the write with `FENCED`.
    On,
}

/// The options of a [`RedisStore`](crate::RedisStore). No option has an upper bound, and
/// [`RedisStore::new`](crate::RedisStore::new) refuses a zero or an empty value.
// It has no `Debug`, since the URL can hold a password.
#[non_exhaustive]
pub struct RedisStoreOptions {
    /// The `redis://` or `rediss://` URL of the server, which also sets the credentials and TLS.
    pub url: String,
    /// The prefix of every key of the Store, `temporal-streams` by default. A key has no Temporal
    /// namespace, so a Stream id is unique in one Redis and key prefix. The prefix has only
    /// letters, digits and the characters `-._~:`, so that a key and an ACL key pattern have it
    /// as written.
    pub key_prefix: String,
    /// How long a Stream stays readable after its close, 7 days by default. A Stream keeps the
    /// Retention of the Store that created it.
    pub retention: Duration,
    /// How long an open Stream stays after its last append. It's `None` by default, and an open
    /// Stream then doesn't expire. A Stream keeps the value of the Store that created it.
    pub producer_idle_ttl: Option<Duration>,
    /// Whether the Store fences. It doesn't by default.
    pub fence: Fence,
    /// The number of batches of each Producer of a Stream whose `request_id` the Store remembers,
    /// 16 by default. An append of the same attempt with a remembered `request_id` is a resend.
    pub resend_window: usize,
    /// How long a call waits for Redis to respond, 5 seconds by default.
    pub response_timeout: Duration,
    /// Maximum number of reads that block on Redis at a time, 256 by default. Each one has its
    /// own connection, and a read over the maximum polls once a second.
    pub max_blocking_connections: usize,
}

impl RedisStoreOptions {
    /// Returns the default options for the server at `url`.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            key_prefix: "temporal-streams".to_string(),
            retention: Duration::from_secs(7 * 24 * 60 * 60),
            producer_idle_ttl: None,
            fence: Fence::Off,
            resend_window: 16,
            response_timeout: Duration::from_secs(5),
            max_blocking_connections: 256,
        }
    }

    pub(crate) fn check(self) -> Result<Settings, AnyError> {
        let invalid = |message: &str| Err(AnyError::invalid_argument(message));
        if self.key_prefix.is_empty() {
            return invalid("key_prefix must not be empty");
        }
        // A `{` would start the hash tag of a key early, and an ACL key pattern reads `*`, `?`,
        // `[` and `\` as a glob.
        let allowed = |c: char| c.is_ascii_alphanumeric() || "-._~:".contains(c);
        if !self.key_prefix.chars().all(allowed) {
            return invalid("key_prefix must have only letters, digits and the characters -._~:");
        }
        if self.resend_window == 0 {
            return invalid("resend_window must be 1 or more");
        }
        if self.response_timeout.is_zero() {
            return invalid("response_timeout must be more than zero");
        }
        if self.max_blocking_connections == 0 {
            return invalid("max_blocking_connections must be 1 or more");
        }
        // The message leaves the URL out, since it can hold a password.
        let server = self.url.as_str().into_connection_info().map_err(|error| {
            let reason = error.detail().unwrap_or("it did not parse");
            AnyError::invalid_argument(format!("The Redis URL is wrong: {reason}"))
        })?;
        Ok(Settings {
            server,
            key_prefix: self.key_prefix,
            retention_ms: millis("retention", self.retention)?,
            producer_idle_ttl_ms: (self.producer_idle_ttl)
                .map(|ttl| millis("producer_idle_ttl", ttl))
                .transpose()?,
            fence: self.fence,
            resend_window: self.resend_window,
            response_timeout: self.response_timeout,
            max_blocking_connections: self.max_blocking_connections,
        })
    }
}

/// The options of a Store after [`RedisStoreOptions::check`].
pub(crate) struct Settings {
    pub(crate) server: ConnectionInfo,
    pub(crate) key_prefix: String,
    pub(crate) retention_ms: u64,
    pub(crate) producer_idle_ttl_ms: Option<u64>,
    pub(crate) fence: Fence,
    pub(crate) resend_window: usize,
    pub(crate) response_timeout: Duration,
    pub(crate) max_blocking_connections: usize,
}

/// The longest TTL that a script gets, in ms. A script adds a TTL to the time, and a Lua number
/// holds an integer exactly only up to 2^53. A longer TTL counts as this one, 142,000 years.
const MAX_TTL_MS: u64 = 1 << 52;

/// Returns a TTL in ms, as the scripts take it.
fn millis(option: &str, ttl: Duration) -> Result<u64, AnyError> {
    let ms = u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX);
    if ms == 0 {
        return Err(AnyError::invalid_argument(format!(
            "{option} must be 1 ms or more"
        )));
    }
    Ok(ms.min(MAX_TTL_MS))
}
