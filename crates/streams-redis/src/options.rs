//! The options of the Redis Store. Each type holds only values that the Store can use.

use crate::error::{RedisStoreError, RedisStoreResult};
use redis::{ConnectionInfo, IntoConnectionInfo, RedisWrite, ToRedisArgs};
use std::{fmt, num::NonZeroUsize, str::FromStr, time::Duration};

/// Whether the Store refuses a write from an earlier attempt of a Producer once a later attempt of
/// it has appended to the Stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fence {
    /// The Store accepts the write. Every Record has its Producer and attempt, so a reader can
    /// still tell the attempts apart.
    Off,
    /// The Store refuses the write with `FENCED`.
    On,
}

/// A Redis server, from its URL. The URL starts with `redis://` or `rediss://`, and it also sets
/// the credentials and TLS.
#[derive(Clone)]
pub struct RedisUrl(ConnectionInfo);

impl RedisUrl {
    pub(crate) fn info(&self) -> &ConnectionInfo {
        &self.0
    }
}

impl FromStr for RedisUrl {
    type Err = RedisStoreError;

    fn from_str(url: &str) -> RedisStoreResult<Self> {
        // The message leaves the URL out, since it can hold a password.
        let info = url.into_connection_info().map_err(|error| {
            let reason = error.detail().unwrap_or("it did not parse");
            RedisStoreError::Invalid(format!("A Redis URL is wrong: {reason}"))
        })?;
        Ok(Self(info))
    }
}

impl TryFrom<String> for RedisUrl {
    type Error = RedisStoreError;

    fn try_from(url: String) -> RedisStoreResult<Self> {
        url.parse()
    }
}

// The debug form has only the address, since a URL can hold a password.
impl fmt::Debug for RedisUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RedisUrl({})", self.0.addr())
    }
}

/// Where the Redis of a Store is.
#[derive(Clone, Debug)]
pub enum RedisServers {
    /// One server.
    Standalone(RedisUrl),
    /// A Redis Cluster, by some of its nodes. The Store finds the other nodes, and it connects to
    /// every node with the credentials and TLS of the first one.
    Cluster {
        /// The first node.
        first: RedisUrl,
        /// The other nodes.
        rest: Vec<RedisUrl>,
    },
}

impl RedisServers {
    /// Returns the server, or the first node of a cluster.
    pub(crate) fn first(&self) -> &RedisUrl {
        match self {
            Self::Standalone(first) | Self::Cluster { first, .. } => first,
        }
    }
}

/// The prefix of every key of the Store, so that Streams can share a Redis with other data. It
/// has only letters, digits and the characters `-._~:`, so that a key and an ACL key pattern have
/// it as written.
#[derive(Clone, Debug)]
pub struct KeyPrefix(String);

impl KeyPrefix {
    /// Returns the prefix as the keys have it.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The prefix `temporal-streams`.
impl Default for KeyPrefix {
    fn default() -> Self {
        Self("temporal-streams".to_string())
    }
}

impl TryFrom<String> for KeyPrefix {
    type Error = RedisStoreError;

    fn try_from(prefix: String) -> RedisStoreResult<Self> {
        if prefix.is_empty() {
            return Err(RedisStoreError::Invalid(
                "key_prefix must not be empty".to_string(),
            ));
        }
        // A `{` would start the hash tag of a key early, and an ACL key pattern reads `*`, `?`,
        // `[` and `\` as a glob.
        let allowed = |c: char| c.is_ascii_alphanumeric() || "-._~:".contains(c);
        if !prefix.chars().all(allowed) {
            return Err(RedisStoreError::Invalid(
                "key_prefix must have only letters, digits and the characters -._~:".to_string(),
            ));
        }
        Ok(Self(prefix))
    }
}

impl FromStr for KeyPrefix {
    type Err = RedisStoreError;

    fn from_str(prefix: &str) -> RedisStoreResult<Self> {
        Self::try_from(prefix.to_string())
    }
}

/// The replicas that a write waits for, with the Redis command `WAIT`.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct WaitForReplicas {
    /// The number of replicas that must have a write before its call succeeds.
    pub replicas: NonZeroUsize,
    /// How long a write waits for them, at least 1 ms. Keep it well below the `response_timeout`
    /// of the Store, or a write that waits this long fails as a call with no response. Redis
    /// ends a wait at its next timer tick, which can be 100 ms after this time.
    pub timeout: Duration,
}

impl WaitForReplicas {
    /// Returns a wait for `replicas` replicas with a timeout of 1 second.
    pub fn new(replicas: NonZeroUsize) -> Self {
        Self {
            replicas,
            timeout: Duration::from_secs(1),
        }
    }
}

const DEFAULT_RESEND_WINDOW: NonZeroUsize = match NonZeroUsize::new(16) {
    Some(window) => window,
    None => panic!("the default is above zero"),
};

const DEFAULT_BLOCKING_CONNECTIONS: NonZeroUsize = match NonZeroUsize::new(256) {
    Some(count) => count,
    None => panic!("the default is above zero"),
};

/// The options of a [`RedisStore`](crate::RedisStore). No option has an upper bound.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct RedisStoreOptions {
    /// Where the Redis is.
    pub servers: RedisServers,
    /// The prefix of every key of the Store, `temporal-streams` by default. A key has no Temporal
    /// namespace, so a Stream id is unique in one Redis and key prefix.
    pub key_prefix: KeyPrefix,
    /// How long a Stream stays readable after its close, 7 days by default and at least 1 ms. A
    /// Stream keeps the Retention of the Store that created it.
    pub retention: Duration,
    /// How long an open Stream stays after its last append, at least 1 ms. It's `None` by
    /// default, and an open Stream then doesn't expire. A Stream keeps the value of the Store
    /// that created it.
    pub producer_idle_ttl: Option<Duration>,
    /// Whether the Store fences. It doesn't by default.
    pub fence: Fence,
    /// The number of batches of each Producer of a Stream whose `request_id` the Store remembers,
    /// 16 by default. An append of the same attempt with a remembered `request_id` is a resend.
    pub resend_window: NonZeroUsize,
    /// How long a call waits for Redis to respond, 5 seconds by default and more than zero.
    pub response_timeout: Duration,
    /// The replicas that each create, append and close waits for. It's `None` by default, and a
    /// write then succeeds once its Redis node has it. Redis replicates after it answers, so a
    /// failover can lose such a write. With a wait, a write that too few replicas have in time
    /// fails with `STORE_UNAVAILABLE`, and a retry with its `request_id` writes nothing again.
    pub wait: Option<WaitForReplicas>,
    /// Maximum number of calls that block on Redis at a time, 256 by default. A read that waits
    /// for a Record and a write that waits for its replicas each have a connection of their
    /// own. The maximum holds for the reads of one node, and again for the writes. A read over
    /// it polls once a second, and a write waits for a free connection.
    pub max_blocking_connections: NonZeroUsize,
}

impl RedisStoreOptions {
    /// Returns the default options for the Redis at `servers`.
    pub fn new(servers: RedisServers) -> Self {
        Self {
            servers,
            key_prefix: KeyPrefix::default(),
            retention: Duration::from_secs(7 * 24 * 60 * 60),
            producer_idle_ttl: None,
            fence: Fence::Off,
            resend_window: DEFAULT_RESEND_WINDOW,
            response_timeout: Duration::from_secs(5),
            wait: None,
            max_blocking_connections: DEFAULT_BLOCKING_CONNECTIONS,
        }
    }
}

/// A time of 1 ms or more, in ms, as a script and `WAIT` take one.
#[derive(Clone, Copy)]
pub(crate) struct Millis(u64);

impl Millis {
    /// Maximum time that a script gets. A script adds a TTL to the time, and a Lua number holds
    /// an integer exactly only up to 2^53. A longer time counts as this one, 142,000 years.
    const MAX: u64 = 1 << 52;

    pub(crate) fn get(self) -> u64 {
        self.0
    }
}

/// A time below 1 ms, which is no [`Millis`].
pub(crate) struct BelowOneMs;

impl TryFrom<Duration> for Millis {
    type Error = BelowOneMs;

    fn try_from(time: Duration) -> Result<Self, BelowOneMs> {
        match u64::try_from(time.as_millis()) {
            Ok(0) => Err(BelowOneMs),
            Ok(ms) => Ok(Self(ms.min(Self::MAX))),
            Err(_) => Ok(Self(Self::MAX)),
        }
    }
}

impl ToRedisArgs for Millis {
    fn write_redis_args<W: ?Sized + RedisWrite>(&self, out: &mut W) {
        self.0.write_redis_args(out);
    }
}

/// The replicas that a write waits for, as `WAIT` takes them.
#[derive(Clone, Copy)]
pub(crate) struct Wait {
    pub(crate) replicas: NonZeroUsize,
    pub(crate) timeout: Millis,
}

/// The options of a Store in the form that its calls use.
pub(crate) struct Settings {
    pub(crate) servers: RedisServers,
    pub(crate) key_prefix: KeyPrefix,
    pub(crate) retention: Millis,
    pub(crate) producer_idle_ttl: Option<Millis>,
    pub(crate) fence: Fence,
    pub(crate) resend_window: NonZeroUsize,
    pub(crate) response_timeout: Duration,
    pub(crate) wait: Option<Wait>,
    pub(crate) max_blocking_connections: NonZeroUsize,
}

impl TryFrom<RedisStoreOptions> for Settings {
    type Error = RedisStoreError;

    fn try_from(options: RedisStoreOptions) -> RedisStoreResult<Self> {
        let millis = |option: &str, time: Duration| {
            Millis::try_from(time).map_err(|BelowOneMs| {
                RedisStoreError::Invalid(format!("{option} must be 1 ms or more"))
            })
        };
        if options.response_timeout.is_zero() {
            return Err(RedisStoreError::Invalid(
                "response_timeout must be more than zero".to_string(),
            ));
        }
        let idle = options.producer_idle_ttl;
        // `WAIT` with a timeout of zero waits with no end.
        let wait = options.wait.map(|wait| {
            let timeout = millis("The timeout of wait", wait.timeout)?;
            Ok::<_, RedisStoreError>(Wait {
                replicas: wait.replicas,
                timeout,
            })
        });
        Ok(Self {
            servers: options.servers,
            key_prefix: options.key_prefix,
            retention: millis("retention", options.retention)?,
            producer_idle_ttl: idle
                .map(|ttl| millis("producer_idle_ttl", ttl))
                .transpose()?,
            fence: options.fence,
            resend_window: options.resend_window,
            response_timeout: options.response_timeout,
            wait: wait.transpose()?,
            max_blocking_connections: options.max_blocking_connections,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_debug_form_of_the_options_has_no_credentials() {
        let url = |host: &str| format!("rediss://user:p4ss@{host}:6380/0").parse().unwrap();
        let cluster = RedisServers::Cluster {
            first: url("a.example"),
            rest: vec![url("b.example")],
        };
        let debug = format!("{:?}", RedisStoreOptions::new(cluster));
        assert!(
            !debug.contains("user") && !debug.contains("p4ss"),
            "{debug}"
        );
        assert!(
            debug.contains("a.example:6380") && debug.contains("b.example:6380"),
            "{debug}"
        );
    }
}
