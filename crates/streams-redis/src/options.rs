//! The options of the Redis Store. Each type holds only values that the Store can use.

use crate::{RedisStoreError, RedisStoreResult};
use std::{num::NonZeroUsize, str::FromStr, time::Duration};
use temporalio_streams::{
    ids::{DEFAULT_KEY_PREFIX, encode_part},
    store::DEFAULT_RETENTION,
};
use url::Url;

/// Where the Store's Redis is. Each server is a `redis://` or `rediss://` URL, which also sets the
/// credentials and TLS.
#[derive(Clone, PartialEq, Eq)]
pub enum Topology<T = String> {
    /// One server.
    Standalone(T),
    /// The seeds of a Redis Cluster. Every node gets the credentials and TLS of the first one.
    Cluster {
        /// The first seed.
        first: T,
        /// The other seeds.
        rest: Vec<T>,
    },
}

impl<T> Topology<T> {
    /// The server, or each seed with the first one first.
    pub fn servers(&self) -> impl Iterator<Item = &T> {
        let (first, rest) = match self {
            Topology::Standalone(first) => (first, &[][..]),
            Topology::Cluster { first, rest } => (first, rest.as_slice()),
        };
        std::iter::once(first).chain(rest)
    }

    /// The same topology with `f` applied to each server, or the first error.
    pub(crate) fn try_map<U, E>(
        &self,
        mut f: impl FnMut(&T) -> Result<U, E>,
    ) -> Result<Topology<U>, E> {
        Ok(match self {
            Topology::Standalone(server) => Topology::Standalone(f(server)?),
            Topology::Cluster { first, rest } => Topology::Cluster {
                first: f(first)?,
                rest: rest.iter().map(&mut f).collect::<Result<_, _>>()?,
            },
        })
    }
}

// The URLs can hold a password, so the debug form shows them with no user info.
impl std::fmt::Debug for Topology<Url> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.try_map(|url| Ok::<_, std::convert::Infallible>(redacted(url))) {
            Ok(Topology::Standalone(url)) => f.debug_tuple("Standalone").field(&url).finish(),
            Ok(Topology::Cluster { first, rest }) => f
                .debug_struct("Cluster")
                .field("first", &first)
                .field("rest", &rest)
                .finish(),
            Err(never) => match never {},
        }
    }
}

/// The Redis connection options and the Stream TTLs.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct RedisStoreOptions {
    /// Where the Redis is.
    pub topology: Topology<Url>,
    /// The prefix of every key.
    pub key_prefix: KeyPrefix,
    /// The Retention of a Stream whose create sets none.
    pub retention: Duration,
    /// How long an open Stream stays after its last write. `None` keeps an open Stream until its
    /// close.
    pub last_write_ttl: Option<Duration>,
    /// How many blocking reads may wait on one node at once. Each has a connection of its own.
    pub blocking_reads_per_node: NonZeroUsize,
    /// How long a short call waits for its answer.
    pub response_timeout: Duration,
    /// The replicas that each create, append and close waits for, if any.
    pub wait: Option<WaitOptions>,
}

const DEFAULT_BLOCKING_READS_PER_NODE: NonZeroUsize = match NonZeroUsize::new(256) {
    Some(count) => count,
    None => panic!("the default is above zero"),
};

impl RedisStoreOptions {
    /// The defaults for one Redis at `url`. The URL is parsed now, so a wrong URL fails here, not
    /// at connect time.
    pub fn new(url: impl AsRef<str>) -> RedisStoreResult<Self> {
        Ok(Topology::Standalone(parse_url(url.as_ref())?).into())
    }
}

/// Parses a server URL once, so the Store carries a typed `Url` and a wrong URL fails early.
pub(crate) fn parse_url(url: &str) -> RedisStoreResult<Url> {
    Url::parse(url)
        .map_err(|error| RedisStoreError::invalid(format!("The Redis URL is wrong. {error}")))
}

/// The defaults for the Redis at `topology`.
impl From<Topology<Url>> for RedisStoreOptions {
    fn from(topology: Topology<Url>) -> Self {
        Self {
            topology,
            key_prefix: KeyPrefix::default(),
            retention: DEFAULT_RETENTION,
            last_write_ttl: None,
            blocking_reads_per_node: DEFAULT_BLOCKING_READS_PER_NODE,
            response_timeout: Duration::from_secs(5),
            wait: None,
        }
    }
}

/// The URL with `***` in place of its user info and its query. A socket URL gives its password in
/// the query, so the query is hidden too.
fn redacted(url: &Url) -> String {
    let mut url = url.clone();
    if !url.username().is_empty() || url.password().is_some() {
        let _ = url.set_username("***");
        let _ = url.set_password(None);
    }
    if url.query().is_some() {
        url.set_query(Some("***"));
    }
    url.to_string()
}

/// How many replicas must have each write before it answers, with `WAIT`. A write that fewer
/// replicas have in `timeout` is a Store failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WaitOptions {
    /// The replicas that must have the write.
    pub replicas: NonZeroUsize,
    /// How long the write waits for them, at least 1 ms and at most half the response timeout.
    pub timeout: Duration,
}

impl WaitOptions {
    /// The timeout when the config sets none.
    pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(1);
}

/// The prefix of every key, so that Streams can share a Redis with other data. It has only
/// letters, digits, `-`, `.`, `_` and `~`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyPrefix(String);

impl KeyPrefix {
    /// The prefix as it appears in the keys and in the ACL key patterns.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for KeyPrefix {
    fn default() -> Self {
        Self(DEFAULT_KEY_PREFIX.to_string())
    }
}

impl TryFrom<String> for KeyPrefix {
    type Error = RedisStoreError;

    fn try_from(prefix: String) -> RedisStoreResult<Self> {
        if prefix.is_empty() {
            return Err(RedisStoreError::invalid("The key prefix must not be empty"));
        }
        // The keys hold the prefix percent-encoded, but the ACL key patterns of `docs/redis.md`
        // show it as written, so a prefix that encoding changes could widen or miss a pattern.
        if encode_part(&prefix) != prefix {
            return Err(RedisStoreError::invalid(
                "The key prefix must have only letters, digits, '-', '.', '_' and '~'",
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_prefix_has_only_the_characters_that_encoding_keeps() {
        for prefix in [
            "",
            "app:streams",
            "app*",
            "app{x}",
            "app}",
            "a?",
            "a[b]",
            "a b",
        ] {
            assert!(
                matches!(
                    prefix.parse::<KeyPrefix>(),
                    Err(RedisStoreError::Invalid(_))
                ),
                "{prefix}"
            );
        }
        assert_eq!(
            "App.streams_2~x".parse::<KeyPrefix>().unwrap().as_str(),
            "App.streams_2~x"
        );
    }

    #[test]
    fn the_debug_form_hides_the_user_info_of_a_url() {
        let options = RedisStoreOptions::new("rediss://user:p@ss@redis.example:6380/0").unwrap();
        let debug = format!("{options:?}");
        assert!(
            !debug.contains("user") && !debug.contains("p@ss"),
            "{debug}"
        );
        assert!(
            debug.contains("rediss://***@redis.example:6380/0"),
            "{debug}"
        );
        let redact = |url: &str| redacted(&Url::parse(url).unwrap());
        assert_eq!(redact("redis://h:1/0"), "redis://h:1/0");
        assert_eq!(
            redact("redis+unix:///tmp/redis.sock?db=0&pass=secret"),
            "redis+unix:///tmp/redis.sock?***"
        );
    }
}
