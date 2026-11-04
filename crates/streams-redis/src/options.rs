//! The options of the Redis Store. Each type holds only values that the Store can use.

use crate::{RedisStoreError, RedisStoreResult};
use std::{num::NonZeroUsize, str::FromStr, time::Duration};
use temporalio_streams::{
    ids::{DEFAULT_KEY_PREFIX, encode_part},
    store::{DEFAULT_RETENTION, DEFAULT_SAFETY_TTL},
};

/// The Redis connection options and the Stream TTLs.
#[derive(Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct RedisStoreOptions {
    /// One `redis://` or `rediss://` URL, or the seed nodes of a cluster. Credentials and TLS
    /// come from the URL.
    pub urls: Vec<String>,
    /// Whether `urls` are the seeds of a Redis Cluster.
    pub cluster: bool,
    /// The prefix of every key.
    pub key_prefix: KeyPrefix,
    /// The Retention of a Stream whose create sets none.
    pub retention: Duration,
    /// How long an open Workflow-owned Stream stays after its last write.
    pub safety_ttl: Duration,
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
    /// The defaults for one Redis at `url`.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            urls: vec![url.into()],
            ..Self::default()
        }
    }
}

/// The defaults with no URL. A Store needs at least one before it connects.
impl Default for RedisStoreOptions {
    fn default() -> Self {
        Self {
            urls: Vec::new(),
            cluster: false,
            key_prefix: KeyPrefix::default(),
            retention: DEFAULT_RETENTION,
            safety_ttl: DEFAULT_SAFETY_TTL,
            blocking_reads_per_node: DEFAULT_BLOCKING_READS_PER_NODE,
            response_timeout: Duration::from_secs(5),
            wait: None,
        }
    }
}

// The URLs can hold a password, so the debug form shows them with no user info.
impl std::fmt::Debug for RedisStoreOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let urls: Vec<String> = self.urls.iter().map(|url| redacted(url)).collect();
        f.debug_struct("RedisStoreOptions")
            .field("urls", &urls)
            .field("cluster", &self.cluster)
            .field("key_prefix", &self.key_prefix)
            .field("retention", &self.retention)
            .field("safety_ttl", &self.safety_ttl)
            .field("blocking_reads_per_node", &self.blocking_reads_per_node)
            .field("response_timeout", &self.response_timeout)
            .field("wait", &self.wait)
            .finish()
    }
}

/// The URL with `***` in place of its user info and its query. A socket URL gives its password in
/// the query.
fn redacted(url: &str) -> String {
    let (url, query) = match url.split_once('?') {
        Some((url, _)) => (url, "?***"),
        None => (url, ""),
    };
    let start = url.find("://").map_or(0, |scheme| scheme + 3);
    let authority = url[start..]
        .find('/')
        .map_or(url.len(), |path| start + path);
    match url[start..authority].rfind('@') {
        Some(at) => format!("{}***{}{query}", &url[..start], &url[start + at..]),
        None => format!("{url}{query}"),
    }
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
        assert_eq!(KeyPrefix::default().as_str(), DEFAULT_KEY_PREFIX);
    }

    #[test]
    fn the_debug_form_hides_the_user_info_of_a_url() {
        let options = RedisStoreOptions::new("rediss://user:p@ss@redis.example:6380/0");
        let debug = format!("{options:?}");
        assert!(
            !debug.contains("user") && !debug.contains("p@ss"),
            "{debug}"
        );
        assert!(
            debug.contains("rediss://***@redis.example:6380/0"),
            "{debug}"
        );
        assert_eq!(redacted("redis://h:1/0"), "redis://h:1/0");
        assert_eq!(
            redacted("redis+unix:///tmp/redis.sock?db=0&pass=secret"),
            "redis+unix:///tmp/redis.sock?***"
        );
    }
}
