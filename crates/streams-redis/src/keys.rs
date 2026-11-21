//! The keys of a Stream. `docs/redis.md` describes what each one holds.

use crate::KeyPrefix;

/// The four keys of one Stream. They have one hash tag, so a Redis Cluster keeps them in one slot
/// and one script can use them all.
pub(crate) struct StreamKeys {
    base: String,
}

impl StreamKeys {
    /// Returns the keys of the Stream `stream_id` under the key prefix `prefix`.
    pub(crate) fn new(prefix: &KeyPrefix, stream_id: &str) -> Self {
        // A `}` in the id would end the hash tag before the id ends, and `%` marks an escape.
        let id = stream_id.replace('%', "%25").replace('}', "%7D");
        Self {
            base: format!("{}:{{{id}}}", prefix.as_str()),
        }
    }

    /// Returns the key of the hash with the state of the Stream.
    pub(crate) fn meta(&self) -> String {
        format!("{}:meta", self.base)
    }

    /// Returns the key of the Redis stream with the Records.
    pub(crate) fn log(&self) -> String {
        format!("{}:log", self.base)
    }

    /// Returns the key of the hash with the state of each Producer.
    pub(crate) fn producers(&self) -> String {
        format!("{}:producers", self.base)
    }

    /// Returns the key of the Redis stream that gets one entry when the Stream closes.
    pub(crate) fn closed(&self) -> String {
        format!("{}:closed", self.base)
    }
}
