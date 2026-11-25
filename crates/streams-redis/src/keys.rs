//! The keys of a Stream. `docs/redis.md` describes what each one holds.

use crate::KeyPrefix;

/// The three keys of one Stream. They have one hash tag, so a Redis Cluster keeps them in one slot
/// and one script can use them all.
pub(crate) struct StreamKeys {
    base: String,
    tag: String,
}

impl StreamKeys {
    /// Returns the keys of the Stream `stream_id` under the key prefix `prefix`.
    pub(crate) fn new(prefix: &KeyPrefix, stream_id: &str) -> Self {
        // A `}` in the id would end the hash tag before the id ends, and `%` marks an escape.
        let tag = stream_id.replace('%', "%25").replace('}', "%7D");
        Self {
            base: format!("{}:{{{tag}}}", prefix.as_str()),
            tag,
        }
    }

    /// Returns the slot of the keys in a Redis Cluster, which Redis computes from their hash tag.
    pub(crate) fn slot(&self) -> u16 {
        crc16::State::<crc16::XMODEM>::calculate(self.tag.as_bytes()) % 16384
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
}
