//! The Redis key layout. A Stream id is opaque text, so each key takes its bytes.
//! `testdata/ids.json` has the shared vectors of the key layout.
//!
//! ```text
//! <prefix>:{<stream id>}:meta     the create key, the fence, dedupe and close state
//! <prefix>:{<stream id>}:log      the log, one entry per Record
//! <prefix>:{<stream id>}:closed   the entry that a close adds, which wakes a waiting read
//! ```
//!
//! A key has no Temporal namespace, so a Stream id is unique in one Store and key prefix. The part
//! in braces is the Redis Cluster hash tag, so that the keys are in one slot for one script. Each
//! part is percent-encoded, so a `:` or a brace in an id can't make two Streams share a key.

/// The key prefix when the Store options set none.
pub const DEFAULT_KEY_PREFIX: &str = "temporal-streams";

/// Percent-encodes every byte but the unreserved ones, so an id never holds a key separator.
pub fn encode_part(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-' | b'~') {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// The keys of one Stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamKeys {
    base: String,
}

impl StreamKeys {
    /// The keys of `stream_id` under `prefix`.
    pub fn new(prefix: &str, stream_id: &str) -> Self {
        Self {
            base: format!("{}:{{{}}}", encode_part(prefix), encode_part(stream_id)),
        }
    }

    /// The hash with the format version, the fence, dedupe and close state.
    pub fn meta(&self) -> String {
        format!("{}:meta", self.base)
    }

    /// The log.
    pub fn log(&self) -> String {
        format!("{}:log", self.base)
    }

    /// The key that a close adds one entry to. A close writes no log entry, so a waiting read also
    /// waits on this key, and the close wakes it.
    pub fn closed(&self) -> String {
        format!("{}:closed", self.base)
    }
}
