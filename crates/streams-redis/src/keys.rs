use crate::KeyPrefix;

// A shared hash tag keeps all keys in one cluster slot for each Lua call.
pub(crate) struct StreamKeys {
    base: String,
    tag: String,
}

impl StreamKeys {
    pub(crate) fn new(prefix: &KeyPrefix, stream_id: &str) -> Self {
        // A `}` in the id would end the hash tag before the id ends, and `%` marks an escape.
        let tag = stream_id.replace('%', "%25").replace('}', "%7D");
        Self {
            base: format!("{}:{{{tag}}}", prefix.as_str()),
            tag,
        }
    }

    pub(crate) fn slot(&self) -> u16 {
        crc16::State::<crc16::XMODEM>::calculate(self.tag.as_bytes()) % 16384
    }

    pub(crate) fn meta(&self) -> String {
        format!("{}:meta", self.base)
    }

    pub(crate) fn log(&self) -> String {
        format!("{}:log", self.base)
    }

    pub(crate) fn producers(&self) -> String {
        format!("{}:producers", self.base)
    }
}
