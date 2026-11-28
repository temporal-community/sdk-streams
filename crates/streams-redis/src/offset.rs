//! Big-endian offsets preserve Redis entry order under unsigned byte comparison.

use redis::{FromRedisValue, ParsingError, RedisWrite, ToRedisArgs, Value};
use std::{fmt, str::FromStr};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct EntryId {
    pub(crate) ms: u64,
    pub(crate) seq: u64,
}

impl EntryId {
    // Redis reserves `0-0`, so it represents the position before any Record.
    pub(crate) const ZERO: Self = Self { ms: 0, seq: 0 };

    pub(crate) fn offset(self) -> Vec<u8> {
        if self == Self::ZERO {
            return Vec::new();
        }
        [self.ms.to_be_bytes(), self.seq.to_be_bytes()].concat()
    }
}

#[derive(Debug, PartialEq, thiserror::Error)]
#[error("it has {0} bytes, and an offset of this Store has 16")]
pub(crate) struct NotAnOffset(usize);

impl TryFrom<&[u8]> for EntryId {
    type Error = NotAnOffset;

    fn try_from(offset: &[u8]) -> Result<Self, NotAnOffset> {
        if offset.is_empty() {
            return Ok(Self::ZERO);
        }
        let Some((ms, seq)) = offset.split_first_chunk::<8>() else {
            return Err(NotAnOffset(offset.len()));
        };
        let seq = <[u8; 8]>::try_from(seq).map_err(|_| NotAnOffset(offset.len()))?;
        Ok(Self {
            ms: u64::from_be_bytes(*ms),
            seq: u64::from_be_bytes(seq),
        })
    }
}

impl fmt::Display for EntryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

impl FromStr for EntryId {
    type Err = std::num::ParseIntError;

    fn from_str(id: &str) -> Result<Self, Self::Err> {
        let (ms, seq) = id.split_once('-').unwrap_or((id, ""));
        Ok(Self {
            ms: ms.parse()?,
            seq: seq.parse()?,
        })
    }
}

impl ToRedisArgs for EntryId {
    fn write_redis_args<W: ?Sized + RedisWrite>(&self, out: &mut W) {
        out.write_arg_fmt(self);
    }
}

impl FromRedisValue for EntryId {
    fn from_redis_value(value: Value) -> Result<Self, ParsingError> {
        let id = String::from_redis_value(value)?;
        id.parse()
            .map_err(|error| format!("{id:?} is no entry id: {error}").into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offsets_order_as_their_ids() {
        // Each value has neighbours that sort the other way round as little-endian bytes or as
        // decimal text.
        let edges = [
            0,
            1,
            9,
            10,
            255,
            256,
            1 << 32,
            1 << 53,
            u64::MAX - 1,
            u64::MAX,
        ];
        let ids: Vec<EntryId> = edges
            .iter()
            .flat_map(|&ms| edges.iter().map(move |&seq| EntryId { ms, seq }))
            .collect();
        for id in &ids {
            let offset = id.offset();
            assert_eq!(EntryId::try_from(&offset[..]), Ok(*id), "{id}");
            assert_eq!(id.to_string().parse(), Ok(*id), "{id}");
        }
        for pair in ids.windows(2) {
            let (id, other) = (pair[0], pair[1]);
            assert!(id.offset() < other.offset(), "{id} and {other}");
        }
        for length in [1, 8, 15, 17] {
            let bytes = vec![1; length];
            assert_eq!(EntryId::try_from(&bytes[..]), Err(NotAnOffset(length)));
        }
    }
}
