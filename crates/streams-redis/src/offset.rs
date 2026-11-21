//! Offsets. An offset is the id of its Record's log entry, `<ms>-<seq>`, in 16 bytes. So every id
//! that Redis can give has an offset, and offsets sort bytewise in the order of the ids.

use redis::{FromRedisValue, ParsingError, RedisWrite, ToRedisArgs, Value};
use std::{fmt, str::FromStr};

/// The id of a log entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct EntryId {
    pub(crate) ms: u64,
    pub(crate) seq: u64,
}

impl EntryId {
    /// The id before the first entry. Redis gives it to no entry.
    pub(crate) const ZERO: Self = Self { ms: 0, seq: 0 };

    /// Returns the offset of the id, which has these big-endian fields.
    ///
    /// 1. The `<ms>` of the id (8 bytes)
    /// 2. The `<seq>` of the id (8 bytes)
    ///
    /// [`ZERO`](Self::ZERO) has the empty offset.
    pub(crate) fn offset(self) -> Vec<u8> {
        if self == Self::ZERO {
            return Vec::new();
        }
        [self.ms.to_be_bytes(), self.seq.to_be_bytes()].concat()
    }
}

/// Bytes that aren't an offset of this Store, with their length.
#[derive(Debug, PartialEq, thiserror::Error)]
#[error("it has {0} bytes, and an offset of this Store has 16")]
pub(crate) struct NotAnOffset(usize);

/// Reads an offset. An empty one is before the first Record.
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
        let seed = 0x9e37_79b9_7f4a_7c15_u64;
        let mut state = seed;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            match state % 3 {
                0 => edges[(state >> 8) as usize % edges.len()],
                _ => state >> (state % 64),
            }
        };
        let ids: Vec<EntryId> = (0..400)
            .map(|_| EntryId {
                ms: next(),
                seq: next(),
            })
            .collect();
        for id in &ids {
            let offset = id.offset();
            assert_eq!(EntryId::try_from(&offset[..]), Ok(*id), "seed {seed}");
            assert_eq!(id.to_string().parse(), Ok(*id), "seed {seed}");
            for other in &ids {
                assert_eq!(
                    offset.cmp(&other.offset()),
                    (id.ms, id.seq).cmp(&(other.ms, other.seq)),
                    "seed {seed}, {id} and {other}"
                );
            }
        }
        for length in [1, 8, 15, 17] {
            let bytes = vec![1; length];
            assert_eq!(EntryId::try_from(&bytes[..]), Err(NotAnOffset(length)));
        }
    }
}
