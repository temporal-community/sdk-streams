//! Offsets. An offset is the id of its Record's log entry, `<ms>-<seq>`, in 16 bytes. So every id
//! that Redis can give has an offset, and offsets sort bytewise in the order of the ids.

use std::{fmt, str::FromStr};

/// The id of a log entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct EntryId {
    pub(crate) ms: u64,
    pub(crate) seq: u64,
}

impl EntryId {
    /// The id before the first entry. Redis gives it to no entry.
    pub(crate) const ZERO: Self = Self { ms: 0, seq: 0 };

    /// Returns the id of an offset, or `None` for bytes that aren't an offset of this Store. An
    /// empty offset is before the first Record.
    pub(crate) fn from_offset(offset: &[u8]) -> Option<Self> {
        if offset.is_empty() {
            return Some(Self::ZERO);
        }
        let (ms, seq) = offset.split_at_checked(8)?;
        Some(Self {
            ms: u64::from_be_bytes(ms.try_into().ok()?),
            seq: u64::from_be_bytes(seq.try_into().ok()?),
        })
    }

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
            assert_eq!(EntryId::from_offset(&offset), Some(*id), "seed {seed}");
            assert_eq!(id.to_string().parse(), Ok(*id), "seed {seed}");
            for other in &ids {
                assert_eq!(
                    offset.cmp(&other.offset()),
                    id.cmp(other),
                    "seed {seed}, {id} and {other}"
                );
            }
        }
        for length in [1, 8, 15, 17] {
            assert_eq!(EntryId::from_offset(&vec![1; length]), None);
        }
    }
}
