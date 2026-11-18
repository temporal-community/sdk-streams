//! The value of a Producer's field in the `:meta` hash. The field is `p|<kind>|<producer id>`, and
//! every Store keeps the same bytes in it. `testdata/fields.json` has the shared vectors.

use crate::digest::DIGEST_LEN;
use temporalio_streams::{Reason, StreamError, StreamResult, epoch::EPOCH_KEY_LEN};

/// The length of a [ProducerField] value.
const PRODUCER_FIELD_LEN: usize = EPOCH_KEY_LEN + 8 + 2 * 12 + DIGEST_LEN;

/// An entry id as a [ProducerField] keeps it, the ms and the seq.
pub type EntryId = (u64, u32);

/// The `p|<kind>|<producer id>` field, one per Producer. Its value is 88 bytes. They hold the fence
/// and the last batch's sequence (8), first and last entry ids (12 each, the ms in 8 and the seq in
/// 4, zeros for none) and digest. Numbers are big-endian.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProducerField {
    /// The newest epoch key of the Producer, which is also the epoch of its last batch.
    pub fence: [u8; EPOCH_KEY_LEN],
    /// The last batch's sequence.
    pub sequence: i64,
    /// The last batch's first and last entry ids, `None` when it wrote no entry.
    pub entries: Option<(EntryId, EntryId)>,
    /// The last batch's digest.
    pub digest: [u8; DIGEST_LEN],
}

impl ProducerField {
    /// The 88 bytes of the field's value.
    pub fn encode(&self) -> Vec<u8> {
        let mut value = Vec::with_capacity(PRODUCER_FIELD_LEN);
        value.extend_from_slice(&self.fence);
        value.extend_from_slice(&self.sequence.to_be_bytes());
        let (first, last) = self.entries.unwrap_or_default();
        for (ms, seq) in [first, last] {
            value.extend_from_slice(&ms.to_be_bytes());
            value.extend_from_slice(&seq.to_be_bytes());
        }
        value.extend_from_slice(&self.digest);
        value
    }
}

/// Refuses a value of another length, which a later format may write.
impl TryFrom<&[u8]> for ProducerField {
    type Error = StreamError;

    fn try_from(value: &[u8]) -> StreamResult<Self> {
        let unknown = || {
            StreamError::new(
                Reason::StorageCorrupt,
                "the Producer field has an unknown format",
            )
        };
        if value.len() != PRODUCER_FIELD_LEN {
            return Err(unknown());
        }
        let (fence, rest) = value.split_at(EPOCH_KEY_LEN);
        let (sequence, rest) = rest.split_at(8);
        let (first, rest) = rest.split_at(12);
        let (last, digest) = rest.split_at(12);
        let entry_id = |bytes: &[u8]| -> StreamResult<EntryId> {
            let (ms, seq) = bytes.split_at(8);
            let ms = ms.try_into().map_err(|_| unknown())?;
            let seq = seq.try_into().map_err(|_| unknown())?;
            Ok((u64::from_be_bytes(ms), u32::from_be_bytes(seq)))
        };
        // A batch has both entry ids or neither.
        let entries = match (entry_id(first)?, entry_id(last)?) {
            ((0, 0), (0, 0)) => None,
            ((0, 0), _) | (_, (0, 0)) => return Err(unknown()),
            ids => Some(ids),
        };
        Ok(Self {
            fence: fence.try_into().map_err(|_| unknown())?,
            sequence: i64::from_be_bytes(sequence.try_into().map_err(|_| unknown())?),
            entries,
            digest: digest.try_into().map_err(|_| unknown())?,
        })
    }
}

/// The [EntryId] of an entry id's text, `<ms>-<seq>`.
pub fn entry_id(text: &str) -> StreamResult<EntryId> {
    let bad = || StreamError::new(Reason::StorageCorrupt, "an entry id isn't <ms>-<seq>");
    let (ms, seq) = text.split_once('-').ok_or_else(bad)?;
    Ok((
        ms.parse().map_err(|_| bad())?,
        seq.parse().map_err(|_| bad())?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_producer_field_matches_the_shared_vectors() {
        let vectors: serde_json::Value =
            serde_json::from_str(include_str!("../testdata/fields.json")).unwrap();
        let hex = |text: &str| -> Vec<u8> {
            (0..text.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
                .collect()
        };
        for case in vectors["producer_values"].as_array().unwrap() {
            let text = |name: &str| case[name].as_str().unwrap();
            let key = |name: &str| case[name].as_str().map(|key| hex(key).try_into().unwrap());
            let entries = (!text("first").is_empty()).then(|| {
                (
                    entry_id(text("first")).unwrap(),
                    entry_id(text("last")).unwrap(),
                )
            });
            let field = ProducerField {
                fence: key("fence").unwrap(),
                sequence: case["sequence"].as_i64().unwrap(),
                entries,
                digest: hex(text("digest")).try_into().unwrap(),
            };
            let value = hex(text("value"));
            assert_eq!(field.encode(), value, "{}", text("name"));
            assert_eq!(ProducerField::try_from(value.as_slice()).unwrap(), field);
        }
    }
}
