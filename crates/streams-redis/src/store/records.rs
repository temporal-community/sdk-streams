//! The log keeps the Producer of a batch once, on its first data entry. The Store takes it off
//! each data Record before the append and puts it back on each one after a read.

use super::corrupt;
use crate::{RedisStoreError, RedisStoreResult};
use prost::Message;
use redis::{FromRedisValue, ParsingError, Value};
use std::{collections::HashMap, fmt, str::FromStr};
use temporalio_streams::{
    offset::offset_of_entry_id,
    proto::{Producer, StreamRecord, stream_record::Item},
    store::Entry,
};

/// The Record with no Producer, when it is a data Record of `producer`.
pub(super) fn without_producer(record: &[u8], producer: &Producer) -> RedisStoreResult<Vec<u8>> {
    let mut stored = StreamRecord::decode(record).map_err(|error| {
        RedisStoreError::invalid(format!("A Record isn't an encoded StreamRecord: {error}"))
    })?;
    match &mut stored.item {
        Some(Item::Data(data)) if data.producer.as_ref() == Some(producer) => {
            data.producer = None;
            Ok(stored.encode_to_vec())
        }
        _ => Ok(record.to_vec()),
    }
}

/// The id of a log entry, `<ms>-<seq>`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct EntryId {
    ms: u64,
    seq: u64,
}

impl FromStr for EntryId {
    type Err = ParsingError;

    fn from_str(id: &str) -> Result<Self, ParsingError> {
        let bad = || ParsingError::from(format!("{id:?} isn't an entry id"));
        let (ms, seq) = id.split_once('-').ok_or_else(bad)?;
        Ok(Self {
            ms: ms.parse().map_err(|_| bad())?,
            seq: seq.parse().map_err(|_| bad())?,
        })
    }
}

impl fmt::Display for EntryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

/// What a log entry holds. Each data Record has a place in its batch, since the log keeps the
/// batch's Producer only on the first one.
#[derive(Debug, PartialEq)]
pub(super) enum Body {
    /// No Record, as on an entry with only a Superseded item.
    NoRecord,
    /// The first data Record of a batch, with the batch's Producer.
    First { record: Vec<u8>, producer: Producer },
    /// A later data Record of a batch, with the id of the batch's first entry.
    Later { record: Vec<u8>, first: EntryId },
}

/// One log entry as Redis keeps it.
#[derive(Debug, PartialEq)]
pub(super) struct LogEntry {
    pub(super) offset: i64,
    pub(super) body: Body,
}

/// An entry of an `XRANGE` reply, `[id, [field, value, ...]]`. `docs/redis.md` has the fields.
impl FromRedisValue for LogEntry {
    fn from_redis_value(value: Value) -> Result<Self, ParsingError> {
        let (text, mut fields): (String, HashMap<String, Vec<u8>>) =
            FromRedisValue::from_redis_value(value)?;
        let id: EntryId = text.parse()?;
        let offset = offset_of_entry_id(&text).map_err(|error| error.to_string())?;
        let record = fields.remove("r");
        let body = match (record, fields.remove("p"), fields.remove("b")) {
            (None, None, None) => Body::NoRecord,
            (Some(record), Some(producer), None) => Body::First {
                record,
                producer: Producer::decode(producer.as_slice())
                    .map_err(|error| format!("{id} has a bad Producer: {error}"))?,
            },
            (Some(record), None, Some(seq)) => Body::Later {
                record,
                first: EntryId {
                    seq: String::from_utf8(seq)?
                        .parse()
                        .map_err(|_| format!("{id} has a bad batch start"))?,
                    ..id
                },
            },
            _ => return Err(format!("{id} has batch fields that don't fit together").into()),
        };
        Ok(Self { offset, body })
    }
}

/// The entries, oldest first, with the batch's Producer on each data Record. `producer` starts as
/// the Producer of the first entry's batch, when that batch starts before the first entry, and
/// each batch's first entry replaces it.
pub(super) fn with_producers(
    entries: Vec<LogEntry>,
    mut producer: Option<Producer>,
) -> RedisStoreResult<Vec<Entry>> {
    entries
        .into_iter()
        .map(|entry| {
            let record = match entry.body {
                Body::NoRecord => None,
                Body::First {
                    record,
                    producer: first,
                } => Some(restored(&record, producer.insert(first))?),
                Body::Later { record, .. } => {
                    let producer = producer.as_ref().ok_or_else(|| {
                        RedisStoreError::corrupt("A data Record of the log has no Producer")
                    })?;
                    Some(restored(&record, producer)?)
                }
            };
            Ok(Entry {
                offset: entry.offset,
                record,
            })
        })
        .collect()
}

fn restored(record: &[u8], producer: &Producer) -> RedisStoreResult<Vec<u8>> {
    let mut stored =
        StreamRecord::decode(record).map_err(|error| corrupt("A stored Record", error))?;
    if let Some(Item::Data(data)) = &mut stored.item
        && data.producer.is_none()
    {
        data.producer = Some(producer.clone());
    }
    Ok(stored.encode_to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use temporalio_streams::proto::{ClientEpoch, ClientProducer, StreamData, producer::Kind};

    fn producer(id: &str) -> Producer {
        Producer {
            id: id.to_string(),
            kind: Some(Kind::Client(ClientProducer {
                epoch: Some(ClientEpoch { attempt: 1 }),
            })),
        }
    }

    fn data(producer: Option<Producer>) -> Vec<u8> {
        StreamRecord {
            item: Some(Item::Data(StreamData {
                producer,
                ..Default::default()
            })),
        }
        .encode_to_vec()
    }

    fn entry(seq: i64, body: Body) -> LogEntry {
        LogEntry { offset: seq, body }
    }

    #[test]
    fn each_data_record_gets_the_producer_of_its_batch_back() {
        let (a, b) = (producer("a"), producer("b"));
        let bare = without_producer(&data(Some(a.clone())), &a).unwrap();
        assert_eq!(bare, data(None));
        // Another Producer's Record keeps its own.
        assert_eq!(
            without_producer(&data(Some(b.clone())), &a).unwrap(),
            data(Some(b.clone()))
        );
        assert!(without_producer(b"not a Record", &a).is_err());
        let later = |record| Body::Later {
            record,
            first: EntryId { ms: 1, seq: 0 },
        };
        let entries = vec![
            entry(1, later(bare.clone())),
            entry(
                2,
                Body::First {
                    record: data(None),
                    producer: b.clone(),
                },
            ),
            entry(3, later(data(None))),
        ];
        let records: Vec<_> = with_producers(entries, Some(a.clone()))
            .unwrap()
            .into_iter()
            .map(|entry| entry.record.unwrap())
            .collect();
        assert_eq!(
            records,
            [data(Some(a)), data(Some(b.clone())), data(Some(b))]
        );
    }

    #[test]
    fn an_entry_reply_gives_its_place_in_the_batch() {
        let reply = |fields: &[(&str, &[u8])]| {
            let fields = fields
                .iter()
                .flat_map(|(name, value)| {
                    [
                        Value::BulkString(name.as_bytes().to_vec()),
                        Value::BulkString(value.to_vec()),
                    ]
                })
                .collect();
            LogEntry::from_redis_value(Value::Array(vec![
                Value::BulkString(b"7-3".to_vec()),
                Value::Array(fields),
            ]))
        };
        let encoded = producer("a").encode_to_vec();
        assert_eq!(
            reply(&[("r", b"x"), ("b", b"1")]).unwrap().body,
            Body::Later {
                record: b"x".to_vec(),
                first: EntryId { ms: 7, seq: 1 },
            }
        );
        assert_eq!(
            reply(&[("r", b"x"), ("p", &encoded)]).unwrap().body,
            Body::First {
                record: b"x".to_vec(),
                producer: producer("a"),
            }
        );
        assert_eq!(reply(&[("s", b"y")]).unwrap().body, Body::NoRecord);
        // A data entry has a Record and one place in its batch, and a batch start is a number.
        for fields in [
            &[("r", b"x".as_slice())][..],
            &[("r", b"x".as_slice()), ("p", &encoded), ("b", b"1")][..],
            &[("p", &encoded)],
            &[("r", b"x"), ("b", b"one")],
        ] {
            assert!(reply(fields).is_err(), "{fields:?}");
        }
    }
}
