//! The log keeps the Producer of a batch once, on its first data entry. The Store takes it off
//! each data Record before the append and puts it back on each one after a read.

use super::{bad_reply, text};
use crate::RedisStoreResult;
use prost::Message;
use redis::Value;
use temporalio_streams::{
    offset::offset_of_entry_id,
    proto::{Producer, StreamRecord, stream_record::Item},
    store::Entry,
};

/// The Record with no Producer, when it is a data Record of `producer`.
pub(super) fn without_producer(record: &[u8], producer: &Producer) -> Vec<u8> {
    let Ok(mut stored) = StreamRecord::decode(record) else {
        return record.to_vec();
    };
    match &mut stored.item {
        Some(Item::Data(data)) if data.producer.as_ref() == Some(producer) => {
            data.producer = None;
            stored.encode_to_vec()
        }
        _ => record.to_vec(),
    }
}

/// One log entry as Redis keeps it.
pub(super) struct LogEntry {
    pub(super) id: String,
    record: Option<Vec<u8>>,
    superseded: Option<Vec<u8>>,
    /// The encoded Producer of the batch, on its first data entry.
    producer: Option<Vec<u8>>,
    /// The seq of the batch's first entry, on each later data entry.
    pub(super) batch_start: Option<String>,
}

impl LogEntry {
    pub(super) fn parse(value: Value) -> RedisStoreResult<Self> {
        let Value::Array(pair) = value else {
            return Err(bad_reply("read", value));
        };
        let [id, Value::Array(fields)] =
            <[Value; 2]>::try_from(pair).map_err(|pair| bad_reply("read", pair))?
        else {
            return Err(bad_reply("read", "an entry with no field array"));
        };
        let mut entry = Self {
            id: text(&id).ok_or_else(|| bad_reply("read", &id))?,
            record: None,
            superseded: None,
            producer: None,
            batch_start: None,
        };
        for pair in fields.chunks(2) {
            let (Some(Value::BulkString(name)), Some(Value::BulkString(value))) =
                (pair.first(), pair.get(1))
            else {
                continue;
            };
            match name.as_slice() {
                b"r" => entry.record = Some(value.clone()),
                b"s" => entry.superseded = Some(value.clone()),
                b"p" => entry.producer = Some(value.clone()),
                b"b" => entry.batch_start = String::from_utf8(value.clone()).ok(),
                _ => {}
            }
        }
        Ok(entry)
    }

    /// The encoded Producer of the batch, when this is the batch's first data entry.
    pub(super) fn producer(&self) -> Option<&[u8]> {
        self.producer.as_deref()
    }
}

/// The entries, oldest first, with the batch's Producer on each data Record. `earlier` is the
/// encoded Producer of the first entry's batch, when that batch starts before the first entry.
pub(super) fn with_producers(
    entries: Vec<LogEntry>,
    earlier: Option<&[u8]>,
) -> RedisStoreResult<Vec<Entry>> {
    let decode = |bytes: &[u8]| Producer::decode(bytes).map_err(|error| bad_reply("read", error));
    let mut producer = earlier.map(decode).transpose()?;
    entries
        .into_iter()
        .map(|entry| {
            if let Some(bytes) = &entry.producer {
                producer = Some(decode(bytes)?);
            }
            // A Record that the log keeps whole, such as a close Record, has neither field.
            let split = entry.producer.is_some() || entry.batch_start.is_some();
            let record = match entry.record {
                Some(record) if split => Some(restored(&record, producer.as_ref())?),
                other => other,
            };
            Ok(Entry {
                offset: offset_of_entry_id(&entry.id)?,
                record,
                superseded: entry.superseded,
            })
        })
        .collect()
}

fn restored(record: &[u8], producer: Option<&Producer>) -> RedisStoreResult<Vec<u8>> {
    let mut stored = StreamRecord::decode(record).map_err(|error| bad_reply("read", error))?;
    if let Some(Item::Data(data)) = &mut stored.item
        && data.producer.is_none()
    {
        let producer = producer.ok_or_else(|| bad_reply("read", "a Record with no Producer"))?;
        data.producer = Some(producer.clone());
    }
    Ok(stored.encode_to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use temporalio_streams::proto::{ProducerEpoch, StreamData};

    fn producer(id: &str) -> Producer {
        Producer {
            id: id.to_string(),
            epoch: Some(ProducerEpoch {
                attempt: 1,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn data(producer: Option<Producer>) -> Vec<u8> {
        StreamRecord {
            item: Some(Item::Data(StreamData {
                producer,
                ..Default::default()
            })),
            ..Default::default()
        }
        .encode_to_vec()
    }

    fn entry(
        seq: u32,
        record: Vec<u8>,
        producer: Option<&Producer>,
        start: Option<u32>,
    ) -> LogEntry {
        LogEntry {
            id: format!("1-{seq}"),
            record: Some(record),
            superseded: None,
            producer: producer.map(Message::encode_to_vec),
            batch_start: start.map(|start| start.to_string()),
        }
    }

    #[test]
    fn each_data_record_gets_the_producer_of_its_batch_back() {
        let (a, b) = (producer("a"), producer("b"));
        let bare = without_producer(&data(Some(a.clone())), &a);
        assert_eq!(bare, data(None));
        // Another Producer's Record keeps its own.
        assert_eq!(
            without_producer(&data(Some(b.clone())), &a),
            data(Some(b.clone()))
        );
        let entries = vec![
            entry(1, bare.clone(), None, Some(0)),
            entry(2, data(None), Some(&b), None),
            entry(3, data(None), None, Some(2)),
            // A whole Record, as an older writer or a close keeps it.
            entry(4, data(Some(a.clone())), None, None),
        ];
        let earlier = a.encode_to_vec();
        let records: Vec<_> = with_producers(entries, Some(&earlier))
            .unwrap()
            .into_iter()
            .map(|entry| entry.record.unwrap())
            .collect();
        assert_eq!(
            records,
            [
                data(Some(a.clone())),
                data(Some(b.clone())),
                data(Some(b)),
                data(Some(a))
            ]
        );
    }
}
