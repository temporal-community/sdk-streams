//! The answers of the Lua scripts, decoded once into typed values. Lua has no bool, and a nil would
//! end an answer's array, so a script gives `''` for none and `1` for true.

use super::records::LogEntry;
use prost::Message;
use redis::{FromRedisValue, ParsingError, Value};
use std::time::Duration;
use temporalio_streams::{
    offset::offset_of_entry_id,
    proto::Producer,
    store::{Appended, Closed, CreateOutcome, Created, StreamState},
};

/// The answer of `create.lua`, `[outcome, Retention ms, flag]`. The flag is set when the create
/// withdrew a previous run's Records.
pub(super) struct CreateReply(pub(super) Created);

impl FromRedisValue for CreateReply {
    fn from_redis_value(value: Value) -> Result<Self, ParsingError> {
        let (outcome, retention, Flag(flagged)): (String, u64, Flag) =
            FromRedisValue::from_redis_value(value)?;
        let outcome = match outcome.as_str() {
            "created" => CreateOutcome::Created,
            "exists" => CreateOutcome::Exists,
            other => return Err(unknown("create", other)),
        };
        Ok(Self(Created {
            outcome,
            retention: Duration::from_millis(retention),
            flagged,
        }))
    }
}

/// The answer of `append.lua`, `[outcome, first id, last id, flag]`. A batch that wrote no entry
/// gives `''` for both ids.
pub(super) struct AppendReply(pub(super) Appended);

impl FromRedisValue for AppendReply {
    fn from_redis_value(value: Value) -> Result<Self, ParsingError> {
        let (outcome, first, last, Flag(flagged)): (String, String, String, Flag) =
            FromRedisValue::from_redis_value(value)?;
        let resend = match outcome.as_str() {
            "ok" => false,
            "resend" => true,
            other => return Err(unknown("append", other)),
        };
        let offsets = match (first.as_str(), last.as_str()) {
            ("", "") => None,
            (first, last) => Some((offset(first)?, offset(last)?)),
        };
        Ok(Self(Appended {
            offsets,
            resend,
            flagged,
        }))
    }
}

/// The answer of `close.lua`, `[outcome]`.
pub(super) struct CloseReply(pub(super) Closed);

impl FromRedisValue for CloseReply {
    fn from_redis_value(value: Value) -> Result<Self, ParsingError> {
        let (outcome,): (String,) = FromRedisValue::from_redis_value(value)?;
        let already = match outcome.as_str() {
            "closed" => false,
            "already" => true,
            other => return Err(unknown("close", other)),
        };
        Ok(Self(Closed { already }))
    }
}

/// The answer of `read.lua`, `[meta fields, TIME, entries, Producer]`. The Producer is the one of
/// the first entry's batch, when that batch starts before the first entry, and `''` otherwise.
pub(super) struct ReadReply {
    pub(super) state: StreamState,
    pub(super) entries: Vec<LogEntry>,
    pub(super) earlier: Option<Producer>,
}

/// The meta fields that `read.lua` reads, in its order. A field that the Stream doesn't have is
/// nil.
type MetaFields = (
    Option<u32>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<u64>,
    Option<i64>,
    Option<Vec<u8>>,
    Option<i32>,
);

impl FromRedisValue for ReadReply {
    fn from_redis_value(value: Value) -> Result<Self, ParsingError> {
        let (meta, (seconds, micros), entries, producer): (MetaFields, (i64, i64), _, Vec<u8>) =
            FromRedisValue::from_redis_value(value)?;
        let earlier = earlier(&producer)?;
        let now_ms = seconds * 1000 + micros / 1000;
        let (version, closed, newest, trimmed, retention, expires, closer, cstat) = meta;
        // A create sets `v`, `ret` and `exp` together, so a Stream with no `v` doesn't exist.
        let Some(version) = version else {
            let texts = [&closed, &newest, &trimmed];
            if texts.iter().any(|text| text.is_some())
                || retention.is_some()
                || expires.is_some()
                || closer.is_some()
                || cstat.is_some()
            {
                return Err("a Stream has meta fields but no format version".into());
            }
            return Ok(Self {
                state: StreamState {
                    now_ms,
                    ..StreamState::default()
                },
                entries,
                earlier,
            });
        };
        // An offset of zero is no entry, as in `StreamState`.
        let offset_or_zero = |id: Option<String>| id.as_deref().map_or(Ok(0), offset);
        let state = StreamState {
            exists: true,
            version,
            closed: closed.is_some(),
            closer,
            close_status: cstat.unwrap_or(0),
            newest: offset_or_zero(newest)?,
            trimmed: offset_or_zero(trimmed)?,
            retention: Duration::from_millis(retention.ok_or("a Stream has no Retention")?),
            expires_ms: expires.ok_or("a Stream has no expiry")?,
            now_ms,
        };
        Ok(Self {
            state,
            entries,
            earlier,
        })
    }
}

/// The answer of `read_back.lua`, `[entries, Producer]`, with the entries newest first. The
/// Producer is the one of the oldest entry's batch, when that batch starts before that entry, and
/// `''` otherwise.
pub(super) struct ReadBackReply {
    pub(super) entries: Vec<LogEntry>,
    pub(super) earlier: Option<Producer>,
}

impl FromRedisValue for ReadBackReply {
    fn from_redis_value(value: Value) -> Result<Self, ParsingError> {
        let (entries, producer): (_, Vec<u8>) = FromRedisValue::from_redis_value(value)?;
        Ok(Self {
            entries,
            earlier: earlier(&producer)?,
        })
    }
}

/// The Producer that a read script gives for a batch that starts before the page.
fn earlier(producer: &[u8]) -> Result<Option<Producer>, ParsingError> {
    match producer {
        b"" => Ok(None),
        bytes => Producer::decode(bytes)
            .map(Some)
            .map_err(|error| format!("a read script gave a bad Producer: {error}").into()),
    }
}

fn offset(id: &str) -> Result<i64, ParsingError> {
    offset_of_entry_id(id).map_err(|error| error.to_string().into())
}

/// A script's flag, `1` when set and `''` otherwise.
struct Flag(bool);

impl FromRedisValue for Flag {
    fn from_redis_value(value: Value) -> Result<Self, ParsingError> {
        match String::from_redis_value(value)?.as_str() {
            "1" => Ok(Self(true)),
            "" => Ok(Self(false)),
            other => Err(format!("a script gave the unknown flag {other:?}").into()),
        }
    }
}

fn unknown(script: &str, answer: &str) -> ParsingError {
    format!("the {script} script gave the unknown answer {answer:?}").into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn answer(items: &[&str]) -> Value {
        Value::Array(
            items
                .iter()
                .map(|item| Value::BulkString(item.as_bytes().to_vec()))
                .collect(),
        )
    }

    #[test]
    fn a_script_answer_decodes_only_in_its_known_forms() {
        let AppendReply(appended) =
            AppendReply::from_redis_value(answer(&["ok", "", "", ""])).unwrap();
        assert_eq!(
            (appended.offsets, appended.resend, appended.flagged),
            (None, false, false)
        );
        let AppendReply(appended) =
            AppendReply::from_redis_value(answer(&["resend", "1-0", "1-2", "1"])).unwrap();
        assert!(appended.offsets.is_some() && appended.resend && appended.flagged);
        for items in [
            &["done", "", "", ""][..],
            &["ok", "1-0", "", ""],
            &["ok", "", "", "yes"],
            &["ok", "", ""],
        ] {
            assert!(
                AppendReply::from_redis_value(answer(items)).is_err(),
                "{items:?}"
            );
        }
        assert!(CreateReply::from_redis_value(answer(&["made", "1000", ""])).is_err());
        assert!(CreateReply::from_redis_value(answer(&["created", "1000"])).is_err());
        let CreateReply(created) =
            CreateReply::from_redis_value(answer(&["exists", "1000", "1"])).unwrap();
        assert!(created.flagged);
        assert!(CloseReply::from_redis_value(answer(&["closed", "x"])).is_err());
    }

    /// A read reply of a closed Stream carries the closer and the status, so a read builds the
    /// final close item from them.
    #[test]
    fn a_read_reply_carries_the_closer_and_the_status() {
        use temporalio_streams::proto::{ClientEpoch, ClientProducer, producer::Kind};
        let producer = Producer {
            id: "client:me".to_string(),
            kind: Some(Kind::Client(ClientProducer {
                epoch: Some(ClientEpoch { attempt: 1 }),
            })),
        };
        let closer = producer.encode_to_vec();
        let meta = Value::Array(vec![
            Value::BulkString(b"1".to_vec()),
            Value::BulkString(b"1".to_vec()),
            Value::BulkString(b"5-0".to_vec()),
            Value::Nil,
            Value::BulkString(b"1000".to_vec()),
            Value::BulkString(b"123".to_vec()),
            Value::BulkString(closer.clone()),
            Value::BulkString(b"1".to_vec()),
        ]);
        let reply = Value::Array(vec![
            meta,
            Value::Array(vec![
                Value::BulkString(b"0".to_vec()),
                Value::BulkString(b"0".to_vec()),
            ]),
            Value::Array(vec![]),
            Value::BulkString(vec![]),
        ]);
        let ReadReply { state, .. } = ReadReply::from_redis_value(reply).unwrap();
        assert!(state.closed);
        assert_eq!(state.close_status, 1);
        assert_eq!(state.closer.as_deref(), Some(closer.as_slice()));
    }
}
