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

/// The answer of `create.lua`, `[outcome, Retention ms]`.
pub(super) struct CreateReply(pub(super) Created);

impl FromRedisValue for CreateReply {
    fn from_redis_value(value: Value) -> Result<Self, ParsingError> {
        let (outcome, retention): (String, u64) = FromRedisValue::from_redis_value(value)?;
        let outcome = match outcome.as_str() {
            "created" => CreateOutcome::Created,
            "exists" => CreateOutcome::Exists,
            other => return Err(unknown("create", other)),
        };
        Ok(Self(Created {
            outcome,
            retention: Duration::from_millis(retention),
            flagged: false,
        }))
    }
}

/// The answer of `append.lua`, `[outcome, first id, last id, flag]`. A batch that wrote no entry
/// gives `''` for both ids.
pub(super) struct AppendReply(pub(super) Appended);

impl FromRedisValue for AppendReply {
    fn from_redis_value(value: Value) -> Result<Self, ParsingError> {
        let (outcome, first, last, flag): (String, String, String, String) =
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
        let flagged = match flag.as_str() {
            "1" => true,
            "" => false,
            other => return Err(unknown("append", other)),
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
);

impl FromRedisValue for ReadReply {
    fn from_redis_value(value: Value) -> Result<Self, ParsingError> {
        let (meta, (seconds, micros), entries, producer): (MetaFields, (i64, i64), _, Vec<u8>) =
            FromRedisValue::from_redis_value(value)?;
        let earlier = match producer.as_slice() {
            b"" => None,
            bytes => Some(
                Producer::decode(bytes)
                    .map_err(|error| format!("the read script gave a bad Producer: {error}"))?,
            ),
        };
        let now_ms = seconds * 1000 + micros / 1000;
        let (version, closed, newest, trimmed, retention, expires) = meta;
        // A create sets `v`, `ret` and `exp` together, so a Stream with no `v` doesn't exist.
        let Some(version) = version else {
            let texts = [&closed, &newest, &trimmed];
            if texts.iter().any(|text| text.is_some()) || retention.is_some() || expires.is_some() {
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

fn offset(id: &str) -> Result<i64, ParsingError> {
    offset_of_entry_id(id).map_err(|error| error.to_string().into())
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
        assert!(CreateReply::from_redis_value(answer(&["made", "1000"])).is_err());
        assert!(CloseReply::from_redis_value(answer(&["closed", "x"])).is_err());
    }
}
