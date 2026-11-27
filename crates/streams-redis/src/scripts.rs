//! Script headers define argument and response order for the Rust conversions.

use crate::{
    Fence,
    offset::EntryId,
    options::{Millis, Wait},
    producer::{AttemptKey, RequestKey},
};
use prost::Message;
use redis::{
    Cmd, ErrorKind, FromRedisValue, ParsingError, RedisResult, RedisWrite, Script, ServerErrorKind,
    ToRedisArgs, Value, aio::ConnectionLike, from_redis_value,
};
use std::{num::NonZeroUsize, sync::LazyLock, time::Duration};
use temporalio_common::protos::temporal::api::common::v1::Payload;
use temporalio_streams::proto::Producer;

const STATE: &str = include_str!("../lua/state.lua");
const WRITE: &str = include_str!("../lua/write.lua");

pub(crate) static CREATE: LazyLock<Script> =
    LazyLock::new(|| write_script(include_str!("../lua/create.lua")));
pub(crate) static APPEND: LazyLock<Script> =
    LazyLock::new(|| write_script(include_str!("../lua/append.lua")));
pub(crate) static READ: LazyLock<Script> =
    LazyLock::new(|| read_script(include_str!("../lua/read.lua")));
pub(crate) static DESCRIBE: LazyLock<Script> =
    LazyLock::new(|| read_script(include_str!("../lua/describe.lua")));

fn write_script(body: &str) -> Script {
    Script::new(&[STATE, WRITE, body].concat())
}

// Read-only scripts permit a Redis user with read-only key access.
fn read_script(body: &str) -> Script {
    Script::new(&["#!lua flags=no-writes\n", STATE, body].concat())
}

pub(crate) struct ScriptCall {
    script: &'static Script,
    cmd: Cmd,
}

impl ScriptCall {
    pub(crate) fn write(script: &'static Script, keys: &[String], args: impl ToRedisArgs) -> Self {
        Self::new("EVALSHA", script, keys, args)
    }

    pub(crate) fn read(script: &'static Script, keys: &[String], args: impl ToRedisArgs) -> Self {
        Self::new("EVALSHA_RO", script, keys, args)
    }

    fn new(
        command: &str,
        script: &'static Script,
        keys: &[String],
        args: impl ToRedisArgs,
    ) -> Self {
        let mut cmd = redis::cmd(command);
        cmd.arg(script.get_hash())
            .arg(keys.len())
            .arg(keys)
            .arg(args);
        Self { script, cmd }
    }

    // `NOSCRIPT` means nothing ran, so loading and retrying cannot duplicate a write.
    // The caller decodes the result to distinguish corrupt data from protocol failures.
    pub(crate) async fn run(&self, conn: &mut impl ConnectionLike) -> RedisResult<Value> {
        match self.cmd.query_async(conn).await {
            Err(error) if error.kind() == ErrorKind::Server(ServerErrorKind::NoScript) => {
                self.script.load_async(conn).await?;
                self.cmd.query_async(conn).await
            }
            result => result,
        }
    }

    // `WAIT` covers this connection's last write. Pipeline both commands to prevent interleaving.
    pub(crate) async fn run_and_wait(
        &self,
        conn: &mut impl ConnectionLike,
        wait: Wait,
    ) -> RedisResult<(Value, usize)> {
        let mut pipe = redis::pipe();
        // A pipeline that fails gives one error with no code. With the answers as values, a
        // refusal of the script keeps its code.
        pipe.ignore_errors().add_command(self.cmd.clone());
        pipe.cmd("WAIT").arg(wait.replicas.get()).arg(wait.timeout);
        let mut answers: (Value, Value) = pipe.query_async(conn).await?;
        if let Value::ServerError(error) = &answers.0
            && error.kind() == Some(ServerErrorKind::NoScript)
        {
            self.script.load_async(conn).await?;
            answers = pipe.query_async(conn).await?;
        }
        let (answer, acknowledged) = answers;
        Ok((
            answer.extract_error()?,
            from_redis_value(acknowledged.extract_error()?)?,
        ))
    }
}

impl ToRedisArgs for Fence {
    fn write_redis_args<W: ?Sized + RedisWrite>(&self, out: &mut W) {
        matches!(self, Fence::On).write_redis_args(out);
    }
}

pub(crate) enum Existing {
    Fail,
    Use,
}

impl ToRedisArgs for Existing {
    fn write_redis_args<W: ?Sized + RedisWrite>(&self, out: &mut W) {
        matches!(self, Existing::Use).write_redis_args(out);
    }
}

// A resend must write again for `WAIT` to cover its original batch on this connection.
#[derive(Clone, Copy)]
pub(crate) enum AfterWrite {
    Answer,
    Wait,
}

impl From<Option<Wait>> for AfterWrite {
    fn from(wait: Option<Wait>) -> Self {
        wait.map_or(Self::Answer, |_| Self::Wait)
    }
}

impl ToRedisArgs for AfterWrite {
    fn write_redis_args<W: ?Sized + RedisWrite>(&self, out: &mut W) {
        matches!(self, AfterWrite::Wait).write_redis_args(out);
    }
}

pub(crate) struct NoArgs;

impl ToRedisArgs for NoArgs {
    fn write_redis_args<W: ?Sized + RedisWrite>(&self, _: &mut W) {}
}

pub(crate) struct CreateArgs {
    pub(crate) request: RequestKey,
    pub(crate) retention: Millis,
    pub(crate) existing: Existing,
    pub(crate) then: AfterWrite,
}

impl ToRedisArgs for CreateArgs {
    fn write_redis_args<W: ?Sized + RedisWrite>(&self, out: &mut W) {
        out.write_arg(&self.request);
        self.retention.write_redis_args(out);
        self.existing.write_redis_args(out);
        self.then.write_redis_args(out);
    }
}

pub(crate) struct AppendArgs<'a> {
    pub(crate) field: &'a [u8],
    pub(crate) attempt: AttemptKey,
    pub(crate) request: RequestKey,
    pub(crate) fence: Fence,
    pub(crate) window: NonZeroUsize,
    pub(crate) then: AfterWrite,
    pub(crate) producer: &'a Producer,
    pub(crate) bodies: Vec<Vec<u8>>,
}

impl ToRedisArgs for AppendArgs<'_> {
    fn write_redis_args<W: ?Sized + RedisWrite>(&self, out: &mut W) {
        out.write_arg(self.field);
        out.write_arg(&self.attempt);
        out.write_arg(&self.request);
        self.fence.write_redis_args(out);
        // A Lua number holds `window * 40` exactly only up to 2^53, so a window above `u32::MAX`
        // counts as that. A value of Redis holds 512 MB, so no Producer has that many batches.
        u32::try_from(self.window.get())
            .unwrap_or(u32::MAX)
            .write_redis_args(out);
        self.then.write_redis_args(out);
        out.write_arg(&self.producer.encode_to_vec());
        self.bodies.write_redis_args(out);
    }
}

pub(crate) struct ReadArgs {
    pub(crate) from: EntryId,
    pub(crate) max_records: usize,
    pub(crate) max_bytes: usize,
}

impl ToRedisArgs for ReadArgs {
    fn write_redis_args<W: ?Sized + RedisWrite>(&self, out: &mut W) {
        self.from.write_redis_args(out);
        self.max_records.write_redis_args(out);
        self.max_bytes.write_redis_args(out);
    }
}

pub(crate) enum Created {
    /// This create made the Stream, or it's a retry of the create that did.
    Made,
    /// Another create made the Stream.
    Found,
}

impl FromRedisValue for Created {
    fn from_redis_value(value: Value) -> Result<Self, ParsingError> {
        match i64::from_redis_value(value)? {
            1 => Ok(Self::Made),
            0 => Ok(Self::Found),
            other => Err(format!("The create script answered {other}").into()),
        }
    }
}

pub(crate) struct Appended {
    pub(crate) first: EntryId,
    pub(crate) last: EntryId,
}

impl FromRedisValue for Appended {
    fn from_redis_value(value: Value) -> Result<Self, ParsingError> {
        let (ms, first_seq, last_seq) = from_redis_value(value)?;
        let id = |seq| EntryId { ms, seq };
        Ok(Self {
            first: id(first_seq),
            last: id(last_seq),
        })
    }
}

pub(crate) struct LogEntry {
    pub(crate) id: EntryId,
    /// The Producer of the batch, which only the first entry of a batch has.
    pub(crate) producer: Option<Producer>,
    pub(crate) body: Payload,
}

impl FromRedisValue for LogEntry {
    fn from_redis_value(value: Value) -> Result<Self, ParsingError> {
        let (id, fields): (EntryId, Vec<Vec<u8>>) = from_redis_value(value)?;
        let [name, head, body_name, body] = fields.as_slice() else {
            return Err(format!("The log entry {id} has {} items", fields.len()).into());
        };
        let producer = match (name.as_slice(), body_name.as_slice()) {
            (b"p", b"r") => Some(decode(head)?),
            (b"b", b"r") => None,
            _ => return Err(format!("The log entry {id} has fields of another format").into()),
        };
        Ok(Self {
            id,
            producer,
            body: decode(body)?,
        })
    }
}

fn decode<M: Message + Default>(bytes: &[u8]) -> Result<M, ParsingError> {
    M::decode(bytes).map_err(|error| format!("A stored message doesn't decode: {error}").into())
}

pub(crate) struct Page {
    pub(crate) entries: Vec<LogEntry>,
    /// The Producer of the first entry, when its batch starts before the page.
    pub(crate) earlier: Option<Producer>,
    /// The time until the Stream expires, by the clock of Redis.
    pub(crate) expires_in: Option<Duration>,
}

impl FromRedisValue for Page {
    fn from_redis_value(value: Value) -> Result<Self, ParsingError> {
        let (entries, earlier, expires_in): (_, Option<Vec<u8>>, Option<u64>) =
            from_redis_value(value)?;
        Ok(Self {
            expires_in: expires_in.map(Duration::from_millis),
            entries,
            earlier: earlier.as_deref().map(decode).transpose()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_answer_of_another_format_is_refused() {
        let value = |wire: &[u8]| redis::parse_redis_value(wire).unwrap();
        assert!(matches!(
            Created::from_redis_value(value(b":1\r\n")),
            Ok(Created::Made)
        ));
        assert!(Created::from_redis_value(value(b":2\r\n")).is_err());
        // An entry with the id `5-0`, and the four items of its two fields.
        let entry = |name: &str, body_name: &str| {
            let fields = format!("$1\r\n{name}\r\n$0\r\n\r\n$1\r\n{body_name}\r\n$0\r\n\r\n");
            let wire = format!("*2\r\n$3\r\n5-0\r\n*4\r\n{fields}");
            LogEntry::from_redis_value(value(wire.as_bytes()))
        };
        assert!(entry("p", "r").is_ok_and(|entry| entry.producer.is_some()));
        assert!(entry("b", "r").is_ok_and(|entry| entry.producer.is_none()));
        assert!(entry("x", "r").is_err());
        assert!(entry("p", "x").is_err());
        let short = value(b"*2\r\n$3\r\n5-0\r\n*2\r\n$1\r\np\r\n$0\r\n\r\n");
        assert!(LogEntry::from_redis_value(short).is_err());
    }
}
