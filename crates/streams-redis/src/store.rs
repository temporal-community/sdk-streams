//! The Redis Store. One Lua script runs each operation, so its checks and its write are one
//! atomic step. `docs/redis.md` describes what the scripts store.

use crate::{
    Fence, RedisStoreOptions,
    conn::Connections,
    error::{from_redis, outage},
    keys::StreamKeys,
    offset::EntryId,
    options::Settings,
    producer::{producer_keys, request_key},
    scripts::{self, ScriptCall},
};
use futures_util::future::BoxFuture;
use prost::Message;
use redis::FromRedisValue;
use std::time::Duration;
use temporalio_common::protos::temporal::api::common::v1::Payload;
use temporalio_streams::{
    AnyError, AppendError, CloseError, CreateError, DescribeError, ReadError, Reason, StreamError,
    StreamStore, WireReason,
    proto::{
        AppendRecordsRequest, AppendRecordsResponse, CloseStreamRequest, CloseStreamResponse,
        CreateStreamRequest, CreateStreamResponse, DescribeStreamRequest, DescribeStreamResponse,
        Producer, ReadRecordsRequest, ReadRecordsResponse, StreamIdConflictPolicy, StreamRecord,
        StreamStatus,
    },
};
use tokio::{sync::OnceCell, time::Instant};

/// The number of Records in a page of a read that sets no `max_records`.
const PAGE_RECORDS: i32 = 1000;

/// The longest that a read waits. An `Instant` can't hold every `wait` that a request can have.
const MAX_WAIT: Duration = Duration::from_secs(30 * 365 * 24 * 60 * 60);

/// A Stream Store that keeps its Streams in Redis 7.0 or later.
///
/// The Store expects the requests that [`StreamRuntime`](temporalio_streams::StreamRuntime)
/// passes on, so wrap it in one. The runtime also takes the limits of a batch, and the Store has
/// none of its own.
pub struct RedisStore {
    settings: Settings,
    conns: OnceCell<Connections>,
}

/// The answer of the read script. It has whether the Stream is closed, the id of its newest
/// Record, the entries of the page with their fields, and the Producer of a batch that starts
/// before the page.
type Page = (i64, String, Vec<(String, Vec<Vec<u8>>)>, Vec<u8>);

impl RedisStore {
    /// Creates a Store. It fails with `INVALID_ARGUMENT` for an option that is zero, empty or
    /// malformed. The Store connects at its first call, so creating it needs no server.
    pub fn new(options: RedisStoreOptions) -> Result<Self, AnyError> {
        Ok(Self {
            settings: options.check()?,
            conns: OnceCell::new(),
        })
    }

    /// Returns the connections, which the first call opens. A failed connect is tried again at
    /// the next call, so a Store that starts before its Redis works once Redis is up.
    async fn conns(&self) -> Result<&Connections, AnyError> {
        let open = || Connections::open(&self.settings);
        let conns = self.conns.get_or_try_init(open).await;
        conns.map_err(|error| AnyError::store_unavailable(outage(&error)))
    }

    fn keys(&self, stream_id: &str) -> StreamKeys {
        StreamKeys::new(&self.settings.key_prefix, stream_id)
    }

    async fn run<T, R>(&self, call: &ScriptCall) -> Result<T, StreamError<Reason<R>>>
    where
        T: FromRedisValue,
        R: WireReason,
    {
        let mut conn = self.conns().await?.shared();
        let reply = call.run(&mut conn).await;
        let reply = reply.map_err(|error| from_redis(&error))?;
        T::from_redis_value(reply).map_err(|error| corrupt("an answer of a script", error))
    }

    /// Runs a script that writes. With the `wait` option, the call succeeds only once enough
    /// replicas have the write.
    async fn write<T, R>(&self, call: &ScriptCall) -> Result<T, StreamError<Reason<R>>>
    where
        T: FromRedisValue,
        R: WireReason,
    {
        let Some(wait) = self.settings.wait else {
            return self.run(call).await;
        };
        let reply = self.conns().await?.write_and_wait(call, wait).await;
        let (reply, acknowledged) = reply.map_err(|error| from_redis(&error))?;
        let (replicas, timeout_ms) = wait;
        if acknowledged < replicas {
            return Err(StreamError::store_unavailable(format!(
                "{acknowledged} of {replicas} replicas have the write after {timeout_ms} ms. A \
                 failover can lose it, and a retry with its request_id doesn't write it again"
            )));
        }
        T::from_redis_value(reply).map_err(|error| corrupt("an answer of a script", error))
    }

    async fn create(
        &self,
        request: CreateStreamRequest,
    ) -> Result<CreateStreamResponse, CreateError> {
        let keys = self.keys(&request.stream_id);
        let use_existing = request.id_conflict_policy == StreamIdConflictPolicy::UseExisting as i32;
        let idle = self.settings.producer_idle_ttl_ms;
        let mut call = ScriptCall::write(&scripts::CREATE, &[keys.meta()]);
        call.arg(&request_key(b"", &request.request_id)[..])
            .arg(self.settings.retention_ms)
            .arg(idle.map(|ms| ms.to_string()).unwrap_or_default())
            .arg(use_existing)
            .arg(self.settings.wait.is_some());
        let created: i64 = self.write(&call).await?;
        Ok(CreateStreamResponse {
            created: created == 1,
        })
    }

    async fn append(
        &self,
        request: AppendRecordsRequest,
    ) -> Result<AppendRecordsResponse, AppendError> {
        let keys = self.keys(&request.stream_id);
        let producer = request.producer.unwrap_or_default();
        let (field, attempt) = producer_keys(&producer)?;
        let mut call = ScriptCall::write(
            &scripts::APPEND,
            &[keys.meta(), keys.log(), keys.producers()],
        );
        call.arg(field)
            .arg(&attempt[..])
            .arg(&request_key(&attempt, &request.request_id)[..])
            .arg(self.settings.fence == Fence::On)
            .arg(self.settings.resend_window)
            .arg(self.settings.wait.is_some())
            .arg(producer.encode_to_vec());
        for record in request.records {
            call.arg(record.body.unwrap_or_default().encode_to_vec());
        }
        let (ms, first_seq, last_seq) = self.write(&call).await?;
        let offset = |seq| EntryId { ms, seq }.offset();
        Ok(AppendRecordsResponse {
            first_offset: offset(first_seq),
            last_offset: offset(last_seq),
        })
    }

    async fn close(&self, request: CloseStreamRequest) -> Result<CloseStreamResponse, CloseError> {
        let keys = self.keys(&request.stream_id);
        let (field, attempt) = producer_keys(&request.producer.unwrap_or_default())?;
        let keys = [keys.meta(), keys.log(), keys.producers(), keys.closed()];
        let mut call = ScriptCall::write(&scripts::CLOSE, &keys);
        call.arg(field)
            .arg(&attempt[..])
            .arg(self.settings.fence == Fence::On)
            .arg(self.settings.wait.is_some());
        let _: i64 = self.write(&call).await?;
        Ok(CloseStreamResponse {})
    }

    async fn read(&self, request: ReadRecordsRequest) -> Result<ReadRecordsResponse, ReadError> {
        // The form of an offset is the Store's own, so the runtime can't check it.
        let Some(from) = EntryId::from_offset(&request.from_offset) else {
            return Err(StreamError::invalid_argument(format!(
                "The from_offset of the read has {} bytes, and an offset of this Store has 16",
                request.from_offset.len()
            )));
        };
        let keys = self.keys(&request.stream_id);
        let max_records = match request.max_records {
            0 => PAGE_RECORDS,
            max => max,
        };
        let mut call = ScriptCall::read(&scripts::READ, &[keys.meta(), keys.log()]);
        call.arg(from.to_string())
            .arg(max_records)
            .arg(request.max_bytes);
        let wait = request.wait.unwrap_or_default();
        let wait = Duration::new(wait.seconds.max(0) as u64, wait.nanos.max(0) as u32);
        let deadline = Instant::now() + wait.min(MAX_WAIT);
        let max_bytes = usize::try_from(request.max_bytes).unwrap_or(0);
        loop {
            let page = page(
                self.run(&call).await?,
                from,
                &request.from_offset,
                max_bytes,
            )?;
            if !page.records.is_empty() || page.done || deadline <= Instant::now() {
                return Ok(page);
            }
            let conns = self.conns().await?;
            let woke = conns.wait_for_entry(&keys, from, deadline).await;
            woke.map_err(|error| from_redis(&error))?;
        }
    }

    async fn describe(
        &self,
        request: DescribeStreamRequest,
    ) -> Result<DescribeStreamResponse, DescribeError> {
        let keys = self.keys(&request.stream_id);
        let call = ScriptCall::read(&scripts::DESCRIBE, &[keys.meta()]);
        let (closed, newest): (i64, String) = self.run(&call).await?;
        let status = match closed {
            1 => StreamStatus::Closed,
            _ => StreamStatus::Open,
        };
        Ok(DescribeStreamResponse {
            status: status as i32,
            last_offset: entry_id(&newest)?.offset(),
        })
    }
}

/// Builds the page of a read that started after `from`. The entries of a batch have its Producer
/// only on the first one, so every later Record gets the Producer of the entry before it.
///
/// The page stops before the Record that takes it over `max_bytes`, unless that is the first
/// one. The script only stops near that limit, since it can't measure an encoded Record. A
/// Record counts as the page encodes it, which is how the runtime counts too.
fn page(
    page: Page,
    from: EntryId,
    from_offset: &[u8],
    max_bytes: usize,
) -> Result<ReadRecordsResponse, AnyError> {
    let (closed, newest, entries, earlier) = page;
    let mut producer = match earlier.as_slice() {
        [] => None,
        earlier => Some(decode::<Producer>(earlier)?),
    };
    let mut records = Vec::with_capacity(entries.len());
    let mut end = from;
    let mut bytes = 0;
    for (id, fields) in entries {
        let id = entry_id(&id)?;
        let [name, value, _, body] = fields.as_slice() else {
            return Err(corrupt(
                "a log entry",
                format!("it has {} items", fields.len()),
            ));
        };
        if name == b"p" {
            producer = Some(decode(value)?);
        }
        let record = StreamRecord {
            producer: producer.clone(),
            body: Some(decode::<Payload>(body)?),
            offset: id.offset(),
        };
        bytes += prost::encoding::message::encoded_len(1, &record);
        if max_bytes > 0 && bytes > max_bytes && !records.is_empty() {
            break;
        }
        records.push(record);
        end = id;
    }
    Ok(ReadRecordsResponse {
        last_offset: match records.last() {
            Some(record) => record.offset.clone(),
            None => from_offset.to_vec(),
        },
        // A closed Stream gets no more Records, so the page that ends at its newest one is the
        // last page.
        done: closed == 1 && end == entry_id(&newest)?,
        records,
    })
}

fn entry_id(id: &str) -> Result<EntryId, AnyError> {
    id.parse().map_err(|error| corrupt("an entry id", error))
}

fn decode<M: Message + Default>(bytes: &[u8]) -> Result<M, AnyError> {
    M::decode(bytes).map_err(|error| corrupt("a stored message", error))
}

/// Returns the error of a value that Redis holds or answers and that the Store can't read. A
/// retry reads the same value.
fn corrupt<E>(what: &str, error: impl std::fmt::Display) -> E
where
    E: From<AnyError>,
{
    AnyError::store_corrupt(format!("The Store can't read {what}: {error}")).into()
}

impl StreamStore for RedisStore {
    fn create_stream(
        &self,
        request: CreateStreamRequest,
    ) -> BoxFuture<'_, Result<CreateStreamResponse, CreateError>> {
        Box::pin(self.create(request))
    }

    fn append_records(
        &self,
        request: AppendRecordsRequest,
    ) -> BoxFuture<'_, Result<AppendRecordsResponse, AppendError>> {
        Box::pin(self.append(request))
    }

    fn read_records(
        &self,
        request: ReadRecordsRequest,
    ) -> BoxFuture<'_, Result<ReadRecordsResponse, ReadError>> {
        Box::pin(self.read(request))
    }

    fn close_stream(
        &self,
        request: CloseStreamRequest,
    ) -> BoxFuture<'_, Result<CloseStreamResponse, CloseError>> {
        Box::pin(self.close(request))
    }

    fn describe_stream(
        &self,
        request: DescribeStreamRequest,
    ) -> BoxFuture<'_, Result<DescribeStreamResponse, DescribeError>> {
        Box::pin(self.describe(request))
    }
}
