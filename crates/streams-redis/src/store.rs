//! The Redis Store. One Lua script runs each operation, so its checks and its write are one
//! atomic step. `docs/redis.md` describes what the scripts store.

use crate::{
    RedisStoreOptions,
    conn::Connections,
    error::{RedisStoreError, RedisStoreResult},
    keys::StreamKeys,
    offset::EntryId,
    options::Settings,
    producer::{producer_keys, request_key},
    scripts::{
        self, AppendArgs, Appended, CreateArgs, Created, Existing, NoArgs, Page, ReadArgs,
        ScriptCall,
    },
};
use futures_util::future::BoxFuture;
use prost::Message;
use redis::FromRedisValue;
use std::time::Duration;
use temporalio_streams::{
    AppendError, CreateError, DescribeError, ReadError, StreamStore,
    proto::{
        AppendRecordsRequest, AppendRecordsResponse, CreateStreamRequest, CreateStreamResponse,
        DescribeStreamRequest, DescribeStreamResponse, ReadRecordsRequest, ReadRecordsResponse,
        StreamIdConflictPolicy, StreamRecord,
    },
};
use tokio::{sync::OnceCell, time::Instant};

/// The number of Records in a page of a read that sets no `max_records`.
const PAGE_RECORDS: usize = 1000;

/// Maximum size of the Records in a page of a read that sets no `max_bytes`, which is 4 MiB. It's
/// the default size of a batch in an SDK's Stream config, so such a page is no larger than that.
const PAGE_BYTES: usize = 4 * 1024 * 1024;

/// A Stream Store that keeps its Streams in Redis 7.0 or later.
///
/// The Store expects the requests that [`StreamRuntime`](temporalio_streams::StreamRuntime)
/// passes on, so wrap it in one. The runtime takes the limits of a batch, which are settings of
/// the Stream config of the SDK that calls the Store. The Store has none of its own.
pub struct RedisStore {
    settings: Settings,
    conns: OnceCell<Connections>,
}

impl RedisStore {
    /// Creates a Store. It fails for a `response_timeout` of zero, and for another time of the
    /// options below 1 ms. The Store connects at its first call, so creating it needs no server.
    pub fn new(options: RedisStoreOptions) -> Result<Self, RedisStoreError> {
        Ok(Self {
            settings: options.try_into()?,
            conns: OnceCell::new(),
        })
    }

    /// Returns the connections, which the first call opens. A failed connect is tried again at
    /// the next call, so a Store that starts before its Redis works once Redis is up.
    async fn conns(&self) -> RedisStoreResult<&Connections> {
        let open = || Connections::open(&self.settings);
        Ok(self.conns.get_or_try_init(open).await?)
    }

    /// Returns the keys of the Stream `stream_id`.
    fn keys(&self, stream_id: &str) -> RedisStoreResult<StreamKeys> {
        // The runtime refuses an empty id, so only a direct call of the Store fails here.
        if stream_id.is_empty() {
            return Err(RedisStoreError::Invalid(
                "The request has no stream_id".to_string(),
            ));
        }
        Ok(StreamKeys::new(&self.settings.key_prefix, stream_id))
    }

    async fn run<T: FromRedisValue>(&self, call: &ScriptCall) -> RedisStoreResult<T> {
        let mut conn = self.conns().await?.shared();
        Ok(T::from_redis_value(call.run(&mut conn).await?)?)
    }

    /// Runs a script that writes. With the `wait` option, the call succeeds only once enough
    /// replicas have the write.
    async fn write<T: FromRedisValue>(&self, call: &ScriptCall) -> RedisStoreResult<T> {
        let Some(wait) = self.settings.wait else {
            return self.run(call).await;
        };
        let (answer, acknowledged) = self.conns().await?.write_and_wait(call, wait).await?;
        if acknowledged < wait.replicas.get() {
            return Err(RedisStoreError::Unavailable(format!(
                "{acknowledged} of {} replicas have the write after {} ms. A failover can lose \
                 it, and a retry with its request id doesn't write it again",
                wait.replicas,
                wait.timeout.get()
            )));
        }
        Ok(answer)
    }

    async fn create(
        &self,
        request: CreateStreamRequest,
        request_id: String,
    ) -> Result<CreateStreamResponse, CreateError> {
        let existing = match StreamIdConflictPolicy::try_from(request.id_conflict_policy) {
            Ok(StreamIdConflictPolicy::UseExisting) => Existing::Use,
            Ok(StreamIdConflictPolicy::Fail | StreamIdConflictPolicy::Unspecified) => {
                Existing::Fail
            }
            Err(unknown) => {
                let message = format!("The create has an {unknown}");
                return Err(RedisStoreError::Invalid(message).into());
            }
        };
        let args = CreateArgs {
            request: request_key(b"", &request_id),
            retention: self.settings.retention,
            existing,
            then: self.settings.wait.into(),
        };
        let keys = self.keys(&request.stream_id)?;
        let call = ScriptCall::write(&scripts::CREATE, &[keys.meta()], args);
        let created = self.write(&call).await?;
        Ok(CreateStreamResponse {
            created: matches!(created, Created::Made),
        })
    }

    async fn append(
        &self,
        request: AppendRecordsRequest,
        request_id: String,
    ) -> Result<AppendRecordsResponse, AppendError> {
        let producer = request.producer.unwrap_or_default();
        let (field, attempt) = producer_keys(&producer)?;
        let bodies = request.records.into_iter().map(|record| {
            let body = record.body.ok_or_else(|| {
                RedisStoreError::Invalid("A Record of the append has no body".to_string())
            })?;
            Ok::<_, RedisStoreError>(body.encode_to_vec())
        });
        let args = AppendArgs {
            field: &field,
            attempt,
            request: request_key(&attempt, &request_id),
            fence: self.settings.fence,
            window: self.settings.resend_window,
            then: self.settings.wait.into(),
            producer: &producer,
            bodies: bodies.collect::<Result<_, _>>()?,
        };
        let keys = self.keys(&request.stream_id)?;
        let keys = [keys.meta(), keys.log(), keys.producers()];
        let call = ScriptCall::write(&scripts::APPEND, &keys, args);
        let appended: Appended = self.write(&call).await?;
        Ok(AppendRecordsResponse {
            first_offset: appended.first.offset(),
            last_offset: appended.last.offset(),
        })
    }

    async fn read(&self, request: ReadRecordsRequest) -> Result<ReadRecordsResponse, ReadError> {
        // The form of an offset is the Store's own, so the runtime can't check it.
        let from = EntryId::try_from(&request.from_offset[..]).map_err(|error| {
            RedisStoreError::Invalid(format!("The from_offset of the read is wrong: {error}"))
        })?;
        let negative =
            |field: &str| RedisStoreError::Invalid(format!("The {field} of the read is negative"));
        let max_records = match usize::try_from(request.max_records) {
            Ok(0) => PAGE_RECORDS,
            Ok(max) => max,
            Err(_) => return Err(negative("max_records").into()),
        };
        let max_bytes = match usize::try_from(request.max_bytes) {
            Ok(0) => PAGE_BYTES,
            Ok(max) => max,
            Err(_) => return Err(negative("max_bytes").into()),
        };
        let wait = Duration::try_from(request.wait.unwrap_or_default());
        let wait = wait.map_err(|_| negative("wait"))?;
        // A wait that an `Instant` can't hold has no deadline.
        let deadline = Instant::now().checked_add(wait);
        let keys = self.keys(&request.stream_id)?;
        let args = ReadArgs {
            from,
            max_records,
            max_bytes,
        };
        let call = ScriptCall::read(&scripts::READ, &[keys.meta(), keys.log()], args);
        loop {
            let answer: Page = self.run(&call).await?;
            let expires_in = answer.expires_in;
            let page = page(answer, &request.from_offset, max_bytes);
            let late = deadline.is_some_and(|deadline| deadline <= Instant::now());
            if !page.records.is_empty() || late {
                return Ok(page);
            }
            // No Record comes after the expiry, so the wait ends there and the next script answers
            // `EXPIRED`. Redis gives the time that is left, so the clock of this host can differ.
            let expiry = expires_in.and_then(|left| Instant::now().checked_add(left));
            let until = match (deadline, expiry) {
                (Some(deadline), Some(expiry)) => Some(deadline.min(expiry)),
                (deadline, expiry) => deadline.or(expiry),
            };
            let conns = self.conns().await?;
            conns.wait_for_entry(&keys, from, until).await?;
        }
    }

    async fn describe(
        &self,
        request: DescribeStreamRequest,
    ) -> Result<DescribeStreamResponse, DescribeError> {
        let keys = self.keys(&request.stream_id)?;
        let call = ScriptCall::read(&scripts::DESCRIBE, &[keys.meta()], NoArgs);
        let newest: EntryId = self.run(&call).await?;
        Ok(DescribeStreamResponse {
            last_offset: newest.offset(),
        })
    }
}

/// Builds the page of a read that started after `from`. The entries of a batch have its Producer
/// only on the first one, so every later Record gets the Producer of the entry before it.
///
/// The page stops before the Record that takes it over `max_bytes`, unless that is the first
/// one. The script only stops near that limit, since it can't measure an encoded Record. A
/// Record counts as the page encodes it, which is how the runtime counts too.
fn page(page: Page, from_offset: &[u8], max_bytes: usize) -> ReadRecordsResponse {
    let mut producer = page.earlier;
    let mut records = Vec::with_capacity(page.entries.len());
    let mut bytes = 0;
    for entry in page.entries {
        producer = entry.producer.or(producer);
        let record = StreamRecord {
            producer: producer.clone(),
            body: Some(entry.body),
            offset: entry.id.offset(),
        };
        bytes += prost::encoding::message::encoded_len(1, &record);
        if bytes > max_bytes && !records.is_empty() {
            break;
        }
        records.push(record);
    }
    ReadRecordsResponse {
        last_offset: match records.last() {
            Some(record) => record.offset.clone(),
            None => from_offset.to_vec(),
        },
        records,
    }
}

impl StreamStore for RedisStore {
    fn create_stream(
        &self,
        request: CreateStreamRequest,
        request_id: String,
    ) -> BoxFuture<'_, Result<CreateStreamResponse, CreateError>> {
        Box::pin(self.create(request, request_id))
    }

    fn append_records(
        &self,
        request: AppendRecordsRequest,
        request_id: String,
    ) -> BoxFuture<'_, Result<AppendRecordsResponse, AppendError>> {
        Box::pin(self.append(request, request_id))
    }

    fn read_records(
        &self,
        request: ReadRecordsRequest,
    ) -> BoxFuture<'_, Result<ReadRecordsResponse, ReadError>> {
        Box::pin(self.read(request))
    }

    fn describe_stream(
        &self,
        request: DescribeStreamRequest,
    ) -> BoxFuture<'_, Result<DescribeStreamResponse, DescribeError>> {
        Box::pin(self.describe(request))
    }
}
