//! The five operations of the Stream service on a Store of log calls.

use crate::{
    digest::batch_digest,
    offset::offset_to_wire,
    store::{Batch, Capabilities, ConflictPolicy, Create, CreateOutcome, LogStore},
};
use std::sync::Arc;
use temporalio_common::telemetry::metrics::TemporalMeter;
use temporalio_streams::{
    Reason, StreamError, StreamResult, StreamRuntime, StreamStore,
    proto::{
        AppendRecordsRequest, AppendRecordsResponse, CloseStreamRequest, CloseStreamResponse,
        CreateStreamRequest, CreateStreamResponse, DescribeStreamRequest, DescribeStreamResponse,
        ReadRecordsRequest, ReadRecordsResponse, StreamIdConflictPolicy, StreamRef, StreamStatus,
    },
    record::data_records,
};

/// The Stream service on a [LogStore] of log calls. The runtime of `temporalio-streams` checks each
/// request before it calls this type, so the type applies only the rules of the log.
pub(crate) struct LogStreamStore {
    /// The name of the Store, which the refs that this type builds carry.
    store_name: String,
    pub(crate) store: Arc<dyn LogStore>,
}

/// Gives the [StreamStore] of a Store of log calls. `store_name` is the name that lang registered
/// the Store under, which the refs that the Store builds carry. A Store that connects later, such
/// as a [crate::store::LazyStore], lets a bridge build the runtime with no async runtime.
pub fn stream_store(
    store_name: impl Into<String>,
    store: Arc<dyn LogStore>,
) -> Arc<dyn StreamStore> {
    Arc::new(LogStreamStore {
        store_name: store_name.into(),
        store,
    })
}

/// Builds the Stream runtime on a Store of log calls, with [stream_store]. The runtime records
/// metrics on `meter`, lang's runtime meter, if set.
pub fn stream_runtime(
    store_name: impl Into<String>,
    store: Arc<dyn LogStore>,
    meter: Option<TemporalMeter>,
) -> Arc<StreamRuntime> {
    StreamRuntime::new(stream_store(store_name, store), meter)
}

/// The conflict policy of a create. Unspecified is FAIL, as for a Workflow start.
fn conflict_policy(value: i32) -> StreamResult<ConflictPolicy> {
    match StreamIdConflictPolicy::try_from(value) {
        Ok(StreamIdConflictPolicy::Unspecified | StreamIdConflictPolicy::Fail) => {
            Ok(ConflictPolicy::Fail)
        }
        Ok(StreamIdConflictPolicy::UseExisting) => Ok(ConflictPolicy::UseExisting),
        Err(_) => Err(StreamError::invalid(format!(
            "The conflict policy {value} is unknown"
        ))),
    }
}

/// Appends a batch that the runtime checked to a Store of log calls.
async fn append_to_log(
    store: &dyn LogStore,
    request: AppendRecordsRequest,
) -> StreamResult<AppendRecordsResponse> {
    let AppendRecordsRequest {
        r#ref,
        producer,
        sequence,
        records,
        ..
    } = request;
    let stream = r#ref.unwrap_or_default();
    let producer = producer.unwrap_or_default();
    // Without the fence, a stale batch would reach Readers.
    let capabilities = store.capabilities();
    capabilities.require(Capabilities::PRODUCER_FENCE)?;
    if sequence == 0 {
        // Only the Store can number the batch in the same step as its dedupe check.
        capabilities.require(Capabilities::NEXT_SEQUENCE)?;
    }
    let digest = batch_digest(&records);
    let records = data_records(&producer, records)?;
    let batch = Batch {
        producer: &producer,
        sequence,
        records: &records,
        digest,
    };
    // An empty batch has no offset, which is the empty wire value.
    let (first_offset, last_offset) = store
        .append(&stream, &batch)
        .await?
        .offsets
        .unwrap_or_default();
    Ok(AppendRecordsResponse {
        first_offset: offset_to_wire(first_offset),
        last_offset: offset_to_wire(last_offset),
    })
}

#[async_trait::async_trait]
impl StreamStore for LogStreamStore {
    async fn create_stream(
        &self,
        request: CreateStreamRequest,
    ) -> StreamResult<CreateStreamResponse> {
        let conflict = conflict_policy(request.id_conflict_policy)?;
        let stream = request.stream_ref.clone().unwrap_or_default();
        // The Store config holds the Retention, so a create takes the Store default.
        let create = Create {
            retention: None,
            conflict,
            request_id: &request.request_id,
        };
        let created = self.store.create(&stream, &create).await?;
        Ok(CreateStreamResponse {
            r#ref: Some(StreamRef {
                id: stream.id,
                store: self.store_name.clone(),
            }),
            created: created.outcome == CreateOutcome::Created,
        })
    }

    async fn append_records(
        &self,
        request: AppendRecordsRequest,
    ) -> StreamResult<AppendRecordsResponse> {
        append_to_log(self.store.as_ref(), request).await
    }

    async fn read_records(&self, request: ReadRecordsRequest) -> StreamResult<ReadRecordsResponse> {
        self.read(request).await
    }

    async fn close_stream(&self, request: CloseStreamRequest) -> StreamResult<CloseStreamResponse> {
        let stream = request.r#ref.clone().unwrap_or_default();
        let producer = request.producer.clone().unwrap_or_default();
        self.store.capabilities().require(Capabilities::CLOSE)?;
        self.store.close(&stream, &producer).await?;
        Ok(CloseStreamResponse {})
    }

    async fn describe_stream(
        &self,
        request: DescribeStreamRequest,
    ) -> StreamResult<DescribeStreamResponse> {
        let stream = request.r#ref.clone().unwrap_or_default();
        let (state, _) = self.store.read(&stream, 0, 0).await?;
        if !state.exists {
            return Err(StreamError::new(
                Reason::NotFound,
                "Nobody created the Stream",
            ));
        }
        // A closed Stream that expired has lost its log, and every read fails.
        let status = if state.expired() {
            StreamStatus::Expired
        } else if state.closed {
            StreamStatus::Closed
        } else {
            StreamStatus::Open
        };
        Ok(DescribeStreamResponse {
            status: status as i32,
            last_offset: offset_to_wire(state.newest),
        })
    }
}
