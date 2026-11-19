//! The byte form of the five Stream operations, for a binding in another language. The binding
//! passes the encoded request and gets the encoded response, so it decodes no proto.

use prost::Message;
use temporalio_streams::{Reason, StreamError, StreamResult, StreamRuntime};

fn decode<M: Message + Default>(input: &[u8]) -> StreamResult<M> {
    M::decode(input).map_err(|error| StreamError::invalid(format!("The request is bad: {error}")))
}

/// Runs one of the five Stream operations on an encoded request, by its name, and gives the
/// encoded response. The names are `CreateStream`, `AppendRecords`, `CloseStream`, `ReadRecords`
/// and `DescribeStream`. Any other name fails with [Reason::Unsupported]. A binding raises its
/// error from [StreamError::code] and [StreamError::details].
pub async fn call(
    runtime: &StreamRuntime,
    operation: &str,
    request: &[u8],
) -> StreamResult<Vec<u8>> {
    let response = match operation {
        "CreateStream" => runtime
            .create_stream(decode(request)?)
            .await?
            .encode_to_vec(),
        "AppendRecords" => runtime
            .append_records(decode(request)?)
            .await?
            .encode_to_vec(),
        "CloseStream" => runtime
            .close_stream(decode(request)?)
            .await?
            .encode_to_vec(),
        "ReadRecords" => runtime
            .read_records(decode(request)?)
            .await?
            .encode_to_vec(),
        "DescribeStream" => runtime
            .describe_stream(decode(request)?)
            .await?
            .encode_to_vec(),
        _ => {
            return Err(StreamError::new(
                Reason::Unsupported,
                format!("Unknown operation {operation}"),
            ));
        }
    };
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use temporalio_common::protos::temporal::api::common::v1::Payload;
    use temporalio_streams::{
        StreamStore,
        proto::{
            AppendRecord, AppendRecordsRequest, AppendRecordsResponse, ClientProducer,
            CloseStreamRequest, CloseStreamResponse, CreateStreamRequest, CreateStreamResponse,
            DescribeStreamRequest, DescribeStreamResponse, Producer, ReadRecordsRequest,
            ReadRecordsResponse, StreamRecord, StreamRef, StreamStatus, producer::Kind,
        },
    };
    use tonic::Code;

    /// A Store that gives the answers that a test set.
    #[derive(Default)]
    struct Scripted {
        page: Mutex<ReadRecordsResponse>,
        appended: Mutex<AppendRecordsResponse>,
        described: Mutex<DescribeStreamResponse>,
    }

    #[async_trait::async_trait]
    impl StreamStore for Scripted {
        async fn create_stream(
            &self,
            request: CreateStreamRequest,
        ) -> StreamResult<CreateStreamResponse> {
            Ok(CreateStreamResponse {
                r#ref: request.stream_ref,
                created: true,
            })
        }

        async fn append_records(
            &self,
            _request: AppendRecordsRequest,
        ) -> StreamResult<AppendRecordsResponse> {
            Ok(self.appended.lock().unwrap().clone())
        }

        async fn read_records(
            &self,
            _request: ReadRecordsRequest,
        ) -> StreamResult<ReadRecordsResponse> {
            Ok(self.page.lock().unwrap().clone())
        }

        async fn close_stream(
            &self,
            _request: CloseStreamRequest,
        ) -> StreamResult<CloseStreamResponse> {
            Ok(CloseStreamResponse {})
        }

        async fn describe_stream(
            &self,
            _request: DescribeStreamRequest,
        ) -> StreamResult<DescribeStreamResponse> {
            Ok(self.described.lock().unwrap().clone())
        }
    }

    /// A runtime on a new [Scripted] Store, and the Store.
    fn runtime() -> (Arc<Scripted>, Arc<StreamRuntime>) {
        let store = Arc::new(Scripted::default());
        let runtime = StreamRuntime::new(store.clone(), None);
        (store, runtime)
    }

    const OPERATIONS: [&str; 5] = [
        "CreateStream",
        "AppendRecords",
        "CloseStream",
        "ReadRecords",
        "DescribeStream",
    ];

    fn stream_ref() -> Option<StreamRef> {
        Some(StreamRef {
            id: "s".to_string(),
            store: "b".to_string(),
        })
    }

    fn client() -> Option<Producer> {
        Some(Producer {
            id: "client:me:p".to_string(),
            kind: Some(Kind::Client(ClientProducer {})),
        })
    }

    fn records() -> Vec<AppendRecord> {
        vec![AppendRecord {
            body: Some(Payload::default()),
        }]
    }

    async fn call<Req: Message, Resp: Message + Default>(
        runtime: &StreamRuntime,
        operation: &str,
        request: Req,
    ) -> Resp {
        let bytes = super::call(runtime, operation, &request.encode_to_vec())
            .await
            .unwrap();
        Resp::decode(bytes.as_slice()).unwrap()
    }

    async fn create(runtime: &StreamRuntime) -> CreateStreamResponse {
        let request = CreateStreamRequest {
            stream_ref: stream_ref(),
            ..Default::default()
        };
        call(runtime, "CreateStream", request).await
    }

    // A typo in an operation name would make it unknown, which is unsupported, so the loop also
    // checks the names that a bridge calls.
    #[tokio::test]
    async fn bad_bytes_are_invalid_and_an_unknown_operation_is_unsupported() {
        let (_, runtime) = runtime();
        for operation in OPERATIONS {
            let error = super::call(&runtime, operation, &[0xff]).await.unwrap_err();
            assert_eq!(error.reason, Reason::Invalid, "{operation}");
        }
        let error = super::call(&runtime, "Nope", &[]).await.unwrap_err();
        assert_eq!(
            (error.reason, error.code(), error.message.as_str()),
            (
                Reason::Unsupported,
                Code::Unimplemented,
                "Unknown operation Nope"
            )
        );
    }

    #[tokio::test]
    async fn each_stream_operation_round_trips_its_bytes() {
        let (store, runtime) = runtime();
        *store.appended.lock().unwrap() = AppendRecordsResponse {
            first_offset: b"a".to_vec(),
            last_offset: b"a".to_vec(),
        };
        *store.page.lock().unwrap() = ReadRecordsResponse {
            records: vec![StreamRecord {
                body: Some(Payload::default()),
                offset: b"a".to_vec(),
                ..Default::default()
            }],
            last_offset: b"a".to_vec(),
            done: false,
        };
        *store.described.lock().unwrap() = DescribeStreamResponse {
            status: StreamStatus::Closed as i32,
            ..Default::default()
        };
        assert_eq!(create(&runtime).await.r#ref.unwrap().store, "b");
        let appended: AppendRecordsResponse = call(
            &runtime,
            "AppendRecords",
            AppendRecordsRequest {
                r#ref: stream_ref(),
                producer: client(),
                sequence: 1,
                records: records(),
                ..Default::default()
            },
        )
        .await;
        let read: ReadRecordsResponse = call(
            &runtime,
            "ReadRecords",
            ReadRecordsRequest {
                r#ref: stream_ref(),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(read.last_offset, appended.last_offset);
        let _: CloseStreamResponse = call(
            &runtime,
            "CloseStream",
            CloseStreamRequest {
                r#ref: stream_ref(),
                producer: client(),
                ..Default::default()
            },
        )
        .await;
        let described: DescribeStreamResponse = call(
            &runtime,
            "DescribeStream",
            DescribeStreamRequest {
                r#ref: stream_ref(),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(described.status, StreamStatus::Closed as i32);
    }
}
