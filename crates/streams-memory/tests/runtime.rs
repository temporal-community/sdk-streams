//! The memory Store runtime, built from its options.

use temporalio_common::protos::temporal::api::common::v1::Payload;
use temporalio_streams::{
    Reason,
    proto::{
        AppendRecord, AppendRecordsRequest, ClientProducer, CreateStreamRequest, Producer,
        ReadRecordsRequest, StreamRef, producer::Kind,
    },
    record::BatchLimits,
};
use temporalio_streams_memory::{MemoryStreamOptions, MemoryStreamRuntime};

const NAMESPACE: &str = "ns";

fn stream_ref() -> Option<StreamRef> {
    Some(StreamRef {
        id: "s".to_string(),
        ..Default::default()
    })
}

fn records(count: usize) -> Vec<AppendRecord> {
    (0..count)
        .map(|i| AppendRecord {
            body: Some(Payload {
                data: vec![b'a' + u8::try_from(i).unwrap()],
                ..Default::default()
            }),
        })
        .collect()
}

#[tokio::test]
async fn a_batch_over_the_limits_of_the_options_is_too_large() {
    let options = MemoryStreamOptions {
        limits: BatchLimits::default().with_max_batch_records(2).unwrap(),
        ..Default::default()
    };
    let runtime = MemoryStreamRuntime::create("memory", options, None);
    runtime
        .create_stream(CreateStreamRequest {
            namespace: NAMESPACE.to_string(),
            stream_ref: stream_ref(),
            ..Default::default()
        })
        .await
        .unwrap();
    let append = |count| AppendRecordsRequest {
        namespace: NAMESPACE.to_string(),
        r#ref: stream_ref(),
        producer: Some(Producer {
            id: "client:me:p".to_string(),
            kind: Some(Kind::Client(ClientProducer {})),
        }),
        sequence: 1,
        records: records(count),
    };
    let error = runtime.append_records(append(3)).await.unwrap_err();
    assert_eq!(error.reason, Reason::TooLarge);
    runtime.append_records(append(2)).await.unwrap();
    let page = runtime
        .read_records(ReadRecordsRequest {
            namespace: NAMESPACE.to_string(),
            r#ref: stream_ref(),
            ..Default::default()
        })
        .await
        .unwrap();
    let data: Vec<_> = page
        .records
        .iter()
        .map(|record| record.body.as_ref().unwrap().data.clone())
        .collect();
    assert_eq!(data, [b"a".to_vec(), b"b".to_vec()]);
}
