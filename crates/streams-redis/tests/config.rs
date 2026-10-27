//! A bridge can build the memory runtime from the config that it builds for Redis.

use prost::Message;
use temporalio_streams::{
    MemoryStreamRuntime,
    proto::{CreateStreamRequest, CreateStreamResponse},
};
use temporalio_streams_redis::proto::RedisStreamConfig;

#[tokio::test]
async fn the_memory_runtime_takes_the_redis_config() {
    let config = RedisStreamConfig {
        urls: vec!["redis://unused:1".to_string()],
        backend: "redis-a".to_string(),
        ..Default::default()
    };
    let runtime = MemoryStreamRuntime::from_config_bytes(&config.encode_to_vec(), None).unwrap();
    let request = CreateStreamRequest {
        namespace: "ns".to_string(),
        stream_id: "s".to_string(),
        ..Default::default()
    };
    let bytes = runtime
        .call("CreateStream", &request.encode_to_vec())
        .await
        .unwrap();
    let created = CreateStreamResponse::decode(bytes.as_slice()).unwrap();
    assert_eq!(created.stream.unwrap().backend, "redis-a");
}
