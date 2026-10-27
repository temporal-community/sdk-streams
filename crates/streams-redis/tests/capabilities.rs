//! The Redis Store says what it does atomically beyond the log.

mod support;

use temporalio_streams::store::{Capabilities, Store};
use temporalio_streams_redis::{RedisStore, RedisStoreOptions};

#[tokio::test]
async fn the_redis_store_says_what_it_can_do() {
    let Some(url) = support::server_url("STREAMS_REDIS_URL") else {
        return;
    };
    let redis = RedisStore::connect(RedisStoreOptions::new(url))
        .await
        .unwrap();
    assert_eq!(
        Store::capabilities(&redis),
        Capabilities::ALL.without(Capabilities::RUN_FENCE)
    );
}
