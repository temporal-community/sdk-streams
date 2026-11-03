//! The Redis Store says what it does atomically beyond the log.

use temporalio_streams::store::{Capabilities, Store};
use temporalio_streams_redis::{RedisStore, RedisStoreOptions, testing::standalone_url};

#[tokio::test]
#[ignore = "needs Redis"]
async fn the_redis_store_says_what_it_can_do() {
    let url = standalone_url();
    let redis = RedisStore::connect(RedisStoreOptions::new(url))
        .await
        .unwrap();
    assert_eq!(Store::capabilities(&redis), Capabilities::ALL);
}
