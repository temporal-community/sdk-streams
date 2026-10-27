//! The shared Store suite on Redis standalone and on a Redis cluster. Start the servers with
//! `scripts/redis-test-servers.sh` and set the two URLs that it prints.

mod support;

use temporalio_streams::testing;

#[tokio::test]
async fn the_store_suite_passes_on_redis() {
    testing::run(support::backends(), testing::store::CASES).await;
}
