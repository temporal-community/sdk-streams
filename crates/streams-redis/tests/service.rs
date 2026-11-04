//! The shared service suite on a standalone Redis and on a Redis cluster. Run it with
//! `cargo test -- --include-ignored` on the servers of `scripts/redis-test-servers.sh`.

mod support;

use temporalio_streams::testing;

#[tokio::test]
#[ignore = "needs Redis"]
async fn the_service_suite_passes_on_redis() {
    testing::run(support::standalone(), testing::service::CASES).await;
}

#[tokio::test]
#[ignore = "needs a Redis cluster"]
async fn the_service_suite_passes_on_a_redis_cluster() {
    testing::run(support::cluster(), testing::service::CASES).await;
}
