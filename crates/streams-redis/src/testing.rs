//! Test support, for the crate's own tests. A case that needs a Redis server is `#[ignore]`, so
//! a plain `cargo test` skips it, and `cargo test -- --include-ignored` runs it.
//! `crates/streams-redis/scripts/redis-test-servers.sh` starts the servers and prints their URLs.

/// The URL of the standalone test server, which has one replica.
pub fn standalone_url() -> String {
    url("STREAMS_REDIS_URL")
}

/// The URL of a seed of the test cluster.
pub fn cluster_url() -> String {
    url("STREAMS_REDIS_CLUSTER_URL")
}

// The cases that call it run only on purpose, so a missing URL is a mistake, not a skip.
fn url(variable: &str) -> String {
    std::env::var(variable).unwrap_or_else(|_| {
        panic!(
            "Set {variable} to run this case. \
             `crates/streams-redis/scripts/redis-test-servers.sh` prints it."
        )
    })
}
