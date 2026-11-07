//! Test support. The cases that need Redis are `#[ignore]`, so a plain `cargo test` skips them.
//! `cargo test -- --include-ignored` runs them on the servers of `scripts/redis-test-servers.sh`.

pub use crate::slots::{SlotNode, SlotRange};

/// The URL of the standalone test server, which has one replica.
pub fn standalone_url() -> String {
    url("STREAMS_REDIS_URL")
}

/// The URL of a seed of the test cluster.
pub fn cluster_url() -> String {
    url("STREAMS_REDIS_CLUSTER_URL")
}

/// The commands that `docs/redis.md` lists in the access bullet that starts with `bullet`, such as
/// `A Producer`. The tests use the docs as the only list, so the docs can't drift from what runs.
pub fn documented_commands(bullet: &str) -> Vec<String> {
    let docs = include_str!("../docs/redis.md");
    let start = docs
        .find(&format!("\n- {bullet}"))
        .unwrap_or_else(|| panic!("docs/redis.md has no bullet that starts with {bullet:?}"));
    let rest = &docs[start + 1..];
    // The bullet ends at the next bullet or at a blank line.
    let end = ["\n- ", "\n\n"]
        .iter()
        .filter_map(|next| rest.find(next))
        .min()
        .unwrap_or(rest.len());
    let commands: Vec<String> = rest[..end]
        .split('`')
        .skip(1)
        .step_by(2)
        .flat_map(str::split_whitespace)
        .filter(|word| word.starts_with('+'))
        .map(|word| word.trim_start_matches('+').to_string())
        .collect();
    assert!(
        !commands.is_empty(),
        "the {bullet:?} bullet lists no commands"
    );
    commands
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
