# sdk-streams

Experimental.

`temporalio-streams-redis` is the Redis Store of the Temporal Stream runtime. The runtime itself,
`temporalio-streams`, is in the [sdk-rust](https://github.com/temporalio/sdk-rust) repo with the
memory Store, and Core has no Stream code. The SDKs link both crates into their bridges. A bridge
builds the runtime per client with `RedisStreamRuntime` and calls `StreamRuntime::call` with a
method name and proto bytes. A Workflow makes each Stream call from a Local Activity.

## Linking from a lang bridge

The crate depends on `temporalio-common` 1.1 and `temporalio-streams` 0.1 from crates.io. A bridge
that also links the sdk-rust crates from its `sdk-core` submodule must patch both to those copies,
so that each type exists once.

```toml
[dependencies.temporalio-streams-redis]
version = "0.1"
default-features = false

[patch.crates-io]
temporalio-common = { path = "./sdk-core/crates/common" }
temporalio-streams = { path = "./sdk-core/crates/streams" }
```

The crate features `tls-ring` and `tls-aws-lc` forward to `temporalio-common`. A FIPS build uses the
`fips` feature with no default features. It turns on `tls-aws-lc` and the FIPS build of `aws-lc-rs`,
which needs CMake and Go.

## Protos

`protos/temporal/sdk/streams/v1/redis.proto` has `RedisStreamConfig`, the settings that lang builds
for the Redis runtime. It keeps the package of the runtime protos of `temporalio-streams`. The
memory runtime's config there has the field numbers of the fields that both have, so a bridge can
send the memory runtime the config that it builds for Redis.

## Building and testing

The build needs `protoc`.

- Format and lint: `cargo fmt --all -- --check`, then
  `cargo clippy --workspace --tests -- -D warnings`.
- Tests: `cargo test --workspace`. The shared suites of `temporalio-streams` (the Store, service
  and writer rules) run on each Redis server that is set. With no server, only the unit tests
  check anything.
- FIPS: `cargo tree -e normal,build --no-default-features --features fips -i ring` prints
  nothing.
- Redis: run `crates/streams-redis/scripts/redis-test-servers.sh`, then the test command with the
  two URLs it prints. It starts one standalone server and a cluster of three primaries. Set
  `STREAMS_STANDALONE_PORT` and `STREAMS_CLUSTER_PORTS` to use other ports. With
  `STREAMS_REQUIRE=redis`, a test with no URL fails instead of skipping.

Until `temporalio-streams` is on crates.io, CI takes it and `temporalio-common` from sdk-rust with
`scripts/patch-sdk-rust.sh`, and a local build patches them in `.cargo/config.toml`.

`crates/streams-redis/docs/redis.md` has the Redis layout.

## License

MIT. See `LICENSE.txt`.
