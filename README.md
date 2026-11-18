# sdk-streams

Experimental.

The Stores of the Temporal Stream runtime. The runtime itself, `temporalio-streams`, is in
[sdk-rust](https://github.com/temporalio/sdk-rust). This repo has three crates.

- `temporalio-streams-log` has the `LogStore` trait of a Store that keeps each Stream as a log,
  what such Stores share, and the shared test suites.
- `temporalio-streams-memory` is a Store that keeps its Streams in the memory of one process, for
  tests.
- `temporalio-streams-redis` is the Redis Store.

A lang bridge builds the runtime with `RedisStreamRuntime`, which connects on its first call that
needs Redis. Then the bridge calls `dispatch::call` of `temporalio-streams-log` with the runtime,
the name of one of the five Stream operations and the encoded request, and it gets the encoded
response.

## Connecting

Lang sends the options as a `RedisStreamConfig`, from
`crates/streams-redis/protos/temporal/sdk/streams/v1/redis.proto`. Rust code can call
`RedisStore::connect` with `RedisStoreOptions`. The Store needs Redis 7.0 or later. The table
lists the main config fields. `RedisStoreOptions` has the same ones, except that
`servers: RedisServers` replaces `urls` and `cluster`, `wait: Option<WaitOptions>` replaces the
two `wait_` fields, and `limits: BatchLimits` replaces the three `max_` fields.

| Option | Default | Use |
|---|---|---|
| `urls` | | One `redis://` or `rediss://` URL, or the seeds of a cluster |
| `cluster` | `false` | Set for a Redis Cluster |
| `key_prefix` | `temporal-streams` | The prefix of every key |
| `store_name` | empty | The name that lang registered the Store under. It goes on each ref |
| `retention` | 7 days | The Retention of each Stream. It counts from the close. |
| `producer_idle_ttl` | none | How long an open Stream stays after the last write of any Producer. None keeps it open |
| `wait_replicas` | 0 | The replicas that must have each write, with `WAIT` |
| `wait_timeout` | 1 s | How long a write waits for its replicas |
| `max_record_bytes` | 1536 KiB | The most bytes of one stored Record. At most the default |
| `max_batch_bytes` | 4 MiB | The most bytes of the stored Records of one batch. At most the default |
| `max_batch_records` | 1000 | The most Records of one batch. At most the default |

The URL sets the credentials and TLS. `crates/streams-redis/docs/redis.md` has the key layout, the
TTLs, the ACLs and the known limits.

## Building

- `temporalio-streams` isn't on crates.io yet, so run `scripts/patch-sdk-rust.sh` before you build.
- A bridge that builds sdk-rust from its `sdk-core` submodule patches `temporalio-common` and
  `temporalio-streams` to those copies in `[patch.crates-io]`, so that each type exists once.
- A FIPS build uses `default-features = false` and the `fips` feature. The default feature links
  `ring`.

## Testing

`cargo test` runs the cases that need no server. The cases that need Redis are `#[ignore]`. To run
them, start the servers with `crates/streams-redis/scripts/redis-test-servers.sh`, set the two URLs
that it prints, and run `cargo test -- --include-ignored`.

## Python

`python/` has two Python packages. Each runs a Store of this repo in its own native module, so the
Python SDK needs no Store code.

- `python/redis` is `temporalio-streams-redis`, the Redis Store. See `python/redis/README.md`.
- `python/memory` is `temporalio-streams-memory`, the memory Store, for tests. See
  `python/memory/README.md`.

`python/native` is the Rust that both native modules share. `python/suite` has the tests that every
Store must pass.

## License

MIT. See `LICENSE.txt`.
