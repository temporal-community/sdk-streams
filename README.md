# sdk-streams

Experimental.

`temporalio-streams-redis` is the Redis Store of Temporal Streams. `RedisStore` implements the
`StreamStore` trait of `temporalio-streams`, which is in
[sdk-rust](https://github.com/temporalio/sdk-rust). It needs Redis 7.0 or later.

The unreleased [Python package](python/README.md) exposes `RedisStreamStore`.

## Using the Store

```rust
let servers = RedisServers::Standalone("redis://localhost:6379".parse()?);
let store = RedisStore::new(RedisStoreOptions::new(servers))?;
let runtime = StreamRuntime::new(Arc::new(store), StoreLimits::default());
```

The Store connects at its first call. Call it through a `StreamRuntime`, which checks each request
before the Store gets it. The runtime takes the limits of a batch, which are settings of the Stream
config of the SDK that calls the Store. The Store has none.

| Option | Default | Use |
|---|---|---|
| `servers` | | One server, or some nodes of a Redis Cluster, each from its URL |
| `key_prefix` | `temporal-streams` | The prefix of every key |
| `retention` | 7 days | How long a Stream stays after its create or its last written batch |
| `fence` | `Fence::Off` | Whether a write of an earlier attempt of a Producer is refused |
| `resend_window` | 16 | The batches of a Producer whose request id the Store remembers |
| `response_timeout` | 5 s | How long a call waits for Redis |
| `wait` | none | The replicas that each write waits for, with `WAIT`, and for how long |
| `max_blocking_connections` | 256 | The reads and the writes that can block at a time |

A URL starts with `redis://` or `rediss://` and has the credentials. `rediss://` connects with
TLS and verifies the server against the root certificates of the system. A private CA works only
through the `SSL_CERT_FILE` variable, and the Store can't send a client certificate.

Counts must be positive. `retention` and `wait.timeout` must be at least 1 ms.
`response_timeout` must be positive. A Stream keeps its original retention.
See the [operator guide](crates/streams-redis/docs/redis.md) for access, persistence and storage.

## Building

- `temporalio-streams` is unreleased. Run
  `SDK_RUST_REF=moe/streams-crate-types scripts/patch-sdk-rust.sh` before building.
  Use `SDK_RUST_PATH` for a local `sdk-rust` checkout.
- A bridge that builds sdk-rust from its `sdk-core` submodule patches `temporalio-common` and
  `temporalio-streams` to those copies in `[patch.crates-io]`, so that each type exists once.
- A FIPS build uses `default-features = false` and the `fips` feature. The default feature links
  `ring`.

## Testing

`cargo test` runs the tests that need no server. `cargo test -- --include-ignored` runs all of
them, and needs `redis-server` in the `PATH`, or its path in `REDIS_SERVER`. Each test starts the
servers that it needs on free ports and stops them. `STREAMS_SEED=<seed>` runs one seed of a
generated test.

## License

MIT. See `LICENSE.txt`.
