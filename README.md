# sdk-streams

Experimental.

`temporalio-streams-redis` is the Redis Store of Temporal Streams. `RedisStore` implements the
`StreamStore` trait of `temporalio-streams`, which is in
[sdk-rust](https://github.com/temporalio/sdk-rust). It needs Redis 7.0 or later.

## Using the Store

```rust
let servers = RedisServers::Standalone("redis://localhost:6379".to_string());
let store = RedisStore::new(RedisStoreOptions::new(servers))?;
let runtime = StreamRuntime::new(Arc::new(store), StoreLimits::default());
```

The Store connects at its first call. Call it through a `StreamRuntime`, which checks each request
before the Store gets it. The runtime also takes the limits of a batch, and the Store has none.

| Option | Default | Use |
|---|---|---|
| `servers` | | The URL of one server, or the URLs of some nodes of a Redis Cluster |
| `key_prefix` | `temporal-streams` | The prefix of every key |
| `retention` | 7 days | How long a Stream stays readable after its close |
| `producer_idle_ttl` | none | How long an open Stream stays after its last append |
| `fence` | `Fence::Off` | Whether a write of an earlier attempt of a Producer is refused |
| `resend_window` | 16 | The batches of a Producer whose `request_id` the Store remembers |
| `response_timeout` | 5 s | How long a call waits for Redis |
| `wait` | none | The replicas that each write waits for, with `WAIT`, and for how long |
| `max_blocking_connections` | 256 | The reads and the writes that can block at a time |

A URL starts with `redis://` or `rediss://` and has the credentials. No option has an upper
bound, and a zero or an empty value is refused. A Stream keeps the
`retention` and the `producer_idle_ttl` of the Store that created it.
`crates/streams-redis/docs/redis.md` describes what the Store keeps in Redis.

## Building

- `temporalio-streams` isn't on crates.io yet, so run `scripts/patch-sdk-rust.sh` before you build.
  `SDK_RUST_PATH` points it at a local checkout of sdk-rust.
- A bridge that builds sdk-rust from its `sdk-core` submodule patches `temporalio-common` and
  `temporalio-streams` to those copies in `[patch.crates-io]`, so that each type exists once.
- A FIPS build uses `default-features = false` and the `fips` feature. The default feature links
  `ring`.

## Testing

`cargo test` needs `redis-server` in the `PATH`, or its path in `REDIS_SERVER`. Each test starts
the servers that it needs on free ports and stops them. `STREAMS_SEED=<seed>` runs one seed of a
generated test.

## License

MIT. See `LICENSE.txt`.
