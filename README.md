# sdk-streams

Experimental.

`temporalio-streams-redis` is the Redis Store of the Temporal Stream runtime. The runtime itself,
`temporalio-streams`, is in [sdk-rust](https://github.com/temporalio/sdk-rust). A lang bridge builds
the runtime with `RedisStreamRuntime`, which connects on its first call that needs Redis.

## Connecting

Lang sends the options as a `RedisStreamConfig`, from
`crates/streams-redis/protos/temporal/sdk/streams/v1/redis.proto`. Rust code can call
`RedisStore::connect` with `RedisStoreOptions`. The Store needs Redis 7.0 or later. These are the
main options.

| Option | Default | Use |
|---|---|---|
| `urls` | | One `redis://` or `rediss://` URL, or the seeds of a cluster |
| `cluster` | `false` | Set for a Redis Cluster |
| `key_prefix` | `temporal-streams` | The prefix of every key |
| `retention` | 7 days | The Retention of a Stream whose create sets none |
| `safety_ttl` | 90 days | How long an open Workflow-owned Stream stays after its last write |
| `wait_replicas` | 0 | The replicas that must have each write, with `WAIT` |
| `wait_timeout` | 1 s | How long a write waits for its replicas |

The URL sets the credentials and TLS. `crates/streams-redis/docs/redis.md` has the key layout, the
TTLs, the ACLs and the known limits.

## Building

- `temporalio-streams` isn't on crates.io yet, so run `scripts/patch-sdk-rust.sh` before you build.
- A bridge that builds sdk-rust from its `sdk-core` submodule patches `temporalio-common` and
  `temporalio-streams` to those copies in `[patch.crates-io]`, so that each type exists once.
- A FIPS build uses `default-features = false` and the `fips` feature. The default feature links
  `ring`.

## License

MIT. See `LICENSE.txt`.
