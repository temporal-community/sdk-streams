# temporalio-streams-redis

Experimental.

`RedisStreamStore` is a Stream Store for the Temporal Python SDK. It keeps Streams in Redis 7.0 or
later. The package has its own native module, which runs the Redis Store of the
`temporalio-streams-redis` crate of this repo. The SDK has no Redis code.

## Install

The package has no release yet. Build it from a checkout of this repo, with Rust installed.

```sh
pip install ./python
```

It needs a `temporalio` that has Streams.

## Use

```python
from temporalio.client import Client, StreamConfig
from temporalio_streams_redis import RedisStreamStore

client = await Client.connect(
    "localhost:7233",
    stream_config=StreamConfig(stores=[RedisStreamStore("redis://localhost:6379")]),
)
```

A Worker of this client uses the same Store for its Activities and Workflows.

## Options

`None` takes the default. A zero or an empty value raises `ValueError`, and no option has an
upper bound.

| Option | Default | Use |
|---|---|---|
| `url` | | One `redis://` or `rediss://` URL, or the URLs of some nodes of a cluster |
| `cluster` | `False` | Set it for a Redis Cluster |
| `key_prefix` | `temporal-streams` | The prefix of every key |
| `retention` | 7 days | How long a Stream stays readable after its close |
| `producer_idle_ttl` | none | How long an open Stream stays after its last append |
| `fence` | `False` | Refuse a write of an earlier attempt of a Producer |
| `resend_window` | 16 | The batches of a Producer whose request id the Store remembers |
| `response_timeout` | 5 s | How long a call waits for Redis |
| `wait_replicas` | none | The replicas that must have a write, with `WAIT` |
| `wait_timeout` | 1 s | How long a write waits for its replicas |
| `max_blocking_connections` | 256 | The reads and the writes that can block at a time |
| `name` | `redis` | The name that a `StreamConfig` registers the Store under |

The URL sets the credentials and TLS. A Redis key has no Temporal namespace, so give each
namespace its own `key_prefix`. A Stream keeps the `retention` and the `producer_idle_ttl` of the
Store that created it. `crates/streams-redis/docs/redis.md` describes what the Store keeps in
Redis, and the access that its Redis user needs.

## Checks

Every call runs the Stream runtime of Temporal's Rust crates in this package, which checks the
request and the response. SDK Core runs the same checks for the appends, closes and reads of
Workflow code, so those calls are checked twice.

## Limits

- A Record is at most 1.5 MiB, and a batch at most 4 MiB and 1000 Records. The package has no
  option for these, since SDK Core checks a call of Workflow code with the same defaults.
- A `rediss://` URL verifies the server against the root certificates of the system. A private
  CA works only through the `SSL_CERT_FILE` variable, and the Store can't send a client
  certificate.
- The Store can't be used in a child process of a fork, since its threads don't exist there.
- No FIPS wheel is built. The native module has a `fips` feature for one.

## Development

```sh
cd python
uv venv && uv pip install --group dev <a temporalio that has Streams>
uv run --no-sync maturin develop
uv run --no-sync pytest
```

The tests start their own `redis-server` processes, from the `PATH` or from `REDIS_SERVER`. They
need no Temporal server.
