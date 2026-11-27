# temporalio-streams-redis

Experimental.

`RedisStreamStore` is a Stream Store for the Temporal Python SDK. It keeps Streams in Redis 7.0 or
later. The package has its own native module, which runs the Redis Store of the
`temporalio-streams-redis` crate of this repo. The SDK has no Redis code.

## Install

The package needs the SDK's unreleased `StreamStore` API. A published `temporalio` release does not
provide it yet. Install Rust and `protoc`, then use the SDK branch below from the repository root.

```sh
SDK_RUST_REF=moe/streams-crate-types scripts/patch-sdk-rust.sh
pip install "temporalio @ git+https://github.com/temporalio/sdk-python@moe/streams-api"
pip install ./python
```

The SDK branch and Store must use compatible `temporalio-streams` types. Before publishing this
package, set its `temporalio` lower bound to the first SDK release with Stream support.

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

`None` uses the default. Counts and durations must be positive.
`retention` and `wait_timeout` must be at least 1 ms.

| Option | Default | Use |
|---|---|---|
| `url` | | One `redis://` or `rediss://` URL, or the URLs of some nodes of a cluster |
| `cluster` | `False` | Set it for a Redis Cluster |
| `key_prefix` | `temporal-streams` | The prefix of every key |
| `retention` | 7 days | How long a Stream stays after its create or its last written batch |
| `fence` | `False` | Refuse a write of an earlier attempt of a Producer |
| `resend_window` | 16 | The batches of a Producer whose request id the Store remembers |
| `response_timeout` | 5 s | How long a call waits for Redis |
| `wait_replicas` | none | The replicas that must have a write, with `WAIT` |
| `wait_timeout` | 1 s | How long a write waits for its replicas |
| `max_blocking_connections` | 256 | The reads and the writes that can block at a time |
| `name` | `redis` | The name that a `StreamConfig` registers the Store under |

The URL sets the credentials and TLS. A Redis key has no Temporal namespace, so give each
namespace its own `key_prefix`. A Stream keeps the `retention` of the Store that created it.
`crates/streams-redis/docs/redis.md` describes what the Store keeps in Redis, and the access that
its Redis user needs.

## Checks

Every call runs the Stream runtime of Temporal's Rust crates in this package, which checks the
request and the response. SDK Core runs the same checks for the appends and reads of Workflow
code, so those calls are checked twice.

## Limits

- The package has no limit of a batch. The limits are `max_record_bytes`, `max_batch_bytes` and
  `max_batch_records` of the SDK's `StreamConfig`, and the SDK checks each append against them.
- A `rediss://` URL verifies the server against the root certificates of the system. A private
  CA works only through the `SSL_CERT_FILE` variable, and the Store can't send a client
  certificate.
- The Store can't be used in a child process of a fork, since its threads don't exist there.
- No FIPS wheel is built. The native module has a `fips` feature for one.

## Development

```sh
cd python
uv venv
uv pip install --group dev \
  "temporalio @ git+https://github.com/temporalio/sdk-python@moe/streams-api"
uv run --no-sync maturin develop
uv run --no-sync pytest
```

The tests start their own `redis-server` processes, from the `PATH` or from `REDIS_SERVER`.
`tests/test_worker.py` also starts a local Temporal server with
`WorkflowEnvironment.start_local()`, which downloads the Temporal CLI on its first run.
