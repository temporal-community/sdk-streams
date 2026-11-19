# temporalio-streams-redis

Experimental.

`RedisStreamStore` is a Stream Store for the Temporal Python SDK. It keeps Streams in Redis 7.0 or
later. The package has its own native module, which runs the Redis Store of the
`temporalio-streams-redis` crate in this repo. The SDK has no Redis code.

The Store has the same public interface as every other Store, `temporalio.client.StreamStore`. The
package needs a `temporalio` that has Streams.

## Install

No release is on PyPI. Build it from a checkout of this repo, with Rust and `maturin` installed:

```sh
pip install ./python/redis
```

## Use

```python
from temporalio.client import Client, StreamConfig
from temporalio_streams_redis import RedisStreamStore

client = await Client.connect(
    "localhost:7233",
    stream_config=StreamConfig(stores=[RedisStreamStore("redis://localhost:6379")]),
)
stream = await client.create_stream(id="my-stream", body_type=str)
await stream.append("hello")
```

A Worker that is built from this client uses the same Store for Activities and Workflows.

## Options

| Option | Default | Use |
|---|---|---|
| `url` | | One `redis://` or `rediss://` URL, or the seed URLs of a cluster |
| `cluster` | `False` | Set for a Redis Cluster |
| `key_prefix` | `temporal-streams` | The prefix of every key |
| `retention` | 7 days | How long a Stream stays readable after its close |
| `producer_idle_ttl` | none | How long an open Stream stays after the last write |
| `response_timeout` | 5 s | The longest time of one Redis call |
| `wait_replicas` | 0 | The replicas that must have each write, with `WAIT` |
| `wait_timeout` | 1 s | How long a write waits for its replicas |
| `max_record_bytes` | 1536 KiB | The most bytes of one stored Record. At most the default |
| `max_batch_bytes` | 4 MiB | The most bytes of the stored Records of one batch. At most the default |
| `max_batch_records` | 1000 | The most Records of one batch. At most the default |
| `fence` | `True` | Refuse an append or a close of a lower epoch of a Producer. `False` takes them |
| `name` | `redis` | The Store name that the refs of its Streams carry |

The URL sets the credentials and TLS. A key has no namespace, so give each Temporal namespace its
own `key_prefix`.

## Workflow code

Client and Activity code call the methods of the Store, which call the Rust of this package. Core
is not in that path.

For Workflow code, Core calls the Store as it calls any Store that is written in Python. It runs
each call as a task on the Worker's event loop. A create or a describe of a Stream in Workflow code
runs in a Local Activity. The package needs no code for this.

## The Workflow sandbox

Build the Store where you build the client. A module that defines Workflows has no need to
import this package. If such a module does import it, pass the import through the sandbox:

    with workflow.unsafe.imports_passed_through():
        from temporalio_streams_redis import RedisStreamStore

## Limits

- The package is experimental, and it has no release on PyPI.
- It has no FIPS wheel. The crate has a `fips` feature, but no wheel is built with it.
- Its calls from client and Activity code record no SDK metric.

## Development

Run `maturin develop` in `python/redis/`. Then run the tests in the same directory:

```sh
STREAMS_REDIS_URL=redis://localhost:6379 pytest tests
```

The tests need a `temporalio` with a bridge that has the Stream runtime. Without the URL, the tests
that need Redis are skipped. The tests that are not about Redis are in `python/suite`, and
`tests/test_suite_on_redis.py` runs them on this Store.
