# temporalio-streams-memory

Experimental. A Stream Store for tests of code that uses Streams with the Temporal Python SDK. It
keeps its Streams in the memory of one process.

It runs the memory Store of the `temporalio-streams-memory` crate, which applies the same rules
as the Redis Store. So a test on it sees the same fences, the same dedupe and the same errors as
code on Redis.

## Install

No release is on PyPI. Build it from a checkout of this repo, with Rust and `maturin` installed:

```sh
pip install ./python/memory
```

## Use

```python
from temporalio.client import Client, StreamConfig
from temporalio_streams_memory import MemoryStreamStore

client = await Client.connect(
    "localhost:7233",
    stream_config=StreamConfig(stores=[MemoryStreamStore()]),
)
stream = await client.create_stream(id="my-stream", body_type=str)
await stream.append("hello")
```

A Worker that is built from this client uses the same Store for Activities and Workflows. Each
`MemoryStreamStore` has its own Streams, so a Worker in another process can't see them.

## Options

| Option | Default | Use |
|---|---|---|
| `name` | `memory` | The Store name that the refs of its Streams carry |
| `retention` | 7 days | How long a closed Stream stays readable |
| `producer_idle_ttl` | none | How long an open Stream stays after the last write |
| `max_record_bytes` | 1536 KiB | The most bytes of one stored Record. At most the default |
| `max_batch_bytes` | 4 MiB | The most bytes of the stored Records of one batch. At most the default |
| `max_batch_records` | 1000 | The most Records of one batch. At most the default |

## The Workflow sandbox

Build the Store where you build the client. A module that defines Workflows has no need to
import this package. If such a module does import it, pass the import through the sandbox:

    with workflow.unsafe.imports_passed_through():
        from temporalio_streams_memory import MemoryStreamStore

## Limits

- The package is experimental, and it has no release on PyPI.
- It can't be used in a child process of a fork.
