import asyncio
import os
import socket
import uuid
from datetime import timedelta
from typing import Any

import pytest
from helpers import (
    REDIS_URL,
    needs_redis,
    new_store,
    new_stream,
    stream_error,
    with_store,
)
from temporalio.api.enums.v1 import StreamErrorReason
from temporalio.api.streamservice.v1 import (
    DescribeStreamRequest,
    DescribeStreamResponse,
)
from temporalio.client import Client, StreamStore
from temporalio.nexus.system.stream_service import (
    ClientProducer,
    Producer,
    ProducerKindClient,
    StreamRef,
    StreamStatus,
)

from temporalio_streams_redis import RedisStreamStore


def test_store_is_a_plain_stream_store():
    assert issubclass(RedisStreamStore, StreamStore)
    for cls in RedisStreamStore.__mro__:
        if cls is StreamStore:
            continue
        parts = cls.__module__.split(".")
        if parts[0] == "temporalio":
            assert not any(part.startswith("_") for part in parts), (
                f"{cls.__qualname__} comes from the private module {cls.__module__}"
            )


@needs_redis
async def test_client_creates_appends_reads_and_closes(client: Client):
    stream_client = with_store(client, new_store())
    stream = await new_stream(stream_client)
    assert stream.created
    assert stream.ref.store == "redis"

    appended = await stream.append("a", "b")
    assert appended.first_offset is not None
    assert appended.last_offset is not None
    records = [record async for record in stream.read()]
    assert [(record.body, record.offset) for record in records] == [
        ("a", appended.first_offset),
        ("b", appended.last_offset),
    ]

    await stream.close()
    described = await stream.describe()
    assert described.status == StreamStatus.StreamStatusClosed
    assert described.last_offset == appended.last_offset
    page = await stream.read_page(from_offset=appended.last_offset)
    assert page.done
    assert page.records == []


@needs_redis
async def test_client_append_with_sequence_writes_once(client: Client):
    stream = await new_stream(with_store(client, new_store()))
    producer = Producer(id="mine", kind=ProducerKindClient(ClientProducer()))
    first = await stream.append("a", "b", producer=producer, sequence=5)
    second = await stream.append("a", "b", producer=producer, sequence=5)
    assert second == first
    records = [record async for record in stream.read()]
    assert [record.body for record in records] == ["a", "b"]
    assert [record.offset for record in records] == [
        first.first_offset,
        first.last_offset,
    ]


@needs_redis
async def test_failed_calls_raise_stream_error_with_reason(client: Client):
    stream_client = with_store(client, new_store())
    stream = await new_stream(stream_client)

    err = await stream_error(
        stream_client.create_stream(id=stream.ref.id, body_type=str)
    )
    assert err.reason == StreamErrorReason.STREAM_ERROR_REASON_ALREADY_EXISTS

    unknown = stream_client.get_stream(
        StreamRef(id=f"stream-{uuid.uuid4()}"), body_type=str
    )
    err = await stream_error(unknown.describe())
    assert err.reason == StreamErrorReason.STREAM_ERROR_REASON_NOT_FOUND

    await stream.close()
    err = await stream_error(stream.append("a"))
    assert err.reason == StreamErrorReason.STREAM_ERROR_REASON_CLOSED


@needs_redis
async def test_read_with_wait_gets_a_later_record(client: Client):
    stream_client = with_store(client, new_store())
    stream = await new_stream(stream_client)

    async def append_later() -> None:
        await asyncio.sleep(0.5)
        await stream.append("late")

    append = asyncio.create_task(append_later())
    page = await stream.read_page(wait=timedelta(seconds=10))
    await append
    assert [record.body for record in page.records] == ["late"]


@needs_redis
async def test_cancelled_read_ends_and_the_store_still_works(client: Client):
    stream_client = with_store(client, new_store())
    stream = await new_stream(stream_client)

    read = asyncio.create_task(stream.read_page(wait=timedelta(seconds=30)))
    await asyncio.sleep(0.2)
    read.cancel()
    _, pending = await asyncio.wait({read}, timeout=2)
    assert not pending, "The read did not end within 2 s of its cancel"
    assert read.cancelled()

    await stream.append("a")
    page = await stream.read_page()
    assert [record.body for record in page.records] == ["a"]


@needs_redis
async def test_key_prefixes_keep_streams_apart(client: Client):
    first = with_store(client, new_store())
    second = with_store(client, new_store())
    stream = await new_stream(first)
    await stream.append("a")

    other = second.get_stream(StreamRef(id=stream.ref.id), body_type=str)
    err = await stream_error(other.describe())
    assert err.reason == StreamErrorReason.STREAM_ERROR_REASON_NOT_FOUND
    created = await second.create_stream(id=stream.ref.id, body_type=str)
    assert created.created


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


async def test_store_without_server_fails_at_first_call(client: Client):
    store = RedisStreamStore(f"redis://127.0.0.1:{_free_port()}")
    stream_client = with_store(client, store)
    err = await stream_error(new_stream(stream_client))
    assert err.reason == StreamErrorReason.STREAM_ERROR_REASON_STORAGE_UNAVAILABLE
    assert not err.non_retryable


@needs_redis
def test_bad_option_fails_with_the_crate_message():
    assert REDIS_URL
    with pytest.raises(ValueError, match="cluster"):
        RedisStreamStore([REDIS_URL, REDIS_URL])


@pytest.mark.parametrize(
    "option", ["retention", "producer_idle_ttl", "response_timeout", "wait_timeout"]
)
def test_negative_duration_fails_with_value_error(option: str):
    options: dict[str, Any] = {option: timedelta(seconds=-1)}
    with pytest.raises(ValueError, match=f"{option} must not be negative"):
        RedisStreamStore("redis://127.0.0.1:6379", **options)


@pytest.mark.skipif(not hasattr(os, "fork"), reason="The platform has no fork")
async def test_store_in_a_forked_child_raises_runtime_error():
    store = RedisStreamStore("redis://127.0.0.1:6379")
    # An empty request fails before any Redis call. The first call starts the package's threads
    # in this process.
    await stream_error(store.describe_stream(DescribeStreamRequest()))

    pid = os.fork()
    if pid == 0:
        status = 1
        try:
            store._runtime.call("DescribeStream", b"", DescribeStreamResponse)  # type: ignore[reportPrivateUsage]
        except RuntimeError as err:
            status = 0 if "child of a fork" in str(err) else 2
        finally:
            os._exit(status)
    _, wait_status = os.waitpid(pid, 0)
    assert os.waitstatus_to_exitcode(wait_status) == 0
