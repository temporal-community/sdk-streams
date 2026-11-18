import asyncio
from datetime import timedelta

from temporalio.api.enums.v1 import StreamErrorReason
from temporalio.client import Client
from temporalio.nexus.system.stream_service import StreamRef, StreamStatus

from suite.helpers import new_stream, stream_error, with_store
from temporalio_streams_memory import MemoryStreamStore


def test_default_store_name_is_memory():
    assert MemoryStreamStore().name == "memory"


async def test_closed_stream_cannot_be_read_after_its_retention(client: Client):
    stream = await new_stream(
        with_store(client, MemoryStreamStore(retention=timedelta(milliseconds=100)))
    )
    await stream.append("a")
    await stream.close()
    # The Retention counts from the close.
    await asyncio.sleep(0.5)

    described = await stream.describe()
    assert described.status == StreamStatus.StreamStatusExpired
    err = await stream_error(stream.read_page())
    assert err.reason == StreamErrorReason.STREAM_ERROR_REASON_EXPIRED


async def test_open_stream_is_gone_after_its_producer_idle_ttl(client: Client):
    store = MemoryStreamStore(producer_idle_ttl=timedelta(milliseconds=100))
    stream = await new_stream(with_store(client, store))
    await stream.append("a")
    await asyncio.sleep(0.5)

    described = await stream.describe()
    assert described.status == StreamStatus.StreamStatusExpired
    err = await stream_error(stream.append("b"))
    assert err.reason == StreamErrorReason.STREAM_ERROR_REASON_EXPIRED
    err = await stream_error(stream.read_page())
    assert err.reason == StreamErrorReason.STREAM_ERROR_REASON_EXPIRED


async def test_two_stores_do_not_share_streams(client: Client):
    first = with_store(client, MemoryStreamStore())
    second = with_store(client, MemoryStreamStore())
    stream = await new_stream(first)
    await stream.append("a")

    other = second.get_stream(StreamRef(id=stream.ref.id), body_type=str)
    err = await stream_error(other.describe())
    assert err.reason == StreamErrorReason.STREAM_ERROR_REASON_NOT_FOUND
    created = await second.create_stream(id=stream.ref.id, body_type=str)
    assert created.created
