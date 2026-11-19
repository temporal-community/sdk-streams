import asyncio
import uuid
from datetime import timedelta

from temporalio.api.enums.v1 import StreamErrorReason
from temporalio.client import Client
from temporalio.nexus.system.stream_service import (
    ClientProducer,
    Producer,
    ProducerKindClient,
    StreamRef,
    StreamStatus,
)

from suite.helpers import NewStore, new_stream, stream_error, with_store


async def test_client_creates_appends_reads_and_closes(
    client: Client, new_store: NewStore
):
    store = new_store()
    stream_client = with_store(client, store)
    stream = await new_stream(stream_client)
    assert stream.created
    assert stream.ref.store == store.name

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


async def test_client_append_with_sequence_writes_once(
    client: Client, new_store: NewStore
):
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


async def test_failed_calls_raise_stream_error_with_reason(
    client: Client, new_store: NewStore
):
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


async def test_read_with_wait_gets_a_later_record(client: Client, new_store: NewStore):
    stream_client = with_store(client, new_store())
    stream = await new_stream(stream_client)

    async def append_later() -> None:
        await asyncio.sleep(0.5)
        await stream.append("late")

    append = asyncio.create_task(append_later())
    page = await stream.read_page(wait=timedelta(seconds=10))
    await append
    assert [record.body for record in page.records] == ["late"]


async def test_cancelled_read_ends_and_the_store_still_works(
    client: Client, new_store: NewStore
):
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


async def test_client_batch_over_the_limits_of_its_store_is_too_large(
    client: Client, new_store: NewStore
):
    stream = await new_stream(with_store(client, new_store(max_batch_records=2)))
    err = await stream_error(stream.append("a", "b", "c"))
    assert err.reason == StreamErrorReason.STREAM_ERROR_REASON_TOO_LARGE

    await stream.append("a", "b")
    # The refused batch wrote nothing.
    assert [record.body async for record in stream.read()] == ["a", "b"]
