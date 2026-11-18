import asyncio
import urllib.parse
import uuid
from datetime import timedelta
from typing import Any, Self

import pytest
from helpers import REDIS_URL, needs_redis
from temporalio import workflow
from temporalio.api.enums.v1 import EventType, StreamErrorReason
from temporalio.client import (
    Client,
    WorkflowExecutionStatus,
    WorkflowHandle,
    WorkflowHistory,
)
from temporalio.exceptions import StreamError, StreamReplayError
from temporalio.nexus.system.stream_service import StreamRecord, StreamRef

from suite.helpers import assert_eventually, filled_stream, new_worker, with_store

with workflow.unsafe.imports_passed_through():
    from temporalio_streams_redis import RedisStreamStore


def _markers(history: WorkflowHistory) -> list[str]:
    return [
        event.marker_recorded_event_attributes.marker_name
        for event in history.events
        if event.event_type == EventType.EVENT_TYPE_MARKER_RECORDED
    ]


def _completed_tasks(history: WorkflowHistory) -> int:
    return sum(
        1
        for event in history.events
        if event.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED
    )


def _hex(value: bytes | None) -> str | None:
    return None if value is None else value.hex()


async def _read_all(client: Client, ref: StreamRef[str]) -> list[StreamRecord[str]]:
    stream = client.get_stream(ref, body_type=str)
    return [record async for record in stream.read()]


# A Direct append.


@workflow.defn
class AppendWorkflow:
    @workflow.run
    async def run(self, ref: StreamRef[str]) -> list[str | None]:
        stream = workflow.get_stream(ref, body_type=str)
        appended = await stream.append("a", "b")
        # A timer ends the Workflow Task, so a Worker with no cache replays the append.
        await workflow.sleep(0.05)
        return [_hex(appended.first_offset), _hex(appended.last_offset)]


# A replay that lost the Records of a read.


class _ReadThenWait:
    def __init__(self) -> None:
        self.go = False

    async def read_then_wait(self, stream_id: str) -> list[str]:
        stream = workflow.get_stream(StreamRef(id=stream_id), body_type=str)
        got = [record.body async for record in stream.read()]
        await workflow.wait_condition(lambda: self.go)
        return got

    @workflow.signal
    def proceed(self) -> None:
        self.go = True


@workflow.defn
class ReadThenWaitWorkflow(_ReadThenWait):
    @workflow.run
    async def run(self, stream_id: str) -> list[str]:
        return await self.read_then_wait(stream_id)


@workflow.defn(failure_exception_types=[StreamReplayError])
class ReadThenWaitFailsWorkflow(_ReadThenWait):
    @workflow.run
    async def run(self, stream_id: str) -> list[str]:
        return await self.read_then_wait(stream_id)


# A Store outage that Core tries again, made with a proxy in front of Redis.


async def _copy(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while data := await reader.read(65536):
            writer.write(data)
            await writer.drain()
    except ConnectionError:
        pass


class _StoreOutage:
    """A TCP proxy in front of Redis that a test takes down and brings up.

    Down, it drops each open connection and each new one, so each call of the Store
    fails with `StorageUnavailable`. Redis itself keeps running, so the tests that
    share it in other processes see no outage. `refused` counts the connections that
    it dropped while down.
    """

    def __init__(self, url: str) -> None:
        self._parts = urllib.parse.urlsplit(url)
        self._server: asyncio.Server | None = None
        self._writers: set[asyncio.StreamWriter] = set()
        self._down = False
        self.url = ""
        self.refused = 0

    async def __aenter__(self) -> Self:
        self._server = await asyncio.start_server(self._serve, "127.0.0.1", 0)
        port = self._server.sockets[0].getsockname()[1]
        credentials, at, _ = self._parts.netloc.rpartition("@")
        netloc = f"{credentials}{at}127.0.0.1:{port}"
        self.url = urllib.parse.urlunsplit(self._parts._replace(netloc=netloc))
        return self

    async def __aexit__(self, *_: object) -> None:
        self.take_down()
        if self._server:
            self._server.close()

    def take_down(self) -> None:
        self._down = True
        for writer in list(self._writers):
            writer.close()

    def bring_up(self) -> None:
        self._down = False

    async def _serve(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        if self._down:
            self.refused += 1
            writer.close()
            return
        try:
            redis_reader, redis_writer = await asyncio.open_connection(
                self._parts.hostname or "127.0.0.1", self._parts.port or 6379
            )
        except OSError:
            writer.close()
            return
        self._writers.update((writer, redis_writer))
        copies = [
            asyncio.create_task(_copy(reader, redis_writer)),
            asyncio.create_task(_copy(redis_reader, writer)),
        ]
        try:
            await asyncio.wait(copies, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for each in (writer, redis_writer):
                self._writers.discard(each)
                each.close()
            await asyncio.gather(*copies)


def _redis_client(client: Client, url: str) -> Client:
    # A short response timeout keeps a call that the outage cuts from waiting long.
    store = RedisStreamStore(
        url,
        key_prefix=f"test-{uuid.uuid4()}",
        response_timeout=timedelta(seconds=1),
    )
    return with_store(client, store)


async def _store_answers(stream: Any) -> None:
    # A client call has no retry, so the test waits until the Store answers again.
    try:
        await stream.describe()
    except StreamError as err:
        raise AssertionError(f"The Store doesn't answer: {err}") from err


@needs_redis
async def test_append_during_a_short_outage_appends_once(client: Client):
    assert REDIS_URL
    async with _StoreOutage(REDIS_URL) as outage:
        stream_client = _redis_client(client, outage.url)
        ref = await filled_stream(stream_client, [], close=False)
        stream = stream_client.get_stream(ref, body_type=str)
        async with new_worker(
            stream_client, AppendWorkflow, max_cached_workflows=0
        ) as worker:
            outage.take_down()
            handle: WorkflowHandle[Any, Any] = await stream_client.start_workflow(
                AppendWorkflow.run,
                ref,
                id=f"workflow-{uuid.uuid4()}",
                task_queue=worker.task_queue,
            )
            # The client gets the error that Core keeps from Workflow code.
            with pytest.raises(StreamError) as err:
                await stream.describe()
            assert (
                err.value.reason
                == StreamErrorReason.STREAM_ERROR_REASON_STORAGE_UNAVAILABLE
            )

            async def task_started() -> None:
                events = (await handle.fetch_history()).events
                assert any(
                    event.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_STARTED
                    for event in events
                )

            await assert_eventually(task_started)
            refused = outage.refused
            await asyncio.sleep(1)
            # The task waits for the append, which Core tries again.
            assert outage.refused > refused
            assert _completed_tasks(await handle.fetch_history()) == 0
            outage.bring_up()
            offsets = await asyncio.wait_for(handle.result(), timeout=30)
        records = await _read_all(stream_client, ref)
    assert [record.body for record in records] == ["a", "b"]
    assert offsets == [record.offset.hex() for record in records]


@workflow.defn
class ReadAcrossAnOutageWorkflow:
    def __init__(self) -> None:
        self._bodies: list[str] = []

    @workflow.run
    async def run(self, ref: StreamRef[str]) -> list[str]:
        stream = workflow.get_stream(ref, body_type=str)
        async for record in stream.read(wait=timedelta(seconds=10)):
            self._bodies.append(record.body)
        return self._bodies

    @workflow.query
    def bodies(self) -> list[str]:
        return self._bodies


@needs_redis
async def test_read_across_a_short_outage_gives_each_record_once(client: Client):
    assert REDIS_URL
    async with _StoreOutage(REDIS_URL) as outage:
        stream_client = _redis_client(client, outage.url)
        ref = await filled_stream(stream_client, ["a", "b", "c"], close=False)
        stream = stream_client.get_stream(ref, body_type=str)
        async with new_worker(stream_client, ReadAcrossAnOutageWorkflow) as worker:
            handle = await stream_client.start_workflow(
                ReadAcrossAnOutageWorkflow.run,
                ref,
                id=f"workflow-{uuid.uuid4()}",
                task_queue=worker.task_queue,
            )

            async def read_three() -> None:
                bodies = await handle.query(ReadAcrossAnOutageWorkflow.bodies)
                assert bodies == ["a", "b", "c"]

            await assert_eventually(read_three)
            # The outage cuts the read that waits for the next Record.
            outage.take_down()
            await asyncio.sleep(1.5)
            # Only the Worker's read asks the Store in the outage.
            assert outage.refused > 0
            outage.bring_up()
            await assert_eventually(lambda: _store_answers(stream))
            await stream.append("d", "e")
            await stream.close()
            result = await asyncio.wait_for(handle.result(), timeout=30)
    assert result == ["a", "b", "c", "d", "e"]


@needs_redis
@pytest.mark.parametrize("opt_in", ["worker", "defn"])
async def test_replay_with_its_store_down_fails_only_the_task(
    client: Client, opt_in: str
):
    assert REDIS_URL
    workflow_type: type[Any] = (
        ReadThenWaitFailsWorkflow if opt_in == "defn" else ReadThenWaitWorkflow
    )
    worker_options: dict[str, Any] = (
        {"workflow_failure_exception_types": [StreamReplayError]}
        if opt_in == "worker"
        else {}
    )
    async with _StoreOutage(REDIS_URL) as outage:
        stream_client = _redis_client(client, outage.url)
        ref = await filled_stream(stream_client, ["a", "b"])
        task_queue = f"tq-{uuid.uuid4()}"
        # No cache, so the next Worker replays the History and reads the Store.
        async with new_worker(
            stream_client, workflow_type, task_queue=task_queue, max_cached_workflows=0
        ):
            handle: WorkflowHandle[Any, Any] = await stream_client.start_workflow(
                workflow_type.run,
                ref.id,
                id=f"workflow-{uuid.uuid4()}",
                task_queue=task_queue,
            )

            async def read_is_in_history() -> None:
                assert "core_stream" in _markers(await handle.fetch_history())

            await assert_eventually(read_is_in_history)
        outage.take_down()
        async with new_worker(
            stream_client, workflow_type, task_queue=task_queue, **worker_options
        ):
            await handle.signal("proceed")

            async def task_failed() -> None:
                messages = [
                    event.workflow_task_failed_event_attributes.failure.message
                    for event in (await handle.fetch_history()).events
                    if event.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED
                ]
                assert messages
                assert messages[0].startswith(
                    f"StreamReplayUnavailable: Stream `{ref.id}` in Store `redis`"
                ), messages[0]

            # Core tries the Store for 5 s before it fails the task.
            await assert_eventually(task_failed, timeout=timedelta(seconds=30))
            # The opt-in doesn't fail the Workflow, since the Records may still be there.
            description = await handle.describe()
            assert description.status == WorkflowExecutionStatus.RUNNING
            outage.bring_up()
            result = await asyncio.wait_for(handle.result(), timeout=60)
    assert result == ["a", "b"]
