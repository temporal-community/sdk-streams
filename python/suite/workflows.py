import uuid
from datetime import timedelta
from typing import Any

from temporalio import activity, workflow
from temporalio.api.enums.v1 import StreamErrorReason
from temporalio.client import Client, WorkflowHandle
from temporalio.exceptions import StreamError
from temporalio.nexus.system.stream_service import (
    ProducerKindLocalActivity,
    ProducerKindWorkflow,
    ProducerKindWorkflowActivity,
    StreamRef,
    StreamStatus,
)
from temporalio.nexus.system.stream_service.workflow import create_stream
from temporalio.worker import Replayer

from suite.helpers import (
    NewStore,
    filled_stream,
    new_stream,
    new_worker,
    stream_error,
    with_store,
)


@activity.defn
async def append_values(ref: StreamRef[str], values: list[str]) -> None:
    await activity.client().get_stream(ref, body_type=str).append(*values)


@workflow.defn
class AppendWorkflow:
    @workflow.run
    async def run(self, stream_id: str) -> StreamRef[str]:
        stream = await create_stream(id=stream_id, body_type=str)
        await workflow.execute_local_activity(
            append_values,
            args=[stream.ref, ["a", "b"]],
            start_to_close_timeout=timedelta(seconds=10),
        )
        return stream.ref


@workflow.defn
class AppendAndCloseWorkflow:
    @workflow.run
    async def run(self, stream_id: str) -> StreamRef[str]:
        stream = await create_stream(id=stream_id, body_type=str)
        await stream.append("a", "b")
        await stream.close()
        return stream.ref


@activity.defn
async def append_in_activity(stream_id: str) -> None:
    stream = activity.client().get_stream(StreamRef(id=stream_id), body_type=str)
    await stream.append("from-activity")


@workflow.defn
class RunActivityWorkflow:
    @workflow.run
    async def run(self, stream_id: str) -> None:
        await workflow.execute_activity(
            append_in_activity,
            stream_id,
            start_to_close_timeout=timedelta(seconds=10),
        )


async def test_workflow_local_activity_appends_with_local_activity_producer(
    client: Client, new_store: NewStore
):
    stream_client = with_store(client, new_store())
    async with new_worker(
        stream_client, AppendWorkflow, activities=[append_values]
    ) as worker:
        handle = await stream_client.start_workflow(
            AppendWorkflow.run,
            f"stream-{uuid.uuid4()}",
            id=f"workflow-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        ref = await handle.result()

    stream = stream_client.get_stream(ref, body_type=str)
    records = [record async for record in stream.read()]
    assert [record.body for record in records] == ["a", "b"]
    run_id = handle.result_run_id
    for record in records:
        assert record.producer is not None
        assert isinstance(record.producer.kind, ProducerKindLocalActivity)
        assert record.producer.kind.value.run_id == run_id


async def test_workflow_appends_and_closes(client: Client, new_store: NewStore):
    stream_client = with_store(client, new_store())
    async with new_worker(stream_client, AppendAndCloseWorkflow) as worker:
        handle = await stream_client.start_workflow(
            AppendAndCloseWorkflow.run,
            f"stream-{uuid.uuid4()}",
            id=f"workflow-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        ref = await handle.result()

    stream = stream_client.get_stream(ref, body_type=str)
    records = [record async for record in stream.read()]
    assert [record.body for record in records] == ["a", "b"]
    run_id = handle.result_run_id
    for record in records:
        # A Workflow writes as itself, with its Workflow Task as the epoch.
        assert record.producer is not None
        assert record.producer.id == f"wf:{run_id}"
        assert isinstance(record.producer.kind, ProducerKindWorkflow)
        assert record.producer.kind.value.run_id == run_id
        epoch = record.producer.kind.value.epoch
        assert epoch is not None
        assert epoch.started_event_id > 0
        assert epoch.attempt == 1
    described = await stream.describe()
    assert described.status == StreamStatus.StreamStatusClosed
    err = await stream_error(stream.append("c"))
    assert err.reason == StreamErrorReason.STREAM_ERROR_REASON_CLOSED


@activity.defn
async def append_in_two_steps(stream_id: str, seen_id: str) -> None:
    client = activity.client()
    stream = client.get_stream(StreamRef(id=stream_id), body_type=str)
    seen = client.get_stream(StreamRef(id=seen_id), body_type=str)
    await stream.append("a", "b")
    # The second step waits until the Workflow says that it has the first, so the
    # Workflow reads while the Activity runs.
    async for _ in seen.read(wait=timedelta(seconds=30)):
        break
    await stream.append("c", "d")
    await stream.close()


@workflow.defn
class ReadWhileActivityRunsWorkflow:
    @workflow.run
    async def run(self, stream_id: str, seen_id: str) -> list[str]:
        writer = workflow.start_activity(
            append_in_two_steps,
            args=[stream_id, seen_id],
            start_to_close_timeout=timedelta(seconds=60),
        )
        stream = workflow.get_stream(StreamRef(id=stream_id), body_type=str)
        seen = workflow.get_stream(StreamRef(id=seen_id), body_type=str)
        got: list[str] = []
        # The wait is far longer than the pause between the two steps, so the loop ends
        # only with the page that is done.
        async for page in stream.read_pages(wait=timedelta(seconds=30)):
            for record in page.records:
                got.append(record.body)
                if record.body == "b":
                    await seen.append("the first step")
            if page.done:
                got.append("done")
        await writer
        return got


async def test_workflow_read_of_an_activity(client: Client, new_store: NewStore):
    stream_client = with_store(client, new_store())
    stream = await new_stream(stream_client)
    seen = await new_stream(stream_client)
    async with new_worker(
        stream_client, ReadWhileActivityRunsWorkflow, activities=[append_in_two_steps]
    ) as worker:
        handle = await stream_client.start_workflow(
            ReadWhileActivityRunsWorkflow.run,
            args=[stream.ref.id, seen.ref.id],
            id=f"workflow-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        assert await handle.result() == ["a", "b", "c", "d", "done"]
    # The default replay source keeps no Record in History, so the replay reads the Store.
    await Replayer(
        workflows=[ReadWhileActivityRunsWorkflow],
        stream_config=stream_client.config()["stream_config"],
    ).replay_workflow(await handle.fetch_history())


async def test_activity_appends_with_activity_producer(
    client: Client, new_store: NewStore
):
    stream_client = with_store(client, new_store())
    stream = await new_stream(stream_client)
    async with new_worker(
        stream_client, RunActivityWorkflow, activities=[append_in_activity]
    ) as worker:
        handle = await stream_client.start_workflow(
            RunActivityWorkflow.run,
            stream.ref.id,
            id=f"workflow-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        await handle.result()

    [record] = [record async for record in stream.read()]
    assert record.body == "from-activity"
    assert record.producer is not None
    assert isinstance(record.producer.kind, ProducerKindWorkflowActivity)
    assert record.producer.kind.value.run_id == handle.result_run_id


async def _run(
    client: Client, workflow_type: type[Any], *args: Any
) -> tuple[WorkflowHandle[Any, Any], Any]:
    async with new_worker(client, workflow_type) as worker:
        handle: WorkflowHandle[Any, Any] = await client.start_workflow(
            workflow_type.run,
            args=list(args),
            id=f"workflow-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        return handle, await handle.result()


# A client's read clamps each of these, or refuses it, and a Workflow read does the same.
_READ_LIMIT_CASES: list[dict[str, Any]] = [
    {"max_bytes": -1},
    {"max_records": 0},
    {"max_records": -1},
    {"max_records": 5000},
    {"wait": timedelta(seconds=-1)},
    {"wait": timedelta(seconds=120)},
]


@workflow.defn
class ReadLimitsWorkflow:
    @workflow.run
    async def run(self, ref: StreamRef[str]) -> list[Any]:
        stream = workflow.get_stream(ref, body_type=str)
        results: list[Any] = []
        for arguments in _READ_LIMIT_CASES:
            try:
                page = await stream.read_page(**arguments)
                results.append(len(page.records))
            except StreamError as err:
                results.append([StreamErrorReason.Name(err.reason), err.message])
        return results


async def test_read_takes_the_limits_of_a_client_read(
    client: Client, new_store: NewStore
):
    stream_client = with_store(client, new_store())
    ref = await filled_stream(stream_client, ["a", "b", "c"])
    _, results = await _run(stream_client, ReadLimitsWorkflow, ref)
    stream = stream_client.get_stream(ref, body_type=str)
    expected: list[Any] = []
    for arguments in _READ_LIMIT_CASES:
        try:
            page = await stream.read_page(**arguments)
            expected.append(len(page.records))
        except StreamError as err:
            expected.append([StreamErrorReason.Name(err.reason), err.message])
    assert results == expected
    assert results[0] == ["STREAM_ERROR_REASON_INVALID", expected[0][1]]


@workflow.defn
class BatchOverLimitsWorkflow:
    @workflow.run
    async def run(self, ref: StreamRef[str]) -> str:
        stream = workflow.get_stream(ref, body_type=str)
        reason = "NO_ERROR"
        try:
            await stream.append("a", "b", "c")
        except StreamError as err:
            reason = StreamErrorReason.Name(err.reason)
        await stream.append("d", "e")
        return reason


async def test_workflow_batch_over_the_limits_of_its_store_is_too_large(
    client: Client, new_store: NewStore
):
    stream_client = with_store(client, new_store(max_batch_records=2))
    stream = await new_stream(stream_client)
    _, reason = await _run(stream_client, BatchOverLimitsWorkflow, stream.ref)
    assert reason == "STREAM_ERROR_REASON_TOO_LARGE"
    # Only the batch within the limits was written.
    assert [record.body async for record in stream.read()] == ["d", "e"]
