"""Exercise both Core and client Store paths against one Stream."""

import asyncio
import uuid
from datetime import timedelta

import pytest
from conftest import Redis, free_port
from temporalio import activity, workflow
from temporalio.client import StreamConfig
from temporalio.nexus.system.stream_service import (
    ProducerKindWorkflow,
    ProducerKindWorkflowActivity,
    StreamRecord,
    StreamRef,
)
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from temporalio_streams_redis import RedisStreamStore

RECORDS = 5


@activity.defn
async def append_in_activity(stream_id: str) -> None:
    stream = activity.client().get_stream(StreamRef[str](id=stream_id))
    await stream.append("activity 1", "activity 2")


@workflow.defn
class AppendingWorkflow:
    @workflow.run
    async def run(self, stream_id: str) -> list[str]:
        stream = await workflow.create_stream(id=stream_id, body_type=str)
        await stream.append("workflow 1")
        await workflow.execute_activity(
            append_in_activity,
            stream_id,
            activity_id="append",
            start_to_close_timeout=timedelta(seconds=30),
        )
        await stream.append("workflow 2", "workflow 3")
        # Streams have no close operation, so stop after the expected Records.
        bodies: list[str] = []
        async for record in stream.read():
            bodies.append(record.body)
            if len(bodies) == RECORDS:
                break
        return bodies


def writer(record: StreamRecord[str]) -> tuple[str, int]:
    assert record.producer
    kind = record.producer.kind
    if isinstance(kind, ProducerKindWorkflow):
        assert kind.value.attempt
        return kind.value.run_id, kind.value.attempt.attempt
    assert isinstance(kind, ProducerKindWorkflowActivity)
    return f"{kind.value.run_id} {kind.value.activity_id}", kind.value.attempt.attempt


async def test_a_workflow_writes_through_core_to_redis(redis: Redis):
    config = StreamConfig(
        [RedisStreamStore(redis.url, key_prefix=f"test-{uuid.uuid4()}")]
    )
    async with await WorkflowEnvironment.start_local(
        port=free_port(), stream_config=config
    ) as env:
        async with Worker(
            env.client,
            task_queue="streams",
            workflows=[AppendingWorkflow],
            activities=[append_in_activity],
        ):
            # A lost Record would leave the reads of the test waiting for it.
            handle = await env.client.start_workflow(
                AppendingWorkflow.run,
                "events",
                id="wf",
                task_queue="streams",
                execution_timeout=timedelta(seconds=60),
            )
            read_by_workflow = await handle.result()

        stream = env.client.get_stream(StreamRef[str](id="events"))
        records: list[StreamRecord[str]] = []
        async with asyncio.timeout(30):
            async for page in stream.read_pages():
                records.extend(page.records)
                if len(records) >= RECORDS:
                    break
        # Workflow code cannot select its attempt, so Core must attach it.
        run = handle.result_run_id
        by_workflow, by_activity = (run, 1), (f"{run} append", 1)
        assert [(record.body, writer(record)) for record in records] == [
            ("workflow 1", by_workflow),
            ("activity 1", by_activity),
            ("activity 2", by_activity),
            ("workflow 2", by_workflow),
            ("workflow 3", by_workflow),
        ]
        assert read_by_workflow == [record.body for record in records]

        # A replay reads the Records of each read again, and fails without them.
        history = await handle.fetch_history()
        await Replayer(
            workflows=[AppendingWorkflow], stream_config=config
        ).replay_workflow(history)
        empty = RedisStreamStore(redis.url, key_prefix=f"test-{uuid.uuid4()}")
        with pytest.raises(RuntimeError, match="StreamReplayError"):
            await Replayer(
                workflows=[AppendingWorkflow], stream_config=StreamConfig([empty])
            ).replay_workflow(history)
