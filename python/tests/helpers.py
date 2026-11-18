import asyncio
import os
import time
import uuid
from collections.abc import Awaitable, Callable, Sequence
from datetime import timedelta
from typing import Any, TypeVar

import pytest
from temporalio.client import Client, StreamConfig, StreamStore
from temporalio.exceptions import StreamError
from temporalio.nexus.system.stream_service.client import Stream
from temporalio.service import RPCError, RPCStatusCode
from temporalio.worker import Worker
from temporalio.worker.workflow_sandbox import (
    SandboxedWorkflowRunner,
    SandboxRestrictions,
)

from temporalio_streams_redis import RedisStreamStore

REDIS_URL = os.environ.get("STREAMS_REDIS_URL")

needs_redis = pytest.mark.skipif(
    not REDIS_URL, reason="STREAMS_REDIS_URL is not set to a Redis server"
)

T = TypeVar("T")


def new_store() -> RedisStreamStore:
    assert REDIS_URL
    # Each test has its own key prefix, so that runs on one Redis don't share Streams.
    return RedisStreamStore(REDIS_URL, key_prefix=f"test-{uuid.uuid4()}")


def with_store(client: Client, store: StreamStore) -> Client:
    config = client.config()
    config["stream_config"] = StreamConfig(stores=[store])
    return Client(**config)


async def new_stream(client: Client) -> Stream[str]:
    return await client.create_stream(id=f"stream-{uuid.uuid4()}", body_type=str)


async def stream_error(call: Awaitable[object]) -> StreamError:
    with pytest.raises(StreamError) as err:
        await call
    return err.value


def sandbox_runner() -> SandboxedWorkflowRunner:
    # The test modules import the package, so the sandbox must not import it again.
    restrictions = SandboxRestrictions.default.with_passthrough_modules(
        "temporalio_streams_redis"
    )
    return SandboxedWorkflowRunner(restrictions=restrictions)


def new_worker(
    client: Client,
    *workflows: type,
    activities: Sequence[Callable[..., Any]] = (),
    task_queue: str | None = None,
    max_cached_workflows: int = 1000,
    **kwargs: Any,
) -> Worker:
    return Worker(
        client,
        task_queue=task_queue or str(uuid.uuid4()),
        workflows=workflows,
        activities=activities,
        workflow_runner=sandbox_runner(),
        max_cached_workflows=max_cached_workflows,
        **kwargs,
    )


async def assert_eventually(
    fn: Callable[[], Awaitable[T]],
    *,
    timeout: timedelta = timedelta(seconds=10),
    interval: timedelta = timedelta(milliseconds=200),
    retry_on_rpc_cancelled: bool = True,
) -> T:
    start_sec = time.monotonic()
    while True:
        try:
            return await fn()
        except AssertionError:
            if timedelta(seconds=time.monotonic() - start_sec) >= timeout:
                raise
        except RPCError as e:
            if not (retry_on_rpc_cancelled and e.status == RPCStatusCode.CANCELLED):
                raise
            if timedelta(seconds=time.monotonic() - start_sec) >= timeout:
                raise
        await asyncio.sleep(interval.total_seconds())
