import asyncio
import time
import uuid
from collections.abc import Awaitable, Callable, Sequence
from datetime import timedelta
from typing import Any, TypeVar

import pytest
from temporalio.client import Client, StreamConfig, StreamStore
from temporalio.exceptions import StreamError
from temporalio.nexus.system.stream_service import StreamRef
from temporalio.nexus.system.stream_service.client import Stream
from temporalio.service import RPCError, RPCStatusCode
from temporalio.worker import Worker

T = TypeVar("T")

# The fixture `new_store` gives this function.
NewStore = Callable[..., StreamStore]


def with_store(client: Client, store: StreamStore) -> Client:
    config = client.config()
    config["stream_config"] = StreamConfig(stores=[store])
    return Client(**config)


async def new_stream(client: Client) -> Stream[str]:
    return await client.create_stream(id=f"stream-{uuid.uuid4()}", body_type=str)


async def filled_stream(
    client: Client, bodies: list[str], *, close: bool = True
) -> StreamRef[str]:
    stream = await client.create_stream(id=f"stream-{uuid.uuid4()}", body_type=str)
    if bodies:
        await stream.append(*bodies)
    if close:
        await stream.close()
    return stream.ref


async def stream_error(call: Awaitable[object]) -> StreamError:
    with pytest.raises(StreamError) as err:
        await call
    return err.value


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
