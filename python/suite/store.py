import dataclasses
import os
from datetime import timedelta
from typing import Any

import pytest
from temporalio.api.enums.v1 import StreamErrorReason
from temporalio.api.streamservice.v1 import (
    DescribeStreamRequest,
    DescribeStreamResponse,
)
from temporalio.client import Client, StreamBatchLimits, StreamStore
from temporalio.nexus.system.stream_service import (
    ActivityEpoch,
    ActivityProducer,
    Producer,
    ProducerKindWorkflowActivity,
    Timestamp,
)

from suite.helpers import NewStore, new_stream, stream_error, with_store


def test_store_is_a_plain_stream_store(new_store: NewStore):
    store_type = type(new_store())
    assert issubclass(store_type, StreamStore)
    for cls in store_type.__mro__:
        if cls is StreamStore:
            continue
        parts = cls.__module__.split(".")
        if parts[0] == "temporalio":
            assert not any(part.startswith("_") for part in parts), (
                f"{cls.__qualname__} comes from the private module {cls.__module__}"
            )


@pytest.mark.parametrize("option", ["retention", "producer_idle_ttl"])
def test_negative_duration_fails_with_value_error(option: str, new_store: NewStore):
    options: dict[str, Any] = {option: timedelta(seconds=-1)}
    with pytest.raises(ValueError, match=f"{option} must not be negative"):
        new_store(**options)


def test_store_gives_the_limits_of_its_options(new_store: NewStore):
    assert new_store().limits == StreamBatchLimits()
    assert new_store(max_batch_records=2).limits == StreamBatchLimits(
        max_batch_records=2
    )
    for option, most in dataclasses.asdict(StreamBatchLimits()).items():
        for value in (0, most + 1):
            options: dict[str, Any] = {option: value}
            with pytest.raises(ValueError, match=option):
                new_store(**options)


@pytest.mark.skipif(not hasattr(os, "fork"), reason="The platform has no fork")
async def test_store_in_a_forked_child_raises_runtime_error(new_store: NewStore):
    store = new_store()
    # An empty request fails before the Store does any work. The first call starts the package's
    # threads in this process.
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


def _activity_producer(attempt: int) -> Producer:
    """The Producer of one Activity. A higher attempt is a higher epoch."""
    return Producer(
        id="activity:run-1:step",
        kind=ProducerKindWorkflowActivity(
            ActivityProducer(
                run_id="run-1",
                epoch=ActivityEpoch(
                    attempt_scheduled_time=Timestamp(seconds=100, nanos=0),
                    attempt=attempt,
                ),
            )
        ),
    )


async def test_store_fences_a_lower_epoch_by_default(
    client: Client, new_store: NewStore
):
    stream = await new_stream(with_store(client, new_store()))
    await stream.append("a", producer=_activity_producer(2), sequence=1)
    err = await stream_error(
        stream.append("b", producer=_activity_producer(1), sequence=1)
    )
    assert err.reason == StreamErrorReason.STREAM_ERROR_REASON_FENCED
    assert [record.body async for record in stream.read()] == ["a"]


async def test_store_with_the_fence_off_takes_a_lower_epoch(
    client: Client, new_store: NewStore
):
    stream = await new_stream(with_store(client, new_store(fence=False)))
    await stream.append("a", producer=_activity_producer(2), sequence=1)
    await stream.append("b", producer=_activity_producer(1), sequence=1)
    assert [record.body async for record in stream.read()] == ["a", "b"]
