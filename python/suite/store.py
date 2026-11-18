import os
from datetime import timedelta
from typing import Any

import pytest
from temporalio.api.streamservice.v1 import (
    DescribeStreamRequest,
    DescribeStreamResponse,
)
from temporalio.client import StreamStore

from suite.helpers import NewStore, stream_error


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
