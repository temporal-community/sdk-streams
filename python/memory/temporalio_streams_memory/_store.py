from __future__ import annotations

from datetime import timedelta

import temporalio.client
from temporalio.api.streamservice.v1 import (
    AppendRecordsRequest,
    AppendRecordsResponse,
    CloseStreamRequest,
    CloseStreamResponse,
    CreateStreamRequest,
    CreateStreamResponse,
    DescribeStreamRequest,
    DescribeStreamResponse,
    ReadRecordsRequest,
    ReadRecordsResponse,
)

from temporalio_streams_memory._native import MemoryRuntime


class MemoryStreamStore(temporalio.client.StreamStore):
    """A Stream Store that keeps its Streams in the memory of this process, for tests.

    It runs the memory Store of the ``temporalio-streams-memory`` crate in this package's own
    native module. That Store applies the same rules as the Redis Store, so a test on it sees
    the same fences, the same dedupe and the same errors as code on Redis. Client and Activity
    code call it directly. For Workflow code, Core calls it as a task on the Worker's event
    loop, as it calls any Store written in Python. A failed call raises
    :py:class:`temporalio.exceptions.StreamError`.

    The Store can't be used in a child process of a fork.

    .. warning::
        This API is experimental and subject to change.
    """

    def __init__(
        self,
        *,
        max_record_bytes: int | None = None,
        max_batch_bytes: int | None = None,
        max_batch_records: int | None = None,
        name: str = "memory",
        retention: timedelta | None = None,
        producer_idle_ttl: timedelta | None = None,
    ) -> None:
        """Create a memory Store. It builds its Stream runtime at its first call.

        A negative duration raises ``ValueError``. A limit below 1 or above its largest value
        raises ``ValueError``.

        Args:
            max_record_bytes: The most bytes of one stored Record, at most 1,572,864. ``None``
                takes that. The stored form of a Record has its Producer and its body.
            max_batch_bytes: The most bytes of the stored Records of one batch, at most
                4,194,304. ``None`` takes that.
            max_batch_records: The most Records of one batch, at most 1,000. ``None`` takes
                that.
            name: Name of the Store, which the refs of its Streams carry.
            retention: How long a closed Stream stays readable. ``None`` takes 7 days.
            producer_idle_ttl: How long an open Stream stays after the last write of any
                Producer. ``None`` keeps an open Stream until its close.
        """
        self._name = name
        # The dataclass checks each limit, so a bad one raises before the runtime exists.
        set_limits = {
            field: value
            for field, value in (
                ("max_record_bytes", max_record_bytes),
                ("max_batch_bytes", max_batch_bytes),
                ("max_batch_records", max_batch_records),
            )
            if value is not None
        }
        self._limits = temporalio.client.StreamBatchLimits(**set_limits)
        self._runtime = MemoryRuntime(
            retention=retention,
            producer_idle_ttl=producer_idle_ttl,
            store_name=name,
            max_record_bytes=self._limits.max_record_bytes,
            max_batch_bytes=self._limits.max_batch_bytes,
            max_batch_records=self._limits.max_batch_records,
        )

    @property
    def name(self) -> str:
        """Name of the Store."""
        return self._name

    @property
    def limits(self) -> temporalio.client.StreamBatchLimits:
        """The limits of one batch on this Store, from its options."""
        return self._limits

    async def create_stream(self, request: CreateStreamRequest) -> CreateStreamResponse:
        """See base class."""
        return await self._runtime.call(
            "CreateStream", request.SerializeToString(), CreateStreamResponse
        )

    async def append_records(
        self, request: AppendRecordsRequest
    ) -> AppendRecordsResponse:
        """See base class."""
        return await self._runtime.call(
            "AppendRecords", request.SerializeToString(), AppendRecordsResponse
        )

    async def read_records(self, request: ReadRecordsRequest) -> ReadRecordsResponse:
        """See base class."""
        return await self._runtime.call(
            "ReadRecords", request.SerializeToString(), ReadRecordsResponse
        )

    async def close_stream(self, request: CloseStreamRequest) -> CloseStreamResponse:
        """See base class."""
        return await self._runtime.call(
            "CloseStream", request.SerializeToString(), CloseStreamResponse
        )

    async def describe_stream(
        self, request: DescribeStreamRequest
    ) -> DescribeStreamResponse:
        """See base class."""
        return await self._runtime.call(
            "DescribeStream", request.SerializeToString(), DescribeStreamResponse
        )


__all__ = ["MemoryStreamStore"]
