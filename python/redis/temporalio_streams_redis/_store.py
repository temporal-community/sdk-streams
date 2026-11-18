from __future__ import annotations

from collections.abc import Sequence
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

from temporalio_streams_redis._native import RedisRuntime


class RedisStreamStore(temporalio.client.StreamStore):
    """A Stream Store that keeps its Streams in Redis 7.0 or later.

    It runs the Redis Store of the ``temporalio-streams-redis`` crate in this package's own
    native module. Client and Activity code call it directly. For Workflow code, Core calls
    it as a task on the Worker's event loop, as it calls any Store written in Python. It
    connects at its first call, so building it needs no server, and a server that is down
    fails that call. A failed call raises :py:class:`temporalio.exceptions.StreamError`.

    A key has no namespace, so a Stream id is unique in one Redis and key prefix. Give each
    namespace its own key prefix to keep their Streams apart.

    The Store can't be used in a child process of a fork.

    .. warning::
        This API is experimental and subject to change.
    """

    def __init__(
        self,
        url: str | Sequence[str],
        *,
        cluster: bool = False,
        key_prefix: str | None = None,
        retention: timedelta | None = None,
        producer_idle_ttl: timedelta | None = None,
        response_timeout: timedelta | None = None,
        wait_replicas: int = 0,
        wait_timeout: timedelta | None = None,
        max_record_bytes: int | None = None,
        max_batch_bytes: int | None = None,
        max_batch_records: int | None = None,
        name: str = "redis",
    ) -> None:
        """Create a Redis Store. It checks its options and connects at its first call.

        A negative duration raises ``ValueError``. So does a bad option, with the message of the
        Redis crate for the options that the crate checks. A limit below 1 or above its largest
        value raises ``ValueError``.

        Args:
            url: A ``redis://`` or ``rediss://`` URL, or the seed URLs of a cluster.
                Credentials and TLS come from the URL.
            cluster: Whether the URLs are of a Redis Cluster.
            key_prefix: The prefix of every key. ``None`` takes ``temporal-streams``.
                A Stream id is unique in one Redis and key prefix.
            retention: The default Retention of a new Stream, which is how long the
                Stream stays readable after its close. ``None`` takes 7 days.
            producer_idle_ttl: How long an open Stream stays after the last write of any
                Producer. ``None`` keeps an open Stream and every Record until its
                close.
            response_timeout: The longest time that one Redis call can take. ``None``
                takes 5 seconds.
            wait_replicas: How many replicas must have each write before it answers,
                with the Redis ``WAIT`` command. Zero sends no ``WAIT``.
            wait_timeout: How long a write waits for its replicas, at most half of
                ``response_timeout``. ``None`` takes 1 second.
            max_record_bytes: The most bytes of one stored Record, at most 1,572,864. ``None``
                takes that. The stored form of a Record has its Producer and its body.
            max_batch_bytes: The most bytes of the stored Records of one batch, at most
                4,194,304. ``None`` takes that.
            max_batch_records: The most Records of one batch, at most 1,000. ``None`` takes
                that.
            name: Name of the Store, which the refs of its Streams carry, so that every
                process can route a ref that it gets.
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
        self._runtime = RedisRuntime(
            [url] if isinstance(url, str) else list(url),
            cluster=cluster,
            key_prefix=key_prefix,
            retention=retention,
            producer_idle_ttl=producer_idle_ttl,
            response_timeout=response_timeout,
            store_name=name,
            wait_replicas=wait_replicas,
            wait_timeout=wait_timeout,
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


__all__ = ["RedisStreamStore"]
