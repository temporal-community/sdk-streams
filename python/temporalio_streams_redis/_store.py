from __future__ import annotations

from collections.abc import Sequence
from datetime import timedelta
from typing import TypeVar

import google.protobuf.message
import temporalio.api.enums.v1
import temporalio.api.streamservice.v1
import temporalio.client
import temporalio.exceptions

from temporalio_streams_redis._native import RedisRuntime, StreamCallError

_Response = TypeVar("_Response", bound=google.protobuf.message.Message)


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
        name: str = "redis",
    ) -> None:
        """Create a Redis Store. It checks its options and connects at its first call.

        A bad option raises ``ValueError``, with the message of the Redis crate for the
        options that the crate checks.

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
            name: Name of the Store, which the refs of its Streams carry, so that every
                process can route a ref that it gets.
        """
        _check_not_negative(
            retention=retention,
            producer_idle_ttl=producer_idle_ttl,
            response_timeout=response_timeout,
            wait_timeout=wait_timeout,
        )
        self._name = name
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
        )

    @property
    def name(self) -> str:
        """Name of the Store."""
        return self._name

    async def create_stream(
        self, request: temporalio.api.streamservice.v1.CreateStreamRequest
    ) -> temporalio.api.streamservice.v1.CreateStreamResponse:
        """See base class."""
        return await self._call(
            "CreateStream",
            request,
            temporalio.api.streamservice.v1.CreateStreamResponse,
        )

    async def append_records(
        self, request: temporalio.api.streamservice.v1.AppendRecordsRequest
    ) -> temporalio.api.streamservice.v1.AppendRecordsResponse:
        """See base class."""
        return await self._call(
            "AppendRecords",
            request,
            temporalio.api.streamservice.v1.AppendRecordsResponse,
        )

    async def read_records(
        self, request: temporalio.api.streamservice.v1.ReadRecordsRequest
    ) -> temporalio.api.streamservice.v1.ReadRecordsResponse:
        """See base class."""
        return await self._call(
            "ReadRecords",
            request,
            temporalio.api.streamservice.v1.ReadRecordsResponse,
        )

    async def close_stream(
        self, request: temporalio.api.streamservice.v1.CloseStreamRequest
    ) -> temporalio.api.streamservice.v1.CloseStreamResponse:
        """See base class."""
        return await self._call(
            "CloseStream",
            request,
            temporalio.api.streamservice.v1.CloseStreamResponse,
        )

    async def describe_stream(
        self, request: temporalio.api.streamservice.v1.DescribeStreamRequest
    ) -> temporalio.api.streamservice.v1.DescribeStreamResponse:
        """See base class."""
        return await self._call(
            "DescribeStream",
            request,
            temporalio.api.streamservice.v1.DescribeStreamResponse,
        )

    async def _call(
        self,
        operation: str,
        request: google.protobuf.message.Message,
        response_type: type[_Response],
    ) -> _Response:
        # The native module takes and gives the messages in their wire form.
        try:
            response = await self._runtime.call(operation, request.SerializeToString())
        except StreamCallError as err:
            reason, message = err.args
            raise temporalio.exceptions.StreamError(
                message, reason=_stream_error_reason(reason)
            ) from None
        return response_type.FromString(response)


def _stream_error_reason(
    value: int,
) -> temporalio.api.enums.v1.StreamErrorReason.ValueType:
    # A reason that this SDK doesn't know gives the unspecified reason.
    if value not in temporalio.api.enums.v1.StreamErrorReason.values():
        value = (
            temporalio.api.enums.v1.StreamErrorReason.STREAM_ERROR_REASON_UNSPECIFIED
        )
    return temporalio.api.enums.v1.StreamErrorReason.ValueType(value)


def _check_not_negative(**durations: timedelta | None) -> None:
    for label, value in durations.items():
        if value is not None and value < timedelta():
            raise ValueError(f"{label} must not be negative")


__all__ = ["RedisStreamStore"]
