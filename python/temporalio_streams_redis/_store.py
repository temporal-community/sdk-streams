from __future__ import annotations

from collections.abc import Sequence
from datetime import timedelta

import temporalio.client
from temporalio.api.streamservice.v1 import (
    AppendRecordsRequest,
    AppendRecordsResponse,
    CreateStreamRequest,
    CreateStreamResponse,
    DescribeStreamRequest,
    DescribeStreamResponse,
    ReadRecordsRequest,
    ReadRecordsResponse,
)

from temporalio_streams_redis._native import RedisStore


class RedisStreamStore(temporalio.client.StreamStore):
    """A Stream Store that keeps its Streams in Redis 7.0 or later.

    The Store connects at its first call. Shared Rust validation gives client,
    Activity and Workflow calls the same request and response checks.

    A failed call raises :py:class:`temporalio.exceptions.StreamError`. A failure
    of Redis or of the Store's setup, such as a wrong password, has the reason
    ``STREAM_ERROR_REASON_STORE_UNAVAILABLE``.

    A Redis key has no Temporal namespace, so a Stream id is unique in one Redis
    and key prefix. Give each namespace its own ``key_prefix`` to keep their
    Streams apart.

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
        fence: bool = False,
        resend_window: int | None = None,
        response_timeout: timedelta | None = None,
        wait_replicas: int | None = None,
        wait_timeout: timedelta | None = None,
        max_blocking_connections: int | None = None,
        name: str = "redis",
    ) -> None:
        """Create a Redis Store.

        ``None`` uses the default. Counts and durations must be positive.
        ``retention`` and ``wait_timeout`` must be at least 1 ms.

        Args:
            url: A ``redis://`` or ``rediss://`` URL, or the URLs of some nodes of
                a Redis Cluster. The credentials and TLS come from the URL, and
                every node of a cluster gets those of the first URL.
            cluster: Whether the URLs are nodes of a Redis Cluster.
            key_prefix: The prefix of every key, ``temporal-streams`` by default.
                It has only letters, digits and the characters ``-._~:``.
            retention: How long a Stream stays after its create or the last
                append that wrote a batch, 7 days by default. The Stream then
                expires. A Stream keeps the Retention of the Store that created it.
            fence: Whether the Store refuses a write from an earlier attempt of a
                Producer, with ``STREAM_ERROR_REASON_FENCED``, once a later
                attempt of it has written to the Stream.
            resend_window: The number of batches of each Producer of a Stream
                whose request id the Store remembers, 16 by default. An append of
                the same attempt with a remembered request id writes nothing and
                returns the offsets of the first write.
            response_timeout: How long a call waits for Redis to respond, 5
                seconds by default.
            wait_replicas: The number of replicas that must have a write before
                its call succeeds, with the Redis command ``WAIT``. By default a
                write waits for none, and a failover can lose it.
            wait_timeout: How long a write waits for those replicas, 1 second by
                default. Keep it well below ``response_timeout``.
            max_blocking_connections: Maximum number of calls that block on Redis
                at a time, each on its own connection, 256 by default. It holds
                for the reads of one Redis node that wait for a Record, and again
                for the writes that wait for their replicas. Values above Tokio's
                permit limit use that limit.
            name: The name that a ``StreamConfig`` registers the Store under.
        """
        self._name = name
        self._store = RedisStore(
            {
                "urls": [url] if isinstance(url, str) else list(url),
                "cluster": cluster,
                "key_prefix": key_prefix,
                "retention": retention,
                "fence": fence,
                "resend_window": resend_window,
                "response_timeout": response_timeout,
                "wait_replicas": wait_replicas,
                "wait_timeout": wait_timeout,
                "max_blocking_connections": max_blocking_connections,
            }
        )

    def name(self) -> str:
        """Return the name that a ``StreamConfig`` registers this Store under."""
        return self._name

    async def create_stream(
        self, request: CreateStreamRequest, *, request_id: str
    ) -> CreateStreamResponse:
        """See base class."""
        response = await self._store.create_stream(
            request.SerializeToString(), request_id
        )
        return CreateStreamResponse.FromString(response)

    async def append_records(
        self, request: AppendRecordsRequest, *, request_id: str
    ) -> AppendRecordsResponse:
        """See base class."""
        response = await self._store.append_records(
            request.SerializeToString(), request_id
        )
        return AppendRecordsResponse.FromString(response)

    async def read_records(self, request: ReadRecordsRequest) -> ReadRecordsResponse:
        """See base class."""
        response = await self._store.read_records(request.SerializeToString())
        return ReadRecordsResponse.FromString(response)

    async def describe_stream(
        self, request: DescribeStreamRequest
    ) -> DescribeStreamResponse:
        """See base class."""
        response = await self._store.describe_stream(request.SerializeToString())
        return DescribeStreamResponse.FromString(response)
