from collections.abc import Awaitable
from datetime import timedelta

class StreamCallError(Exception):
    """A failed call. The args are the reason (an int) and the message."""

class RedisRuntime:
    def __init__(
        self,
        urls: list[str],
        *,
        cluster: bool,
        key_prefix: str | None,
        retention: timedelta | None,
        producer_idle_ttl: timedelta | None,
        response_timeout: timedelta | None,
        store_name: str,
        wait_replicas: int,
        wait_timeout: timedelta | None,
    ) -> None: ...
    def call(self, operation: str, request: bytes) -> Awaitable[bytes]: ...
