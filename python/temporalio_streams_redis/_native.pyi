from collections.abc import Awaitable
from datetime import timedelta

class RedisStore:
    def __init__(
        self,
        urls: list[str],
        *,
        cluster: bool,
        key_prefix: str | None,
        retention: timedelta | None,
        producer_idle_ttl: timedelta | None,
        fence: bool,
        resend_window: int | None,
        response_timeout: timedelta | None,
        wait_replicas: int | None,
        wait_timeout: timedelta | None,
        max_blocking_connections: int | None,
        max_record_bytes: int | None,
        max_batch_bytes: int | None,
        max_batch_records: int | None,
    ) -> None: ...
    def create_stream(self, request: bytes) -> Awaitable[bytes]: ...
    def append_records(self, request: bytes) -> Awaitable[bytes]: ...
    def read_records(self, request: bytes) -> Awaitable[bytes]: ...
    def close_stream(self, request: bytes) -> Awaitable[bytes]: ...
    def describe_stream(self, request: bytes) -> Awaitable[bytes]: ...
