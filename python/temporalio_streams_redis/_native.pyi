from collections.abc import Awaitable
from datetime import timedelta
from typing import TypedDict

class Options(TypedDict):
    urls: list[str]
    cluster: bool
    key_prefix: str | None
    retention: timedelta | None
    producer_idle_ttl: timedelta | None
    fence: bool
    resend_window: int | None
    response_timeout: timedelta | None
    wait_replicas: int | None
    wait_timeout: timedelta | None
    max_blocking_connections: int | None

class RedisStore:
    def __init__(self, options: Options) -> None: ...
    def create_stream(self, request: bytes, request_id: str) -> Awaitable[bytes]: ...
    def append_records(self, request: bytes, request_id: str) -> Awaitable[bytes]: ...
    def read_records(self, request: bytes) -> Awaitable[bytes]: ...
    def close_stream(self, request: bytes) -> Awaitable[bytes]: ...
    def describe_stream(self, request: bytes) -> Awaitable[bytes]: ...
