from collections.abc import Awaitable
from datetime import timedelta
from typing import TypeVar

from google.protobuf.message import Message

_Response = TypeVar("_Response", bound=Message)

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
    def call(
        self, operation: str, request: bytes, response_type: type[_Response]
    ) -> Awaitable[_Response]: ...
