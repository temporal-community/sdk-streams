from collections.abc import Awaitable
from datetime import timedelta
from typing import TypeVar

from google.protobuf.message import Message

_Response = TypeVar("_Response", bound=Message)

class MemoryRuntime:
    def __init__(
        self,
        *,
        retention: timedelta | None,
        producer_idle_ttl: timedelta | None,
        store_name: str,
        max_record_bytes: int,
        max_batch_bytes: int,
        max_batch_records: int,
        fence: bool,
    ) -> None: ...
    def call(
        self, operation: str, request: bytes, response_type: type[_Response]
    ) -> Awaitable[_Response]: ...
