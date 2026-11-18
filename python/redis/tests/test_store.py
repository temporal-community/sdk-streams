import socket
from datetime import timedelta
from typing import Any

import pytest
from helpers import REDIS_URL, needs_redis
from temporalio.api.enums.v1 import StreamErrorReason
from temporalio.client import Client
from temporalio.nexus.system.stream_service import StreamRef

from suite.helpers import NewStore, new_stream, stream_error, with_store
from temporalio_streams_redis import RedisStreamStore


def test_default_store_name_is_redis():
    assert RedisStreamStore("redis://127.0.0.1:6379").name == "redis"


@needs_redis
async def test_key_prefixes_keep_streams_apart(client: Client, new_store: NewStore):
    first = with_store(client, new_store())
    second = with_store(client, new_store())
    stream = await new_stream(first)
    await stream.append("a")

    other = second.get_stream(StreamRef(id=stream.ref.id), body_type=str)
    err = await stream_error(other.describe())
    assert err.reason == StreamErrorReason.STREAM_ERROR_REASON_NOT_FOUND
    created = await second.create_stream(id=stream.ref.id, body_type=str)
    assert created.created


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


async def test_store_without_server_fails_at_first_call(client: Client):
    store = RedisStreamStore(f"redis://127.0.0.1:{_free_port()}")
    stream_client = with_store(client, store)
    err = await stream_error(new_stream(stream_client))
    assert err.reason == StreamErrorReason.STREAM_ERROR_REASON_STORAGE_UNAVAILABLE
    assert not err.non_retryable


@needs_redis
def test_bad_option_fails_with_the_crate_message():
    assert REDIS_URL
    with pytest.raises(ValueError, match="cluster"):
        RedisStreamStore([REDIS_URL, REDIS_URL])


@pytest.mark.parametrize("option", ["response_timeout", "wait_timeout"])
def test_negative_redis_duration_fails_with_value_error(option: str):
    options: dict[str, Any] = {option: timedelta(seconds=-1)}
    with pytest.raises(ValueError, match=f"{option} must not be negative"):
        RedisStreamStore("redis://127.0.0.1:6379", **options)
