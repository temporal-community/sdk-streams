import uuid
from typing import Any

import pytest
from helpers import REDIS_URL
from temporalio.client import StreamStore

from suite.helpers import NewStore
from temporalio_streams_redis import RedisStreamStore


@pytest.fixture
def new_store(request: pytest.FixtureRequest) -> NewStore:
    url = REDIS_URL
    if not url:
        # A test with no `client` builds a Store and sends it no request that reaches Redis.
        if "client" in request.fixturenames:
            pytest.skip("STREAMS_REDIS_URL is not set to a Redis server")
        url = "redis://127.0.0.1:6379"

    def build(**options: Any) -> StreamStore:
        # Each Store has its own key prefix, so that runs on one Redis don't share Streams.
        return RedisStreamStore(url, key_prefix=f"test-{uuid.uuid4()}", **options)

    return build
