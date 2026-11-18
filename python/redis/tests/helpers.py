import os

import pytest

REDIS_URL = os.environ.get("STREAMS_REDIS_URL")

needs_redis = pytest.mark.skipif(
    not REDIS_URL, reason="STREAMS_REDIS_URL is not set to a Redis server"
)
