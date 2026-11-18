"""The Redis Store of Temporal Streams.

A client takes it in ``stream_config``, and its Workers use the same Store::

    from temporalio.client import Client, StreamConfig
    from temporalio_streams_redis import RedisStreamStore

    client = await Client.connect(
        "localhost:7233",
        stream_config=StreamConfig(stores=[RedisStreamStore("redis://localhost:6379")]),
    )

.. warning::
    This package is experimental and subject to change.
"""

from temporalio_streams_redis._store import RedisStreamStore

__all__ = ["RedisStreamStore"]
