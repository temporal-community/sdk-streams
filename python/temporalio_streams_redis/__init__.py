"""The Redis Store of Temporal Streams.

A client takes the Store in its Stream config, and the Workers of that client use the
same Store::

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
