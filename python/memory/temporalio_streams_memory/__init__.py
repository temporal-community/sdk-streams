"""The memory Store of Temporal Streams, for tests.

A client takes it in ``stream_config``, and its Workers use the same Store::

    from temporalio.client import Client, StreamConfig
    from temporalio_streams_memory import MemoryStreamStore

    client = await Client.connect(
        "localhost:7233",
        stream_config=StreamConfig(stores=[MemoryStreamStore()]),
    )

.. warning::
    This package is experimental and subject to change.
"""

from temporalio_streams_memory._store import MemoryStreamStore

__all__ = ["MemoryStreamStore"]
