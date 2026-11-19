import pytest

from suite.helpers import NewStore
from temporalio_streams_memory import MemoryStreamStore


@pytest.fixture
def new_store() -> NewStore:
    # Each Store has its own Streams.
    return MemoryStreamStore
