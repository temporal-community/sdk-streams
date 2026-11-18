import asyncio
from collections.abc import AsyncGenerator, Iterator

import pytest
import pytest_asyncio
from temporalio.client import Client
from temporalio.testing import WorkflowEnvironment


# pytest-asyncio 0.21 runs each test on the loop of its fixtures. The session fixture of the
# dev server needs one loop for the whole session.
@pytest.fixture(scope="session")
def event_loop() -> Iterator[asyncio.AbstractEventLoop]:
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="session")  # type: ignore[reportUntypedFunctionDecorator]
async def env() -> AsyncGenerator[WorkflowEnvironment, None]:
    env = await WorkflowEnvironment.start_local()
    yield env
    await env.shutdown()


@pytest_asyncio.fixture  # type: ignore[reportUntypedFunctionDecorator]
async def client(env: WorkflowEnvironment) -> Client:
    return env.client
