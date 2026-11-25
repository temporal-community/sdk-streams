"""The Redis Store and the memory Store of the SDK answer the same generated calls alike.

``STREAMS_SEED`` runs one seed.
"""

import os
import random
import uuid
from dataclasses import dataclass, field
from typing import Any

import pytest
from conftest import Redis
from google.protobuf.message import Message
from temporalio.api.common.v1 import Payload
from temporalio.api.stream.v1 import (
    ActivityAttempt,
    ClientProducer,
    LocalActivityAttempt,
    LocalActivityProducer,
    Producer,
    StandaloneActivityProducer,
    WorkflowActivityProducer,
    WorkflowProducer,
    WorkflowTaskAttempt,
)
from temporalio.api.streamservice.v1 import (
    AppendRecordsRequest,
    AppendRecordsResponse,
    CreateStreamRequest,
    DescribeStreamRequest,
    DescribeStreamResponse,
    ReadRecordsRequest,
    ReadRecordsResponse,
)
from temporalio.client import StreamStore
from temporalio.exceptions import StreamError
from temporalio.testing import MemoryStreamStore

from temporalio_streams_redis import RedisStreamStore

# The batches that a Store remembers for a Producer. It's small, so a seed passes its edge.
WINDOW = 3
STEPS = 150
STREAMS = ("a", "b")


@dataclass
class Side:
    """One of the two Stores, and the offsets of each Stream that it gave, in their order."""

    store: StreamStore
    offsets: dict[str, list[bytes]] = field(default_factory=dict)

    def rank(self, stream_id: str, offset: bytes) -> int:
        """Return the place of an offset among those of its Stream, 0 for none.

        The form of an offset is each Store's own, so two offsets compare by their places.
        """
        return self.offsets.get(stream_id, []).index(offset) + 1 if offset else 0

    def offset(self, stream_id: str, rank: int) -> bytes:
        """Return the offset at a place, or one above the newest for the place after it."""
        offsets = self.offsets.get(stream_id, [])
        if rank <= len(offsets):
            return offsets[rank - 1] if rank else b""
        newest = offsets[-1]
        return (int.from_bytes(newest) + 1).to_bytes(len(newest))

    async def answer(self, call: Message, request_id: str, from_rank: int) -> Any:
        """Send a call, and return its answer in a form that both Stores share."""
        try:
            if isinstance(call, CreateStreamRequest):
                return await self.store.create_stream(call, request_id=request_id)
            if isinstance(call, DescribeStreamRequest):
                described = await self.store.describe_stream(call)
                return self._described(call.stream_id, described)
            if isinstance(call, AppendRecordsRequest):
                appended = await self.store.append_records(call, request_id=request_id)
                await self._learn(call.stream_id)
                return self._appended(call.stream_id, appended)
            assert isinstance(call, ReadRecordsRequest)
            read = ReadRecordsRequest()
            read.CopyFrom(call)
            read.from_offset = self.offset(call.stream_id, from_rank)
            return self._page(call.stream_id, await self.store.read_records(read))
        except StreamError as err:
            return err.reason

    async def _learn(self, stream_id: str) -> None:
        read = ReadRecordsRequest(stream_id=stream_id)
        page = await self.store.read_records(read)
        self.offsets[stream_id] = [record.offset for record in page.records]

    def _appended(self, stream_id: str, appended: AppendRecordsResponse) -> Any:
        first = self.rank(stream_id, appended.first_offset)
        return ("appended", first, self.rank(stream_id, appended.last_offset))

    def _described(self, stream_id: str, described: DescribeStreamResponse) -> Any:
        return ("described", self.rank(stream_id, described.last_offset))

    def _page(self, stream_id: str, page: ReadRecordsResponse) -> Any:
        records = [
            (record.producer, record.body, self.rank(stream_id, record.offset))
            for record in page.records
        ]
        return ("page", records, self.rank(stream_id, page.last_offset))


def producer(rng: random.Random) -> Producer:
    """Return the Producer of one of the five kinds, with a random attempt.

    The test has one Producer of each kind, so each one sends more batches than a Store
    remembers. The two kinds of Activity that a Workflow runs have equal ids, and so have a
    Workflow and a Standalone Activity, so only the kind tells their Producers apart.
    """
    number = lambda: rng.choice([1, 255, 256, 2**31 - 1])  # noqa: E731
    task = lambda: WorkflowTaskAttempt(  # noqa: E731
        started_event_id=number(), attempt=rng.choice([1, 2])
    )

    def activity() -> ActivityAttempt:
        attempt = ActivityAttempt(attempt=rng.choice([1, 2]))
        attempt.attempt_scheduled_time.seconds = number()
        return attempt

    kind = rng.randrange(5)
    if kind == 0:
        workflow = WorkflowProducer(
            namespace="ns", workflow_id="w", run_id="r", attempt=task()
        )
        return Producer(workflow=workflow)
    if kind == 1:
        local = LocalActivityProducer(
            namespace="ns",
            workflow_id="w",
            run_id="r",
            activity_id="a",
            attempt=LocalActivityAttempt(task=task(), attempt=rng.choice([1, 256])),
        )
        return Producer(local_activity=local)
    if kind == 2:
        scheduled = WorkflowActivityProducer(
            namespace="ns",
            workflow_id="w",
            run_id="r",
            activity_id="a",
            attempt=activity(),
        )
        return Producer(workflow_activity=scheduled)
    if kind == 3:
        standalone = StandaloneActivityProducer(
            namespace="ns", activity_id="w", run_id="r", attempt=activity()
        )
        return Producer(standalone_activity=standalone)
    return Producer(client=ClientProducer(namespace="ns", id="r"))


@dataclass
class Calls:
    """The generator of the calls of one seed."""

    rng: random.Random
    # The appends that each kind of Producer sent to a Stream with their request ids, the newest
    # last, for the resends.
    sent: dict[tuple[str, str | None], list[tuple[AppendRecordsRequest, str]]] = field(
        default_factory=dict
    )
    created: set[str] = field(default_factory=set)

    def next(self, records: dict[str, int]) -> tuple[Message, str, int]:
        """Return a call, the request id of a create or an append, and for a read the
        place of the offset that it reads from.
        """
        rng = self.rng
        stream_id = rng.choice(STREAMS)
        roll = rng.randrange(100)
        # A Stream that isn't created refuses every other call, so its create comes early.
        if roll < 6 or (stream_id not in self.created and roll < 50):
            self.created.add(stream_id)
            request_id = rng.choice(["", "c1", "c2"])
            policy: Any = rng.randrange(3)
            create = CreateStreamRequest(stream_id=stream_id, id_conflict_policy=policy)
            return create, request_id, 0
        if roll < 60:
            batch, request_id = self._append(stream_id)
            return batch, request_id, 0
        if roll < 68:
            return DescribeStreamRequest(stream_id=stream_id), "", 0
        known = records.get(stream_id, 0)
        # A place of a Record, the start, or the place after the newest Record.
        from_rank = rng.randrange(known + 2) if known else 0
        read = ReadRecordsRequest(stream_id=stream_id, max_records=rng.randrange(6))
        return read, "", from_rank

    def _append(self, stream_id: str) -> tuple[AppendRecordsRequest, str]:
        rng = self.rng
        batch = AppendRecordsRequest(stream_id=stream_id, producer=producer(rng))
        request_id = ""
        kind = batch.producer.WhichOneof("kind")
        earlier = self.sent.setdefault((stream_id, kind), [])
        if earlier and rng.random() < 0.4:
            # The request id of a batch around the place where a Store forgets one. With the
            # earlier attempt it's a resend, and with the new attempt it's none.
            again, request_id = rng.choice(earlier[-(WINDOW + 2) :])
            if rng.random() < 0.85:
                batch.producer.CopyFrom(again.producer)
        elif rng.random() < 0.7:
            request_id = f"r{sum(map(len, self.sent.values()))}"
        # A resend has other Records than its first send, since a Store compares none.
        for _ in range(1 + rng.randrange(3)):
            batch.records.add(body=Payload(data=rng.randbytes(rng.randrange(20))))
        earlier.append((batch, request_id))
        return batch, request_id


@pytest.mark.parametrize("fence", [False, True])
async def test_the_redis_store_answers_as_the_memory_store(redis: Redis, fence: bool):
    seed = os.environ.get("STREAMS_SEED")
    for seed in [int(seed)] if seed else range(1, 21):
        calls = Calls(random.Random(seed))
        prefix = f"compare-{uuid.uuid4()}"
        redis_side = Side(
            RedisStreamStore(
                redis.url, key_prefix=prefix, fence=fence, resend_window=WINDOW
            )
        )
        memory_side = Side(MemoryStreamStore(fence=fence, resend_window=WINDOW))
        for step in range(STEPS):
            records = {
                stream: len(found) for stream, found in redis_side.offsets.items()
            }
            call, request_id, from_rank = calls.next(records)
            on_redis = await redis_side.answer(call, request_id, from_rank)
            in_memory = await memory_side.answer(call, request_id, from_rank)
            note = (
                f"seed {seed}, fence {fence}, step {step}, request id {request_id!r}, "
                f"from place {from_rank}"
            )
            assert on_redis == in_memory, f"{note}\n{type(call).__name__}\n{call}"
