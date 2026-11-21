"""The boundary between Python and the native module of the Redis Store."""

import asyncio
import os
import subprocess
import time
import uuid
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from conftest import Redis, free_port
from google.protobuf.duration_pb2 import Duration
from temporalio.api.common.v1 import Payload
from temporalio.api.enums.v1 import StreamErrorReason, StreamStatus
from temporalio.api.stream.v1 import (
    ActivityAttempt,
    AppendRecord,
    ClientProducer,
    Producer,
    StreamRecord,
    WorkflowActivityProducer,
)
from temporalio.api.streamservice.v1 import (
    AppendRecordsRequest,
    CloseStreamRequest,
    CloseStreamResponse,
    CreateStreamRequest,
    CreateStreamResponse,
    DescribeStreamRequest,
    DescribeStreamResponse,
    ReadRecordsRequest,
    ReadRecordsResponse,
)
from temporalio.exceptions import StreamError

from temporalio_streams_redis import RedisStreamStore

CLIENT = Producer(client=ClientProducer(namespace="default", id="client:test"))


def new_store(redis: Redis, **options: Any) -> RedisStreamStore:
    # Each Store has its own key prefix, so the tests of one server share no Stream.
    return RedisStreamStore(redis.url, key_prefix=f"test-{uuid.uuid4()}", **options)


def activity(attempt: int) -> Producer:
    """Return the Producer of one attempt of an Activity."""
    produced = ActivityAttempt(attempt=attempt)
    produced.attempt_scheduled_time.seconds = attempt
    activity = WorkflowActivityProducer(
        namespace="default",
        workflow_id="wf",
        run_id="run",
        activity_id="act",
        attempt=produced,
    )
    return Producer(workflow_activity=activity)


def body(data: bytes) -> AppendRecord:
    return AppendRecord(body=Payload(metadata={"encoding": b"binary/plain"}, data=data))


async def test_the_five_calls_cross_the_boundary(redis: Redis):
    store = new_store(redis)
    created = await store.create_stream(
        CreateStreamRequest(stream_id="s", request_id="create-1")
    )
    assert created == CreateStreamResponse(created=True)
    batch = AppendRecordsRequest(
        stream_id="s",
        producer=activity(1),
        request_id="append-1",
        records=[body(b"a"), body(b"b")],
    )
    appended = await store.append_records(batch)
    assert appended.first_offset < appended.last_offset
    # A resend has the answer of the first write, which shows that the request id crossed.
    assert await store.append_records(batch) == appended

    page = await store.read_records(ReadRecordsRequest(stream_id="s", max_records=1))
    first = StreamRecord(
        producer=activity(1), body=body(b"a").body, offset=appended.first_offset
    )
    assert page == ReadRecordsResponse(records=[first], last_offset=first.offset)
    described = await store.describe_stream(DescribeStreamRequest(stream_id="s"))
    assert described == DescribeStreamResponse(
        status=StreamStatus.STREAM_STATUS_OPEN, last_offset=appended.last_offset
    )
    closed = await store.close_stream(
        CloseStreamRequest(stream_id="s", producer=CLIENT)
    )
    assert closed == CloseStreamResponse()
    rest = await store.read_records(
        ReadRecordsRequest(stream_id="s", from_offset=first.offset)
    )
    second = StreamRecord(
        producer=activity(1), body=body(b"b").body, offset=appended.last_offset
    )
    assert rest == ReadRecordsResponse(
        records=[second], last_offset=second.offset, done=True
    )


async def test_a_refusal_keeps_its_reason(redis: Redis):
    reasons = StreamErrorReason
    store = new_store(redis, fence=True, retention=timedelta(milliseconds=1))
    append = AppendRecordsRequest(
        stream_id="s", producer=activity(2), records=[body(b"a")]
    )
    create = CreateStreamRequest(stream_id="s")

    async def refused(call: Any) -> StreamErrorReason.ValueType:
        with pytest.raises(StreamError) as raised:
            await call
        assert raised.value.non_retryable
        return raised.value.reason

    assert (
        await refused(store.append_records(append))
        == reasons.STREAM_ERROR_REASON_NOT_FOUND
    )
    await store.create_stream(create)
    assert (
        await refused(store.create_stream(create))
        == reasons.STREAM_ERROR_REASON_ALREADY_EXISTS
    )
    await store.append_records(append)
    earlier = AppendRecordsRequest(
        stream_id="s", producer=activity(1), records=[body(b"b")]
    )
    assert (
        await refused(store.append_records(earlier))
        == reasons.STREAM_ERROR_REASON_FENCED
    )
    # The limits of a batch are those of the SDK's Stream config, so the Store has none.
    many = AppendRecordsRequest(
        stream_id="s", producer=CLIENT, records=[body(b"c")] * 1001
    )
    await store.append_records(many)
    high = ReadRecordsRequest(stream_id="s", from_offset=b"\xff" * 16)
    assert (
        await refused(store.read_records(high))
        == reasons.STREAM_ERROR_REASON_OFFSET_TOO_HIGH
    )
    short = ReadRecordsRequest(stream_id="s", from_offset=b"12345")
    assert (
        await refused(store.read_records(short))
        == reasons.STREAM_ERROR_REASON_INVALID_ARGUMENT
    )
    await store.close_stream(CloseStreamRequest(stream_id="s", producer=CLIENT))
    # The Retention of 1 ms has passed by the next call or the one after it.
    while True:
        reason = await refused(store.append_records(append))
        if reason == reasons.STREAM_ERROR_REASON_EXPIRED:
            break
        assert reason == reasons.STREAM_ERROR_REASON_CLOSED
    # A Store that doesn't fence takes the earlier attempt, as the default is.
    plain = new_store(redis)
    await plain.create_stream(create)
    await plain.append_records(append)
    await plain.append_records(earlier)


def blocked_reads(redis: Redis) -> int:
    clients = redis.command("CLIENT", "LIST").splitlines()
    return sum(" cmd=xread " in client and " flags=b" in client for client in clients)


async def test_a_cancelled_read_frees_its_connection(redis: Redis):
    store = new_store(redis)
    await store.create_stream(CreateStreamRequest(stream_id="s"))
    waits = ReadRecordsRequest(stream_id="s", wait=Duration(seconds=60))
    before = blocked_reads(redis)
    read = asyncio.create_task(store.read_records(waits))
    while blocked_reads(redis) == before:
        await asyncio.sleep(0.01)
    read.cancel()
    with pytest.raises(asyncio.CancelledError):
        await read
    deadline = time.monotonic() + 20
    while blocked_reads(redis) != before:
        assert time.monotonic() < deadline, "the read still blocks on Redis"
        await asyncio.sleep(0.01)


# The fork is what the test is about, and the child only makes one call and exits.
@pytest.mark.filterwarnings(
    "ignore:This process .* is multi-threaded:DeprecationWarning"
)
async def test_a_store_in_a_forked_child_raises(redis: Redis):
    store = new_store(redis)
    describe = DescribeStreamRequest(stream_id="s")
    # The first call starts the threads of the Store in this process.
    with pytest.raises(StreamError):
        await store.describe_stream(describe)
    pid = os.fork()
    if pid == 0:
        code = 1
        try:
            store.describe_stream(describe).send(None)
        except RuntimeError as err:
            code = 0 if "child of a fork" in str(err) else 2
        finally:
            os._exit(code)
    _, status = os.waitpid(pid, 0)
    assert os.waitstatus_to_exitcode(status) == 0


async def test_a_store_with_no_server_fails_as_an_outage():
    store = RedisStreamStore(f"redis://127.0.0.1:{free_port()}")
    with pytest.raises(StreamError) as raised:
        await store.create_stream(CreateStreamRequest(stream_id="s"))
    assert (
        raised.value.reason == StreamErrorReason.STREAM_ERROR_REASON_STORE_UNAVAILABLE
    )
    assert not raised.value.non_retryable


def openssl(directory: Path, *args: str) -> None:
    subprocess.run(["openssl", *args], cwd=directory, check=True, capture_output=True)


async def test_a_tls_server_is_reached(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    # A CA of the test, and a certificate for 127.0.0.1 that it signs. The verifier refuses a
    # CA certificate as the certificate of a server.
    new_key = ("-newkey", "rsa:2048", "-nodes")
    ca = ("-keyout", "ca.key", "-out", "ca.pem")
    openssl(tmp_path, "req", "-x509", *new_key, "-days", "1", "-subj", "/CN=test", *ca)
    server_key = ("-keyout", "server.key", "-out", "server.csr")
    openssl(tmp_path, "req", *new_key, "-subj", "/CN=127.0.0.1", *server_key)
    extensions = "subjectAltName=IP:127.0.0.1\nbasicConstraints=CA:FALSE\n"
    (tmp_path / "extensions").write_text(extensions)
    signed = ("-CA", "ca.pem", "-CAkey", "ca.key", "-CAcreateserial")
    signed += ("-extfile", "extensions", "-in", "server.csr", "-out", "server.pem")
    openssl(tmp_path, "x509", "-req", "-days", "1", *signed)
    port = free_port()
    files = {"cert": "server.pem", "key": "server.key", "ca-cert": "ca.pem"}
    tls = [arg for name, file in files.items() for arg in (f"--tls-{name}-file", file)]
    tls += ["--tls-auth-clients", "no", "--tls-port", str(port), "--port", "0"]
    server = Redis(*tls, port=port, directory=tmp_path)
    try:
        # The Redis client verifies the server against the system's root certificates, and
        # this variable replaces them.
        monkeypatch.setenv("SSL_CERT_FILE", str(tmp_path / "ca.pem"))
        # A wheel with no TLS provider fails here, at the first TLS connection of the process.
        store = RedisStreamStore(f"rediss://127.0.0.1:{port}")
        created = await store.create_stream(CreateStreamRequest(stream_id="s"))
        assert created == CreateStreamResponse(created=True)
    finally:
        server.stop()


@pytest.mark.parametrize(
    "options, named",
    [
        ({"key_prefix": ""}, "key_prefix"),
        ({"retention": timedelta(0)}, "retention"),
        ({"producer_idle_ttl": timedelta(0)}, "producer_idle_ttl"),
        ({"resend_window": 0}, "resend_window"),
        ({"response_timeout": timedelta(0)}, "response_timeout"),
        ({"wait_replicas": 0}, "wait"),
        ({"wait_replicas": 1, "wait_timeout": timedelta(0)}, "timeout of wait"),
        ({"wait_timeout": timedelta(seconds=1)}, "wait_replicas"),
        ({"max_blocking_connections": 0}, "max_blocking_connections"),
    ],
)
def test_each_option_reaches_the_crate(options: dict[str, Any], named: str):
    # A zero is refused and never read as "use the default", and the error has the option.
    with pytest.raises(ValueError, match=named):
        RedisStreamStore("redis://127.0.0.1:1", **options)


def test_the_urls_fit_the_kind_of_servers():
    with pytest.raises(ValueError, match="cluster=True"):
        RedisStreamStore(["redis://127.0.0.1:1", "redis://127.0.0.1:2"])
    with pytest.raises(ValueError, match="one URL or more"):
        RedisStreamStore([], cluster=True)
