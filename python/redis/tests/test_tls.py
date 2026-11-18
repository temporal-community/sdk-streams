import asyncio
import shutil
import socket
import subprocess
import uuid
from collections.abc import AsyncGenerator
from pathlib import Path

import pytest
import pytest_asyncio
from helpers import new_stream, with_store
from temporalio.client import Client

from temporalio_streams_redis import RedisStreamStore

pytestmark = pytest.mark.skipif(
    shutil.which("redis-server") is None or shutil.which("openssl") is None,
    reason="redis-server and openssl are needed to start a Redis with TLS",
)


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _openssl(*args: str) -> None:
    subprocess.run(["openssl", *args], check=True, capture_output=True)


def _make_certificates(directory: Path) -> Path:
    """Write a CA and a server certificate that it signs for 127.0.0.1. Give the CA file."""
    ca, ca_key = directory / "ca.pem", directory / "ca-key.pem"
    key, request = directory / "key.pem", directory / "server.csr"
    cert, extensions = directory / "cert.pem", directory / "extensions.cnf"
    # The verifier refuses a CA certificate as the certificate of the server itself.
    extensions.write_text("subjectAltName=IP:127.0.0.1\nbasicConstraints=CA:FALSE\n")
    _openssl(
        *("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1"),
        *("-subj", "/CN=test-ca", "-keyout", str(ca_key), "-out", str(ca)),
    )
    _openssl(
        *("req", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=127.0.0.1"),
        *("-keyout", str(key), "-out", str(request)),
    )
    _openssl(
        *("x509", "-req", "-days", "1", "-in", str(request)),
        *("-CA", str(ca), "-CAkey", str(ca_key), "-CAcreateserial"),
        *("-extfile", str(extensions), "-out", str(cert)),
    )
    return ca


@pytest_asyncio.fixture  # type: ignore[reportUntypedFunctionDecorator]
async def tls_redis_url(tmp_path: Path) -> AsyncGenerator[str, None]:
    ca = _make_certificates(tmp_path)
    port = _free_port()
    log_path = tmp_path / "redis.log"
    log = log_path.open("w")
    server = subprocess.Popen(
        [
            *("redis-server", "--port", "0", "--tls-port", str(port)),
            *("--tls-cert-file", str(tmp_path / "cert.pem")),
            *("--tls-key-file", str(tmp_path / "key.pem")),
            *("--tls-ca-cert-file", str(ca), "--tls-auth-clients", "no"),
            *("--save", "", "--appendonly", "no", "--dir", str(tmp_path)),
        ],
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    try:
        for _ in range(100):
            assert server.poll() is None, (
                f"redis-server stopped:\n{log_path.read_text()}"
            )
            try:
                socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
                break
            except OSError:
                await asyncio.sleep(0.1)
        else:
            pytest.fail("redis-server did not accept connections within 10 s")
        yield f"rediss://127.0.0.1:{port}"
    finally:
        server.terminate()
        server.wait(timeout=10)
        log.close()


async def test_tls_round_trip(
    client: Client,
    tls_redis_url: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    # The Redis client checks the certificate of the server against the system's root
    # certificates, and `SSL_CERT_FILE` replaces them. A URL with `#insecure` fails, since the
    # crate builds the client without `tls-rustls-insecure`.
    monkeypatch.setenv("SSL_CERT_FILE", str(tmp_path / "ca.pem"))
    store = RedisStreamStore(tls_redis_url, key_prefix=f"test-{uuid.uuid4()}")
    stream = await new_stream(with_store(client, store))

    appended = await stream.append("a", "b")
    records = [record async for record in stream.read()]
    assert [(record.body, record.offset) for record in records] == [
        ("a", appended.first_offset),
        ("b", appended.last_offset),
    ]
    await stream.close()
    page = await stream.read_page(from_offset=appended.last_offset)
    assert page.done
