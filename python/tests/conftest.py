"""The Redis servers that the tests start. A test never uses a server that it didn't start."""

import os
import shutil
import socket
import subprocess
import tempfile
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

# Other software of the host uses these ports.
_FORBIDDEN_PORTS = (6379, 6399, 7299)


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port: int = sock.getsockname()[1]
    assert port not in _FORBIDDEN_PORTS
    return port


class Redis:
    """A ``redis-server`` process on a free port, which ``stop`` ends."""

    def __init__(
        self, *args: str, port: int | None = None, directory: Path | None = None
    ) -> None:
        """Start a server with ``args``, which have its port when ``port`` is given.

        The server runs in ``directory``, so ``args`` can have files of it by their names.
        """
        self.port = port or free_port()
        self._dir = str(directory or tempfile.mkdtemp(prefix="streams-redis-"))
        binary = os.environ.get("REDIS_SERVER", "redis-server")
        self._process = subprocess.Popen(
            [binary, "--bind", "127.0.0.1", "--save", "", "--appendonly", "no"]
            + ["--dir", self._dir, *(args or ("--port", str(self.port)))],
            cwd=self._dir,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        deadline = time.monotonic() + 20
        while True:
            try:
                socket.create_connection(("127.0.0.1", self.port), timeout=1).close()
                return
            except OSError:
                assert self._process.poll() is None, "redis-server didn't start"
                assert time.monotonic() < deadline, "redis-server didn't start"
                time.sleep(0.02)

    @property
    def url(self) -> str:
        return f"redis://127.0.0.1:{self.port}"

    def command(self, *args: str) -> str:
        """Send one command and return its reply as text, for a reply of one string."""
        with socket.create_connection(("127.0.0.1", self.port), timeout=5) as sock:
            sock.sendall((" ".join(args) + "\r\n").encode())
            reply = b""
            while not reply.endswith(b"\r\n") or reply.count(b"\r\n") < 2:
                chunk = sock.recv(65536)
                if not chunk or reply.startswith((b"+", b"-", b":")):
                    reply += chunk
                    break
                reply += chunk
            return reply.decode()

    def stop(self) -> None:
        self._process.kill()
        self._process.wait()
        shutil.rmtree(self._dir, ignore_errors=True)


@pytest.fixture(scope="session")
def redis() -> Iterator[Redis]:
    server = Redis()
    yield server
    server.stop()
