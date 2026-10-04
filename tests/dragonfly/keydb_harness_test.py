"""Tests of the KeyDB and Redis server harness (instance.py, conftest.py).

None of them needs a real KeyDB: they run against stub scripts, so they run everywhere.
"""

import stat
import sys

import pytest

from .instance import KeyDBServer, RedisServer


def write_script(path, body):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body)
    path.chmod(path.stat().st_mode | stat.S_IEXEC)
    return str(path)


@pytest.fixture
def clean_env(monkeypatch, tmp_path):
    """No KeyDB or Redis configured, and an empty PATH: nothing can be found by name."""
    for name in ("KEYDB_SERVER_PATH", "KEYDB_REQUIRED", "REDIS_SERVER_PATH"):
        monkeypatch.delenv(name, raising=False)
    (tmp_path / "bin").mkdir()
    monkeypatch.setenv("PATH", str(tmp_path / "bin"))
    return tmp_path / "bin"


def test_missing_keydb_skips_unless_required(clean_env):
    reason, fatal = KeyDBServer.unavailable()
    assert "keydb-server not found" in reason
    assert not fatal


@pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "Yes", " 1 "])
def test_missing_keydb_fails_when_required(clean_env, monkeypatch, value):
    monkeypatch.setenv("KEYDB_REQUIRED", value)
    reason, fatal = KeyDBServer.unavailable()
    assert fatal, reason


@pytest.mark.parametrize("value", ["", "0", "false", "no"])
def test_missing_keydb_skips_when_not_required(clean_env, monkeypatch, value):
    monkeypatch.setenv("KEYDB_REQUIRED", value)
    reason, fatal = KeyDBServer.unavailable()
    assert not fatal, reason


def test_bad_keydb_server_path_fails_even_when_not_required(clean_env, tmp_path, monkeypatch):
    # A working keydb-server is on PATH, which a mistyped KEYDB_SERVER_PATH must not fall back to.
    write_script(clean_env / "keydb-server", "#!/bin/sh\nexit 0\n")
    assert KeyDBServer.unavailable() is None

    monkeypatch.setenv("KEYDB_SERVER_PATH", str(tmp_path / "typo" / "keydb-server"))
    assert KeyDBServer.find_binary() is None
    reason, fatal = KeyDBServer.unavailable()
    assert fatal and "typo" in reason, (reason, fatal)

    not_executable = tmp_path / "not-executable"
    not_executable.write_text("#!/bin/sh\n")
    monkeypatch.setenv("KEYDB_SERVER_PATH", str(not_executable))
    reason, fatal = KeyDBServer.unavailable()
    assert fatal and "not-executable" in reason, (reason, fatal)


def test_good_keydb_server_path_is_available(clean_env, tmp_path, monkeypatch):
    monkeypatch.setenv("KEYDB_SERVER_PATH", write_script(tmp_path / "mine", "#!/bin/sh\nexit 0\n"))
    assert KeyDBServer.unavailable() is None


# What the stub below needs of a KeyDB: it answers INFO with a `process_id` and everything else
# with +OK, like the handshake of redis-py and the readiness poll of KeyDBServer expect.
FAKE_KEYDB = f"""#!{sys.executable}
import os
import socketserver
import sys

args = sys.argv[1:]
port = int(args[args.index("--port") + 1])
pid = int(os.environ.get("FAKE_KEYDB_PID") or os.getpid())


class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        while chunk := self.request.recv(4096):
            if b"\\r\\nINFO\\r\\n" in chunk.upper():
                body = f"# Server\\r\\nprocess_id:{{pid}}\\r\\n".encode()
                self.request.sendall(b"$%d\\r\\n%s\\r\\n" % (len(body), body))
            else:
                self.request.sendall(b"+OK\\r\\n")


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


Server(("localhost", port), Handler).serve_forever()
"""


def test_keydb_ready_when_its_own_process_answers(clean_env, tmp_path, monkeypatch, port_picker):
    monkeypatch.setenv("KEYDB_SERVER_PATH", write_script(tmp_path / "keydb-server", FAKE_KEYDB))
    server = KeyDBServer(port_picker.get_available_port())
    server.start(timeout=10)
    try:
        assert server._served_by() == server.proc.pid
    finally:
        server.stop()


def test_keydb_not_ready_when_another_process_answers(
    clean_env, tmp_path, monkeypatch, port_picker
):
    """A stale server on a reused port answers PING and INFO as well as ours would."""
    monkeypatch.setenv("KEYDB_SERVER_PATH", write_script(tmp_path / "keydb-server", FAKE_KEYDB))
    monkeypatch.setenv("FAKE_KEYDB_PID", "1")
    server = KeyDBServer(port_picker.get_available_port())
    try:
        with pytest.raises(RuntimeError, match="served by process 1, not by the keydb-server"):
            server.start(timeout=10)
        assert server.proc is None, "start() must stop the process it started"
    finally:
        server.stop()  # only does something if the checks above failed


@pytest.mark.parametrize(
    "version, redis7, falls_back",
    [
        ("6.2.14", None, True),
        ("7.0.15", None, True),
        ("7.0.15", True, True),
        ("8.0.1", True, True),
        ("6.2.14", True, False),
        ("unparsable", True, False),
    ],
)
def test_redis_server_fallback_must_satisfy_redis7(clean_env, version, redis7, falls_back):
    fallback = write_script(
        clean_env / "redis-server",
        f'#!/bin/sh\n[ "$1" = "--version" ] && echo "Redis server v={version} sha=0:0"\nexit 0\n',
    )
    server = RedisServer(port=0)
    if falls_back:
        server.start(redis7=redis7)
        server.proc.wait(timeout=10)
        assert server.server_bin == fallback
    else:
        # Nothing runnable is left, which the redis_server fixture turns into a skip outside CI.
        with pytest.raises(FileNotFoundError):
            server.start(redis7=redis7)
