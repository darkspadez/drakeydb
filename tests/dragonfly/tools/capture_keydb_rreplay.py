#!/usr/bin/env python3
"""Captures, byte for byte, what an active KeyDB v6.3.4 streams to a classic replica.

The golden vectors for ParseRreplayEnvelope and the classic-link applier come from here. The script
starts the KeyDB servers itself (the binary is $KEYDB_SERVER_PATH, else `keydb-server` on PATH) and
attaches a hand-rolled replica that speaks what drakeydb's classic handshake speaks (PING, REPLCONF
listening-port, REPLCONF capa eof capa psync2, REPLCONF UUID, PSYNC ? -1) and ACKs like a replica.
It consumes the full-sync payload, performs a fixed script of writes and keeps every byte the master
sends afterwards.

Two captures are written to --out-dir (tests/dragonfly/data by default):

  keydb_v6.3.4_rreplay_stream.bin         one active KeyDB: its own writes, each in an RREPLAY
                                          envelope
  keydb_v6.3.4_rreplay_nested_stream.bin  KeyDB A replicates from KeyDB B (multi-master, forwarding
                                          on) and A's replica sees B's writes forwarded inside A's
                                          own envelope, plus one write made on A itself

The segment table printed at the end (offset, length, step, decoded envelope, C-escaped bytes) is
what tests/dragonfly/data/README.md documents. KeyDB generates its node uuid and MVCC clock on every
run, so a re-capture changes those bytes; the structure stays.

Run it from the repository root, with tests/dragonfly/requirements.txt installed:

  KEYDB_SERVER_PATH=/path/to/keydb-server python3 tests/dragonfly/tools/capture_keydb_rreplay.py
"""

import argparse
import os
import socket
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

import redis

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from tests.dragonfly.instance import KeyDBServer  # noqa: E402

KEYDB_VERSION = "6.3.4"
SOLO_NAME = "keydb_v6.3.4_rreplay_stream.bin"
NESTED_NAME = "keydb_v6.3.4_rreplay_nested_stream.bin"

# The port a real replica announces with REPLCONF listening-port; KeyDB only lists it in INFO.
ANNOUNCED_PORT = 6399


def free_port():
    with socket.socket() as s:
        s.bind(("localhost", 0))
        return s.getsockname()[1]


def encode_command(*args):
    parts = [b"*%d\r\n" % len(args)]
    for arg in args:
        arg = arg if isinstance(arg, bytes) else str(arg).encode()
        parts.append(b"$%d\r\n%s\r\n" % (len(arg), arg))
    return b"".join(parts)


def parse_frames(data):
    """Splits `data` into (offset, end, args) RESP arrays of bulk strings, else raises."""
    frames = []
    pos = 0
    while pos < len(data):
        start = pos
        if data[pos : pos + 1] != b"*":
            raise ValueError(f"expected an array at offset {pos}, found {data[pos:pos + 16]!r}")
        eol = data.index(b"\r\n", pos)
        argc = int(data[pos + 1 : eol])
        pos = eol + 2
        args = []
        for _ in range(argc):
            if data[pos : pos + 1] != b"$":
                raise ValueError(f"expected a bulk string at offset {pos}")
            eol = data.index(b"\r\n", pos)
            size = int(data[pos + 1 : eol])
            pos = eol + 2
            args.append(data[pos : pos + size])
            if data[pos + size : pos + size + 2] != b"\r\n":
                raise ValueError(f"bulk string at offset {pos} is not CRLF terminated")
            pos += size + 2
        frames.append((start, pos, args))
    return frames


def c_escape(data):
    """`data` as the body of a C string literal (octal escapes, so a following digit is safe)."""
    out = []
    for b in data:
        ch = chr(b)
        if ch == "\r":
            out.append("\\r")
        elif ch == "\n":
            out.append("\\n")
        elif ch in '"\\':
            out.append("\\" + ch)
        elif 32 <= b < 127:
            out.append(ch)
        else:
            out.append("\\%03o" % b)
    return "".join(out)


def describe(args):
    """One line for a frame; an RREPLAY envelope shows its uuid, db, mvcc and the decoded inner."""
    name = args[0].decode(errors="replace").upper()
    if name != "RREPLAY" or len(args) < 3:
        return " ".join(a.decode(errors="replace") for a in args)
    inner_text = " ; ".join(describe(a) for _, _, a in parse_frames(args[2]))
    db = args[3].decode() if len(args) > 3 else "-"
    mvcc = args[4].decode() if len(args) > 4 else "-"
    return f"RREPLAY uuid={args[1].decode()} db={db} mvcc={mvcc} inner=[{inner_text}]"


class RawReplica:
    """A hand-rolled classic replica that keeps every byte it is sent.

    Nothing is parsed beyond what the handshake and the full-sync framing need, so what ends up in
    `stream` is exactly what the master wrote after the RDB payload.
    """

    def __init__(self, port):
        self.sock = socket.create_connection(("localhost", port), timeout=10)
        self.buf = bytearray()
        self.stream = bytearray()
        self.handshake_replies = []
        self.rdb_size = None
        self.rdb_framing = None
        self.master_uuid = None
        self.base_offset = None
        self.last_ack = 0.0

    def close(self):
        self.sock.close()

    def send(self, *args):
        self.sock.sendall(encode_command(*args))

    def _recv(self):
        data = self.sock.recv(65536)
        if not data:
            raise ConnectionError("the master closed the connection")
        self.buf += data

    def _read_until(self, delim):
        while True:
            idx = self.buf.find(delim)
            if idx >= 0:
                end = idx + len(delim)
                chunk = bytes(self.buf[:end])
                del self.buf[:end]
                return chunk
            self._recv()

    def _read_exact(self, size):
        while len(self.buf) < size:
            self._recv()
        chunk = bytes(self.buf[:size])
        del self.buf[:size]
        return chunk

    def command(self, *args):
        """Sends a handshake command and returns the one reply line, without its CRLF."""
        self.send(*args)
        reply = self._read_until(b"\r\n")[:-2].decode()
        self.handshake_replies.append((" ".join(str(a) for a in args), reply))
        return reply

    def full_sync_start(self):
        """Runs the handshake and PSYNC ? -1, which must be answered with +FULLRESYNC."""
        self.command("PING")
        self.command("REPLCONF", "listening-port", ANNOUNCED_PORT)
        self.command("REPLCONF", "capa", "eof", "capa", "psync2")
        # The master answers with its own node uuid, the one its RREPLAY envelopes carry.
        self.master_uuid = self.command("REPLCONF", "UUID", uuid.uuid4())[1:]
        reply = self.command("PSYNC", "?", "-1")
        if not reply.startswith("+FULLRESYNC "):
            raise RuntimeError(f"expected +FULLRESYNC, got {reply!r}")
        self.base_offset = int(reply.split()[2])

    def read_rdb(self):
        """Consumes the full-sync payload (either framing); returns its size in bytes."""
        while True:
            line = self._read_until(b"\n").rstrip(b"\r\n")
            # A bare newline is the master's keepalive while its child process still saves.
            if line:
                break
        if line.startswith(b"$EOF:"):
            mark = line[5:]
            self.rdb_framing = f"$EOF:{mark.decode()}"
            payload = self._read_until(mark)[: -len(mark)]
            self.rdb_size = len(payload)
        elif line.startswith(b"$"):
            self.rdb_framing = line.decode()
            self.rdb_size = len(self._read_exact(int(line[1:])))
        else:
            raise RuntimeError(f"expected an RDB header, got {line!r}")
        return self.rdb_size

    def _ack_if_due(self):
        """Sends REPLCONF ACK like a replica does every second.

        A master that streamed its RDB over a socket only starts streaming writes after the first
        ACK it receives once its child process is reaped, and an ACK sent right after the last RDB
        byte can still beat that, so one ACK is not enough.
        """
        if time.monotonic() - self.last_ack >= 0.25:
            self.send("REPLCONF", "ACK", self.base_offset + len(self.stream))
            self.last_ack = time.monotonic()

    def drain(self, quiet=0.4, deadline=10.0):
        """Appends to `stream` what the master sends, until it was silent for `quiet` seconds."""
        self.stream += self.buf
        self.buf.clear()
        self._ack_if_due()
        end = time.monotonic() + deadline
        self.sock.settimeout(quiet)
        try:
            while time.monotonic() < end:
                try:
                    data = self.sock.recv(65536)
                except socket.timeout:
                    return
                if not data:
                    raise ConnectionError("the master closed the connection")
                self.stream += data
        finally:
            self.sock.settimeout(10)

    def drain_until(self, predicate, deadline=15.0):
        """Keeps draining until `predicate(frames_of_stream)` holds."""
        end = time.monotonic() + deadline
        while time.monotonic() < end:
            self.drain(quiet=0.3, deadline=2.0)
            try:
                if predicate(parse_frames(bytes(self.stream))):
                    return
            except ValueError:
                pass  # the stream ends inside a frame, the rest is still on its way
        raise TimeoutError("the awaited frame never arrived")


@dataclass
class Step:
    label: str
    start: int
    end: int


def run_steps(replica, steps):
    """Runs (label, action) pairs; each step owns the stream bytes that arrive after its action."""
    marks = []
    for label, action in steps:
        start = len(replica.stream)
        action()
        replica.drain()
        marks.append(Step(label, start, len(replica.stream)))
    return marks


def wait_for(predicate, what, timeout=20.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if predicate():
            return
        time.sleep(0.05)
    raise TimeoutError(f"timed out waiting for {what}")


def new_keydb(servers, log_dir, **kwargs):
    """Starts a KeyDB and registers it in `servers`, whose members the caller stops."""
    server = KeyDBServer(free_port(), log_dir=log_dir, **kwargs)
    servers.append(server)
    server.start()
    info = redis.Redis(port=server.port, decode_responses=True).info("server")
    version = info.get("redis_version")
    if version != KEYDB_VERSION:
        raise SystemExit(f"this capture is for KeyDB {KEYDB_VERSION}, the binary is {version}")
    return server


def client(server, db=0):
    return redis.Redis(port=server.port, db=db, decode_responses=True)


def attach_in_sync_window(server, replica, save_delay_us, live_write):
    """Starts a full sync that is slowed by a per-key delay and writes while it runs.

    The write lands after the master forked its RDB child and before the transfer ends, so it is
    not part of the RDB: KeyDB queues it on the replica's output buffer and delivers it right after
    the RDB, once the replica's first ACK arrived. Returns with that write in the replica's stream.
    """
    k = client(server)
    k.config_set("rdb-key-save-delay", save_delay_us)
    replica.full_sync_start()
    wait_for(lambda: k.info("persistence")["rdb_bgsave_in_progress"] == 1, "the RDB child to start")
    live_write()
    state = k.info("replication")["slave0"]["state"]
    if state == "online":
        raise RuntimeError("the RDB transfer ended before the live write; raise --save-delay-us")
    replica.read_rdb()
    k.config_set("rdb-key-save-delay", 0)
    replica.drain_until(lambda frames: bool(frames))


def capture_solo(servers, log_dir, args):
    """One active KeyDB; returns the replica, holding the whole stream, and its steps."""
    server = new_keydb(
        servers,
        log_dir,
        active_replica=True,
        multi_master=True,
        no_forward=False,
        repl_ping_replica_period=3600,
    )
    k, k3 = client(server), client(server, db=3)
    k.execute_command("DEBUG", "POPULATE", args.keys, "seed", 32)
    k.sadd("seed:set", "m1", "m2", "m3")
    k.hset("seed:hash", mapping={"f1": "1"})

    replica = RawReplica(server.port)
    attach_in_sync_window(
        server, replica, args.save_delay_us, lambda: k.set("live:during_sync", "streamed")
    )
    replica.drain()
    steps = [Step("write during the full sync", 0, len(replica.stream))]

    def multi_exec():
        pipe = k.pipeline(transaction=True)
        pipe.set("a", 1)
        pipe.incr("c")
        pipe.execute()

    expire_at_ms = int(time.time() * 1000) + 3_600_000

    def expired_key():
        k.set("expiring", "v", px=200)
        time.sleep(1.5)
        assert not k.exists("expiring"), "the key should have expired by now"

    def expired_member():
        k.execute_command("EXPIREMEMBER", "seed:set", "m3", 1)
        time.sleep(2.5)
        assert not k.sismember("seed:set", "m3"), "the member should have expired by now"

    def cron_ping():
        k.config_set("repl-ping-replica-period", 1)
        replica.drain_until(lambda fs: any("ping" in describe(f[2]).lower() for f in fs))
        k.config_set("repl-ping-replica-period", 3600)

    steps += run_steps(
        replica,
        [
            ("SET k v", lambda: k.set("k", "v")),
            ("SET k2 v2 EX 100", lambda: k.set("k2", "v2", ex=100)),
            ("SELECT 3 + SET k3 v3", lambda: k3.set("k3", "v3")),
            ("MULTI / SET a 1 / INCR c / EXEC", multi_exec),
            ("DEL k", lambda: k.delete("k")),
            (
                "EXPIREMEMBER seed:set m1 100",
                lambda: k.execute_command("EXPIREMEMBER", "seed:set", "m1", 100),
            ),
            (
                "PEXPIREMEMBERAT seed:set m2 <ms>",
                lambda: k.execute_command("PEXPIREMEMBERAT", "seed:set", "m2", expire_at_ms),
            ),
            ("EXPIREMEMBER seed:set m3 1, then it expires", expired_member),
            ("SET expiring v PX 200, then it expires", expired_key),
            ("cron PING", cron_ping),
        ],
    )
    return replica, steps


def capture_nested(servers, log_dir, args):
    """KeyDB A replicates from KeyDB B, forwarding on; the replica is attached to A."""
    flags = dict(active_replica=True, multi_master=True, no_forward=False)
    node_b = new_keydb(servers, log_dir, **flags)
    node_a = new_keydb(servers, log_dir, **flags, repl_ping_replica_period=3600)
    b, b3, a = client(node_b), client(node_b, db=3), client(node_a)
    b.execute_command("DEBUG", "POPULATE", args.keys, "seed", 32)
    a.execute_command("REPLICAOF", "localhost", node_b.port)
    wait_for(lambda: a.dbsize() == args.keys, "A to load B's keyspace")
    wait_for(lambda: a.info("replication").get("master_link_status") == "up", "the A <- B link")

    replica = RawReplica(node_a.port)
    attach_in_sync_window(
        node_a, replica, args.save_delay_us, lambda: b.set("live:during_sync", "x")
    )
    replica.drain()
    steps = [Step("write on B during A's full sync to the replica", 0, len(replica.stream))]

    def multi_exec():
        pipe = b.pipeline(transaction=True)
        pipe.set("a", 1)
        pipe.incr("c")
        pipe.execute()

    steps += run_steps(
        replica,
        [
            ("on B: SET k v", lambda: b.set("k", "v")),
            ("on B: SELECT 3 + SET k3 v3", lambda: b3.set("k3", "v3")),
            ("on B: MULTI / SET a 1 / INCR c / EXEC", multi_exec),
            ("on B: DEL k", lambda: b.delete("k")),
            ("on A itself: SET local v", lambda: a.set("local", "v")),
        ],
    )
    return replica, steps


def print_segments(title, replica, steps):
    data = bytes(replica.stream)
    print(f"\n=== {title}: {len(data)} bytes after the RDB ===")
    print(f"handshake: {replica.handshake_replies}")
    print(f"master uuid: {replica.master_uuid}")
    print(f"full sync framing: {replica.rdb_framing}, payload {replica.rdb_size} bytes")
    for i, (start, end, frame_args) in enumerate(parse_frames(data)):
        step = next(s for s in steps if s.start <= start < s.end)
        print(f"[{i}] step={step.label!r} offset={start} length={end - start}")
        print(f"    {describe(frame_args)}")
        print(f'    "{c_escape(data[start:end])}"')


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--keydb-server", help="keydb-server binary (default $KEYDB_SERVER_PATH)")
    parser.add_argument("--out-dir", type=Path, default=REPO_ROOT / "tests/dragonfly/data")
    parser.add_argument("--log-dir", help="where KeyDB logs go (default: none)")
    parser.add_argument("--keys", type=int, default=300, help="keys DEBUG POPULATE seeds")
    parser.add_argument(
        "--save-delay-us",
        type=int,
        default=5000,
        help="rdb-key-save-delay: keeps the full sync open long enough for a live write",
    )
    parser.add_argument("--skip-nested", action="store_true")
    args = parser.parse_args()

    if args.keydb_server:
        os.environ["KEYDB_SERVER_PATH"] = args.keydb_server
    if KeyDBServer.find_binary() is None:
        raise SystemExit("keydb-server not found: pass --keydb-server or set KEYDB_SERVER_PATH")
    args.out_dir.mkdir(parents=True, exist_ok=True)

    servers = []
    try:
        replica, steps = capture_solo(servers, args.log_dir, args)
        (args.out_dir / SOLO_NAME).write_bytes(bytes(replica.stream))
        print_segments("active KeyDB, its own writes", replica, steps)
        replica.close()

        if not args.skip_nested:
            replica, steps = capture_nested(servers, args.log_dir, args)
            (args.out_dir / NESTED_NAME).write_bytes(bytes(replica.stream))
            print_segments("KeyDB A replicating from KeyDB B", replica, steps)
            replica.close()
    finally:
        for server in servers:
            server.stop()


if __name__ == "__main__":
    main()
