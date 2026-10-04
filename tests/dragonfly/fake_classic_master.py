"""A scripted classic (Redis-protocol) replication master, for what a Proxy in front of a real
server cannot express: byte-exact, deterministic PSYNC replies and streams, malformed ones included.

It speaks just enough of the replica handshake (PING, REPLCONF ..., PSYNC) to bring a drakeydb
replica to the PSYNC reply, answers that with a scripted byte string and then, optionally, sends
scripted stream bytes. Every request and every connection is recorded.
"""

import asyncio
from pathlib import Path

EMPTY_RDB = (Path(__file__).resolve().parents[2] / "src/server/testdata/empty.rdb").read_bytes()

# The smallest RDB the loader accepts: the magic, the EOF opcode and a zero checksum (not verified).
# Short enough that the header, the RDB and a few commands arrive in the replica's very first read.
MINIMAL_RDB = b"REDIS0009\xff" + b"\x00" * 8

REPL_ID = b"0123456789abcdef0123456789abcdef01234567"  # 40 chars, like a real replication id
EOF_TOKEN = b"0123456789012345678901234567890123456789"  # 40 chars: the diskless-sync EOF mark


def full_resync_header(repl_id=REPL_ID, offset=0):
    return b"+FULLRESYNC " + repl_id + b" " + str(offset).encode() + b"\r\n"


def resp_command(*words):
    """A command as the RESP array a master streams to its replicas."""
    out = b"*" + str(len(words)).encode() + b"\r\n"
    for word in words:
        word = word if isinstance(word, bytes) else str(word).encode()
        out += b"$" + str(len(word)).encode() + b"\r\n" + word + b"\r\n"
    return out


def diskless_full_sync(rdb=EMPTY_RDB, token=EOF_TOKEN, tail=None, offset=0, stream=b""):
    """A complete diskless full sync: header, `$EOF:<token>`, the RDB and the closing token, then
    `stream` (replication stream bytes written right behind it).

    `tail` replaces the closing token (default: a copy of `token`); pass b"" to omit it."""
    closing = token if tail is None else tail
    return full_resync_header(offset=offset) + b"$EOF:" + token + b"\r\n" + rdb + closing + stream


def disk_full_sync(rdb=EMPTY_RDB, declared_size=None, offset=0, stream=b""):
    """A disk-style full sync: header, `$<size>`, the RDB, then `stream` (what a master flushes
    right behind it). `declared_size` defaults to the RDB's real length; pass another number to
    make the two disagree."""
    size = len(rdb) if declared_size is None else declared_size
    return full_resync_header(offset=offset) + b"$" + str(size).encode() + b"\r\n" + rdb + stream


class FakeClassicMaster:
    """Listens on `port` (0: any free one, read it back from `.port` after start()).

    Answers PING with +PONG; REPLCONF UUID, DRAKEY-VERSION and PEER with the error a pre-fork
    master gives for an unknown option (REPLCONF UUID with a bare `+<uuid>`, as KeyDB does, after
    script_uuid()); every other REPLCONF (listening-port, capa, ip-address) with +OK (every capa
    one with what script_capa_reply() set, those naming a word it was given `only_for` with that
    word's reply), except `REPLCONF ACK`, which is never answered.
    PSYNC/SYNC is answered by one write() of the bytes given to script_psync(), so coalescing on
    the wire is deterministic; the optional stream bytes follow in a second write, after
    `stream_delay` seconds if given (long enough for the replica to read the first write alone:
    coalescing is deterministic only then). Afterwards the connection stays open, still recording
    requests, until the replica closes it or `close_after_psync` is set; `send_stream()` writes
    more stream bytes into it whenever a test wants them, to time commands against the clock.

    Recorded: `connection_count` (every accepted connection), `requests` (every request as a list
    of words, all connections, in arrival order) and `psync_requests` (the PSYNC ones).
    """

    def __init__(self, host="127.0.0.1", port=0):
        self.host = host
        self.port = port
        self.connection_count = 0
        self.requests = []
        self._psync_reply = diskless_full_sync()
        self._psync_stream = b""
        self._stream_delay = 0
        self._close_after_psync = False
        self._uuid = None
        self._capa_reply = b"+OK\r\n"
        self._capa_rules = {}
        self._server = None
        self._handler_tasks = set()
        self._writers = set()
        self._stream_writer = None

    def script_psync(self, reply, stream=b"", close_after_psync=False, stream_delay=0):
        """Sets what every following PSYNC is answered with (see the class docstring)."""
        self._psync_reply = reply
        self._psync_stream = stream
        self._stream_delay = stream_delay
        self._close_after_psync = close_after_psync

    def script_uuid(self, uuid):
        """Makes every following `REPLCONF UUID` be answered with `+<uuid>`, like a KeyDB does."""
        self._uuid = uuid

    def script_capa_reply(self, reply, only_for=None):
        """Sets the reply (a complete RESP reply: a line, or an array's whole bytes) to the
        `REPLCONF capa ...` requests, e.g. b"+OK active-replica\\r\\n" for what an active KeyDB
        says. Without `only_for` it is the reply to every one of them. With `only_for` (a
        capability word, e.g. "dragonfly" or "activeExpire", any case), only the requests that name
        it get `reply`, in addition to whatever the default is (+OK until set); one call per word.
        """
        if only_for is None:
            self._capa_reply = reply
        else:
            self._capa_rules[only_for.lower()] = reply

    async def send_stream(self, data):
        """Writes `data` into the replication stream of the connection that was last answered a
        PSYNC, behind whatever script_psync() wrote there (the scripted stream, once its delay has
        passed, included), as a live master streams a command that was just run. The caller owns
        the timing: nothing is written until it is called."""
        writer = self._stream_writer
        assert writer is not None, "no replica is in the replication stream yet"
        writer.write(data)
        await writer.drain()

    async def drop_connections(self):
        """Closes every open connection, as a master that went away does, and keeps listening."""
        for writer in list(self._writers):
            writer.close()

    @property
    def psync_requests(self):
        return [r for r in self.requests if r and r[0].upper() in ("PSYNC", "SYNC")]

    @property
    def ack_offsets(self):
        """The offset of every `REPLCONF ACK <offset>` received, in arrival order."""
        return [int(r[2]) for r in self.requests if len(r) == 3 and r[:2] == ["REPLCONF", "ACK"]]

    async def wait_for_settled_ack(self, since=0, repeats=3, timeout=30):
        """Waits until the last `repeats` of the ACKs received after the first `since` ones carry
        the same offset, i.e. the replica went `repeats - 1` ACK intervals without moving it, and
        returns that offset (None on timeout). Pass `since=len(ack_offsets)` taken once the replica
        is known to have applied everything, so that ACKs sent before that cannot settle it.

        A test that checks an offset must wait for this: an ACK of the expected value may appear
        early and be followed by a larger one, and checking `expected in ack_offsets` would pass."""
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() <= deadline:
            offsets = self.ack_offsets[since:]
            if len(offsets) >= repeats and len(set(offsets[-repeats:])) == 1:
                return offsets[-1]
            await asyncio.sleep(0.05)
        return None

    async def wait_for_connections(self, count, timeout=30):
        """Waits until at least `count` connections were accepted; returns the actual number."""
        deadline = asyncio.get_running_loop().time() + timeout
        while self.connection_count < count:
            if asyncio.get_running_loop().time() > deadline:
                break
            await asyncio.sleep(0.05)
        return self.connection_count

    async def start(self):
        self._server = await asyncio.start_server(self._handle, self.host, self.port)
        self.port = self._server.sockets[0].getsockname()[1]

    async def close(self):
        if self._server is not None:
            self._server.close()
            self._server = None
        for writer in list(self._writers):
            writer.close()
        for task in list(self._handler_tasks):
            task.cancel()
        await asyncio.gather(*self._handler_tasks, return_exceptions=True)

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    @staticmethod
    async def _read_request(reader):
        """One request as a list of words (inline or RESP array form); None on EOF."""
        while True:
            line = await reader.readline()
            if not line:
                return None
            line = line.rstrip(b"\r\n")
            if line:  # an empty line is the replica's keepalive newline
                break
        if not line.startswith(b"*"):
            return line.decode(errors="replace").split()
        words = []
        for _ in range(int(line[1:])):
            length = int((await reader.readline()).rstrip(b"\r\n")[1:])
            words.append((await reader.readexactly(length + 2))[:-2].decode(errors="replace"))
        return words

    async def _handle(self, reader, writer):
        task = asyncio.current_task()
        self._handler_tasks.add(task)
        self._writers.add(writer)
        self.connection_count += 1
        try:
            while True:
                request = await self._read_request(reader)
                if request is None:
                    break
                self.requests.append(request)
                if not await self._answer(request, writer):
                    break
        except (ConnectionError, asyncio.IncompleteReadError, asyncio.CancelledError):
            pass
        finally:
            self._writers.discard(writer)
            if self._stream_writer is writer:
                self._stream_writer = None
            self._handler_tasks.discard(task)
            writer.close()

    async def _answer(self, request, writer):
        """Writes the reply to `request`; returns False when the connection should be closed."""
        name = request[0].upper()
        if name == "PING":
            writer.write(b"+PONG\r\n")
        elif name == "REPLCONF":
            option = request[1].upper() if len(request) > 1 else ""
            if option == "ACK":
                return True
            if option == "UUID" and self._uuid is not None:
                writer.write(b"+" + self._uuid.encode() + b"\r\n")
            elif option in ("UUID", "DRAKEY-VERSION", "PEER"):
                writer.write(b"-ERR Unrecognized REPLCONF option: " + option.encode() + b"\r\n")
            elif option == "CAPA":
                named = [word.lower() for word in request[1:]]
                writer.write(
                    next(
                        (self._capa_rules[w] for w in named if w in self._capa_rules),
                        self._capa_reply,
                    )
                )
            else:
                writer.write(b"+OK\r\n")
        elif name in ("PSYNC", "SYNC"):
            writer.write(self._psync_reply)
            await writer.drain()
            if self._psync_stream:
                await asyncio.sleep(self._stream_delay)
                writer.write(self._psync_stream)
            # Only now: a send_stream() during the delay above would land ahead of the scripted
            # stream, and the caller asked for it to come behind it.
            self._stream_writer = writer
            await writer.drain()
            return not self._close_after_psync
        else:
            writer.write(b"+OK\r\n")
        await writer.drain()
        return True
