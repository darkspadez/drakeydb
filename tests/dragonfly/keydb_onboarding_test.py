"""Interop tests for a drakeydb node replicating from a real KeyDB master (v6.3.4).

They need a keydb-server binary: $KEYDB_SERVER_PATH, else `keydb-server` on PATH (see
docs/build-from-source.md). Without one they skip, and with KEYDB_REQUIRED=1 they fail instead, as
they do whenever $KEYDB_SERVER_PATH is set but is not an executable.
"""

import asyncio

import pytest
import redis

from .fake_classic_master import (
    EOF_TOKEN,
    EMPTY_RDB,
    FakeClassicMaster,
    MINIMAL_RDB,
    diskless_full_sync,
    disk_full_sync,
    full_resync_header,
    resp_command,
)
from .instance import DflyInstanceFactory
from .utility import assert_eventually, wait_available_async

pytestmark = pytest.mark.keydb

PRE_STRINGS = {f"pre:{i}": f"v{i}" for i in range(100)}

# DEBUG POPULATE adds BULK_KEYS keys to KeyDB, and its RDB child sleeps RDB_KEY_SAVE_DELAY_US per
# key (rdb-key-save-delay), so the full sync lasts at least BULK_KEYS * RDB_KEY_SAVE_DELAY_US = 2s:
# long enough to write into the sync window, whatever the machine's speed.
BULK_KEYS = 5000
RDB_KEY_SAVE_DELAY_US = 400

# A live write counts as made during the full sync only if KeyDB still reported the sync running
# after it. A test needs this many of them, so that a sync window that closed early fails the test
# instead of voiding it.
MIN_WRITES_IN_SYNC_WINDOW = 20
MAX_LIVE_WRITES = 1000


async def seed_before_attach(keydb):
    """Writes the data a replica must receive through the full sync: every type, a key with a TTL,
    a key in db 1 and a sizable bulk keyspace, whose slow transfer opens the sync window."""
    async with keydb.client() as k, keydb.client(db=1) as k1:
        await k.mset(PRE_STRINGS)
        await k.hset("pre:hash", mapping={"f1": "1", "f2": "2"})
        await k.rpush("pre:list", "a", "b", "c")
        await k.sadd("pre:set", "x", "y")
        await k.zadd("pre:zset", {"m1": 1, "m2": 2})
        await k.set("ttl:key", "v", ex=1000)
        await k1.set("db1:key", "in-db-1")
        await k.execute_command("DEBUG", "POPULATE", BULK_KEYS, "bulk", 64)
        await k.config_set("rdb-key-save-delay", RDB_KEY_SAVE_DELAY_US)


async def write_during_full_sync(k):
    """Writes into KeyDB while its replica is still receiving the full sync.

    Every round sets a live key and bumps a counter, then asks KeyDB whether the replica is online
    yet; it is once the RDB transfer ends. Returns the live keys, and the counter equals their
    number. KeyDB forks its RDB child as soon as the replica attaches, so none of these writes is in
    the snapshot: KeyDB queues them behind it and the replica gets them from the stream.
    """

    @assert_eventually(times=100)
    async def replica_attached():
        assert "slave0" in await k.info("replication")

    await replica_attached()
    live = {}
    in_window = 0
    for i in range(MAX_LIVE_WRITES):
        live[f"live:{i}"] = f"v{i}"
        await k.set(f"live:{i}", f"v{i}")
        await k.incr("live:counter")
        slave = (await k.info("replication")).get("slave0")
        assert slave is not None, "the replica dropped off KeyDB during its full sync"
        if slave["state"] == "online":
            break
        in_window += 1
        await asyncio.sleep(0.01)
    assert in_window >= MIN_WRITES_IN_SYNC_WINDOW, f"only {in_window} writes inside the sync window"
    return live


@assert_eventually(times=300)
async def assert_full_sync_arrived(c):
    """Peer mode reports its link up before the full sync's data is visible, so this polls."""
    assert await c.mget(list(PRE_STRINGS)) == list(PRE_STRINGS.values())
    assert await c.hgetall("pre:hash") == {"f1": "1", "f2": "2"}
    assert await c.lrange("pre:list", 0, -1) == ["a", "b", "c"]
    assert await c.smembers("pre:set") == {"x", "y"}
    assert await c.zrange("pre:zset", 0, -1, withscores=True) == [("m1", 1.0), ("m2", 2.0)]


async def wait_for_peer_link(c):
    """Waits until an --active_replica node shows its single peer link up and done syncing."""

    @assert_eventually(times=300)
    async def link_up():
        info = await c.info("replication")
        assert info["connected_masters"] == 1, info
        peer = info["master0"]
        assert peer["link_status"] == "up" and peer["sync_in_progress"] == 0, peer

    await link_up()


@assert_eventually(times=300)
async def assert_ttl_and_db1_arrived(c, c1):
    """The TTL survived with its time left, and the key of db 1 is in db 1 (`c1` selects it)."""
    ttl = await c.ttl("ttl:key")
    assert 900 < ttl <= 1000, ttl
    assert await c1.get("db1:key") == "in-db-1"


@assert_eventually(times=300)
async def assert_live_writes_arrived(c, live):
    """Every write made during the full sync is there, none lost and none applied twice."""
    values = await c.mget(list(live))
    # Not comparing the two lists: this runs in a polling loop, and pytest's explanation of a failed
    # list comparison costs a second per attempt.
    missing = [key for key, value in zip(live, values) if value != live[key]]
    assert not missing, f"{len(missing)} of {len(live)} live writes missing, e.g. {missing[:3]}"
    assert await c.get("live:counter") == str(len(live))


@assert_eventually(times=300)
async def assert_keyspaces_match(c, k):
    """Everything in KeyDB's db 0 is on the replica too, the bulk keys included."""
    assert await c.dbsize() == await k.dbsize()
    for i in (0, BULK_KEYS // 2, BULK_KEYS - 1):
        expected = await k.get(f"bulk:{i}")
        assert expected is not None
        assert await c.get(f"bulk:{i}") == expected


async def assert_keydb_saw_one_full_sync(k):
    """KeyDB served exactly one full sync, to one replica, and no partial resync: the replica never
    dropped its link and reconnected. A reconnect would deliver data the stream failed to carry by
    a second full sync or a replayed backlog, and hide the failure."""
    stats = await k.info("stats")
    assert stats["sync_full"] == 1, stats
    assert stats["sync_partial_ok"] == 0 and stats["sync_partial_err"] == 0, stats
    assert (await k.info("replication"))["connected_slaves"] == 1


async def test_keydb_plain_master_full_sync_and_stream(
    df_factory: DflyInstanceFactory, keydb_server_factory, tmp_path
):
    """A plain drakeydb replica of a plain (non-active) KeyDB master: the full sync (every type, a
    TTL, a key in db 1, a bulk keyspace), the writes made while it runs, and the stream that follows
    it all arrive, over one full sync and one uninterrupted link."""
    keydb = keydb_server_factory(active_replica=False)
    node = df_factory.create(proactor_threads=2, dir=str(tmp_path / "df"))
    node.start()
    c, c1 = node.client(), node.client(db=1)

    async with keydb.client() as k:
        await seed_before_attach(keydb)
        assert await c.execute_command(f"REPLICAOF localhost {keydb.port}") == "OK"
        live = await write_during_full_sync(k)
        await wait_available_async(c)
        info = await c.info("replication")
        assert info["role"] == "slave" and info["master_link_status"] == "up", info
        await assert_full_sync_arrived(c)
        await assert_ttl_and_db1_arrived(c, c1)
        await assert_live_writes_arrived(c, live)

        await k.set("post:key", "streamed")
        await k.incr("post:counter")
        await k.incr("post:counter")
        await k.hset("post:hash", "f", "v")
        await k.delete("pre:0")

        @assert_eventually(times=300)
        async def stream_arrived():
            assert await c.get("post:key") == "streamed"
            assert await c.get("post:counter") == "2"
            assert await c.hget("post:hash", "f") == "v"
            assert await c.exists("pre:0") == 0

        await stream_arrived()
        await assert_keyspaces_match(c, k)
        await assert_keydb_saw_one_full_sync(k)
        info = await c.info("replication")
        assert info["master_link_status"] == "up", info


async def test_keydb_active_handshake_and_full_sync(
    df_factory: DflyInstanceFactory, keydb_server_factory, tmp_path
):
    """A plain drakeydb replica of an active-replica KeyDB gets through the handshake and loads
    its full sync (every type, a TTL, a key in db 1, a bulk keyspace).

    An active KeyDB answers every `REPLCONF capa ...` with `+OK active-replica`, so a greeting that
    insists on exactly `+OK` refuses the link ('Bad response to "REPLCONF capa eof capa psync2"',
    REPLICAOF fails with 'replication cancelled') and no key ever arrives. What an active KeyDB
    streams after the full sync is covered by test_keydb_active_live_write_during_full_sync.
    """
    keydb = keydb_server_factory(active_replica=True)
    node = df_factory.create(proactor_threads=2, dir=str(tmp_path / "df"))
    node.start()
    c, c1 = node.client(), node.client(db=1)

    async with keydb.client() as k:
        await seed_before_attach(keydb)
        assert await c.execute_command(f"REPLICAOF localhost {keydb.port}") == "OK"
        await wait_available_async(c)
        info = await c.info("replication")
        assert info["role"] == "slave" and info["master_link_status"] == "up", info
        await assert_full_sync_arrived(c)
        await assert_ttl_and_db1_arrived(c, c1)
        await assert_keyspaces_match(c, k)


async def test_keydb_active_handshake_peer_mode(
    df_factory: DflyInstanceFactory, keydb_server_factory, tmp_path
):
    """As above, with the drakeydb node in peer mode (--active_replica), whose link is reported
    through the masterN block of INFO instead of the plain replica fields."""
    keydb = keydb_server_factory(active_replica=True)
    node = df_factory.create(proactor_threads=2, dir=str(tmp_path / "df"), active_replica="true")
    node.start()
    c, c1 = node.client(), node.client(db=1)

    async with keydb.client() as k:
        await seed_before_attach(keydb)
        assert await c.execute_command(f"REPLICAOF localhost {keydb.port}") == "OK"
        await wait_for_peer_link(c)
        await assert_full_sync_arrived(c)
        await assert_ttl_and_db1_arrived(c, c1)
        await assert_keyspaces_match(c, k)


# drakeydb: strict xfail until P7-1 Task 1.2 unwraps RREPLAY envelopes; a pass then fails the run so
# the marker cannot outlive the fix.
@pytest.mark.xfail(strict=True, reason="needs RREPLAY unwrap (P7-1 Task 1.2)")
@pytest.mark.parametrize("peer_mode", [False, True], ids=["plain_replica", "peer_mode"])
async def test_keydb_active_live_write_during_full_sync(
    df_factory: DflyInstanceFactory, keydb_server_factory, tmp_path, peer_mode
):
    """Writes made on an active KeyDB while its replica is still receiving the full sync reach the
    replica, plain or in peer mode.

    An active KeyDB wraps everything it streams in an RREPLAY envelope, including what it queued
    behind the RDB for a syncing replica (tests/dragonfly/data/README.md has the captured bytes), so
    the replica can only apply these writes once it unwraps the envelope (Task 1.2). Until then the
    full sync lands and these writes are dropped.
    """
    keydb = keydb_server_factory(active_replica=True)
    args = {"active_replica": "true"} if peer_mode else {}
    node = df_factory.create(proactor_threads=2, dir=str(tmp_path / "df"), **args)
    node.start()
    c, c1 = node.client(), node.client(db=1)

    async with keydb.client() as k:
        await seed_before_attach(keydb)
        assert await c.execute_command(f"REPLICAOF localhost {keydb.port}") == "OK"
        live = await write_during_full_sync(k)
        if peer_mode:
            await wait_for_peer_link(c)
        else:
            await wait_available_async(c)
        await assert_full_sync_arrived(c)
        await assert_ttl_and_db1_arrived(c, c1)
        await assert_live_writes_arrived(c, live)
        await assert_keyspaces_match(c, k)
        await assert_keydb_saw_one_full_sync(k)


async def attach_and_watch_retries(df_factory, tmp_path, master, retries=3):
    """Starts a plain replica, points it at the scripted master and waits until the master has seen
    `retries` connections (a replica that keeps retrying) or the replica process died. Returns the
    node, still running when it survived."""
    node = df_factory.create(proactor_threads=2, dir=str(tmp_path / "df"))
    node.start()
    c = node.client()
    assert await c.execute_command(f"REPLICAOF 127.0.0.1 {master.port}") == "OK"
    for _ in range(300):
        if master.connection_count >= retries or node.proc.poll() is not None:
            break
        await asyncio.sleep(0.1)
    return node


async def assert_replica_survived(node, master, log_pattern, retries=3):
    """The replica process is alive, answers INFO, kept reconnecting and logged the error."""
    assert node.proc.poll() is None, f"the replica process died with {node.proc.poll()}"
    assert master.connection_count >= retries, master.connection_count
    assert len(master.psync_requests) >= retries
    c = node.client()
    info = await c.info("replication")
    assert info["role"] == "slave", info
    for _ in range(50):  # each attempt enters LOADING and must leave it again
        try:
            await c.ping()
            break
        except redis.exceptions.BusyLoadingError:
            await asyncio.sleep(0.1)
    else:
        pytest.fail("the replica is stuck in LOADING after the failed full syncs")
    node.stop()
    assert node.find_in_logs(log_pattern), f"no log line matching {log_pattern!r}"


async def test_fake_classic_master_valid_full_sync_reaches_stable(
    df_factory: DflyInstanceFactory, tmp_path
):
    """The control for the malformed-reply tests below: the scripted master's default reply is a
    well-formed (empty) diskless full sync, which the replica takes in one connection."""
    async with FakeClassicMaster() as master:
        node = df_factory.create(proactor_threads=2, dir=str(tmp_path / "df"))
        node.start()
        c = node.client()
        assert await c.execute_command(f"REPLICAOF 127.0.0.1 {master.port}") == "OK"
        await wait_available_async(c)
        info = await c.info("replication")
        assert info["role"] == "slave" and info["master_link_status"] == "up", info
        assert master.connection_count == 1
        assert master.psync_requests == [["PSYNC", "?", "-1"]]
        assert ["REPLCONF", "capa", "eof", "capa", "psync2"] in master.requests


@pytest.mark.parametrize("token", [b"short", b"", b"x" * 41], ids=["short", "empty", "long"])
async def test_psync_bad_eof_token_size_does_not_abort_replica(
    df_factory: DflyInstanceFactory, tmp_path, token
):
    """`$EOF:` must be followed by exactly 40 bytes; any other size used to abort the process
    (a CHECK in Replica::ParseReplicationHeader). It is now a bad header: the replica logs it and
    reconnects."""
    async with FakeClassicMaster() as master:
        master.script_psync(full_resync_header() + b"$EOF:" + token + b"\r\n")
        node = await attach_and_watch_retries(df_factory, tmp_path, master)
        await assert_replica_survived(node, master, r"Bad replication header: \$EOF:")


# A full sync whose tail disagrees with its header is the master's malformed output. Each of these
# used to abort the replica process on a CHECK in Replica::InitiatePSync; now it is logged and the
# replica reconnects. (The tuples are the master's reply, whether it hangs up after sending it, and
# the log line expected.)
MALFORMED_FULL_SYNCS = {
    # The master hangs up in the middle of the closing EOF token, or right after the RDB.
    "eof_token_cut_short": (
        diskless_full_sync(tail=EOF_TOKEN[:10]),
        True,
        r"EOF token is 10 bytes long|Failed to read the full sync EOF token",
    ),
    "eof_token_missing": (
        diskless_full_sync(tail=b""),
        True,
        r"EOF token is 0 bytes long|Failed to read the full sync EOF token",
    ),
    "eof_token_mismatch": (
        diskless_full_sync(tail=b"f" * 40),
        False,
        r"EOF token does not match the one in the header",
    ),
    # A disk-style `$<size>` full sync whose RDB ends before the declared size, or does not end
    # within it.
    "disk_rdb_shorter_than_declared": (
        disk_full_sync(declared_size=len(EMPTY_RDB) + 22),
        False,
        rf"RDB is {len(EMPTY_RDB)} bytes long, the header said {len(EMPTY_RDB) + 22}",
    ),
    "disk_rdb_longer_than_declared": (
        disk_full_sync(declared_size=len(EMPTY_RDB) - 10),
        False,
        r"Out of bound read",
    ),
}


@pytest.mark.parametrize("scenario", list(MALFORMED_FULL_SYNCS))
async def test_psync_full_sync_tail_mismatch_does_not_abort_replica(
    df_factory: DflyInstanceFactory, tmp_path, scenario
):
    """A malformed tail (wrong, short or missing EOF token; an RDB that is not as long as
    declared) is an error: the replica logs it, leaves LOADING and reconnects."""
    reply, close_after_psync, log_pattern = MALFORMED_FULL_SYNCS[scenario]
    async with FakeClassicMaster() as master:
        master.script_psync(reply, close_after_psync=close_after_psync)
        node = await attach_and_watch_retries(df_factory, tmp_path, master)
        await assert_replica_survived(node, master, log_pattern)


SET_A = resp_command("SET", "a", "1")
SET_B = resp_command("SET", "b", "2")
SYNC_OFFSET = 1000

# The bytes a master streams right behind a correct full sync: (reply written in one write(), a
# second write that follows it, the commands' total length). A disk-based master (`$<len>`, KeyDB's
# default and plain Redis's) flushes what it buffered while producing the RDB right behind it.
STREAM_BEHIND_FULL_SYNC = {
    # Small RDB: the header, the RDB and the commands all arrive in the replica's first read.
    "disk": (
        disk_full_sync(rdb=MINIMAL_RDB, offset=SYNC_OFFSET, stream=SET_A + SET_B),
        b"",
        len(SET_A + SET_B),
    ),
    "diskless": (
        diskless_full_sync(offset=SYNC_OFFSET, tail=EOF_TOKEN + SET_A + SET_B),
        b"",
        len(SET_A + SET_B),
    ),
    # The second command is cut in the middle: the rest of it comes in a later write.
    "disk_command_cut_in_two": (
        disk_full_sync(rdb=MINIMAL_RDB, offset=SYNC_OFFSET, stream=SET_A + SET_B[:11]),
        SET_B[11:],
        len(SET_A + SET_B),
    ),
}


@pytest.mark.parametrize("scenario", list(STREAM_BEHIND_FULL_SYNC))
async def test_psync_stream_bytes_behind_full_sync_are_applied(
    df_factory: DflyInstanceFactory, tmp_path, scenario
):
    """Replication stream bytes that arrive in the same write as the end of the full sync are not a
    malformed tail: the replica applies them, in order, and counts them into its offset exactly.

    Falsifying: with the old tail checks the replica aborts (CHECK on the bytes left over after
    the RDB); with the bytes dropped instead of handed to ConsumeRedisStream, "a" never arrives and
    the acknowledged offset stays at the full sync's.
    """
    reply, later_write, stream_len = STREAM_BEHIND_FULL_SYNC[scenario]
    async with FakeClassicMaster() as master:
        master.script_psync(reply, stream=later_write)
        node = df_factory.create(proactor_threads=2, dir=str(tmp_path / "df"))
        node.start()
        c = node.client()
        assert await c.execute_command(f"REPLICAOF 127.0.0.1 {master.port}") == "OK"

        @assert_eventually(times=100)
        async def applied():
            assert node.proc.poll() is None, f"the replica process died with {node.proc.poll()}"
            assert await c.get("a") == "1"
            assert await c.get("b") == "2"

        await applied()
        assert await master.wait_for_ack(SYNC_OFFSET + stream_len), master.ack_offsets
        assert master.connection_count == 1, "the replica reconnected: the full sync was refused"
        assert (await c.info("replication"))["master_link_status"] == "up"
