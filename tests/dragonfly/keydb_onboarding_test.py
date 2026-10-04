"""Interop tests for a drakeydb node replicating from a real KeyDB master (v6.3.4), and tests that
script a classic master instead (fake_classic_master.py).

The KeyDB tests (marked `keydb`) need a keydb-server binary: $KEYDB_SERVER_PATH, else `keydb-server`
on PATH (see docs/build-from-source.md). Without one they skip, and with KEYDB_REQUIRED=1 they fail
instead, as they do whenever $KEYDB_SERVER_PATH is set but is not an executable. The scripted-master
tests need no KeyDB.
"""

import asyncio
import functools

import pytest
import redis
from redis import asyncio as aioredis

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


def retry_while_loading(check):
    """Turns the replica's LOADING reply into a failed assertion, so assert_eventually polls again.

    A node loading a full sync answers every command with -LOADING, which redis-py raises as a
    BusyLoadingError: a ConnectionError, and assert_eventually retries only AssertionErrors. The
    check hits that window whenever it polls while the sync's RDB is being loaded."""

    @functools.wraps(check)
    async def wrapper(*args, **kwargs):
        try:
            return await check(*args, **kwargs)
        except redis.exceptions.BusyLoadingError as e:
            raise AssertionError(f"the node is still loading the full sync: {e}") from e

    return wrapper


@assert_eventually(times=300)
@retry_while_loading
async def assert_full_sync_arrived(c):
    """Peer mode reports its link up before the full sync's data is visible, so this polls."""
    assert await c.mget(list(PRE_STRINGS)) == list(PRE_STRINGS.values())
    assert await c.hgetall("pre:hash") == {"f1": "1", "f2": "2"}
    assert await c.lrange("pre:list", 0, -1) == ["a", "b", "c"]
    assert await c.smembers("pre:set") == {"x", "y"}
    assert await c.zrange("pre:zset", 0, -1, withscores=True) == [("m1", 1.0), ("m2", 2.0)]


async def wait_for_peer_link(c):
    """Waits until an --active_replica node shows its single peer link up and not syncing.

    That is true as soon as the TCP connection is: a peer link only reports sync_in_progress once
    the master's `$` header has arrived, which is after its `+FULLRESYNC` (the harness's KeyDB
    streams the RDB diskless, so it sends the header when the transfer starts). So this can return
    before the full sync has begun, and the node is then LOADING for all of it. The assert_* checks
    below poll for the data and retry through that (retry_while_loading)."""

    @assert_eventually(times=300)
    async def link_up():
        info = await c.info("replication")
        assert info["connected_masters"] == 1, info
        peer = info["master0"]
        assert peer["link_status"] == "up" and peer["sync_in_progress"] == 0, peer

    await link_up()


@assert_eventually(times=300)
@retry_while_loading
async def assert_ttl_and_db1_arrived(c, c1):
    """The TTL survived with its time left, and the key of db 1 is in db 1 (`c1` selects it)."""
    ttl = await c.ttl("ttl:key")
    assert 900 < ttl <= 1000, ttl
    assert await c1.get("db1:key") == "in-db-1"


@assert_eventually(times=300)
@retry_while_loading
async def assert_live_writes_arrived(c, live):
    """Every write made during the full sync is there, none lost and none applied twice."""
    values = await c.mget(list(live))
    # Not comparing the two lists: this runs in a polling loop, and pytest's explanation of a failed
    # list comparison costs a second per attempt.
    missing = [key for key, value in zip(live, values) if value != live[key]]
    assert not missing, f"{len(missing)} of {len(live)} live writes missing, e.g. {missing[:3]}"
    assert await c.get("live:counter") == str(len(live))


@assert_eventually(times=300)
@retry_while_loading
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


@pytest.mark.keydb
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


@pytest.mark.keydb
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


@pytest.mark.keydb
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


@pytest.mark.keydb
@pytest.mark.parametrize("peer_mode", [False, True], ids=["plain_replica", "peer_mode"])
async def test_keydb_active_live_write_during_full_sync(
    df_factory: DflyInstanceFactory, keydb_server_factory, tmp_path, peer_mode
):
    """Writes made on an active KeyDB while its replica is still receiving the full sync reach the
    replica, plain or in peer mode.

    An active KeyDB wraps everything it streams in an RREPLAY envelope, including what it queued
    behind the RDB for a syncing replica (tests/dragonfly/data/README.md has the captured bytes), so
    the replica can only apply these writes by unwrapping the envelope.

    Falsifying: with the unwrap removed the full sync lands but "live:*" never arrives, and the
    replica's offset moves past the envelopes it dropped.
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


async def attach_plain_replica(df_factory, tmp_path, keydb, **args):
    """A started plain replica of `keydb`, with its client."""
    node = df_factory.create(proactor_threads=2, dir=str(tmp_path / "df"), **args)
    node.start()
    c = node.client()
    assert await c.execute_command(f"REPLICAOF localhost {keydb.port}") == "OK"
    await wait_available_async(c)
    return node, c


@pytest.mark.keydb
async def test_plain_replica_unwraps_keydb_rreplay(
    df_factory: DflyInstanceFactory, keydb_server_factory, tmp_path
):
    """Every kind of write an active KeyDB makes after the full sync reaches a plain replica, in
    db 0 and db 1: strings (with a TTL, set both ways), counters, hashes, lists, a delete.

    KeyDB sends each of them as its own RREPLAY envelope, rewritten to the absolute and idempotent
    forms it replicates (`INCR` as `INCRBY`, `EX` as `PXAT`, `EXPIRE` as `PEXPIREAT`), and a plain
    replica that does not unwrap them applies none (it counts them as unknown commands).

    Falsifying: with the unwrap removed (ConsumeRedisStream dispatching the RREPLAY command like
    any other) none of the keys arrives.
    """
    keydb = keydb_server_factory(active_replica=True)
    node, c = await attach_plain_replica(df_factory, tmp_path, keydb)
    c1 = node.client(db=1)

    async with keydb.client() as k, keydb.client(db=1) as k1:
        await k.set("str", "v")
        await k.set("str:ex", "v", ex=500)
        await k.set("str:px", "v", px=500_000)
        for _ in range(3):
            await k.incr("counter")
        await k.incrby("counter", 10)
        await k.hset("hash", mapping={"f1": "1", "f2": "2"})
        await k.lpush("list", "a", "b", "c")
        await k.set("gone", "x")
        await k.delete("gone")
        await k.set("expire", "v")
        await k.expire("expire", 1000)
        await k1.set("db1:str", "in-db-1")
        await k1.incr("db1:counter")
        await k1.rpush("db1:list", "x", "y")

        @assert_eventually(times=300)
        @retry_while_loading
        async def arrived():
            assert await c.get("str") == "v"
            assert 0 < await c.ttl("str:ex") <= 500
            assert 0 < await c.pttl("str:px") <= 500_000
            assert await c.get("counter") == "13"
            assert await c.hgetall("hash") == {"f1": "1", "f2": "2"}
            assert await c.lrange("list", 0, -1) == ["c", "b", "a"]
            assert await c.exists("gone") == 0
            assert 900 < await c.ttl("expire") <= 1000
            assert await c1.get("db1:str") == "in-db-1"
            assert await c1.get("db1:counter") == "1"
            assert await c1.lrange("db1:list", 0, -1) == ["x", "y"]
            assert await c.exists("db1:str") == 0  # db 0 is untouched by what db 1 got

        await arrived()
        assert await c.dbsize() == await k.dbsize()
        info = await c.info("replication")
        assert info["role"] == "slave" and info["master_link_status"] == "up", info
        await assert_keydb_saw_one_full_sync(k)


@pytest.mark.keydb
async def test_plain_replica_unwraps_nested_keydb_rreplay(
    df_factory: DflyInstanceFactory, keydb_server_factory, tmp_path
):
    """What a KeyDB forwards reaches a replica of the forwarder: KeyDB B is written to, KeyDB A
    replicates from B and forwards what it replays (`multi-master-no-forward no`, so each write of
    B arrives at A's replica as an RREPLAY envelope of A's that holds B's own: depth 2), and the
    plain drakeydb replica of A applies it. A's own writes, depth 1, arrive on the same link.

    Falsifying: with the unwrap one level deep (the inner envelope dispatched as a command) B's
    writes never arrive, and with a replica that counts the inner envelope's bytes wrong the offset
    drifts (test_unwrap_keeps_offsets_exact).
    """
    flags = dict(active_replica=True, multi_master=True, no_forward=False)
    b = keydb_server_factory(**flags)
    a = keydb_server_factory(**flags)

    async with a.client() as ka, b.client() as kb:
        await ka.execute_command("REPLICAOF", "localhost", b.port)

        @assert_eventually(times=300)
        async def a_synced_from_b():
            assert (await ka.info("replication"))["master_link_status"] == "up"

        await a_synced_from_b()
        node, c = await attach_plain_replica(df_factory, tmp_path, a)

        await kb.set("from:b", "v")
        for _ in range(5):
            await kb.incr("b:counter")
        await kb.hset("b:hash", "f", "v")
        await ka.set("from:a", "w")
        await ka.incr("a:counter")

        @assert_eventually(times=300)
        @retry_while_loading
        async def arrived():
            assert await c.get("from:b") == "v"
            assert await c.get("b:counter") == "5"
            assert await c.hget("b:hash", "f") == "v"
            assert await c.get("from:a") == "w"
            assert await c.get("a:counter") == "1"

        await arrived()
        assert await c.dbsize() == await ka.dbsize()
        assert (await c.info("replication"))["master_link_status"] == "up"


@pytest.mark.keydb
async def test_unwrap_keeps_offsets_exact(
    df_factory: DflyInstanceFactory, keydb_server_factory, tmp_path
):
    """The replica's offset is KeyDB's, to the byte, once the stream is idle: every envelope's
    bytes are counted, the cron PINGs and the `REPLCONF GETACK *` of a WAIT (which are wrapped like
    data and applied as nothing) included.

    `WAIT` returns 1 only once the replica has acknowledged an offset that covers the write, and
    the idle comparison is repeated over several ping periods, so an envelope counted twice or not
    at all shows as a gap that never closes.

    Falsifying: with `repl_offs_ += total_read` removed from the envelope branch, the replica's
    offset stays at the full sync's.
    """
    # A ping every second, so that the stream carries a few envelopes that apply nothing.
    keydb = keydb_server_factory(active_replica=True, repl_ping_replica_period=1)
    node, c = await attach_plain_replica(df_factory, tmp_path, keydb, replication_acks_interval=100)

    async with keydb.client() as k:
        for i in range(20):
            await k.set(f"key:{i}", "v" * i)
            await k.incr("counter")
        # KeyDB asks its replicas for an ACK (an envelope of its own) and counts the ones whose
        # acknowledged offset reaches its write's.
        assert await k.execute_command("WAIT", 1, 5000) == 1

        @assert_eventually(times=300)
        async def offsets_equal():
            master_offset = int((await k.info("replication"))["master_repl_offset"])
            replica_offset = int((await c.info("replication"))["slave_repl_offset"])
            assert replica_offset == master_offset, (replica_offset, master_offset)

        # Past a few cron PINGs: the offset moves under both, and they meet again each time.
        start = int((await k.info("replication"))["master_repl_offset"])
        for _ in range(3):
            await offsets_equal()
            await asyncio.sleep(1.2)
        await offsets_equal()
        assert int((await k.info("replication"))["master_repl_offset"]) > start, "no PING came"
        await assert_keydb_saw_one_full_sync(k)


@pytest.mark.keydb
async def test_stopping_the_link_while_envelopes_stream_is_clean(
    df_factory: DflyInstanceFactory, keydb_server_factory, tmp_path
):
    """A replica stopped in the middle of an RREPLAY stream stops cleanly, whichever envelope or
    raw batch it is at, and attaches again.

    KeyDB takes INCRs as fast as one client can send them while the replica is attached and stopped
    (`REPLICAOF NO ONE`) four times, at different points of the stream; the replica must stay up
    and answer. Attached once more with the writes over, it holds KeyDB's counter exactly: nothing
    the stop interrupted is lost or repeated by the next full sync.

    A stop lands between two envelopes, or in the middle of one being applied, which is finished
    (the stop path of Replica::ConsumeRedisStream: `!exec_st_.IsRunning()` after the flush of the
    raw batch). Neither is observable here but through the replica not crashing or hanging, and
    through the debug build's DCHECKs on that path; the applier's own cancellation rules are
    ClassicApplyFamilyTest.RunningFalse*.
    """
    keydb = keydb_server_factory(active_replica=True)
    node = df_factory.create(proactor_threads=2, dir=str(tmp_path / "df"))
    node.start()
    c = node.client()

    async with keydb.client() as k:
        await k.set("seed", "v")
        stop = asyncio.Event()

        async def writer():
            while not stop.is_set():
                await k.incr("counter")

        task = asyncio.create_task(writer())
        try:
            for i in range(4):
                assert await c.execute_command(f"REPLICAOF localhost {keydb.port}") == "OK"
                await wait_available_async(c)
                await asyncio.sleep(0.2 + 0.15 * i)
                assert await c.execute_command("REPLICAOF NO ONE") == "OK"
                assert node.proc.poll() is None, f"the replica died with {node.proc.poll()}"
                assert (await c.info("replication"))["role"] == "master"
        finally:
            stop.set()
            await task

        assert await c.execute_command(f"REPLICAOF localhost {keydb.port}") == "OK"
        await wait_available_async(c)
        expected = await k.get("counter")

        @assert_eventually(times=300)
        @retry_while_loading
        async def converged():
            assert await c.get("counter") == expected

        await converged()
        assert await c.get("seed") == "v"


async def attach_and_watch_retries(df_factory, tmp_path, master, retries=3):
    """Starts a plain replica, points it at the scripted master and waits until the master has seen
    `retries` PSYNCs (a replica that keeps retrying) or the replica process died. Returns the node,
    still running when it survived. (A connection is accepted a few requests before its PSYNC
    arrives, so counting connections can end the wait one PSYNC short.)"""
    node = df_factory.create(proactor_threads=2, dir=str(tmp_path / "df"))
    node.start()
    c = node.client()
    assert await c.execute_command(f"REPLICAOF 127.0.0.1 {master.port}") == "OK"
    for _ in range(300):
        if len(master.psync_requests) >= retries or node.proc.poll() is not None:
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
    (a CHECK in Replica::ParseReplicationHeader). It is now a bad header: the replica logs the size
    it got and reconnects."""
    async with FakeClassicMaster() as master:
        master.script_psync(full_resync_header() + b"$EOF:" + token + b"\r\n")
        node = await attach_and_watch_retries(df_factory, tmp_path, master)
        pattern = (
            rf"Bad replication header: the \$EOF: token is {len(token)} bytes long, expected 40"
        )
        await assert_replica_survived(node, master, pattern)


async def test_psync_bad_header_line_is_logged_as_received(
    df_factory: DflyInstanceFactory, tmp_path
):
    """The replica logs the header line it refused, whole. The line is the second one, and longer
    than the first (54 bytes): a log that kept a view of the first line (copied by value) would show
    that many bytes of whatever the IoBuf holds by then, which is not this line. Both lines fit the
    128-byte buffer the header is read into."""
    line = b"not-a-dollar-line-" + b"x" * 47
    async with FakeClassicMaster() as master:
        master.script_psync(full_resync_header() + line + b"\r\n")
        node = await attach_and_watch_retries(df_factory, tmp_path, master)
        await assert_replica_survived(node, master, rf"Bad replication header: {line.decode()}$")


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

# The second write of a scenario below comes this long after the first, by when the replica has read
# the first alone and is waiting on the socket for more.
SECOND_WRITE_DELAY_S = 0.5

# The bytes a master streams right behind a correct full sync: (reply written in one write(), a
# second write that follows it, the commands' total length). A disk-based master (`$<len>`, KeyDB's
# default and plain Redis's) flushes what it buffered while producing the RDB right behind it.
STREAM_BEHIND_FULL_SYNC = {
    # Small RDB: the header, the RDB and the commands are written together, so the commands start
    # inside what the replica has already read (its first reads are 128 bytes).
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
    # The loader ends up holding only the first 10 bytes of the closing EOF token: the rest of it,
    # and the commands behind it, are still on the socket and must be read from there.
    "diskless_token_split": (
        diskless_full_sync(rdb=MINIMAL_RDB, offset=SYNC_OFFSET, tail=EOF_TOKEN[:10]),
        EOF_TOKEN[10:] + SET_A + SET_B,
        len(SET_A + SET_B),
    ),
}


@pytest.mark.parametrize("scenario", list(STREAM_BEHIND_FULL_SYNC))
async def test_psync_stream_bytes_behind_full_sync_are_applied(
    df_factory: DflyInstanceFactory, tmp_path, scenario
):
    """Replication stream bytes that arrive in the same write as the end of the full sync are not a
    malformed tail: the replica applies them, in order, and counts them into its offset exactly:
    the ACKs, once they settle, never exceeded and finally equal the full sync's offset plus the
    commands' length.

    Falsifying: with the old tail checks the replica aborts (CHECK on the bytes left over after
    the RDB); with the bytes dropped instead of handed to ConsumeRedisStream, "a" never arrives and
    the acknowledged offset stays at the full sync's; with them counted twice, the offset overshoots.
    """
    reply, later_write, stream_len = STREAM_BEHIND_FULL_SYNC[scenario]
    async with FakeClassicMaster() as master:
        master.script_psync(reply, stream=later_write, stream_delay=SECOND_WRITE_DELAY_S)
        # Frequent ACKs: the offset is checked over many of them, and settles sooner.
        node = df_factory.create(
            proactor_threads=2, dir=str(tmp_path / "df"), replication_acks_interval=100
        )
        node.start()
        c = node.client()
        assert await c.execute_command(f"REPLICAOF 127.0.0.1 {master.port}") == "OK"

        @assert_eventually(times=100)
        @retry_while_loading
        async def applied():
            assert node.proc.poll() is None, f"the replica process died with {node.proc.poll()}"
            assert await c.get("a") == "1"
            assert await c.get("b") == "2"

        await applied()
        # Exact, not "reached at some point": an offset counted twice would still show the
        # expected value in an early ACK, so wait for the ACKs sent from now on to stop moving
        # and check them all.
        expected = SYNC_OFFSET + stream_len
        settled = await master.wait_for_settled_ack(since=len(master.ack_offsets))
        assert settled == expected, master.ack_offsets
        assert max(master.ack_offsets) == expected, master.ack_offsets
        assert master.ack_offsets[-1] == expected, master.ack_offsets
        assert master.connection_count == 1, "the replica reconnected: the full sync was refused"
        assert (await c.info("replication"))["master_link_status"] == "up"


# An empty command name: a valid RESP array of one empty bulk string. Any classic master's stream
# bytes can say it, so the replica must take it as the unknown command it is.
EMPTY_COMMAND_NAME = b"*1\r\n$0\r\n\r\n"

# What follows a valid sync, by how the master's writes split: (the PSYNC reply, the stream that
# follows it in a second write, after SECOND_WRITE_DELAY_S). `behind_rdb` has the stream in the same
# write as the RDB's end, so the replica starts the stable sync with those bytes already read.
EMPTY_NAME_SCENARIOS = {
    "diskless_later_write": (diskless_full_sync(offset=SYNC_OFFSET), EMPTY_COMMAND_NAME + SET_A),
    "disk_later_write": (
        disk_full_sync(rdb=MINIMAL_RDB, offset=SYNC_OFFSET),
        EMPTY_COMMAND_NAME + SET_A,
    ),
    "disk_behind_rdb": (
        disk_full_sync(rdb=MINIMAL_RDB, offset=SYNC_OFFSET, stream=EMPTY_COMMAND_NAME + SET_A),
        b"",
    ),
}


@pytest.mark.parametrize("scenario", list(EMPTY_NAME_SCENARIOS))
async def test_classic_stream_empty_command_name_does_not_abort(
    df_factory: DflyInstanceFactory, tmp_path, scenario
):
    """`*1\\r\\n$0\\r\\n\\r\\n` in the replication stream used to abort the replica (ISSUE-REGISTER
    U-13): Replica::ConsumeRedisStream read the first byte of the command name before checking that
    there is one (an assertion in a debug build, a 1-byte out-of-bounds read in a release one). It
    is an unknown command now: dropped like any other, its bytes counted into the offset, the
    commands after it applied.

    Falsifying: with the check removed, the replica process dies of SIGABRT (`i < size()` in
    absl/types/span.h) as it reads the empty name.
    """
    reply, later_write = EMPTY_NAME_SCENARIOS[scenario]
    async with FakeClassicMaster() as master:
        master.script_psync(reply, stream=later_write, stream_delay=SECOND_WRITE_DELAY_S)
        node = df_factory.create(
            proactor_threads=2, dir=str(tmp_path / "df"), replication_acks_interval=100
        )
        node.start()
        c = node.client()
        assert await c.execute_command(f"REPLICAOF 127.0.0.1 {master.port}") == "OK"

        @assert_eventually(times=100)
        @retry_while_loading
        async def applied():
            assert node.proc.poll() is None, f"the replica process died with {node.proc.poll()}"
            assert await c.get("a") == "1"

        await applied()
        expected = SYNC_OFFSET + len(EMPTY_COMMAND_NAME + SET_A)
        settled = await master.wait_for_settled_ack(since=len(master.ack_offsets))
        assert settled == expected, master.ack_offsets
        assert max(master.ack_offsets) == expected, master.ack_offsets
        assert master.connection_count == 1, "the replica reconnected"
        assert (await c.info("replication"))["master_link_status"] == "up"


# Valid RESP whose command has an array where its name should be: `*0\r\n` and `*-1\r\n`. The
# stream that follows a valid sync, by where such a command sits: first (nothing is queued before
# it, so its bytes are counted at once), or behind a raw command that is still waiting in the batch
# (its bytes are deferred onto that command's entry). The batch is flushed as soon as the read
# drains, so the test cannot see the deferral; it pins the outcome: the offset the master settles on
# is the exact length of the stream, the skipped bytes neither dropped nor counted twice.
ARRAY_NAME_SCENARIOS = {
    "empty_array": b"*0\r\n" + SET_A,
    "nil_array": b"*-1\r\n" + SET_A,
    "behind_a_queued_command": SET_B + b"*0\r\n" + SET_A,
}


@pytest.mark.parametrize("scenario", list(ARRAY_NAME_SCENARIOS))
async def test_classic_stream_command_with_an_array_for_a_name_does_not_abort(
    df_factory: DflyInstanceFactory, tmp_path, scenario
):
    """`*0\\r\\n` and `*-1\\r\\n` in the replication stream used to abort the replica (ISSUE-REGISTER
    U-14): they parse to a lone array, not a string, and Replica::ConsumeRedisStream read the string
    of the command's name (std::bad_variant_access, which terminates). Such a command is skipped
    now, with a warning, its bytes counted into the offset exactly, and the commands around it
    applied.

    Falsifying: with the check removed, the replica process dies of SIGABRT (an uncaught
    std::bad_variant_access out of RespExpr::GetView) as it reads the command.
    """
    stream = ARRAY_NAME_SCENARIOS[scenario]
    async with FakeClassicMaster() as master:
        master.script_psync(
            diskless_full_sync(offset=SYNC_OFFSET), stream=stream, stream_delay=SECOND_WRITE_DELAY_S
        )
        node = df_factory.create(
            proactor_threads=2, dir=str(tmp_path / "df"), replication_acks_interval=100
        )
        node.start()
        c = node.client()
        assert await c.execute_command(f"REPLICAOF 127.0.0.1 {master.port}") == "OK"

        @assert_eventually(times=100)
        @retry_while_loading
        async def applied():
            assert node.proc.poll() is None, f"the replica process died with {node.proc.poll()}"
            assert await c.get("a") == "1"

        await applied()
        if scenario == "behind_a_queued_command":
            assert await c.get("b") == "2"
        expected = SYNC_OFFSET + len(stream)
        settled = await master.wait_for_settled_ack(since=len(master.ack_offsets))
        assert settled == expected, master.ack_offsets
        assert max(master.ack_offsets) == expected, master.ack_offsets
        assert master.connection_count == 1, "the replica reconnected"
        assert (await c.info("replication"))["master_link_status"] == "up"

    node.stop()
    assert node.find_in_logs(r"Skipping a command without a name from 127\.0\.0\.1:\d+")


def rreplay(*command, uuid="b1198d29-cb88-4110-922a-a6c99bd08471", db=0, mvcc=1):
    """The RREPLAY envelope an active KeyDB wraps `command` in."""
    return resp_command("RREPLAY", uuid, resp_command(*command), db, mvcc)


@pytest.mark.parametrize("peer_mode", [False, True], ids=["plain_replica", "peer_mode"])
async def test_unwrap_flushes_raw_batch_before_envelope(
    df_factory: DflyInstanceFactory, tmp_path, peer_mode
):
    """An envelope does not overtake the raw commands before it: the stream applies in order.

    Raw commands wait in a batch until the read buffer drains, and an envelope is dispatched on its
    own. Here the stream (all of it in one write, behind the full sync's RDB, so it is in the
    replica's buffer at once) alternates the two on one key: raw `SET a 1`, envelope `SET a 2`, raw
    `INCR a`, envelope `INCR a`, and the key must end at 4. A later raw write makes sure a batch
    left behind would be applied too. The master did not advertise active-replica (the scripted
    one never does): the unwrap goes by what the stream holds, not by the handshake.

    Falsifying: without the flush before the envelope the batch waits behind it and the key ends at
    2 (the envelopes first, `SET a 1` and `INCR a` after them).
    """
    stream = SET_A + rreplay("SET", "a", 2) + resp_command("INCR", "a") + rreplay("INCR", "a")
    marker = resp_command("SET", "marker", "1")
    args = {"active_replica": "true"} if peer_mode else {}
    async with FakeClassicMaster() as master:
        if peer_mode:
            master.script_uuid("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee")
        master.script_psync(
            diskless_full_sync(offset=SYNC_OFFSET, tail=EOF_TOKEN + stream),
            stream=marker,
            stream_delay=SECOND_WRITE_DELAY_S,
        )
        node = df_factory.create(
            proactor_threads=2, dir=str(tmp_path / "df"), replication_acks_interval=100, **args
        )
        node.start()
        c = node.client()
        assert await c.execute_command(f"REPLICAOF 127.0.0.1 {master.port}") == "OK"

        @assert_eventually(times=100)
        @retry_while_loading
        async def applied():
            assert node.proc.poll() is None, f"the replica process died with {node.proc.poll()}"
            assert await c.get("marker") == "1"

        await applied()
        value = await c.get("a")
        assert value == "4", f"a ended at {value}: the stream was not applied in order"
        expected = SYNC_OFFSET + len(stream + marker)
        settled = await master.wait_for_settled_ack(since=len(master.ack_offsets))
        assert settled == expected, master.ack_offsets
        assert max(master.ack_offsets) == expected, master.ack_offsets
        assert master.connection_count == 1, "the replica reconnected"


async def test_unwrap_selected_db_is_the_one_the_raw_commands_after_it_run_in(
    df_factory: DflyInstanceFactory, tmp_path
):
    """An envelope's db is the stream's selected db, as for KeyDB's own master client: a raw command
    after an envelope in db 2 runs in db 2, so does an envelope without a db (KeyDB's 3-argument
    form), until the next envelope selects another db.

    The stream is `RREPLAY ... SET e 1 2 1`, raw `SET c 1`, 3-argument `RREPLAY ... SET d 1`, then
    `RREPLAY ... SET f 1 0 2` and raw `SET g 1`: e, c and d belong in db 2, f and g in db 0, and
    the offsets are exact.

    Falsifying: with the applier given a connection context of its own (not the one the raw
    commands use), the raw `SET c` lands in db 0 ("c is not in db 2").
    """
    uuid = "b1198d29-cb88-4110-922a-a6c99bd08471"
    stream = (
        rreplay("SET", "e", 1, db=2, mvcc=1)
        + resp_command("SET", "c", 1)
        + resp_command("RREPLAY", uuid, resp_command("SET", "d", 1))
        + rreplay("SET", "f", 1, db=0, mvcc=2)
        + resp_command("SET", "g", 1)
    )
    async with FakeClassicMaster() as master:
        master.script_psync(diskless_full_sync(offset=SYNC_OFFSET, tail=EOF_TOKEN + stream))
        node = df_factory.create(
            proactor_threads=2, dir=str(tmp_path / "df"), replication_acks_interval=100
        )
        node.start()
        c, c2 = node.client(), node.client(db=2)
        assert await c.execute_command(f"REPLICAOF 127.0.0.1 {master.port}") == "OK"

        @assert_eventually(times=100)
        @retry_while_loading
        async def applied():
            assert node.proc.poll() is None, f"the replica process died with {node.proc.poll()}"
            assert await c.get("g") == "1"

        await applied()
        for key in ("e", "c", "d"):
            assert await c2.get(key) == "1", f"{key} is not in db 2"
            assert await c.get(key) is None, f"{key} is in db 0"
        for key in ("f", "g"):
            assert await c.get(key) == "1", f"{key} is not in db 0"
            assert await c2.get(key) is None, f"{key} is in db 2"
        expected = SYNC_OFFSET + len(stream)
        settled = await master.wait_for_settled_ack(since=len(master.ack_offsets))
        assert settled == expected, master.ack_offsets
        assert max(master.ack_offsets) == expected, master.ack_offsets
        assert master.connection_count == 1, "the replica reconnected"


async def test_unwrap_skips_malformed_envelopes_without_disconnect(
    df_factory: DflyInstanceFactory, tmp_path
):
    """An envelope the replica cannot apply is skipped, counted in the offset and warned about, and
    the stream carries on: never a disconnect (a reconnect would be sent the same bytes again).

    The stream holds a bad uuid, a bad db, an inner that is two commands, an inner that is not a
    command, an inner `*0` (valid RESP, but an array for a name: U-14), an unknown inner command,
    an inner PING and an inner SELECT (nothing applies), among envelopes and a raw command that do
    apply, one of them in db 3.

    Falsifying: a replica that disconnects on a malformed envelope reconnects (the master sees a
    second connection and the offsets restart); one that accepts a second inner command applies
    "x" and "y"; one that does not check an inner command's name is a string aborts (SIGABRT, an
    uncaught std::bad_variant_access) at the `*0`.
    """
    uuid = "b1198d29-cb88-4110-922a-a6c99bd08471"
    two_commands = resp_command("SET", "x", 1) + resp_command("SET", "y", 1)
    stream = b"".join(
        [
            resp_command("RREPLAY", "not-a-uuid", resp_command("SET", "bad", 1), 0, 1),
            resp_command("RREPLAY", uuid, resp_command("SET", "bad", 1), 99, 2),
            resp_command("RREPLAY", uuid, two_commands, 0, 3),
            resp_command("RREPLAY", uuid, b"not a command", 0, 4),
            resp_command("RREPLAY", uuid, b"", 0, 5),
            resp_command("RREPLAY", uuid, b"*0\r\n", 0, 6),
            rreplay("NOSUCHCOMMAND", "z", mvcc=7),
            rreplay("PING", mvcc=8),
            rreplay("SELECT", 5, mvcc=9),
            rreplay("SET", "good", 1, mvcc=10),
            resp_command("SET", "raw", 1),
            rreplay("SET", "in3", 1, db=3, mvcc=11),
        ]
    )
    async with FakeClassicMaster() as master:
        master.script_psync(diskless_full_sync(offset=SYNC_OFFSET, tail=EOF_TOKEN + stream))
        node = df_factory.create(
            proactor_threads=2, dir=str(tmp_path / "df"), replication_acks_interval=100
        )
        node.start()
        c, c3 = node.client(), node.client(db=3)
        assert await c.execute_command(f"REPLICAOF 127.0.0.1 {master.port}") == "OK"

        @assert_eventually(times=100)
        @retry_while_loading
        async def applied():
            assert node.proc.poll() is None, f"the replica process died with {node.proc.poll()}"
            assert await c3.get("in3") == "1"

        await applied()
        assert await c.get("good") == "1"
        assert await c.get("raw") == "1"
        for key in ("bad", "x", "y", "z"):
            assert await c.get(key) is None, key
        assert await c.dbsize() == 2  # good, raw
        expected = SYNC_OFFSET + len(stream)
        settled = await master.wait_for_settled_ack(since=len(master.ack_offsets))
        assert settled == expected, master.ack_offsets
        assert max(master.ack_offsets) == expected, master.ack_offsets
        assert master.connection_count == 1, "the replica reconnected"
        assert (await c.info("replication"))["master_link_status"] == "up"

    node.stop()
    assert node.find_in_logs(r"Skipping a malformed RREPLAY envelope from 127\.0\.0\.1:\d+")


@pytest.mark.parametrize("size", [100_000, 400_000, 3_000_000])
async def test_unwrap_applies_envelope_larger_than_the_read_buffer(
    df_factory: DflyInstanceFactory, tmp_path, size
):
    """An envelope bigger than what the replica reads at once (its buffer is 128KB, and TCP splits
    a megabytes-long write anyway) is parsed over several reads and its command applied whole, with
    the envelope's bytes counted once. The command is a bulk string inside a bulk string: what the
    applier sees of it must stay valid for the whole apply.

    Falsifying: a replica that counts only the last read of an envelope (`left_in_buffer`, not
    `total_read`) acknowledges an offset short by the earlier reads (only this test sees it: the
    other tests' envelopes arrive in one read).
    """
    value = "x" * (size - 1) + "y"
    stream = rreplay("SET", "big", value, mvcc=1) + rreplay("SET", "small", 1, mvcc=2)
    marker = resp_command("SET", "marker", "1")
    async with FakeClassicMaster() as master:
        master.script_psync(
            diskless_full_sync(offset=SYNC_OFFSET, tail=EOF_TOKEN + stream),
            stream=marker,
            stream_delay=SECOND_WRITE_DELAY_S,
        )
        node = df_factory.create(
            proactor_threads=2, dir=str(tmp_path / "df"), replication_acks_interval=100
        )
        node.start()
        c = node.client()
        assert await c.execute_command(f"REPLICAOF 127.0.0.1 {master.port}") == "OK"

        @assert_eventually(times=100)
        @retry_while_loading
        async def applied():
            assert node.proc.poll() is None, f"the replica process died with {node.proc.poll()}"
            assert await c.get("marker") == "1"

        await applied()
        stored = await c.get("big")
        assert stored == value, f"{len(stored)} of {len(value)} bytes, ends in {stored[-3:]!r}"
        assert await c.get("small") == "1"
        expected = SYNC_OFFSET + len(stream + marker)
        settled = await master.wait_for_settled_ack(since=len(master.ack_offsets))
        assert settled == expected, master.ack_offsets
        assert max(master.ack_offsets) == expected, master.ack_offsets
        assert master.connection_count == 1, "the replica reconnected"


# The counters of a classic link, in the order INFO shows them, as /metrics names them (the series
# is `dragonfly_<name>_total`).
CLASSIC_COUNTERS = [
    "rreplay_unwrapped",
    "rreplay_malformed",
    "rreplay_self_dropped",
    "keydb_cmds_dropped",
    "classic_unknown_cmds_dropped",
    "classic_apply_errors",
]


def classic_fields(info, peer_mode):
    """The classic counters of the one link `info` (INFO replication) reports, in INFO's order: the
    `key:value` lines of a plain replica, the `,key=value` pairs of the master0 line in peer mode.
    """
    link = info["master0"] if peer_mode else info
    return {name: link[name] for name in link if name in CLASSIC_COUNTERS}


def classic_series(metrics):
    """The classic series of a /metrics scrape (`DflyInstance.metrics()`): name to value."""
    series = {}
    for name in CLASSIC_COUNTERS:
        family = metrics.get(f"dragonfly_{name}")
        if family is not None:
            assert family.type == "counter", family
            (sample,) = family.samples
            assert sample.name == f"dragonfly_{name}_total", sample
            series[name] = sample.value
    return series


@pytest.mark.keydb
async def test_keydb_only_commands_dropped_with_counters(
    df_factory: DflyInstanceFactory, keydb_server_factory, tmp_path
):
    """What an active KeyDB streams that drakeydb has no equivalent of is dropped and counted, and
    everything around it still applies: a member TTL (`EXPIREMEMBER`, which KeyDB streams as
    `PEXPIREMEMBERAT`) and a cron job (`KEYDB.CRON`) between a set and a plain key.

    The set arrives whole, without the member's TTL, `keydb_cmds_dropped` is exactly the two
    commands, none of them is an unknown command or an apply error, and the replica stays up.

    Falsifying: with IsKeyDbOnlyCommand returning false the two commands land in
    `classic_unknown_cmds_dropped` and `keydb_cmds_dropped` stays 0 (they are not dispatched
    either way: an unknown command is skipped before the dispatcher).
    """
    keydb = keydb_server_factory(active_replica=True)
    node, c = await attach_plain_replica(df_factory, tmp_path, keydb)

    async with keydb.client() as k:
        await k.sadd("s", "a", "b")
        await k.execute_command("EXPIREMEMBER", "s", "a", 100)
        await k.execute_command("KEYDB.CRON", "job", "single", 3_600_000, "return 1")
        await k.set("normal", "v")

        @assert_eventually(times=300)
        @retry_while_loading
        async def arrived():
            assert node.proc.poll() is None, f"the replica process died with {node.proc.poll()}"
            assert await c.get("normal") == "v"

        await arrived()
        assert await c.smembers("s") == {"a", "b"}
        info = await c.info("replication")
        assert info["keydb_cmds_dropped"] == 2, info
        assert info["rreplay_unwrapped"] >= 4, info  # SADD, EXPIREMEMBER, KEYDB.CRON, SET, PINGs
        assert info["classic_unknown_cmds_dropped"] == 0, info
        assert info["classic_apply_errors"] == 0, info
        assert info["rreplay_malformed"] == 0, info
        assert info["master_link_status"] == "up", info
        await assert_keydb_saw_one_full_sync(k)


@pytest.mark.keydb
@pytest.mark.parametrize("peer_mode", [False, True], ids=["plain_replica", "peer_mode"])
async def test_info_and_metrics_show_classic_counters(
    df_factory: DflyInstanceFactory, keydb_server_factory, tmp_path, peer_mode
):
    """A classic link to an active KeyDB shows all its counters in INFO replication, in the link's
    own block (`key:value` lines of a plain replica, the master0 line with a `repl_offset` in peer
    mode), and /metrics exports them as `dragonfly_<name>_total`, on a plain replica too (which
    never reaches the master-side branch of the metrics).

    Falsifying: omitting the INFO branch leaves no field; omitting the replica-side /metrics
    branch leaves a plain replica without the series (a peer node has them in the master-side
    one).
    """
    keydb = keydb_server_factory(active_replica=True)
    args = {"active_replica": "true"} if peer_mode else {}
    node = df_factory.create(proactor_threads=2, dir=str(tmp_path / "df"), **args)
    node.start()
    c = node.client()
    assert await c.execute_command(f"REPLICAOF localhost {keydb.port}") == "OK"
    if peer_mode:
        await wait_for_peer_link(c)
    else:
        await wait_available_async(c)

    async with keydb.client() as k:
        await k.sadd("s", "a")
        await k.execute_command("EXPIREMEMBER", "s", "a", 100)
        await k.set("normal", "v")

        @assert_eventually(times=300)
        @retry_while_loading
        async def arrived():
            assert await c.get("normal") == "v"

        await arrived()

    info = await c.info("replication")
    fields = classic_fields(info, peer_mode)
    assert list(fields) == CLASSIC_COUNTERS, info
    assert fields["keydb_cmds_dropped"] == 1, fields
    assert fields["rreplay_unwrapped"] >= 3, fields  # SADD, EXPIREMEMBER, SET, and KeyDB's PINGs
    if peer_mode:
        assert info["master0"]["repl_offset"] > 0, info

    series = classic_series(await node.metrics())
    assert list(series) == CLASSIC_COUNTERS, series
    assert series["keydb_cmds_dropped"] == 1, series
    for name in CLASSIC_COUNTERS:  # the process-wide sum is never behind the link's own count
        assert series[name] >= fields[name], (name, series, fields)


async def attach_scripted_master(df_factory, tmp_path, master, peer_mode):
    """A node (a peer node if `peer_mode`) replicating from `master`, with its client."""
    args = {"active_replica": "true"} if peer_mode else {}
    node = df_factory.create(
        proactor_threads=2, dir=str(tmp_path / "df"), replication_acks_interval=100, **args
    )
    node.start()
    c = node.client()
    assert await c.execute_command(f"REPLICAOF 127.0.0.1 {master.port}") == "OK"
    return node, c


@pytest.mark.parametrize("peer_mode", [False, True], ids=["plain_replica", "peer_mode"])
async def test_scripted_active_master_exact_classic_counters(
    df_factory: DflyInstanceFactory, tmp_path, peer_mode
):
    """The exact counters of a master that says `active-replica`, in INFO and /metrics, for a
    stream with every outcome: an applied envelope, a member-expiry envelope and a raw one (both
    KeyDB-only), an unknown command and a `KEYDB.MVCCRESTORE` (which is applied only from P7-2: it
    is an unknown command here, never a KeyDB-only one, decision 22), a wrong-arity `SET`, a
    malformed envelope, and a raw command.

    5 envelopes were taken apart (the malformed one was not), so rreplay_unwrapped is 5, and the
    link is shown whole, zeros included.

    Falsifying: a KeyDB-only check that also took KEYDB.MVCCRESTORE puts it in keydb_cmds_dropped
    (3, with unknown 1); one that took nothing leaves keydb_cmds_dropped 0 and unknown 3.
    """
    stream = b"".join(
        [
            rreplay("SET", "a", 1, mvcc=1),
            rreplay("EXPIREMEMBER", "s", "m", 100, mvcc=2),
            resp_command("PEXPIREMEMBERAT", "s", "m", 1791058571443),
            rreplay("NOSUCHCMD", "x", mvcc=3),
            rreplay("KEYDB.MVCCRESTORE", "k", 1878060925646274561, -1, "payload", mvcc=4),
            rreplay("SET", "wrong-arity", mvcc=5),
            resp_command("RREPLAY", "not-a-uuid", resp_command("SET", "bad", 1), 0, 6),
            resp_command("SET", "b", 2),
        ]
    )
    async with FakeClassicMaster() as master:
        master.script_capa_reply(b"+OK active-replica\r\n")
        if peer_mode:
            master.script_uuid("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee")
        master.script_psync(diskless_full_sync(offset=SYNC_OFFSET, tail=EOF_TOKEN + stream))
        node, c = await attach_scripted_master(df_factory, tmp_path, master, peer_mode)

        @assert_eventually(times=100)
        @retry_while_loading
        async def applied():
            assert node.proc.poll() is None, f"the replica process died with {node.proc.poll()}"
            assert await c.get("b") == "2"

        await applied()
        assert await c.get("a") == "1"
        assert await c.dbsize() == 2  # a, b
        expected = SYNC_OFFSET + len(stream)
        settled = await master.wait_for_settled_ack(since=len(master.ack_offsets))
        assert settled == expected, master.ack_offsets

        info = await c.info("replication")
        assert classic_fields(info, peer_mode) == {
            "rreplay_unwrapped": 5,
            "rreplay_malformed": 1,
            "rreplay_self_dropped": 0,
            "keydb_cmds_dropped": 2,
            "classic_unknown_cmds_dropped": 2,
            "classic_apply_errors": 1,
        }, info
        assert list(classic_fields(info, peer_mode)) == CLASSIC_COUNTERS
        if peer_mode:
            assert info["master0"]["repl_offset"] == expected, info
        assert classic_series(await node.metrics()) == {
            "rreplay_unwrapped": 5,
            "rreplay_malformed": 1,
            "rreplay_self_dropped": 0,
            "keydb_cmds_dropped": 2,
            "classic_unknown_cmds_dropped": 2,
            "classic_apply_errors": 1,
        }


@pytest.mark.parametrize("peer_mode", [False, True], ids=["plain_replica", "peer_mode"])
async def test_scripted_quiet_active_master_shows_zero_counters(
    df_factory: DflyInstanceFactory, tmp_path, peer_mode
):
    """A master that says `active-replica` shows its link whole from the start: six zeros, and in
    peer mode the offset, before a single envelope has arrived.

    Falsifying: showing only the counters that moved leaves INFO and /metrics empty here.
    """
    async with FakeClassicMaster() as master:
        master.script_capa_reply(b"+OK active-replica\r\n")
        if peer_mode:
            master.script_uuid("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee")
        master.script_psync(diskless_full_sync(offset=SYNC_OFFSET))
        node, c = await attach_scripted_master(df_factory, tmp_path, master, peer_mode)

        @assert_eventually(times=100)
        @retry_while_loading
        async def linked():
            assert node.proc.poll() is None, f"the replica process died with {node.proc.poll()}"
            info = await c.info("replication")
            link = info["master0"] if peer_mode else info
            assert link["master_link_status" if not peer_mode else "link_status"] == "up", info

        await linked()
        info = await c.info("replication")
        assert classic_fields(info, peer_mode) == dict.fromkeys(CLASSIC_COUNTERS, 0), info
        if peer_mode:
            assert info["master0"]["repl_offset"] == SYNC_OFFSET, info
        assert classic_series(await node.metrics()) == dict.fromkeys(CLASSIC_COUNTERS, 0)


async def test_scripted_stock_master_raw_keydb_only_command_shows_only_that_counter(
    df_factory: DflyInstanceFactory, tmp_path
):
    """A master that did not say `active-replica` (a plain KeyDB streams `PEXPIREMEMBERAT` raw) has
    its KeyDB-only command dropped and counted on the raw path as well, its bytes counted into the
    offset, and the commands around it applied. Its link shows only the counter that moved, and
    /metrics only that series: nothing else of a stock master's INFO changes.

    An unknown raw command is not this task's to count: it stays on upstream's accounting, so
    `classic_unknown_cmds_dropped` stays out of both.

    Falsifying: without the raw-path check the command reaches the dispatcher (an unknown
    command), `keydb_cmds_dropped` stays 0 and no field is shown.
    """
    stream = (
        resp_command("SET", "a", 1)
        + resp_command("PEXPIREMEMBERAT", "s", "m", 1791058571443)
        + resp_command("NOSUCHRAWCMD", "x")
        + resp_command("pexpirememberat", "s", "m", 1791058571443)
        + resp_command("SET", "b", 2)
    )
    async with FakeClassicMaster() as master:
        master.script_psync(diskless_full_sync(offset=SYNC_OFFSET, tail=EOF_TOKEN + stream))
        node, c = await attach_scripted_master(df_factory, tmp_path, master, peer_mode=False)

        @assert_eventually(times=100)
        @retry_while_loading
        async def applied():
            assert node.proc.poll() is None, f"the replica process died with {node.proc.poll()}"
            assert await c.get("b") == "2"

        await applied()
        assert await c.get("a") == "1"
        expected = SYNC_OFFSET + len(stream)
        settled = await master.wait_for_settled_ack(since=len(master.ack_offsets))
        assert settled == expected, master.ack_offsets
        assert master.connection_count == 1, "the replica reconnected"

        info = await c.info("replication")
        assert classic_fields(info, False) == {"keydb_cmds_dropped": 2}, info
        assert classic_series(await node.metrics()) == {"keydb_cmds_dropped": 2}
        stats = await c.info("commandstats")
        assert any(name.lower() == "unknown_nosuchrawcmd" for name in stats), stats


async def write_and_wait(master_client, c):
    """Writes a few keys of different kinds to a master, and waits until a replica has them."""
    await master_client.set("stock:str", "v")
    await master_client.incr("stock:counter")
    await master_client.hset("stock:hash", mapping={"f": "1"})
    await master_client.set("stock:last", "done")

    @assert_eventually(times=300)
    @retry_while_loading
    async def arrived():
        assert await c.get("stock:last") == "done"
        assert await c.get("stock:counter") == "1"
        assert await c.hgetall("stock:hash") == {"f": "1"}

    await arrived()


async def assert_no_classic_fields(node, c, peer_mode, has_master=True):
    """Neither INFO replication nor /metrics shows a classic field or series.

    Where the node has a master, the link's block also ends where it did before the classic fields
    existed: the plain replica's with `psync_successes`, a peer line with `clock_skew_ms`."""
    info = await c.info("replication")
    link = info["master0"] if peer_mode else info
    for name in CLASSIC_COUNTERS + ["repl_offset"]:
        assert name not in link, (name, info)
    assert classic_series(await node.metrics()) == {}
    assert not any("rreplay" in name or "classic" in name or "keydb" in name for name in link), link
    if has_master:
        assert list(link)[-1] == ("clock_skew_ms" if peer_mode else "psync_successes"), link


async def test_info_and_metrics_absent_for_stock_master(
    df_factory: DflyInstanceFactory, redis_server, tmp_path
):
    """A plain Redis master, every counter zero: INFO replication and /metrics have no classic
    field and no classic series, exactly as before this feature.

    Falsifying: rendering the classic fields (or the series) unconditionally shows six zeros.
    """
    node = df_factory.create(proactor_threads=2, dir=str(tmp_path / "df"))
    node.start()
    c = node.client()
    assert await c.execute_command(f"REPLICAOF localhost {redis_server.port}") == "OK"
    await wait_available_async(c)
    async with aioredis.Redis(port=redis_server.port, decode_responses=True) as master:
        await write_and_wait(master, c)
    info = await c.info("replication")
    assert info["role"] == "slave" and info["master_link_status"] == "up", info
    await assert_no_classic_fields(node, c, peer_mode=False)


@pytest.mark.keydb
@pytest.mark.parametrize("peer_mode", [False, True], ids=["plain_replica", "peer_mode"])
async def test_info_and_metrics_absent_for_keydb_that_is_not_active(
    df_factory: DflyInstanceFactory, keydb_server_factory, tmp_path, peer_mode
):
    """The same for a KeyDB that is not an active replica (it streams raw commands, no envelope),
    on a plain replica and on a peer node."""
    keydb = keydb_server_factory(active_replica=False)
    args = {"active_replica": "true"} if peer_mode else {}
    node = df_factory.create(proactor_threads=2, dir=str(tmp_path / "df"), **args)
    node.start()
    c = node.client()
    assert await c.execute_command(f"REPLICAOF localhost {keydb.port}") == "OK"
    if peer_mode:
        await wait_for_peer_link(c)
    else:
        await wait_available_async(c)
    async with keydb.client() as k:
        await write_and_wait(k, c)
    await assert_no_classic_fields(node, c, peer_mode)


@pytest.mark.parametrize("peer_mode", [False, True], ids=["plain_replica", "peer_mode"])
async def test_info_and_metrics_have_no_classic_fields_between_dfly_nodes(
    df_factory: DflyInstanceFactory, tmp_path, peer_mode
):
    """A DFLY-to-DFLY link, plain or between two peer nodes, shows no classic field on either
    node, and neither node exports a classic series.

    Falsifying: rendering for every link that is not marked classic shows the zeros here.
    """
    args = {"active_replica": "true"} if peer_mode else {}
    master = df_factory.create(proactor_threads=2, dir=str(tmp_path / "master"), **args)
    replica = df_factory.create(proactor_threads=2, dir=str(tmp_path / "replica"), **args)
    df_factory.start_all([master, replica])
    cm, cr = master.client(), replica.client()
    assert await cr.execute_command(f"REPLICAOF localhost {master.port}") == "OK"
    if peer_mode:
        await wait_for_peer_link(cr)
    else:
        await wait_available_async(cr)
    await write_and_wait(cm, cr)
    await assert_no_classic_fields(replica, cr, peer_mode)
    await assert_no_classic_fields(master, cm, False, has_master=False)


async def test_active_replica_boot_warning_names_keydb_drops(
    df_factory: DflyInstanceFactory, tmp_path
):
    """An --active_replica node warns at boot that KeyDB member TTLs and cron jobs are dropped on
    onboarding, among its known limitations; a node that is not active prints no such warning.

    Falsifying: leaving the sentence out of the warning (multi_master.cc) fails the first check.
    """
    active = df_factory.create(
        proactor_threads=2, dir=str(tmp_path / "active"), active_replica="true"
    )
    plain = df_factory.create(proactor_threads=2, dir=str(tmp_path / "plain"))
    df_factory.start_all([active, plain])
    await wait_available_async(active.client())
    await wait_available_async(plain.client())
    active.stop()
    plain.stop()
    sentence = r"KeyDB member TTLs and cron jobs are dropped on onboarding"
    assert active.find_in_logs(sentence), "the boot limitations warning does not name the drops"
    assert not plain.find_in_logs(sentence)
