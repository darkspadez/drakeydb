"""Interop tests for a drakeydb node replicating from a real KeyDB master (v6.3.4), and tests that
script a classic master instead (fake_classic_master.py).

The KeyDB tests (marked `keydb`) need a keydb-server binary: $KEYDB_SERVER_PATH, else `keydb-server`
on PATH (see docs/build-from-source.md). Without one they skip, and with KEYDB_REQUIRED=1 they fail
instead, as they do whenever $KEYDB_SERVER_PATH is set but is not an executable. The scripted-master
tests need no KeyDB.
"""

import asyncio
import contextlib
import functools
import json
import logging
import os
import random
import re
import shutil
import statistics
import subprocess
import time

import psutil
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
from .instance import DflyInstanceFactory, RedisServer
from .replication_utils import get_metric_value
from .utility import assert_eventually, skip_if_not_in_github, wait_available_async

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
# The uuid a scripted master answers `REPLCONF UUID` with, which a peer node needs of its master.
SCRIPTED_PEER_UUID = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"

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


@pytest.mark.parametrize("scenario", ["raw", "in_envelope"])
async def test_classic_stream_info_command_does_not_abort(
    df_factory: DflyInstanceFactory, tmp_path, scenario
):
    """An `INFO` in the replication stream, raw or inside an RREPLAY envelope, used to kill the
    replica (ISSUE-REGISTER U-15): ServerFamily::Info asks its connection whether it is privileged
    and for its TLS certificate, and a replicated apply has no connection (a null `conn()`). It is
    an error reply now, which the stream discards: the replica stays up, the offset the master
    settles on is the exact length of the stream, and the commands around it apply. Inside an
    envelope the failure also shows as a `classic_apply_errors`.

    Falsifying: with the guard removed the replica process dies as it reads the command (SIGSEGV
    or SIGABRT out of ServerFamily::Info), so `a` or `b` never arrives.
    """
    info_command = resp_command("INFO") if scenario == "raw" else rreplay("INFO")
    stream = SET_A + info_command + SET_B
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
            assert await c.get("b") == "2"

        await applied()
        assert await c.get("a") == "1"
        expected = SYNC_OFFSET + len(stream)
        settled = await master.wait_for_settled_ack(since=len(master.ack_offsets))
        assert settled == expected, master.ack_offsets
        assert max(master.ack_offsets) == expected, master.ack_offsets
        assert master.connection_count == 1, "the replica reconnected"
        info = await c.info("replication")
        assert info["master_link_status"] == "up", info
        if scenario == "in_envelope":
            assert info["classic_apply_errors"] == 1, info
            assert info["rreplay_unwrapped"] == 1, info

    node.stop()
    if scenario == "in_envelope":
        assert node.find_in_logs(r"did not apply and is skipped: INFO: .*No connection")


EVAL_OF_CONNECTION_COMMANDS = [
    "return redis.call('INFO')",
    "return redis.call('HELLO', '3')",
    "return redis.call('QUIT')",
]


@pytest.mark.parametrize("scenario", ["raw", "in_envelope"])
async def test_classic_stream_eval_of_a_connection_command_does_not_abort(
    df_factory: DflyInstanceFactory, tmp_path, scenario
):
    """The guards of ISSUE-REGISTER U-15 also cover a script: `redis.call` runs its command on the
    context of the EVAL, which in a classic stream has no connection either, and INFO, HELLO and
    QUIT are not NOSCRIPT. An EVAL of INFO or HELLO is an error the stream discards (a script error
    that carries the handler's `No connection`), an EVAL of QUIT has nothing to close and applies,
    and the replica stays up with the offset exact. Inside an envelope the two failed scripts also
    show as `classic_apply_errors`.

    Falsifying: with the INFO or HELLO guard removed the replica process dies as it runs the
    script (SIGSEGV out of ServerFamily::Info or Hello), so `b` never arrives.
    """
    wrap = resp_command if scenario == "raw" else rreplay
    stream = SET_A
    for body in EVAL_OF_CONNECTION_COMMANDS:
        stream += wrap("EVAL", body, "0")
    stream += SET_B
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
            assert await c.get("b") == "2"

        await applied()
        assert await c.get("a") == "1"
        expected = SYNC_OFFSET + len(stream)
        settled = await master.wait_for_settled_ack(since=len(master.ack_offsets))
        assert settled == expected, master.ack_offsets
        assert max(master.ack_offsets) == expected, master.ack_offsets
        assert master.connection_count == 1, "the replica reconnected"
        info = await c.info("replication")
        assert info["master_link_status"] == "up", info
        if scenario == "in_envelope":
            assert info["classic_apply_errors"] == 2, info
            assert info["rreplay_unwrapped"] == len(EVAL_OF_CONNECTION_COMMANDS), info

    node.stop()
    if scenario == "in_envelope":
        assert node.find_in_logs(r"did not apply and is skipped: EVAL: .*No connection")


# `SORT <source that cannot be sorted> STORE <dst>`, with `dst` on another shard than the source
# (the replica runs two shards; the pairs below were checked to split, and the falsification below
# shows it): the commands that set the scene, the SORT, and what `dst` holds once it ran (None:
# deleted).
SORT_STORE_SCENARIOS = {
    "missing_source": (
        [("RPUSH", "d1", "stale")],
        ("SORT", "nosuch", "STORE", "d1"),
        "d1",
        None,
    ),
    "wrong_type_source": (
        [("SET", "s", "x"), ("RPUSH", "d1", "kept")],
        ("SORT", "s", "STORE", "d1"),
        "d1",
        ["kept"],
    ),
    "non_numeric_source": (
        [("RPUSH", "l", "a", "b"), ("RPUSH", "d2", "kept")],
        ("SORT", "l", "STORE", "d2"),
        "d2",
        ["kept"],
    ),
}


@pytest.mark.parametrize("framing", ["raw", "in_envelope"])
@pytest.mark.parametrize("scenario", list(SORT_STORE_SCENARIOS))
async def test_classic_stream_sort_store_of_an_unsortable_source_does_not_abort(
    df_factory: DflyInstanceFactory, tmp_path, scenario, framing
):
    """A `SORT` with a source that does not exist, has the wrong type or holds non-numbers, and a
    `STORE` destination on another shard, killed the process that ran it (ISSUE-REGISTER D-33, a
    P4-0 regression: the shard callback returned the failure as the hop's status, and a hop of a
    multi-shard transaction CHECK-fails on anything but OK). Any classic master's stream can carry
    it, raw or inside an RREPLAY envelope. It is a command now like any other: a missing source
    deletes the destination and replies 0, as in Redis and KeyDB; the other two are errors the
    stream discards with the destination untouched. The replica stays up, the commands around the
    SORT apply, and the offset the master settles on is the exact length of the stream.

    Falsifying: with the fetch callback returning the failure as the hop's status again, the
    replica process dies as it applies the SORT (a CHECK in Transaction::RunCallback), so `b`
    never arrives; with the missing-source STORE back to an empty-array reply, `d1` survives.
    """
    setup, sort, dst, expected = SORT_STORE_SCENARIOS[scenario]
    wrap = resp_command if framing == "raw" else rreplay
    commands = [*setup, sort, ("SET", "b", "2")]
    stream = b"".join(wrap(*command) for command in commands)
    async with FakeClassicMaster() as master:
        master.script_psync(
            diskless_full_sync(offset=SYNC_OFFSET), stream=stream, stream_delay=SECOND_WRITE_DELAY_S
        )
        node = df_factory.create(
            proactor_threads=2,
            num_shards=2,
            dir=str(tmp_path / "df"),
            replication_acks_interval=100,
        )
        node.start()
        c = node.client()
        assert await c.execute_command(f"REPLICAOF 127.0.0.1 {master.port}") == "OK"

        @assert_eventually(times=100)
        @retry_while_loading
        async def applied():
            assert node.proc.poll() is None, f"the replica process died with {node.proc.poll()}"
            assert await c.get("b") == "2"

        await applied()
        if expected is None:
            assert await c.exists(dst) == 0
        else:
            assert await c.lrange(dst, 0, -1) == expected
        expected_offset = SYNC_OFFSET + len(stream)
        settled = await master.wait_for_settled_ack(since=len(master.ack_offsets))
        assert settled == expected_offset, master.ack_offsets
        assert max(master.ack_offsets) == expected_offset, master.ack_offsets
        assert master.connection_count == 1, "the replica reconnected"
        info = await c.info("replication")
        assert info["master_link_status"] == "up", info
        if framing == "in_envelope":
            assert info["rreplay_unwrapped"] == len(commands), info
            # (the field is not shown while it is 0)
            assert info.get("classic_apply_errors", 0) == (0 if expected is None else 1), info


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
            master.script_uuid(SCRIPTED_PEER_UUID)
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
            master.script_uuid(SCRIPTED_PEER_UUID)
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


ZERO_COUNTERS = dict.fromkeys(CLASSIC_COUNTERS, 0)


async def wait_for_synced_link(node, c, peer_mode):
    """Waits until the link to a scripted master (a diskless full sync at SYNC_OFFSET) is up and its
    full sync looks over, so that what INFO then says is settled.

    `link_status` is `up` as soon as the socket is, before `+FULLRESYNC` is parsed, so that alone
    is not enough: the link must also not be syncing, and show the offset the sync ended at.

    The two branches do not prove the same. The plain block's `slave_repl_offset` is printed only
    once the sync is done (`R_SYNC_OK`), so that branch does wait for the RDB. A peer line's
    `repl_offset` is the stream offset `repl_offs_`, which `Replica::ParseReplicationHeader` sets as
    it parses `+FULLRESYNC`: before the `$EOF:` line is read and before `R_SYNCING` is set. So the
    peer branch can pass before the RDB has loaded. That is harmless here only because the scripted
    master writes the whole full sync at once; the check does not show that the RDB is in."""

    @assert_eventually(times=100)
    @retry_while_loading
    async def synced():
        assert node.proc.poll() is None, f"the replica process died with {node.proc.poll()}"
        info = await c.info("replication")
        if peer_mode:
            link = info["master0"]
            assert link["link_status"] == "up" and link["sync_in_progress"] == 0, info
            assert link.get("repl_offset") == SYNC_OFFSET, info
        else:
            assert info["master_link_status"] == "up", info
            assert info["master_sync_in_progress"] == 0, info
            assert info.get("slave_repl_offset") == SYNC_OFFSET, info

    await synced()


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
            master.script_uuid(SCRIPTED_PEER_UUID)
        master.script_psync(diskless_full_sync(offset=SYNC_OFFSET))
        node, c = await attach_scripted_master(df_factory, tmp_path, master, peer_mode)
        await wait_for_synced_link(node, c, peer_mode)

        info = await c.info("replication")
        assert classic_fields(info, peer_mode) == ZERO_COUNTERS, info
        if peer_mode:
            assert info["master0"]["repl_offset"] == SYNC_OFFSET, info
        assert classic_series(await node.metrics()) == ZERO_COUNTERS


@pytest.mark.parametrize("peer_mode", [False, True], ids=["plain_replica", "peer_mode"])
@pytest.mark.parametrize("outage", ["master_gone", "greet_fails"])
async def test_scripted_active_master_down_link_keeps_zero_counters(
    df_factory: DflyInstanceFactory, tmp_path, peer_mode, outage
):
    """The link to an active KeyDB is shown whatever its state: while it is down, or reconnecting
    with a handshake that does not complete, INFO still shows its six zeros (and in peer mode the
    offset), as it did while the link was up.

    `master_gone`: the master closes every connection and its listener, so every reconnect is
    refused and the link is `down`. `greet_fails`: the master stays up but answers every `REPLCONF
    capa` with an error, so each reconnect fails its handshake (no second PSYNC arrives).

    Falsifying: reading the master's `active-replica` answer only while the link is greeted (as
    the first version did) leaves the link without a single field here.
    """
    async with FakeClassicMaster() as master:
        master.script_capa_reply(b"+OK active-replica\r\n")
        if peer_mode:
            master.script_uuid(SCRIPTED_PEER_UUID)
        master.script_psync(diskless_full_sync(offset=SYNC_OFFSET))
        node, c = await attach_scripted_master(df_factory, tmp_path, master, peer_mode)
        await wait_for_synced_link(node, c, peer_mode)
        assert classic_fields(await c.info("replication"), peer_mode) == ZERO_COUNTERS

        connections = master.connection_count
        if outage == "master_gone":
            await master.close()

            @assert_eventually(times=100)
            async def down():
                info = await c.info("replication")
                link = info["master0"] if peer_mode else info
                assert link["link_status" if peer_mode else "master_link_status"] == "down", info

            await down()
        else:
            master.script_capa_reply(b"-ERR capa refused\r\n")
            await master.drop_connections()
            # Two attempts after the drop: the first handshake failed before the second began.
            assert await master.wait_for_connections(connections + 2) >= connections + 2
            assert len(master.psync_requests) == 1, master.psync_requests

        assert node.proc.poll() is None, f"the replica process died with {node.proc.poll()}"
        info = await c.info("replication")
        assert classic_fields(info, peer_mode) == ZERO_COUNTERS, info
        if peer_mode:
            assert info["master0"]["repl_offset"] == SYNC_OFFSET, info


@pytest.mark.parametrize("peer_mode", [False, True], ids=["plain_replica", "peer_mode"])
async def test_scripted_active_master_series_outlive_the_link(
    df_factory: DflyInstanceFactory, tmp_path, peer_mode
):
    """Once a master that says `active-replica` has completed a handshake, /metrics exports the six
    classic series (zeros included) for the rest of the process's life: removing the link does not
    take them away, whatever the links of the node say afterwards (owner decision 25).

    Falsifying: deciding it per scrape from the node's links (as the first version did) leaves no
    series once the link is gone.
    """
    async with FakeClassicMaster() as master:
        master.script_capa_reply(b"+OK active-replica\r\n")
        if peer_mode:
            master.script_uuid(SCRIPTED_PEER_UUID)
        master.script_psync(diskless_full_sync(offset=SYNC_OFFSET))
        node, c = await attach_scripted_master(df_factory, tmp_path, master, peer_mode)
        await wait_for_synced_link(node, c, peer_mode)
        assert classic_series(await node.metrics()) == ZERO_COUNTERS

        if peer_mode:
            assert await c.execute_command(f"REPLICAOF REMOVE 127.0.0.1 {master.port}") == "OK"
            assert (await c.info("replication"))["connected_masters"] == 0
        else:
            assert await c.execute_command("REPLICAOF NO ONE") == "OK"
            assert (await c.info("replication"))["role"] == "master"
        assert classic_series(await node.metrics()) == ZERO_COUNTERS


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


# Keys that expire while a plain replica is attached: EXPIRING_KEYS with a TTL that is short but
# leaves the replica time to receive them, one without a TTL and one with a long one.
EXPIRING_KEYS = 50
EXPIRE_TTL_MS = 3000


async def write_expiring_keys(client):
    for i in range(EXPIRING_KEYS):
        await client.set(f"exp:{i}", "v", px=EXPIRE_TTL_MS)
    await client.set("keep", "v")
    await client.set("long", "v", ex=1000)


@assert_eventually(times=100)
@retry_while_loading
async def assert_dbsize(c, expected):
    """Polls DBSIZE, which counts entries and reads no key: asking for an expired key would delete
    it (a replica with --replica_delete_expired, KeyDB and Redis all do), and so hide what the
    sweep did or did not do."""
    assert await c.dbsize() == expected


@pytest.mark.keydb
@pytest.mark.parametrize("attach", ["plain", "with_slot_range", "boot_replicaof"])
async def test_plain_replica_of_active_keydb_expires_keys(
    df_factory: DflyInstanceFactory, keydb_server_factory, tmp_path, attach
):
    """A plain replica of an active KeyDB expires keys itself (decision 13, spec D-9): KeyDB, told
    by `REPLCONF capa activeExpire` that its replica does, streams no DEL for a key that expired.
    The replica's DBSIZE, polled without ever reading a key, goes from the full keyspace to the two
    keys that did not expire, after KeyDB's own has. The test harness starts the replica with
    --replica_delete_expired=false, so the read-path gate is open only because of the link.

    `with_slot_range` attaches with `REPLICAOF <host> <port> 0 16383`: the node's main link then
    has a slot range, and still drives the flag (the marker, not `!slot_range_`, decides).
    `boot_replicaof` attaches with the `--replicaof` flag: the fiber greets that link itself, so
    the flag comes from the in-loop apply of MainReplicationFb, which no `REPLICAOF` command
    reaches for its first greet (Start() greets there, and the fiber applies it at its top).

    Falsifying: with the shard flag never set, the replica's DBSIZE stays at the full keyspace
    while KeyDB's is down to two. With a link that only counts when it has no slot range, the
    `with_slot_range` case fails the same way; without the in-loop apply, `boot_replicaof` does.
    """
    keydb = keydb_server_factory(active_replica=True)
    flags = {"replicaof": f"localhost:{keydb.port}"} if attach == "boot_replicaof" else {}
    node = df_factory.create(proactor_threads=2, dir=str(tmp_path / "df"), **flags)
    node.start()
    c = node.client()
    if attach != "boot_replicaof":
        slots = " 0 16383" if attach == "with_slot_range" else ""
        assert await c.execute_command(f"REPLICAOF localhost {keydb.port}{slots}") == "OK"
    await wait_available_async(c)

    async with keydb.client() as k:
        await write_expiring_keys(k)
        await assert_dbsize(c, EXPIRING_KEYS + 2)  # they did arrive, before any of them is due

        # KeyDB drops its own expired keys, and the replica is expected to do the same.
        await assert_dbsize(k, 2)
        await assert_dbsize(c, 2)
        assert await c.get("keep") == "v"
        assert await c.get("long") == "v"
        info = await c.info("replication")
        assert info["role"] == "slave" and info["master_link_status"] == "up", info


async def test_plain_replica_of_plain_redis_never_expires_on_its_own(
    df_factory: DflyInstanceFactory, port_picker, df_log_dir, tmp_path
):
    """The control of the test above: a replica of a master that does not say `active-replica` is
    never made to expire keys on its own. A Redis master with its active expiry turned off
    (`DEBUG SET-ACTIVE-EXPIRE 0`, which needs `enable-debug-command yes`, hence a Redis 7 of its
    own) propagates no DEL either, so the replica's DBSIZE stays at the full keyspace well past the
    TTL; reading an expired key on the master, which expires it lazily and propagates the DEL,
    then brings it down by exactly that key, so the link was live all along. No key is read on the
    replica, nor on the master before that.

    Falsifying: with the shard flag set for any master (or `activeExpire` taken to mean it is),
    the replica's DBSIZE falls to two while Redis still holds every key.
    """
    master = RedisServer(port_picker.get_available_port(), log_dir=df_log_dir)
    try:
        master.start(redis7=True, **{"enable-debug-command": "yes"})
    except FileNotFoundError:
        skip_if_not_in_github()
        raise
    try:
        async with aioredis.Redis(port=master.port, decode_responses=True) as r:

            for _ in range(100):
                try:
                    await r.ping()
                    break
                except redis.exceptions.ConnectionError:
                    await asyncio.sleep(0.1)
            else:
                pytest.fail("the Redis master did not come up")
            assert await r.execute_command("DEBUG", "SET-ACTIVE-EXPIRE", "0") == "OK"

            node = df_factory.create(proactor_threads=2, dir=str(tmp_path / "df"))
            node.start()
            c = node.client()
            assert await c.execute_command(f"REPLICAOF localhost {master.port}") == "OK"
            await wait_available_async(c)

            await write_expiring_keys(r)
            await assert_dbsize(c, EXPIRING_KEYS + 2)
            await asyncio.sleep(EXPIRE_TTL_MS / 1000 + 2)  # well past every TTL
            assert await r.dbsize() == EXPIRING_KEYS + 2, "Redis expired a key it was not asked to"
            assert await c.dbsize() == EXPIRING_KEYS + 2

            assert await r.get("exp:0") is None  # lazy expiry on the master, which propagates a DEL
            await assert_dbsize(c, EXPIRING_KEYS + 1)
    finally:
        master.stop()


async def test_replica_active_expiry_follows_the_master_of_every_reconnect(
    df_factory: DflyInstanceFactory, tmp_path
):
    """The flag is decided again by every successful `Greet()`, the one a reconnect makes in the
    replication fiber included (MainReplicationFb's in-loop apply): the master that answers
    `active-replica`, the same address then answering plain `+OK`, then `active-replica` again,
    each time with a full resync whose stream sets keys with a short TTL.

    The replica's DBSIZE, never reading a key, goes down to the one key without a TTL while the
    master says `active-replica`, stays at the full keyspace well past the TTL once it does not
    (the flag was cleared), and goes down again when it says it once more (it was set again).

    Falsifying: without the apply after a reconnect's `Greet()`, the middle phase fails (the flag
    stays on from the first); applying `false` there instead, the last phase fails.
    """
    ttl_ms = 3000

    async with FakeClassicMaster() as master:

        def serve(offset, prefix, active):
            """What the next handshake answers and the next full sync's stream sets."""
            master.script_capa_reply(b"+OK active-replica\r\n" if active else b"+OK\r\n")
            stream = b"".join(
                resp_command("SET", f"{prefix}:{i}", "v", "PX", str(ttl_ms))
                for i in range(EXPIRING_KEYS)
            ) + resp_command("SET", f"{prefix}:keep", "v")
            master.script_psync(diskless_full_sync(offset=offset), stream=stream)

        async def reconnect_to(offset, prefix, active):
            """Makes the master drop the link and serve `serve(...)` to the reconnect."""
            serve(offset, prefix, active)
            psyncs = len(master.psync_requests)
            await master.drop_connections()

            @assert_eventually(times=200)
            async def resynced():
                assert len(master.psync_requests) > psyncs

            await resynced()

        serve(1000, "a", active=True)
        node = df_factory.create(proactor_threads=2, dir=str(tmp_path / "df"))
        node.start()
        c = node.client()
        assert await c.execute_command(f"REPLICAOF 127.0.0.1 {master.port}") == "OK"
        await assert_dbsize(c, EXPIRING_KEYS + 1)
        await assert_dbsize(c, 1)  # active: the keys expire on the replica

        await reconnect_to(2000, "b", active=False)
        await assert_dbsize(c, EXPIRING_KEYS + 1)
        await asyncio.sleep(ttl_ms / 1000 + 2)  # well past every TTL
        assert await c.dbsize() == EXPIRING_KEYS + 1, "a replica of a plain master expired keys"

        await reconnect_to(3000, "c", active=True)

        @assert_eventually(times=200)
        @retry_while_loading
        async def resynced_keys_arrived():
            # The last key of the stream: the full sync has flushed the keys of the phase before
            # (which are as many, so the size alone cannot tell) and the new ones are in.
            assert await c.exists("c:keep") == 1

        await resynced_keys_arrived()
        await assert_dbsize(c, EXPIRING_KEYS + 1)
        await assert_dbsize(c, 1)  # and the next greet sets it again
        assert master.connection_count == 3, "one connection per phase"


def capa_requests(master):
    """The REPLCONF capa requests the scripted master got, in order."""
    return [r for r in master.requests if r[:2] == ["REPLCONF", "capa"]]


@pytest.mark.parametrize("peer_mode", [False, True], ids=["plain_replica", "peer_mode"])
@pytest.mark.parametrize(
    "reveals,reveal_request",
    [
        (None, None),
        ("both", ["REPLCONF", "capa", "eof", "capa", "psync2"]),
        ("eof", ["REPLCONF", "capa", "eof", "capa", "psync2"]),
        ("dragonfly", ["REPLCONF", "capa", "dragonfly"]),
    ],
    ids=["never", "both_sites", "first_site_only", "second_site_only"],
)
async def test_greet_sends_capa_active_expire_only_after_active_replica_reply(
    df_factory: DflyInstanceFactory, tmp_path, peer_mode, reveals, reveal_request
):
    """A master that never answers `active-replica` gets the handshake it always got: no `REPLCONF
    capa activeExpire` (spec D-2, byte identity for stock masters). One that does, at either of the
    two `REPLCONF capa` replies, gets it exactly once per handshake, as its own command right after
    the request whose reply revealed it. Peer links send it too.

    Falsifying: sending it unconditionally fails `never`; sending it from only one site fails the
    other site's case; sending it at every capa reply that says `active-replica` fails the
    `both_sites` count.
    """
    async with FakeClassicMaster() as master:
        if reveals == "both":
            master.script_capa_reply(b"+OK active-replica\r\n")
        elif reveals:
            master.script_capa_reply(b"+OK active-replica\r\n", only_for=reveals)
        if peer_mode:
            master.script_uuid(SCRIPTED_PEER_UUID)
        master.script_psync(diskless_full_sync(offset=SYNC_OFFSET))
        node, c = await attach_scripted_master(df_factory, tmp_path, master, peer_mode)

        @assert_eventually(times=100)
        async def handshake_is_over():
            assert node.proc.poll() is None, f"the replica process died with {node.proc.poll()}"
            assert master.psync_requests

        await handshake_is_over()
        active_expire = ["REPLCONF", "capa", "activeExpire"]
        capas = capa_requests(master)
        assert master.connection_count == 1, "the replica reconnected"
        if reveal_request is None:
            assert active_expire not in capas, capas
        else:
            assert capas.count(active_expire) == 1, capas
            at = master.requests.index(reveal_request)
            assert master.requests[at + 1] == active_expire, master.requests


LENIENT_ACTIVE_EXPIRE_REPLIES = {
    "error": b"-ERR Unrecognized capability\r\n",
    "two_element_array": b"*2\r\n+OK\r\n+active-replica\r\n",
    "integer": b":1\r\n",
}


@pytest.mark.parametrize("peer_mode", [False, True], ids=["plain_replica", "peer_mode"])
@pytest.mark.parametrize(
    "reply", list(LENIENT_ACTIVE_EXPIRE_REPLIES.values()), ids=list(LENIENT_ACTIVE_EXPIRE_REPLIES)
)
async def test_greet_survives_any_reply_to_capa_active_expire(
    df_factory: DflyInstanceFactory, tmp_path, peer_mode, reply
):
    """The master has already shown it is active, so whatever it answers `REPLCONF capa
    activeExpire` is a warning at most, never a failed handshake (spec D-2): an error, a two-element
    array (read as two response words, not one) and an integer each leave the link up, and the
    requests after it are answered in order, so the reply was consumed whole and nothing desynced.

    Falsifying: a `Greet()` that fails on a reply that is not an OK ends it at the first of them,
    before the PSYNC.
    """
    async with FakeClassicMaster() as master:
        master.script_capa_reply(b"+OK active-replica\r\n")
        master.script_capa_reply(reply, only_for="activeExpire")
        if peer_mode:
            master.script_uuid(SCRIPTED_PEER_UUID)
        master.script_psync(diskless_full_sync(offset=SYNC_OFFSET))
        node, c = await attach_scripted_master(df_factory, tmp_path, master, peer_mode)

        @assert_eventually(times=100)
        async def handshake_is_over():
            assert node.proc.poll() is None, f"the replica process died with {node.proc.poll()}"
            assert master.psync_requests

        await handshake_is_over()
        assert master.connection_count == 1, "the replica reconnected"

        def step(request):
            return " ".join(request[:2]) if request[0] == "REPLCONF" else request[0]

        at = master.requests.index(["REPLCONF", "capa", "activeExpire"])
        expected = ["REPLCONF UUID", "REPLCONF DRAKEY-VERSION"]
        expected += ["REPLCONF PEER"] if peer_mode else []
        expected += ["REPLCONF capa", "PSYNC"]
        assert [step(r) for r in master.requests[at + 1 : at + 1 + len(expected)]] == expected


@pytest.mark.parametrize("attach", ["replicaof_command", "boot_replicaof"])
async def test_dfly_master_that_says_active_replica_never_turns_replica_expiry_on(
    df_factory: DflyInstanceFactory, proxy_factory, tmp_path, attach
):
    """Only a classic master that says `active-replica` drives replica active expiry (the same
    reading INFO has). A Dragonfly master answers `capa dragonfly` with an array, so it can say
    nothing there; a proxy makes it say `+OK active-replica` at the first capa reply instead, which
    sets `master_active_replica_` (and gets `capa activeExpire` sent) while `classic_master_` stays
    false. The replica must not expire keys on its own: the master (with `--hz=0`, so that it
    sweeps nothing, and nothing reads a key) keeps its expired keys, and so does the replica.

    `replicaof_command` is applied by the top of MainReplicationFb, `boot_replicaof` (the
    `--replicaof` flag) by its in-loop apply: each site has its own `classic_master_` check.

    Falsifying: reading `master_active_replica_` alone at the first site fails
    `replicaof_command`, at the in-loop one `boot_replicaof`: the replica's DBSIZE falls to two
    while the master still holds every key.
    """
    master = df_factory.create(proactor_threads=2, hz=0, dir=str(tmp_path / "master"))
    master.start()
    cm = master.client()
    proxy = await proxy_factory(master.port)
    await proxy.override_next_response(b"REPLCONF capa eof", b"+OK active-replica\r\n")
    flags = {"replicaof": f"localhost:{proxy.port}"} if attach == "boot_replicaof" else {}
    replica = df_factory.create(proactor_threads=2, dir=str(tmp_path / "replica"), **flags)
    replica.start()
    cr = replica.client()
    if attach == "replicaof_command":
        assert await cr.execute_command(f"REPLICAOF localhost {proxy.port}") == "OK"
    await wait_available_async(cr)

    await write_expiring_keys(cm)
    await assert_dbsize(cr, EXPIRING_KEYS + 2)
    await asyncio.sleep(EXPIRE_TTL_MS / 1000 + 2)  # well past every TTL
    assert await cm.dbsize() == EXPIRING_KEYS + 2, "the master swept with --hz=0"
    assert await cr.dbsize() == EXPIRING_KEYS + 2, "the replica expired keys on its own"
    info = await cr.info("replication")
    assert info["role"] == "slave" and info["master_link_status"] == "up", info


@pytest.mark.keydb
async def test_keydb_is_told_the_replica_expires_keys_itself(
    df_factory: DflyInstanceFactory, keydb_server_factory, tmp_path
):
    """A real active KeyDB logs `Warning: replica ... does not support active expiration` once a
    replica that never sent `capa activeExpire` comes online. After the full sync of a plain
    replica of ours, its log has the line for a finished synchronization and not that warning.

    Falsifying: with `REPLCONF capa activeExpire` not sent, the warning is in the log.
    """
    keydb = keydb_server_factory(active_replica=True)
    node, c = await attach_plain_replica(df_factory, tmp_path, keydb)

    @assert_eventually(times=100)
    async def synchronized():
        assert re.search(r"Synchronization with replica \S+ succeeded", keydb.log_text())

    await synchronized()
    log = keydb.log_text()
    assert "does not support active expiration" not in log, log


async def test_fake_master_send_stream_lands_behind_the_scripted_stream():
    """`FakeClassicMaster.send_stream()` writes behind the stream `script_psync()` scripted: while
    that stream is still waiting out its `stream_delay`, no replica is in the stream yet, and the
    call is refused instead of landing ahead of it. Once the scripted stream is written, what
    `send_stream()` sends follows it. No drakeydb is involved: the client is a bare socket.

    Falsifying: with the stream writer set before the delay, the first `send_stream()` is accepted
    and its bytes reach the client ahead of the scripted ones.
    """
    async with FakeClassicMaster() as master:
        master.script_psync(b"+REPLY\r\n", stream=b"SCRIPTED", stream_delay=0.5)
        reader, writer = await asyncio.open_connection("127.0.0.1", master.port)
        try:
            writer.write(b"PSYNC ? -1\r\n")
            await writer.drain()
            assert await reader.readline() == b"+REPLY\r\n"

            with pytest.raises(AssertionError, match="no replica is in the replication stream"):
                await master.send_stream(b"EARLY")

            assert await asyncio.wait_for(reader.readexactly(8), 5) == b"SCRIPTED"
            await master.send_stream(b"LATE")
            assert await asyncio.wait_for(reader.readexactly(4), 5) == b"LATE"
        finally:
            writer.close()


# The window of owner decision 24 (option A), scripted with the commands an active KeyDB streams:
# every TTL is absolute (`SET k v PXAT <ms>`, `PEXPIREAT`) and `INCR` is `INCRBY c 1`, as
# tests/dragonfly/data/README.md shows. A key is due at `deadline`, WINDOW_TTL_MS after it is
# streamed (long enough that the SET is applied first, however loaded the box), and the late
# commands are streamed once the clock is WINDOW_PAST_MS past it. The replica's clock is this
# host's, so they are applied at least that far past the deadline whatever the load: a busy box
# only delays the apply, which moves it further past.
WINDOW_TTL_MS = 3000
WINDOW_PAST_MS = 1000


def now_ms():
    return time.time_ns() // 1_000_000


async def sleep_until_ms(ms):
    await asyncio.sleep(max(0, ms - now_ms()) / 1000)


def keydb_mvcc(ms):
    """The mvcc an active KeyDB mints at `ms` on its clock: the milliseconds above the 20 low bits
    of a counter (`MVCC_MS_SHIFT`, `server.h:960`; `incrementMvccTstamp`). The envelopes of
    tests/dragonfly/data/README.md carry the very `mstime()` the same command's `PXAT` was
    computed from (the mvcc's milliseconds equal the `PXAT` minus the relative TTL asked for)."""
    return ms << 20


def stream_command(framing, *command, master_ms=0):
    """`command` as the master streams it: raw, or in the RREPLAY envelope an active KeyDB wraps it
    in, whose mvcc says the master ran it at `master_ms` on its clock."""
    if framing == "raw":
        return resp_command(*command)
    return rreplay(*command, mvcc=keydb_mvcc(master_ms))


async def attach_to_stream_live(df_factory, tmp_path, master, sweep):
    """A plain replica of a scripted active KeyDB, its full sync done and its stream open for
    `master.send_stream()`. `sweep="access_only"` starts it with --hz=0: no heartbeat reaps
    anything, so the only way a due key goes is an access to it."""
    master.script_capa_reply(b"+OK active-replica\r\n")
    master.script_psync(diskless_full_sync(offset=SYNC_OFFSET))
    flags = {"hz": 0} if sweep == "access_only" else {}
    node = df_factory.create(proactor_threads=2, dir=str(tmp_path / "df"), **flags)
    node.start()
    c = node.client()
    assert await c.execute_command(f"REPLICAOF 127.0.0.1 {master.port}") == "OK"
    await wait_for_synced_link(node, c, peer_mode=False)
    return node, c


async def stream_and_wait_applied(master, c, marker, *commands):
    """Streams `commands` and then `SET <marker> 1`, and waits for the marker on the replica: a
    stream applies in order, so every command before it is applied by then. Nothing else is read:
    reading a due key on the replica would delete it, and test something else."""
    await master.send_stream(b"".join(commands) + resp_command("SET", marker, "1"))

    @assert_eventually(times=300)
    async def applied():
        assert await c.get(marker) == "1"

    await applied()


async def assert_only_an_access_can_delete(c, key):
    """With --hz=0 nothing sweeps, so `key` (a due key and the first marker) must still be in the
    table: DBSIZE counts entries and reads none."""
    size = await c.dbsize()
    assert size == 2, f"{key} and a marker should be there, DBSIZE is {size}: was the SET late?"


@pytest.mark.parametrize("sweep", ["sweep", "access_only"])
@pytest.mark.parametrize("framing", ["raw", "envelope"])
async def test_plain_replica_of_active_keydb_loses_a_ttl_refresh_that_arrives_after_the_deadline(
    df_factory: DflyInstanceFactory, tmp_path, framing, sweep
):
    """Pins owner decision 24, option A, as the spec D-9 documents it. The master set `k` with a
    deadline 3 s ahead and, 300 ms before that deadline, streamed `PEXPIREAT k <now + 60 s>`; the
    replica applies that a second after the deadline. The replica has deleted `k` by then (the
    sweep, or, with --hz=0, the access the refresh makes), so the refresh finds no key, is a no-op,
    and `k` is gone for good, while the master still holds it. It stays gone.

    `access_only` is the case that pins "any access deletes": no sweep runs, and the test checks
    that `k` is still in the table, due, before the refresh. A replica that only swept (option C)
    would pass the `sweep` cases and fail these. `framing` is raw or RREPLAY, whose mvcc carries
    the master's own time: 300 ms before the deadline.

    Decision 27 (plan Task 2.8) makes an enveloped command run at its author's time, so the
    `envelope-access_only` case flips: the refresh runs at 300 ms before the deadline, `k` is still
    in the table, and survives with a TTL of about 60 s. The `raw` cases do not (a raw command
    keeps the replica's clock), and neither does `envelope-sweep`: the sweep keeps the replica's
    clock too and deletes `k` at its own deadline, a second before the refresh arrives, which no
    apply clock can undo.

    Falsifying: a replica that serves a due key as live (`ExpireIfNeeded` returns it) applies the
    refresh to it: `k` is still there.
    """
    async with FakeClassicMaster() as master:
        node, c = await attach_to_stream_live(df_factory, tmp_path, master, sweep)
        t0 = now_ms()
        deadline = t0 + WINDOW_TTL_MS
        set_k = stream_command(framing, "SET", "k", "v", "PXAT", deadline, master_ms=t0)
        await stream_and_wait_applied(master, c, "marker:set", set_k)
        await sleep_until_ms(deadline + WINDOW_PAST_MS)
        if sweep == "access_only":
            await assert_only_an_access_can_delete(c, "k")

        refresh = stream_command(
            framing, "PEXPIREAT", "k", now_ms() + 60_000, master_ms=deadline - 300
        )
        await stream_and_wait_applied(master, c, "marker:refresh", refresh)
        for _ in range(10):  # for a second
            assert await c.exists("k") == 0, "k survived a refresh that arrived after its deadline"
            await asyncio.sleep(0.1)
        assert await c.dbsize() == 2  # the two markers
        assert master.connection_count == 1, "the replica reconnected"


COUNTER_SCENARIOS = {
    # What the replica's clock alone decides: no author time on a raw command.
    "raw": ("raw", 200),
    # The master ran INCR 200 ms after the deadline: it deleted `c` lazily and replied 1.
    "envelope_applied_after_the_deadline": ("envelope", 200),
    # The master ran INCR 300 ms before the deadline: it replied 6, and holds 6.
    "envelope_applied_before_the_deadline": ("envelope", -300),
}


@pytest.mark.parametrize("sweep", ["sweep", "access_only"])
@pytest.mark.parametrize("scenario", list(COUNTER_SCENARIOS))
async def test_plain_replica_of_active_keydb_recomputes_a_counter_from_nothing_after_the_deadline(
    df_factory: DflyInstanceFactory, tmp_path, scenario, sweep
):
    """Pins owner decision 24, option A, for a read-then-write command. The master set `c` to 5
    with a deadline 3 s ahead, and later streamed `INCRBY c 1` and `PEXPIREAT c <now + 60 s>`; the
    replica applies them a second after the deadline and computes from nothing: `c` is "1", with a
    TTL of about 60 s.

    In the first two scenarios that is what the master holds too: it ran the `INCR` after the
    deadline, deleted `c` lazily (an active KeyDB propagates no DEL for it) and replied 1. A
    replica that served the stale `c` (KeyDB's own plain replica, option B, or sweep only, option
    C) would apply it to 5 and hold 6. In the third the master ran it before the deadline and holds
    6: the documented window, and the replica holds 1. That is the scenario decision 27 (plan Task
    2.8) flips, in its `access_only` case: an enveloped command runs at its author's time, so the
    replica holds 6 there. The first two keep their "1" under it (the raw one has no author time,
    the second's is after the deadline), and so does `sweep` of the third: the sweep keeps the
    replica's clock and has deleted `c` at its own deadline, before the late commands arrive.

    `access_only` starts the replica with --hz=0 and checks that `c` is still in the table, due,
    before the late commands: only an access can delete it, which the `sweep` cases cannot show.

    Falsifying: a replica that serves a due key as live reads "6" in every scenario.
    """
    framing, master_offset_ms = COUNTER_SCENARIOS[scenario]
    async with FakeClassicMaster() as master:
        node, c = await attach_to_stream_live(df_factory, tmp_path, master, sweep)
        t0 = now_ms()
        deadline = t0 + WINDOW_TTL_MS
        set_c = stream_command(framing, "SET", "c", 5, "PXAT", deadline, master_ms=t0)
        await stream_and_wait_applied(master, c, "marker:set", set_c)
        await sleep_until_ms(deadline + WINDOW_PAST_MS)
        if sweep == "access_only":
            await assert_only_an_access_can_delete(c, "c")

        at = deadline + master_offset_ms
        expire_at = now_ms() + 60_000
        await stream_and_wait_applied(
            master,
            c,
            "marker:late",
            stream_command(framing, "INCRBY", "c", 1, master_ms=at),
            stream_command(framing, "PEXPIREAT", "c", expire_at, master_ms=at + 1),
        )
        value = await c.get("c")
        assert value == "1", f"c is {value!r}: the replica computed it from a due key"
        assert 0 < await c.pttl("c") <= 60_000
        assert master.connection_count == 1, "the replica reconnected"


@pytest.mark.parametrize("sweep", ["sweep", "access_only"])
async def test_plain_replica_of_active_keydb_keeps_a_ttl_less_orphan_of_a_ttl_keeping_write(
    df_factory: DflyInstanceFactory, tmp_path, sweep
):
    """Pins the permanent orphan of owner decision 24, option A, which decision 31 keeps as a known
    interim limitation of P7-1 (spec D-9, ISSUE-REGISTER D-32). The master set `c` (a string), `h`
    (a hash) and `kt` (a string) to expire at a deadline 3 s ahead. Later, 300 ms before that
    deadline, it ran a write that keeps the TTL on each of them (`INCR c`, streamed as `INCRBY c 1`;
    `HSET h g 2`; `SET kt new KEEPTTL`), so on the master all three still expire at the deadline,
    and an active KeyDB never streams a DEL for that. The replica applies the writes a second after
    the deadline, when it has deleted the keys (the sweep did, or with --hz=0 the access that
    applies the write does): each write finds no key and creates one, with no TTL. Nothing ever
    removes it. The test reads `PTTL == -1` on all three, and that they are all still there 2 s
    later with the sweep running, as DBSIZE (which reads no key) shows by counting them. `swept` is
    the control for that: a key with the same deadline that nothing touches, which the sweep has
    deleted by then; with --hz=0 it is still in the table.

    The writes are enveloped, as an active KeyDB streams them. A counter that is followed by
    `PEXPIREAT` (a rate limiter's window start) hides the orphan, which is why the counter test
    above streams one; here there is none.

    Decisions 27 and 29 (plan Tasks 2.8 and 2.9, P7-2) flip it: the writes run at their author's
    time, before the deadline, on keys the sweep has not deleted because it runs on the stream
    clock, so they find `c`, `h` and `kt` with their TTL, keep it, and the keys are deleted at the
    deadline.

    Falsifying: a replica that serves a due key as live (`ExpireIfNeeded` returns it) applies the
    writes to the old keys, which keep their TTL: no orphan. With --hz=0 that is `c` at "6"; with
    the sweep the setup fails first, as nothing deletes the due keys.
    """
    async with FakeClassicMaster() as master:
        node, c = await attach_to_stream_live(df_factory, tmp_path, master, sweep)
        t0 = now_ms()
        deadline = t0 + WINDOW_TTL_MS

        def streamed(at, *command):
            return stream_command("envelope", *command, master_ms=at)

        await stream_and_wait_applied(
            master,
            c,
            "marker:set",
            streamed(t0, "SET", "c", 5, "PXAT", deadline),
            streamed(t0, "HSET", "h", "f", 1),
            streamed(t0, "PEXPIREAT", "h", deadline),
            streamed(t0, "SET", "kt", "old", "PXAT", deadline),
            streamed(t0, "SET", "swept", "v", "PXAT", deadline),
        )
        await sleep_until_ms(deadline + WINDOW_PAST_MS)
        if sweep == "access_only":
            size = await c.dbsize()
            assert size == 4 + 1, f"the four keys and a marker should be there, DBSIZE is {size}"
        else:

            @assert_eventually(times=100)
            async def swept_by_the_sweep():
                size = await c.dbsize()
                assert size == 1, f"DBSIZE is {size}: the sweep did not delete the due keys"

            await swept_by_the_sweep()

        at = deadline - 300
        await stream_and_wait_applied(
            master,
            c,
            "marker:late",
            streamed(at, "INCRBY", "c", 1),
            streamed(at, "HSET", "h", "g", 2),
            streamed(at, "SET", "kt", "new", "KEEPTTL"),
        )
        value = await c.get("c")
        assert value == "1", f"c is {value!r}: the replica applied INCR to the old key"
        assert await c.hlen("h") == 1
        assert await c.get("kt") == "new"
        for key in ("c", "h", "kt"):
            pttl = await c.pttl(key)
            assert pttl == -1, f"{key} has a TTL ({pttl} ms): the replica did not recreate it"

        for _ in range(20):  # for 2 s
            assert await c.exists("c", "h", "kt") == 3, "an orphan was removed"
            await asyncio.sleep(0.1)
        # The three orphans and the two markers; with --hz=0 also `swept`, which nothing deleted.
        assert await c.dbsize() == 5 + (1 if sweep == "access_only" else 0)
        assert master.connection_count == 1, "the replica reconnected"


# Throughput: the RREPLAY path must keep up with KeyDB (spec D-12, owner decision 12).
#
# The release bar (DRAKEYDB_PERF=1, a release drakeydb on a quiet 4-cpu box) pins KeyDB's one
# server thread, the replica under test and the load generator, which shares its cpu with this
# test's light sampling, to cpus of their own, so that none of them takes time from another.
KEYDB_CPUS = [0]
REPLICA_CPUS = [1, 2]
LOADER_CPUS = [3]

KEYSPACE = 100_000  # redis-benchmark's `-r`: the number of keys a write can pick
PERF_WINDOW_S = 30
SMOKE_WINDOW_S = 10
SMOKE_OPS_PER_S = 5000
# The load is sampled once a second, and the steady window leaves out this many samples (seconds)
# at both ends, where the load starts and stops.
STEADY_TRIM = 2
MB = 1 << 20
# The bar's second half (spec D-12): drakeydb's max lag within COMPARATOR_FACTOR times of a KeyDB
# active replica's on the same load, or COMPARATOR_FLOOR over it where that is more (see
# comparator_bound).
COMPARATOR_FACTOR = 1.5
COMPARATOR_FLOOR = MB
# The share of the writes KeyDB was offered that a drakeydb replica must have unwrapped from an
# RREPLAY envelope: every write of an active KeyDB goes out in one.
ENVELOPE_SHARE = 0.9


def perf_mode():
    """Whether to run the release bar of spec D-12 and not the functional smoke."""
    return os.environ.get("DRAKEYDB_PERF", "").strip().lower() in ("1", "true", "yes")


def require_perf_box(df_factory):
    """Fails a DRAKEYDB_PERF=1 run that cannot be the release bar, and says why."""
    cpus = set(KEYDB_CPUS + REPLICA_CPUS + LOADER_CPUS)
    allowed = os.sched_getaffinity(0)
    if not cpus <= allowed:
        pytest.fail(
            f"DRAKEYDB_PERF=1 pins to cpus {sorted(cpus)} and this process may only use "
            f"{sorted(allowed)}"
        )
    if shutil.which("redis-benchmark") is None:
        pytest.fail("DRAKEYDB_PERF=1 needs redis-benchmark on PATH")
    binary = os.path.realpath(df_factory.params.path)
    if "build-dbg" in binary:
        pytest.fail(f"DRAKEYDB_PERF=1 is the release bar and {binary} is a debug build")


@pytest.fixture
def restore_cpu_affinity():
    """Puts the cpu mask of the thread that runs the test's event loop back at teardown.

    The release bar pins that thread (driver_pinned_to), and a test that pytest-timeout aborts
    never runs the `finally` of its coroutine, which would leave every later test on one cpu."""
    before = os.sched_getaffinity(0)
    yield
    os.sched_setaffinity(0, before)


def comparator_bound(comparator_max_lag):
    """The largest max lag (bytes) drakeydb may show when a KeyDB replica's was `comparator_max_lag`:
    COMPARATOR_FACTOR times it, or COMPARATOR_FLOOR over it where that is more. Two INFOs read a few
    milliseconds apart skew a lag by about that much (see summarise), which would otherwise turn two
    lags of a few hundred KB into a ratio of 2."""
    return max(COMPARATOR_FACTOR * comparator_max_lag, comparator_max_lag + COMPARATOR_FLOOR)


def pin_threads(pid, cpus, spread_proactors=False):
    """Pins every thread of process `pid` to `cpus`.

    helio pins Proactor<i> to one cpu of the mask its process starts with. With `spread_proactors`
    each one keeps that single cpu (cpus[i % len(cpus)]), as it would under `taskset -c`."""
    for tid in os.listdir(f"/proc/{pid}/task"):
        mask = set(cpus)
        with contextlib.suppress(FileNotFoundError, ProcessLookupError):  # a thread that exited
            if spread_proactors:
                with open(f"/proc/{pid}/task/{tid}/comm") as f:
                    proactor = re.fullmatch(r"Proactor(\d+)", f.read().strip())
                if proactor:
                    mask = {cpus[int(proactor.group(1)) % len(cpus)]}
            os.sched_setaffinity(int(tid), mask)


@contextlib.contextmanager
def driver_pinned_to(cpus):
    """Pins the calling thread, which runs the test's event loop, to `cpus` for the block."""
    before = os.sched_getaffinity(0)
    os.sched_setaffinity(0, cpus)
    try:
        yield
    finally:
        os.sched_setaffinity(0, before)


def cpu_seconds(pid):
    times = psutil.Process(pid).cpu_times()
    return times.user + times.system


class BenchmarkLoad:
    """The release bar's load: redis-benchmark's pipelined SETs and INCRs, as fast as KeyDB takes
    them.

    The spec's `redis-benchmark -P 100 -c 50 -t set,incr -r 100000` runs the two tests one after
    the other, `-n` requests each. Here they run side by side, 25 connections each, so that the
    whole window carries both. The window is the time between start() and stop(), which kills
    them: redis-benchmark 7.0 has no duration, and an `-n` sized for one machine would make the
    window as long as that machine is slow. KeyDB executes the commands it has read whole, so a
    kill mid-pipeline leaves a clean stream.

    Their stderr goes to redis-benchmark-<test>.log in `log_dir`. One that exits before stop()
    leaves that in `error` (see BackgroundTask).
    """

    def __init__(self, port, cpus, log_dir):
        self.port = port
        self.cpus = cpus
        self.log_dir = log_dir
        self.procs = []  # (process, its log's path)
        self.error = None

    def start(self):
        for test in ("set", "incr"):
            command = ["redis-benchmark", "-p", str(self.port), "-P", "100", "-c", "25"]
            command += ["-t", test, "-r", str(KEYSPACE), "-n", "2000000000", "-q"]
            log_path = os.path.join(self.log_dir, f"redis-benchmark-{test}.log")
            with open(log_path, "w") as log:
                proc = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=log)
            pin_threads(proc.pid, self.cpus)
            self.procs.append((proc, log_path))

    async def stop(self):
        for proc, log_path in self.procs:
            if proc.poll() is not None:
                self.error = f"redis-benchmark exited by itself, code {proc.returncode}: {log_path}"
            proc.terminate()
        for proc, _ in self.procs:
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()

    def cpu_seconds(self):
        return sum(cpu_seconds(proc.pid) for proc, _ in self.procs)


class BackgroundTask:
    """An asyncio task that runs from start() to stop(), beside the sampling.

    One that dies by itself leaves what it died of in `error`: stop() runs in a `finally`, where an
    exception would skip the other stop and replace the failure being handled. The run asserts on
    `error` once the window is over."""

    def __init__(self):
        self.task = None
        self.error = None

    def start(self):
        self.task = asyncio.create_task(self._run())

    async def stop(self):
        if self.task is None:
            return
        self.task.cancel()
        try:
            await self.task
        except asyncio.CancelledError:
            pass
        except Exception as e:  # it died before the cancel
            self.error = e


class CappedLoad(BackgroundTask):
    """The smoke's load: SETs and INCRs of the same keys, pipelined in batches over one connection
    at about `rate` commands a second, which a debug build on a shared box can keep up with."""

    BATCH = 100

    def __init__(self, client, rate):
        super().__init__()
        self.client = client
        self.rate = rate

    async def _run(self):
        period = self.BATCH / self.rate
        next_at = time.monotonic()
        while True:
            pipe = self.client.pipeline(transaction=False)
            for _ in range(self.BATCH // 2):
                i = random.randrange(KEYSPACE)
                pipe.set(f"key:{i:012d}", "xxx")
                pipe.incr(f"counter:{i:012d}")
            await pipe.execute()
            next_at += period
            await asyncio.sleep(max(0.0, next_at - time.monotonic()))

    def cpu_seconds(self):
        return 0.0  # runs inside this process


class ReplicationProbe(BackgroundTask):
    """Times the replication delay a client sees: every 100 ms it writes a marker key into KeyDB
    and polls the replica, every millisecond, for the time until it can read it. That is the
    replica's apply time plus its own GET service time and this process's scheduling, so it is not
    a lag: a replica that is busy answers its reads later. It is a second view, in milliseconds,
    where two INFOs a few milliseconds apart resolve a lag only to a few hundred KB at the release
    bar's rate. The cost is ten writes and some hundreds of reads a second."""

    GIVE_UP_S = 5.0

    def __init__(self, master, replica, started):
        super().__init__()
        self.master = master
        self.replica = replica
        self.started = started
        self.delays = []  # (second of the window the marker was written at, delay in seconds)

    async def _run(self):
        for n in range(1, 1 << 30):
            key = f"marker:{n}"
            await self.master.set(key, n)
            written = time.monotonic()
            while (
                await self.replica.get(key) is None and time.monotonic() - written < self.GIVE_UP_S
            ):
                await asyncio.sleep(0.001)
            self.delays.append((written - self.started, time.monotonic() - written))
            await asyncio.sleep(0.1)

    def summary(self, first_s, last_s):
        """The delays, in ms, of the markers written between `first_s` and `last_s` of the window."""
        delays = sorted(1000 * d for at, d in self.delays if first_s <= at <= last_s)

        def quantile(q):
            return round(delays[min(len(delays) - 1, int(q * len(delays)))], 1)

        return {"n": len(delays), "p50": quantile(0.5), "p99": quantile(0.99), "max": quantile(1)}


async def take_sample(started, master, replica, cpu_sources):
    """One reading of both servers, taken concurrently. Each time stamp is the middle of its own
    request: under load a server can take milliseconds to answer."""

    async def timed(request):
        sent = time.monotonic()
        reply = await request
        return reply, (sent + time.monotonic()) / 2 - started

    (m, tm), (stats, _), (r, tr) = await asyncio.gather(
        timed(master.info("replication")),
        timed(master.info("commandstats")),
        timed(replica.info("replication")),
    )
    # KeyDB drops a replica whose output buffer outgrows its limit (256 MB by default), one that
    # has fallen that far behind; INFO then has no offset.
    assert (
        r.get("master_link_status") == "up" and "slave_repl_offset" in r
    ), f"the replica's link is down: {r}"
    return {
        "tm": tm,
        "tr": tr,
        "m": int(m["master_repl_offset"]),
        "r": int(r["slave_repl_offset"]),
        # An active KeyDB rewrites INCR into the INCRBY it replicates, and counts it as that.
        "ops": sum(
            stats.get(f"cmdstat_{name}", {}).get("calls", 0) for name in ("set", "incr", "incrby")
        ),
        # drakeydb counts the envelopes it applied; KeyDB has no such field.
        "envelopes": r.get("rreplay_unwrapped"),
        "cpu": {name: source() for name, source in cpu_sources.items()},
    }


def summarise(samples):
    """The rates, lag and CPU use of one second-by-second series, over its steady window."""
    # The two offsets are read within milliseconds of each other and the master's moves by
    # megabytes a second, so a lag carries a noise of a few hundred KB at the release bar's rate (it
    # can read below 0): far below the bounds, but coarse for comparing two replicas, which is why
    # comparator_bound has a floor. The client-visible delay of ReplicationProbe is a second view;
    # it includes the replica's GET service time.
    lags = [sample["m"] - sample["r"] for sample in samples]

    steady = samples[STEADY_TRIM:-STEADY_TRIM]
    steady_lags = lags[STEADY_TRIM:-STEADY_TRIM]
    first, last = steady[0], steady[-1]
    master_s, replica_s = last["tm"] - first["tm"], last["tr"] - first["tr"]
    offered = (last["ops"] - first["ops"]) / master_s
    produced = (last["m"] - first["m"]) / master_s
    assert offered > 0 and produced > 0, f"no load reached KeyDB: {first} {last}"
    applied = (last["r"] - first["r"]) / replica_s
    bytes_per_op = produced / offered
    result = {
        "offered_ops_per_s": round(offered),
        "produce_bytes_per_s": round(produced),
        "apply_bytes_per_s": round(applied),
        "apply_ops_per_s": round(applied / bytes_per_op),  # at the master's bytes per command
        "bytes_per_op": round(bytes_per_op, 1),
        "ratio": round(applied / produced, 4),
        "max_lag": max(steady_lags),
        "median_lag": round(statistics.median(steady_lags)),
        "lag_series": lags,
        "cpu_pct": {
            name: round(100 * (last["cpu"][name] - first["cpu"][name]) / master_s, 1)
            for name in first["cpu"]
        },
    }
    if last["envelopes"] is not None:
        result["applied_envelopes_per_s"] = round(
            (last["envelopes"] - first["envelopes"]) / replica_s
        )
    return result


def bar_bounds(perf, produced):
    """The bounds of spec D-12: the release bar under DRAKEYDB_PERF, else the smoke's loose ones."""
    if perf:
        return {"min_ratio": 0.95, "max_lag": max(2 * produced, 8 * MB), "max_drain_s": 2.0}
    return {"min_ratio": 0.5, "max_lag": 32 * MB, "max_drain_s": 10.0}


async def wait_drained(master, replica, stopped_at, give_up_s):
    """Seconds from `stopped_at` until the replica holds everything KeyDB wrote, None if that takes
    longer than `give_up_s`.

    Polls every 50 ms for a poll with equal offsets that the next poll finds unchanged on the
    master's side: KeyDB may still be working through what the generators sent, and an offset that
    only met the master's on its way up does not count. A replica whose link is down has no offset
    and never matches, so the caller's link checks then say what happened."""
    equal_at = equal_offset = None
    while time.monotonic() - stopped_at <= give_up_s:
        polled_at = time.monotonic()
        m = int((await master.info("replication"))["master_repl_offset"])
        r = (await replica.info("replication")).get("slave_repl_offset")
        if equal_at is not None and m == equal_offset:
            return equal_at - stopped_at
        equal_at, equal_offset = (polled_at, m) if r is not None and int(r) == m else (None, None)
        await asyncio.sleep(0.05)
    return None


@assert_eventually(times=300)
@retry_while_loading
async def assert_idle_synced(master, replica):
    """The link is up, KeyDB lists the replica as online, and the replica is at the master's offset."""
    link = await replica.info("replication")
    assert link["master_link_status"] == "up", link
    info = await master.info("replication")
    assert info["slave0"]["state"] == "online", info
    assert int(link["slave_repl_offset"]) == int(info["master_repl_offset"]), (link, info)


async def assert_same_keys(master, replica):
    """Both servers hold the same keys with the same values: the SET keys and the INCR counters
    (their values are the number of INCRs each took), every one of the KEYSPACE names of each."""
    assert await replica.dbsize() == await master.dbsize()
    for prefix in ("key", "counter"):
        for start in range(0, KEYSPACE, 1000):
            names = [f"{prefix}:{i:012d}" for i in range(start, start + 1000)]
            wanted, got = await master.mget(names), await replica.mget(names)
            differ = [name for name, w, g in zip(names, wanted, got) if w != g]
            assert not differ, f"{len(differ)} {prefix} keys differ here, e.g. {differ[:3]}"


async def sync_counts(k):
    """KeyDB's count of the full syncs and partial resyncs (granted, refused) it has served."""
    stats = await k.info("stats")
    return stats["sync_full"], stats["sync_partial_ok"], stats["sync_partial_err"]


def record_throughput(name, result):
    """Logs a run's numbers, and appends them as a JSON line to $DRAKEYDB_PERF_OUT when it is set."""
    line = json.dumps({"test": name, **result})
    logging.info("throughput %s", line)
    if path := os.environ.get("DRAKEYDB_PERF_OUT"):
        with open(path, "a") as f:
            f.write(line + "\n")


async def run_throughput(df_factory, keydb_server_factory, tmp_path, *, replica_kind, bar):
    """Writes into an active KeyDB (a plain one for "drakeydb_raw") for a window of PERF_WINDOW_S
    (SMOKE_WINDOW_S without DRAKEYDB_PERF), with a replica attached and idle-synced first:

    - "drakeydb": a plain drakeydb replica, which unwraps every RREPLAY envelope (the path under test),
    - "keydb": a second active KeyDB, the comparator of the bar,
    - "drakeydb_raw": a plain drakeydb replica of the plain KeyDB, whose raw stream it squashes.

    Samples both offsets once a second, times the drain from the moment the load is dead, then
    checks that the link held, and that the keys match. With `bar` it also asserts the absolute
    bounds of spec D-12; the comparison with a KeyDB replica needs two runs (see
    test_keydb_onboarding_lag_within_1_5x_of_a_keydb_replica). Returns the numbers. The KeyDBs it
    started are stopped before it returns, so that a run that follows starts from nothing else.
    """
    perf = perf_mode()
    if perf:
        require_perf_box(df_factory)

    master = keydb_server_factory(active_replica=replica_kind != "drakeydb_raw")
    async with contextlib.AsyncExitStack() as stack:
        # Pushed before the clients are entered, so that it runs after they are closed.
        stack.callback(master.stop)
        k = await stack.enter_async_context(master.client())
        node = None
        if replica_kind == "keydb":
            follower = keydb_server_factory(active_replica=True)
            stack.callback(follower.stop)
            replica_pid = follower.proc.pid
            replica = await stack.enter_async_context(follower.client())
            await replica.execute_command("REPLICAOF", "localhost", master.port)
        else:
            # The release bar runs drakeydb with its own defaults, not the harness's (a vmodule
            # that turns VLOG(1) on and per-command latency tracking).
            flags = {"latency_tracking": "false", "vmodule": ""} if perf else {}
            node, replica = await attach_plain_replica(df_factory, tmp_path, master, **flags)
            replica_pid = node.proc.pid
        await assert_idle_synced(k, replica)
        synced = await sync_counts(k)
        assert synced[0] == 1, synced
        reconnects = (
            await get_metric_value(node, "dragonfly_replica_reconnect_count") if node else 0
        )

        if perf:
            pin_threads(master.proc.pid, KEYDB_CPUS)
            pin_threads(replica_pid, REPLICA_CPUS, spread_proactors=node is not None)
            stack.enter_context(driver_pinned_to(LOADER_CPUS))
            load = BenchmarkLoad(master.port, LOADER_CPUS, df_factory.params.log_dir)
            window = PERF_WINDOW_S
        else:
            load_client = await stack.enter_async_context(master.client())
            load, window = CappedLoad(load_client, SMOKE_OPS_PER_S), SMOKE_WINDOW_S
        cpu_sources = {
            "keydb_master": lambda: cpu_seconds(master.proc.pid),
            "replica": lambda: cpu_seconds(replica_pid),
            "loader": load.cpu_seconds,
        }

        load_before = os.getloadavg()[0]
        samples = []
        probe = None
        try:
            load.start()
            started = time.monotonic()
            if perf:
                probe = ReplicationProbe(k, replica, started)
                probe.start()
            for i in range(window + 1):
                await asyncio.sleep(max(0.0, started + i - time.monotonic()))
                samples.append(await take_sample(started, k, replica, cpu_sources))
        finally:
            # Neither stop() raises about what its task died of (see BackgroundTask), so both run
            # and the failure being handled is the one reported.
            if probe:
                await probe.stop()
            await load.stop()
        stopped_at = time.monotonic()

        died = [f"{type(b).__name__}: {b.error!r}" for b in (load, probe) if b and b.error]
        assert not died, f"something that ran beside the sampling died: {died}"
        result = summarise(samples)
        if probe:
            result["replication_delay_ms"] = probe.summary(STEADY_TRIM, window - STEADY_TRIM)
        result["samples"] = [
            [round(s["tm"], 3), round(s["tr"], 3), s["m"], s["r"], s["ops"]] for s in samples
        ]
        bounds = bar_bounds(perf, result["produce_bytes_per_s"])
        result.update(replica=replica_kind, perf=perf, window_s=window, bounds=bounds)
        result["loadavg_1m_before"] = load_before
        if node:
            result["binary"] = os.path.realpath(df_factory.params.path)

        # The drain is timed first, from the moment the load is dead: the link, counter and key
        # checks below make requests of their own (a /metrics scrape among them) that would count
        # as drain time. It is waited for as long as the bound allows, or a minute when there are
        # no bounds and only the numbers matter; it is recorded before the bounds are asserted, so
        # that a run that fails them still records all its numbers.
        give_up_s = bounds["max_drain_s"] + 3 if bar else 60
        drain_s = await wait_drained(k, replica, stopped_at, give_up_s)
        result["drain_s"] = None if drain_s is None else round(drain_s, 3)
        record_throughput(f"{replica_kind}:{'perf' if perf else 'smoke'}", result)
        skipped = ("lag_series", "samples")
        summary = {key: value for key, value in result.items() if key not in skipped}

        # (1) The link held: no reconnect, and KeyDB served no other sync than the first.
        link = await replica.info("replication")
        assert link["master_link_status"] == "up", link
        if node:
            assert await get_metric_value(node, "dragonfly_replica_reconnect_count") == reconnects
        assert await sync_counts(k) == synced, "KeyDB served another sync during the load"
        assert (await k.info("replication"))["connected_slaves"] == 1

        # The envelope path ran: about one unwrapped envelope for every write KeyDB took. A
        # replica that took a raw stream keeps up too, and must not pass for this one.
        if replica_kind == "drakeydb":
            envelopes = result.get("applied_envelopes_per_s")
            assert (
                envelopes is not None and envelopes >= ENVELOPE_SHARE * result["offered_ops_per_s"]
            ), f"the replica unwrapped {envelopes} envelopes/s of the writes offered: {summary}"

        # (2) The replica kept up over the steady window, and (3) the lag drained in time.
        if bar:
            assert result["ratio"] >= bounds["min_ratio"], summary
            assert result["max_lag"] <= bounds["max_lag"], summary
        assert drain_s is not None, f"not drained {give_up_s} s after the load stopped: {summary}"
        if bar:
            assert drain_s <= bounds["max_drain_s"], summary

        # Both hold the same keys.
        await assert_same_keys(k, replica)
        return result


@pytest.mark.slow
@pytest.mark.keydb
async def test_keydb_onboarding_keeps_up_under_load(
    df_factory: DflyInstanceFactory, keydb_server_factory, tmp_path, restore_cpu_affinity
):
    """A plain replica of an active KeyDB keeps up with a write load that KeyDB takes at full speed
    (owner decision 12, spec D-12). Per-command dispatch of the RREPLAY envelopes gives up the
    squasher's batching, and this is the measurement that it still keeps up.

    Over a window of pipelined SETs and INCRs, sampled once a second, minus the first and last 2 s:
    the link never reconnects and KeyDB serves one full sync; the replica unwrapped about one
    envelope for every write; its offset advances at least `min_ratio` as fast as the master's,
    and its lag never exceeds `max_lag`; within `max_drain_s` of the load stopping the lag is 0;
    and both servers then hold the same keys, the INCR counters included.

    By default this is a smoke, to show the test and the plumbing work on a debug build or a shared
    CI box: about 5000 writes a second from one asyncio connection, and loose bounds (ratio 0.5,
    lag 32 MB, drain 10 s). It does not claim the bar. Its 32 MB lag bound is the spec's number
    and cannot fire: the smoke writes about 7.4 MB in all. What it asserts is the ratio, the drain,
    the link, the envelopes and the keys.

    With DRAKEYDB_PERF=1 and a release DRAGONFLY_PATH on a quiet 4-cpu box it asserts the absolute
    half of the bar: redis-benchmark at full speed (pinned, see BenchmarkLoad) and a ratio of
    0.95, a lag of at most max(2 s of the master's output, 8 MB) and a drain within 2 s. The other
    half, a lag within 1.5 times of a KeyDB active replica's on the same load, is asserted by
    test_keydb_onboarding_lag_within_1_5x_of_a_keydb_replica, which runs both.

    Falsifying: a 1 ms sleep per enveloped command in ClassicApplier::ApplyCommand caps the replica
    at about 1000 commands a second, and the smoke's ratio assertion fails (0.18). Under
    DRAKEYDB_PERF=1 the same replica falls so far behind that KeyDB drops it, when its output
    buffer reaches the 256 MB limit, and the sampling reports the link down; a busy wait of 3 us
    per command fails the bar's ratio instead (0.86).
    """
    await run_throughput(
        df_factory, keydb_server_factory, tmp_path, replica_kind="drakeydb", bar=True
    )


@pytest.mark.slow
@pytest.mark.keydb
@pytest.mark.skipif(not perf_mode(), reason="the 1.5x half of the release bar: DRAKEYDB_PERF=1")
async def test_keydb_onboarding_lag_within_1_5x_of_a_keydb_replica(
    df_factory: DflyInstanceFactory, keydb_server_factory, tmp_path, restore_cpu_affinity
):
    """Owner decision 12 in full: under the release bar's load, drakeydb's max lag is within 1.5
    times of a KeyDB active replica's (comparator_bound).

    Runs the comparator, a second active KeyDB as the replica, and then drakeydb, one after the
    other under the same pinning and load, so that neither takes the other's cpus (the drakeydb
    run asserts the absolute bar as well, see test_keydb_onboarding_keeps_up_under_load). The
    bound is on the max lag of each run's steady window; the marker delays and the drains are
    recorded, not asserted."""
    comparator = await run_throughput(
        df_factory, keydb_server_factory, tmp_path, replica_kind="keydb", bar=False
    )
    drakeydb = await run_throughput(
        df_factory, keydb_server_factory, tmp_path, replica_kind="drakeydb", bar=True
    )
    bound = comparator_bound(comparator["max_lag"])
    record_throughput(
        "comparator_bound",
        {
            "comparator_max_lag": comparator["max_lag"],
            "drakeydb_max_lag": drakeydb["max_lag"],
            "bound": bound,
            "binary": drakeydb["binary"],
        },
    )
    assert drakeydb["max_lag"] <= bound, (
        f"drakeydb's max lag of {drakeydb['max_lag']} B ({drakeydb['binary']}) is over "
        f"{bound:.0f} B, the bound on a KeyDB replica's of {comparator['max_lag']} B: "
        f"max({COMPARATOR_FACTOR} x it, it + {COMPARATOR_FLOOR} B)"
    )


@pytest.mark.slow
@pytest.mark.keydb
@pytest.mark.skipif(
    not perf_mode(), reason="the reference setups of the release bar: DRAKEYDB_PERF=1"
)
@pytest.mark.parametrize("replica_kind", ["keydb", "drakeydb_raw"])
async def test_keydb_throughput_reference_setups(
    df_factory: DflyInstanceFactory,
    keydb_server_factory,
    tmp_path,
    restore_cpu_affinity,
    replica_kind,
):
    """The numbers the release bar is judged against, under its load and pinning, and no bounds:
    a second active KeyDB as the replica (drakeydb's lag must be within 1.5 times of its, which
    test_keydb_onboarding_lag_within_1_5x_of_a_keydb_replica asserts), and a drakeydb replica of a
    plain KeyDB, whose raw stream the squasher batches (the reference the envelope path pays
    against). They run apart from the drakeydb run, so that no replica takes the cpus of another;
    the link, drain and key checks still hold."""
    await run_throughput(
        df_factory, keydb_server_factory, tmp_path, replica_kind=replica_kind, bar=False
    )


def test_comparator_bound_is_1_5_times_with_a_floor_of_1_mb():
    """comparator_bound, the bar's comparison with a KeyDB replica's max lag: 1 MB over a small
    one, 1.5 times a large one, and the two meet at twice the floor."""
    assert comparator_bound(0) == MB
    assert comparator_bound(500_000) == 500_000 + MB
    assert comparator_bound(2 * MB) == 3 * MB
    assert comparator_bound(10 * MB) == 15 * MB


async def test_background_task_stop_leaves_what_the_task_died_of_in_error():
    """stop() of a task that died by itself returns and leaves its exception in `error`, so that
    the stops in run_throughput's `finally` neither skip each other nor replace the failure being
    handled; a task stopped while it runs leaves no error, and one never started is a no-op."""

    class Dies(BackgroundTask):
        async def _run(self):
            raise ConnectionError("the replica went away")

    class Runs(BackgroundTask):
        async def _run(self):
            await asyncio.sleep(3600)

    never_started, dies, runs = Runs(), Dies(), Runs()
    await never_started.stop()
    dies.start()
    runs.start()
    await asyncio.sleep(0)  # both run to their first await, or to their death
    await dies.stop()
    await runs.stop()
    assert isinstance(dies.error, ConnectionError)
    assert runs.error is None and runs.task.cancelled()
    assert never_started.error is None


class ScriptedInfo:
    """A client whose `info("replication")` answers from a script; the last answer repeats."""

    def __init__(self, *answers):
        self.answers = list(answers)

    async def info(self, section):
        assert section == "replication"
        return self.answers.pop(0) if len(self.answers) > 1 else self.answers[0]


async def test_wait_drained_counts_a_replica_without_an_offset_as_not_drained():
    """A replica whose link is down has no slave_repl_offset: wait_drained gives up with None and
    leaves the link assertion of run_throughput to say what happened, where it used to raise a
    KeyError. One that catches up is drained once the master has stood still for a poll."""
    master = ScriptedInfo({"master_repl_offset": 100})
    down = ScriptedInfo({"master_link_status": "down"})
    assert await wait_drained(master, down, time.monotonic(), give_up_s=0.2) is None

    catching_up = ScriptedInfo({"slave_repl_offset": 90}, {"slave_repl_offset": 100})
    drained = await wait_drained(master, catching_up, time.monotonic(), give_up_s=5)
    assert drained is not None and drained < 1
