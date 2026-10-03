"""Interop tests for a drakeydb node replicating from a real KeyDB master (v6.3.4).

They need a keydb-server binary: $KEYDB_SERVER_PATH, else `keydb-server` on PATH (see
docs/build-from-source.md). Without one they skip, and with KEYDB_REQUIRED=1 they fail instead.
"""

import pytest

from .instance import DflyInstanceFactory
from .utility import assert_eventually, wait_available_async

pytestmark = pytest.mark.keydb

PRE_STRINGS = {f"pre:{i}": f"v{i}" for i in range(100)}


async def seed_before_attach(k):
    """Writes the data a replica must receive through the full sync."""
    await k.mset(PRE_STRINGS)
    await k.hset("pre:hash", mapping={"f1": "1", "f2": "2"})
    await k.rpush("pre:list", "a", "b", "c")
    await k.sadd("pre:set", "x", "y")
    await k.zadd("pre:zset", {"m1": 1, "m2": 2})


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


async def test_keydb_plain_master_full_sync_and_stream(
    df_factory: DflyInstanceFactory, keydb_server_factory, tmp_path
):
    """A plain drakeydb replica of a plain (non-active) KeyDB master: both the full sync and the
    stream that follows it carry data."""
    keydb = keydb_server_factory(active_replica=False)
    node = df_factory.create(proactor_threads=2, dir=str(tmp_path / "df"))
    node.start()
    c = node.client()

    async with keydb.client() as k:
        await seed_before_attach(k)
        assert await c.execute_command(f"REPLICAOF localhost {keydb.port}") == "OK"
        await wait_available_async(c)
        info = await c.info("replication")
        assert info["role"] == "slave" and info["master_link_status"] == "up", info
        await assert_full_sync_arrived(c)

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


async def test_keydb_active_handshake_and_full_sync(
    df_factory: DflyInstanceFactory, keydb_server_factory, tmp_path
):
    """A plain drakeydb replica of an active-replica KeyDB gets through the handshake and loads
    its full sync.

    An active KeyDB answers every `REPLCONF capa ...` with `+OK active-replica`, so a greeting that
    insists on exactly `+OK` refuses the link ('Bad response to "REPLCONF capa eof capa psync2"',
    REPLICAOF fails with 'replication cancelled') and no key ever arrives. Replaying what an active
    KeyDB streams after the full sync is not covered here.
    """
    keydb = keydb_server_factory(active_replica=True)
    node = df_factory.create(proactor_threads=2, dir=str(tmp_path / "df"))
    node.start()
    c = node.client()

    async with keydb.client() as k:
        await seed_before_attach(k)
        assert await c.execute_command(f"REPLICAOF localhost {keydb.port}") == "OK"
        await wait_available_async(c)
        info = await c.info("replication")
        assert info["role"] == "slave" and info["master_link_status"] == "up", info
        await assert_full_sync_arrived(c)


async def test_keydb_active_handshake_peer_mode(
    df_factory: DflyInstanceFactory, keydb_server_factory, tmp_path
):
    """As above, with the drakeydb node in peer mode (--active_replica), whose link is reported
    through the masterN block of INFO instead of the plain replica fields."""
    keydb = keydb_server_factory(active_replica=True)
    node = df_factory.create(proactor_threads=2, dir=str(tmp_path / "df"), active_replica="true")
    node.start()
    c = node.client()

    async with keydb.client() as k:
        await seed_before_attach(k)
        assert await c.execute_command(f"REPLICAOF localhost {keydb.port}") == "OK"
        await wait_for_peer_link(c)
        await assert_full_sync_arrived(c)
