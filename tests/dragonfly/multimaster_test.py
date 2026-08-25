"""Multi-master identity and active-replica fan-in tests."""

import asyncio
import logging
import re
import time

import async_timeout
import pytest

import redis

from .instance import DflyInstanceFactory, DflyStartException
from .utility import assert_eventually, wait_available_async

UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
FIXED_UUID = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"


async def assert_start_fails(node):
    try:
        node.start()
    except DflyStartException:
        return

    # Startup listens before ServerFamily validates node identity, so a fast machine can observe
    # the dynamic port just before the process exits and make node.start() return successfully.
    # Budget matches the other waits in this file (20s): validation runs after the listener opens,
    # so a loaded CI machine can take far longer than the exit itself suggests.
    for _ in range(100):
        return_code = node.proc.poll()
        if return_code is not None:
            node.stop(kill=True)  # clear the failed process from fixture teardown
            assert return_code != 0
            return
        await asyncio.sleep(0.2)
    pytest.fail("node remained running despite invalid identity configuration")


async def test_node_uuid_reported_and_persisted(df_factory: DflyInstanceFactory, tmp_path):
    node = df_factory.create(proactor_threads=2, dir=str(tmp_path / "n1"))
    node.start()
    c = node.client()
    uuid1 = (await c.info("replication"))["node_uuid"]
    assert UUID_RE.match(uuid1)
    assert (tmp_path / "n1" / "drakeydb.uuid").read_text().strip() == uuid1
    node.stop()
    node.start()
    c = node.client()
    assert (await c.info("replication"))["node_uuid"] == uuid1


async def test_node_uuid_override_is_ephemeral(df_factory: DflyInstanceFactory, tmp_path):
    d = tmp_path / "n1"
    node = df_factory.create(proactor_threads=1, dir=str(d), node_uuid=FIXED_UUID)
    node.start()
    c = node.client()
    assert (await c.info("replication"))["node_uuid"] == FIXED_UUID
    node.stop()
    assert not (d / "drakeydb.uuid").exists()
    node2 = df_factory.create(proactor_threads=1, dir=str(d))  # no override
    node2.start()
    c2 = node2.client()
    fresh = (await c2.info("replication"))["node_uuid"]
    assert fresh != FIXED_UUID and UUID_RE.match(fresh)


async def test_invalid_node_uuid_flag_fails_boot(df_factory: DflyInstanceFactory, tmp_path):
    node = df_factory.create(proactor_threads=1, dir=str(tmp_path / "n1"), node_uuid="nonsense")
    await assert_start_fails(node)


async def test_corrupt_uuid_file_fails_boot(df_factory: DflyInstanceFactory, tmp_path):
    d = tmp_path / "n1"
    d.mkdir()
    (d / "drakeydb.uuid").write_text("garbage-not-a-uuid\n")
    node = df_factory.create(proactor_threads=1, dir=str(d))
    await assert_start_fails(node)


async def test_replconf_uuid_wire_reply(df_factory: DflyInstanceFactory, tmp_path):
    node = df_factory.create(proactor_threads=1, dir=str(tmp_path / "n1"))
    node.start()
    c = node.client()
    reply = await c.execute_command("REPLCONF", "UUID", "01234567-89ab-4cde-8f01-23456789abcd")
    token_uuid, token_ms = reply.split(" ")
    assert token_uuid == (await c.info("replication"))["node_uuid"]
    assert abs(int(token_ms) - time.time() * 1000) < 60_000
    with pytest.raises(redis.exceptions.ResponseError, match="Invalid UUID"):
        await c.execute_command("REPLCONF", "UUID", "not-a-uuid")


async def test_uuid_exchange_master_and_replica_info(df_factory: DflyInstanceFactory, tmp_path):
    master = df_factory.create(proactor_threads=2, dir=str(tmp_path / "m"))
    replica = df_factory.create(proactor_threads=2, dir=str(tmp_path / "r"))
    df_factory.start_all([master, replica])
    c_master, c_replica = master.client(), replica.client()
    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_available_async(c_replica)
    mi = await c_master.info("replication")
    ri = await c_replica.info("replication")
    assert mi["node_uuid"] != ri["node_uuid"]
    assert ri["master_node_uuid"] == mi["node_uuid"]
    assert mi["slave0"]["node_uuid"] == ri["node_uuid"]  # redis-py parses the csv into a dict


async def test_uuid_survives_master_restart(df_factory: DflyInstanceFactory, tmp_path, port_picker):
    # Pin the master's port: df_factory.create() without an explicit port= uses a dynamic
    # (--port -1) port that DflyInstance resets to None on stop(), so a restarted instance would
    # otherwise come back listening on a different port and the replica's REPLICAOF target below
    # would go stale forever. Same idiom as redis_replication_test.py / sentinel_test.py.
    master = df_factory.create(
        proactor_threads=2, dir=str(tmp_path / "m"), port=port_picker.get_available_port()
    )
    replica = df_factory.create(proactor_threads=2, dir=str(tmp_path / "r"))
    df_factory.start_all([master, replica])
    c_replica = replica.client()
    await c_replica.execute_command(f"REPLICAOF localhost {master.port}")
    await wait_available_async(c_replica)
    uuid_before = (await c_replica.info("replication"))["master_node_uuid"]
    master.stop()
    master.start()
    # Check persistence directly on the master, then require a post-restart write on the replica
    # so stale INFO state cannot make the reconnect assertion pass vacuously.
    c_master = master.client()
    assert (await c_master.info("replication"))["node_uuid"] == uuid_before
    await c_master.set("written-after-restart", "yes")
    for _ in range(100):
        try:
            ri = await c_replica.info("replication")
            replicated_value = await c_replica.get("written-after-restart")
        except (redis.exceptions.BusyLoadingError, redis.exceptions.ConnectionError):
            await asyncio.sleep(0.2)
            continue
        if (
            ri.get("master_link_status") == "up"
            and ri.get("master_node_uuid") == uuid_before
            and replicated_value == "yes"
        ):
            break
        await asyncio.sleep(0.2)
    else:
        pytest.fail("replica did not reconnect after master restart")
    assert ri["master_node_uuid"] == uuid_before


@pytest.mark.parametrize(
    "unsupported_reply",
    [b"+OK\r\n", b"-ERR unknown REPLCONF option\r\n"],
    ids=["ok", "error"],
)
async def test_uuid_cleared_when_reconnect_master_lacks_exchange(
    df_factory: DflyInstanceFactory, tmp_path, proxy_factory, unsupported_reply
):
    master = df_factory.create(proactor_threads=2, dir=str(tmp_path / "m"))
    replica = df_factory.create(proactor_threads=2, dir=str(tmp_path / "r"))
    df_factory.start_all([master, replica])
    c_master, c_replica = master.client(), replica.client()
    proxy = await proxy_factory(master.port)

    await c_replica.execute_command(f"REPLICAOF localhost {proxy.port}")
    await wait_available_async(c_replica)
    master_uuid = (await c_master.info("replication"))["node_uuid"]
    for _ in range(100):
        info = await c_replica.info("replication")
        if info.get("master_link_status") == "up" and info.get("master_node_uuid") == master_uuid:
            break
        await asyncio.sleep(0.2)
    else:
        pytest.fail("initial UUID exchange did not complete")

    await proxy.override_next_response(b"REPLCONF UUID ", unsupported_reply)
    await proxy.close()
    await proxy.start_serving()
    await c_master.set("written-after-unsupported-reconnect", "yes")

    for _ in range(100):
        try:
            info = await c_replica.info("replication")
            replicated_value = await c_replica.get("written-after-unsupported-reconnect")
        except (redis.exceptions.BusyLoadingError, redis.exceptions.ConnectionError):
            await asyncio.sleep(0.2)
            continue
        if (
            info.get("master_link_status") == "up"
            and "master_node_uuid" not in info
            and replicated_value == "yes"
        ):
            break
        await asyncio.sleep(0.2)
    else:
        pytest.fail("replica did not reconnect without stale UUID state")


async def test_configure_dfly_master_sends_and_tolerates_drakey_version_and_peer(
    df_factory: DflyInstanceFactory, proxy_factory
):
    """Greet() (replica.cc) sends REPLCONF DRAKEY-VERSION on every drakeydb replica, and
    additionally REPLCONF PEER 1 in peer mode -- strictly before REPLCONF capa dragonfly, like
    REPLCONF UUID, so an active master's admission check can see them (P3 T7). This test proxies
    an old-style -ERR reply to both pairs to prove the replica still tolerates a master that
    predates this task (or any master that simply doesn't recognize them, e.g. plain Redis) and
    keeps replicating regardless -- our own master here understands both pairs now, but the
    tolerance path must stay live for a real old peer.

    Falsifying: override_next_response only clears its pending override once its marker is
    actually seen on the wire (see proxy.py), and refuses to arm a second one while the first is
    still pending. Greet() -- and thus the blocking REPLICAOF below -- only returns once it has
    finished sending both REPLCONF pairs for that attempt, so if either stopped being sent, its
    override would still be pending by the time the harmless probe below is armed, and that call
    would raise "a response override is already pending" instead of letting the assertions that
    follow run. Each half uses its own proxy so the two probes can't interfere with each other.
    """
    master = df_factory.create(proactor_threads=2)
    active = df_factory.create(proactor_threads=2, active_replica="true")
    df_factory.start_all([master, active])
    c_master, c_active = master.client(), active.client()
    await c_master.set("k1", "v1")

    # --- REPLCONF DRAKEY-VERSION: sent on every drakeydb replica, peer or not.
    proxy1 = await proxy_factory(master.port)
    await proxy1.override_next_response(
        b"REPLCONF DRAKEY-VERSION ", b"-ERR unknown REPLCONF option\r\n"
    )
    assert await c_active.execute_command(f"REPLICAOF localhost {proxy1.port}") == "OK"
    await proxy1.override_next_response(b"__unused_probe_marker__", b"+PONG\r\n")
    # wait_available_async (PING-based) is not a reliable full-sync-done signal for a peer link:
    # unlike a plain replica's LOADING state, it does not reliably fail PING with
    # BusyLoadingError. wait_for_peers checks sync_in_progress==0 on the peer link itself instead
    # -- the same signal every other fan-in test in this file uses for the same reason.
    await wait_for_peers(c_active, 1)
    assert await c_active.get("k1") == "v1"
    assert await c_active.execute_command(f"REPLICAOF REMOVE localhost {proxy1.port}") == "OK"

    # --- REPLCONF PEER 1: peer-mode only. A fresh proxy and a second key so this half doesn't
    # depend on (or get masked by) the first half's already-merged data.
    await c_master.set("k2", "v2")
    proxy2 = await proxy_factory(master.port)
    await proxy2.override_next_response(b"REPLCONF PEER ", b"-ERR unknown REPLCONF option\r\n")
    assert await c_active.execute_command(f"REPLICAOF localhost {proxy2.port}") == "OK"
    await proxy2.override_next_response(b"__unused_probe_marker__", b"+PONG\r\n")
    await wait_for_peers(c_active, 1)
    assert await c_active.get("k2") == "v2"


async def test_replicaof_real_redis_tolerates_missing_uuid(
    df_factory: DflyInstanceFactory, redis_server, tmp_path
):
    node = df_factory.create(proactor_threads=1, dir=str(tmp_path / "n1"))
    node.start()
    c = node.client()
    import redis.asyncio as aioredis

    r = aioredis.Redis(port=redis_server.port, decode_responses=True)
    try:
        await r.set("k", "v")
        await c.execute_command(f"REPLICAOF localhost {redis_server.port}")
        await wait_available_async(c)
        assert await c.get("k") == "v"
        info = await c.info("replication")
        assert "node_uuid" in info
        assert "master_node_uuid" not in info  # old master answered -ERR; tolerated
    finally:
        await r.aclose()


async def test_active_replica_refuses_redis_master_without_uuid(
    df_factory: DflyInstanceFactory, redis_server, tmp_path
):
    """D2b: an active node's REPLICAOF now requires the source to identify itself via REPLCONF
    UUID. A real Redis master never does, so the handshake must now be refused instead of merging
    -- this reverses this test's own pre-D2b behavior (plain-Redis/stock-Dragonfly fan-in was
    never a stated goal; see PLAN.md's D2b entry). Non-active REPLICAOF against the same real
    Redis master is untouched -- see test_replicaof_real_redis_tolerates_missing_uuid above.

    Falsifying: with the D2b refusal branch reverted to its old "optional uuid" behavior, the
    REPLICAOF call below succeeds instead of raising, and this test fails at pytest.raises.
    """
    node = df_factory.create(
        proactor_threads=2,
        dir=str(tmp_path / "active-redis"),
        active_replica="true",
    )
    node.start()
    c = node.client()
    import redis.asyncio as aioredis

    r = aioredis.Redis(port=redis_server.port, decode_responses=True)
    try:
        await c.set("local-only", "local")
        await r.set("redis-only", "redis")
        # "replication cancelled" is what actually surfaces here, not a message naming D2b/uuid:
        # Replica::Start()'s RETURN_ON_ERR coerces the GenericError from check_connection_error
        # through operator std::error_code(), which drops a string-only GenericError's message
        # (its underlying std::error_code stays default/empty), so Start() falls through to `return
        # {}` and PeerReplicationManager::Add() reports the generic IsContextCancelled() fallback
        # text instead -- the same pre-existing quirk
        # test_active_node_admits_fork_consumers_refuses_others_and_takeover documents for the
        # plain-REPLICAOF-failure case. Verified empirically against this exact
        # scenario (an active node's blocking REPLICAOF against a real, uuid-less Redis master); not
        # this task's bug to fix. match= still ties this to a real REPLICAOF-handshake failure
        # instead of any ResponseError whatsoever.
        with pytest.raises(redis.exceptions.ResponseError, match="replication cancelled"):
            await c.execute_command(f"REPLICAOF localhost {redis_server.port}")
        assert (await c.info("replication"))["connected_masters"] == 0
        # A refused handshake must not merge the source's data, drop our own, or stop writes.
        assert await c.get("local-only") == "local"
        assert await c.get("redis-only") is None
        assert await c.set("still-writable", "yes")
    finally:
        await r.aclose()


async def test_active_replica_merges_redis_full_sync_via_synthetic_uuid(
    df_factory: DflyInstanceFactory, redis_server, proxy_factory, tmp_path
):
    """The peer-mode Redis-protocol full-sync merge branch (replica.cc's InitiatePSync:
    `if (IsPeerMode())` skips the flush and merges instead, and
    `loader.SetOverrideExistingKeys(true)`) is still shipped production code -- it is the
    KeyDB-onboarding path this fork exists for. D2b
    (test_active_replica_refuses_redis_master_without_uuid above) means that branch can no
    longer be reached with a real, unmodified Redis master, since real Redis never sends
    REPLCONF UUID. This test puts a proxy in front of real Redis that answers REPLCONF UUID with
    a synthetic-but-valid uuid -- the closest KeyDB stand-in this environment allows (no real
    keydb-server binary is available here) -- so a peer-mode replica gets past D2b's admission
    check and actually exercises the merge branch, the same way this test's now-deleted P2
    predecessor (test_active_replica_merges_redis_full_sync_and_stays_writable) did before D2b
    withdrew plain-Redis fan-in.

    Falsifying: temporarily reverting InitiatePSync's `if (IsPeerMode())` guard so a peer full
    sync flushes like an ordinary replica (i.e. always taking the `else` FlushAll/FlushSlots
    branch) drops "local-only", and merged()'s first assertion below fails. Verified by hand.
    """
    SYNTHETIC_UUID = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
    node = df_factory.create(
        proactor_threads=2,
        dir=str(tmp_path / "active-redis-proxy"),
        active_replica="true",
    )
    node.start()
    c = node.client()
    import redis.asyncio as aioredis

    r = aioredis.Redis(port=redis_server.port, decode_responses=True)
    proxy = await proxy_factory(redis_server.port)
    try:
        await c.mset({"local-only": "local", "conflict": "local"})
        await r.mset({"redis-only": "redis", "conflict": "redis"})
        # A bare uuid simple-string reply is the KeyDB-compatible format (no " <ms>" suffix) --
        # see ParseReplconfUuidReply / node_identity.h.
        await proxy.override_next_response(b"REPLCONF UUID ", f"+{SYNTHETIC_UUID}\r\n".encode())
        assert await c.execute_command(f"REPLICAOF localhost {proxy.port}") == "OK"
        info = await c.info("replication")
        assert info["connected_masters"] == 1
        assert info["master0"]["node_uuid"] == SYNTHETIC_UUID

        @assert_eventually(times=300)
        async def merged():
            assert await c.get("local-only") == "local"  # not flushed -- peer merge, not replace
            assert await c.get("redis-only") == "redis"  # full sync actually landed
            assert await c.get("conflict") == "redis"  # SetOverrideExistingKeys(true): last wins

        await merged()
        assert await c.set("still-writable", "yes")
    finally:
        await r.aclose()


async def test_harness_gives_each_instance_a_distinct_identity(df_factory: DflyInstanceFactory):
    # No dir= on purpose: these share the session cwd, so without the harness default they would
    # all load the same drakeydb.uuid file.
    a = df_factory.create(proactor_threads=1)
    b = df_factory.create(proactor_threads=1)
    df_factory.start_all([a, b])
    ua = (await a.client().info("replication"))["node_uuid"]
    ub = (await b.client().info("replication"))["node_uuid"]
    assert UUID_RE.match(ua) and UUID_RE.match(ub)
    assert ua != ub
    a.stop()
    a.start()  # same args -> same identity across restart
    assert (await a.client().info("replication"))["node_uuid"] == ua


async def wait_for_peers(c, n, timeout=90):
    """Wait until INFO shows n attached peers, all link up and not syncing; returns the info."""
    async with async_timeout.timeout(timeout):
        while True:
            info = await c.info("replication")
            peers = [info.get(f"master{i}") for i in range(int(info.get("connected_masters", 0)))]
            if len(peers) == n and all(
                p and p["link_status"] == "up" and p["sync_in_progress"] == 0 for p in peers
            ):
                return info
            await asyncio.sleep(0.2)


async def wait_for_peer_links(c, total, up, timeout=90):
    """Wait until INFO shows `total` attached peers and at least `up` established links."""
    async with async_timeout.timeout(timeout):
        while True:
            info = await c.info("replication")
            peers = [info.get(f"master{i}") for i in range(int(info.get("connected_masters", 0)))]
            if (
                len(peers) == total
                and sum(bool(p and p["link_status"] == "up") for p in peers) >= up
            ):
                return info
            await asyncio.sleep(0.2)


async def test_replicaof_flag_list_requires_active_replica(
    df_factory: DflyInstanceFactory, tmp_path
):
    pidfile = tmp_path / "requires-active.pid"
    node = df_factory.create(
        proactor_threads=1,
        pidfile=str(pidfile),
        replicaof="localhost:1,localhost:2",
    )
    with pytest.raises(DflyStartException):
        node.start()
    assert not pidfile.exists()


async def test_replicaof_flag_list_requires_multi_master(df_factory: DflyInstanceFactory, tmp_path):
    pidfile = tmp_path / "requires-multi-master.pid"
    node = df_factory.create(
        proactor_threads=1,
        pidfile=str(pidfile),
        active_replica="true",
        replicaof="localhost:1,localhost:2",
    )
    with pytest.raises(DflyStartException):
        node.start()
    assert not pidfile.exists()


@pytest.mark.parametrize(
    "replicaof",
    [",localhost:1", "localhost:1,", "localhost:1,,localhost:2"],
)
async def test_replicaof_flag_list_rejects_empty_targets(
    df_factory: DflyInstanceFactory, replicaof: str
):
    await assert_start_fails(df_factory.create(proactor_threads=1, replicaof=replicaof))


async def test_replicaof_flag_list_attaches_all_peers(df_factory: DflyInstanceFactory, port_picker):
    b = df_factory.create(proactor_threads=1, port=port_picker.get_available_port())
    c = df_factory.create(proactor_threads=1, port=port_picker.get_available_port())
    df_factory.start_all([b, c])
    a = df_factory.create(
        proactor_threads=2,
        active_replica="true",
        multi_master="true",
        replicaof=f"localhost:{b.port},localhost:{c.port}",
    )
    a.start()
    info = await wait_for_peers(a.client(), 2)
    assert {info["master0"]["port"], info["master1"]["port"]} == {b.port, c.port}


async def test_replicaof_flag_aliases_admit_one_peer_uuid(df_factory: DflyInstanceFactory):
    master = df_factory.create(proactor_threads=2)
    master.start()
    c_master = master.client()
    await c_master.set("alias:counter", 0)

    active = df_factory.create(
        proactor_threads=2,
        active_replica="true",
        multi_master="true",
        replicaof=f"localhost:{master.port},127.0.0.1:{master.port}",
    )
    active.start()
    c_active = active.client()

    await wait_for_peer_links(c_active, total=2, up=1)
    # The losing background link keeps retrying, but must never be admitted while the winner owns
    # this UUID. Give it several reconnect intervals before checking the steady link count.
    await asyncio.sleep(1.5)
    info = await c_active.info("replication")
    peers = [info[f"master{i}"] for i in range(info["connected_masters"])]
    assert sum(p["link_status"] == "up" for p in peers) == 1

    assert await c_master.incr("alias:counter") == 1
    await wait_for_value(c_active, "alias:counter", "1")
    await asyncio.sleep(1.0)
    assert await c_active.get("alias:counter") == "1"

    winner = next(p for p in peers if p["link_status"] == "up")
    assert (
        await c_active.execute_command(f"REPLICAOF REMOVE {winner['host']} {winner['port']}")
        == "OK"
    )
    await wait_for_peers(c_active, 1)
    assert await c_master.incr("alias:counter") == 2
    await wait_for_value(c_active, "alias:counter", "2")


# ---- Phase 2: fan-in ----


def active_args(multi: bool = True, **extra):
    args = {"proactor_threads": 2, "active_replica": "true"}
    if multi:
        args["multi_master"] = "true"
    args.update(extra)
    return args


async def wait_for_value(c, key, value, timeout=30):
    async with async_timeout.timeout(timeout):
        while (await c.get(key)) != value:
            await asyncio.sleep(0.1)


async def attach(c_a, *nodes):
    for n in nodes:
        assert await c_a.execute_command(f"REPLICAOF localhost {n.port}") == "OK"


async def test_fanin_merges_two_masters_and_stays_writable(df_factory: DflyInstanceFactory):
    a = df_factory.create(**active_args())
    b = df_factory.create(proactor_threads=2)
    c = df_factory.create(proactor_threads=2)
    df_factory.start_all([a, b, c])
    c_a, c_b, c_c = a.client(), b.client(), c.client()
    assert (await c_b.info("replication"))["node_uuid"] != (await c_c.info("replication"))[
        "node_uuid"
    ]
    await c_b.execute_command("DEBUG POPULATE 300 b 50")
    await c_c.execute_command("DEBUG POPULATE 300 c 50")
    await c_a.set("a:own", "1")
    await attach(c_a, b, c)  # back-to-back: second REPLICAOF must not be rejected with LOADING
    info = await wait_for_peers(c_a, 2)
    assert info["role"] == "master" and info["active_replica"] == 1 and info["multi_master"] == 1
    assert {info["master0"]["port"], info["master1"]["port"]} == {b.port, c.port}
    assert info["master0"]["node_uuid"] in {
        (await c_b.info("replication"))["node_uuid"],
        (await c_c.info("replication"))["node_uuid"],
    }

    @assert_eventually(times=300)
    async def merged():
        assert await c_a.dbsize() == 601

    await merged()
    assert await c_a.get("a:own") == "1"
    assert await c_a.get("b:0") is not None and await c_a.get("c:0") is not None
    assert await c_a.set("a:new", "2")  # writable, no READONLY
    await c_b.set("live:b", "1")
    await c_c.set("live:c", "1")
    await wait_for_value(c_a, "live:b", "1")
    await wait_for_value(c_a, "live:c", "1")
    # the active node expires its own keys (shards were never flipped into replica mode)
    await c_a.set("a:ttl", "v", px=300)
    await wait_for_value(c_a, "a:ttl", None, timeout=10)
    assert (await c_a.info("replication"))["connected_slaves"] == 0


async def test_fanin_remove_and_no_one_keep_data(df_factory: DflyInstanceFactory):
    a = df_factory.create(**active_args())
    b = df_factory.create(proactor_threads=2)
    c = df_factory.create(proactor_threads=2)
    df_factory.start_all([a, b, c])
    c_a, c_b, c_c = a.client(), b.client(), c.client()
    await c_b.set("b:k", "1")
    await c_c.set("c:k", "1")
    await attach(c_a, b, c)
    await wait_for_peers(c_a, 2)
    await wait_for_value(c_a, "b:k", "1")
    await wait_for_value(c_a, "c:k", "1")
    assert await c_a.execute_command(f"REPLICAOF REMOVE localhost {b.port}") == "OK"
    info = await wait_for_peers(c_a, 1)
    assert info["master0"]["port"] == c.port
    assert await c_a.get("b:k") == "1"  # data delivered by B stays
    await c_b.set("b:after", "1")
    await asyncio.sleep(1.0)
    assert await c_a.get("b:after") is None  # B is detached
    with pytest.raises(redis.exceptions.ResponseError, match="Not attached"):
        await c_a.execute_command(f"REPLICAOF REMOVE localhost {b.port}")
    assert await c_a.execute_command("REPLICAOF NO ONE") == "OK"
    info = await c_a.info("replication")
    assert info["connected_masters"] == 0 and info["role"] == "master"
    assert await c_a.get("c:k") == "1"
    assert await c_a.set("still", "writable")


async def test_active_node_admits_fork_consumers_refuses_others_and_takeover(
    df_factory: DflyInstanceFactory,
):
    """P3 T7 updates this P2 regression: an active node's admission gate (server_family.cc
    ReplConf, the CAPA dragonfly case) moved from a blanket top-of-function refusal to a check
    that admits a consumer completing the fork handshake (REPLCONF DRAKEY-VERSION, sent by every
    drakeydb replica's Greet). `d` below is a real drakeydb node, so its REPLICAOF now succeeds
    as a plain (non-peer, full-stream) replica instead of being refused -- `d` never sends
    REPLCONF PEER, so it isn't asking for peer mode. A raw consumer that never announces itself
    (a stock Dragonfly, an older drakeydb, or anything speaking plain Redis replication) is still
    refused, with the same error text the old blanket check used. REPLTAKEOVER is untouched by
    this task and stays refused unconditionally.

    Falsifying: reverting the CAPA dragonfly admission check to the old blanket refusal makes
    `d`'s REPLICAOF below raise instead of succeeding, and connected_slaves stays 0.
    """
    a = df_factory.create(**active_args())
    d = df_factory.create(proactor_threads=2)
    df_factory.start_all([a, d])
    c_a, c_d = a.client(), d.client()

    with pytest.raises(redis.exceptions.ResponseError, match="active-replica"):
        await c_a.execute_command("REPLTAKEOVER 1")

    # A raw, unidentified consumer -- no REPLCONF UUID/DRAKEY-VERSION ever sent on this
    # connection -- is refused right at CAPA dragonfly, same as a stock Dragonfly or a pre-P3
    # drakeydb build would be.
    with pytest.raises(redis.exceptions.ResponseError, match="active-replica"):
        await c_a.execute_command("REPLCONF capa dragonfly")

    # A real drakeydb node's REPLICAOF completes the full fork handshake and is admitted.
    assert await c_d.execute_command(f"REPLICAOF localhost {a.port}") == "OK"
    await wait_available_async(c_d)
    info_d = await c_d.info("replication")
    assert info_d["master_link_status"] == "up"
    assert (await c_a.info("replication"))["connected_slaves"] == 1


async def test_plain_replica_of_active_node_gets_full_unfiltered_stream(
    df_factory: DflyInstanceFactory,
):
    """The failure mode that matters most for P3 T7: a PLAIN (non-peer) replica must never get a
    filtered stream. JournalStreamer::ShouldWrite's `if (!config_.peer_mode) return true;` (P3
    T5, streamer.cc) is what guarantees this once peer_mode is false; T7 is what actually sets
    config_.peer_mode per consumer (DflyCmd::StartStableSyncInThread, fed from
    ReplicaInfo::IsPeer(), which server_family.cc's admission check only ever sets true for a
    REPLCONF PEER 1 request). Getting that boolean backwards -- peer_mode wired true for a plain
    consumer -- would silently drop every entry the active node relays in from its OWN peers
    (ShouldWrite's origin_idx != kSelfIdx branch) while leaving the active node's own writes
    untouched, so a test that only checks the active node's own writes reach the replica would
    keep passing under that bug. This test specifically writes on B (A's fan-in source, never
    directly connected to D) and checks B's write reaches D purely by relay through A.

    Falsifying: hardcoding peer_mode=true in StartStableSyncInThread's JournalStreamer::Config
    (in place of the peer_mode parameter) makes "relayed-through-a" never appear on D, and
    wait_for_value below times out.
    """
    a = df_factory.create(**active_args())
    b = df_factory.create(proactor_threads=2)
    d = df_factory.create(proactor_threads=2)
    df_factory.start_all([a, b, d])
    c_a, c_b, c_d = a.client(), b.client(), d.client()

    await attach(c_a, b)
    await wait_for_peers(c_a, 1)
    assert await c_d.execute_command(f"REPLICAOF localhost {a.port}") == "OK"
    await wait_available_async(c_d)

    await c_b.set("relayed-through-a", "1")
    await c_a.set("a-own", "1")

    await wait_for_value(c_d, "a-own", "1")
    await wait_for_value(c_d, "relayed-through-a", "1")


async def test_peer_mesh_own_writes_not_echoed_back(df_factory: DflyInstanceFactory):
    """P3 T7's core guarantee: an active node's outbound peer stream carries only its OWN writes
    (JournalStreamer::ShouldWrite's peer_mode gate, streamer.cc), so a write is never echoed back
    toward the peer it came from. Two active nodes in a mutual peer mesh -- each REPLICAOF-ing
    the other, so both links are peer-admitted (replica.cc's Greet sends REPLCONF PEER 1
    whenever IsPeerMode(), which peer_replication.cc sets on every Replica an active node creates
    for its own masters -- see ReplicaOfInternal's `if (IsActiveReplica())` routing in
    server_family.cc) -- is the smallest topology where a forwarding bug is observable in the
    FINAL STATE, not just on the wire. INCR is not idempotent, so an echoed-back replay of an
    already-applied entry inflates the counter deterministically -- unlike a merely stale key,
    which a slow/flaky link could also explain. In THIS test's mutual mesh, the same ShouldWrite
    guards both outbound streams at once, so a break does not stay at a fixed +1: Falsifying
    below documents it compounding into unbounded amplification instead.

    This is genuinely new coverage: every other fan-in test in this file attaches at most one
    active node to plain masters, so this is the first test where an active node is ever on the
    RECEIVING end of a peer-filtered stream (as opposed to only ever being the one requesting
    PEER 1) -- i.e. the first test where peer_mode has ever been true on a live production
    JournalStreamer at all, active or not.

    Falsifying (two independent reverts, both confirmed by hand): (1) hardcoding peer_mode=false
    in StartStableSyncInThread's JournalStreamer::Config (undoing this task's wiring entirely, so
    every consumer gets a full stream) makes the mesh itself never form cleanly -- B's REPLICAOF A
    times out during attach, since both links silently disagree with what the peer-mode receive
    path expects. (2) The narrower revert -- disabling only ShouldWrite's `origin_idx !=
    kSelfIdx` check (streamer.cc), leaving peer_mode and every other filter untouched -- lets
    exactly the echo path back in. That check is the only thing stopping re-forwarding in EITHER
    direction, and both A's and B's outbound streams share the same ShouldWrite, so disabling it
    breaks the loop-guard symmetrically: one INCR on A amplifies through an unbounded A<->B
    ping-pong. The existence poll below (not an exact-match wait_for_value) only guards "did the
    first hop land at all" -- it does not race the storm -- so the echo itself surfaces as a hard
    value assertion two lines later: observed `assert await c_a.get("cnt") == "1"` failing with
    `AssertionError: assert equals failed / -'80' / +'1'` (an earlier run of the same revert, on
    a raw sleep+read probe instead of this test's own assertions, saw cnt at 72 within 1s of the
    single INCR, climbing ~80/s indefinitely on both sides -- consistent with this run's 80).
    Neither revert leaves this test passing.
    """
    a = df_factory.create(**active_args())
    b = df_factory.create(**active_args())
    df_factory.start_all([a, b])
    c_a, c_b = a.client(), b.client()

    # Sequential, not concurrent: a node mid full-sync-load answers PING with -LOADING, which
    # the connecting side's Greet treats as a hard handshake failure -- so B's REPLICAOF A must
    # wait for A's own full sync (from attach(c_a, b)) to finish before it can succeed.
    await attach(c_a, b)  # A REPLICAOF B
    await wait_for_peers(c_a, 1)
    await attach(c_b, a)  # B REPLICAOF A
    await wait_for_peers(c_b, 1)

    assert await c_a.incr("cnt") == 1
    # Existence, not an exact-match wait_for_value poll: an equality poll can race past
    # "1" if it climbs faster than the poll interval (see the amplification-storm evidence
    # below), turning a real echo into a misleading TimeoutError instead of a wrong value.
    async with async_timeout.timeout(30):
        while not await c_b.exists("cnt"):  # confirm the direct A->B hop landed
            await asyncio.sleep(0.1)
    # Give a would-be echo hop (B->A, carrying A's own write back) time to arrive and misapply.
    await asyncio.sleep(1.0)
    assert await c_a.get("cnt") == "1"
    assert await c_b.get("cnt") == "1"

    # The mesh is still healthy end to end in the other direction too.
    assert await c_b.incr("cnt2") == 1
    async with async_timeout.timeout(30):
        while not await c_a.exists("cnt2"):
            await asyncio.sleep(0.1)
    await asyncio.sleep(1.0)
    assert await c_a.get("cnt2") == "1"
    assert await c_b.get("cnt2") == "1"


# D-7 review round 2: fixed, ordered uuids so a test that DOES observe the tiebreak firing knows
# in advance which side MUST be the one that deferred -- ShouldRefuseReciprocalPeer refuses iff
# self_uuid < peer_uuid (peer_replication.h/.cc), so the larger uuid's own outbound link is
# always the one that can be refused. Summing both sides' find_in_logs results (as an earlier
# version of one of these tests did) cannot tell "the right side deferred" apart from "the wrong
# side deferred, by a swapped-operand bug" -- both produce one non-empty match.
TIEBREAK_UUID_LO = "10000000-0000-4000-8000-000000000000"
TIEBREAK_UUID_HI = "20000000-0000-4000-8000-000000000000"

# D-7 review round 3, item 1: a single reciprocal-attach attempt only observes the tiebreak
# firing 70-100% of the time (round 3 measured 9/10 and 10/10 on two different scenarios; see
# task-8-report.md) -- narrowing the "unestablished" window to the handshake alone (round 2's
# fix 1) also narrows how often two independently-timed handshakes happen to overlap enough for
# either side to observe the race at all. Bounding a retry loop at this many attempts keeps the
# whole loop's miss probability under 1% even at the pessimistic 70% single-attempt rate:
# 0.3 ** MAX_TIEBREAK_ATTEMPTS ~= 0.24%.
MAX_TIEBREAK_ATTEMPTS = 5


def _assert_tiebreak_correct_if_observed(a, b) -> bool:
    """Checks the reciprocal-connect uuid tiebreak's *direction* whenever there is log evidence
    to check on this attempt, and returns whether it fired at all (on either side) so callers can
    track that separately across repeated attempts -- see
    test_simultaneous_reciprocal_replicaof_converges's bounded observe-loop and
    MAX_TIEBREAK_ATTEMPTS above.

    Narrowing the tiebreak's "unestablished" window to the handshake alone (round 2's fix 1) also
    narrows how long the reciprocal race lasts -- both sides' handshakes are a handful of small,
    fast REPLCONF round trips, so it is common (round 3 measured ~1-in-4 to ~1-in-3 across
    repeated single-shot runs) for NEITHER side's own claim to still be unestablished when the
    other's admission check runs: both connections are simply admitted, and the tiebreak never
    fires at all. That is not a bug -- ShouldRefuseReciprocalPeer is still correct whenever both
    sides do observe the race (see its own C++ unit test, which pins the predicate directly and
    deterministically), and D-7 explicitly tolerates this exact case ("if both sides check before
    either has started serving, the retry path still resolves it").

    An earlier version of these tests tried to force the race deterministically with a
    byte-delaying TCP relay in front of one side's connection. That approach was abandoned: it
    reliably introduced multi-minute hangs of its own (the relay sees Replica::Greet() AND the
    DFLY full-sync protocol that follows it on the same connection -- and, for peer links, on
    every per-shard flow connection dialed to the same target -- so bounding the delay to a
    fixed chunk count still could not cleanly separate "delay the handshake" from "delay the
    sync data" without much deeper protocol-aware interception than a generic relay can safely
    do). See task-8-report.md for the fuller writeup.

    Hard, unconditional assertion whenever fired=True -- this IS the "if observed" conditional
    the name promises (an earlier body only ever computed a_deferred, never read b, and had no
    branch at all; fixed here to read both a and b and to actually branch on whether either
    fired): b (the larger uuid) must be the one that deferred, and a (the smaller uuid) must not
    have. That specific wrong-direction combination can only mean ShouldRefuseReciprocalPeer's
    operands are swapped -- a real regression, never a timing artifact -- so it is never silently
    tolerated, fired or not, on any attempt.
    """
    a_deferred = bool(a.find_in_logs("reciprocal-connect uuid tiebreak"))
    b_deferred = bool(b.find_in_logs("reciprocal-connect uuid tiebreak"))
    fired = a_deferred or b_deferred
    if fired:
        # a (the smaller uuid) is never eligible to be the one refused -- ShouldRefuseReciprocalPeer
        # refuses iff self_uuid < peer_uuid, so only b's own outbound link (self_uuid = the larger
        # uuid) can ever be the refused side.
        assert b_deferred and not a_deferred, (
            "reciprocal-connect uuid tiebreak fired in the wrong direction -- "
            f"a_deferred={a_deferred} b_deferred={b_deferred}; ShouldRefuseReciprocalPeer's "
            "operands must be swapped (b, the larger uuid, should be the one that can be "
            "refused, never a)"
        )
        # D-7 review round 3, item 2: make a firing run distinguishable in the log from a
        # non-firing one (see the else branch) instead of both looking silently identical.
        # logging.warning (not info) so this shows under pytest.ini's default log_cli level
        # regardless of -q/-v.
        logging.warning(
            "reciprocal-connect uuid tiebreak fired correctly this attempt (b deferred, a did "
            "not)"
        )
    else:
        logging.warning(
            "reciprocal-connect uuid tiebreak did NOT fire on either side this attempt -- both "
            "connections were naturally admitted (D-7 tolerates this on any single attempt; see "
            "this function's own docstring)"
        )
    return fired


async def test_simultaneous_reciprocal_replicaof_converges(
    df_factory: DflyInstanceFactory, port_picker
):
    """D-7: when two active nodes' --replicaof boot flags point at each other, both start racing
    to full-sync from the other at nearly the same instant. The reciprocal-connect uuid tiebreak
    (server_family.cc's ReplConf admission check, backed by
    PeerReplicationManager::HasUnestablishedPeerWithUuid / ShouldRefuseReciprocalPeer in
    peer_replication.{h,cc}) settles this deterministically WHENEVER both sides observe the
    race: the side whose own uuid sorts later (`b`, TIEBREAK_UUID_HI) has its outbound link
    refused with a retryable error while `a`'s outbound link to it is still unestablished, so
    only one direction pays for the first full sync; the refused side's Greet() (replica.cc)
    retries on its normal 500ms loop and is admitted normally once the winner's link is up.

    Two boot-time --replicaof flags (StartMode::kBackground), not two interactive REPLICAOF
    commands gathered via asyncio.gather: an interactive REPLICAOF is a *single* blocking
    handshake attempt (PeerReplicationManager::Add's kBlockingHandshake); losing the tiebreak on
    that one attempt now falls back to the background and still returns OK (D-7 review round 2 --
    see test_reciprocal_blocking_replicaof_falls_back_to_background_and_converges below for that
    path specifically). Boot-time --replicaof exercises the plain 500ms retry loop
    (Replica::MainReplicationFb) directly, and is also the natural way "two active nodes
    REPLICAOF each other simultaneously" actually happens when bringing up a mesh from static
    config. df_factory.start_all() launches both processes back to back (not
    one-after-the-other-and-wait), so both sides' outbound connection attempts genuinely race.

    D-7 review round 3, item 1: a single attempt does not reliably observe the race (see
    _assert_tiebreak_correct_if_observed's own docstring -- ~70-100% per attempt), so this test
    repeats the whole boot-attach-converge-teardown scenario -- a fresh node pair and fresh ports
    every time -- up to MAX_TIEBREAK_ATTEMPTS times, stopping as soon as one attempt observes the
    tiebreak firing. This is a bounded OBSERVE-loop, not a retry-until-pass loop: every attempt's
    mesh convergence is asserted unconditionally (a convergence failure fails the test immediately,
    full stop, no retry), and every attempt that DOES observe the tiebreak firing has its
    direction asserted hard and immediately via _assert_tiebreak_correct_if_observed -- a
    wrong-direction firing fails on the attempt it happens, unconditionally, and is never "tried
    again" away. The only thing the loop tolerates across attempts is the race's own absence,
    which round 3 established is an expected, bounded-probability outcome of narrowing the window
    to the handshake, not a failure. If the tiebreak is never observed in MAX_TIEBREAK_ATTEMPTS
    attempts, the test fails: this restores (in a form that tolerates the race's genuine
    non-determinism instead of flaking on it) the positive assertion that a plain
    `assert b.find_in_logs(...)` used to provide before round 3 weakened this test to purely
    conditional -- with the tiebreak call site itself deleted, no attempt can ever observe it, so
    this test fails deterministically regardless of the attempt budget.

    Falsifying: see task-8-report.md.
    """
    observed_tiebreak = False
    for attempt in range(1, MAX_TIEBREAK_ATTEMPTS + 1):
        a_port = port_picker.get_available_port()
        b_port = port_picker.get_available_port()
        a = df_factory.create(
            **active_args(port=a_port, replicaof=f"localhost:{b_port}", node_uuid=TIEBREAK_UUID_LO)
        )
        b = df_factory.create(
            **active_args(port=b_port, replicaof=f"localhost:{a_port}", node_uuid=TIEBREAK_UUID_HI)
        )
        df_factory.start_all([a, b])
        c_a, c_b = a.client(), b.client()

        await asyncio.gather(wait_for_peers(c_a, 1), wait_for_peers(c_b, 1))

        # The mesh actually converges both ways, not just link_status flipping to up --
        # unconditional, every attempt, regardless of whether the tiebreak fires on it.
        assert await c_a.incr("cnt") == 1
        await wait_for_value(c_b, "cnt", "1")
        assert await c_b.incr("cnt2") == 1
        await wait_for_value(c_a, "cnt2", "1")

        a.stop()
        b.stop()
        # Hard, unconditional per-attempt check: if the tiebreak fired, it must have fired in the
        # correct direction. Only "did it fire at all" is allowed to vary across attempts.
        if _assert_tiebreak_correct_if_observed(a, b):
            observed_tiebreak = True
            logging.warning(
                "reciprocal-connect uuid tiebreak observed on attempt %d/%d",
                attempt,
                MAX_TIEBREAK_ATTEMPTS,
            )
            break

    assert observed_tiebreak, (
        f"reciprocal-connect uuid tiebreak was never observed in {MAX_TIEBREAK_ATTEMPTS} "
        "attempts -- either the admission wiring (server_family.cc's ReplConf tiebreak call) is "
        "broken/missing, or this is an astronomically unlucky run at the measured 70-100% "
        "per-attempt firing rate"
    )


async def test_narrowed_window_admits_peer_during_winners_full_sync(
    df_factory: DflyInstanceFactory, port_picker
):
    """D-7 review round 2 (fix 1): the reciprocal-connect tiebreak's "unestablished" window is
    scoped to the handshake only (Replica::Greet(), R_GREETED) -- not the full sync that follows
    it (R_SYNC_OK). Before this fix, a peer link stayed "unestablished" (eligible to refuse a
    reciprocal connect) for the sync's *entire* duration, so an ordinary, non-racing REPLICAOF
    issued on the peer while the winner was still mid-sync would lose a uuid coin flip and get
    refused -- turning sequential mesh bring-up into an intermittent failure over seconds to
    minutes, not settling a genuine microsecond handshake-time race.

    `winner` (TIEBREAK_UUID_LO) pulls a large dataset from `loser` (TIEBREAK_UUID_HI), so
    winner's own full sync from loser takes a real, observable amount of time -- during which
    winner has long since passed its own handshake with loser (R_GREETED) but not yet R_SYNC_OK.
    While winner is confirmed still mid-sync, `loser` issues REPLICAOF winner (the reciprocal
    direction: loser pulling from winner). Under the old wide window, winner's own claim on loser
    would still read "unestablished" here, so the tiebreak would incorrectly refuse loser's
    REPLICAOF for the sync's entire duration -- an ordinary sequential attach, not a race.

    D-7 review round 3: winner being genuinely mid full sync also means winner's own process is
    in GlobalState::LOADING for that whole window (SyncGate/RequestExclusiveLoadingState), which
    independently rejects even loser's very first PING (main_service.cc's LOADING gate has no
    exemption for a not-yet-established connection) -- a real, reproduced failure mode entirely
    unrelated to the tiebreak that surfaced through the exact same generic "-ERR replication
    cancelled" coercion (see Greet()'s own PING-time LOADING check and task-8-report.md). Both
    the tiebreak's own narrowed-window fallback and this LOADING fallback make
    PeerReplicationManager::Add reply OK from the same code path (round 2 fix 2), so the
    `== "OK"` assertion below no longer means "admitted immediately, without hitting either
    transient" -- it did in round 2, but round 3's LOADING fallback removed that guarantee. All
    `== "OK"` proves now is that the command did not hard-fail; that is satisfied equally by true
    immediate admission and by either fallback silently backstopping it. The assertion that
    actually targets THIS test's own regression (a reverted, wide "unestablished" window) is the
    negative log check after teardown, not the `== "OK"` check.

    That negative log check is itself only a DETERMINISTIC catch of a wide-window regression for
    the slice of R_SYNCING before winner calls EnterLoadingState(): once winner's process has
    actually flipped to GlobalState::LOADING, loser's PING is rejected by the LOADING gate before
    the connection ever reaches REPLCONF capa dragonfly (where the tiebreak itself lives), so a
    reverted wide window would produce no tiebreak log line in that (later, and likely more
    common) slice either -- indistinguishable in this test from the fix being correct. The
    `sync_in_progress == 1` poll below does not (and cannot cheaply) pin down which slice of
    R_SYNCING loser's REPLICAOF actually lands in. This test does not need to (and does not)
    distinguish which of the two transient causes applies on a given run: both are covered by
    PeerReplicationManager::Add's fallback, and a real regression in either one still fails this
    test loudly via convergence or via the log check. But this test's regression coverage for a
    wide-window revert specifically is PROBABILISTIC, not deterministic -- it depends on loser's
    connection landing in the pre-EnterLoadingState sliver of the race, the same kind of
    non-determinism (for a different underlying reason) that
    _assert_tiebreak_correct_if_observed's own docstring documents for the other two tests in
    this file.

    Falsifying: see task-8-report.md.
    """
    winner_port = port_picker.get_available_port()
    loser_port = port_picker.get_available_port()
    winner = df_factory.create(**active_args(port=winner_port, node_uuid=TIEBREAK_UUID_LO))
    loser = df_factory.create(**active_args(port=loser_port, node_uuid=TIEBREAK_UUID_HI))
    df_factory.start_all([winner, loser])
    c_winner, c_loser = winner.client(), loser.client()

    # Enough data that winner's full sync from loser takes multiple seconds, not milliseconds.
    await c_loser.execute_command("DEBUG POPULATE 700000 k 500")

    assert await c_winner.execute_command(f"REPLICAOF localhost {loser_port}") == "OK"

    # Confirm winner is genuinely mid full sync -- not accidentally already done, which would
    # make this test pass even with the old, wide window too.
    async with async_timeout.timeout(30):
        while True:
            info = await c_winner.info("replication")
            m0 = info.get("master0")
            if m0 and m0.get("sync_in_progress") == 1:
                break
            await asyncio.sleep(0.02)

    # This must not hard-fail -- but see the docstring: OK alone no longer proves immediate
    # admission, since round 3's LOADING fallback replies OK the same way the tiebreak's own
    # narrowed-window fallback does. Convergence is confirmed below regardless; the log check
    # after teardown is what actually targets a wide-window regression (see docstring for that
    # check's own, narrower, probabilistic guarantee).
    assert await c_loser.execute_command(f"REPLICAOF localhost {winner_port}") == "OK"
    await wait_for_peers(c_loser, 1)

    winner.stop()
    loser.stop()
    # Deterministic only for the pre-EnterLoadingState slice of the race -- see docstring.
    assert not loser.find_in_logs("reciprocal-connect uuid tiebreak"), (
        "loser's REPLICAOF was refused while winner was still mid full sync -- the "
        "'unestablished' window is still too wide"
    )


async def test_reciprocal_blocking_replicaof_falls_back_to_background_and_converges(
    df_factory: DflyInstanceFactory,
):
    """D-7 review round 2 (fix 2): an interactive (blocking) REPLICAOF that loses the
    reciprocal-connect uuid tiebreak on its one-shot PeerReplicationManager::Add
    kBlockingHandshake attempt must NOT fail the command -- it must publish the peer, fall back
    to the background retry loop, and reply OK, exactly as if EnableReplication() (kBackground)
    had been used from the start. Before this fix, the losing side's REPLICAOF got
    "-ERR replication cancelled" back -- byte-identical to a DNS failure or an own-uuid refusal,
    and strictly worse than pre-task behavior (both used to just succeed).

    Both interactive REPLICAOFs are gathered concurrently -- the literal "two active nodes
    REPLICAOF each other simultaneously" scenario -- which only became testable this way once
    this fallback existed (see test_simultaneous_reciprocal_replicaof_converges's own docstring
    for why a bare kBlockingHandshake gather could not be used before this fix). Both commands
    returning "OK" is checked unconditionally: that holds whether or not the tiebreak happens to
    fire on this particular run (a naturally-admitted pair also returns OK, OK), but it would
    fail loudly -- asyncio.gather propagating a ResponseError -- if the tiebreak DOES fire and
    the fallback this test exists for is broken. See
    _assert_tiebreak_correct_if_observed's own docstring for why this test does not force (and
    does not require) the tiebreak to actually fire on any given run; test_narrowed_window_
    admits_peer_during_winners_full_sync is what reliably drives a blocking REPLICAOF through
    this same fallback machinery (there, primarily via the LOADING transient rather than the
    tiebreak -- see that test's own docstring).

    Falsifying: see task-8-report.md.
    """
    a = df_factory.create(**active_args(node_uuid=TIEBREAK_UUID_LO))
    b = df_factory.create(**active_args(node_uuid=TIEBREAK_UUID_HI))
    df_factory.start_all([a, b])
    c_a, c_b = a.client(), b.client()

    results = await asyncio.gather(
        c_a.execute_command(f"REPLICAOF localhost {b.port}"),
        c_b.execute_command(f"REPLICAOF localhost {a.port}"),
    )
    assert results == ["OK", "OK"], (
        "both interactive REPLICAOFs must return OK even when one loses the tiebreak -- got "
        f"{results}"
    )

    await asyncio.gather(wait_for_peers(c_a, 1), wait_for_peers(c_b, 1))
    assert await c_a.incr("cnt") == 1
    await wait_for_value(c_b, "cnt", "1")
    assert await c_b.incr("cnt2") == 1
    await wait_for_value(c_a, "cnt2", "1")

    a.stop()
    b.stop()
    _assert_tiebreak_correct_if_observed(a, b)


async def test_non_tiebreak_blocking_replicaof_failure_still_fails_command(
    df_factory: DflyInstanceFactory,
):
    """D-7 review round 2: the background fallback in PeerReplicationManager::Add must trigger
    ONLY on the reciprocal-connect tiebreak's specific errc (Replica::LastGreetEc() ==
    std::errc::device_or_resource_busy). An unrelated blocking-handshake failure -- here, an
    unreachable host, so Start() fails during DNS resolution and never even reaches Greet() --
    must still fail the REPLICAOF command outright and leave no peer attached, exactly as before
    this task. Silently backgrounding a genuine misconfiguration (bad host, own uuid, a uuid
    already claimed by another live peer) would hide it from the operator instead of reporting it.

    Falsifying: see task-8-report.md.
    """
    node = df_factory.create(**active_args())
    node.start()
    c = node.client()

    with pytest.raises(redis.exceptions.ResponseError):
        await c.execute_command("REPLICAOF invalidhost 1")

    info = await c.info("replication")
    assert info.get("connected_masters", 0) == 0


async def test_drakey_handshake_pairs_no_longer_log_parse_errors(df_factory: DflyInstanceFactory):
    """P3 T7: REPLCONF DRAKEY-VERSION / PEER are now parsed by every drakeydb master (active or
    not), not just tolerated by the replica -- see server_family.cc ReplConf. Before this task,
    every drakeydb->drakeydb handshake hit the unrecognized-REPLCONF fallback on the master and
    logged an ERROR once per pair actually sent (twice here, since `active` is a peer and so also
    sends REPLCONF PEER). That log line is a real diagnostic signal in production (a genuinely
    unrecognized REPLCONF option, or a protocol mismatch); a drakeydb master that can't parse its
    own fork's handshake pairs would be a regression worth flagging loudly, so this pins its
    absence explicitly rather than leaving it as incidental background noise nobody checks.

    Falsifying: reverting the DRAKEY-VERSION/PEER cases in ReplConf back to falling through to
    the unrecognized-option branch makes the find_in_logs call below return a non-empty list.
    """
    master = df_factory.create(proactor_threads=2)
    active = df_factory.create(proactor_threads=2, active_replica="true")
    df_factory.start_all([master, active])
    c_master, c_active = master.client(), active.client()
    await c_master.set("k", "v")
    assert await c_active.execute_command(f"REPLICAOF localhost {master.port}") == "OK"
    await wait_for_peers(c_active, 1)
    assert await c_active.get("k") == "v"
    assert await c_active.execute_command(f"REPLICAOF REMOVE localhost {master.port}") == "OK"

    active.stop()
    master.stop()
    assert master.find_in_logs("Error in receiving command") == []


async def test_same_uuid_peer_refused(df_factory: DflyInstanceFactory):
    a = df_factory.create(**active_args())
    a.start()
    c_a = a.client()
    clone = df_factory.create(
        proactor_threads=1, node_uuid=(await c_a.info("replication"))["node_uuid"]
    )
    clone.start()
    with pytest.raises(redis.exceptions.ResponseError):
        await c_a.execute_command(f"REPLICAOF localhost {clone.port}")
    assert (await c_a.info("replication"))["connected_masters"] == 0


async def test_same_master_alias_endpoint_refused(df_factory: DflyInstanceFactory):
    a = df_factory.create(**active_args())
    master = df_factory.create(proactor_threads=2)
    df_factory.start_all([a, master])
    c_a, c_master = a.client(), master.client()
    await c_master.set("blocking-alias:counter", 0)

    assert await c_a.execute_command(f"REPLICAOF localhost {master.port}") == "OK"
    await wait_for_peers(c_a, 1)
    with pytest.raises(redis.exceptions.ResponseError):
        await c_a.execute_command(f"REPLICAOF 127.0.0.1 {master.port}")
    await wait_for_peers(c_a, 1)

    assert await c_master.incr("blocking-alias:counter") == 1
    await wait_for_value(c_a, "blocking-alias:counter", "1")
    await asyncio.sleep(1.0)
    assert await c_a.get("blocking-alias:counter") == "1"


async def test_active_replica_single_peer_replaces(df_factory: DflyInstanceFactory):
    a = df_factory.create(**active_args(multi=False))
    b = df_factory.create(proactor_threads=2)
    c = df_factory.create(proactor_threads=2)
    df_factory.start_all([a, b, c])
    c_a, c_b = a.client(), b.client()
    await c_b.set("b:k", "1")
    await attach(c_a, b)
    await wait_for_peers(c_a, 1)
    await wait_for_value(c_a, "b:k", "1")
    await attach(c_a, c)
    info = await wait_for_peers(c_a, 1)
    assert info["multi_master"] == 0 and info["master0"]["port"] == c.port
    assert await c_a.get("b:k") == "1"


async def test_fanin_restart_remerge(df_factory: DflyInstanceFactory, tmp_path, port_picker):
    a_port = port_picker.get_available_port()
    a_args = active_args(dir=str(tmp_path / "a"), dbfilename="dump", port=a_port)
    a = df_factory.create(**a_args)
    b = df_factory.create(proactor_threads=2, port=port_picker.get_available_port())
    c = df_factory.create(proactor_threads=2, port=port_picker.get_available_port())
    df_factory.start_all([a, b, c])
    c_a, c_b, c_c = a.client(), b.client(), c.client()
    await c_b.execute_command("DEBUG POPULATE 200 b 50")
    await c_c.execute_command("DEBUG POPULATE 200 c 50")
    await attach(c_a, b, c)
    await wait_for_peers(c_a, 2)
    await c_a.set("a:own", "1")
    await wait_for_value(c_a, "b:199", await c_b.get("b:199"))  # B fully merged
    await wait_for_value(c_a, "c:199", await c_c.get("c:199"))  # C fully merged
    a.stop()  # dbfilename is set -> snapshot on shutdown
    await c_b.set("b:while_down", "1")
    a2 = df_factory.create(**a_args, replicaof=f"localhost:{b.port},localhost:{c.port}")
    a2.start()
    c_a2 = a2.client()
    await wait_for_peers(c_a2, 2)
    assert await c_a2.get("a:own") == "1"  # own snapshot was loaded, not suppressed by --replicaof
    await wait_for_value(c_a2, "b:while_down", "1")

    @assert_eventually(times=300)
    async def remerged():
        assert await c_a2.dbsize() == 200 + 200 + 1 + 1

    await remerged()
    assert await c_a2.set("after:restart", "ok")


async def test_multi_master_flag_requires_active_replica(df_factory: DflyInstanceFactory):
    await assert_start_fails(df_factory.create(proactor_threads=1, multi_master="true"))


async def test_active_replica_rejects_cluster_mode(df_factory: DflyInstanceFactory):
    await assert_start_fails(
        df_factory.create(proactor_threads=1, active_replica="true", cluster_mode="emulated")
    )
