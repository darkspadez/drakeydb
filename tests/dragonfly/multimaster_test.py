"""Multi-master identity and active-replica fan-in tests."""

import asyncio
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
    """ConfigureDflyMaster (replica.cc) sends REPLCONF DRAKEY-VERSION on every drakeydb replica,
    and additionally REPLCONF PEER 1 in peer mode. Our own master doesn't recognize either pair
    yet (the master-side parser lands in a later task) and answers -ERR; the replica must
    tolerate that and keep replicating regardless -- this is load-bearing, not optional, until
    that lands.

    Falsifying: override_next_response only clears its pending override once its marker is
    actually seen on the wire (see proxy.py), and refuses to arm a second one while the first is
    still pending. Greet() -- and thus the blocking REPLICAOF below -- only returns once
    ConfigureDflyMaster has finished sending both REPLCONF pairs for that attempt, so if either
    stopped being sent, its override would still be pending by the time the harmless probe below
    is armed, and that call would raise "a response override is already pending" instead of
    letting the assertions that follow run. Each half uses its own proxy so the two probes can't
    interfere with each other.
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
        with pytest.raises(redis.exceptions.ResponseError):
            await c.execute_command(f"REPLICAOF localhost {redis_server.port}")
        assert (await c.info("replication"))["connected_masters"] == 0
        # A refused handshake must not merge the source's data, drop our own, or stop writes.
        assert await c.get("local-only") == "local"
        assert await c.get("redis-only") is None
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


async def test_active_node_refuses_consumers_and_takeover(df_factory: DflyInstanceFactory):
    a = df_factory.create(**active_args())
    d = df_factory.create(proactor_threads=2)
    df_factory.start_all([a, d])
    c_a, c_d = a.client(), d.client()
    with pytest.raises(redis.exceptions.ResponseError, match="active-replica"):
        await c_a.execute_command("REPLCONF listening-port 1")
    with pytest.raises(redis.exceptions.ResponseError, match="active-replica"):
        await c_a.execute_command("REPLTAKEOVER 1")
    # A plain node pointing at A fails its synchronous greet (A refuses REPLCONF). Start()'s
    # check_connection_error() builds a GenericError from a plain string ("could not greet
    # master ..."), but ReplicaOfInternal's RETURN_ON_ERR coerces that GenericError through
    # operator std::error_code(), which returns only the (empty) ec_ and silently drops the
    # string. So Start() reports success, and ReplicaOfInternal falls back to its generic
    # "replication cancelled" text (see server_family.cc ReplicaOfInternal, and replica.cc
    # check_connection_error). This is deterministic, independent of active/peer mode, and
    # already pinned for the plain-REPLICAOF-failure case by
    # MultiMasterFamilyTest.ReplicaOfGrammarAndNoPeersPaths in multi_master_test.cc.
    with pytest.raises(redis.exceptions.ResponseError, match="replication cancelled"):
        await c_d.execute_command(f"REPLICAOF localhost {a.port}")
    assert (await c_d.info("replication"))["role"] == "master"
    assert (await c_a.info("replication"))["connected_slaves"] == 0


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
