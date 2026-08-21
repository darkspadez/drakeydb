"""Phase 1 multi-master identity tests: node uuid persistence + REPLCONF UUID exchange."""

import asyncio
import re
import time

import pytest

import redis

from .instance import DflyInstanceFactory, DflyStartException
from .utility import wait_available_async

UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
FIXED_UUID = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"


async def assert_start_fails(node):
    try:
        node.start()
    except DflyStartException:
        return

    # Startup listens before ServerFamily validates node identity, so a fast machine can observe
    # the dynamic port just before the process exits and make node.start() return successfully.
    for _ in range(50):
        return_code = node.proc.poll()
        if return_code is not None:
            node.stop(kill=True)  # clear the failed process from fixture teardown
            assert return_code != 0
            return
        await asyncio.sleep(0.02)
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
