"""drakeydb P4-3 Task 9: randomized two-node merge convergence fuzzer, plus explicit
resurrection pins.

What is under test is `MergeAccepts` (`src/server/mvcc.h`): when a peer link applies an
incoming key (full sync or incremental journal replay), the incoming write is accepted only
when `stored < incoming` under `MvccStamp::operator<` -- lexicographic on `(Mvcc(), origin_hash)`
with the tombstone bit masked out of `Mvcc()`. Ties favor the stored side. A tombstone (left by
an explicit DEL or a natural TTL expiry -- both `kExplicit`/`kExpired`, never `kEvicted`/
`kSlotFlush`) is itself just a stamp, ordered the same way, so a peer's tombstone that is newer
than our live value deletes our key on merge, while an older one is silently dropped.

Two nodes agreeing on a VALUE is not proof of correct LWW merge -- they could agree by luck (the
same trap the echo-storm tests in multimaster_test.py document: a convergence-only assertion is
structurally blind to a whole class of bugs). So every assertion below checks value AND stamp
(mvcc + origin) equality between the two nodes, AND compares the surviving stamp against an
oracle computed independently of the merge itself (see `_snapshot`/`_pick` below) -- this is what
makes it an LWW test rather than a value-convergence test.
"""

import asyncio
import os
import random

import pytest

from .instance import DflyInstanceFactory
from .multimaster_test import (
    _exists,
    _parse_mvcc,
    active_args,
    attach,
    wait_for_peers,
    wait_for_value,
)
from .utility import assert_eventually, wait_available_async

KEY_SPACE = 50
DEFAULT_ROUNDS = 6
DEFAULT_OPS_PER_ROUND = 40
EXPIRE_TTL_MS_RANGE = (80, 160)
# > max EXPIRE_TTL_MS_RANGE with comfortable margin for scheduler/asyncio jitter -- the wait is
# for real wall-clock TTL expiry, not for replication convergence, so a bounded poll doesn't
# apply here; the forced GET below (not the sleep alone) is what makes settling deterministic.
EXPIRE_SETTLE_S = 0.6


def _stamp_order(stamp: dict):
    """Sort key matching `MvccStamp::operator<` (mvcc.h): lexicographic on
    `(Mvcc(), origin_hash)`. `state:absent` (key never written, or -- unreachable in this test's
    short duration versus the 600s default `--multi_master_tombstone_ttl` -- a GC'd tombstone)
    sorts below every real stamp, matching `MergeAccepts(std::nullopt, incoming)` always
    accepting (mvcc_test.cc's `MergeAcceptsFavoursTheStoredSideOnATie` pins this exact case)."""
    if stamp.get("state") == "absent":
        return (-1, -1)
    return (int(stamp["mvcc"]), int(stamp["origin"], 16))


async def _snapshot(c, keys):
    """One node's authoritative state for every key: its DEBUG MVCC stamp, plus the value when
    the state is live. Used both as the pre-reattach independent oracle (this is `MergeAccepts`'s
    `stored` or `incoming` side, read straight from the node, never through the merge under
    test) and, after convergence, as the actual post-merge result to check against it."""
    out = {}
    for key in keys:
        stamp = _parse_mvcc(await c.execute_command("debug", "mvcc", key))
        value = await c.get(key) if stamp.get("state") == "value" else None
        out[key] = {"stamp": stamp, "value": value}
    return out


def _pick(snap_a_key, snap_b_key, invert=False):
    """The independent oracle's winner for one key: the snapshot with the higher `(Mvcc(),
    origin_hash)`, matching `MergeAccepts`. `invert=True` is the falsification knob (Task 9 Step
    2): picking the LOWER stamp instead turns this into a test that must fail against a correct
    merge -- see the report for the seed and verbatim failure this produced."""
    oa, ob = _stamp_order(snap_a_key["stamp"]), _stamp_order(snap_b_key["stamp"])
    if oa == ob:
        return snap_a_key  # identical either way (only reachable when neither side touched the
        # key this round, so both already hold the same converged stamp from a prior round)
    if invert:
        return snap_b_key if oa > ob else snap_a_key
    return snap_a_key if oa > ob else snap_b_key


async def _detach_both(c_a, c_b):
    await c_a.execute_command("REPLICAOF NO ONE")
    await c_b.execute_command("REPLICAOF NO ONE")


async def _reattach_both(c_a, a, c_b, b):
    # Sequential, not concurrent: a node mid full-sync-load answers PING with -LOADING, which the
    # connecting side's Greet treats as a hard handshake failure -- see
    # test_peer_mesh_own_writes_not_echoed_back's docstring in multimaster_test.py.
    await attach(c_a, b)
    await wait_for_peers(c_a, 1)
    await attach(c_b, a)
    await wait_for_peers(c_b, 1)
    await wait_available_async([c_a, c_b])


async def _wait_converged(c_a, c_b, keys, seed, round_idx, timeout=30):
    """Bounded poll on DEBUG MVCC agreement -- not a fixed sleep -- so this both (a) tolerates a
    full sync that legitimately takes a variable amount of time and (b) still fails promptly (at
    `timeout`) if the two sides settle on genuinely different stamps rather than merely being
    slow to converge."""

    @assert_eventually(timeout=timeout)
    async def converged():
        snap_a = await _snapshot(c_a, keys)
        snap_b = await _snapshot(c_b, keys)
        for key in keys:
            assert snap_a[key]["stamp"] == snap_b[key]["stamp"], (
                f"seed={seed} round={round_idx} key={key}: " f"a={snap_a[key]} b={snap_b[key]}"
            )
        return snap_a, snap_b

    return await converged()


async def _apply_random_ops(rng, c_a, c_b, key_space, count, round_idx):
    """Apply a random mix of SET / DEL / EXPIRE(short TTL) / re-SET to a random side of a
    detached pair, over a shared key space small enough (default 50 keys, 40 ops/round) that
    conflicting writes to the same key from both sides are frequent. Sequential awaits give a
    well-defined total order, but that order only matters for each node's own final local state
    per key -- MergeAccepts (the thing under test) never runs on these local writes, only on the
    reattach afterwards.

    Returns the (side, key) pairs that got a short TTL this round, so the caller can force their
    expiry to settle deterministically before snapshotting."""
    expiring = set()
    for i in range(count):
        key = f"k{rng.randrange(key_space)}"
        side, c = rng.choice([("a", c_a), ("b", c_b)])
        kind = rng.choices(["set", "del", "expire"], weights=[55, 25, 20])[0]
        if kind == "set":
            await c.execute_command("set", key, f"v-{round_idx}-{i}-{rng.randint(0, 1 << 30)}")
            expiring.discard((side, key))
        elif kind == "del":
            await c.execute_command("del", key)
            expiring.discard((side, key))
        else:  # expire: (re-)write, then arm a short TTL so it expires before reattach
            await c.execute_command("set", key, f"v-{round_idx}-{i}-ttl")
            await c.execute_command("pexpire", key, rng.randint(*EXPIRE_TTL_MS_RANGE))
            expiring.add((side, key))
    return expiring


async def _settle_expiries(c_a, c_b, expiring):
    if not expiring:
        return
    await asyncio.sleep(EXPIRE_SETTLE_S)
    for side, key in expiring:
        c = c_a if side == "a" else c_b
        await c.get(key)  # force the lazy-expiry check; don't rely on the active-expire cycle's
        # own timing to have already reaped it by the time we snapshot


async def _run_fuzzer(df_factory, seed, rounds, ops_per_round, invert_winner=False):
    rng = random.Random(seed)
    keys = [f"k{i}" for i in range(KEY_SPACE)]
    a = df_factory.create(**active_args(proactor_threads=4))
    b = df_factory.create(**active_args(proactor_threads=4))
    df_factory.start_all([a, b])
    c_a, c_b = a.client(), b.client()

    print(f"[multimaster_merge_fuzzer] seed={seed} rounds={rounds} ops_per_round={ops_per_round}")

    await _reattach_both(c_a, a, c_b, b)  # establish the baseline mesh (both empty)

    for round_idx in range(rounds):
        await _detach_both(c_a, c_b)
        expiring = await _apply_random_ops(rng, c_a, c_b, KEY_SPACE, ops_per_round, round_idx)
        await _settle_expiries(c_a, c_b, expiring)

        # Independent oracle, read from both sides BEFORE the merge that is meant to reproduce
        # it -- this snapshot cannot be contaminated by the merge outcome it predicts.
        snap_a = await _snapshot(c_a, keys)
        snap_b = await _snapshot(c_b, keys)
        expected = {key: _pick(snap_a[key], snap_b[key], invert=invert_winner) for key in keys}

        await _reattach_both(c_a, a, c_b, b)
        got_a, got_b = await _wait_converged(c_a, c_b, keys, seed, round_idx)

        for key in keys:
            exp = expected[key]
            got = got_a[key]
            assert (
                got == got_b[key]
            ), f"seed={seed} round={round_idx} key={key}: {got} != {got_b[key]}"
            assert got["stamp"].get("state") == exp["stamp"].get("state"), (
                f"seed={seed} round={round_idx} key={key}: expected state "
                f"{exp['stamp'].get('state')}, got {got['stamp'].get('state')} "
                f"(a={snap_a[key]} b={snap_b[key]})"
            )
            if exp["stamp"].get("state") == "absent":
                continue
            assert got["stamp"].get("mvcc") == exp["stamp"].get("mvcc") and got["stamp"].get(
                "origin"
            ) == exp["stamp"].get("origin"), (
                f"seed={seed} round={round_idx} key={key}: winner stamp mismatch -- "
                f"expected {exp['stamp']}, got {got['stamp']} (a={snap_a[key]} b={snap_b[key]})"
            )
            assert got["value"] == exp["value"], (
                f"seed={seed} round={round_idx} key={key}: winner value mismatch -- "
                f"expected {exp['value']!r}, got {got['value']!r}"
            )


@pytest.mark.slow
async def test_merge_fuzzer_random_two_node(df_factory: DflyInstanceFactory):
    """Randomized two-node merge convergence fuzzer (Task 9 Step 1). Default round count is
    CI-sized; scale up locally with MULTIMASTER_FUZZ_ROUNDS / MULTIMASTER_FUZZ_OPS. Seed is
    random per run unless MULTIMASTER_FUZZ_SEED is set, and is embedded in every assertion
    message (pytest shows captured stdout/failure text on failure) so a failure reproduces via
    MULTIMASTER_FUZZ_SEED=<seed>.

    Falsifying (Task 9 Step 2, done by hand -- not shipped as a permanent knob, see the report
    for the verbatim failure): temporarily changing `_pick`'s default call site to pass
    `invert=True` (equivalent to swapping `>` for `<`, i.e. picking the LOWER stamp) reliably
    fails a round with a real seed, because the two sides' local writes this round almost never
    tie -- the hybrid clock plus per-shard counter make two independently-authored stamps land at
    the same (Mvcc(), origin_hash) only in the untouched-key case, which `_pick` special-cases
    separately from the invertible branch.
    """
    seed = int(os.environ.get("MULTIMASTER_FUZZ_SEED", random.randrange(2**31)))
    rounds = int(os.environ.get("MULTIMASTER_FUZZ_ROUNDS", DEFAULT_ROUNDS))
    ops_per_round = int(os.environ.get("MULTIMASTER_FUZZ_OPS", DEFAULT_OPS_PER_ROUND))
    await _run_fuzzer(df_factory, seed, rounds, ops_per_round)


# ---- Explicit resurrection pins (Task 9 Step 3) -- fuzzers find these by luck; these pin them. ----


async def test_resurrection_pin_peer_tombstone_deletes_stale_value_on_full_sync(
    df_factory: DflyInstanceFactory,
):
    """A and B both hold K. Detach. DEL K on B (leaves a tombstone -- kExplicit). No write on A.
    Reattach (full sync both ways): B's tombstone is newer than A's stale live value, so
    MergeAccepts deletes K on A. Pins the resurrection-prevention property directly, without
    relying on the fuzzer to stumble into this exact interleaving.

    Falsifying: this is the production analogue of test_merge_fuzzer_random_two_node's
    invert-the-oracle falsification, but on the real merge code instead of the test's own oracle
    -- reverting MergeAccepts's `stored < incoming` to `incoming < stored` (mvcc.h) would make
    this test observe K resurrected on A after reattach instead of deleted.
    """
    a = df_factory.create(**active_args())
    b = df_factory.create(**active_args())
    df_factory.start_all([a, b])
    c_a, c_b = a.client(), b.client()
    await _reattach_both(c_a, a, c_b, b)

    await c_a.set("K", "v1")
    await wait_for_value(c_b, "K", "v1")

    await _detach_both(c_a, c_b)
    assert await c_b.delete("K") == 1
    b_tombstone = _parse_mvcc(await c_b.execute_command("debug", "mvcc", "K"))
    assert b_tombstone["state"] == "tombstone", b_tombstone

    await _reattach_both(c_a, a, c_b, b)

    @assert_eventually(timeout=30)
    async def converged_and_gone():
        assert not await _exists(c_a, "K")
        stamp = _parse_mvcc(await c_a.execute_command("debug", "mvcc", "K"))
        assert stamp["state"] == "tombstone", stamp
        return stamp

    a_stamp = await converged_and_gone()
    assert a_stamp["mvcc"] == b_tombstone["mvcc"], (a_stamp, b_tombstone)
    assert a_stamp["origin"] == b_tombstone["origin"], "A's tombstone must name B as the author"
    assert not await _exists(c_b, "K")


async def test_resurrection_pin_later_write_survives_delete_on_other_side(
    df_factory: DflyInstanceFactory,
):
    """Mirror of the pin above: DEL K on B, then a LATER SET K v2 on A, both while detached.
    A's write postdates B's tombstone, so on reattach A's write wins on both sides -- K survives
    as v2 everywhere, with matching stamps.

    Falsifying: the same MergeAccepts inversion as the pin above would instead let B's older
    tombstone win, deleting A's v2 on reattach instead of preserving it.
    """
    a = df_factory.create(**active_args())
    b = df_factory.create(**active_args())
    df_factory.start_all([a, b])
    c_a, c_b = a.client(), b.client()
    await _reattach_both(c_a, a, c_b, b)

    await c_a.set("K", "v1")
    await wait_for_value(c_b, "K", "v1")

    await _detach_both(c_a, c_b)
    assert await c_b.delete("K") == 1
    # Comfortable real-time margin over any localhost clock granularity/skew (see
    # LOCALHOST_SKEW_TOLERANCE_MS in multimaster_test.py) so A's write is unambiguously later.
    await asyncio.sleep(0.2)
    await c_a.set("K", "v2")
    a_stamp_before = _parse_mvcc(await c_a.execute_command("debug", "mvcc", "K"))
    assert a_stamp_before["state"] == "value", a_stamp_before

    await _reattach_both(c_a, a, c_b, b)

    @assert_eventually(timeout=30)
    async def converged():
        assert await c_a.get("K") == "v2"
        assert await c_b.get("K") == "v2"
        stamp_a = _parse_mvcc(await c_a.execute_command("debug", "mvcc", "K"))
        stamp_b = _parse_mvcc(await c_b.execute_command("debug", "mvcc", "K"))
        assert stamp_a == stamp_b, (stamp_a, stamp_b)
        return stamp_a

    stamp_a = await converged()
    assert (
        stamp_a["mvcc"] == a_stamp_before["mvcc"] and stamp_a["origin"] == a_stamp_before["origin"]
    )


async def test_resurrection_pin_natural_expiry_deletes_stale_value_on_full_sync(
    df_factory: DflyInstanceFactory,
):
    """Expiry variant of the first pin: K exists on both sides; a 1-second TTL on B expires
    while detached (kExpired -- also leaves a tombstone, per the flag docs on
    --multi_master_tombstone_ttl and the DeleteReason history in db_slice.h). No write on A.
    After reattach, A has no K -- the natural-expiry tombstone prevents resurrection exactly like
    an explicit DEL's tombstone does.

    Falsifying: same MergeAccepts inversion as the other two pins would resurrect K on A instead
    of deleting it; a narrower revert -- making kExpired behave like kEvicted/kSlotFlush (erase,
    no tombstone) -- would instead leave B's own DEBUG MVCC reporting `state:absent` for K after
    expiry (checked below) rather than `state:tombstone`, and A would then also fail to delete K
    on reattach since MergeAccepts(nullopt-equivalent, stored) never fires against a live value.
    """
    a = df_factory.create(**active_args())
    b = df_factory.create(**active_args())
    df_factory.start_all([a, b])
    c_a, c_b = a.client(), b.client()
    await _reattach_both(c_a, a, c_b, b)

    await c_a.set("K", "v1")
    await wait_for_value(c_b, "K", "v1")

    await _detach_both(c_a, c_b)
    assert await c_b.pexpire("K", 1000)

    await asyncio.sleep(1.2)
    assert await c_b.get("K") is None  # force the lazy-expiry check
    b_tombstone = _parse_mvcc(await c_b.execute_command("debug", "mvcc", "K"))
    assert b_tombstone["state"] == "tombstone", b_tombstone

    await _reattach_both(c_a, a, c_b, b)

    @assert_eventually(timeout=30)
    async def converged_and_gone():
        assert not await _exists(c_a, "K")
        stamp = _parse_mvcc(await c_a.execute_command("debug", "mvcc", "K"))
        assert stamp["state"] == "tombstone", stamp
        return stamp

    a_stamp = await converged_and_gone()
    assert a_stamp["mvcc"] == b_tombstone["mvcc"] and a_stamp["origin"] == b_tombstone["origin"]
    assert not await _exists(c_b, "K")
