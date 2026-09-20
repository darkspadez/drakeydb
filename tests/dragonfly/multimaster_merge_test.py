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
oracle -- `_Model` below -- that carries its OWN model of the expected post-merge state, built
exclusively by reading back each write's own resulting stamp on its AUTHORING node as it lands,
never by comparing the two sides' final states against each other.

That distinction is load-bearing, not cosmetic: an earlier version of this oracle compared each
side's final post-round DEBUG MVCC snapshot against the other side's, defaulting an unwritable
key to "lowest possible" and letting the other side's state win by default. That is blind to a
bug upstream of the merge -- e.g. `--multi_master_tombstone_ttl=0`, which makes every delete
erase its slot with no tombstone at all, same as an eviction. A node that just deleted a key such
a way reads back `state:absent`, indistinguishable (to a same-final-state comparison) from
"never touched it" -- so the old oracle would silently adopt the *other* side's stale value as
its own expectation, and the resulting resurrection would pass as "expected". `_Model` instead
remembers that a write happened, independent of what the storage engine can later show for it.
"""

import asyncio
import os
import random
from collections import defaultdict

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
# 80, not 40: the I-2 coverage assertion (>= 1 contested key per round) is a real per-round
# requirement, not just an aggregate one, so its false-failure rate is a function of collision
# density. A monte-carlo simulation of this exact (key, side, kind-weight) distribution measured
# P(a round has zero doubly-touched keys) at ~2% for 40 ops over 50 keys -- enough to explain a
# genuine ~1-in-10 spurious failure observed empirically -- versus ~0.0005% at 80 ops (a run of 6
# rounds then fails this way roughly 1 in 33,000 times). This is a collision-density fix, not a
# timing one: the two "did the merge land" checks (_wait_converged's poll, EXPIRE_SETTLE_S) were
# never implicated -- see the Task 9 fix-round report.
DEFAULT_OPS_PER_ROUND = 80
EXPIRE_TTL_MS_RANGE = (80, 160)
# > max EXPIRE_TTL_MS_RANGE with comfortable margin for scheduler/asyncio jitter -- the wait is
# for real wall-clock TTL expiry, not for replication convergence, so a bounded poll doesn't
# apply here; the forced GET below (not the sleep alone) is what makes settling deterministic.
EXPIRE_SETTLE_S = 0.6
# 12 keys, not 1: multimaster_test.py's own multi-shard guard (test_replicated_key_stamp_matches_
# origin, ~line 2232) uses the same idiom -- one key can only ever land on one shard, so proving
# the tombstone-merge path works across shards needs a small batch of keys, not a single "K".
PIN_KEYS = [f"K{i}" for i in range(12)]

# ---- Straddling-TTL mode (P4-3 final fix wave, F-1) ----------------------------------------
#
# The blind spot this closes: every `expire` op above is SETTLED before `_reattach_both` runs --
# `_settle_expiries` sleeps past the TTL *and* forces a `GET` on the authoring node, which reaps
# the key. So no key ever crossed the reattach boundary either (a) still live with a pending TTL
# or (b) expired-but-not-yet-reaped, and those two states are exactly what F-1 needs. The
# adversarial review called `EXPIRE_SETTLE_S` a structural blind spot rather than a tuning knob:
# raising the op/round/shard count or changing the seed cannot reach either state.
#
# This mode converts a small fraction of `expire` ops into *straddling* ones. They are armed LAST
# in the round (after `_settle_expiries`, immediately before `_reattach_both`) so the straddling
# SET is unambiguously the newest write on its key for the round -- which makes the expected
# outcome deterministic (the key must end ABSENT on both nodes) without having to predict which
# of the three sync-window phases each key happens to land in:
#
#   (a) TTL still live when the snapshot serializes it -> the peer adopts value + TTL and expires
#       it independently;
#   (b) TTL already elapsed but NOT yet reaped -> the peer receives a key with an elapsed
#       `expire_ms`. This is F-1: before the fix, both loader gates dropped it before any
#       `MergeAccepts` compare, so the peer silently KEPT its own strictly older value;
#   (c) TTL elapsed and already reaped -> the author's tombstone travels in the opcode-225
#       section and the peer adopts it verbatim.
STRADDLE_FRACTION = float(os.environ.get("MULTIMASTER_FUZZ_STRADDLE", "0.15"))
STRADDLE_TTL_MS_RANGE = (100, 900)
# Armed, then this long before reattaching: the shorter half of STRADDLE_TTL_MS_RANGE has just
# elapsed when the full sync starts (phase (b)) while the longer half is still live (phase (a)).
STRADDLE_PRESYNC_S = 0.35
# Phase (b) needs the authoring node's ACTIVE-expire sweep to still be behind when its snapshot
# serializes the key. On a real, loaded server that is the normal state (the review measured a
# ~5.7 s sweep lag behind 300k keys); on a 50-key fuzzer table the default --hz=100 sweep reaps
# within a few ms, so phase (b) would essentially never occur. Lowering the background-task
# frequency reproduces the same lag without paying for a 300k-key DEBUG POPULATE on every round's
# full sync. It does not change WHAT is reaped, only when -- every read path's own lazy-expiry
# check (and therefore every assertion below) is unaffected by --hz.
STRADDLE_HZ = int(os.environ.get("MULTIMASTER_FUZZ_HZ", "1"))


async def _read_one(c, key):
    """One key's ground truth on one node: its DEBUG MVCC stamp, plus its value when live."""
    stamp = _parse_mvcc(await c.execute_command("debug", "mvcc", key))
    value = await c.get(key) if stamp.get("state") == "value" else None
    return stamp, value


async def _snapshot(c, keys):
    out = {}
    for key in keys:
        stamp, value = await _read_one(c, key)
        out[key] = {"stamp": stamp, "value": value}
    return out


class _Model:
    """The fuzzer's own, independent model of expected post-merge state, keyed by key.

    Every write (SET/DEL/EXPIRE), on either side, is recorded here immediately as it lands, via
    `record()` -- by reading the stamp back on the node that authored it, the same idiom the
    resurrection pins already use for capturing a delete's tombstone. A later record supersedes
    an earlier one for the same key using the real `(Mvcc(), origin_hash)` order when BOTH have
    one (matching `MergeAccepts` exactly); when at least one side of the comparison has no real
    stamp to read back (the `--multi_master_tombstone_ttl=0` case: a delete erases with nothing
    left to read), supersession falls back to `seq` -- our own strictly increasing issuance
    counter, which is exact ground truth for "which of these two things happened later in real
    time" regardless of what either node's own MVCC table can show for it. `seq` is global across
    the whole run (not reset per round), so a write from round 3 always outranks anything from
    round 1 without needing a separate round dimension: rounds fully converge before the next one
    starts, so within-run issuance order already IS real-world chronology.
    """

    def __init__(self):
        self._seq = 0
        self.expected = {}  # key -> {"seq", "real": (mvcc,origin)|None, "value", "kind", "side"}

    @property
    def seq(self):
        return self._seq

    def next_seq(self):
        self._seq += 1
        return self._seq

    def record(self, key, seq, kind, side, stamp, value):
        real = None
        if stamp.get("mvcc") is not None:
            real = (int(stamp["mvcc"]), int(stamp["origin"], 16))
        candidate = {"seq": seq, "real": real, "value": value, "kind": kind, "side": side}
        cur = self.expected.get(key)
        if cur is None or self._supersedes(candidate, cur):
            self.expected[key] = candidate

    @staticmethod
    def _supersedes(new, cur):
        if new["real"] is not None and cur["real"] is not None:
            return new["real"] > cur["real"]
        return new["seq"] > cur["seq"]


async def _detach_both(c_a, c_b):
    await c_a.execute_command("REPLICAOF NO ONE")
    await c_b.execute_command("REPLICAOF NO ONE")
    # M-4: don't just fire REPLICAOF NO ONE and trust it -- confirm the partition actually took
    # effect before any round-local write is applied, so a write that landed while a link was
    # still draining can never masquerade as a genuine partitioned write.
    info_a = await c_a.info("replication")
    info_b = await c_b.info("replication")
    assert info_a["connected_masters"] == 0, f"A still attached after REPLICAOF NO ONE: {info_a}"
    assert info_b["connected_masters"] == 0, f"B still attached after REPLICAOF NO ONE: {info_b}"


async def _reattach_both(c_a, a, c_b, b):
    # Sequential, not concurrent: a node mid full-sync-load answers PING with -LOADING, which the
    # connecting side's Greet treats as a hard handshake failure -- see
    # test_peer_mesh_own_writes_not_echoed_back's docstring in multimaster_test.py.
    await attach(c_a, b)
    await wait_for_peers(c_a, 1)
    await attach(c_b, a)
    await wait_for_peers(c_b, 1)
    await wait_available_async([c_a, c_b])


def _comparable(entry, stamp_free):
    """What two peers must agree on for one key.

    Normally: everything -- value AND the full `{mvcc, origin}` stamp (see this module's
    docstring on why a value-only assertion is structurally blind).

    `stamp_free` is for keys whose winning write was an **expiry** (P4-3 final fix wave, F-2).
    An expiry tombstone's stamp is minted LOCALLY, by the node that reaped the key
    (`CommitOwnTombstone`/`RecordExpiryBlocking`, `tx_base.cc`), and expiry DELs are deliberately
    dropped from every peer link (`kEntryFlagExpired` in `journal::PassesPeerEchoFilter`,
    `journal/types.cc`, applied to the full-sync journal blob too, `snapshot.cc`). So when the
    same TTL fires on two peers -- which is the normal outcome whenever the TTL itself
    replicated, i.e. every straddling key in phase (a) above -- each node legitimately holds a
    DIFFERENT `{mvcc, origin}` for the same key, and `DEBUG MVCC` legitimately differs between
    them. What must still converge is existence and value: absent on both.

    Exact tombstone-stamp propagation for an expiry that was reaped on ONE node only is still
    pinned, with a full `{mvcc, origin}` equality assertion, by
    `test_resurrection_pin_natural_expiry_deletes_stale_value_on_full_sync` below -- this
    relaxation is scoped to the fuzzer, which cannot tell the two situations apart.
    """
    if stamp_free:
        return {"value": entry["value"]}
    return entry


async def _wait_converged(c_a, c_b, keys, label, timeout=30, stamp_free_keys=frozenset()):
    """Bounded poll on DEBUG MVCC agreement -- not a fixed sleep -- so this both (a) tolerates a
    full sync that legitimately takes a variable amount of time and (b) still fails promptly (at
    `timeout`) if the two sides settle on genuinely different stamps rather than merely being
    slow to converge."""

    @assert_eventually(timeout=timeout)
    async def converged():
        snap_a = await _snapshot(c_a, keys)
        snap_b = await _snapshot(c_b, keys)
        for key in keys:
            free = key in stamp_free_keys
            assert _comparable(snap_a[key], free) == _comparable(
                snap_b[key], free
            ), f"{label} key={key}: a={snap_a[key]} b={snap_b[key]}"
        return snap_a, snap_b

    return await converged()


async def _apply_random_ops(rng, c_a, c_b, key_space, count, round_idx, model):
    """Apply a random mix of SET / DEL / EXPIRE(short TTL) / re-SET to a random side of a
    detached pair, over a shared key space small enough (default 50 keys, 40 ops/round) that
    conflicting writes to the same key from both sides are frequent. Sequential awaits give a
    well-defined total order, and every SET/DEL is fed into `model` immediately via its own
    read-back on the authoring node -- EXPIRE's read-back is deferred to `_settle_expiries`,
    since the key is still live (with a pending TTL) at the point this function issues it.

    Returns `(expiring, touched, straddling)`: `expiring` is the `(side, key)` pairs that got a
    short TTL this round, for the caller to settle; `straddling` is the `(side, key)` pairs whose
    `expire` op was converted to the straddling mode (see STRADDLE_FRACTION above) and is armed by
    the caller AFTER settling, immediately before reattach; `touched` maps key -> the set of sides
    that wrote it this round, for the I-2 coverage assertions. A `DEL` against a key that turns
    out not to exist (returns 0) is NOT recorded and does NOT mark `touched` -- it had no effect,
    so it must not be allowed to out-rank a real prior write via `_Model`'s seq fallback, and it
    is not a genuine contest participant for I-2's purposes either.

    Fix-round note: a `DEL` can return 0 precisely BECAUSE this key's own earlier short TTL (from
    a PRIOR `expire` op this round, on the SAME side) already elapsed -- touching an expired key
    triggers Dragonfly's own lazy-expiry check before the DEL itself runs, which is what actually
    arms the tombstone (kExpired), while the DEL command that observed nothing there reports 0.
    So `expiring.discard(...)` must NOT run on the no-op path: discarding here would drop the
    pending settle-time read-back for exactly the round that needs it, and the model would never
    learn this write happened at all (silently losing it, rather than merely deferring it).
    """
    expiring = set()
    straddling = set()
    touched = defaultdict(set)
    for i in range(count):
        key = f"k{rng.randrange(key_space)}"
        side, c = rng.choice([("a", c_a), ("b", c_b)])
        kind = rng.choices(["set", "del", "expire"], weights=[55, 25, 20])[0]
        if kind == "set":
            value = f"v-{round_idx}-{i}-{rng.randint(0, 1 << 30)}"
            await c.execute_command("set", key, value)
            stamp, _ = await _read_one(c, key)
            model.record(key, model.next_seq(), "set", side, stamp, value)
            touched[key].add(side)
            expiring.discard((side, key))
        elif kind == "del":
            deleted = await c.execute_command("del", key)
            if not deleted:
                continue  # leaves any pending `expiring` entry alone -- see the docstring note
            expiring.discard((side, key))
            stamp, _ = await _read_one(c, key)
            model.record(key, model.next_seq(), "del", side, stamp, None)
            touched[key].add(side)
        elif rng.random() < STRADDLE_FRACTION:
            # Straddling expire (F-1): deferred to _arm_straddling_expiries so its TTL is armed
            # last, right before reattach, and therefore lands INSIDE the sync window. Nothing is
            # issued here -- but the key still counts as touched by this side, because this side
            # is about to write it.
            straddling.add((side, key))
            touched[key].add(side)
        else:  # expire: (re-)write, then arm a short TTL so it expires before reattach
            await c.execute_command("set", key, f"v-{round_idx}-{i}-ttl")
            await c.execute_command("pexpire", key, rng.randint(*EXPIRE_TTL_MS_RANGE))
            expiring.add((side, key))
            touched[key].add(side)
    return expiring, touched, straddling


async def _settle_expiries(c_a, c_b, expiring, model):
    if not expiring:
        return
    await asyncio.sleep(EXPIRE_SETTLE_S)
    for side, key in expiring:
        c = c_a if side == "a" else c_b
        await c.get(key)  # force the lazy-expiry check; don't rely on the active-expire cycle's
        # own timing to have already reaped it by the time we read it back
        stamp, value = await _read_one(c, key)
        model.record(key, model.next_seq(), "expire", side, stamp, value)


async def _arm_straddling_expiries(rng, c_a, c_b, straddling, round_idx, model):
    """Arm the round's straddling TTLs (F-1) and record them in the model as expiry deletes.

    Deliberately NOT settled: no sleep past the TTL, no forced GET. The point is for the key to
    cross the reattach boundary either still-live or expired-but-unreaped.

    The model entry uses the SET's own read-back stamp as its `real` ordering key. That is not
    the stamp of the eventual tombstone (an expiry tombstone is minted locally at reap time, and
    is therefore strictly newer) -- it is only used to decide WHICH write wins this key, and
    since these ops are issued last in the round, the value's stamp already out-ranks every other
    write on that key this round. The winning entry's `kind` is "expire", which is what makes the
    final assertion drop to existence+value (see `_comparable`).

    Returns the maximum TTL armed, in seconds (0.0 if nothing was armed).
    """
    max_ttl_ms = 0
    for side, key in sorted(straddling):
        c = c_a if side == "a" else c_b
        ttl_ms = rng.randint(*STRADDLE_TTL_MS_RANGE)
        await c.execute_command("set", key, f"v-{round_idx}-straddle-{side}")
        stamp, _ = await _read_one(c, key)
        await c.execute_command("pexpire", key, ttl_ms)
        model.record(key, model.next_seq(), "expire", side, stamp, None)
        max_ttl_ms = max(max_ttl_ms, ttl_ms)
    return max_ttl_ms / 1000.0


async def _run_fuzzer(df_factory, seed, rounds, ops_per_round, extra_a=None, extra_b=None):
    rng = random.Random(seed)
    keys = [f"k{i}" for i in range(KEY_SPACE)]
    a = df_factory.create(**active_args(proactor_threads=4, hz=STRADDLE_HZ, **(extra_a or {})))
    b = df_factory.create(**active_args(proactor_threads=4, hz=STRADDLE_HZ, **(extra_b or {})))
    df_factory.start_all([a, b])
    c_a, c_b = a.client(), b.client()

    print(f"[multimaster_merge_fuzzer] seed={seed} rounds={rounds} ops_per_round={ops_per_round}")

    await _reattach_both(c_a, a, c_b, b)  # establish the baseline mesh (both empty)

    # I-3: multi-shard by assertion, not luck. Shard(key, num_shards) is a pure function of the
    # key and the shard count, invariant for the whole run, so this only needs checking once.
    shards_seen = {
        _parse_mvcc(await c_a.execute_command("debug", "mvcc", key))["shard"] for key in keys
    }
    assert len(shards_seen) >= 2, (
        f"the {KEY_SPACE}-key space maps to only {len(shards_seen)} shard(s) ({shards_seen}) -- "
        "this fuzzer must exercise more than one shard"
    )

    model = _Model()
    any_delete_won_a_contest = False

    for round_idx in range(rounds):
        await _detach_both(c_a, c_b)
        seq_before = model.seq
        expiring, touched, straddling = await _apply_random_ops(
            rng, c_a, c_b, KEY_SPACE, ops_per_round, round_idx, model
        )
        await _settle_expiries(c_a, c_b, expiring, model)
        straddle_ttl_s = await _arm_straddling_expiries(rng, c_a, c_b, straddling, round_idx, model)
        straddle_keys = {key for _, key in straddling}

        # I-2: a coverage oracle. A fuzzer round that never pits both sides against the same key
        # proves nothing about the merge; MULTIMASTER_FUZZ_OPS=0 must fail here, not pass
        # vacuously by never disagreeing with itself.
        contested = {key for key, sides in touched.items() if len(sides) >= 2}
        assert contested, (
            f"seed={seed} round={round_idx}: no key was written by BOTH sides this round "
            f"(ops_per_round={ops_per_round}, {len(touched)} key(s) touched total) -- this round "
            "contests nothing"
        )
        for key in contested:
            winner = model.expected[key]
            if winner["seq"] > seq_before and winner["kind"] in ("del", "expire"):
                any_delete_won_a_contest = True

        if straddling:
            # Let the shorter TTLs elapse (phase (b)) while the longer ones stay live (phase
            # (a)), then sync. Deliberately no GET here: touching the key would reap it on the
            # author and turn every straddler into the already-covered phase (c).
            await asyncio.sleep(STRADDLE_PRESYNC_S)

        await _reattach_both(c_a, a, c_b, b)

        if straddling:
            # Post-sync: give every straddling TTL time to elapse on BOTH nodes, then force the
            # lazy-expiry check on both so the comparison below is not racing a pending reap.
            # This observes the merged state; it cannot repair it -- a key the merge wrongly left
            # holding the peer's stale, TTL-less value (F-1) reads back exactly that value here.
            await asyncio.sleep(straddle_ttl_s + EXPIRE_SETTLE_S)
            for key in straddle_keys:
                await c_a.get(key)
                await c_b.get(key)

        # F-2: an expiry's tombstone stamp is per-node by design, so expiry winners are compared
        # on existence + value only. SET/DEL winners keep full {mvcc, origin} equality.
        stamp_free = {k for k, exp in model.expected.items() if exp["kind"] == "expire"}
        got_a, got_b = await _wait_converged(
            c_a, c_b, keys, f"seed={seed} round={round_idx}", stamp_free_keys=stamp_free
        )

        for key in keys:
            got = got_a[key]
            free = key in stamp_free
            assert _comparable(got, free) == _comparable(
                got_b[key], free
            ), f"seed={seed} round={round_idx} key={key}: {got} != {got_b[key]}"

            exp = model.expected.get(key)
            if exp is None:
                assert got["stamp"].get("state") == "absent", (
                    f"seed={seed} round={round_idx} key={key}: model has no record of this key "
                    f"ever being written, expected absent, got {got}"
                )
                continue

            assert got["value"] == exp["value"], (
                f"seed={seed} round={round_idx} key={key}: winner value mismatch -- model "
                f"expected {exp['value']!r} (kind={exp['kind']}, side={exp['side']}), got "
                f"{got['value']!r} (stamp={got['stamp']})"
            )
            if exp["real"] is not None and not free:
                got_real = None
                if got["stamp"].get("mvcc") is not None:
                    got_real = (int(got["stamp"]["mvcc"]), int(got["stamp"]["origin"], 16))
                assert got_real == exp["real"], (
                    f"seed={seed} round={round_idx} key={key}: winner stamp mismatch -- model "
                    f"expected {exp}, got {got['stamp']}"
                )
            # `free` (an expiry winner): F-2 -- the surviving tombstone stamp is whichever node
            # reaped the key, so it is not predictable and not required to match across nodes.
            # The `got["value"] == exp["value"]` assertion above (exp["value"] is None for every
            # expiry) is what catches a resurrection, and it is what F-1 fails on.
            # else: the model's winning write left no discoverable stamp on its own author (e.g.
            # --multi_master_tombstone_ttl=0 erasing a delete's tombstone) -- the value check
            # above is what catches a resurrection in that case; there is no real stamp left
            # anywhere to additionally cross-check.

    assert any_delete_won_a_contest, (
        f"seed={seed}: no contested key was ever won by a DEL/expiry across all {rounds} "
        "round(s) -- this run exercised value-vs-value conflicts only, never proved a delete "
        "can beat a stale value in a genuine contest"
    )


@pytest.mark.slow
async def test_merge_fuzzer_random_two_node(df_factory: DflyInstanceFactory):
    """Randomized two-node merge convergence fuzzer (Task 9 Step 1). Default round count is
    CI-sized; scale up locally with MULTIMASTER_FUZZ_ROUNDS / MULTIMASTER_FUZZ_OPS. Seed is
    random per run unless MULTIMASTER_FUZZ_SEED is set, and is embedded in every assertion
    message (pytest shows captured stdout/failure text on failure) so a failure reproduces via
    MULTIMASTER_FUZZ_SEED=<seed>.

    Falsifying: see the Task 9 fix-round report for the verbatim failure produced by running
    `_run_fuzzer` with one node started under `--multi_master_tombstone_ttl=0` (every delete on
    that node erases with no tombstone, so a stale value on the other side resurrects it on
    reattach) -- `_Model`'s value check catches the resurrection even though the offending node's
    own DEBUG MVCC can no longer show any trace of the delete that should have won.

    Falsifying, second: the straddling-TTL mode (STRADDLE_FRACTION above) was run against the
    pre-fix binary (`3a227349`) and fails there -- restoring `CreateObjectOnShard`'s /
    `ShouldDiscardKey`'s unconditional "drop an already-expired incoming key" early return
    (`rdb_load.cc`) reproduces it: the receiving node keeps its own strictly older value for a
    key the peer had already overwritten and expired. Set `MULTIMASTER_FUZZ_STRADDLE=0` to turn
    the mode off (it must then stop detecting that regression, which is the point of the knob).
    """
    seed = int(os.environ.get("MULTIMASTER_FUZZ_SEED", random.randrange(2**31)))
    rounds = int(os.environ.get("MULTIMASTER_FUZZ_ROUNDS", DEFAULT_ROUNDS))
    ops_per_round = int(os.environ.get("MULTIMASTER_FUZZ_OPS", DEFAULT_OPS_PER_ROUND))
    await _run_fuzzer(df_factory, seed, rounds, ops_per_round)


# ---- Explicit resurrection pins (Task 9 Step 3) -- fuzzers find these by luck; these pin them. ----
# All three run at proactor_threads=4 (3 shards) over PIN_KEYS (12 keys), with an explicit
# `len(shards) >= 2` assertion (I-3) rather than trusting a single key to land somewhere
# representative.


async def test_resurrection_pin_peer_tombstone_deletes_stale_value_on_full_sync(
    df_factory: DflyInstanceFactory,
):
    """A and B both hold every key in PIN_KEYS. Detach. DEL each key on B (leaves a tombstone --
    kExplicit). No write on A. Reattach (full sync both ways): B's tombstone is newer than A's
    stale live value, so MergeAccepts deletes every key on A. Pins the resurrection-prevention
    property directly, without relying on the fuzzer to stumble into this exact interleaving.

    The origin check is absolute (I-4), not merely "B's stamp equals B's stamp": `a_own_origin`
    is captured from A's OWN write below (any key A authors carries A's own origin hash, whether
    read before or after it propagates), so the assertions below prove the tombstone's origin is
    both NOT A's own identity and IS the exact one B reported for its own delete.

    Falsifying: this is the production analogue of test_merge_fuzzer_random_two_node's
    tombstone-ttl-0 falsification, but on the real merge code instead of the model -- reverting
    MergeAccepts's `stored < incoming` to `incoming < stored` (mvcc.h) would make this test
    observe every key resurrected on A after reattach instead of deleted.
    """
    a = df_factory.create(**active_args(proactor_threads=4))
    b = df_factory.create(**active_args(proactor_threads=4))
    df_factory.start_all([a, b])
    c_a, c_b = a.client(), b.client()
    await _reattach_both(c_a, a, c_b, b)

    for key in PIN_KEYS:
        await c_a.set(key, "v1")
    for key in PIN_KEYS:
        await wait_for_value(c_b, key, "v1")
    a_own_origin = _parse_mvcc(await c_a.execute_command("debug", "mvcc", PIN_KEYS[0]))["origin"]

    await _detach_both(c_a, c_b)
    b_tombstones = {}
    for key in PIN_KEYS:
        assert await c_b.delete(key) == 1
        stamp = _parse_mvcc(await c_b.execute_command("debug", "mvcc", key))
        assert stamp["state"] == "tombstone", stamp
        b_tombstones[key] = stamp

    await _reattach_both(c_a, a, c_b, b)
    got_a, got_b = await _wait_converged(c_a, c_b, PIN_KEYS, "pin-peer-tombstone")

    shards_seen = {got_a[key]["stamp"]["shard"] for key in PIN_KEYS}
    assert len(shards_seen) >= 2, f"pin only touched shard(s) {shards_seen}"

    for key in PIN_KEYS:
        a_stamp = got_a[key]["stamp"]
        assert a_stamp["state"] == "tombstone", (key, a_stamp)
        assert a_stamp["mvcc"] == b_tombstones[key]["mvcc"], key
        assert a_stamp["origin"] == b_tombstones[key]["origin"], f"{key}: must name B as author"
        assert a_stamp["origin"] != a_own_origin, f"{key}: must not be A's own origin"
        assert not await _exists(c_a, key)
        assert not await _exists(c_b, key)


async def test_resurrection_pin_later_write_survives_delete_on_other_side(
    df_factory: DflyInstanceFactory,
):
    """Mirror of the pin above: DEL every key in PIN_KEYS on B, then a LATER SET ...=v2 on A,
    both while detached. A's write postdates B's tombstone, so on reattach A's write wins on both
    sides -- every key survives as v2, with matching stamps.

    Falsifying: the same MergeAccepts inversion as the pin above would instead let B's older
    tombstones win, deleting A's v2 writes on reattach instead of preserving them.
    """
    a = df_factory.create(**active_args(proactor_threads=4))
    b = df_factory.create(**active_args(proactor_threads=4))
    df_factory.start_all([a, b])
    c_a, c_b = a.client(), b.client()
    await _reattach_both(c_a, a, c_b, b)

    for key in PIN_KEYS:
        await c_a.set(key, "v1")
    for key in PIN_KEYS:
        await wait_for_value(c_b, key, "v1")

    await _detach_both(c_a, c_b)
    for key in PIN_KEYS:
        assert await c_b.delete(key) == 1
    # Comfortable real-time margin over any localhost clock granularity/skew (see
    # LOCALHOST_SKEW_TOLERANCE_MS in multimaster_test.py) so A's writes are unambiguously later.
    await asyncio.sleep(0.2)
    a_before = {}
    for key in PIN_KEYS:
        await c_a.set(key, "v2")
        stamp = _parse_mvcc(await c_a.execute_command("debug", "mvcc", key))
        assert stamp["state"] == "value", stamp
        a_before[key] = stamp

    await _reattach_both(c_a, a, c_b, b)
    got_a, got_b = await _wait_converged(c_a, c_b, PIN_KEYS, "pin-later-write")

    shards_seen = {got_a[key]["stamp"]["shard"] for key in PIN_KEYS}
    assert len(shards_seen) >= 2, f"pin only touched shard(s) {shards_seen}"

    for key in PIN_KEYS:
        assert got_a[key]["value"] == "v2", key
        assert got_a[key]["stamp"]["mvcc"] == a_before[key]["mvcc"], key
        assert got_a[key]["stamp"]["origin"] == a_before[key]["origin"], key


async def test_resurrection_pin_natural_expiry_deletes_stale_value_on_full_sync(
    df_factory: DflyInstanceFactory,
):
    """Expiry variant of the first pin: every key in PIN_KEYS exists on both sides; a 1-second
    TTL on B expires while detached (kExpired -- also leaves a tombstone, per the flag docs on
    --multi_master_tombstone_ttl and the DeleteReason history in db_slice.h). No write on A.
    After reattach, A has none of PIN_KEYS -- the natural-expiry tombstone prevents resurrection
    exactly like an explicit DEL's tombstone does.

    Falsifying: same MergeAccepts inversion as the other two pins would resurrect every key on A
    instead of deleting it; a narrower revert -- making kExpired behave like kEvicted/kSlotFlush
    (erase, no tombstone) -- would instead leave B's own DEBUG MVCC reporting `state:absent` after
    expiry (checked below) rather than `state:tombstone`, exactly the class of bug
    test_merge_fuzzer_random_two_node's tombstone-ttl-0 falsification demonstrates end to end.
    """
    a = df_factory.create(**active_args(proactor_threads=4))
    b = df_factory.create(**active_args(proactor_threads=4))
    df_factory.start_all([a, b])
    c_a, c_b = a.client(), b.client()
    await _reattach_both(c_a, a, c_b, b)

    for key in PIN_KEYS:
        await c_a.set(key, "v1")
    for key in PIN_KEYS:
        await wait_for_value(c_b, key, "v1")
    a_own_origin = _parse_mvcc(await c_a.execute_command("debug", "mvcc", PIN_KEYS[0]))["origin"]

    await _detach_both(c_a, c_b)
    for key in PIN_KEYS:
        assert await c_b.pexpire(key, 1000)

    await asyncio.sleep(1.2)
    b_tombstones = {}
    for key in PIN_KEYS:
        assert await c_b.get(key) is None  # force the lazy-expiry check
        stamp = _parse_mvcc(await c_b.execute_command("debug", "mvcc", key))
        assert stamp["state"] == "tombstone", stamp
        b_tombstones[key] = stamp

    await _reattach_both(c_a, a, c_b, b)
    got_a, got_b = await _wait_converged(c_a, c_b, PIN_KEYS, "pin-natural-expiry")

    shards_seen = {got_a[key]["stamp"]["shard"] for key in PIN_KEYS}
    assert len(shards_seen) >= 2, f"pin only touched shard(s) {shards_seen}"

    for key in PIN_KEYS:
        a_stamp = got_a[key]["stamp"]
        assert a_stamp["state"] == "tombstone", (key, a_stamp)
        assert a_stamp["mvcc"] == b_tombstones[key]["mvcc"], key
        assert a_stamp["origin"] == b_tombstones[key]["origin"], f"{key}: must name B as author"
        assert a_stamp["origin"] != a_own_origin, f"{key}: must not be A's own origin"
        assert not await _exists(c_a, key)
        assert not await _exists(c_b, key)
