"""drakeydb P4: the memory cost of the MVCC side table.

Deliverable for the P4-1 gate (Task 12). Also a regression guard: assertion 1 fails
loudly if anyone changes MvccStamp's size or the dash bucket geometry the estimate
rests on.

Marker: `@pytest.mark.large`, not a new `slow` marker. `tests/pytest.ini` registers
only opt_only/exclude_epoll/debug_only/large/replication; `large` already means "heavy
test that requires large runners and significant resources" (deselected by default via
`addopts = ... -m "not large"`) and is wired into real CI -- heavy-tests.yml and the
ci.yml regression job both invoke `pytest -m "$FILTER"` with FILTER=large on a schedule
(.github/actions/regression-tests). memory_test.py, this file's closest sibling and
also a large-population used_memory/RSS measurement, already uses this same marker. A
brand-new `slow` marker would be registered but wired into no CI job at all. Because
addopts deselects `large` by default, running this file directly needs an explicit
`-m large` override.

Do not compare `table_used_memory`: `DbTable::table_memory()` returns
`prime.mem_usage()` only, so the side table is invisible to it and a benchmark reading
it reports a ~0 delta and concludes, wrongly, that MVCC is free. Compare `used_memory`:
the side table is allocated from the shard's MiMemoryResource, so it *is* inside
`used_memory` and *is* subject to `maxmemory`.
"""

import pytest

from .instance import DflyInstanceFactory
from .multimaster_test import active_args

KEY_COUNT = 1_000_000
VALUE_SIZE = 100


async def _mem(client) -> dict:
    info = await client.info("memory")
    return {
        k: int(info[k])
        for k in ("used_memory", "used_memory_rss", "table_used_memory")
        if k in info
    }


@pytest.mark.large
@pytest.mark.parametrize("key_len", [8, 16, 24, 32])
async def test_mvcc_table_memory_cost(df_factory: DflyInstanceFactory, key_len: int):
    # DEBUG POPULATE builds StrCat(prefix, ":", index) with a PLAIN UNPADDED decimal
    # (debugcmd.cc:1015, :1029) -- there is no fixed-width suffix. For KEY_COUNT=1e6 the
    # index runs 0..999_999: 900_000 of the 1_000_000 keys (90%) draw a 6-digit index,
    # so len(prefix) + 1 (':') + 6 lands exactly on key_len for 90% of keys; the other
    # 10% (index < 100_000) are shorter, down to key_len-5 for the 10 single-digit-index
    # keys. See task-12-report.md for the exact achieved length distribution.
    prefix = "k" * max(1, key_len - 7)

    off = df_factory.create(proactor_threads=4)
    on = df_factory.create(**active_args(multi=False, proactor_threads=4))
    df_factory.start_all([off, on])
    c_off, c_on = off.client(), on.client()

    for c in (c_off, c_on):
        await c.execute_command("debug", "populate", KEY_COUNT, prefix, VALUE_SIZE)

    m_off, m_on = await _mem(c_off), await _mem(c_on)
    info_on = await c_on.info("memory")
    mvcc_bytes = int(info_on["mvcc_table_bytes"])
    entries = int(info_on["mvcc_entries"])

    assert entries == KEY_COUNT, f"expected a stamp per key, got {entries}"

    # 1. Geometry guard. sizeof(DbTable::MvccTable::Segment_t::Bucket) == 504 (Task 5,
    #    pinned by a test); 504 B / 14 slots = 36.03 B/slot, ~41 B/key at an 87.5% load
    #    factor. Outside [34, 48] the sizing argument in the design spec is stale and
    #    must be recomputed before P4-2. Falsified by any change to MvccStamp's size, or
    #    to the dash bucket geometry (kSlotNum, stash layout) the band is derived from.
    #    Do NOT widen this band to make a failure pass -- P4-5's tombstone cap is sized
    #    from the same geometry.
    per_key = mvcc_bytes / entries
    assert 34 <= per_key <= 48, f"{per_key:.1f} B/key is outside the geometric bound [34, 48]"

    # 2. The metric must actually account for the delta. NOT table_used_memory -- see
    #    the module docstring; DbTable::table_memory() returns prime.mem_usage() only.
    #    KNOWN GAP, measured, not merely predicted (see task-12-report.md): DashTable::
    #    mem_usage() (dash.h) is documented as "not including the memory allocated by
    #    the hosted objects". For key_len above CompactObj::kInlineLen (16,
    #    compact_object.h:154), every db.mvcc->Insert(key, ...) (db_slice.cc SetMvcc)
    #    heap-allocates a second, independent copy of the key bytes (a PrimeKey stored
    #    inside the side table's own bucket, separate from the prime table's copy --
    #    see db_slice.cc:1351/1386) that mvcc_table_bytes structurally cannot see. That
    #    second allocation lands in used_memory (the shard's MiMemoryResource) but not
    #    in mvcc_bytes. Measured: key_len=8/16 (both <= kInlineLen) pass, delta within
    #    ~1% of mvcc_bytes; key_len=24/32 FAIL this assertion -- delta overshoots
    #    mvcc_bytes by ~43%/~77%, far past the 15% tolerance. This is a real,
    #    reproducible property of the two metrics (mvcc_bytes undercounts long-key
    #    cost), not flakiness. Kept exactly as specified per task-12 scope discipline
    #    ("implement the brief as written and flag it") rather than loosened to pass --
    #    see task-12-report.md for the full measured table.
    delta = m_on["used_memory"] - m_off["used_memory"]
    assert (
        abs(delta - mvcc_bytes) < 0.15 * mvcc_bytes
    ), f"used_memory moved {delta} but mvcc_table_bytes reports {mvcc_bytes}"

    # Diagnostic only -- not asserted. mvcc_unstamped_writes measured 0 in every run (DEBUG
    # POPULATE's DoPopulateBatch still commits each key through RecordEntry, so every write
    # is stamped -- corroborates the P4-1 exit gate's "mvcc_unstamped_writes == 0 after a
    # seeder write workload"). mvcc_stale_epoch also measured 0: DEBUG POPULATE drives writes
    # through a stub transaction and calls OnCbFinishBlocking directly (debugcmd.cc:
    # 1067-1073), so EndOfWriteEpoch() never fires on this path and the hop memo is only ever
    # reset by the 50ms kMaxEpochMs backstop (mvcc.h) -- but a saturated populate commits far
    # more often than every 50ms per shard, so the backstop is never actually exercised by
    # this workload. Not asserted either way -- see task-12-brief correction 3.
    repl_on = await c_on.info("replication")
    unstamped = int(repl_on["mvcc_unstamped_writes"])
    stale_epoch = int(repl_on["mvcc_stale_epoch"])

    print(
        f"\nkey_len={key_len}: mvcc={mvcc_bytes / 2**20:.1f} MiB "
        f"({per_key:.1f} B/key), used_memory delta={delta / 2**20:.1f} MiB "
        f"({100 * delta / m_off['used_memory']:.1f}% over baseline), "
        f"mvcc_unstamped_writes={unstamped}, mvcc_stale_epoch={stale_epoch}"
    )
