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
addopts deselects `large` by default, run this file with an explicit `-m large`:

    docker exec drakeydb-p2 sh -c \
      'cd /src && DRAGONFLY_PATH=/src/build-dbg/dragonfly /tmp/tv/bin/python -m pytest \
       tests/dragonfly/multimaster_memory_test.py -m large -q -s'

Do not compare `table_used_memory`: `DbTable::table_memory()` returns
`prime.mem_usage()` only, so the side table is invisible to it and a benchmark reading
it reports a ~0 delta and concludes, wrongly, that MVCC is free. Compare `used_memory`:
the side table is allocated from the shard's MiMemoryResource, so it *is* inside
`used_memory` and *is* subject to `maxmemory`.

`mvcc_table_bytes` itself used to have a narrower version of the same blind spot: it
was `DashTable::mem_usage()` alone, documented in dash.h as excluding "memory
allocated by the hosted objects". For a key over `CompactObj::kInlineLen` (16 B),
`SetMvcc` heap-allocates a second, independent copy of the key -- a real cost, inside
`used_memory`, that the field could not see. Measured at the time (task-12-report.md):
mvcc_table_bytes read an identical 46,497,792 across key lengths 8-32 -- a 63%
under-report of the true side-table cost at 32-byte keys.

Fixed in P4-2 Task 4 (task-4-report.md): `DbTableStats::mvcc_key_dup_bytes` now
accounts that second copy's `MallocUsed()` at the same three mutation points that
already maintain `mvcc_entries` (`DbSlice::SetMvcc`/`EnsureMvcc`/`EraseMvcc`,
db_slice.cc), and `DbTable::mvcc_table_memory()` folds it directly into
`mvcc_table_bytes`. The external compensation model this docstring used to point
readers at (`_expected_duplication_bytes`, an independent Python port of
CompactObj::SetString/EncodeString's tag decisions) is gone along with the gap it
compensated for -- assertion 2 below now compares `mvcc_table_bytes` to the measured
`used_memory` delta directly, no separate model required. This paragraph is kept as
history of the gap and its size, in case it ever reopens.
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
    # keys. See task-12-report.md for the printed per-length breakdown (this file no
    # longer needs the exact distribution itself -- assertion 2 below compares against
    # measured used_memory directly, not a per-length-bucket model).
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

    # 1. Geometry guard, key_len 8/16 only. sizeof(DbTable::MvccTable::Segment_t::Bucket)
    #    == 504 (Task 5, pinned by a test); 504 B / 14 slots = 36.03 B/slot, ~41 B/key at
    #    an 87.5% load factor. Outside [34, 48] the sizing argument in the design spec is
    #    stale and must be recomputed before P4-2. Falsified by any change to MvccStamp's
    #    size, or to the dash bucket geometry (kSlotNum, stash layout) the band is
    #    derived from. Do NOT widen this band to make a failure pass -- P4-5's tombstone
    #    cap is sized from the same geometry.
    #
    #    Scoped to key_len in (8, 16), not all four: this band measures the table's
    #    fixed per-slot bucket geometry, which is content-independent -- true only for
    #    key lengths whose ASCII-packed form still fits CompactObj's 16-byte inline
    #    union (ASCII-packing 8 source bytes -> 7 packed, so raw lengths up to 16 pack to
    #    <= 16 and never leave it). Before P4-2 Task 4, mvcc_table_bytes was blind to key
    #    content entirely, so this band held for key_len=24/32 too (identical 46.5 B/key
    #    every case, task-12-report.md) -- but that was the bug this task fixed, not a
    #    property worth re-asserting. Since the fix, key_len=24/32 legitimately carry
    #    real, content-dependent duplicated-key heap bytes on top of this same
    #    structural floor, so per_key for those two cases is expected to exceed 48 -- see
    #    task-4-report.md for the measured numbers. Assertion 2 below is their regression
    #    guard instead: it would fail if that duplicated-key accounting broke.
    per_key = mvcc_bytes / entries  # reported below regardless; only asserted for 8/16.
    if key_len in (8, 16):
        assert 34 <= per_key <= 48, f"{per_key:.1f} B/key is outside the geometric bound [34, 48]"

    # 2. The metric must actually account for the delta. Before P4-2 Task 4,
    #    mvcc_table_bytes was DashTable::mem_usage() alone, which by its own documented
    #    contract excludes the second heap-allocated key copy SetMvcc makes for keys over
    #    kInlineLen -- comparing delta directly to mvcc_bytes failed for key_len=24/32 by
    #    exactly that gap (task-12-report.md's first run), which is why this assertion
    #    used to need an external model (_expected_duplication_bytes) to compensate.
    #    Task 4 (task-4-report.md) made mvcc_table_bytes account for that second copy
    #    directly (DbTableStats::mvcc_key_dup_bytes, maintained in db_slice.cc's
    #    SetMvcc/EnsureMvcc/EraseMvcc), so the comparison is now direct, no model, across
    #    all four key lengths. NOT table_used_memory -- see the module docstring;
    #    DbTable::table_memory() returns prime.mem_usage() only.
    #    Falsified by: the duplicated-key accounting being removed or broken (delta would
    #    then exceed mvcc_bytes by roughly the missing duplication cost and this
    #    assertion would fail loudly, the same way it did before Task 4 for key_len=24/32).
    delta = m_on["used_memory"] - m_off["used_memory"]
    assert (
        abs(delta - mvcc_bytes) < 0.15 * mvcc_bytes
    ), f"used_memory moved {delta}, mvcc_table_bytes reports {mvcc_bytes}"

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

    # mvcc_table_bytes (what INFO memory shows an operator) vs. the measured used_memory
    # delta (what the side table actually costs) -- post-Task-4 these two should track
    # each other directly (assertion 2 above); the remaining gap, reported here for
    # visibility rather than asserted tightly, is DashTable's own load-factor slack plus
    # sampling noise from the two instances' independent allocator state, not a blind spot.
    print(
        f"\nkey_len={key_len}: mvcc_table_bytes={mvcc_bytes / 2**20:.1f} MiB ({per_key:.1f} B/key), "
        f"used_memory delta={delta / 2**20:.1f} MiB "
        f"({100 * delta / m_off['used_memory']:.1f}% over baseline), "
        f"gap={100 * (delta - mvcc_bytes) / mvcc_bytes:.1f}%, "
        f"mvcc_unstamped_writes={unstamped}, mvcc_stale_epoch={stale_epoch}"
    )
