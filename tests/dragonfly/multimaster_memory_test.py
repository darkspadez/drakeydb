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

`mvcc_table_bytes` itself has a narrower version of the same blind spot: it is
`DashTable::mem_usage()`, documented in dash.h as excluding "memory allocated by the
hosted objects". For a key over `CompactObj::kInlineLen` (16 B), `SetMvcc` heap-
allocates a second, independent copy of the key -- a real cost, inside `used_memory`,
invisible to `mvcc_table_bytes`. See `_expected_duplication_bytes` below for the model,
and task-12-report.md for how each constant in it was obtained.
"""

import pytest

from .instance import DflyInstanceFactory
from .multimaster_test import active_args

KEY_COUNT = 1_000_000
VALUE_SIZE = 100

# mimalloc's small-object bin sizes: a request of N bytes is rounded up to the smallest
# bin >= N. Not a formula anyone should guess -- these are the actual bin edges of the
# vendored build (build-dbg/third_party/libs/mimalloc2/lib/libmimalloc.a), read by
# compiling a 12-line probe against that exact .a and calling mi_good_size(1..300); see
# task-12-report.md for the probe source and full output. Only the entries this test's
# four key_len values can reach are needed; extend the table if a case exceeds 320B.
_MI_GOOD_SIZE_BINS = (8, 16, 32, 48, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320)


def _mi_good_size(n: int) -> int:
    for bin_size in _MI_GOOD_SIZE_BINS:
        if n <= bin_size:
            return bin_size
    raise ValueError(f"n={n} exceeds the probed mi_good_size table -- extend it")


def _binpacked_len(n: int) -> int:
    # Port of dfly::detail::binpacked_len (core/detail/bitpacking.h): 7-bit ASCII
    # packing, 8 source bytes -> 7 packed bytes. Not an implementation detail liable to
    # drift silently -- compact_object.cc pins it with static_asserts (binpacked_len(24)
    # == 21, etc.), so a change here would need a change there too.
    return (n * 7 + 7) // 8


def _key_length_distribution(key_len: int) -> dict:
    """Exact {raw key byte length: count} for KEY_COUNT DEBUG POPULATE keys built as
    prefix + ':' + index, index in [0, KEY_COUNT). Mirrors debugcmd.cc's
    PopulateRangeFiber key construction exactly (StrCat(prefix, ':'), then
    StrAppend(&key, index) -- a plain, unpadded decimal, no fixed width) -- see the
    correction 1 discussion in task-12-report.md for how this was verified against the
    C++ source rather than assumed."""
    prefix_len = max(1, key_len - 7)
    dist = {}
    lo = 0
    for digits in range(1, len(str(KEY_COUNT - 1)) + 1):
        hi = min(KEY_COUNT, 10**digits)
        count = hi - lo
        if count <= 0:
            break
        length = prefix_len + 1 + digits  # prefix + ':' + digits
        dist[length] = dist.get(length, 0) + count
        lo = hi
    assert sum(dist.values()) == KEY_COUNT
    return dist


def _expected_duplication_bytes(key_len: int) -> int:
    """Model of the second, side-table-only key copy SetMvcc makes for every key whose
    encoded representation still exceeds CompactObj::kInlineLen (16 B) -- the cost
    `mvcc_table_bytes` cannot see (see module docstring). Traces CompactObj::SetString/
    EncodeString's actual decision path (compact_object.cc), not a guess:

    1. str2ll integer parsing (str.size() <= 20): never taken here -- every key in this
       test contains the "k" prefix and a ":" separator, so it is never a valid integer.
    2. ASCII 7-bit packing (kUseAsciiEncoding, a compile-time true, unconditional):
       our keys are plain lowercase-ASCII-plus-digits-plus-colon, so validate_ascii_fast
       always succeeds and every key is packed via _binpacked_len before anything else
       is tried.
    3. Huffman encoding is skipped: it requires a trained per-key encoder
       (tl.huff_keys), armed only by --huffman_table (main_service.cc), which this
       benchmark never sets. Verified empirically, not just by flag absence: INFO stats
       huffenc_attempt_total stayed 0 across every case in the run this model was
       validated against (see task-12-report.md) -- TryHuffEncode() is gated on
       encoder.valid() and is simply never called when the table is untrained.
    4. A packed key <= kInlineLen (16 B) stores entirely inline in the 16-byte
       CompactObj union slot -- a cost mvcc_table_bytes already counts as part of the
       fixed per-slot bucket geometry. Zero marginal cost; this is the key_len=8/16
       case for every raw length DEBUG POPULATE can produce here.
    5. A packed key > kInlineLen goes through SmallString (small_string.h): the first
       kPrefLen=10 bytes stay inline in that same union slot (still free), and only the
       remainder is heap-allocated, via SegmentAllocator::Allocate -> mi_heap_malloc
       (segment_allocator.h), whose cost SegmentAllocator's own accounting rounds up to
       mi_good_size(). This is the key_len=24/32 case.

    Mutations that would break this model (and are exactly what it exists to catch):
    kInlineLen changing, the ASCII-packing ratio or kUseAsciiEncoding changing,
    SmallString.kPrefLen changing, mi_good_size's bin table changing (a mimalloc
    upgrade), or the second-copy allocation being optimized away entirely (a real
    improvement, but one this model would flag as a large over-prediction, not a
    silent pass). It does NOT model huffman-encoded keys -- if this benchmark ever
    starts passing --huffman_table, this function must change too.
    """
    total = 0
    for raw_len, count in _key_length_distribution(key_len).items():
        packed = _binpacked_len(raw_len)
        if packed <= 16:  # CompactObj::kInlineLen: fits inline, no second allocation
            continue
        overflow = packed - 10  # SmallString::kPrefLen (10 B) stays inline
        total += count * _mi_good_size(overflow)
    return total


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
    # keys. _key_length_distribution above computes this exactly; see task-12-report.md
    # for the printed table.
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
    #    from the same geometry. mvcc_bytes depends only on entry count (bucket
    #    capacity), never on key content, so per_key is expected to be identical across
    #    all four key_len cases -- confirmed: 46.5 B/key in every case (task-12-report).
    per_key = mvcc_bytes / entries
    assert 34 <= per_key <= 48, f"{per_key:.1f} B/key is outside the geometric bound [34, 48]"

    # 2. The metric must actually account for the delta -- against an accurate model,
    #    not against mvcc_table_bytes alone. NOT table_used_memory -- see the module
    #    docstring; DbTable::table_memory() returns prime.mem_usage() only. An earlier
    #    version of this assertion compared delta directly to mvcc_bytes and failed for
    #    key_len=24/32 (documented in task-12-report.md's first run): mvcc_bytes is
    #    DashTable::mem_usage(), which by its own documented contract excludes the
    #    second heap-allocated key copy SetMvcc makes for keys over kInlineLen (see
    #    _expected_duplication_bytes' docstring for the full mechanism). That gap is
    #    real and reproducible, not test noise -- so the fix is not a wider tolerance on
    #    the same wrong comparison, it's comparing against the right total.
    #    Falsified by: kInlineLen changing, ASCII-packing changing or being disabled,
    #    SmallString.kPrefLen changing, a mimalloc bin-table change, or the side table's
    #    second-copy cost being removed entirely (which would make delta collapse
    #    toward mvcc_bytes alone and this assertion over-predict and fail loudly).
    expected_duplication = _expected_duplication_bytes(key_len)
    expected_total = mvcc_bytes + expected_duplication
    delta = m_on["used_memory"] - m_off["used_memory"]
    assert abs(delta - expected_total) < 0.15 * expected_total, (
        f"used_memory moved {delta}, model predicts {expected_total} "
        f"(mvcc_bytes={mvcc_bytes} + duplication={expected_duplication})"
    )

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

    # Both numbers reported, not smoothed into one: table-only (mvcc_bytes, what
    # mvcc_table_bytes shows an operator) vs. true (expected_total/measured delta, what
    # used_memory actually pays) -- the gap between them IS the finding.
    print(
        f"\nkey_len={key_len}: table_only={mvcc_bytes / 2**20:.1f} MiB ({per_key:.1f} B/key), "
        f"true_model={expected_total / 2**20:.1f} MiB, "
        f"used_memory delta={delta / 2**20:.1f} MiB "
        f"({100 * delta / m_off['used_memory']:.1f}% over baseline), "
        f"gap over table-only={100 * (delta - mvcc_bytes) / mvcc_bytes:.1f}%, "
        f"mvcc_unstamped_writes={unstamped}, mvcc_stale_epoch={stale_epoch}"
    )
