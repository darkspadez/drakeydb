# P4-3: Merge-on-full-sync LWW and bounded tombstones

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a full sync between active peers *merge* instead of overwrite — a stale incoming
key loses to a newer resident one — and close the delete-resurrection hole by giving deletes
bounded tombstones that survive a peer's absence, persist through RDB, and are applied on merge.

**Architecture:** Deletes stop erasing the side-table slot and instead leave a tombstone: the same
16-byte `MvccStamp` with bit 63 set, minted through the existing arm/commit machinery so a
tombstone carries exactly the stamp its own journal entry carried. Tombstones are bounded by a TTL
plus a hard per-shard cap enforced inline (an idle-task GC cannot be relied on precisely when
tombstones accumulate). `RdbLoader::CreateObjectOnShard` compares the incoming stamp against the
stored one — value *or* tombstone — immediately before `AddOrUpdate`, on the target shard's thread,
and skips the write when the stored side wins. Tombstones ride the RDB in their own opcode so a
peer's delete can also *apply* on merge. Merge-LWW is enabled only on authenticated peer links;
a plain replica still loads its master's snapshot verbatim.

**Tech Stack:** C++20, Dragonfly RDB serializer/loader, DashTable, GoogleTest, pytest,
CMake/Ninja in the `drakeydb-p2` container.

**Spec:** `docs/superpowers/specs/2026-08-25-phase4-mvcc-lww-design.md` — **D-8** (merge LWW) and
**D-10** (bounded tombstones) are the binding sections; D-1 (ordering), D-4 (memory), D-7 (RDB
format) are touched. The spec wins over this plan on conflict; record any deviation in the ledger.

## Global Constraints

- **Ties are won by the stored side.** The incoming value is written only when
  `stored < incoming` under `MvccStamp::operator<` (lexicographic on `(Mvcc(), origin_hash)`, with
  the tombstone bit masked). Equal stamps never churn. **Owner decision, 2026-08-30.**
- Merge-LWW applies **only** to authenticated peer links. `SetOverrideExistingKeys` must not be
  repurposed: it has three live callers and two of them (DEBUG LOAD / restore, and the plain-replica
  DF full sync) must keep loading verbatim.
- The `GetMvcc` read and the `AddOrUpdate` + `SetMvcc` write must sit on the **same shard thread
  with no yield between them** — `main_service.cc` allows `is_replicating` commands during
  `LOADING`, so other peers' stable-sync applies run concurrently on these threads.
- Tombstone application must itself be LWW-guarded; that is *why* no ordering constraint is needed
  between the tombstone section, the key stream, and the concurrent journal blob. Say so in the code.
- **Eviction is not deletion**: `kEvicted` and `kSlotFlush` erase the slot and write no tombstone.
  `kExplicit` and `kExpired` write one.
- The read path stays unconditional; the write path stays active-only (D-7's compatibility rule).
- The P3 golden-buffer journal test must still pass with `--active_replica` off, and an
  `--active_replica` off node's RDB output must stay byte-identical to upstream.
- Do not edit `helio/`. Do not touch `src/core/dash.h`, `src/core/compact_object.*`. **`engine_shard.{h,cc}` stays untouched** — the GC registers from `db_slice.cc` (D-10 is explicit about this).
- `mvcc.cc` stays independent of `GetCurrentTimeMs()`; callers pass time in.
- Build/test inside container: `docker exec drakeydb-p2 sh -c 'cd /src/build-dbg && ninja -j4 <target>'`
  (`-j4` mandatory, 8 GB VM). Pytest:
  `docker exec drakeydb-p2 sh -c 'cd /src && DRAGONFLY_PATH=/src/build-dbg/dragonfly /tmp/tv/bin/python -m pytest tests/dragonfly/<file> -x -q'`.
  Git and `~/.venvs/precommit/bin/pre-commit run --files <files>` run on the host from
  `/Users/darkspadez/.paseo/worktrees/2wtglncc/roasted-moth`.
- Commit lines ≤ 100 chars, imperative subject, suffix `(P4)`.
- **Falsify every test**: revert the code under test, observe the failure, restore, record the
  verbatim failure text in the report.
- CI uses `-Werror`; the tree sets `-Wno-unused-parameter`.
- Anchors below were verified against `e3ccbc8a` (merged P4-2) on 2026-08-30. Re-verify before editing.

## Verified anchors (post-P4-2 merge, e3ccbc8a)

| What | Where |
|---|---|
| `CreateObjectOnShard(const DbContext&, const Item*, DbSlice*)` | def `rdb_load.cc:3263-3414`, decl `rdb_load.h:463` |
| `AddOrUpdate` inside it | `rdb_load.cc:3348` |
| P4-2 stamp apply | `rdb_load.cc:3389` `SetMvcc(db, item->key, item->has_mvcc ? item->mvcc : MvccStamp{})` |
| `RunWithoutMvccArm()` | `rdb_load.cc:3400`; def `db_slice.cc:684` |
| `ShouldDiscardKey` (loader fiber — **wrong thread for the merge check**) | decl `rdb_load.h:449`, def `rdb_load.cc:3607`, called `rdb_load.cc:3554` |
| `SetOverrideExistingKeys` | decl `rdb_load.h:316`; callers `server_family.cc:1721` (DEBUG LOAD), `replica.cc:780` (peer redis path, already `if (IsPeerMode())`), `replica.cc:1431` (**unconditional — plain replicas hit this too**) |
| `SetApplyOrigin` / `SetLoadOriginHash` | `rdb_load.h:333` / `:345`; `override_existing_keys_` `rdb_load.h:499` |
| `PerformDeletionAtomic(const Iterator&, DbTable*, bool async, DeleteReason)` | decl `db_slice.h:673`, def `db_slice.cc:2628`; `reason` is `[[maybe_unused]]` today |
| Disarm+erase on delete | `db_slice.cc:2651-2654`, gated `mvcc_enabled_` |
| `DeleteReason` enum | `db_slice.h:402` — `kExplicit, kExpired, kEvicted, kSlotFlush` |
| Non-default reason sites (7) | `db_slice.cc:301` kEvicted, `:1107` `:1188` kSlotFlush, `:1539` `:1759` `:2203` kExpired, `:2285` kEvicted |
| `MvccClock::kTombstoneBit = 1ULL << 63`, `kStampMask` | `mvcc.h:30-31` |
| `MvccStamp` API: `Mvcc()`, `IsTombstone()`, `MsPart()`, `Empty()`, `operator<` (masks bit 63), `operator==` (**raw `packed`, does NOT mask**) | `mvcc.h:67-99` |
| `MvccStamper`: `Arm`, `Disarm` (erases *all* matching arms), `Commit(mvcc, origin_idx, CommitFn)`, `EndOfWriteEpoch`, `HopStamp(now_ms)`, `struct Armed{db_index, off, len}` | `mvcc.h:122-217` |
| Side-table accessors | `db_slice.cc:1353-1443`; `DbTable::GetMvcc(const PrimeKey&)` `table.cc:148` |
| `mvcc_entries` / `mvcc_tombstones` / `mvcc_key_dup_bytes` | `table.h:74,76,91`; `static_assert(kDbSz == 104)` `table.cc:51` |
| Dense invariant | `db_slice.cc:2783` `DCHECK_EQ(mvcc->size() - stats.mvcc_tombstones, prime.size())` |
| `AddOnIdleTask` | `helio/util/fibers/proactor_base.h:166`; in-tree precedent **inside db_slice.cc** at `db_slice.cc:359` (`async_deleter`) |
| `DbSlice` ctor | `db_slice.cc:522` |
| Flags + validation | `multi_master.cc:14-19` (`active_replica`, `multi_master`), `ValidateMultiMasterFlags()` `multi_master.cc:34-59` |
| SORT | `generic_family.cc`: registration `:3061` `CI{"SORT", CO::JOURNALED \| CO::STORE_LAST_KEY, ...}` (**no NO_AUTOJOURNAL**); `SortGeneric` `:2278`; `OpStore` `:1979`, destination write `:2021` |
| Opcodes (225 is free; 224 is the last) | `rdb_extensions.h:28-71` |
| Per-shard section precedent — a **prologue**, before `IterateBucketsFb` | emit `snapshot.cc:115-120`, parse `rdb_load.cc:2667` → `HandleShardDocIndex()` `rdb_load.cc:3690`; `RdbSaver::SaveEpilog` `rdb_save.cc:1835` has **no** per-shard hook |
| INFO mvcc fields | memory `server_family.cc:2898-2902`, replication `:3133-3147` |
| Test fixtures | `MvccStoreTest` `multi_master_test.cc:722`, `ApplyReplicatedCommand` `:763`; `RdbMvccTest` `rdb_test.cc:2487`; pytest `active_args` `multimaster_test.py:595`, `attach` `:609`, `_parse_mvcc` `:2238` |

**Plan decision (record in the ledger):** D-7 says the tombstone section rides "the shard epilogue".
No per-shard epilogue hook exists — `SaveEpilog` is global. Emit the section in the **prologue**
slot instead, beside `SearchSerializer::Serialize` (`snapshot.cc:115-120`), which is a real
per-shard extension point. This is sound precisely because tombstone application is LWW-guarded, so
its position relative to the key stream carries no meaning.

---

### Task 1: Merge decision and tombstone stamp helpers

**Files:** Modify `src/server/mvcc.h`, `src/server/mvcc.cc`; Test `src/server/mvcc_test.cc`

**Interfaces:**
- Consumes: `MvccStamp`, `MvccClock::kTombstoneBit`/`kStampMask` (`mvcc.h:30-31`).
- Produces:
  - `MvccStamp MvccStamp::AsTombstone() const` — same `origin_hash`, `packed | kTombstoneBit`.
  - `uint64_t MvccStamp::DeadlineMs(uint64_t ttl_ms) const` — `MsPart() + ttl_ms`; the GC deadline
    is derived, never stored (D-10: "no third field and no size growth").
  - `bool MergeAccepts(const std::optional<MvccStamp>& stored, const MvccStamp& incoming)` — free
    function in `namespace dfly`. `true` when there is no stored stamp at all, otherwise
    `*stored < incoming`. **Ties and equal stamps return false** (stored wins).

- [ ] **Step 1: Write the failing tests** in `mvcc_test.cc`:

```cpp
TEST(MvccStamp, MergeAcceptsFavoursTheStoredSideOnATie) {
  const MvccStamp a{0x1000, 0xAAAA};
  EXPECT_FALSE(MergeAccepts(a, a));                              // exact tie -> keep stored
  EXPECT_FALSE(MergeAccepts(MvccStamp{}, MvccStamp{}));          // {0,0} vs {0,0} -> keep stored
  EXPECT_TRUE(MergeAccepts(std::nullopt, MvccStamp{}));          // nothing stored -> write
  EXPECT_TRUE(MergeAccepts(MvccStamp{}, a));                     // unversioned loses to stamped
  EXPECT_FALSE(MergeAccepts(a, MvccStamp{}));                    // stamped beats unversioned
  EXPECT_TRUE(MergeAccepts(a, MvccStamp{0x1000, 0xBBBB}));       // equal mvcc, higher origin wins
}

TEST(MvccStamp, ATombstoneComparesByItsStampNotItsBit) {
  const MvccStamp live{0x2000, 0xAAAA};
  const MvccStamp tomb = MvccStamp{0x3000, 0xAAAA}.AsTombstone();
  EXPECT_TRUE(tomb.IsTombstone());
  EXPECT_EQ(tomb.Mvcc(), 0x3000u);                 // bit 63 masked out of the comparison value
  EXPECT_FALSE(MergeAccepts(tomb, live));          // newer tombstone beats an older resurrection
  EXPECT_TRUE(MergeAccepts(tomb, MvccStamp{0x4000, 0xAAAA}));  // a later write beats the tombstone
  EXPECT_EQ(tomb.DeadlineMs(600'000), (0x3000u >> MvccClock::kCounterBits) + 600'000);
}
```

- [ ] **Step 2: Run, observe failure.** `ninja -j4 mvcc_test && ./mvcc_test --gtest_filter='MvccStamp.*'`
  — expected: `MergeAccepts`/`AsTombstone`/`DeadlineMs` do not exist.

- [ ] **Step 3: Implement** the three additions in `mvcc.h`. `MergeAccepts` is three lines:

```cpp
inline bool MergeAccepts(const std::optional<MvccStamp>& stored, const MvccStamp& incoming) {
  return !stored.has_value() || *stored < incoming;
}
```

**Do not change `operator<` or `operator==`.** Note in a comment that `operator==` compares raw
`packed` (so a tombstone never compares equal to the live stamp it replaced) while `operator<`
masks bit 63 — that asymmetry is deliberate and load-bearing here.

- [ ] **Step 4: Run tests; falsify** by inverting the tie (`*stored <= incoming`), observe
  `MergeAcceptsFavoursTheStoredSideOnATie` fail, restore, record verbatim.

- [ ] **Step 5: pre-commit; commit** `feat: add merge-LWW decision and tombstone stamp helpers (P4)`.

### Task 2: Tombstones are written on delete, and bounded

**Files:** Modify `src/server/mvcc.h`, `src/server/mvcc.cc`, `src/server/db_slice.h`,
`src/server/db_slice.cc`, `src/server/table.h`, `src/server/table.cc`,
`src/server/multi_master.h`, `src/server/multi_master.cc`;
Test `src/server/multi_master_test.cc`, `src/server/mvcc_test.cc`

**Interfaces:**
- Consumes: Task 1's `AsTombstone()`; `MvccStamper::{Arm,Disarm,Commit}` (`mvcc.h:153-171`);
  `DbSlice::DeleteReason` (`db_slice.h:402`); the `[[maybe_unused]] reason` parameter
  (`db_slice.cc:2628`).
- Produces:
  - `void MvccStamper::ArmTombstone(DbIndex, std::string_view)` — like `Arm`, but the recorded
    `Armed` entry carries `tombstone = true`, and `Commit` ORs `kTombstoneBit` into the stamp it
    hands that key's `CommitFn`. Add the flag to `struct Armed` (`mvcc.h:188`).
  - `void DbSlice::SetTombstone(DbIndex, std::string_view, const MvccStamp&)` — writes a tombstone
    slot, maintaining `mvcc_entries`, `mvcc_tombstones`, and `mvcc_key_dup_bytes`.
  - `size_t DbTableStats::mvcc_tombstones_dropped` (new; bump `kDbSz` 104 → 112 and add to `ADD()`).
  - Flags in `multi_master.cc`: `--multi_master_tombstone_ttl` (seconds, default `600`, `0`
    disables tombstoning entirely), `--multi_master_max_tombstones` (default `1000000`, per shard),
    `--multi_master_tombstone_gc_budget` (default `64` buckets). Accessors in `multi_master.h`.

**Why the arm/commit route rather than minting at deletion:** a tombstone must carry *exactly* the
stamp its own journal entry carried, or the tombstone and the DEL that propagated it disagree and
the phase invariant breaks. `Commit` is the one place that already knows the entry's minted stamp.

- [ ] **Step 1: Write the failing tests.**

In `mvcc_test.cc`, that `Commit` sets bit 63 for tombstone arms only:

```cpp
TEST_F(MvccStamperTest, CommitMarksOnlyTombstoneArms) {
  auto* s = MvccStamper::tlocal();
  s->Arm(0, "live");
  s->ArmTombstone(0, "dead");
  std::map<std::string, MvccStamp> got;
  s->Commit(0x5000, 0, [&](DbIndex, std::string_view k, const MvccStamp& st) {
    got[std::string(k)] = st;
  });
  EXPECT_FALSE(got["live"].IsTombstone());
  EXPECT_TRUE(got["dead"].IsTombstone());
  EXPECT_EQ(got["live"].Mvcc(), got["dead"].Mvcc());  // same epoch stamp, one bit apart
}
```

In `multi_master_test.cc` on `MvccStoreTest`, the delete-reason matrix — this is the task's
headline test:

```cpp
TEST_F(MvccStoreTest, ExplicitAndExpiredDeletesLeaveTombstonesEvictionDoesNot) {
  Run({"set", "gone", "v"});
  const MvccStamp before = *StampOf("gone");
  Run({"del", "gone"});
  auto tomb = StampOf("gone");
  ASSERT_TRUE(tomb.has_value()) << "an explicit DEL must leave a tombstone";
  EXPECT_TRUE(tomb->IsTombstone());
  EXPECT_GE(tomb->Mvcc(), before.Mvcc());   // the tombstone is at least as new as the value
  EXPECT_EQ(GetMetrics().db_stats[0].mvcc_tombstones, 1u);
}
```

Plus: a `kEvicted` delete leaves **no** slot (drive it through the eviction path the
`db_slice.cc:301`/`:2285` sites use, or call `Del(..., kEvicted)` directly on the fixture); and at
`--multi_master_max_tombstones=1` a second delete erases instead of tombstoning and increments
`mvcc_tombstones_dropped`.

- [ ] **Step 2: Run, observe failure** — today every delete erases the slot, so `StampOf("gone")`
  is `nullopt` and `mvcc_tombstones` is 0.

- [ ] **Step 3: Implement.** In `PerformDeletionAtomic` (`db_slice.cc:2628`), replace the
  unconditional disarm+erase at `:2651-2654`:

```cpp
if (mvcc_enabled_) {
  // Any arm this key already has describes the value we are removing (e.g. HDEL's PostUpdate
  // armed it just before DeleteIfEmpty got here), so drop those first -- P4-1 made Disarm erase
  // every matching arm for exactly this reason -- then re-arm as a tombstone when the reason
  // earns one. Eviction and slot flush are capacity/topology decisions, not deletions: a peer's
  // copy is authoritative and resurrection is desirable, so they erase outright.
  MvccStamper::tlocal()->Disarm(table->index, del_it.key());
  if (TombstonesEnabled() && (reason == DeleteReason::kExplicit || reason == DeleteReason::kExpired) &&
      table->stats.mvcc_tombstones < absl::GetFlag(FLAGS_multi_master_max_tombstones)) {
    MvccStamper::tlocal()->ArmTombstone(table->index, del_it.key());
  } else {
    if (TombstonesEnabled() && (reason == DeleteReason::kExplicit || reason == DeleteReason::kExpired))
      ++table->stats.mvcc_tombstones_dropped;   // at the cap: degrade to erase, visibly
    EraseMvcc(table->index, del_it.key());
  }
}
```

The cap check is O(1) and inline **on purpose**: an idle-task GC never runs on a saturated server,
which is exactly when tombstones accumulate. Degrading to today's resurrection behaviour beats an
OOM, and `mvcc_tombstones_dropped` makes the degradation visible.

The armed tombstone slot is written when the DEL's journal entry commits. **Verify and record in
the report** that a delete's `RecordEntry` → `Commit` path runs for `kExplicit` and `kExpired`
deletes on both the author and the applier — P4-1 threaded `DbContext::repl_mvcc` and
`RecordDelete`/`RecordExpiryBlocking` for this. **STOP and report** if either reason reaches
`PerformDeletionAtomic` on a path that never journals: a tombstone that no peer learns about is
worse than none, and the fix is a design change, not an adjustment.

Accounting: a tombstone slot keeps its `mvcc_entries` and `mvcc_key_dup_bytes` contribution and
adds one to `mvcc_tombstones`; the dense invariant at `db_slice.cc:2783` already subtracts
tombstones, so it should hold unchanged — if it does not, that is a real defect, not a DCHECK to
relax.

Add the three flags and a `--cache_mode` boot warning in `ValidateMultiMasterFlags()`
(`multi_master.cc:34`): eviction cannot free tombstones and evicted keys deliberately get none, so
resurrection semantics differ under `--cache_mode`.

- [ ] **Step 4: Run tests; falsify** by making `kEvicted` tombstone too — the eviction assertion
  must fail. Restore, record verbatim.

- [ ] **Step 5: Run `multi_master_test`, `mvcc_test`, and `ctest -L DFLY`** (the dense invariant
  fires from many suites in debug builds). pre-commit; commit
  `feat: leave bounded tombstones on explicit and expired deletes (P4)`.

### Task 3: Tombstone GC

**Files:** Modify `src/server/db_slice.h`, `src/server/db_slice.cc`, `src/server/table.h`,
`src/server/table.cc`; Test `src/server/multi_master_test.cc`

**Interfaces:**
- Consumes: Task 2's flags and `mvcc_tombstones`; `MvccStamp::DeadlineMs` from Task 1.
- Produces: `bool DbSlice::TombstoneGcStep()` (returns true while work remains, matching
  `OnIdleTask`'s contract), a `MvccTable::Cursor mvcc_gc_cursor` on `DbTable` beside the existing
  `mvcc_defrag_cursor`, and registration from the `DbSlice` constructor (`db_slice.cc:522`)
  mirroring the in-file `AddOnIdleTask` precedent at `db_slice.cc:359`.

**`engine_shard.{h,cc}` must not be touched** — D-10 names this constraint explicitly and records
the alternative (three lines in `RetireExpiredAndEvict`) as an owner decision deferred until a
benchmark shows tombstone lag under load.

- [ ] **Step 1: Write the failing test** on `MvccStoreTest`: with
  `--multi_master_tombstone_ttl=1`, delete 200 keys, advance the clock past the deadline
  (`TEST_current_time_ms` / the fixture's existing time control), drive `TombstoneGcStep()` until
  it returns false, and assert `mvcc_tombstones` is 0 while unexpired tombstones from a second
  batch survive. Assert the budget is honoured: one step erases at most
  `--multi_master_tombstone_gc_budget` buckets' worth.

- [ ] **Step 2: Run, observe failure** (`TombstoneGcStep` does not exist).

- [ ] **Step 3: Implement.** Advance `mvcc_gc_cursor` by the budget per call using
  `MvccTable::Traverse` (the same call the defrag mirror uses), erasing entries where
  `it->second.IsTombstone() && it->second.DeadlineMs(ttl_ms) <= now_ms`. Decrement
  `mvcc_tombstones`, `mvcc_entries`, and `mvcc_key_dup_bytes` exactly as `EraseMvcc` does — reuse
  it rather than duplicating the accounting. Skip entirely when the TTL flag is 0.

- [ ] **Step 4: Run tests; falsify** by dropping the deadline comparison (GC erases live
  tombstones) — the second-batch survival assertion must fail. Restore, record verbatim.

- [ ] **Step 5: pre-commit; commit** `feat: expire tombstones on an idle-task GC step (P4)`.

### Task 4: Merge-LWW on the full-sync load path

**Files:** Modify `src/server/rdb_load.h`, `src/server/rdb_load.cc`, `src/server/replica.cc`;
Test `src/server/rdb_test.cc`, `src/server/multi_master_test.cc`

**Interfaces:**
- Consumes: Task 1's `MergeAccepts`; `DbSlice::GetMvcc` (`db_slice.cc:1402`).
- Produces: `void RdbLoader::SetMergeLww(bool enable, uint64_t sender_origin_hash)` and members
  `merge_lww_`, `merge_origin_hash_`.

- [ ] **Step 1: Write the failing tests** in `rdb_test.cc` on `RdbMvccTest`: build a snapshot whose
  key `k` carries stamp `{0x1000, PEER}`, install a resident `k` stamped `{0x2000, SELF}`, load
  with `SetMergeLww(true, PEER)`, and assert the resident value and stamp are **unchanged**. Then
  the reverse (incoming `{0x3000, PEER}` beats resident `{0x2000, SELF}`) writes both value and
  stamp. Then the tie case: identical stamps leave the resident value in place. Then the guard: the
  same stale snapshot loaded **without** `SetMergeLww` overwrites, proving plain replicas and
  `DEBUG LOAD` still load verbatim.

- [ ] **Step 2: Run, observe failure** — every case currently overwrites.

- [ ] **Step 3: Implement** in `CreateObjectOnShard`, immediately before the `AddOrUpdate` at
  `rdb_load.cc:3348`:

```cpp
if (merge_lww_) {
  // Same shard thread, no yield between this read and the AddOrUpdate+SetMvcc below: LOADING
  // admits other peers' is_replicating applies (main_service.cc), so a yield here would let a
  // concurrent apply land between the compare and the write. The stored side includes
  // TOMBSTONES -- that is what stops a peer's stale snapshot resurrecting a key we deleted.
  const MvccStamp incoming = item->has_mvcc ? item->mvcc : MvccStamp{};
  if (!MergeAccepts(db_slice->GetMvcc(db_cntx.db_index, item->key), incoming))
    return;  // stored side wins; the deserialized value is dropped (D-8 accepts that cost)
}
```

`ShouldDiscardKey` (`rdb_load.cc:3607`, called from `:3554`) looks like a cheaper home for this and
**is wrong**: it runs on the loader fiber, not the target shard, so it would read the side table
cross-thread. Put that reason in the comment.

Call `SetMergeLww` from exactly two peer-mode sites. `replica.cc:780` is already inside
`if (IsPeerMode())`. **`replica.cc:1431` is unconditional** — a plain replica of a Dragonfly master
reaches it too, so the new call there must be guarded by the same peer-mode predicate; adding it
unguarded would make plain replicas silently diverge from their master. Leave
`SetOverrideExistingKeys` exactly as it is at all three of its call sites.

- [ ] **Step 4: Run tests; falsify** by inverting the `MergeAccepts` result — the stale-loses test
  must fail. Restore, record verbatim. Also confirm `test_plain_replica_of_active_node_gets_full_unfiltered_stream` still passes.

- [ ] **Step 5: pre-commit; commit** `feat: merge on full sync instead of overwriting stale keys (P4)`.

### Task 5: Tombstones ride the RDB

**Files:** Modify `src/server/rdb_extensions.h`, `src/server/rdb_save.h`, `src/server/rdb_save.cc`,
`src/server/snapshot.cc`, `src/server/rdb_load.h`, `src/server/rdb_load.cc`;
Test `src/server/rdb_test.cc`

**Interfaces:**
- Produces: `RDB_OPCODE_DF_TOMBSTONES = 225`; payload
  `[db_index][count][count x {key, packed, origin_hash}]` (`packed` retains bit 63);
  `RdbLoader::HandleTombstones()` mirroring `HandleShardDocIndex()` (`rdb_load.cc:3690`).

Emit from the **per-shard prologue** beside `SearchSerializer::Serialize` (`snapshot.cc:115-120`),
not an epilogue — no per-shard epilogue hook exists (`RdbSaver::SaveEpilog`, `rdb_save.cc:1835`, is
global). Position is semantically free because tombstone application is LWW-guarded.

Write gated on `IsActiveReplica()` and a non-empty table; **read unconditional** (D-7's rule);
install only when active. Drop already-expired tombstones at save time (save-time GC, D-10).

- [ ] **Step 1: Write the failing round-trip test** on `RdbMvccTest`: delete two keys to create
  tombstones, save, reload into a fresh active instance, assert both tombstones are present with
  their exact `{packed, origin_hash}` (bit 63 intact) and that `mvcc_tombstones` matches. Add a
  non-active loader case: the section parses without error and installs nothing.

- [ ] **Step 2: Run, observe failure** (opcode 225 does not exist).

- [ ] **Step 3: Implement** the constant, the emit, and the parse. Follow
  `HandleShardDocIndex`'s structure for the handler and the `rdb_load.cc:2667` dispatch.

- [ ] **Step 4: Run tests; falsify** by skipping the emit when the table is non-empty — the
  round-trip must fail. Restore, record verbatim. Confirm an `--active_replica` off save emits no
  225 byte.

- [ ] **Step 5: pre-commit; commit** `feat: persist tombstones in RDB_OPCODE_DF_TOMBSTONES (P4)`.

### Task 6: Applying a peer's tombstone deletes our key

**Files:** Modify `src/server/rdb_load.cc`; Test `src/server/rdb_test.cc`,
`src/server/multi_master_test.cc`

**Interfaces:** Consumes Task 4's `merge_lww_` and Task 5's loaded tombstones.

This is the half that actually closes the resurrection hole: peer B deleted `K` at t=100 while we
were down; we still hold `K@t=50`; the merge must **delete** our `K`, not merely decline to write
B's (B's snapshot has no `K` to write).

- [ ] **Step 1: Write the failing test**: resident `K` stamped `{0x1000, SELF}`; load a snapshot
  containing a tombstone for `K` at `{0x2000, PEER}` and no `K` key; assert `K` is gone afterwards
  and the tombstone is stored. Then the guard case: a tombstone at `{0x0500, PEER}` (older than the
  resident value) leaves `K` intact — the apply is itself LWW-guarded.

- [ ] **Step 2: Run, observe failure** — the tombstone loads but `K` survives.

- [ ] **Step 3: Implement** inside the Task 5 handler, for each incoming tombstone when
  `merge_lww_` is on: compare against the stored stamp with `MergeAccepts`; when the tombstone
  wins, delete the live key (via the shard's normal delete path so all bookkeeping runs) and store
  the tombstone. Do this on the owning shard's thread, like the key path. State in the comment that
  **because this apply is LWW-guarded, its ordering relative to the key stream and the concurrent
  journal blob carries no meaning** — that is why no ordering constraint is imposed anywhere.

- [ ] **Step 4: Run tests; falsify** by dropping the `MergeAccepts` guard on the apply — the
  older-tombstone case must then wrongly delete a newer key. Restore, record verbatim.

- [ ] **Step 5: pre-commit; commit** `feat: apply a peer's winning tombstone on merge (P4)`.

### Task 7: `SORT ... STORE` replication

**Files:** Modify `src/server/generic_family.cc` (and only what the investigation proves is
needed); Test `src/server/generic_family_test.cc` or `src/server/multi_master_test.cc`,
`tests/dragonfly/multimaster_test.py`

**Context and the trap:** the issue register records that `SORT ... STORE` does not replicate,
reproduced with `--active_replica` **off**. But SORT is registered `CO::JOURNALED |
CO::STORE_LAST_KEY` with **no** `NO_AUTOJOURNAL` (`generic_family.cc:3061`), which normally means
the dispatcher auto-journals the verbatim command. So the cause is **not** obvious from the
registration, and the destination is written through a plain `AddOrUpdate` at
`generic_family.cc:2021` inside `OpStore`.

- [ ] **Step 1: Reproduce and diagnose before changing anything.** Write a test that performs
  `SORT src ... STORE dst` on a master with a replica attached and asserts `dst` exists on the
  replica. Run it, confirm it fails, and then **find the actual mechanism** — likely candidates to
  check and rule in or out explicitly: whether the auto-journal fires at all for this command,
  whether `IsOmittableWrite` suppresses it, whether the destination write happens on a shard the
  transaction did not journal for, and whether `CO::STORE_LAST_KEY` affects key routing. Record the
  mechanism, with the file:line evidence, in the report **before** writing a fix.

- [ ] **Step 2: Fix the mechanism you actually found.** If the auto-journal is suppressed for a
  reason that also protects other commands, prefer an explicit `RecordJournal` of the resulting
  `SET`/`DEL` on the destination shard (the pattern MOVE and UNLINK in this same file already use
  with `NO_AUTOJOURNAL`) over changing shared dispatch behaviour. **STOP and report** if the fix
  would require changing how auto-journaling works for commands other than SORT — that is a
  scope change the owner should rule on, not an implementation detail.

- [ ] **Step 3: Prove it in both modes.** The destination must replicate with `--active_replica`
  off (plain replication) and must be stamped and propagated with it on. Add a pytest case to
  `multimaster_test.py` for the active-mode path.

- [ ] **Step 4: Falsify** (revert the fix, observe the replica missing `dst`, restore, verbatim).

- [ ] **Step 5: pre-commit; commit** `fix: replicate SORT ... STORE's destination key (P4)`.

### Task 8: Observability and operator docs

**Files:** Modify `src/server/server_family.cc`, `src/server/debugcmd.cc`;
Create `docs/multi-master.md`; Modify `docs/differences.md`, `docs/PLAN.md`,
`docs/ISSUE-REGISTER.md`; Test `src/server/multi_master_test.cc`

**Interfaces:** Consumes Task 2's `mvcc_tombstones_dropped` and Task 3's GC.

- [ ] **Step 1: Write the failing test**: after a delete, `DEBUG MVCC <key>` reports the key as a
  tombstone with its stamp (P4-1 shipped a tombstone branch in `debugcmd.cc` that no test has ever
  reached — this is the task that reaches it); `INFO memory` exposes `mvcc_tombstones_dropped`
  alongside the existing `mvcc_tombstones` (`server_family.cc:2898-2902`).

- [ ] **Step 2: Run, observe failure.**

- [ ] **Step 3: Implement** the INFO field and whatever `DEBUG MVCC` needs to report a tombstone
  accurately.

- [ ] **Step 4: Write `docs/multi-master.md`** — the operator page this phase makes necessary.
  Cover: what merge-on-full-sync does and does not do; the three tombstone flags with defaults and
  how to size the TTL against expected partition length; that **`FLUSHALL`/`FLUSHDB` destroys every
  tombstone, mesh-wide, so the delete-resurrection window reopens immediately after a flush**
  (`DbSlice::FlushDbIndexes` swaps the whole `DbTable`); the `--cache_mode` interaction; and that
  hitting `--multi_master_max_tombstones` degrades to resurrection, visible as
  `mvcc_tombstones_dropped`. Link it from `docs/differences.md` and `docs/README.md`.

- [ ] **Step 5: Update `docs/PLAN.md`'s P4-3 row and close the issue-register entries this phase
  resolves** (D-3 if Task 7 landed it, and the merge/tombstone items). Do not delete entries this
  phase did not close.

- [ ] **Step 6: pre-commit; commit** `docs: document merge LWW, tombstone flags, and flush hazards (P4)`.

### Task 9: Convergence fuzzer and multi-shard end-to-end coverage

**Files:** Create `tests/dragonfly/multimaster_merge_test.py`; Modify
`tests/dragonfly/multimaster_test.py`

**Interfaces:** Consumes everything above. Uses `active_args` (`multimaster_test.py:595`), `attach`
(`:609`), `_parse_mvcc` (`:2238`).

- [ ] **Step 1: Write the randomized two-node merge fuzzer.** Two active nodes,
  `proactor_threads=4` (→ 3 shards, since `num_shards` defaults to `proactor_threads - 1`). Seeded
  `random.Random(seed)` so failures reproduce; print the seed on failure. Each round: partition
  (detach), apply a random mix of SET/DEL/EXPIRE to both sides over a shared key space, reattach,
  wait for convergence, then assert **both nodes hold identical values *and* identical stamps for
  every key** — stamp equality is what makes this a real LWW test rather than a value-convergence
  test. Assert the winner is the higher `{mvcc, origin_hash}` in each conflict, not merely that the
  two agree. Mark it `@pytest.mark.slow` (register the marker in `tests/pytest.ini` if absent) and
  keep the default round count modest enough for CI.

- [ ] **Step 2: Prove the fuzzer can fail.** Run it against a build with Task 4's `MergeAccepts`
  inverted and record the verbatim failure with its seed. A fuzzer that has never failed is not
  evidence.

- [ ] **Step 3: Add the resurrection case explicitly** (fuzzers find it by luck; this pins it):
  node A and B hold `K`; detach; delete `K` on B; write nothing on A; reattach and full-sync A from
  B; assert `K` is gone on A. Then the mirror: delete on B, then a *later* write on A, and assert
  A's write survives.

- [ ] **Step 4: Run each new test 10×; record pass rates.** pre-commit; commit
  `test: fuzz two-node merge convergence and pin the resurrection case (P4)`.

### Task 10: Exit gate

**Files:** none. Ledger: `.superpowers/sdd/2026-08-30-phase4-3-merge-lww-tombstones/progress.md`.

- [ ] `ninja -j4 dragonfly` warning-free; full `ctest -V -L DFLY`.
- [ ] Pytest: `multimaster_test.py`, `multimaster_merge_test.py`, `multimaster_memory_test.py -m large`,
  `replication_test.py`, `replication_specific_test.py`, `replication_resilience_test.py`.
- [ ] The P3 golden-buffer journal test passes with `--active_replica` off, and an
  `--active_replica` off RDB save emits neither opcode 221 nor 225.
- [ ] `mvcc_unstamped_writes == 0` after a pure write workload; `mvcc_tombstones` returns to 0
  after the TTL elapses on an idle node.
- [ ] Memory: record `mvcc_table_bytes` and `used_memory` for a delete-heavy workload and confirm
  the tombstone cap holds the side table under the D-10 bound (~41 MB/shard at the default cap).
- [ ] Every falsification recorded verbatim in the ledger; `pre-commit` clean across the branch.

---

### Task 11: Expiry deletes earn tombstones (added 2026-08-30 by controller ruling; run after Task 3)

Task 2 established, and its review independently confirmed, that `kExpired` cannot tombstone today:
two of its five call sites journal **before** they delete, so the tombstone's arm is orphaned and no
`Commit` ever lands its stamp. Spec D-10 requires expiry tombstones ("a peer whose copy has not yet
expired and which then legitimately writes the key *later* still wins"), so this closes that gap in
its own reviewable unit rather than as a hunk inside Task 2.

**Files:** Modify `src/server/db_slice.cc`; Test `src/server/multi_master_test.cc`,
`src/server/journal/journal_test.cc`, `tests/dragonfly/replication_test.py`

**Interfaces:** Consumes Task 2's `ArmTombstone`, the rollback in `EndOfWriteEpoch`, and
`SetExistingMvcc`'s restored missing-slot-is-fatal `CHECK` — that `CHECK` is the net for this task's
likeliest mistake, so do not weaken it.

**The two unsafe sites, both confirmed twice:**
- `DbSlice::ExpireIfNeeded` — `RecordExpiryBlocking(cntx, key)` at `db_slice.cc:1825` runs before
  `Del(...)` at `:1840`.
- `DbSlice::DeleteReapedContainer` — `RecordDerivedDelete(cntx, key)` at `:2283` before `Del(...)`
  at `:2284`.

The three other `kExpired` sites already delete before journaling and need no change:
`HSetFamily::DeleteIfEmpty` (`hset_family.cc:1648-1677`), `SetFamily::DeleteSetIfEmpty`
(`set_family.cc:1663-1683`), and `UpdateExpire`'s negative-TTL branch (`db_slice.cc:1617-1624`).

- [ ] **Step 1: Write the failing test.** On `MvccStoreTest`: set a key with a short TTL, let it
  expire through the lazy-expiry read path, and assert the side table holds a **tombstone with a
  non-zero stamp and the self origin** — not an absent slot and not `{0,0}`. Add the reaped-container
  equivalent for `DeleteReapedContainer`. Both must fail today (`kExpired` does not tombstone).

- [ ] **Step 2: Run, observe failure.**

- [ ] **Step 3: Reorder the two sites so the delete precedes the journal record**, matching every
  `kExplicit` site and matching the six journal-before-arm reorderings P4-1 already landed for this
  exact defect class. Then add `kExpired` to Task 2's `earns_tombstone` condition. Keep the journal
  entry's content identical — only its position within the callback moves.

- [ ] **Step 4: Prove the wire did not change for non-active nodes.** This is the constraint that
  makes this task risky: the reorder touches a path shared with plain replication. Run the P3
  golden-buffer journal test with `--active_replica` off, `journal_test` in full, and
  `replication_test.py`; then specifically verify that an expiry's journal entry still reaches the
  replica with the same content and in an order that keeps the key's baseline before its mutation.
  **STOP and report** if the reorder changes what a non-active replica observes — a correctness fix
  for active mode must not perturb plain replication.

- [ ] **Step 5: Falsify** (revert the reorder, observe the orphaned-arm symptom from Task 2's
  report — a stuck `{kTombstoneBit, 0}` or a rolled-back slot where the test expects a real stamp —
  restore, record verbatim).

- [ ] **Step 6:** Run `ctest -L DFLY`, `multi_master_test`, `mvcc_test`. pre-commit; commit
  `fix: journal expiry after the delete so expired keys earn tombstones (P4)`.
