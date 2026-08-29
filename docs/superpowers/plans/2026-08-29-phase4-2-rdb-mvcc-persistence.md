# P4-2: RDB MVCC Persistence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist every key's `{mvcc, origin_hash}` stamp through RDB save/load (opcode 221) so
snapshots, restarts, and full syncs carry authorship instead of degrading to `{0,0}`; accept
KeyDB's `mvcc-tstamp` aux on read; make `mvcc_table_bytes` report the side table's true cost; add
multi-shard end-to-end stamp coverage.

**Architecture:** A dedicated opcode `RDB_OPCODE_DF_MVCC = 221` (16 raw LE bytes) is emitted per
key beside the existing `DF_MASK` block, threaded exactly like `mc_flags` through
`SerializerBase::SerializeEntry` → `SerializeEntryLocked` → `RdbSerializer::SaveEntry`. The write
is implicitly gated on active mode (side-table lookup returns nothing otherwise); **the read is
unconditional** — a non-active binary parses and discards. The loader replaces P4-1's `{0,0}`
fallback with the persisted stamp when present, still creating an entry for every key so the
side table stays dense. KeyDB's per-key `mvcc-tstamp` aux (decimal string) maps to
`{parsed_u64, load_origin_hash_}`.

**Tech Stack:** C++20, Dragonfly RDB serializer/loader, DashTable, GoogleTest, pytest,
CMake/Ninja in the `drakeydb-p2` container.

**Spec:** `docs/superpowers/specs/2026-08-25-phase4-mvcc-lww-design.md` — D-7 is the binding
section; D-4 (KeyDB layout), D-13 (metric) are touched. The spec wins over this plan on conflict;
record any deviation in the ledger.

## Global Constraints

- Invariant: a key's stamp advances iff the resulting value state is propagated carrying that
  stamp. Loading is propagation-by-snapshot: loaded stamps are installed verbatim, never minted.
- **The opcode read path is unconditional; the write path is active-only.** (Spec D-7: "the
  single most important compatibility rule in the phase.")
- The P3 golden-buffer journal test must still pass with `--active_replica` off (byte-identical
  wire/file for non-active nodes; the breadcrumb aux and opcode must not appear).
- Do not edit `helio/`. Do not touch `src/core/dash.h`, `src/core/compact_object.*`,
  `src/server/engine_shard.{h,cc}`.
- `mvcc.cc` stays independent of `GetCurrentTimeMs()`.
- Build/test inside container: `docker exec drakeydb-p2 sh -c 'cd /src/build-dbg && ninja -j4 <target>'`
  (`-j4` mandatory, 8 GB VM). Pytest:
  `docker exec drakeydb-p2 sh -c 'cd /src && DRAGONFLY_PATH=/src/build-dbg/dragonfly /tmp/tv/bin/python -m pytest tests/dragonfly/<file> -x -q'`.
  Git runs on the host from `/Users/darkspadez/.paseo/worktrees/2wtglncc/roasted-moth`.
- Format: `~/.venvs/precommit/bin/pre-commit run --files <files>` before every commit.
- Commit lines ≤ 100 chars, imperative subject, suffix `(P4)`.
- **Falsify every test**: revert the code under test, observe the failure, restore, record the
  verbatim failure text in your report.
- CI uses `-Werror`; the tree sets `-Wno-unused-parameter`, so unused params are legal.
- Plan line numbers were verified against `5c470c91` (post-P4-1 merge) on 2026-08-29; treat them
  as strong hints, re-verify before editing.

## Verified anchors (post-merge, 5c470c91)

| What | Where |
|---|---|
| `SaveEntry(pk, pv, expire_ms, mc_flags, dbid)` | `rdb_save.cc:269`, decl `rdb_save.h:321` |
| EXPIRETIME→DF_MASK→type/key/value emission order | `rdb_save.cc:292-330` |
| `SaveAux` + `SaveAuxFieldStrStr` precedent | `rdb_save.cc:1758,1761-1765` |
| `SerializeEntry` (mc_flags lookup, external enqueue) | `serializer_base.cc:131-146` |
| `SerializeFetchedEntry(tde, pv)` delayed path | `serializer_base.cc:148-152` |
| `SerializeEntryLocked` virtual | `serializer_base.h:196`; overriders `snapshot.cc:253`, `journal/streamer.cc:751` |
| `TieredDelayedEntry{dbid, key, value, expire, mc_flags}` | `serializer_base.h:86-92` |
| `RDB_OPCODE_DF_MASK = 220`, `VECTOR_INDEX = 222` (221 free) | `rdb_extensions.h:47,58` |
| DF_MASK load case (`settings.mc_flags`) | `rdb_load.cc:2489-2494` |
| `ObjSettings` mc_flags fields + per-key `Reset()` | `rdb_load.cc:2340-2361` |
| `Item::has_mc_flags/mc_flags` | `rdb_load.h:401-402` |
| `HandleAux()` (no params today; `table-mem` `SimpleAtoi` precedent) | `rdb_load.cc:3016`, call `:2594` |
| Loader `{0,0}` stamp + promise comment + `RunWithoutMvccArm` | `rdb_load.cc:3276-3307` |
| Unrecognized-type hard fail | `rdb_load.cc:2672` |
| `SetMvcc`/`EnsureMvcc`/`SetExistingMvcc`/`GetMvcc`/`EraseMvcc` | `db_slice.cc:1353-1422`, decls `db_slice.h:361-376` |
| `mvcc_entries` accounting (insert/insert/erase) | `db_slice.cc:1369,1379,1421` |
| `DbTable::Clear()` resets `stats = DbTableStats{}` | `table.cc:132-141` |
| `kDbSz == 96` static_assert + stats `ADD()` merge | `table.cc:49-55` |
| pytest `num_shards` defaults to `proactor_threads - 1` | `tests/dragonfly/instance.py:122-128` |
| Acceptance test | `tests/dragonfly/multimaster_test.py:2186` |
| Loader pending-arm regression test | `multi_master_test.cc:1186` |
| KeyDB save: `rdbSaveAuxFieldStrStr(rdb,"mvcc-tstamp", szT)`, `%PRIu64` decimal, per key, before type/key/value, only if `fActiveReplica` | `KeyDB/src/rdb.cpp:1164-1168` (main checkout, read-only) |
| KeyDB load: `mvcc_tstamp = strtoull(auxval, nullptr, 10)`, applied to the next key | `KeyDB/src/rdb.cpp:3286-3288` |

---

### Task 1: Opcode constant and the save side

**Files:**
- Modify: `src/server/rdb_extensions.h` (new constant)
- Modify: `src/server/rdb_save.h:321`, `src/server/rdb_save.cc:269-330,1758-1766`
- Modify: `src/server/serializer_base.h:86-92,196-204`, `src/server/serializer_base.cc:131-152`
- Modify: `src/server/snapshot.h/.cc:253`, `src/server/journal/streamer.h/.cc:751`
- Modify: `src/server/db_slice.h/.cc` (one new `GetMvcc` overload)
- Test: `src/server/rdb_test.cc`

**Interfaces:**
- Consumes: `DbSlice::GetMvcc(DbIndex, string_view)` (`db_slice.h:371`), `MvccStamp` (`mvcc.h`).
- Produces: `RDB_OPCODE_DF_MVCC = 221`;
  `SaveEntry(const PrimeKey&, const PrimeValue&, uint64_t expire_ms, uint32_t mc_flags, DbIndex, const MvccStamp& mvcc)`;
  `SerializeEntryLocked(DbIndex, const PrimeKey&, const PrimeValue&, time_t, uint32_t mc_flags, const MvccStamp& mvcc)`;
  `TieredDelayedEntry::mvcc`;
  `std::optional<MvccStamp> DbSlice::GetMvcc(DbIndex, const PrimeKey&)` — **checks `db.mvcc`
  for null BEFORE calling `pk.GetSlice(&scratch)`**, so non-active nodes pay one branch, no
  allocation (same F5 trap the loader comment at `rdb_load.cc:3282-3296` documents).

- [ ] **Step 1: Write the failing byte-level test**

In `rdb_test.cc` (fixture with `--active_replica`; follow the file's existing serializer-buffer
tests). Serialize one stamped key and one unstamped key; scan the buffer:

```cpp
// Serialize a db slice holding key "k1" whose side-table stamp was set to
// MvccStamp{0x0123456789ABCDEFULL, 0xFEDCBA9876543210ULL} via db_slice->SetMvcc,
// and key "k0" left at MvccStamp{} (zero).
// Assert: the serialized buffer contains exactly one 0xDD (221) opcode byte,
// followed by Store64-LE(0x0123456789ABCDEF) then Store64-LE(0xFEDCBA9876543210),
// positioned before k1's type byte. Assert no 221 opcode precedes k0.
```

Also a gating test: with `--active_replica` off (or stamp zero), the buffer contains **no** 221
byte and no `drakeydb-mvcc` aux string.

- [ ] **Step 2: Run, observe failure** (`ninja -j4 rdb_test && ./rdb_test --gtest_filter='*Mvcc*'`
  in the container). Expected: no 221 byte found.

- [ ] **Step 3: Implement**

`rdb_extensions.h` beside 220/222:

```cpp
constexpr uint8_t RDB_OPCODE_DF_MVCC = 221; /* {mvcc, origin_hash}, 16 raw LE bytes */
```

`rdb_save.cc` in `SaveEntry`, immediately after the DF_MASK block (`:313`), before
`RdbObjectType`:

```cpp
if (!mvcc.IsZero()) {  // add IsZero() to MvccStamp if absent; zero == unstamped == absent
  uint8_t buf[17] = {RDB_OPCODE_DF_MVCC};
  absl::little_endian::Store64(buf + 1, mvcc.packed);
  absl::little_endian::Store64(buf + 9, mvcc.origin_hash);
  if (auto ec = WriteRaw(Bytes{buf, 17}); ec)
    return make_unexpected(ec);
}
```

`serializer_base.cc` `SerializeEntry` beside the `GetMCFlag` lookup (`:137`):

```cpp
MvccStamp mvcc = db_slice_->GetMvcc(db_index, pk).value_or(MvccStamp{});
```

Thread `mvcc` into both `SerializeEntryLocked` calls, into `EnqueueOffloaded` /
`TieredDelayedEntry` (new field beside `mc_flags`), and through both overriders to `SaveEntry`.
`RestoreStreamer::SerializeEntryLocked` (`streamer.cc:751`) accepts and **ignores** the
parameter, exactly as it ignores `mc_flags` (cluster mode is incompatible with active mode).

`rdb_save.cc` `SaveAux` (`:1758`), beside the existing fields:

```cpp
if (IsActiveReplica()) {
  // Breadcrumb so a stock Dragonfly logs "Unrecognized RDB AUX field: 'drakeydb-mvcc'"
  // before it hard-fails on opcode 221 (spec D-7).
  RETURN_ON_ERR(impl_->SaveAuxFieldStrStr("drakeydb-mvcc", "1"));
}
```

The compiler enforces the `SaveEntry`/`SerializeEntryLocked` signature change at every caller —
update them all; do not add a defaulted parameter (defaults hide missed call sites).

- [ ] **Step 4: Run tests, then falsify** — revert the `WriteRaw` block, observe the byte-scan
  test fail, restore. Record the failure text.

- [ ] **Step 5: Build `dragonfly` + run `rdb_test`, `multi_master_test`, `dfly_bench`-free ctest
  targets touched; pre-commit; commit** `feat: emit RDB_OPCODE_DF_MVCC per stamped key (P4)`.

### Task 2: The loader — persisted stamps replace `{0,0}`

**Files:**
- Modify: `src/server/rdb_load.h:401` (Item), `src/server/rdb_load.cc:2340-2361` (ObjSettings),
  `:2489` area (new opcode case), `:3276-3300` (apply + comment rewrite)
- Test: `src/server/multi_master_test.cc` (reload harness at `MvccStoreTest.Reload*`, pending-arm
  test at `:1186`), `src/server/rdb_test.cc` (round-trip)

**Interfaces:**
- Consumes: Task 1's opcode and byte layout; `DbSlice::SetMvcc(DbIndex, string_view, const MvccStamp&)`.
- Produces: `ObjSettings::{has_mvcc, mvcc}` and `Item::{has_mvcc, mvcc}` (`MvccStamp` by value),
  reset with the other per-key settings.

- [ ] **Step 1: Failing round-trip test** — in `MvccStoreTest` (reload harness with
  `GetTestTempPath` dirs already exists): write keys, capture `DEBUG MVCC` stamps, `SAVE` +
  restart-equivalent reload, assert stamps are **equal to the originals** (today they come back
  `{0,0}`). Add an absent-opcode case: load a snapshot produced with `--active_replica` off →
  stamps are `{0,0}` (the fallback survives).

- [ ] **Step 2: Run, observe both directions** — equality test fails now; fallback test passes
  (it pins existing behavior).

- [ ] **Step 3: Implement**

New case in the load-loop opcode switch (beside DF_MASK, `:2489`):

```cpp
if (type == RDB_OPCODE_DF_MVCC) {
  // Unconditional read (spec D-7): every binary must consume these 16 bytes.
  SET_OR_RETURN(FetchInt<uint64_t>(), settings.mvcc.packed);
  SET_OR_RETURN(FetchInt<uint64_t>(), settings.mvcc.origin_hash);
  settings.has_mvcc = true;
  continue;
}
```

Carry into `Item` where `mc_flags` is carried; apply at `:3297`, replacing the `MvccStamp{}`
argument:

```cpp
db_slice->SetMvcc(db_cntx.db_index, item->key,
                  item->has_mvcc ? item->mvcc : MvccStamp{});
```

`SetMvcc` no-ops on non-active nodes (`db.mvcc` null) — that IS the "parses, discards" row of
the spec's compatibility matrix; do not add an `IsActiveReplica()` check here. Rewrite the
`:3276-3280` comment block: the P4-2 promise is now fulfilled; keep the density rationale and
the F5 string_view-overload rationale.

- [ ] **Step 4: Extend the pending-arm regression** (`multi_master_test.cc:1186`): the loaded key
  in that test now carries a **non-zero** persisted stamp; assert the concurrent transaction's
  pending arm still commits its own stamp and the loaded key keeps the persisted one (this
  extends P4-1's closure of the loader/`running_tx_` residual to the stamped path).

- [ ] **Step 5: Run, falsify** — revert the `:3297` apply to `MvccStamp{}`, observe the
  round-trip test fail, restore, record.

- [ ] **Step 6: Compat runs** — `./multi_master_test`, `./rdb_test`, plus pytest
  `test_plain_replica_of_active_node_gets_full_unfiltered_stream` (a non-active consumer of an
  active node's snapshot stream must still load cleanly) and the P3 golden-buffer journal test
  with `--active_replica` off. Pre-commit; commit
  `feat: load persisted MVCC stamps, keep {0,0} for unversioned snapshots (P4)`.

### Task 3: KeyDB `mvcc-tstamp` read branch

**Files:**
- Modify: `src/server/rdb_load.h` (member + setter), `src/server/rdb_load.cc:3016-3110`
  (`HandleAux`), its call site `:2594`
- Modify: `src/server/replica.cc` (wire `SetLoadOriginHash` where the full-sync `RdbLoader` is
  constructed; pass `peer_origin_hash_`, which is 0 when unknown)
- Test: `src/server/rdb_test.cc` (synthetic bytes)

**Interfaces:**
- Consumes: Task 2's `ObjSettings::{has_mvcc, mvcc}`.
- Produces: `void RdbLoader::SetLoadOriginHash(uint64_t)`; `HandleAux(ObjSettings* settings)`.

- [ ] **Step 1: Failing synthetic-bytes test** — hand-craft an RDB byte string in `rdb_test.cc`:
  header/magic, then aux pair `"mvcc-tstamp"` → `"81985529216486895"` (decimal for
  `0x0123456789ABCDEF`, matching KeyDB's `%PRIu64` — verified against `KeyDB/src/rdb.cpp:1167`),
  then a string key/value, then EOF opcode. Load via `RdbLoader` with
  `SetLoadOriginHash(0xFEDCBA9876543210)`. Assert the loaded key's side-table stamp is
  `{0x0123456789ABCDEF, 0xFEDCBA9876543210}`. Add a second key **without** a preceding aux in
  the same stream and assert it loads `{0,0}` (one-shot semantics — KeyDB emits the aux per key;
  a stale value must not leak onto the next key).

- [ ] **Step 2: Run, observe failure** (unknown aux → warn + `{0,0}`).

- [ ] **Step 3: Implement** — thread `ObjSettings*` into `HandleAux` (it is a local in
  `RdbLoader::Load`; the aux precedes the key it describes and `settings` resets per key, which
  gives one-shot semantics for free — verify the reset ordering before relying on it). New
  branch beside `table-mem` (`:3098`):

```cpp
} else if (auxkey == "mvcc-tstamp") {
  // KeyDB (fActiveReplica) writes this decimal-u64 aux before every key
  // (KeyDB/src/rdb.cpp:1167). Same packed layout as MvccClock (D-3/D-4), no
  // tombstone bit. Author identity is not in the file; use the link's origin.
  uint64_t v;
  if (absl::SimpleAtoi(auxval, &v)) {
    settings->mvcc = MvccStamp{v, load_origin_hash_};
    settings->has_mvcc = true;
  } else {
    LOG(WARNING) << "Ignoring malformed mvcc-tstamp aux: '" << auxval << "'";
  }
}
```

A malformed value must warn and load the key unstamped — never fail the load.

- [ ] **Step 4: Run, falsify** — revert the branch, observe the unknown-aux warning path and the
  test failing on `{0,0}`, restore, record.

- [ ] **Step 5: Build `dragonfly`, run `rdb_test` + `multi_master_test`; pre-commit; commit**
  `feat: accept KeyDB mvcc-tstamp aux on RDB load (P4)`.

### Task 4: `mvcc_table_bytes` true cost

**Files:**
- Modify: `src/server/table.h` (`DbTableStats::mvcc_key_dup_bytes`), `src/server/table.cc:49-55`
  (static_assert 96→104, `ADD()`), `mvcc_table_memory()` in `table.h/.cc`
- Modify: `src/server/db_slice.cc:1353-1422` (accounting at all three points)
- Modify: `tests/dragonfly/multimaster_memory_test.py` (assertion 2 + module docstring)
- Modify: `docs/superpowers/specs/2026-08-25-phase4-mvcc-lww-design.md` (one-line D-13 metric
  definition update)
- Test: `src/server/multi_master_test.cc`

**Interfaces:**
- Produces: `mvcc_table_bytes = mvcc->mem_usage() + stats.mvcc_key_dup_bytes`, where
  `mvcc_key_dup_bytes` is maintained via the stored key's `MallocUsed()` at exactly the three
  mutation points that already maintain `mvcc_entries` (`db_slice.cc:1369,1379,1421`).
  `DbTable::Clear()` resets it for free (`stats = DbTableStats{}`, `table.cc:138`).

- [ ] **Step 1: Failing unit test** — insert N side-table entries with 32-char keys and N with
  8-char keys (through `SetMvcc`); assert the 32-char table's `mvcc_table_bytes` exceeds the
  8-char one by at least N × (heap remainder). Today they are equal (the measured P4-1 defect:
  identical 46,497,792 across key lengths 8–32).

- [ ] **Step 2: Run, observe equality failure.**

- [ ] **Step 3: Implement** — on insert: after `Insert`, `+= it->first.MallocUsed()`; on erase:
  `-= it->first.MallocUsed()` before `Erase`; `SetMvcc`'s overwrite path (not inserted) does not
  touch it (same key object). First **verify** `PrimeKey/CompactObj::MallocUsed()` returns the
  SmallString heap remainder for keys > `kInlineLen` (the benchmark docstring at
  `multimaster_memory_test.py:90-115` documents the SmallString split: 10 bytes inline, remainder
  heap). If `MallocUsed()` proves wrong for any key tag stored here, stop and report — do not
  substitute a length model. Enumerate every `db.mvcc->` mutation site
  (`grep -n 'mvcc->' src/server/db_slice.cc src/server/table.cc`) and prove each is either
  routed through the accounting or net-zero (defrag relocation of the same key is net-zero:
  same length, same size class).

- [ ] **Step 4: Rewrite benchmark assertion 2** — delete the external
  `_expected_duplication_bytes` compensation model; assert `mvcc_table_bytes` directly tracks
  the measured `used_memory` side-table delta within the existing tolerance, across all four
  key lengths. Update the module docstring (the blind spot is fixed, keep the history). Update
  the spec's D-13 metric sentence to say the field includes duplicated key heap bytes.

- [ ] **Step 5: Run, falsify** — revert the accounting, unit test fails, restore, record. Run
  the benchmark: `python -m pytest tests/dragonfly/multimaster_memory_test.py -x -q -m large`
  (it is `@pytest.mark.large`).

- [ ] **Step 6: Full `multi_master_test` + `ctest -L DFLY` smoke of touched targets; pre-commit;
  commit** `fix: account duplicated key heap bytes in mvcc_table_bytes (P4)`.

### Task 5: Multi-shard end-to-end stamp coverage

**Files:**
- Modify: `tests/dragonfly/multimaster_test.py:2186` (acceptance test) + one new test

**Interfaces:**
- Consumes: `DEBUG MVCC <key>` reply (parsed by the file's `_parse_mvcc`, includes the shard
  field), `df_factory.create(proactor_threads=N)` — `num_shards` defaults to
  `proactor_threads - 1` (`instance.py:122-128`), which is why P4-1's coverage was
  single-shard-only.

- [ ] **Step 1: Widen the acceptance test** — `proactor_threads=4` on both nodes (→ 3 shards),
  write ~12 keys (`k0..k11`), assert per-key stamp equality between A and B, and
  `assert len({stamp["shard"] for ...}) >= 2` so the test **fails loudly if it ever runs
  single-shard again** (mirror the C++ `ASSERT_GT(num_shards, 1u)` discipline from
  `multi_master_test.cc`).

- [ ] **Step 2: New test `test_stamps_survive_full_sync_and_restart`** — A (active,
  `proactor_threads=4`, `dir=tmp_path`) takes writes; B attaches and full-syncs → per-key stamps
  equal (this exercises Task 1+2 over the real replication snapshot, multi-shard); then A
  `SAVE`s, restarts with the same dir, and every stamp equals its pre-restart value (file path).
  Falsification for the full-sync leg: it must fail against a build with Task 2's apply
  reverted — coordinate with the Task 2 falsification evidence rather than re-reverting if the
  harness makes that impractical, and say which you did.

- [ ] **Step 3: Run both 10× for flake rate**
  (`--count` via loop; record pass rates). Pre-commit; commit
  `test: multi-shard e2e stamp propagation and persistence (P4)`.

### Task 6: Exit gate

**Files:** none new. Ledger: `.superpowers/sdd/2026-08-25-phase4-mvcc-lww/progress.md`.

- [ ] Full `ninja -j4 dragonfly && ctest -V -L DFLY` in the container — all pass, warning-free.
- [ ] Pytest: `multimaster_test.py`, `multimaster_memory_test.py -m large`,
  `replication_test.py`, `replication_specific_test.py`, `replication_resilience_test.py`.
- [ ] P3 golden-buffer journal test with `--active_replica` off — byte-identical.
- [ ] `mvcc_unstamped_writes == 0` after a pure write workload on an active node.
- [ ] Docs: `docs/PLAN.md` phase row for P4-2; document the stock-Dragonfly cliff (an active
  node's snapshot hard-fails a stock binary on opcode 221 — one-way door, file-only, never via
  replication because P3 admission refuses stock consumers) in `docs/differences.md`.
- [ ] Every falsification recorded verbatim in the ledger; pre-commit clean across the branch.
