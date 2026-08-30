# Issue register

A running list of defects and deferred work found while building drakeydb, for things that have
no home in the phase currently being worked on. Two parts:

- **Part 1 — Upstream Dragonfly bugs.** Reproduce with `--active_replica` off, so they are not
  ours. They belong in `dragonflydb/dragonfly` issues; until filed they live here so the next
  person does not rediscover them.
- **Part 2 — drakeydb deferred work.** Ours, deliberately not fixed yet, each with the reason and
  the phase that should close it.

Add entries as they are found. Delete an entry only when it is filed upstream (Part 1, with the
issue link recorded) or landed (Part 2). Every entry states how it was established, so a reader
can tell a live-proven defect from a static argument.

Related: [UPSTREAM-SYNC.md](UPSTREAM-SYNC.md) (merge workflow), [PLAN.md](PLAN.md) (phase plan).

---

## Part 1 — Upstream Dragonfly bugs

### U-1. Snapshot reads `mc_flags` from the live DbSlice, not the captured table

**Where:** `src/server/serializer_base.cc`, `SerializerBase::SerializeEntry` —
`db_slice_->GetMCFlag(db_index, pk)`.

`SerializerBase` captures the table array at `RegisterChangeListener`
(`db_array_ = db_slice_->databases(); // copy pointers to survive flush`) and serializes values
from it, but looks memcached flags up through the **live** `DbSlice`. A `FLUSHALL` during a save
swaps the live array for fresh empty tables, so values come from the captured table while flags
come from a table that no longer holds them: flags are lost, or — for keys re-created after the
flush — a pre-flush value is written with a post-flush key's flags.

Three routes reach it, including the non-obvious one: `ProcessBucket` →
`DbSlice::FlushChangeToEarlierCallbacks` hands an *earlier* registered consumer buckets owned by
the *traversing* consumer's captured table, so "is this an update?" does not identify the owning
table. Triggering that route needs two concurrent consumers (a BGSAVE plus a replica full sync,
or two replicas syncing).

**How established:** found and live-proven for the drakeydb MVCC stamp, which sat on the same
line and had the identical shape (P4-2 adversarial review; 2,996/4,000 keys lost their stamp and
763/1,000 got a fabricated value/attribute pair). We fixed our field by resolving against the
bucket's owning table (`&it.owner()`); `mc_flags` was deliberately left alone as out of scope.
The `mc_flags` case is argued from the shared mechanism, not separately reproduced.

**Status:** not filed. Lower severity than our case (memcached flags, not conflict-resolution
authority), but the same class.

### U-2. `rdb_bgsave_in_progress` stays 1 after a BGSAVE that overlapped a full sync

**Where:** `src/server/server_family.cc`, around `WaitUntilSaveFinished` (~`:2007`, `:3083`).

After a successful BGSAVE that overlapped a replica full sync, `INFO persistence` reports
`rdb_bgsave_in_progress:1` while `saving:0` and `rdb_last_bgsave_status:ok`. Monitoring and any
tooling that waits on that field hangs.

**How established:** observed live during P4-2 final review. Untouched by this fork
(`git diff` over the branch shows no hunk there); blamed to upstream `2612541a` (#5655).

**Status:** not filed.

### U-3. `DbTable::table_memory()` excludes hosted-object memory

**Where:** `src/server/table.h`, `DbTable::table_memory()` — returns `prime.mem_usage()` only.

`DashTable::mem_usage()` excludes, by its own documented contract, memory allocated by hosted
objects. For keys longer than `CompactObj::kInlineLen` (16 B) the heap-allocated remainder is
therefore invisible, so the reported table memory understates the real cost, which `used_memory`
does account for.

**How established:** exposed by the drakeydb memory benchmark in P4-1, which measured our
side table reporting an identical figure across key lengths 8–32 B while `used_memory` deltas
grew (48.0 / 48.0 / 63.3 / 78.5 MiB). We fixed *our* field in P4-2 by accumulating the duplicated
key bytes; upstream's field still has the gap.

**Status:** not filed. Arguably intended behavior — but the field's name invites the
misreading, and capacity planning uses it.

---

## Part 2 — drakeydb deferred work

### D-1. No mvcc half for Redis-protocol / KeyDB peer links (P7)

`serializer.cc` writes `mvcc` only under `extended_framing_`, i.e. `IsActiveReplica()`, and the
plain-Redis wire has no slot for it. Genuinely needs a wire mechanism that does not exist; the
design spec assigns it to P7. The `origin_hash` half **is** implemented. Never tested
end-to-end — three separate agents independently said so.

**Owner:** P7. **From:** P4-1.

### D-2. Old-peer upgrade is lockstep and undocumented

A drakeydb peer built before P4-2 still advertises `DRAKEY-VERSION 65`, so a P4-2 active master
will full-sync to it and the old peer hard-fails on opcode 221 mid-stream. Nothing refuses the
pairing in advance. Either bump the version floor or document that active meshes must be upgraded
together.

**Owner:** unassigned — should be settled before any deployment spanning versions. **From:** P4-2.

### D-3. `SORT ... STORE` does not replicate

Reproduced with `--active_replica` **off**, so the non-replication itself is upstream behavior.
It matters to us because the destination key *is* locally stamped and will carry authority for a
value no peer has — which is a merge hazard once LWW lands.

**Owner:** P4-3 (merge LWW) must decide whether to stamp it, skip it, or refuse it. **From:** P4-1.

### D-4. `origin_hash` residual on an expiry-swept sibling key

Wrong `origin_hash`, correct `mvcc`, and only on an exact tie — `operator<` is lexicographic on
`(Mvcc(), origin_hash)`. Closing it needs `Commit()` to accept a stamp differing from its
enclosing journal entry, which is a design change.

**Owner:** unassigned (revisit if ties become observable). **From:** P4-1.

### D-5. A non-active node still emits `node_uuid:` in INFO replication

So "byte-identical with `--active_replica` off" is true for the journal wire, the RDB file, and
INFO memory, but not for INFO as a whole.

**Owner:** unassigned; introduced in P1/P3. **From:** P4-1.

### D-6. Ownership-registry hardening

The thread-local prime→`DbTable` registry added in P4-2 (`table.cc`, maintained in `DbTable`'s
constructor and destructor) has three loose ends: `tl_prime_owners` has external linkage where its
own comment cites `snapshot.cc`'s anonymous-namespace precedent; `DbTable::FromPrime` returns a
mutable `DbTable*` from a `const PrimeTable*`; and a constructor `DCHECK` would turn the
release-mode orphan residual (a cross-thread destroy) into a CI-visible failure at no cost — a
reviewer ran exactly that as a `CHECK` across a four-thread hammer and it never fired.

**Owner:** any phase touching serialization. **From:** P4-2 final review.

### D-7. Registry's ≥3-table path has no dedicated test

`RdbMvccTest.EarlierConsumerStampsForeignBucketsFromTheOwningTable` passes even with
`DbTable::FromPrime` stubbed to return `nullptr`, because both probes capture the same table and
resolve via the pointer-compare fast path. The registry is exercised only by the direct `MvccOf`
probes in the sibling test; the ≥3-table case it exists for is closed by construction, not by a
test.

**Owner:** any phase touching serialization. **From:** P4-2 final review.

### D-8. Both stamp forms for one key are untested

If a single RDB stream carries both `RDB_OPCODE_DF_MVCC` and a KeyDB `mvcc-tstamp` aux for the
same key, the behavior is deterministic last-in-stream-wins (the opcode wins in natural order,
since the aux precedes the key). Coherent, but only reasoned about, never tested. No producer
emits both today.

**Owner:** P7 (KeyDB onboarding). **From:** P4-2 final review.

### D-9. Documentation loose ends

- `docs/differences.md` describes the stock-Dragonfly RDB cliff as a one-way door but omits the
  escape hatch: reload under `--active_replica=false` and re-save produces a stock-compatible
  file, losing the stamps. That escape hatch is the design spec's own stated motivation for
  making the read path unconditional.
- `docs/PLAN.md`'s top status table still reads "P4 … next up" and is dated 2026-08-25, three
  merged phases ago. Progress is recorded only in the Phase 4 prose section.
- Commit `0d59e9fc`'s message carries a wrong "63%" figure for the `mvcc_table_bytes`
  under-report (the true figure is ~43.6% below true cost). The code, the benchmark docstring,
  and `PLAN.md` were all corrected; the commit message cannot be without a rebase.

**Owner:** any phase. **From:** P4-2.

### D-10. Multi-shard pytest coverage is new and narrow

P4-2 widened the stamp acceptance test to three shards and added a full-sync-plus-restart test,
but the rest of `multimaster_test.py` still runs at one or two shards. Anything a future phase
asserts about shard routing needs its own multi-shard test — the file's default will not give it.

**Owner:** P4-3 onward. **From:** P4-2.
