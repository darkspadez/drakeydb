# drakeydb Phase 4 — MVCC Store, Stamping, and Last-Write-Wins — Design Spec

> Status: approved 2026-08-25. Branch `feat/phase4-mvcc-lww` off `origin/main`
> (`7ca2b8db`). Ships as six stacked PRs — see [PR stack](#pr-stack).
>
> This phase absorbs the roadmap's original P4 (store + stamping + wire), P5
> (streaming LWW guard), and P6 (merge-on-full-sync LWW), plus three pull-forwards:
> bounded tombstones (was a v1 non-goal), the clock-skew warning/metric (was P8),
> and the P3 carry-forward derived-DEL bug. `docs/PLAN.md` remains canonical for
> the fork as a whole; where this spec and PLAN.md disagree on P4 mechanism, **this
> spec is newer** and PLAN.md is updated to match at the end of the phase.

## Context

P3 left the fork one step short of resolving conflicts. A mesh node can send,
receive, and correctly attribute writes: journal framing v2 carries
`{origin_idx, mvcc, entry_flags}`, `journal::PassesPeerEchoFilter` stops
amplification on both the stable-sync and full-sync paths, and origin threads
end-to-end from `ConnectionContext` through `Transaction` into both the auto- and
manual-journal paths.

But nothing resolves conflicts:

- **`mvcc` is written as `0`** (`journal/serializer.cc:102-110`). The wire slot
  exists; nothing fills it.
- **No per-key version is stored anywhere.** `db_slice.{h,cc}`, `table.h`, and
  `compact_object.h` have no mvcc concept.
- **Streaming applies in arrival order.** Two nodes writing the same key
  concurrently diverge permanently.
- **Peer full sync clobbers newer local data.** `SetOverrideExistingKeys(true)` is
  behaviourally inert — `rdb_load.cc:3273` gates only a `LOG(WARNING)` on it;
  `AddOrUpdate` overwrites regardless.

Phase 4 closes all four, and the outcome is the fork's headline claim:
`--active_replica` delivers **strictly convergent per-key LWW** for state-carrying
commands, across streaming and merge, surviving restart, with delete-resurrection
closed.

## Decisions locked while grilling

| # | Decision | Rationale |
|---|---|---|
| D1 | Stored value is **16 bytes, `{u64 mvcc, u64 origin_hash}`**, compared lexicographically | KeyDB's `dbMerge` is `if (old_mvcc <= incoming) overwrite` (`KeyDB/src/db.cpp:384`) — incoming wins ties. In a mesh, A takes B's value and B takes A's: they swap, permanently. Ties are **common, not rare** — the 20-bit counter sits at 0 under light load, so two nodes writing in the same millisecond collide. `origin_hash` is a stable 64-bit hash of the **author** node's uuid (never the sender's) and makes the order total. |
| D2 | Side `DashTable<PrimeKey, MvccStamp>` on `DbTable`, mcflag-style, constructed only in active mode | Follows the one existing per-key-metadata precedent (`table.h:128`). `CompactObj` must not grow — `sizeof(CompactObj) == 18` is a hard static_assert (`core/compact_object.cc:613`). |
| D3 | Local stamp = **`max(clock_tick(), stored.mvcc + 1)`** | A backward-skewed node otherwise stamps a local write *below* the stored remote value: its client sees `OK`, every peer drops the write as stale, and nothing detects it. Per-key `max` repairs it exactly. Deliberately **not** a per-shard HLC ratchet — that lets one fast-clock peer poison a whole shard's clock permanently. |
| D4 | Stamp persists per key via **`RDB_OPCODE_DF_MVCC = 221`**; loader also accepts KeyDB's bare-u64 `mvcc-tstamp` | Dragonfly has no per-key aux facility, and extending DF_MASK would make older readers skip the flag without consuming its payload. A dedicated opcode fails deterministically on an unsupported loader. P7 onboarding still works because the loader reads both forms. |
| D5 | Multi-key commands are **split per key** | `MSET` and multi-key `DEL` are on the guard's own command list. All-or-nothing silently drops fresh updates to non-stale keys; single-key-only exempts a whole command class. Highest-risk piece of the phase. |
| D6 | **Bounded tombstones in scope**, persisted in the RDB, **GC'd at save time** | Closes delete-resurrection (risk register #7). Memory-only tombstones miss the headline scenario — a node restarting and full-syncing from a peer that still holds the deleted key. |
| D7 | **Stacked PRs on one branch**; the branch merges to main only when the whole stack is green | Keeps each PR P3-sized and independently reviewable while the final whole-branch review still sees the complete convergence story. |
| D8 | SDD **plus an adversarial review pass** | Sonnet 5 implementer per task; Opus 5 spec+quality review per task; Opus 5 whole-branch review per PR boundary; **plus one independent Opus reviewer per PR briefed only to refute the convergence claim**. P3 found seven vacuously-passing tests; this phase's central claims are distributed-systems properties, not observable behaviour. |
| D9 | Every new test is **falsified** before it counts | Revert the plumbing, observe the failure, restore, record the observed failure text in the SDD ledger. Carried from P3. |

**Explicitly unchanged.** Mesh-only / no-forward v1. RMW commands (INCR, APPEND,
LPUSH, HSET…) still resolve by arrival order and can still diverge — CRDTs remain
out of scope. `--multi_master_stream_lww=false` disables **only** the streaming
guard; merge LWW stays on, because KeyDB does merge LWW too and simply has no
streaming guard to disable.

## Corrections to the approved plan

The plan approved on 2026-08-25 was written before the apply-side design pass. Three
of its mechanism claims are superseded here. The **decisions** (D1-D9) are unchanged;
only the mechanisms that implement them move.

| Approved plan said | Corrected to | Why |
|---|---|---|
| Persist the stamp as a **new flag bit on `RDB_OPCODE_DF_MASK`** | A **new opcode, `RDB_OPCODE_DF_MVCC = 221`**, modelled on DF_MASK's threading | DF_MASK's reader consumes trailing fields only for flags it knows (`rdb_load.cc:2487-2496`). An unknown bit means our 16 bytes are never consumed and the next opcode is read out of the middle of a stamp — usually a garbage type error, occasionally a valid type byte and a **silent misparse**. A new opcode fails deterministically. |
| `repl_mvcc` is already plumbed; "P4 populates the field, it does not plumb it" | True **outbound only**. The inbound path drops it and must be built (T0) | `TransactionData::AddEntry` (`tx_executor.cc:74-79`) copies only `command`/`dbid`/`txid`; `TransactionData` has no `mvcc` field; `ConnectionContext::repl_mvcc` is written by nobody. |
| An applied write's `origin_hash` comes from the entry's `origin_idx` via `PeerRegistry` | It comes from the **link's peer uuid** | `PassesPeerEchoFilter` only forwards `origin_idx == kSelfIdx`, so every COMMAND arriving on a peer link carries `origin_idx == 0`. The registry lookup would resolve every foreign write to "self". Under no-forward v1 sender *is* author on the streaming path, so the link uuid is correct — and this is also why no framing change is needed. |

| `DFLY MVCC <key>` | **`DEBUG MVCC <key>`** | `DFLY` is `CO::GLOBAL_TRANS` (`server_family.cc:4308`), so a per-key stamp read would take a global transaction across every shard — serialising against all traffic and perturbing what it measures. `DEBUG` costs one shard hop, has the same `CO::ADMIN` posture, works during LOADING, and has the per-key precedent. |

Two further placement decisions were open in the plan and are now settled: the LWW
compare runs **inside the transaction under the key locks**, not before dispatch
(a pre-dispatch check causes permanent divergence — see D-9); and the per-key split
happens **inside `OpMSet`/`DeleteKeys`**, not as an executor-level command rewrite.

**One design-agent disagreement, resolved by direct verification.** The storage pass
proposed `DF_MASK_FLAG_MVCC = 1<<2` (no new opcode); the apply pass proposed a new
opcode. Reading `rdb_load.cc:2487-2496` settles it in favour of the new opcode: the
DF_MASK reader consumes trailing fields only for flags it recognises, so an unknown
bit leaves our 16 bytes in the stream. The failure is silent; a new opcode's is not.

**Two decisions gained an amendment rather than a reversal.** D1's 16-byte value now
carries the tombstone marker in reserved **bit 63 of `mvcc`**, masked before
comparison, so the lexicographic order is unchanged and the value stays 16 bytes
(D-10). And `DbContext::repl_mvcc` is a **hard prerequisite** the plan does not
mention — without it an applied delete cannot reproduce the peer's tombstone stamp
(D-5).

## Design

### D-1. Verified facts this design rests on (read at `7ca2b8db`)

Each was confirmed by reading the tree during the grilling session, not inferred
from PLAN.md. Several correct PLAN.md.

1. **The wire is half-plumbed — outbound only.** `EntryBase::mvcc`
   (`journal/types.h:41`) exists, and `Transaction::LogJournalOnShard`
   (`transaction.cc:1668`) already passes `repl_mvcc_` into `journal::RecordEntry`.
   **But the inbound path drops it**: `TransactionData::AddEntry`
   (`journal/tx_executor.cc:74-79`) copies only `command`/`dbid`/`txid` from the
   `ParsedEntry`, and `TransactionData` (`tx_executor.h:48-54`) has no `mvcc` field
   at all. `ConnectionContext::repl_mvcc` (`conn_context.h:370`) is written by
   nobody. So a stamp put on the wire today never reaches the applying transaction.
   **T0 exists to close that**; framing itself needs no change (self-describing
   packed-uint).

2. **`DbSlice::PostUpdate` is the single "this key was mutated" hook.**
   `db_slice.cc:1426`, called from exactly one place —
   `DbSlice::AutoUpdater::Run()` (`db_slice.cc:588`). Because `AutoUpdater` is
   returned by `FindMutable`/`AddOrFind`/`AddOrUpdate`, it catches **in-place RMW**
   (LPUSH, HSET) that never calls `AddOrUpdate` at all. Stamping anywhere else
   would miss those.

3. **`DbSlice::PerformDeletionAtomic` is the single deletion choke point.**
   `db_slice.cc:2025`, exactly one caller (`db_slice.cc:927`). It already erases
   the mcflag entry, gated on `del_it->second.HasFlag()` — a bit inside
   `PrimeValue`. There is **no equivalent bit for mvcc**, so either every delete
   pays an unconditional dash probe in active mode, or we take one of the two free
   bits at `core/compact_object.h:648` (`uint8_t unused : 2`). See D-4.

4. **Multi-key journal entries are already shard-scoped.**
   `Transaction::LogAutoJournalOnShard` journals `GetShardArgs(shard->shard_id())`
   whenever `unique_shard_cnt_ > 1`. A cross-shard `MSET k1 v1 k2 v2` therefore
   emits *two* entries, each carrying only its own shard's keys — a replica never
   sees an entry spanning the master's shards. **D5's split works on an
   already-narrowed key list.** It can still be >1 key: same-shard multi-key
   commands exist, and the replica may have a different shard count, so the entry
   re-shards on re-dispatch.

5. **Key extraction already exists.** `DetermineKeys(cid, args)` →
   `OpResult<KeyIndex>` (`main_service.cc:1251-1260`, wrapped by
   `Service::FindKeys`) is what dispatch itself uses. The split must reuse it and
   must not invent a parser.

6. **Dragonfly has no per-key RDB aux, but it has a per-key opcode pattern.**
   `RDB_OPCODE_AUX` (`rdb_save.cc:1369`) writes file-level key/value strings in the
   header. The per-key mechanism is `RDB_OPCODE_DF_MASK = 220`
   (`rdb_extensions.h:47`), written immediately before each key
   (`rdb_save.cc:299-311`), parsed at `rdb_load.cc:2487`, threaded via
   `RdbLoader::ObjSettings` into `Item` and consumed at `rdb_load.cc:3268-3271`.
   `DF_MASK_FLAG_MC_FLAGS` *is the mcflag side table's persistence* — the precedent
   is literally the table this phase mirrors, and it is a complete template to copy.

   **But DF_MASK must be copied, not extended.** Its reader
   (`rdb_load.cc:2487-2496`) reads the 4-byte mask, then reads `mc_flags` *only if
   that flag bit is set*, then `continue`s. A loader that does not know a new
   `DF_MASK_FLAG_MVCC` bit would skip the flag and **not consume our 16 bytes**,
   then read the first byte of the stamp as the next opcode — usually a garbage
   `Unrecognized rdb object type`, occasionally a *valid* type byte and a silent
   misparse. A **new opcode fails deterministically instead**, so D-7 uses one.
   (This reverses the mechanism recorded in the approved plan file; see
   [Corrections](#corrections-to-the-approved-plan).)

7. **`CompactObj` is `__attribute__((packed))`**, so `sizeof == 18` exactly with
   alignment 1 — a `DashTable` slot holds it unpadded. Dash geometry: 12 slots per
   bucket, `KeyType key[12]` + `ValueType value[12]`
   (`core/dash_internal.h:286, 312-313`). This is the basis for the memory
   estimate, which must nonetheless be **measured, not quoted**.

8. **The merge write site is unconditional.** `rdb_load.cc:3258`,
   `db_slice->AddOrUpdate(db_cntx, item->key, std::move(pv), item->expire_ms)`.
   `SetOverrideExistingKeys` does not gate it (`rdb_load.cc:3273` gates only a
   `LOG(WARNING)`), so **D-8 must implement the behaviour rather than assume it**.

9. **Expiry DELs already carry a flag.** `RecordExpiryBlocking` (`tx_base.cc:79-84`)
   sets `journal::kEntryFlagExpired`, and `PassesPeerEchoFilter`
   (`journal/types.cc:51-66`) suppresses those to peers. `entry_flags` is a `uint8_t`
   with only bit 0 used — spare bits are available.

### D-2. Stamp representation

```
struct MvccStamp {
  uint64_t mvcc;         // ms << 20 | counter   (KeyDB layout, KeyDB/src/server.cpp:7263-7287)
  uint64_t origin_hash;  // stable 64-bit hash of the AUTHOR node's uuid
};
```

Comparison is lexicographic on `(mvcc, origin_hash)` — a total order, so every node
independently reaches the same verdict. `origin_hash` must be **stable across
processes and builds** (it is persisted in the RDB and compared against values
written by other nodes), so it is an explicit hash of the 36-char uuid string, not
`std::hash`.

The KeyDB `mvcc` layout is kept verbatim so a KeyDB-sourced stamp (P7) compares
meaningfully against a locally-generated one.

**Where `origin_hash` comes from — and why not from the wire.** For a local write:
the node's own uuid. For a *streaming* applied write it must come from the **link's
peer uuid**, not from the entry's `origin_idx` — because `PassesPeerEchoFilter`
(`journal/types.cc:51-66`) only forwards entries whose `origin_idx == kSelfIdx`, so
**every COMMAND arriving on a peer link carries `origin_idx == 0`**, meaning "the
sender". Under no-forward v1, sender *is* author on the streaming path, so the
link's uuid is the correct and only available source. `Replica` already derives
`peer_origin_idx_` from `master_context_.master_node_uuid` (`replica.cc:473`);
`peer_origin_hash_` is computed alongside it.

The author/sender distinction still bites on the **merge** path: peer B's database
contains keys authored by C, so B's snapshot must carry each key's own
`origin_hash`. That is exactly why the stamp is persisted (D-7) rather than
reconstructed from the sender.

`uint64_t NodeUuidHash(std::string_view)` belongs in `node_identity.h` — XXH64 with
a fixed seed (precedent: `LockTag::Fingerprint`, `tx_base.cc:96`). It is persisted
and compared across nodes, so it must be stable across builds and architectures;
`std::hash` is not.

### D-3. `MvccClock`

New file pair `src/server/mvcc.{h,cc}`. `MvccClock` is a plain member of a
**thread-local** `MvccStamper`, not an atomic — that is the Dragonfly-native win
over KeyDB's process-global `g_pserver->mvcc_tstamp`
(`KeyDB/src/server.cpp:7263-7287`), which serialises every write in the process on
one cache line. Deliberately *not* on `EngineShard` or `ServerState`:
`engine_shard.{h,cc}` are on the untouched list, and a thread-local in the new TU is
per-proactor-thread, which is the right granularity (several `DbSlice`s may share a
thread via namespaces; the clock only needs to be monotone).

**Millisecond source: `GetCurrentTimeMs()`** (`engine_shard_set.h:155`) —
`absl::GetCurrentTimeNanos()/1e6`, 5-10 ns, with the header's own note that it is
2-4x faster than `clock_gettime(CLOCK_REALTIME)`. There is no cached-ms value to
reuse: `Transaction::time_now_ms_` is per-transaction and not reachable at
`PostUpdate` or `journal::RecordEntry`, and `ServerState`/`ProactorBase` have no
wall clock. The cost is a non-issue because **the clock is read once per
shard-callback, not once per write** (below), and `JournalSlice::CallOnChange`
already calls `GetCurrentTimeMs()` per journal entry — using the same source also
makes the stamp's ms field agree with `JournalItem::time_ms` by construction.
It honours `TEST_current_time_ms`, which keeps the *integration*-level clock
deterministic; the `mvcc_test` units do not rely on that, because they pass `now_ms`
explicitly (see below).

**The clock is read by the caller, not inside `mvcc.cc`.** `MvccClock::Next`,
`MvccClock::AheadMs` and `MvccStamper::HopStamp` take `now_ms` as a parameter
(`MvccStamper::Commit` needs no clock at all — it stores the stamp it is given and
`DCHECK`s that one was given, which is what makes "the stamp in the table is the stamp
on the wire" structural rather than conventional); `mvcc.cc` does not include `engine_shard_set.h`. This is a
link-layering requirement, not a style choice: `mvcc.cc` compiles into
`dfly_transaction`, while `GetCurrentTimeMs()`'s `TEST_current_time_ms` is defined in
`engine_shard.cc` in `dragonfly_lib`, and only the `dragonfly_lib -> dfly_transaction`
edge is declared. Reaching up would require declaring a circular dependency between two
upstream CMake targets — the largest available widening of the upstream merge surface,
against this phase's global constraints. It also matches the fork's existing discipline
(`engine_shard.cc` resolves `IsActiveReplica()` and passes a bool down through
`DeleteExpiredOptions` rather than letting `db_slice` read the flag), and it makes the
unit tests strictly more deterministic than mutating a process-global override.
`JournalSlice::CallOnChange` already computes `GetCurrentTimeMs()` per journal entry, so
the Phase 4 hook has the value in hand at the one site that needs it.

```cpp
uint64_t MvccClock::Next(uint64_t now_ms) {
  const uint64_t cand = now_ms << 20;          // KeyDB layout: ms << 20 | counter
  last_ = (cand > last_) ? cand : last_ + 1;   // strictly increasing, always
  return last_;
}
```

**NTP step-back.** If `now_ms` retreats, `cand <= last_` and `Next` returns
`last_ + 1`; stamps advance one per hop until the wall clock catches up. Same
semantics as `incrementMvccTstamp`, without the atomic. Consequence to document and
expose: after a step back of D ms this shard stays D ms "in the future" and wins
every conflict for D ms — surfaced as `mvcc_clock_ahead_ms` (D-13).

**Counter overflow.** `last_ + 1` carries out of the 20-bit counter into the ms
field automatically, exactly as KeyDB's `fetch_add(1)` does. No special case.

**20 bits is ample, for a reason specific to this design.** 2^20 = 1,048,576 stamps
per ms per shard = ~1.05 G/s/shard; a Dragonfly shard tops out around 1-5 M ops/s,
so a sustained 5 M writes/s/shard uses **0.48%** of the counter space. And because
the stamp is minted **once per shard-callback**, consumption is per-*hop*, not
per-*key*: a 100-key `MSET` or a 50-command squashed `EXEC` consumes one value, not
100 or 50. **Do not widen the counter** — KeyDB wire parity for P7 `mvcc-tstamp`
ingest depends on `ms << 20 | counter`.

**One stamp per shard-callback.** `MvccStamper::HopStamp(now_ms)` memoises the value for
the current callback, with a self-healing backstop: if the memo is older than
`kMaxEpochMs` (50 ms) it re-mints and increments `mvcc_stale_epoch`, so a missed
epoch end degrades and is *visible* rather than silently reusing a stale stamp.
This memo is what makes author and applier stamps bit-identical even when the
author lumps several journal entries into one callback (D-5).

### D-4. Side table, its touch-points, and its real cost

`DbTable` gains `std::unique_ptr<MvccTable> mvcc`, allocated in the constructor
(`table.cc:110-115`) only when `IsActiveReplica()` — so a non-active node pays 8
bytes per `DbTable`, not a table. `DbSlice` gains a `mvcc_enabled_` bool
initialised once from the flag, beside `journal_omit_redundant_writes_`
(`db_slice.cc:477`), so the hot-path check is one predictable branch.

#### Real memory cost — computed, not estimated

`ExpireTablePolicy` is `kSlotNum = 14, kBucketNum = 56, kUseVersion = false`
(`detail/table.h:43-45`), `kStashBucketNum = 4`. With `sizeof(CompactObj) == 18`
and `alignof == 1` (packed):

```
BucketBase<14>            26   (cross-checks the in-tree static_assert
PrimeKey key[14]   14x18 = 252   sizeof(BucketBase<12>)==24 -> 4+12+4+4)
pad to alignof(MvccStamp)   2
MvccStamp value[14] 14x16 = 224
sizeof(Bucket)            504,  Segment = 60 buckets = 30,264 B / 840 slots
```

**36.03 B/slot**, ~41.2 B/key at a 87.5% load factor. For comparison the prime
table (`kUseVersion = true`, `VersionedBB<14>` = 34, value is an 18-byte
`PrimeValue`) is 538 B/bucket -> **38.46 B/slot**. So:

> **The mvcc side table costs ~94% of the prime table's per-slot overhead — it
> roughly doubles table memory in active mode.**

**Plus a term the plan's estimate omitted entirely.** `kInlineLen = 16`
(`compact_object.h:154`): keys of <=16 bytes live inline in the 18-byte slot at zero
extra heap, but **longer keys are heap-allocated a second time** in the side table.
mcflag does the same, but mcflag is sparse and this table is dense.

| Workload (1M keys) | table | key duplication | total |
|---|---|---|---|
| 16-byte keys | 41 MB | 0 | **~41 MB** |
| 24-byte keys | 41 MB | ~32 MB | **~73 MB** |
| 32-byte keys | 41 MB | ~38 MB | **~79 MB** |

Against a ~155-165 MB baseline for 1M x 100-byte values, that is **+26% RSS for
short keys and +45% for 24-byte keys**. If the target deployment uses long keys
that is a materially different memory story, and it should be surfaced before P4
lands rather than after the benchmark.

#### The metric trap

`DbTable::table_memory()` (`table.h:174-176`) returns **`prime.mem_usage()` only** —
mcflag has never been counted. Therefore:

- **`table_used_memory` in INFO will NOT move.** A benchmark comparing it reports a
  ~0 delta and is simply wrong.
- **`used_memory` WILL move**: `DbTable` is constructed with the shard's
  `MiMemoryResource` (`table.cc:110`), whose `used()` feeds
  `EngineShard::UsedMemory()`. So the side table is inside `used_memory` and **is
  subject to `maxmemory` enforcement** — which is the right safety property, and is
  precisely why the tombstone hard cap in D-10 is necessary: eviction cannot free
  tombstones.

**Do not fold `mvcc->mem_usage()` into `DbTable::table_memory()`** — that breaks
`DCHECK_EQ(table->table_memory(), table_before)` at `db_slice.cc:2087`, which asserts
that deletes do not shrink the prime table. Track it in a separate
`mvcc_table_memory_` accumulator maintained in `CreateDb` and `FlushDbIndexes`.

#### Touch-points that must mirror

Beyond the obvious add/delete paths, these are the ones that would otherwise rot
silently:

| Site | Action |
|---|---|
| `DbSlice::DefragTableSegments` (`db_slice.cc:2005-2023`) | **Mirror with a second loop** over `db_table->mvcc` and its own cursor. Nothing else relocates the side table's segments, so without this it accumulates mimalloc fragmentation forever. |
| Per-object defrag (`engine_shard.cc:375`) | **No action** — it relocates a `PrimeValue`'s heap; the side table holds its own key copy. |
| `DbSlice::FlushDbIndexes` (`db_slice.cc:1055-1106`) | Free: it moves the whole `DbTable` out and `CreateDb`s a fresh one, so FLUSHALL/FLUSHDB drop the table and every tombstone. Only the `mvcc_table_memory_` decrement is needed. |
| `DbTable::Clear` (`table.cc:126-132`) | **Dead code** — no caller. Mirror for hygiene, do not rely on it. |
| `OpRen` / `OpCopy` / `OpMove` / `OpRestore` (`generic_family.cc`) | **No new code.** The stamp does not "move": the source is tombstoned by the delete path and the destination is armed and committed by the command's own journal entry — which is correct, because the applier replays the same command and reaches the same two decisions. |
| `SerializerBase::SerializeEntry` (`serializer_base.cc:132-147`) | Fetch the stamp here, beside the existing `mc_flags` lookup, and thread it to `SaveEntry`. |
| `DbSlice::GetStats` (`db_slice.cc:493-514`) | `mvcc_table_bytes`, `mvcc_entries`, `mvcc_tombstones`. |

#### The debug invariant

```
DCHECK_EQ(db.mvcc->size() - db.mvcc_tombstones, db.prime.size())
```

**Every live prime key has exactly one non-tombstone stamp, and vice versa.** O(1),
sampled from `DbSlice::OnCbFinishBlocking`. It fires on a delete path that erases
from `prime` without touching the side table, an insert path that bypasses
`PostUpdate`, a tombstone whose key was never erased, and a forgotten flush mirror.
Pair it with a full-scan `TEST_VerifyMvccTable()` for the gtests and a
`DEBUG MVCC VERIFY` subcommand — that is the one that catches a missed defrag
mirror.

### D-5. Stamping: the landmine, the invariant, and arm/commit

#### The landmine — verify this before writing a line of code

```
transaction.cc:717   db_slice.OnCbFinishBlocking();          <-- obvious epoch end
transaction.cc:730     LogAutoJournalOnShard(shard, result);   <-- entry emitted HERE
transaction.cc:1541  db_slice.OnCbFinishBlocking();          <-- same inversion
transaction.cc:1543    LogAutoJournalOnShard(shard, result);
```

*(Confirmed by direct read at `7ca2b8db`.)* `OnCbFinishBlocking` is the natural
place to end a stamping epoch and it runs **before** the journal entry is emitted.
Put the epoch end there and every auto-journaled command — `INCR`, `LPUSH`, `HSET`,
`SETRANGE`, `APPEND`, `ZADD`, `SADD`, i.e. the majority of writes — arms, discards,
then journals with an empty arm list. The stamp never lands. **Peers still converge
by arrival order, every pytest still passes, and the streaming LWW guard then
silently does nothing because every stamp is 0.** This is the single most likely way
P4 ships broken-and-green.

The epoch must end as the **last statement** of `Transaction::RunCallback`
(after `:730`) and of `Transaction::RunSquashedMultiCb` (after `:1543`), plus at the
exit of the four non-transactional sweep paths in `db_slice.cc`
(`DeleteExpiredStep`, `ExpireAllIfNeeded`, `FreeMemWithEvictionStepAtomic`, and per
bucket-batch in `FlushSlotsFb`). A dedicated gtest must fail on exactly this
placement.

#### The invariant, stated precisely

Not quite "stamp iff journaled". The correct form is:

> **The stamp must advance if and only if the resulting value state is propagated
> to peers carrying that same stamp.**

Two channels qualify: a journal COMMAND entry, and the full-sync RDB snapshot (once
D-7's per-key stamp ships). The distinction matters because it rescues
`DbSlice::IsOmittableWrite` (`db_slice.cc:2152-2166`), where a write is deliberately
*not* journalled because a single snapshot consumer will carry it — stamping there
is correct and required.

#### Does every `AutoUpdater::Run()` correspond to a journaled write? **No.**

The over-arm paths, in decreasing severity. Note `Run()` is reached both
explicitly (~35 sites) and via the destructor (`db_slice.cc:538`), so "the caller
didn't call Run()" is not a defence.

1. **`GETEX <key>` with no options — a pure read that over-arms.** Registered
   `CO::JOURNALED | ... | CO::NO_AUTOJOURNAL` (`string_family.cc:1850`); `CmdGetEx`
   calls `FindMutable` unconditionally (`:1425`) but journals only inside
   `if (shard->journal() && exp_params.IsDefined())` (`:1436`). Reachable from any
   client. No `RecordEntry` follows, so the arm is discarded at epoch end rather
   than committed as a stamp. **This still kills the idea of gating on
   `cid_->IsJournaled()`** — GETEX *is* journaled even though this form emits no entry.
2. **`EXPIRE key ttl {XX|NX|GT|LT}` that is not satisfied.** `OpExpire` runs
   `post_updater.Run()` (`generic_family.cc:847`) *before* `UpdateExpire`, which
   returns `SKIPPED` when the predicate fails; the journal block is gated on
   `res.ok()` and EXPIRE is `NO_AUTOJOURNAL`.
3. **Lazy collection-field expiry on a read path — the structural one.**
   `hset_family.cc:996`, `set_family.cc:1178`, `doc_accessors.cc:220`. The mutation
   is real and the non-journaling is *by design* (each node expires its own fields).
   **There is no call-site fix**, which is what rules out "stamp in `PostUpdate` and
   patch the offenders".
4. **Mutation followed by a non-OK status** — `LogAutoJournalOnShard` returns early
   at `transaction.cc:1645`.
5. **`NO_AUTOJOURNAL` commands that journal conditionally** — 53 registrations;
   `OpDelV2` (`generic_family.cc:1263`) is the clean example.

**Symmetric no-ops are fine.** A command that mutates nothing but still emits an
entry (`SETRANGE k 0 ""`, `PERSIST` on a TTL-less key) is harmless: the applier runs
the identical command through the identical path and reaches the identical
stamp-or-not decision. The general principle: **the applier executes the same code
path as the author, so any hook inside that path is automatically symmetric.** Only
effect-rewriting (`SPOP`->`SREM`) and callback-level grouping break it — the hop
memo (D-3) handles both.

#### Arm / commit

`PostUpdate` **arms** `(db, key)` into a thread-local `MvccStamper`;
`journal::RecordEntry` (`journal/journal.cc:90` — the single funnel for every
COMMAND entry, reached from `LogJournalOnShard`, both `RecordJournal` overloads,
`RecordDelete`, and `RecordExpiryBlocking`) **mints** the stamp if the caller
supplied none and then **commits** it to every armed key. End-of-epoch discards
anything still armed and increments `mvcc_unstamped_writes`.

Arming uses a reused arena plus `(offset, len)` rather than a `std::string` per arm:
libstdc++ SSO is 15 chars, so a 16-byte key would allocate on every write.

**Deletes stamp directly, not through arm/commit**, because `ExpireIfNeeded`
journals *before* deleting. `PerformDeletionAtomic` must first **disarm** the key —
`OpDelV2` runs `post_updater.Run()` before `Del` and journals after, so without the
disarm the commit would overwrite the tombstone with a value stamp for a key that no
longer exists.

**`DbContext::repl_mvcc` is a hard prerequisite** the approved plan does not
mention: without it an applied delete cannot reproduce the peer's tombstone stamp,
and `RENAME`/`GETDEL`/`RESTORE REPLACE` diverge between author and applier. Add it
beside `repl_origin_idx` (`tx_base.h:76`) and copy it in
`Transaction::GetDbContext()` — an exact mirror of what P3 did for origin.

#### Why author and applier stamps are bit-identical

- **Author**: `entry.mvcc = HopStamp(GetCurrentTimeMs())`, `origin_idx = kSelfIdx` -> stores `{S, H_A}`.
- **Applier**: the parsed entry's mvcc reaches `repl_mvcc`, so `RecordEntry` sees a
  non-zero value and does **not** re-mint; the authenticated link's
  `peer_origin_hash_` supplies A's uuid hash -> stores `{S, H_A}`. Identical.
- **Lumping**: if one author callback arms `k1` and `k2` and then emits entries X and
  Y, the author's first commit flushes both arms with X's mvcc while the applier
  stamps `k1` from X and `k2` from Y. Because X and Y were minted from the *same
  memoised hop stamp*, `X.mvcc == Y.mvcc` and both sides agree. Without the hop memo
  this design would be off by one counter tick on every effect-rewriting command.

`origin_hash` must be the **author's**, never a receiver-local interpretation of a
wire index. Peer-applied streaming entries use `origin_idx = kSelfIdx`, so author
identity comes from the authenticated link UUID already stored as
`peer_origin_hash_`; `PeerRegistry` indices are not portable across nodes.

#### The residual, made visible

`mvcc_unstamped_writes` (incremented by the discarded arm count at each epoch end)
is the falsifier for the whole list above and the canary to watch when enabling
`--active_replica`: it must stay **0** on a pure write workload, and moves by 1 for
each discarded over-arm such as `GETEX k`, unsatisfied `EXPIRE ... XX`, or `HTTL`
on a hash with expired fields. These operations do not leave a stamp behind. This
is strictly better than a `DCHECK`, because case 3 makes a hard assertion impossible.

### D-6. Wire

Populate `EntryBase::mvcc` from the transaction's allocated stamp. No framing
change (D-1.1). The `origin_hash` is **not** added to the wire: a peer receiver uses
the authenticated link UUID's `peer_origin_hash_`. Adding a second identity field
would be redundant and would widen every entry.

### D-7. RDB persistence

**New opcode `RDB_OPCODE_DF_MVCC = 221`** (free: 220 DF_MASK, 222 VECTOR_INDEX,
223 SHARD_DOC_INDEX, 224 TAGGED_CHUNK), carrying 16 raw LE bytes
`{mvcc, origin_hash}`, emitted per key in `RdbSerializer::SaveEntry`
(`rdb_save.cc:269`) beside the existing `DF_MASK` block at `:298`, only when the
stamp is non-zero. Threading mirrors `mc_flags` exactly: the side-table lookup goes
in `SerializerBase::SerializeEntry` (`serializer_base.cc:132-138`), which means
widening the `SerializeEntryLocked` virtual (`serializer_base.h:196`) — only **two**
overriders exist: `SliceSnapshot` (`snapshot.cc:253`) and `RestoreStreamer`
(`streamer.cc:751`, which already ignores `mc_flags` and correctly ignores this too,
since active mode is incompatible with cluster mode).

**The write is gated on `IsActiveReplica()`. The read is unconditional.** This is
the single most important compatibility rule in the phase, and it mirrors the
discipline P3 already applied to the journal reader (deliberately version-agnostic,
never consults `IsActiveReplica()` — `serializer.cc:295-299`). Without it, an
operator restarting an active node *without* the flag cannot load its own snapshot.

Also emit a file-level breadcrumb aux `drakeydb-mvcc: 1` in `RdbSaver::SaveAux`
(`rdb_save.cc:1758`), so a stock Dragonfly logs `Unrecognized RDB AUX field:
'drakeydb-mvcc'` *before* it dies on the opcode.

**KeyDB fallback.** KeyDB's `mvcc-tstamp` arrives as a per-key `RDB_OPCODE_AUX`
string pair. `HandleAux` (`rdb_load.cc:3012`) already warns-and-ignores unknown keys
(`:3100-3103`); add a branch storing into `ObjSettings` with
`origin_hash = merge_origin_hash_` (the sending master's uuid hash), per D4.
`HandleAux()` needs `ObjSettings*` threaded in — it is currently a local in
`RdbLoader::Load`. **The exact encoding (decimal string vs. raw 8 bytes) must be
verified against `KeyDB/src/rdb.cpp:1164-1168` at implementation time**; the KeyDB
tree is gitignored and lives only in the main checkout, not in this worktree.

**Stamp for a key with no aux.** An unversioned snapshot has no sound ordering
relative to stamped live data. In particular, its `ctime` can be ahead of the
loader after a wall-clock rollback, so synthesizing `ctime_ms << 20` can make an
old snapshot overwrite newer live writes. Use the conservative `{0,0}` fallback:
an absent destination still loads normally, while any stamped resident value wins
a merge. This may prefer resident data during the first mixed-version merge, but it
never fabricates authority the snapshot does not contain and is rollback-safe.

**Compatibility matrix.**

| Loader | Sees `RDB_OPCODE_DF_MVCC` | Sees KeyDB `mvcc-tstamp` aux |
|---|---|---|
| Active drakeydb | parses, seeds the side table | parses, origin = sender uuid hash |
| Non-active drakeydb | parses, **discards** | parses, **discards**, silently (amended in P4-2: this cell said "warns per key, ignores"; the implementation warns only on a *malformed* or unrepresentable value, because a per-key warning on a loader that correctly ignores a well-formed value is log spam) |
| Stock Dragonfly | **hard fail** — `Unrecognized rdb object type: 221` (`rdb_load.cc:2670-2678`) | warns per key, loads fine |

The stock-Dragonfly cliff is a one-way door and must be documented. It is **not**
reachable via replication — P3's `DRAKEY-VERSION >= 65` admission already refuses
stock consumers — only by handing an RDB file to a stock binary.

**Tombstone persistence** is a separate opcode, `RDB_OPCODE_DF_TOMBSTONES = 225`,
one batch per shard in the shard epilogue, format
`[db_index, count, count x {key, mvcc, origin_hash, deadline_ms}]` — modelled on
`RDB_OPCODE_SHARD_DOC_INDEX` (`rdb_extensions.h:63-65`). Emitted only in active mode
with a non-empty table; parsed unconditionally; installed only if active and
`deadline_ms > now`. Save-time GC (D6) drops already-expired entries rather than
writing them to immediately expire.

### D-8. Merge-on-full-sync LWW

Hook in `RdbLoader::CreateObjectOnShard` (`rdb_load.cc:3173`), immediately before
the unconditional `AddOrUpdate` at **`rdb_load.cc:3258`** — after chunked-value
reassembly, so it runs exactly once per key, on the target shard's thread. Read the
incoming stamp (or the `{0,0}` unversioned fallback), compare against the stored stamp
**including tombstones**, and `return` without writing when the stored value wins;
otherwise `AddOrUpdate` as today and then `SetStamp`.

**This is not a quiescent window.** `main_service.cc:1403` allows `is_replicating`
commands during `LOADING`, so other peers' stable-sync applies run concurrently with
a merge load on the same shard threads. The `GetStamp` read and the
`AddOrUpdate`+`SetStamp` write must sit on the same shard thread with no yield
between them.

Cost accepted: a losing key's value is fully deserialized before being dropped.
Moving the check earlier to `ShouldDiscardKey` (`rdb_load.cc:3481`) is **wrong** —
that runs on the loader fiber, not the target shard, so it would be a cross-thread
read of the side table.

**Do not repurpose `SetOverrideExistingKeys`.** It is inert only in the sense that
it gates a `LOG(WARNING)`; it is not unused. It has three live callers —
`server_family.cc:1679` (DEBUG LOAD / restore), `replica.cc:1278` (plain-replica DF
full sync), and `replica.cc:696` (peer redis-path merge) — and the first two must
**not** get merge-LWW: a plain replica must load its master's snapshot verbatim.
Add a separate `RdbLoader::SetMergeLww(bool, uint64_t sender_origin_hash)` called
only from the two peer-mode sites (`replica.cc:696-706`, `replica.cc:1277-1278`),
and leave `SetOverrideExistingKeys` as the warning suppressor it already is.

**Tombstones on this path do two jobs, not one.** (a) *Read* — a stored tombstone
makes an incoming stale resurrection lose; free, once `GetStamp` returns tombstone
stamps. (b) *Apply* — if peer B deleted `K` at t=100 while we were down and we still
hold `K@t=50`, the merge must **delete** our `K`. Without (b) the resurrection hole
is only half closed. Ordering of tombstone application relative to the key stream
and the concurrent journal blob is irrelevant **provided tombstone application is
itself LWW-guarded** — state that in the code comment, because it is the reason no
ordering constraint is needed.

### D-9. Streaming LWW guard and the per-key split

Gated by `--multi_master_stream_lww` (default true), per link, not per command:
`peer_mode_ && IsActiveReplica() && flag`. **A plain replica must never be
guarded** — it has one master, arrival order is authoritative, and dropping would be
silent data loss. Gating on `peer_mode_` (rather than on `origin_idx != kSelfIdx`)
makes that explicit and survives local transactions gaining a non-zero `repl_mvcc`.

#### The compare must run under the key locks

**A pre-dispatch check in `JournalExecutor::Execute` produces permanent
divergence**, not a transient one: a local write `W@t2` landing between the check
and the apply of `V@t1` (t1 < t2) means we write V and stamp t1, while the peer
applied W and kept it — and neither side ever re-sends. That is precisely the
conflict case the feature exists for, so it reproduces under any conflict-load test.

The compare therefore sits in **`Transaction::RunCallback` (`transaction.cc:687`)**,
which covers both the normal (`:619`) and optimistic-during-schedule (`:1319`) call
sites and so inherits exactly the locking the write would have had — plus
**`Transaction::RunSquashedMultiCb` (`transaction.cc:1517`)**, a second run point
that bypasses `RunCallback` entirely. The second is unreachable from today's apply
path but must be covered or the next refactor silently opens a hole.

Skipping the callback is **not** sufficient on its own: `SETNX`, `GETDEL`,
`PERSIST`, and `RESTORE` are auto-journaled, so `LogAutoJournalOnShard` must be
suppressed on the drop path too, or a dropped write still reaches sub-replicas.

#### The classifier vocabulary is much smaller than it looks

Dragonfly **normalizes on the master before journaling**, so most of the plan's
command list never appears on the wire at all. `SETEX`/`PSETEX`/`GETSET`/`GAT`/
`SET`-with-options all funnel through `SetCmd::RecordJournal`
(`string_family.cc:1075-1098`) and emit plain `SET`. `UNLINK` emits `DEL`
(`generic_family.cc:1288`). `EXPIRE`/`PEXPIRE`/`EXPIREAT`/`PEXPIREAT`/`GETEX` emit
`PEXPIREAT` or `DEL` (`generic_family.cc:861-882`, `string_family.cc:1436-1449`).
`RENAME`/`COPY` emit `DEL src` + `RESTORE dest ... REPLACE ABSTTL`
(`generic_family.cc:511`, `:578`). `MSETNX` emits `MSET`. **`COPY`, `UNLINK`,
`EXPIRE`, `SETEX`, `GETSET`, `GETEX`, and `GAT` never reach the guard.**

The guarded set is eight rows, keyed on the **journaled** name:

| Journaled name | Class | Note |
|---|---|---|
| `SET` | single-key | Normalized form of the whole SET family; all options are argument-carried. |
| `SETNX` | single-key **+ rewrite to `SET`** | **Trap.** Journals *verbatim* and conditional-on-existence (`string_family.cc:1837`), so applying it reproduces the author's *command*, not the author's *result*. Rewriting matches what `SetCmd::RecordJournal` already does for `SET ... NX`. Gate the rewrite on the flag so `false` stays KeyDB-parity. |
| `MSET` | multi-key, self-guarded | Also the normalized form of `MSETNX`. |
| `DEL` | multi-key, self-guarded | Normalized form of DEL/UNLINK/expired-SET/expired-EXPIRE/RENAME-src. |
| `GETDEL` | single-key | Auto-journaled verbatim; effect identical to `DEL` on the replica. |
| `PEXPIREAT` | single-key | Absolute ms, so state-carrying. |
| `PERSIST` | single-key | Auto-journaled verbatim. |
| `RESTORE` | single-key **+ inject `REPLACE`** | **Trap.** Without `REPLACE` it errors if the key exists, and `DispatchCommand` reports a reply-level error as `OK` — so it diverges **silently**, with no apply failure and no metric. |

Deliberately **unguarded**: all RMW (`INCR`, `APPEND`, `SETRANGE`, `HDEL`, `SREM`,
`SADD`, `ZADD`, `XADD`, `LPUSH`, `JSON.MSET`, ...). Dropping an `INCR` because the
stored stamp is newer **permanently loses an increment** — strictly worse than
arrival order. Note `SETRANGE` and `APPEND` live in the "SET family" but are RMW and
journal verbatim; they must not be guarded. `MOVE` is `CO::GLOBAL_TRANS`, so
`DetermineKeys` yields an empty `KeyIndex` and there is nothing to compare.

**A static name-to-class table, not a `CO::` flag bit.** Three reasons: (a) there is
no `CO::WRITE` — the nearest, `CO::JOURNALED`, covers every RMW command, so deriving
from it would guard `INCR`, which is actively wrong; (b) a `CO::` bit lives on the
*client* command while the guard sees the *journaled* one, and the two vocabularies
are not in bijection (`COPY` would need the bit but never arrives; `RESTORE` arrives
from three different client commands); (c) an unknown name defaulting to *unguarded*
is fail-safe, whereas a wrongly-set bit silently drops writes.

**P7 reopens this.** The table is closed only against a Dragonfly master. KeyDB's
`RREPLAY` rewrite set is close to Redis's but unverified against Dragonfly's — audit
it against a live `eqalpha/keydb` before enabling the guard on redis-protocol links.

#### The split happens inside the two multi-key handlers, not in the executor

`OpMSet` (`string_family.cc:405-435`) and `DeleteKeys` (`generic_family.cc:1265-1295`)
already loop per key and already build a narrowed journal arg list for partial
application. Adding a per-key skip there yields the locked semantics while keeping
**one transaction and one journal entry**; an executor-level rewrite of `MSET` into
N x `SET` would fan a 1000-key single-shard `MSET` into 1000 scheduled transactions,
lose same-shard atomicity, and *still* need the in-transaction compare anyway.

- `MSET k1 v1 k2 v2` with `k2` stale -> one `MSET k1 v1`, journaled as one entry.
  The existing prefix-resize (`stored*2`) must become a `push_back` of survivors,
  because a skipped *middle* key breaks the prefix assumption.
- `DEL k1 k2 k3` with `k2` stale -> one `DEL k1 k3`. The skip goes *before*
  `FindMutable` so a stale key neither deletes nor lands in `journal_args`.
- All keys stale -> callback runs, writes nothing, `journal::ClearBuffer()`, returns
  `OpStatus::OK`.

**Key enumeration.** Post-narrowing, every command the generic veto sees is
single-key, and `Transaction::GetShardArgs(sid)` already yields exactly that shard's
key args. `DetermineKeys` is used only as a defensive DCHECK in the classifier's
unit test, never on the hot path. **Trap:** `GetShardArgs` on an interleaved command
(`MSET`) yields keys *and* values in one contiguous range, not stepped — which is
exactly why `MSET` must be self-guarded and must never reach the generic veto.

**A dropped entry counts as applied.** `ExecuteTx` returns one bool per journal
entry; a fully-dropped entry returns `true`, `journal_rec_executed_` advances, and
`apply_failed_` stays `false` so `AdoptAuthoritativeLsn` (`replica.cc:1338`) keeps
working. This is correct — the entry was consumed and its effect is final — and
getting it wrong the other way would make every conflicting write force a full
resync.

**EXEC and the squasher are unreachable from the apply path** (the journal contains
no MULTI/EXEC entries; `JournalExecutor::Execute` dispatches one command at a time
with no `conn()`), but both were patched in P3 to carry origin. Making the guard
flag a parameter of the existing `SetReplOrigin` lets it ride those two sites for
free with zero new call sites. Note also that **multi-shard atomicity is already not
preserved by replication** — a cross-shard `MSET` journals two per-shard entries
applied by two independent flows with no barrier. Nothing here makes that worse, but
it belongs in `docs/multi-master.md` so the per-key semantics do not read as a new
regression.

**Cost of this placement:** the guard lives in three places (`RunCallback`,
`RunSquashedMultiCb`, and the two Op functions) rather than one, so a future
multi-key state-carrying journaled command would be silently unguarded. Mitigated by
(a) the generic veto DCHECKing `keys.Size() == 1` and, in release, logging and
refusing to guard rather than guessing, and (b) a vocabulary-closure test that pins
the normalization against upstream drift.

Metric: `multimaster_lww_dropped` on `ServerState::Stats` — note this will trip the
`static_assert(sizeof(Stats) == 30 * 8)` at `server_state.cc:67`, which is the
desired forcing function. `VLOG(2)` per drop plus a `LOG_EVERY_T(INFO, 60)` rollup;
a conflicting workload drops thousands per second, so per-drop `LOG(INFO)` would be
a self-inflicted outage.

### D-10. Bounded tombstones

#### Representation: bit 63 of `mvcc`, masked before comparison

**Amendment to D1, required for it to hold.** Reserve bit 63 of the packed `mvcc`
word as the tombstone marker. `ms << 20` with today's `ms ~ 1.77e12 ~ 2^40.7`
occupies up to bit 60; bit 61 is not reached until ~2079, so bits 62-63 are free
indefinitely. Comparison masks the bit off, so D1's "lexicographic on
`{mvcc, origin_hash}`" is preserved **exactly**, and the GC deadline is free
(`ms_part + ttl`) with no third field and no size growth.

A tombstone and a value with equal `{mvcc, origin_hash}` cannot arise across nodes:
distinct authors have distinct `origin_hash`, and one author's counter is strictly
increasing.

Rejected: a separate `uint8_t flags` field (17 B packed pushes the bucket 504 -> 516
and forces unaligned `uint64_t` loads); stealing a bit from `origin_hash`
(perturbs the tiebreak); stealing a counter bit (perturbs ordering).

#### Not every delete gets a tombstone

`DbSlice::Del` gains a trailing `DeleteReason`; only **five** of its ~35 call sites
need a non-default value, and all five are inside `db_slice.cc`:

| Site | Reason | Action |
|---|---|---|
| `db_slice.cc:249` `PrimeEvictionPolicy::Evict` | `kEvicted` | **erase, no tombstone** |
| `db_slice.cc:1725` `FreeMemWithEvictionStepAtomic` | `kEvicted` | **erase, no tombstone** |
| `db_slice.cc:1497` `ExpireIfNeeded` | `kExpired` | tombstone |
| `db_slice.cc:962`, `:1035` slot flush | `kSlotFlush` | erase |
| everything else (DEL/UNLINK/RENAME src/GETDEL/RESTORE REPLACE/`DeleteIfEmpty`/...) | `kExplicit` | tombstone |

**Eviction is not deletion.** It is a local capacity decision; writing a tombstone
while trying to free memory is self-defeating, and resurrection from a peer is
*desirable* because the peer's copy is authoritative. Note eviction already journals
an **expiry-flagged** DEL (`db_slice.cc:246-248`), which `PassesPeerEchoFilter`
suppresses to peers anyway.

**Expiry does get a tombstone**, stamped at expiry time — so a peer whose copy has
not yet expired and which then legitimately writes the key *later* still wins.

#### GC, bounds, and graceful degradation

| Flag | Default | Meaning |
|---|---|---|
| `--multi_master_tombstone_ttl` | 600 s | Retention window; must exceed the worst expected peer partition. `0` disables tombstones. |
| `--multi_master_max_tombstones` | 1,000,000 / shard | Hard cap. |
| `--multi_master_tombstone_gc_budget` | 64 buckets | Work per GC step. |

*Primary GC:* a proactor idle task registered from `DbSlice`'s constructor (the same
`AddOnIdleTask` mechanism `engine_shard.cc:830-843` already uses, but registered
from `db_slice.cc` so **`engine_shard.cc` stays untouched**), advancing a cursor by
the budget and erasing expired tombstones.

*Backstop — the part that actually matters:* an idle task never runs on a saturated
server, which is exactly when tombstones accumulate. So `PerformDeletionAtomic`
enforces the cap **inline and in O(1)**: at the cap, erase the entry instead of
writing a tombstone and increment `mvcc_tombstones_dropped`. That degrades to
today's KeyDB-parity resurrection behaviour rather than to an OOM, and the counter
makes the degradation visible.

At the default cap the worst case is ~41 MB per shard, and because tombstone bytes
count against `maxmemory` (D-4) while eviction cannot free them, the cap is what
prevents a structure eviction cannot touch from driving eviction pressure.

**Open decision for the owner:** three lines inside
`EngineShard::RetireExpiredAndEvict` (`engine_shard.cc:861`) would be strictly more
robust — that function already runs on a timer, already holds
`journal::DisableFlushGuard`, and already has budget-scaling machinery. The only
cost is one small hunk in a reserved file. Recommendation: ship the idle-task + cap
design, and promote to the heartbeat if the benchmark shows tombstone lag under load.

#### FLUSHALL

`DbSlice::FlushDbIndexes` swaps the whole `DbTable` out, so FLUSHALL/FLUSHDB drop
every tombstone for free. Document in `docs/multi-master.md`: **FLUSHALL destroys
all tombstones mesh-wide, so the delete-resurrection window reopens immediately
after a flush** — consistent with the existing risk #8 hazard note.

#### `--cache_mode` interaction

`--active_replica` with `--cache_mode` deserves a boot warning: eviction cannot free
tombstones, and evicted keys deliberately get none, so resurrection semantics differ
from the non-cache case.

### D-11. Derived-DEL fix (the P3 carry-forward blocker)

PLAN.md describes this as "a 'why is this empty' signal" threaded through the collection
cleanup sites. There are **28** current call sites funneled through two helper implementations,
`HSetFamily::DeleteIfEmpty` and `SetFamily::DeleteSetIfEmpty`, plus the generic hash wrapper's
distinct `DeleteHw` path. All three record derived DELs by default after this phase;
clock-dependent conditional callers retain explicit forwarded-DEL carve-outs.

The adversarial call-site inventory disproved the original two-leg argument. No
default-suppressed helper or wrapper call is backed by a source-effect command that a peer can
replay: the callers are read-only, or (SORT) journal only a destination effect and
therefore use the explicit non-derived carve-out. Peer-applied commands do not
repair a source mutation that was never sent.

Therefore peer suppression is safe only because the proactive member-expiry reaper
is a **correctness mechanism**: every active node independently walks TTL-bearing
containers and derives the same local deletion on its own clock. Its active-mode
budget cannot be disabled (`0` is an effective `1`), and the heartbeat visits every
local namespace and DB with independent incremental round-robin cursors. The container
budget advances home slots/buckets; collision chains and extension vectors remain whole
work units, so it is not a hard latency bound under adversarial collisions. Non-default
namespace cleanup stays local because the replication wire has no namespace identity.
Consequently D-11 and the proactive reaper are one merge unit: derived-DEL suppression
must not land on an active replica without the reaper already integrated.

SORT partial expiry is compensated explicitly with `SREM` before the verbatim SORT
entry; a failed numeric parse completes the source walk and journals only members actually
expired, while full expiry retains the forwarded DEL carve-out. Focused regressions pin both
fetch implementations, the error path, `DeleteHw`, and the reaper's direct non-preempting
deletion helper.

This must precede tombstones: an incorrectly forwarded DEL would otherwise create a
*persistent* tombstone on a peer that still holds live data.

### D-12. Clock-skew observability

`master_clock_ms` has been exchanged in the handshake since P1 and read nowhere.
Compare it against local time, `LOG(WARNING)` past a threshold, and expose
`clock_skew_ms`. Small, and it makes the phase's central assumption observable
instead of assumed — including the fabricated-future-timestamp consequence of D3.

### D-13. Observability

#### `DEBUG MVCC <key>`, not `DFLY MVCC <key>` — a correction to the plan

`DFLY` is registered `CO::ADMIN | CO::GLOBAL_TRANS | CO::HIDDEN`
(`server_family.cc:4308`). **`CO::GLOBAL_TRANS` is decisive**: a per-key stamp read
must not take a global transaction across every shard — it would serialise against
all traffic and perturb the very thing being inspected, and on a mesh under load you
would be blocking the writes whose stamps you are reading.

`DEBUG` is `CO::ADMIN | CO::LOADING` (`server_family.cc:4282`), so the admin/ACL
posture is identical, it costs a single shard hop, **it works during LOADING** —
exactly when you most want to inspect stamps during a merge sync — it has the
per-key precedent (`DEBUG OBJECT`, `debugcmd.cc:1202-1261`), and it lives in
`debugcmd.cc` rather than in replication-critical `dflycmd.cc`.

Output is a flat RESP simple string in `DEBUG OBJECT` style, carrying all three
fields D1 requires plus the decomposed clock, because the first question when
debugging a divergence is "whose clock was ahead":

```
> DEBUG MVCC user:42
"state:value mvcc:1858419230638080 ms:1772345678901 counter:0 origin:9f3c1a02b77d4e51 shard:3"
> DEBUG MVCC deleted:key
"state:tombstone mvcc:1858419241123840 ms:... origin:... gc_at_ms:1772346278911 shard:1"
> DEBUG MVCC never:existed
"state:absent shard:5"
```

Plus `DEBUG MVCC` (no key) for per-shard aggregates, and `DEBUG MVCC VERIFY` to run
the full-scan invariant (D-4).

#### INFO fields

`INFO memory`: `mvcc_table_bytes` (side table structural bytes plus each duplicated
key's heap remainder over `CompactObj::kInlineLen`, P4-2 Task 4), `mvcc_entries`,
`mvcc_tombstones`.
`INFO replication` (inside the existing active block): `mvcc_clock_ahead_ms`,
`mvcc_unstamped_writes`, `mvcc_tombstones_dropped`, `mvcc_stale_epoch`,
`clock_skew_ms`, `multimaster_lww_dropped`.

Two matter operationally above the rest: **`mvcc_unstamped_writes`** tells you a
read-mutation path is over-arming, and **`mvcc_clock_ahead_ms`** tells you NTP has
stepped backwards and this node is about to win every conflict.

### D-14. Tests

Falsification (D9) applies to every one. The standing lesson from P3: **a
convergence assertion cannot detect an echo storm** — with the origin filter
removed, two nodes still converge, because symmetric amplification makes both
replay the same operations and `@assert_eventually` finds a coinciding moment. The
load-bearing detector is `assert_no_command_storm`
(`tests/dragonfly/multimaster_test.py:1239-1257`), whose bound is topology-dependent
(`3 × (shard_flows × peers × 1/s REPLCONF ACK) + 1 self-INFO`; measured 4 for two
nodes, 7 for three) and must be recalibrated if mesh shape changes.

**Apply the same skepticism here.** A test asserting "A and B agree" is *not* an LWW
test — LWW tests must assert **which value survived**, and the losing write must be
one the node actually accepted from its own client. For each test, state what it
would still pass under if the feature were removed.

Required pytest coverage (`tests/dragonfly/multimaster_test.py`, reusing
`active_args`/`attach`/`wait_for_peers`/`SeederV2`):

| Test | Falsify by |
|---|---|
| Concurrent conflicting SETs on A and B converge to the **higher-stamp value on both** (KeyDB "MVCC Updates Correctly" parity, incl. its 2 ms slop) | Removing the streaming guard |
| **Exact-tie regression**: force identical `mvcc` on two nodes; the `origin_hash` tiebreak must make both pick the same winner | Comparing on `mvcc` alone — this is the test that justifies D1's extra 8 bytes/key; without it D1 is unfalsified |
| **Backward-skew regression** (D3): a node with a retarded clock writes a key whose stored stamp came from a peer; the write must still reach the peer | Reverting `max(tick, stored+1)` to a bare tick |
| Merge-on-full-sync: node with newer local writes full-syncs from a peer holding older values → newer survive; plus the reconnect-merge variant | Reverting the `rdb_load.cc:3258` compare |
| **Per-key split** (D5): `MSET k1 v1 k2 v2` with only `k2` stale → `k1` applies, `k2` does not | Reverting to all-or-nothing *and* to always-apply — both must produce named, distinct failures |
| **Tombstones** (D6): delete on A, restart A, A full-syncs from B which still holds the key → key stays deleted | Making tombstones memory-only |
| **Stamp identity**: a replicated key's stamp on B equals A's origin stamp exactly | Applying `max()` on the replica path instead of taking the wire value verbatim |

C++ coverage: `MvccClock` monotonicity and overflow; `MvccStamp` comparison
totality; side-table mirror invariant across every touch-point in D-4;
`RDB_OPCODE_DF_MVCC` round-trip incl. the absent-opcode and KeyDB-form paths
(`rdb_test.cc`); classifier
membership. The P3 golden-buffer journal test is extended, not replaced, to prove
`--active_replica` off still produces byte-identical journal output.

## PR stack

Six stacked PRs on `feat/phase4-mvcc-lww`. The ordering deliberately separates the
three changes that rewrite deletion semantics — the per-key split, tombstones, and
the derived-DEL fix — so they never land together.

| PR | Scope |
|---|---|
| **P4-0** | Proactive member-expiry reaper + D-11 derived-DEL fix + D-12 clock-skew warning/metric. Mergeable independently of P4-1...P4-5 only as this complete unit; D-11 must not land without its reaper prerequisite. |
| **P4-1** | **T0 inbound plumbing** (`TransactionData.mvcc`, `SetApplyMvcc`, `peer_origin_hash_`, widened `SetReplOrigin`), D-2 stamp, D-3 clock, D-4 side table, D-5 stamping, D-6 wire, D-13 `DEBUG MVCC` + `mvcc_table_bytes`, memory benchmark. Starts the active-node journal at boot so peerless writes are stamped and retains non-default ACL namespaces as local-only because the wire has no namespace identity; no conflict-resolution behavior changes yet. |
| **P4-2** | D-7 RDB persistence (`RDB_OPCODE_DF_MVCC` save + load, KeyDB read branch). |
| **P4-3** | D-8 merge-on-full-sync LWW. First real behaviour change. |
| **P4-4** | D-9 streaming guard + per-key split, `--multi_master_stream_lww`, `multimaster_lww_dropped`. Highest risk; lands on a foundation already proven by P4-1..3. |
| **P4-5** | D-10 tombstones. Last: touches both compare paths and the RDB format the earlier PRs established. |

## File map

| Path | Change | PR |
|---|---|---|
| `src/server/mvcc.{h,cc}` **(new)** | `MvccClock`, `MvccStamp`, `MvccStamper` (arm/commit, hop memo, stats) | P4-1, P4-5 |
| `src/server/mvcc_test.cc` **(new)** | Pure units: clock monotonicity/overflow/step-back, stamp ordering + tombstone masking, arm/commit | P4-1, P4-5 |
| `src/server/multimaster_lww.{h,cc}` **(new)** | `ClassifyJournaledCommand`, `LwwShouldDropKey`, `--multi_master_stream_lww` | P4-4 |
| `src/server/multi_master.{h,cc}` | `PeerRegistry` += `idx_to_hash_` / `GetUuidHash`; `--multi_master_tombstone_ttl`, `--multi_master_max_tombstones`, `--multi_master_tombstone_gc_budget` | P4-1, P4-5 |
| `src/server/table.{h,cc}` | `DbTable` += mvcc side DashTable; `Clear()` mirrors it | P4-1 |
| `src/server/db_slice.{h,cc}` | `SetMvcc`/`GetMvcc`/`DelMvcc`; stamping in `PostUpdate`; erase in `PerformDeletionAtomic`; every mirror site in D-4 | P4-1, P4-5 |
| `src/server/tx_base.{h,cc}` | `DbContext` += `repl_mvcc` | P4-1 |
| `src/server/transaction.{h,cc}` | Allocate the shard-transaction stamp; populate `EntryBase::mvcc` | P4-1 |
| `src/server/journal/executor.{h,cc}` | Apply-context mvcc; streaming guard pre-check and per-key split | P4-1, P4-4 |
| `src/server/rdb_save.cc` | `RDB_OPCODE_DF_MVCC` write; tombstone section + save-time GC | P4-2, P4-5 |
| `src/server/rdb_load.cc` | `RDB_OPCODE_DF_MVCC` read (**unconditional**); KeyDB `mvcc-tstamp` branch; merge-LWW hook at `:3258`; tombstone section | P4-2, P4-3, P4-5 |
| `src/server/rdb_extensions.h` | `RDB_OPCODE_DF_MVCC = 221` constant | P4-2 |
| `src/server/hset_family.cc`, `set_family.cc` | Derived-DEL suppression in the two helpers | P4-0 |
| `src/server/replica.cc`, `server_family.cc` | Clock-skew comparison, warning, metric | P4-0 |
| `src/server/debugcmd.cc` | `DEBUG MVCC <key>`, `DEBUG MVCC`, `DEBUG MVCC VERIFY` | P4-1 |
| `src/server/multi_master_test.cc`, `rdb_test.cc`, `journal/journal_test.cc` | C++ coverage | all |
| `tests/dragonfly/multimaster_test.py` | pytest coverage per D-14 | all |
| `docs/PLAN.md`, `docs/UPSTREAM-SYNC.md` | Status, P4/P5/P6 rewrite, watchlist additions | final |

**Newly exposed to upstream churn.** `db_slice.{h,cc}`, `table.{h,cc}`,
`rdb_save.cc`, `rdb_load.cc`, `rdb_extensions.h`, and — if a mask bit is taken —
`core/compact_object.h`. `db_slice.cc` is a **high-churn upstream file** and this is
the first phase to touch it; `docs/UPSTREAM-SYNC.md` must gain all of them.

## Global constraints

1. **`--active_replica` off must remain byte-identical to upstream** on the journal
   wire and, as far as the mvcc feature is concerned, in the RDB. Proved by
   extending P3's golden-buffer test, not by inspection.
2. **The RDB loader's `RDB_OPCODE_DF_MVCC` handling is unconditional** — see D-7.
3. `CompactObj` must not grow. Taking one of the two `unused` mask bits is
   permitted if measurement justifies it, but changes no size.
4. Fiber-safe primitives only (`util::fb2`); the side table and the tombstone GC
   both add new shared per-shard state.
5. Additive, flag-gated changes; keep the upstream merge surface minimal.

## Verification

**Environment.** OrbStack (`orbctl start` — the daemon is usually down); container
`drakeydb-p2` from `drakeydb-build:deps-p2`, worktree mounted at `/src`,
**`--security-opt seccomp=unconfined`** or io_uring aborts and every
`BaseFamilyTest` fixture dies. Build with
`./helio/blaze.sh -DWITH_AWS=OFF -DWITH_GCP=OFF && cd build-dbg && ninja -j4`
(`-j4` — 12-way OOM-kills `cc1plus` in the 8 GB VM). Run a **complete `ninja` before
`ctest -L DFLY`** or unbuilt binaries report as "Not Run". `DRAGONFLY_PATH` **must
be absolute**. Git works host-side only (the worktree's `.git` points outside the
mount). Host pre-commit: `~/.venvs/precommit`.

**Per-PR gate**, for each of P4-0 … P4-5:

1. Full `ninja`, warning-free; journal golden-buffer test green.
2. `ctest -L DFLY` — 87/87 today; new C++ tests raise the count.
3. pytest: `multimaster_test.py`, `replication_test.py`,
   `replication_specific_test.py`, `replication_resilience_test.py`.
   Timing-adjacent tests run 10–15× to establish a pass rate, not a single green run.
4. `clang++ -Wthread-safety` over the concurrency-relevant TUs. `CMakeLists.txt:77-79`
   gates this on Clang and the container builds with g++, so it never runs in the
   ordinary local gate — and this phase adds two pieces of new shared state.
5. `pre-commit run --files <changed>`.

**Memory benchmark** (P4-1 deliverable) — `tests/dragonfly/multimaster_memory_test.py`,
two instances identical but for `--active_replica`, no peers attached (this measures
storage cost, not replication), `DEBUG POPULATE 1000000 key 100`.

**Compare `used_memory`, never `table_used_memory`** — `DbTable::table_memory()`
returns `prime.mem_usage()` only, so the side table is invisible to it and a
benchmark reading it reports a ~0 delta and is wrong (D-4).

Assertions, so the benchmark is a regression guard and not just a number:
1. `mvcc_table_bytes / num_entries` within `[34, 48]` B/key — the geometric bound
   from D-4 across the plausible load-factor range. Fails loudly if anyone changes
   `MvccStamp`'s size or alignment.
2. `used_memory(on) − used_memory(off) ≈ mvcc_table_bytes(on)` within 15% — proves
   the metric actually accounts for the delta.
3. Sweep key lengths 8 / 16 / 24 / 32 to expose the `kInlineLen = 16` cliff, and emit
   the table into the report. **The expected result is ~41 MB for short keys and
   ~73-79 MB for 24-32 byte keys per 1M keys** — i.e. +26% to +45% RSS. If the number
   lands outside that band, the geometry assumption is wrong and the design needs
   revisiting before P4-2.
4. A second pass populating then deleting half the keyspace, asserting
   `mvcc_tombstones -> 0` within `2 × --multi_master_tombstone_ttl`.

## Risks / watch items

| # | Risk | Mitigation |
|---|---|---|
| 0 | **The `OnCbFinishBlocking` epoch-ordering landmine** (D-5). It runs *before* `LogAutoJournalOnShard`, so the obvious epoch placement drops the stamp on every auto-journaled write — silently, with every convergence test still green. | Called out in D-5 with the verified line numbers, given a dedicated failing gtest (`AutoJournaledCommandStampsKey`), and made the first item in P4-1's task brief. `mvcc_unstamped_writes` catches it in production. |
| 1 | **Per-key split vs. Dragonfly's transaction model** (D5). Rewriting a journaled command into a partial form may not compose with EXEC, the squasher, or peer-mode LSN accounting. | Isolated in P4-4, landing on a foundation already proven by P4-1..3. Explicit design-agent question. If it proves structurally unworkable, the fallback is single-key-only, which is a documented scope reduction, not a redesign. |
| 2 | **Missed side-table touch-point** → stale stamps, wrong LWW verdicts, and a slow leak. | Dense debug invariant (mvcc size == prime size + tombstones); exhaustive mirror list in D-4; `DEBUG MVCC` for inspection. |
| 3 | **Memory.** 16 B/key of payload plus a duplicated key and dash overhead, on a dense table. | Measured, not estimated; `mvcc_table_bytes` metric; active-mode-only allocation. |
| 4 | **RDB format compatibility.** A non-active build misparsing an active-mode snapshot is silent corruption. | The loader learns the flag unconditionally (D-7) — stated as a global constraint, with a round-trip test in both modes. |
| 5 | **Tombstone growth** under delete-heavy load. | TTL-bounded, GC'd on expiry and at save time; growth is observable via `mvcc_table_bytes`. |
| 6 | **Upstream merge surface widens** into `db_slice.cc`, a high-churn file. | Changes kept additive and flag-gated; `docs/UPSTREAM-SYNC.md` watchlist updated; the multimaster pytest suite remains the merge gate. |
| 7 | **Vacuously-passing convergence tests** — the P3 failure mode, seven times over. | D9 falsification on every test; D8's adversarial reviewer; the "which value survived" rule in D-14. |
| 8 | **`SETNX` and `RESTORE` diverge silently today**, independent of this phase. `SETNX` journals verbatim and conditional-on-existence; `RESTORE` without `REPLACE` fails with a *reply-level* error that `DispatchCommand` reports as `OK` — no apply failure, no `apply_failed_`, no metric. | Normalized in D-9 (`SETNX`->`SET`, inject `REPLACE`), gated on the guard flag so `false` stays KeyDB-parity. Worth a standalone regression test since these are pre-existing. |
| 9 | **A plain sub-replica of an active node stamps foreign-origin keys with the link's origin**, since it applies a multi-origin stream through a single link. | Harmless today (no-forward means such a node is never a mesh member and its writes never reach peers), but it means **a plain replica cannot be promoted into a mesh without a full resync**. Document in `docs/multi-master.md`; the real fix is consuming `Op::ORIGIN`, already on the wire and currently only `VLOG`'d (`tx_executor.cc:76-80`). |
| 10 | **D3 fabricates future timestamps** under skew, so a skewed node's writes beat correctly-clocked peers for the skew window. | Inherent to the repair and bounded by the skew — strictly better than the write vanishing. Made visible by D-12's metric. |
