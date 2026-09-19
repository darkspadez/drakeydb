# Multi-master (drakeydb fork): merge, tombstones, and operator guidance

This page covers `--active_replica` / `--multi_master` behavior added by drakeydb's Phase 4:
merge-on-full-sync last-write-wins (LWW), delete tombstones, and the operational hazards that
come with both. It assumes you already know [`docs/differences.md`](differences.md)'s
active-replica RDB section (opcode 221, the stock-Dragonfly cliff) — this page builds on it.

## What merge-on-full-sync LWW does

An active-replica node (`--active_replica`) stamps every locally written or peer-merged key with
an MVCC stamp `{clock, origin_hash}` (`src/server/mvcc.h`). When such a node receives a **full
sync** from a peer — another drakeydb active node, a classic Redis/KeyDB master, or its own
restart-from-RDB — every incoming key is compared against whatever this node currently has stored
for that key, and only overwrites it if the incoming stamp is genuinely newer.

**The comparison rule, in one sentence: ties are won by the stored side.** An incoming write is
installed only when it is *strictly* newer than what is already there (`MergeAccepts`,
`src/server/mvcc.h`); equal stamps never churn. This is a deliberate owner decision (2026-08-30),
not an oversight — it is what makes a retried or duplicated apply idempotent.

**What it does not do:** merge-LWW only ever compares against *this node's own resident stamp*
for a key. It has no notion of a global "true" value, no quorum, and no read-repair outside a
full sync. Two nodes can each accept different values for the same key from different partitions
without either node knowing the other one exists; the next full sync between them is what
resolves it (again by the tie rule above).

### Stamped peers vs. unstamped peers

Not every peer can tell you a per-key write time:

- **A drakeydb active peer speaking the DFLY multi-shard protocol** carries a real, per-key
  `{mvcc, origin_hash}` stamp on the wire (RDB opcode 221, `RDB_OPCODE_DF_MVCC`). Merge-LWW
  compares that stamp directly against the stored one. This is the precise, intended case.
- **A classic-protocol peer** — a plain Redis master, a KeyDB master without its own
  `mvcc-tstamp` aux, or any other RDB source with no per-key stamp — has no such information.
  For these links, an unstamped key is assigned an approximate authority derived from the
  snapshot's own `ctime` (its whole-file save timestamp, 1-second granularity), clamped so it can
  never claim to be newer than this node's own clock: `stamp = min(ctime_ms + 999, now_ms)`. That
  key then runs through the exact same tie-break compare as a stamped one.

**Say this plainly: a classic-protocol peer's unstamped keys can resurrect a tombstone.** The
`+999` and `min(..., now)` clamps only narrow the exposure, they do not close it. A classic link
carries one snapshot-wide timestamp, not a per-key write time, so there is no way to distinguish
"the peer legitimately rewrote this key after our delete" from "this is just the peer's stale,
pre-delete copy". In practice: if you write a key on a plain Redis master, delete it locally, and
later full-sync from that master, the deleted key comes back — because from that master's point
of view it was never deleted, and the sync has no finer time signal to argue otherwise. This is
tracked in [`docs/ISSUE-REGISTER.md`](ISSUE-REGISTER.md) (D-12); it is inherent to using a
classic RDB stream as a merge source, not a bug to be fixed by more tuning. A DFLY-to-DFLY link
between two drakeydb active nodes does not have this exposure — every key it carries has a real
per-key stamp.

## Tombstones

Before this phase, every delete simply erased its slot; a peer's stale full sync could then
"resurrect" the deleted key with no way to tell that a delete had ever happened. drakeydb now
keeps a **tombstone** — the deleted key's MVCC slot, with its top bit set, kept instead of erased
— for `kExplicit` (an ordinary `DEL`/`UNLINK`/etc.) and `kExpired` (lazy or active TTL expiry)
deletes. A peer's incoming write for that key is then just another candidate in the same
`MergeAccepts` compare: it only overwrites the tombstone if it is newer.

**Eviction is not deletion.** A key removed by `kEvicted` (memory-pressure eviction) or
`kSlotFlush` (cluster slot migration/removal) gets **no tombstone** — it is simply erased, exactly
as before this phase. This is intentional: eviction is a local capacity decision, not a durable
fact about the dataset, and the peer's copy is treated as authoritative for that key. See the
`--cache_mode` section below for the operational consequence.

### The three tombstone flags

| Flag | Default | Effect |
|---|---|---|
| `--multi_master_tombstone_ttl` | `600` (seconds) | How long a tombstone is retained before the idle-task GC (`DbSlice::TombstoneGcStep`) reclaims it. **`0` disables tombstoning entirely** — every delete erases immediately, identical to pre-Phase-4 behavior, and the GC step no-ops. |
| `--multi_master_max_tombstones` | `1000000` | Per-shard cap on live tombstones. A delete that would push a shard over this cap **degrades to a plain erase instead of arming a tombstone** — see "Hitting the cap" below. |
| `--multi_master_tombstone_gc_budget` | `64` | Side-table buckets the idle-task GC visits per tick. Must be `>= 1` (validated at boot); bounds one GC tick's total work across however many databases still need visiting. |

**Sizing the TTL against expected partition length.** A tombstone protects a delete only for as
long as it is retained: if a peer is partitioned (network split, long maintenance window, extended
`REPLICAOF` gap) for longer than `--multi_master_tombstone_ttl`, and a key on this node was
deleted during that gap, the tombstone may already be gone by the time the peer reconnects and
full-syncs — and the peer's stale copy resurrects the key exactly as if tombstoning were off. Set
the TTL comfortably longer than the longest partition you expect to recover from automatically;
600s (10 minutes) is a starting point for a mesh on a stable LAN, not a universal answer. There is
no cost to setting it much higher other than the tombstone's own memory (one MVCC side-table slot,
independent of the tombstoned key's own now-freed prime-table slot) and the risk of hitting
`--multi_master_max_tombstones` sooner on a delete-heavy shard.

**Hitting the cap degrades to resurrection.** When a shard's tombstone count is already at
`--multi_master_max_tombstones`, the next delete on that shard does not error and does not block —
it silently falls back to a plain erase, trading resurrection risk for bounded memory. This is
**visible, not silent**: it increments `mvcc_tombstones_dropped`, reported both in `INFO memory`
(beside `mvcc_tombstones`) and in `DEBUG MVCC`'s per-shard aggregate
(`shard<N>_tombstones_dropped:`). A non-zero, growing `mvcc_tombstones_dropped` on a shard means
that shard's delete-resurrection protection is currently degraded and either the cap needs
raising or the delete rate needs investigating.

### `FLUSHALL`/`FLUSHDB` destroys every tombstone, mesh-wide

`FLUSHALL`/`FLUSHDB` does not walk the keyspace deleting one key at a time — `DbSlice::
FlushDbIndexes` swaps the entire `DbTable` (prime table **and** its MVCC side table, tombstones
included) for a fresh, empty one. Every tombstone that table held is gone instantly, along with
every live key's stamp. **This reopens the delete-resurrection window immediately after a flush**:
until fresh writes and their stamps propagate around the mesh, a peer that was merely partitioned
(not flushed) and later reconnects has no tombstones to be rejected by, on either side. There is
no flag to protect against this today — treat a `FLUSHALL`/`FLUSHDB` on an active-replica mesh as
an event that needs the same care as bringing up a brand new node: expect a window where deletes
made shortly before or during the flush can resurface from any peer that missed them.

### `--cache_mode` interaction

`--cache_mode` evicts keys under memory pressure. Eviction is `kEvicted` (see above): it
deliberately writes no tombstone, and — because there is no tombstone to reclaim — the tombstone
GC cannot free any memory on an eviction-heavy, cache-mode workload; tombstone memory pressure
there can only come from actual deletes/expiries. The flip side: an evicted key is fair game for
resurrection by any peer's next full sync, which is the intended behavior for a cache (the peer's
copy is treated as authoritative), not a bug. If you need a delete to be durably remembered even
under `--cache_mode`, issue an explicit `DEL`/`UNLINK` — that path still tombstones normally and
is unaffected by eviction pressure. Boot-time validation logs a `WARNING` when both
`--cache_mode` and tombstoning (`--multi_master_tombstone_ttl != 0`) are enabled together, as a
reminder of this split, not because the combination is unsupported.

## Observability

- `INFO memory`: `mvcc_table_bytes`, `mvcc_entries`, `mvcc_tombstones`, `mvcc_tombstones_dropped`
  (all gated on `--active_replica`; absent, not zero, on a non-active node).
- `DEBUG MVCC <key>`: prints `state:value|tombstone|absent`, and for `value`/`tombstone` the raw
  `mvcc:`, `ms:`, `counter:` (value only), and `origin:` fields for that key on its own shard.
- `DEBUG MVCC` (no key): per-shard aggregate — `shard<N>_entries`, `shard<N>_tombstones`,
  `shard<N>_tombstones_dropped`, `shard<N>_bytes`, `shard<N>_clock_last`,
  `shard<N>_clock_ahead_ms`, `shard<N>_unstamped_writes`.
- `DEBUG MVCC VERIFY`: from-scratch dense-invariant check across every shard/db; reports
  `mismatches:<N>`.

All three `DEBUG MVCC` forms require `--active_replica` and are local-only (default namespace
only — the journal wire has no namespace identity to carry a non-default one's state).

## Compatibility: the RDB one-way doors

Two RDB opcodes are drakeydb-specific and hard-fail an older loader:

- **`RDB_OPCODE_DF_MVCC` (221)**, added in P4-2: per-key `{mvcc, origin_hash}`. A stock
  (upstream) Dragonfly or a pre-P4-2 drakeydb cannot parse it — see
  [`docs/differences.md`](differences.md) for the exact failure text.
- **`RDB_OPCODE_DF_TOMBSTONES` (225)**, added in this phase (P4-3): a per-shard tombstone section.
  A stock Dragonfly or a P4-2-era (pre-P4-3) drakeydb cannot parse this one either, for the same
  reason.

Both opcodes are written **only** by an active-replica node's save path, and the read side parses
them unconditionally (any drakeydb binary, active or not, consumes and — if not active — discards
them). `kDrakeydbReplVersion` (currently `67`) is the fork's own replication protocol version,
exchanged via `REPLCONF DRAKEY-VERSION` before a single RDB byte is sent; an active node refuses
to admit a consumer advertising a version older than its own, so a pre-P4-3 drakeydb peer is
refused *before* full sync rather than being admitted and hard-failing mid-stream on opcode 225.

**The remaining one-way door is a file handed by hand.** Live drakeydb-to-drakeydb replication is
protected by the version gate above; there is no way to receive an incompatible stream over the
wire. Copying an active node's RDB **file** onto an older binary's `--dir` (or loading it there
via `DEBUG RELOAD`) bypasses that gate entirely and hard-fails on the first opcode 221 or 225 byte
it cannot recognize. To move a snapshot backward deliberately, load it with a current drakeydb
under `--active_replica=false` and save it again — the non-active write side omits both opcodes,
producing an older-compatible file at the cost of discarding all stamps and tombstones.

**Upgrade a mesh in lockstep.** Because the version gate refuses admission rather than negotiating
a reduced feature set, an active mesh should be upgraded node by node with each new binary able to
admit and be admitted by the others before traffic depends on it — there is no mixed-version
"tombstones on some links, not others" steady state; a link either meets the minimum version and
gets the full feature set, or it is refused at handshake.

## Known residual exposures

Tracked in [`docs/ISSUE-REGISTER.md`](ISSUE-REGISTER.md), Part 2:

- **D-12** — the classic-protocol ctime-resurrection exposure described above (inherent, tested).
- **D-11** — a proposed tightening (`clamp(ctime - clock_skew_ms, psync_sent_local_ms, now)`) that
  would close the *peer-clock-behind* half of the exposure; not yet implemented.
- **D-13** — a same-shard `SORT ... STORE` still journals the sort recipe rather than its result;
  a `BY`/`GET` pattern key is not a transaction key, so a peer can legitimately compute a different
  destination value under an identical MVCC stamp, and the tie-favors-stored rule then never
  repairs the divergence.

See that document for the full list, upstream-bug cross-references, and each entry's owning phase.
