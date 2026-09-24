# Multi-master (drakeydb fork): merge, tombstones, and operator guidance

This page covers `--active_replica` / `--multi_master` behavior added by drakeydb's Phase 4:
merge-on-full-sync last-write-wins (LWW), delete tombstones, and the operational hazards that
come with both. It assumes you already know [`docs/differences.md`](differences.md)'s
active-replica RDB section (opcode 221, the stock-Dragonfly cliff) — this page builds on it.

## What merge-on-full-sync LWW does

An active-replica node (`--active_replica`) stamps every locally written or peer-merged key with
an MVCC stamp `{clock, origin_hash}` (`src/server/mvcc.h`). When such a node receives a **full
sync from a peer** — another drakeydb active node, or a classic Redis/KeyDB master — every
incoming key is compared against whatever this node currently has stored for that key, and only
overwrites it if the incoming stamp is genuinely newer.

**Merge-LWW is full-sync-only; streaming (stable-sync) writes get their own, sibling compare.**
`MergeAccepts` (`src/server/mvcc.h`) is the one comparison rule both use. The full-sync loader
(`rdb_load.cc`; `merge_lww_` is set only by `replica.cc`'s two peer-mode call sites) calls it
directly for a whole-key snapshot compare at full-sync time. Since P4-4, `multimaster_lww.h`'s
`LwwShouldDropKey` calls the same function for a per-write compare on the streaming (stable-sync)
path — see "Streaming LWW" below for that guard's own scope, gating, and vocabulary; the two
compares are deliberately the same rule (ties favor the stored side either way) so a value can
never look accepted on one path and rejected on the other. This node's own restart-from-RDB (a
local RDB file, `DEBUG LOAD`, `DEBUG RELOAD`, `RESTORE`) is still **not** a merge source — it loads
verbatim, exactly like pre-Phase-4 Dragonfly, overwriting whatever was resident with no comparison
at all (pinned by `RdbMvccTest.WithoutMergeLwwStaleSnapshotStillOverwrites`, `src/server/
rdb_test.cc`).

**The comparison rule, in one sentence: ties are won by the stored side.** An incoming write is
installed only when it is *strictly* newer than what is already there (`MergeAccepts`,
`src/server/mvcc.h`); equal stamps never churn. This is a deliberate owner decision (2026-08-30),
not an oversight — it is what makes a retried or duplicated apply idempotent.

**A local write always mints a stamp strictly above whatever it is about to overwrite.** A
locally-originated write's stamp is not simply a wall-clock tick (`MvccStamper::HopStamp`) — it is
`max(clock tick, max over the entry's keys of stored.mvcc + 1)` (`LocalMintFloor`, `mvcc.h`, wired
into `journal::RecordEntry`), so a local write that happens to overwrite a key already carrying a
higher stamp (e.g. merged in earlier from a peer with a fast clock) still mints something at least
one tick higher, so that overwrite is not silently doomed to lose the *next* full-sync merge
against a third node. It is computed per journal entry, not a per-shard ratchet: an unrelated key's
mint in the same epoch is unaffected. The floor is capped at the stamp's own bit-mask; at that cap
only the `mvcc` portion *ties* the stored stamp's — `origin_hash` still decides which one actually
wins the next compare (`MergeAccepts`' own tie-break), so a local write does not automatically lose
just because it hit the cap. The cap itself is reachable only with a corrupt, hostile, or (at this
encoding's millisecond granularity) roughly year-2248 stamp. An expiry's own tombstone mint follows
the identical rule, floored against the value it deletes.

**An already-expired incoming key is the peer's DELETE.** A full sync can ship a key whose
whole-key TTL has already elapsed — the normal state of an expiring key on a loaded server, whose
active-expire sweep runs behind. On a *merge* load such a key is not silently dropped: it is
applied as a delete for that key, carrying the incoming key's own stamp with the tombstone bit
set, through exactly the same `MergeAccepts` compare and tombstone-install path an opcode-225
tombstone record takes. If this node's own value for that key is newer, it wins and nothing
changes; if it is older, it is deleted and the peer's stamp is recorded as a tombstone. That
tombstone carries the peer's *write-time* stamp, so its GC deadline is `write time +
--multi_master_tombstone_ttl`: for a key whose TTL was longer than the tombstone TTL it is born
already reapable and gives this node almost no resurrection-protection window (the delete itself
still stands). Without
this, a peer's `SET k v2 PX 1000` issued during a partition would leave this node holding the
older `v1` forever — the peer's tombstone is not in the snapshot's prologue-emitted opcode-225
section (the key had not expired yet when that ran) and its expiry `DEL` never crosses a peer
link (see "An expiry's tombstone stamp is per-node" below). A **non-merge** load — a local RDB
file, `DEBUG LOAD`, a plain Dragonfly replica's full sync — keeps dropping an already-expired key
verbatim, exactly as upstream does.

**What it does not do:** merge-LWW only ever compares against *this node's own resident stamp*
for a key. It has no notion of a global "true" value, no quorum, and no read-repair outside a
full sync. Two nodes can each accept different values for the same key from different partitions
without either node knowing the other one exists; the next full sync between them is what
resolves it (again by the tie rule above).

### Stamped peers vs. unstamped peers

Not every peer can tell you a per-key write time:

- **A drakeydb active peer speaking the DFLY multi-shard protocol** carries a real, per-key
  `{mvcc, origin_hash}` stamp on the wire (RDB opcode 221, `RDB_OPCODE_DF_MVCC`) for every key it
  has ever stamped. Merge-LWW compares that stamp directly against the stored one. This is the
  precise, intended case. **If a DFLY-protocol key genuinely arrives unstamped** (`{0,0}` — should
  not happen for a key an active peer ever wrote, but can happen for data the peer itself only
  ever loaded verbatim, e.g. a plain sub-replica's own resident data), it keeps `{0,0}` and always
  loses: `MergeAccepts` never accepts an all-zero incoming stamp over anything already resident.
  There is no ctime-style fallback on this path — the rule below applies **only** to
  classic-protocol links, never to DFLY-protocol ones. (This distinction is deliberate: an earlier
  version of the merge rule applied a ctime-like override regardless of protocol, and review found
  it could clobber a DFLY peer's own resident dataset; narrowing it to
  `merge_classic_protocol_` only is what Task 13 changed it to.)
- **A classic-protocol peer** — a plain Redis master, a KeyDB master without its own
  `mvcc-tstamp` aux, or any other RDB source with no per-key stamp — has no per-key stamp to send
  at all. For these links **only** (`merge_lww_ && merge_classic_protocol_ && !item->has_mvcc`,
  `rdb_load.cc`), an unstamped key is assigned an approximate authority derived from the
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

## Streaming LWW (stable-sync compare)

**Stable-sync (steady-state) replicated writes are LWW-compared too, per peer link.** Once the
initial full sync completes, an active-replica node's *peer* links (never a plain replica — see
below) run every subsequently streamed guarded write through the same tie-favors-the-stored-side
rule merge uses, gated by `--multi_master_stream_lww` (default `true`). The compare
(`LwwShouldDropKey`, `src/server/multimaster_lww.h`) runs *inside* the transaction, under the
write's own key lock (`Transaction::ShouldDropForLww`, or the self-guarded per-key loop inside
`OpMSet`/`OpDelV2` for `MSET`/`DEL` — see below), never before dispatch, so it always sees the
value the lock is actually protecting. A dropped write writes nothing, journals nothing, and still
reports success rather than an error — a stream of drops from a stale peer link is a normal, if
wasteful, steady state, not a fault that should force a resync.

**Only a guarded peer link, carrying a real stamp, is ever compared:**
- A **plain replica** (a consumer that never sent `REPLCONF PEER`) is never guarded — the guard bit
  is `peer_mode_ && IsActiveReplica() && --multi_master_stream_lww`, read exactly once per link, at
  flow setup (`DflyShardReplica`'s constructor, `replica.cc`), never re-read per entry.
- An entry whose incoming `mvcc` is `0` is never guarded, on any link (`LwwGuardActive`,
  `multimaster_lww.h`): a classic Redis/KeyDB link never carries an `mvcc` at all, so guarding it
  would silently drop its entire stream.
- A full sync's own concurrent journal blob — writes that arrive *during* that same full sync,
  applied by the RDB loader's own second applier (`RdbLoaderBase::HandleJournalBlob`) — shares the
  identical guard bit with the flow's stable-sync executor (`ConnectionContext::repl_lww_guard`),
  so the two appliers of one link can never disagree about whether it is guarded.

**The guarded vocabulary is keyed on the JOURNALED command name**, not the client-facing one
(`ClassifyJournaledCommand`, `multimaster_lww.h`): `SET`, `SETNX`, `GETSET`, `GETDEL`, `PEXPIREAT`,
`PERSIST`, `RESTORE` (single-key, compared generically before the Op function runs), and
`MSET`/`DEL` (multi-key, self-guarded — see the per-key split below). Every other journaled name is
unguarded; an unrecognized name fails open, never closed. The master already normalizes several
client-facing commands onto one of those names before journaling — `SETEX`/`SET ... EX` → `SET`,
`UNLINK` → `DEL`, the `EXPIRE` family → `PEXPIREAT`/`DEL` (it sets a TTL, or deletes the key
outright when the given expiry is already in the past — it never just removes a TTL while leaving
the key alive; that is `PERSIST`'s own separate top-level command, which the `EXPIRE` family never
journals under), `MSETNX` → `MSET`, and `GETEX`'s own expiry path and
`GAT` (a separate call site, picking the same three names independently) → `PEXPIREAT`/`DEL`/
`PERSIST` — so those inherit the guard through whichever guarded name they land on. State-carrying
RMW results that journal under a guarded name are
deliberately guarded too, because a journaled `SET`/`DEL`/`RESTORE` is a blind full-state write on
the receiver exactly like any other: `PFMERGE` → `SET` (`hll_family.cc`), `BITOP` → `SET`/`DEL`
(`bitops_family.cc`), a `*STORE`-family command's empty result → `DEL` (every `*STORE` command
below gets this treatment, guarded), a cross-shard `SORT ... STORE`'s *non*-empty result →
`RESTORE ... REPLACE` specifically (the set/zset `*STORE` commands' own non-empty result does NOT
get this treatment — see the residual exposure just below), `COPY` (same-shard or cross-shard — it
always goes through the RESTORE-journaling path, never the verbatim-recipe one) →
`RESTORE ... REPLACE`, and a cross-shard `RENAME`'s `DEL` src + `RESTORE ... REPLACE` dest.
**Delta-journaled RMW is deliberately never guarded** — `INCR`,
`APPEND`, `LPUSH`, `HSET`-style commands, `PFADD`, and similar always resolve by plain arrival
order, guard on or off: dropping a delta permanently loses it rather than merely reordering it, and
there is no full "result" to journal instead.

**A non-empty `SINTERSTORE`/`SUNIONSTORE`/`SDIFFSTORE`, `ZUNIONSTORE`/`ZINTERSTORE`/
`ZDIFFSTORE`/`ZRANGESTORE`, or `GEORADIUS`/`GEORADIUSBYMEMBER` `STORE`/`STOREDIST` result is a
known gap, not yet fixed.** Unlike `SORT ... STORE`'s own destination write, every one of these
commands' `Op` functions is called with `overwrite`/`override` hardcoded `true` — unconditionally,
whether or not the destination previously existed — so a non-empty result always journals `DEL`
(guarded) and `SADD`/`ZADD` (delta-journaled, unguarded) as two SEPARATE entries. A guarded
receiver whose own destination is newer correctly drops the `DEL`, but then blindly applies the
unguarded add on top of its own untouched, newer value anyway, producing a third state neither
node ever actually held. Their EMPTY-result case is not the same shape both ways: a destination
that *becomes* absent (it held a value, now deleted) journals a bare, guarded `DEL`; a destination
that *stays* absent (nothing was there to delete) journals nothing at all — neither has this
problem, since there is never an add to follow either way. See
[`docs/ISSUE-REGISTER.md`](ISSUE-REGISTER.md) (D-21).

**A same-shard `RENAME`/`RENAMENX`, a same-shard `SORT ... STORE`, an exact (non-approximate,
non-`MAXLEN`) `XTRIM`, and a handful of name-ambiguous commands are unguarded by design, not by
oversight.** The first three revive auto-journal at runtime (`Transaction::ReviveAutoJournal`) and
journal the client's own command verbatim under its own name — none of `RENAME`/`RENAMENX`/`SORT`/
`XTRIM` is in the guarded table, so all four resolve by plain arrival order on a guarded link (the
*cross-shard* form of `RENAME`/`RENAMENX`/`SORT ... STORE` takes the RESTORE-journaling path above
instead, and is guarded). Separately, `JSON.SET`/`JSON.MERGE`/`JSON.DEL`/`JSON.FORGET`/
`JSON.CLEAR` at the root (`"$"`) path are full-value writes under a name that cannot tell that path
from an ordinary partial one, so guarding the name would also guard-and-drop their everyday partial
uses; `CMS.MERGE` (always a full reset-then-recompute of the destination sketch, but journaled
under its own name rather than a guarded one) and `BF.LOADCHUNK`'s `cursor == 1` init phase
(overwrites any existing key wholesale, same name as every other, incremental chunk) are the same
class of problem for a different reason — all left unguarded, resolving by arrival order like any
other unguarded command. See
[`docs/ISSUE-REGISTER.md`](ISSUE-REGISTER.md) (D-18) for the full account.

**Two commands are rewritten on the receiver before dispatch, so their journaled form reproduces
the author's *result* rather than the author's *command*, but only when the link is actually
guarded for that entry** (`LwwGuardActive(repl_lww_guard, repl_mvcc)` — the flag off, a plain
replica, or an unstamped entry all skip the rewrite and apply verbatim, same as before P4-4):
`SETNX` (conditional on non-existence — applying it verbatim on a receiver that already holds the
key would be a silent no-op even once the guard has already let it through) becomes a plain `SET`;
`RESTORE` without `REPLACE` errors on an existing key, and that reply-level error is reported back
by `DispatchCommand` as an applied `OK` — so `REPLACE` is injected. Once triggered, both rewrites
(`ApplyLwwRewrites`, `multimaster_lww.h`) are unconditional name/arg edits with no lock hazard, so
they run pre-dispatch; the stamp *compare* itself still runs inside the transaction, under the
key's lock, exactly like every other guarded command.

**A guarded `MSET`/`DEL` applies — and journals — only its non-stale keys.** `GetShardArgs` hands
these two commands a whole run of keys (and, for `MSET`, values) rather than a single key, so the
generic single-key veto skips them by design; `OpMSet` and `OpDelV2`
(`string_family.cc`/`generic_family.cc`) run the per-key compare themselves, under the same
shard's key locks, and journal exactly the surviving keys in their original relative order — never
a prefix of the original argument list, and never all-or-nothing (one stale key in an `MSET` does
not sink the fresh keys beside it). This is not a new atomicity hole: a cross-shard `MSET`/`DEL`
was never atomic across shards under replication even before this guard existed — each shard's own
`OpMSet`/`OpDelV2` call already journals its own entry independently, applied by that shard's own
flow with no cross-shard barrier — the per-key split only adds the possibility of a *single
shard's* own partial application, on top of an atomicity gap that already existed one level up.

**Classic (non-DFLY-protocol) links are never guarded, by construction. A DFLY link to a
non-active peer is guarded exactly like any other peer link, by construction — but never actually
drops anything, because every entry it carries fails the guard's own per-entry data check
instead.** These are two different mechanisms, not one.

`Replica::ConsumeRedisStream` (`replica.cc`) never touches `JournalExecutor` at all: it builds its
own bare `ConnectionContext` and sets `is_replicating`, `journal_emulated`, `skip_acl_validation`,
`ns`, and `repl_origin_idx` on it directly — but never calls `SetApplyLwwGuard`, so `repl_lww_guard`
stays at its default `false` for the whole classic link's lifetime regardless. That is a structural
property of the link, fixed before a single command ever dispatches (independently, this protocol
also never carries a per-key `mvcc` at all, so `LwwGuardActive` would exclude it a second way even
if the bit were somehow true). A non-transactional command with no keys at all (`SELECT`, `PING`)
never even reaches a `Transaction`'s own callback machinery in the first place, so it is trivially
"never guarded" too — there is no callback for a veto to skip.

The stream's own batching does not change any of that, even though the batching itself is not
specific to replication at all: `Service::DispatchSquashedBatch` (`main_service.cc`) is the SAME
pipeline-squashing mechanism an ordinary client connection's own pipelined command burst uses too
(`facade::Connection`'s own dispatch loop calls it directly) — `ConsumeRedisStream` is just one of
its callers, not the reason it builds a non-atomic transaction. It collects a run of commands,
stopping at the first one it cannot batch at all (an unknown command, `MULTI`/`EXEC`, `EVAL`, a
blocking command, an admin command, a connection-state command, or a subscribe-family one) and
returns how many it consumed; `ConsumeRedisStream`'s own loop dispatches whatever it stopped on
through an ordinary `DispatchCommand` call before retrying the batch from there. Whatever it *did*
collect gets handed, once per batch, to one **non-atomic** `MultiCommandSquasher` — built over a
`Transaction` started with `StartMultiNonAtomic(Transaction::DEFAULT)`, which is what
`DispatchSquashedBatch` always builds, for any caller. Inside it, `TrySquash` rejects any command
that is not transactional, is `CO::BLOCKING`, is `CO::GLOBAL_TRANS`, or spans more than one shard's
keys; those run through `ExecuteStandalone`, directly on that same non-atomic `Transaction`.
Everything `TrySquash` *does* accept is grouped per shard and later dispatched by `SquashedHopCb`,
which — in non-atomic mode — runs directly on each shard's own thread against a per-shard
`Transaction` built with `StartMultiNonAtomic(Transaction::SHARD_LOCAL)`.

Neither of *those* two transactions is ever a `SQUASHED_STUB`, so neither reaches
`RunSquashedMultiCb` directly — but the classic link CAN reach that function by a different route.
`EVAL`/`EVALSHA` is in `DispatchSquashedBatch`'s own break list above, so it falls straight to
`ConsumeRedisStream`'s ordinary `DispatchCommand` fallback and takes the normal Lua-eval path. When
a script's declared keys all hash to one shard, `CanRunSingleShardMulti` builds its own stub with
`new Transaction{tx, real_sid, ...}` — the *other* stub constructor, the one that unconditionally
sets `SQUASHED_STUB` no matter what mode its parent `tx` (the script's own outer transaction) is
in — so the script's inner commands DO run through `RunSquashedMultiCb`. (The same constructor
also backs two other `SQUASHED_STUB` producers, for completeness: `DEBUG POPULATE`'s own stub,
built directly over an explicitly non-atomic `SHARD_LOCAL` parent — proof by construction that this
role is not atomic-only — and the atomic squasher's own asynchronous `EVAL` command flush.) None of
this actually changes anything observable on the classic link, though: that constructor also
copies `repl_lww_guard_` straight from its own parent `tx`, and `tx` itself got that bit from the
SAME always-`false` connection, the ordinary way any transaction on this link does. So
`RunSquashedMultiCb`'s `LOG(DFATAL)` tripwire still never fires here — not because the classic link
cannot reach the function, but because `IsLwwGuarded()` reads `false` there exactly as it does
everywhere else on this link.

A DFLY-protocol peer link to a peer that is itself non-active is a genuinely different case. The
guard *bit* is set exactly the way it would be for any other peer link — `peer_mode_ &&
IsActiveReplica() && --multi_master_stream_lww` depends only on THIS node's own state, never on
whether the remote peer is active — so `repl_lww_guard` reads `true` there too. What keeps it from
ever dropping anything is `LwwGuardActive`'s other half, `incoming_mvcc != 0`, evaluated fresh for
every entry: a non-active peer never stamps anything on its own side (`MvccEnabled()` is false
there), so it writes journal framing v1 — the format with no `mvcc` field on the wire at all (see
`docs/UPSTREAM-SYNC.md`'s framing-version row) — and every entry it ever sends therefore decodes
here with `mvcc == 0`, excluded by `LwwGuardActive` on that basis alone, every single time. That is
a runtime fact about each entry's own data, not a structural property of the link the way the
classic case above is.

**`DEBUG MVCC`'s `origin:` field can end up naming no real node in the mesh.** The applied-write
stamp floor (`FloorAppliedStamp`, `mvcc.h`) that protects an *unguarded* applied write (arrival
order — guard off, a delta-RMW command, or a plain replica) whose author stamp is older than the
key's stored stamp `S` commits `{S.mvcc, S.origin_hash - 1}` instead of that older stamp verbatim —
one tick below `S`, keeping the key's stamp monotone for practical purposes, so a later, clean full
sync from a peer holding `S` still wins the next merge compare and the two copies re-converge.
(Edge case: `FloorAppliedStamp` already excludes `S.Mvcc() == 0` — the fresh-key/uncommitted-
placeholder shape — before reaching this branch at all, so `S.origin_hash == 0` here means a real,
non-zero-`mvcc` stamp whose hash field happens to be exactly `0`; the floor then lands at
`{S.mvcc - 1, UINT64_MAX}`, dropping the `mvcc` field by one tick instead of underflowing a hash of
`0`.) The cost is that the resulting `origin_hash` is an arbitrary
derived number, not any node's own registered hash, so `DEBUG MVCC <key>`'s `origin:` line can show
a value that matches no peer in the mesh once this has happened even once. Extend the "do not diff
`DEBUG MVCC` across peers" rule (see Observability, below) to cover this case too, alongside an
expiry tombstone's own per-node stamp.

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

**An expiry's tombstone stamp is order-equivalent to the value it replaces.** A `kExpired`
tombstone carries the expired value's OWN pre-deletion stamp — the same `mvcc` AND the same
`origin`, with the tombstone bit set (`RecordExpiryBlocking`/`CommitOwnTombstone`,
`tx_base.cc`/`mvcc.cc`) — never a freshly minted one, unless the value carried no real stamp at
all (a genuinely fresh key), in which case a fresh self stamp is minted instead, exactly as before.
This is the same rule a merge load already applies to an incoming key whose TTL has already
elapsed (`ApplyMergeTombstoneOnShard`, `rdb_load.cc`): reusing the value's own stamp keeps the
tombstone comparing exactly as the value it replaces would have, so a peer's write with a stamp
strictly newer than the expired value still wins the next `MergeAccepts` compare, and a peer's
write with an OLDER stamp still loses — a freshly minted reap-time stamp, by contrast, would
outrank every write between the value's own stamp and the moment of reaping, including ones that
should have won.

An expiry `DEL` is still deliberately **not** forwarded on a peer link: `journal::
PassesPeerEchoFilter` (`journal/types.cc`) drops every entry carrying `kEntryFlagExpired`, and
`SliceSnapshot::ConsumeJournalChange` applies the same filter to a full sync's concurrent journal
blob. This remains intentional even though the tombstone's stamp is now order-equivalent to the
value: an expiry is still each node's own local decision about when to reap, and forwarding it as
a foreign delete would apply one node's reap timing to another node's copy of the key.

The consequence: when the same TTL fires on two peers (the normal case, since the TTL itself
replicates), both nodes reap the SAME logical value and install the SAME `{mvcc, origin}`
tombstone for that key — `DEBUG MVCC <key>` on the two nodes now **agrees**. What can still differ
is the tombstone's own age relative to `--multi_master_tombstone_ttl`: because the stamp is the
value's write-time, not the reap-time, its GC deadline (`DeadlineMs`, `mvcc.h`) is `write_time +
tombstone_ttl`, not `reap_time + tombstone_ttl` — see "Sizing the TTL" below for the operational
consequence. (A tombstone reaped on exactly one node still propagates verbatim to peers via the
opcode-225 section of that node's next full sync, exactly as before.)

On a PLAIN (non-active-mesh) replica specifically, this expiry `DEL` still arrives — only a peer
link filters it — and carries the tombstone's own masked `mvcc` on the wire, so that replica's own
floor lands on or under the SAME value the source node just committed, rather than minting one of
its own. Its recorded `origin`, though, can differ from the source's: the replicated command's
origin is resolved from this link's own registered author (the source node itself, from the
replica's point of view), never from the expired value's original `origin_hash`, which does not
travel on the wire at all. A plain replica already needs a full resync before it can join the mesh
as a peer, so this divergence is accepted rather than fixed here.

**Tombstones are default-namespace-only.** `PerformDeletionAtomic`'s tombstone-earning check
(`db_slice.cc`) is gated on `ns_ == &namespaces->GetDefaultNamespace()`. A delete in an ACL
namespace (`ACL NAMESPACE:` / `AUTH`-selected non-default namespace) gets no tombstone at all,
regardless of the tombstone flags — it behaves exactly like a pre-Phase-4 delete, and is fully
exposed to resurrection by any peer's full sync. This mirrors the journal wire's own limitation
(non-default namespaces have no replicated identity — see `DEBUG MVCC`'s own default-namespace-only
restriction below) rather than being a separate gap.

### The three tombstone flags

| Flag | Default | Effect |
|---|---|---|
| `--multi_master_tombstone_ttl` | `600` (seconds) | How long a tombstone is retained before the idle-task GC (`DbSlice::TombstoneGcStep`) reclaims it. **`0` disables tombstoning entirely**, on both the write and the load side: a live delete erases immediately instead of arming a tombstone (identical to pre-Phase-4 behavior), loading an already-persisted tombstone skips installing it instead, and the GC step itself no-ops. The skip is **not** silent-or-loud the same way everywhere: reloading your own prior `SAVE`/`DEBUG LOAD` file logs one warning per process (`rdb_load.cc`'s non-merge install gate), but a peer's full sync hits a separate gate on the merge-apply branch that logs nothing at all — see the mesh-divergence note below for what that means operationally. `RESTORE` never reaches either gate: it restores a single key via `RdbRestoreValue`, never Dragonfly's RDB opcode loop, so it can never carry or skip a tombstone section. See the restart caveat below for what this means across a TTL change. |
| `--multi_master_max_tombstones` | `1000000` | Cap on live tombstones **per (database, shard) pair, not per shard overall**: `table->stats` (`db_slice.cc`) lives on one `DbTable` per `SELECT`-able database index on each shard, so a node with N databases can hold up to N times this many tombstones on a single shard. A delete that would push its own (database, shard) pair over this cap **degrades to a plain erase instead of arming a tombstone** — see "Hitting the cap" below. |
| `--multi_master_tombstone_gc_budget` | `64` | Side-table buckets the idle-task GC visits per tick. Must be `>= 1` (validated at boot); bounds one GC tick's total work across however many databases still need visiting. |

**Flipping the TTL from `0` to a nonzero value across a restart protects only future deletes.**
A node run with `--multi_master_tombstone_ttl=0` writes no tombstones for its own live deletes,
and (see the row above) will not even *install* an already-persisted tombstone it loads from an
older file written while tombstoning was on. If you re-enable tombstoning by restarting with a
nonzero TTL, deletes made **before** that restart have no tombstone to protect them — only deletes
made after the flip get the protection. There is no way to retroactively tombstone a delete that
already happened while tombstoning was off.

**A `ttl=0` node in a mesh never agrees with a peer's tombstone at the MVCC level.** The peer's
delete authority is still real and still applies: when a peer's tombstone wins a full-sync merge
against this node's stale live value, the value itself is deleted here too — it converges to
absent on both sides, with no resurrection and no oscillation. What does **not** converge is the
MVCC metadata, because this node's own merge-apply tombstone-install gate is `TombstonesEnabled()`
too: this node never installs an MVCC tombstone of its own for that key, so `DEBUG MVCC <key>`
reports `state:absent` here, while the peer that sent the delete keeps reporting `state:tombstone`
for the same key — a divergence that is bounded by the *peer's* own tombstone TTL, not this node's:
the peer's `TombstoneGcStep` (`db_slice.cc`) reaps its tombstone once that TTL elapses, after which
both sides report `state:absent`. Until then, the practical consequence is a second-order
resurrection risk: with no stamp of
its own to defend that key, this `ttl=0` node has nothing to compare a *later*, stale write from a
*third* peer against, so that stale write is accepted unconditionally
(`MergeAccepts(std::nullopt, incoming)` always accepts) — resurrecting the value even though the
correct peer's tombstone is still live and unexpired elsewhere in the mesh.

**Sizing the TTL against expected partition length -- and against key TTLs.** A tombstone protects
a delete only for as long as it is retained: if a peer is partitioned (network split, long
maintenance window, extended `REPLICAOF` gap) for longer than `--multi_master_tombstone_ttl`, and
a key on this node was deleted during that gap, the tombstone may already be gone by the time the
peer reconnects and full-syncs — and the peer's stale copy resurrects the key exactly as if
tombstoning were off. Set the TTL comfortably longer than the longest partition you expect to
recover from automatically; 600s (10 minutes) is a starting point for a mesh on a stable LAN, not a
universal answer. There is no cost to setting it much higher other than the tombstone's own memory
(one MVCC side-table slot, independent of the tombstoned key's own now-freed prime-table slot) and
the risk of hitting `--multi_master_max_tombstones` sooner on a delete-heavy shard.

An expiry tombstone's GC deadline is measured from the expired value's OWN write time, not from
when it was actually reaped (see "order-equivalent to the value it replaces" above) — so for a key
whose own TTL is close to, or exceeds, `--multi_master_tombstone_ttl` (e.g. `SET k v EX 3600` under
the 600s default), the tombstone can be born already past its deadline, or close to it, and get
reclaimed by the very next idle GC pass — losing the resurrection-protection window for that key
entirely, or nearly so. Size the tombstone TTL comfortably above the LONGEST key TTL you expect to
use, in addition to the longest partition length above (the two add, they do not take the max):
a key with a 1-hour TTL that expires right as a peer reconnects from an hour-long partition needs
tombstone protection for the partition length PLUS however long that key's own value had already
been live.

**Hitting the cap degrades to resurrection.** When a (database, shard) pair's tombstone count is
already at `--multi_master_max_tombstones`, the next delete for a key on that database and shard
does not error and does not block — it silently falls back to a plain erase, trading resurrection
risk for bounded memory. This is **visible, not silent**: it increments `mvcc_tombstones_dropped`,
reported both in `INFO memory` (beside `mvcc_tombstones`, summed across every database) and in
`DEBUG MVCC`'s per-shard aggregate (`shard<N>_tombstones_dropped:`, also summed across every
database on that shard). A non-zero, growing `mvcc_tombstones_dropped` means at least one
(database, shard) pair's delete-resurrection protection is currently degraded and either the cap
needs raising or the delete rate needs investigating; the aggregate does not tell you which
database.

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
- `INFO replication`: `multimaster_lww_dropped` — counts one per dropped *key*, not per replicated
  command (a guarded `MSET`/`DEL` naming several keys can contribute more than one to a single
  applied entry), on this node's peer links, summed across every shard/proactor thread
  (`ServerState::Stats::multimaster_lww_dropped`, `multimaster_lww.h`'s `NoteLwwDrop`). Same gate
  as the other `mvcc_*` replication fields: gated on `--active_replica`, absent (not zero)
  otherwise. Also exported as the Prometheus counter `dragonfly_multimaster_lww_dropped_total`,
  same gate.
- `DEBUG MVCC <key>`: prints `state:value|tombstone|absent`, and for `value`/`tombstone` the raw
  `mvcc:`, `ms:`, `counter:` (value only), and `origin:` fields for that key on its own shard.
- `DEBUG MVCC` (no key): per-shard aggregate — `shard<N>_entries`, `shard<N>_tombstones`,
  `shard<N>_tombstones_dropped`, `shard<N>_bytes`, `shard<N>_clock_last`,
  `shard<N>_clock_ahead_ms`, `shard<N>_unstamped_writes`.
- `DEBUG MVCC VERIFY`: from-scratch dense-invariant check across every shard/db; reports
  `mismatches:<N>`.

All three `DEBUG MVCC` forms require `--active_replica` and are local-only (default namespace
only — the journal wire has no namespace identity to carry a non-default one's state).

**A failed conditional delete or `SET ... NX` bumps `mvcc_unstamped_writes` even though nothing
changed.** `DELEX key IFEQ v` (predicate false) and `SET key v NX` (key already present) both open
the key with `FindMutable`, which arms an MVCC slot for it regardless of whether anything is
actually written; when the predicate/condition then skips the write, nothing journals, so
`EndOfWriteEpoch` finds that arm still pending and counts it as an unstamped write. This is benign
— no data changed, nothing was journaled — but a lock-release-style pattern that polls a
conditional delete or a `SET NX` in a loop (e.g. `DELEX lock IFEQ <token>`) will visibly raise this
counter without indicating any real problem.

**Do not diff `DEBUG MVCC <key>` across peers.** Two nodes that each expired the same key hold
different `{mvcc, origin}` tombstones for it, by design — see "An expiry's tombstone stamp is
per-node" above. Both will report `state:tombstone` (or `absent`, once the tombstone is GC'd) and
both will return nil; only the stamps differ. For a `DEL`, and for an expiry reaped on exactly one
node, the stamps do match across peers. The applied-write stamp floor (see "Streaming LWW" above)
is the same kind of case: a floored stamp's `origin:` is a number derived from, not equal to, a
real registered origin hash, so it is not comparable to any peer's own reporting either.

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
them). `kDrakeydbReplVersion` (currently `68`, bumped from `67` in P4-4) is the fork's own
replication protocol version, exchanged via `REPLCONF DRAKEY-VERSION` before a single RDB byte is
sent; an active node refuses to admit a consumer advertising a version older than its own, so a
pre-P4-3 drakeydb peer is refused *before* full sync rather than being admitted and hard-failing
mid-stream on opcode 225 — and, since the P4-4 bump, a pre-P4-4 peer (one that predates the
streaming LWW guard) is refused at that same handshake step too, even though it carries no new RDB
opcode of its own to hard-fail on. Upgrade a mesh in lockstep (see below).

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
- **D-18** — the runtime-revived recipes (same-shard `RENAME`/`RENAMENX`/`SORT ... STORE`, exact
  `XTRIM`) and the name-ambiguous full-value writes (`JSON.SET`/`MERGE`/`DEL`/`FORGET`/`CLEAR` at
  the root path, `CMS.MERGE`, `BF.LOADCHUNK`'s init chunk) described above, all unguarded by
  design.
- **D-20** — a guarded `DEL` newer than an absent key's older tombstone leaves that tombstone
  untouched (nothing to delete, nothing armed); a later write stamped between the two is then
  accepted here while a peer that saw the `DEL` against a still-live value holds a newer
  tombstone — a divergence a subsequent full sync from that peer repairs while its tombstone is
  still live, but not otherwise. Owned by P4-5 (tombstone lifecycle).
- **D-21** — a non-empty `SINTERSTORE`/`SUNIONSTORE`/`SDIFFSTORE`, `ZUNIONSTORE`/`ZINTERSTORE`/
  `ZDIFFSTORE`/`ZRANGESTORE`, or `GEORADIUS`/`GEORADIUSBYMEMBER` `STORE`/`STOREDIST` result always
  journals `DEL` (guarded) then `SADD`/`ZADD` (delta, unguarded) as two entries; a guarded receiver
  with a newer destination drops the `DEL` but still applies the add, merging a stale result into
  its own newer value. A later full sync from a node still holding the clean, correctly-stamped
  value repairs it; nothing in steady-state streaming does. Owner: open.

See that document for the full list, upstream-bug cross-references, and each entry's owning phase.
