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

**Namespace note:** this register's `U-N`/`D-N` ids are its own namespace, assigned in the order
entries are added here. Code comments and `.superpowers/sdd/*/task-N-report.md` files also cite
`D-N` ids from the *design spec*'s own decisions table (e.g. `docs/superpowers/specs/
2026-08-25-phase4-mvcc-lww-design.md`'s "D-8, merge-on-full-sync LWW") — a different, unrelated
numbering. When in doubt which one a citation means, check which document it appears in.

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

The reachable failure is the ordinary captured-table traversal after a mid-save flush: the value
still comes from the retained table while `GetMCFlag` consults its replacement. A proposed third
route through `FlushChangeToEarlierCallbacks` was disproved during P4-2 review. An occupied bucket
created after the earlier snapshot has a newer insertion version and is not forwarded to it; a
bucket old enough to be forwarded belongs to the table both consumers captured.

**How established:** found and live-proven for the drakeydb MVCC stamp, which sat on the same
line and had the identical shape (P4-2 adversarial review; 2,996/4,000 keys lost their stamp and
763/1,000 got a fabricated value/attribute pair). We fixed our field by accepting the bucket's
stamp only from the serializer's captured table; `mc_flags` was deliberately left alone as out
of scope. The `mc_flags` case is argued from the shared mechanism, not separately reproduced.

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

### U-4. `ProactorBase::RemoveOnIdleTask` does not clamp `on_idle_next_` when the array shrinks

**Where:** `helio/util/fibers/proactor_base.cc` — `RemoveOnIdleTask` (~`:238-246`) pops trailing
empty slots off `on_idle_arr_` but never adjusts `on_idle_next_`; the DCHECK it can violate is
`RunOnIdleTasks`'s `DCHECK_LT(on_idle_next_, on_idle_arr_.size())` (`:190`).

Any two on-idle tasks registered in order A, B where A outlives B (B is removed first, and A's
removal later empties or further shrinks the array) can leave `on_idle_next_` pointing past the
array's new end the next time `RunOnIdleTasks` resumes mid-round-robin (i.e. it last exited with
`on_idle_next_` sitting on the now-removed trailing slot's index). Debug builds `CHECK`-fail;
release builds read out of bounds.

**How established:** live-proven during P4-3 Task 3's development —
`test_narrowed_window_admits_peer_during_winners_full_sync` failed 3/12 with exactly this DCHECK
(`F proactor_base.cc:190 Check failed: on_idle_next_ < on_idle_arr_.size() (1 vs. 1)`) once a
second on-idle task (the tombstone GC) was registered above a pre-existing one (defrag) and the
shutdown path removed them out of order. Root cause confirmed by reading `RemoveOnIdleTask`
directly. Since `helio/` is off-limits to edit, the actual fix landed on our side instead, and not
via the self-unregistering-task design first tried (round 2: the GC task returning `-1` from its
own callback behind an alive-flag) — round 2 was found INSUFFICIENT (4/20 still failed, because
`PreShutdown` removing defrag completes before `~DbSlice` ever runs to flip the flag) and dropped.
The landed fix (round 3, deterministic) reorders removal instead of self-unregistering:
`main_service.cc`'s `Service::Shutdown` calls `namespaces->StopTombstoneGc()` (`:1240`) BEFORE
`shard_set->PreShutdown()` (`:1242`), so the GC task (registered with a lower id than defrag) is
always removed first, leaving a hole rather than shrinking the array below a possibly-stale
`on_idle_next_`. This avoids the ordering that trips the defect; it does not fix
`RemoveOnIdleTask` itself, which remains reachable by a different task-registration order (see
U-5).

**Status:** not filed.

### U-5. `EngineShard::StopPeriodicFiber` removes defrag then huffman — the identical abort, reachable on a plain node

**Where:** `src/server/engine_shard.cc`, `StopPeriodicFiber` (`RemoveOnIdleTask(defrag_task_id_)`
then `RemoveOnIdleTask(huffman_check_task_id_)`); `async_deleter`'s own `AddOnIdleTask` call sits
between the two registrations, in `src/server/db_slice.cc:359`.

Same mechanism as U-4, pre-existing and independent of anything this fork added: `huffman` is the
trailing on-idle task, so its removal pops the array; if `on_idle_next_` was sitting on that
now-removed trailing index (the array shrank exactly one entry short) when `async_deleter` is
still registered and mid-drain at shutdown, the next `RunOnIdleTasks` DCHECKs identically to U-4,
on a stock `[defrag, async_deleter, huffman]` node — no active-replica flag required.

**How established:** reasoned from source during P4-3 Task 3's review, not independently
reproduced under this exact three-task shape; the mechanism is the same DCHECK U-4 was live-proven
against, and the code layout that reaches it is confirmed by reading `engine_shard.cc` and
`db_slice.cc` directly.

**Status:** not filed. Root cause is U-4; this is a second, pre-existing trigger for it.

### U-6. `AddOrFindInternal`'s insert branch force-inserts without re-checking existence after its own yield

**Where:** `src/server/db_slice.cc`, `AddOrFindInternal` (`:1010-1104`) — the not-found branch
calls `CallChangeCallbacks` (`:1051`, blocking: it can invoke `SerializerBase::OnChange`, which
synchronously waits when a `SliceSnapshot` is registered on the shard), then falls through to an
unconditional `db.prime.InsertNew(key, PrimeValue{}, evp)` a few dozen lines later with no
intervening `Find` to check whether that same key was inserted by someone else during the yield.

A concurrent insert of the identical key in that window (e.g. another peer's apply, or a BGSAVE
callback) produces a second, duplicate prime-table entry for one logical key — a dense-table
invariant violation whose downstream effects (double iteration, double accounting) were not
characterized further.

**How established:** found by static reading during P4-3 Task 4's review while diagnosing a
different, related hazard in the same function (the merge-LWW compare-before-mutate race that
motivated switching this fork's own loader from `AddOrUpdate` to `AddOrFind` plus an
authoritative post-yield recheck — see `task-4-report.md`). Not independently reproduced with a
race harness; the yield and the missing re-check are both directly visible in source.

**Status:** not filed. Predates this fork (`AddOrFindInternal` is upstream code); reachable
without `--active_replica`, since any registered `change_cb_` (e.g. a plain `BGSAVE`) creates the
yield.

### U-7. `CMS.MERGE`'s multi-shard auto-journal emits a truncated, replica-rejected command

**Where:** `src/server/cms_family.cc:500` — `CI{"CMS.MERGE", CO::JOURNALED | CO::DENYOOM |
CO::VARIADIC_KEYS, -4, 3, 3}`: no `CO::NO_AUTOJOURNAL`, and no `RecordJournal` call anywhere in
the file.

`CMS.MERGE dest numkeys src1 [src2 ...]` is variadic-keys and can span multiple shards. Because
it carries no `NO_AUTOJOURNAL` and never hand-journals, Dragonfly's ordinary per-shard
auto-journal (`LogAutoJournalOnShard`, `transaction.cc`) fires on every participating shard and
builds each shard's journal entry from `GetShardArgs` — and `GetShardArgs` carries only the
**keys** this shard owns from the command's own key spec, not the full argv: `DetermineKeys`
(`transaction.cc:~1904-1911`) scopes CMS.MERGE's key range to `src1..srcN` alone (`bonus = 0`
separately marks `dest`, at argv index 0, as a lone bonus key — the same mechanism `SORT ...
STORE` uses for its destination), and `numkeys` is never a key at all, so it never appears in any
shard's `GetShardArgs` output regardless. A shard that owns one or more source keys but not
`dest` therefore auto-journals a bare `CMS.MERGE <one src key on this shard>` — missing `dest`,
missing `numkeys`, and missing every other shard's source keys — 2 total arguments (command name
+ one key) against the command's own declared minimum arity (`-4`, at least 4), which a replica
parses and rejects outright rather than silently corrupting state.

**How established:** found by static reading during P4-3 Task 7's review, by analogy with the
`SORT ... STORE` defect that same task fixed (same shape: variadic/cross-shard command,
auto-journal splits args per shard, no compensating hand-journal). Not reproduced against a live
multi-shard replica; the registration flags and absence of `RecordJournal` are confirmed directly
in source.

**Status:** not filed.

### U-8. `GEORADIUS`/`GEORADIUSBYMEMBER`'s `STORE` destination never replicates

**Where:** `src/server/geo_family.cc:779,782` — both registered
`CO::JOURNALED | CO::STORE_LAST_KEY | CO::NO_AUTOJOURNAL`, and the file has no `RecordJournal`
call anywhere.

`NO_AUTOJOURNAL` suppresses the ordinary per-shard auto-journal entirely, and — unlike `SORT`
after P4-3 Task 7's fix — nothing hand-journals the `STORE` destination as a replacement. The
destination key computed by `STORE`/`STOREDIST` is therefore local-only: it is written on the
issuing node and never reaches any replica or peer, regardless of configuration.

**How established:** confirmed directly in source (registration flags, and an exhaustive grep for
`RecordJournal` in the file returns nothing) during P4-3 Task 7's review, prompted by the same
`WillAutoJournalVerbatim`/`NO_AUTOJOURNAL` audit that found the `SORT` defect. Not reproduced
against a live replica.

**Status:** not filed.

---

## Part 2 — drakeydb deferred work

### D-1. No mvcc half for Redis-protocol / KeyDB peer links (P7)

`serializer.cc` writes `mvcc` only under `extended_framing_`, i.e. `IsActiveReplica()`, and the
plain-Redis wire has no slot for it. Genuinely needs a wire mechanism that does not exist; the
design spec assigns it to P7. The `origin_hash` half **is** implemented. Never tested
end-to-end — three separate agents independently said so.

**Owner:** P7. **From:** P4-1.

### D-4. `origin_hash` residual on an expiry-swept sibling key

Wrong `origin_hash`, correct `mvcc`, and only on an exact tie — `operator<` is lexicographic on
`(Mvcc(), origin_hash)`. Closing it needs `Commit()` to accept a stamp differing from its
enclosing journal entry, which is a design change.

**Owner:** unassigned (revisit if ties become observable). **From:** P4-1.

### D-5. A non-active node still emits `node_uuid:` in INFO replication

So "byte-identical with `--active_replica` off" is true for the journal wire, the RDB file, and
INFO memory, but not for INFO as a whole.

**Owner:** unassigned; introduced in P1/P3. **From:** P4-1.

### D-8. Both stamp forms for one key are untested

If a single RDB stream carries both `RDB_OPCODE_DF_MVCC` and a KeyDB `mvcc-tstamp` aux for the
same key, the behavior is deterministic last-in-stream-wins (the opcode wins in natural order,
since the aux precedes the key). Coherent, but only reasoned about, never tested. No producer
emits both today.

**Owner:** P7 (KeyDB onboarding). **From:** P4-2 final review.

### D-9. Historical commit-message figure

Commit `0d59e9fc`'s message carries a wrong "63%" figure for the `mvcc_table_bytes` under-report
(the true figure is ~43.6% below true cost). The code, benchmark docstring, and `PLAN.md` are
correct; the historical message can only be changed by rebasing the PR.

**Owner:** any phase. **From:** P4-2.

### D-10. Multi-shard pytest coverage is new and narrow

P4-2 widened the stamp acceptance test to three shards and added a full-sync-plus-restart test,
but the rest of `multimaster_test.py` still runs at one or two shards. Anything a future phase
asserts about shard routing needs its own multi-shard test — the file's default will not give it.

**Owner:** P4-3 onward. **From:** P4-2.

### D-11. Classic-PSync ctime authority should subtract clock skew and floor at the PSYNC send time

**Where:** `src/server/rdb_load.cc`, the classic-protocol unstamped-key branch
(`merge_lww_ && merge_classic_protocol_ && !item->has_mvcc`, ~`:3423-3452`): today's rule is
`stamp_ms = min(ctime_ms + 999, now_ms)`.

Follow-up identified during P4-3 Task 12's review: `clamp(ctime_ms - clock_skew_ms_,
psync_sent_local_ms, now_ms)` — subtracting this link's already-measured `clock_skew_ms_`
(`replica.cc:~466`) and flooring at the local wall-clock time this node sent its own `PSYNC` —
would close the *peer-clock-BEHIND* half of the exposure that today's `min(..., now_ms)` alone
does not cover: a classic peer whose clock is meaningfully behind ours can otherwise claim
authority (via the plain `+999` ms ceiling) over every local key written since the true fork
point, not just since the peer's own clock reading. The floor is provably safe because the fork
being merged provably post-dates this node's own `PSYNC` send.

**How established:** statically argued during review, not implemented or tested. The reviewer
proved the current rule's clobber bound both ways (a classic peer can overwrite local writes made
at most 999 ms after the true fork, never more) and measured that the onboarding pytest passes
today because of the `min(..., now)` clamp, not because of the `+999` ceiling — so this follow-up
is a genuine tightening, not a fix for an observed failure.

**Owner:** unassigned (revisit alongside D-12 if classic-protocol clock skew becomes an observed
operational problem). **From:** P4-3 Task 12.

### D-12. A classic-PSync unstamped key resurrects any resident tombstone older than `min(ctime + 999 ms, now)`

**Where:** `src/server/rdb_load.cc:3413-3452` (the ctime-authority rule above); documented in
[`docs/multi-master.md`](multi-master.md)'s "classic-protocol peers" section and pinned by
`RdbMvccTest.MergeLwwClassicUnstampedIncomingResurrectsTombstoneOlderThanCtime`
(`src/server/rdb_test.cc`).

A classic-protocol peer (plain Redis, or any RDB source with no per-key `mvcc-tstamp`) carries
exactly one whole-snapshot timestamp, not a per-key write time. `MergeAccepts` masks the
tombstone bit for its comparison, so a resident tombstone loses to *any* unstamped incoming key
whose ctime-derived stamp is newer — indistinguishable, from the classic side's single timestamp,
from a legitimate post-delete rewrite. This replaced Task 12's withdrawn unconditional-override
rule (which had the same class of exposure but far wider: it could resurrect every tombstone on
the link, and clobber concurrent applies from other peers too) with a narrower one; it is a
genuine, inherent-to-the-mechanism exposure, not a residual bug to be patched away by more tuning
of the ctime formula, short of D-11 above or giving classic links a genuine per-key time source
(neither Redis nor KeyDB, without KeyDB's own `mvcc-tstamp`, has one to give).

**How established:** live-proven. Reproduced empirically during Task 12's review (write a key on
the classic master, delete it locally, wait, full-sync — the local tombstone is replaced and the
peer's stale value wins, 3/3) and pinned as a permanent regression test in `rdb_test.cc`, not left
as a one-off review finding.

**Owner:** unassigned; narrow via D-11, or accept and keep documenting (current state). **From:**
P4-3 Tasks 12-13.

### D-13. A same-shard `SORT ... STORE` still journals the sort recipe, not its computed result

**Where:** `src/server/generic_family.cc:2015` — `hand_journal = op_args.shard->journal() &&
op_args.tx->GetUniqueShardCnt() != 1`, i.e. hand-journaling (a `RESTORE` of the computed
destination, added by P4-3 Task 7) is deliberately skipped when source and destination land on
the same shard; that case still relies on the ordinary auto-journaled `SORT ... STORE` command
being replayed verbatim on every peer.

Closes the cross-shard half of former entry D-3 (`SORT ... STORE` not replicating at all) — this
is the narrower residual Task 7 left standing. A `BY`/`GET` pattern key is not a transaction key
(SORT's own keyspec covers only the sorted key and `STORE`'s destination), so a peer can
legitimately compute a *different* sort order or a different fetched value than this node did —
different pattern-key contents, or, for `BY nosort` against a plain `SET`, an iteration order that
is a local implementation detail — while still executing the identical journaled command under
the identical mvcc stamp. `MergeAccepts` ties favor the stored side, so once that divergence
exists, no future merge ever repairs it: both sides consider their own value current and neither
stamp is ever strictly newer than the other's.

**How established:** found by static reading during P4-3 Task 7's review (the same audit that
found and fixed the cross-shard case); not reproduced with an actual `BY nosort` divergence
between two live nodes. The owner explicitly ruled against fixing it in Task 7: hand-journaling
the same-shard case too would be correct but changes the wire format for a path that works today
under the pre-P4-3 contract, and the size/complexity trade-off of doing so for what is a narrow,
pattern-key-dependent edge case was left for a future owner decision.

**Owner:** unassigned; owner to decide whether the divergence-under-`BY`-pattern case is worth the
added journal size. **From:** P4-3 Task 7.
