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

### U-9. `EvalInternal`'s connection migration can null-deref on a classic replicated-apply link

**Where:** `src/server/main_service.cc`, `Service::EvalInternal` — the single-shard `EVAL`
migration branch: `if (sid.has_value() && *sid != ss->thread_index()) { ...
conn_cntx->conn()->RequestAsyncMigration(shard_set->pool()->at(real_sid), false); }`.

`conn_cntx->conn()` returns `ConnectionContext::owner_` (`facade::ConnectionContext`), which is
`nullptr` for a replicated-apply context: `JournalExecutor`'s own constructor builds its
`conn_context_` as `{nullptr, acl::UserCredentials{}}` (`journal/executor.cc`), and this is the
context every applied journal entry — including a replicated `EVAL`/`EVALSHA` — dispatches
through. If a single-shard `EVAL`'s declared key hashes to a DIFFERENT shard than the one
currently applying it (`*sid != ss->thread_index()`), this branch calls `RequestAsyncMigration` on
a null `Connection*`, dereferencing it unconditionally with no null check.

Reachable on a classic (non-DFLY-protocol) replicated-apply link specifically — a KeyDB
active-replica master's or a real Redis master's stream, replayed through `JournalExecutor` the
same way `Replica::ConsumeRedisStream`'s own fallback dispatch does — whenever that stream
replicates a single-shard `EVAL`/`EVALSHA` whose key does not hash to the applying thread's own
shard. A DFLY-protocol peer link's own applied commands go through the same `JournalExecutor`
path too, so it is not exclusive to classic links, but a classic upstream master is the most
direct route to a client-issued `EVAL` reaching this exact branch via replication.

**How established:** static reading of `EvalInternal`'s migration branch composed with
`JournalExecutor`'s constructor; not reproduced against a live crash (would need a single-shard
`EVAL` whose key lands on a different shard than the one that dispatches the replicated apply).

**Status:** not filed.

---

## Part 2 — drakeydb deferred work

### D-1. No mvcc half for Redis-protocol / KeyDB peer links (P7)

`serializer.cc` writes `mvcc` only under `extended_framing_`, i.e. `IsActiveReplica()`, and the
plain-Redis wire has no slot for it. Genuinely needs a wire mechanism that does not exist; the
design spec assigns it to P7. The `origin_hash` half **is** implemented. Never tested
end-to-end — three separate agents independently said so.

**Owner:** P7. **From:** P4-1.

### D-4. `origin_hash` residual on an expiry-swept sibling key -- resolved

Described a residual on the sibling key an expiry's own journal entry used to sweep into ITS
Commit() call, mid a multi-key command: that sweep stamped the sibling with the expiry entry's own
`mvcc` paired with a separately-supplied origin index for the sibling's true author, which could
diverge from the enclosing command's own origin on an exact `mvcc` tie (`operator<` is
lexicographic on `(Mvcc(), origin_hash)`).

Resolved as a side effect of a later change: an expiry's own journal entry no longer sweeps any
key but its own (`RecordExpiryBlocking`/`journal::RecordEntry`, `tx_base.cc`/`journal.cc`); a
sibling armed earlier in the same epoch is left alone until the enclosing command's own, later
journal entry commits it, with that entry's own `{mvcc, origin_idx}` pair directly -- there is no
longer a second, separately-supplied origin index for `Commit()` to disagree with `mvcc` on.

**Owner:** none (resolved). **From:** P4-1.

### D-5. `--active_replica`-off byte-identity has two documented exceptions

1. A non-active node still emits `node_uuid:` in INFO replication (P1/P3).
2. Since P4-3, a **cross-shard `SORT ... STORE` hand-journals its effect** (`RESTORE <dst>`) on
   every node, active or not (`generic_family.cc` — `SORT` is `CO::NO_AUTOJOURNAL`, `OpStore`
   hand-journals when `GetUniqueShardCnt() != 1`). This is a deliberate owner ruling, not an
   oversight: upstream's per-shard auto-journal payload is built from `GetShardArgs(shard_id)` and
   dropped the destination effect entirely, so a **plain replica did not converge** on a
   cross-shard `SORT ... STORE` before P4-3. Gating the fix on `--active_replica` would re-open
   that bug for plain replicas purely to preserve the slogan, so it stays on for everyone.

So "byte-identical with `--active_replica` off" is true for the journal wire *except* cross-shard
`SORT ... STORE`, true for the RDB file and INFO memory, and not true for INFO as a whole. Stated
that way in `docs/UPSTREAM-SYNC.md`, `docs/PLAN.md` and `docs/differences.md`.

**Owner:** unassigned; (1) introduced in P1/P3, (2) ruled deliberate in P4-3. **From:** P4-1,
restated P4-3 final review.

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

A classic-protocol peer (plain Redis or KeyDB — specifically the classic-PSYNC links that set
`merge_classic_protocol_` in the loader; a DFLY-protocol full sync or a local RDB load with
unstamped keys gets D-7's `{0,0}`, not ctime authority) carries exactly one whole-snapshot
timestamp, not a per-key write time. `MergeAccepts` masks the
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

### D-14. `HandleTombstones`' cross-shard dispatch can still steal a concurrent same-key arm

**Where:** `src/server/rdb_load.cc` — `HandleTombstones` dispatches its per-key apply with
`shard_set->Add(sid, ...)`, fire-and-forget and unserialized against ordinary command traffic on
that shard. Inside `RdbLoader::ApplyMergeTombstoneOnShard`, the `found_mutable` branch calls
`MvccStamper::tlocal()->Disarm(db_index, key)`, and `MvccStamper::tlocal()` is a per-**shard
thread** singleton, not per-callback.

Task 13's review fix (I3) scoped that `Disarm` to `found_mutable`, which closes the case where
this callback never touched the key at all. The residual: when `found_mutable` IS true, the
`Disarm` erases *every* arm matching `(db_index, key)` on that thread — including a different,
concurrent fiber's still-pending, legitimate arm for the same key name (e.g. a client's own
`DEL k` that yielded between `ArmTombstone` and its journal `Commit()`, on a slow replica or a
full ring buffer). That fiber's `Commit()` then finds no arm to stamp and its `{kTombstoneBit, 0}`
placeholder is never overwritten with a real, minted stamp: an immortal, unreapable tombstone
(`TombstoneGcStep`'s reap predicate requires `Mvcc() != 0`), i.e. a silently-lost delete.

The window requires a full sync's tombstone section and a client `DEL`/expiry for the *same key*
to interleave inside one shard thread's yield. Closing it properly needs arm identity (an owner
token on each arm, so `Disarm` can only cancel its own), not a narrower scope.

**How established:** static reading during P4-3 Task 13's review and re-confirmed in the final
whole-branch review; not reproduced. `RdbMvccTest.MergeLwwTombstoneInstallForAbsentKeyDoesNot
StealConcurrentArm` pins the half that IS closed.

**Owner:** unassigned; needs per-arm ownership in `MvccStamper`. **From:** P4-3 Tasks 6/13, final
review.

### D-15. Tombstone merge is only ever tested with two peers

**Where:** every tombstone/merge test in the branch — `rdb_test.cc`'s `RdbMvcc*` cases,
`multi_master_test.cc`, `tests/dragonfly/multimaster_merge_test.py` (both the fuzzer and the three
resurrection pins) — uses exactly **two** nodes, or one node plus a hand-built RDB stream.

The merge rule itself is pairwise and stateless, so two peers exercise the decision function
fully. What is untested is the *composition*: three or more peers where a stale intermediate value
can sit between two nodes' stamps. Concretely — the reasoning that motivated Task 13's
`would_grow` cap fix and several `Disarm` scopings is all of the form "a THIRD peer's later write,
correctly losing against the newer stamp, could wrongly win against a stale one left behind". That
argument has never been run. The same applies to an expiry tombstone, whether it comes from a
merge load's synthetic tombstone for an already-expired incoming key (`ApplyMergeTombstoneOnShard`,
`rdb_load.cc`) or from a live reap on this node (`CommitOwnTombstone`, `mvcc.cc`): both now derive
their stamp from the incoming or expired *value's own* stamp, one origin_hash tick above it
(`ExpiryTombstoneFor`, `mvcc.h`), so both order strictly newer than that value on a third peer that
still holds it live — believed safe by the same one-tick-above argument `ExpiryTombstoneFor`'s own
doc comment makes, but that too is reasoned rather than measured, not composed across three peers.

**How established:** coverage audit during P4-3's final whole-branch review. No failure is known;
this is an untested risk, not a reproduced defect.

**Owner:** unassigned; wants a three-node pytest topology (fan-in mesh already exists in
`multimaster_test.py`, so the fixture cost is low). **From:** P4-3 final review.

### D-16. An expiry tombstone can be born GC-eligible

**Where:** originally `src/server/rdb_load.cc`'s merge-load path for an incoming key whose TTL has
already elapsed (`ApplyMergeTombstoneOnShard`); as of the change that put every `kExpired`
tombstone (local lazy/active expiry, the member-expiry reaper, and this merge-load path) through
one shared rule (`ExpiryTombstoneFor`, `src/server/mvcc.h`), this applies to essentially every
`kExpired` tombstone, not only the merge-load one. `DbSlice::TombstoneGcStep` reaps when
`DeadlineMs(ttl) <= now`, and `DeadlineMs` is `MsPart() + ttl` (`src/server/mvcc.h`).

Every path derives its tombstone from the value's *write-time* stamp, one origin_hash tick above
it (bit 63 set) — the one-tick advance almost always lands in `origin_hash`, essentially never in
`mvcc` (see `ExpiryTombstoneFor`'s own doc comment for the rare carry case) — so the tombstone's
`mvcc` field, and therefore its GC deadline, is still `write_time + tombstone_ttl`, not
`reap_time + tombstone_ttl`. For any key whose own TTL exceeds `--multi_master_tombstone_ttl`
(default 600 s — e.g. `SET k v EX 3600`), the resulting tombstone is born already past its
deadline, or close to it, and is reaped on the next idle GC pass (or soon after), then excluded
from outgoing opcode-225 sections. The delete itself always stands; what is lost is the
resurrection-protection window that tombstone was meant to provide. See `docs/multi-master.md`'s
"Sizing the TTL against expected partition length -- and against key TTLs" for the operator-facing
guidance this motivates: size `--multi_master_tombstone_ttl` above the longest key TTL in use, in
addition to the longest partition length expected, since the two needs add rather than take a max.

A member-TTL container (a set/hash whose members carry individual TTLs — `SADD`+`FIELDEXPIRE`,
`HSET`+`HEXPIRE`, …) is even more exposed than a whole-key TTL: its eventual empty-container
tombstone derives from the container's own *last write* stamp (`DeleteReapedContainer`,
`SetFamily::DeleteSetIfEmpty`/`HSetFamily::DeleteIfEmpty` — all through the same `CommitOwnTombstone`/
`ExpiryTombstoneFor` rule), not from any one member's own TTL. A container can receive its last
write long before its last member finally expires — each member's TTL is independent and can be far
longer than `--multi_master_tombstone_ttl`, and the container itself may never be written again in
between — so the gap between that write-time stamp and the moment the container actually empties can
be arbitrarily larger than a single key's own TTL, making the resulting tombstone born even further
past its GC deadline than the whole-key-expiry case above.

Failure scenario (three peers, merge-load case): C partitioned before A's `SET k v2 EX 3600`; A's
key expires and A's sweep lags; full sync A→B applies the synthetic tombstone, which is GC'd within
an idle tick; C rejoins and full-syncs to B carrying its older live `k` → B has no tombstone →
`MergeAccepts` accepts → `k` resurrects on B (not on A) -- repairable only by a LATER full sync
from A that still has something authoritative to say about `k` (a live write, or a tombstone that
has not itself also been prematurely GC'd by then, the same hazard this entry describes); once A's
own tombstone for `k` is gone too, A's full syncs to B carry nothing about `k` at all, and the
resurrected copy on B persists indefinitely with nothing left to trigger a repair. The live-reap
case is the same shape without needing a merge load at all: A's own `k` (TTL 3600s) expires
locally, A installs a tombstone already 3000s past its GC deadline, A's idle GC reaps it almost
immediately, and A's next full sync to any peer carries no tombstone for `k` at all -- a peer
holding a stale live copy of `k` (never having applied A's delete) resurrects it on that peer. Both
are strictly better than erasing with no tombstone at all, which is what happens once the GC
deadline passes regardless.

Why not mint a fresh stamp instead, sized to give the tombstone its full protection window: that
would fabricate authority the value's own write never carried, and (for the live-reap case) is
exactly the reap-time-mint design this rule replaces -- a fresh mint outranks writes it has no
business outranking, which is worse than a short protection window, not better. The honest
alternatives are a separate reap-deadline field (rejected for the 16-byte per-key layout) or
clamping the deadline to `max(write_time, receive_time_or_reap_time) + ttl` at install -- a design
choice left open here.

**How established:** static analysis in the scoped re-review that added the merge-load path; the
three-peer scenario is unmeasured (see D-15). The live-reap and member-expiry-reaper paths'
identical exposure was identified when those paths were changed to derive their stamp from the
value's own too.

**Status:** open. **Owner:** unassigned (tombstone lifecycle). **From:** the merge-load synthetic
tombstone's own introduction; widened when the local-reap paths adopted the same rule.

### D-18. Runtime-revived recipes and name-level full-value writes are unguarded

**Where:** `src/server/generic_family.cc` — `RenameGeneric` calls `Transaction::ReviveAutoJournal`
for a same-shard `RENAME`/`RENAMENX` ("Safe to use RENAME with single shard"), and `SortGeneric`
does the same for a same-shard `SORT ... STORE`; `src/server/stream_family.cc`'s `CmdXTrim` does
the same for an exact (non-approximate, non-`MAXLEN`) `XTRIM`. All three re-enable auto-journal at
runtime and journal the client's own command verbatim, under that command's own name —
`RENAME`/`RENAMENX`/`SORT`/`XTRIM` — and none of those four names are in `multimaster_lww.cc`'s
guarded table, so a receiver re-executes the recipe against its own copy (arrival order), never
LWW-compared against the destination's stored stamp. The **cross-shard** form of `RENAME`/`RENAMENX`
and of `SORT ... STORE` takes a different path (`Renamer::DelSrc`/`DeserializeDest`, `OpStore`'s
hand-journal) that journals *state* — `DEL` src + `RESTORE ... REPLACE` dst, or `DEL` — under
guarded names, and so is already covered by the streaming LWW guard.

Separately, and for a different reason: a handful of commands whose name alone cannot distinguish a
full-value write from a partial one are unguarded by design, not by omission —
`JSON.SET`/`JSON.MERGE` (a `"$"` root path replaces the whole document, but the same name also
covers an ordinary partial patch), `JSON.DEL`/`JSON.FORGET`/`JSON.CLEAR` (same ambiguity for a
delete/clear at an arbitrary path vs. the root), `CMS.MERGE` (always resets the destination sketch
then writes the weighted sum of the sources — the same blind, state-carrying recompute `PFMERGE`'s
`SET` result is guarded for, but journaled under `CMS.MERGE`'s own name instead), and
`BF.LOADCHUNK`'s `cursor==1` init phase (overwrites any existing key wholesale). Adding any of
these to the guarded table would also guard-and-drop their ordinary partial-write uses, which is
worse than leaving the whole name unguarded.

**How established:** static reading during the guarded-vocabulary review; not reproduced with a
live divergent value. `MvccStoreTest.EmittedNamePinsMatchClassifiedGuardedNames` pins the four
runtime-revived names (verified against a running build); `MvccStoreTest.
RegistryClosureEveryAutoJournaledNameIsGuardedOrKnown` pins the JSON/`CMS.MERGE`/`BF.LOADCHUNK`
names as a reviewed, explicitly-known-unguarded allowlist rather than an accidental gap — both
tests fail by name if a future normalization change silently moves one of these onto, or off, a
guarded name.

**Owner:** unassigned; same-shape precedent as D-13 (same-shard `SORT ... STORE` journaling the
recipe rather than the result) — the owner ruled there that hand-journaling the result for the
same-shard case is correct but changes the wire format for a path that works today, and left it for
a future decision. Fix path if wanted: journal the *result* (state) for the runtime-revived
recipes, the same way the cross-shard forms already do. **From:** P4-4.

### D-19. Duplicate plain re-arms of a re-created key in one applied entry commit verbatim

**Where:** `MvccStamper::Arm` (`src/server/mvcc.cc`) inherits a pending arm's real previous stamp
only when the NEW arm's own `prev_stamp` is tombstone-shaped with `Mvcc() == 0` — the shape
`PerformDeletionAtomic`'s synchronous tombstone arm writes as an UNCOMMITTED, same-callback
placeholder (`SetTombstone`+`ArmTombstone`, before that delete's own journal commit ever runs), and
which `DbSlice::EnsureMvcc` (`db_slice.cc`) returns verbatim if that SAME placeholder is cleared
again later in the same callback (a delete-then-recreate sequence). For a genuinely COMMITTED,
pre-existing tombstone, `EnsureMvcc`'s clearing branch instead returns the real stamp
(`Mvcc() != 0`) verbatim — never the placeholder shape. Two `EnsureMvcc` calls for the same key,
both against an already-committed tombstone (e.g. `MSET k a k b` over a previously-tombstoned
`k`), therefore never trip the inheritance gate either time: the first call clears the real
tombstone and returns it verbatim (`Mvcc() != 0` — correctly not the placeholder shape, since
there is nothing to inherit yet); the second call finds the slot already cleared to a plain,
non-tombstone `MvccStamp{}` and falls through to `EnsureMvcc`'s "already live, unchanged" branch,
returning `{0, 0}` — not tombstone-shaped at all, so the gate never even looks for anything to
inherit. `armed_` still holds the first arm's real prior stamp one slot away, but nothing ever
asks it.

With the streaming guard OFF (the flag false, or a plain replica mirroring such a master), an
applied `MSET k a k b` over a previously-tombstoned `k` reaches exactly this shape: the first pair
clears the tombstone and arms with the real prior stamp as its `prev`; the second pair arms the
same key again with `prev = {0, 0}`. At commit time `FloorAppliedStamp` sees `stored.Mvcc() == 0`
for that second arm and returns the author's stamp verbatim, with no floor applied — so the key can
end up committed with a stamp *below* the tombstone it had before this entry, even though the
tombstone's own real prior stamp was sitting one arm slot away in the same `armed_` list.

**Unreachable on the guarded path**: a guarded `MSET`/`DEL` compares each pair against the key's
*currently stored* stamp before arming it at all (`OpMSet`/`OpDelV2`'s own per-key `LwwShouldDropKey`
check), so the first pair is dropped outright — never reaching `EnsureMvcc`/`Arm` for that key —
whenever its own author stamp is not already newer than the real prior tombstone; the precondition
for this defect (an arm committing below a stamp it never legitimately beat) therefore cannot arise
there.

**How established:** static reading of `Arm`'s inheritance-scan gate and `EnsureMvcc`'s
tombstone-clearing branch during the applied-write stamp-floor work; not reproduced against a live
two-node `MSET k a k b` scenario. Widening `Arm`'s inheritance scan to catch a plain-to-plain
duplicate re-arm (not only a tombstone-placeholder one) would add an `O(armed_.size())` scan to
every fresh insert, not only the already-narrow tombstone-placeholder case.

**Owner:** unassigned; only reachable with the streaming guard off, so lower priority than the
guarded-path defects above. **From:** P4-4.

### D-20. A newer `DEL` of an absent key does not advance an older tombstone

**Where:** `GenericFamily::OpDelV2` (`src/server/generic_family.cc`) — the per-key LWW skip check
runs before `FindMutable`, so a guarded `DEL` whose author stamp is newer than an absent key's
existing tombstone `T1` is NOT dropped by that check (it is not stale relative to `T1`) and falls
through to `FindMutable`; finding nothing valid there, it simply `continue`s to the next key,
without ever calling `db_slice.Del`/`PerformDeletionAtomic` and therefore without ever arming or
committing any stamp for that key at all. `T1` is left exactly as it was — the incoming `DEL`'s own
(newer) stamp is discarded, recorded nowhere.

A peer that instead held a *live* value for that same key at the time would accept this same `DEL`
and install a fresh tombstone at (approximately) the `DEL`'s own stamp — call it `T2`, with
`T1 < T2`. If a third write `W` arrives later stamped strictly between `T1` and `T2`, this node
compares it only against `T1` (the only thing it has) and accepts it, installing a live value; the
peer holding `T2` rejects the identical write as stale. The two nodes now disagree — one live
(this node, holding `W`), one tombstoned (the peer, holding `T2`) — for a write both should have
treated identically. **Nothing repairs this in steady-state streaming**: this node's own stream of
ordinary replicated writes never re-sends `T2`. The *next full sync* from the peer holding `T2`
does repair it, and by the ordinary mechanism, not a special case: `RdbLoader::
ApplyMergeTombstoneOnShard` runs `MergeAccepts(stored=W, incoming=T2)` for the incoming opcode-225
tombstone record, `T2` is strictly newer than `W`, so it wins and installs the tombstone here too —
both nodes converge to absent. The repair is contingent on timing, though: if the peer's own
`TombstoneGcStep` reaps `T2` (its TTL elapsed) before that full sync ever happens, the peer no
longer sends anything for this key at all (no live value, no tombstone), and this node's `W` stands
permanently — the delete is lost, not merely delayed. It is also contingent on nothing else landing
on `W` first: an unguarded delta applied to this node's `W` between now and that full sync commits
its own stamp verbatim if not older (D-23), which can push this node's stored stamp up to or past
`T2` -- `MergeAccepts` favors ties to the stored side, so equalling `T2` already loses, not only
exceeding it -- making the incoming tombstone lose the merge compare instead of winning it.

**A second cause, not just an absent key's skip.** An applied, guarded `DEL`/`GETDEL` of a key
whose TTL has already elapsed but which this node has not yet reaped hits the same symptom from a
different angle. The guard's own veto compares the author's stamp `X` against the key's LIVE,
still-unreaped stamp `S` (a pure side-table read that never itself triggers expiry) and passes it
(`X` is newer than `S`); only INSIDE the callback does `FindMutable` (`OpDelV2`/`OpGetDel`'s own
lookup) lazily reap the key, installing `ExpiryTombstoneFor(S)` — one origin_hash tick above `S`,
`src/server/mvcc.h` — as its tombstone (`ExpireIfNeeded` → `RecordExpiryBlocking` →
`CommitOwnTombstone`). Because the key is then already gone, `OpDelV2`'s own `IsValid` check skips
it exactly as the absent-key case above does: it is never added to `journal_args`, so the author's
own stamp `X` is never committed here at all — the stored tombstone is `ExpiryTombstoneFor(S)`,
not `X`. Since `X` can be arbitrarily larger than `ExpiryTombstoneFor(S)`, a third peer's write `W`
stamped strictly between them applies HERE (it beats the low tombstone) while it is correctly
dropped AT THE AUTHOR (whose own copy of the key was still live, non-expired, at command time, so
its local `DEL` committed the real `X` directly, no expiry involved) — diverging exactly as the
absent-key case does, repaired the same way (subject to the same D-23 caveat above) by the author's
next full sync, and lost the same way if that tombstone is GC'd first.

**How established:** static reading of `OpDelV2`'s per-key skip-before-`FindMutable` ordering and
of `ApplyMergeTombstoneOnShard`'s own `MergeAccepts` call; not reproduced with a live three-node
scenario. Pre-existing in the tombstone mechanism since P4-3 (the per-key LWW skip itself is new in
P4-4, but the underlying "a DEL of an absent key touches no tombstone" behavior is not); newly
documented here rather than fixed, since a real fix belongs with the rest of the
tombstone-lifecycle work. The second cause was identified alongside the change that made every
`kExpired` tombstone derive from the expired value's own stamp (`ExpiryTombstoneFor`, `mvcc.h`);
it exists regardless of that change (an applied delete's own stamp was always at risk of being
silently discarded by a mid-command lazy expiry this way), but that change is what makes the
resulting gap between the discarded `X` and the installed tombstone precisely `X -
ExpiryTombstoneFor(S)` rather than something already partly closed by a reap-time mint.

**Owner:** P4-5 (tombstone lifecycle). **From:** P4-4.

### D-21. `*STORE`'s `DEL` + add split can merge a stale result into a newer destination

**Where:** `SetFamily::OpAdd` (`src/server/set_family.cc:520-636`) is called with `overwrite=true`
UNCONDITIONALLY — regardless of whether the destination previously existed — by
`SINTERSTORE`/`SUNIONSTORE`/`SDIFFSTORE`'s destination write (`set_family.cc:1384,1487,1577`); its
non-empty-result branch journals `DEL key` (guarded, since `overwrite` is always true for these
callers) followed unconditionally by `SADD key <members...>` (unguarded) as two SEPARATE entries.
`ZSetFamily::OpAdd` (`src/server/zset_family.cc`, its `zparams.override` branch, ~line 2054) does
the identical thing for `ZUNIONSTORE`/`ZINTERSTORE` (`zset_family.cc:1619`), `ZRANGESTORE`
(`:1769`), and `ZDIFFSTORE` (`:2361`) — all three call sites also pass `override=true`
unconditionally. `GeoFamily`'s `GEORADIUS`/`GEORADIUSBYMEMBER` `STORE`/`STOREDIST` modes
(`geo_family.cc:651-657`) route through the SAME `ZSetFamily::OpAdd`, also with
`override=true, journal_update=true` unconditionally, and get the identical split. `DEL` is in the
guarded table; `SADD`/`ZADD` are delta-journaled RMW and are not.

On a guarded receiver whose own `key` is newer than the incoming author stamp, the `DEL` entry is
correctly dropped (it is stale) — but the `SADD`/`ZADD` entry that follows it in the SAME applied
command is unguarded and applies unconditionally, blindly adding the author's freshly-computed
members into whatever this receiver's own, untouched, newer destination value already was. The
result is neither the author's fresh set/zset (which the receiver's newer value should have kept)
nor a value either node ever actually held — a third, merged state manufactured by the split
itself. This is not permanent, though: the `SADD`/`ZADD` is an applied write whose own author
stamp is OLDER than the receiver's stored stamp `S`, so `FloorAppliedStamp` (`mvcc.h`) commits the
merged value one tick BELOW `S` (`{S.mvcc, S.origin_hash - 1}`), never at `S` itself or above. A
later full sync from any node still holding the clean value stamped exactly `S` therefore wins the
next merge compare and overwrites the corrupted merge. Nothing in steady-state streaming repairs
it on its own (an ordinary streamed write only ever compares against whatever is *currently*
stored, never specifically targets undoing this) — only a subsequent full sync from a clean-`S`
holder does, PROVIDED no other unguarded delta lands on this same key first: a delta applied after
this floor commits its OWN author's stamp verbatim on top of the floored one (D-23), which can
reach or pass `S` and leave the corrupted merge at an identical-or-newer stamp that full sync's own
tie-breaking treats as equally or more authoritative than the clean copy.

**How established:** static reading of `SetFamily::OpAdd`/`ZSetFamily::OpAdd`'s journaling
branches, every call site that passes `overwrite`/`override`, and the applied-write stamp floor
(`FloorAppliedStamp`, `mvcc.h`) that governs the merged value's eventual repair; not reproduced with
a live two-node divergence. Same shape as the `*STORE`-family empty-result case already guarded (a
destination that *becomes* absent journals a bare, guarded `DEL`) and as cross-shard
`SORT ... STORE`'s own `RESTORE ... REPLACE` result-journaling (D-13's sibling, already guarded) —
this is the non-empty-result case those two commands' own set/zset/geo equivalents never received
the same treatment for.

**Owner:** open. Fix path if wanted: journal the destination's result as state
(`RESTORE ... REPLACE`, one entry, guarded — the same treatment cross-shard `SORT ... STORE`
already gets) instead of a `DEL` + delta-add pair, for all nine affected commands
(`SINTERSTORE`/`SUNIONSTORE`/`SDIFFSTORE`, `ZUNIONSTORE`/`ZINTERSTORE`/`ZDIFFSTORE`/`ZRANGESTORE`,
`GEORADIUS`/`GEORADIUSBYMEMBER`). **From:** P4-4.

### D-22. An unguarded applied re-create after an expiry tombstone ties a third peer at `P`

**Where:** `FloorAppliedStamp` (`src/server/mvcc.h`, `src/server/mvcc.cc`) landing one origin_hash
tick *below* `stored`, applied to the specific case where `stored` is an expiry tombstone
`T = {P.Mvcc(), P.origin_hash + 1}` (`ExpiryTombstoneFor`'s own output, mvcc.h) and `incoming` is an
UNGUARDED applied write (a delta RMW — `INCR`/`APPEND`/… — never LWW-compared before applying)
whose own author stamp is older than `T`. `FloorAppliedStamp`'s `stored.origin_hash != 0` branch
computes `{T.Mvcc(), T.origin_hash - 1}`, which is exactly `{P.Mvcc(), P.origin_hash}` — `P`
itself, bit for bit, since `T` is *always* exactly one origin_hash tick above `P` by construction.

Consequence: this node's `k` is now live again, stamped exactly `P`, holding a value the delta RMW
manufactured from scratch (the key was absent going in — same "third, merged state neither node
ever actually held" shape as D-21's `*STORE` split, not `P`'s own original value). A third peer
that has not yet reaped its own still-live copy of `k` — plausible, since an expiry-caused DEL is
peer-suppressed and every node reaps on its own clock, not on a schedule replication drives — is
still holding that original value at that exact same stamp `P`. Whichever side of the next
comparison between them is evaluating keeps its own stored side (`MergeAccepts` ties favor stored),
so neither node's value is wrongly resurrected onto the other, but the two sides do not converge
either: this node's manufactured value and the third peer's original value both sit at stamp `P`
indefinitely, immune to each other's writes. Resolution arrives only when the third peer
independently reaps its own copy: its own tombstone is `{P.Mvcc(), P.origin_hash + 1}`, strictly
greater than this node's `P`-stamped value, so the next full sync between them converges — to
absent, since a tombstone is what wins — provided no unguarded delta lands on this node's own copy
of `k` first and pushes its stamp up to or past the third peer's tombstone (D-23); if one does, the
full sync's compare no longer favors the incoming tombstone.

**How established:** static derivation from `FloorAppliedStamp`'s documented one-tick-below
landing (mvcc.h) composed with `ExpiryTombstoneFor`'s documented one-tick-above landing; not
reproduced against a live three-node topology (see D-15's identical three-peer coverage gap).

**Owner:** unassigned; only reachable because a delta RMW (`INCR`/`APPEND`/…) is never guarded by
design for the re-creating write, independent of whether the link's streaming guard itself is on
or off (see the guarded-name vocabulary, `multimaster_lww.h`), and because a third peer can lag
behind this node's own reap — both already-accepted shapes elsewhere in this register (D-19,
D-21). **From:** P4-4.

### D-23. Any divergence on a key becomes an identical-stamp divergence at the next unguarded delta

**Where:** every unguarded applied write (a delta RMW — `INCR`/`APPEND`/`HSET`-style commands/
`PFADD`/… — never LWW-compared before applying, by design: see the guarded-vocabulary table,
`multimaster_lww.h`) goes through `FloorAppliedStamp` (`src/server/mvcc.h`, `mvcc.cc`) like any
other applied write. That function commits the incoming author stamp VERBATIM whenever it is NOT
older than the key's own stored stamp; the floor (one origin_hash tick below `stored`, see
`FloorAppliedStamp`'s own doc comment in `mvcc.h`) only fires for the opposite case, an incoming
stamp OLDER than `stored`
(D-21/D-22). So the stamps go identical specifically in the not-older case: whenever the delta's
own author stamp equals or exceeds the receiver's stored stamp, it is committed as-is.

Consequence: an unguarded delta applies to whatever value THIS node already holds for the key, then
commits the author's stamp verbatim to it — regardless of whether some OTHER node holds a
DIFFERENT value for the same key at an OLDER stamp. Direct form, no prior divergence needed: node A
runs `SET k 5` stamped `S`; node B independently runs `INCR k` (starting from `0`) stamped `I`, with
`I > S`. A's guarded `SET` reaches B: `I > S`, so B's guard correctly drops it (ties/older favor
stored) — B keeps `1@I`, its own `INCR` result. B's unguarded `INCR` reaches A: a delta is never
LWW-compared, so it applies to A's OWN stored value (`5`); `I` is not older than A's stored stamp
`S`, so `FloorAppliedStamp` commits it verbatim — A ends at `6@I`. The two nodes now sit at the SAME
stamp `I` with DIFFERENT values (`6` and `1`) — ties favor the stored side, so a full sync cannot
tell them apart either; only the next full-state write (`SET`/`DEL`/`RESTORE`, or one of the
TTL-changing commands above) heals it in general. The key's own expiry heals it too, but only when
both copies share the SAME absolute TTL deadline (deleting both sides at the same instant) — not
when the key has no TTL at all (this example's own `SET k 5`/`INCR k`, neither of which sets one),
or when the two copies' TTLs differ. Consequence for the floor text above
and D-21: their "heals at full sync" claims hold only up to the NEXT delta that lands on the same
key — the floor (or the full sync itself) fixes the value/stamp pairing at that instant, but a delta
applied afterward commits its own stamp on top whenever that stamp is not older than what is
currently stored, and can re-open the identical-stamp gap this entry describes.

**How established:** static derivation from `FloorAppliedStamp`'s not-older-than-`stored` verbatim
branch (`mvcc.cc`) composed with `MergeAccepts`'s tie-favors-stored rule (`mvcc.h`); not reproduced
against a live two-node divergence.

**Owner:** future work (CRDT-style deltas, or a per-field/per-key stamp finer than one MVCC stamp
per key, so a delta and a full-state write on the same key stop competing for the same stamp).
**From:** P4-4.

### D-24. A TTL change racing a concurrent delta clobbers the delta on one node

**Where:** `OpExpire`/`OpPersist` (`generic_family.cc`) and `CmdGetEx`/`FindKeyAndSetExpiry`
(`string_family.cc`), on an active node, ship the key's CURRENT full value alongside its TTL change
(see the full-state TTL paragraph, `docs/multi-master.md`) — a guarded write, so a receiver that
applies it OVERWRITES whatever value it locally holds, exactly like any other guarded `SET`.

Consequence: node A and node B both run `INCR k` (converging on the same count once both deltas
land on both sides), then A alone runs `EXPIRE k 60` before B's own `INCR` has reached it. A's
`EXPIRE` ships a guarded `SET k <A's own count> PXAT <abs>` — A's count at that instant, which does
NOT yet include B's still-in-flight `INCR`. This is the same shape as D-23 — a guarded full-state
write and an unguarded delta racing on the same key — specialized to the TTL-changing commands,
since those are the ones that turn an everyday `EXPIRE`/`PERSIST`/`GETEX` into a full-state,
guarded write. Which stamp ends up newer decides which of two different outcomes
results:

- **TTL-change-newer order: bounded.** When that guarded `SET` reaches B with a newer stamp than
  B's own `INCR`, it applies: B's own more-current count is overwritten wholesale by A's
  stale-relative-to-B count. One node ends up one count behind the other, with the TTL itself
  identical on both (an absolute deadline, carried verbatim) — but this is NOT permanent: every
  LATER TTL change on the same key ships the CURRENT value again (whatever it is by then), so the
  next `EXPIRE`/`PERSIST`/`GETEX`/`SET ... KEEPTTL` re-converges the two nodes' counts, and once
  both copies carry the same absolute deadline, they expire together.
- **Delta-newer order: an identical-stamp divergence, not new here.** If instead B's `INCR`'s own
  stamp is NEWER than A's `EXPIRE`-derived `SET`'s stamp, A's guarded `SET` loses at B (B's guard
  compares it against B's own newer, `INCR`-derived stamp and correctly drops it as stale) while
  B's `INCR`, unguarded, applies to A verbatim (`FloorAppliedStamp` commits it as-is: it is not
  older than A's own stored stamp) — an unguarded delta is never LWW-compared before applying,
  regardless of which stamp is newer. Both nodes now sit at the SAME stamp with the SAME count, but
  only A's copy carries the TTL — B's does not, since `INCR` never touches TTL. Expiry deletes are
  never forwarded on peer links (see D-22), so A's key eventually vanishes locally while B's
  identically-stamped, TTL-less copy lives on: the divergence persists until the next full-state
  write touches this key (or one of D-23's own resolution conditions). This is D-23's general case,
  not a new exposure that shipping the TTL change as full state introduced — any delta racing any
  guarded full-state write on the same key already had this shape before full-state TTL
  journaling; that journaling inherits it rather than causing it.
- **Operator rule:** avoid mixing cross-node deltas with concurrent TTL changes (or `SET`s) on the
  same key from a different node; each is safe alone, only the combination has this gap.

**How established:** static derivation from the full-state TTL design composed with
`MergeAccepts`'s guarded-write-wins-on-newer-stamp rule (`mvcc.h`) and D-23's own delta-verbatim-
commit mechanism; not reproduced against a live two-node divergence.

**Owner:** the TTL-change-newer order is self-healing and needs no separate fix. The delta-newer
order IS D-23 — not a distinct defect with its own owner. A per-key TTL stamp, decoupled from the
value's own stamp, would close BOTH of D-24's own orders (a TTL change would then never need to
carry the value at all), but it is narrower than D-23's own general fix: it does nothing for a
plain `SET`-vs-`INCR` race that carries no TTL change at all, which still needs D-23's own broader
fix (a per-field/per-key stamp finer than one MVCC stamp per key, or CRDT-style deltas). **From:**
P4-4.

### D-25. Upstream `PFMERGE` writes a phantom destination on every participating shard

**Where:** `HllFamily::PFMergeInternal`'s destination-write callback, `set_cb` (`hll_family.cc`).
`tx->Execute(set_cb, true)` runs `set_cb` on EVERY shard the transaction touches — the destination
key's own shard plus every source key's shard — not just the destination's. The callback itself
never checks which shard it is running on before calling `db_slice.AddOrFind(t->GetDbContext(),
key, OBJ_STRING)` on the closure-captured destination `key`: on a shard that does not own that key,
`AddOrFind` still succeeds (a `DashTable` has no notion of "the wrong shard" for a key it is simply
asked to insert), creating an independent, phantom entry named identically to the real destination,
living only in that shard's own table. A live probe against a non-active binary confirmed it:
`DBSIZE` read back one higher than the number of distinct keys actually written, and `SCAN`
returned the destination's name twice.

Effects: on a non-active node, `DBSIZE`/`SCAN`/`KEYS` all double-count the destination for as long
as the phantom's shard is never asked to overwrite or delete it independently. `DEL dest`/`PFADD
dest ...` never reach it: ordinary command routing hashes `dest` to exactly one (the real,
owning) shard, and the phantom lives on a DIFFERENT shard purely because `set_cb` ran there
without checking — there is no client-issued command that targets the phantom's shard by that
same key name, so nothing ever touches it independently and it is permanently orphaned, not merely
inert for now. It is a genuine entry in that shard's own dense table, so a snapshot taken on that
node (a periodic `SAVE`/`BGSAVE`, or the RDB a full sync sends downstream) serializes it exactly
like any other live key, on that same (wrong) shard's own RDB stream/file; reloading that snapshot
(a restart, or a downstream node's own full sync FROM this node) reinstalls the phantom on the
same shard again, with its ORIGINAL value and no TTL at all (the phantom's bare `AddOrFind`+
`SetString` write never runs any TTL-preserving logic, unlike the owning shard's own write). If the
real destination is later deleted or naturally expires, this orphaned, TTL-less phantom survives
independently and keeps resurrecting across restarts/re-syncs, indistinguishable from a live key to
`DBSIZE`/`SCAN`/`KEYS` on that shard, with no TTL to ever reap it. Under multi-master before this
fix, the phantom was additionally armed, stamped by ITS OWN shard's clock, and
journaled as its own separate `SET dest v` entry racing the owning shard's, letting whichever of the
two arrived at a guarded receiver second silently overwrite whatever the first one carried
(including a TTL the owning shard's own write correctly preserved) — see the TTL-preserving
full-state fix above, and `PfmergeAcrossShardsJournalsOnlyFromOwningShard`/
`NonActivePfmergeAcrossShardsKeepsUpstreamPhantomShape` (`multi_master_test.cc`) for the pins.

**How established:** static reading of `PFMergeInternal`'s `set_cb`, confirmed live against a
non-active binary (`DBSIZE`/`SCAN` both showed the duplicate) and against an active one (the
cross-shard journal duplication, closed by the `dest_shard` filter on `IsActiveReplica()`
links only). `BITOP`'s own `store_cb` (`bitops_family.cc`) already filtered to its `dest_shard`
and never had this defect; `PFMERGE`'s `set_cb` did not. The snapshot-serialization, DEL/PFADD-
never-reach-it, and reload-resurrection consequences above are static derivations from how
`SliceSnapshot` iterates a shard's own dense table and how key routing hashes a command to exactly
one shard — not reproduced against a live save/reload cycle.

**Under multi-master:** fixed on active nodes — `set_cb` now skips every shard that does not own the
destination when `IsActiveReplica()`. A non-active node keeps upstream's own phantom-writing shape
exactly, unchanged, per the byte-identity invariant.

**Owner:** candidate upstream report (the defect is upstream's own; drakeydb only fixes it on the
active-node path, since a non-active node must stay byte-identical to upstream, phantom copies
included). **From:** P4-4.

### D-26. A BUSYKEY-rejected `RESTORE` leaks one `mvcc_unstamped_writes`

**Where:** `src/server/generic_family.cc`, `OpRestore` — `db_slice.FindMutable(op_args.db_cntx,
key)` runs (and, on an active node, arms an MVCC slot for `key` via the normal touch-on-open path)
BEFORE the `Replace()`/`KEY_EXISTS` check. When the key already exists and `REPLACE` was not
given, `OpRestore` returns `OpStatus::KEY_EXISTS` immediately, without ever deleting the key or
reaching any journal call — so the arm `FindMutable` placed is never committed. At this
transaction's own `EndOfWriteEpoch`, that still-armed slot rolls back uncommitted, bumping
`mvcc_unstamped_writes` by exactly one even though nothing about the key changed.

This is the same benign shape `docs/multi-master.md`'s Observability section already documents for
a failed conditional delete or `SET ... NX` (`FindMutable`-style opens arm the key
unconditionally, regardless of whether the write that follows actually happens) — pre-existing,
not new; `RESTORE` without `REPLACE` against an existing key is simply another member of that
same class, previously undocumented.

**How established:** static reading of `OpRestore`'s own control flow; not reproduced against a
live `DEBUG MVCC` counter read (the mechanism is identical to the already-documented and tested
`SET ... NX`/`DELEX ... IFEQ` cases, so a live repro would be expected to show the same +1).

**Status:** not filed (Part 2, since the arming-before-checking shape is drakeydb's own MVCC
addition, not an upstream defect). **Owner:** none needed — benign, already covered by the
existing operator-facing note in `docs/multi-master.md`. **From:** P4-4.
