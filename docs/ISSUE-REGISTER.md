# Issue register

A running list of defects and deferred work found while building drakeydb, for things that have
no home in the phase currently being worked on. Two parts:

- **Part 1 — Upstream Dragonfly bugs.** Reproduce with `--active_replica` off, so they are not
  ours. They belong in `dragonflydb/dragonfly` issues; until filed they live here so the next
  person does not rediscover them.
- **Part 2 — drakeydb deferred work.** Ours, deliberately not fixed yet, each with the reason and
  the phase that should close it.

Add entries as they are found. Delete an entry only when it is filed upstream (Part 1, with the
issue link recorded), landed (Part 2), or withdrawn (either part; keep a withdrawn entry for one
phase as a record first). Every entry states how it was established, so a reader can tell a
live-proven defect from a static argument.

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

**How established:** found by static reading while diagnosing a different, related hazard in the
same function (the merge-LWW compare-before-mutate race that motivated switching this fork's own
loader from `AddOrUpdate` to `AddOrFind` plus an authoritative post-yield recheck, P4-3 Task 4).
Not independently reproduced with a race harness; the yield and the missing re-check are both
directly visible in source.

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

### U-8. `GEORADIUS`/`GEORADIUSBYMEMBER`'s `STORE` destination never replicates -- withdrawn (not a bug)

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

**Status (2026-10-03): withdrawn — not a bug.** The static argument above missed that the
`STORE` callback sets `zparams.journal_update = true` (`geo_family.cc:654`) and writes through
`ZSetFamily::OpAdd`, which hand-journals the destination itself: a bare `DEL` for an empty result
(`zset_family.cc:1963`) and, for a non-empty one, `DEL` then `ZADD` (`:2053` opens the branch;
`DEL` is recorded at `:2055`, `ZADD` at `:2072`). What does remain true of these commands is the
`DEL` + `ZADD` split that D-21 registers — a different defect (a stale result merging into a newer
destination), not a failure to replicate. Live re-test (P7-0 Task 0.1, debug build of `c60dfdb`,
`--proactor_threads 4` → 4 shards): after stable sync was confirmed with a marker key, 8
`GEORADIUS … STORE` and 8 `GEORADIUSBYMEMBER … STOREDIST` destinations written on the master all
reached a plain replica byte-for-byte (16/16, `ZRANGE … WITHSCORES` digests equal). Kept here for
one phase as a record, then delete.

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
active-replica master's or a real Redis master's stream, replayed through
`Replica::ConsumeRedisStream`'s OWN dispatch, which builds its own bare `ConnectionContext{nullptr,
{}}` (`replica.cc`) rather than going through `JournalExecutor` at all (that `ConnectionContext` is
explicitly separate from `JournalExecutor`'s own, per that constructor's own comment) — whenever
that stream replicates a single-shard `EVAL`/`EVALSHA` whose key does not hash to the applying
thread's own shard. `JournalExecutor`'s applied commands (the DFLY-protocol peer/stable-sync path)
share the identical hazard independently, via their own null `owner_`, so it is not exclusive to
classic links either way, but a classic upstream master is the most direct route to a client-issued
`EVAL` reaching this exact branch via replication.

**How established:** static reading of `EvalInternal`'s migration branch composed with
`JournalExecutor`'s constructor; reproduced deterministically by P7-0 Task 0.5 (a `JournalExecutor`
on thread 0 applying `EVAL "return redis.call('SET', KEYS[1], 'v')" 1 <key on another shard>`
raised SIGSEGV in `Connection::RequestAsyncMigration` 3/3 on the unfixed build).

**Status (2026-10-03): fixed in this fork** (P7-0 Task 0.5): the migration branch also requires
`conn_cntx->conn() != nullptr` — migration is a latency optimisation, so skipping it is
semantically neutral. Regression test `DflyEngineTest.EvalReplicatedApplyNoConnNoCrash`
(`src/server/dragonfly_test.cc`). Still not filed upstream.

### U-10. `VerifyCommandState`'s `TAKEN_OVER` branch dereferences a null `conn()` on a replicated apply

**Where:** `src/server/main_service.cc`, `Service::VerifyCommandState`, `case
GlobalState::TAKEN_OVER:` — `dfly_cntx.conn()->IsPrivileged() || ...`. The restricted-command check
a few lines above guards the same call with `dfly_cntx.conn() != nullptr` ("no connection owner
means the command is internal, therefore always permitted"); this branch does not.

Every replicated apply dispatches through `Service::DispatchCommand` with a context whose `conn()`
is `nullptr` (`JournalExecutor`; `Replica::ConsumeRedisStream`'s own bare `ConnectionContext`). A
node in `TAKEN_OVER` that is still applying its own master's stream therefore crashes on the first
applied command. `TAKEN_OVER` is set by `DFLY TAKEOVER` (`dflycmd.cc`) on the node a replica takes
over, and a node that is itself a replica is only possible with cascaded replication
(`--experimental_cascaded_partial_sync`, off by default; `ServerFamily::ReplConf` refuses to
replicate a replica otherwise) — so reachable on any apply path (DFLY stable sync and a classic
link alike) of a cascaded node whose upstream keeps writing during a `REPLTAKEOVER`.

**How established:** live and in-process. Chain `master -> r1 -> r2`, `--proactor_threads 4
--experimental_cascaded_partial_sync`, ~1 s of pipelined `APPEND`s on `master`, then `REPLTAKEOVER 10`
on `r2`: `r1` died with SIGSEGV 4/4 on the unfixed build (stack `DflyShardReplica::StableSyncDflyReadFb
-> ExecuteTx -> JournalExecutor::Execute -> Service::DispatchCommand`) and exited 0 with
`REPLTAKEOVER` answering `OK` 5/5 after the fix. Deterministically, `DflyEngineTest.
ReplicatedApplyDuringTakeoverNoCrash` crashes at `main_service.cc:1430` (gdb) without it.

**Status (2026-10-03): fixed in this fork** (P7-0 Task 0.5, same commit as U-9): a context with no
connection is never refused by the `TAKEN_OVER` gate, as at the restricted-command check — refusing
it would drop the upstream's writes on a takeover that then fails back to `ACTIVE`. Still not
filed upstream.

**The cost of "allow":** `WaitReplicaFlowToCatchup` (`dflycmd.cc`) waits until the taking-over
replica's acked LSN reaches `journal::GetLsn()`, and a cascaded node that keeps applying its
master's stream keeps appending to its own journal, so that target moves. Under sustained upstream
writes a `REPLTAKEOVER` whose old master is a cascaded node can therefore run out its timeout and
answer `Takeover failed!` (`DflyCmd::TakeOver`). That follows from the code; it was not observed:
the probe above kept writing throughout the takeover and `REPLTAKEOVER` still answered `OK` 5/5.
Refusing the applies would avoid that wait, since nothing new reaches the journal, and is worse:
a refused apply is dropped while the stream moves on. `Replica::ConsumeRedisStream` advances
`repl_offs_` after every dispatch whatever its result, and the DFLY stable-sync path logs a `DFATAL`
for a failed entry only while the node is `ACTIVE`. Once the takeover then fails back to `ACTIVE`,
the node has lost writes its upstream believes were applied, and nothing re-sends them. A takeover
that times out is loud and can be retried.

### U-11. A classic full sync aborts the replica when stream bytes arrive with the end of the RDB

**Where:** `Replica::InitiatePSync` after `RdbLoader::Load` — `CHECK_EQ(0u,
loader.Leftover().size())`, `CHECK_EQ(snapshot_size, loader.bytes_read())` and
`CHECK(ps.UnusedPrefix().empty())` on a `$<len>` sync; `CHECK(chained.UnusedPrefix().empty())` after
the `$EOF:` token; `RdbLoader::Load`'s first `ReadAtLeast(bytes, 9)` ignores `source_limit_`.

A disk-based classic master (KeyDB's default; Redis with `repl-diskless-sync no`) flushes the writes
it buffered during its BGSAVE right behind the file. When the RDB's end lands in the loader's first
(16 KB) read, that unclamped read swallows stream bytes and the `Leftover()` CHECK aborts the
replica; even without the abort those writes and their offset were dropped. The neighbouring CHECKs
abort on malformed tails, and an RDB longer than `$<len>` trips the helio `io.cc:143` DCHECK in a
debug build.

**How established:** live on the unmodified main build — plain KeyDB v6.3.4, disk-based sync, an
`INCR` loop during the attach: `replica.cc:831] Check failed: 0u == loader.Leftover().size() (0 vs.
2507)` in 4 of 8 runs. With a scripted master, deterministically `(0 vs. 46)` (disk) and
`replica.cc:829 Check failed: chained.UnusedPrefix().empty()` (diskless); also `replica.cc:1910
Check failed: kRdbEofMarkSize == token.size()` and `io.cc:143 … (0 vs. 8)`.

**Status (2026-10-03): fixed in this fork** (P7-0 Task 0.6): the loader's first read honors
the source limit, the bytes behind a correct full sync go to `ConsumeRedisStream` and into
`repl_offs_`, and every other tail disagreement is an error that reconnects. Tests
`RdbTest.LoaderFirstReadHonorsTheSourceLimit`,
`RdbTest.LoaderSourceLimitShorterThanTheRdbIsAnError`,
`keydb_onboarding_test.py::test_psync_{stream_bytes_behind_full_sync_are_applied,
full_sync_tail_mismatch_does_not_abort_replica,bad_eof_token_size_does_not_abort_replica}`. Not
filed upstream.

**Retry behavior, not changed:** a persistently malformed master is retried every ~0.5 s with no
backoff (`MainReplicationFb`'s reconnect loop sleeps 500 ms), where it used to abort the process.
Each attempt logs an `ERROR` and a `WARNING`; a plain replica `FlushAll`s its dataset per attempt
(the flush precedes the load, so it is empty again each time); a peer-mode node takes exclusive
`LOADING` and re-merges per attempt. That is strictly better than the abort and matches upstream's
pattern for its other full-sync failures. Follow-up candidate: rate-limited logging and a reconnect
backoff for a master that fails the same way repeatedly.

**Related, not changed:** a `$0` header (`+FULLRESYNC <id> <offset>` then `$0`) is not a full sync
here. `InitiatePSync`'s `if (snapshot_size || token != nullptr)` (`replica.cc:759`) sends it to the
else branch, "Re-established sync with Redis master", which is the partial-resync branch: nothing is
flushed, so stale data stays, and the offset from the `+FULLRESYNC` line has already been adopted.
Pre-existing, upstream's; P7-3 (classic partial PSYNC) takes over that branch and must tell a `$0`
full sync from a partial resync.

### U-12. `Service::DispatchCommand` closes a null connection when a handler throws on a replicated apply

**Where:** `src/server/main_service.cc`, `Service::DispatchCommand`, after `InvokeCmd` —
`cmd_cntx->SendError("Internal Error"); dfly_cntx->conn()->MarkForClose();`.

`InvokeCmd` catches a `std::exception` thrown by a command handler, logs `Internal error, system
probably unstable` and returns `DispatchResult::ERROR`, the only way to reach that block. A
replicated apply (`JournalExecutor`; `Replica::ConsumeRedisStream`'s own context) has no connection
(see U-9, U-10), so a handler that throws while applying turned an already-logged internal error
into a SIGSEGV in `Connection::MarkForClose`.

**How established:** deterministically in-process: `DflyEngineTest.
ReplicatedApplyHandlerThrowNoConnNoCrash` replaces `ECHO`'s handler with one that throws
`std::runtime_error` and applies `ECHO x` through a `JournalExecutor`; without the guard it dies
with SIGSEGV in `facade::Connection::MarkForClose <- Service::DispatchCommand`. No production
command throws deterministically, so a live trigger was not reproduced: it needs a `std::exception`
thrown on a handler's coordinator side (a shard callback's `bad_alloc` becomes `OUT_OF_MEMORY` and
its other exceptions abort, both in `Transaction::RunCallback`).

**Status (2026-10-03): fixed in this fork** (P7-0 review fix round): the close requires `conn() !=
nullptr`. The failed command is still dropped, logged and not retried, and the replication link
stays up, so a replica that hits this diverges silently on that key. Not filed upstream.

### U-13. An empty command name in a classic stream aborts the replica

**Where:** `Replica::ConsumeRedisStream` (`src/server/replica.cc`), the debug dump of a command
whose name starts with `\r`: `LastResponseArgs()[0].GetBuf()[0] == '\r'`, evaluated for every
command that is not `MULTI`/`EXEC`, before the check that the name has a first byte.

The stream bytes `*1\r\n$0\r\n\r\n` are a valid RESP array of one empty bulk string: a command with
an empty name. The parser accepts it and the read of its first byte is out of bounds: SIGABRT in a
debug build (`absl/types/span.h:335` `assert(false && "i < size()")`), a 1-byte out-of-bounds read
in a release one. Any classic master, plain Redis and Valkey included, can send it in its
replication stream, so it is reachable from a malformed or hostile master after any valid sync.

**How established:** a scripted master (an adversarial pass over P7-0): a valid diskless or disk
full sync, then `*1\r\n$0\r\n\r\n`, then `SET a 1` aborts the debug replica with SIGABRT in
`Span<>::operator[]` called from `Replica::ConsumeRedisStream`.

**Status (2026-10-04): fixed in this fork** (P7-0): the dump requires a non-empty name, so the
empty name is dispatched as the unknown command it is (dropped, counted in `unknown_cmds`, its bytes
counted into `repl_offs_` exactly) and the commands after it apply. Tests
`keydb_onboarding_test.py::test_classic_stream_empty_command_name_does_not_abort` (the
`diskless_later_write`, `disk_later_write` and `disk_behind_rdb` cases). Pre-existing in upstream
Dragonfly; not filed upstream. On the byte-identity exception list (spec, item 2).

### U-14. A command whose name is an array in a classic stream aborts the replica

**Where:** `Replica::ConsumeRedisStream` (`src/server/replica.cc`): `auto cmd =
last_args[0].GetView();`, reached for every command that is not an RREPLAY envelope.

The stream bytes `*0\r\n` and `*-1\r\n` are valid RESP: an empty array and a nil array. The
`RedisParser` in server mode accepts both and yields a one-element vector whose only element is an
`ARRAY` (`*0`) or a `NIL_ARRAY` (`*-1`), not a string. `RespExpr::GetView()` is `std::get<Buffer>`
on that element, which throws `std::bad_variant_access`; nothing on the replication fiber catches
it, so the process terminates (`std::terminate`; observed in a debug build, and nothing on the path
depends on the build type). Any classic master, plain Redis and Valkey included, can send it in its
replication stream, so it is reachable from a malformed or hostile master after any valid sync. Same
family as U-13 (a stream command whose name is not a readable string), found by the P7-1 review.

**How established:** a scripted master (the P7-1 review's probe): a valid diskless full sync, then
`*0\r\n` (or `*-1\r\n`), then `SET after 1` aborts the debug replica with SIGABRT,
`std::__throw_bad_variant_access <- RespExpr::GetView <- Replica::ConsumeRedisStream`. Inside an
RREPLAY envelope the same bytes as the inner command took the same abort in `ClassicApplier::
ApplyCommand` until the applier required a string name (P7-1 fix round); that half is ours, not
upstream's.

**Status (2026-10-04): fixed in this fork** (P7-1 review fix round): such a command is skipped, like
`MULTI`/`EXEC`, with a rate-limited warning (`Skipping a command without a name from <master>`), its
bytes counted into `repl_offs_` exactly (immediately, or with the batch ahead of it when one is
queued) and the commands around it applied. The guard is one `// drakeydb: U-14` hunk ahead of the
queuing branch. A nil *string* name (`*1\r\n$-1\r\n`) does not crash (it reads as the empty name)
and is left to U-13's path, an unknown command. Tests
`keydb_onboarding_test.py::test_classic_stream_command_with_an_array_for_a_name_does_not_abort`
(`empty_array`, `nil_array`, `behind_a_queued_command`). Pre-existing in upstream Dragonfly; not
filed upstream. On the byte-identity exception list (spec, item 2).

### U-15. A connection command in a classic stream dereferences a null `conn()`

**Where:** `ServerFamily::Info` (`src/server/server_family.cc`): `cmd_cntx->conn()->IsPrivileged()`
and `->GetTlsCertInfo()`, the first thing it does with the connection once it has collected the
metrics.

`Replica::ConsumeRedisStream` builds its apply context as `ConnectionContext{nullptr, {}}` (U-9,
U-10, U-12 are the same context) and dispatches whatever the master streams through
`Service::DispatchCommand` or the squasher. Nothing on that path refuses a command that is about the
client's own connection: `VerifyCommandState` takes a context without an owner for an internal one
and permits everything, and ACL validation is skipped. So `INFO` (read-only, so the squasher runs it
standalone: `ServerFamily::Info <- CommandId::Invoke <- MultiCommandSquasher::ExecuteStandalone`)
kills the replica with SIGSEGV. Any classic master, plain Redis and Valkey included, can put
`*1\r\n$4\r\nINFO\r\n` in its stream, so it is reachable from a malformed or hostile master after
any valid sync, raw or inside an RREPLAY envelope (`INFO` is a known command, so it passes the
unknown-command check). Found by the P7-1 Task 1.3 review (O-1), whose probe was a scripted master:
a valid full sync, then `SET a 1`, `INFO`, `SET b 2`.

The same context reaches every handler that dereferences `conn()` without a check. The audit
(`task-1.3-report.md`, "U-15 audit") found, besides `INFO`: `CLIENT SETNAME`, `GETNAME`, `INFO`,
`ID` and `KILL`, `AUTH`, `HELLO`, `REPLCONF` (its `capa dragonfly` and `listening-port` options, on
a node that is a master: a peer-mode node, or with `--experimental_cascaded_partial_sync`; a plain
replica refuses `REPLCONF` before it gets there), `QUIT`, `DFLY THREAD <n>`, and two that do not
fail at the command: `MONITOR` leaves a null connection in the monitor list, which the next command
of any client dereferences, and `SUBSCRIBE`/`SSUBSCRIBE`/`PSUBSCRIBE` register the stream's context
in the channel store (a context that is gone when the link ends), which the next client's `PUBLISH`
dereferences.

The re-review of that fix found a third of the second kind, `WATCH`. `Service::Watch` registers a
pointer to `exec_info.watched_dirty` of its context in each shard's watched-key table
(`DbSlice::RegisterWatchedKey`), and only the connection's close, `UNWATCH`, `EXEC` or `RESET`
unregisters it. On the classic stream that context is a stack local of `Replica::ConsumeRedisStream`,
so once the stream ends, a write to the key (`DbSlice::PostUpdate`) or a `FLUSHDB`
(`InvalidateDbWatches`) stores through a dangling pointer. A debug build does not notice the store
(it lands in a dead fiber stack): the reviewer's `watch_probe.py`, run again for this fix on the
build without the guard, left the process up through `REPLICAOF NO ONE`, a `SET` of the key and a
`FLUSHALL`, raw and in an envelope. It is silent memory corruption, not a crash, which is why its
test reads the registration itself.

**What the guards also cover, and what they change besides the crash.**

- A script. `redis.call` runs its command on the context of the `EVAL` (`CallFromScript`,
  `main_service.cc`), which in a classic stream has no connection either, and `INFO`, `HELLO` and
  `QUIT` are not `CO::NOSCRIPT`. The re-review probed `EVAL "return redis.call('INFO')" 0`, raw and
  in an envelope (`eval_probe.py`): the replica survives, because the guards sit in the handlers,
  and the script fails with an error that carries `No connection`. With the `Info` and `Hello`
  guards removed the same scripts kill the replica with SIGSEGV (this follow-up's falsification).
- The other contexts without a connection, `JournalExecutor` (a Dragonfly master's stream) and the
  RDB-load search-aux path (`LoadSearchCommandFromAux`, a fixed `FT.CREATE` or `FT.SYNUPDATE`), run
  on the same kind of context. The aux command name is not the wire's to choose, so nothing there
  reaches a guarded handler today; the guards are what would keep it safe.
- `REPLCONF` is guarded at the top of its handler, so every variant now replies `No connection`,
  including those that never dereferenced the connection (`GETACK`, `ACK`, `UUID`, `PEER`, ...).
  `REPLCONF GETACK *` is real stock traffic (a master asking for an ACK). What changes is the text
  of a reply the stream discards: `Replicating a replica is unsupported` on a plain replica,
  `syntax error` where the handler got past that refusal. Where it did, which is where `IsMaster()`
  is true (a peer-mode node) or `--experimental_cascaded_partial_sync` is on for a non-active node,
  every `GETACK` also logged `Error in receiving command, num args: 2` at ERROR, from the generic
  error callback; that line is gone. The effect is log-only.
- `CLIENT` has one guard, at the top of `ServerFamily::Client`, for every subcommand (it replaced
  five per-subcommand guards). It therefore also refuses the subcommands that reached their
  handlers without a crash (`LIST`, `PAUSE`, `UNPAUSE`, `TRACKING`, `CACHING`, `MIGRATE`, `HELP`)
  and turns the syntax error of a malformed or unknown one into `No connection`. No stock master
  propagates `CLIENT`, and it is `CO::NOSCRIPT`, so only a hostile or broken master can send it.
  `CLIENT SETINFO` keeps the guard upstream gave it.

**How established:** the review's probe, and two throwaway probes as a gtest (each case in its own
process, on the apply context of the stream, before the fix): 47 commands inside envelopes, then 31
dispatched raw, which also reaches what an envelope skips, such as `REPLCONF`. `INFO`, the five
`CLIENT` subcommands, `AUTH`, `HELLO`, `QUIT`, `REPLCONF listening-port|capa` and `DFLY THREAD 1`
die with SIGSEGV; `MONITOR` and `SUBSCRIBE` die on the next client command (`DispatchMonitor`, the
channel store's `Borrow()`); every other probed command (`CLIENT
LIST`, `PAUSE`, `TRACKING`, `CACHING` and `MIGRATE` among them) did not. The re-review's probes
(`watch_probe.py`, `eval_probe.py`) found `WATCH` and the script path; `WATCH` is pinned by the
registration it leaves in `DbTable::watched_keys`, which the test counts across every shard and db.

**Status (2026-10-04): fixed in this fork** (P7-1 review fix round, owner decision 26; `WATCH` and
the `CLIENT` consolidation in the re-review follow-up): each of the handlers above replies `No
connection` (the error `CLIENT SETINFO` already gives) when its context has no connection, and
`QUIT` replies `OK` and has nothing to close. The failed command is dropped and, inside an envelope,
counted in `classic_apply_errors`; the link stays up and the commands around it apply. Fourteen
`// drakeydb: U-15` guards, over a shared `ReplyIfNoConnection` in `server_family.cc`: five in
`server_family.cc` (`ServerFamily::Client`, once for every subcommand, then `Auth`, `Info`, `Hello`
and `ReplConf`), five in `main_service.cc` (`Quit`, `Monitor`, `Subscribe`, `PSubscribe`, `Watch`),
two in `dflycmd.cc` (`DFLY THREAD`, `DFLY FLOW`) and two in `cluster_family.cc` (emulated `CLUSTER`
and `DFLYMIGRATE FLOW`, both below). The last three files are beyond what owner decision 26 named.
The first fix had fourteen guards: the `CLIENT` consolidation removed the five per-subcommand ones
and added one. Tests
`ClassicNoConnectionTest.*` (one case per guarded handler, plus `Watch`, three `EVAL` cases,
`REPLCONF GETACK` and the `CLIENT` subcommands that never crashed; `classic_replay_test.cc`),
`ClassicApplyFamilyTest.ReplicatedMonitorAndSubscribeLeaveNothingForClientsToTripOver`,
`.ReplicatedWatchLeavesNoRegistrationBehind`, `.InfoInAnEnvelopeIsAnApplyErrorNotACrash` and
`.EvalOfAConnectionCommandInAnEnvelopeIsAnApplyErrorNotACrash`, and
`keydb_onboarding_test.py::test_classic_stream_info_command_does_not_abort` and
`::test_classic_stream_eval_of_a_connection_command_does_not_abort` (each `raw`, `in_envelope`).
Pre-existing in upstream Dragonfly; not filed upstream. On the byte-identity exception list (spec,
item 2).

**Emulated `CLUSTER` (P7-1 close, 2026-10-05, adversarial finding I3).** Under
`--cluster_mode=emulated`, `CLUSTER INFO`, `SLOTS`, `NODES` and `SHARDS` answer with the address the
client connected to (`ClusterFamily::GetEmulatedShardInfo`, via `GetShardInfos`), and on the apply
context all four die with SIGSEGV in `facade::Connection::LocalBindAddress`, raw, inside an envelope,
and as `EVAL "return redis.call('CLUSTER','INFO')"` (`CLUSTER` is script-callable). The adversarial
pass reproduced them (`targeted_cluster.jsonl`); the earlier text here called them "not probed" and
left out `CLUSTER INFO`. The narrowest guard closes them: `ClusterFamily::Cluster` replies `No
connection` for those four subcommands when `IsClusterEmulated()` and `conn() == nullptr`, ahead of
the dispatch on the subcommand. `HELP`, `MYID`, `KEYSLOT`, an unknown subcommand and every other
cluster mode (answered from the config) are untouched, and a real client always has a connection.
Tests `ClassicNoConnectionEmulatedClusterTest` instantiation `I3` (`ClusterInfo`, `ClusterSlots`,
`ClusterNodes`, `ClusterShards`, `EvalClusterInfo`), the control
`ClassicEmulatedClusterTest.ClientsAndConnectionlessSubcommandsStillAnswer`, and
`keydb_onboarding_test.py::test_classic_stream_emulated_cluster_query_does_not_abort` (8 cases).
Falsified: without the guard each gtest case dies with SIGSEGV in `Connection::LocalBindAddress`
(one process each) and every pytest case loses the replica with the same frame; with the condition
flipped to `conn() != nullptr` the control fails (a real client's `CLUSTER INFO` gets `ERR No
connection`).

**Hidden commands (P7-1 close, 2026-10-05, whole-branch re-review finding I-1).** The adversarial
pass listed the commands to fuzz through `COMMAND`, which leaves out every `CO::HIDDEN` one
(`Service::Command`), so five were never sent: `DFLY`, `DFLYCLUSTER`, `DFLYMIGRATE`, `GAT` and
`_XGROUP_HELP` (a registry dump has no others; a `--command_alias` clone is hidden too and shares
its source's handler). One of them was a further U-15: `DFLYMIGRATE FLOW <id> <shard>`
(`ClusterFamily::DflyMigrateFlow`) names the connection it arrives on (`conn()->SetName`) before it
looks up the migration, and `DFLYMIGRATE` is registered whatever the cluster mode, so the stream of
any classic master, raw or in an envelope, killed the replica with SIGSEGV in `Connection::SetName`.
An earlier version of this entry named `DFLYCLUSTER FLOW` and said "only with cluster mode on": that
command has no `FLOW` (it answers `Cluster is disabled`), so the adversarial pass's probe of it
could not have found this. `DFLYMIGRATE FLOW` goes on to `Migrate()` the connection and keep its
`socket()`, so the guard is the first statement of `DflyMigrateFlow`; `INIT` and `ACK` never read
the connection.

**The sweep.** Each hidden command and each subcommand its handler dispatches on, with junk and with
plausible arguments, plus `ROLE`, `DEBUG REPLICA PAUSE|RESUME|OFFSET`, `DEBUG REPLDIAG` and
`SHUTDOWN`: 70 cases (`DFLY` 29, `DFLYCLUSTER` 16, `DFLYMIGRATE` 15, `GAT` 2, `_XGROUP_HELP` 1, the
rest 7), raw and inside an envelope, one fresh process per case, streamed by a scripted active-KeyDB
master with a probe write behind each, with `--cluster_mode` unset, `emulated`, and `yes` (a config
pushed first): 420 runs per build. The outcome is survived (the probe applied, the settled ACK
offset exact, one connection, link up, role, keys and cluster nodes unchanged), crash, stall or side
effect. Before the fixes (`ac35f61`) 386 survived and 34 did not: 12 are `SHUTDOWN` (below); the
other 22 are three crashes. `DFLYMIGRATE FLOW x 0` and with a shard number that parses (`x 99999`):
SIGSEGV in `Connection::SetName`, 12 runs, every mode. `DFLYMIGRATE ACK x 1`: SIGSEGV in
`ClusterConfig::GetIncomingMigrations`, 4 runs (every mode but `yes`, where a config exists). A bare
`DFLYMIGRATE`: SIGABRT, an `assert` in the destructor of the command's argument parser, 6 runs. The
last two do not read the connection and are U-21; a client can send them too. After the fixes 408
runs survive and the 12 `SHUTDOWN` runs end the process (below); all 90 `DFLYMIGRATE` runs survive.
The `DFLY` subcommands, `DFLYCLUSTER` (answered from the config; `CONFIG` hands
`DispatchTracker` the null issuer it accepts), `GAT` (refuses a caller that is not memcache) and
`_XGROUP_HELP` read no connection and survived throughout; `ROLE` and `DEBUG REPLICA|REPLDIAG`
survive a single command and deadlock a concurrent client's `REPLICAOF NO ONE` (U-19).
`--cluster_mode=yes`, which the adversarial pass could not judge (its probe key got `MOVED`), is
covered by pushing a config first.

`DFLY FLOW` is guarded too (`DflyCmd::Flow`). The junk arguments of the sweep
stop at its replid and session checks, so it survived; but with a live session (`REPLCONF capa
dragonfly` creates one, in the preparation state) and this node's replid, `DFLY FLOW <replid> <sync
id> 0` on the apply context dies with SIGSEGV in `Connection::SetName` (`SetupFlowConnection`), as a
gtest shows. A classic stream would need this node's replid and the id of a live session, and a
replica refuses to create sessions (`REPLCONF` is refused on a replica) unless it is a peer-mode
node or runs `--experimental_cascaded_partial_sync`; no way for a stream to learn the replid was
found, so the guard closes a window nothing is known to open, instead of leaving it argued away.
`DFLY SYNC`, `STARTSTABLE` and `TAKEOVER` with the same session do not crash (a session leaves the
preparation state only through `FLOW`; `DFLY SYNC` replies `invalid state`).

**`SHUTDOWN` in a stream.** Raw, in an envelope, and as `SHUTDOWN NOSAVE NOW`, it makes the replica
exit with status 0, cleanly, within the probe's 5 s, in all three modes; the sweep's harness counts
those runs as crashes because the process is gone, but the status is 0. By reading the code:
`ShutdownCmd` only stops the listeners (`acceptor_->Stop()`), the main thread's `acceptor->Wait()`
returns and runs `Service::Shutdown`, which stops the replica from there, so the replication fiber
that ran the command is not joined by itself (U-17's abort does not apply). Unchanged, and a
question for the owner: a master can still stop its replicas this way, and `SHUTDOWN
SAVE|NOSAVE|FORCE` also sets `save_on_shutdown_` for that exit. No stock master streams it (it is
not a write command).

Tests: `ClassicNoConnectionTest` case `DflymigrateFlow` (instantiation `U15`),
`ClassicApplyFamilyTest.DflyFlowOfALiveSessionIsAnErrorNotACrash`, and
`keydb_onboarding_test.py::test_classic_stream_command_without_a_connection_does_not_abort`
(`dflymigrate_flow`, each `raw` and `in_envelope`). Falsified guard by guard: without the one in
`DflyMigrateFlow` the gtest dies with SIGSEGV in `Connection::SetName` and both pytest cases lose
the replica (exit -11); without the one in `DflyCmd::Flow` its gtest dies with SIGSEGV in
`Connection::SetName` (`SetupFlowConnection`).

**A stream-boundary filter was considered and rejected.** The class could be closed once, by the
dispatcher or `ConsumeRedisStream` refusing connection-bound commands for a context without a
connection. It does not hold up:

- No existing `CO::` or ACL flag marks the set. `SELECT` and `PING` are `acl::CONNECTION` and stock
  masters propagate them; `REPLCONF` is `CO::ADMIN` and is streamed as `GETACK`; `INFO` carries only
  `CO::LOADING`.
- `EVAL` bodies bypass a filter on the command name: `redis.call` reaches the handler through
  `CallFromScript`.
- `JournalExecutor` and the RDB-load search-aux path never pass through the stream boundary.
- `VerifyCommandState` would skip `RecordLatency` and log an unthrottled WARNING per refused command.

So the per-handler guards stay; any new upstream handler that dereferences `conn()` is a new U-15,
caught by `ClassicNoConnectionTest` only if a case is added.

### U-16. A blocking command in a classic stream stalls the link and the replica's shutdown

**Where:** `Replica::ConsumeRedisStream` (`src/server/replica.cc`), the dispatch of a raw command on
the apply context (`service_.DispatchCommand`, the context U-15 describes).

The replication fiber applies what the master streams by dispatching it itself. A blocking command
with no timeout, `BLPOP q 0`, parks that fiber until something pushes to `q`; on a replica nothing
can, because the only writer is the stream the fiber has stopped reading. The link stays `up`, no
later command applies, and the paths that stop the link (`REPLICAOF NO ONE`, shutdown) do not
complete. Where exactly they wait was not traced.

It is not only `BLPOP`: every blocking command that waits on a key stalls the link the same way
(the list under *How established*), **raw and inside an RREPLAY envelope**. The enveloped path is no
safer: `ClassicApplier::Dispatch` (`classic_replay.cc`) runs the inner command with
`DispatchCommand(..., ONLY_SYNC)`, and `HandleRreplay`'s one `running()` check (depth 1, before
anything is dispatched, spec D-3 step 2) cannot interrupt a dispatch that is already parked.

It needs a hostile or broken master: Redis and KeyDB propagate the effect of a satisfied blocking
pop (`LPOP`, `RPOP`, ...), never the blocking command, so a conforming master does not send `BLPOP`.
Pre-existing in upstream Dragonfly (the dispatch is upstream's), not specific to drakeydb's classic
support.

**How established:** the P7-1 re-review's probe, `blpop_probe.py` (a scripted master: a valid diskless
full sync, then `SET a 1`, `BLPOP q 0`, `SET b 2`): `a` arrives, `b` does not,
`master_link_status` stays `up`, `REPLICAOF NO ONE` gets no reply within 10 s, and SIGTERM did not
stop the process within about 40 s. The P7-1 adversarial pass widened it (a scripted master, a
debug build): a sweep of every registered command with junk arguments and a unique key per case
(4536 cases, raw and enveloped) hangs on **`BLPOP`, `BRPOP`, `BRPOPLPUSH`, `BZPOPMIN` and
`BZPOPMAX`** and nothing else. A targeted run then confirmed, raw **and** enveloped, **`BLPOP`,
`BRPOPLPUSH`, `BLMOVE`, `BZPOPMIN`, `BZMPOP`, `BLMPOP` and `XREAD BLOCK 0`**: the link stalls,
`REPLICAOF NO ONE` never answers, and SIGTERM does not stop the process within 15 s. So the set is
`BLPOP`, `BRPOP`, `BRPOPLPUSH`, `BLMOVE`, `BZPOPMIN`, `BZPOPMAX`, `BZMPOP`, `BLMPOP` and `XREAD
BLOCK 0` (`XREADGROUP .. BLOCK 0` was in neither run). The adversarial scripts are
`adv-p71/fuzz_cmds.py` (with `UNIQUE=1`) and `adv-p71/targeted.py` in the orchestrator's
scratchpad.

**Status (2026-10-04, widened 2026-10-05): open, out of P7 scope.** Not fixed, not filed upstream,
no code written. Candidate fix: refuse blocking commands (`CO::BLOCKING`) on replicated contexts,
or give them zero timeout semantics (try once, never wait). **Owner:** after P7, with the upstream
sync.

### U-17. A command that rewires the replica's own link, in a classic stream, aborts or stalls the replica

**Where:** `ServerFamily::ReplicaOf` (serves `REPLICAOF` and `SLAVEOF`), `ServerFamily::AddReplicaOf`
and `ServerFamily::ReplTakeOver` (`src/server/server_family.cc`), reached from
`Replica::ConsumeRedisStream`'s dispatch of a raw command, or of the inner command of an RREPLAY
envelope, on the apply context U-15 describes (no connection).

`REPLICAOF NO ONE`, `SLAVEOF NO ONE` and `REPLICAOF <host> <port>` run `Replica::Stop` on the link
they replace, which is the link the command arrived on. `Stop` joins the replication fiber, and that
fiber is the one running the command: `fiber_interface.cc:373 Check failed: active != this`
(`Replica::Stop <- ServerFamily::ReplicaOfNoOne <- DispatchCommand <- ConsumeRedisStream`), a SIGABRT
in release builds too. On a peer (`--active_replica`) node the same abort comes through `REPLICAOF NO
ONE` and `REPLICAOF REMOVE <this link's master>`. A raw `REPLTAKEOVER` parks the replication fiber on
the master socket the fiber reads itself, for the takeover timeout plus 10 s (a classic master never
answers): the link stays `up` and nothing more applies. `ADDREPLICAOF` does not crash, but opens a
second link to a master the stream chose. No stock master propagates any of them (none is a write
command), so only a hostile or broken master sends them. The raw path is upstream's; P7-1's envelope
path is a second way in.

**How established:** the P7-1 adversarial pass (findings I1, I2), a scripted master, raw and
enveloped. For the fix, `keydb_onboarding_test.py::test_classic_stream_link_command_does_not_abort`
without the guards: all 12 `REPLICAOF`/`SLAVEOF` cases (plain replica: `REPLICAOF NO ONE`, `SLAVEOF NO
ONE`, `REPLICAOF <reachable>`; peer node: `NO ONE`, `SLAVEOF NO ONE`, `REPLICAOF REMOVE <master>`; raw
and in an envelope) die with `Check failed: active != this`, and both `ADDREPLICAOF` cases open a
second connection to the master.

**Status (2026-10-05): fixed in this fork** (P7-1 close, `8b860ea`): `ReplicaOf`, `AddReplicaOf` and
`ReplTakeOver` reply `No connection` (U-15's `ReplyIfNoConnection`) as their first statement when the
context has no connection. The command is dropped and, inside an envelope, counted in
`classic_apply_errors`; the link stays up, the offset is exact and the commands around it apply.
Three `// drakeydb: U-17` guards, all in `server_family.cc`. Unaffected: the `--replicaof` boot path
(`ServerFamily::Replicate` calls `ReplicaOfInternal` directly; pinned by the `[boot_replicaof]` cases
of `test_plain_replica_of_active_keydb_expires_keys` and
`test_dfly_master_that_says_active_replica_never_turns_replica_expiry_on`) and every client-issued
command (a client has a connection). A script cannot call any of them, in any mode: `CO::ADMIN`
implies `CO::NOSCRIPT` (the `CommandId` constructor), which `VerifyCommandState` refuses before it
looks at a script's multi mode, so even a global script (`--!df flags=allow-undeclared-keys`) gets
`This Redis command is not allowed from script` for `REPLICAOF` (tried against a real server), and
there is no `EVAL` variant to guard or to test. Tests
`ClassicNoConnectionTest` instantiation `U17` (`ReplicaofNoOne`, `SlaveofNoOne`, `ReplicaofHost`,
`SlaveofHost`, `ReplicaofRemove`, `Addreplicaof`, `ReplTakeover`, `ReplTakeoverSave`) and the pytest
above (16 cases). Falsified: without the guards the 8 gtests and 15 of the 16 pytest cases fail; the
raw `REPLTAKEOVER` case passes while U-18's refusal is in place, and with both removed both
`REPLTAKEOVER 30` cases fail (`b` never arrives); the gtest and the `in_envelope` pytest fail with
only the U-17 guard removed. The `ReplTakeOver` guard is not redundant with U-18: U-18 reads only the
main link (`replica_`), so on a node whose main link is a Dragonfly master and whose `ADDREPLICAOF`
link is classic, a `REPLTAKEOVER` streamed on the add-link would pass U-18 and run a real `DFLY
TAKEOVER` against the Dragonfly master, promoting this node and shutting that master down. The U-17
guard refuses it first. Pre-existing in upstream Dragonfly (the raw path);
not filed upstream. On the byte-identity exception list (spec, item 2).

### U-18. A client `REPLTAKEOVER` on a replica of a classic master consumes replication stream bytes

**Where:** `ServerFamily::ReplTakeOver` -> `Replica::TakeOver` (`src/server/replica.cc`), which sends
`DFLY TAKEOVER <timeout> <session id>` on the master socket and reads the reply from it with the
shared parser.

On a classic link that socket is the replication stream, which `ConsumeRedisStream` reads too, and a
current classic master does not answer a replica's command on it (KeyDB 6.3, Redis 7+ and Valkey
feed a replica from the replication backlog, so a reply never reaches it; Redis 6.2 and older would
put an error into the stream, which is no better). So the
"reply" `TakeOver` reads is whatever the master streams next: that command is applied nowhere, the
replica's offset stays behind by its bytes for good, the link stays `up`, and the master lists the
replica `online lag 0`. Silent, permanent divergence; the client sees `Couldn't execute takeover: Bad
message`, or waits out the timeout against an idle master. Before that, `StartJournalAtOwnLSN` has
already started a journal on a node that stays a replica. Operator-triggered, but it is the natural
cutover command for an operator moving off KeyDB.

**How established:** the P7-1 adversarial pass (finding C2, `takeover_load.py`): an active KeyDB
taking ~1.5k `INCR`/s while a client ran `REPLTAKEOVER` on the replica; every takeover failed with
`Bad response to "DFLY TAKEOVER 0 ": "*5 RREPLAY ... INCRBY ctr 1 ..."`, and the replica lost 8 writes
(32 against a stock KeyDB master). For the fix, the pytest without the refusal: the replica logs `Bad
response to "DFLY TAKEOVER 1 ": "*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n"`, the streamed `SET b 2`
read as the reply.

**Status (2026-10-05): fixed in this fork** (P7-1 close, `8b860ea`): `ReplTakeOver` refuses with
`REPLTAKEOVER is not supported on a replica of a classic (Redis protocol) master` when
`replica_->GetSummary().classic_link` (the protocol of the last completed `Greet()`), and sends nothing
to the master. The check comes after `IsMaster()` (an idempotent `OK` on a master) and before
`StartJournalAtOwnLSN`, so a refused takeover starts no journal. One `// drakeydb: U-18` hunk in
`server_family.cc`. A takeover from a Dragonfly master is unchanged: the DFLY takeover tests
(`replication_resilience_test.py` `test_take_over_*`, `test_double_take_over`,
`multimaster_test.py::test_active_node_admits_fork_consumers_refuses_others_and_takeover`,
`cluster_test.py::test_replica_takeover_moved`; 14 runs) pass. A link whose first `Greet()` never
completed reads `classic_link` false and still gets upstream's `Full sync not done`, as before (a
client `REPLICAOF` cannot leave such a link, since `Start()` greets before it returns `OK`; the
`--replicaof` boot path can, harmlessly). Test
`keydb_onboarding_test.py::test_client_repltakeover_on_a_replica_of_a_classic_master_is_refused` (a
scripted master silent to `DFLY`, as KeyDB is: the error names `REPLTAKEOVER` and "classic", no `DFLY`
request reaches the master, the later `SET b 2` applies, the ACK offset is exact, one connection, role
`slave`, link `up`). Falsified: refusal removed -> `Couldn't execute takeover: Bad message`. Not re-run
against real KeyDB after the fix. Pre-existing in upstream Dragonfly; not filed upstream. On the
byte-identity exception list (spec, item 2).

### U-19. A command in a classic stream that takes `replicaof_mu_` can deadlock a client's `REPLICAOF NO ONE`

**Where:** `ServerFamily::Role` (`src/server/server_family.cc`), `DebugCmd::Replica` (`DEBUG REPLICA
PAUSE|RESUME|OFFSET`) and `DebugCmd::ReplDiag` (`DEBUG REPLDIAG`) (`src/server/debugcmd.cc`). Each
takes `ServerFamily::replicaof_mu_`, directly or through `PauseReplication`, `GetReplicaOffsetInfo` and
`GetReplicaMasterSocketUnreadBytes`, and is reached from `Replica::ConsumeRedisStream`'s dispatch of a
raw command, or of the inner command of an RREPLAY envelope, on the apply context U-15 describes (no
connection).

A client's `REPLICAOF NO ONE` (and `REPLICAOF <host> <port>`) holds `replicaof_mu_` across
`Replica::Stop`, which cancels the link and joins its replication fiber (`sync_fb_.JoinIfNeeded()`).
A command streamed by a classic master runs on that fiber. If the fiber is parked on `replicaof_mu_`
while the client holds it, neither goes on: the fiber never ends, the `REPLICAOF` never replies, and
the mutex is never released. Measured (`deadlock_probe.py`) on a replica streamed `ROLE` without a
pause: `REPLICAOF NO ONE` gets no reply, and neither do `ROLE`, `CLIENT LIST`, `REPLTAKEOVER` and
`DEBUG REPLICA OFFSET` after it. `INFO replication` still answers, because `REPLICAOF NO ONE` flips
the master flag before it waits and `INFO` takes the mutex only for a replica (a `REPLICAOF <host>
<port>`, which leaves the node a replica throughout, would hang `INFO` too; read, not measured).
`PING` and `SET` work, and SIGTERM does not stop the process within 20 s, because
`ServerFamily::Shutdown` takes the same mutex: it takes SIGKILL. No stock master streams `ROLE` or
`DEBUG` (neither is a write command), so only a hostile or broken one does, and a client's command
has to arrive while the stream is busy with them.

`DEBUG REPLICA PAUSE` has a second effect. It pauses the link it arrived on, and `MainReplicationFb`
does not reconnect a paused link (`if (is_paused_) continue;` ahead of the connect), so when the master
drops the connection the replica stays down until a client sends `DEBUG REPLICA RESUME`.

**Every acquisition of `replicaof_mu_`** (`server_family.cc`, none elsewhere) and what reaches it:

- `Shutdown`: `Service::Shutdown` on exit. `SHUTDOWN` itself only stops the listeners, so no command
  runs it (see U-15, "`SHUTDOWN` in a stream").
- `PauseReplication`, `GetReplicaOffsetInfo`: `DEBUG REPLICA ...`. Guarded here.
  `GetReplicaMasterSocketUnreadBytes`: `DEBUG REPLDIAG`. Guarded here.
- `Role`: guarded here.
- `GetReplicaSummary`: `Info` and the `GetMetrics` it calls (U-15 guard first), the emulated
  `CLUSTER` (`GetEmulatedShardInfo`, behind its U-15 guard), and `/metrics`, varz and memcached
  `stats`, which run on their own fibers and do not come from a replication stream.
- `GetMasterLinkClientInfo`, `IsMasterLinkClientId`: `CLIENT LIST|KILL`, behind the one `Client` guard.
- `AddReplicaOf`, `ReplTakeOver`, and `ReplicaOfInternal` / `ReplicaOfNoOne` through `ReplicaOf`: the
  U-17 guards come first. `Replicate()`, the `--replicaof` boot path, calls `ReplicaOfInternal` itself,
  from the boot flow.
- `ReplConf`: its U-15 guard is the first statement, ahead of its lock. `GetLineageId`: `ReplConf`, and
  `DflyCmd::Flow`, which is guarded (U-15).
- The peers' own mutex (`PeerReplicationManager::mu_`, also held across `Stop`) is taken by
  `PauseReplication` (here), `INFO`'s peer summaries, `REPLICAOF` on a peer node and `REPLCONF`'s
  reciprocal check: all behind a guard above.

**How established:** the whole-branch re-review's M-1, by reading the code. For the fix: a scripted
master floods the stream with the command while a client sends `REPLICAOF NO ONE`
(`keydb_onboarding_test.py::test_classic_stream_replicaof_mutex_command_cannot_deadlock_replicaof_no_one`):
without the guards all 8 cases (`ROLE`, `DEBUG REPLICA PAUSE|RESUME|OFFSET`, raw and in an envelope)
get no reply in 30 s. `test_classic_stream_debug_replica_pause_does_not_strand_the_link`: without the
guard both cases never see a second connection.

**Status (2026-10-05): fixed in this fork** (P7-1 close, guards round 2): three `// drakeydb: U-19`
guards, each before the lock and replying `No connection`: `ReplyIfNoConnection` at the top of
`ServerFamily::Role`, and an inline null-connection check at the top of `DebugCmd::Replica` and of
`DebugCmd::ReplDiag`. The command is dropped and, inside an envelope, counted in
`classic_apply_errors`; the link stays up and the commands around it apply. A client has a
connection, so `ROLE` and `DEBUG` answer it as before, and no script can call either (`ROLE` is
`NOSCRIPT`, `DEBUG` is `ADMIN`). Tests `ClassicNoConnectionTest` instantiation `U19` (`Role`,
`DebugReplicaPause`, `DebugReplicaResume`, `DebugReplicaOffset`, `DebugReplDiag`: the fixture's node
is a master, so they pin the reply, and the deadlock is pinned by the pytests above), and
`test_classic_stream_command_without_a_connection_does_not_abort` (the same five, raw and in an
envelope). Falsified one guard at a time: without `Role`'s, its gtest fails and so do 3 of the 4
pytest cases (both deadlock cases, which get no reply in 30 s, and the envelope case; the raw case
cannot tell); without `DebugCmd::Replica`'s, its three gtests fail, and 11 of 14 pytest cases (all
six deadlock cases, both strand cases, the three envelope cases); without `DebugCmd::ReplDiag`'s,
its gtest and its envelope case fail (a single `REPLDIAG` holds the mutex only at its start, so
nothing else pins it). Pre-existing in upstream Dragonfly; not filed upstream. On the byte-identity
exception list (spec, item 2).

### U-20. `SORT .. LIMIT` with an offset plus count beyond `uint32` replies garbage or crashes

**Where:** `GetSortRange` and the partial sort's end in `SortVisitor` (`src/server/generic_family.cc`).

`LIMIT 1 4294967295` and `LIMIT 4294967295 1` are valid in Redis and KeyDB (everything from the
offset on; nothing). Here `offset + count` wrapped around a `uint32`, so the range ended before it
began: the plain form replied a garbage array length, and the `BY` and `BY nosort` forms killed the
server with SIGSEGV. Any client can send it, and so can a classic master's stream (`SORT .. LIMIT ..
STORE` is replicated verbatim).

**Status (2026-10-05): fixed in this fork** (P7-1, `bb2a2a0`, found while fixing D-34): both sums
are 64-bit. Two `// drakeydb: P7-1` hunks, counted apart from decision 34's in the UPSTREAM-SYNC
`generic_family.cc` row. Tested in `GenericSortOrderTest` and the D-34 pytests; falsified (each sum
back to 32 bits: the test binary dies with SIGSEGV, the server dies in all four pytests; the partial
sort's alone: one gtest and four pytests fail). Negative or beyond-`uint32` `LIMIT` arguments are
still an error here where Redis and KeyDB clamp them (D-34, "Left open"). Pre-existing in upstream
Dragonfly; not filed upstream. On the byte-identity exception list (spec, item 2).

### U-21. `DFLYMIGRATE ACK` on a node without a cluster config dereferences a null config; a bare `DFLYMIGRATE` fails a debug assert

**Where:** `ClusterFamily::DflyMigrateAck` and `ClusterFamily::DflyMigrate`
(`src/server/cluster/cluster_family.cc`).

`DflyMigrateAck` read `ClusterConfig::Current()->GetIncomingMigrations()`, and `Current()` is null
until a cluster config has been pushed: always with `--cluster_mode` unset or `emulated`, and in
`yes` mode until the first `DFLYCLUSTER CONFIG`. `DFLYMIGRATE ACK <id> <attempt>` then dies with
SIGSEGV in `ClusterConfig::GetIncomingMigrations`: a call through a null pointer, so a release build
too, by reading (the sweep ran a debug build). The command is `CO::ADMIN | CO::HIDDEN` and
registered in every cluster mode; an admin command is refused to a client only for a command named
in `--restricted_commands`, so any client the server takes commands from can kill a node that is not
in cluster mode. A classic master's stream can send it as well. Second, `DFLYMIGRATE` alone passes
its arity check (-1), `DflyMigrate` reads a subcommand that is not there, and nobody takes the
parser's error: in a debug build `~CmdArgParser` asserts (`Parsing error occured but not checked`),
SIGABRT; a release build answers an unknown subcommand and is unaffected.

**How established:** the P7-1 close's hidden-command sweep (U-15, "The sweep"): `DFLYMIGRATE ACK x
1` and a bare `DFLYMIGRATE` in a classic stream, raw and in an envelope, killed the replica (`ACK`
in every mode but `yes`). Both reproduce for a plain client too: the gtests below die in
`BaseFamilyTest::Run`, before their stream half.

**Status (2026-10-05): fixed in this fork** (P7-1 close, guards round 2): two `// drakeydb: U-21`
hunks. `DflyMigrate` takes the parse error of the subcommand (`RETURN_ON_PARSE_ERROR`, a `syntax
error` reply). `DflyMigrateAck` treats a missing config as no incoming migration, which it already
answers `UNKNOWN_MIGRATION` (a simple string, not an error) for one that is not in the config. Tests
`ClassicApplyFamilyTest.DflymigrateAckWithoutAClusterConfigIsUnknownMigration` and
`.BareDflymigrateIsAnErrorNotAnAbort` (a client, then the stream's context), and
`keydb_onboarding_test.py::test_classic_stream_dflymigrate_without_a_cluster_config_does_not_abort`
(`ack`, `bare`, each `raw` and `in_envelope`). Falsified: with `RETURN_ON_PARSE_ERROR` removed the
gtest aborts on the assert and both `bare` pytest cases lose the replica; with the config check
removed (back to `Current()->`) the gtest dies with SIGSEGV and both `ack` pytest cases lose the
replica. Pre-existing in upstream Dragonfly; not filed upstream, and worth filing (a remote crash of
any non-cluster node). On the byte-identity exception list (spec, item 2).

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
   that bug for plain replicas purely to preserve the slogan, so it stays on for everyone. A
   cross-shard `STORE` with an empty result journals `DEL <dst>`. The same code writes three more
   `SORT` entries that the merge base does not, also ungated (listed in `docs/UPSTREAM-SYNC.md`): a
   same-shard `SORT <missing source> STORE <existing dst>`, its `BY nosort` form and a source the
   fetch's own member expiry emptied journal `DEL <dst>` alone (D-33, decision 32; the merge base
   journals the verbatim `SORT`, upstream main `DEL <dst>`); a same-shard sorted `STORE` whose own
   fetch emptied the source journals `DEL <dst>` ahead of the verbatim `SORT` (P4-3 review wave);
   and a `SORT` that lazily expires some members of a set with member TTLs journals `SREM <key>
   <members>` ahead of itself (P4-0). Upstream main has since changed SORT's journaling model
   (`DEL` + `RPUSH` from `OpStore` on every `STORE`, no revived auto-journal), which fixes the
   premise above in another shape: this exception inverts at the sync, and the `generic_family.cc`
   row of `docs/UPSTREAM-SYNC.md` records the decision the sync must take.

So "byte-identical with `--active_replica` off" is true for the journal wire *except* the
`SORT ... STORE` entries of item 2, true for the RDB file and INFO memory, and not true for INFO as
a whole. Stated that way in `docs/UPSTREAM-SYNC.md`, `docs/PLAN.md` and `docs/differences.md`.

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

Upstream main has dropped the recipe altogether: `SORT` is `CO::NO_AUTOJOURNAL` there with no
revive, and `OpStore` journals `DEL <dst>` + `RPUSH <dst> ...` for every `STORE`. Taking that model
at the sync closes this entry (and D-18's SORT part); `docs/UPSTREAM-SYNC.md`'s `generic_family.cc`
row records the choice.

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

**Owner:** tombstone-lifecycle phase (scheduled after P7); needs per-arm ownership in
`MvccStamper`. **From:** P4-3 Tasks 6/13, final review.

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

**How established:** static analysis performed while adding the merge-load path; the
three-peer scenario is unmeasured (see D-15). The live-reap and member-expiry-reaper paths'
identical exposure was identified when those paths were changed to derive their stamp from the
value's own too.

**Status:** open. **Owner:** tombstone-lifecycle phase (scheduled after P7). **From:** the
merge-load synthetic tombstone's own introduction; widened when the local-reap paths adopted the
same rule.

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

A third, separate shape: `MOVE` (`GenericFamily::Move`, `src/server/generic_family.cc`) is
hand-journaled `RecordJournal("MOVE"sv, ...)`, unconditionally under its own client-facing name,
for BOTH databases involved — never decomposed into guarded state the way cross-shard `RENAME`
is. It is `CO::NO_AUTOJOURNAL`, so `MvccStoreTest.RegistryClosureEveryAutoJournaledNameIsGuardedOrKnown`'s
forward loop (which skips every `CO::NO_AUTOJOURNAL` command before its guarded/known-unguarded
check ever runs) cannot see it at all — it is neither in the guarded table nor in that test's
explicit unguarded allowlist, invisible to both. It is also journaled EVEN WHEN `OpMove` itself
fails on the author (`res != OpStatus::IO_ERROR` is the only thing that suppresses the journal
call — `KEY_NOTFOUND` and `KEY_EXISTS`, `OpMove`'s two ordinary failure returns, both still
journal): a verbatim `MOVE key target_db` replayed on a receiver re-runs `OpMove`'s own
preconditions against THAT receiver's independent copy of both databases, so the replay can itself
hit `KEY_EXISTS` in the destination db even though the author's own move genuinely succeeded (or
vice versa) — a divergence the streaming guard has no way to catch, since `MOVE` never carries a
per-key stamp compare at all.

Separately, and for a different reason: a handful of commands whose name alone cannot distinguish a
full-value write from a partial one are unguarded by design, not by omission —
`JSON.SET`/`JSON.MERGE` (a `"$"` root path replaces the whole document, but the same name also
covers an ordinary partial patch), `JSON.MSET` (each `key path value` triple in one command calls
the same `OpSet` JSON.SET uses internally, so a `"$"` path on any one triple is the identical
whole-document replace, journaled under `JSON.MSET`'s own name instead of `JSON.SET`'s),
`JSON.DEL`/`JSON.FORGET`/`JSON.CLEAR` (same ambiguity for a delete/clear at an arbitrary path vs.
the root), `CMS.MERGE` (always resets the destination sketch then writes the weighted sum of the
sources — the same blind, state-carrying recompute `PFMERGE`'s `SET` result is guarded for, but
journaled under `CMS.MERGE`'s own name instead), and `BF.LOADCHUNK`'s `cursor==1` init phase
(overwrites any existing key wholesale). Adding any of these to the guarded table would also
guard-and-drop their ordinary partial-write uses, which is worse than leaving the whole name
unguarded.

**How established:** static reading of the guarded-vocabulary table (`multimaster_lww.cc`) against
every command's own journaling call site; not reproduced with a live divergent value.
`MvccStoreTest.EmittedNamePinsMatchClassifiedGuardedNames` pins the four runtime-revived names
(verified against a running build) and, separately, `MOVE`'s own emitted name and its `kUnguarded`
classification; `MvccStoreTest.RegistryClosureEveryAutoJournaledNameIsGuardedOrKnown` pins the
JSON/`CMS.MERGE`/`BF.LOADCHUNK` names as a reviewed, explicitly-known-unguarded allowlist rather
than an accidental gap (that test's own forward loop cannot see `MOVE`, per the `CO::NO_AUTOJOURNAL`
skip described above) — both tests fail by name if a future normalization change silently moves
one of these onto, or off, a guarded name.

**Owner:** unassigned; same-shape precedent as D-13 (same-shard `SORT ... STORE` journaling the
recipe rather than the result) — the owner ruled there that hand-journaling the result for the
same-shard case is correct but changes the wire format for a path that works today, and left it for
a future decision. Fix path if wanted: journal the *result* (state) for the runtime-revived
recipes and for `MOVE`, the same way the cross-shard `RENAME` form already does. **From:** P4-4.

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
(newer) stamp is discarded, recorded nowhere. `GETDEL` of an absent key takes this exact same skip,
not a separate one: `ApplyLwwRewrites` (`multimaster_lww.h`) rewrites a guarded `GETDEL` to a plain
`DEL` pre-dispatch, so by the time this skip check runs, a replicated `GETDEL` IS `OpDelV2`'s
own `DEL` — there is no separate `GETDEL` code path to name here.

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

**A second cause, not just an absent key's skip.** An applied, guarded `DEL` (a `GETDEL` arrives as
one too, rewritten pre-dispatch — see D-20's first cause, above) of a key whose TTL has already
elapsed but which this node has not yet reaped hits the same symptom from a different angle. The
guard's own veto compares the author's stamp `X` against the key's LIVE, still-unreaped stamp `S`
(a pure side-table read that never itself triggers expiry) and passes it (`X` is newer than `S`);
only INSIDE the callback does `FindMutable` (`OpDelV2`'s own lookup) lazily reap the key, installing
`ExpiryTombstoneFor(S)` — one origin_hash tick above `S`,
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

**Owner:** tombstone-lifecycle phase (scheduled after P7). **From:** P4-4.

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
indefinitely, immune to each other's writes.

**Resolution requires a peer that never itself applied this delta.** Such a peer's own, untouched
copy reaps independently, on its own clock, to `{P.Mvcc(), P.origin_hash + 1}` — `T` itself,
`ExpiryTombstoneFor(P)` — strictly greater than this node's `P`-stamped manufactured value, so the
next full sync between them converges to absent, provided no unguarded delta lands on this node's
own copy of `k` first and pushes its stamp up to or past that tombstone (D-23). This does NOT
generalize to every peer that eventually reaps, though: a peer that instead received and applied
this SAME delta itself, before reaping, ends up at the identical `P` magnitude too, never at `T` —
two ways, both reachable independently of the shape above:
  - A peer applying the delta AFTER its own deadline has already passed sees the identical
    `T`-as-`stored` case this node does, floors the same way, and lands live at exactly `P` — the
    SAME shape as this node, not the healing one.
  - A peer applying the delta BEFORE its own deadline sees a LIVE `stored=P` (the delta's own
    author stamp is, by construction, older than `P` for the headline floor to have fired at all
    elsewhere), floors ONE TICK BELOW `P`, and only later reaps THAT floored live value on its own
    clock — `ExpiryTombstoneFor` of a stamp one tick below `P` lands exactly back on `P` itself
    (with the tombstone bit set), the SAME magnitude as this node's live value, not `T`.

Either of those two peers ties this node's manufactured value bit-for-bit (mod the tombstone bit,
which `MergeAccepts`' `operator<` masks) — `MergeAccepts` returns false in BOTH directions between
them, so neither ever heals the other, indefinitely. Only a peer that reaps a copy of `k` it never
applied this delta to breaks the tie, by landing on `T` instead of `P`.

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

### D-27. An unguarded delta authored before a key's TTL deadline, applied after it elsewhere,
re-creates the key with no TTL

**Where:** every delta-journaled RMW (`INCR`, `APPEND`, `HINCRBY`, ...) is unguarded by design
(`ClassifyJournaledCommand`, `multimaster_lww.h` — dropping a delta permanently loses it rather
than merely reordering it, so it always applies in plain arrival order against whatever this node
currently holds) and per-node whole-key expiry runs independently on each node's own clock. The
combination re-creates a key that should stay expired:

1. `A` writes `SET k 5 PX 100` stamped `P`; it converges to every peer.
2. `B` runs `INCR k` stamped `I > P` at `t=50` on its own still-live copy (a delta, unguarded, no
   compare against anything).
3. `INCR k`'s journal entry reaches `A` only after `A`'s own deadline for `k` has already passed:
   `A`'s copy is gone, replaced by an expiry tombstone at `{P, hA+1}` (`ExpiryTombstoneFor(P)`,
   `mvcc.h`). The applied, unguarded `INCR` re-creates `k=1` on `A`, stamped `I`, with NO TTL — an
   ordinary `INCR` on an absent key always creates a plain integer, and this apply path has no
   compare against the tombstone to reject on (unguarded RMW never consults the mvcc side table at
   all).
4. `B`'s own copy of `k` independently expires at its own deadline and becomes a tombstone at
   `{I, hB+1}`.

`A` now holds a live, TTL-less `k=1` while every other peer holds a tombstone for the SAME key at
a stamp derived from the SAME `I`. Nothing in this describes a one-shot glitch that self-heals: the
next ordinary `INCR` on `A` mints a fresh stamp `I2 > I` and commits `A=2@I2`; the next `INCR`
reaching `B` (or any peer) after ITS OWN tombstone re-creates `B=1@I2` the identical way — an
IDENTICAL-STAMP split (D-23) that a later full sync's `MergeAccepts` tie-break (ties favor the
stored side) cannot resolve either, since neither node's own value is authoritative over the
other's. **Impact on the classic rate-limiter pattern** ("`INCR`; `EXPIRE` only when `INCR` returns
1"): once `A`'s copy has been silently re-created with no TTL this way, `A`'s own copy of that
counter never expires again — every future window's `INCR` returns something other than 1 (the
counter is never actually absent from `A`'s point of view), so the pattern's own `EXPIRE` never
fires, permanently.

**Operator mitigation:** run `EXPIRE k ttl NX` after EVERY `INCR`, not only when `INCR` returns 1.
`NX` means "set expiry only when the key currently has none" (Redis semantics), which is exactly
the self-healing property needed here: on an ordinary `INCR` the key already carries its correct
TTL, so `NX`'s own precondition fails, `UpdateExpire` (`db_slice.cc`) — called unconditionally on
every `EXPIRE`, regardless of the precondition — returns `SKIPPED` (a cheap NX/XX/GT/LT condition
check only, no O(value) work), and `OpExpire`'s own `res.ok()` gate (`generic_family.cc`) is what
keeps its full-state-ship branch from ever being reached for it. Nothing is journaled — no wasted
O(value) cost on the common path. Once D-27 silently
re-creates the key with NO TTL, the very next `EXPIRE ... NX` finds none, its precondition holds,
and it ships the key's full current state (`OpExpire`'s `JournalFullStateSet` branch,
`generic_family.cc`), which re-converges every peer's copy on that state, closing the split the
same way any other guarded full-state write would.

**How established:** static reading of `ClassifyJournaledCommand`'s delta-RMW exclusion, whole-key
expiry's per-node independence, and `FloorAppliedStamp`'s own scope (it governs an applied RMW's
COMMITTED stamp when a live value is present to floor against — it has nothing to floor against
here, since the key is absent at apply time); not reproduced with a live three-step repro.

**Owner:** tombstone-lifecycle phase (scheduled after P7). An expiry tombstone that records the
EXPIRED VALUE'S OWN deadline `D` (not merely its stamp) would let a receiver applying a delta
authored strictly before `D` drop it outright instead of re-creating the key: the key would have
expired at `D` on every node anyway, author included, so a delta timestamped before `D` describes
a value that no longer exists anywhere once `D` passes.

**Related: D-32.** The same mechanism reaches a node that applies a classic master's stream. D-32 is
its instance on a plain replica of an active KeyDB (the replica's own sweep passes the deadline and
KeyDB never streams the `DEL`), which P7-2 Task 2.9 closes. It does not close this entry's peers:
a drakeydb peer, one attached to an active KeyDB through a classic peer link included, sweeps and
serves reads on its local clock, so this entry owns that share of the orphan after Task 2.9.

This is not new with P4-4: deltas have always applied in plain arrival order against each
node's own, independently-timed expiry: the streaming guard on
`SET`/`SETNX`/`GETSET`/`GETDEL`/`RESTORE`/`MSET`/`DEL` is what is new here, not the underlying
gap this describes. **From:** P4-4 (documented alongside the guard; the gap itself predates it).

### D-28. Member-TTL commands replay a relative deadline, computed from each receiver's own
arrival time

**Where:** `HEXPIRE`, `FIELDEXPIRE`, `SADDEX`, `HSETEX` (`src/server/hset_family.cc`,
`src/server/set_family.cc`, `src/server/generic_family.cc`) are all `CO::JOURNALED` without
`CO::NO_AUTOJOURNAL` — each auto-journals the client's own verbatim command, including its
relative seconds-from-now TTL argument, the same shape `RESTORE`'s own client-facing relative ttl
had for whole-key TTLs before `OpRestore` (`generic_family.cc`) started shipping an ABSOLUTE one
on an active node instead. Every one of these commands
sets a MEMBER-level (hash field / set element) deadline, not the key's own whole-key expiry, and
each is delta-journaled RMW (touches one member/field/entry, leaves the rest of the key's value
untouched) — deliberately unguarded by design (`ClassifyJournaledCommand`, `multimaster_lww.h`).
Each receiver that applies one of these entries — whether a plain replica or an active-node peer —
computes ITS OWN member deadline as `receiver_arrival_time + seconds`, not the author's own
`author_time + seconds`: under any nonzero replication lag, the member's actual deadline drifts
later on every downstream hop, compounding with each additional replica in a chain.

**How established:** static reading of each command's `CO::JOURNALED` registration (no
`CO::NO_AUTOJOURNAL`) and its own op function computing the member deadline from
`op_args.db_cntx.time_now_ms` (the RECEIVING node's own transaction clock, same as `RESTORE`'s
`UpdateExpiration` did before it was corrected to ship an absolute deadline instead) against the
client's relative seconds argument; not reproduced
with a live lagged-replica repro.

**Owner:** open; same shape as `RESTORE`'s relative-ttl replay (fixed for `RESTORE` itself — an
active node now ships an ABSOLUTE ttl via an explicit, `CO::NO_AUTOJOURNAL`-gated journal call).
Extending the identical treatment to these four member-TTL commands — an active-node explicit
journal shipping each member's ABSOLUTE deadline instead of the client's relative seconds — is a
plausible fix path, deliberately not taken here: registered only, not fixed, since the fix touches
four more call sites and deserves its own review rather than riding along with `RESTORE`'s.
**From:** P4-4.

### D-29. An unstamped value's expiry erases with no tombstone; a peer's own {0,0} copy can
resurrect it

**Where:** `RecordExpiryBlocking` (`src/server/tx_base.cc`) reads back
`MvccStamper::CommitOwnTombstone`'s (`mvcc.cc`) `has_prior_stamp` for the key being expired.
When `has_prior_stamp` is true (the ordinary case — the expiring value carried a real stamp),
`CommitOwnTombstone` installs a genuine tombstone, `ExpiryTombstoneFor(value)` (one origin_hash
tick above the value's own stamp, tombstone bit set), and `committed` captures that real,
non-zero value. When `has_prior_stamp` is false — the value being expired never carried a real
stamp of its own (mvcc 0, e.g. a `D-7`-style verbatim-loaded or otherwise unauthoritative key) —
`CommitOwnTombstone`'s own callback instead calls `EraseMvcc`: the key's mvcc side-table slot is
REMOVED entirely, not tombstoned, and `committed` stays default (`Mvcc() == 0`).

The `kEntryFlagExpired` journal entry for this expiry's own `DEL` (`RecordEntry`, `journal.cc` —
not a `kEntryFlagDerived` one; that flag names a different class, a collection-emptying delete
issued indirectly rather than directly by the client, covering BOTH a command-caused empty (e.g.
`RecordDerivedDelete`, `tx_base.cc`) and an expiry-caused one, per that flag's own comment,
`journal/types.h`) still gets a freshly MINTED local stamp whenever its own incoming mvcc argument
is exactly 0 — which is exactly
what this erase branch sends (`committed.Mvcc()`, i.e. 0) — the same minting every other
self-originated, never-stamped local write receives. That minted stamp only ever reaches a PLAIN
(non-mesh) replica, though: `journal::PassesPeerEchoFilter` drops every `kEntryFlagExpired` entry
before it reaches a peer-mesh link, and `SliceSnapshot::ConsumeJournalChange` applies the same
filter to a full sync's concurrent journal window — so the minted wire stamp plays no role
whatsoever in a peer-mesh merge comparison; only the LOCAL mvcc side-table state (erased, in this
case) does.

**Consequence:** this node now holds NO mvcc-table entry at all for the expired key —
`DbSlice::GetMvcc` returns `nullopt`, indistinguishable from a key that was never present. A peer
that later sends its OWN unstamped ({0,0}) copy of the SAME key — a full sync from a node that
itself never gave this key a real stamp, the same shape D-7's own {0,0} rule addresses for an
incoming key — merges via `MergeAccepts(nullopt, {0,0})`, which VACUOUSLY ACCEPTS (a `nullopt`
stored side loses every compare, by construction): the key is resurrected, with no TTL of its
own protection to reject it, even though this node deliberately let it expire. A REAL tombstone in
its place would NOT have this exposure — `ExpiryTombstoneFor`'s output always has a non-zero
`Mvcc()`, which strictly beats an incoming `{0,0}` in `MergeAccepts` — this residual is specific to
the never-stamped-to-begin-with case, where installing a tombstone was rejected as unsafe (an
`Mvcc()==0` tombstone would itself lose every compare against ANY later write, real or forged,
which is a worse, unconditional-loss failure mode than simply erasing).

**How established:** static reading of `RecordExpiryBlocking`/`CommitOwnTombstone`'s
`has_prior_stamp` branch and `MergeAccepts`'s own `nullopt`-always-accepts rule; not reproduced
with a live two-node repro (an unstamped key reaching this path at all requires either a
never-stamped local write or a D-7-style unauthoritative merge load, followed by that same key's
own natural expiry).

**Owner:** tombstone-lifecycle phase (scheduled after P7). Fix path if wanted: distinguish "no arm
found" from "arm found but carries no real stamp" more finely, or accept the resurrection risk as
inherent to a genuinely unstamped key (which, by definition, this fork never had real authority
over to begin with). **From:** P4-4 (found while auditing `RecordExpiryBlocking`'s own text against
its code; the code path itself predates this find).

### D-30. `HDEL`'s own auto-journal entry can propagate a stamp its author's local tombstone never
carries

**Where:** `DeleteHw` (`src/server/hset_family.cc`), called from `ExecuteW`'s write path (e.g.
`HDEL` removing the last field), deletes the now-empty hash through `db_slice.Del` and then, since
the journal is active, commits that key's OWN tombstone arm via an explicit `RecordDerivedDelete`
call — `journal::kEntryFlagDerived`, peer-suppressed by `PassesPeerEchoFilter` — right there,
inside the callback. `HDEL` itself is separately `CO::JOURNALED` (auto-journal, not
`NO_AUTOJOURNAL`): once the callback returns, the transaction's own generic per-arm commit logic
(inside `journal::RecordEntry`, for `HDEL`'s own auto-journaled entry) looks for an arm to consume
for this same key — but `DeleteHw`'s own `RecordDerivedDelete` call already consumed and removed
it. Finding no arm, `HDEL`'s own entry mints a bare `HopStamp` (wall-clock derived, no floor
applied — flooring needs a prior stamp to floor against, and there is none left to read).

When the hash's own pre-delete stamp `S` is NEWER than that bare `HopStamp` (reachable whenever
`S` was itself replicated from a peer whose clock, or accumulated write rate, put it ahead of this
node's own `HopStamp` at this instant), the AUTHOR's own local tombstone — committed via the
peer-suppressed derived DEL, properly floored/derived from `S` — ends up at `{max(HopStamp, S +
1), self}` (i.e. exactly `{S + 1, self}` under this precondition) with the tombstone bit set. But
`HDEL`'s own auto-journaled entry, the ONLY one that actually reaches a peer (the derived DEL is
peer-suppressed), ships the bare, unfloored `HopStamp` instead — strictly OLDER than `S`, hence
strictly older than what the author itself actually committed. A peer applying that entry as an
ordinary unguarded delta (`HDEL` is delta-journaled RMW, never LWW-compared) goes through
`journal::RecordEntry`'s own generic per-arm commit path too, exactly like the author's own
derived-DEL commit did: `FloorAppliedStamp(prev_stamp=S, incoming=HopStamp)` floors to
`{S.Mvcc(), S.origin_hash - 1}`, and because the peer's own `Del()` call armed this delete as a
TOMBSTONE (`ArmTombstone`, matching the author's own arm shape), `RecordEntry`'s commit callback
forces the tombstone bit onto that floored value regardless of what `FloorAppliedStamp`'s own
wire-derived bit computation gave it (`journal.cc`'s `Commit()` callback:
`floored.packed |= MvccClock::kTombstoneBit` when the arm's own `tombstone` flag is set) — so the
peer's own committed value is a TOMBSTONE too, `{S.Mvcc(), S.origin_hash - 1}`, one origin_hash
tick BELOW `S`, not a live value. Verified directly (`DEBUG MVCC`-equivalent stamp reads on both
sides of a captured-and-replayed `HDEL`, `S` set above the real `HopStamp` so the precondition
holds): author `{S.Mvcc() + 1, self}|T`, peer `{S.Mvcc(), S.origin_hash - 1}|T`.

The two sides do not disagree on tombstone-ness, then — both end up tombstoned — but on the
tombstone's own MAGNITUDE: one origin_hash tick ABOVE `S` on the author, one tick BELOW `S` on the
peer. A later write stamped strictly BETWEEN the two (older than the author's `S + 1` but newer
than the peer's `S - 1`) is accepted by the peer (it beats the peer's lower tombstone) and rejected
by the author (it loses to the author's higher one) — a genuine per-node application disagreement
for that third write, not merely a stamp bookkeeping curiosity. Same general class as D-23 (an
unguarded delta's committed stamp depends on what each node's OWN clock/stored-stamp happened to be
at apply time, not on any shared ground truth), reached here through a narrower, more specific
mechanism: the auto-journal entry racing its own command's already-consumed tombstone arm.

**How established:** static reading of `DeleteHw`'s explicit `RecordDerivedDelete` call composed
with `HDEL`'s own `CO::JOURNALED` (non-`NO_AUTOJOURNAL`) registration and the generic per-arm
commit logic in `journal::RecordEntry`, confirmed with a scratch probe (an artificially inflated
`S`, a real local `HDEL`, the captured `HDEL` wire entry replayed onto a second key seeded with the
same `S`) — not carried as a committed regression test, since D-30 is registered, not fixed.

**Owner:** open. Registered only, not fixed: a narrow, hard-to-trigger precondition (`S >
HopStamp` at this exact `HDEL`), not a general convergence hole. **From:** P4-4.

### D-31. A `RESTORE ... REPLACE` whose payload fails to load deletes the old key locally and
journals nothing

**Where:** `OpRestore` (`src/server/generic_family.cc`). When the target key already exists and
`REPLACE` was given, `OpRestore` deletes it through the ordinary `DelMutable` path (arming a
tombstone placeholder, exactly like the `Expired()` branch two entries above) BEFORE ever calling
`RdbRestoreValue::Add` on the caller-supplied payload. If `Add` then fails — `INVALID_VALUE` for a
malformed/untrusted body, or `OpStatus::SKIPPED` when every member of the incoming value expired
during deserialize (`rdb::errc::value_expired`, `RdbRestoreValue::Add`) — `OpRestore` returns that
status directly, never reaching its own explicit journal calls (all of which live either in the
`Expired()` branch above or after a successful `Add`, further down).

`RESTORE`'s own registration is `CO::JOURNALED | CO::NO_AUTOJOURNAL` (`generic_family.cc`), not
`CO::JOURNALED` alone. On an ACTIVE node, the `Restore` command handler never calls
`Transaction::ReviveAutoJournal()` (guarded on `!IsActiveReplica()`, since `OpRestore` journals
explicitly instead, with an ABSOLUTE ttl, on its own success path below), so
`IsAutoJournalSuppressed()` (`transaction.cc`) would suppress the generic auto-journal on its own,
independent of this failure. On a NON-active node,
`Restore` DOES call `ReviveAutoJournal()` (a plain replica has no per-key stamp to protect and must
see the exact upstream shape), reviving the ordinary verbatim auto-journal — but
`LogAutoJournalOnShard`'s own `if (result.status != OpStatus::OK) return;` gate, checked BEFORE
`IsAutoJournalSuppressed()` in program order, already fires first here regardless of node type,
since `OpRestore`'s own return status is never `OK` on this path: the same universal "a failed
auto-journaled command never journals" rule every command relies on, not a P4-4/LWW-specific
mechanism. Nothing about this delete ever reaches the wire, on either node type, for two related
but distinct reasons.

Locally, this is not a no-op: the old value is genuinely gone (`DelMutable` performs the real prime-
table erase immediately; it does not wait for a journal commit). Only the leftover MVCC side-table
arm is undone — `RollbackUncommittedTombstone` (`db_slice.cc`), running at this transaction's own
`EndOfWriteEpoch`, finds the still-armed placeholder tombstoned and calls `EraseMvcc` on it, which
clears the stamp table entry but has no effect on the prime table the key's actual value already
left. `RestoreReplaceFailureRollsBackTheOrphanedTombstone` (`multi_master_test.cc`) already covers
this rollback and confirms `StampOf(key)` comes back empty afterward, but only checks the stamp
side table, not `EXISTS`/`GET` — it does not (and was never meant to) observe that the key's real
value is also gone. Every peer that never attempted this same failing `RESTORE` still holds the old
value: a genuine, silent divergence, with no tombstone left behind on this node to let the next
guarded write reconcile it the way an expiry- or LWW-driven delete would.

A guarded receiver hitting the `SKIPPED` (all-members-expired) case specifically is also a member-
TTL variant of the same class of gap `InstallAbsentKeyTombstone` closes for whole-key expiry: on a
link with the LWW guard active, this silent delete installs no `ExpiryTombstoneFor`-style marker
either, so an older write
for the same key arriving afterward from a third peer is wrongly accepted here instead of being
rejected — the guarded sibling of D-28's already-registered member-TTL family (`HEXPIRE`,
`FIELDEXPIRE`, `SADDEX`, `HSETEX`), not the relative-deadline replay D-28 itself describes.

A separate, narrower instance of the same gap reaches a genuinely ABSENT key too: `Add`'s own
`SKIPPED` return (member-level expiry discovered only during deserialize) is entirely independent
of `restore_args.Expired()`'s own whole-key-TTL check above — `found_prev` can be `false` (the key
never existed here at all) while `Add` still returns `SKIPPED`, and this branch has no
tombstone-install logic of its own, unlike the `Expired()` + `!found_prev` branch, which installs
one via `InstallAbsentKeyTombstone`. A guarded RESTORE with this exact shape — absent key, every
member already expired inside the dump payload itself — installs nothing at all here: no value, no
tombstone, no trace this write was ever authored. A strictly OLDER write for the same key, arriving
afterward from a third peer, is then wrongly accepted, with no tombstone here to reject it.
Registered, not fixed, same as the `found_prev` case above.

**A related asymmetry, not a divergence.** The same write landing on a PRESENT live key instead
never reaches `InstallAbsentKeyTombstone`: `found_prev`'s delete above (`SetCmd::DeleteExpiredKey`'s
identical shape, `string_family.cc`) commits `FloorAppliedStamp(S, X)` = `X|tombstone`, one
origin_hash tick below this call's own `ExpiryTombstoneFor(X)` for the same write against an absent
key. No real stamp falls between the two (floored stamps never go on the wire), and a merge-load
raises the lower one to match — this present-key path predates `InstallAbsentKeyTombstone`.

**How established:** static reading of `OpRestore`'s control flow (the `DelMutable`-before-`Add`
ordering, both `Add` failure returns, the early, journal-call-free `return add_res.status();`, and
the absence of any tombstone-install call reachable from that same early return) composed with
`RollbackUncommittedTombstone`'s own body (`EraseMvcc` only, no prime-table restore) and
`LogAutoJournalOnShard`'s non-`OK` gate; not reproduced with a live two-node divergence repro.
`RestoreReplaceFailureRollsBackTheOrphanedTombstone` (`multi_master_test.cc`) already exercises the
identical `DelMutable`-then-`Add`-fails control flow this entry's `found_prev` half describes, but
for a different purpose (pinning `RollbackUncommittedTombstone`'s own arm-rollback behavior, not
this entry's divergence claim) — its own assertions were read to confirm they stop at the stamp
table, never observing `EXISTS`/`GET`.

**Owner:** open; pre-existing upstream shape (`DelMutable`-then-`Add` for `REPLACE` predates
drakeydb's own MVCC/LWW work), not introduced by P4-4. Registered only, not fixed. **From:** P4-4.

### D-32. A TTL-keeping write the master ran before a key's deadline leaves a permanent TTL-less
orphan on a plain replica of an active KeyDB

**Where:** `DbSlice::ExpireIfNeeded` (`db_slice.cc`) and the heartbeat sweep, both opened for a
plain replica whose master said `active-replica` (P7-1 Task 1.4, spec D-9, ledger decision 13). An
active KeyDB never streams an expiry `DEL` (`db.cpp:1980`), so such a replica expires keys itself,
on its own clock, and the two disagree about a key whenever a command crosses a deadline in
flight:

1. The master holds `c` with a deadline `E`. At `Tm < E` it runs a write that keeps the TTL when
   the key exists and creates the key when it does not: `INCR` (streamed as `INCRBY c 1`),
   `APPEND`, `SETRANGE`, `HSET`, `HSETNX`, `SADD`, `LPUSH`, `SET .. KEEPTTL`, ... On the master `c`
   keeps `E`.
2. The command reaches the replica at or after `E` on the replica's clock (the stream's lag plus
   the clocks' skew). The replica has deleted `c` by then, in the sweep or at the first access.
3. The write finds no key and creates one from nothing, with no TTL.
4. The master's `c` expires at `E`, and nothing is streamed for it. The replica's `c` stays for
   ever: the sweep has no TTL to act on and DBSIZE never moves.

**Reproduced** (the reviewer's script, `orphan.py`; now the pytest below): a fake master saying
`active-replica` streams `SET c 5 PXAT E`, `HSET h f 1` with `PEXPIREAT h E` and `SET kt old PXAT
E`, and a second past `E` the enveloped `INCRBY c 1`, `HSET h g 2` and `SET kt new KEEPTTL`,
stamped before `E`. After them the replica has `c`, `h` and `kt` with `pttl -1`, and DBSIZE stays
5 for 3 s (the pytest checks 2 s). A rate limiter (`INCR`, then `EXPIRE` only when the value is 1) meets it at a window
rollover on a hot key. A counter whose `EXPIRE` is streamed right behind the `INCR` is not
orphaned (the `PEXPIREAT` gives the recreated key a TTL), and `SET .. NX` is not affected (a failed
one is never propagated, `server.cpp:4624`, `t_string.cpp:104-109`). A TTL refresh, `SET .. XX`
with no expiry and the movers and STORE commands lose data rather than orphan it: a refresh that
sets a deadline only until that deadline, `PERSIST`, `SET .. XX` without an expiry, a moved element
and a STORE result for good. `RENAME` and `COPY` onto a **live** `dst` leave a **permanent** stale
key: the master's `RENAME` gives `dst` the source's deadline `E` (`db.cpp:1507-1511`) and the key is
gone there at `E`, while the replica's `RENAME` finds no source, fails and keeps its old `dst`,
which stays for ever unless it carries a TTL of its own. Spec D-9 has the outcome per class.

**Scope.** A Redis, Valkey or Dragonfly master streams an expiry `DEL` (`RecordExpiryBlocking`,
`db_slice.cc:2159`), which removes the recreated key, so the same replica behaviour is transient
under them. KeyDB's own *active* replicas (`expireIfNeeded` falls through to the delete,
`db.cpp:2101`) and drakeydb peers (`PassesPeerEchoFilter` drops `kEntryFlagExpired`) share the
orphan. Options B (copy KeyDB's plain replica) and C (sweep only) of ledger decision 24 do not
reliably avoid it either. Under C the replica's own sweep usually reaps the due key first, and the
late write recreates it with no TTL, the same orphan. B converges only when the write lands before
KeyDB's slow reap (seconds to tens of minutes), the write then going with the stale object. They
were rejected as wider for every other class. A full resync (a `REPLICAOF` again, or a reconnect
that falls back to one) rebuilds the replica from the master's snapshot and drops the orphans.

**Related: D-27.** D-27 is the same mechanism between drakeydb peers: an unguarded delta that keeps
the TTL when the key exists and creates the key when it does not, authored before a deadline and
applied after it, re-creates the key with no TTL. This entry is its instance on a plain replica of
an active KeyDB, where the deadline is passed by the replica's own sweep and an active KeyDB never
streams the `DEL` that would clean up. Task 2.9 closes only that instance: a plain replica's main
link publishes the stream clock and runs the replica sweep (`ApplyReplicaActiveExpiry` returns for a
peer-mode or non-main link). A drakeydb peer, a classic peer link to an active KeyDB included, still
sweeps and serves reads on its local clock, so its share of the orphan stays open under D-27, whose
owner is the tombstone-lifecycle phase. Task 2.8 narrows it there too (an enveloped command runs at
its author's time), but only for a key the local sweep has not yet deleted.

**How established:** the live fake-master run above against the P7-1 build, and now the pytest
`test_plain_replica_of_active_keydb_keeps_a_ttl_less_orphan_of_a_ttl_keeping_write[sweep|access_only]`
and `ReplicaActiveExpiryTest.EveryCommandClassOfTheWindowHasItsDocumentedOutcome` (`PTTL == -1` on
every recreated key), both falsified by serving due keys as live (`task-1.4-report.md`, "Decision
31 round"). Not reproduced against a real KeyDB end to end: the stream's forms are the captured
ones (`tests/dragonfly/data/README.md`), and Task 2.9 adds a real-KeyDB rate-limiter test.

**Status:** interim, owner decision 31: documented in P7-1 (spec D-9, this entry, the P7-1 PR
description), not fixed there. **Owner:** P7-2 Tasks 2.8 and 2.9 (plan): the write then runs at its
author's time, before `E`, on a key the sweep, on the stream clock, has not deleted, so it finds the
key with its TTL, and the two pytests flip. They close it while the link is healthy; it stays open
while the stream is more than 60 s behind the local clock (the floor) or a stamp is unusable (the
local clock is kept). They close it for a plain replica only: when Task 2.9 lands this entry is
narrowed, not deleted. The plain-replica part is deleted (landed, per the register's rule) and the
peer and classic-peer share moves to D-27, which says so. **From:** P7-1 (Task 1.4; found by the
Opus re-review of `425eeb9`).

### D-33. `SORT .. STORE` of a missing or unsortable source aborted the server -- resolved

**Where:** `SortGeneric`'s fetch hops (`src/server/generic_family.cc`). P4-0 (`1b6a2e82`) made the
fetch callback return `fetch_result.status()`, so that a failed single-shard SORT is not
auto-journaled (`LogAutoJournalOnShard` skips a non-OK result). On a transaction of more than one
shard `Transaction::RunCallback` does `CHECK_EQ(OpStatus::OK, result)` on every hop
(`transaction.cc:771`, a `CHECK`, so release builds too). With `STORE`'s destination on another
shard than the source, a source that is missing (`KEY_NOTFOUND`), of the wrong type (`WRONG_TYPE`)
or holds elements a numeric sort cannot convert (`INVALID_NUMERIC_RESULT`) killed the server:
`Check failed: OpStatus::OK == result (0 vs. 2)`, `0 vs. 8`, `0 vs. 17`. Reachable by any client, by
any classic master's stream (raw or inside an RREPLAY envelope; the replica runs the SORT like a
client does), by the D-9 window, where a due source is a missing one on a flagged replica, and by
a DFLY-protocol replica or a peer whose shard count differs from its master's: a one-shard master
journals its failing `SORT .. STORE` verbatim (see the residual below) and the replay runs on the
replica's own shards, where the destination can be on another one than the source. With
the destination on the source's shard there was no abort, but a missing source replied an empty
array and left `dst` as it was, where Redis and KeyDB delete it and reply `:0`
(`sort.cpp:575-586`).

**Fixed in P7-1 by `1404897` (ledger decision 32):**

1. A multi-shard hop returns `OK` (`GetUniqueShardCnt() == 1 ? fetch_result.status() : OK`); the
   failure reaches `SortGeneric` through `fetch_result` either way. A single-shard SORT keeps
   returning it.
2. `SortStoreNothing` (upstream main has the function with the same two call sites; only this fork's
   `OpStore` takes the extra `source_deleted_by_fetch`): a missing source, or one the unsorted
   fetch's own lazy member expiry emptied, with STORE deletes `dst`, whatever its type or TTL, and
   replies `:0`, as Redis and KeyDB do. The delete is hand-journaled as `DEL dst`, on one shard too,
   and only when there was a `dst`. On one shard that `DEL` is the whole wire (M-4 of the review of
   `f281564`): the hop returns `OpStatus::SKIPPED`, which `LogAutoJournalOnShard` treats as "do not
   journal", so the verbatim `SORT` that the revived auto-journal would record behind it is left
   out. Before, a peer that held a newer `dst` dropped the LWW-guarded `DEL` and then deleted that
   `dst` anyway by replaying the `SORT` (D-18's class). The wire is now upstream main's and Redis's:
   `DEL dst`, or nothing when there was no `dst`. Across shards the hop still returns `OK` (a
   `CHECK` there) and the wire is unchanged.
3. A wrong-type or non-numeric source replies its error and leaves `dst` alone, as Redis and KeyDB
   do (both errors come before the destination is touched, `sort.cpp:278-285`, `:515`).

**Residual, not fixed:** on one shard a failing STORE (WRONGTYPE, non-numeric) still journals its
verbatim `SORT`. Measured on the P7-1 build (a one-shard master with one replica, the replica's
`slave_repl_offset` before and after each command): `SORT <string> STORE dst`, the same with `BY
nosort`, and `SORT <list of words> STORE dst` each advance it by 1, while `GET` and the same two
failing SORTs without STORE advance it by 0; the two sorted STORE cases measured again after M-4,
still 1 each. By reading, the fetch hop is not the last hop of a STORE form: the empty concluding
hop (`Conclude()`) is `OK`, and that is the one whose result `LogAutoJournalOnShard` sees. A
replica replays the entry, gets the same error and leaves `dst` alone, so nothing diverges; it
costs one journal entry per failed command. Across shards SORT stays `CO::NO_AUTOJOURNAL` and the
two failures journal nothing (pinned by `CrossShardStoreOfMissingSourceJournalsDestinationDelete`,
`multi_master_test.cc`).

The missing-source residual this entry first carried is closed by M-4: `SORT <missing> STORE dst`
advances the replica by 1 with a `dst` (the `DEL dst`) and by 0 without one, where it advanced it by
2 and 1 (the `DEL dst` and the `SORT`; the `SORT`); `BY nosort` likewise
(`SameShardStoreOfMissingSourceJournalsOnlyTheDestinationDelete`). One neighbour keeps the old
shape: a same-shard sorted `STORE` whose set the fetch itself emptied through member expiry (the
fetch succeeded with nothing, so it is not `SortStoreNothing`'s) journals `DEL src`, `DEL dst` and
then the verbatim `SORT` (3 entries measured; `FullExpirySortStoreJournalsDestinationDelete`). A
peer that holds a newer `dst` drops the guarded `DEL dst` and its replay of the `SORT` deletes it:
D-13's and D-18's same-shard exposure, not a new class, and left as it is.

**How established:** the abort was reproduced on `main`'s binary by the Opus review of `2bdf3d7`
(ledger decision 32; not re-run here). The fix is pinned by `GenericFamilyTest.SortStoreOf*`
(2 shards, `dst` on and off the source's shard, seven option forms, `BY nosort` and `BY` pattern
included), the journal tests `CrossShardStoreOfMissingSourceJournalsDestinationDelete` and
`SameShardStoreOfMissingSourceJournalsOnlyTheDestinationDelete` (`multi_master_test.cc`), and the
classic-stream pytest
`test_classic_stream_sort_store_of_an_unsortable_source_does_not_abort`. Each of the fix's parts is
falsified (`task-1.4b-report.md`, "Decision 32 (SORT .. STORE)"): the hop returning the failure
aborts with the three statuses above, the empty-array reply fails every missing-source test, and
`source_deleted_by_fetch=false` drops the same-shard `DEL dst`. The review round's change is
falsified the same way (`task-1.4b-report.md`, "Review of f281564: M-4 and docs"): `OK` from the
one-shard hop journals the verbatim `SORT` again, and `SKIPPED` without the one-shard gate aborts
the cross-shard cases (`0 vs. 4`).

**Owner:** none (resolved; the residual is journal noise, not divergence). **From:** P4-0
(`1b6a2e82`); found by the Opus review of `2bdf3d7` (C1), fixed in P7-1.

### D-34. `SORT` orders tied `BY` weights, a missing `ALPHA BY` weight and `BY nosort` unlike Redis and KeyDB -- fixed in P7-1

**Where:** `SortGeneric` and the ordering it sorts with (`src/server/generic_family.cc`; as read at
`fb037bb`: the `BY` weight fill and comparison around `:1977-1987` and `:2615-2621`, and the
`nosort` branch around `:2740`).

Four differences from Redis and from KeyDB's `sort.cpp`, which follows it:

1. **Ties under `BY`.** With `BY`, a `SortEntry`'s `key` holds the weight, so two elements with the
   same weight compare equal and `std::sort` leaves them in the order the fetch produced them (a
   set's iteration order, a list's position). Redis and KeyDB's `sortCompare` breaks a numeric tie
   on the element itself (`compareStringObjects(so1->obj, so2->obj)`, `sort.cpp:153-156`: "this
   way the result of SORT is deterministic"). Every `BY` form with ties is affected: shared
   weights, a pattern whose keys do not exist (every weight is 0), and `DESC` of either.
2. **`BY nosort` on a SET that is stored or scripted.** Redis and KeyDB sort such a set
   alphabetically (`dontsort && type == OBJ_SET && (storekey || lua)`: "so the result is
   consistent across scripting and replication", `sort.cpp:296-310`); a list and a sorted set keep
   their native order. Dragonfly keeps the set's iteration order.
3. **`BY nosort` on a LIST or a ZSET under `DESC`.** KeyDB walks the list from its tail and the
   zset by descending rank, and takes `LIMIT offset count` from that walk (`sort.cpp:356-380`,
   `:401-439`). Dragonfly ignored `DESC` on this path: the reply and the `STORE` came out ascending,
   and `LIMIT` counted from the head.
4. **A missing weight under `ALPHA BY`.** `sortCompare` puts a weight key that is absent, or not a
   string, before every present weight, the empty string included, and two missing weights tie
   (`sort.cpp:160-168`; `lookupKeyByPattern` returns NULL for both). Dragonfly mapped a missing
   weight to `""`, so "missing" and "present but empty" were the same: with `c` missing and `b`
   empty, `SORT sa BY aw_* ALPHA DESC` is `e a d f b c` on KeyDB and was `e a d f c b` here, and
   `SORT sa BY aw_* ALPHA LIMIT 1 3` is `b f d` and was `c f d`. (A numeric `BY` needs no such rule:
   a missing weight is the score 0 in both, and ties with a present `"0"`.)

**Why it matters:** a classic master replicates `SORT .. STORE` as the command, not as its result
(an active KeyDB wraps it in RREPLAY, verbatim), so the replica re-runs it and must order exactly
as the master did. Redis and KeyDB fixed the orders above precisely for that. A drakeydb replica of
any classic master therefore ends up with the same members in a different order, silently: the
link stays up, every counter is clean, and nothing ever corrects it. The difference is upstream
Dragonfly's; P7-1 is what puts active KeyDB's RREPLAY stream, and so its verbatim `SORT .. STORE`,
on every classic link. drakeydb to drakeydb did not diverge in the probes: both sides run the same
code, and a cross-shard `STORE` journals its computed `RESTORE` (D-13 covers the same-shard
recipe).

**How established (live):** the P7-1 adversarial pass (`sort_keydb.py`, C1): an active KeyDB
v6.3.4 master and three drakeydb plain replicas of 1, 2 and 4 shards. These converged: a missing
source (`dst` deleted), `dst == src` (list and set), `DESC LIMIT`, `GET nokey_*`, `MULTI`/`EXEC`,
`EVAL`, and odd numerics (`0x10`, `" 5"`, `1e3`, `+-inf`, the empty string). (A wrong-type or
non-numeric source converged only vacuously: a failing command is not replicated.) These diverged
on every replica, ten keys each, permanently:
`SORT s BY nokey_* STORE d` (KeyDB `a .. j`, replica `f i h g j c e d b a`),
`SORT s BY w_* STORE d` with tied weights (also inside `MULTI` and `EVAL`, and with
`GET # GET h_*`), `SORT l BY w_* STORE d` on a list with missing weights (the replica keeps list
order), `SORT s BY nosort [LIMIT 0 3] STORE d` on a set (KeyDB `a .. j` and `a b c`, the replica
the set's iteration order), `SORT s BY nokey_* DESC STORE d` and `SORT s BY w_* ALPHA STORE d`.
`SORT .. STORE` forms between drakeydb nodes (a meshed peer pair of 2 and 3 shards, and a DFLY
master and replica of 1 and 4, 4 and 1, and 3 and 2 shards, 25 forms) converged
(`sort_peers.py`, `sort_dfly.py`). Rules 3 and 4 were found by reading `sort.cpp` against the code
for decision 34 and measured the same way, on standalone KeyDB 6.3.4 and Redis 7.0.15 against the
build before the fix (80 of 115 forms differed).

**Status (2026-10-05): fixed in P7-1** (owner decision 34): drakeydb's `SORT` adopts the four rules
for every caller, reply and `STORE` alike, matching Redis and KeyDB. A client-visible change in
reply order for tied, nosort and missing-weight cases versus upstream Dragonfly, and an
upstreamable Redis-compatibility fix; it changes no journal wire (`SortStoreNothing`, `OpStore`'s
hand-journal and the single-shard auto-journal are as they were; only the order of what is stored
or replied changes). What changed, all in `generic_family.cc`:

- `SortEntry::less` breaks a tie on the element (`ResultKey()`, bytewise); `DESC` reverses the whole
  comparison, the tie-break included, as KeyDB negates `cmp`.
- An `ALPHA` entry carries a `weight_missing` bit, set from `OpFetchStringValue`'s new `found`
  out-parameter (absent, expired or not a string), and `less` puts a missing weight first.
- A SET under `BY nosort` with `STORE`, or inside a script (`conn_state.script_info` of the
  command's context, which a replicated apply has although it has no connection), is sorted `ALPHA`
  by the element with the `BY` dropped; `GET`, `DESC` and `LIMIT` apply after the sort. Any other
  SET under `nosort` keeps its iteration order, as in Redis.
- A LIST or ZSET under `BY nosort DESC` is reversed before `LIMIT` and `GET` are applied.

Checked after the fix: 123 forms against standalone KeyDB on 1, 2 and 4 shards, with the same
replies at every shard count. 93 equal; the 30 that differ are the `ALPHA BY` ties (below), a plain
`BY nosort` set under `LIMIT` (no order in either), `SORT_RO` (KeyDB 6.3.4 has none, and drakeydb
equals Redis 7.0.15 on it), and the differences listed below and in D-35. The adversarial probe
again against an active KeyDB master: the one form that still differs is `SORT s BY w_* ALPHA
STORE` (once per replica, the residual below), and without it `BAD 0`; `sort_peers.py` and
`sort_dfly.py` are clean.

**Found on the way, fixed in the same change (an ungated crash, upstream's):** `LIMIT 1 4294967295`
and `LIMIT 4294967295 1` are valid in Redis and KeyDB (everything from the offset on; nothing). Here
`offset + count` wrapped around a `uint32` in `GetSortRange` and in the partial sort's end, so the
range ended before it began: the plain form replied a garbage array length and the `BY`/`nosort`
forms killed the server (`SIGSEGV`). A client, or a KeyDB master's stream, could do it. Both sums
are 64-bit now.

**Residual, a documented limitation:** `ALPHA BY` ties. In Redis and KeyDB the order of two elements
with the same `BY` value under `ALPHA` is the order the master's sort received them in (`pqsort` is
not stable; a set's hash order differs even between two KeyDB processes, measured), which a replica
cannot reproduce, so a master and a replica can still disagree on those ties. drakeydb breaks them
on the element, so its own replicas and peers agree. See `docs/differences.md`.

**Also found while checking the forms, not orderings, left open** (each a difference from Redis and
KeyDB that a client sees; the first three can leave a replica of a KeyDB master with a different or
a stale `dst`, silently):

- `LIMIT` with a negative argument, or one beyond `uint32`, is the error `value is not an integer
  or out of range` here (upstream's `SortNegativeLimit` pins it). Redis and KeyDB accept them: a
  negative offset is 0, a negative count means all, a larger count is clamped. `SORT l LIMIT 0 -1
  STORE d` succeeds on a KeyDB master and fails on its drakeydb replica, which leaves `d` as it was.
- `GET <pattern>` of a key that does not exist replies the empty string here and nil in Redis and
  KeyDB (a `STORE` keeps `""` in both); a `GET` pattern without `*` reads that literal key here
  where Redis and KeyDB give nil: `SORT s GET str STORE d` stores `str`'s value here and `""` there.
- Numeric parsing, probed with 46 spellings as a `BY` weight. Equal for `0x1A`, `0x1p3`, `1e5`,
  `.5`, `5.`, `+5`, `-0`, a leading space/tab/newline, `inf`/`-Infinity`, `nan` (an error in both)
  and the empty string. KeyDB's `strtod` check rejects a trailing space, tab or newline (`"5 "`)
  and an out-of-range number (`1e400`, `1e-400`); drakeydb accepts both (a failing SORT is not
  replicated, so no replica diverges). KeyDB accepts a NUL byte after the number (`"5\0"`,
  `strtod` stops there); drakeydb rejects it, so that `SORT .. STORE` leaves a drakeydb replica's
  `dst` stale (binary weights only).
- Hash-field patterns (`BY w_*->field`, `GET h_*->field`): D-35.
- Not a drakeydb difference, noted because it breaks naive probes: Redis 7.0 and KeyDB 6.3.4 keep
  `errno` between commands, so after a numeric SORT of a value that overflows (`1e400`) every later
  numeric SORT of non-integer elements fails with "can't be converted" until some syscall changes
  `errno` (checked on both).

**Owner:** none (resolved; the residual is documented). **From:** the P7-1 adversarial pass (C1);
decision 34 in the ledger.

### D-35. `SORT` hash-field patterns (`->`) are unsupported; a classic stream that uses them diverges silently

**Where:** `PopulateSortEntriesFromByPattern` and `FetchGetPatternValues`
(`src/server/generic_family.cc`) build the weight or `GET` key by putting the element where the
first `*` is and read it as a string, so `->` is part of the key name. Redis and KeyDB's
`lookupKeyByPattern` (`sort.cpp:61-137`) read a pattern `key_*->field` as "the hash at
`key_<element>`, its field `field`", for `BY` and `GET` alike; a missing key, a key that is not a
hash and a missing field are all NULL. The fakeredis test `test_sort_with_hash`
(`tests/fakeredis/test/test_mixins/test_generic_commands.py`) is marked
`unsupported_server_types("dragonfly")` for the same reason.

**How established (live, standalone KeyDB v6.3.4 against the P7-1 build, 2 shards):** a set `s` of
`a .. j` and a hash `hw_<c>` per element with `f` = the letter's code mod 3 and `g` = `G<c>`.
`SORT s BY hw_*->f` is `c f i a d g j b e h` on KeyDB (weights 0 0 0 1 1 1 1 2 2 2, ties on the
element) and `a b c d e f g h i j` here: the key `hw_a->f` never exists, so every weight is
missing, 0, a tie. `SORT s ALPHA GET hw_*->g` is `Ga .. Gj` on KeyDB and ten empty strings here.
With `STORE d` the same two lists land in `d`. An active KeyDB master replicates such a `SORT ..
STORE` verbatim, so a drakeydb replica of it ends with a different `d` and nothing says so: the link
stays up and every counter is clean (the class of D-34, for a command D-34's fix does not cover).

**Status:** open; owner decision pending. Not fixed in P7-1 (decision 34 covers orderings only).
`SORT` is a command a classic stream carries, so a stream that uses `->` diverges until it is
supported, or until the choice is made to document it as a limitation of onboarding from KeyDB.
**Owner:** the owner's decision (pending). **From:** the P7-1 SORT-ordering work (decision 34),
probe `probe.py` cases `hash_by_field*` and `hash_get_field*`.
