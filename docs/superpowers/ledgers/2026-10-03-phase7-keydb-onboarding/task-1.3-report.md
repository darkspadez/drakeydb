# Task 1.3 report: KeyDB-only and unknown commands, per-link counters, INFO and Prometheus

Branch `feat/phase7-1-rreplay-unwrap`, started at HEAD `558312e`. Not committed (the orchestrator
commits). Implementer: Sonnet. Plan: Task 1.3; spec D-7, D-13, D-15; ledger decisions 8, 14, 15, 22.
Scratch material (good copies of every touched file, build logs, the per-falsification outputs) is
in the orchestrator's scratchpad under `impl-p71b/` (`mut-*.txt`, `good13/`).

## Summary

- A KeyDB-only command (`IsKeyDbOnlyCommand`, the spec D-7 list: the member-expiry family,
  `PERSIST key subkey`, five `KEYDB.*` commands, `RREPLAY`) is skipped, counted in
  `keydb_cmds_dropped` and warned about at a limited rate, on the raw stream **and** inside
  envelopes. A command inside an envelope that the dispatcher has no entry for is skipped before
  dispatch and counted in `classic_unknown_cmds_dropped` (it used to be a `classic_apply_errors`
  from the dispatcher). `KEYDB.MVCCRESTORE` is the second kind, not the first (decision 22), and a
  test pins it.
- Every counter bump feeds the link's `ClassicLinkStats` and the process-wide `ClassicTotals()`.
  `ReplicaSummary` carries `classic_link`, `master_active_replica` and a `ClassicLinkCounts classic`.
- INFO shows the counters in the plain-replica block (`key:value`) and on a peer line
  (`,key=value`, then `,repl_offset=`), and `/metrics` exports `dragonfly_<name>_total`, on a plain
  replica too. All of it behind spec D-13's predicate: nothing for a stock master with every
  counter zero, nothing for a DFLY link.
- The boot limitations warning of an `--active_replica` node says KeyDB member TTLs and cron jobs
  are dropped on onboarding.
- 8 new gtests in `classic_replay_test` (26 became 34), 2 in `multi_master_test` (220 became 222),
  14 new pytest cases (the file collects 50). Every change is falsified (table below).

## What changed

| File | Change |
|---|---|
| `src/server/classic_replay.h` | `IsKeyDbOnlyCommand`; `ClassicLinkStats` gains `keydb_cmds_dropped`, `classic_unknown_cmds_dropped` and `Snapshot()`; `ClassicTotals()`; `ClassicCounterValue`, `ClassicLinkShown`, `ClassicMasterActive`, `ClassicLinkFields`, `ClassicTotalSeries`; `ClassicApplier::SkipKeyDbOnly` (public) and `Count` (private); comments of `rreplay_unwrapped` and `ApplyCommand` brought up to date. Includes `replica_types.h`. |
| `src/server/classic_replay.cc` | The counter table `kClassicCounters` (name, help, member of the atomics, member of the snapshot: the single place INFO, peer lines and `/metrics` get the list from, in spec D-13's order), the KeyDB-only name table, the predicates, `SkipKeyDbOnly`, `Count` (the four existing bumps go through it), and `ApplyCommand`'s two new skips (KeyDB-only, then the unknown check with `CommandRegistry::FindExtended`, the dispatcher's own lookup). |
| `src/server/replica_types.h` | `struct ClassicLinkCounts`; `ReplicaSummary` gains `classic_link`, `master_active_replica`, `classic`. |
| `src/server/replica.{h,cc}` | `classic_master_` (set at the end of `Greet()`, not cleared by a reconnect); `GetSummary()` fills the three fields (as first built it read `master_active_replica_` only with `R_GREETED` set; the review fix round below replaced that with a sticky per-link flag); the raw path's queuing condition gains `&& !classic_applier.SkipKeyDbOnly(last_args)`; the comment of `master_active_replica_` says who reads it now. |
| `src/server/server_family.cc` | Plain-replica block of `FormatInfoMetrics`: `ClassicLinkFields(rinfo)` after `psync_successes`. (As first built, `GetMetrics` also computed `Metrics::classic_master_active` from the replica and peer summaries; the review fix round removed it.) |
| `src/server/metrics.cc` | One loop in `Print` after the replica/master branches that emits `ClassicTotalSeries(ClassicTotals().Snapshot(), ActiveKeyDbMasterSeen())` as `<name>_total` counters (as first built it also added `Metrics::classic_master_active` to `metrics.h`, removed in the review fix round). |
| `src/server/multi_master.cc` | `RenderPeerReplicationInfo`: for a shown classic link the fields and `,repl_offset=`; the boot warning's new sentence. |
| `docs/UPSTREAM-SYNC.md`, `docs/PLAN.md` | Rows for the touched upstream files (`replica`, `server_family`, `metrics`, new `replica_types.h`, and the fork-only `classic_replay` row). |
| spec D-7, D-13, D-15; plan Task 1.3 | `FindExtended`, not `FindCmd`; the db-before-check order; "as built" paragraph of D-13; D-15 rows for the new tests and their falsifications; the plan names the render tests as built. |

`ServerState::Stats` is untouched.

## Design notes

1. **One table, three renderers.** INFO (`key:value`), the peer line (`,key=value`) and `/metrics`
   (`<name>_total`) read the same `kClassicCounters` through `ClassicLinkFields` and
   `ClassicTotalSeries`; later tasks add a counter with one line there and one field in the two
   structs. `ClassicLinkCounts` is a plain struct in `replica_types.h` (a summary is a copyable
   value); `ClassicLinkStats` keeps its named atomics, so the Task 1.2 tests did not change.
2. **Process-wide totals** are a function-local `static ClassicLinkStats` (`ClassicTotals()`), bumped
   by `Count` beside the per-link atomic. Relaxed monotonic atomics only, outside
   `ServerState::Stats` on purpose (spec D-13, D-1.13: that struct is per thread and its size is
   static_asserted; as first built the comment compared them to `multimaster_lww_dropped`, which is
   a per-thread `ServerState::Stats` field, so that was wrong); the comment says why (a link's fiber
   and the fiber that renders run on different threads, and a per-thread counter neither follows a
   link nor survives a `Replica` that is gone). A Prometheus counter must not fall when `REPLICAOF NO ONE` destroys the
   link, which is why `/metrics` reads the totals and INFO reads the link.
3. **The predicate is per field** (spec D-13 says "whose own counter is nonzero"): an active KeyDB's
   link shows all six counters, zeros included; any other classic link shows the counters that
   moved, and on a peer line `repl_offset` follows when anything is shown. A non-active KeyDB that
   sends `PEXPIREMEMBERAT` raw therefore shows `keydb_cmds_dropped:1` and nothing else
   (`test_scripted_stock_master_raw_keydb_only_command_shows_only_that_counter`).
4. **`classic_link` vs `master_active_replica`.** `classic_link` is the protocol of the last
   *completed* Greet, so a link that is down keeps showing the counters it has. As first built
   `master_active_replica` was read only while `R_GREETED` was set, so a brand-new link with every
   counter zero stopped showing its six zeros while it was down, against the spec's "shown whatever
   its state" (review M-1). It is now a sticky per-`Replica` flag, `classic_master_was_active_`: set
   at the end of a completed `Greet()` beside `classic_master_` when the master answered
   `active-replica`, and never cleared for that `Replica` (a reconnect, or a failed `Greet()`, leave
   it), so INFO shows the six zeros (and in peer mode the offset) while the link is down. The
   per-Greet `master_active_replica_` (cleared at every `Greet()`) stays for Task 1.4's expiry
   decision, which must read it only once `R_GREETED` is set. One edge: a `Replica` that once had an
   active master and whose endpoint later turns into a non-active one keeps showing zeros (the
   process restarts, or the link is removed and re-added, to drop it).
5. **Unknown check on the envelope path only, after the db selection.** `HandleRreplay` selects each
   layer's db while it validates the layer (Task 1.2 fix round), and `ApplyCommand` runs after that,
   so a KeyDB-only or unknown leaf leaves the db its layers selected, as KeyDB selects before it
   knows the command. `KeyDbOnlyAndUnknownLeavesKeepTheSelectedDb` pins it (db 3, db 5, and a nested
   db 7 under db 2). The lookup is `FindExtended(ParsedArgs{cmd})` so that `ACL <sub>` is judged as the
   dispatcher judges it. The raw path keeps upstream's unknown accounting (`unknown_` in INFO
   commandstats): `test_scripted_stock_master_raw_...` sends a raw `NOSUCHRAWCMD` and sees it there
   and not in `classic_unknown_cmds_dropped`.
6. **`KEYDB.MVCCRESTORE`** is not in `kKeyDbOnlyCommands` and is not in the registry, so it lands in
   `classic_unknown_cmds_dropped` until P7-2 Task 2.6 translates it. Three tests pin that (the table,
   the family test with its own comment, the scripted exact-counter stream), and F2 shows the
   alternative: `keydb_cmds_dropped` 3, unknown 1.
7. **The series gate** (review I-1, owner decision 25). As first built,
   `Metrics::classic_master_active` was computed in `GetMetrics` from `replica_side_info` or
   `peers_->Summaries()`, which hopped to every peer's thread under `PeerReplicationManager::mu_` on
   every INFO and scrape of an active node, just to decide this. It is now a process-wide sticky flag, `NoteActiveKeyDbMaster()` /
   `ActiveKeyDbMasterSeen()` in `classic_replay.{h,cc}`, set by `Greet()` and never cleared; `Print`
   reads it directly, and `Metrics` is as it was (the `static_assert` comments in `metrics.cc` are
   true again). It deviates from spec D-13's per-link wording on purpose.
8. **Warnings** are `LOG_EVERY_T(WARNING, 60)` (spec D-7), naming the link and the command (escaped,
   32 bytes at most), one site for KeyDB-only (shared by the raw and the envelope path) and one for
   unknown commands. `LOG_EVERY_T` is per call site, not per link: the second link's first drop within
   a minute is not logged, only counted.

## Tests

### gtest (`cd /home/user/drakeydb/build-dbg && nice ninja -j3 classic_replay_test multi_master_test && ./classic_replay_test && ./multi_master_test`)

`classic_replay_test.cc` (8 new, 1 changed):

- `ClassicReplayTest.IsKeyDbOnlyCommandTable`: 37 spellings; each name with and without case;
  `PERSIST k m` true, `PERSIST k`, bare `PERSIST`, `PERSIST k m x` false; `KEYDB.MVCCRESTORE` false;
  near misses (`KEYDB.CRONX`, `EXPIREMEMBERS`, `PEXPIREMEMBER`, ...); an empty vector and a non-string
  name are false and do not throw.
- `ClassicReplayTest.ClassicLinkFieldsFollowTheRenderPredicate` and
  `.ClassicTotalSeriesFollowTheRenderPredicate`: all six in order for an active master, none for a
  stock master with zeros, the moved counters only otherwise, nothing for a non-classic link.
- `ClassicApplyFamilyTest.KeyDbOnlyDroppedAndCounted`: ten spellings in db 3, each one
  `keydb_cmds_dropped`, none unknown or an apply error, `UknownCmdMap()` empty (none reached the
  dispatcher), `PERSIST k m` leaves the TTL and `PERSIST k` clears it, the stream carries on.
- `.UnknownInnerCommandCountedNotDispatched`: an unknown command, one in lower case, `ACL NOSUCHSUB`;
  a known command that applies and one the dispatcher rejects (wrong arity) are not unknown.
- `.KeyDbMvccRestoreCountedUnknownUntilItIsApplied`: decision 22.
- `.KeyDbOnlyAndUnknownLeavesKeepTheSelectedDb`: the db after a dropped leaf, three cases.
- `.CountsAlsoFeedTheProcessWideTotals`: deltas of `ClassicTotals()` equal the link's six counters.
- `.RejectedDispatchCountsAndConsumes` changed: its `NOSUCHCMD` assertion (counted in
  `classic_apply_errors` "until a later task") moved to the new unknown test.

`multi_master_test.cc` (2 new): `PeerReplicationInfo.ShowsClassicFieldsOnlyForClassicLinks` (a KeyDB
link with every field and `repl_offset=1234`, a DFLY link with identical numbers shows none, no peer
lines for a non-privileged viewer) and `.OmitsClassicFieldsWhenMasterNotActiveAndCountersZero` (the
pre-change line byte for byte, one moved counter and the offset, a quiet active KeyDB with six zeros).
They are plain `TEST`s rather than `MultiMasterFamilyTest.Render...` (the plan's names): a pure render
needs no service.

### pytest (`keydb_onboarding_test.py`, 14 new cases)

- `test_keydb_only_commands_dropped_with_counters` (real KeyDB): `SADD`, `EXPIREMEMBER`, `KEYDB.CRON`,
  a plain `SET`; the set arrives whole, `keydb_cmds_dropped == 2`, no unknown, no apply error, the node
  up, one full sync.
- `test_info_and_metrics_show_classic_counters[plain_replica|peer_mode]` (real KeyDB): the six fields in
  order (the `master0` line with `repl_offset` in peer mode), the six series, each at least the link's
  count.
- `test_scripted_active_master_exact_classic_counters[plain_replica|peer_mode]`: one stream with every
  outcome behind a master that says `active-replica`: exact INFO and `/metrics` (unwrapped 5,
  malformed 1, KeyDB-only 2 (one envelope, one raw), unknown 2 (`NOSUCHCMD`, `KEYDB.MVCCRESTORE`),
  apply errors 1), the settled ACK exact, peer `repl_offset` equal to it.
- `test_scripted_quiet_active_master_shows_zero_counters[plain_replica|peer_mode]`: six zeros before any
  envelope.
- `test_scripted_stock_master_raw_keydb_only_command_shows_only_that_counter`: raw
  `PEXPIREMEMBERAT`/`pexpirememberat`, a raw unknown command; `{keydb_cmds_dropped: 2}` in INFO and
  `/metrics` and nothing else, the unknown one in `unknown_` of commandstats, the ACK exact.
- `test_info_and_metrics_absent_for_stock_master` (Redis 7.0.15), `..._absent_for_keydb_that_is_not_active
  [plain_replica|peer_mode]`, `..._have_no_classic_fields_between_dfly_nodes[plain_replica|peer_mode]`: no
  classic field or series, and the link's block still ends where it did (`psync_successes`,
  `clock_skew_ms`), which catches a stray appended field as well as a named one.
- `test_active_replica_boot_warning_names_keydb_drops`: the sentence in an `--active_replica` node's
  log, none in a plain node's.

## Step 2: failing before (stubs of the new interfaces, no behavior)

The new declarations were compiled with trivial bodies (`IsKeyDbOnlyCommand` false, empty
`Snapshot()`, no rendering), so that the tests failed at their assertions rather than at compile time.

- `./classic_replay_test`: 8 failed, 26 passed. `CountsAlsoFeedTheProcessWideTotals`,
  `KeyDbMvccRestoreCountedUnknownUntilItIsApplied`, `KeyDbOnlyAndUnknownLeavesKeepTheSelectedDb`,
  `KeyDbOnlyDroppedAndCounted` (`link.keydb_dropped()` 0 vs 1), `UnknownInnerCommandCountedNotDispatched`,
  `ClassicLinkFieldsFollowTheRenderPredicate`, `ClassicTotalSeriesFollowTheRenderPredicate`,
  `IsKeyDbOnlyCommandTable`.
- `./multi_master_test --gtest_filter='PeerReplicationInfo.*'`: the two new tests fail
  (`multi_master_test.cc:446`, `:477`, `:484`), `RendersCountsAndPeerLines` passes.
- pytest of the new cases, `DRAGONFLY_PATH=/home/user/drakeydb/build-dbg/dragonfly`: 9 failed, 5 passed.
  The failures are the keydb-only test (`KeyError: 'keydb_cmds_dropped'`), both info/metrics tests, both
  scripted exact tests, both quiet tests, the scripted raw test and the boot warning test. The five that
  pass are the absence tests, which a stub cannot fail; they are falsified by F6 and F6b below.

## Step 4 falsification (each change made alone, built, run, restored from a saved copy and diffed)

`nice ninja -j3 classic_replay_test multi_master_test dragonfly`, then `./classic_replay_test`,
`./multi_master_test --gtest_filter='PeerReplicationInfo.*'`, and `flock /tmp/drakey-pytest.lock env
DRAGONFLY_PATH=/home/user/drakeydb/build-dbg/dragonfly KEYDB_SERVER_PATH=<scratchpad>/KeyDB/src/keydb-server
KEYDB_REQUIRED=1 /root/drakey-venv-pinned/bin/python -m pytest tests/dragonfly/keydb_onboarding_test.py -k <..>`.

| Change | Observed |
|---|---|
| **F1** `IsKeyDbOnlyCommand` returns false (plan) | gtest: `IsKeyDbOnlyCommandTable` (`PEXPIREMEMBERAT / 4`: false vs true, and every other name), `KeyDbOnlyDroppedAndCounted` (`keydb_dropped()` 0 vs 1), `KeyDbOnlyAndUnknownLeavesKeepTheSelectedDb`, `CountsAlsoFeedTheProcessWideTotals`. pytest: real KeyDB `assert 0 == 2` (`keydb_cmds_dropped`); scripted exact: unknown 3 vs 2 and `keydb_cmds_dropped` 0 vs 2 (both modes); the raw test fails too |
| **F2** `KEYDB.MVCCRESTORE` added to the KeyDB-only names (decision 22) | gtest: `IsKeyDbOnlyCommandTable` (`KEYDB.MVCCRESTORE / 5`: true vs false), `KeyDbMvccRestoreCountedUnknownUntilItIsApplied`. pytest scripted exact (both modes): `keydb_cmds_dropped` 3 vs 2, `classic_unknown_cmds_dropped` 1 vs 2 |
| **F3** the pre-dispatch unknown check removed (`if (false)`) | gtest: `UnknownInnerCommandCountedNotDispatched` (`unknown_dropped()` 0 vs 1, `apply_errors()` 1 vs 0), `KeyDbMvccRestoreCountedUnknownUntilItIsApplied`, `KeyDbOnlyAndUnknownLeavesKeepTheSelectedDb`, `CountsAlsoFeedTheProcessWideTotals`. pytest scripted exact: `classic_apply_errors` 3 vs 1, unknown 0 vs 2 |
| **F4** `PERSIST` matched whatever its arity | gtest: `IsKeyDbOnlyCommandTable` (`PERSIST / 2`), `KeyDbOnlyDroppedAndCounted` (`PERSIST k` dropped, the TTL stays) |
| **F10** the boot warning's new sentence removed (run with F4) | pytest `test_active_replica_boot_warning_names_keydb_drops`: `the boot limitations warning does not name the drops`, `find_in_logs(...) == []` |
| **F5** INFO branch of the plain-replica block removed (plan) | pytest, plain variants only: `test_info_and_metrics_show_classic_counters[plain_replica]` (`[]` vs the six names), scripted exact and quiet (`{}` vs the counters), the scripted raw test; the peer variants pass |
| **F5b** peer-line branch removed (`if (false)`) | gtest `PeerReplicationInfo.ShowsClassic...` and `.OmitsClassic...`; pytest peer variants only (`[]` vs the six names; `{}` vs the counters) |
| **F6** rendered unconditionally (`ClassicLinkShown` true, `ClassicLinkFields` all with zeros) (plan) | gtest `ClassicLinkFieldsFollowTheRenderPredicate` (`ClassicLinkShown(stock)` true vs false), and the pre-existing `PeerReplicationInfo.RendersCountsAndPeerLines` plus both new peer tests (a DFLY peer line gains the fields). pytest: all five absence tests (`'rreplay_unwrapped' not in {...}`: stock Redis, non-active KeyDB plain and peer, DFLY pair plain and peer) and the scripted raw test |
| **F6b** the series unconditional (`ClassicTotalSeries(totals, true)`) | gtest `ClassicTotalSeriesFollowTheRenderPredicate`; pytest the same six absence tests (`classic_series` not `{}`) |
| **F7** the series emitted only when `!m.replica_side_info` (the replica-side Prometheus branch omitted) (plan) | pytest, plain variants only: `show_classic_counters[plain_replica]` (`classic_series` `{}`), scripted exact and quiet (plain), the scripted raw test; the peer variants pass |
| **F8** `Count` not feeding `ClassicTotals()` | gtest `CountsAlsoFeedTheProcessWideTotals` (`after - before` 0 vs 4 for `rreplay_unwrapped`, 0 vs 1 for `rreplay_malformed`, ...). pytest `show_classic_counters` (both modes: `assert 0 == 1`, the series are zeros) and scripted exact (both) |
| **F9** the raw-path check removed (`true` for `!SkipKeyDbOnly(...)`) | pytest scripted raw test (`{}` vs `{'keydb_cmds_dropped': 2}`) and scripted exact (`keydb_cmds_dropped` 1 vs 2, both modes) |
| **F11** `ClassicLinkShown` ignoring `master_active_replica` | gtest `ClassicLinkFieldsFollowTheRenderPredicate` (0 vs 6 fields for a quiet active master), `PeerReplicationInfo.OmitsClassic...`; pytest both quiet variants |
| **F12** a dropped command (KeyDB-only, unknown) resetting the db to 0 | gtest `KeyDbOnlyAndUnknownLeavesKeepTheSelectedDb` (`db_index` 0 vs 3, 0 vs 5), nothing else |

## Final results (debug build, gcc 13.3, `nice ninja -j3 classic_replay_test multi_master_test dragonfly server_family_test peer_replication_test`, no warnings)

- `./build-dbg/classic_replay_test`: `[  PASSED  ] 34 tests.`
- `./build-dbg/multi_master_test`: `[  PASSED  ] 222 tests.`
- `./build-dbg/peer_replication_test`: `[  PASSED  ] 23 tests.`
- `./build-dbg/server_family_test`: 43 passed, 1 failed: `ServerFamilyTest.GetTcpSocketInfoIPv6`
  (`Failed to create IPv6 socket`, the sandbox has no IPv6; the test opens a socket and does not touch
  anything changed here; not run on `558312e` to compare).
- `keydb_onboarding_test.py` and `keydb_harness_test.py` (real KeyDB, `KEYDB_REQUIRED=1`): 71 passed in
  100 s (50 + 21).
- `redis_replication_test.py -m "not large"`: 12 passed, 7 deselected, in 62 s.
- `multimaster_test.py -k "info or metrics or classic or keydb or greet or capa or skew or peer_lines or
  wait_for_peers"`: 23 passed, 54 deselected, in 35 s.
- `pre-commit run --files <the changed files>`: see Checks.

## Deviations from the brief, plan and spec, with reasons

1. **Warning rate is 60 s, not 1 s.** Spec D-7 says `LOG_EVERY_T(WARNING, 60)` for KeyDB-only drops, the
   file's `NoteMalformed` uses 60, and the brief allowed "the file's existing idiom".
2. **The `/metrics` loop is one block after the replica/master branches**, not one in each (plan
   Step 3). It covers both, and a node promoted with counters still nonzero keeps exporting them (a
   counter series that vanishes on `REPLICAOF NO ONE` is worse). The replica-side branch's omission is
   still a falsification (F7) because the block is conditioned on the same predicate.
3. **No process-wide counters in INFO yet.** Spec D-13 puts them beside `multimaster_lww_dropped` and
   after the per-link fields, but the ones it names (`multimaster_rreplay_deduped`, ...) belong to
   P7-2 and P7-4. The totals Task 1.3 creates are exported only as `/metrics` series, the per-link
   values being in INFO already. The spec's "as built" paragraph says so.
4. **Render tests are `PeerReplicationInfo.*`**, plain `TEST`s, not `MultiMasterFamilyTest.*` (no
   service needed).
5. **The lookup is `FindExtended`**, not `FindCmd` (spec D-7, plan): `FindCmd(name)` expects an upper
   case name and knows nothing of `ACL <sub>`; the dispatcher's own lookup is the one to mirror.
6. **`ReplicaSummary` carries a nested `ClassicLinkCounts`**, not flat fields (spec: "the per-link
   fields"): the struct is shared by the summary and `ClassicLinkStats::Snapshot()` and the table
   needs a member pointer to it.
7. **Extras beyond the plan:** the per-field predicate (design note 3), the `classic_link`/`R_GREETED`
   split (4), the scripted exact-counter and quiet-link tests, the raw-path test, the DFLY-pair and
   non-active-KeyDB absence tests, the "block still ends where it did" assertion, and the totals test.

## Not done / not verified

- No release, ASAN or UBSAN build. No D-12 throughput measurement: the raw path now asks
  `IsKeyDbOnlyCommand` of every command it queues (nine case-insensitive name compares and the
  `PERSIST` arity test; they compare lengths first), and the envelope path does one more
  `FindExtended` per command, which upper-cases the name into a temporary a second time (the
  dispatcher does it again). Task 1.5 is where that gets measured.
- **A byte-for-byte comparison of a stock master's INFO and `/metrics` against `558312e`** was not made:
  the absence tests pin that no classic name appears and that the link's block ends where it did, and the
  new code paths are empty for a stock link by construction, but no old build was kept to diff against.
- `server_family_test.GetTcpSocketInfoIPv6` fails here for lack of IPv6 (see above).
- The new gtests do not render the plain-replica INFO block (it needs a live link): pytest covers it.
- *(Superseded by the review fix round: INFO's `master_active_replica` no longer drops while a link reconnects, design note 4.)*

## Open risks and what a reviewer should attack

1. **`KEYDB.MVCCRESTORE` is on the wrong counter on purpose.** P7-2 Task 2.6 must take it out of the
   pre-dispatch unknown check (translate it first) and then update
   `KeyDbMvccRestoreCountedUnknownUntilItIsApplied` and the expected values of the scripted exact-counter
   stream, whose two "unknown" commands are `NOSUCHCMD` and `KEYDB.MVCCRESTORE`.
2. *(Removed by the review fix round: `GetMetrics` no longer calls `peers_->Summaries()`.)*
3. *(Removed by the review fix round: `Metrics::classic_master_active` no longer exists.)*
4. **`LOG_EVERY_T` is per call site**: one warning a minute for all links together, per kind.
5. **The raw path now drops `PERSIST k m`, `RREPLAY` and the `KEYDB.*`/`EXPIREMEMBER*` names from any
   master**, a stock one included. No Redis or Valkey command has those names, but a future one
   could; `IsKeyDbOnlyCommandTable` lists them for whoever adds a name.
6. **Counters of a replica a `REPLICAOF NO ONE` destroys** survive only in the totals (and so in
   `/metrics`), not in INFO; INFO of a promoted node shows none of them.

## Checks

`pre-commit run --files <the source, test and doc files above>`: `pyflakes`, `trim trailing whitespace`,
`fix end of files`, `check python ast`, `Clang formatting`, `black` all Passed. The local `clang-format`
is v18 and the hook's is v14: running the former by hand reformatted unrelated lines (`OnLink`,
two `GetMC` captures, a `(total)*100.0`), which the hook then restored; only the hook's output is in the
tree, and the C++ files tested are byte-identical to the ones that went through it.

## Review fix round (Opus review of `8865c43`)

Same branch, HEAD `8989d04` (`8865c43` plus a ledger-only commit), still uncommitted; `helio/`
untouched. Findings I-1, M-1, M-2, M-3, M-4, M-5, M-7 fixed, M-6 recorded (plan only), and the
pre-existing crash U-15 (owner decision 26) fixed with an audit. Scratch material (the 47- and
31-case probes and their logs, the falsification logs, a good copy of every touched file, the
byte-identity run `out/fix`) is in the orchestrator's scratchpad under `u15/` and
`rev-p713/bytecmp/out/fix`.

### What changed, per finding

| Finding | Change |
|---|---|
| **I-1** (series gate hops to every peer thread) | `classic_replay.{h,cc}`: a process-wide `std::atomic<bool>` behind `NoteActiveKeyDbMaster()` / `ActiveKeyDbMasterSeen()` (relaxed, never cleared, beside `ClassicTotals()`). `Replica::Greet()` calls the first at its end, when a classic master answered `active-replica`. `Metrics::Print` passes `ActiveKeyDbMasterSeen()` to `ClassicTotalSeries`, whose rule is now: the flag is set, or the total is nonzero. `GetMetrics` lost its block (no `peers_->Summaries()`, no `rng::any_of`). `Metrics::classic_master_active` is deleted. Spec D-13 and its "As built" paragraph say the gate deviates from the per-link wording (decision 25). |
| **M-2** (stale `static_assert` comments) | Resolved by the deletion: `git diff 8865c43^ -- src/server/metrics.h` is empty, and `metrics.cc` differs from `8865c43^` by the include and the one loop. 8865c43 never edited the two `static_assert` comments ("22 fields ... + 4-byte alignment padding"), which are true again without the field. |
| **M-1** (down link hides its zeros) | `replica.{h,cc}`: a per-`Replica` `classic_master_was_active_`, set at the end of a completed `Greet()` beside `classic_master_` when the master answered `active-replica`, never cleared for that `Replica`. `GetSummary()` reports `master_active_replica = classic_master_ && classic_master_was_active_` (no `R_GREETED` gate). `master_active_replica_` (per Greet, cleared at its top) stays for Task 1.4, and its comment says INFO no longer reads it. `replica_types.h` comment updated. Spec "As built" and the report's note 4 rewritten. |
| **M-3** (wrong precedent in a comment) | `classic_replay.h`: `ClassicTotals()` cites spec D-13 and D-1.13 (`ServerState::Stats` is per thread and its size is `static_assert`ed). The report's note 2 records that `multimaster_lww_dropped` is a per-thread `Stats` field. |
| **M-4** (`PERSIST k m` in db 3) | `KeyDbOnlyDroppedAndCounted`: `PERSIST k m` is applied in db 0, where `k` is. The TTL assertion for it **cannot fail** (below), so it was not kept: the drop is pinned by `keydb_dropped` +1 with `apply_errors` 0 (dispatched, it is an arity error and counts as an apply error), and the test says so. The one TTL assertion left is the one that can fail: `PERSIST k` clears it. |
| **M-5** (`linked()` too early) | `wait_for_synced_link` (keydb_onboarding_test.py): alive, `up`, not syncing, and the offset the sync ended at (`slave_repl_offset` of the plain block, the peer line's `repl_offset`). The quiet test and the new tests use it. See Notes for why `sync_in_progress=0` alone is not enough on a peer link. |
| **M-7** | `RenderPeerReplicationInfo` calls `ClassicLinkFields` once and renders when it is non-empty (`ClassicLinkFields` is empty exactly when the link is not shown, which its comment says). `AnyCounterMoved` uses `ranges::any_of` (and `<iterator>`, added only for `begin`/`end`, is gone). `"RREPLAY"` stays in `kKeyDbOnlyCommands` with a comment: kept per D-7, unreachable today because the stream loop and `HandleRreplay` take an envelope apart first. |
| **M-6** (record only) | Plan Task 3.1, hazard (j): a raw KeyDB-only command dropped behind queued commands is counted when read but its bytes are deferred onto the batch, so a link that stops before the flush replays it on a partial resync and counts it twice. Options: count at flush time, or accept and document. |
| **U-15** | `server_family.cc`: an anonymous-namespace `ReplyIfNoConnection(cmd_cntx)` (replies `No connection`, the error `CLIENT SETINFO` already gives) and one `// drakeydb: U-15` call at the top of `ClientSetName`, `ClientGetName`, `ClientInfo`, `ClientId`, `ClientKill`, `ServerFamily::Auth`, `Info`, `Hello`, `ReplConf`. Beyond `server_family.cc` (noted in the register and the spec): `main_service.cc` (`Quit` closes only a connection that exists; `Monitor`, `Subscribe`, `PSubscribe` reply `No connection`) and `dflycmd.cc` (`DFLY THREAD`). ISSUE-REGISTER U-15, spec byte-identity item 2, D-15 rows and file map, UPSTREAM-SYNC rows (`replica.cc`, `server_family.cc`, `main_service.cc`, `dflycmd.cc`, `metrics.cc`). |

### Tests added or changed

gtest (`classic_replay_test.cc`, 34 became 56):

- `ClassicNoConnectionTest.ReplicatedApplyOfAConnectionCommandIsAnErrorNotACrash/<case>` (19 cases: `Info`,
  `InfoSection`, `ClientSetName`, `ClientGetName`, `ClientInfo`, `ClientId`, `ClientKill`, `Auth`, `AuthUser`,
  `Hello`, `HelloSetName`, `ReplconfListeningPort`, `ReplconfCapaDragonfly`, `Quit`, `Monitor`, `Subscribe`,
  `Ssubscribe`, `Psubscribe`, `DflyThread`): the command is dispatched the way the raw path of the stream
  does, on a context with no connection, and replies `No connection` (`OK` for `QUIT`). Raw dispatch, because
  an envelope skips `REPLCONF` as a control command.
- `ClassicApplyFamilyTest.ReplicatedMonitorAndSubscribeLeaveNothingForClientsToTripOver`: envelopes with
  `MONITOR`, `SUBSCRIBE`, `PSUBSCRIBE` are three apply errors, what follows applies, and a client's `SET` and
  `PUBLISH` afterwards neither crash nor reach a subscriber.
- `ClassicApplyFamilyTest.InfoInAnEnvelopeIsAnApplyErrorNotACrash`: one apply error, the commands around it
  applied.
- `ClassicReplayTest.ActiveKeyDbMasterSeenSticksOnceNoted`.
- `ClassicApplyFamilyTest.KeyDbOnlyDroppedAndCounted` changed (M-4).

pytest (`keydb_onboarding_test.py`, 50 became 58):

- `test_classic_stream_info_command_does_not_abort[raw|in_envelope]` (fake master: `SET a 1`, `INFO`, `SET b 2`;
  replica alive, both keys, settled ACK exact, one connection; in an envelope also `classic_apply_errors == 1`,
  `rreplay_unwrapped == 1` and the logged reason).
- `test_scripted_active_master_down_link_keeps_zero_counters[plain_replica|peer_mode]x[master_gone|greet_fails]`.
- `test_scripted_active_master_series_outlive_the_link[plain_replica|peer_mode]`.
- `test_scripted_quiet_active_master_shows_zero_counters` now waits with `wait_for_synced_link` (M-5);
  `SCRIPTED_PEER_UUID` replaces the uuid literal of four tests.

### Falsification (each change made alone, built, run, then restored from a saved copy and diffed)

`cd /home/user/drakeydb/build-dbg && nice ninja -j3 dragonfly classic_replay_test`, then the gtests by
`--gtest_filter` (one case per process for the crashing ones) and
`flock /tmp/drakey-pytest.lock env DRAGONFLY_PATH=/home/user/drakeydb/build-dbg/dragonfly
KEYDB_SERVER_PATH=<scratchpad>/KeyDB/src/keydb-server KEYDB_REQUIRED=1 /root/drakey-venv-pinned/bin/python -m
pytest tests/dragonfly/keydb_onboarding_test.py -k <..>`.

| Change | Observed |
|---|---|
| **U-15, every guard off** (`ReplyIfNoConnection` always false; the `Quit`, `Monitor`, `Subscribe`, `PSubscribe` and `DFLY THREAD` guards disabled) | gtest, one case per process: 15 cases die with SIGSEGV (exit 139): `Info` and `InfoSection` in `ServerFamily::Info`, `ClientSetName`, `ClientGetName`, `ClientInfo`, `ClientId` (`Connection::GetClientId`), `ClientKill`, `Auth` and `AuthUser` in `ServerFamily::Auth`, `Hello` (`GetClientId`), `HelloSetName`, `ReplconfListeningPort`, `ReplconfCapaDragonfly`, `Quit`, `DflyThread` (`Connection::Migrate`). `Monitor`, `Subscribe`, `Ssubscribe`, `Psubscribe` do not crash at the command: they fail `classic_replay_test.cc:1375` (`error.has_value()` false: the command was accepted and registered). `ReplicatedMonitorAndSubscribe...` dies in `DispatchMonitor` (the next client command), `InfoInAnEnvelope...` in `ServerFamily::Info`. Each case dispatches one command with one guard, so this shows every case detects its own guard. pytest `-k info_command`: both cases fail (`raw`, `in_envelope`): `ConnectionError ... Connect call failed` on the replica's port, and each teardown reports `Dragonfly did not terminate gracefully, exit code -11` (SIGSEGV). |
| **I-1** the `NoteActiveKeyDbMaster()` call removed from `Greet()` | 8 failed, 4 passed: `test_scripted_quiet_active_master_shows_zero_counters` (both), `test_scripted_active_master_series_outlive_the_link` (both): `classic_series` is `{}` where six zeros are expected; `test_scripted_active_master_exact_classic_counters` (both) and `test_info_and_metrics_show_classic_counters` (both, real KeyDB): only the nonzero series, `{'rreplay_unwrapped': 3, 'keydb_cmds_dropped': 1}`. The four down-link cases pass (they do not read `/metrics`). |
| **M-1a** `GetSummary()` back to `classic_master_ && (state_mask_ & R_GREETED) && master_active_replica_` | 4 failed, 6 passed: the four `down_link_keeps_zero_counters` cases (INFO shows no classic field for the down link); quiet, exact and series cases pass. |
| **M-1b** `classic_master_was_active_ = false` at the top of `Greet()` | 2 failed, 8 passed: `down_link_keeps_zero_counters[greet_fails-plain_replica|peer_mode]`. `master_gone` passes, because a refused connection never reaches `Greet()`: both variants are needed. |
| **M-4** `PERSIST` dropped for no arity (`args.size() == 99`), so `PERSIST k m` is dispatched | `KeyDbOnlyDroppedAndCounted` fails at the counters: `classic_replay_test.cc:1138` `keydb_dropped()` 9 vs 10 and `:1142` `apply_errors()` 1 vs 0, then the same two again after `PERSIST k` (`:1147`, `:1148`); `IsKeyDbOnlyCommandTable` fails twice at `:484` (`only` false where true is expected: the 3-argument `PERSIST` rows). The TTL assertion at the end (`ttl k == -1`) does **not** fail: `PERSIST k` is still applied. On the real binary, a dispatched `PERSIST k m` is `ERR wrong number of arguments for 'persist' command`, `TTL k` stays 100, and `INFO commandstats` has no `cmdstat_persist` row for it (it is rejected before it is invoked; `PERSIST k` makes `calls=1`), so neither the TTL nor the commandstats of `persist` can tell a drop from a rejection: only the counters do. |
| **I-1** `NoteActiveKeyDbMaster` made a no-op | `ActiveKeyDbMasterSeenSticksOnceNoted` fails twice (`ActiveKeyDbMasterSeen()` false, expected true). |
| **M-5** the old `linked()` (`up` only) against a master that answers `PSYNC` after 1.5 s (a throwaway test, deleted) | plain: `KeyError: 'slave_repl_offset'`; peer: `assert 0 == 1000` (`repl_offset`). The same test with `wait_for_synced_link` passes in both modes. The old quiet test did not assert the plain offset, so only the peer variant could have flaked in the shipped form. |

Not falsified: M-7 (a refactor; `PeerReplicationInfo.*` and the peer pytests pass before and after), the M-3 and
M-2 text changes, M-6 (a plan edit).

### U-15 audit

Probes (throwaway gtest, deleted, each case in its own process on `ConnectionContext{nullptr, {}}`, before any
guard): 47 commands applied inside envelopes, then 31 dispatched raw (which reaches what an envelope skips:
`REPLCONF`, `PING`, `SELECT`, `MULTI`, `EXEC`). "Probe" below names what it showed.

| Site (`8989d04`) | Handler | Reachable from a replicated apply? | Disposition |
|---|---|---|---|
| `server_family.cc:411` | `CLIENT SETNAME` | yes, SIGSEGV | guarded |
| `:422` | `CLIENT GETNAME` | yes, SIGSEGV | guarded |
| `:433` | `CLIENT INFO` | yes, SIGSEGV | guarded |
| `:646` | `CLIENT SETINFO` | already guarded (upstream #8084) | none |
| `:670` | `CLIENT ID` | yes, SIGSEGV | guarded |
| `:711` | `CLIENT KILL` | yes, SIGSEGV (after it parsed its arguments) | guarded |
| `:2127` | `SendInvalidationMessages` (`FLUSHALL`/`FLUSHDB`) | no: it walks the listeners' own connections, whose contexts have owners | none |
| `:2189` | `AUTH` | yes, SIGSEGV | guarded |
| `:3419-3431` | `INFO` | yes, SIGSEGV (the review's stack) | guarded |
| `:3501`, `:3528` | `HELLO` (`SETNAME`, and the id in every reply) | yes, SIGSEGV for every `HELLO` that reaches them | guarded |
| `:3885`, `:3914` | `REPLCONF capa dragonfly`, `listening-port` | yes on a node that `IsMaster()` (a peer-mode node: `is_master` stays true there, `rdb_load.cc:4188`) or with `--experimental_cascaded_partial_sync`; a plain replica answers `Replicating a replica is unsupported` first. Probe (raw): SIGSEGV | guarded (top of `ReplConf`) |
| `:4336` | `CLIENT PAUSE` | reachable, but not a dereference: `DispatchTracker` only compares the issuer pointer | none |
| `CLIENT LIST`, `UNPAUSE`, `TRACKING`, `CACHING`, `MIGRATE`, `RESET` | no `conn()` use; `TRACKING` needs RESP3 and only `HELLO 3` (now guarded) sets it | probe: no crash | none |
| `main_service.cc:1999` | `QUIT` | yes, SIGSEGV | guarded (nothing to close; replies `OK`) |
| `main_service.cc:2818`, `conn_context.cc:119-122` | `MONITOR` | yes: no crash at the command, it leaves a null connection in the monitor list and sets `monitor` on the stream's context, so the next command of any client dies in `DispatchMonitor` and the stream's own later commands are refused | guarded |
| `channel_store.cc:163` via `Service::Subscribe`/`PSubscribe` | `SUBSCRIBE`, `SSUBSCRIBE`, `PSUBSCRIBE` | yes: no crash at the command, the stream's context is registered in the channel store (and is gone when the link ends), the next client's `PUBLISH` dies in `Borrow()` | guarded |
| `dflycmd.cc:270` | `DFLY THREAD <n>` | yes (n other than the apply thread), SIGSEGV in `Connection::Migrate` | guarded |
| `dflycmd.cc:106-117`, `:332-334` | `DFLY FLOW` | only after the master replid matches and a sync session in the preparation state is found, which only a real replica connection of this node creates | no (practically); follow-up |
| `dflycmd.cc:547`, `cluster_family.cc:590` | `DFLY TAKEOVER`, `DFLYCLUSTER CONFIG` | pointer handed to `DispatchTracker`, never dereferenced | none |
| `cluster_family.cc:138,147,160` | `CLUSTER SLOTS\|SHARDS\|NODES` (`GetEmulatedShardInfo`) | only with `--cluster_mode=emulated`; not probed | follow-up (needs a design decision: it answers with the address the client connected to) |
| `cluster_family.cc:1003,1023,1029` | `DFLYCLUSTER FLOW` | only in cluster mode; not probed | follow-up |
| `acl_log.cc:42` | `AclLog::Add` | its only caller is `AUTH`, now guarded | none |
| `main_service.cc:832` | tracking callback | needs `CLIENT TRACKING ON`, i.e. RESP3, i.e. `HELLO 3` | no |
| `main_service.cc:926` | `StoreInMultiBlock` | needs a collecting `MULTI`, which the stream never dispatches (skipped raw and inside envelopes) | no |
| `main_service.cc:1572` | blocking command in an async pipeline | only for a context with `async_dispatch`, a real connection's | no |
| `main_service.cc:3136`, `conn_context.cc:340` | `OnConnectionClose`, slowlog | a real connection's context; the slowlog site checks for null | no |

Guarded: nine sites in `server_family.cc` plus five outside it. The outside ones are beyond what the brief
named; they are the default-reachable ones and each is a two- or three-line hunk. The class could instead be
closed once, in the dispatcher, by refusing connection-bound commands for a context without a connection,
which needs a flag on the command table: a design change, recorded as such in the register.

### Final results (debug build, gcc 13.3, `nice ninja -j3 dragonfly classic_replay_test multi_master_test server_family_test`, no warnings)

- `./build-dbg/classic_replay_test`: `[  PASSED  ] 56 tests.` (34 before this round).
- `./build-dbg/multi_master_test`: `[  PASSED  ] 222 tests.` and one skip,
  `NodeIdentityFile.UnwritableDirIsEphemeral` (`geteuid() == 0`: "root ignores permission bits"; the sandbox
  user is root).
- `./build-dbg/server_family_test`: 43 passed, 1 failed, `ServerFamilyTest.GetTcpSocketInfoIPv6` (no IPv6 in
  the sandbox, as the brief says).
- `keydb_onboarding_test.py` and `keydb_harness_test.py` (real KeyDB, `KEYDB_REQUIRED=1`): 79 passed in 112 s
  (58 + 21), run twice (the second time on the final rebuilt binary).
- `redis_replication_test.py` (default `-m "not large"` from `tests/pytest.ini`): 12 passed, 7 deselected, 60 s.
- `multimaster_test.py -k "info or metrics or classic or keydb or greet or capa or skew or peer_lines or
  wait_for_peers"`: 23 passed, 54 deselected, 35 s.
- Byte identity: see note 7.
- `pre-commit run --files <the 18 changed files>`: `pyflakes`, `trim trailing whitespace`, `fix end of files`,
  `check python ast`, `Clang formatting`, `black` all Passed, and the hooks changed no file (every source
  file is byte-identical to the copy that was built and tested).

### Notes, disagreements and what this round supersedes

1. **`sync_in_progress=0` is not enough for M-5 on a peer link.** The brief suggested waiting for
   `sync_in_progress=0` (peer line) or `master_sync_in_progress:0` (plain block). On a peer link that holds
   before the sync starts as well as after it (the docstring of `wait_for_peer_link` says a peer link only
   reports it once the master's `$` header has arrived; not re-measured here), so it does not rule out the early
   read. `wait_for_synced_link` therefore also waits for the offset the sync ended at, which only a finished
   `+FULLRESYNC` produces. The slow-`PSYNC` run above shows the difference.
2. **A guarded `PERSIST k m` has no TTL witness** (M-4): see the falsification table. The brief anticipated it.
3. **U-15 went beyond `server_family.cc`** (five guards in `main_service.cc` and `dflycmd.cc`), because the
   probe showed `QUIT`, `MONITOR`, `SUBSCRIBE` and `DFLY THREAD` are as reachable as `INFO` and two of them
   fail on a later, unrelated client command, which is worse than the stream failing. If the owner wants the
   fix confined to `server_family.cc`, drop those hunks, their `ClassicNoConnectionTest` cases and the
   `ReplicatedMonitorAndSubscribe...` test.
4. **`ReplConf` is guarded at its top, not at the two dereferences.** A node that is a master (a peer-mode
   node) used to log `Error in receiving command, num args: 2` at ERROR level for a raw `REPLCONF GETACK *` from
   a classic master; with a null connection it now replies `No connection` silently. A log difference only.
5. **The sticky per-link flag is never cleared, as briefed.** A `Replica` whose endpoint was an active KeyDB and
   later answers without `active-replica` keeps showing six zeros until the link is removed and added again.
6. **`classic_apply_errors` counts an enveloped `INFO`** (it replied an error); a raw `INFO`, `QUIT` and the like
   leave no counter and no log line, as every other raw command whose reply the stream discards.
7. **Byte identity.** The reviewer's script, `rev-p713/bytecmp/run.sh fix <new binary> 36100`, then `mask.py`
   as for the stored `main` outputs: the masked INFO key sets of the stock-master replica (`A_info_default`,
   `A_info_repl`), the DFLY master and replica (`B_master_info_repl`, `B_replica_info_repl`) and the active node
   (`C_info_repl`) are identical to `main`'s, and so are the sorted `/metrics` key sets of all four
   (`A_metrics`, `B_master_metrics` after normalizing the `replica_port` label, `B_replica_metrics`,
   `C_metrics`). No classic field or series appears in any of the nine files.

### Not done / not verified

- No release, ASAN or UBSAN build; no D-12 throughput measurement; the full `ctest -L DFLY` and
  `replication_test.py` were not run (the brief's list was).
- The U-15 guards were falsified with all of them off at once, one case per process: each case runs one command
  through one guard, so that shows each case catches its own, but no build removed a single guard alone (except
  the `INFO` pytest, which needs only the `INFO` guard).
- The cluster-mode sites of the audit were not probed (there is no cluster-mode fixture for it here).
- `GetTcpSocketInfoIPv6` fails for lack of IPv6, as before.

### Open risks and what a reviewer should attack

1. **Five guards outside `server_family.cc`** (`Quit`, `Monitor`, `Subscribe`, `PSubscribe`, `DFLY THREAD`) are in
   upstream-hot files; the UPSTREAM-SYNC rows say what to re-run after a merge.
2. **Any new handler that touches `conn()` is a new U-15.** Nothing but `ClassicNoConnectionTest` and a reviewer
   notices. The register lists the central fix as a follow-up.
3. **The process-wide flag outlives its links** by design (decision 25): a node that once talked to an active
   KeyDB exports six zero series until restart.
4. **M-6 is open until Task 3.1**: `keydb_cmds_dropped` can over-count a raw drop across a partial resync.
5. **`ClassicTotalSeries` and `ClassicLinkFields` now disagree on purpose**: the series follow a node-wide flag,
   INFO a per-link one. A later task adding a series must pick the right one.
