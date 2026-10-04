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
| `src/server/replica.{h,cc}` | `classic_master_` (set at the end of `Greet()`, not cleared by a reconnect); `GetSummary()` fills the three fields, reading `master_active_replica_` only with `R_GREETED` set (the Task 1.4 interface note); the raw path's queuing condition gains `&& !classic_applier.SkipKeyDbOnly(last_args)`; the comment of `master_active_replica_` says who reads it now. |
| `src/server/server_family.cc` | Plain-replica block of `FormatInfoMetrics`: `ClassicLinkFields(rinfo)` after `psync_successes`. `GetMetrics`: `Metrics::classic_master_active` from the replica and peer summaries. |
| `src/server/metrics.{h,cc}` | `Metrics::classic_master_active`; one loop in `Print` after the replica/master branches that emits `ClassicTotalSeries(ClassicTotals().Snapshot(), classic_master_active)` as `<name>_total` counters. |
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
   by `Count` beside the per-link atomic. Relaxed monotonic atomics only, as
   `multimaster_lww_dropped` is; the comment says why (a link's fiber and the fiber that renders run
   on different threads, and a per-thread `ServerState::Stats` neither follows a link nor survives a
   `Replica` that is gone). A Prometheus counter must not fall when `REPLICAOF NO ONE` destroys the
   link, which is why `/metrics` reads the totals and INFO reads the link.
3. **The predicate is per field** (spec D-13 says "whose own counter is nonzero"): an active KeyDB's
   link shows all six counters, zeros included; any other classic link shows the counters that
   moved, and on a peer line `repl_offset` follows when anything is shown. A non-active KeyDB that
   sends `PEXPIREMEMBERAT` raw therefore shows `keydb_cmds_dropped:1` and nothing else
   (`test_scripted_stock_master_raw_keydb_only_command_shows_only_that_counter`).
4. **`classic_link` vs `master_active_replica`.** `classic_link` is the protocol of the last
   *completed* Greet, so a link that is down keeps showing the counters it has. `master_active_replica`
   is read only while `R_GREETED` is set: a failed Greet leaves `master_active_replica_` stale, and
   `state_mask_` is cleared on disconnect, so a brand-new link with every counter zero stops showing
   its six zeros while it is down. Once the link has unwrapped one envelope (KeyDB's cron `PING`s are
   envelopes) the counters are nonzero and the link is shown whatever its state.
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
7. **`Metrics::classic_master_active`** is the "some link has an active KeyDB for master" half of the
   predicate for the series, computed in `GetMetrics` from `replica_side_info` (plain node) or
   `peers_->Summaries()` (active node). It sits after `blocked_tasks` because it fits the padding
   that follows it, so the size checks in `Merge()` and `InitFromThread()` need no change (the first
   attempt, after `replica_side_info`, changed `sizeof(Metrics)` and tripped them at compile time).
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
- INFO's `master_active_replica` for a link in the middle of a reconnect is false (design note 4).

## Open risks and what a reviewer should attack

1. **`KEYDB.MVCCRESTORE` is on the wrong counter on purpose.** P7-2 Task 2.6 must take it out of the
   pre-dispatch unknown check (translate it first) and then update
   `KeyDbMvccRestoreCountedUnknownUntilItIsApplied` and the expected values of the scripted exact-counter
   stream, whose two "unknown" commands are `NOSUCHCMD` and `KEYDB.MVCCRESTORE`.
2. **`GetMetrics` on an active node now calls `peers_->Summaries()`** (a hop to every peer's proactor,
   under `PeerReplicationManager::mu_`) on every `/metrics` scrape and every INFO that collects metrics;
   INFO's active-node block then asks again. Cheap for a handful of peers, but it is a second hop.
3. **`Metrics::classic_master_active` relies on the layout** (it fills the padding after `blocked_tasks`).
   If a later upstream field changes the padding, the size checks of `Merge()` and `InitFromThread()`
   fire at compile time, which is the safe failure.
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
