# Task 1.4b report: review-round fixes after Task 1.4

Branch `feat/phase7-1-rreplay-unwrap`. Not committed by the implementer. Brief:
`brief-sort-and-findings.md` (part A). Scratch material (the good copy of `generic_family.cc`, each
falsification's build log and raw test output, the journal probe) is in the orchestrator's
scratchpad under `p71sort/`.

## Decision 32 (SORT .. STORE)

### What the fix is

Fix commit `1404897` (ledger decision 32; ISSUE-REGISTER D-33). A fork regression from P4-0
(`1b6a2e82`, the only commit that ever added `return fetch_result.status();` to the fetch hop of
`SortGeneric`): on a transaction of more than one shard `Transaction::RunCallback` does
`CHECK_EQ(OpStatus::OK, result)` (`transaction.cc:771`, active in release builds), so a source that
is missing, of the wrong type or non-numeric, with `STORE`'s destination on another shard, killed
the server. `generic_family.cc` has three hunks now:

1. The fetch hop returns `t->GetUniqueShardCnt() == 1 ? fetch_result.status() : OpStatus::OK`.
2. `SortStoreNothing`, called at two sites (the unsorted fetch's empty result and the sorted fetch's
   `KEY_NOTFOUND`), runs the concluding hop and `OpStore` with nothing to store: the destination is
   deleted, a `DEL dst` is hand-journaled for a destination that was there
   (`source_deleted_by_fetch` is passed as `true`), and the reply is `:0`.
3. Wrong-type and non-numeric sources never reach it: they reply their error with `dst` untouched.

### Semantics, checked against the sources

| Case | KeyDB `sort.cpp` (scratchpad `KeyDB/src/`) | Upstream Dragonfly main | This fork |
|---|---|---|---|
| Missing source with `STORE` | `lookupKeyWrite` gives NULL and an empty list is made (`:274-293`); `outputlen == 0` takes `dbDelete(storekey)` and replies `:0` (`:575-586`) | `SortStoreNothing` (`generic_family_main.cc:2166`), two call sites (`:2261`, `:2303`); `OpStore` with an empty result deletes the key and journals `DEL` | the same, plus `source_deleted_by_fetch=true` |
| Wrong-type source | `WRONGTYPE` before anything is touched (`:278-285`) | `SendError(WRONG_TYPE)` after `Conclude()`; `dst` untouched (its test `SortStoreMissingSource`, `generic_family_test_main.cc:1177`, pins it for one placement) | the same, every placement |
| Non-numeric element | `int_conversion_error` (`:478-484`) replies the error (`:514-515`) ahead of the store branches; `ALPHA` and `BY nosort` convert nothing (`:460`) | `"One or more scores can't be converted into double"` (`generic_family_main.cc:2308`); `dst` untouched | the same |

Upstream's `SortGeneric` has three hops, and all return `OK` (`generic_family_main.cc:2248-2252`,
`:2287-2292`, `:2361-2366`), so upstream never had the abort; the merge base `29e6bea` has no
`SortStoreNothing` (0 matches), so its missing-source STORE replied an empty array and kept `dst`.
The claim in the code comment, that upstream main has `SortStoreNothing` with the same two call
sites, holds. Upstreamable differences: none for the abort (upstream is already right). What
upstream lacks is test coverage: the cross-shard placements, `BY nosort` and `BY` pattern,
non-numeric sources, the member-expiry-emptied set and the journal pins
(`GenericFamilyTest.SortStoreOf*` and the two journal tests are written against the fork's `OpStore`
signature and would need the `source_deleted_by_fetch` argument dropped). `source_deleted_by_fetch`
and the single-shard branch of the hop are fork-only (P4-3 hand-journal, P4-0 journal suppression).

One residual, measured on the P7-1 build with the real binary (a one-shard master with one replica;
the replica's `slave_repl_offset` before and after each command; `journal_probe.py`): `GET`,
`SORT <string>` and `SORT <list of words>` (no `STORE`) advance it by 0. `SORT <string> STORE dst`,
the same with `BY nosort`, and `SORT <words> STORE dst` advance it by 1 each: the failing STORE
still journals its verbatim `SORT` on one shard. `SORT <missing> STORE dst` advances it by 1 with no
`dst` and by 2 with one (`DEL dst`, then the `SORT`). The replica replays the entry, gets the same
error and leaves `dst` alone; `dst` ended `[]` on both nodes. It is journal noise, not divergence,
and is recorded in D-33. Across shards nothing journals it (`CrossShardStoreOfMissingSource...`
expects exactly one entry, the `DEL dst`).

### Falsifications

Each one: `cp src/server/generic_family.cc` to the scratchpad as the good copy (sha256
`9130e75c...`), apply the mutation, `cd /home/user/drakeydb/build-dbg && nice ninja -j3
generic_family_test multi_master_test dragonfly`, run, then `cp` the good copy back and check
`git diff --quiet HEAD -- src/ tests/`. Each gtest below ran as its own process
(`--gtest_filter=<one test>`), because an abort takes the binary down.

**(a) The hop returns `fetch_result.status()` again** (the line goes back to `return
fetch_result.status();`). Built clean.

```
multi_master_test --gtest_filter=MultiShardOriginJournalFamilyTest.CrossShardStoreOfMissingSource\
JournalsDestinationDelete
  F... transaction.cc:771] Check failed: OpStatus::OK == result (0 vs. 2)    -> SIGABRT, rc=134
generic_family_test --gtest_filter=GenericFamilyTest.SortStoreOfMissingSourceDeletesDestination
  Check failed: OpStatus::OK == result (0 vs. 2)       (KEY_NOTFOUND)        -> SIGABRT, rc=134
generic_family_test --gtest_filter=GenericFamilyTest.SortStoreOfWrongTypeSourceKeepsDestination
  Check failed: OpStatus::OK == result (0 vs. 8)       (WRONG_TYPE)          -> SIGABRT, rc=134
generic_family_test --gtest_filter=GenericFamilyTest.SortStoreOfUnparsableSourceKeepsDestination
  Check failed: OpStatus::OK == result (0 vs. 17)      (INVALID_NUMERIC_RESULT) -> SIGABRT, rc=134
generic_family_test --gtest_filter=GenericFamilyTest.SortStoreOfFullyExpiredSetDeletesDestination
  [       OK ]                                                               -> passes, rc=0
```

The three statuses are the ones in the test comments. `SortStoreOfFullyExpiredSetDeletesDestination`
does not abort under (a), as it does not go through a failing hop (the set is found, its members
expire inside the fetch); it is falsified by (b). Run one test per process over every `*Sort*` case
of both binaries (17 in `generic_family_test`, 4 in `multi_master_test`): the three aborts above and
nothing else fail.

**(b) Both `SortStoreNothing` call sites removed** (the missing-source STORE falls back to the old
empty-array reply; `SortStoreNothing` stays defined, with external linkage, so `-Werror` is quiet).
Built clean.

```
generic_family_test --gtest_filter=GenericFamilyTest.SortStoreOf*      rc=1
  SortStoreOfMissingSourceDeletesDestination     FAILED  (Run(cmd): Actual: [] (an empty array,
      not :0); Run({"exists", dst}): Actual: i1; Run({"ttl", dst}): Actual: i1000)
  SortStoreOfFullyExpiredSetDeletesDestination   FAILED  (only the three unsorted-fetch variants,
      `BY nosort`, `BY nosort GET #` and `BY weight_*`, on and off the source's shard: Actual: [];
      exists dst: "Actual : 1 expected: 0"; the sorted-fetch variants pass)
  SortStoreOfWrongTypeSourceKeepsDestination     OK
  SortStoreOfUnparsableSourceKeepsDestination    OK
multi_master_test --gtest_filter=*StoreOfMissingSourceJournalsDestinationDelete*   rc=1
  OriginJournalFamilyTest.SameShard...BeforeSort  FAILED
      :9758 Run({"sort", src, "store", dst}).GetInt() Which is: (nullopt) vs 0
      :9759 Run({"exists", dst}).GetInt() Which is: (1) vs 0
      :9765 Run({"sort", src, "by", "nosort", "store", dst}).GetInt() Which is: (nullopt) vs 0
      :9771 3u vs consumer.entries.size() Which is: 2
  MultiShardOriginJournalFamilyTest.CrossShard... FAILED
      :9718 Run({"sort", src, "store", dst}).GetInt() Which is: (nullopt) vs 0
      :9719 Run({"exists", dst}).GetInt() Which is: (1) vs 0
      :9720 the second run, "nothing left to delete": (nullopt) vs 0
      :9730 1u vs consumer.entries.size() Which is: 0   "only the delete that happened is journaled"
pytest (keydb_onboarding_test.py -k sort): 2 failed, 4 passed
  test_classic_stream_sort_store_of_an_unsortable_source_does_not_abort
      [df_factory0-missing_source-raw] and [df_factory0-missing_source-in_envelope]:
      `assert await c.exists(dst) == 0` -> `E  assert 1 == 0`
      the wrong_type and non_numeric scenarios, raw and in_envelope: passed
```

As the brief expected: the reply is not 0 and `dst` survives. The error scenarios pass under (b), as
they should (they are falsified by (a)).

**(c) `SortStoreNothing` passes `/*source_deleted_by_fetch=*/false`** (line 2653). Built clean.

```
multi_master_test --gtest_filter=*StoreOfMissingSourceJournalsDestinationDelete*   rc=1
  OriginJournalFamilyTest.SameShardStoreOfMissingSourceJournalsDestinationDeleteBeforeSort FAILED
      multi_master_test.cc:9771: Failure
      Expected equality of these values:  3u  vs  consumer.entries.size()  Which is: 2
      DEL dst, SORT, then only the SORT of the second run
  MultiShardOriginJournalFamilyTest.CrossShardStoreOfMissingSourceJournalsDestinationDelete  OK
generic_family_test --gtest_filter=GenericFamilyTest.SortStoreOf*      rc=0 (4 passed)
```

Only the same-shard journal test fails, on the entry count: the `DEL dst` entry is gone and only the
verbatim `SORT` entries remain (2 instead of 3), as the brief expected. The pytest was not run under
(a) or (c): the brief asked for it under (b) only.

### Green on the restored build

After the last restore `git diff --quiet HEAD -- src/ tests/` was clean, and `generic_family_test
multi_master_test dragonfly` were rebuilt (`build-final.log`; `classic_replay_test` was stale
against the rebuilt library, so it was relinked with `ninja classic_replay_test` as well).

| Binary or selection | Result |
|---|---|
| `generic_family_test` | 88 of 88 passed |
| `multi_master_test` | 224 passed, 225 ran, 1 skipped (`NodeIdentityFile.UnwritableDirIsEphemeral`) |
| `classic_replay_test` | 75 of 75 passed |
| the 6 new cases (`SortStoreOf*`, the two journal tests) | all passed |
| pytest `-k "sort or orphan or deadline"` | 18 passed |

pytest ran under `flock /tmp/drakey-pytest.lock /root/drakey-venv-pinned/bin/python -m pytest
tests/dragonfly/keydb_onboarding_test.py` with `KEYDB_SERVER_PATH` (the scratchpad KeyDB),
`KEYDB_REQUIRED=1` and `DRAGONFLY_PATH=/home/user/drakeydb/build-dbg/dragonfly`, after `cat drakeydb
> /dev/null`. `-k sort` alone selects the 6 cases of the new pytest (3 scenarios, raw and in an
envelope).

### Not done, not verified

- The abort on `main`'s binary is the Opus review's reproduction (decision 32); not re-run here.
- The pytest under falsifications (a) and (c), and the whole `keydb_onboarding_test.py` file, were
  not run in this round.
- The one-shard residual was measured with `--num_shards=1`; the same-shard placement on a
  multi-shard server follows from the same revive condition (`GetUniqueShardCnt() == 1`) and was not
  measured.
- `build-opt` was not touched.

## Review of f281564: M-4 and docs

Brief `brief-delta-fix-A.md`; review `review-delta-f281564.md` (M-4 is its MINOR-4; the docs items
are IMPORTANT-1, MINOR-1, MINOR-5 and NIT-2). Not committed by the implementer. Scratch (the good
copies of the two edited sources, each falsification's build log and raw output, the end-to-end
probe) is in the orchestrator's scratchpad under `deltaA/`.

### M-4: what changed

On one shard `SortStoreNothing`'s hop now returns `OpStatus::SKIPPED`
(`t->GetUniqueShardCnt() == 1 ? OpStatus::SKIPPED : OpStatus::OK`). `LogAutoJournalOnShard` returns
before journaling on any non-OK result (`transaction.cc:1816`), so the verbatim `SORT` that the
revived auto-journal would record behind the destination delete is no longer written. The `DEL dst`
that `OpStore` hand-journals (`source_deleted_by_fetch=true`, unchanged) is the whole entry, and
`OpStore` journals a `DEL` only for a destination it deleted, so with no `dst` nothing is journaled:
upstream main's wire (`OpStore`, `generic_family_main.cc:1896`) and Redis's (no `dirty`, no
propagation). The reply stays `:0` and the destination delete is untouched.

Why the gate: `RunCallback` does `CHECK_EQ(OpStatus::OK, result)` on every hop of a multi-shard
transaction (`transaction.cc:771`), so across shards the hop must keep returning `OK`. On one shard
`RunCallback` stores the result in `local_result_` (`:762-764`); `Execute` returns void and
`SortStoreNothing` replies from `store_len`, so nothing reads it. The fetch hop above it already
returns a non-OK status on one shard, so a squashed stub (`RunSquashedMultiCb`, no `CHECK`) has the
same precedent. Comments updated: `OpStore`'s `source_deleted_by_fetch` block, `SortStoreNothing`'s
header and the fetch callback's (which now says what a STORE form's journal sees for a missing
source and that a wrong-type or unparsable source still journals the verbatim `SORT`, D-33).

### New wire

Measured end to end on the real binary with a one-shard master and one replica (the replica's
`slave_repl_offset` before and after each command; `deltaA/journal_probe.py`, output
`journal_probe.fixed.out`), and pinned by the gtests below.

| Case | Before (`1404897`) | Now |
|---|---|---|
| one shard, missing or emptied source, `dst` existed | `DEL dst`, `SORT ...` (2, measured) | `DEL dst` (1) |
| one shard, missing or emptied source, no `dst` | `SORT ...` (1, measured) | nothing (0) |
| `BY nosort` forms of the two rows above | as above, by reading | 1 and 0, measured |
| several shards, `dst` existed | `DEL dst` on `dst`'s shard | the same (gtest) |
| several shards, no `dst` | nothing | the same (gtest) |
| one shard, sorted `STORE` whose own fetch emptied the set (not `SortStoreNothing`'s) | `DEL src`, `DEL dst`, `SORT` | the same (3, measured) |
| one shard, wrong-type or non-numeric source | the verbatim `SORT` (D-33 residual) | the same (1, measured) |

The fully-expired set with `BY nosort` and a `dst` advances the replica by 2 now (by reading `DEL
src`, `DEL dst`); its value before the change was not measured (by reading: 3, with the `SORT`).
The `dst` was `[]` on master and replica at the end of the probe.

### Tests

`OriginJournalFamilyTest.SameShardStoreOfMissingSourceJournalsDestinationDeleteBeforeSort` is
renamed `SameShardStoreOfMissingSourceJournalsOnlyTheDestinationDelete` and rewritten: for the
sorted fetch and for `BY nosort`, a run with a `dst` must journal exactly `[DEL dst]`, and the
repeat with no `dst` must journal nothing. `SortDerivedDeleteReachesPeersButSortRoStaysSuppressed`
keeps its assertion (the source `DEL` is still not derived); its comment and failure message now say
that this DEL is all the STORE journals. `generic_family_test.cc` has no journal pin of the old wire
(its fixture has no journal), so nothing changed there. `multi_master_test.cc` gained the
`str_join.h` include.

### Falsifications

Each one: the good copy of `generic_family.cc` is in `deltaA/`, the line (2665 then) is changed,
`cd /home/user/drakeydb/build-dbg && nice ninja -j3 generic_family_test multi_master_test
dragonfly`, run, then the good copy goes back. The tree was clean against it after each.

**F1, the hop returns `OK` again** (the change reverted). Built clean.

```
multi_master_test --gtest_filter='*StoreOfMissingSource*:*Sort*'   rc=1
  OriginJournalFamilyTest.SameShardStoreOfMissingSourceJournalsOnlyTheDestinationDelete FAILED
  multi_master_test.cc:9788  journaled_by(cmd)  Which is: { { "DEL", "sort-gone-dst" },
      { "SORT", "sort-gone-src", "store", "sort-gone-dst" } }   vs  { { "DEL", "sort-gone-dst" } }
      "a dst was there: its delete is the whole wire"                         (trace: sorted)
  multi_master_test.cc:9790  journaled_by(cmd)  Which is: { { "SORT", "sort-gone-src", "store",
      "sort-gone-dst" } }   vs  {}   "no dst, nothing changed: nothing is journaled"
  the same two failures with { "SORT", ..., "by", "nosort", "store", ... }   (trace: by nosort)
  the other four tests of the selection, the cross-shard one included: OK
```

**F2, `return OpStatus::SKIPPED;` without the one-shard gate.** Built clean. The gate is load
bearing: every cross-shard case aborts (each gtest in its own process).

```
multi_master_test MultiShardOriginJournalFamilyTest.CrossShardStoreOfMissingSourceJournals\
DestinationDelete
  transaction.cc:771] Check failed: OpStatus::OK == result (0 vs. 4)               rc=134
generic_family_test GenericFamilyTest.SortStoreOfMissingSourceDeletesDestination
  Check failed: OpStatus::OK == result (0 vs. 4)                                   rc=134
generic_family_test GenericFamilyTest.SortStoreOfFullyExpiredSetDeletesDestination
  Check failed: OpStatus::OK == result (0 vs. 4)                                   rc=134
generic_family_test ...SortStoreOfWrongTypeSource..., ...SortStoreOfUnparsableSource...: OK
OriginJournalFamilyTest.SameShardStoreOfMissingSourceJournalsOnlyTheDestinationDelete: OK
```

**F3, `SortStoreNothing` passes `/*source_deleted_by_fetch=*/false`** (the register's earlier
falsification, re-run against the new test). With the status `SKIPPED` the two parts are coupled:
nothing at all is journaled and the destination delete never reaches a peer.

```
multi_master_test --gtest_filter='*StoreOfMissingSource*'   rc=1
  SameShardStoreOfMissingSourceJournalsOnlyTheDestinationDelete FAILED, twice (sorted, by nosort):
  multi_master_test.cc:9788  journaled_by(cmd)  Which is: {}   vs  { { "DEL", "sort-gone-dst" } }
  the cross-shard test: OK;  generic_family_test SortStoreOf*: 4 passed
```

### Results on the final tree

`build-dbg` rebuilt (`generic_family_test multi_master_test dragonfly`, `ninja -n` reports no work)
after the last edit to `generic_family.cc`, which was a comment.

| Selection | Result |
|---|---|
| `generic_family_test` | 88 of 88 passed |
| `multi_master_test` | 224 passed, 225 ran, 1 skipped (`NodeIdentityFile.UnwritableDirIsEphemeral`) |
| pytest `keydb_onboarding_test.py -k sort` | 6 passed, 107 deselected |
| pytest `multimaster_test.py -k sort` | 2 passed (`test_sort_store_replicates_cross_shard_plain`, `..._stamped`), 75 deselected |

pytest ran from the repo root under `flock /tmp/drakey-pytest.lock
/root/drakey-venv-pinned/bin/python -m pytest ... -p no:cacheprovider` with `KEYDB_SERVER_PATH`
(the scratchpad KeyDB), `KEYDB_REQUIRED=1` and
`DRAGONFLY_PATH=/home/user/drakeydb/build-dbg/dragonfly`, after `cat drakeydb > /dev/null`. The 6
onboarding cases are the classic-stream SORT scenarios; none of the 8 pytest cases asserts the
same-shard wire, so only the gtests falsify M-4.

### Docs

- **I-1** (`docs/UPSTREAM-SYNC.md`, the `generic_family.cc` row): the three fork differences from
  upstream main's `SortStoreNothing` now include the one-shard `SKIPPED`; a decision point records
  that upstream main changed SORT's whole model (`CO::NO_AUTOJOURNAL`, no revive but RENAME's,
  `OpStore` journals `DEL dst` + `RPUSH dst ...` (+ `STICK`) on every `STORE`, `:2984`, `:1215`,
  `:1890-1912`), so the sync chooses between that model and the fork's single-entry `RESTORE`
  hand-journal plus the single-shard verbatim revive, and "keep the extra argument" holds only under
  the second. Not decided. ISSUE-REGISTER D-5 and D-13 point at it (D-5's cross-shard exception
  inverts at the sync; D-13 closes if upstream's model is taken).
- **M-1** (`docs/UPSTREAM-SYNC.md:7-19`, ISSUE-REGISTER D-5, `docs/PLAN.md` upstream-sync summary,
  `docs/differences.md`, the spec's byte-identity section): the same-shard missing-source `DEL dst`,
  P4-3's same-shard `DEL dst` ahead of the verbatim `SORT`, P4-0's `SREM` compensation and the
  cross-shard empty-result `DEL dst` are listed with their merge-base and upstream-main wires.
- **M-5** (spec, stream clock): the reset is on a master **uuid** change or the flag clearing only,
  with the reason (`ApplyReplicaActiveExpiry` runs after `Greet()`, `replica.cc:279`, `:341`,
  before the PSYNC reply sets `master_repl_id`, `:2025`; a `+CONTINUE <newid>` must not reset).
- **D-33**: item 2 gives the new wire; the residual paragraph is rewritten (the missing-source
  residual is closed, the failing-STORE one stays, the fully-expired sorted STORE keeps its old
  shape and is named); test name and falsification sentence updated; the trigger list adds a
  DFLY-protocol replica or peer whose shard count differs from its master's.

### Not done, not verified

- The nit "cite `ScanCb`'s `ExpireIfNeeded` at `:813`, not `:814`" was **not applied**: at HEAD
  (and in the tree) `ScanCb` starts at `generic_family.cc:809`, `:813` is `if
  (prime_it->first.HasExpire()) {` and the `ExpireIfNeeded` call is `:814`, so the citations in the
  spec, the register's neighbours, `docs/UPSTREAM-SYNC.md` and the plan are right. The plan's cite
  is in the other coder's file in any case.
- The pre-change journal size of the fully-expired `BY nosort` case was not measured.
- A peer holding a newer `dst` was not reproduced end to end (the old divergence is the reviewer's
  reading plus D-18's mechanism); the wire is pinned, and the guarded `DEL` is covered by P4-4's
  tests.
- `classic_replay_test` and `ctest -L DFLY` as a whole were not run; `build-opt` was not touched.
- The sorted-fetch same-shard STORE that the fetch itself emptied (not `SortStoreNothing`'s) still
  journals `DEL src`, `DEL dst`, `SORT`: D-13/D-18's exposure, named in D-33, not changed.
