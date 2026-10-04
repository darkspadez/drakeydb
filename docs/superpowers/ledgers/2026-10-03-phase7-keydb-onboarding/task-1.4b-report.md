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
