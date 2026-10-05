# Task 0.9: `MemberExpiryReaperDoesNotBlockOnConcurrentBgsave` robust under load

Implemented in `a0ee234`. Written afterwards, in the review fix round, from `git show a0ee234`, the
implementer's saved outputs (`scratchpad/p7-0/t09-*.txt`, `sf-*.rc`, `sf-*.N.M.txt`) and a
re-run of the stall falsification (the implementer's injection source was not saved). Output is
copied unedited (`…` marks a cut).

## Root cause

The test's last assertion (`"the reaper did not resume once the snapshot consumer unregistered"`,
`Run({"exists", "rs"})` was 1) failed once in the baseline `ctest -L DFLY -j3` on a loaded box
(Task 0.2) and passed 20 of 20 isolated.

`DbSlice::DeleteExpiredStep`'s per-call budget is a 1 ms quota: `quota_remains()` is
`ToUsec(ThisFiber::GetRunningTimeCycles() - quota_start) < 1000` (`db_slice.cc:2282-2286`), checked
before every `Traverse` step (`:2580`, `:2586`) and before each member walk (`:2395`).
`GetRunningTimeCycles()` is the wall time since the fiber was last scheduled in, not CPU time, so
the time a thread spends descheduled counts. With the default `reset_time_quota = false`
(`db_slice.h:643`) `quota_start` is 0 and the quota runs from the fiber's last switch-in. On a loaded
box a thread the OS stops for a millisecond ends the sweep before it reaches `rs`'s bucket. The test
called the follow-up reap exactly once. Production does not have the problem: the heartbeat calls
again on the next tick. It is a test defect, not a product one, and no `db_slice.cc` change was
needed.

## What changed (at `a0ee234`)

`src/server/multi_master_test.cc:10139-10161`
(`ReaperJournalFamilyTest.MemberExpiryReaperDoesNotBlockOnConcurrentBgsave`): the follow-up reap is a
loop of at most `kMaxReapCalls = 100` calls (`:10146`), each `DeleteExpiredStep(db_cntx, 100,
{.reset_time_quota = true})` (a fresh quota per call), until `exists rs` is 0
(`:10157`); the failure message now says how many calls it took. The comment says what is asserted:
that the reaper reaches `rs` once the consumer is gone, not that one call does. The earlier
assertion that `rs` survives while the consumer is registered is unchanged.

## Tests

The test itself, plus the sibling reaper tests and `MvccStoreTest.TombstoneGc*` as a regression
check. `gtests-final.txt` at `a0ee234`: `multi_master_test` `[  PASSED  ] 220 tests.`

## Falsification (verbatim)

**1. Under load, before and after** (`stress_full.sh <prefix> <parallel> <iters> <hogs> <filter>`:
`<parallel>` copies of `./multi_master_test --gtest_filter=<filter>`, each repeated `<iters>` times,
next to `<hogs>` `while :; do :; done` loops; the shape is 3 x 25 = 75 runs, `sf-suite-*`, filter
`ReaperJournalFamilyTest.*`; the hog count was not recorded). Counting the runs in which the target
test itself failed (`grep -l '^\[  FAILED  \] ReaperJournalFamilyTest.MemberExpiryReaperDoesNotBlockOnConcurrentBgsave ('`):

```
before: runs where the target test failed: 3 of 75   (sf-suite-before.1.22.txt  sf-suite-before.1.7.txt  sf-suite-before.3.20.txt)
after:  0 of 75
```

Isolated, on an idle box, the same single test passed `20/20` (`t09-unloaded.txt`).

**2. A forced 3 ms stall** (what a descheduled thread does, reproduced in this round because the
implementer's injection was not saved: `absl::SleepFor(absl::Milliseconds(3));`, a blocking thread
sleep, put in the follow-up reap's callback just before the `DeleteExpiredStep` call; both variants
built in isolation with `scratchpad/fixround/isobuild2.py`, which recompiles only
`multi_master_test.cc` from a scratch copy and links a separate binary). Command:
`./multi_master_test --gtest_filter=ReaperJournalFamilyTest.MemberExpiryReaperDoesNotBlockOnConcurrentBgsave`,
three runs each.

The test as it was before `a0ee234` (`git show a0ee234^:src/server/multi_master_test.cc`) with the
stall:

```
[ RUN      ] ReaperJournalFamilyTest.MemberExpiryReaperDoesNotBlockOnConcurrentBgsave
…/multi_master_test.cc:10149: Failure
    Which is: (1)
the reaper did not resume once the snapshot consumer unregistered
[  FAILED  ] ReaperJournalFamilyTest.MemberExpiryReaperDoesNotBlockOnConcurrentBgsave (252 ms)
```
(3 of 3 failed: 252, 242 and 239 ms.)

The test at `a0ee234` with the same stall in each loop iteration:

```
[       OK ] ReaperJournalFamilyTest.MemberExpiryReaperDoesNotBlockOnConcurrentBgsave (253 ms)
[       OK ] ReaperJournalFamilyTest.MemberExpiryReaperDoesNotBlockOnConcurrentBgsave (241 ms)
[       OK ] ReaperJournalFamilyTest.MemberExpiryReaperDoesNotBlockOnConcurrentBgsave (247 ms)
```

(3 of 3 passed; the unstalled `build-dbg/multi_master_test` also passed 3 of 3: 229, 240, 229 ms.) The
implementer's own stall runs (`t09-stall-orig-{1,2,3}.txt`, `t09-stall-fixed-{1,2,3}.txt`) read the
same: 3 failures at `multi_master_test.cc:10149` with `Which is: (1)`, then 3 passes.

## Not fixed: sibling tests are still load-sensitive

Under the same heavy artificial load other tests of the same fixture, and `MvccStoreTest.TombstoneGc*`,
still fail now and then. Runs with at least one failure: `sf-suite-before` 24 of 75 and
`sf-suite-after` 19 of 75 (filter `ReaperJournalFamilyTest.*`; after the fix mostly
`MemberExpiryReaperReconcilesMemoryAccounting`, 9 runs, down from 17); `sf-before`, 24 runs of the
whole 221-test `multi_master_test` binary, 20 runs with a failure, `MvccStoreTest.
TombstoneGcReapsExpiredTombstonesWithinBudget` in 14 of them. A follow-up to give them the same
quota treatment is queued; this task fixed the one test the baseline gate failed on.

## Plan items not run

- **Step 4(a), "200/200 under the same load" was not run.** What ran instead: the 3 x 25 = 75-run
  loaded comparison above (the target test failed in 3 of 75 runs before the fix and in 0 of 75
  after), the isolated 20/20 idle run, and the forced 3 ms stall (3 of 3 failed before, 3 of 3 passed
  after). 0 failures in 75 runs bounds the residual failure rate only to about 4% (95% confidence),
  where the plan's 200 runs would have bounded it to about 1.5%; the stall run is the deterministic
  evidence.
- **Step 4(b), the falsification that the fixed test still catches a reaper that never resumes, was
  not run** (making the reaper skip permanently, or removing the `HasRegisteredCallbacks()` term, to
  see the follow-up and the first assertion fail). That the loop fails after `kMaxReapCalls` calls
  when `rs` is never reaped is by reading the test, not by observation.
- Step 1 (50 idle plus 50 loaded runs of the original) was replaced by the 20 idle runs and the
  75-run comparison. (The same load on the single test alone, `sf-iso-before.*` and `sf-iso-after.*`,
  did not reproduce the failure even before the fix, 0 of 60 runs, so those runs show nothing either
  way and are not counted above.) Step 2's `traversed` and entry-time instrumentation output is not
  among the saved evidence: the root cause above rests on reading `db_slice.cc` and on the stall
  falsification, and the only saved loaded failure (`t09-failing-run-evidence.txt`) is the failing
  run itself.
- Step 3 as built: at most 100 calls with `{.reset_time_quota = true}` only; the plan sketched 64
  calls with `.ensure_member_reaping = true, .journal_deletions = false` as well.
- **Sibling tests stay load-sensitive** (see "Not fixed" above); giving them the same treatment is a
  follow-up with a task card, not part of this task.

## Deviations from the plan

- The plan listed three hypotheses (H1 the wall-time quota, H2 a time-dependent `expire_cursor`, H3
  the consumer still registered). The root cause recorded by the implementer is H1; the saved
  evidence does not show H2 and H3 being ruled out separately.
- The plan allowed a `db_slice.cc` change if the cause was a product defect; it was not.
