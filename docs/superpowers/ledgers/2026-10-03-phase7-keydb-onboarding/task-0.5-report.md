# Task 0.5: U-9 and U-10, null `conn()` on a replicated apply

Implemented in `a0ee234`. Written afterwards, in the review fix round, from `git show a0ee234`, the
implementer's saved outputs (`scratchpad/p7-0/t05-*.txt`, `u10-probe-run.txt`,
`zz_u10_probe_test.py.txt`) and the review. Output is copied unedited (`…` marks a cut, `@` stack
frames are trimmed to the ones that matter). The shell commands were not saved: "Command" lines are
the invocation shape.

## What changed (at `a0ee234`)

A replicated apply dispatches through `Service::DispatchCommand` with a context whose `conn()` is
`nullptr` (`JournalExecutor`'s `conn_context_{nullptr, ...}`, `journal/executor.cc:37`;
`Replica::ConsumeRedisStream`'s own bare `ConnectionContext`).

| File | What |
| --- | --- |
| `src/server/main_service.cc:2463` | U-9: `EvalInternal`'s migration branch is `if (sid.has_value() && *sid != ss->thread_index() && conn_cntx->conn() != nullptr)`. Migration is a latency optimisation, so skipping it is neutral. |
| `src/server/main_service.cc:1432` | U-10 (found next to U-9, same commit): the `TAKEN_OVER` branch of `VerifyCommandState` is `dfly_cntx.conn() == nullptr \|\| dfly_cntx.conn()->IsPrivileged() \|\| ...`, as the restricted-command check a few lines above already treated a null `conn()` ("no connection owner means the command is internal"). |
| `src/server/dragonfly_test.cc:291` | `DflyEngineTest.EvalReplicatedApplyNoConnNoCrash`: a `JournalExecutor` on thread 0 applies `EVAL "return redis.call('SET', KEYS[1], 'v')" 1 <key owned by another shard>`; asserts `DispatchResult::OK` and that the key is set. |
| `src/server/dragonfly_test.cc:319` | `DflyEngineTest.ReplicatedApplyDuringTakeoverNoCrash`: the node is switched `ACTIVE -> TAKEN_OVER`, a `JournalExecutor` applies `SET`, the state is switched back; asserts `OK` and that the key is set. |
| `docs/ISSUE-REGISTER.md` | U-9 and U-10 entries. |

## Falsification (verbatim)

**U-9, guard removed** (the unfixed build; `t05-before-1..3.txt`, three runs, identical modulo
addresses). Command: `cd build-dbg && ./dragonfly_test
--gtest_filter=DflyEngineTest.EvalReplicatedApplyNoConnNoCrash`:

```
[ RUN      ] DflyEngineTest.EvalReplicatedApplyNoConnNoCrash
*** SIGSEGV received at time=1791057388 on cpu 0 ***
PC: @     0x563f5ef152ba  (unknown)  facade::Connection::RequestAsyncMigration()
    @     0x563f5e054142        688  dfly::Service::EvalInternal()
    @     0x563f5e0528b0        224  dfly::Service::CallSHA()
    @     0x563f5e04e436        368  dfly::Service::InvokeCmd()
    @     0x563f5e04d824        432  dfly::Service::DispatchCommand()
    @     0x563f5e971164        112  dfly::JournalExecutor::Execute()
    @     0x563f5dc335dd        848  dfly::DflyEngineTest_EvalReplicatedApplyNoConnNoCrash_Test::TestBody()::{lambda()#1}::operator()()
```

SIGSEGV 3 of 3 without the guard; with it the test passed 20 of 20 (implementer's count; the 20-run
output was not saved, the final single pass is in `gtests-final.txt`: `dragonfly_test`
`[  PASSED  ] 53 tests.`).

**U-10, guard removed** (`t05-u10-before.txt`), `--gtest_filter=DflyEngineTest.ReplicatedApplyDuringTakeoverNoCrash`:

```
[ RUN      ] DflyEngineTest.ReplicatedApplyDuringTakeoverNoCrash
*** SIGSEGV received at time=1791057599 on cpu 0 ***
PC: @     0x55aae75b94f2  (unknown)  dfly::Service::VerifyCommandState()
    @     0x55aae75badec        432  dfly::Service::DispatchCommand()
    @     0x55aae7edefde        112  dfly::JournalExecutor::Execute()
    @     0x55aae7edf146        416  dfly::JournalExecutor::SelectDb()
    @     0x55aae7edebda        400  dfly::JournalExecutor::Execute()
```

**U-10, live** (`zz_u10_probe_test.py.txt`, a throwaway pytest, not committed: chain `master -> r1 ->
r2`, all `--proactor_threads=4 --experimental_cascaded_partial_sync`; a task pipelines 200 `APPEND`s
at a time on `master`; after 1 s `REPLTAKEOVER 10` on `r2`; the writer keeps going until the command
returns; the test fails if `r1` has exited non-zero). Unfixed, `u10-probe-run.txt`:

```
REPLTAKEOVER failed: server:ResponseError
… dfly::Service::DispatchCommand()
… dfly::JournalExecutor::Execute()
… dfly::DflyShardReplica::ExecuteTx()
… dfly::DflyShardReplica::StableSyncDflyReadFb()
FAILED 😰  tests/dragonfly/zz_u10_probe_test.py::test_u10_probe[df_factory0]
========================== 1 failed, 1 error in 2.24s ==========================
```

Implementer's counts: `r1` died with SIGSEGV 4 of 4 unfixed; 5 of 5 fixed runs ended with `r1`
alive (exit 0 at the end) and `REPLTAKEOVER` answering `OK`. Those multi-run outputs were not saved;
the single failing run above was.

## Deviations from the plan

- Plan Step 1 built a bare `ConnectionContext{nullptr, ...}` the way `ConsumeRedisStream` does; the
  test uses a `JournalExecutor`, which has the identical null `conn()` and is the shape the
  existing `OriginJournalFamilyTest`s use.
- U-10 was not in the plan's task text beyond Step 5's note. It was found next to U-9, fixed in the
  same commit with its own test, and registered as U-10.
- U-10's allow-vs-refuse decision has a cost, now recorded in the register: a takeover on a
  cascaded node can wait on a moving journal LSN.

## Review fix round (committed in `2298570`, P7-0 whole-branch review round 1)

**U-12, a third null-`conn()` site, fixed.** `Service::DispatchCommand` ends with
`cmd_cntx->SendError("Internal Error"); dfly_cntx->conn()->MarkForClose();` when `InvokeCmd` returns
`ERROR`, which it does only after catching a `std::exception` from a handler. On a replicated apply
that is the same null dereference.

Fix, `src/server/main_service.cc` (`DispatchCommand`, after `InvokeCmd`): the close requires
`conn() != nullptr`. Test `DflyEngineTest.ReplicatedApplyHandlerThrowNoConnNoCrash`
(`dragonfly_test.cc`): the registry's `ECHO` handler is replaced (the precedent is
`MultiTest.SquashedCallbackBadAlloc`; each test has its own `Service` and registry) with one that
replies `OK` and throws `std::runtime_error`; a `JournalExecutor` applies `ECHO x`; the result must
be `DispatchResult::ERROR` and the process must live.

No production command throws deterministically, which is why the handler is stubbed; a real trigger
(a `std::exception` thrown on a handler's coordinator side) was not reproduced. Registered as U-12.

With the fix:

```
[ RUN      ] DflyEngineTest.EvalReplicatedApplyNoConnNoCrash
[       OK ] DflyEngineTest.EvalReplicatedApplyNoConnNoCrash (63 ms)
[ RUN      ] DflyEngineTest.ReplicatedApplyDuringTakeoverNoCrash
[       OK ] DflyEngineTest.ReplicatedApplyDuringTakeoverNoCrash (39 ms)
[ RUN      ] DflyEngineTest.ReplicatedApplyHandlerThrowNoConnNoCrash
E1003 22:17:34.528711   18265 main_service.cc:1750] Internal error, system probably unstable handler failure
[       OK ] DflyEngineTest.ReplicatedApplyHandlerThrowNoConnNoCrash (40 ms)
[==========] 3 tests from 1 test suite ran. (144 ms total)
[  PASSED  ] 3 tests.
```

Guard removed (the original `dfly_cntx->conn()->MarkForClose();` put back, rebuilt with
`nice -n 10 ninja -C build-dbg -j3 dragonfly_test`, then restored and rebuilt):

```
[ RUN      ] DflyEngineTest.ReplicatedApplyHandlerThrowNoConnNoCrash
E1003 22:17:19.570737   18233 main_service.cc:1748] Internal error, system probably unstable handler failure
*** SIGSEGV received at time=1791065839 on cpu 0 ***
PC: @     0x55acfad8db58  (unknown)  std::__uniq_ptr_impl<>::_M_ptr()
    @     0x55acfbf6bdd6         32  std::unique_ptr<>::operator bool()
    @     0x55acfbf4a4b4         32  facade::Connection::MarkForClose()
    @     0x55acfb092a49        432  dfly::Service::DispatchCommand()
    @     0x55acfb9b5842        112  dfly::JournalExecutor::Execute()
    @     0x55acfac7073f        800  dfly::DflyEngineTest_ReplicatedApplyHandlerThrowNoConnNoCrash_Test::TestBody()::{lambda()#1}::operator()()
exit=139
```

Open risk: with the guard the failed command is dropped (logged, not retried) and the replication
link stays up, so a replica that hits a throwing handler diverges silently on that key.
