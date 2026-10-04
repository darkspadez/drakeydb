# Task 1.2 report: unwrap in `ConsumeRedisStream`

Branch `feat/phase7-1-rreplay-unwrap` (stacked on P7-0, HEAD `dbc7e6c`). Not committed (the
orchestrator commits). Implementer: Sonnet. Plan: Task 1.2 incl. Step 4b; spec D-3, D-5
("Cancellation and offsets" only), D-15; ledger decisions 6, 11, 14, 23.

Scope kept to P7-1: envelope commands apply per command with the link's own apply context, **without**
mvcc stamping, author mapping, dedup or the LWW guard (P7-2). KeyDB-only and unknown inner commands
are Task 1.3, replica active expiry Task 1.4.

## Summary

- `ClassicApplier` (new, `classic_replay.{h,cc}`) unwraps an `RREPLAY` envelope, one command per
  envelope, nested up to 64, dispatching each command on its own in the envelope's db.
- `Replica::ConsumeRedisStream` hands every `RREPLAY` command to it, after flushing the raw batch, and
  counts the envelope's bytes into `repl_offs_` only once the envelope is consumed.
- The P7-0 interim refusal of an active-KeyDB master is gone, with its tests and helpers (Step 4b).
  The four formerly strict-xfail KeyDB tests pass as normal tests.
- 11 new `ClassicApplyFamilyTest` cases, 1 new `ClassicReplayTest` case, and 7 new pytest test
  functions (10 cases once parametrized): 4 against a real KeyDB v6.3.4, 3 against a scripted master.

## What changed

| File | Change |
|---|---|
| `src/server/classic_replay.h:72-168` | `ClassicLinkStats` (relaxed atomics `rreplay_unwrapped`, `rreplay_malformed`, `rreplay_self_dropped`, `classic_apply_errors`), `EnvelopeResult`, `ClassicApplier` (`kMaxNesting = 64`, `IsRreplay`, `HandleRreplay(args, depth = 1)`). |
| `src/server/classic_replay.cc:123-267` | `ClassicApplier`: ctor/dtor, `IsRreplay`, `HandleRreplay` (the loop), `ParseSingleCommand`, `ApplyCommand`, `Dispatch`, `SelectDb`, `NoteMalformed`, `NoteApplyError`; file-local `IsControlCommand`, `RreplayParseName`. |
| `src/server/replica.h:16`, `:297-300` | includes `classic_replay.h`; member `ClassicLinkStats classic_stats_`; the `master_active_replica_` comment no longer says it is never set on a greeted link. |
| `src/server/replica.cc:1176-1182` | the `ClassicApplier` of the stream, built beside `null_builder`. |
| `src/server/replica.cc:1222-1266` | the batch-dispatch block, unchanged in body, moved into a `flush_batch` lambda. |
| `src/server/replica.cc:1290-1305` | the envelope branch: flush; `if (!exec_st_.IsRunning()) break;`; `HandleRreplay`, `kNotConsumed` breaks; `ConsumeInput`; `repl_offs_ += total_read`; `replica_waker_.notify()`; `continue`. |
| `src/server/replica.cc:1350` | the old bottom dispatch is now `if (io_buf.InputLen() == 0 \|\| batch.size() >= max_batch) flush_batch();`. |
| `src/server/replica.cc` (Step 4b) | removed: `Start()`'s `protocol_not_supported` block; the `protocol_not_supported` clause of `MainReplicationFb`'s quiet-greeting condition (the original text restored); `refuse_active_replica_master` and its two call sites in `Greet()`; the `rreplay_dropped` counter and its `LOG_EVERY_T(ERROR, 30)`. `ParseCapaReply` and `master_active_replica_` stay. `grep -rn "P7-0 interim" src tests` is empty. |
| `src/server/classic_replay_test.cc:399-` | `ClassicApplyFamilyTest` (11 cases) and `ClassicReplayTest.IsRreplayIsCaseInsensitiveAndNeedsAString`. |
| `tests/dragonfly/keydb_onboarding_test.py` | Step 4b removals, 10 new test cases, one incidental fix (below). |
| `tests/dragonfly/multimaster_test.py` | Step 4b: `test_greet_refuses_active_replica_capa_reply` and its two constants deleted; `+OK active-replica` and `+OK active-replica keydb-fastsync-save` added to the parameters of `test_greet_accepts_capa_reply_with_capability_words` (name kept). |
| `docs/UPSTREAM-SYNC.md`, `docs/PLAN.md` | the `replica.cc` watchlist row now describes the unwrap hook and the moved batch block (and no longer the refusal); the `classic_replay.*` row names Tasks 1.1/1.2; PLAN.md's note says the refusal was lifted. |

## Design notes

- **The unwrap is a loop, not a recursion.** `HandleRreplay` keeps one `RespVec` for the next layer and
  walks down. A fiber's stack is 40 KB in a release build (`dfly_main.cc:156-160`), 56 KB in a debug one;
  `sizeof` gives `RedisParser` 192 B, `RespVec` 24 B, `RreplayEnvelope` 64 B, so a recursive frame per
  level is a few hundred bytes before the callees', times 64, plus the leaf dispatch. I did not measure
  a recursive version; the loop makes the question moot. `depth` keeps its plan meaning (the level the
  call starts at, 1 off the stream), and `NestedUnwrapAllowedTo64AndRefuses65th` runs 64 and 65 levels on
  a default-sized test fiber. Views are safe because a layer's `inner` points at the stream bytes, not into the
  vector it was parsed from.
- **The applier owns its `ONLY_ERR` builder**, instead of receiving one. `NONE` records nothing and
  `InvokeCmd` consumes `last_error_` itself, so a command that failed (`WRONGTYPE`) would look like a
  success. Owning it makes the mode a property the gtest can falsify (see (g)).
- **Counters** (all per layer): `rreplay_unwrapped` counts layers that parsed, were not self-authored,
  and whose inner bytes were exactly one command, added once the call returns consumed, so a
  `kNotConsumed` call moves nothing; `rreplay_malformed` the layers that were not (parse failure,
  inner not one command, nesting past 64); `rreplay_self_dropped`; `classic_apply_errors` the commands
  that did not apply (rejected by the dispatcher or replied an error; also a failed synthetic SELECT,
  which skips the command). A malformed inner envelope leaves the outer layer counted as unwrapped
  (decision 14's "per level").
- **Cancellation**: `running()` is asked once, at depth 1, after parse and self checks and before the
  first dispatch (the synthetic SELECT is one). The loop also checks `exec_st_.IsRunning()` right after
  the flush, with no yield between the two, so in the real stream the applier's own check is not
  reachable as false; the gtests exercise the applier's rule, the live test the loop's.
- **The db is sticky**, as KeyDB's master client's: the innermost envelope that carries a db decides
  (`replication.cpp:5413-5423` parses it, `:5468` and `:5478`/`:5488` carry `c->db->id` down to the inner client and back); a 3-argument envelope runs in the db
  already selected, which is the context's `conn_state.db_index`, shared with the raw path. The first use
  of a db dispatches a real `SELECT` (as `JournalExecutor::SelectDb`); later uses set the index.
- **Unknown inner commands** are dispatched and rejected by `DispatchCommand` ("unknown command"), so they
  are counted in `classic_apply_errors` and warned about with `LOG_EVERY_T`; not silently lost. They
  also bump the dispatcher's `unknown_*` stat and rows in `INFO commandstats`, which Task 1.3 removes by
  giving them `classic_unknown_cmds_dropped` and a pre-dispatch check.
- Offsets: the stream loop is the only place that moves `repl_offs_`. Synthetic SELECTs never do.

## Tests

### gtest (`ninja -C /home/user/drakeydb/build-dbg -j4 classic_replay_test && ./build-dbg/classic_replay_test`)

`ClassicApplyFamilyTest` (a `BaseFamilyTest`; each case builds a `Link` on `pp_->at(0)->LaunchFiber`):

| Test | What it pins |
|---|---|
| `AppliesInnerCommandInEnvelopeDb` | SET/INCRBY/HSET/DEL in db 0 and db 3, results read back per db |
| `SelectSetsEnvelopeDb` | first use of a db selects it, later uses switch, 3-arg form keeps the selected db, nested: innermost db wins, an inner without a db inherits the outer's |
| `SkipsInnerControlCommands` | `ping`, `PING`, `REPLCONF GETACK *`, `MULTI`, `Exec`, `SELECT 5` (each carrying db 3) are not dispatched: counting stubs on `PING` and `REPLCONF`, `db_index` stays 0, no MULTI state, no `EXEC` error; a following `SET` applies at once |
| `DropsSelfAuthoredEnvelope` | lowercase and uppercase self uuid, and self nested inside another author's (outer layer unwrapped, inner dropped) |
| `MalformedEnvelopeSkippedAndCounted` | bad uuid, bad db, negative db, bad mvcc, arity, empty inner, two commands, trailing bytes, trailing newline, not RESP, huge array and huge string lengths, and the inner cut at **every** length: each consumed, `rreplay_malformed` +1, nothing applied, then the next envelope applies |
| `MalformedInnerEnvelopeLeavesOuterUnwrapped` | per-level malformed |
| `NestedUnwrapAllowedTo64AndRefuses65th` | 64 levels apply with `running()` asked once; 65 levels: the 65th malformed, 64 layers unwrapped, consumed; `HandleRreplay(args, 64)` applies, `(args, 65)` is malformed; a lowercase inner `rreplay` is an envelope |
| `KnownCommandErrorReplyCounted` | `INCR` on a hash: `classic_apply_errors == 1`, consumed, the next envelope applies |
| `RejectedDispatchCountsAndConsumes` | wrong-arity `SET k`, and an unknown command |
| `RunningFalseBeforeDispatchReturnsNotConsumed` | nothing dispatched, `db_index` unchanged (no synthetic SELECT), no counter moves, for flat and nested envelopes; a call at depth 2 does not ask |
| `RunningFalseDuringFirstDispatchStillConsumesWholeEnvelope` | `running()` answers true once and false after; a 3-level envelope in db 3 applies whole, consumed, asked exactly once |

Plus `ClassicReplayTest.IsRreplayIsCaseInsensitiveAndNeedsAString`.

Final run: `[  PASSED  ] 24 tests.` (13 `ClassicReplayTest`, 11 `ClassicApplyFamilyTest`).

### pytest (`flock /tmp/drakey-pytest.lock env DRAGONFLY_PATH=/home/user/drakeydb/build-dbg/dragonfly KEYDB_SERVER_PATH=<scratchpad>/KeyDB/src/keydb-server KEYDB_REQUIRED=1 /root/drakey-venv-pinned/bin/python -m pytest tests/dragonfly/keydb_onboarding_test.py -q`)

| Test | Master | What it pins |
|---|---|---|
| `test_plain_replica_unwraps_keydb_rreplay` | real active KeyDB | strings (plain, `EX`, `PX`), counters, hash, list, delete, `EXPIRE`, db 0 and db 1; dbsize equal; one full sync |
| `test_plain_replica_unwraps_nested_keydb_rreplay` | real KeyDB A replicating from B, forwarding on | B's writes (depth 2) and A's own (depth 1) reach A's replica; `INCR` exact |
| `test_unwrap_keeps_offsets_exact` | real KeyDB, 1 s cron PING | `WAIT 1` returns 1; replica `slave_repl_offset` equals KeyDB `master_repl_offset` at idle, repeated over several ping periods |
| `test_stopping_the_link_while_envelopes_stream_is_clean` | real KeyDB, a saturating `INCR` writer | 4 stops with `REPLICAOF NO ONE` mid-stream, replica alive, then exact convergence |
| `test_unwrap_flushes_raw_batch_before_envelope[plain_replica\|peer_mode]` | scripted | raw `SET a 1`, env `SET a 2`, raw `INCR a`, env `INCR a` end at 4; offsets exact; one connection |
| `test_unwrap_skips_malformed_envelopes_without_disconnect` | scripted | bad uuid, bad db, two-command inner, non-command inner, empty inner, unknown, `PING`, `SELECT` skipped; good ones apply (one in db 3); one connection; exact offsets; the warning is logged |
| `test_unwrap_applies_envelope_larger_than_the_read_buffer[100000\|400000\|3000000]` | scripted | envelope split over several reads: value intact, offset exact |
| `test_keydb_active_handshake_and_full_sync`, `_peer_mode`, `test_keydb_active_live_write_during_full_sync[plain_replica\|peer_mode]` | real KeyDB | **no longer xfail**; pass |
| `test_greet_accepts_capa_reply_with_capability_words[active\|active_fastsync-capa_eof\|capa_dragonfly]` | proxy over Redis | the active replies are accepted again at both capa sites |

The scripted master never answers `active-replica`: the unwrap goes by what the stream holds, so
this also pins that a master that did not advertise it is unwrapped.

## Step 2: failing before

**gtests.** The applier had been written before its tests, so "failing first" was shown by stubbing
`HandleRreplay` to `return EnvelopeResult::kConsumed;` and running
`./build-dbg/classic_replay_test --gtest_filter='ClassicApplyFamilyTest.*'`:

```
[==========] 11 tests from 1 test suite ran. (293 ms total)
[  PASSED  ] 0 tests.
[  FAILED  ] 11 tests, listed below:
[  FAILED  ] ClassicApplyFamilyTest.AppliesInnerCommandInEnvelopeDb
[  FAILED  ] ClassicApplyFamilyTest.SelectSetsEnvelopeDb
[  FAILED  ] ClassicApplyFamilyTest.SkipsInnerControlCommands
[  FAILED  ] ClassicApplyFamilyTest.DropsSelfAuthoredEnvelope
[  FAILED  ] ClassicApplyFamilyTest.MalformedEnvelopeSkippedAndCounted
[  FAILED  ] ClassicApplyFamilyTest.MalformedInnerEnvelopeLeavesOuterUnwrapped
[  FAILED  ] ClassicApplyFamilyTest.NestedUnwrapAllowedTo64AndRefuses65th
[  FAILED  ] ClassicApplyFamilyTest.KnownCommandErrorReplyCounted
[  FAILED  ] ClassicApplyFamilyTest.RejectedDispatchCountsAndConsumes
[  FAILED  ] ClassicApplyFamilyTest.RunningFalseBeforeDispatchReturnsNotConsumed
[  FAILED  ] ClassicApplyFamilyTest.RunningFalseDuringFirstDispatchStillConsumesWholeEnvelope
11 FAILED TESTS
```

**pytest.** With the refusal and the xfail markers removed, but the stream hook disabled
(`if (false && ClassicApplier::IsRreplay(last_args[0]))`, so `RREPLAY` is batched and dispatched as an
unknown command as before), rebuilt (`ninja -C build-dbg -j4 dragonfly`), run with
`... pytest tests/dragonfly/keydb_onboarding_test.py -q --tb=line -k "unwrap or active_live_write"`:

```
FAILED  test_keydb_active_live_write_during_full_sync[df_factory0-plain_replica]   E   AssertionError: 331 of 331 live writes missing, e.g. ['live:0', 'live:1', 'live:2']
FAILED  test_keydb_active_live_write_during_full_sync[df_factory0-peer_mode]       E   AssertionError: 332 of 332 live writes missing, e.g. ['live:0', 'live:1', 'live:2']
FAILED  test_plain_replica_unwraps_keydb_rreplay[df_factory0]
FAILED  test_plain_replica_unwraps_nested_keydb_rreplay[df_factory0]
PASSED  test_unwrap_keeps_offsets_exact[df_factory0]
FAILED  test_unwrap_flushes_raw_batch_before_envelope[df_factory0-plain_replica]
FAILED  test_unwrap_flushes_raw_batch_before_envelope[df_factory0-peer_mode]
FAILED  test_unwrap_skips_malformed_envelopes_without_disconnect[df_factory0]
```

(`test_keydb_active_handshake_and_full_sync` and `_peer_mode` passed in that run too: their full sync
lands and they write nothing after it. The big-envelope and stop tests did not exist yet.)
`test_unwrap_keeps_offsets_exact` passes **without** the unwrap, as the plan says it would: today's
build advances the offset past the dropped envelopes, so the test guards the new branch's accounting
(falsification (b)), not the unwrap.

## Step 4 falsification (verbatim excerpts)

Every one: edit, rebuild, run, restore the source (`diff` against the saved copy empty), rebuild.
gtest ones: `ninja -C /home/user/drakeydb/build-dbg -j4 classic_replay_test && ./build-dbg/classic_replay_test
--gtest_filter='ClassicApplyFamilyTest.*'`; pytest ones: `ninja ... dragonfly` then the `flock ... pytest ... -k`
shown. Line numbers are those of the file at the time.

**(a) drop the pre-envelope flush** (`flush_batch();` removed from the envelope branch;
`-k test_unwrap_flushes_raw_batch_before_envelope`):

```
E   AssertionError: a ended at 2: the stream was not applied in order
E   AssertionError: a ended at 2: the stream was not applied in order
FAILED  test_unwrap_flushes_raw_batch_before_envelope[df_factory0-plain_replica]
FAILED  test_unwrap_flushes_raw_batch_before_envelope[df_factory0-peer_mode]
======================= 2 failed, 26 deselected in 2.49s =======================
```
(The plan expects `a == 1`; my four-command stream ends at 2: the envelopes run first, 2 then 3, and the
batch `SET a 1`, `INCR a` after them gives 2.)

**(b) skip `repl_offs_ += response->total_read` on the envelope branch**
(`-k "test_unwrap_keeps_offsets_exact or test_unwrap_flushes or test_unwrap_skips"`):

```
E   assert 0 == 1                                    (the WAIT 1 of test_unwrap_keeps_offsets_exact)
E   AssertionError: [0, 1000, 1048, 1048, 1048, 1048, ...]   (test_unwrap_flushes_..., plain; offset stays at the sync's 1000)
E   AssertionError: [0, 1000, 1048, 1048, 1048, 1048, ...]   (peer_mode)
E   AssertionError: [0, 1000, 1029, 1029, 1029, 1029, ...]   (test_unwrap_skips_malformed_...)
====================== 4 failed, 24 deselected in 10.47s ======================
```
(The list is pytest's truncated repr of the ACKs. They start at the full sync's 1000 and rise by the raw
commands only: the envelopes' bytes are never counted, so the settled offset ends short of 1000 plus the
stream's length by exactly the envelopes' total.)

**(p) count only the last read of an envelope** (`repl_offs_ += response->left_in_buffer`;
`-k "larger_than or test_unwrap_keeps_offsets_exact"`): only the big-envelope test sees it.

```
E   AssertionError: [0, 1000, 84964, 84964, 84964, 84964, ...]   ([...-100000])
E   AssertionError: [0, 1000, 73682, 73794, 73794, 73794, ...]   ([...-400000])
E   AssertionError: [0, 1000, 88084, 88196, 88196, 88196, ...]   ([...-3000000])
PASSED  test_unwrap_keeps_offsets_exact[df_factory0]
================= 3 failed, 1 passed, 27 deselected in 10.61s =================
```

**(c) remove the 65th-nesting refusal** (the `if (depth > kMaxNesting)` block deleted):

```
classic_replay_test.cc:697: Failure   link.malformed()  Which is: 0   (expected 1u)
classic_replay_test.cc:698: Failure   link.unwrapped()  Which is: 129 (expected 64u + 64u = 128)
classic_replay_test.cc:704, :707, :712: Failure  link.malformed() Which is: 0 (expected 1u, 2u, 2u)
classic_replay_test.cc:720: Failure   Get("deep65") Which is: ("v")   (expected nullopt)
classic_replay_test.cc:722: Failure   Get("start65") Which is: ("v")
[  FAILED  ] ClassicApplyFamilyTest.NestedUnwrapAllowedTo64AndRefuses65th (31 ms)
[  PASSED  ] 10 tests.
```

**(d) check `running()` after the first dispatch instead of before it** (`SelectDb(*env.db)` inserted
ahead of the check):

```
classic_replay_test.cc:771: Failure   link.cntx.conn_state.db_index  Which is: 3  (expected 0u)
[  FAILED  ] ClassicApplyFamilyTest.RunningFalseBeforeDispatchReturnsNotConsumed (27 ms)
[  FAILED  ] ClassicApplyFamilyTest.SkipsInnerControlCommands (25 ms)     (its db_index check: a skipped command selected db 3)
[  PASSED  ] 9 tests.
```

**(e) check `running()` before every dispatch** (`if (!running_()) return;` ahead of the SELECT in
`ApplyCommand`):

```
classic_replay_test.cc:693: Failure   running_calls.load()  Which is: 2  (expected 1)
classic_replay_test.cc:789: Failure   Get("k2")  Which is: (nullopt)    (the depth-2 call, running() false)
classic_replay_test.cc:803: Failure   running_calls.load()  Which is: 2  (expected 1)
classic_replay_test.cc:807: Failure   Get("k", 3)  Which is: (nullopt)  (the leaf was skipped)
[  FAILED  ] NestedUnwrapAllowedTo64AndRefuses65th, RunningFalseBeforeDispatchReturnsNotConsumed,
             RunningFalseDuringFirstDispatchStillConsumesWholeEnvelope
[  PASSED  ] 8 tests.
```

**(f) accept a second inner command** (`consumed <= bytes.size()` in `ParseSingleCommand`): gtest
`MalformedEnvelopeSkippedAndCounted` fails (`link.malformed()` stays behind `++expected` from the two-command
case on, 6 vs 7, 6 vs 8, ...); pytest `-k test_unwrap_skips_malformed_envelopes_without_disconnect` fails
too: `AssertionError: x` (the first command of the two-command inner was applied).

**(g) the `NONE` builder** (`facade::ReplyMode::NONE` in the constructor):

```
classic_replay_test.cc:732: Failure   link.apply_errors()  Which is: 0  (expected 1u)
classic_replay_test.cc:738: Failure   link.apply_errors()  Which is: 0  (expected 1u)
[  FAILED  ] ClassicApplyFamilyTest.KnownCommandErrorReplyCounted (23 ms)
[  PASSED  ] 10 tests.
```
(`RejectedDispatchCountsAndConsumes` still passes under `NONE`: a rejected dispatch returns
`DispatchResult::ERROR`, which `Dispatch` counts without the payload. Only an error reply from a command
that ran needs the payload.)

Mine, beyond the plan:

- **(h) dispatch inner `PING`**: `SkipsInnerControlCommands`: `pings.load() Which is: 2 (expected 0)`.
- **(i) dispatch inner `MULTI`**: `Actual: true` for `exec_info.IsCollecting()`, and `Get("after")` is
  `(nullopt)`: the following `SET` is only queued.
- **(j) ignore the envelope's db**: `AppliesInnerCommandInEnvelopeDb` (`Get("k3", 3)` is `(nullopt)`,
  `Get("k3", 0)` is `("v3")`) and `SelectSetsEnvelopeDb` (`db_index` stays 0 where 3 and 5 are expected).
- **(k) the outermost envelope's db wins**: `SelectSetsEnvelopeDb`: `Get("e", 7)` is `(nullopt)`.
- **(n) unwrap one level only** (the inner `RREPLAY` is dispatched as a command): pytest
  `-k "plain_replica_unwraps_nested or plain_replica_unwraps_keydb"`: the nested test fails
  (`assert equals failed`, line 410), the flat one passes (`1 failed, 1 passed`).

### Step 4b falsification (the refusal put back)

`replica.cc` with the five removed pieces re-inserted from `HEAD`, rebuilt, run with
`-k "active_handshake or active_live_write or (test_greet_accepts_capa_reply_with_capability_words and active)"`
on `keydb_onboarding_test.py` and `multimaster_test.py`:

```
FAILED  keydb_onboarding_test.py::test_keydb_active_handshake_and_full_sync[df_factory0]
FAILED  keydb_onboarding_test.py::test_keydb_active_handshake_peer_mode[df_factory0]
FAILED  keydb_onboarding_test.py::test_keydb_active_live_write_during_full_sync[df_factory0-plain_replica]
FAILED  keydb_onboarding_test.py::test_keydb_active_live_write_during_full_sync[df_factory0-peer_mode]
FAILED  multimaster_test.py::test_greet_accepts_capa_reply_with_capability_words[df_factory0-active-capa_eof]
FAILED  multimaster_test.py::test_greet_accepts_capa_reply_with_capability_words[df_factory0-active-capa_dragonfly]
FAILED  multimaster_test.py::test_greet_accepts_capa_reply_with_capability_words[df_factory0-active_fastsync-capa_eof]
FAILED  multimaster_test.py::test_greet_accepts_capa_reply_with_capability_words[df_factory0-active_fastsync-capa_dragonfly]
E   redis.exceptions.ResponseError: Protocol not supported: master advertises active-replica; unsupported until P7-1   (x8)
====================== 8 failed, 97 deselected in 10.61s ======================
```

## Final results (debug build of the final tree, gcc 13.3)

```
$ ./build-dbg/classic_replay_test
[==========] 24 tests from 2 test suites ran. (296 ms total)
[  PASSED  ] 24 tests.

$ ... pytest tests/dragonfly/keydb_onboarding_test.py                                         # KEYDB_REQUIRED=1
======================== 32 passed in 81.91s (0:01:21) =========================
$ ... pytest tests/dragonfly/keydb_harness_test.py
============================== 21 passed in 1.77s ==============================
$ ... pytest tests/dragonfly/multimaster_test.py -k "greet or capa or keydb or flushed"
====================== 18 passed, 59 deselected in 28.33s ======================
$ ... pytest tests/dragonfly/redis_replication_test.py
====================== 12 passed, 7 deselected in 59.17s =======================
```

Also run, because `replica.h` changed: `peer_replication_test` (23 passed), `rdb_test` (138 passed),
`multi_master_test` (220 passed, 1 skipped: `NodeIdentityFile.UnwritableDirIsEphemeral`, running as
root). `ctest -L DFLY` and the other pytest files of the gate are the sub-PR gate's, not run here.

Repeats of the timing-sensitive tests (pass rates):

- `-k "test_unwrap_ or psync_bad_header or empty_command_name"` x10 (11 tests per loop): 110 of 110.
- `-k "plain_replica_unwraps or active_live_write or active_handshake"` x5 (6 tests): 30 of 30.
- `-k stopping_the_link` x3, then x6 more while instrumented (below): 9 of 9.

## What each test would still pass under with the feature removed

- `test_unwrap_keeps_offsets_exact`: passes with the unwrap removed entirely (recorded above). It also
  passes with `left_in_buffer` for `total_read` (falsification (p)): envelopes arrive in one read there.
- `test_keydb_active_handshake_and_full_sync` / `_peer_mode`: pass with the unwrap removed; they fail
  only if the refusal is back. `test_stopping_the_link_while_envelopes_stream_is_clean` asserts liveness
  and convergence after a full sync, so it too passes with the unwrap removed (not run that way); I
  instrumented the stop path once (a temporary `LOG`, removed, binary rebuilt and checked free of it)
  to confirm what it reaches: every one of its four stops per run took the loop's
  `!exec_st_.IsRunning()` break right after the flush, in 6 of 6 runs; the applier's `kNotConsumed`
  was never reached through the loop (see "Open risks").
- The gtests do not exercise `ConsumeRedisStream`; they pin the applier. A stream-loop regression
  (envelope branch order, offsets, flush) is caught by the pytest tests only.
- `RejectedDispatchCountsAndConsumes` passes under the `NONE` builder (above).

## Deviations from the plan and spec, with reasons

1. **No `AuthorDedup*` / `ClassicAuthorMap*` seams and no link-uuid argument** on `ClassicApplier`. The
   plan lists them as optional and null until P7-2; unread members would be dead code (and trip clang's
   `-Wunused-private-field`, which CI's clang leg would turn into an error). P7-2 adds them with their first reader.
2. **The applier owns its `ONLY_ERR` builder** rather than receiving it (design notes): the plan's falsification
   (g) is only meaningful if the builder's mode is chosen where the test can reach it.
3. **Iterative unwrap**, same public signature and semantics (design notes).
4. `SelectSetsEnvelopeDb` is the plan's `SelectSetsDbWithoutTouchingOffsets`: the applier has no offset
   interface, so "without touching offsets" is covered by the pytest exactness tests, not the gtest.
5. `RunningFalseDuringFirstDispatch...` makes `running()` answer true once and false ever after and asserts
   it is asked exactly once, instead of flipping a flag from inside the first dispatch. Both plan
   falsifications, (d) and (e), fail it; a stub of `SELECT` would have had to re-implement SELECT.
6. **Unknown inner commands** are not given a counter here (Task 1.3); they count as `classic_apply_errors`
   (see design notes).
7. Beyond the plan: `MalformedInnerEnvelopeLeavesOuterUnwrapped`, the `IsRreplay` test, the three
   scripted-master pytest cases (malformed envelopes without a disconnect, big envelope over several reads,
   peer-mode flush) and the live stop test. The big-envelope test is the only one that sees falsification (p).
8. Step 4b: `running_node_log_lines` (unused after the removals) and the `re` import of
   `keydb_onboarding_test.py` were deleted with it; `script_uuid`, `script_capa_reply`, `drop_connections` of
   `fake_classic_master.py` stay, as the plan allows.
9. **Incidental fix to a P7-0 test.** `test_psync_bad_header_line_is_logged_as_received` failed once in a
   full run: `assert 2 >= 3` on `master.psync_requests` after `attach_and_watch_retries` had stopped waiting
   at the third *connection*, which is accepted a few requests before its PSYNC arrives. It is 10 of 10 in
   isolation and unrelated to this task, but it would flake the gate, so the helper now waits for the
   PSYNCs it asserts on.
10. `docs/UPSTREAM-SYNC.md` and `docs/PLAN.md` were updated as described; the spec's D-2 "interim refusal"
    paragraph and the plan's checkboxes are the orchestrator's to fold.

## Not done / not verified

- No release, ASAN or UBSAN build. A `clang++ -fsyntax-only` of the three touched translation units
  (`classic_replay.cc`, `classic_replay_test.cc`, `replica.cc`) reports no diagnostic located in them; its
  only error is the upstream `compact_object.h` unused private field `reserved_` that this tree already has.
- The stack claim behind the iterative design is by `sizeof`, not by measuring a recursive build.
- `ctest -L DFLY` in full and the other gate pytest files were not run.
- Task 1.3 (KeyDB-only drop, INFO, Prometheus) and 1.4 (activeExpire) untouched; the counters have no
  reader beyond the gtests until 1.3.

## Open risks and what a reviewer should attack

1. **Offset exactness** is by construction (`repl_offs_ += total_read` after the envelope is consumed) and by
   test: real KeyDB with cron PINGs and `WAIT`'s GETACK (idle offsets equal), the scripted master (exact
   ACKs for flat, malformed, mixed and 100 KB / 400 KB / 3 MB envelopes). Attack: an envelope that
   `ReadRespReply` returns after an `INPUT_PENDING` split where `total_read` and the bytes the envelope
   occupies could disagree (the P7-0 hand-off, `pending_stream_bytes_`, feeds the same path; the
   `diskless_token_split` case covers a cut command).
2. **The stream loop's two `running` checks** (the loop's, then the applier's depth-1 one) read the same
   condition with no yield between, so the applier's `kNotConsumed` is unreachable through the loop today;
   it matters when P7-2's `Reserve` can wait. Verify that the loop's `break` leaves `repl_offs_` at the first
   unapplied raw command (`flush_batch` leaves unapplied commands uncounted).
3. **Sticky db shared with the raw path.** After an envelope selects db 3, a raw command with no preceding
   raw `SELECT` runs in db 3 (KeyDB's own master client behaves so). A stream mixing both is not expected
   from KeyDB (an active one wraps everything), so this is parity, not isolation.
4. **Unknown / KeyDB-only inner commands** (`PEXPIREMEMBERAT`, `KEYDB.*`) are dispatched until Task 1.3: each
   is an "unknown command" in `INFO commandstats` and a `classic_apply_errors` count, and a rejected command
   logs a WARNING from `DispatchCommand` itself, unthrottled (arity errors; unknown commands do not).
5. **Parser noise on garbage**: a malformed inner can make `RedisParser` log (`Failed to parse len ...`,
   `Multibulk len is too large`) at ERROR/WARNING without a rate limit. The skip and counter are exact.
6. **Per-envelope allocations**: one `RespVec` growth, one `RreplayEnvelope::uuid` string (36 chars: heap), a
   local `CommandContext`, and the dispatcher's own work per command. Task 1.5 measures; a reusable member
   `RespVec` and context are the obvious first step.
7. **Merge hazard**: the moved batch block in `replica.cc` conflicts with any upstream edit of it; the
   UPSTREAM-SYNC row says how to resolve.
8. **Peer links**: applied with the link's `repl_origin_idx` and `repl_mvcc = 0`, exactly as raw commands are,
   so no-forward holds as before; real stamps and the guard are P7-2. `decision 6` is covered by the
   `peer_mode` variants (scripted and live-write).

## Checks

`pre-commit run --files <the source, test and doc files above>` at the end of the task: `pyflakes`,
`trim trailing whitespace`, `fix end of files`, `check python ast`, `Clang formatting`, `black` all Passed
(the first pass reformatted one C++ file; the second was clean, and the tree was rebuilt and the gtest rerun).
