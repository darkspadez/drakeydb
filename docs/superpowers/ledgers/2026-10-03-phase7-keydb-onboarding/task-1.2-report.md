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

## Review fix round (Opus review of `c419ead`)

Same branch, HEAD `1d7b324`, still uncommitted. Findings C1, m1, m2, m4, m6, m9 fixed; m3, m5, m7, m8
recorded below (no code). Scratch material (probes, falsified binaries, outputs) is in the
orchestrator's scratchpad under `impl-p71/`.

### What changed, per finding

| Finding | Change |
|---|---|
| **C1** (inner `*0` / `*-1` aborts the replica) | `ClassicApplier::ParseSingleCommand` returns false unless the first element is a `STRING`, so the layer is `rreplay_malformed` and skipped (decision 14). The parser gives `*0` an `ARRAY`, `*-1` a `NIL_ARRAY` and `*1\r\n$-1\r\n` a `NIL` with an empty buffer (`redis_parser.cc:330`, `:334`, `:381-383`); all three fall under the rule. Before the fix `*0` and `*-1` aborted (`bad_variant_access` in `ApplyCommand`) and the nil name was dispatched as an unknown command with an empty name (a `classic_apply_errors`, not a malformed layer). |
| **C1** (raw `*0` / `*-1`, upstream) | `replica.cc`, one `// drakeydb: U-14` hunk: a command whose first element is an `ARRAY` or `NIL_ARRAY` is skipped with `LOG_EVERY_T(WARNING, 60)` ("Skipping a command without a name from <master>") and falls through to the existing not-queued bookkeeping, so its bytes are counted exactly (at once, or deferred onto the batch ahead of it). The hunk turns `if (!last_args.empty())` into an `else if`. A raw nil *string* name (`*1\r\n$-1\r\n`) does not crash (it reads as an empty name) and stays on U-13's path (dispatched as an unknown command), so only what crashed changed. Registered as **U-14** in `docs/ISSUE-REGISTER.md`, listed in the spec's byte-identity exceptions (item 2, after U-13) and in the `replica.cc` row of `docs/UPSTREAM-SYNC.md` (U-13 was there). |
| **m1** (sticky db) | KeyDB's order, read from `replication.cpp` and then checked against a real KeyDB (below): arity (`:5389`), uuid (`:5398`), arg2 (`:5406`), then **db parsed, range-checked and `selectDb`'d (`:5416`)**, then mvcc (`:5427`), then self (`:5435`), then nesting `FPush` (`:5442`). So a bad mvcc *after a good db leaves the db selected*, and a failure before the db does not. `HandleRreplay` now selects the db of every layer that is `kOk` or `kBadMvcc` right after the depth-1 `running()` check and before the self check; the leaf runs in whatever the context has selected (`ApplyCommand` lost its db parameter), and a failed `SelectDb` skips the leaf unless a deeper layer selects another db (the old "innermost db decides" semantics, kept). `ParseRreplayEnvelope` hands back `out->db` on `kBadMvcc` (the one failure after the db was taken). The nesting check moved after the self check, as in KeyDB (`:5435` then `:5442`): a 65th layer selects its db before it is refused, and a self-authored 65th layer is a self drop, not an overflow. |
| m1 (consequence) | The `running()` check now precedes the db select, so it precedes the self check too: a self-authored depth-1 envelope (or one with a bad mvcc and a db) on a stopping link returns `kNotConsumed` instead of being consumed. Nothing is touched, and the replayed envelope is dropped again after the resume. |
| m1 (text) | `classic_replay.h` class comment, `replica.cc` comment at the applier's construction (it now says what is true), spec D-3 steps 2-3 rewritten (cancellation is now step 2, "db, then self, then nesting" step 3; the two `D-3.3` references in D-5 became `D-3.2`), spec "Inner db" became "Selected db", spec D-15 rows added. |
| **m2** | `test_unwrap_selected_db_is_the_one_the_raw_commands_after_it_run_in` (scripted master). |
| **m4** | `replica.h`: "Nothing reads it yet: Task 1.3 (INFO) and Task 1.4 (the activeExpire decision) will, and must do so only once R_GREETED is set". `docs/PLAN.md` `replica.h/.cc` row: P7-1 (Task 1.2) delivers the unwrap and the U-14 skip; `capa activeExpire` stays future. |
| **m6** | "would not fit it" became "could exceed it" (unmeasured). |
| **m9** | `DCHECK_GE(depth, 1u)` at the top of `HandleRreplay`. Exercised once with a temporary gtest calling `Apply(..., 0)`: `Check failed: depth >= 1u (0 vs. 1)`, SIGABRT; the temporary test was removed (a permanent death test is unsafe next to the proactor threads of `BaseFamilyTest`). |

### Tests added or changed

gtest (`classic_replay_test.cc`), 24 became 25 tests:

- `ClassicApplyFamilyTest.MalformedEnvelopeSkippedAndCounted`: three inner cases added, `*0\r\n`, `*-1\r\n`, `*1\r\n$-1\r\n`, each consumed with `rreplay_malformed` +1.
- `ClassicApplyFamilyTest.SkipsInnerControlCommands`: pinned the old behaviour (`db_index` stays 0); now each control envelope carries db 3, the context ends in 3 (not 5: the inner `SELECT 5` is not dispatched), and a 3-argument `SET` afterwards lands in db 3 and nowhere else.
- **New** `ClassicApplyFamilyTest.DbIsSelectedBeforeTheAuthorAndInnerChecks`: self-authored envelope in db 3 then a 3-argument envelope (applies in db 3: the case the brief asked for); bad mvcc after a good db; inner that is not a command; inner `*0`; arity, uuid and db failures leave the db alone; nested self-authored inner; nested bad-mvcc inner; the 65th layer.
- `ClassicApplyFamilyTest.RunningFalseBeforeDispatchReturnsNotConsumed`: a self-authored and a bad-mvcc envelope with a db are `kNotConsumed` and leave `db_index` 0; an envelope that fails before its db is consumed.
- `ClassicReplayTest.ParseRreplayEnvelopeChecksInKeyDbOrder`: `kBadMvcc` hands back the db.

pytest (`keydb_onboarding_test.py`), four new cases (the file now collects 36):

- **New** `test_classic_stream_command_with_an_array_for_a_name_does_not_abort[empty_array|nil_array|behind_a_queued_command]` (raw `*0`, raw `*-1`, and a raw `*0` behind a queued raw command, which takes the deferred-ACK path): replica alive, keys applied, settled ACK exact, one connection, the warning logged.
- **New** `test_unwrap_selected_db_is_the_one_the_raw_commands_after_it_run_in` (m2): envelope in db 2, raw `SET c`, 3-argument envelope `SET d`, envelope in db 0, raw `SET g`: e, c, d in db 2; f, g in db 0; exact ACKs.
- `test_unwrap_skips_malformed_envelopes_without_disconnect`: an inner `*0` added (mvcc values renumbered to stay increasing).

### Falsification (each against the change it guards; observed, then restored and diffed against a saved copy)

gtest, `cd /home/user/drakeydb/build-dbg && nice ninja -j3 classic_replay_test && ./classic_replay_test --gtest_filter=<..>`:

| Change reverted | Filter | Observed |
|---|---|---|
| F1: `ParseSingleCommand` without the `STRING` check | `ClassicApplyFamilyTest.MalformedEnvelopeSkippedAndCounted` | exit 134; `std::__throw_bad_variant_access <- std::get <- RespExpr::GetBuf <- RespExpr::GetView <- ClassicApplier::ApplyCommand <- ClassicApplier::HandleRreplay` |
| F1b: check weakened to reject only `ARRAY`/`NIL_ARRAY` | same | `inner nil name`: `link.malformed()` 14 vs `++expected` 15, and later `apply_errors()` 1 vs 0, `unwrapped()` 2 vs 1 (the nil name was dispatched as an unknown command) |
| F2e: db selected after the self check (and so not for `kBadMvcc`) | all | only `DbIsSelectedBeforeTheAuthorAndInnerChecks` fails: `db_index` 0 vs 3 (self-authored), 0 vs 4 (bad mvcc), 2 vs 8 and 2 vs 9 (nested), `after_self` and the others land in db 0 |
| F2b: `ParseRreplayEnvelope` does not hand back the db on `kBadMvcc` | all | `ParseRreplayEnvelopeChecksInKeyDbOrder` (`env.db.has_value()` false) and `DbIsSelectedBeforeTheAuthorAndInnerChecks` (`db_index` 3 vs 4, 2 vs 9) |
| F2c: nesting check back before the parse and select | all | `DbIsSelectedBeforeTheAuthorAndInnerChecks`: `db_index` 1 vs 10 for the 65th layer |
| F2d: `running()` checked after the select | all | `RunningFalseBeforeDispatchReturnsNotConsumed`: `db_index` 3 vs 0 (three places) |
| Whole round reverted: `classic_replay.{h,cc}` from `HEAD` with the new tests | one test per run | `ParseRreplayEnvelopeChecksInKeyDbOrder` fails (`classic_replay_test.cc:403`); `SkipsInnerControlCommands` fails (three expectations); `DbIsSelected...` fails then aborts (`bad_variant_access`); `RunningFalse...` fails (five expectations); `MalformedEnvelope...` aborts, exit 134 |

pytest, `cd /home/user/drakeydb && DRAGONFLY_PATH=<copy of the binary> KEYDB_SERVER_PATH=... KEYDB_REQUIRED=1 flock /tmp/drakey-pytest.lock /root/drakey-venv-pinned/bin/python -m pytest tests/dragonfly/keydb_onboarding_test.py -k "array_for_a_name or malformed_envelopes or selected_db" -q`:

| Binary | Observed |
|---|---|
| `HEAD` sources (before this round) | 4 failed, 1 passed, 4 teardown errors. Failures: `ConnectionRefusedError ... Connect call failed` on the replica's port (the process died) for the three U-14 cases and for `test_unwrap_skips_malformed_envelopes_without_disconnect`; each teardown: `Dragonfly did not terminate gracefully, exit code -6`. The m2 test passed: `HEAD` already shared the context, so it is a regression pin, falsified below. |
| the U-14 hunk of `replica.cc` removed alone | the three U-14 cases fail (process dead, exit -6); the malformed-envelopes test passes (the applier fix is intact) |
| the `STRING` check of `ParseSingleCommand` removed alone | `test_unwrap_skips_malformed_envelopes_without_disconnect` fails (process dead, exit -6); the three U-14 cases pass |
| the applier given a `ConnectionContext` of its own in `ConsumeRedisStream` | `test_unwrap_selected_db_is_the_one_the_raw_commands_after_it_run_in` fails: `AssertionError: c is not in db 2` |

Probes (reviewer's `probe.py`, copied to `impl-p71/` with the binary path and log directory made
configurable, plus a `raw_nil_array` case): on the `HEAD` binary `empty_array`, `nil_array`,
`raw_empty_array` and `raw_nil_array` die with SIGABRT (rc -6); `nil_name` survives as an unknown
command. On the fixed binary all six cases are alive on one connection, the follow-up key applied, the
settled ACK equal to the bytes sent (`control` 1139, `empty_array` 1115, `nil_array` 1116, `nil_name`
1120, `raw_empty_array` 1035, `raw_nil_array` 1036). The probes cannot read the counters (no INFO
reader until Task 1.3): the gtests pin them.

### The sticky-db rules against a real KeyDB

`impl-p71/db_order.py` sends one scripted stream (fake classic master, `FakeClassicMaster` with the
stream built at PSYNC time from the uuid the replica announced) to a real KeyDB 6.3.4 active replica
and to drakeydb, and compares the db each trailing raw probe `SET p_<case> v` lands in. A fresh author
per case, so that KeyDB's per-author mvcc watermark cannot drop an envelope. Probe db by case:

| Case | KeyDB 6.3.4 | drakeydb `HEAD` | drakeydb now |
|---|---|---|---|
| control (`PING`) envelope, db 3 | 3 | 0 | 3 |
| self-authored, db 4 | 4 | 0 | 4 |
| bad mvcc (`abc`) after db 5 | 5 | 0 | 5 |
| unknown inner command, db 6 | 6 | 6 | 6 |
| bad uuid with db 7 / db 99 / uuid only | 6 / 6 / 6 | 6 / 6 / 6 | 6 / 6 / 6 |
| 3-argument envelope (key lands in) | db 6 | db 6 | db 6 |
| nested: self-authored inner in db 8 under db 2 | 8 | 6 | 8 |
| nested: bad-mvcc inner in db 9 under db 2 | 9 | 6 | 9 |
| 65th layer in db 10 under 64 layers in db 1 | 10 | 6 | 10 |
| wrapped `SELECT 5` in an envelope in db 3 | **5** | 6 | **3** |
| envelope in db 0 | 0 | 0 | 0 |

Identical but for the wrapped `SELECT` (KeyDB's `cFake` runs it and `selectDb(c, cFake->db->id)`
hands db 5 up, `:5478`/`:5488`; the applier skips it, spec D-3 step 5; KeyDB never wraps a SELECT, the
db travels in the envelope). The keys of the commands that must not run (`s_self`, `s_badmvcc`,
`s_u`, `s_baddb`, `s_nself`, `s_nbad`, `s_deep`) exist in no db on either side.

A side observation, from the first run of that script (which used `-5` for the bad mvcc): **KeyDB
accepts `-5` as an mvcc.** `getUnsignedLongLongFromObject` (`object.cpp:743-769`) is a bare
`strtoull`: it rejects only an argument with no digits at all (and a zero result with `errno`), so
`-5` wraps to 2^64-5, `12abc` and an overflowing number are accepted, and the command is applied. The
author's watermark then became 2^64-5, so every later envelope of that author with an mvcc was
dropped by KeyDB's dedup. drakeydb's `ParseUnsignedDecimal` is stricter (digits only, fits 64 bits)
and rejects those as `kBadMvcc`. Not changed here (D-1.4 says "u64"); P7-2's dedup is safer for it.

### Record only (no code)

- **m3.** The inner `RedisParser` logs unthrottled for a bad envelope: `Unexpected format`
  (`redis_parser.cc:271`), `Failed to parse len` (`:299`), `Multibulk len is too large` (`:320`).
  A known deviation from "warned about at a limited rate"; the file is upstream's and is not touched.
  The gtest run shows one (`E ... Failed to parse len x x\r\n`). A bad-envelope flood is a log flood.
- **m5.** Per-envelope allocations stay: a fresh `RespVec` (`inner_args`), the 36-character uuid
  string plus its lowercase temporary, a `CommandContext` per command. Deferred to Task 1.5's
  measurement (a reusable member `RespVec` and context are the first thing to try).
- **m7.** P7-2 needs an explicit per-level `(uuid, mvcc)` stack in `HandleRreplay` for the bottom-up
  `IsApplied`/`Advance` of D-5 (an outer level advances after its inner returned `kConsumed`, a
  `kNotConsumed` inner advances nothing): the loop keeps no per-level state today, and
  `kNotConsumed` is only ever returned at depth 1 before anything ran.
- **m8.** `test_stopping_the_link_while_envelopes_stream_is_clean` is a liveness test: it passes with
  the unwrap removed, so it is not falsified coverage of D-5's cancellation. P7-3 must cover
  `kNotConsumed` at stream level (a stop landing between the loop's check and the applier's, which
  cannot be reached today: no yield sits between them).

### Final results (debug build, gcc 13.3, `nice ninja -j3 dragonfly classic_replay_test`, no warnings)

- `./build-dbg/classic_replay_test`: `[  PASSED  ] 25 tests.`
- `keydb_onboarding_test.py` (real KeyDB, `KEYDB_REQUIRED=1`): 36 passed in 83.6 s.
- `redis_replication_test.py`: 12 passed, 7 deselected (the repo's `-m "not large"`) in 61.7 s.
- `multimaster_test.py -k "greet or capa or classic or keydb"`: 18 passed, 59 deselected in 28.2 s.
- Probes (6 cases) as above; `drip.py` at `CHUNK=1 SPLIT=0`, `CHUNK=1 SPLIT=60`: alive, settled ACK
  7110 == expected, max 7110. A second dripped stream with raw and inner `*0`/`*-1`/nil-name commands
  (`drip2.py`, `CHUNK=1 SPLIT=0`, `CHUNK=1 SPLIT=7`, `CHUNK=2 SPLIT=31`, `CHUNK=3 SPLIT=31`): alive,
  settled ACK 1637 == expected.
- `pre-commit run --files <the ten changed files>`: all hooks passed (clang-format left only my own
  lines to reformat; no existing line moved).

### Notes, disagreements and what this round supersedes

1. **The brief's m1 wording was extended once:** "for each layer that parses `kOk`" also covers
   `kBadMvcc`, because KeyDB reads the mvcc after it selected the db (`:5416` before `:5427`) and the
   live run shows it (`bad_mvcc` row). The brief did ask for this to be verified and matched.
2. The cancellation/self ordering of spec D-3 changed as a consequence of m1 (see the table); this
   is a spec edit the brief allowed ("if its wording says otherwise, update the spec text").
3. **Superseded in this report's earlier sections:** the design note "The db is sticky ... the
   innermost envelope that carries a db decides" (every layer selects as it is validated; the
   innermost still decides the leaf's db), the `SkipsInnerControlCommands` row of the gtest table
   (`db_index` stays 0), and open risk 3 (sticky db shared with the raw path: still true, and now
   pinned by a test and checked against KeyDB).
4. U-14's raw guard skips the command, where U-13's dispatches the empty name as an unknown command:
   an array head cannot be dispatched (`FillBackedArgs` would throw on it too).
5. A wrapped `SELECT` is the one deviation from KeyDB's master-client db (above).

### Not done / not verified

- No release, ASAN or UBSAN build; the "nothing on the path depends on the build type" claim of U-14
  is by reading, the abort itself was observed in the debug build only.
- The KeyDB differential is by one scripted stream against KeyDB 6.3.4 only, and stays a scratchpad
  script (`db_order.py`), not a test in the tree: it needs a KeyDB and the stream is built from the
  announced uuid. Its rows are pinned in the gtest and the scripted-master pytest instead.
- In cluster mode a failing synthetic `SELECT` is now attempted once per layer that carries the db
  (`ensured_dbs_` is only set on success), not once per envelope: a few extra counted failures for
  a nested envelope in db > 0. Not measured.
