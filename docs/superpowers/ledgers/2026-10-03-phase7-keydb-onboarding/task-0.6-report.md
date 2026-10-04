# Task 0.6: graceful PSYNC `CHECK`s, the full-sync tail, the fake classic master

Implemented in `a0ee234`. Written afterwards, in the review fix round, from `git show a0ee234`, the
implementer's saved outputs (`scratchpad/p7-0/t06-*.txt`), the Opus reviewer's runs
(`scratchpad/review-baseline-falsify*.txt`, `review-head-run.txt`, `review-baseline-keydb.txt`,
`review-logs-*`) and this round's runs. Output is copied unedited (`…` marks a cut). The
implementer's shell commands were not saved: "Command" lines are the invocation shape. "Baseline"
is the `drakeydb` built from the unmodified main (`c60dfdb`), `scratchpad/baseline-bin/drakeydb`.

## What changed (at `a0ee234`)

The bug (ISSUE-REGISTER U-11): a disk-based classic master (KeyDB's default) flushes the writes it
buffered during its BGSAVE right behind the RDB. `RdbLoader::Load`'s first read ignored the source
limit, so it swallowed them; `Replica::InitiatePSync` then aborted on `CHECK_EQ(0u,
loader.Leftover().size())`, and the other tail `CHECK`s aborted on any malformed tail.

| File | What |
| --- | --- |
| `src/server/rdb_load.cc:2427-2431` | `RdbLoader::Load`: the first read is clamped to `source_limit_` when `source_limit_ < bytes.size()` (the first-read buffer, 16 KB); a limit under 9 bytes, which cannot hold the RDB signature, is `errc::wrong_signature`. Only a caller that set a limit is affected (`replica.cc` is the only one). |
| `src/server/rdb_load.cc:2822-2827` | `EnsureReadInternal`: fewer bytes left under the limit than the read needs is `rdb_file_corrupted` with a log line; `ReadAtLeast` would `DCHECK` (`helio io.cc:143`) in a debug build. |
| `src/server/replica.cc:732` | `InitiatePSync` clears `pending_stream_bytes_` on entry. |
| `src/server/replica.cc:838-848` | `bad_tail` (logs, returns `errc::bad_message`) and `keep_stream_bytes` (appends to `pending_stream_bytes_`). |
| `src/server/replica.cc:850-878` | `$EOF:` framing: a failed or short read of the closing token, and a token that differs from the header's, are errors; whatever follows it (`chained.UnusedPrefix()`) is kept. `$<len>` framing: `loader.bytes_read() - loader.Leftover().size()` must equal the declared size, else an error. Then `ps.UnusedPrefix()` is kept. The six `CHECK`s are gone. |
| `src/server/replica.cc:1156-1161` | `ConsumeRedisStream` writes `pending_stream_bytes_` into its `io_buf` before reading the socket, so those bytes are applied and counted into `repl_offs_` like any other. |
| `src/server/replica.cc:1961-1962` | `ParseReplicationHeader`: an `$EOF:` token that is not `kRdbEofMarkSize` bytes is a bad header, not a `CHECK_EQ`. |
| `src/server/replica.h:297` | `std::string pending_stream_bytes_`. |
| `tests/dragonfly/fake_classic_master.py` (new, 193 lines) | `FakeClassicMaster`: answers PING and the `REPLCONF` handshake, answers `PSYNC` with one scripted `write()` and optionally a second one; records requests and connection counts. Builders `full_resync_header`, `diskless_full_sync`, `disk_full_sync`, `resp_command`, `EMPTY_RDB`, `MINIMAL_RDB`. |
| `tests/dragonfly/keydb_onboarding_test.py` | The fake-master tests below. |
| `tests/dragonfly/multimaster_test.py:544` | The live-KeyDB twin. |
| `src/server/rdb_test.cc:5944`, `:5981` | The two loader unit tests. |

## Tests

| Test | What it pins |
| --- | --- |
| `RdbTest.LoaderFirstReadHonorsTheSourceLimit` | With a limit equal to the RDB's size and a command behind it, `bytes_read == rdb.size()`, `Leftover() == 0`, and the command is still unread in the source. |
| `RdbTest.LoaderSourceLimitShorterThanTheRdbIsAnError` | A limit that cuts the RDB short is an error return, not a `DCHECK`. |
| `keydb_onboarding_test.py::test_fake_classic_master_valid_full_sync_reaches_stable` | The control: the fake master's default reply is a valid empty diskless sync and the replica takes it in one connection. |
| `::test_psync_bad_eof_token_size_does_not_abort_replica` (`short`, `empty`, `long`) | `$EOF:` with 5, 0 or 41 bytes: process alive, `INFO` answers, leaves LOADING, reconnects 3 times, logs it. |
| `::test_psync_full_sync_tail_mismatch_does_not_abort_replica` (5 scenarios: `eof_token_cut_short`, `eof_token_missing`, `eof_token_mismatch`, `disk_rdb_shorter_than_declared`, `disk_rdb_longer_than_declared`) | Same, for a tail that disagrees with its header. |
| `::test_psync_stream_bytes_behind_full_sync_are_applied` (`disk`, `diskless`, `disk_command_cut_in_two`) | `SET a 1` and `SET b 2` behind a correct sync, in the same write or the next: both applied, link stays up, the ACKed offset equals `SYNC_OFFSET + len(commands)`. |
| `multimaster_test.py::test_plain_replica_of_keydb_applies_stream_flushed_behind_full_sync` (`disk_based_sync`, `diskless_sync`; real KeyDB) | An `INCR` loop on KeyDB while a replica attaches, 6 times; the counters must match exactly, and the replica must never log `Bad full sync tail`. A smoke test, not deterministic (see the round below). |

## Falsification (verbatim)

**1. The unfixed build, against the new tests** (baseline binary; `t06-baseline-binary.txt`,
`t06-final-vs-baseline.txt`; command: `DRAGONFLY_PATH=…/baseline-bin/dragonfly pytest
tests/dragonfly/keydb_onboarding_test.py -k <test>`). Every one aborts the replica:

```
test_psync_stream_bytes_behind_full_sync_are_applied[disk]                -> F1003 20:41:40.928246   32689 replica.cc:831] Check failed: 0u == loader.Leftover().size() (0 vs. 46)
test_psync_stream_bytes_behind_full_sync_are_applied[diskless]            -> F1003 20:41:47.971668   32718 replica.cc:829] Check failed: chained.UnusedPrefix().empty()
test_psync_stream_bytes_behind_full_sync_are_applied[disk_command_cut_in_two] -> F1003 20:41:53.542428   32747 replica.cc:831] Check failed: 0u == loader.Leftover().size() (0 vs. 46)
test_psync_bad_eof_token_size_does_not_abort_replica[short]               -> F1003 21:05:17.751704    7770 replica.cc:1910] Check failed: kRdbEofMarkSize == token.size() (40 vs. 5) short
test_psync_full_sync_tail_mismatch_does_not_abort_replica[eof_token_cut_short] -> F1003 21:05:19.354658    7794 replica.cc:823] Check failed: eof_res && *eof_res == kRdbEofMarkSize
test_psync_full_sync_tail_mismatch_does_not_abort_replica[eof_token_missing]   -> F1003 21:05:20.938598    7818 replica.cc:823] Check failed: eof_res && *eof_res == kRdbEofMarkSize
test_psync_full_sync_tail_mismatch_does_not_abort_replica[eof_token_mismatch]  -> F1003 21:05:22.469126    7853 replica.cc:828] Check failed: 0 == memcmp(token->data(), buf, kRdbEofMarkSize) (0 vs. -54)
test_psync_full_sync_tail_mismatch_does_not_abort_replica[disk_rdb_shorter_than_declared] -> F1003 21:05:24.018394    7877 replica.cc:832] Check failed: snapshot_size == loader.bytes_read() (120 vs. 98)
test_psync_full_sync_tail_mismatch_does_not_abort_replica[disk_rdb_longer_than_declared]  -> F1003 21:05:25.560402    7901 io.cc:143] Check failed: dest.size() >= min_size (0 vs. 8)
```

Each pytest then reports `1 failed, 1 error` (the node is dead: `AssertionError: the replica process
died with -6`). The reviewer re-ran the deterministic tests against the baseline binary
(`review-baseline-falsify.txt`): all 11 `FAILED`, and `E  AssertionError: the replica process died
with -6`.

**2. A real KeyDB master on the unfixed build** (`t06-keydb-baseline.txt`; baseline binary,
`multimaster_test.py::test_plain_replica_of_keydb_applies_stream_flushed_behind_full_sync`, run
three times):

```
== baseline-bin run 1
======================= 2 passed, 71 deselected in 2.21s =======================
== baseline-bin run 2
FAILED 😰  tests/dragonfly/multimaster_test.py::test_plain_replica_of_keydb_applies_stream_flushed_behind_full_sync[df_factory0-disk_based_sync]
F1003 20:45:38.787549    1255 replica.cc:831] Check failed: 0u == loader.Leftover().size() (0 vs. 16149)
== baseline-bin run 3
FAILED 😰  tests/dragonfly/multimaster_test.py::test_plain_replica_of_keydb_applies_stream_flushed_behind_full_sync[df_factory0-disk_based_sync]
F1003 20:45:46.262045    1311 replica.cc:831] Check failed: 0u == loader.Leftover().size() (0 vs. 16149)
```

The disk-based variant aborted 2 of 3 runs; the diskless one never (it is the control: KeyDB streams
only after the replica's first ACK there). The reviewer reports 4 of 8 disk-based runs aborting
and 0 of 8 diskless; the logs kept for three of those runs (`review-logs-baseline3/`) show 2 aborts,
`replica.cc:831] Check failed: 0u == loader.Leftover().size() (0 vs. 2507)` and `(0 vs. 6071)`, and
one pass. A single run of both variants passed (`review-baseline-keydb.txt`: `2 passed`), so this
test cannot be a gate on its own.

**3. Each `CHECK` restored, one at a time, on the fixed tree** (`t06-falsify-checks.txt`, during
development; the scenario names are the earlier test's: `bytes_after_eof_token`,
`disk_size_larger_than_rdb` and `disk_extra_bytes_inside_size` became legitimate stream bytes or
`disk_rdb_*_than_declared` in the final version):

```
== …bad_eof_token_size…[short]  F1003 19:…  replica.cc:1925] Check failed: kRdbEofMarkSize == token.size() (40 vs. 5) short
== …bad_eof_token_size…[empty]  …  (40 vs. 0)
== …bad_eof_token_size…[long]   …  (40 vs. 41) xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
== …tail_mismatch…[eof_token_cut_short]       replica.cc:838] Check failed: eof_res && *eof_res == kRdbEofMarkSize
== …tail_mismatch…[eof_token_missing]         replica.cc:838] Check failed: eof_res && *eof_res == kRdbEofMarkSize
== …tail_mismatch…[eof_token_mismatch]        replica.cc:843] Check failed: 0 == memcmp(token->data(), buf, kRdbEofMarkSize) (0 vs. -54)
== …tail_mismatch…[bytes_after_eof_token]     replica.cc:844] Check failed: chained.UnusedPrefix().empty()
== …tail_mismatch…[disk_size_larger_than_rdb] replica.cc:847] Check failed: snapshot_size == loader.bytes_read() (120 vs. 98)
== …tail_mismatch…[disk_extra_bytes_inside_size] replica.cc:846] Check failed: 0u == loader.Leftover().size() (0 vs. 22)
```

**4. The first read un-clamped** (`rdb_load.cc`'s clamp reverted; `t06-rdb-reverted.txt`, command:
`./rdb_test --gtest_filter=RdbTest.Loader*`):

```
[ RUN      ] RdbTest.LoaderFirstReadHonorsTheSourceLimit
/home/user/drakeydb/src/server/rdb_test.cc:5966: Failure
    Which is: 57
    Which is: 43
/home/user/drakeydb/src/server/rdb_test.cc:5967: Failure
    Which is: 14
    Which is: 0
[ RUN      ] RdbTest.LoaderSourceLimitShorterThanTheRdbIsAnError
/home/user/drakeydb/src/server/rdb_test.cc:5995: Failure   (x4)
[  FAILED  ] RdbTest.LoaderSourceLimitShorterThanTheRdbIsAnError (26 ms)
```

(`bytes_read` 57 instead of 43, `Leftover()` 14 instead of 0.)

**5. The hand-off dropped** (`keep_stream_bytes` not called; `t06-drop-variant.txt`):

```
== drop-variant: disk
[2026-10-03 20:43:55.799 ERROR] Test failed dragonfly/keydb_onboarding_test.py::test_psync_stream_bytes_behind_full_sync_are_applied[df_factory0-disk] with logs:
== drop-variant: diskless
E       AssertionError: assert equals failed
== drop-variant: disk_command_cut_in_two
E       AssertionError: assert equals failed
```

(`a` is `None` on the replica: the implementer's reading of the `assert equals failed`.)

**6. At `a0ee234`, passing**: reviewer's run (`review-head-run.txt`): `12 passed, 5 deselected in
19.36s`; implementer's `gtests-final.txt`: `rdb_test` `[  PASSED  ] 138 tests.`.

The plan's last falsification, counting the hand-off twice, was not run in Task 0.6; it is the
review's I2 finding and is done below.

## Deviations from the plan

- The tail test names changed from the plan's (`*_token_mismatch*`, `*_length_*`) to
  `test_psync_full_sync_tail_mismatch_does_not_abort_replica[<scenario>]`.
- The plan wanted the offset asserted through `slave_repl_offset`; the fake master asserts it through
  the `REPLCONF ACK` offsets it receives, which is what a real master sees.
- U-11 was to be registered by the orchestrator, not by this commit; it was added in the review
  round (below).
- Not changed, noted in U-11: a `$0` header takes `InitiatePSync`'s partial-resync branch (stale data
  kept, master offset adopted); pre-existing, for P7-3.

## Review fix round (committed in `2298570`, P7-0 whole-branch review round 1)

**I2, offset exactness.** `FakeClassicMaster.wait_for_ack(offset)` passed as soon as the expected
offset appeared anywhere in the ACK list. It is replaced by `wait_for_settled_ack(since, repeats=3)`
(`fake_classic_master.py`): wait until the last 3 ACKs received after `since` are equal. In
`test_psync_stream_bytes_behind_full_sync_are_applied` (now run with `replication_acks_interval=100`)
the test takes `since = len(ack_offsets)` after `applied()`, then asserts the settled value, `max`
and the last ACK all equal `SYNC_OFFSET + stream_len`. A first version took the 3 equal ACKs from
before the commands had been applied and read the stale value; `since` fixes that.

Falsification: `repl_offs_ += pending_stream_bytes_.size();` added after
`keep_stream_bytes(ps.UnusedPrefix());` in `InitiatePSync`, built from a scratch copy of
`replica.cc` (`scratchpad/fixround/isobuild.py`: the build's own compile command with the scratch
source, linked ahead of `libdragonfly_lib.a`) so the shared tree, which another agent is editing,
was never touched. The old test (a pristine `git archive HEAD` tests tree) and the new test against
that binary:

```
old test, double-counting binary:
FAILED  …[df_factory0-disk]                  E  AssertionError: [0, 1046, 1100, 1100, 1100, 1100, ...]
PASSED  …[df_factory0-diskless]
FAILED  …[df_factory0-disk_command_cut_in_two]
============ 2 failed, 1 passed, 14 deselected in 62.35s (0:01:02) =============

new test, double-counting binary:
E  AssertionError: [0, 1046, 1100, 1100, 1100]            assert 1100 == 1054      [disk]
E  AssertionError: [0, 1054, 1108, 1108, 1108]            assert 1108 == 1054      [diskless]
E  AssertionError: [0, 1038, 1038, 1038, 1038, 1038, ...] assert 1092 == 1054      [disk_command_cut_in_two]
PASSED  …[df_factory0-diskless_token_split]
==================== 3 failed, 1 passed, 15 deselected in 5.02s ====================
```

So the old test caught the mutation by timing out in 2 of 3 scenarios, and passed the third: there
the double count alone made the first ACK equal the expected value. The new test fails all three
scenarios that have bytes to hand over; `diskless_token_split` (M7) has none, so this mutation
cannot show there (it has its own mutation below). On the real binary: `4 passed, 15 deselected in
5.15s`.

**M7, a split EOF token.** New scenario `diskless_token_split` in
`STREAM_BEHIND_FULL_SYNC`: the first write ends 10 bytes into the closing token, the second (after
`SECOND_WRITE_DELAY_S = 0.5`, a new `stream_delay` argument of `script_psync`) holds the other 30
bytes of the token and both commands. The loader's leftover is a partial token and the rest is on the
socket. The delay also makes `disk_command_cut_in_two` a real split instead of a likely coalesced
write. Mutation: `chained.Read(...)` replaced by a single
`static_cast<io::Source&>(chained).ReadSome(...)`, which returns only the leftover prefix:

```
FAILED  …[df_factory0-diskless_token_split]    E  AssertionError: assert equals failed
================= 1 failed, 3 passed, 15 deselected in 14.23s ==================
```

**M1, `bad_header` captured the first header line by value** (`replica.cc` `ParseReplicationHeader`).
`str` moves on to the second line, so a copy of the first views bytes the `IoBuf` has since
compacted, or reallocated, over. Fixed by capturing by reference. The `$EOF:` size branch now also
logs the size it got (`Bad replication header: the $EOF: token is 5 bytes long, expected 40`): the
earlier test pattern, `Bad replication header: \$EOF:`, matched the corrupted log too. New test
`test_psync_bad_header_line_is_logged_as_received` (a second header line of 65 bytes, longer than
the first's 54). Against the build before the fix (`scratchpad/fixround/dragonfly-head`) all four
tests (3 EOF sizes + the new one) fail, against the fixed build they pass:

```
E1003 22:30:07.683152   20787 replica.cc:1918] Bad replication header: +FULLRESYNC 0123456789abcdef0123456789abcdef01234567 0
=> badline-head rc=1 ======================= 1 failed, 18 deselected in 1.75s =======================
=> badline-new  rc=0 ======================= 1 passed, 18 deselected in 1.73s =======================
m1-newtests-on-headbin rc=1 ======================= 4 failed, 15 deselected in 6.78s =======================
m1-newtests-on-newbin  rc=0 ======================= 4 passed, 15 deselected in 7.20s =======================
```

(The log line shows the defect: it names the first line, which was valid, instead of the bad second
one. At `a0ee234` the `$EOF:` case logged `Bad replication header: $EOF:short\r` followed by the
tail of the first header line, `0123… 0`, on the next log line.)

**`source_limit_ < 9`, untested at `a0ee234`.** `RdbTest.LoaderSourceLimitShorterThanTheRdbIsAnError`
now also tries limits of 8 and 0. Without the `if (source_limit_ < 9)` return (a copy of the file
was not needed: the guard was removed, `ninja rdb_test`, run, restored):

```
[ RUN      ] RdbTest.LoaderSourceLimitShorterThanTheRdbIsAnError
F1003 22:33:47.950525   21546 io.cc:143] Check failed: dest.size() >= min_size (8 vs. 9)
*** SIGABRT received at time=1791066828 on cpu 0 ***
exit=134
```

With it: `[       OK ] RdbTest.LoaderSourceLimitShorterThanTheRdbIsAnError (26 ms)`, `[  PASSED  ] 2 tests.`

**M2.** `multimaster_test.py`'s live-KeyDB test docstring now says it is a non-deterministic smoke
(disk-based aborted 4 of 8 runs on the unfixed build, diskless 0 of 8 = the control) and points at
the fake-master tests for the deterministic coverage. **I4.** ISSUE-REGISTER U-11 and U-12 added; U-10
gained the cost of "allow".
