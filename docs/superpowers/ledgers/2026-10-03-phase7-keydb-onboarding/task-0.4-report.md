# Task 0.4: `Greet()` accepts `+OK <suffix>`

Implemented in `a0ee234`. This report was written afterwards, in the review fix round, from
`git show a0ee234`, the implementer's saved outputs (`scratchpad/p7-0/t04-*.txt`) and the Opus
reviewer's own runs (`scratchpad/review-capa/`). Output below is copied from those files unedited
(`…` marks a cut). The implementer's shell commands were not saved: each "Command" line is the
invocation shape, with the node ids taken from the headers the runner scripts printed.

## What changed (at `a0ee234`)

An active KeyDB answers every `REPLCONF capa ...` with `+OK active-replica` (and possibly ` keydb-fastsync-save`).
`Replica::Greet()` insisted on exactly `+OK` at both capa sites, so drakeydb could not connect to
an active KeyDB at all: `REPLICAOF` failed with `replication cancelled`.

| File | What |
| --- | --- |
| `src/server/classic_replay.h:1-24` (new) | `struct CapaReply { ok, active_replica, keydb_fastsync_save }` and `CapaReply ParseCapaReply(std::string_view)`. |
| `src/server/classic_replay.cc:13-29` (new) | `ParseCapaReply`: `ok` iff the first token is exactly `OK` (case-sensitive) and what follows is empty or starts with a space; later tokens are space-separated, `active-replica` and `keydb-fastsync-save` are recognized, unknown words are ignored. |
| `src/server/replica.cc:401` | `read_capa_reply` lambda in `Greet()`: parses `LastResponseArgs()` (exactly one `STRING` arg) with `ParseCapaReply`. |
| `src/server/replica.cc:447`, `:626` | The two strict `CheckRespIsSimpleReply("OK")` calls (`REPLCONF capa eof capa psync2`, and the Redis branch of `REPLCONF capa dragonfly`) become `PC_RETURN_ON_BAD_RESPONSE(read_capa_reply().ok)`. Every other `CheckRespIsSimpleReply("OK")` in `Greet()` is unchanged. |
| `src/server/replica.cc:395`, `src/server/replica.h:291` | `master_active_replica_` is cleared at the top of every `Greet()` and set when a capa reply carries `active-replica` (a `VLOG(1)` says so). Nothing reads it yet (P7-1). |
| `src/server/CMakeLists.txt:114`, `:201`, `:209` | `classic_replay.cc` in `dragonfly_lib`; `helio_cxx_test(classic_replay_test ...)`; added to `check_dfly`. |

Not changed: the stock-Redis path. A reply of exactly `+OK` parses to `ok = true` as before.

## Tests

| Test | What it pins |
| --- | --- |
| `ClassicReplayTest.ParseCapaReplyAcceptsOkAndSuffixes` (`classic_replay_test.cc:10`) | `OK`, `OK active-replica`, `OK keydb-fastsync-save`, both, extra spaces, an unknown word ignored. |
| `ClassicReplayTest.ParseCapaReplyRejectsNonOk` (`:43`) | `OKAY`, `ok`, `Ok`, `ERR`, empty, ` `, ` OK`, `OKactive-replica`, `OK-active-replica`, `active-replica OK`, `ERR active-replica`. |
| `multimaster_test.py::test_greet_accepts_keydb_active_replica_capa_reply` (`:470`; site x reply = 4 cases) | A `proxy_factory` proxy in front of a real Redis master overrides the reply to one capa command; the replica must sync, link up, receive a seeded key and a streamed one. |
| `multimaster_test.py::test_greet_still_refuses_malformed_capa_reply` (`:523`; 2 sites x 4 bad replies = 8 cases) | `+OKAY`, `+ok active-replica`, `+OKactive-replica`, `+active-replica` still give `replication cancelled` and the node stays a master. |
| `keydb_onboarding_test.py::test_keydb_active_handshake_and_full_sync`, `::test_keydb_active_handshake_peer_mode` (real KeyDB, plain and `--active_replica` replica) | The live twin. |

The two proxy tests live in `multimaster_test.py`, not `keydb_onboarding_test.py`, because the
latter's module marker (`pytestmark = pytest.mark.keydb`) skips them wherever there is no KeyDB
binary, and they need only a stock Redis.

Results at `a0ee234` (implementer, `t04-after.txt`, venv `/root/drakey-venv`):

```
====================== 15 passed, 59 deselected in 20.27s ======================
```

(4 accepts + 8 refuses + the plain-master and the two active-KeyDB smoke tests.) `classic_replay_test`:
`[  PASSED  ] 2 tests.` (`gtests-final.txt`).

## Falsification

**Before the fix** (baseline `drakeydb` built from `c60dfdb`, `t04-before.txt`). Command:
`flock /tmp/drakey-pytest.lock env DRAGONFLY_PATH=… KEYDB_SERVER_PATH=… KEYDB_REQUIRED=1
/root/drakey-venv/bin/python -m pytest tests/dragonfly/keydb_onboarding_test.py -k "handshake or plain_master"`.

```
E           redis.exceptions.ResponseError: replication cancelled
FAILED 😰  tests/dragonfly/keydb_onboarding_test.py::test_keydb_active_handshake_and_full_sync[df_factory0]
FAILED 😰  tests/dragonfly/keydb_onboarding_test.py::test_keydb_active_handshake_peer_mode[df_factory0]
========================= 2 failed, 1 passed in 1.77s ==========================
```

and the replica's own log, from the live probe recorded in `progress.md` before any code change:
`replica.cc:432] Bad response to "REPLCONF capa eof capa psync2": "+OK active-replica\r\n"`.

**Strict checks restored** (`Greet()`'s two `read_capa_reply().ok` put back to
`CheckRespIsSimpleReply("OK")`; the `classic_replay` unit tests are untouched by this mutation;
build log `build04-fals.log` warns `variable 'read_capa_reply' set but not used`, which is how the
mutation shows). `t04-falsify.txt`, one pytest per node id:

```
== tests/dragonfly/keydb_onboarding_test.py::test_keydb_active_handshake_and_full_sync
E           redis.exceptions.ResponseError: replication cancelled
============================== 1 failed in 0.55s ===============================
== tests/dragonfly/keydb_onboarding_test.py::test_keydb_active_handshake_peer_mode
E           redis.exceptions.ResponseError: replication cancelled
============================== 1 failed in 0.45s ===============================
== tests/dragonfly/multimaster_test.py::test_greet_accepts_keydb_active_replica_capa_reply[df_factory0-active-capa_eof]
E           redis.exceptions.ResponseError: replication cancelled
============================== 1 failed in 1.50s ===============================
== …[active-capa_dragonfly] … 1 failed in 1.49s
== …[active_fastsync-capa_eof] … 1 failed in 1.60s
== …[active_fastsync-capa_dragonfly] … 1 failed in 1.58s
```

and the logs name both sites:

```
      6 E1003 19:52:27.943489    6724 replica.cc:447] Bad response to "REPLCONF capa eof capa psync2": "+OK active-replica\r\n"
      6 E1003 19:52:32.807657    6768 replica.cc:626] Bad response to "REPLCONF capa dragonfly": "+OK active-replica\r\n"
```

**Space check removed** (`ParseCapaReply` without the `front() != ' '` guard, `build04-fals2.log`;
`t04-falsify-neg.txt`): the two cases that only the guard refuses are accepted, the two the first
test already refused stay refused (controls):

```
== okay capa_eof: … test_greet_still_refuses_malformed_capa_reply[df_factory0-okay-capa_eof] … E       Failed: DID NOT RAISE ResponseError
== okay capa_dragonfly: … E       Failed: DID NOT RAISE ResponseError
== no_space capa_eof: … E       Failed: DID NOT RAISE ResponseError
== no_space capa_dragonfly: … E       Failed: DID NOT RAISE ResponseError
== lowercase capa_eof: ============================== 1 passed in 1.27s ===============================
== lowercase capa_dragonfly: … 1 passed
== no_ok capa_eof: … 1 passed
== no_ok capa_dragonfly: … 1 passed
```

**Reviewer's independent check** (`scratchpad/review-capa/`: `main.cc` runs the 11 rejected and 3
accepted inputs through `ParseCapaReply` built from `classic_replay.cc`; `fals.cc` is the same file
with the two guard lines removed):

```
good:  0 failures
fals:  WRONGLY ACCEPTED: 'OKAY'
       WRONGLY ACCEPTED: 'OKactive-replica'
       WRONGLY ACCEPTED: 'OK-active-replica'
       3 failures
```

## Deviations from the plan

- Plan Step 3 said not to record `active_replica` yet (Task 1.4). `master_active_replica_` is
  recorded (spec D-2): the parse already yields it, and P7-1 needs the bit from this connection.
  It is read by nothing in this commit.
- The two proxy tests are in `multimaster_test.py` (see above); the plan named
  `keydb_onboarding_test.py`.
- The plan's "Done" asked for `redis_replication_test.py` to stay green; the Phase 7 baseline gate
  ran it before (12 passed) and the final gate reruns it. Not rerun for this report.

## Review fix round (not committed when this was written)

`keydb_onboarding_test.py::test_keydb_active_handshake_peer_mode` failed 20 of 20 runs under CPU
load and 10 of 10 without it. For a peer node `link_status=up` only means the TCP connection is up,
and `sync_in_progress` turns 1 only once the master's `$` header arrives (`replica.cc` `GetSummary`:
`R_SYNCING` is set after the header is parsed). KeyDB sends `+FULLRESYNC` at once and the header when
its transfer starts (the harness runs KeyDB with `repl-diskless-sync yes`, `instance.py`: the header
is `$EOF:<token>` and the RDB streams for at least 2 s with the test's `rdb-key-save-delay`), so
`wait_for_peer_link` returns early and the polled reads that follow find the replica LOADING.
redis-py raises that as `BusyLoadingError`, a `ConnectionError`, which `assert_eventually`
(`utility.py`) does not retry (it retries `AssertionError` only). The review's diagnosis said the
header comes only after the BGSAVE; that is the disk-based case, the mechanism is the same.

Fix, `tests/dragonfly/keydb_onboarding_test.py`: `retry_while_loading` turns `BusyLoadingError`
into `AssertionError`, applied under `@assert_eventually` in `assert_full_sync_arrived`,
`assert_ttl_and_db1_arrived`, `assert_live_writes_arrived` and `assert_keyspaces_match`; the
`wait_for_peer_link` docstring now says what it does and does not wait for.

Evidence, `flock /tmp/drakey-pytest.lock …/i1-loop.sh <root> <out> 20 4 <node id> <binary>`
(20 runs of `test_keydb_active_handshake_peer_mode`, 4 busy `while :; do :; done` loops, the same
binary for both rows: a copy of `build-dbg/dragonfly` as built from the `a0ee234` sources, taken
before this round's C++ edits; the "before" row ran against a pristine `git archive HEAD` copy of the
tests tree):

| | runs | passed | failed | per-run time |
| --- | --- | --- | --- | --- |
| before | 20 | 0 | 20 | 1.4-1.9 s |
| after | 20 | 20 | 0 | 4.8-5.4 s |
| after, final binary of this round (with the `replica.cc` edits in Task 0.6's round) | 20 | 20 | 0 | 4.3-5.3 s |
| before, no load (0 busy loops) | 10 | 0 | 10 | 1.1-1.4 s |
| after, no load, final binary | 10 | 10 | 0 | 4.6-4.8 s |

Every "before" failure is the same:

```
tests/dragonfly/keydb_onboarding_test.py:91: in assert_full_sync_arrived
E               redis.exceptions.BusyLoadingError: Dragonfly is loading the dataset in memory
============================== 1 failed in 1.90s ===============================
```
