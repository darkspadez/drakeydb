# Tasks 0.7 / 0.8: review fix round

Fixes for the Opus review of `5199f34` (KeyDB harness, onboarding smoke tests, `drakeydb-ci.yml`).
Nothing is committed. Every pytest run below was wrapped in `flock /tmp/drakey-pytest.lock`, with
`DRAGONFLY_PATH` the unmodified-main snapshot binary (`baseline-bin/dragonfly`, built from `c60dfdb`)
and `KEYDB_SERVER_PATH` the KeyDB v6.3.4 built here (`KeyDB server v=6.3.4 sha=7e7e5e57:0
malloc=libc`), unless a row says otherwise.

## First real KeyDB sync

`tests/dragonfly/keydb_onboarding_test.py::test_keydb_plain_master_full_sync_and_stream` (plain
drakeydb replica, plain KeyDB master), final version, baseline binary:

```
test_keydb_plain_master_full_sync_and_stream[df_factory0] PASSED 😃  [  3%]
```

KeyDB log of that run (`keydb-server-5555.log`, port 5555; the drakeydb node on an OS-chosen port):

```
Replica 127.0.0.1:40481 asks for synchronization
Full resync requested by replica 127.0.0.1:40481
Starting BGSAVE for SYNC with target: replicas sockets
Background RDB transfer started by pid 31732                 20:38:02.748
Background RDB transfer terminated with success              20:38:06.466
Streamed RDB transfer with replica 127.0.0.1:40481 succeeded (socket). Waiting for REPLCONF ACK ...
Synchronization with replica 127.0.0.1:40481 succeeded
```

drakeydb log of the same run: `Initiate replication with: localhost:5555`, `Starting full sync with
Redis master` (20:38:03.110), `Transitioned into stable sync` (20:38:06.466). The sync window was 3.7s
for 5105 keys in db 0 (100 strings, a hash, list, set, zset, a TTL key, 5000 `DEBUG POPULATE` keys) and
one key in db 1, plus a few hundred live writes made while it ran, then the stream writes after it.

## Fixes

| Item | Where | What |
| --- | --- | --- |
| I1 | `tests/dragonfly/keydb_onboarding_test.py`: `seed_before_attach` (about `:42`), `write_during_full_sync` (`:57`), the `assert_*` helpers (`:110-146`) and the four `test_keydb_*` tests (`:150-270`; the file is shared with Task 0.6, so lines drift by one or two, names do not) | Seeds `SET ttl:key v EX 1000`, a db-1 key (`db=1` client on both ends), and `DEBUG POPULATE 5000 bulk 64` with `rdb-key-save-delay 400` (the child sleeps 400us a key, so the sync lasts at least 2s whatever the machine). Asserts `900 < TTL <= 1000`, the db-1 key through a `db=1` client, and the db 0 `DBSIZE` plus sampled bulk values against KeyDB's own. `write_during_full_sync` waits for `slave0` on KeyDB, then loops `SET live:i` + `INCR live:counter` while KeyDB reports the replica not yet `online`; at least 20 such writes are required, else the test fails (a window that closed early cannot pass vacuously). After the sync, every live key and the counter (= number of writes) are asserted. Plain-master test: all of it. The two active-KeyDB tests: the seeded data (TTL, db 1, bulk keyspace). Live write for active KeyDB: new `test_keydb_active_live_write_during_full_sync` (plain and peer mode), see "Deferred". |
| I2 | `keydb_onboarding_test.py` `assert_keydb_saw_one_full_sync` (about `:139`), called by the plain-master test and the live-write test | KeyDB `INFO stats`: `sync_full == 1`, `sync_partial_ok == 0`, `sync_partial_err == 0` (field names verified in KeyDB `server.cpp:6017-6019`), `connected_slaves == 1`; the plain test then also asserts drakeydb `master_link_status == up`. |
| I3 | `.github/workflows/drakeydb-ci.yml:65-118` | `actions/cache/restore@v5` (`:78`) plus `actions/cache/save@v5` (`:114`) right after Build KeyDB, both gated on `cache-hit != 'true'`, so a red run still saves. One computed key (`:65-71`): `keydb-v6.3.4-BUILD_TLS_no-USE_SYSTEMD_no-MALLOC_libc-Linux-ubuntu-dev-24`, derived from `KEYDB_MAKE_FLAGS` (`:37`), which is also what `make` receives (`:108`), so key and flags cannot drift. |
| Minor 1 | `drakeydb-ci.yml:131` | `ccache-save: 'false'`; comment at `:123-126` says why. |
| Minor 3 | `drakeydb-ci.yml:158` | `-m "not large and not opt_only"`, ci.yml's Debug leg filter; `multimaster_test.py` stays. `--collect-only` selects 87 tests with and without it today, so nothing is dropped. |
| Minor 4 | `drakeydb-ci.yml:7-10` | Comment: the `push: main` run saves the KeyDB build under main's cache scope, which PRs can restore. |
| Minor 9 | `drakeydb-ci.yml:84-100`, `:150-167` | Build dependencies installed only on a miss; on a hit only `libuuid1 libcurl4t64` (below). On `timeout` (exit 124) the last test's log dir moves to `/tmp/failed`, like `.github/actions/regression-tests/action.yml`. |
| Minor 5 | `tests/dragonfly/instance.py:667` `find_binary`, `:676` `required`, `:681` `unavailable`; `conftest.py:635-650`; `tests/pytest.ini:30-32`; `docs/build-from-source.md:135-137` | A set `KEYDB_SERVER_PATH` that is not an executable always fails (never falls back to PATH); `KEYDB_REQUIRED` accepts 1/true/yes in any case. The decision is in `KeyDBServer.unavailable()` so it is unit-testable. |
| Minor 6 | `instance.py:753` `_served_by`, `:762` `_wait_ready` | Readiness is `INFO server` `process_id == proc.pid`; another process answering on the port raises at once ("port N is served by process P, not by the keydb-server we started"). |
| Minor 7 | `instance.py:571` `_major_version`, `:585-591` | The fallback `redis-server` is used for `redis7=True` only if `--version` reports major >= 7, else `FileNotFoundError` as before; `# drakeydb:` marker added. |
| Minor 8 | `instance.py:708-711` | Comment now says KeyDB warns whenever `multi-master-no-forward` is yes (`validateMultiMasterNoForward`, `config.cpp:2705-2709`). |

Runtime libraries on a cache hit: `readelf -d keydb-server` NEEDED = libuuid, libcurl, libstdc++, libm,
libgcc_s, libc. `libuuid1` and `libcurl4t64` are the Ubuntu 24.04 packages (the container is
`ubuntu-dev:24`); every library `ldd` reports (31 packages) is in the `apt-cache depends --recurse`
closure of `libuuid1 libcurl4t64 libstdc++6 libgcc-s1 libc6` (`comm` of the two lists was empty).

New file `tests/dragonfly/keydb_harness_test.py` (21 tests, no KeyDB needed): the `KEYDB_REQUIRED`
values, the bad-path rule, readiness against a stub that answers INFO with a chosen pid, and the
`redis7` fallback against stub `redis-server` scripts.

## Test results (verbatim)

Final tree, baseline binary (`KEYDB_REQUIRED=1`), command:

```
flock /tmp/drakey-pytest.lock python -m pytest tests/dragonfly/keydb_onboarding_test.py \
  tests/dragonfly/keydb_harness_test.py -p no:cacheprovider -v \
  -k "keydb_plain or keydb_active or keydb_harness or redis_server_fallback or keydb_ready or keydb_not_ready or missing_keydb or bad_keydb or good_keydb"
```

```
test_keydb_plain_master_full_sync_and_stream[df_factory0] PASSED 😃
test_keydb_active_handshake_and_full_sync[df_factory0] FAILED 😰       E  redis.exceptions.ResponseError: replication cancelled
test_keydb_active_handshake_peer_mode[df_factory0] FAILED 😰           E  redis.exceptions.ResponseError: replication cancelled
test_keydb_active_live_write_during_full_sync[...-plain_replica] FAILED 😰   (same)
test_keydb_active_live_write_during_full_sync[...-peer_mode] FAILED 😰       (same)
keydb_harness_test.py: 21 PASSED
================= 4 failed, 22 passed, 12 deselected in 6.17s ==================
```

The four failures are the Greet bug on `main` (Task 0.4), as before this round.

Exploratory only, not a gate run: the same onboarding tests against a copy of `build-dbg/drakeydb` made
at 20:08 (it includes the uncommitted Task 0.4 change in the working tree):

```
test_keydb_plain_master_full_sync_and_stream           PASSED
test_keydb_active_handshake_and_full_sync              PASSED
test_keydb_active_handshake_peer_mode                  PASSED
test_keydb_active_live_write_during_full_sync[plain_replica]  FAILED  E AssertionError: 323 of 323 live writes missing
test_keydb_active_live_write_during_full_sync[peer_mode]      FAILED  E AssertionError: 325 of 325 live writes missing
============ 2 failed, 3 passed, 12 deselected in 85.41s ============
```

So with Task 0.4 the handshake and the full sync of an active KeyDB (TTL, db 1, 5000 keys) work in both
modes, and the live writes are dropped, which is what the capture predicts (below).

## I2 falsification

Reviewer's falsification: `CLIENT KILL TYPE replica` on KeyDB between the full sync and the stream writes,
inserted in scratch copies of `tests/` (nothing in the repo), via `scratchpad/fals/i2_fals.py`:

| Test | Result |
| --- | --- |
| Old test (`git show 5199f34:tests/dragonfly/keydb_onboarding_test.py`) + kill | `1 passed, 2 deselected in 1.45s`: the hole the reviewer found, the replica reconnects, a second full sync re-delivers everything and the test cannot tell |
| New test + kill | `FAILED ... E assert 2 == 1` at `assert stats["sync_full"] == 1`: `sync_full` is 2, KeyDB served a second full sync |
| New test, no kill | `1 passed, 15 deselected in 4.61s` |

I1 falsifications (scratch copies, plain-master test): the seeded key without `EX` fails with `assert 900 <
-1`; the db-1 key seeded in db 0 fails with `-None +'in-db-1'`; `RDB_KEY_SAVE_DELAY_US = 0` fails with
`only 2 writes inside the sync window` (`assert 2 >= 20`).

Harness falsifications (scratch copies of `tests/`, one revert each, `scratchpad/fals/harness_fals.py`):

| Revert | Failing tests |
| --- | --- |
| `unavailable()` without the "configured path is fatal" branch | `test_bad_keydb_server_path_fails_even_when_not_required` |
| `required()` as `== "1"` | `test_missing_keydb_fails_when_required[true,TRUE,yes,Yes, 1 ]` (5) |
| `_wait_ready` accepts any answering server | `test_keydb_not_ready_when_another_process_answers` |
| fallback ignores `redis7` | `test_redis_server_fallback_must_satisfy_redis7[6.2.14-True-False]`, `[unparsable-True-False]` |

`KEYDB_REQUIRED` / `KEYDB_SERVER_PATH` through the real fixture (`-k plain_master`, `scratchpad/fals/env_matrix.sh`),
new harness against the one at `HEAD`:

| Case | `HEAD` | now |
| --- | --- | --- |
| path unset, `REQUIRED` unset | skipped | skipped |
| path unset, `REQUIRED=1` | error | error |
| path unset, `REQUIRED=TRUE` | **skipped** | error (`keydb-server not found ...`) |
| path unset, `REQUIRED=0` | skipped | skipped |
| `KEYDB_SERVER_PATH=/nonexistent` | **skipped** | error (`KEYDB_SERVER_PATH='/nonexistent' is not an executable`) |
| `KEYDB_SERVER_PATH=/nonexistent`, `REQUIRED=1` | error | error |

("error" is pytest's report of `pytest.fail` raised in a fixture: not a skip, run exit code 1.)

## CI workflow validation

`yaml.safe_load` parses it. `actionlint` 1.7.12 reports only `could not parse action metadata in
".github/actions/builder": unexpected key "type"`, which it reports identically for `ci.yml:130`; with that
upstream metadata quirk stripped in a scratch copy it exits 0. `shellcheck` is not installed, so the `run:`
scripts were not shell-linted. Run by hand: the key step (`keydb-v6.3.4-BUILD_TLS_no-USE_SYSTEMD_no-MALLOC_libc-Linux-ubuntu-dev-24`),
the timeout step with a stand-in for pytest (exit 124 with a last-log file: logs moved, exit 124; no file:
exit 124; failing tests: exit 1; passing: exit 0), and `apt-get -s install libuuid1 libcurl4t64`. Not run:
the KeyDB build itself (done earlier this session, `progress.md`), and nothing on GitHub, so the real
`cache/restore` + `cache/save` behaviour is unobserved (the versions are the ones the builder action uses).

## Golden RREPLAY captures (I4)

`tests/dragonfly/tools/capture_keydb_rreplay.py` (documented in its docstring; run from the repo root with
`KEYDB_SERVER_PATH` set) starts the KeyDBs, attaches a raw replica that sends `PING`, `REPLCONF
listening-port`, `REPLCONF capa eof capa psync2`, `REPLCONF UUID`, `PSYNC ? -1`, consumes the EOF-framed RDB
and ACKs like a replica, then writes and keeps every stream byte. Fixtures, with `tests/dragonfly/data/README.md`
(per-segment offsets and lengths, the envelope layout, two literals) and a `.gitattributes` marking `*.bin` binary:

- `keydb_v6.3.4_rreplay_stream.bin`, 1953 bytes, sha256 `433c872d...d33a27`, 14 envelopes: a write made during
  the full sync, `SET k v`, `SET k2 v2 EX 100`, `SELECT 3` + `SET k3 v3`, `MULTI`/`SET a 1`/`INCR c`/`EXEC` (4
  envelopes), `DEL k`, `EXPIREMEMBER` and `PEXPIREMEMBERAT` on set members, a member and a key that expire, and the
  cron `PING`.
- `keydb_v6.3.4_rreplay_nested_stream.bin`, 1944 bytes, sha256 `e5a5b600...20fb4e`, 9 envelopes: KeyDB A (active,
  multi-master, `multi-master-no-forward no`) replicating from KeyDB B, A's stream while writing to B; the
  nested depth-2 envelope (A's wrapping B's) for 8 of them, and one plain envelope for a write made on A itself.

What the capture shows that the plan did not say, each also in the README:

1. Everything an active KeyDB streams is wrapped, the cron `PING` (inner `ping`, lower case) and the writes
   queued behind the RDB for a syncing replica included. No bare `SELECT` is ever sent; the db is the 4th arg.
2. KeyDB rewrites before it streams: `EX` to `PXAT <ms>`, `INCR` to `INCRBY c 1`, `EXPIREMEMBER` to
   `PEXPIREMEMBERAT key member <ms>`.
3. `MULTI`/`EXEC` is four envelopes with rising, not consecutive, mvcc values.
4. **An active KeyDB does not stream expiries**: after the key's `PX 200` and the member's 1s expiry (1.5s and 2.5s
   waits) nothing follows, no `DEL`, no `SREM` (`db.cpp:1980` and `:2024`). A replica has to expire from the
   absolute times it was given.
5. The master logs, for a replica that sends no `REPLCONF capa activeExpire` (drakeydb sends none):
   "does not support active expiration. This client may not correctly process key expirations" and
   "Connections between active replicas and traditional replicas is deprecated. This will be refused in future
   versions." Worth a decision for the spec: it is a version-skew risk for the whole onboarding path.
6. `REPLCONF UUID <x>` is answered with `+<the master's uuid>`, the uuid of its envelopes.
7. After an EOF-framed RDB the master streams only after a `REPLCONF ACK` that arrives once its child is reaped;
   an ACK sent straight after the last RDB byte can be too early (the raw replica's first attempt hung on this).

## Deferred, not done, risks

- **Live writes on an active KeyDB need Task 1.2.** They are RREPLAY-wrapped (point 1), so a replica without the
  unwrap drops them (shown above: 323 of 323 missing). The brief asked for the live write in the two active tests;
  putting it there would have kept them red after Task 0.4 for a second reason. They are split into
  `test_keydb_active_live_write_during_full_sync` (plain and peer mode), a normal test that fails today and is the
  Task 1.2 target; the two original active tests carry the rest of I1 and, with Task 0.4's change, pass (exploratory
  run above). If the P7-0 gate must be green without Task 1.2, that test needs an `xfail`/skip, which I did not add.
- `keydb_onboarding_test.py` is also being edited by another agent (Task 0.6, the `FakeClassicMaster` tests). Both
  sets of edits are in the file now (checked at 20:43); a whole-file rewrite from either side would drop the other's.
- `_wait_ready` compares `process_id` with the `Popen` pid, so a `KEYDB_SERVER_PATH` that is a wrapper script forking
  the real server (rather than `exec`ing it) is now refused.
- `rdb-key-save-delay` is a KeyDB debug-style config; it exists in v6.3.4 (`config.cpp:2922`), a later KeyDB may drop it.
- The tests rely on `INFO replication`'s `slave0` field `state` and on `INFO stats` `sync_*`, as printed by v6.3.4.
- `docs/ISSUE-REGISTER.md` is modified in the working tree by someone else; not touched here.
- Left alone on purpose: `src/`, `helio/`, `multimaster_test.py`, the plan, spec and the other ledger files.
- `pre-commit run --files` (pyflakes, trailing whitespace, end-of-file, ast, black) passes on every file changed.
