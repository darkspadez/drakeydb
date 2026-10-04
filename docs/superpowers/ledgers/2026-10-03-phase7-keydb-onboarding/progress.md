# Phase 7 progress ledger

Spec: `docs/superpowers/specs/2026-10-03-phase7-keydb-onboarding-design.md` ·
Plan: `docs/superpowers/plans/2026-10-03-phase7-keydb-onboarding.md` ·
Decisions: `decisions.md` · Design source: `advisor-design.md`

Process per task: brief → implementer (Sonnet) implements + falsifies → reviewer (Opus) spec +
quality review → fix loop → commit. Per sub-PR: whole-branch review → adversarial pass → gate → PR.

## Environment (session container)

- Ubuntu 24.04 x86_64, 4 cores, 15 GB RAM; gcc 13.3, clang 18, cmake 3.28, ninja 1.11.
- Debug build: `./helio/blaze.sh -DWITH_AWS=OFF -DWITH_GCP=OFF`, `ninja -C build-dbg -j4`
  (search ON). Third-party GitHub `/archive/` tarballs come from a local mirror via a
  never-committed shim (see `decisions.md`, "Local sandbox note").
- pytest venv: **`/root/drakey-venv-pinned`** (redis-py 7.4.1, pytest 9.1.1): what
  `tests/dragonfly/requirements.txt` resolves to under its `redis>=5.2.1,<8.0.0` pin, as CI gets it.
  `/root/drakey-venv` (redis-py 8.1.0, from the earlier unpinned `redis>=5.2.1`) is **superseded**:
  redis-py 8 retries a slow command up to 10 times, which broke tests that are fine under the pin
  (see the baseline triage below). Do not use it for a gate. `task-0.4-report.md` records its runs
  as made in `/root/drakey-venv`, before the pin.
- **One pytest session at a time:** `tests/dragonfly/conftest.py:155` `rmtree`s
  `/tmp/dragonfly_logs` at session start, so concurrent sessions in one container destroy each
  other's logs (`INTERNALERROR … FileNotFoundError` in `copy_failed_logs`). Every pytest run is
  wrapped in `flock /tmp/drakey-pytest.lock`.
- KeyDB v6.3.4 built from source: `make -j2 BUILD_TLS=no USE_SYSTEMD=no MALLOC=libc` — built
  cleanly on gcc 13.3 with no workaround (`KeyDB server v=6.3.4 sha=7e7e5e57:0 malloc=libc`),
  closing the advisor's [unverified] gcc-13 build risk.

## P7-0 `feat/phase7-0-closeout-and-harness`

| Task | Status | Notes |
|---|---|---|
| 0.1 P4 close-out docs + U-8 live test | done: `a162d75`, `a549973` | U-8 withdrawn (live: 16/16 STORE destinations replicate); its reviewer pass is the whole-branch review |
| 0.2 Baseline full gate on main | done: `e5cb771` (ctest), `5098963`, `44a0cc0`, `b11594d` (pytest, the pin), counts in `docs/PLAN.md` | ctest 86/88 (IPv6 env, reaper load flake fixed in 0.9); every non-IPv6 pytest failure was the redis-py 8 client, re-run green under the pin; see "Baseline gate" below |
| 0.3 Spec + plan docs | done: `8e362ef`, `87d5312`, `f94b38d` | decisions folded in after the spec/plan review and the advisor's resolutions |
| 0.4 Greet accepts `+OK <suffix>` | done: `a0ee234`; review fixes `2298570` | `task-0.4-report.md` |
| 0.5 U-9 EvalInternal null `conn()` (+ U-10; U-12 in the review round) | done: `a0ee234` (U-9, U-10), `2298570` (U-12) | `task-0.5-report.md` |
| 0.6 Graceful PSYNC CHECKs and the full-sync tail | done: `a0ee234`; review fixes `2298570` | `task-0.6-report.md`; ISSUE-REGISTER U-11 |
| 0.7 KeyDB harness + smoke test | done: `5199f34`, `89414e5`; review fixes `2298570` | `task-0.7-report.md` |
| 0.8 `drakeydb-ci.yml` | done: `5199f34`, `89414e5` (not yet observed on GitHub) | `task-0.7-report.md` |
| 0.9 Reaper-resume test robust under load | done: `a0ee234`; report `2298570` | `task-0.9-report.md`: the plan's 200/200-under-load run and falsification (b) were not run; sibling tests stay load-sensitive (follow-up queued) |
| 0.10 `test_narrowed_window_admits_peer_during_winners_full_sync` "hang" (ledger-only, not a plan task) | done, no code change: not a server bug | redis-py 8 re-sent a 30 s `DEBUG POPULATE` up to 10 times; resolved by the pin (`44a0cc0`), recorded in `b11594d`; see the triage below |
| Whole-branch review fix round (I-1, I-2, M-1 ... M-8) | done: `bcd101d` | the RREPLAY-drop log lines and `test_active_keydb_stream_drop_is_logged` (I-1; the test is replaced by the next row's refusal), docs status, the xfail `raises=`, stable references in the register, the `keydb` marker, `// drakeydb:` markers and UPSTREAM-SYNC watchlist rows, U-11 retry note, the malformed-input carve-out |
| Adversarial pass fixes: C1 (decision 23) and M2 (U-13) | applied, awaiting commit | **C1:** `Greet()` refuses a master that advertises `active-replica` until P7-1 (`replica.cc`, `// drakeydb: P7-0 interim`; `std::errc::protocol_not_supported`, quiet retry WARNING); `test_active_keydb_stream_drop_is_logged` replaced by `test_active_keydb_link_refused_until_p7_1` (plain/peer x `REPLICAOF`/`--replicaof`), the two handshake tests and the live-write test are strict-xfail on the refusal, the proxy capa test split into accept-suffix and refuse-active; P7-1 Task 1.2 Step 4b removes all of it. **M2 = U-13:** an empty command name in a classic stream aborted the replica (`ConsumeRedisStream`'s `GetBuf()[0]`); guarded, `test_classic_stream_empty_command_name_does_not_abort`. Spec D-2/D-15/byte-identity list, plan Tasks 1.2/1.4, UPSTREAM-SYNC row updated |
| Opus review minors of `0b411a1` (M1-M5) | applied, awaiting commit | **M1:** a refused `REPLICAOF` replies with the reason (`Replica::Start()` returns `GenericError{protocol_not_supported, "master advertises active-replica; unsupported until P7-1"}`; before this every failed `Greet()` reached the client as `replication cancelled`), in plain and peer mode and `ADDREPLICAOF`; decision 23 / spec D-2 reworded (the ERROR log stays rate-limited). **M2:** the refusal also clears `master_node_uuid`, `master_clock_ms` and `clock_skew_ms_` and calls `ReleasePeerIdentityClaim()` (the first capa site is before the uuid exchange that cleared them, so a link that was up kept its old uuid in INFO and, in peer mode, its UUID admission); `test_refused_active_master_leaves_no_stale_identity` (fake master, plain/peer; `FakeClassicMaster` gained `script_uuid`, `script_capa_reply`, `drop_connections`). **M3:** the retry-WARNING count matches only the refusal (`REFUSED_GREET_WARNING`), so a `node.stop()` that interrupts a retry cannot add a line. **M4:** `ActiveKeyDBRefused(AssertionError)`, `raises=` of the three strict xfails. **M5:** spec D-2 wording ("before `PSYNC`, and before UUID at the first capa site"), the peer-mode already-attached `OK`, 100-column rewraps (plan, U-13). Falsified: M1 (11 of 14 selected tests fail), M2 clears (both variants), M2 claim release (peer variant); the evidence is in the round's report |
| Opus review of the fixes / gate / PR | pending | |

### Live evidence recorded before any code change (debug build of `c60dfdb`)

- **U-8 re-test** — master + plain replica, `--proactor_threads 4` (4 shards); stable sync confirmed
  with a marker key before writing; 8 × `GEORADIUS src 15 37 200 km STORE d<i>` and 8 ×
  `GEORADIUSBYMEMBER src Palermo 300 km STOREDIST e<i>`; second marker awaited on the replica; all 16
  destinations present on the replica with identical `ZRANGE … WITHSCORES` digests → **0/16
  mismatches**. Mechanism: `geo_family.cc:654` sets `journal_update`, `ZSetFamily::OpAdd`
  hand-journals (`zset_family.cc:1963`, `:2053`). U-8 withdrawn in `ISSUE-REGISTER.md`.
- **Active-KeyDB handshake Critical** — KeyDB v6.3.4 `--active-replica yes` on :17101; a plain
  drakeydb and an `--active_replica` drakeydb each run `REPLICAOF localhost 17101`. Both reply
  `ERR replication cancelled`; both logs show
  `replica.cc:432] Bad response to "REPLCONF capa eof capa psync2": "+OK active-replica\r\n"`;
  neither receives `before`/`after` keys. Task 0.4's falsification target.

### Baseline gate (Task 0.2) — unmodified `c60dfdb`, debug, 4 cores

- **`ctest -L DFLY -j3`** (full `ninja` first, 88 tests): **86/88 passed** in 220 s. Failures:
  - `ServerFamilyTest.GetTcpSocketInfoIPv6` — environment: the container has no IPv6
    (`socket(AF_INET6)` → `EAFNOSUPPORT`, errno 97). Upstream test; not attributable to code.
  - `ReaperJournalFamilyTest.MemberExpiryReaperDoesNotBlockOnConcurrentBgsave` — failed its final
    "reaper did not resume once the snapshot consumer unregistered" assertion under `-j3` load;
    **20/20 passed isolated**. Load-sensitive fork test from P4-0 → Task 0.9 (root-cause + fix).
- **pytest run 1 — discarded**: concurrent pytest sessions from implementer agents wiped
  `/tmp/dragonfly_logs` mid-run (INTERNALERROR in 4 suites) and competing builds loaded the CPU.
- **pytest run 2** (serialized under the lock; `main` snapshot binary; tests tree = `origin/main` +
  the harness-only files from `5199f34` so `redis-server` 7.0.15 is usable; redis-py 8.1.0):

  | Suite | Result |
  |---|---|
  | `multimaster_test.py` | 58 passed, 1 failed (+1 teardown error) |
  | `multimaster_merge_test.py` | 4 passed |
  | `redis_replication_test.py` | 12 passed, 7 deselected |
  | `replication_test.py` | 38 passed, 5 failed, 21 deselected |
  | `replication_specific_test.py` | see triage (6 failed) |
  | `replication_resilience_test.py` | 39 passed, 2 failed, 1 xfailed, 8 deselected |
  | `replication_config_test.py` | see triage (4 failed) |
  | `cluster_test.py::test_cluster_migrations_sequence` | 1 passed |

- **Triage — every run-2 failure except IPv6 is the redis-py 8 client, not code.**
  - redis-py 8.0 (released 2026-05-28) defaults asyncio clients to a 5 s `socket_timeout` with 10
    retries on timeout, and changed several reply shapes. `tests/dragonfly/requirements.txt` was
    `redis>=5.2.1`, so a fresh install got 8.1.0. Upstream already pins `redis>=5.2.1,<8.0.0`; the
    fork adopted the identical line (`44a0cc0`), which resolves to redis-py 7.4.1.
  - **Re-run alone on the `main` snapshot under the pin (redis-py 7.4.1): 14/14 pass** —
    `test_replicate_search_index_to_old_replica`, `test_search`, `test_search_with_stream`,
    `test_xreadgroup_replication`, `test_rewrites`, `test_save_with_replication`,
    `test_empty_hash_map_replicate_old_master`, `test_empty_hashmap_loading_bug`,
    `test_narrowed_window_admits_peer_during_winners_full_sync`, `test_replicaof_reject_on_load`,
    `test_replication_timeout_on_full_sync`, `test_big_containers` (2), `test_master_too_big`, and
    the whole `test_replication_all` (24 passed). The "resource-bound timeouts" seen first were
    the same client timeout.
  - **Task 0.10** (`test_narrowed_window_admits_peer_during_winners_full_sync` "hang"): not a server
    bug. Its `DEBUG POPULATE 700000` takes ~30 s on this debug build; redis-py 8 re-sent it up to 10
    times, the server ran every copy (~338 s of work), and teardown exceeded 120 s. Passes 4/4 on
    redis-py 5.3.1, 1/1 on 7.4.1, 3/3 with `proactor_threads=4`. Evidence: backtraces show only
    `populate_range` fibers; `cmdstat_debug:calls=11` for one client call.
  - **Environment only:** `test_ipv6_replication` (no IPv6 in the container).
  - **Gate venv:** `/root/drakey-venv-pinned` (redis-py 7.4.1, the pin's resolution, as CI gets).

### P7-0 gate — debug build of `0b411a1` (full `ninja`, no warnings)

- **`ctest -L DFLY -j2`: 88/89 passed** in 246 s (`classic_replay_test` is the new 89th). The one
  failure is `ServerFamilyTest.GetTcpSocketInfoIPv6` — environment (no IPv6), same as baseline.
- **pytest** (`/root/drakey-venv-pinned`, `KEYDB_REQUIRED=1`, snapshot of the `0b411a1` binary):

  | Suite | Result |
  |---|---|
  | `keydb_onboarding_test.py` | 23 passed, 4 xfailed (strict, P7-1 removes them) |
  | `keydb_harness_test.py` | 21 passed |
  | `multimaster_test.py` | 76 passed, 1 failed (see below) |
  | `multimaster_merge_test.py` | 4 passed |
  | `redis_replication_test.py` | 12 passed, 7 deselected |
  | `replication_test.py` | 43 passed, 21 deselected |

- **`test_simultaneous_reciprocal_replicaof_converges` is a pre-existing flake on `main`, not a
  P7-0 regression.** It requires the reciprocal-connect tiebreak race to *fire* in at least one of
  5 attempts (log evidence), and the handshake-only race window rarely opens on this box. A first
  sequential comparison looked like a regression (P7-0 binary 2/5 pass while a compile ran, `main`
  5/5 afterwards), so it was re-run interleaved under identical load: **P7-0 binary 5/8 pass,
  `main` binary 2/8 pass.** Follow-up (out of P7 scope): make the positive tiebreak coverage
  deterministic — e.g. a flag-gated hold inside `Replica::Greet` before `REPLCONF capa dragonfly`
  so both sides overlap, or split convergence (always) from a forced-overlap direction test — and
  keep it falsifiable by swapping `ShouldRefuseReciprocalPeer`'s operands.
- After the gate, the Opus review of `0b411a1` (approve; minors M1–M5) was applied in the next
  commit; its touched suites were re-run (see that row in the table above) and the gate's
  `ctest` + the replication-path suites are re-run on the PR head before the PR is opened.

### P7-0 PR head `dbc7e6c` — re-gate and PR CI

- **Re-gate** (full `ninja`, no warnings): `ctest -L DFLY` 88/89 (IPv6 only); pytest
  `keydb_onboarding` 25 passed + 4 strict xfail, `keydb_harness` 21, `multimaster` 77,
  `multimaster_merge` 4, `redis_replication` 12, `replication` 43 — all green.
- **PR CI** ([darkspadez/drakeydb#10](https://github.com/darkspadez/drakeydb/pull/10), run
  37168885667): pre-commit, `keydb-interop`, fakeredis and five of six builds (ASAN/UBSAN
  included) green. `build (ubuntu-dev:24, Debug, g++)` failed one test of 806:
  `replication_resilience_test.py::test_replicaof_reject_on_load` (`DID NOT RAISE
  BusyLoadingError`). Not this PR's: the test documents its own `INFO`→`REPLICAOF` race
  (`:705-709`) — a short startup load can finish between the two calls — and P7-0's loader edits
  are inert for startup loads (`source_limit_ == SIZE_MAX`). Local interleaved 5x each, PR binary
  vs `main` binary: 10/10 passed. The session's GitHub integration cannot re-run jobs (403); the
  owner re-ran it once from the Actions tab: **passed** (attempt 2, 06:22Z), confirming the race.
  `large-tests-arm` and `fuzz-pr` sit queued with no runner, as on darkspadez/drakeydb#9.

### Reaper and tombstone-GC gtest fixes from another session (owner, 2026-10-04)

The owner fixed load-sensitive `multi_master_test` cases in another session (branch
`claude/relaxed-franklin-b0qhqn`, cut from P7-0 `bcd101d`) and asked for them on this work. They
were cherry-picked onto P7-1 (the owner's choice, so PR #10 stays as reviewed):

- `dfbe84d`: direct reaper assertions loop with a fresh `reset_time_quota`, at most 100 calls;
- `c6ebff7`: the two GC-budget tests stop the background tombstone GC first;
- `32a63f8`: `LocalOnlyReaperDoesNotJournalNamespaceBlindDelete` pauses the heartbeat reaper.

This closes the "reaper sibling tests are load-sensitive" follow-up. Verified here (the first
commit had not been built where it was written): clean build, no warnings; the 17 reaper/GC tests
pass, and pass 40 repeats with three CPU spinners; full `multi_master_test` 222 passed, 1 skipped
(root-only skip).

### PR #10 safety-net check-ins stopped (2026-10-04 20:16Z)

Check-ins #3–#5 (11:55Z, 16:15Z, 20:16Z) found nothing new: every runnable check green,
`large-tests-arm`/`fuzz-pr` queued for lack of fork runners, no review threads, waiting only on the
owner's review and merge. After three quiet check-ins they stopped; the next GitHub event or owner
message resumes the watch.

## P7-1 … P7-4

Pending (see plan).
