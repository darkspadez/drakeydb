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
- pytest venv: `/root/drakey-venv` (`tests/dragonfly/requirements.txt`).
- **One pytest session at a time:** `tests/dragonfly/conftest.py:155` `rmtree`s
  `/tmp/dragonfly_logs` at session start, so concurrent sessions in one container destroy each
  other's logs (`INTERNALERROR … FileNotFoundError` in `copy_failed_logs`). Every pytest run is
  wrapped in `flock /tmp/drakey-pytest.lock`.
- Python test deps resolve to `redis` 8.1.0 / `pytest` 9.1.1 (requirements are `>=`-only).
- KeyDB v6.3.4 built from source: `make -j2 BUILD_TLS=no USE_SYSTEMD=no MALLOC=libc` — built
  cleanly on gcc 13.3 with no workaround (`KeyDB server v=6.3.4 sha=7e7e5e57:0 malloc=libc`),
  closing the advisor's [unverified] gcc-13 build risk.

## P7-0 `feat/phase7-0-closeout-and-harness`

| Task | Status | Notes |
|---|---|---|
| 0.1 P4 close-out docs + U-8 live test | docs committed `a162d75`; review pending | U-8 withdrawn (live: 16/16 STORE destinations replicate) |
| 0.2 Baseline full gate on main | ctest done; pytest re-run pending | see "Baseline gate" below |
| 0.3 Spec + plan docs | in progress | |
| 0.4 Greet accepts `+OK <suffix>` | pending | |
| 0.5 U-9 EvalInternal null `conn()` | pending | |
| 0.6 Graceful PSYNC CHECKs | pending | |
| 0.7 KeyDB harness + smoke test | pending | |
| 0.8 `drakeydb-ci.yml` | pending | |
| Whole-branch review / adversarial / gate / PR | pending | |

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
  Raw logs kept out of tree. Re-run serialized on a quiet box (run 2) — results pending.

## P7-1 … P7-4

Pending (see plan).
