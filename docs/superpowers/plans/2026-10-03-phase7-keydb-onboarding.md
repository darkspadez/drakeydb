# Phase 7: KeyDB one-way onboarding (P7-0 … P7-4)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a drakeydb node — plain or `--active_replica` — attach to one or more active KeyDB
v6.3.4 masters: complete the handshake, unwrap and apply the `RREPLAY` stream without loss or
duplication, resolve conflicts by LWW with KeyDB-authored stamps, resume with a partial PSYNC after
a link drop, keep up with KeyDB under sustained load, and fail loudly instead of silently on what
it cannot represent. P4 close-out and the KeyDB test harness come first (P7-0).

**Architecture:** New fork-only `src/server/classic_replay.{h,cc}` holds everything new: capa-reply
parsing, the `RREPLAY` envelope parser and a socket-free `ClassicApplier`, the author map and the
process-wide author dedup, KeyDB-only command classification, the classic-link rewrites, counters
and `--classic_partial_psync`. `Replica` gets small hooks: a capa parse in `Greet()`, an envelope
branch in `ConsumeRedisStream` that flushes the raw batch and dispatches each inner command with its
own mvcc/origin, a PSYNC offset/`+CONTINUE` path with a leftover hand-off, and a per-shard flag that
lets a plain replica of an active KeyDB run active expiry. The RDB loader learns to skip KeyDB's
type 64 and member-TTL aux. A fork-owned CI workflow builds KeyDB and runs the suite.

**Tech Stack:** C++20, Dragonfly replication/RDB loader, GoogleTest, pytest + a real KeyDB v6.3.4
and a scripted fake classic master, CMake/Ninja, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-10-03-phase7-keydb-onboarding-design.md` — D-1…D-15 are the
binding sections; the spec wins over this plan on conflict, record any deviation in the ledger.
Owner decisions: `docs/superpowers/ledgers/2026-10-03-phase7-keydb-onboarding/decisions.md`.
Ledger (reports, progress): the same directory.

## Global Constraints

- **Owner decisions override the advisor design** (`advisor-design.md`) wherever they differ:
  decision 13 (replica active expiry), 10 (partial PSYNC in scope), 12 (perf bar), 16
  (`KEYDB_REQUIRED`), 17 (CI). `capa activeExpire` is a **separate** `REPLCONF`, sent only after an
  `active-replica` capa reply, on plain and peer links.
- **No-forward invariant:** foreign-authored writes are stamped with a non-self `origin_idx` so
  `PassesPeerEchoFilter` refuses them. Never stamp a classic author with `kSelfIdx`.
- **`--active_replica` off stays byte-identical** to upstream on the journal wire and RDB output
  except the spec's "Byte-identity exceptions". `kDrakeydbReplVersion` stays 68. The P3
  golden-buffer journal test must stay green.
- **New fork logic goes in new files** (`classic_replay.{h,cc}`, `classic_replay_test.cc`); hooks
  into upstream files are minimal. Do not edit `helio/`; do not touch `src/core/dash.h` or
  `src/core/compact_object.*`.
- CLAUDE.md: read before edit; `util::fb2::*` sync (a mutex whose critical section never yields is
  still `fb2::Mutex` here); no `std::regex`; Google style, `snake_case` vars, `PascalCase`
  functions, `kPascalCase` constants; no commented-out code; comment density matches the file.
- Malformed input from a master is **skipped, counted and warned with `LOG_EVERY_T`, never a
  disconnect, never a `CHECK`**; `repl_offs_` stays exact.
- Build: `ninja -C /home/user/drakeydb/build-dbg -j4 <target>` (never more than `-j4`; `-Werror`;
  keep `WITH_SEARCH` ON; run a complete `ninja` before any `ctest -L DFLY`).
- Pytest (absolute paths, from the repo root):
  `cd /home/user/drakeydb && DRAGONFLY_PATH=/home/user/drakeydb/build-dbg/dragonfly
  KEYDB_SERVER_PATH=<abs keydb-server> /root/drakey-venv/bin/python -m pytest
  tests/dragonfly/<file> -x -q`. Gate runs add `KEYDB_REQUIRED=1`. Timing-sensitive tests run x10.
  **Wrap every pytest run in `flock /tmp/drakey-pytest.lock …`**: `conftest.py` `rmtree`s
  `/tmp/dragonfly_logs` at session start, so concurrent sessions in one container corrupt each other.
- `pre-commit run --files <changed files>` before every commit. Commit subjects <= 100 characters,
  conventional-commit prefix, suffix `(P7)`, every body line <= 100. No `Signed-off-by` trailer
  (owner squash-merges; the local signed-commit hook is not enforced in CI). Do not commit or push
  unless the orchestrator says so; never push to `main`.
- **Falsify every test:** revert the code under test (or flip the guarded condition), rebuild, run,
  capture the failing output, restore, rerun. Record the exact commands and the verbatim failing and
  passing output in
  `docs/superpowers/ledgers/2026-10-03-phase7-keydb-onboarding/task-<N.M>-report.md`.
  Also record what each test would still pass under with the feature removed.
- **The gate** (every sub-PR; `KEYDB_REQUIRED=1`, absolute paths): a complete
  `ninja -C /home/user/drakeydb/build-dbg -j4`; then the **full** suite — never a subset, because a
  PR regresses suites it did not expect to touch —
  ```
  cd /home/user/drakeydb/build-dbg && ctest -L DFLY --output-on-failure
  ```
  (about 220 s here; `classic_replay_test` exists from Task 0.4); then the pytest command above on
  `multimaster_test.py`, `keydb_onboarding_test.py`, `keydb_harness_test.py` and
  **`redis_replication_test.py`** (the upstream suite that drives `Greet`, `InitiatePSync` and
  `ConsumeRedisStream` against a plain Redis; it runs locally through the `redis-server` fallback),
  plus `replication_test.py` for P7-3. Timing-sensitive tests run x10 for a pass rate. The PR also
  needs upstream `ci.yml` and `drakeydb-ci.yml` green before it is called done.
- **Performance is measured on release builds** (spec D-12): `./helio/blaze.sh -release
  -DWITH_AWS=OFF -DWITH_GCP=OFF`, then only `CCACHE_DISABLE=1 ninja -C
  /home/user/drakeydb/build-opt -j4 dragonfly` (free disk first by deleting the non-gate debug
  test binaries; they relink from objects later). Debug builds and CI run the functional smoke only.
- Anchors below were re-read at `c60dfdb`; `src/` is unchanged since except the P7-0
  implementation commit `a0ee234` (Tasks 0.4-0.6 and 0.9, which shifted lines in `replica.cc`,
  `rdb_load.cc` and `main_service.cc`); the other commits since are ledger/doc commits and the
  test/CI harness (`5199f34`, `89414e5`). Re-verify before editing; if one drifted, fix it here in
  the same commit.

## Verified anchors (c60dfdb)

| What | Where |
|---|---|
| `Replica::Greet`; capa send + strict `OK` check | `replica.cc:390`; `:431-432` |
| UUID exchange; peer admission; origin-hash registration (`AwaitBrief`) | `:439-482`; `:486-564`; `:545-549` |
| `DRAKEY-VERSION` / `PEER` tolerance; `REPLCONF capa dragonfly`; Redis-branch `OK` check; `R_GREETED` | `:573-584`; `:588`; `:610-611`; `:620` |
| `Replica::Start` (its `Greet()` call precedes `MainReplicationFb`) | `:126-171` (`:163`) |
| `MainReplicationFb`: `SetShardStates(true)`; greet; full sync; stable; revert | `:267-388`: `:272-273`; `:309-334`; `:337-362`; `:364-377`; `:383-385` |
| `Replica::SetShardStates` | `:1345-1347` (decl `replica.h:135`) |
| `InitiatePSync`: local `io_buf{128}`; id/offs; send; header parse | `:715-851`: `:716`; `:719-725`; `:727`; `:732` |
| `InitiatePSync`: LOADING; peer merge/flush; loader setup; `Load`; EOF `CHECK`s | `:751`; `:758-772`; `:774-811`; `:814`; `:818-835` (six `CHECK`s: `:823,828,829,831,832,835`) |
| `ConsumeRedisStream`: local `io_buf`; context; NONE builder; `ACK 0` | `:1111-1280`: `:1112`; `:1113-1124`; `:1127`; `:1131` |
| `ConsumeRedisStream`: batch; read; MULTI/EXEC skip; queue; `ConsumeInput`; skipped bytes; dispatch; offset | `:1154-1167`; `:1176`; `:1196`; `:1206-1211`; `:1215`; `:1217-1227`; `:1229-1270`; `:1262` |
| `RedisStreamAcksFb`; `DflyShardReplica` ctor; guard expression | `:1671-1695`; `:1731-1783`; `:1755-1756` |
| `ParseReplicationHeader`: FULLRESYNC overwrite; EOF `CHECK_EQ`; `CONTINUE` | `:1858-1935`: `:1880-1889`; `:1910`; `:1922-1928` |
| `Replica::GetSummary` | `:1937-1983`; `ReplicaSummary` `replica_types.h:14-35` |
| `CheckRespIsSimpleReply`; `ReadRespReply` (`total_read`); `ResetParser`; `PC_RETURN_ON_BAD_RESPONSE` | `protocol_client.cc:433-436` (decl `.h:108`); `:282-331` (`:293,308,314`); `:470-473`; `protocol_client.h:192-201` |
| Unknown command into NONE builder; `ReportUnknownCmd`; `FindCmd` | `main_service.cc:1538-1548`; `:1918-1927`; `:1960` |
| `PrepareTransaction` `SetReplOrigin`; `DispatchSquashedBatch` `SetReplOrigin` | `main_service.cc:891-894`; `:1815-1816`; squasher `multi_command_squasher.cc:114-115` |
| U-9 migration branch; `TAKEN_OVER` deref | `main_service.cc:2458-2463` (`:2459`, `:2462`); `:1430` |
| `ConnectionContext` repl fields; `CommandContext` | `conn_context.h:369-378`; `:433` (`ParsedCommand` is a `cmn::BackedArguments`, `facade/parsed_command.h:46`) |
| `JournalExecutor`: ctor; `Execute(dbid, cmd)` + rewrite gate; `Execute(cmd)`; `SelectDb` | `journal/executor.cc:34-42`; `:47-62` (`:57-58`); `:80-83`; `:85-100` |
| Guard table (7 names); `ApplyLwwRewrites`; `IncomingStamp`; `LwwGuardActive` | `multimaster_lww.cc:37-42`; `:65`; `:130-144`; `multimaster_lww.h:88` |
| `ShouldDropForLww`; `RunSquashedMultiCb` + `LOG(DFATAL)` tripwire | `transaction.cc:807-842`; `:1624`, `:1628-1648` |
| `OpMSet` guard; `SetCmd::Set` NX/XX; `CmdMSetNx` | `string_family.cc:419`; `:971-992`; `:1763-1798` |
| `OpDelV2` guard; `DEL`/`RESTORE` registration; RESTORE options | `generic_family.cc:1488`; `:3408`, `:3482`; `:2881` |
| `PassesPeerEchoFilter`; `RecordEntry`; `Commit` call | `journal/types.cc:51-67`; `journal/journal.cc:104`, `:121`, `:172` |
| `MvccStamper::RegisterOriginHash`/`OriginHash`; stamp from origin | `mvcc.cc:66`, `:72`, `:203` |
| `PeerRegistry` (append-only, `Op::ORIGIN` fan-out) | `multi_master.h:93-130`; `multi_master.cc:226-278` |
| `IsValidNodeUuid`; `NormalizeNodeUuid`; `kDrakeydbReplVersion` | `node_identity.h:47`, `:50`; `:29` (= 68) |
| `FLAGS_dbnum` | `generic_family.cc:57` |
| `Heartbeat`; the replica expiry gate; `RetireExpiredAndEvict`; `eviction_goal`; `DeleteExpiredStep` call; eviction call | `engine_shard.cc:776`; `:847`; `:863`; `:885`; `:942`; `:964` |
| `SetReplica` / `is_replica_`; `RetireExpiredAndEvict` decl | `engine_shard.h:158`, `:327`; `:302` |
| `--replica_delete_expired`; `ExpireIfNeeded` gate; sweep call; `FreeMemWithEvictionStepAtomic` + `DCHECK` | `db_slice.cc:60`; `:2097-2098`; `:2574`; `:2674`, `:2681` |
| RDB load loop; `DF_MVCC`; `AUX`; `DF_TOMBSTONES`; type check; `settings.Reset()` | `rdb_load.cc:2468-2720`; `:2508-2523`; `:2620-2622`; `:2672`; `:2703-2711`; `:2718` |
| `ObjSettings`; `HandleAux`; `mvcc-tstamp` branch; bit-63 warning; unknown-aux warning | `rdb_load.cc:2338-2375`; `:3054-3219`; `:3160-3211`; `:3193`; `:3215` |
| `rdbIsObjectTypeDF`; DF type range; opcodes 220-225 | `rdb_extensions.h:21-26`; `:12-19`; `:47-90` |
| INFO: active block; plain-replica block; `need_metrics` | `server_family.cc:3138-3162`; `:3164-3192`; `:3383` |
| Peer INFO line; boot warnings | `multi_master.cc:195-216`; `:144-158` |
| Prometheus: replica side; `multimaster_lww_dropped_total`; replica summary gauges | `metrics.cc:486-489`; `:511-518`; `:700-716` |
| `ServerState::Stats` size assert | `server_state.cc:67` (`31 * 8`) |
| CMake: `dragonfly_lib` sources; tests; `check_dfly` deps | `src/server/CMakeLists.txt:109-125` (`:114`); `:172`, `:200`; `:202-207` |
| Fixtures: `MvccStoreTest`; `ApplyReplicatedCommand`; `DflyEngineTest` | `multi_master_test.cc:793`, `:839`; `dragonfly_test.cc:134` |
| pytest: `RedisServer`; `redis_server` fixture; `Proxy`; `wait_for_peers`; `active_args`; `attach`; `STORM_BOUND` / `assert_no_command_storm`; `_parse_mvcc`; metrics | `instance.py:563` (`RedisServer`), `:636` (`KeyDBServer`); `conftest.py:614` (`redis_server`), `:635`, `:665` (`keydb_server*`); `proxy.py:21-37`, `:174-211`; `multimaster_test.py:475`, `:595`, `:609`, `:1404`, `:1407`, `:2292`; `instance.py:391` |
| Docs: "classic links never guarded" paragraph; stamped vs unstamped peers | `docs/multi-master.md:311-381`; `:87-122` |
| KeyDB v6.3.4 (read-only reference): capa reply; warning; wire; nesting; dedup | `replication.cpp:1740-1753`; `:1798-1803`; `:562-694`; `:5297`; `:5435`, `:5453`, `:5485` |
| KeyDB `KEYDB.MVCCRESTORE`: command entry; handler (mvcc, expire, verify skip, bad data, merge, TTL); emitter + caller; `INVALID_EXPIRE`; `dbMerge`; `RDB_TYPE_CRON` | `server.cpp:1168`; `cluster.cpp:5206` (`:5212`, `:5215`, `:5219`, `:5232`, `:5238-5239`); `replication.cpp:5573-5594`, `rdb.cpp:2942`; `expire.h:8`; `db.cpp:376-390`; `rdb.h:96` |
| drakeydb `RESTORE`: footer check (`ignore_crc`); type gate; `OpRestore` delete-before-load; handler; guard rewrite | `generic_family.cc:69-97`; `:191-199`; `:716`, `:734`; `:2878`, `:2892`; `multimaster_lww.cc:111-125` |
| KeyDB: config names; propagate forms; RDB cron/aux | `config.cpp:2976`, `:2949`, `:742-753`; `aof.cpp:682-726`; `rdb.cpp:1167`, `:1194-1195`, `:2577-2588`, `rdb.h:96` |
| Full-sync tail: loader first read (unclamped) and later reads (clamped); the tail `CHECK`s; `FlushSlots` / `FlushAll` | `rdb_load.cc:2421-2427`; `:1203`; `replica.cc:823-835`; `:769`, `:771` |
| `DispatchCommand` reject paths; `InvokeCmd` `ConsumeLastError` and the DENYOOM gate; the `MarkForClose` null `conn()` (side finding) | `main_service.cc:1589-1610`, `:1628-1632`; `:1740`, `:1750`, `:1721-1724`; `:1645-1647`, `:1744-1747` |
| `IsLwwGuarded`; `ReplicaOfInternal` critical section; `PeerRegistry::Size` | `transaction.h:400-401`; `server_family.cc:3667-3677`; `multi_master.h` |
| `GetRdbVersion` (rejects `size <= 10`); `RESTORE` rejects length/version before `OpRestore`; aux stamp ignored when `load_origin_hash_ == 0`; aux overwrite | `generic_family.cc:69-73`; `:2891-2895`; `rdb_load.cc:3196`; `:3206` |
| `CapturingReplyBuilder` records `last_error_` before `SKIP_LESS`; `ONLY_ERR` mode | `facade/reply_capture.cc:22-26`; `reply_mode.h:10` |
| helio: `fb2::Mutex` non-reentrant; `CondVarAny::wait_for`; `GetRunningTimeCycles` (time since last resume) | `synchronization.cc:70`, `:94`; `synchronization.h`; `fiber_interface.cc:336-339`, `:565` |
| KeyDB: uuid minted per start; mvcc `ms << 20`; only dirtying commands propagate; failed NX/XX and `MSETNX` return before the write; no-forward warning and forward skip; `repl-diskless-sync` default | `server.cpp:4082`; `server.h:960`, `server.cpp:7270-7287`; `server.cpp:4618-4648`; `t_string.cpp:104-108`, `:556-563`; `config.cpp:2705-2710`, `replication.cpp:5507`; `config.cpp:2826` |
| Golden RREPLAY captures (per-segment table in the README) | `tests/dragonfly/data/keydb_v6.3.4_rreplay_{stream,nested_stream}.bin` |

---

## P7-0 `feat/phase7-0-closeout-and-harness`

### Task 0.1: P4 close-out docs and the U-8 live test (done; review pending)

**Goal:** Make the repo's status text true before building on it: PLAN.md/README status, P4-4
record, version 67 to 68, Phase 5/6 notes, stale "(once it exists)", re-owned register items, and
settle the U-8 dispute.
**Files:** `docs/PLAN.md`, `docs/README.md`/`README.md`, `docs/UPSTREAM-SYNC.md`,
`docs/ISSUE-REGISTER.md`.
- [x] Committed as `a162d75` and `a549973`. U-8 was **withdrawn** by a live re-test (16/16
  `GEORADIUS*/STORE*` destinations replicated; `geo_family.cc:654` sets `journal_update` and
  `ZSetFamily::OpAdd` hand-journals, `zset_family.cc:1963`, `:2053-2072`). The active-KeyDB
  handshake failure was also reproduced live and recorded in `progress.md`.
- [ ] Reviewer pass (spec + quality); fix loop.

**Falsification:** docs-only; the live U-8 test is its own evidence (ledger `progress.md`).
**Done:** review green; register owners no longer say "P4-5"/"PR-B".

### Task 0.2: Baseline gate on unmodified main

**Goal:** A full P4 exit-gate baseline, so P7 regressions are separable from pre-existing flakes.
**Files:** none (ledger `progress.md`, `docs/PLAN.md` P4-4 record).
- [ ] `ninja -C /home/user/drakeydb/build-dbg -j4` (warning-free), then
  `cd /home/user/drakeydb/build-dbg && ctest -L DFLY` (full).
- [ ] Pytest, each file separately: `multimaster_test.py`, `multimaster_merge_test.py`,
  `redis_replication_test.py` (the upstream suite for `Greet`/`InitiatePSync`/`ConsumeRedisStream`,
  runnable here through the `redis-server` fallback), `replication_test.py`,
  `replication_specific_test.py`, `replication_resilience_test.py`, `replication_config_test.py`,
  `cluster_test.py::test_cluster_migrations_sequence`.
- [ ] Triage each failure: rerun x3 in isolation, classify as flake or real, record evidence. The
  baseline's `ReaperJournalFamilyTest.MemberExpiryReaperDoesNotBlockOnConcurrentBgsave` failure
  (`multi_master_test.cc:10148`, loaded `-j3` run only) is owned by Task 0.9.
- [ ] Counts into the ledger and into `docs/PLAN.md`'s P4-4 record.

**Falsification:** n/a (measurement). **Done:** 0 unexplained failures; counts recorded.

### Task 0.3: This spec and plan

**Goal:** Commit the Phase 7 design spec and implementation plan in the P4 documents' house style.
**Files:** `docs/superpowers/specs/2026-10-03-phase7-keydb-onboarding-design.md`,
`docs/superpowers/plans/2026-10-03-phase7-keydb-onboarding.md`.
- [ ] Reviewer pass; fix loop; the orchestrator commits.

**Falsification:** n/a. **Done:** both documents committed; `pre-commit` clean.

### Task 0.4: `Greet()` accepts `+OK <suffix>`

**Goal:** Remove the handshake Critical: `REPLCONF capa ...` replies of the form `OK` or
`OK <words>` are accepted at both `Greet()` sites. Nothing else about the handshake changes.
**Files:** Create `src/server/classic_replay.h`, `src/server/classic_replay.cc`,
`src/server/classic_replay_test.cc`; Modify `src/server/replica.cc`, `src/server/CMakeLists.txt`
(`:114` add `classic_replay.cc` to `dragonfly_lib`; `:200` `helio_cxx_test(classic_replay_test
dfly_test_lib LABELS DFLY)`; `:202-207` add to `check_dfly`); Test
`tests/dragonfly/keydb_onboarding_test.py`.
**Interfaces:** Produces `struct CapaReply { bool ok; bool active_replica; bool
keydb_fastsync_save; }` and `CapaReply ParseCapaReply(std::string_view)` (first token exactly `OK`,
case-sensitive; later tokens space-separated, unknown words ignored).

- [ ] **Step 1: Failing tests.** `ClassicReplayTest.ParseCapaReplyAcceptsOkAndSuffixes` (`OK`,
  `OK active-replica`, `OK keydb-fastsync-save`, `OK active-replica keydb-fastsync-save`, extra
  spaces) and `ClassicReplayTest.ParseCapaReplyRejectsNonOk` (`OKAY`, `ok`, `ERR`, empty,
  `OKactive-replica`). Pytest `test_greet_accepts_keydb_active_replica_capa_reply`, two halves,
  each with its own `proxy_factory` proxy in front of the `redis_server` fixture master and a plain
  drakeydb replica: half A overrides the reply to `REPLCONF capa eof` with `+OK active-replica\r\n`
  (site `replica.cc:432`); half B overrides the reply to `REPLCONF capa dragonfly` (site `:611`).
  Each asserts `REPLICAOF` returns `OK`, `master_link_status:up`, and a seeded key arrives.
- [ ] **Step 2: Run, observe failure.** gtest does not build (no header). Pytest: `REPLICAOF`
  fails; the drakeydb log shows `Bad response to "REPLCONF capa eof capa psync2":
  "+OK active-replica\r\n"` (as reproduced live in `progress.md`).
- [ ] **Step 3: Implement** `ParseCapaReply`; in `Greet()` replace the two strict checks with
  `ParseCapaReply` over `LastResponseArgs()` (exactly one `STRING` arg), keeping
  `PC_RETURN_ON_BAD_RESPONSE` diagnostics. Do **not** record or act on `active_replica` yet
  (Task 1.4). All other `CheckRespIsSimpleReply("OK")` calls in `Greet()` stay.
- [ ] **Step 4: Run tests; falsify** by restoring the strict-equality checks: both pytest halves
  and the `ParseCapaReply*` tests fail. Restore, record verbatim.
- [ ] **Step 5:** `ninja -j4 classic_replay_test dragonfly`, `./classic_replay_test`, the pytest;
  pre-commit; commit
  `fix: accept "+OK <suffix>" capa replies so an active KeyDB can be greeted (P7)`.

**Done:** both halves pass against a real Redis master and `redis_replication_test.py` stays green
(the stock-Redis handshake path is unchanged; a byte-level check needs the `Proxy` request capture
that arrives with Task 1.4, so this task does not claim one).

### Task 0.5: U-9 — null `conn()` in `EvalInternal`

**Goal:** A replicated single-shard `EVAL` whose key lives on another shard no longer dereferences a
null connection.
**Files:** Modify `src/server/main_service.cc` (`:2459`); Test `src/server/dragonfly_test.cc`;
`docs/ISSUE-REGISTER.md`.
- [ ] **Step 1: Failing test** `DflyEngineTest.EvalReplicatedApplyNoConnNoCrash`: build a
  `ConnectionContext{nullptr, acl::UserCredentials{}}` exactly as `ConsumeRedisStream`
  (`replica.cc:1113-1117`) does (`is_replicating`, `journal_emulated`, `skip_acl_validation`, `ns`),
  a `facade::CapturingReplyBuilder{ReplyMode::NONE}`, and dispatch `EVAL "return
  redis.call('SET', KEYS[1], 'v')" 1 <key>` inside `pp_->at(0)->LaunchFiber(...).Join()` (same shape
  as `MvccStoreTest::ApplyReplicatedCommand`, `multi_master_test.cc:839`) with `<key>` chosen so
  `Shard(key, shard_set->size())` differs from thread 0's shard. Assert the key is then set. Needs
  `proactor_threads >= 2` shards; check the fixture's shard count.
- [ ] **Step 2: Run, observe failure:** SIGSEGV in `RequestAsyncMigration` (null `this`).
- [ ] **Step 3: Implement** `&& conn_cntx->conn() != nullptr` in the condition at `:2459`.
- [ ] **Step 4: Run; falsify** by removing the guard again: the test binary crashes. Restore,
  record.
- [ ] **Step 5:** `ISSUE-REGISTER.md`: mark U-9 resolved by this task. The adjacent unguarded
  `dfly_cntx.conn()->IsPrivileged()` in the `TAKEN_OVER` branch (`main_service.cc:1430`),
  reachable on the same null-`conn()` apply context, was fixed in the same commit (`a0ee234`) and
  registered as U-10. Run
  `./dragonfly_test --gtest_filter='DflyEngineTest.Eval*'`; pre-commit; commit
  `fix: guard EvalInternal's migration against a null connection (P7)`.

**Done:** test crashes without the one-line guard and passes with it; the `TAKEN_OVER` deref is closed as U-10.

### Task 0.6: Graceful PSYNC `CHECK`s, the full-sync tail, and the fake classic master

**Goal:** A malformed full-sync header or tail from a master reconnects instead of aborting the
process; **a correct full sync whose stream follows the RDB at once no longer counts as corrupt, and
those stream bytes are applied** (spec D-10, review C2); and a scripted fake master exists for every
later test the `Proxy` cannot express.
**Evidence:** bytes after the RDB are the start of the replication stream. `RdbLoader::Load`'s
first read (`rdb_load.cc:2421-2427`) is the one read `source_limit_` does not clamp (later reads
are, `:1203`), and a disk-based master (KeyDB's default `repl-diskless-sync no`,
`config.cpp:2826`) flushes its buffered commands right behind the file. Live, 2 of 6 trials hit
`Bad full sync tail ... bytes left over` and looped in resync; on unmodified main the tail
`CHECK`s abort the process (an upstream latent crash).
**Files:** Modify `src/server/replica.{h,cc}` (`:823-835`, `:1910`; a `Replica` stream-prefix
buffer that `InitiatePSync` fills and `ConsumeRedisStream` drains before its first read),
`src/server/rdb_load.cc` (the first read clamped to `source_limit_`; an RDB running past its
declared size is `rdb_file_corrupted`, not a `DCHECK`); Create
`tests/dragonfly/fake_classic_master.py`; Test `tests/dragonfly/keydb_onboarding_test.py`.
**Interfaces:** `FakeClassicMaster(port)` — asyncio server that answers the handshake (`PING`,
`REPLCONF listening-port|capa|UUID|DRAKEY-VERSION|PEER`), answers `PSYNC` with a scripted byte
string (written as one `write()` so coalescing is deterministic), then sends scripted stream
bytes; records every request line and connection count. A valid empty RDB comes from
`src/server/testdata/empty.rdb`.
**Design:** only a short read, an overrun (the RDB runs past its declared size) or an EOF-token
mismatch is malformed (`LOG(ERROR)` plus an error code: reconnect). After a *correct* load
everything behind the RDB — the loader's `Leftover()` and the `UnusedPrefix()` of `ps` (the
`$<len>` framing) or of the chained source (the `$EOF:` framing, after the token) — goes into the
stream-prefix buffer, which `ConsumeRedisStream` parses before its first socket read and counts into
`repl_offs_`. D-8 reuses the same buffer for `+CONTINUE`.

- [ ] **Step 1: Failing tests** (plain drakeydb replica, fake master, `REPLICAOF` non-blocking):
  `test_psync_bad_eof_token_size_does_not_abort_replica` (`+FULLRESYNC <id> 0` then `$EOF:short`,
  site `:1910`); `test_psync_full_sync_tail_mismatch_does_not_abort_replica` — (a) a **token
  mismatch** (a valid empty RDB then a wrong 40-byte token) and (b) a `$<len>` that is shorter than
  the RDB (overrun) and one that is longer (short read): sites `:823-835`. Each asserts the process
  is alive, `INFO` answers, the error is logged, and the fake master sees a **second** connection
  (the replica reconnects). And the C2 tests, `test_psync_stream_bytes_behind_full_sync_are_applied`:
  the fake master writes `+FULLRESYNC`, `$<len>`, the RDB bytes and a raw `SET a 1` in **one**
  `write()` (and the `$EOF:<token>` framing with the token and the command in one write): `a == 1`,
  the link stays up, and `slave_repl_offset` equals the bytes of the command (exact offsets).
- [ ] **Step 2: Run, observe failure:** the drakeydb process dies on a `Check failed:` line and the
  pytest sees a dead node; for the C2 tests the replica aborts or loops in resync and `a` is never
  set.
- [ ] **Step 3: Implement:** replace `CHECK_EQ(kRdbEofMarkSize, token.size())` and the six
  `InitiatePSync` `CHECK`s with `LOG(ERROR)` plus `return std::make_error_code(errc::bad_message)`
  (or the `illegal_byte_sequence` the neighbouring header errors use); clamp the loader's first
  read; hand the bytes behind a correct RDB to the stream-prefix buffer. Clean up the `LOADING`
  state via the existing `absl::Cleanup` (`:756`).
- [ ] **Step 4: Run; falsify** by restoring one `CHECK` at a time (`:1910`, then `:823`): the
  matching test fails with the abort; then un-clamp the first read: the C2 tests fail; then drop
  the hand-off (discard the prefix): `a == 0`; then count the hand-off twice: the offset assertion
  fails. Restore, record.
- [ ] **Step 5:** pre-commit; commit
  `fix: report a malformed PSYNC reply as an error and keep the stream behind the RDB (P7)`. The
  upstream latent crash (a master that sends stream bytes right after the RDB aborts the replica)
  is registered as an ISSUE-REGISTER U-item by the orchestrator, not by this task's commit.

**Done:** all tests pass; the fake master is reusable by Tasks 1.2, 3.1.

### Task 0.7: KeyDB harness and the first real-KeyDB smoke test (done: `5199f34`, `89414e5`)

**Goal:** Tests can start a real KeyDB v6.3.4; locally they skip without it, gates fail without it.
**Files:** `tests/dragonfly/instance.py` (`RedisServer` fallback, new `KeyDBServer`),
`tests/dragonfly/conftest.py`, `tests/pytest.ini`, `docs/build-from-source.md`,
`tests/dragonfly/keydb_onboarding_test.py`, `tests/dragonfly/keydb_harness_test.py`,
`tests/dragonfly/data/`, `tests/dragonfly/tools/capture_keydb_rreplay.py`.
- [x] **Delivered.** `RedisServer.start` falls back to `$REDIS_SERVER_PATH`, then `redis-server` on
  `PATH` (honoring `redis7`); `KeyDBServer` (`--active-replica yes` before any `replicaof`,
  `config.cpp:742-753`; `--multi-master yes`, `--save ""`, `--appendonly no`, `--protected-mode no`,
  `--logfile`), with readiness meaning the answering process is the one we started; fixtures
  `keydb_server` / `keydb_server_factory` honoring `KEYDB_SERVER_PATH` (a set-but-bad path always
  fails) and `KEYDB_REQUIRED` (1/true/yes); the `keydb` marker; the KeyDB build recipe in
  `docs/build-from-source.md`. Smoke tests `test_keydb_plain_master_full_sync_and_stream`,
  `test_keydb_active_handshake_and_full_sync` and `test_keydb_active_handshake_peer_mode` seed
  strings, a hash, a list, a set, a zset, a TTL key (`900 < TTL <= 1000`), a db-1 key and 5000 bulk
  keys under `rdb-key-save-delay` (so the sync lasts at least 2 s), and `assert_keydb_saw_one_full_sync`
  (`sync_full == 1`, no partial) keeps a silent resync from passing; `keydb_harness_test.py` (21
  tests, no KeyDB needed) pins the harness rules. The first real KeyDB sync (5105 keys in 3.7 s)
  and every falsification are in `task-0.7-report.md`.
- [x] **Golden RREPLAY captures** from a real KeyDB v6.3.4:
  `tests/dragonfly/data/keydb_v6.3.4_rreplay_stream.bin` (1953 bytes, 14 envelopes) and
  `keydb_v6.3.4_rreplay_nested_stream.bin` (1944 bytes, 9 envelopes, depth 2), with a README of
  per-segment offsets and lengths, regenerated by `tests/dragonfly/tools/capture_keydb_rreplay.py`.
  Task 1.1 uses them as test vectors.

**Residuals.** The live write during a full sync against an *active* KeyDB is covered by
`test_keydb_active_live_write_during_full_sync[plain_replica|peer_mode]`. It fails until RREPLAY is
unwrapped, so it carries a **strict `xfail`** (`reason="needs RREPLAY unwrap (P7-1 Task 1.2)"`) and
**Task 1.2 removes the marker** — a pass with the marker still on fails the run. `_wait_ready`
compares `process_id` with the `Popen` pid, so a `KEYDB_SERVER_PATH` wrapper script that forks the
real server is refused; `rdb-key-save-delay` is a v6.3.4 config; the tests read `INFO replication`'s
`slave0.state` and `INFO stats` `sync_*` as v6.3.4 prints them.
**Done:** the smoke tests pass with the binary (the two active ones with Task 0.4), skip without
it, and fail under `KEYDB_REQUIRED=1`.

### Task 0.8: `drakeydb-ci.yml` (done: `5199f34`, `89414e5`)

**Goal:** CI builds drakeydb and KeyDB v6.3.4 and runs the KeyDB suite, unable to skip silently.
**Files:** `.github/workflows/drakeydb-ci.yml`.
- [x] **Delivered.** Triggers: `pull_request` to `main`, `workflow_dispatch`, and `push` to `main`
  (a run on main saves the KeyDB build under main's cache scope, which PRs can restore). Builds
  drakeydb with `./.github/actions/builder`; restores and **explicitly saves** the KeyDB build with
  `actions/cache/restore` + `actions/cache/save` keyed on the make flags, compiler and tag (a red run
  still saves it); installs only the runtime libraries on a hit; runs `keydb_onboarding_test.py` and
  `multimaster_test.py` with `KEYDB_REQUIRED=1`, filtered `-m "not large and not opt_only"` like
  `ci.yml`'s debug leg, and moves the last failing test's logs aside on a timeout. `yaml.safe_load`
  and `actionlint` pass (its only complaint is the upstream builder-action metadata quirk that
  `ci.yml` has too); the no-silent-skip property is falsified (`KEYDB_REQUIRED=1` with the path unset
  fails, not skips).
**Residual.** Nothing was observed on GitHub: the real cache restore/save and the first green run,
with the KeyDB tests shown *passed, not skipped* in the summary, belong to the P7-0 PR (below).
**Done:** the workflow is green on the P7-0 PR with the KeyDB tests actually executed.

### Task 0.9: Make `ReaperJournalFamilyTest.MemberExpiryReaperDoesNotBlockOnConcurrentBgsave` robust

**Goal:** Remove a load-dependent flake from the P7 gate: root-cause why the test's last assertion
fails under CPU contention, and make it deterministic — in the test, or in the code if it is a real
bug.
**Evidence:** Task 0.2's baseline `ctest -L DFLY -j3` on a loaded box failed `multi_master_test` at
`src/server/multi_master_test.cc:10148`: `Run({"exists", "rs"}).GetInt()` was 1, expected 0 (`"the
reaper did not resume once the snapshot consumer unregistered"`), after 228 ms in-suite; the same
test passed 20/20 in isolation.
**Files:** Test `src/server/multi_master_test.cc` (the test is `:10050-10150`); read
`src/server/db_slice.{h,cc}`; modify `db_slice.cc` only if the root cause is a product defect.
**Hypotheses to confirm or kill** (read from the code, not yet proven): (H1) the follow-up reap is
`DeleteExpiredStep(db_cntx, 100)` with default options, and `reset_time_quota` defaults to false
(`db_slice.h:643`), so `quota_start` is 0 (`db_slice.cc:2283`) and `quota_remains()` (`:2284-2287`)
compares `ThisFiber::GetRunningTimeCycles()` with 1 ms. That accessor is the time since the fiber
was last resumed (`cpu_tsc_` is stamped at every switch-in, `fiber_interface.cc:565`, read at
`:336-339`), not a cumulative total, but the conclusion holds: the quota gates every `Traverse`
iteration (`:2580`, `:2587`) and the member walk itself (`:2394`), and a cycle count includes time
the thread was descheduled, so under load the sweep can end before it reaches `rs`. (H2) With no
deletions the sweep makes at most `count / 3` = 33 `Traverse` steps per call (`:2580-2587`) from the
persistent `db.expire_cursor`; the first call, skipped because the consumer is registered, moves
that cursor by a time-dependent number of steps, so whether the second call's 33 steps reach the
bucket of `rs` among 33 keys depends on timing. (H3) The consumer is still registered when the
follow-up call runs (unregistration versus `WaitSnapshotting`).
- [ ] **Step 1: Reproduce.** `ninja -C /home/user/drakeydb/build-dbg -j4 multi_master_test`; run the
  one test x50 idle for a baseline pass rate, then x50 under artificial load shaped like the failing
  gate: `stress-ng --cpu 6` if present, else six `sh -c 'while :; do :; done'` spinners plus three
  concurrent copies of the test (`--gtest_repeat=50
  --gtest_filter='ReaperJournalFamilyTest.MemberExpiryReaperDoesNot*'`). Record both failure
  rates. Do not change the test yet.
- [ ] **Step 2: Root-cause, test-side instrumentation only.** Keep the `DeleteExpiredStats` each
  `DeleteExpiredStep` call returns (`traversed`), log `ThisFiber::GetRunningTimeCycles()` at the
  entry of each call and, if the accessor is reachable, `HasRegisteredCallbacks()`. A failing run
  that shows a small `traversed` or a large entry time confirms H1/H2; a registered consumer
  confirms H3. Record the output in `task-0.9-report.md`; remove the instrumentation.
- [ ] **Step 3: Fix.** If H1/H2 (a test that demands one bounded call hit a specific bucket): make
  the follow-up reap a bounded loop — call `DeleteExpiredStep(db_cntx, 100,
  {.ensure_member_reaping = true, .journal_deletions = false, .reset_time_quota = true})` until
  `exists rs` is 0, at most 64 times (enough to wrap the table several times), and on exhaustion
  fail with the per-call `traversed` list. The assertion keeps its meaning (the reaper resumes once
  the consumer is gone) without depending on one 1 ms quota. Leave the first, skip-while-registered
  assertion untouched. If H3, wait for the unregistration explicitly. If instead the evidence shows
  a product defect (for example `reset_time_quota` semantics starving a container in the production
  heartbeat), fix `db_slice.cc` with its own failing test and keep this test strict.
- [ ] **Step 4: Falsify under load, both directions.** (a) The original test under the Step 1 load
  fails at the recorded rate and the fixed one passes 200/200 under the same load. (b) The fixed
  test still catches what it exists for: make the reaper skip permanently (force the
  `!HasRegisteredCallbacks()` term at `db_slice.cc:2394` to `false`): the follow-up assertion
  fails; remove that term: the first assertion (`exists rs == 1`) fails. Restore each; record the
  commands and verbatim outputs.
- [ ] **Step 5:** `./multi_master_test --gtest_filter='ReaperJournalFamilyTest.*'` x10 idle plus the
  loaded runs; pre-commit; commit
  `test: make the reaper-resume assertion independent of the sweep's time quota (P7)`.

**Done:** the root cause is recorded with the instrumentation output, the test passes 200/200 under
the artificial load, and each falsification fails as named. Lands before the P7-0 gate so
`multi_master_test` is not a flake source.

### P7-0 gate and PR

- [ ] Whole-branch review (Opus) and an adversarial pass; fix loops.
- [ ] Run **the gate** (defined under Global Constraints), `KEYDB_REQUIRED=1`.
- [ ] Open the PR against `main` (CLAUDE.md PR format), subscribe to its activity, drive to
  mergeable; upstream `ci.yml` and `drakeydb-ci.yml` green. The owner merges.

---

## P7-1 `feat/phase7-1-rreplay-unwrap` (stacked on P7-0)

### Task 1.1: `ParseRreplayEnvelope`

**Goal:** A pure, KeyDB-faithful envelope parser.
**Files:** Modify `src/server/classic_replay.{h,cc}`; Test `src/server/classic_replay_test.cc`.
**Interfaces:** `struct RreplayEnvelope { std::string uuid; std::string_view inner;
std::optional<DbIndex> db; uint64_t mvcc = 0; }` — `uuid` is an **owned**, normalized (lowercase)
string, because the normalized form is not a view of the wire bytes and it keys the dedup and
author maps; `inner` views the stream buffer and must not outlive the loop iteration;
`enum class RreplayParse { kOk, kBadArity, kBadUuid, kBadDb, kBadMvcc }`;
`RreplayParse ParseRreplayEnvelope(const facade::RespVec&, RreplayEnvelope*)`. Validation mirrors
`replication.cpp:5389-5433`: `argc >= 3`; uuid is `IsValidNodeUuid` (case-insensitive,
returned/keyed normalized); inner is a string; optional db an integer with `0 <= db < FLAGS_dbnum`;
optional mvcc a `u64`.
- [ ] **Step 1: Failing tests** `ClassicReplayTest.ParseRreplayEnvelope*`: build `RespVec`s by
  running `RedisParser` over the **golden captures** from Task 0.7
  (`tests/dragonfly/data/keydb_v6.3.4_rreplay_{stream,nested_stream}.bin`; its README gives each
  segment's offset and length and prints segment 1 and the nested segment 1 as literals, which the
  gtest embeds, since it does not read `tests/`) and hand-built variants: full 5-arg `kOk` (the
  real `SET`, the `MULTI`/`EXEC` segments, the cron `ping` in lower case, and the nested depth-2
  envelope whose `inner` is itself an envelope); 3-arg `kOk` (db nullopt, mvcc 0); `kBadArity` at 2
  and 1 args; `kBadUuid` (35 chars, bad dash position, non-hex; uppercase accepted and returned
  lowercase); `kBadDb` (`-1`, `16` with dbnum 16, `abc`, overflow); `kBadMvcc` (`-5`, `abc`,
  overflow).
- [ ] **Step 2: Run, observe failure** (symbols absent). **Step 3: Implement.**
- [ ] **Step 4: Falsify** by changing `db < dbnum` to `<=` (the `16` case fails) and by accepting
  only lowercase (the uppercase case fails). Restore, record.
- [ ] **Step 5:** pre-commit; commit `feat: parse KeyDB RREPLAY envelopes (P7)`.

**Done:** all cases pass; vectors include real KeyDB bytes.

### Task 1.2: Unwrap in `ConsumeRedisStream`

**Goal:** RREPLAY commands apply on every classic link, in order, with exact offsets, no dispatch
of control commands, and a defined outcome when the link is cancelled (spec D-3 and D-5
"Cancellation and offsets").
**Files:** Modify `src/server/classic_replay.{h,cc}` (`ClassicApplier`, `ClassicLinkStats`),
`src/server/replica.{h,cc}`; Test `src/server/classic_replay_test.cc`,
`tests/dragonfly/keydb_onboarding_test.py` (remove the strict `xfail` from
`test_keydb_active_live_write_during_full_sync`).
**Interfaces:** `ClassicApplier` (constructed with `Service*`, `ConnectionContext*`, a link-owned
`facade::CapturingReplyBuilder{ReplyMode::ONLY_ERR}` — **not** the `NONE` builder, which records
nothing while `InvokeCmd` consumes `last_error_` itself, `main_service.cc:1740`, `:1750` — self
uuid, link uuid, `ClassicLinkStats*`, a `running()` callback, and **optional** seams `AuthorDedup*`
/ `ClassicAuthorMap*` that stay null until P7-2) with `static bool IsRreplay(const RespExpr&)`,
`EnvelopeResult HandleRreplay(const facade::RespVec&, unsigned depth = 1)` (`kConsumed` /
`kNotConsumed`), a `SelectDb(DbIndex)` helper mirroring `journal/executor.cc:85-100`, and the
inner-command handling of spec D-3 (exactly one command per inner, parsed by a `RedisParser`
local to each call).

- [ ] **Step 1: Failing tests.** `ClassicApplyFamilyTest` (a `BaseFamilyTest` fixture, driving the
  applier on `pp_->at(0)->LaunchFiber(...).Join()`): `AppliesInnerCommandInEnvelopeDb`,
  `SkipsInnerControlCommands` (`PING`, `MULTI`, `EXEC`, `REPLCONF`, `SELECT`),
  `SelectSetsDbWithoutTouchingOffsets` (the synthetic `SELECT` for the envelope db),
  `DropsSelfAuthoredEnvelope`, `MalformedEnvelopeSkippedAndCounted` (bad uuid, bad db, bad mvcc,
  arity, empty inner, partial inner, **two commands in one inner**, trailing bytes — none of them
  applies anything), `NestedUnwrapAllowedTo64AndRefuses65th` (the 65th is malformed, the 64th
  level's call still returns `kConsumed`), `KnownCommandErrorReplyCounted` (`INCR` on a hash key:
  `classic_apply_errors == 1`, `kConsumed`, the next envelope applies),
  `RunningFalseBeforeDispatchReturnsNotConsumed` (nothing dispatched, no synthetic `SELECT`, no
  counter moves), `RunningFalseDuringFirstDispatchStillConsumesWholeEnvelope` (`running()` flips
  false from inside the first dispatch; the whole tree completes and the result is `kConsumed`),
  `RejectedDispatchCountsAndConsumes` (an inner command that `VerifyCommandState` rejects before it
  runs — a wrong-arity `SET k`, `main_service.cc:1412` — gives `classic_apply_errors == 1` and
  `kConsumed`). Pytest (`keydb` marker, real KeyDB
  active): `test_plain_replica_unwraps_keydb_rreplay` (SET/INCR/HSET/LPUSH/DEL/EXPIRE across db 0
  and 1), `test_plain_replica_unwraps_nested_keydb_rreplay` (KeyDB A and B active, forwarding on,
  drakeydb replicates B only, key written on A arrives), `test_unwrap_keeps_offsets_exact` (idle:
  drakeydb `slave_repl_offset` equals KeyDB `master_repl_offset`, through cron `PING` and `GETACK`
  envelopes), and the already-written
  `test_keydb_active_live_write_during_full_sync[plain_replica|peer_mode]` with its `xfail` marker
  removed. Fake master: `test_unwrap_flushes_raw_batch_before_envelope` (raw `SET a 1`, then
  envelope `SET a 2`, then wait for idle: `a == 2`).
- [ ] **Step 2: Run, observe failure** (today the keys never arrive; offsets advance past
  dropped envelopes; with the marker removed the live-write tests fail).
- [ ] **Step 3: Implement** `ClassicApplier` and the stream hook: refactor the batch-dispatch block
  (`replica.cc:1229-1270`) into a lambda; before queuing, if `ClassicApplier::IsRreplay(last_args[0])`:
  call the lambda to flush; `if (!exec_st_.IsRunning()) break;` (without touching the envelope:
  `repl_offs_` is then the first undispatched raw command, the right PSYNC resume point); call
  `HandleRreplay(last_args)`; on `kNotConsumed` break; otherwise `io_buf.ConsumeInput(...)`,
  `repl_offs_ += response->total_read` (**after** any dedup `Commit`/`Advance`, which run inside
  `HandleRreplay`), `replica_waker_.notify()`, `continue`. Process the envelope **before**
  `ConsumeInput` (its views point into `io_buf`) and let no `string_view` outlive the iteration.
  Keep `RREPLAY` out of the registry. Do not advance `repl_offs_` for synthetic `SELECT`s.
  **Side finding, not fixed here** (the orchestrator registers it): `DispatchCommand` ends with
  `dfly_cntx->conn()->MarkForClose()` when `InvokeCmd` returns `ERROR` after catching an exception
  (`main_service.cc:1645-1647`, `:1744-1747`) — a null `conn()` on a replica apply context, the same
  family as U-9 and U-10.
- [ ] **Step 4: Run; falsify** each of these, recording each: (a) drop the pre-envelope flush: the
  fake master test ends with `a == 1`; (b) skip `repl_offs_ += total_read` on the envelope branch:
  `test_unwrap_keeps_offsets_exact` fails with the offsets apart; (c) remove the 65th-nesting
  refusal: `NestedUnwrapAllowedTo64AndRefuses65th` fails; (d) check `running()` after the first
  dispatch instead of before it: `RunningFalseBeforeDispatchReturnsNotConsumed` fails; (e) check it
  before every inner dispatch: `RunningFalseDuringFirstDispatchStillConsumesWholeEnvelope` fails;
  (f) accept a second inner command: the two-commands case of `MalformedEnvelopeSkippedAndCounted`
  fails; (g) use the `NONE` builder: `KnownCommandErrorReplyCounted` fails.
- [ ] **Step 5:** `ninja -j4 classic_replay_test dragonfly`; run both suites; pre-commit; commit
  `feat: unwrap RREPLAY envelopes on classic replication links (P7)`.

**Done:** every KeyDB-written key type reaches a plain replica; offsets exact; mixed streams
ordered; cancellation leaves a resumable offset; the strict `xfail` is gone and both
`test_keydb_active_live_write_during_full_sync` variants pass.

### Task 1.3: KeyDB-only and unknown commands, per-link counters, INFO and Prometheus

**Goal:** What drakeydb cannot represent is dropped loudly and counted, never silently, and a stock
master's INFO and `/metrics` stay exactly upstream's. `KEYDB.MVCCRESTORE` is **excluded** here: it
is applied, not dropped (decision 22, Task 2.6); until that task lands it is counted as an unknown
command.
**Files:** Modify `src/server/classic_replay.{h,cc}`, `src/server/replica.{h,cc}`,
`src/server/replica_types.h`, `src/server/server_family.cc`, `src/server/multi_master.cc`,
`src/server/metrics.cc`; Test `src/server/classic_replay_test.cc`,
`tests/dragonfly/keydb_onboarding_test.py`.
**Interfaces:** `bool IsKeyDbOnlyCommand(const facade::RespVec&)` (list in spec D-7; `PERSIST` only
with 3 args; case-insensitive; `KEYDB.*` prefix for the five named commands only);
`ClassicLinkStats` relaxed atomics (`rreplay_unwrapped`, `rreplay_malformed`,
`rreplay_self_dropped`, `keydb_cmds_dropped`, `classic_unknown_cmds_dropped`,
`classic_apply_errors`); `ReplicaSummary` gains `classic_link`, `master_active_replica` and the
per-link fields; process-wide counters in `classic_replay.cc`. **Rendering rule** (spec D-13): a
classic field renders only for a classic link whose master answered `active-replica` or whose own
counter is nonzero; process-wide counters use the same predicate.
- [ ] **Step 1: Failing tests.** `ClassicReplayTest.IsKeyDbOnlyCommand*` (table-driven, incl.
  `PERSIST k` vs `PERSIST k m`, mixed case, `KEYDB.MVCCRESTORE` **not** matched);
  `ClassicApplyFamilyTest.KeyDbOnlyDroppedAndCounted`, `.UnknownInnerCommandCountedNotDispatched`;
  `MultiMasterFamilyTest.RenderPeerReplicationInfoShowsClassicFieldsOnlyForClassicLinks` and
  `.OmitsClassicFieldsWhenMasterNotActiveAndCountersZero` (pure render; includes `repl_offset=`).
  Pytest: `test_keydb_only_commands_dropped_with_counters` (KeyDB `SADD s a b` + `EXPIREMEMBER s a
  100`, `KEYDB.CRON ...`, and a normal key: the normal key syncs, `keydb_cmds_dropped >= 1`,
  `rreplay_unwrapped` increments, drakeydb stays up); `test_info_and_metrics_show_classic_counters`
  (INFO block fields on a classic link to an active KeyDB; `/metrics` via
  `DflyInstance.metrics()`, `instance.py:391`: `<name>_total` present **on a plain replica too**,
  which never reaches the master-side Prometheus branch; **absent** on a DFLY-to-DFLY pair);
  `test_info_and_metrics_absent_for_stock_master` (a plain Redis master, every counter zero: no
  classic field in `INFO replication`, no classic series in `/metrics`);
  `test_active_replica_boot_warning_names_keydb_drops` (`find_in_logs` on an `--active_replica`
  node's boot log).
- [ ] **Step 2: Run, observe failure. Step 3: Implement** (raw path: only the KeyDB-only check;
  envelope path: KeyDB-only, then `FindCmd == nullptr`, each rate-limited with `LOG_EVERY_T`).
  Counters bump the per-link atomic and the process-wide total. Render per-link fields in the
  plain-replica block (`server_family.cc:3164-3192`) and the peer line (`multi_master.cc:195-216`);
  render process-wide counters in the active-node block beside `multimaster_lww_dropped`
  (`server_family.cc:3138-3162`) and in the plain-replica block after the per-link fields;
  Prometheus in `metrics.cc` in **both** the replica-side branch (`:486-489`, the only one a plain
  replica reaches) and the master-side branch beside `multimaster_lww_dropped_total` (`:511-518`),
  all behind the same predicate. Append "KeyDB member TTLs and cron jobs are dropped on onboarding"
  to the boot limitations warning (`multi_master.cc:144-151`). Do not touch `ServerState::Stats`.
- [ ] **Step 4: Falsify:** make `IsKeyDbOnlyCommand` return false: the drop test fails (the command
  lands in `classic_unknown_cmds_dropped`, `keydb_cmds_dropped` stays 0); omit the render branch:
  the INFO test fails; render unconditionally: `..._absent_for_stock_master` fails; omit the
  replica-side Prometheus branch: the plain-replica `/metrics` assertion fails. Restore, record.
- [ ] **Step 5:** pre-commit; commit
  `feat: drop and count KeyDB-only commands and expose classic-link counters (P7)`.

**Done:** counters visible in INFO and Prometheus, on a plain replica as well; INFO and `/metrics`
for a stock master unchanged.

### Task 1.4: `capa activeExpire` and replica active expiry

**Goal:** A plain replica of an active KeyDB advertises `activeExpire` and expires keys itself
(decision 13). A plain Redis master never triggers it.
**Files:** Modify `src/server/classic_replay.h`, `src/server/replica.{h,cc}`,
`src/server/engine_shard.{h,cc}`, `src/server/db_slice.cc`, `tests/dragonfly/proxy.py` (additive
request capture); Test `src/server/classic_replay_test.cc` (or `engine_shard_set_test.cc`),
`tests/dragonfly/keydb_onboarding_test.py`.
**Interfaces:** `Replica::master_active_replica_` (set per `Greet()` from
`CapaReply::active_replica`, cleared at its top); `EngineShard::SetReplicaActiveExpiry(bool)` /
`ReplicaActiveExpiry()`; `RetireExpiredAndEvict(bool expire_only = false)`.
- [ ] **Step 1: Failing tests.** gtest `ReplicaActiveExpiryTest.ReapsExpiredKeysButNeverEvicts` (a
  fixture friended in `engine_shard.h` to reach `Heartbeat()`; shards in replica mode with the flag
  on: expired keys are reaped, eviction is never invoked — `--cache_mode` with a tiny `maxmemory`
  evicts nothing; flag off: nothing reaped) and `...BypassesReplicaDeleteExpiredFlag`
  (`--replica_delete_expired=false` still reaps with the flag on). Pytest:
  `test_plain_replica_of_active_keydb_expires_keys` (KeyDB sets N keys `PX 500`; drakeydb polls
  `DBSIZE` **without reading** the keys: reaches 0) plus the control
  `test_plain_replica_of_plain_redis_never_expires_on_its_own` (Redis with `enable-debug-command
  yes` and `DEBUG SET-ACTIVE-EXPIRE 0` so it propagates no DELs: the replica's `DBSIZE` stays N);
  `test_greet_sends_capa_active_expire_only_after_active_replica_reply` (KeyDB with a logfile: the
  line `does not support active expiration` (`replication.cpp:1800`) must be absent after sync;
  a Redis master behind a capturing proxy sees no `capa activeExpire`; an active KeyDB behind the
  proxy sees exactly one, as its own `REPLCONF`, after the first capa reply).
- [ ] **Step 2: Run, observe failure** (no keys expire; the KeyDB warning is logged).
- [ ] **Step 3: Implement** per spec D-9 and D-2: record `master_active_replica_` from both capa
  replies (OR); send `REPLCONF capa activeExpire` right after the `:432` check when set, parse its
  reply leniently; `SetShardStates`'s sibling sets/clears the shard flag at `:272-273` / `:383-385`
  and after each **successful** `Greet()` (a failed one neither sets nor clears it), and only for
  the node's main `replica_` link (`!slot_range_`): cluster `ADDREPLICAOF` replicas run the same
  `MainReplicationFb`/`SetShardStates` (last writer wins), so replica active expiry through them is
  unsupported and documented; `Heartbeat` split with `expire_only` forcing `eviction_goal = 0` (so
  `db_slice.cc:2681`'s `DCHECK` stays untouched and unreachable on a replica); the one-line gate at
  `db_slice.cc:2097-2098` honours the flag; deletions journal when a journal exists.
- [ ] **Step 4: Run; falsify:** do not set the shard flag: `DBSIZE` never reaches 0; do not send
  `activeExpire`: the KeyDB warning appears; call eviction on the replica path: the `DCHECK` fires
  in the gtest. Restore, record each.
- [ ] **Step 5:** pre-commit; commit
  `feat: let a plain replica of an active KeyDB expire keys itself (P7)`.

**Done:** control passes (Redis replica still never self-expires); non-KeyDB handshakes unchanged.

### Task 1.5: Throughput — "must keep up with KeyDB"

**Goal:** Measure, on **release builds**, whether per-command dispatch keeps up; record the numbers
either way (spec D-12).
**Files:** Test `tests/dragonfly/keydb_onboarding_test.py`; ledger `task-1.5-report.md`.
- [ ] **Step 1: Build for the measurement.** `./helio/blaze.sh -release -DWITH_AWS=OFF
  -DWITH_GCP=OFF`, then only `CCACHE_DISABLE=1 ninja -C /home/user/drakeydb/build-opt -j4 dragonfly`
  (free disk first by deleting the non-gate debug test binaries under `build-dbg`; they relink from
  objects later).
- [ ] **Step 2: The test.** `test_keydb_onboarding_keeps_up_under_load` (`slow`, `keydb`): KeyDB
  active with `--server-threads 1`, drakeydb a plain replica with `--proactor_threads 2`, attached
  and idle-synced; each server pinned with `taskset` to its own cpuset and the load generator to a
  third; sustained pipelined writes over a fixed window (`redis-benchmark -P 100 -c 50 -t set,incr
  -r 100000`, which is on `PATH` here; else an asyncio pipeline loader), sampling KeyDB's
  `master_repl_offset` and drakeydb's `slave_repl_offset` at 1 Hz. Assert (1) the link never
  reconnects (`reconnect_count` unchanged, KeyDB `sync_full == 1`); (2) over the steady window
  `apply_rate / produce_rate >= 0.95` and the maximum lag `<= max(2 s x produce_rate, 8 MB)`; (3)
  the lag reaches 0 within 2 s of the load stopping and the final key counts match. The release
  bar runs under `DRAKEYDB_PERF=1`; **by default** the test is a rate-capped (about 5k ops/s)
  functional smoke with loose bounds (ratio `>= 0.5`, lag `< 32 MB`, drain `< 10 s`), so CI and
  debug builds exercise the plumbing without claiming the bar.
- [ ] **Step 3: Measure.** Three release runs, median, `DRAKEYDB_PERF=1`; also the **comparator**
  (the same load with a second KeyDB attached as an active replica of the same master: drakeydb's
  lag within 1.5x of its lag) and the raw squashed path (a non-active KeyDB or a Redis master) as
  the reference. Record offered ops/s, rates, the lag series and drain times in
  `task-1.5-report.md`.
- [ ] **Step 4: Falsify** the test's sensitivity: add a 1 ms `ThisFiber::SleepFor` per inner
  command (temporary): the smoke's ratio assertion must fail (and the release one under
  `DRAKEYDB_PERF=1`). Restore.
- [ ] **Step 5:** pre-commit; commit
  `test: pin that the RREPLAY path keeps up with KeyDB under load (P7)`.

**Done:** the release bar passes with recorded numbers — **or** it fails and Task 1.6 is opened.

### Task 1.6: Squasher micro-batch fallback (conditional on Task 1.5 failing)

Run **only if** the **release-build** bar of Task 1.5 fails (or Task 2.4's peer-mode re-run of it).
A failure of the CI/debug smoke is a bug in the test or the plumbing, not a trigger. Owner decision
12: "else optimize within P7".
**Goal:** Recover throughput by micro-batching same-author, same-shard envelope commands through
the squasher **with per-command mvcc**.
**Files:** Modify `src/server/classic_replay.{h,cc}`, `src/server/replica.cc`, and — only after the
advisor signs the design — `src/server/main_service.cc`, `src/server/multi_command_squasher.cc`,
`src/server/transaction.{h,cc}`.
- [ ] **Step 1: Measure first.** Profile where the time goes (parse, per-command `DispatchCommand`,
  shard hops, dedup mutex, stamping) and record it. Optimizations that do not touch the squasher
  (inner-parser reuse, avoiding per-envelope allocation, cheaper classification) come first.
- [ ] **Step 2: Design note and STOP.** `repl_mvcc` is batch-level (`main_service.cc:1815`,
  `multi_command_squasher.cc:114`), so micro-batching needs per-command stamp plumbing. Write the
  design (candidate: a per-`CommandContext` mvcc/origin read when the squasher builds each per-shard
  transaction) and get the advisor's sign-off before editing squasher/transaction files.
- [ ] **Step 3: Failing test** first: `ClassicApplyFamilyTest.MicroBatchKeepsPerCommandStamps` (two
  consecutive same-author envelopes in one batch end with their **own** envelope mvcc, not the first
  one's); the Task 1.5 test must pass afterwards.
- [ ] **Step 4: Falsify** by sharing the first command's mvcc across the batch: the stamp test
  fails.
- [ ] **Step 5:** pre-commit; commit
  `perf: micro-batch classic envelope commands with per-command stamps (P7)`.

**Done:** Task 1.5 passes; raw streams still squash as before.

### P7-1 gate and PR

- [ ] Whole-branch review, adversarial pass, fix loops. Gate and PR as in P7-0.
- [ ] The PR description must say, in so many words: (1) until P7-2's dedup lands, a peer attached
  to two or more **forwarding** KeyDB masters double-applies deltas (`INCR`, `APPEND`, ...); (2)
  `KEYDB.MVCCRESTORE` (decision 22, applied in Task 2.6 of P7-2) is unhandled until then: it is
  counted and warned as an unknown command, not applied.

---

## P7-2 `feat/phase7-2-author-stamps-dedup-guard`

### Task 2.1: `ClassicAuthorMap`

**Goal:** Resolve an envelope's author to a registered `PeerRegistry` index and origin hash, with a
bounded, observable cost (spec D-4 "Author cap").
**Files:** Modify `src/server/classic_replay.{h,cc}` (map, `--classic_author_cap`, counters),
`src/server/replica.{h,cc}` (`classic_link_uuid_changes`), `src/server/replica_types.h`,
`src/server/server_family.cc`, `src/server/multi_master.cc`, `src/server/metrics.cc` (the counters
and the registry-size gauge, rendered by spec D-13's rule); Test `src/server/classic_replay_test.cc`,
`tests/dragonfly/keydb_onboarding_test.py`.
**Interfaces:** `uint32_t ClassicAuthorMap::IdxFor(std::string_view uuid)`: the link uuid ->
`peer_origin_idx_` (never counted against the cap); any other (forwarded) uuid ->
`PeerRegistry::AddOrGet` plus `MvccStamper::RegisterOriginHash(idx, NodeUuidHash(uuid))` on every
proactor via `shard_set->pool()->AwaitBrief` (the primitive at `replica.cc:545-549`); memoized per
link. `--classic_author_cap` (default 4096, declared in `classic_replay.cc`) bounds the distinct
**forwarded** authors, process-wide; beyond it the **link's** idx is returned (never `kSelfIdx`),
the envelope's mvcc is kept, `multimaster_keydb_author_overflow` is bumped and a `LOG_EVERY_T`
warns. No reclamation: the registry is append-only and the indices live in journaled entries.
`Replica` remembers the previous master uuid across `Greet()`s (the per-`Greet()` clear of
`master_context_.master_node_uuid` at `replica.cc:436-437` wipes the one it would compare with) and
bumps `classic_link_uuid_changes` when the master presents a different one (a KeyDB restart,
D-1.16). `multimaster_peer_registry_size` is a gauge over `PeerRegistry::Size()`. A null registry
(tests) means stamping is off.
- [ ] **Step 1: Failing tests** (`MultiMasterFamilyTest`-style fixture with proactors):
  `ClassicAuthorMapTest.DistinctUuidsGetDistinctIdxAndRegisteredHashOnEveryProactor`,
  `.LinkUuidMapsToPeerIdx`, `.MemoizesPerLink`, `.CapFallsBackToLinkIdxAndCounts` (one author past
  a small `--classic_author_cap`: the link's idx, never `kSelfIdx`, the envelope mvcc kept, the
  counter bumped), `.CapIgnoresLinkUuid` (link uuids beyond the cap still map to their own idx).
  Pytest `test_keydb_restart_consumes_one_author_slot`: a peer-mode drakeydb attached to a KeyDB;
  restart KeyDB (a new uuid, D-1.16) and wait for the re-attach: `classic_link_uuid_changes == 1` on
  the link, `master0:node_uuid` changed, and `multimaster_peer_registry_size` grew by exactly one
  (and by one more per further restart — the append-only growth of Risk 7).
- [ ] **Step 2: Run, observe failure. Step 3: Implement.**
- [ ] **Step 4: Falsify:** skip `RegisterOriginHash`: `OriginHash(idx) == 0` and the hash assertion
  fails; remove the cap: the overflow test fails; count link uuids against the cap:
  `CapIgnoresLinkUuid` fails; stamp the overflow with `kSelfIdx`: `CapFallsBackToLinkIdxAndCounts`
  fails. Restore, record.
- [ ] **Step 5:** pre-commit; commit `feat: map classic RREPLAY authors to registered origins (P7)`.

**Done:** every proactor resolves each registered author's hash; the cap holds; the registry's
growth is visible.

### Task 2.2: Per-command stamps on peer links

**Goal:** Peer-mode classic commands carry `{envelope mvcc, author hash}`; plain replicas do not.
**Files:** Modify `src/server/classic_replay.cc`, `src/server/replica.cc`; Test
`src/server/classic_replay_test.cc`, `tests/dragonfly/keydb_onboarding_test.py`.
- [ ] **Step 1: Failing tests.**
  `ClassicApplyFamilyTest.PeerEnvelopeStampsKeyWithEnvelopeMvccAndAuthorHash` (peer-mode applier,
  `--active_replica` fixture like `MvccStoreTest`, `StampOf(key)` equals
  `{envelope mvcc, NodeUuidHash(author)}`), `.NestedEnvelopeStampsWithInnerAuthor`,
  `.PlainApplierLeavesMvccZero`, `.RestoresRawContextAfterEnvelope` (`repl_mvcc == 0`,
  `repl_origin_idx == peer_origin_idx_`). Pytest `test_keydb_write_stamped_with_author_hash`
  (`DEBUG MVCC <key>`, `debugcmd.cc:793`): keys written on KeyDB A and B show different `origin:`
  values, same-author keys the same, and the origin differs from drakeydb's own; a key written on A
  arriving through B's nested envelope shows A's origin, not B's.
- [ ] **Step 2: Run, observe failure. Step 3: Implement** spec D-4 (peer mode only; set before
  dispatch, restore after; author = innermost envelope).
- [ ] **Step 4: Falsify:** do not set `repl_mvcc` (the stamp is a local mint, not the envelope's);
  stamp with the link origin (the nested-author assertion fails). Restore, record.
- [ ] **Step 5:** pre-commit; commit
  `feat: stamp classic-applied writes with the envelope mvcc and author (P7)`.

**Done:** `DEBUG MVCC` shows KeyDB-authored stamps; no foreign write is forwarded
(`origin != self`).

### Task 2.3: `AuthorDedup`

**Goal:** A mesh of forwarding KeyDB masters never double-applies a delta — not even when the same
envelope reaches two links at the same instant (spec D-5; review C1, I1, I2).
**Files:** Modify `src/server/classic_replay.{h,cc}`, `src/server/replica.cc` (`Clear()` at the
flush point), `src/server/multi_master.cc` and `server_family.cc` (INFO), `src/server/metrics.cc`;
Test `src/server/classic_replay_test.cc`, `tests/dragonfly/keydb_onboarding_test.py`.
**Interfaces:** process-wide `AuthorDedup`: `Entry {applied, inflight, last_seen_ms}` under a
`util::fb2::Mutex` plus `CondVarAny` — a **leaf** lock, never held across a dispatch or an
`AddOrGet`, non-reentrant, with the map re-looked-up after every wait (it is not pointer-stable);
`Verdict Reserve(uuid, mvcc, running)` (`kApply` / `kDrop` / `kCancelled`), `Commit`, `Release`,
`Advance`, `IsApplied`, `Clear()`; bound `kMaxAuthorDedupEntries = 4096`, evicting the smallest
`last_seen_ms` among `inflight == 0`. The applier holds an RAII reservation, so every exit of the
leaf commits or releases. The exact protocol is spec D-5: one reservation per envelope tree on the
innermost level only; outer levels read `IsApplied` and `Advance`; `running()` is checked once per
outermost envelope (the point of no return); `Commit`/`Advance` happen **before** `repl_offs_ +=`;
`ERROR`/`OOM` is `Release` plus `classic_apply_errors` and still consumed.
- [ ] **Step 1: Failing tests.** `AuthorDedupTest.*` (two fibers on `pp_->at(0)` / `pp_->at(1)`):
  `ConcurrentSameEnvelopeAppliesOnce` (the dispatch stub sleeps 10 ms before `Commit`: exactly one
  `kApply`, the other `kDrop`), `ReleaseLetsSecondLinkApply`, `CancelledWaiterReturnsNotConsumed`
  (`running()` flips while a second link waits: `kCancelled`), `MvccZeroNeverDedupedNeverAdvances`,
  `AdvanceOnlyForward`, `EvictionSkipsInflight` (at the bound the oldest idle entry goes, a
  reserved one never), `ClearResetsAppliedKeepsInflight`. `ClassicApplyFamilyTest`:
  `NestedOuterAdvancesWhenInnerDeduped`, `AdvancesAfterSkippedInner`,
  `DoesNotAdvanceWhenMalformedOrSelf`, `Depth65MalformedStillAdvancesOuter`,
  `RunningFalseBeforeDispatchReturnsNotConsumedNoAdvance`,
  `RunningFalseDuringFirstDispatchStillConsumesWholeEnvelope` (now also asserting the commit),
  `RejectedDispatchCountsBytesDoesNotAdvance` (a wrong-arity `SET k`, rejected before it runs:
  `classic_apply_errors == 1`, `applied` unchanged, the envelope consumed, and a well-formed
  envelope with the same author and mvcc then applies once),
  `ReplayAfterCommitBeforeCountIsDeduped` (the harness stops after `HandleRreplay` returns, as a
  cancel between the commit and the offset count would: replaying the same bytes is dropped,
  `rreplay_deduped == 1`), and
  `ClassicAuthorMapTest.DedupStillWorksForOverCapAuthor` (an over-cap author's duplicate is still
  dropped). Pytest `test_keydb_mesh_forwarded_duplicates_deduped`: KeyDB A and B active, mutually
  `replicaof`, forwarding on, one drakeydb (`--active_replica --multi_master`) attached to both;
  **pipelined `INCR c` x N at full rate** on A, so the two links' deliveries overlap: the final
  value is exactly N and `multimaster_rreplay_deduped >= N`. Pytest
  `test_master_switch_full_sync_resets_author_dedup`: KeyDB C1 and C2 active, mutually `replicaof`,
  forwarding on; a plain drakeydb D replicates C1; a proxy on C2's link to C1 is paused; `INCR c`
  x N on C1 (D applies them, C2 does not have them yet); `REPLICAOF C2` on D (a flushing full
  sync); resume the proxy, so C2 receives the N increments and forwards them nested to D:
  **`c == N`** (without the `Clear()` D's watermark for C1 still holds the old link's mvcc and
  drops them, `c == N - k`).
- [ ] **Step 2: Run, observe failure** (value 2N; the master-switch test ends below N).
  **Step 3: Implement**; wire the counter `multimaster_rreplay_deduped` by spec D-13's rendering
  rule; call `Clear()` right after `FlushAll` (`replica.cc:771`) and `FlushSlots` (`:769`) in
  `InitiatePSync`, and neither on a peer merge sync nor on `+CONTINUE`.
- [ ] **Step 4: Falsify**, one at a time, restore and record: (a) the old check-only shape —
  `Reserve` reads `applied` but never sets `inflight` and never waits (`ShouldDrop`, dispatch,
  `Advance`): `ConcurrentSameEnvelopeAppliesOnce` sees two `kApply` (the deterministic proof) and
  the pytest mesh test ends in (N, 2N] (statistical: record the values over 5 runs, and if it does
  not exceed N on this box say so and rely on the gtest); (b) advance on `kNotConsumed`:
  `RunningFalseBeforeDispatchReturnsNotConsumedNoAdvance` fails; (c) `Commit` on `ERROR`/`OOM`
  instead of `Release`: `RejectedDispatchCountsBytesDoesNotAdvance` fails; (d) defer the `Commit`
  until after the return: `ReplayAfterCommitBeforeCountIsDeduped` fails; (e) skip `Clear()`: the
  master-switch pytest ends at `N - k` and `ClearResetsAppliedKeepsInflight` fails; (f) disable
  dedup entirely: the mesh test sees 2N.
- [ ] **Step 5:** pre-commit; commit
  `feat: dedup RREPLAY envelopes per author across classic links (P7)`.

**Done:** exactly-N under forwarding and under simultaneous delivery; the counter moves; a master
switch loses nothing.

### Task 2.4: Guard ON for classic peer links; rewrites and EVAL on every envelope link

**Goal:** KeyDB's state-carrying writes are LWW-guarded on peer links (decision 7), and conditional
commands replay as their author's effect on every envelope link, plain or peer (spec D-4.3, D-4.4,
D-6).
**Files:** Modify `src/server/classic_replay.{h,cc}`, `src/server/replica.cc`,
`src/server/transaction.cc` and `src/server/multimaster_lww.cc` (comments only),
`docs/multi-master.md:311-381`; Test `src/server/classic_replay_test.cc`,
`tests/dragonfly/keydb_onboarding_test.py`.
**Interfaces:** `bool ClassicApplyRewrites(cmn::BackedArguments*)`: `SET` loses `NX`/`XX`/`GET`
tokens (anywhere after the value, case-insensitive; `PX|PXAT|EX|EXAT <n>` and `KEEPTTL` kept)
**except** that `KEEPTTL` is dropped when `NX` was present (`SET k v NX KEEPTTL` becomes `SET k v`;
`SET k v XX KEEPTTL` becomes `SET k v KEEPTTL`); `MSETNX` becomes `MSET`. `repl_lww_guard` is set
once per link to `IsPeerMode() && IsActiveReplica() && FLAGS_multi_master_stream_lww`. On **every**
envelope dispatch, plain and peer: `ApplyLwwRewrites` then `ClassicApplyRewrites`,
unconditionally (the guard decides only whether the dispatch is vetoed; raw streams are untouched;
`JournalExecutor`'s `LwwGuardActive` gate for DFLY links is unchanged; `ApplyLwwRewrites`'s
contract comment gets the envelope carve-out). `EVAL`/`EVALSHA`: `repl_mvcc = envelope.mvcc` and
the author's `repl_origin_idx` with `repl_lww_guard = false` for that dispatch, restored after
(peer mode; a plain link keeps `repl_mvcc = 0`). Evidence: KeyDB propagates only commands that
dirtied the dataset (`server.cpp:4618-4648`) — a failed NX/XX returns before `dirty++`
(`t_string.cpp:104-108`), `MSETNX` returns 0 before `setKey` (`:556-563`) — so every conditional on
the wire already succeeded on its author.
- [ ] **Step 1: Failing tests.** `ClassicReplayTest.ClassicApplyRewrites*` (table-driven, incl. `NX
  KEEPTTL`, `XX KEEPTTL`, `NX GET`, lower case, `MSETNX`; non-SET and unrelated args untouched);
  `ClassicApplyFamilyTest.StaleKeyDbSetLosesToNewerLocalWrite`, `.NxSetAppliesAsGuardedSet`,
  `.MsetnxAppliesAsGuardedMset`, `.PlainApplierRewritesConditionalSet` (a plain link: `SET k v NX`
  over a resident older `k` overwrites it), `.EvalStampedWithEnvelopeMvccUnguardedNoDfatal`
  (`StampOf(key)` equals `{envelope mvcc, author hash}`, no `LOG(DFATAL)`),
  `.EvalAppliesEvenWhenLocalIsNewer`, `.GuardOffAppliesInArrivalOrder`
  (`--multi_master_stream_lww=false`). Pytest: `test_keydb_lww_concurrent_set_converges` (pause the
  KeyDB link with the proxy, write `k` on KeyDB at t1, then write `k` on drakeydb at t2 > t1,
  resume: drakeydb **keeps its newer value**; with `--multi_master_stream_lww=false` it takes
  KeyDB's); `test_keydb_set_nx_wins_over_stale_local` (drakeydb holds an older local `k`; a later
  `SET k v NX` on KeyDB, which succeeded there, must win on drakeydb);
  `test_plain_replica_conditional_set_applies_despite_expiry_skew` (fake master, plain link:
  envelope `SET k old PXAT <far future>`, then envelope `SET k new NX` — the state an active KeyDB
  produces when `k` expired on it, which it never propagates, and was re-created: `k == new`); and
  `test_keydb_writes_not_forwarded_to_peers`: a KeyDB master and **two** `--active_replica
  --multi_master` drakeydb nodes that are each other's peers **and** both attached to the KeyDB;
  after N `INCR`s on KeyDB each node holds exactly N, and `assert_no_command_storm`
  (`multimaster_test.py:1407`) passes. (A plain sub-replica of the first node would make the
  `kSelfIdx` falsification vacuous: it receives the journal either way.)
- [ ] **Step 2: Run, observe failure** (stale KeyDB write overwrites; NX is skipped).
- [ ] **Step 3: Implement.** Rewrite the `RunSquashedMultiCb` comment (`transaction.cc:1628-1648`),
  the `ApplyLwwRewrites` contract comment and `docs/multi-master.md:311-381`: classic links are now
  guarded when they carry a real envelope mvcc; raw classic commands (mvcc 0) stay unguarded and
  `EVAL` is stamped but unguarded.
- [ ] **Step 4: Falsify:** guard bit off (the stale write wins); remove `ClassicApplyRewrites` (the
  NX test fails because NX evaluates against the stale local key); gate the rewrites on
  `LwwGuardActive` (`test_plain_replica_conditional_set_applies_despite_expiry_skew` and
  `.PlainApplierRewritesConditionalSet` fail); keep `KEEPTTL` under `NX` (the table's KEEPTTL rows
  fail); dispatch `EVAL` with the guard on (the tripwire fires) or with `repl_mvcc = 0` (the stamp
  assertion fails); stamp classic authors with `kSelfIdx` (the not-forwarded test sees 2N).
  Restore, record.
- [ ] **Step 5: Peer-mode perf re-run:** parametrize Task 1.5's test over peer mode (per-link
  `repl_offset=` makes lag observable); record the release-build numbers. A failing peer-mode
  **release** bar re-opens Task 1.6.
- [ ] **Step 6:** pre-commit; commit
  `feat: LWW-guard KeyDB writes on classic peer links, rewrite conditionals (P7)`.

**Done:** stale KeyDB writes lose, fresh ones win, "which value survived" is asserted; conditional
commands replay as their effect on plain links too.

### Task 2.5: Close ISSUE-REGISTER D-1

**Files:** `docs/ISSUE-REGISTER.md`, `docs/PLAN.md`.
- [ ] Mark D-1 ("No mvcc half for Redis-protocol / KeyDB peer links") resolved by Tasks 2.1-2.4,
  stating what remains (RDB-aux stamps carry the sender's origin hash; raw Redis links stay
  unstamped). Commit `docs: close D-1 now that KeyDB writes carry real stamps (P7)`.

**Falsification:** docs-only. **Done:** register consistent with the code.

### Task 2.6: `KEYDB.MVCCRESTORE` — applied, LWW-guarded (decision 22)

**Goal:** A KeyDB mesh resync no longer loses keys. An active KeyDB with replicas emits one
`KEYDB.MVCCRESTORE key <mvcc> <expire> <DUMP payload>` per key it loads from an RDB, RREPLAY-wrapped
(`replication.cpp:5573-5594`); the applier translates it to `RESTORE key <ttl> <payload> REPLACE
ABSTTL`, stamps it with the command's **own** `<mvcc>` (the envelope's when that one is unusable)
and the envelope author's hash, and the LWW guard decides it on peer links. Spec D-7a is binding.
**Files:** Modify `src/server/classic_replay.{h,cc}` (`TranslateMvccRestore`, the applier hook,
`ClassicLinkStats::keydb_mvccrestore_failed`), `src/server/replica_types.h`,
`src/server/server_family.cc`, `src/server/multi_master.cc`, `src/server/metrics.cc` (the counter,
rendered like `keydb_cmds_dropped`); Test `src/server/classic_replay_test.cc`,
`tests/dragonfly/keydb_onboarding_test.py`. No edits to `generic_family.cc`.
**Depends on:** Tasks 1.2, 1.3 (applier, counters) and 2.1, 2.2, 2.3, 2.4 (author map, stamps,
dedup, guard).
**Interfaces:** `enum class MvccRestoreParse { kOk, kBadArity, kBadMvcc, kBadExpire, kBadPayload,
kCronPayload }`; `MvccRestoreParse TranslateMvccRestore(cmn::BackedArguments* args, uint64_t*
own_mvcc)` rewrites in place, returns the command's own mvcc through `own_mvcc` (0 when unusable:
0, or bit 63 set — then the envelope's mvcc is used, a clarification of decision 22). The `<expire>`
mapping: negative or `LLONG_MAX` becomes `0`; `0` becomes `1`; anything else is kept (spec D-7a
table). Payload pre-checks mirror what a replicated-apply `RESTORE` accepts: longer than the
10-byte footer (`GetRdbVersion` rejects `size <= 10`), footer version `<= RDB_VERSION`, first byte
in `rdbIsObjectTypeDF` (`rdb_extensions.h:21`); the CRC is not verified. A first byte of 64
(`RDB_TYPE_CRON`, a KeyDB-only value drakeydb cannot represent) is `kCronPayload`: it is counted in
`keydb_cmds_dropped`, **not** `keydb_mvccrestore_failed`. The length and version checks duplicate
what `RESTORE` rejects itself before `OpRestore` (`generic_family.cc:2891-2895`); the type-byte check
is the one that protects a resident key from ISSUE-REGISTER D-31. `GetRdbVersion` is anonymous in
`generic_family.cc`, so reimplement the ten-line length/version check and say why in a comment.

- [ ] **Step 0: Re-probe KeyDB before coding** (the design session did this on 2026-10-03; repeat
  and record in `task-2.6-report.md`): (a) with the Task 0.7 harness, an active KeyDB X with a raw
  replica attached merge-syncs from an active KeyDB Z holding a TTL'd key, a TTL-less key and a
  hash, and the capture shows one `KEYDB.MVCCRESTORE` envelope per key; keep the bytes as a golden
  vector for the gtests; (b) `DUMP` each type from KeyDB and `RESTORE ... REPLACE ABSTTL` it onto a
  drakeydb; (c) `RESTORE` with `ABSTTL` `0`, `1`, `-1`, `9223372036854775807` and read `PTTL`.
- [ ] **Step 1: Failing tests.** `ClassicReplayTest.TranslateMvccRestore*`: `MapsExpire` (`-1`,
  `-7`, `LLONG_MAX` give `0`; `0` gives `1`; an absolute ms deadline is unchanged),
  `ExtractsOwnMvcc` (a valid value; `0`, `1<<63` and all-ones give `*own_mvcc == 0` and still
  `kOk`), `RejectsMalformed` (4 and 6 arguments; `<mvcc>` `-5`, `abc`, overflow; `<expire>`
  `abc`, overflow), `RejectsUnloadablePayload` (empty; shorter than the footer; **exactly 10
  bytes**; footer version `RDB_VERSION + 1`; a first byte of 99 with a valid footer, all
  `kBadPayload`), `ClassifiesCronPayloadAsDrop` (first byte 64 with footer version 9 gives
  `kCronPayload`), `OutputIsByteExact` (binary payload with `\r\n` and NULs, key with a space;
  result is exactly `RESTORE key ttl payload REPLACE ABSTTL`). `ClassicApplyFamilyTest` (the peer
  applier of Task 2.2): `.MvccRestoreStampsOwnMvccWithAuthorHash` (envelope mvcc 900, command
  `<mvcc>` 500: `StampOf(key) == {500, NodeUuidHash(author)}`),
  `.MvccRestoreFallsBackToEnvelopeMvccWhenOwnInvalid` (all-ones `<mvcc>` gives `{900, hash}`),
  `.StaleMvccRestoreLosesToNewerLocalWrite` (local stamp 700 > 500: the local value survives,
  `multimaster_lww_dropped` +1), `.NewerMvccRestoreWinsOverOlderLocalWrite` (800 over 700),
  `.PlainApplierAppliesMvccRestoreVerbatimUnstamped`, `.NoTtlRestoreHasNoTtl` (`INVALID_EXPIRE`
  gives `PTTL == -1`), `.AbsoluteTtlRestoredAsDeadline`,
  `.BadPayloadLeavesResidentKeyCountsAndAdvancesWatermark` (resident `k = good`; a type-99 payload:
  `k == good`, `keydb_mvccrestore_failed == 1`, `keydb_cmds_dropped == 0`, the D-5 watermark
  advanced — the D-31 contract) and `.CronPayloadDroppedCountedAsKeydbCmdsDropped` (a type-64
  payload: `keydb_cmds_dropped == 1`, `keydb_mvccrestore_failed == 0`, `k == good`, watermark
  advanced). Pytest (`keydb` marker): `test_keydb_mvccrestore_from_keydb_mesh_merge_applies` —
  KeyDB A (active) seeded with 2000 `bulk:<i>` strings, `ttl_key` (`PX 3600000`), `plain_key` (no
  TTL), a hash, a list and `stale = "from-A"` (written first); KeyDB B (active, **empty**); an
  `--active_replica` drakeydb D and a plain drakeydb D2 both `REPLICAOF B` and synced; then `sleep
  0.05` and `SET stale newer-on-D` on D; then `REPLICAOF A` on B (a merge full sync: B emits one
  `KEYDB.MVCCRESTORE` per key to D and D2, and since B started empty every key reaching D/D2 came
  through it). Assert on both: all keys equal A's; `PTTL ttl_key` is a real remaining deadline
  (`3_000_000 .. 3_600_000` ms); `PTTL plain_key == -1`; on D `GET stale == "newer-on-D"` and
  `multimaster_lww_dropped >= 1`; on D2 `GET stale == "from-A"` (a plain replica applies verbatim);
  `keydb_mvccrestore_failed == 0`. Record the time from `REPLICAOF A` to full arrival.
  `test_keydb_mvccrestore_unloadable_payload_skipped_and_counted` (fake master, plain link; wrap
  every pytest run in `flock /tmp/drakey-pytest.lock`): `SET k good`, then four scripted
  `KEYDB.MVCCRESTORE` envelopes — a type-64 payload with footer version 9, a footer version
  `0xFFFF`, a four-argument one, and a type-99 payload with a valid footer — then `SET after 1`:
  `k == good`, `after == 1`, `keydb_cmds_dropped == 1` (the type-64 one), `keydb_mvccrestore_failed
  == 3` (the other three), the link stays up, offsets exact.
- [ ] **Step 2: Run, observe failure.** gtest: no symbol. Pytest: today the command reaches the
  unknown-command path, so D and D2 stay empty and `classic_unknown_cmds_dropped` rises.
- [ ] **Step 3: Implement.** In the applier's inner loop, before the `FindCmd == nullptr` path: on
  `KEYDB.MVCCRESTORE` call `TranslateMvccRestore`; a non-`kOk` result bumps
  `keydb_mvccrestore_failed` — or, for `kCronPayload`, `keydb_cmds_dropped` — logs with
  `LOG_EVERY_T(WARNING, 60)` (key and reason), counts the envelope as consumed (`Advance` only) and
  moves on, never dispatching. Otherwise it is a leaf like any dispatched command (the D-5
  reservation): in peer mode set `repl_mvcc = own_mvcc != 0 ? own_mvcc : envelope.mvcc` and the
  author's `repl_origin_idx`, run the usual D-4.3 sequence, dispatch through the link's `ONLY_ERR`
  builder and count an error reply as `keydb_mvccrestore_failed` (not `classic_apply_errors`); a
  plain replica keeps `repl_mvcc = 0`. Then the usual restore of the context. Add
  `keydb_mvccrestore_failed` to `ClassicLinkStats`, `ReplicaSummary`, the plain-replica INFO block,
  the peer line and the Prometheus mirror; `IsKeyDbOnlyCommand` stays unchanged.
- [ ] **Step 4: Run; falsify**, one at a time, restore and record verbatim: (a) stamp with
  `envelope.mvcc`: `MvccRestoreStampsOwnMvccWithAuthorHash` and the pytest `stale` assertion on D
  fail (D takes "from-A"); (b) drop the `INVALID_EXPIRE` mapping: `MapsExpire`,
  `NoTtlRestoreHasNoTtl` and the pytest `plain_key` assertion fail (`PTTL` near 268435454996); (c)
  drop the type-byte pre-check: `BadPayloadLeavesResidentKey...` fails with `k` gone (D-31); (c2)
  classify type 64 as a failure: `CronPayloadDroppedCountedAsKeydbCmdsDropped` fails; (d) clear the
  link's `repl_lww_guard`: `StaleMvccRestoreLosesToNewerLocalWrite` fails; (e) skip the hook so the
  command falls to the unknown path: both pytest and the apply gtests fail; (f) do not bump the
  counters: the counter assertions fail. Record what each test would still pass under without the
  feature: the fake-master test's `k == good` and `after == 1` pass vacuously, so its counter
  assertions are the load-bearing ones.
- [ ] **Step 5:** `ninja -j4 classic_replay_test dragonfly`; both gtest and pytest suites; if the
  recorded resync time falls behind the Task 1.5 release bar, open Task 1.6; pre-commit; commit
  `feat: apply KEYDB.MVCCRESTORE with LWW on classic peer links (P7)`.

**Done:** every key of a KeyDB mesh merge resync reaches D and D2 with the right TTL, a stale
restore loses to a newer local write on the peer and not on the plain replica, a bad payload never
erases a resident key, a cron payload is a counted drop, and every rejection is counted.

### Task 2.7: Clock-skew estimate on classic links

**Goal:** A classic link reports a clock-skew estimate although KeyDB's `REPLCONF UUID` reply is a
bare `+<uuid>` with no clock (spec D-13, D-1.17), so INFO `clock_skew_ms` and the
`IsClockSkewConcerning` warning work for KeyDB peers too.
**Files:** Modify `src/server/classic_replay.{h,cc}`, `src/server/replica.{h,cc}`; Test
`src/server/classic_replay_test.cc`, `tests/dragonfly/keydb_onboarding_test.py`.
**Interfaces:** the applier samples, for a **depth-1** envelope with `mvcc != 0`, `(mvcc >> 20) -
now_ms` (KeyDB's mvcc is `ms << 20 | counter`, `MVCC_MS_SHIFT 20`, `server.h:960`), at most once a
second; the link keeps the maximum of the last 60 samples (the least-aged one: each sample is a
lower bound on the true skew) in the existing `clock_skew_ms_` atomic and warns through the
existing `IsClockSkewConcerning` path (`LOG_EVERY_T`). A DFLY peer's handshake echo is untouched.
- [ ] **Step 1: Failing tests.** `ClassicApplyFamilyTest.SkewSampleUsesMvccMilliseconds` (an
  envelope whose mvcc encodes `now + 5000 ms`: the estimate is within a small tolerance of +5000,
  and 0 before any envelope), `.SkewSampleIgnoresNestedAndZeroMvcc`, `.SkewEstimateIsMaxOfWindow`.
  Pytest `test_keydb_clock_skew_estimate_from_envelopes`: fake master, plain link — an envelope
  minted 60 s ahead: `clock_skew_ms` is about +60000 and the skew warning is logged
  (`find_in_logs`); real KeyDB on the same host: after some writes `-2000 < clock_skew_ms <= 100`.
- [ ] **Step 2: Run, observe failure** (the field stays 0). **Step 3: Implement.**
- [ ] **Step 4: Falsify:** read `mvcc` instead of `mvcc >> 20` (a skew about a million times too
  large: the tolerance assertion fails); sample nested envelopes (`SkewSampleIgnoresNested...`
  fails). Restore, record.
- [ ] **Step 5:** pre-commit; commit
  `feat: estimate a KeyDB peer's clock skew from RREPLAY envelope stamps (P7)`.

**Done:** `clock_skew_ms` is meaningful on a KeyDB link; a DFLY peer's value is unchanged.

### P7-2 gate and PR

- [ ] Whole-branch review, adversarial pass (briefed only to refute the no-forward and convergence
  claims and the dedup reservation's exactly-once claim), fix loops. Gate and PR as in P7-0.

---

## P7-3 `feat/phase7-3-classic-partial-psync`

### Task 3.1: `--classic_partial_psync`

**Goal:** A classic link resumes with `PSYNC <replid> <offset+1>` and `+CONTINUE` instead of a full
resync, without loss or duplication.
**Files:** Modify `src/server/classic_replay.{h,cc}` (flag, counters),
`src/server/replica.{h,cc}`, `src/server/server_family.cc`/`multi_master.cc` (INFO), `metrics.cc`;
Test `src/server/classic_replay_test.cc`, `tests/dragonfly/keydb_onboarding_test.py`,
`tests/dragonfly/replication_test.py` (regression only).
**Interfaces:** `Replica::classic_stable_reached_` (set when `InitiatePSync` **returns success** —
the loader finished, the EOF token / `$<len>` tail validated and the bytes behind the RDB handed
over (spec D-10) — or on `+CONTINUE`; **cleared where `ParseReplicationHeader` parses
`+FULLRESYNC`**, `replica.cc:1880-1889`); the `Replica` stream-prefix buffer **from Task 0.6**
(filled by `InitiatePSync`, drained into `ConsumeRedisStream`'s `io_buf`), to which this task adds
only the `+CONTINUE` producer; counters `classic_psync_partial_ok`,
`classic_psync_partial_fallback`.
- [ ] **Step 1: Failing tests** against `redis_server` (7.0.15), real KeyDB and the fake master,
  each asserting master `sync_partial_ok`/`sync_full`/`sync_partial_err` (`server.cpp:6017-6019` for
  KeyDB) and **exact** value counts: (a) leftover after `CONTINUE` — fake master sends
  `+CONTINUE <id>\r\n` and stream bytes in **one** write, count exact; also a real-Redis variant
  with a backlog written while the link is down (repeat x10); (b) off-by-one — INCR writer,
  `Proxy.drop_connection()`, `sync_partial_ok == 1`, `sync_full == 1`, `sync_partial_err == 0`,
  counter == N; (c) deferred MULTI/EXEC bytes — transactions on the master across the drop; (d)
  envelope bytes — KeyDB active variant of (b); (e) parser `INPUT_PENDING` across reads — values
  larger than the read buffer straddling the drop; (f) mid-load drop — large dataset, drop during
  `master_sync_in_progress:1`: `sync_full == 2`, `sync_partial_ok == 0`, dataset equal; (g)
  malformed `+FULLRESYNC` second line via the fake master then a reconnect: the next request is a
  full `PSYNC ? -1`/`<id> -1`, never a stale offset; plus new-replid `+CONTINUE <newid>` adoption,
  `--classic_partial_psync=false` (each reconnect is a full resync) and
  `classic_psync_partial_fallback` when FULLRESYNC answers a partial request.
- [ ] **Step 2: Run, observe failure** (today every reconnect sends `-1`).
- [ ] **Step 3: Implement** spec D-8: request `offs = (flag && !master_repl_id.empty() &&
  classic_stable_reached_) ? repl_offs_ + 1 : -1`; `+CONTINUE [<newid>]` consumes its line, adopts
  `<newid>`, keeps `repl_offs_`, skips LOADING/loader/flush/merge, sets `R_SYNC_OK`; put the bytes
  behind the `+CONTINUE` line into the Task 0.6 stream-prefix buffer; verify `REPLCONF ACK 0` after
  a partial is harmless against both masters (else send the real offset).
- [ ] **Step 4: Run; falsify** each hazard separately and record: use a fresh `io_buf` (a); send
  `repl_offs_` instead of `+ 1` (b); count deferred MULTI/EXEC bytes early (c); do not clear
  `classic_stable_reached_` at the FULLRESYNC parse (g); ignore the flag (flag-off test).
  Run the leftover and offset tests x10 for a pass rate.
- [ ] **Step 5:** `replication_test.py` unchanged-green; pre-commit; commit
  `feat: resume classic replication links with a partial PSYNC (P7)`.

**Done:** exact counts across drops on Redis and KeyDB; hazards each caught by a named test.

### Task 3.2: Peer-mode partial resync skips the merge load

**Goal:** A peer link's partial resync does no merge load and no tombstone churn.
**Files:** Test `tests/dragonfly/keydb_onboarding_test.py`, `src/server/classic_replay_test.cc`;
`src/server/replica.cc` only if Task 3.1 left a merge call on the `+CONTINUE` path.
- [ ] **Step 1: Failing test** `test_peer_partial_resync_does_not_merge`: `--active_replica`
  drakeydb on KeyDB, delete a key locally (tombstone), drop and resume:
  `classic_psync_partial_ok == 1`, the `Peer full sync: merging` log line absent, `mvcc_tombstones`
  unchanged, writes continue.
- [ ] **Step 2-4:** run, fix if needed, falsify by routing `+CONTINUE` through the merge load.
- [ ] **Step 5:** pre-commit; commit
  `test: pin that a peer partial resync skips the merge load (P7)`.

**Done:** no merge load on partial resync.

### P7-3 gate and PR

- [ ] Whole-branch review, adversarial pass (briefed on the leftover hand-off), fix loops. Gate and
  PR as in P7-0, **plus** `replication_test.py`; the hazard tests run x10.

---

## P7-4 `feat/phase7-4-keydb-rdb-extras-docs-exit`

### Task 4.1: RDB type 64 (`KEYDB.CRON`) parse-skip

**Goal:** A KeyDB full sync that contains cron jobs loads instead of failing.
**Files:** Modify `src/server/rdb_load.{h,cc}`, `src/server/classic_replay.{h,cc}` (counter); Test
`src/server/rdb_test.cc`, `tests/dragonfly/keydb_onboarding_test.py`.
- [ ] **Step 1: Failing tests.** `RdbKeyDbTest` is a fixture that runs **under `--active_replica`**:
  `SetMvcc` no-ops without it (`db.mvcc` is null), so every stamp assertion would pass vacuously on
  a non-active node. Each test first proves stamps are observable: a control key loaded with its
  own `mvcc-tstamp` aux reads back that stamp through `StampOf`.
  `RdbKeyDbTest.CronTypeSkippedAndNextKeyUnstamped`: a synthetic RDB (built in the test) with `AUX
  mvcc-tstamp`, type 64 (key, script, two 8-byte LE ms, counted keys, counted args) then a normal
  string key with **no** aux: the load succeeds, the cron key is absent, the string key is present
  and **not** stamped with the cron's mvcc (loaded with a **nonzero** `SetLoadOriginHash`: the aux
  branch ignores the stamp when `load_origin_hash_ == 0`, `rdb_load.cc:3196`, which would pass
  vacuously). Pytest `test_keydb_full_sync_with_cron_and_member_ttl` (a `KEYDB.CRON` job and a TTL'd
  set member on KeyDB: drakeydb syncs, normal keys intact; and — where the process-wide counters
  render on a **non-active** node — `multimaster_keydb_rdb_cron_skipped:1` in the plain replica's
  `INFO replication` and `multimaster_keydb_rdb_cron_skipped_total` in `/metrics`, per spec D-13).
- [ ] **Step 2: Run, observe failure:** `Unrecognized rdb object type: 64` (`rdb_load.cc:2704`).
- [ ] **Step 3: Implement** before the `rdbIsObjectTypeDF` check (`:2703`): skip the body, count
  `multimaster_keydb_rdb_cron_skipped`, rate-limited rollup warning, `settings.Reset()`, `continue`.
- [ ] **Step 4: Falsify:** drop `settings.Reset()`: the next key inherits the cron's stamp and the
  unstamped assertion fails. Restore, record.
- [ ] **Step 5:** pre-commit; commit `feat: skip KeyDB cron jobs in the RDB loader (P7)`.

### Task 4.2: Member-TTL aux, aux noise, bit-63 throttle

**Files:** Modify `src/server/rdb_load.cc` (`HandleAux`, `:3054-3219`),
`src/server/classic_replay.{h,cc}`, `src/server/server_family.cc`, `src/server/metrics.cc` (the
loader counters, rendered by spec D-13's rule); Test `src/server/rdb_test.cc` (`RdbKeyDbTest`,
**under `--active_replica`**, each test starting with a control key that proves stamps are
observable), `tests/dragonfly/keydb_onboarding_test.py`.
- [ ] **Step 1: Failing tests.** `RdbKeyDbTest.SubexpireAuxSkippedSilentlyAndCounted` (pairs
  `keydb-subexpire-key`/`-when` after a key: no per-aux warning, counter moves once per `-when`,
  `settings` untouched, one rollup); `.UnknownAuxWarnedOncePerNamePerLoader`;
  `.ReplMastersAuxRecognized`; `.MvccInvalidThrottledAndCounted` (1000 keys with the all-ones
  sentinel: counter 1000, log lines bounded). Pytest: `test_keydb_full_sync_with_cron_and_member_ttl`
  also asserts `multimaster_keydb_rdb_subexpire_dropped` in INFO and `/metrics` on the plain
  replica (loader counters can be nonzero when onboarding from a **non-active** KeyDB).
- [ ] **Step 2-3:** run, implement (counters `multimaster_keydb_rdb_subexpire_dropped`,
  `multimaster_keydb_rdb_mvcc_invalid`; the bit-63 warning moves behind `LOG_EVERY_T`; render all
  three loader counters per spec D-13: active-node block, plain-replica block, both Prometheus
  branches).
- [ ] **Step 4: Falsify:** unthrottled warning (log-count assertion fails); count at `-key` instead
  of `-when` (the count assertion fails); skip the plain-replica INFO render (the pytest fails).
  Restore, record.
- [ ] **Step 5:** pre-commit; commit `feat: quiet and count KeyDB-only RDB aux fields (P7)`.

### Task 4.3: D-8 precedence test

**Goal:** Pin that opcode 221 beats a KeyDB `mvcc-tstamp` aux for the same key (ISSUE-REGISTER D-8).
**Files:** Test `src/server/rdb_test.cc` (the `RdbKeyDbTest` fixture, **under `--active_replica`**).
- [ ] **Step 1:** `RdbKeyDbTest.Opcode221BeatsMvccTstampAuxForSameKey`, run under
  `--active_replica` with a nonzero `SetLoadOriginHash` (without them the stamp comes from opcode
  221 alone and the test would pass vacuously): aux `mvcc-tstamp` = A, then opcode 221 = B, then
  the key: the stored stamp is B; and the reverse stream order (221 = B, then aux = A) proves
  last-in-stream-wins: the stored stamp is A.
- [ ] **Step 2-3:** pass as-is (behaviour exists; the test is the deliverable).
- [ ] **Step 4: Falsify** by flipping the **precedence**: make the aux branch first-wins (set the
  stamp only when none is set — `rdb_load.cc:3206` overwrites unconditionally today, so "letting
  the aux branch overwrite" would change nothing and pass vacuously) or make opcode 221 not
  overwrite an already-set stamp: the first order stores A and the second B, so the test fails.
  Restore.
- [ ] **Step 5:** pre-commit; commit `test: pin that opcode 221 wins over a KeyDB mvcc aux (P7)`.

### Task 4.4: Operator docs and registers

**Files:** `docs/multi-master.md`, `docs/differences.md`, `docs/UPSTREAM-SYNC.md`,
`docs/ISSUE-REGISTER.md`, `docs/PLAN.md`.
- [ ] `docs/multi-master.md`: new "Onboarding from KeyDB" section — topology (every drakeydb node
  attaches to KeyDB directly; no-forward kept); the KeyDB directive `multi-master-no-forward yes`
  **only when every drakeydb node is `--active_replica --multi_master` and attached to every KeyDB
  master** (a plain replica attaches to one master only; KeyDB itself warns that the directive
  needs a mesh or data is lost, `config.cpp:2705-2710`, and skips the forward at
  `replication.cpp:5507`; with a single KeyDB master there is nothing to forward), otherwise
  forwarding stays on and the dedup absorbs the duplicates; `repl-backlog-size` sizing for partial
  resync, cutover with `REPLICAOF REMOVE`, expiry semantics (decision 13, `--replica_delete_expired`
  note; replica active expiry is for the main `replica_` link only), member-TTL and cron loss,
  `KEYDB.MVCCRESTORE` applied (and `keydb_mvccrestore_failed`), the counters and where they render;
  the unsupported or by-design combinations — the dedup is reset by a flushing full sync (so a
  cluster `ADDREPLICAOF` of a forwarding mesh is unsupported) and not by an operator `FLUSHALL`
  (KeyDB parity), mvcc-0 envelopes are not deduped (v6.3.4 never sends one), a KeyDB restart is a
  new author and costs one registry index (`--classic_author_cap`, `classic_link_uuid_changes`,
  `multimaster_peer_registry_size`); rewrite `:87-122` (classic peers now carry real stamps when
  KeyDB is active) and confirm `:311-381` matches Task 2.4.
- [ ] `docs/differences.md` entry; `docs/UPSTREAM-SYNC.md` watchlist rows for the newly touched
  files (`replica.{h,cc}`, `engine_shard.{h,cc}`, `db_slice.cc`, `rdb_load.cc`, `metrics.cc`) and
  the "(once it exists)" fix if still present.
- [ ] `docs/ISSUE-REGISTER.md`: close D-8 (Task 4.3); confirm U-9/D-1 are closed; confirm the
  orchestrator registered the full-sync-tail U-item (Task 0.6) and the `MarkForClose` null-`conn()`
  finding (Task 1.2); add the member-TTL-conversion follow-up; update D-5's byte-identity list to
  the spec's exceptions.
- [ ] Commit `docs: document KeyDB onboarding, its limits, and the byte-identity exceptions (P7)`.

**Falsification:** docs-only; every command and flag quoted is run once and the output recorded.

### Task 4.5: Phase exit gate

**Files:** `docs/PLAN.md`, ledger.
- [ ] `ninja -C /home/user/drakeydb/build-dbg -j4` warning-free; full
  `cd /home/user/drakeydb/build-dbg && ctest -V -L DFLY`.
- [ ] Pytest with `KEYDB_REQUIRED=1`: `multimaster_test.py`, `multimaster_merge_test.py`,
  `keydb_onboarding_test.py` (incl. `slow`), `keydb_harness_test.py`, `redis_replication_test.py`,
  `replication_test.py`, `replication_specific_test.py`, `replication_resilience_test.py`,
  `replication_config_test.py`, `cluster_test.py::test_cluster_migrations_sequence`; 0 failures;
  flake triage recorded; the timing-sensitive KeyDB tests x10.
- [ ] The D-12 **release-build** measurement is re-run on the final tree (`DRAKEYDB_PERF=1`, plain
  and peer mode, x3 median, with the KeyDB-replica comparator); the numbers go into the ledger and
  `docs/PLAN.md`.
- [ ] The P3 golden-buffer journal test passes with `--active_replica` off; an `--active_replica`
  off save emits neither opcode 221 nor 225; no `kDrakeydbReplVersion` change.
- [ ] Every falsification recorded verbatim in the ledger; `pre-commit run --all-files` clean across
  the branch; upstream `ci.yml` and `drakeydb-ci.yml` green.
- [ ] `docs/PLAN.md` status: P7 complete with the counts; next = the upstream-sync PR, then the
  tombstone-lifecycle phase. Commit `docs: record Phase 7 complete and its exit gate (P7)`.

**Done:** gate green; PLAN.md updated; PR opened, watched, mergeable.
