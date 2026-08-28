# drakeydb Master Plan

> **Continue here.** This is the approved, living plan and canonical copy for the drakeydb fork.
> Update the status block below as phases land.

## Current status (last updated 2026-08-25)

| Phase | Status | Where |
|---|---|---|
| **P0 — Repo hygiene + rename** | ✅ **complete, verified** | PR [#1](https://github.com/darkspadez/drakeydb/pull/1), branch `feat/phase0-drakeydb-rename`, commit `7ca99f4c` |
| **P1 — Identity foundations** | ✅ **complete, verified** | branch `feat/phase1-identity`, commits `9e653ab3..9c491fac` |
| **P2 — Writable multi-source replica** | ✅ **complete, verified** | PR [#3](https://github.com/darkspadez/drakeydb/pull/3) **merged** as `cd8e0602` — see [Phase 2](#phase-2) |
| **P3 — Origin-tagged journal + active pair/mesh** | ✅ **complete, verified** | PR [#4](https://github.com/darkspadez/drakeydb/pull/4), branch `feat/phase3-origin-journal`, 30 commits `6b0c995a..7bddd7fe` — see [Phase 3](#phase-3) |
| **P4 — MVCC store + stamping + wire** | ⏭️ **next up** | see [Phase 4](#phase-4--mvcc-store--stamping--wire) |
| P5–P9 | not started | — |

**P0 verification record** (Ubuntu 24.04 arm64 container, OrbStack): debug build produces
`build-dbg/drakeydb` + `dragonfly` compat symlink; `--version` → `drakeydb dev-…`; live server
answers PING/SET/GET and INFO still reports `dragonfly_version`/`redis_version` (compat intact);
`journal_test` 8/8; `pymemcached_test.py::TestMemcached::test_basic` passes via the harness's
default binary path; pre-commit clean; dry-run `git merge upstream/main` conflict-free.

**P1 verification record** (Ubuntu 24.04 arm64 container, OrbStack, `--security-opt
seccomp=unconfined` so io_uring is exercised rather than the epoll fallback):
full `ctest -L DFLY` **86/86 passed, 0 failed** (983 s) after a complete `ninja` build — note
`check_dfly` builds only a subset, so a bare `ctest -L DFLY` after it reports unbuilt binaries as
"Not Run"; `multi_master_test` 20 passed + 1 skip (`UnwritableDirIsEphemeral` self-skips as root,
and was separately proven to execute and pass under a non-root user); `multimaster_test.py`
9 passed + 1 skip; full `replication_test.py` **43 passed, 0 failed** (334 s) with **no flakes, so
no triage was required**; `cluster_test.py::test_cluster_migrations_sequence` passed (run
explicitly because the `slaveN` INFO change is the one most exposed to its lag parser);
pre-commit clean across all 18 changed files.

**Known P1 coverage gap:** `multimaster_test.py::test_replicaof_real_redis_tolerates_missing_uuid`
**skips locally** — the harness's `RedisServer` looks for version-suffixed binaries
(`redis-server-7.2.2` / `redis-server-6.2.11` / `valkey-server-8.0.1`, `tests/dragonfly/instance.py`),
which this container lacks. The proxy-backed reconnect regression now automates both `-ERR` and
exact `+OK` tolerance, including stale-identity clearing, but it does not replace the real-binary
interoperability check. That case was hand-validated against a real unmodified
`redis-server 7.0.15` (full sync succeeded, `master_node_uuid` absent throughout, expected WARNING
logged, replication continued), but **watch CI for it**.

**P3 prerequisites recorded during P1** (neither is a P1 defect; both bite later):
1. **Per-instance test identity.** The pytest harness's dragonfly cwd is *session*-scoped
   (`conftest.py` `determine_scope` returns `"session"`), and `create()` never defaults `--dir`, so
   every instance without an explicit `dir=` now shares one `drakeydb.uuid` and comes up with an
   **identical `node_uuid`**. Inert while nothing consumes the uuid; from P3 on, self-origin drop
   keys on exactly this identity, so a mesh test would see every node claiming to be every other
   node and the failure would look like a protocol bug. Give each `df_factory` instance a distinct
   identity by default (per-instance `dir=`, or a generated `--node_uuid`) before P3 lands.
   **Done in P2:** `tests/dragonfly/instance.py`'s `DflyInstanceFactory.create()` now injects a
   random `--node_uuid` per instance whenever `"dir"` is absent from its args (version-gated).
2. **Duplicate-identity detection.** If a peer presents our own uuid (cloned VM image, copied
   `drakeydb.uuid`), `PeerRegistry::AddOrGet` returns `kSelfIdx` and P3 would drop those entries as
   self-originated — silent data loss with no diagnostic. KeyDB checks for this during the
   handshake. P1 deliberately does not, to keep scope tight.
   **Done in P2, peer-mode only:** `Replica::Greet()` (`src/server/replica.cc`) refuses a peer that
   presents our own node uuid before registering it. A non-active or non-peer clone (e.g. a stock
   `REPLICAOF`) is still undetected — that stays out of scope beyond active-replica peer mode.

**P1 note for P2/P3:** `MasterContext::master_node_uuid` and `master_clock_ms` are cleared before
each UUID exchange, so reconnecting to a build that lacks the exchange cannot leave stale INFO
data; `Replica::clock_skew_ms_` (P4-0) is reset alongside them for the same reason. P4-0 reads
`master_clock_ms` for its clock-skew warning/metric (`ComputeClockSkewMs`,
`src/server/multi_master.h`, surfaced via `Replica::GetSummary()`); it was otherwise unread
through P3. Peer registration
is deliberately deferred until a later phase defines trusted peer admission: `REPLCONF` and sync
session creation are both reachable without authentication when `requirepass` is unset, while
`PeerRegistry` has no reclamation API. It remains initialized with only the local node in P1, so
untrusted clients cannot grow its process-lifetime storage.

**P1 KeyDB interop is one-directional:** inbound (drakeydb replica ← KeyDB master) is the claimed
scope and works — our parser accepts KeyDB's bare-uuid reply, and `IsValidNodeUuid` matches KeyDB's
`strlen != 36` + `uuid_parse` check exactly. Outbound (KeyDB replica ← drakeydb master) does
**not**: real KeyDB validates our `REPLCONF UUID` reply with an exact-length check
(`KeyDB/src/replication.cpp:3654-3657`), and drakeydb's `+<36-char uuid> <13-digit ms>` reply
(51 chars) fails it outright. This is scope, not a bug — the ms suffix was a deliberate P1 choice —
but costly to revisit later: the exchange predates `capa dragonfly`/`CLIENT-VERSION`, so there is no
peer signal yet to gate the suffix on, and P8's clock-skew work will come to depend on it.

**P2 verification record** (Ubuntu 24.04 arm64 container `drakeydb-p2` from the re-committed
`drakeydb-build:deps-p2` image, OrbStack, `--security-opt seccomp=unconfined`): full `ninja` (273
targets) warning-free; `ctest -L DFLY` **87/87 passed** (P1's 86 + `peer_replication_test`; 272 s,
re-run after the final-review fix wave on `84bdfc7b`: 87/87, 265 s); pytest with an absolute
`DRAGONFLY_PATH`: `multimaster_test.py` **21 passed** + 1 pre-existing skip (real-redis binary);
`replication_test.py` **43 passed** (21 `large` deselected); `replication_config_test.py` **18 passed**
+ 6 skipped (versioned redis binaries absent); `replication_resilience_test.py` **41 passed** +
1 pre-existing xfail (8 deselected); `cluster_test.py` **55 passed** + 2 skipped (9 deselected);
`replication_specific_test.py` **60 passed** (13 deselected) — 0 failures, no flakes; the three
replication/multimaster suites were re-run green on the final HEAD. Host pre-commit clean over all
changed files. Review: every task had an Opus spec+quality review (two fix rounds: SyncGate
notify-under-mutex + bounded test waits; PeerReplicationManager stored endpoints + clang
thread-safety annotation), plus a final whole-branch review whose four Important findings
(`Summaries()` vs `Stop()` serialization, `--replicaof` lists requiring `--multi_master`, member
destruction order, stale flag docs) landed in `761f9ac6..84bdfc7b` and were re-reviewed clean.

**Resuming work — environment notes (macOS dev machine):**
- Container runtime is **OrbStack**: `orbctl start` if the docker daemon is down. VM is
  linux/arm64 with 8 GB RAM / 12 CPUs.
- Build container: `docker run -d --name drakeydb-build --security-opt seccomp=unconfined
  -v "$PWD":/src -w /src ubuntu:24.04 sleep infinity` + apt packages from
  `docs/build-from-source.md` (add `binutils` for pytest teardown symbolization, plus
  `redis-tools redis-server python3-venv` for smoke and interop tests). Then
  `./helio/blaze.sh -DWITH_AWS=OFF -DWITH_GCP=OFF && cd build-dbg && ninja -j4 dragonfly`.
  **Use `-j4`** — default 12-way parallelism OOM-kills `cc1plus` in the 8 GB VM.
  **`--security-opt seccomp=unconfined` is required**: without it Docker's default seccomp profile
  blocks the io_uring syscalls, `UringProactor::Init()` aborts, and every proactor-based test
  (including all `BaseFamilyTest` fixtures) dies unless you pass `--force_epoll` everywhere —
  which also means you stop exercising the io_uring path CI uses. A long-lived container plus
  `docker exec` avoids reinstalling apt packages on every run; `docker commit` it before recreating
  so the deps and the pytest venv survive.
- **P2 container note:** the committed `drakeydb-build:deps` image lacked `patch`, `make`,
  `automake`, `autoconf-archive` (helio now applies abseil patches at configure time, and the
  third-party configure steps need make/autotools); install them via apt or use the re-committed
  image `drakeydb-build:deps-p2`. The P2 container is `drakeydb-p2`, mounting the worktree at
  `/src`.
- pytest deps: `python3 -m venv /tmp/tv && /tmp/tv/bin/pip install -r tests/dragonfly/requirements.txt`.
- **`DRAGONFLY_PATH` must be absolute** (e.g. `/src/build-dbg/dragonfly`). A relative path fails for
  every test in this harness, because `instance.py` spawns the binary with `cwd` set to the
  harness's temp directory rather than the repo root.
- Full C++ gate: run a complete `ninja -j4` **before** `ctest -L DFLY`. The `check_dfly` target
  builds and runs only a subset, so `ctest` afterwards reports the unbuilt binaries as "Not Run" —
  which is easy to misread as failures.
- Host has no pre-commit/pipx: `python3 -m venv <dir> && <dir>/bin/pip install pre-commit`,
  then `pre-commit run --files <changed files>`.
- Remotes: `origin` = `git@github.com:darkspadez/drakeydb.git`, `upstream` =
  `https://github.com/dragonflydb/dragonfly`. See `docs/UPSTREAM-SYNC.md` before merging upstream.

---

## Context

This repo began as a pristine clone of `dragonflydb/dragonfly` with a full KeyDB clone sitting
untracked in `KeyDB/` (local reference only, gitignored). The goals:

1. **Rebrand** the fork as **drakeydb** (branding + binary name; internals stay `dfly` to keep
   upstream merges cheap).
2. **Port KeyDB's active-replica / multi-master replication** into Dragonfly's shared-nothing,
   journal-based architecture — the first of possibly more KeyDB features.
3. **Stay continuously mergeable** with upstream dragonflydb patches.

### Decisions locked with the user

| Decision | Choice |
|---|---|
| Rename depth | Branding + binary/docker/helm named `drakeydb`; internal namespaces, `DFLY` wire commands, file paths, Prometheus metric names, INFO fields stay Dragonfly |
| Milestone target | Full N-master mesh (every active node `REPLICAOF` every other; 2-node pair is the same mechanism) |
| KeyDB interop | **One-way onboarding only**: drakeydb consumes from live KeyDB masters (PSYNC + RREPLAY stream). Serving replication *to* KeyDB replicas is out of scope |
| Conflict resolution | **Strictly convergent per-key LWW** for state-carrying commands on both streaming and full-sync merge (see semantics caveats) |
| KeyDB/ folder | Keep on disk as reference, add to `.gitignore` |
| Distribution | Public fork on GitHub — license/NOTICE hygiene required |
| Dev environment | Docker/OrbStack containers on macOS (Dragonfly is Linux-first) |

### Licensing (flag, not blocker)

- Dragonfly is **BSL 1.1**; see [LICENSE.md](../LICENSE.md) for the complete terms. The license
  changes to Apache 2.0 on Nov 1, 2030, or on the fourth anniversary of the first public
  distribution of a specific version, whichever comes first. Before that version's applicable
  change date, the Additional Use Grant permits use only as part of your own product or service
  when it is not an in-memory data store product or service, and prohibits using, providing,
  distributing, or making the Licensed Work available as a Service. A Service is a commercial
  offering, product, hosted, or managed service that lets third parties access the work or a
  substantial set of its features through SaaS, PaaS, IaaS, or similar services that compete with
  the Licensor's products or services. Obtain legal review before commercialization.
- KeyDB is BSD-3 (`KeyDB/COPYING`) — ported logic is attributed in `NOTICE`.
- Never touch the 468 per-file `// Copyright DragonflyDB authors` headers.

### Key verified architecture facts this plan builds on

- **Upstream already ships cascaded replication** (`--experimental_cascaded_partial_sync`,
  `src/server/replica.cc:66`): replicas re-journal applied writes through normal dispatch and
  serve sub-replicas. Half the active-active plumbing exists and is tested.
- Replica-applied writes go through **full normal command dispatch** (`journal/executor.cc:71-74`
  → `Service::DispatchCommand`); re-journaling's only gate is `shard->journal()`
  (`transaction.cc:1635`). **The journal has no origin identity** (`journal/types.h:20-26`) —
  that's the A↔B echo hazard.
- Journal COMMAND entries carry a **deprecated varint** written as `1`, parsed and discarded by
  readers (`journal/serializer.cc:78`, `:228-230`) — a compatible extension slot.
- Entries are **serialized once per shard**, same string broadcast to all consumers
  (`journal_slice.cc:120-147`); per-consumer filtering lives in `JournalStreamer::ShouldWrite`.
- Read-only enforcement is one check (`main_service.cc:1446`) plus replica data-plane gates
  (`engine_shard.cc:846`, `db_slice.cc:815/827/1473`).
- Fan-in precedent exists: `ADDREPLICAOF` + `cluster_replicas_` (`server_family.cc:3330-3358`,
  `server_family.h:376`).
- Full sync is destructive (`FlushAll` at `replica.cc:654-670`); `RdbLoader::SetOverrideExistingKeys`
  exists (`rdb_load.h:302`); the unconditional write site for an LWW hook is `rdb_load.cc:3238`.
- `sizeof(CompactObj)==18` is a hard static_assert; per-key metadata goes in a **side DashTable**
  following the `mcflag` precedent (`table.h:128`).
- KeyDB mechanisms to reproduce (from `KeyDB/src`): node UUID + `REPLCONF uuid` exchange +
  reciprocal-connect tiebreak (`replication.cpp:1557-1600`); `RREPLAY <origin-uuid> <cmd> [db]
  [mvcc]` envelope with self-echo drop (`:5438`); MVCC stamp = `ms << 20 | 20-bit counter`
  (`server.cpp:7263-7287`); merge-not-flush full sync with per-key LWW (`db.cpp:376-397`);
  per-key RDB aux `mvcc-tstamp` (`rdb.cpp:1164-1168`); local expiry with DEL propagation
  suppressed (`db.cpp:1988-1990`), gated on `capa activeExpire` (`replication.cpp:1798`);
  FAILOVER rejected in active mode.
- KeyDB things NOT ported: dead `mvccLastSync`/`staleKeyMap` machinery (verified never fires);
  KeyDB's **reciprocal-connect tiebreak is also dead code** (`replication.cpp:1575-1588`): it
  guards on `FSameUuidNoNil` (true only when the two uuids are equal, `:92-98`) and then
  `memcmp`s those same two buffers, so the result is always 0 and `freeClientAsync` never
  fires — the comparison was meant to be against `cserver.uuid`. P3 ports the intent, fixed;
  arrival-order-only streaming (we add an LWW guard); per-boot UUID regeneration (we persist);
  `processReplconfUuid`'s reciprocal-connect tiebreak (`replication.cpp:1557-1599`) is also dead
  code -- it guards on `FSameUuidNoNil(mi->master_uuid, c->uuid)`, true only when the two uuids
  are *equal* (`replication.cpp:92-98`), so the following `memcmp(mi->master_uuid, c->uuid, ...)
  < 0` compares a value with itself and is always `0`; `freeClientAsync` never fires. D-7 ports
  the *intent* (self uuid vs. peer uuid), not the code: `ShouldRefuseReciprocalPeer`
  (`peer_replication.{h,cc}`) plus `PeerReplicationManager::HasUnestablishedPeerWithUuid`, called
  from the admission check in `server_family.cc`'s `ReplConf`.
- Auto-update pings dragonflydb.io on `v`-prefixed tags (`dfly_main.cc:481`); `DflyVersion`
  (`version.h`, VER6) is the upstream replication protocol version — leave to upstream.

---

## Design overview (multi-master)

Governing choices:

1. **An active node is a master that also consumes.** `ServerState::is_master` stays `true`,
   `EngineShard::is_replica_` stays `false`. Peer-mode `Replica` instances never flip those flags
   → **zero edits** to the READONLY check, WAIT, expiry/eviction gates, `DEBUG LOAD`.
   Expiry/eviction run locally like any master (KeyDB semantics).
2. **Mesh-only, no-forward v1.** Peer consumers receive **only locally-originated entries**
   (send-side origin filter); an N-node mesh is N×(N−1) independent one-way streams — no echo
   possible, no dedup or RREPLAY-nesting port needed. Plain (read-only) replicas of an active
   node receive the full stream (all origins + expiry DELs). Ring/tree forwarding topologies are v2.
3. **Journal framing v2, active-mode only.** The deprecated varint becomes a header: non-active
   nodes still write `1` (byte-identical to upstream); active nodes write `2` followed by
   `{varint origin_idx, varint mvcc, varint flags}` (flags bit0 = expiry-DEL). Active masters
   refuse replication consumers that didn't negotiate the fork protocol, so stock readers never
   see v2.
4. **Fork protocol version = 65** (`kDrakeydbReplVersion`), sent via existing
   `REPLCONF CLIENT-VERSION` — far above upstream's VER6 so future upstream bumps never collide.
   Non-active nodes interop with stock Dragonfly unchanged.
5. **Persistent node UUID** in `<dir>/drakeydb.uuid` (fixes KeyDB's per-boot regeneration),
   exchanged KeyDB-style: `REPLCONF UUID <36char>` → `+<peer uuid> <peer ms-clock>` (clock echo
   enables skew warnings). Works against DF and real KeyDB masters.
6. **MVCC stamps**: KeyDB layout (`ms << 20 | counter`), per-shard clock, stored in a side
   `DashTable<PrimeKey, uint64_t>` on `DbTable` (constructed only in active mode; ~40-48 B/key
   incl. dash overhead). `CompactObj` untouched.
7. **LWW**: full-sync merge does per-key compare (KeyDB parity); streaming apply additionally
   drops older-than-stored writes for **state-carrying single-key commands** (SET family,
   DEL/UNLINK, EXPIRE family, PERSIST, RESTORE, COPY, GETSET/GETDEL) behind
   `--multi_master_stream_lww` (default on). RMW commands (INCR/APPEND/LPUSH/HSET…) resolve by
   arrival order — same as KeyDB (see caveats).
8. **Origin/mvcc apply-context rides ConnectionContext → Transaction.** `JournalExecutor` sets
   `{repl_origin_id, repl_mvcc}` beside the existing `is_replicating` (`conn_context.h:352`);
   consumed in `Transaction::LogAutoJournalOnShard`/`LogJournalOnShard` **and** the manual
   `RecordJournal` helpers in `tx_base.cc` (covers SPOP→SREM-style effect rewrites without
   touching their ~40 call sites). Default when absent: origin=self + fresh clock tick.

### New files (zero merge-conflict surface)

| Path | Contents |
|---|---|
| `src/server/node_identity.h/.cc` | UUID create/load/persist, `--node_uuid` override, `kDrakeydbReplVersion = 65` |
| `src/server/multi_master.h/.cc` | `PeerRegistry` (uuid ↔ origin_idx, 0 = self, P1). **P2 (done):** `active_replica`/`multi_master` flags, `IsActiveReplica()`/`IsMultiMaster()`, `ValidateMultiMasterFlags()`, `ParsePeerReplicaOfArgs()`, `RenderPeerReplicationInfo()`. `MvccClock` (P4, not yet built) |
| `src/server/peer_replication.h/.cc` (new, P2) | `SyncGate` (FIFO ticket queue, cancellable, deferred while the process is LOADING for another reason, notifies under its mutex), `PeerReplicationManager` (peer links keyed by stored endpoint; add/remove/no-one; replace-vs-append by `--multi_master`) |
| `src/server/multi_master_test.cc` | C++ units (registry; **P2:** flag validation, arg parser, INFO renderer, `ActiveReplicaFamilyTest`) |
| `src/server/peer_replication_test.cc` (new, P2) | `SyncGateTest`, `PeerManagerFamilyTest` |
| `tests/dragonfly/multimaster_test.py`, `tests/dragonfly/keydb_onboarding_test.py` | pytest suites (**P2:** fan-in suite added to `multimaster_test.py`) |
| `docs/multi-master.md` | user-facing docs (written in P9) |

### New flags (declared in `multi_master.cc`)

`--active_replica` (bool, boot-only) · `--multi_master` (bool, requires active_replica,
KeyDB-parity startup error otherwise) · `--multi_master_no_forward` (must be true in v1; false →
startup error) · `--multi_master_stream_lww` (default true) · `--replica_quorum` (default −1, P8)
· `--node_uuid` (test override). Validation: active mode incompatible with `--cluster_mode`,
tiering, and `--experimental_cascaded_partial_sync`.

### Existing files touched

| File | Change |
|---|---|
| `journal/types.h` | `EntryBase` += `origin_idx`, `mvcc`, `entry_flags`; `JournalItem` += `origin_idx` (ring-buffer filtering without reparse) |
| `journal/serializer.cc` (~57-85, 205-235) | COMMAND case: header `2` + ext fields in active mode, else byte-identical `1`; reader accepts both |
| `journal/journal_slice.cc` (~107-134) | thread origin into `JournalItem`/`JournalChangeItem` |
| `journal/streamer.h/.cc` | `Config` += `{peer_mode}` (**as built** — a `peer_uuid` field was added then removed in the P3 fix wave: it was never read, since the filter keys on `origin_idx`, not on the peer's uuid); live + partial-replay paths drop non-self-origin, expiry-flagged, and derived-delete entries for peer consumers, via the shared `journal::PassesPeerEchoFilter` |
| `journal/executor.h/.cc` (~36-74) | set/clear apply-context around `Execute`; P5 streaming-LWW pre-check for classified commands |
| `transaction.cc` (~1622-1663) | `LogJournalOnShard` reads origin/mvcc from context (default self + clock tick) |
| `tx_base.cc` (~55-70) | manual `RecordJournal` helpers read origin context; expiry `DEL` sets entry-flag bit0; collection-derived `DEL` sets bit1 |
| `conn_context.h` (~352) | += `repl_origin_id` (u32), `repl_mvcc` (u64) |
| `replica.h/.cc` | P1 (done): greeting adds `REPLCONF UUID`. **P2 (done):** `ReplicaPeerMode{SyncGate*, PeerRegistry*, PeerIdentityClaims*}` trailing ctor param, `IsPeerMode()`; guards the `SetShardStates` flip (both directions in `MainReplicationFb`), a `SyncGate::Lease` around full sync, and the flush skip in `InitiateDflySync`/`InitiatePSync` (the latter also adds `SetOverrideExistingKeys(true)`; the DF path already had it set), plus self/duplicate UUID refusal, live claim release, and `PeerRegistry::AddOrGet`. P7 (future): `capa activeExpire` on the redis path, `ConsumeRedisStream` RREPLAY unwrap |
| `dflycmd.cc` | **P2 (done):** `DflyCmd::TakeOver` refuses on an active node. (Future) `ReplicaInfo` stores peer uuid; refuse version <65 / missing UUID while active |
| `debugcmd.cc` | (Future) additive `DEBUG MVCC <key>` debug subcommand |
| `server_family.cc` | P1 (done): additive `REPLCONF UUID` case. **P2 (done):** `peers_` member; `ReplicaOfInternal` delegates to `ReplicaOfActive` (`PeerReplicationManager`) when active (replace-vs-append by `--multi_master`, `REPLICAOF REMOVE <h> <p>`, NO ONE clears all); `ReplConf` refuses all replication consumers on an active node (single choke point; P3 replaces it with peer admission); `REPLTAKEOVER` refused; INFO block appended after `master_replid`; `Shutdown`/`PauseReplication` route through `peers_`; `--replicaof` now parses a comma-separated peer list (`ParseOneReplicaOf`) and, in active mode, `Init` loads the node's own snapshot before attaching peers |
| `dfly_main.cc` | banner/usage strings, `version_check` default. **P2 (done):** `ValidateReplicaOfFlags()` and `ValidateMultiMasterFlags()` added to the boot-time validator conjunction, before the proactor pool starts |
| `main_service.h/.cc` | **P2 (done):** exclusive LOADING reservation for peer full sync, preventing overlap with loaders outside the peer sync gate |
| `tests/dragonfly/instance.py` | **P2 (done):** `DflyInstanceFactory.create()` gives every instance without an explicit `dir=` (version ≥ 100) a unique `--node_uuid` default |
| `table.h` (~126-128) | `DbTable` += mvcc side DashTable (active mode only) |
| `db_slice.h/.cc` | `SetMvcc/GetMvcc/DelMvcc`; rule: **mirror every `mcflag` touch-point** (AddOrUpdate/Del/flush/defrag) |
| `rdb_save.cc` (~1761) | per-key aux `mvcc-tstamp` in active mode (KeyDB's exact format — one codepath serves DF↔DF and KeyDB ingest) |
| `rdb_load.cc` (~3024, ~3238) | parse `mvcc-tstamp` aux → seed side table; peer-merge LWW hook at 3238 (skip incoming when stored mvcc newer); log-and-ignore KeyDB `repl-masters` aux |

**Deliberately untouched hot files:** `dash.h`, `compact_object.*`. `engine_shard.cc` is now
touched by the active-mode reaper so every namespace and DB receives bounded heartbeat coverage.

### INFO / protocol surface

INFO replication in active mode: `role:master` (client-lib compat) + `active_replica:1`,
`multi_master:1`, `node_uuid`, `connected_masters:N`, per-peer
`master_N_{host,port,link_status,last_io_seconds_ago,sync_in_progress}`,
`multimaster_lww_dropped`, `mvcc_table_bytes`. `Op::ORIGIN (16)` control entries publish the
origin_idx↔uuid dictionary to full-stream (non-peer) sub-replicas.

### Semantics caveats (documented in `docs/multi-master.md`, agreed with the owner)

- **RMW commands** (INCR/APPEND/LPUSH/HSET/…) conflict-resolve by arrival order — concurrent
  same-key RMW across nodes can diverge (identical KeyDB limitation; CRDTs are out of scope).
  LWW guard covers the state-carrying command set. Recommend per-node key ownership for RMW-heavy
  workloads.
- **Delete resurrection**: no tombstones — a deleted key has no stored mvcc, so an older incoming
  value can resurrect during merge-sync (KeyDB's fix for this is verified dead code; tombstone
  side-table is v2 work).
- **NTP is a hard requirement** for LWW quality; handshake clock-echo produces a skew warning +
  `clock_skew_ms` metric.
- **Initial peer full sync keeps Dragonfly LOADING semantics** (commands blocked during the
  merge-load; steady-state fully writable). KeyDB serves stale reads during load —
  `allow-write-during-load` parity is future work.
- **FLUSHALL propagates mesh-wide** (KeyDB parity); documented operational hazard.

---

## Phase 0 — Repo hygiene + rename ✅ (landed via PR #1)

Done: `origin`/`upstream` remotes; helio submodule init; `.gitignore` KeyDB/; `.gitattributes`
merge=ours (README); `project(DRAKEYDB)`; binary `OUTPUT_NAME drakeydb` + `dragonfly` symlink;
README replaced (+4 translations removed); `BRANDING.md`, `NOTICE`, `docs/UPSTREAM-SYNC.md`;
helm chart copied to `contrib/charts/drakeydb` (golden harness dropped from copy); publish
workflows removed; `--version_check=false` default; banner/usage/version strings rebranded.
Tag scheme `drakey-X.Y.Z` (no `v` prefix) — first tag cut in P9.

## Phase 1 — Identity foundations ✅ (branch `feat/phase1-identity`)
`node_identity` (persisted UUID at `<dir>/drakeydb.uuid`), `REPLCONF UUID` exchange (additive
case in `ReplConf`), `PeerRegistry`, INFO `node_uuid`. No behavior change otherwise.
**Verify:** C++ unit; pytest: two nodes exchange + report UUIDs; existing replication tests green.

Delivered: `node_identity.{h,cc}` (uuid generate/validate/normalize, `ParseReplconfUuidReply`
accepting both KeyDB's bare-uuid and drakeydb's `<uuid> <ms>` reply, persisted identity with the
6-row boot policy, `--node_uuid` override); `multi_master.{h,cc}` (`PeerRegistry`, append-only
uuid↔origin_idx with self at 0, fiber-safe, no reclamation API by design); identity load in
`ServerFamily::Init()`; master-side `REPLCONF UUID` arm; replica-side exchange in `Greet()`
tolerating `-ERR` or exact `+OK` from pre-exchange masters; INFO `node_uuid` (both roles),
`master_node_uuid` (replica), and `,node_uuid=` inside `slaveN:` (master). The process-wide
`PeerRegistry` is initialized with self only; remote registration is intentionally deferred until
peer admission is defined. 21 C++ cases + 10 pytest cases.
Upstream-file footprint deliberately small and additive: `server_family.{h,cc}` +19/-0 in P1's
wiring task and +12/-0 in the ReplConf task.

Note: inside `slaveN:`, `node_uuid` is inserted **before** `lag`, not appended last. `lag` must
stay the trailing field — `replication_test.py` and `cluster_test.py` both parse it with a
`lag=([0-9]+)\r\n` regex that anchors on the line ending.

<a id="phase-2"></a>

## Phase 2 — Writable multi-source replica (fan-in) ✅ (branch `feat/phase2-fanin`)
Flags + validation; peer-mode `Replica` (no read-only flip; skip flush; override-load merge,
last-loaded-wins until P6); `REPLICAOF` append / `REPLICAOF REMOVE <h> <p>` / NO-ONE-clears-all
via `PeerReplicationManager`; serialized round-robin handshakes; **refuse all replication
consumers while active** (prevents loops until P3).
**Verify:** pytest — active A `REPLICAOF` plain masters B and C: A merges both, stays writable
steady-state, expires keys itself; restart → clean re-merge.

**Delivered:** all gated on `--active_replica`; non-active behaviour stays byte-identical to
upstream. `multi_master.{h,cc}`: `active_replica`/`multi_master` flags, `IsActiveReplica()`/
`IsMultiMaster()`, `ValidateMultiMasterFlags()` (multi_master requires active_replica;
active_replica incompatible with `--cluster_mode`/`--tiered_prefix`/
`--experimental_cascaded_partial_sync`), called from `dfly_main.cc` before the proactor pool;
`ParsePeerReplicaOfArgs()` and `RenderPeerReplicationInfo()` (`masterN:` INFO lines mirroring
`slaveN:`). New files `peer_replication.{h,cc}` + `peer_replication_test.cc`: `SyncGate` (FIFO,
cancellable, deferred while LOADING for another reason, notifies under its own mutex) and
`PeerReplicationManager` (peers identified by stored endpoint; `Add` blocks for `REPLICAOF` or
backgrounds for boot `--replicaof`; replace-vs-append by `--multi_master`; `Remove`/`RemoveAll`/
`Shutdown`/`PauseAll`). `replica.{h,cc}`: `ReplicaPeerMode` trailing ctor param guards 5 sites (no
`SetShardStates` flip, a sync-gate lease around full sync, skip-flush on both the DF and Redis
full-sync paths plus an explicit `SetOverrideExistingKeys(true)` on the Redis one (the DF path
already had it set), and a `Greet()` self-uuid refusal + `PeerRegistry` registration).
`server_family.{h,cc}`: `peers_` member, `ReplicaOfActive`, `ReplConf`/
`REPLTAKEOVER` refusal on active nodes, the INFO block, `Shutdown`/`PauseReplication` hooks, and a
`--replicaof` comma-separated list (`ParseOneReplicaOf`) with an own-snapshot load before
attaching peers. `dflycmd.cc`: `DflyCmd::TakeOver` refusal. `instance.py`: per-instance
`--node_uuid` default. Tests: `multi_master_test.cc` (`ActiveReplicaFamilyTest` plus flag/parser/
INFO-renderer units), `peer_replication_test.cc` (`SyncGateTest`, `PeerManagerFamilyTest`), and
the Phase 2 suite added to `multimaster_test.py`.

**Decisions refined during P2:**
- LOADING is kept for the whole merge-load duration (D1); steady state is fully writable.
- Replace-vs-append is controlled by `--multi_master` (KeyDB parity): off replaces the single
  peer, on appends (`PeerReplicationManager::Add`).
- INFO renders one `masterN:` line per peer, mirroring `slaveN:` (`RenderPeerReplicationInfo`).
- `REPLICAOF` is accepted while another peer is mid-full-sync and queues behind `SyncGate`,
  instead of upstream's LOADING rejection on the active path.
- Consumer refusal happens at a single choke point in `ReplConf`
  (`"Replicating from an active-replica node is not supported"`); P3 replaces it with peer
  admission.
- `--replicaof` accepts a comma-separated peer list; in active mode `Init` loads the node's own
  snapshot before attaching every peer. A list of more than one target requires both
  `--active_replica` and `--multi_master` (boot error otherwise).
- Peer full sync reserves LOADING exclusively after sync-gate admission, closing the race with
  boot/DEBUG loaders that could otherwise enter between gate admission and the actual merge load.
- Peer uuids are registered in `PeerRegistry` from inside `Greet()`, not from a separate
  admission step.
- The clone-uuid refusal in `Greet()` only fires in peer mode; a non-peer `--replicaof` clone is
  still undetected (see the P3 prerequisite note above).
- The pytest harness gives every instance without an explicit `dir=` a distinct identity via a
  generated `--node_uuid` default.

**Deferred / non-goals in P2:**
- Per-peer auth/TLS: the global `--masterauth`/`--masteruser`/`--tls_replication` flags apply to
  every peer; per-peer credentials are v2.
- `ROLE` is unchanged (`master`); peers are not listed.
- Per-peer Prometheus metrics (P8).
- Serve-during-merge: LOADING is kept, so clients see `-LOADING` for the whole merge-load
  duration.
- `Replica::Stop()`'s last-LSN data is not handed to a re-attached peer: `REMOVE` then `ADD` is
  always a full re-sync, never a partial resync across the gap.
- An unreachable peer surfaces upstream's pre-existing generic `"replication cancelled"` error
  text, not a peer-specific message.

**P3 notes:**
(a) Replace the `ReplConf` active-mode refusal with real peer admission (fork protocol version
    65 + UUID) instead of refusing every consumer outright.
(b) Peers' `PING` journal entries are re-recorded into the local journal by `replica.cc`'s
    stable-sync loop (`journal::RecordEntry(0, journal::Op::PING, 0, nullopt, {})`), unguarded by
    peer mode — decide origin handling before P3 stamps origins on journal entries.
(c) Tests that explicitly pass `dir="{DRAGONFLY_TMP}/"` (opting out of the new `--node_uuid`
    default — e.g. `replication_specific_test.py`'s `test_bgsave_during_stable_sync`, ~line 1613)
    still resolve to one shared `drakeydb.uuid` file, since `DRAGONFLY_TMP` is the session tmp
    dir; harmless in P2 (the clone-uuid refusal is peer-mode only).
(d) `SyncGate`'s private members carry no `ABSL_GUARDED_BY` annotations — it locks its
    `fb2::Mutex` via `std::unique_lock`/`std::lock_guard` rather than `util::fb2::LockGuard`,
    unlike `PeerReplicationManager`'s `peers_`/`closed_`. Fine on GCC/Clang today, worth
    reconciling.
(e) `node_identity.cc`'s `LoadOrCreateNodeIdentity`/`PersistUuid` has a TOCTOU race: two
    processes booting concurrently against the same empty `--dir` can each read "no such file",
    each generate a different uuid, and each see their own `PersistUuid` (mkstemp + atomic
    rename) succeed — the loser keeps running with a uuid that no longer matches the file a
    restart would load.

<a id="phase-3"></a>

## Phase 3 — Origin-tagged journal + active pair/mesh ✅ (branch `feat/phase3-origin-journal`)
Journal v2 framing; origin on `JournalItem`/`JournalChangeItem`; apply-context plumbing
(ConnectionContext → Transaction → auto + manual journal paths); streamer peer filter
(self-origin only, expiry-DELs suppressed to peers, full stream to plain sub-replicas); consumer
UUID/version gating in `DflyCmd`; expiry entry-flag.
**Verify:** `journal_test.cc` v2 round-trip; pytest — A↔B bidirectional convergence under seeder
load (hash-compare via `replication_utils`), echo-storm absence (write counters plateau),
FLUSHALL flood parity, plain sub-replica of A sees B-origin writes + expiry DELs; 3-node mesh
convergence; node kill/restart. Manual: 3-container compose, cross-writes, `DEBUG` digest compare.


**Delivered.** All gated on `--active_replica`; with the flag off, **journal bytes are
byte-identical to upstream** (proved by a golden-buffer test, not by inspection). `journal/types.h`:
`Op::ORIGIN=16`, `EntryBase` += `origin_idx`/`mvcc`/`entry_flags` (+`kEntryFlagExpired`), and —
load-bearing — `JournalItem` += `origin_idx`/`entry_flags`/`opcode`, because the ring buffer stores
`JournalItem` and the partial-replay path rebuilds a bare `JournalChangeItem` from `data` alone.
`serializer.{h,cc}`: `extended_framing` ctor flag; the deprecated varint becomes `1` (legacy) or `2`
+ `{origin_idx, mvcc, entry_flags}`; reader is version-agnostic and now **errors** on an unknown
header instead of silently misparsing it as COMMAND. Apply-context rides `ConnectionContext` →
`Transaction`, hooked once at `PrepareTransaction`, plus `dist_trans` (EXEC) and the non-atomic
squash stub. `RecordExpiryBlocking` sets the expiry flag; a `DbContext`-carried origin gives derived
DELs (`DeleteIfEmpty`/`DeleteSetIfEmpty`) the originating peer's identity. `journal::PassesPeerEchoFilter`
is the single shared predicate used by **both** `JournalStreamer::ShouldWrite` and
`SliceSnapshot::ConsumeJournalChange`, so stable sync and the full-sync window cannot drift apart.
Peer admission replaces P2's blanket refusal: `REPLCONF DRAKEY-VERSION` + `REPLCONF PEER`, sent in
`Greet()` before `capa dragonfly` so the master sees them at admission time. Peer-mode LSN accounting
(`Op::LSN` markers on filtered gaps, adopted authoritatively) keeps a filtered peer's resume offset
correct. Reciprocal-connect uuid tiebreak with a background fallback for interactive `REPLICAOF`.

**Verified:** full `ninja` warning-free; `ctest -L DFLY`; pytest `multimaster_test.py` **41 passed**,
`replication_test.py` **43 passed / 0 failed**, `replication_specific_test.py` **61 passed / 0 failed**.
Timing-adjacent tests were each run 10-15x to establish a pass rate rather than trusting a single
green run. `clang++ -Wthread-safety` run over all 11 concurrency-relevant TUs: **0 diagnostics** —
worth knowing because `CMakeLists.txt:77-79` gates that analysis on Clang and the local container
builds with g++, so it never runs in the ordinary local gate.

**Decisions and behaviour changes to know about:**
- **D2b — an active node now REFUSES a peer-mode source that does not identify itself.** P2
  accepted fan-in from a stock Dragonfly master (uuid simply absent). Without a uuid there is no
  origin, and stamping such writes self-origin would forward them to peers. On the consumer side,
  the UUID requirement is likewise scoped to `PEER 1`: a fork-versioned non-PEER consumer remains
  a plain full-stream replica even without a UUID. Missing or old `DRAKEY-VERSION` is a separate
  refusal condition. KeyDB masters do reply with a uuid, so P7 onboarding is unaffected; what stops
  working is peer-mode fan-in from **stock Dragonfly or plain Redis** while `--active_replica` is on.
- **The replica handshake is no longer wire-identical to upstream** (two additive REPLCONF pairs,
  which pre-fork masters reject harmlessly). It must be unconditional, because admission requires
  `DRAKEY-VERSION` even from a plain sub-replica of an active node. **Journal bytes** — the property
  that protects upstream mergeability and stock interop — are untouched.
- The spec's earlier claim that the `PrepareTransaction` hook covers *every* journaling path was
  **false**: EXEC's `dist_trans` and the non-atomic squash `local_tx` bypass it. Both now inherit
  origin explicitly.
- `SetOverrideExistingKeys(true)` is **behaviourally inert** — `rdb_load.cc:3273` gates only a
  `LOG(WARNING)` on it; `AddOrUpdate` overwrites regardless. **P6's merge-LWW must implement that
  behaviour rather than assume it exists.**

**Known limitations carried forward (P4+):**
- **Fixed in P4-0 Task 1** (see Phase 4's own section below for what shipped): a *locally* issued
  command whose lazy field expiry empties a collection used to emit an unflagged, self-originated
  DEL that the peer filter forwarded, and a plain `HTTL` on a lazily-expired collection could
  delete the key on a peer whose copy still held unexpired fields under clock skew.
- A peer link whose traffic is entirely filtered emits markers only on the drop-path throttle; after a
  *failed apply* the sticky `apply_failed_` flag stops adoption, so the counter drifts and a reconnect
  likely forces a full resync rather than replaying the failed entry. Safe (never ahead of applied),
  but layering `ReportError` on the peer-mode apply failure would make it prompt.
- `replica.cc:635`'s legacy Redis/KeyDB-protocol loader is peer-*aware* but never gets
  `SetApplyOrigin`. Unreachable today (a redis-protocol master emits no `RDB_OPCODE_JOURNAL_BLOB`) —
  **but P7 is exactly the phase that would make it carry real data.**
- The plain-replica misconfiguration guard is a `DCHECK`, so its loud failure protects debug/CI only;
  a release build degrades to a warning plus silent drops.
- **Phantom containers on a read-asymmetric mesh (P4-0 Task 1 fix-wave finding; closed by
  Task 2b's member-expiry reaper).** `DbSlice::FindInternal` (`db_slice.cc:673-687`) checks only
  whole-key `HasExpire()`; member-level (hash-field/set-member) expiry used to be reaped
  exclusively by the ~25 `DeleteIfEmpty`/`DeleteSetIfEmpty` call sites enumerated for the P4-0
  fix, every one of which needs a command to touch the key to run at all — for the read-path call
  sites, whose causing command is never journaled by design, a peer kept a logically-empty
  container **indefinitely** until *it* was asked to read that same key. Task 2b's reaper
  (`DbSlice::DeleteExpiredStep`, `db_slice.cc`) adds a proactive walk: on `--active_replica`
  nodes, the heartbeat's existing prime-table traversal also force-expires and reaps any
  container with a live member TTL (`HasMemberExpiration()`), incrementally per container
  (`--reaper_member_walk_budget`, resumable across ticks; collision chains/extension vectors are
  visited as a unit rather than hard-latency-bounded), so a peer that is never itself asked to
  read the key still converges on its own clock. This reaper is load-bearing for correctness, not
  hygiene: the adversarial call-site inventory found no suppressed helper call whose source
  effect is reproduced by a journaled command. Active mode therefore treats a configured budget
  of 0 as 1. Every node derives its own DEL
  (`journal::kEntryFlagDerived`, suppressed from peer links exactly like the read-path carve-outs
  above), so this does not change what crosses the wire, only how promptly each node's own copy
  converges — including a container that also carries a not-yet-due whole-key TTL (fix round 6
  Critical 1: an earlier version of the reaper skipped exactly that shape, since it lived entirely
  inside the "no whole-key TTL" arm of the heartbeat's existing dispatch, reopening this same
  phantom-container divergence, up to permanent divergence after a recreate, for the ordinary
  session/cache-hash pattern of a member TTL plus a key-level TTL). The default namespace is
  visited every tick; one additional namespace and one of its DBs are visited per tick, each with
  an independent round-robin cursor, so the heartbeat stays bounded while every local DB remains
  reachable. Non-default sweeps are local-only because the journal wire has no namespace identity.
  `HSetFamily::DeleteHw`, the third derived-DEL path used by the generic `HMapWrap` wrapper and
  `HSETEX`, now records the same derived flag by default. Its conditional HSETEX caller retains a
  forwarded-DEL carve-out because a lagging peer can otherwise choose the opposite FNX/FXX branch;
  focused regressions pin both behaviors and their journal ordering.
  Same root cause, corollary regression, unaffected by the above: `DEBUG OBJHIST`/`DEBUG STRINGS`
  used to be an inadvertent **mesh-wide** phantom-container sweep (every node's derived DEL
  reached every peer); they now only clean up the node they run on. P4-2's tombstone GC and the
  design spec's D-4 full-scan invariant both have to account for these keys explicitly — neither
  can assume "logically empty" and "absent from the peer" coincide before the local reaper runs.
- **SORT partial expiry is compensated.** `OpFetchSortEntries` and
  `OpFetchContainerElements` capture the TTL-bearing set before lazy expiry, then journal an
  explicit `SREM` for members removed by a partial pass before SORT's verbatim entry. The walk
  completes even after a numeric parse failure so an errored SORT also journals exactly the
  members it actually expired. Full expiry still emits DEL, and SORT_RO remains non-journaled.
  Tests cover both fetch implementations, the error path, and source-before-destination ordering.

**Testing lesson worth keeping.** A **convergence assertion cannot detect an echo storm.** With the
origin filter reverted, two nodes still converge: symmetric amplification makes every node replay the
same bounced ops, and `@assert_eventually`'s retry loop then finds a moment where they coincide — so
convergence-under-load is *structurally incapable* of catching it, not merely unlucky. The load-bearing
detector is the command-counter check (`assert_no_command_storm`); its bound is topology-dependent and
decomposes as `3 x (shard_flows x peers x 1/s REPLCONF ACK) + 1 self-INFO` (measured 4 for 2 nodes,
7 for 3 — both matching prediction exactly). Recalibrate it when the topology changes.

## Phase 4 — MVCC store + stamping + wire
`MvccClock`; side table (mirror all `mcflag` touch-points incl. Del/flush); stamp local writes
(clock tick) and applied writes (context mvcc); journal v2 mvcc field live; `DEBUG MVCC <key>`;
INFO `mvcc_table_bytes`.
**Verify:** C++ stamping unit; pytest — replicated key's stamp on B equals A's origin stamp.

**P4-0 Task 1 delivered** (resolves the "Blocker on enabling `--active_replica` by default" noted
against Phase 3, above): `journal::kEntryFlagDerived` (`journal/types.h`) marks a DEL derived from
`HSetFamily::DeleteIfEmpty`/`SetFamily::DeleteSetIfEmpty` emptying a collection;
`journal::PassesPeerEchoFilter` (`journal/types.cc`) drops it for peer links exactly as it already
does for `kEntryFlagExpired`, while a plain full-stream replica still receives it.
`dfly::RecordDerivedDelete` (`tx_base.h`/`.cc`) sets the flag; both helpers default to it via a
trailing `derived = true` parameter. Safe as the default: every enumerated call site other than
the two carve-outs named below is reached only when the transaction's own causing command is
itself never journaled (read-only, or DEBUG-only and non-propagating), so the peer replays
nothing there and instead reaches the same empty-collection conclusion independently, via its own
lazy member expiry.

Two call sites pass `derived = false` (forward the DEL like any other command-caused one) because
their causing command auto-journals verbatim and a peer's own replay of it cannot be relied on to
reproduce the same emptying: `OpFieldExpire`'s two branches (`generic_family.cc`) — a lagging peer
can *arm* an already-expired field/member with the command's own new TTL instead of also
discovering it expired — and `OpFetchSortEntries`/`OpFetchContainerElements`'s `SORT` case
(`generic_family.cc`, keyed off the transaction's own `CommandId` via `WillAutoJournalVerbatim`,
not a hardcoded command name, so `SORT_RO` — which shares the same call sites but never
auto-journals — still gets the suppressed default). Both carve-outs mirror `OpHExpire`'s
pre-existing plain-forwarded-DEL precedent for HEXPIRE/HPEXPIRE (`hset_family.cc`) rather than
inventing a new exception shape.

**P4-1 Task 12 delivered**: the MVCC side-table memory benchmark
(`tests/dragonfly/multimaster_memory_test.py`), 1M keys x {8,16,24,32}-byte keys, an
`--active_replica` instance vs. a plain baseline. `mvcc_table_bytes` tracks entry count
only (46,497,792 B / 44.3 MiB identically in all four cases) and its own per-key cost
stays inside the `[34, 48]` B/key geometry band predicted from
`sizeof(DbTable::MvccTable::Segment_t::Bucket) == 504` (measured: 46.5 B/key, every
case). The metric has a narrower version of the same blind spot `table_used_memory` has,
though: it is `DashTable::mem_usage()`, documented as excluding "memory allocated by the
hosted objects", so it cannot see the second, independent key copy `DbSlice::SetMvcc`
heap-allocates for every key whose ASCII-packed form still exceeds
`CompactObj::kInlineLen` (16 B). Measured (all four `used_memory`-delta assertions pass
against a model of that second copy, not against `mvcc_table_bytes` alone — see
`_expected_duplication_bytes` in the test and task-12-report.md):

| key_len | mvcc_table_bytes (table-only) | used_memory delta (true cost) | gap over table-only |
|---:|---:|---:|---:|
| 8  | 44.3 MiB | 48.0 MiB | +8.3% |
| 16 | 44.3 MiB | 48.0 MiB | +8.3% |
| 24 | 44.3 MiB | 63.3 MiB | +42.7% |
| 32 | 44.3 MiB | 78.5 MiB | +77.1% |

For capacity planning on a long-key workload, `used_memory` is the figure to trust, not
`mvcc_table_bytes` alone — documented at the `mvcc_table_bytes` INFO call site
(`server_family.cc`) and in the test itself. Full data, the mimalloc-bin-table probe the
duplication model is built from, and the falsification for every assertion:
`.superpowers/sdd/2026-08-25-phase4-mvcc-lww/task-12-report.md`.

## Phase 5 — Streaming LWW guard
Command classifier + pre-exec compare/drop in `JournalExecutor`; `multimaster_lww_dropped`
metric; `--multi_master_stream_lww` off = KeyDB-parity arrival order.
**Verify:** pytest — concurrent conflicting SETs on A and B converge to the higher-mvcc value on
both (KeyDB's "MVCC Updates Correctly" parity incl. its 2 ms slop).

## Phase 6 — Merge-on-full-sync LWW
`mvcc-tstamp` per-key aux save/load; LWW hook at `rdb_load.cc:3238`.
**Verify:** `rdb_test.cc` aux round-trip; pytest — node with newer local writes full-syncs from a
peer holding older values → newer survive ("Active Replica Merges Database On Sync" parity);
reconnect-merge.

## Phase 7 — KeyDB one-way onboarding
Redis-path greeting adds `REPLCONF uuid` + `capa activeExpire` (KeyDB refuses active full sync
without it, KeyDB `replication.cpp:1798`); `ConsumeRedisStream` RREPLAY unwrap (recursion ≤64,
self-uuid drop, per-link monotonic mvcc dedup, apply-context from envelope); KeyDB RDB
`mvcc-tstamp` parse (shared with P6); graceful skip+warn for KeyDB-specific artifacts;
EXPIREMEMBER documented as unsupported.
**Verify:** docker `eqalpha/keydb` (active-replica yes) + drakeydb: seed KeyDB incl. mid-sync
writes → converge; drakeydb local writes unaffected; suite gated on image availability.

## Phase 8 — Mesh hardening + ops
Reciprocal-connect UUID tiebreak; `--replica_quorum` serve-stale gating; reject
FAILOVER/REPLTAKEOVER/DFLY TAKEOVER in active mode; clock-skew warning + metric; per-peer lag
metrics; 4-node chaos pytest (random kills + seeder + convergence assert).

## Phase 9 — CI + docs + release
`drakeydb-ci.yml` (build, ctest, pytest incl. multimaster + gated KeyDB job);
`docs/multi-master.md`; helm mesh values; first `drakey-0.1.0` tag.

---

## Risk register

| # | Risk | Mitigation |
|---|---|---|
| 1 | Serialize-once journal vs per-consumer filtering (ring-buffer partial replay must filter without reparsing) | `JournalItem.origin_idx` memory-side field; filter in live + `MaybePartialStreamLSNs` paths; C++ mixed-origin backlog test |
| 2 | Upstream owns the deprecated varint / journal code | Version constant 65 dodges upstream VERs; `docs/UPSTREAM-SYNC.md` watchlist; multimaster pytest suite is the merge gate; framing diff is ~30 lines in one low-churn file |
| 3 | LSN/partial-sync semantics across peers | Each peer pair is its own lineage; peer-mode `Replica` never adopts lineage / never `StartJournalAtOwnLSN`; cascaded flag mutually exclusive with active mode; per-peer restart partial-sync test |
| 4 | Clock skew breaks LWW | Hybrid stamp absorbs small skew; handshake skew warning + metric; NTP documented as hard requirement |
| 5 | Concurrent same-key RMW diverges (arrival order) | Same hole as KeyDB; loud docs; LWW guard covers state commands; recommend per-node key ownership for RMW |
| 6 | MVCC side-table memory (~40-48 B/key) | Active-mode-only allocation; INFO metric; documented; CompactObj growth explicitly rejected |
| 7 | Delete resurrection during merge (no tombstones) | KeyDB-identical hole (their fix is dead code); documented; serialized handshakes shrink the window; tombstone table = v2 |
| 8 | FLUSHALL floods mesh / races merge-sync | Parity behavior + test; global cmds already rendezvous via `MultiShardExecution`; ops guidance; future `--multi_master_protect_flush` |

## Upstream sync workflow (ongoing)

See `docs/UPSTREAM-SYNC.md`. Summary: merge (not rebase) `upstream/main` monthly + after upstream
releases; fork changes stay additive and flag-gated so `--active_replica`-off behavior is
byte-identical to upstream; verification gate = build + `ctest -L DFLY` + replication pytest
subset + multimaster suite.

## Non-goals for v1

Forwarding topologies (ring/tree — `multi_master_no_forward` hardwired true); cluster mode,
tiering, or cascaded-flag combined with active mode (rejected at boot); serving Redis/KeyDB
replication as a master; bidirectional KeyDB mesh membership; KeyDB extras (EXPIREMEMBER,
`KEYDB.MVCCRESTORE`, FLASH, fastsync, `repl-masters` offset adoption); CRDT semantics for RMW;
FAILOVER/sentinel in active mode; deep rename items (see `BRANDING.md`).
