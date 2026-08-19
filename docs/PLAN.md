# drakeydb Master Plan

> **Continue here.** This is the approved, living plan for the drakeydb fork. Update the status
> block below as phases land. A local mirror of the originally approved plan lives at
> `~/.claude/plans/this-folder-is-a-vast-puppy.md` on the original dev machine; **this file is
> the canonical copy.**

## Current status (last updated 2026-08-19)

| Phase | Status | Where |
|---|---|---|
| **P0 — Repo hygiene + rename** | ✅ **complete, verified** | PR [#1](https://github.com/darkspadez/drakeydb/pull/1), branch `feat/phase0-drakeydb-rename`, commit `7ca99f4c` |
| **P1 — Identity foundations** | ⏭️ **next up** | see [Phase 1](#phase-1--identity-foundations) |
| P2–P9 | not started | — |

**P0 verification record** (Ubuntu 24.04 arm64 container, OrbStack): debug build produces
`build-dbg/drakeydb` + `dragonfly` compat symlink; `--version` → `drakeydb dev-…`; live server
answers PING/SET/GET and INFO still reports `dragonfly_version`/`redis_version` (compat intact);
`journal_test` 8/8; `pymemcached_test.py::TestMemcached::test_basic` passes via the harness's
default binary path; pre-commit clean; dry-run `git merge upstream/main` conflict-free.

**Resuming work — environment notes (macOS dev machine):**
- Container runtime is **OrbStack**: `orbctl start` if the docker daemon is down. VM is
  linux/arm64 with 8 GB RAM / 12 CPUs.
- Build container: `docker run -v "$PWD":/src -w /src ubuntu:24.04` + apt packages from
  `docs/build-from-source.md` (add `binutils` for pytest teardown symbolization, plus
  `redis-tools python3-venv` for smoke tests). Then `./helio/blaze.sh -DWITH_AWS=OFF
  -DWITH_GCP=OFF && cd build-dbg && ninja -j4 dragonfly`.
  **Use `-j4`** — default 12-way parallelism OOM-kills `cc1plus` in the 8 GB VM.
- pytest deps: `python3 -m venv /tmp/tv && /tmp/tv/bin/pip install -r tests/dragonfly/requirements.txt`.
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

- Dragonfly is **BSL 1.1** (`LICENSE.md`, change date Nov 1, 2030 → Apache 2.0). Additional Use
  Grant excludes offering the work as an in-memory data-store product/service to third parties.
  Publishing fork source on GitHub is fine; **selling/hosting drakeydb as a service is not, until
  2030**. Lawyer question if commercialization is intended.
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
- Journal COMMAND entries carry a **deprecated varint** written as `1`, parse-and-discarded by
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
  arrival-order-only streaming (we add an LWW guard); per-boot UUID regeneration (we persist).
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
| `src/server/multi_master.h/.cc` | `PeerReplicationManager` (vector of peer `Replica`s; add/REMOVE/NO-ONE; serialized round-robin handshakes; reciprocal-connect UUID tiebreak), `PeerRegistry` (uuid ↔ origin_idx, 0 = self), `MvccClock`, INFO renderer, all new flags |
| `src/server/multi_master_test.cc` | C++ units (registry, clock, framing) |
| `tests/dragonfly/multimaster_test.py`, `tests/dragonfly/keydb_onboarding_test.py` | pytest suites |
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
| `journal/streamer.h/.cc` | `Config` += `{peer_uuid, peer_mode}`; live + partial-replay paths drop non-self-origin and expiry-flagged entries for peer consumers |
| `journal/executor.h/.cc` (~36-74) | set/clear apply-context around `Execute`; P5 streaming-LWW pre-check for classified commands |
| `transaction.cc` (~1622-1663) | `LogJournalOnShard` reads origin/mvcc from context (default self + clock tick) |
| `tx_base.cc` (~55-70) | manual `RecordJournal` helpers read origin context; expiry `DEL` sets entry-flag bit0 |
| `conn_context.h` (~352) | += `repl_origin_id` (u32), `repl_mvcc` (u64) |
| `replica.h/.cc` | peer-mode ctor flag: skip `SetShardStates`/master-flag flip (238, 952-954); skip flush, `SetOverrideExistingKeys(true)` (654-670, 497-501); greeting adds `REPLCONF UUID` (+ `capa activeExpire` on redis path, 330-373); `ConsumeRedisStream` (725-887) RREPLAY unwrap |
| `dflycmd.cc` | `ReplicaInfo` stores peer uuid; refuse version <65 / missing UUID while active (285-382, 770-795); additive `DFLY MVCC <key>` debug subcommand |
| `server_family.cc` (keep diff ≤ ~60 added lines) | `ReplConf` additive `UUID` case (~3581); `ReplicaOfInternal` (~3420-3491) delegates to `PeerReplicationManager` in active mode (append semantics, `REPLICAOF REMOVE <h> <p>`, NO ONE clears all); one INFO delegate call (~2968); reject `REPLTAKEOVER` in active mode |
| `table.h` (~126-128) | `DbTable` += mvcc side DashTable (active mode only) |
| `db_slice.h/.cc` | `SetMvcc/GetMvcc/DelMvcc`; rule: **mirror every `mcflag` touch-point** (AddOrUpdate/Del/flush/defrag) |
| `rdb_save.cc` (~1761) | per-key aux `mvcc-tstamp` in active mode (KeyDB's exact format — one codepath serves DF↔DF and KeyDB ingest) |
| `rdb_load.cc` (~3024, ~3238) | parse `mvcc-tstamp` aux → seed side table; peer-merge LWW hook at 3238 (skip incoming when stored mvcc newer); log-and-ignore KeyDB `repl-masters` aux |

**Deliberately untouched hot files:** `main_service.cc`, `engine_shard.cc`, `dash.h`,
`compact_object.*`.

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
  `mvcc_clock_skew_ms` metric.
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

## Phase 1 — Identity foundations ⏭️ NEXT
`node_identity` (persisted UUID at `<dir>/drakeydb.uuid`), `REPLCONF UUID` exchange (additive
case in `ReplConf`), `PeerRegistry`, INFO `node_uuid`. No behavior change otherwise.
**Verify:** C++ unit; pytest: two nodes exchange + report UUIDs; existing replication tests green.

## Phase 2 — Writable multi-source replica (fan-in)
Flags + validation; peer-mode `Replica` (no read-only flip; skip flush; override-load merge,
last-loaded-wins until P6); `REPLICAOF` append / `REPLICAOF REMOVE <h> <p>` / NO-ONE-clears-all
via `PeerReplicationManager`; serialized round-robin handshakes; **refuse all replication
consumers while active** (prevents loops until P3).
**Verify:** pytest — active A `REPLICAOF` plain masters B and C: A merges both, stays writable
steady-state, expires keys itself; restart → clean re-merge.

## Phase 3 — Origin-tagged journal + active pair/mesh
Journal v2 framing; origin on `JournalItem`/`JournalChangeItem`; apply-context plumbing
(ConnectionContext → Transaction → auto + manual journal paths); streamer peer filter
(self-origin only, expiry-DELs suppressed to peers, full stream to plain sub-replicas); consumer
UUID/version gating in `DflyCmd`; expiry entry-flag.
**Verify:** `journal_test.cc` v2 round-trip; pytest — A↔B bidirectional convergence under seeder
load (hash-compare via `replication_utils`), echo-storm absence (write counters plateau),
FLUSHALL flood parity, plain sub-replica of A sees B-origin writes + expiry DELs; 3-node mesh
convergence; node kill/restart. Manual: 3-container compose, cross-writes, `DEBUG` digest compare.

## Phase 4 — MVCC store + stamping + wire
`MvccClock`; side table (mirror all `mcflag` touch-points incl. Del/flush); stamp local writes
(clock tick) and applied writes (context mvcc); journal v2 mvcc field live; `DFLY MVCC <key>`;
INFO `mvcc_table_bytes`.
**Verify:** C++ stamping unit; pytest — replicated key's stamp on B equals A's origin stamp.

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
