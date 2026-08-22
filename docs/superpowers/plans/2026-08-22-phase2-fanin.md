# drakeydb Phase 2 — Writable Multi-Source Replica (Fan-In) — Design + Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to
> implement this plan task-by-task (Sonnet 5 implementers, Opus 5 reviewers — see "Execution
> workflow"). Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a drakeydb node started with `--active_replica` (and optionally `--multi_master`)
`REPLICAOF` one or more plain masters at once while staying a writable master: it merges each
peer's dataset on full sync instead of flushing, keeps expiring/evicting keys itself, serializes
peer full syncs, refuses anyone replicating *from* it (until Phase 3), and exposes the peer links
in `INFO replication`.

**Architecture:** A peer-mode flag on upstream's `Replica` (4 guarded sites: no shard-state flip,
no flush on DF + Redis full sync, FIFO sync gate, duplicate-uuid refusal + registry registration in
`Greet`) plus a new `PeerReplicationManager` (vector of peer `Replica`s, add/remove/no-one,
replace-vs-append by `--multi_master`) that `ServerFamily` delegates to when active. All new logic
lives in new files (`peer_replication.*`, additions to `multi_master.*`); upstream files get small
additive hooks only (`server_family.cc` ≈ 60 lines, `replica.cc` ≈ 35, `dfly_main.cc` 1 line,
`dflycmd.cc` 3 lines).

**Tech Stack:** C++20, helio fibers (`util::fb2::Mutex/CondVarAny`), abseil flags, gtest
(`BaseFamilyTest`), pytest harness in `tests/dragonfly/` (`DflyInstanceFactory`), OrbStack
Ubuntu 24.04 arm64 build container.

**Spec:** this file, "Design" section (copied to
`docs/superpowers/specs/2026-08-22-phase2-fanin-design.md` in Task 0; the plan is copied to
`docs/superpowers/plans/2026-08-22-phase2-fanin.md`). Canonical project plan: `docs/PLAN.md`.

---

## Context

drakeydb is Jon's public fork of Dragonfly that ports KeyDB-style active-replica / multi-master
replication (`docs/PLAN.md`, 10 phases). P0 (rename) and P1 (persistent node uuid, `REPLCONF UUID`
exchange, `PeerRegistry`, INFO `node_uuid`) are merged into `origin/main` (`33da9919`, PR #2).
P2 is the first phase that changes replication *behaviour*: it delivers the "writable multi-source
replica" that P3 (origin-tagged journal, active pair/mesh) builds on. Verification target from
PLAN.md: *active A `REPLICAOF` plain masters B and C: A merges both, stays writable steady-state,
expires keys itself; restart → clean re-merge.*

Repo state at planning time: worktree `/Users/darkspadez/.paseo/worktrees/2wtglncc/roasted-moth`,
branch `plan-dev-task` at `33da9919` (= origin/main, clean). helio submodule NOT checked out, no
build dir, OrbStack stopped. Main checkout (`~/Documents/qops/git/drakeydb`) has its own container.

## Decisions made while grilling (all confirmed by Jon)

| # | Decision |
|---|---|
| D1 | A peer's **full sync keeps Dragonfly LOADING semantics** (clients get `-LOADING` for the merge-load duration). Steady state fully writable. Serve-during-merge stays future work. |
| D2 | `--replicaof` accepts a **comma-separated list** (`h1:p1,h2:p2,[::1]:p3`) — only meaningful with `--active_replica`; >1 target without it is a boot error. In active mode the node **also loads its own snapshot** at boot (upstream skips the snapshot when `--replicaof` is set). |
| D3 | P3 prerequisites pulled into P2: **(a)** pytest harness gives every instance a distinct node identity by default; **(b)** a peer presenting **our own uuid is refused** (peer mode only — non-active behaviour stays byte-identical to upstream). |
| D4 | Review cadence: Opus 5 **per task** (spec-compliance review, then code-quality review) **plus a final whole-branch review**. Sonnet 5 implements. |
| D5 | **KeyDB parity:** without `--multi_master`, `REPLICAOF h p` **replaces** the single peer (still writable, no flush); with `--multi_master` it **appends**. `REPLICAOF REMOVE h p` and `REPLICAOF NO ONE` work in both. |
| D6 | INFO: `connected_masters:N` + **one line per peer** `masterN:host=..,port=..,link_status=up\|down,last_io_seconds_ago=..,sync_in_progress=0\|1[,node_uuid=..]` (mirrors `slaveN:`; redis-py parses it into a dict). `role:master` stays. |
| D7 | `REPLICAOF` is **accepted while another peer is mid-full-sync** (new peer connects+greets, then queues behind the sync gate). No upstream-style LOADING rejection on the active path. |
| D8 | In P2 scope: reject `REPLTAKEOVER` / `DFLY TAKEOVER` on an active node; Redis-path (`InitiatePSync`) gets the same no-flush/override guard; `PauseReplication` pauses peers; peer uuids are registered in `PeerRegistry` on handshake. |
| D9 | Branch `feat/phase2-fanin` (rename from `plan-dev-task`); spec+plan committed under `docs/superpowers/` **and** `docs/PLAN.md` updated (P1 convention). |
| D10 | New long-lived build container for this worktree, fresh debug build. |
| D11 | Final gate: full `ninja` + `ctest -L DFLY`, `replication_test.py`, `replication_config_test.py`, **full** `replication_resilience_test.py`, **full** `cluster_test.py`, `multimaster_test.py`, pre-commit clean, Opus branch review. |

Other calls made in design (not asked, defaults stated): duplicate `REPLICAOF` of an attached peer
is a no-op `OK`; peers share the global `--masterauth/--masteruser/--tls_replication` (per-peer
credentials are v2); `ROLE` output unchanged (`master`); no Prometheus per-peer metrics (P8);
peers that lack the UUID exchange (stock Dragonfly masters) are accepted (uuid just absent).

---

## Design

### D-1. Verified facts the design rests on (from code, `33da9919`)

- `Replica::Start()` is synchronous (resolve→connect→`Greet()`), returns `GenericError` to the
  `REPLICAOF` caller; `EnableReplication()` is the async variant used by boot `--replicaof`
  (`replica.cc:117-170`). `MainReplicationFb` (`replica.cc:236-329`) flips shard state via
  `SetShardStates(true/false)` at 239/326 — the **only** writer of `EngineShard::is_replica_`,
  which gates expiry/eviction (`engine_shard.cc:846`), lazy expiry (`db_slice.cc:1470`), memory
  limit (`db_slice.cc:815`), cache eviction (`db_slice.cc:827`).
- `ServerState::is_master` (READONLY gate `main_service.cc:1446`) is flipped **only** by
  `ServerFamily::SetMasterFlagOnAllThreads` (`server_family.cc:352`), called at `3499`
  (`ReplicaOfInternal`), `3422`, `3561`. `Replica` never touches it.
- Full sync: `InitiateDflySync` enters LOADING via refcounted `service_.RequestLoadingState()`
  only when all flows are FULL (`replica.cc:682-687`), then `FlushAll()` at **696**; the
  `absl::Cleanup` at 625-633 calls `RemoveLoadingState()` before the function returns (stable
  sync starts after). Partial syncs never enter LOADING nor flush. Redis path `InitiatePSync`:
  LOADING at 518, flush at 525-529, loader at 531-534 (no override flag).
- `FullSyncDflyFb` already calls `rdb_loader_->SetOverrideExistingKeys(true)` (`replica.cc:1093`)
  and `DbSlice::AddOrUpdate` overwrites unconditionally (`rdb_load.cc:3238`,
  `db_slice.cc:1311-1339`): **merge = skip the flush; nothing else.** The flag only silences a
  per-key WARNING. `RdbLoader::ShouldDiscardKey` drops already-expired keys when `is_master`
  (`rdb_load.cc:3477`) — consistent with "expires keys itself".
- Journal-applied commands bypass both LOADING and READONLY gates (`is_replicating`,
  `main_service.cc:1392,1446`): peer B's stable stream keeps applying while peer C's full sync
  holds LOADING. Nothing serializes concurrent full syncs today.
- Handshake order replica→master: `REPLCONF listening-port` → `capa eof capa psync2` →
  **`UUID`** (P1, `replica.cc:363`) → **`capa dragonfly`** (`replica.cc:386`, master side creates
  the sync session in `ReplConf` `server_family.cc:3596-3618`) → `CLIENT-ID` → `CLIENT-VERSION`
  → `DFLY FLOW/SYNC/STARTSTABLE`. The only existing consumer-refusal gate is
  `server_family.cc:3574-3580` keyed on `!IsMaster()` (won't fire for an active node). Dragonfly
  does not serve `SYNC/PSYNC`, so refusing in `ReplConf` covers every consumer.
- `ReplicaOfArgs::FromCmdArgs` (`server_family.cc:797-823`) parses `NO ONE | host port [slots]`;
  `REPLICAOF` arity is -3 so `REPLICAOF REMOVE h p` passes arity. `--replicaof` is a custom
  `ReplicaOfFlag{host,port}` parsed by `AbslParseFlag` (`server_family.cc:180-220`,
  `find_last_of(':')`, IPv6 brackets); consumed in `Init` at 1292-1298 where it **suppresses**
  `LoadFromSnapshot()`.
- Boot flag validators run in `dfly_main.cc:1136-1143` before the proactor pool
  (`ValidateServerTlsFlags() || ValidateClientTlsFlags() || ...`); `InitNodeIdentityOrExit` exits
  from `ServerFamily::Init` (precedent for Init-time exits).
- INFO replication master branch: `server_family.cc:2974-2995` (`append(key, value)` writes
  `key:value\r\n` into `info`); `slaveN:` lines are gated by `show_managed_info`; `lag=` must stay
  trailing in `slaveN:` (regex in `replication_test.py:909`).
- pytest harness: `DflyInstanceFactory.create()` (`instance.py:431-460`) injects defaults with
  `args.setdefault`, version-gated (`if version > 1.37: ...`); `cwd` is the **session** tmp dir
  (`conftest.py:211-223`), so instances without `dir=` share one `drakeydb.uuid`. `DflyInstance`
  keeps its args across `stop()/start()`. `dbfilename` defaults to `""` (no snapshot on shutdown).
  Multi-source INFO today repeats identical keys (`server_family.cc:3019-3024`).
- P1 artifacts: `PeerRegistry` (`multi_master.h`, `AddOrGet` idempotent, no removal),
  `node_identity.h` (`IsValidNodeUuid`, `NormalizeNodeUuid`, `ParseReplconfUuidReply` returns a
  normalized uuid, `--node_uuid`), `ServerFamily::node_uuid()`, `multi_master_test.cc` fixtures
  (`PeerRegistryFiberTest` pool fixture, `MultiMasterFamilyTest : BaseFamilyTest` with
  `absl::FlagSaver`), CMake `helio_cxx_test(multi_master_test dfly_test_lib LABELS DFLY)`.

### D-2. Approaches considered

1. **Peer-mode flag on `Replica` + `PeerReplicationManager` (chosen).** Four guarded sites in
   `replica.cc`, upstream state machine reused verbatim, manager in a new file. Smallest diff,
   lowest merge risk, everything testable through existing fixtures.
2. Subclass `PeerReplica : Replica` with virtual hooks. Cleaner OOP but requires virtualizing
   upstream's private methods (`SetShardStates`, flush sites, `Greet`) → larger, conflict-prone
   diff for no behavioural gain.
3. Reuse `ADDREPLICAOF`/`cluster_replicas_`. Requires the node to be a read-only replica first
   (`!IsMaster()`), flips shard states per instance (pre-existing last-writer-wins bug), flushes
   on full sync, slot ranges mandatory → would need all the same guards plus restructuring.

### D-3. Flags and validation (`multi_master.h/.cc`, `dfly_main.cc`)

- `ABSL_FLAG(bool, active_replica, false, ...)` — stay a writable master while replicating from
  the masters given to `REPLICAOF`/`--replicaof` (KeyDB active-replica). Boot-only.
- `ABSL_FLAG(bool, multi_master, false, ...)` — let `REPLICAOF` attach several masters at once
  (fan-in). Requires `--active_replica` (KeyDB multi-master). Boot-only.
- Accessors `bool IsActiveReplica(); bool IsMultiMaster();` (plain `absl::GetFlag` reads).
- `bool ValidateMultiMasterFlags()`: `--multi_master` without `--active_replica` → error;
  `--active_replica` with non-empty `--cluster_mode`, non-empty `--tiered_prefix`, or
  `--experimental_cascaded_partial_sync=true` → error. Logs `LOG(ERROR)` and returns false; wired
  into the `dfly_main.cc:1140` conjunction (one token) so a bad combination exits 1 before the
  proactor pool exists. `--replicaof` with >1 target and no `--active_replica` is checked in
  `ServerFamily::Init` (the flag struct is TU-local there) and exits 1 like `InitNodeIdentityOrExit`.
- `--multi_master_no_forward`, `--multi_master_stream_lww`, `--replica_quorum` are NOT declared
  in P2 (declared by the phase that consumes them: P3, P5, P8).

### D-4. Peer-mode `Replica` (`replica.h/.cc`)

```cpp
class SyncGate;      // server/peer_replication.h
class PeerRegistry;  // server/multi_master.h

// drakeydb: configuration of a peer-mode Replica — an active node consuming from one of its
// masters. A peer-mode Replica never flips the process into read-only replica mode, never flushes
// the local dataset on full sync (it merges; last-loaded-wins until P6), serializes its full syncs
// through `sync_gate`, refuses a peer that presents our own node uuid, and registers the peer uuid.
struct ReplicaPeerMode {
  SyncGate* sync_gate = nullptr;    // null: full syncs are not serialized (unit tests)
  PeerRegistry* registry = nullptr; // null: peer uuids are not registered (unit tests)
};

Replica(std::string master_host, uint16_t port, Service* se, std::string_view id,
        std::optional<cluster::SlotRange> slot_range,
        std::optional<ReplicaPeerMode> peer_mode = std::nullopt);   // new trailing param
bool IsPeerMode() const { return peer_mode_.has_value(); }
```

Guarded sites (all additive, each with a `// drakeydb:` comment):
1. `MainReplicationFb`: `if (!IsPeerMode()) SetShardStates(true);` (line 239) and the mirror at 326.
2. `MainReplicationFb` step 3: before `InitiateDflySync/InitiatePSync`, `SyncGate::Lease` acquired
   when `peer_mode_ && peer_mode_->sync_gate`; an empty lease (cancelled) resets `state_mask_ &=
   R_ENABLED` and `continue`s; the lease is released when the block exits (RAII), i.e. after full
   sync completes and before stable sync starts.
3. `InitiateDflySync` 692-697: in peer mode skip `FlushSlots/FlushAll` (log "merging without flush").
   `RequestLoadingState()` stays (D1).
4. `InitiatePSync` 525-533: same skip; additionally `loader.SetOverrideExistingKeys(true)` in peer
   mode (the DF path already sets it at 1093).
5. `Greet()` right after P1's UUID block (after line 382): in peer mode, if
   `master_context_.master_node_uuid` is non-empty and equals
   `service_.server_family().node_uuid()` → `LOG(ERROR)` with both uuids and return
   `std::make_error_code(std::errc::operation_not_permitted)` (client sees
   `could not greet master Operation not permitted`; the log carries the details — same UX class as
   every other greet failure upstream). Otherwise, if `peer_mode_->registry` and the uuid is
   non-empty → `registry->AddOrGet(master_context_.master_node_uuid)` (idempotent; uuid already
   normalized by `ParseReplconfUuidReply`).

Nothing else changes: LSN/partial-sync bookkeeping, `Stop()`, `Pause()`, `GetSummary()` (already
carries host/port/link/last-io/sync-in-progress/`master_node_uuid`).

### D-5. `SyncGate` (`peer_replication.h/.cc`)

Serializes peer full-sync handshakes process-wide: at most one peer `Replica` is inside
`Initiate*Sync` at a time (LOADING is process-global; two merge-loads must not interleave).
FIFO among waiters (ticket deque). A lease is also **withheld while the process is LOADING for a
non-peer reason** (boot snapshot, `DEBUG LOAD`), so a peer merge never races another loader —
implemented through an injected `std::function<bool()> external_loading` (manager passes
`[] { auto* ss = ServerState::tlocal(); return ss && ss->gstate() == GlobalState::LOADING; }`;
unit tests pass their own flag). `Acquire(absl::FunctionRef<bool()> cancelled)` blocks on a
`fb2::CondVarAny` with `wait_for(lk, 100ms)` polling so a `Replica::Stop()` (which cancels
`exec_st_`) unblocks the waiter within ~100 ms without the gate knowing about `Stop`; returns an
empty `Lease` when cancelled. `Lease` is a movable RAII handle that releases on destruction and
wakes the next waiter (`notify_all`). Test hooks: `IsHeld()`, `NumWaiting()`.

### D-6. `PeerReplicationManager` (`peer_replication.h/.cc`)

```cpp
class PeerReplicationManager {
 public:
  struct Endpoint { std::string host; uint16_t port = 0; };
  enum class StartMode { kBlockingHandshake, kBackground };  // REPLICAOF vs boot --replicaof
  PeerReplicationManager(Service* service, PeerRegistry* registry);
  ~PeerReplicationManager();  // calls Shutdown()
  // REPLICAOF <host> <port> in active mode. kBlockingHandshake: connect+greet synchronously like
  // upstream REPLICAOF — on failure nothing changes and the error is returned. kBackground: start
  // the reconnect loop and return success. Without --multi_master the previously attached peer is
  // stopped only after the new one is registered (replace semantics); with it, appends. Attaching
  // an endpoint that is already attached is a no-op (*already_attached = true, success).
  GenericError Add(const Endpoint& ep, std::string_view self_replid, StartMode mode,
                   bool* already_attached);
  bool Remove(const Endpoint& ep);  // REPLICAOF REMOVE: stop that link; delivered data stays
  void RemoveAll();                 // REPLICAOF NO ONE: stop every link; data stays
  void Shutdown();                  // RemoveAll + refuse further Add (ServerFamily::Shutdown)
  void PauseAll(bool pause);        // ServerFamily::PauseReplication
  std::vector<ReplicaSummary> Summaries() const;  // attach order; one GetSummary() hop each
  std::vector<Endpoint> Endpoints() const;
  size_t Size() const;
  SyncGate& sync_gate();
 private:
  mutable util::fb2::Mutex mu_;
  std::vector<std::shared_ptr<Replica>> peers_ ABSL_GUARDED_BY(mu_);  // attach order
  bool closed_ ABSL_GUARDED_BY(mu_) = false;
  SyncGate gate_;
  Service* service_;
  PeerRegistry* registry_;
};
```
`Add` algorithm: (1) under `mu_`: refuse if `closed_`; if endpoint present → already, OK.
(2) unlocked: `make_shared<Replica>(host, port, service_, self_replid, nullopt,
ReplicaPeerMode{&gate_, registry_})`; blocking → `Start()` (error or
`IsContextCancelled()` → return error, nothing registered); background → `EnableReplication()`.
(3) under `mu_`: if `closed_` or the endpoint appeared meanwhile → `Stop()` the new one, return
accordingly; if `!IsMultiMaster()` move all current peers out (`replaced`); push the new one.
(4) unlocked: `Stop()` each replaced peer; blocking → `StartMainReplicationFiber(nullopt)`
(same ordering as `ReplicaOfInternal`: register, then start the fiber). Endpoint equality is
exact string host + port. `Remove/RemoveAll/Shutdown/PauseAll/Summaries` copy the `shared_ptr`s out
under `mu_` and call `Replica` (thread-safe, hops to its proactor) outside the lock. `Replica`
objects are constructed on the calling proactor (REPLICAOF connection thread, or the
`GetNextProactor()` used by `Init`) — same as upstream.

### D-7. `ServerFamily` / `DflyCmd` wiring (additive)

- `server_family.h`: `class PeerReplicationManager;` forward decl; member
  `std::unique_ptr<PeerReplicationManager> peers_;`; private
  `void ReplicaOfActive(facade::ParsedArgs args, CommandContext* cmd_cntx, ActionOnConnectionFail on_error);`.
- ctor: `peers_ = std::make_unique<PeerReplicationManager>(&service_, &peer_registry_);` (after
  `dfly_cmd_`); `server_family.cc` includes `server/peer_replication.h`.
- `ReplicaOfInternal` first statement: `if (IsActiveReplica()) return ReplicaOfActive(args, cmd_cntx, on_error);`
  (before `FromCmdArgs`, because the grammar differs and there is no LOADING rejection — D7).
- `ReplicaOfActive`: `ParsePeerReplicaOfArgs` → `kNoOne` → `peers_->RemoveAll()` + OK;
  `kRemove` → `Remove` or error `"Not attached to the specified master"`; `kAdd` → `LOG(INFO)`,
  `peers_->Add(..., master_replid(), mode, &already)` with mode from `on_error`
  (`kReturnOnError`→blocking, `kContinueReplication`→background) → OK or `ec.Format()`.
- `ReplConf` after the cascaded gate (3580): `if (IsActiveReplica()) return
  cmd_cntx->SendError("Replicating from an active-replica node is not supported");` — the single
  choke point: no sync session can ever be created, so `DFLY FLOW/SYNC/...` fail with their
  existing id-not-found errors. P3 replaces this with peer admission.
- `ReplTakeOver` after parsing, before the lock: error
  `"REPLTAKEOVER is not supported on an active-replica node"`. `DflyCmd::TakeOver` (after
  `RETURN_ON_PARSE_ERROR`): `"TAKEOVER is not supported on an active-replica node"` (unreachable in
  P2 — no sessions — but defensive for P3).
- INFO: in the master branch right after `append("master_replid", master_replid_)`:
  `if (IsActiveReplica()) info.append(RenderPeerReplicationInfo(peers_->Summaries(), IsMultiMaster(), show_managed_info));`
  Non-active INFO is byte-identical to upstream.
- `Shutdown`: `peers_->Shutdown();` right after `StopAllClusterReplicas();` (same `pb_task_` lambda).
- `PauseReplication`: `peers_->PauseAll(pause);` after the existing block.
- `Init` (1292-1298): `if (flag.peers.size() > 1 && !IsActiveReplica()) { LOG(ERROR) << ...; exit(1); }`;
  in active mode call `LoadFromSnapshot()` first (D2), then `Replicate(host, port)` for **every**
  entry of `flag.peers` inside one `GetNextProactor()->Await`. Non-active: unchanged.
- `ReplicaOfFlag` gains `std::vector<std::pair<std::string, std::string>> peers;` (host/port mirror
  `peers[0]`); `AbslParseFlag` splits on `,` and parses each piece with the existing single-endpoint
  logic (extracted into a static helper), `AbslUnparseFlag` joins with `,`.
- `ReplicaOfNoOne`, `AddReplicaOf`, `ROLE`, `Metrics` untouched: active mode never reaches
  `ReplicaOfNoOne` (handled in `ReplicaOfActive`), `AddReplicaOf` already errors for masters,
  `ROLE` reports `master` (peers not listed), no `replica_side_info` metrics for the active node.

### D-8. INFO rendering (`multi_master.h/.cc`)

`std::string RenderPeerReplicationInfo(const std::vector<ReplicaSummary>& peers, bool multi_master, bool show_peer_lines)` returns
```
active_replica:1\r\n
multi_master:<0|1>\r\n
connected_masters:<peers.size()>\r\n
master<i>:host=<host>,port=<port>,link_status=<up|down>,last_io_seconds_ago=<n>,sync_in_progress=<0|1>[,node_uuid=<uuid>]\r\n   (only if show_peer_lines)
```
`link_status=up` iff `master_link_established`; `sync_in_progress` = `full_sync_in_progress`;
`node_uuid` appended last only when non-empty (trailing optional field like `slaveN:`'s ordering
rule — nothing parses `masterN:` by regex yet, but keep `node_uuid` last for consistency).

### D-9. Test harness identity (`tests/dragonfly/instance.py`)

In `DflyInstanceFactory.create()`, after the version-gated defaults:
`if version >= 100 and "dir" not in args: args.setdefault("node_uuid", str(uuid.uuid4()))`.
Instances with an explicit `dir` keep file-based persistent identity (production behaviour, all P1
tests); the rest — which share the session cwd — get a unique ephemeral identity that survives
`stop()/start()` because `DflyInstance.args` is fixed at construction. Old-binary tests
(`version=1.x`, e.g. `test_replicate_old_master`) are excluded by the version guard.
Known remaining sharing: tests that pass `dir="{DRAGONFLY_TMP}/"` for master+replica
(`replication_specific_test.py:1610`) share a uuid — harmless in P2 (refusal is peer-mode only),
recorded as a P3 note.

### D-10. Semantics summary (documented in PLAN.md; caveats agreed earlier)

- Peer full sync merges (override existing keys, last-loaded-wins); keys present only locally are
  kept; `REPLICAOF REMOVE`/`NO ONE` keep delivered data. Conflicting same-key writes across
  peers resolve by arrival order until P5/P6.
- `FLUSHALL` executed on a peer is applied on the active node (wipes everything, incl. other
  peers' data) — KeyDB parity, documented hazard (PLAN.md caveats).
- A peer master restart (new replid) triggers a full re-merge (no flush) — stale keys the peer no
  longer has remain (no delete propagation; tombstones are v2).
- Per-peer auth/TLS: global flags only. `WAIT` returns 0 on the active node (no consumers).
  `DEBUG LOAD` on an active node pauses peers' loaders/reconnects (not stable-stream apply).

---

## File map

| File | Change |
|---|---|
| `src/server/multi_master.h/.cc` (P1) | + flags `active_replica`/`multi_master`, `IsActiveReplica()`, `IsMultiMaster()`, `ValidateMultiMasterFlags()`, `PeerReplicaOfCmd` + `ParsePeerReplicaOfArgs()`, `RenderPeerReplicationInfo()` |
| `src/server/peer_replication.h/.cc` (new) | `SyncGate`, `PeerReplicationManager` |
| `src/server/replica.h/.cc` | `ReplicaPeerMode`, ctor param, `IsPeerMode()`, 5 guarded sites |
| `src/server/server_family.h/.cc` | `peers_` member, `ReplicaOfActive`, hooks in `ReplicaOfInternal`/`ReplConf`/`ReplTakeOver`/INFO/`Shutdown`/`PauseReplication`/ctor/`Init`, `--replicaof` list parser |
| `src/server/dflycmd.cc` | `TakeOver` guard |
| `src/server/dfly_main.cc` | `ValidateMultiMasterFlags()` in the validator conjunction |
| `src/server/CMakeLists.txt` | `peer_replication.cc` source; `peer_replication_test` registration + `check_dfly` dep |
| `src/server/multi_master_test.cc` (P1) | + flag validation, arg parser, INFO render, `ActiveReplicaFamilyTest` command-level tests |
| `src/server/peer_replication_test.cc` (new) | `SyncGate` fiber tests, `PeerReplicationManager` family tests |
| `tests/dragonfly/instance.py` | per-instance `--node_uuid` default |
| `tests/dragonfly/multimaster_test.py` (P1) | + Phase 2 fan-in suite |
| `docs/PLAN.md`, `docs/UPSTREAM-SYNC.md`, `docs/superpowers/specs/…`, `docs/superpowers/plans/…` | status, decisions, watchlist, spec/plan copies |

## Global constraints

- Non-active (`--active_replica` off) behaviour must stay byte-identical to upstream: every new
  code path is gated by `IsActiveReplica()`/`IsPeerMode()`. INFO emits the new fields only in
  active mode.
- Upstream-file diffs stay small and additive (`docs/UPSTREAM-SYNC.md`): `server_family.cc`
  ≈ 60 added lines, `replica.cc` ≈ 35, `dflycmd.cc` ≈ 3, `dfly_main.cc` 1. Never edit `helio/`,
  `main_service.cc`, `engine_shard.cc`, `dash.h`, `compact_object.*`.
- Keep `lag=` the trailing field of `slaveN:` lines; do not add a second `lag=` anywhere.
- C++: clang-format (100 cols), `// drakeydb:` comment on each hook in an upstream file, no
  `std::mutex`/`std::thread` (fibers only), new files carry `// Copyright 2026, drakeydb authors.`
  Python: black, 100 cols. Commit subject/body lines ≤ 100 chars; trailer
  `Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>` (implementer's model), suffix `(P2)`
  on feature commits like P1.
- Tests run inside the build container (`drakeydb-p2`, worktree mounted at `/src`):
  C++ `docker exec drakeydb-p2 bash -lc "cd /src/build-dbg && ninja -j4 <target> && ./<target> --gtest_filter='<F>'"`;
  pytest `docker exec drakeydb-p2 bash -lc "cd /src && DRAGONFLY_PATH=/src/build-dbg/dragonfly /tmp/tv/bin/python3 -m pytest tests/dragonfly/<file> -k '<expr>' -xvs"`.
  `DRAGONFLY_PATH` must be absolute. Format on the host: `~/.venvs/precommit/bin/pre-commit run --files <files>`.

---

## Execution workflow

- Orchestrator: this session (Fable 5) runs Task 0 itself, then for each task dispatches a
  **Sonnet 5** implementer subagent (`model: "sonnet"`, fresh context, full task text + Global
  constraints + container commands), waits, then dispatches an **Opus 5** spec-compliance reviewer
  and an **Opus 5** code-quality reviewer (`model: "opus"`) per superpowers:subagent-driven-development;
  fix loops go back to the implementer. After the last task: Opus 5 whole-branch review, then the
  final gate, then the PR.
- Each task ends with a commit on `feat/phase2-fanin`. Push only when the user asks / at PR time.
- Branch: `git branch -m plan-dev-task feat/phase2-fanin` (Task 0). PR target
  `darkspadez/drakeydb` main: `gh pr create --repo darkspadez/drakeydb` (the default remote is the
  upstream parent and fails).

---

## Tasks

### Task 0: Environment, branch, spec/plan docs (orchestrator)

**Files:** `docs/superpowers/specs/2026-08-22-phase2-fanin-design.md` (create),
`docs/superpowers/plans/2026-08-22-phase2-fanin.md` (create).

- [ ] `git branch -m plan-dev-task feat/phase2-fanin`; `git submodule update --init --recursive`
      (helio is empty in this worktree).
- [ ] `orbctl start`; `docker images` — if a committed deps image from P1 exists (look for
      `drakeydb*`), use it; else `ubuntu:24.04` + the apt package list from
      `docs/build-from-source.md` plus `binutils redis-tools redis-server python3-venv`.
      `docker run -d --name drakeydb-p2 --security-opt seccomp=unconfined -v "$PWD":/src -w /src <image> sleep infinity`
      (`seccomp=unconfined` is mandatory — io_uring). Only one container builds at a time (8 GB VM).
- [ ] Kick off in background: `docker exec drakeydb-p2 bash -lc "./helio/blaze.sh -DWITH_AWS=OFF -DWITH_GCP=OFF && cd build-dbg && ninja -j4 dragonfly multi_master_test"`
      (`-j4`, default parallelism OOM-kills cc1plus). Then
      `docker exec drakeydb-p2 bash -lc "python3 -m venv /tmp/tv && /tmp/tv/bin/pip install -r tests/dragonfly/requirements.txt"`.
      Host: `python3 -m venv ~/.venvs/precommit && ~/.venvs/precommit/bin/pip install pre-commit` if missing.
- [ ] Write the spec (this file's Context + Decisions + Design + File map + Global constraints) to
      `docs/superpowers/specs/2026-08-22-phase2-fanin-design.md` and this whole plan to
      `docs/superpowers/plans/2026-08-22-phase2-fanin.md`; `pre-commit run --files` both;
      `git commit -m "docs: Phase 2 fan-in design spec and implementation plan (P2)"`.
- [ ] Smoke once the build is up: `docker exec drakeydb-p2 bash -lc "cd /src/build-dbg && ./multi_master_test"` → all P1 cases pass.

---

### Task 1: Flags, accessors, validation, main() hook

**Files:**
- Modify: `src/server/multi_master.h`, `src/server/multi_master.cc`, `src/server/dfly_main.cc:1140`
- Test: `src/server/multi_master_test.cc`

**Interfaces:**
- Produces: `bool IsActiveReplica(); bool IsMultiMaster(); bool ValidateMultiMasterFlags();`
  (namespace `dfly`, declared in `multi_master.h`), flags `active_replica`, `multi_master`.

- [ ] **Step 1: failing tests** — append to `src/server/multi_master_test.cc` (add
  `ABSL_DECLARE_FLAG(bool, active_replica); ABSL_DECLARE_FLAG(bool, multi_master); ABSL_DECLARE_FLAG(std::string, cluster_mode); ABSL_DECLARE_FLAG(std::string, tiered_prefix); ABSL_DECLARE_FLAG(bool, experimental_cascaded_partial_sync);`
  next to the existing declares; include `absl/flags/reflection.h` for `absl::FlagSaver`):

```cpp
TEST(MultiMasterFlags, DefaultsAreValidAndOff) {
  absl::FlagSaver saver;
  EXPECT_FALSE(IsActiveReplica());
  EXPECT_FALSE(IsMultiMaster());
  EXPECT_TRUE(ValidateMultiMasterFlags());
}

TEST(MultiMasterFlags, MultiMasterRequiresActiveReplica) {
  absl::FlagSaver saver;
  absl::SetFlag(&FLAGS_multi_master, true);
  EXPECT_FALSE(ValidateMultiMasterFlags());
  absl::SetFlag(&FLAGS_active_replica, true);
  EXPECT_TRUE(ValidateMultiMasterFlags());
  EXPECT_TRUE(IsActiveReplica());
  EXPECT_TRUE(IsMultiMaster());
}

TEST(MultiMasterFlags, ActiveReplicaRejectsIncompatibleFlags) {
  absl::FlagSaver saver;
  absl::SetFlag(&FLAGS_active_replica, true);
  absl::SetFlag(&FLAGS_cluster_mode, "emulated");
  EXPECT_FALSE(ValidateMultiMasterFlags());
  absl::SetFlag(&FLAGS_cluster_mode, "");
  absl::SetFlag(&FLAGS_tiered_prefix, "/tmp/x");
  EXPECT_FALSE(ValidateMultiMasterFlags());
  absl::SetFlag(&FLAGS_tiered_prefix, "");
  absl::SetFlag(&FLAGS_experimental_cascaded_partial_sync, true);
  EXPECT_FALSE(ValidateMultiMasterFlags());
  absl::SetFlag(&FLAGS_experimental_cascaded_partial_sync, false);
  EXPECT_TRUE(ValidateMultiMasterFlags());
}
```
- [ ] **Step 2:** `ninja -j4 multi_master_test` → compile failure (symbols undefined).
- [ ] **Step 3: implement** — `multi_master.h` (after the includes, before `PeerRegistry`):

```cpp
// --active_replica / --multi_master accessors (boot-only flags, declared in multi_master.cc).
bool IsActiveReplica();
bool IsMultiMaster();

// Validates the multi-master flag combination: --multi_master requires --active_replica, and
// --active_replica is incompatible with --cluster_mode, tiering (--tiered_prefix) and
// --experimental_cascaded_partial_sync. Logs and returns false on a bad combination. Called from
// main() before the proactor pool starts (see dfly_main.cc), like the TLS/snapshot validators.
bool ValidateMultiMasterFlags();
```
  `multi_master.cc`:
```cpp
#include "absl/flags/declare.h"
#include "absl/flags/flag.h"
#include "base/logging.h"

ABSL_FLAG(bool, active_replica, false,
          "drakeydb: stay a writable master while replicating from the masters given to "
          "REPLICAOF / --replicaof (KeyDB active-replica). Boot-only.");
ABSL_FLAG(bool, multi_master, false,
          "drakeydb: let REPLICAOF attach several masters at once (fan-in). Requires "
          "--active_replica (KeyDB multi-master). Boot-only.");
ABSL_DECLARE_FLAG(std::string, cluster_mode);
ABSL_DECLARE_FLAG(std::string, tiered_prefix);
ABSL_DECLARE_FLAG(bool, experimental_cascaded_partial_sync);

namespace dfly {

bool IsActiveReplica() { return absl::GetFlag(FLAGS_active_replica); }
bool IsMultiMaster() { return absl::GetFlag(FLAGS_multi_master); }

bool ValidateMultiMasterFlags() {
  const bool active = absl::GetFlag(FLAGS_active_replica);
  if (absl::GetFlag(FLAGS_multi_master) && !active) {
    LOG(ERROR) << "--multi_master requires --active_replica";
    return false;
  }
  if (!active)
    return true;
  if (!absl::GetFlag(FLAGS_cluster_mode).empty()) {
    LOG(ERROR) << "--active_replica is incompatible with --cluster_mode";
    return false;
  }
  if (!absl::GetFlag(FLAGS_tiered_prefix).empty()) {
    LOG(ERROR) << "--active_replica is incompatible with tiering (--tiered_prefix)";
    return false;
  }
  if (absl::GetFlag(FLAGS_experimental_cascaded_partial_sync)) {
    LOG(ERROR) << "--active_replica is incompatible with --experimental_cascaded_partial_sync";
    return false;
  }
  return true;
}
```
  `dfly_main.cc:1140`: extend the conjunction with `|| !dfly::ValidateMultiMasterFlags()`
  (`server/multi_master.h` is already reachable through `server/server_family.h`; add a direct
  `#include "server/multi_master.h"` next to the other `server/` includes for clarity).
- [ ] **Step 4:** `ninja -j4 multi_master_test && ./multi_master_test --gtest_filter='MultiMasterFlags.*'` → 3 PASS; then `ninja -j4 dragonfly` still links.
- [ ] **Step 5:** pre-commit on the 4 files; commit `feat: --active_replica/--multi_master flags with boot validation (P2)`.

---

### Task 2: Active-mode REPLICAOF grammar parser + INFO renderer

**Files:** Modify `src/server/multi_master.h/.cc`; Test `src/server/multi_master_test.cc`.

**Interfaces:**
- Produces:
```cpp
struct PeerReplicaOfCmd {
  enum class Kind : uint8_t { kAdd, kRemove, kNoOne };
  Kind kind = Kind::kAdd;
  std::string host;
  uint16_t port = 0;
};
// Active-mode REPLICAOF grammar: "<host> <port>" | "REMOVE <host> <port>" | "NO ONE".
// Slot ranges (cluster fan-in syntax) are rejected: active mode is incompatible with cluster mode.
nonstd::expected<PeerReplicaOfCmd, facade::ErrorReply> ParsePeerReplicaOfArgs(facade::ParsedArgs args);
// INFO replication block for an active node (see D-8).
std::string RenderPeerReplicationInfo(const std::vector<ReplicaSummary>& peers, bool multi_master, bool show_peer_lines);
```
  Includes needed in `multi_master.h`: `<nonstd/expected.hpp>`, `"facade/facade_types.h"`,
  `"server/replica_types.h"`. Precedent for the parser: `ReplicaOfArgs::FromCmdArgs`
  (`server_family.cc:797-823`, `CmdArgParser`, `Positive<uint16_t>` from `facade/cmd_arg_parser.h`).

- [ ] **Step 1: failing tests** (append to `multi_master_test.cc`; helper builds args like
  `ServerFamily::Replicate` does: `CmdArgVec v{...}; CmdArgList list = absl::MakeSpan(v);`):

```cpp
namespace {
nonstd::expected<PeerReplicaOfCmd, facade::ErrorReply> ParsePeer(std::vector<std::string> words) {
  CmdArgVec vec;
  for (auto& w : words)
    vec.emplace_back(w);
  CmdArgList list = absl::MakeSpan(vec);
  return ParsePeerReplicaOfArgs(list);
}
}  // namespace

TEST(PeerReplicaOfArgs, ParsesAddRemoveAndNoOne) {
  auto add = ParsePeer({"localhost", "6379"});
  ASSERT_TRUE(add.has_value());
  EXPECT_EQ(PeerReplicaOfCmd::Kind::kAdd, add->kind);
  EXPECT_EQ("localhost", add->host);
  EXPECT_EQ(6379, add->port);

  auto rem = ParsePeer({"REMOVE", "10.0.0.7", "7000"});
  ASSERT_TRUE(rem.has_value());
  EXPECT_EQ(PeerReplicaOfCmd::Kind::kRemove, rem->kind);
  EXPECT_EQ("10.0.0.7", rem->host);
  EXPECT_EQ(7000, rem->port);

  auto none = ParsePeer({"NO", "ONE"});
  ASSERT_TRUE(none.has_value());
  EXPECT_EQ(PeerReplicaOfCmd::Kind::kNoOne, none->kind);
  auto none_lc = ParsePeer({"no", "one"});
  ASSERT_TRUE(none_lc.has_value());
}

TEST(PeerReplicaOfArgs, RejectsBadForms) {
  EXPECT_FALSE(ParsePeer({"localhost"}).has_value());
  EXPECT_FALSE(ParsePeer({"localhost", "0"}).has_value());
  EXPECT_FALSE(ParsePeer({"localhost", "70000"}).has_value());
  EXPECT_FALSE(ParsePeer({"localhost", "abc"}).has_value());
  EXPECT_FALSE(ParsePeer({"REMOVE", "localhost"}).has_value());
  EXPECT_FALSE(ParsePeer({"localhost", "6379", "0", "100"}).has_value());  // slot range
  EXPECT_FALSE(ParsePeer({"NO"}).has_value());
}

TEST(PeerReplicationInfo, RendersCountsAndPeerLines) {
  ReplicaSummary up{};
  up.host = "localhost"; up.port = 7001; up.master_link_established = true;
  up.full_sync_in_progress = false; up.master_last_io_sec = 3;
  up.master_node_uuid = "01234567-89ab-4cde-8f01-23456789abcd";
  ReplicaSummary down{};
  down.host = "10.0.0.9"; down.port = 7002; down.master_link_established = false;
  down.full_sync_in_progress = true; down.master_last_io_sec = 0;
  std::string s = RenderPeerReplicationInfo({up, down}, true, true);
  EXPECT_EQ(
      "active_replica:1\r\nmulti_master:1\r\nconnected_masters:2\r\n"
      "master0:host=localhost,port=7001,link_status=up,last_io_seconds_ago=3,"
      "sync_in_progress=0,node_uuid=01234567-89ab-4cde-8f01-23456789abcd\r\n"
      "master1:host=10.0.0.9,port=7002,link_status=down,last_io_seconds_ago=0,"
      "sync_in_progress=1\r\n",
      s);
  EXPECT_EQ("active_replica:1\r\nmulti_master:0\r\nconnected_masters:2\r\n",
            RenderPeerReplicationInfo({up, down}, false, false));
  EXPECT_EQ("active_replica:1\r\nmulti_master:0\r\nconnected_masters:0\r\n",
            RenderPeerReplicationInfo({}, false, true));
}
```
- [ ] **Step 2:** build → compile failure.
- [ ] **Step 3: implement** in `multi_master.cc` (mirror `FromCmdArgs`; `using facade::CmdArgParser; using facade::Positive;` or qualify):

```cpp
nonstd::expected<PeerReplicaOfCmd, facade::ErrorReply> ParsePeerReplicaOfArgs(
    facade::ParsedArgs args) {
  PeerReplicaOfCmd cmd;
  facade::CmdArgParser parser(args);
  if (parser.Check("NO")) {
    parser.ExpectTag("ONE");
    cmd.kind = PeerReplicaOfCmd::Kind::kNoOne;
  } else {
    if (parser.Check("REMOVE"))
      cmd.kind = PeerReplicaOfCmd::Kind::kRemove;
    cmd.host = parser.Next<std::string>();
    cmd.port = parser.Next<facade::Positive<uint16_t>>("port is out of range");
    if (auto err = parser.TakeError(); err)
      return nonstd::make_unexpected(facade::ErrorReply("port is out of range"));
  }
  if (parser.HasNext())
    return nonstd::make_unexpected(
        facade::ErrorReply("slot ranges are not supported in active-replica mode"));
  if (auto err = parser.TakeError(); err)
    return nonstd::make_unexpected(err.MakeReply());
  return cmd;
}

std::string RenderPeerReplicationInfo(const std::vector<ReplicaSummary>& peers,
                                      bool multi_master, bool show_peer_lines) {
  std::string out = absl::StrCat("active_replica:1\r\nmulti_master:", multi_master ? 1 : 0,
                                 "\r\nconnected_masters:", peers.size(), "\r\n");
  if (!show_peer_lines)
    return out;
  for (size_t i = 0; i < peers.size(); ++i) {
    const ReplicaSummary& p = peers[i];
    absl::StrAppend(&out, "master", i, ":host=", p.host, ",port=", p.port,
                    ",link_status=", p.master_link_established ? "up" : "down",
                    ",last_io_seconds_ago=", p.master_last_io_sec,
                    ",sync_in_progress=", p.full_sync_in_progress ? 1 : 0);
    if (!p.master_node_uuid.empty())
      absl::StrAppend(&out, ",node_uuid=", p.master_node_uuid);
    absl::StrAppend(&out, "\r\n");
  }
  return out;
}
```
  (If `Next<std::string>()` on a missing arg does not set a parser error — check
  `cmd_arg_parser.h` — add an explicit `if (!parser.HasNext()) return unexpected(syntax)` before
  reading host. The "NO" without "ONE" case must fail via `ExpectTag`.)
- [ ] **Step 4:** `./multi_master_test --gtest_filter='PeerReplicaOfArgs.*:PeerReplicationInfo.*'` → PASS.
- [ ] **Step 5:** pre-commit; commit `feat: active-mode REPLICAOF grammar parser and INFO renderer (P2)`.

---

### Task 3: `SyncGate` (serialized, FIFO, cancellable peer sync gate)

**Files:** Create `src/server/peer_replication.h`, `src/server/peer_replication.cc`,
`src/server/peer_replication_test.cc`; Modify `src/server/CMakeLists.txt:95,152,186`.

**Interfaces:**
- Produces (`peer_replication.h`, namespace `dfly`):
```cpp
class SyncGate {
 public:
  using ExternalLoadingFn = std::function<bool()>;
  explicit SyncGate(ExternalLoadingFn external_loading = nullptr);
  class Lease {
   public:
    Lease() = default;
    Lease(Lease&& o) noexcept;
    Lease& operator=(Lease&& o) noexcept;
    ~Lease();                      // releases if held
    explicit operator bool() const { return gate_ != nullptr; }
   private:
    friend class SyncGate;
    explicit Lease(SyncGate* g) : gate_(g) {}
    SyncGate* gate_ = nullptr;
  };
  // Blocks until this caller holds the gate (FIFO among waiters) and external_loading() is false.
  // Returns an empty Lease if cancelled() becomes true while waiting (checked every 100ms and on
  // every release). Must be called from a fiber.
  Lease Acquire(absl::FunctionRef<bool()> cancelled);
  bool IsHeld() const;
  size_t NumWaiting() const;
 private:
  void Release();
  mutable util::fb2::Mutex mu_;
  util::fb2::CondVarAny cv_;
  bool held_ = false;
  uint64_t next_ticket_ = 0;
  std::deque<uint64_t> waiters_;  // FIFO of tickets still waiting
  ExternalLoadingFn external_loading_;
};
```
- [ ] **Step 1: CMake + failing tests.** `CMakeLists.txt:95` → `node_identity.cc multi_master.cc peer_replication.cc`;
  after line 152 add `helio_cxx_test(peer_replication_test dfly_test_lib LABELS DFLY)`; append
  `peer_replication_test` to the `add_dependencies(check_dfly ...)` list (line 186).
  Create `peer_replication_test.cc` with the pool fixture copied from
  `multi_master_test.cc:253-272` (name it `SyncGateTest`, include `util/fibers/pool.h`,
  `absl/flags/declare.h`, `ABSL_DECLARE_FLAG(bool, force_epoll)`) and:

  Rule for these tests: the bare gtest thread is not a fiber, so it never touches the gate
  directly — every `IsHeld()/NumWaiting()/Acquire` call runs inside a pool fiber (`LaunchFiber`)
  or inside `pp_->at(i)->Await([&] { ... })`; the main thread only launches, `Await`s, sleeps with
  `std::this_thread::sleep_for`, and `Join`s (same discipline as `PeerRegistryFiberTest`).

```cpp
namespace {
// Polls `pred` inside proactor `i` until true (or 5s). Returns the final value.
bool AwaitUntil(util::ProactorPool* pp, unsigned i, absl::FunctionRef<bool()> pred) {
  for (int n = 0; n < 1000; ++n) {
    if (pp->at(i)->Await([&] { return pred(); }))
      return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  return false;
}
}  // namespace

TEST_F(SyncGateTest, SerializesAndIsFifo) {
  SyncGate gate;
  std::atomic_int order_idx{0};
  std::array<int, 3> order{-1, -1, -1};
  auto never = [] { return false; };
  util::fb2::Fiber a = pp_->at(0)->LaunchFiber(util::fb2::Launch::post, [&] {
    auto lease = gate.Acquire(never);
    ASSERT_TRUE(lease);
    while (gate.NumWaiting() < 2)  // hold until two waiters are queued behind us
      util::ThisFiber::SleepFor(std::chrono::milliseconds(5));
    EXPECT_TRUE(gate.IsHeld());
    order[order_idx++] = 0;
  });
  ASSERT_TRUE(AwaitUntil(pp_.get(), 1, [&] { return gate.IsHeld(); }));
  util::fb2::Fiber b = pp_->at(1)->LaunchFiber(util::fb2::Launch::post, [&] {
    auto lease = gate.Acquire(never);
    ASSERT_TRUE(lease);
    order[order_idx++] = 1;
  });
  ASSERT_TRUE(AwaitUntil(pp_.get(), 1, [&] { return gate.NumWaiting() >= 1; }));
  util::fb2::Fiber c = pp_->at(0)->LaunchFiber(util::fb2::Launch::post, [&] {
    auto lease = gate.Acquire(never);
    ASSERT_TRUE(lease);
    order[order_idx++] = 2;
  });
  a.Join();
  b.Join();
  c.Join();
  EXPECT_EQ((std::array<int, 3>{0, 1, 2}), order);
  EXPECT_FALSE(pp_->at(0)->Await([&] { return gate.IsHeld(); }));
  EXPECT_EQ(0u, pp_->at(0)->Await([&] { return gate.NumWaiting(); }));
}

TEST_F(SyncGateTest, CancelledWaiterGetsEmptyLease) {
  SyncGate gate;
  std::atomic_bool cancel{false}, release{false};
  util::fb2::Fiber holder = pp_->at(0)->LaunchFiber(util::fb2::Launch::post, [&] {
    auto lease = gate.Acquire([] { return false; });
    while (!release.load())
      util::ThisFiber::SleepFor(std::chrono::milliseconds(5));
  });
  ASSERT_TRUE(AwaitUntil(pp_.get(), 1, [&] { return gate.IsHeld(); }));
  std::atomic_bool got{true};
  util::fb2::Fiber waiter = pp_->at(1)->LaunchFiber(util::fb2::Launch::post, [&] {
    auto lease = gate.Acquire([&] { return cancel.load(); });
    got = static_cast<bool>(lease);
  });
  ASSERT_TRUE(AwaitUntil(pp_.get(), 0, [&] { return gate.NumWaiting() >= 1; }));
  cancel = true;
  waiter.Join();  // returns within ~100ms although the holder never released
  EXPECT_FALSE(got.load());
  EXPECT_EQ(0u, pp_->at(0)->Await([&] { return gate.NumWaiting(); }));
  release = true;
  holder.Join();
  EXPECT_FALSE(pp_->at(0)->Await([&] { return gate.IsHeld(); }));
}

TEST_F(SyncGateTest, ExternalLoadingDefersGrant) {
  std::atomic_bool loading{true};
  SyncGate gate([&] { return loading.load(); });
  std::atomic_bool acquired{false};
  util::fb2::Fiber f = pp_->at(0)->LaunchFiber(util::fb2::Launch::post, [&] {
    auto lease = gate.Acquire([] { return false; });
    acquired = static_cast<bool>(lease);
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(250));
  EXPECT_FALSE(acquired.load());
  loading = false;
  f.Join();
  EXPECT_TRUE(acquired.load());
}
```
- [ ] **Step 2:** `ninja -j4 peer_replication_test` → compile failure.
- [ ] **Step 3: implement** `peer_replication.cc`:

```cpp
SyncGate::SyncGate(ExternalLoadingFn external_loading)
    : external_loading_(std::move(external_loading)) {}

SyncGate::Lease::Lease(Lease&& o) noexcept : gate_(std::exchange(o.gate_, nullptr)) {}
SyncGate::Lease& SyncGate::Lease::operator=(Lease&& o) noexcept {
  if (this != &o) { if (gate_) gate_->Release(); gate_ = std::exchange(o.gate_, nullptr); }
  return *this;
}
SyncGate::Lease::~Lease() { if (gate_) gate_->Release(); }

SyncGate::Lease SyncGate::Acquire(absl::FunctionRef<bool()> cancelled) {
  std::unique_lock lk(mu_);
  const uint64_t my = next_ticket_++;
  waiters_.push_back(my);
  auto ready = [&] {
    return !held_ && waiters_.front() == my && !(external_loading_ && external_loading_());
  };
  while (!ready()) {
    if (cancelled()) {
      waiters_.erase(std::find(waiters_.begin(), waiters_.end(), my));
      cv_.notify_all();  // under mu_: fb2::CondVarAny must be notified while holding the lock
      return Lease{};
    }
    cv_.wait_for(lk, std::chrono::milliseconds(100));
  }
  waiters_.pop_front();
  held_ = true;
  return Lease{this};
}

void SyncGate::Release() {
  std::lock_guard lk(mu_);
  held_ = false;
  cv_.notify_all();  // under mu_ (helio CondVarAny has no internal lock)
}
bool SyncGate::IsHeld() const { std::lock_guard lk(mu_); return held_; }
size_t SyncGate::NumWaiting() const { std::lock_guard lk(mu_); return waiters_.size(); }
```
  (`external_loading_()` is evaluated under `mu_` — it must be cheap and non-blocking: the
  production lambda reads `ServerState::tlocal()->gstate()`.)
- [ ] **Step 4:** `./peer_replication_test --gtest_filter='SyncGateTest.*'` → PASS (run 5× to
  check for flakes: `--gtest_repeat=5`).
- [ ] **Step 5:** pre-commit; commit `feat: SyncGate - FIFO, cancellable peer full-sync serialization (P2)`.

---

### Task 4: Peer mode in `Replica`

**Files:** Modify `src/server/replica.h:30-60,166-217`, `src/server/replica.cc:98-107,236-329,356-394,510-534,676-705`.

**Interfaces:**
- Consumes: `SyncGate` (Task 3), `PeerRegistry::AddOrGet`, `ServerFamily::node_uuid()`.
- Produces: `struct ReplicaPeerMode { SyncGate* sync_gate = nullptr; PeerRegistry* registry = nullptr; };`,
  `Replica(..., std::optional<ReplicaPeerMode> peer_mode = std::nullopt)`, `bool IsPeerMode() const`.

- [ ] **Step 1:** `replica.h` — forward-declare `class SyncGate; class PeerRegistry;` near
  `class DflyShardReplica;`, add the `ReplicaPeerMode` struct (doc comment from D-4) before
  `class Replica`, the new ctor parameter, `bool IsPeerMode() const { return peer_mode_.has_value(); }`
  in the public section, and `std::optional<ReplicaPeerMode> peer_mode_;` among the members.
- [ ] **Step 2:** `replica.cc` ctor: add `peer_mode_(peer_mode)` to the init list (`#include "server/peer_replication.h"` and `"server/multi_master.h"`).
- [ ] **Step 3:** `MainReplicationFb`:
```cpp
  // drakeydb: a peer-mode replica belongs to an active node that stays a master, so it must not
  // flip the shards into replica mode (that would stop expiry/eviction process-wide).
  if (!IsPeerMode())
    SetShardStates(true);
  ...
    // 3. Initiate full sync
    if ((state_mask_ & R_SYNC_OK) == 0) {
      // drakeydb: peer full syncs are serialized process-wide (LOADING is global); an empty lease
      // means we were stopped while waiting.
      SyncGate::Lease sync_lease;
      if (peer_mode_ && peer_mode_->sync_gate) {
        sync_lease = peer_mode_->sync_gate->Acquire([this] { return !exec_st_.IsRunning(); });
        if (!sync_lease) {
          state_mask_ &= R_ENABLED;
          continue;
        }
      }
      if (HasDflyMaster()) { ... unchanged ... }
  ...
  // Revert shard states to normal state.
  if (!IsPeerMode())
    SetShardStates(false);
```
- [ ] **Step 4:** `InitiateDflySync` (692-697):
```cpp
      passed_full_sync_ = false;
      if (IsPeerMode()) {
        // drakeydb: an active node merges the peer's snapshot into its own dataset instead of
        // replacing it. RdbLoader already overrides existing keys (last-loaded-wins until P6).
        LOG(INFO) << "Peer full sync: merging without flush " << this;
      } else if (slot_range_.has_value()) {
        JournalExecutor{&service_}.FlushSlots(slot_range_.value());
      } else {
        JournalExecutor{&service_}.FlushAll();
      }
```
  `InitiatePSync` (525-533): same `if (IsPeerMode()) { LOG(INFO) ... } else if ... else ...` and
  after `loader.SetLoadUnownedSlots(true);` add `if (IsPeerMode()) loader.SetOverrideExistingKeys(true);  // drakeydb: merge`.
- [ ] **Step 5:** `Greet()` after the P1 UUID block (after line 382):
```cpp
  // drakeydb: peer-mode identity checks. A peer that presents our own uuid is a clone of this
  // node's data dir (P3's origin tagging would silently drop its entries) - refuse it. Otherwise
  // register the peer so later phases can map its uuid to an origin index.
  if (IsPeerMode() && !master_context_.master_node_uuid.empty()) {
    const std::string& self_uuid = service_.server_family().node_uuid();
    if (master_context_.master_node_uuid == self_uuid) {
      LOG(ERROR) << "Peer " << server().Description() << " presents our own node uuid "
                 << self_uuid << "; refusing to replicate from a clone of this node";
      return std::make_error_code(std::errc::operation_not_permitted);
    }
    if (peer_mode_->registry)
      peer_mode_->registry->AddOrGet(master_context_.master_node_uuid);
  }
```
- [ ] **Step 6:** build `ninja -j4 dragonfly multi_master_test dragonfly_test` and run
  `./multi_master_test` and `./dragonfly_test --gtest_filter='DflyEngineTest.ReplicaofRejectOnLoad'`
  → green (non-peer path unchanged). No new unit test is possible in-process for these guards
  (no C++ test constructs a live `Replica`); they are exercised by Task 5's manager tests
  (construction/Start/background paths) and Task 9's pytest suite.
- [ ] **Step 7:** pre-commit; commit `feat: peer-mode Replica - no shard flip, merge without flush, sync gate, uuid guard (P2)`.

---

### Task 5: `PeerReplicationManager`

**Files:** Modify `src/server/peer_replication.h/.cc`; Test `src/server/peer_replication_test.cc`.

**Interfaces:**
- Consumes: `Replica` peer mode (Task 4), `SyncGate` (Task 3), `PeerRegistry`, `IsMultiMaster()`.
- Produces: the class in D-6 exactly (`Endpoint`, `StartMode`, `Add`, `Remove`, `RemoveAll`,
  `Shutdown`, `PauseAll`, `Summaries`, `Endpoints`, `Size`, `sync_gate`).
  `peer_replication.h` forward-declares `class Replica; class Service; class PeerRegistry;`
  and includes `server/replica_types.h`, `server/common.h` (GenericError), `absl/base/thread_annotations.h`.

- [ ] **Step 1: failing tests** in `peer_replication_test.cc` — fixture modelled on
  `MultiMasterFamilyTest` (`multi_master_test.cc:298-314`): `class PeerManagerFamilyTest : public BaseFamilyTest`
  whose ctor sets `FLAGS_dir` to `base::GetTestTempPath("peer_mgr")`, `FLAGS_active_replica=true`,
  `FLAGS_multi_master=true` (declare the flags; `absl::FlagSaver saver_;` last member). **Every**
  manager call — including `Size()/Summaries()/Remove/Shutdown` — runs inside a proactor fiber
  (`pp_->at(0)->Await([&] { ... })`): the gtest thread is not a fiber and must not take
  `fb2::Mutex`es; end each test with an explicit `mgr.Shutdown()` inside the `Await` so the
  destructor (on the gtest thread) finds nothing left to stop.

```cpp
using StartMode = PeerReplicationManager::StartMode;
constexpr char kReplid[] = "0123456789abcdef0123456789abcdef01234567";

TEST_F(PeerManagerFamilyTest, BlockingAddToUnreachablePortFailsAndLeavesNoPeer) {
  PeerRegistry reg;
  reg.Init(GenerateNodeUuid());
  PeerReplicationManager mgr(service_.get(), &reg);
  pp_->at(0)->Await([&] {
    bool already = false;
    GenericError ec = mgr.Add({"localhost", 1}, kReplid, StartMode::kBlockingHandshake, &already);
    EXPECT_TRUE(ec) << "port 1 must refuse the connection";
    EXPECT_FALSE(already);
    EXPECT_EQ(0u, mgr.Size());
    EXPECT_TRUE(mgr.Summaries().empty());
    mgr.Shutdown();
  });
}

TEST_F(PeerManagerFamilyTest, BackgroundAddRemoveNoOneAndDuplicate) {
  PeerRegistry reg;
  reg.Init(GenerateNodeUuid());
  PeerReplicationManager mgr(service_.get(), &reg);
  pp_->at(0)->Await([&] {
    bool already = false;
    EXPECT_FALSE(mgr.Add({"localhost", 1}, kReplid, StartMode::kBackground, &already));
    EXPECT_FALSE(already);
    EXPECT_FALSE(mgr.Add({"localhost", 1}, kReplid, StartMode::kBackground, &already));
    EXPECT_TRUE(already);  // duplicate endpoint is a no-op
    EXPECT_FALSE(mgr.Add({"localhost", 2}, kReplid, StartMode::kBackground, &already));
    EXPECT_FALSE(already);
    EXPECT_EQ(2u, mgr.Size());
    auto sums = mgr.Summaries();
    ASSERT_EQ(2u, sums.size());
    EXPECT_EQ("localhost", sums[0].host);
    EXPECT_EQ(1, sums[0].port);
    EXPECT_EQ(2, sums[1].port);
    EXPECT_FALSE(sums[0].master_link_established);
    EXPECT_TRUE(mgr.Remove({"localhost", 1}));
    EXPECT_FALSE(mgr.Remove({"localhost", 1}));
    EXPECT_EQ(1u, mgr.Size());
    mgr.RemoveAll();
    EXPECT_EQ(0u, mgr.Size());
    mgr.Shutdown();
  });
}

TEST_F(PeerManagerFamilyTest, SinglePeerReplaceWhenMultiMasterOff) {
  absl::SetFlag(&FLAGS_multi_master, false);
  PeerRegistry reg;
  reg.Init(GenerateNodeUuid());
  PeerReplicationManager mgr(service_.get(), &reg);
  pp_->at(0)->Await([&] {
    bool already = false;
    EXPECT_FALSE(mgr.Add({"localhost", 1}, kReplid, StartMode::kBackground, &already));
    EXPECT_FALSE(mgr.Add({"localhost", 2}, kReplid, StartMode::kBackground, &already));
    ASSERT_EQ(1u, mgr.Size());
    EXPECT_EQ(2, mgr.Endpoints()[0].port);
    mgr.Shutdown();
  });
}

TEST_F(PeerManagerFamilyTest, ShutdownRefusesFurtherAdds) {
  PeerRegistry reg;
  reg.Init(GenerateNodeUuid());
  PeerReplicationManager mgr(service_.get(), &reg);
  pp_->at(0)->Await([&] {
    bool already = false;
    EXPECT_FALSE(mgr.Add({"localhost", 1}, kReplid, StartMode::kBackground, &already));
    mgr.Shutdown();
    EXPECT_TRUE(mgr.Add({"localhost", 2}, kReplid, StartMode::kBackground, &already));
    EXPECT_EQ(0u, mgr.Size());
  });
}
```
  (Background peers to ports 1/2 loop reconnecting every 500 ms with WARNING logs until stopped —
  expected; `Remove/RemoveAll/Shutdown` join them within ~1 s.)
- [ ] **Step 2:** build → compile failure.
- [ ] **Step 3: implement** per D-6 (Add/Remove/RemoveAll/Shutdown/PauseAll/Summaries/Endpoints/Size;
  `~PeerReplicationManager() { Shutdown(); }`; gate constructed with
  `[] { auto* ss = ServerState::tlocal(); return ss != nullptr && ss->gstate() == GlobalState::LOADING; }`
  — include `server/server_state.h`). `Add` step (2) must run with `mu_` released (blocking
  network); `Replica` ctor receives `ReplicaPeerMode{&gate_, registry_}`.
- [ ] **Step 4:** `./peer_replication_test` → all PASS; `./multi_master_test` still green.
- [ ] **Step 5:** pre-commit; commit `feat: PeerReplicationManager - add/remove/no-one for active-node peer links (P2)`.

---

### Task 6: `ServerFamily` + `DflyCmd` wiring (REPLICAOF, REPLCONF refusal, INFO, shutdown, takeover)

**Files:** Modify `src/server/server_family.h:149-400`, `src/server/server_family.cc:1176-1192,1379-1394,1659-1668,2974-2995,3435-3441,3510-3533,3570-3580`, `src/server/dflycmd.cc:501-516`; Test `src/server/multi_master_test.cc`.

**Interfaces:**
- Consumes: Tasks 1, 2, 5.
- Produces: `ServerFamily::ReplicaOfActive` (private), `peers_` member; runtime behaviour per D-7.

- [ ] **Step 1: failing tests** — in `multi_master_test.cc` add
  `class ActiveReplicaFamilyTest : public MultiMasterFamilyTest { protected: ActiveReplicaFamilyTest() { absl::SetFlag(&FLAGS_active_replica, true); absl::SetFlag(&FLAGS_multi_master, true); } };`
  (the base `saver_` restores both flags) and:

```cpp
TEST_F(ActiveReplicaFamilyTest, ReplconfRefusedWhileActive) {
  auto resp = Run({"replconf", "listening-port", "1"});
  EXPECT_THAT(resp, ErrArg("active-replica"));
  resp = Run({"replconf", "capa", "dragonfly"});
  EXPECT_THAT(resp, ErrArg("active-replica"));
}

TEST_F(ActiveReplicaFamilyTest, ReplTakeoverRefusedWhileActive) {
  EXPECT_THAT(Run({"repltakeover", "1"}), ErrArg("active-replica"));
}

TEST_F(ActiveReplicaFamilyTest, InfoShowsActiveFieldsAndStaysMaster) {
  auto resp = Run({"info", "replication"});
  std::string info{ToSV(resp.GetBuf())};
  EXPECT_NE(std::string::npos, info.find("role:master\r\n"));
  EXPECT_NE(std::string::npos, info.find("active_replica:1\r\n"));
  EXPECT_NE(std::string::npos, info.find("multi_master:1\r\n"));
  EXPECT_NE(std::string::npos, info.find("connected_masters:0\r\n"));
  EXPECT_EQ(std::string::npos, info.find("master_host:"));
}

TEST_F(ActiveReplicaFamilyTest, ReplicaOfGrammarAndNoPeersPaths) {
  EXPECT_THAT(Run({"replicaof", "remove", "localhost", "1"}), ErrArg("Not attached"));
  EXPECT_EQ("OK", Run({"replicaof", "no", "one"}));
  EXPECT_THAT(Run({"replicaof", "localhost", "6379", "0", "100"}), ErrArg("slot ranges"));
  // unreachable peer: error, nothing attached, still a writable master
  EXPECT_THAT(Run({"replicaof", "localhost", "1"}), ErrArg("could not connect"));
  std::string info{ToSV(Run({"info", "replication"}).GetBuf())};
  EXPECT_NE(std::string::npos, info.find("connected_masters:0\r\n"));
  EXPECT_EQ("OK", Run({"set", "k", "v"}));
}

TEST_F(MultiMasterFamilyTest, NonActiveInfoHasNoActiveFields) {
  std::string info{ToSV(Run({"info", "replication"}).GetBuf())};
  EXPECT_EQ(std::string::npos, info.find("active_replica:"));
  EXPECT_EQ(std::string::npos, info.find("connected_masters:"));
}
```
- [ ] **Step 2:** build → failures (`ErrArg` mismatches / missing fields).
- [ ] **Step 3: implement** exactly per D-7:
  - `server_family.h`: `class PeerReplicationManager;` forward decl after the includes; in the
    private section near `cluster_replicas_`: `std::unique_ptr<PeerReplicationManager> peers_;  // drakeydb: active-replica peer links`;
    near `ReplicaOfInternal`: `void ReplicaOfActive(facade::ParsedArgs args, CommandContext* cmd_cntx, ActionOnConnectionFail on_error);`.
  - `server_family.cc`: `#include "server/peer_replication.h"`; ctor line after `dfly_cmd_`:
    `peers_ = std::make_unique<PeerReplicationManager>(&service_, &peer_registry_);`.
  - `ReplicaOfInternal` first lines:
```cpp
  // drakeydb: an active node manages its masters through PeerReplicationManager (fan-in).
  if (IsActiveReplica()) {
    return ReplicaOfActive(args, cmd_cntx, on_error);
  }
```
  - New method (place after `ReplicaOfInternal`):
```cpp
void ServerFamily::ReplicaOfActive(facade::ParsedArgs args, CommandContext* cmd_cntx,
                                   ActionOnConnectionFail on_error) {
  auto cmd = ParsePeerReplicaOfArgs(args);
  if (!cmd.has_value()) {
    return cmd_cntx->SendError(cmd.error());
  }
  using Kind = PeerReplicaOfCmd::Kind;
  PeerReplicationManager::Endpoint ep{cmd->host, cmd->port};
  switch (cmd->kind) {
    case Kind::kNoOne:
      LOG(INFO) << "Detaching all peer masters";
      peers_->RemoveAll();
      return cmd_cntx->rb()->SendOk();
    case Kind::kRemove:
      LOG(INFO) << "Detaching peer master " << ep.host << ":" << ep.port;
      if (!peers_->Remove(ep)) {
        return cmd_cntx->SendError("Not attached to the specified master");
      }
      return cmd_cntx->rb()->SendOk();
    case Kind::kAdd: {
      LOG(INFO) << "Attaching peer master " << ep.host << ":" << ep.port;
      bool already = false;
      auto mode = on_error == ActionOnConnectionFail::kReturnOnError
                      ? PeerReplicationManager::StartMode::kBlockingHandshake
                      : PeerReplicationManager::StartMode::kBackground;
      GenericError ec = peers_->Add(ep, master_replid(), mode, &already);
      if (ec) {
        return cmd_cntx->SendError(ec.Format());
      }
      LOG_IF(INFO, already) << "Already attached to " << ep.host << ":" << ep.port;
      return cmd_cntx->rb()->SendOk();
    }
  }
}
```
  - `ReplConf` after the cascaded gate block:
```cpp
  // drakeydb: an active node does not serve replication consumers yet (Phase 3 admits peers).
  if (IsActiveReplica()) {
    return cmd_cntx->SendError("Replicating from an active-replica node is not supported");
  }
```
  - `ReplTakeOver` after `RETURN_ON_PARSE_ERROR`/negative check, before the lock:
    `if (IsActiveReplica()) return cmd_cntx->SendError("REPLTAKEOVER is not supported on an active-replica node");  // drakeydb`
  - `dflycmd.cc` `TakeOver` after `RETURN_ON_PARSE_ERROR(parser, cmd_cntx);`:
    `if (IsActiveReplica()) return cmd_cntx->SendError("TAKEOVER is not supported on an active-replica node");  // drakeydb`
  - INFO after `append("master_replid", master_replid_);`:
```cpp
      if (IsActiveReplica()) {  // drakeydb: peer links of an active node (fan-in)
        info.append(RenderPeerReplicationInfo(peers_->Summaries(), IsMultiMaster(), show_managed_info));
      }
```
    (check the name of the string `append` writes into — it is the local `string info` — and that
    `show_managed_info` is in scope; both are defined earlier in `Info()`.)
  - `Shutdown`: `peers_->Shutdown();  // drakeydb` after `StopAllClusterReplicas();`.
  - `PauseReplication`: `peers_->PauseAll(pause);  // drakeydb` after the `if (!IsMaster())` block.
- [ ] **Step 4:** `ninja -j4 dragonfly multi_master_test peer_replication_test && ./multi_master_test && ./peer_replication_test` → PASS;
  also `./dragonfly_test --gtest_filter='DflyEngineTest.ReplicaofRejectOnLoad'` and
  `./server_family_test` green.
- [ ] **Step 5:** pre-commit; commit `feat: wire active-replica REPLICAOF/INFO/REPLCONF refusal into ServerFamily (P2)`.

---

### Task 7: Test-harness per-instance identity

**Files:** Modify `tests/dragonfly/instance.py:1-20,431-460`.

- [ ] **Step 1: failing test** — append to `tests/dragonfly/multimaster_test.py`:
```python
async def test_harness_gives_each_instance_a_distinct_identity(df_factory: DflyInstanceFactory):
    # No dir= on purpose: these share the session cwd, so without the harness default they would
    # all load the same drakeydb.uuid file.
    a = df_factory.create(proactor_threads=1)
    b = df_factory.create(proactor_threads=1)
    df_factory.start_all([a, b])
    ua = (await a.client().info("replication"))["node_uuid"]
    ub = (await b.client().info("replication"))["node_uuid"]
    assert UUID_RE.match(ua) and UUID_RE.match(ub)
    assert ua != ub
    a.stop()
    a.start()  # same args -> same identity across restart
    assert (await a.client().info("replication"))["node_uuid"] == ua
```
  Run: `pytest tests/dragonfly/multimaster_test.py -k harness_gives -xvs` → FAIL (`ua == ub`).
- [ ] **Step 2: implement** in `instance.py` (`import uuid` at the top), inside `create()` after the `fiber_safety_margin` default:
```python
        # drakeydb: every instance needs its own node identity. Instances with an explicit --dir
        # persist one in <dir>/drakeydb.uuid (production behaviour); the rest share the session
        # cwd, so give them a unique ephemeral --node_uuid instead. A restarted DflyInstance keeps
        # its args, hence its identity. Old upstream binaries (version < 100) lack the flag.
        if version >= 100 and "dir" not in args:
            args.setdefault("node_uuid", str(uuid.uuid4()))
```
- [ ] **Step 3:** run the new test → PASS; run `pytest tests/dragonfly/multimaster_test.py -x` (P1 suite, all use `dir=`) and a quick
  `pytest tests/dragonfly/replication_test.py -k 'test_replication_all and not cache' -x` → PASS.
- [ ] **Step 4:** `black`/pre-commit; commit `test: give every harness instance a distinct node identity by default (P2)`.

---

### Task 8: `--replicaof` list + snapshot load in active mode

**Files:** Modify `src/server/server_family.cc:93-104,180-224,1292-1298`.

- [ ] **Step 1: failing tests** — append to `multimaster_test.py`:
```python
async def test_replicaof_flag_list_requires_active_replica(df_factory: DflyInstanceFactory):
    node = df_factory.create(proactor_threads=1, replicaof="localhost:1,localhost:2")
    await assert_start_fails(node)


async def test_replicaof_flag_list_attaches_all_peers(df_factory: DflyInstanceFactory, port_picker):
    b = df_factory.create(proactor_threads=1, port=port_picker.get_available_port())
    c = df_factory.create(proactor_threads=1, port=port_picker.get_available_port())
    df_factory.start_all([b, c])
    a = df_factory.create(
        proactor_threads=2, active_replica="true", multi_master="true",
        replicaof=f"localhost:{b.port},localhost:{c.port}",
    )
    a.start()
    info = await wait_for_peers(a.client(), 2)
    assert {info["master0"]["port"], info["master1"]["port"]} == {b.port, c.port}
```
  (`wait_for_peers` helper is defined in Task 9 Step 1 — add it now; see there.) Run → first
  passes only if boot already rejects the list (it doesn't: parser takes `find_last_of(':')`),
  second FAILs.
- [ ] **Step 2: implement** — `ReplicaOfFlag` gains
  `std::vector<std::pair<string, string>> peers;  // drakeydb: every target of a comma-separated list; host/port mirror peers[0]`.
  Extract the body of `AbslParseFlag` (from `find_last_of` to the end) into
  `static bool ParseOneReplicaOf(std::string_view in, string* host, string* port, string* err)`;
  new `AbslParseFlag`: empty → `ReplicaOfFlag{}`; else `for (string_view piece : absl::StrSplit(in, ',', absl::SkipEmpty()))` parse each into `peers`; fail if none;
  `flag->host/port = peers.front()`. `AbslUnparseFlag`: `absl::StrJoin` of `host:port` pieces.
  `Init`:
```cpp
  // check for '--replicaof' before loading anything
  if (ReplicaOfFlag flag = GetFlag(FLAGS_replicaof); flag.has_value()) {
    if (flag.peers.size() > 1 && !IsActiveReplica()) {  // drakeydb
      LOG(ERROR) << "--replicaof with several targets requires --active_replica";
      exit(1);
    }
    if (IsActiveReplica()) {  // drakeydb: an active node keeps its own snapshot and merges peers
      LoadFromSnapshot();
    }
    service_.proactor_pool().GetNextProactor()->Await([this, &flag]() {
      for (const auto& [host, port] : flag.peers)
        this->Replicate(host, port);
    });
  } else {  // load from snapshot only if --replicaof is empty
    LoadFromSnapshot();
  }
```
- [ ] **Step 3:** build `dragonfly`; run both new tests + `pytest tests/dragonfly/replication_config_test.py -k test_replicaof_flag -x` → PASS.
- [ ] **Step 4:** pre-commit; commit `feat: --replicaof accepts a peer list and keeps the snapshot in active mode (P2)`.

---

### Task 9: pytest fan-in suite

**Files:** Modify `tests/dragonfly/multimaster_test.py` (append a `# ---- Phase 2: fan-in ----` section).

- [ ] **Step 1: helpers** (top of the P2 section; add `import async_timeout` and
  `from .utility import assert_eventually, wait_available_async`):
```python
def active_args(multi: bool = True, **extra):
    args = {"proactor_threads": 2, "active_replica": "true"}
    if multi:
        args["multi_master"] = "true"
    args.update(extra)
    return args


async def wait_for_peers(c, n, timeout=90):
    """Wait until INFO shows n attached peers, all link up and not syncing; returns the info."""
    async with async_timeout.timeout(timeout):
        while True:
            info = await c.info("replication")
            peers = [info.get(f"master{i}") for i in range(int(info.get("connected_masters", 0)))]
            if (
                len(peers) == n
                and all(p and p["link_status"] == "up" and p["sync_in_progress"] == 0 for p in peers)
            ):
                return info
            await asyncio.sleep(0.2)


async def wait_for_value(c, key, value, timeout=30):
    async with async_timeout.timeout(timeout):
        while (await c.get(key)) != value:
            await asyncio.sleep(0.1)


async def attach(c_a, *nodes):
    for n in nodes:
        assert await c_a.execute_command(f"REPLICAOF localhost {n.port}") == "OK"
```
- [ ] **Step 2: tests** (each fails until the feature exists; write all, run, fix):
```python
async def test_fanin_merges_two_masters_and_stays_writable(df_factory: DflyInstanceFactory):
    a = df_factory.create(**active_args())
    b = df_factory.create(proactor_threads=2)
    c = df_factory.create(proactor_threads=2)
    df_factory.start_all([a, b, c])
    c_a, c_b, c_c = a.client(), b.client(), c.client()
    assert (await c_b.info("replication"))["node_uuid"] != (await c_c.info("replication"))["node_uuid"]
    await c_b.execute_command("DEBUG POPULATE 300 b 50")
    await c_c.execute_command("DEBUG POPULATE 300 c 50")
    await c_a.set("a:own", "1")
    await attach(c_a, b, c)  # back-to-back: second REPLICAOF must not be rejected with LOADING
    info = await wait_for_peers(c_a, 2)
    assert info["role"] == "master" and info["active_replica"] == 1 and info["multi_master"] == 1
    assert {info["master0"]["port"], info["master1"]["port"]} == {b.port, c.port}
    assert info["master0"]["node_uuid"] in {
        (await c_b.info("replication"))["node_uuid"], (await c_c.info("replication"))["node_uuid"]
    }

    @assert_eventually(times=300)
    async def merged():
        assert await c_a.dbsize() == 601

    await merged()
    assert await c_a.get("a:own") == "1"
    assert await c_a.get("b:0") is not None and await c_a.get("c:0") is not None
    assert await c_a.set("a:new", "2")  # writable, no READONLY
    await c_b.set("live:b", "1")
    await c_c.set("live:c", "1")
    await wait_for_value(c_a, "live:b", "1")
    await wait_for_value(c_a, "live:c", "1")
    # the active node expires its own keys (shards were never flipped into replica mode)
    await c_a.set("a:ttl", "v", px=300)
    await wait_for_value(c_a, "a:ttl", None, timeout=10)
    assert (await c_a.info("replication"))["connected_slaves"] == 0


async def test_fanin_remove_and_no_one_keep_data(df_factory: DflyInstanceFactory):
    a = df_factory.create(**active_args())
    b = df_factory.create(proactor_threads=2)
    c = df_factory.create(proactor_threads=2)
    df_factory.start_all([a, b, c])
    c_a, c_b, c_c = a.client(), b.client(), c.client()
    await c_b.set("b:k", "1")
    await c_c.set("c:k", "1")
    await attach(c_a, b, c)
    await wait_for_peers(c_a, 2)
    await wait_for_value(c_a, "b:k", "1")
    await wait_for_value(c_a, "c:k", "1")
    assert await c_a.execute_command(f"REPLICAOF REMOVE localhost {b.port}") == "OK"
    info = await wait_for_peers(c_a, 1)
    assert info["master0"]["port"] == c.port
    assert await c_a.get("b:k") == "1"  # data delivered by B stays
    await c_b.set("b:after", "1")
    await asyncio.sleep(1.0)
    assert await c_a.get("b:after") is None  # B is detached
    with pytest.raises(redis.exceptions.ResponseError, match="Not attached"):
        await c_a.execute_command(f"REPLICAOF REMOVE localhost {b.port}")
    assert await c_a.execute_command("REPLICAOF NO ONE") == "OK"
    info = await c_a.info("replication")
    assert info["connected_masters"] == 0 and info["role"] == "master"
    assert await c_a.get("c:k") == "1"
    assert await c_a.set("still", "writable")


async def test_active_node_refuses_consumers_and_takeover(df_factory: DflyInstanceFactory):
    a = df_factory.create(**active_args())
    d = df_factory.create(proactor_threads=2)
    df_factory.start_all([a, d])
    c_a, c_d = a.client(), d.client()
    with pytest.raises(redis.exceptions.ResponseError, match="active-replica"):
        await c_a.execute_command("REPLCONF listening-port 1")
    with pytest.raises(redis.exceptions.ResponseError, match="active-replica"):
        await c_a.execute_command("REPLTAKEOVER 1")
    # A plain node pointing at A fails its synchronous greet (A refuses REPLCONF), so REPLICAOF
    # itself errors ("could not greet master ...") and D stays a master.
    with pytest.raises(redis.exceptions.ResponseError, match="could not greet master"):
        await c_d.execute_command(f"REPLICAOF localhost {a.port}")
    assert (await c_d.info("replication"))["role"] == "master"
    assert (await c_a.info("replication"))["connected_slaves"] == 0


async def test_same_uuid_peer_refused(df_factory: DflyInstanceFactory):
    a = df_factory.create(**active_args())
    a.start()
    c_a = a.client()
    clone = df_factory.create(proactor_threads=1, node_uuid=(await c_a.info("replication"))["node_uuid"])
    clone.start()
    with pytest.raises(redis.exceptions.ResponseError):
        await c_a.execute_command(f"REPLICAOF localhost {clone.port}")
    assert (await c_a.info("replication"))["connected_masters"] == 0


async def test_active_replica_single_peer_replaces(df_factory: DflyInstanceFactory):
    a = df_factory.create(**active_args(multi=False))
    b = df_factory.create(proactor_threads=2)
    c = df_factory.create(proactor_threads=2)
    df_factory.start_all([a, b, c])
    c_a, c_b = a.client(), b.client()
    await c_b.set("b:k", "1")
    await attach(c_a, b)
    await wait_for_peers(c_a, 1)
    await wait_for_value(c_a, "b:k", "1")
    await attach(c_a, c)
    info = await wait_for_peers(c_a, 1)
    assert info["multi_master"] == 0 and info["master0"]["port"] == c.port
    assert await c_a.get("b:k") == "1"


async def test_fanin_restart_remerge(df_factory: DflyInstanceFactory, tmp_path, port_picker):
    a_port = port_picker.get_available_port()
    a_args = active_args(dir=str(tmp_path / "a"), dbfilename="dump", port=a_port)
    a = df_factory.create(**a_args)
    b = df_factory.create(proactor_threads=2, port=port_picker.get_available_port())
    c = df_factory.create(proactor_threads=2, port=port_picker.get_available_port())
    df_factory.start_all([a, b, c])
    c_a, c_b, c_c = a.client(), b.client(), c.client()
    await c_b.execute_command("DEBUG POPULATE 200 b 50")
    await c_c.execute_command("DEBUG POPULATE 200 c 50")
    await attach(c_a, b, c)
    await wait_for_peers(c_a, 2)
    await c_a.set("a:own", "1")
    await wait_for_value(c_a, "b:199", await c_b.get("b:199"))  # B fully merged
    await wait_for_value(c_a, "c:199", await c_c.get("c:199"))  # C fully merged
    a.stop()  # dbfilename is set -> snapshot on shutdown
    await c_b.set("b:while_down", "1")
    a2 = df_factory.create(**a_args, replicaof=f"localhost:{b.port},localhost:{c.port}")
    a2.start()
    c_a2 = a2.client()
    await wait_for_peers(c_a2, 2)
    assert await c_a2.get("a:own") == "1"  # own snapshot was loaded, not suppressed by --replicaof
    await wait_for_value(c_a2, "b:while_down", "1")

    @assert_eventually(times=300)
    async def remerged():
        assert await c_a2.dbsize() == 200 + 200 + 1 + 1

    await remerged()
    assert await c_a2.set("after:restart", "ok")


async def test_multi_master_flag_requires_active_replica(df_factory: DflyInstanceFactory):
    await assert_start_fails(df_factory.create(proactor_threads=1, multi_master="true"))


async def test_active_replica_rejects_cluster_mode(df_factory: DflyInstanceFactory):
    await assert_start_fails(
        df_factory.create(proactor_threads=1, active_replica="true", cluster_mode="emulated")
    )
```
  Notes for the implementer: bool flags are passed as `"true"`; `wait_for_value(..., None)` waits
  for deletion; `DEBUG POPULATE 200 b 50` creates keys `b:0..b:199`; the restarted node is a *new*
  `DflyInstance` on the same `dir/dbfilename/port` so the boot `--replicaof` list can be passed
  (`DflyInstance.args` is fixed at creation). A plain node's `REPLICAOF` towards an active node
  surfaces upstream's `could not greet master Bad message` wrapper (our REPLCONF error text is in
  D's log, not in the reply) — hence the `match="could not greet master"`.
- [ ] **Step 3:** run `pytest tests/dragonfly/multimaster_test.py -xvs` (whole file, ~3-5 min) → all
  pass; re-run 3× (`--count` via pytest-repeat: `--count=3 -x`) to shake out flakes.
- [ ] **Step 4:** black; commit `test: Phase 2 fan-in pytest suite (P2)`.

---

### Task 10: Docs — PLAN.md status/record, UPSTREAM-SYNC watchlist, spec touch-ups

**Files:** Modify `docs/PLAN.md:6-13,209-216,306-312`, `docs/UPSTREAM-SYNC.md:45-56`;
`docs/superpowers/specs/2026-08-22-phase2-fanin-design.md` (sync any deviations discovered).

- [ ] `docs/PLAN.md`: status row `P2 — ✅ complete, verified | branch feat/phase2-fanin, PR #<n>`;
  P3 row "next up". Add a **P2 verification record** paragraph (exact numbers from Task 11).
  Under Phase 2 add "Delivered:" listing D1–D8 decisions as shipped semantics (LOADING kept,
  replace-vs-append, `masterN:` INFO lines, accept-and-queue, `SyncGate`, `ReplConf` refusal,
  `--replicaof` list + snapshot, peer uuids registered in `Greet`, duplicate-uuid refusal peer-mode
  only, harness identity via `--node_uuid` default) and "Deferred:" (per-peer auth/TLS, ROLE
  unchanged, metrics P8, serve-during-merge). Update the "New files" table (`peer_replication.*`,
  `peer_replication_test.cc`). Mark the two P3 prerequisites as done, and add P3 notes: (a) replace
  the `ReplConf` active-mode refusal with peer admission (version 65 + UUID); (b) peers' PING
  entries are re-recorded into the local journal (`replica.cc:1163`) — decide origin handling;
  (c) tests sharing `dir="{DRAGONFLY_TMP}/"` across master+replica still share a uuid.
- [ ] `docs/UPSTREAM-SYNC.md` watchlist: `server_family.cc` row += "`--replicaof` list parser,
  active-mode REPLICAOF/INFO/REPLCONF hooks"; `dfly_main.cc` row += "`ValidateMultiMasterFlags()`
  call"; `src/server/CMakeLists.txt` row += "`peer_replication.cc` + test registration"; new row
  `tests/dragonfly/instance.py` — "per-instance `--node_uuid` default".
- [ ] pre-commit; commit `docs: record Phase 2 delivery, decisions and P3 notes (P2)`.

---

### Task 11: Final gate, whole-branch review, PR (orchestrator)

- [ ] Full build: `docker exec drakeydb-p2 bash -lc "cd /src/build-dbg && ninja -j4"` (everything —
  `check_dfly` builds only a subset and unbuilt tests show as "Not Run").
- [ ] `ctest -L DFLY` (expect 88/88: P1's 86 + `peer_replication_test` + nothing else new — adjust
  to the actual count) — timeout 25 m.
- [ ] pytest (absolute `DRAGONFLY_PATH`, from `/src`): `multimaster_test.py`, `replication_test.py`,
  `replication_config_test.py`, `replication_resilience_test.py`, `cluster_test.py` (full; ≥ 30 min),
  `replication_specific_test.py::` the dir-sharing test at ~1610. Record pass counts + any flakes
  (re-run a flaky test 3× alone before triaging).
- [ ] Host: `~/.venvs/precommit/bin/pre-commit run --files $(git diff --name-only origin/main...HEAD)` clean.
- [ ] Opus 5 whole-branch review (`git diff origin/main...HEAD`) against this spec; fix loop via Sonnet.
- [ ] Task 10's verification record filled in, committed.
- [ ] `git push -u origin feat/phase2-fanin`; `gh pr create --repo darkspadez/drakeydb --title "feat: writable multi-source replica - active-replica fan-in (P2)" --body <concise summary per CLAUDE.md PR guidelines, Fixes/Refs PLAN.md Phase 2>`.
  Do **not** merge; the user merges.

---

## Verification (end-to-end)

1. Unit: `multi_master_test` (P1 21 + ~10 new), `peer_replication_test` (~7), all `ctest -L DFLY`.
2. Behaviour (pytest, Task 9): fan-in merge of two masters + writable + own expiry; REMOVE/NO ONE
   keep data; consumer + takeover refusal; clone-uuid refusal; single-peer replace without
   `--multi_master`; restart → re-merge with `--replicaof` list and own snapshot; flag validation at boot.
3. Regression: D11 gate (replication/config/resilience/cluster suites, INFO `lag=` regex
   consumers, `test_replication_info` metric invariants, `test_replicaof_flag`, `test_replica_of_replica`).
4. Manual smoke (optional, in the container): start B, C, A(`--active_replica --multi_master`),
   `redis-cli -p A REPLICAOF localhost B`, `... C`, `INFO replication` shows `master0/master1`,
   `SET` on A works, `redis-cli -p D REPLICAOF localhost A` logs the refusal on D.

## Risks / watch items

- `Replica::Stop()` on a peer waiting at the gate completes within ~100 ms (cancel poll); a peer
  holding the gate during a long merge delays other peers' *full* syncs only (partial syncs are
  quick but also gated — by design, to avoid interleaving).
- `ShouldDiscardKey` drops already-expired keys from peer RDBs when `is_master` — expected.
- Peer `FLUSHALL`/master restart semantics (D-10) are documented hazards, not bugs.
- Harness identity default only applies when `dir` is absent; tests sharing `{DRAGONFLY_TMP}/`
  still share a uuid (P3 note).
- `--replicaof` parser change touches upstream code (watchlisted); keep it a pure superset
  (single `host:port` parses exactly as before).
- Flake budget: fan-in tests poll with generous timeouts; if `test_fanin_restart_remerge` flakes on
  snapshot timing, wait for the `dump` file before restarting.
