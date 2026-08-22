# drakeydb Phase 2 — Writable Multi-Source Replica (Fan-In) — Design Spec


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
| D2 | `--replicaof` accepts a **comma-separated list** (`h1:p1,h2:p2,[::1]:p3`) — only meaningful with `--active_replica`; >1 target without it is a boot error. In active mode the node **also loads its own snapshot** at boot (upstream skips the snapshot when `--replicaof` is set). **(as built)** >1 target also requires `--multi_master`, not only `--active_replica` — without fan-in, `PeerReplicationManager::Add`'s replace semantics would silently keep only the last target. |
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

**(as built)** every `notify_all()` — the cancel path in `Acquire()`, and `Release()` — runs while
`mu_` is still held, not after releasing it: `util::fb2::CondVarAny` has no internal mutex of its
own (unlike `std::condition_variable_any`), so notifying outside the lock races the wait queue it
touches. The first landed version notified outside `mu_`; code review caught the race and it was
fixed before the branch was done (commit `4aa0a093`).

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

**(as built)** the private member shipped as `std::vector<PeerLink> peers_ ABSL_GUARDED_BY(mu_)`
with `struct PeerLink { Endpoint ep; std::shared_ptr<Replica> replica; }`, not the
`std::vector<std::shared_ptr<Replica>>` sketched above. Pairing the endpoint with the `Replica` at
`Add()` time lets `Add()`/`Remove()` identify a peer by the exact endpoint it was given, as a pure
in-memory comparison under `mu_` — never a `GetSummary()` hop while the lock is held. `Endpoints()`
is hop-free too; `Summaries()` still hops outside the lock (unavoidable — it reads live link
state).

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
  **(as built)** for a peer whose connection is refused immediately (the common unreachable-peer
  case), `ec` carries upstream's pre-existing generic `"replication cancelled"` text: by the time
  `Start()`'s `check_connection_error()` runs after `ConnectAndAuth()` fails, `exec_st_` has
  already flipped to cancelled, so it takes the generic branch instead of the specific
  `"could not connect to master: ..."` one — same wording classic (non-active) `REPLICAOF` hits
  for the same race (see `replica.cc`, and `multi_master_test.cc`'s
  `ReplicaOfGrammarAndNoPeersPaths`), not a new peer-specific message.
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
  **(as built)** >1 target also requires `--multi_master`: when `flag.peers.size() > 1`,
  `!IsActiveReplica()` keeps the existing exit, and `IsActiveReplica() && !IsMultiMaster()` now
  exits with `"--replicaof with several targets requires --multi_master"` too.
- `ReplicaOfFlag` gains `std::vector<std::pair<std::string, std::string>> peers;` (host/port mirror
  `peers[0]`); `AbslParseFlag` splits on `,` and parses each piece with the existing single-endpoint
  logic (extracted into a static helper — **(as built)** `ParseOneReplicaOf`), `AbslUnparseFlag`
  joins with `,`.
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
