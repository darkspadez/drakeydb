// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <absl/container/inlined_vector.h>

#include <atomic>
#include <boost/fiber/barrier.hpp>
#include <queue>
#include <variant>

#include "facade/facade_types.h"
#include "facade/redis_parser.h"
#include "io/io_buf.h"
#include "server/cluster/cluster_defs.h"
#include "server/execution_state.h"
#include "server/journal/tx_executor.h"
#include "server/journal/types.h"
#include "server/protocol_client.h"
#include "server/replica_types.h"
#include "server/version.h"
#include "util/fiber_socket_base.h"

namespace dfly {

class Service;
class ConnectionContext;
class JournalExecutor;
struct JournalReader;
class DflyShardReplica;
class SyncGate;            // server/peer_replication.h
class PeerIdentityClaims;  // server/peer_replication.h
class PeerRegistry;        // server/multi_master.h

// The attributes of the master we are connecting to.
struct MasterContext {
  std::string master_repl_id;
  std::string dfly_session_id;  // Sync session id for dfly sync.
  unsigned num_flows = 0;
  DflyVersion version = DflyVersion::VER1;
  std::string lineage_id;  // lineage id of master
  std::string master_node_uuid;
  uint64_t master_clock_ms = 0;  // drakeydb reply extension; 0 = unknown (KeyDB master)
};

// drakeydb: configuration of a peer-mode Replica -- an active node consuming from one of its
// masters. A peer-mode Replica never flips the process into read-only replica mode, never flushes
// the local dataset on full sync (it merges; last-loaded-wins until P6), serializes its full syncs
// through `sync_gate`, refuses self/duplicate peer uuids, and registers the peer uuid.
struct ReplicaPeerMode {
  SyncGate* sync_gate = nullptr;                  // null: full syncs are not serialized (tests)
  PeerRegistry* registry = nullptr;               // null: peer uuids are not registered (tests)
  PeerIdentityClaims* identity_claims = nullptr;  // null: duplicate uuids are not rejected (tests)
};

// This class manages replication from both Dragonfly and Redis masters.
class Replica : ProtocolClient {
 private:
  // The flow is : R_ENABLED -> R_TCP_CONNECTED -> (R_SYNCING) -> R_SYNC_OK.
  // SYNCING means that the initial ack succeeded. It may be optional if we can still load from
  // the journal offset.
  enum State : unsigned {
    R_ENABLED = 1,  // Replication mode is enabled. Serves for signaling shutdown.
    R_TCP_CONNECTED = 2,
    R_GREETED = 4,     // Initial handshake with the master is done.
    R_SYNCING = 8,     // In process of full sync with the master.
    R_SYNC_OK = 0x10,  // Signals successful ending of full-sync state, exclusive with R_SYNCING.
  };

 public:
  Replica(std::string master_host, uint16_t port, Service* se, std::string_view id,
          std::optional<cluster::SlotRange> slot_range,
          std::optional<ReplicaPeerMode> peer_mode = std::nullopt);
  ~Replica();

  // Spawns a fiber that runs until link with master is broken or the replication is stopped.
  // Returns true if initial link with master has been established or
  // false if it has failed.
  GenericError Start();
  using LastMasterSyncData = dfly::LastMasterSyncData;
  void StartMainReplicationFiber(std::optional<LastMasterSyncData> data);

  // Sets the server state to have replication enabled.
  // It is like Start(), but does not attempt to establish
  // a connection right-away, but instead lets MainReplicationFb do the work.
  void EnableReplication();

  std::optional<LastMasterSyncData> Stop();  // thread-safe

  void Pause(bool pause);

  std::error_code TakeOver(unsigned timeout, bool save_flag);

  bool IsContextCancelled() const {
    return !exec_st_.IsRunning();
  }

 private: /* Main standalone mode functions */
  // Coordinate state transitions. Spawned by start.
  void MainReplicationFb(std::optional<LastMasterSyncData> data);

  std::error_code Greet();  // Send PING and REPLCONF.

  std::error_code HandleCapaDflyResp();
  std::error_code ConfigureDflyMaster();

  std::error_code InitiatePSync();                                           // Redis full sync.
  std::error_code InitiateDflySync(std::optional<LastMasterSyncData> data);  // Dragonfly full sync.

  std::error_code ConsumeRedisStream();  // Redis stable state.
  std::error_code ConsumeDflyStream();   // Dragonfly stable state.

  void RedisStreamAcksFb();

  // Joins all the flows when doing sharded replication. This is called in two
  // places: Once at the end of full sync to join the full sync fibers, and twice
  // if a stable sync is interrupted to join the cancelled stable sync fibers.
  void JoinDflyFlows();
  void SetShardStates(bool replica);  // Call SetReplica(replica) on all shards.
  bool EnterLoadingState();

  // drakeydb: releases this peer-mode Replica's live UUID admission, if one was claimed.
  void ReleasePeerIdentityClaim();

  // Send DFLY ${kind} to the master instance.
  std::error_code SendNextPhaseRequest(std::string_view kind);

 private: /* Utility */
  struct PSyncResponse {
    // string - end of sync token (diskless)
    // size_t - size of the full sync blob (disk-based).
    // if fullsync is 0, it means that master can continue with partial replication.
    std::variant<std::string, size_t> fullsync;
  };

  std::error_code ParseReplicationHeader(base::IoBuf* io_buf, PSyncResponse* dest);

 public: /* Utility */
  using Summary = ReplicaSummary;

  Summary GetSummary() const;  // thread-safe, blocks fiber, makes a hop.

  bool HasDflyMaster() const {
    return !master_context_.dfly_session_id.empty();
  }

  // drakeydb: true if this Replica was constructed with peer_mode set (see ReplicaPeerMode).
  bool IsPeerMode() const {
    return peer_mode_.has_value();
  }

  // The replication id of the lineage root master. Equals the direct master's id, unless the
  // direct master advertised an ancestor id (cascaded replication), in which case it is the
  // ancestor's id. Used to negotiate partial sync when reconnecting up the chain.
  std::string GetLineageId() const {
    return master_context_.lineage_id;
  }

  std::vector<uint64_t> GetReplicaOffset() const;
  std::string GetSyncId() const;

  // Get the current replication phase based on state_mask_
  std::string GetCurrentPhase() const;

  std::string GetClientInfo() const;

  uint32_t GetClientId() const {
    return client_id_;
  }

  // Used *only* in TakeOver flow and replicaof no one. There is small data race if
  // thread_flow_map_ gets written by the MainReplicationFiber thread but
  // the chances for that are extremely rare.
  std::vector<unsigned> GetFlowMapAtIndex(size_t index) const;

  size_t GetRecCountExecutedPerShard(const std::vector<unsigned>& indexes) const;

  // Investigation-only (DEBUG REPLDIAG): bytes currently sitting unread in the
  // master socket's kernel receive buffer, or -1 if unavailable. Remove once closed.
  int GetMasterSocketUnreadBytes();

  // Start the journal in every shard thread at this replica's per-shard executed LSN, so the
  // journal continues the master's LSN numbering. Enables partial sync from the same source master
  // (failover) and cascaded partial sync (sub-replicas share the lineage root's LSN space).
  void StartJournalAtOwnLSN();

 private:
  ExecutionState exec_st_;

  util::fb2::ProactorBase* proactor_ = nullptr;
  Service& service_;
  MasterContext master_context_;

  // In redis replication mode.
  util::fb2::Fiber sync_fb_;
  util::fb2::Fiber acks_fb_;
  util::fb2::EventCount replica_waker_;

  std::vector<std::unique_ptr<DflyShardReplica>> shard_flows_;
  std::vector<std::vector<unsigned>> thread_flow_map_;  // a map from proactor id to flow list.

  // A vector of the last executer LSNs when a replication is interrupted.
  // Allows partial sync on reconnects.
  std::optional<std::vector<LSN>> last_journal_LSNs_;
  std::shared_ptr<MultiShardExecution> multi_shard_exe_;

  // Guard operations where flows might be in a mixed state (transition/setup)
  util::fb2::Mutex flows_op_mu_;

  // repl_offs - till what offset we've already read from the master.
  // ack_offs_ last acknowledged offset.
  // initial_repl_offs_ - master-supplied offset at FULLRESYNC; subtract from
  // repl_offs_ to derive bytes read since this connection was established.
  size_t repl_offs_ = 0, ack_offs_ = 0, initial_repl_offs_ = 0;
  unsigned state_mask_ = 0;  // see State enum above.

  // When replica starts full sync it is set to false and true when it completes the full sync.
  // Disconnects do not reset this, so this variable is still true if the master
  // is not connected and the state_mask_ is cleared.
  // Furthermore, on reconnects that enter full sync
  // again this variable is set to false until full sync completes.
  // Therefore, we have a consistent view of the replica:
  // 1. True. Replica passed full sync even if master disconnects. In fact, once a
  // node reached stable, the deltas from journal are the only missing items.
  // 2. False. Replica has not passed full sync or a disconnect started full sync again.
  bool passed_full_sync_ = false;

  bool is_paused_ = false;
  std::string id_;

  std::optional<cluster::SlotRange> slot_range_;

  uint32_t reconnect_count_ = 0;
  size_t psync_attempts_ = 0;
  size_t psync_successes_ = 0;

  const time_t creation_time_;
  const uint32_t client_id_;

  // drakeydb: set iff this Replica is a peer-mode replica of an active node (see IsPeerMode()).
  std::optional<ReplicaPeerMode> peer_mode_;

  // drakeydb: Phase 3 T6 -- the PeerRegistry origin index for master_context_.master_node_uuid,
  // captured by Greet() from PeerRegistry::AddOrGet(). Threaded down to each DflyShardReplica
  // (see InitiateDflySync) and to ConsumeRedisStream's own ConnectionContext, so writes applied
  // from this peer are journaled with its origin instead of kSelfIdx. Stays at its default for a
  // non-peer Replica, since Greet()'s peer-only branches never run for one.
  // 0 == PeerRegistry::kSelfIdx (server/multi_master.h); a literal, not the named constant,
  // matching ConnectionContext::repl_origin_idx's own convention (conn_context.h) to avoid
  // pulling that header into this one just for a constant.
  uint32_t peer_origin_idx_ = 0;
};

class RdbLoader;
// This class implements a single shard replication flow from a Dragonfly master instance.
// Multiple DflyShardReplica objects are managed by a Replica object.
class DflyShardReplica : public ProtocolClient {
  // drakeydb: Phase 3 T6 -- lets DflyShardReplicaOriginTest (peer_replication_test.cc)
  // construct a flow directly (no socket) and inspect that the origin idx passed at
  // construction reaches executor_'s ConnectionContext.
  friend class DflyShardReplicaOriginTest;

  // drakeydb: Phase 3 T6b -- lets DflyShardReplicaPeerModeTest (peer_replication_test.cc)
  // construct a flow directly (no socket) and drive AdoptAuthoritativeLsn() to observe its effect
  // on journal_rec_executed_, the same way DflyShardReplicaOriginTest above drives origin_idx.
  friend class DflyShardReplicaPeerModeTest;

 public:
  // `origin_idx`: this flow's PeerRegistry origin index (Replica::Greet() obtains it from
  // PeerRegistry::AddOrGet()); PeerRegistry::kSelfIdx (0) for a non-peer flow. Threaded straight
  // to executor_->SetApplyOrigin() at construction -- see that method's doc comment
  // (journal/executor.h) for why this is set once here rather than reaching back into Replica.
  //
  // `peer_mode`: drakeydb: Phase 3 T6b -- true iff this flow belongs to a peer-mode Replica (see
  // Replica::IsPeerMode()). Threaded in at construction the same way origin_idx is -- this class
  // has no way to reach back into the owning Replica -- but, unlike origin_idx, kept as a member
  // (see peer_mode_ below) rather than forwarded once and forgotten: it is consulted repeatedly,
  // later, by StableSyncDflyReadFb, both to select TransactionReader's adopt-vs-compare behavior
  // for Op::LSN and to gate AdoptAuthoritativeLsn().
  DflyShardReplica(ServerContext server_context, MasterContext master_context, uint32_t flow_id,
                   Service* service, std::shared_ptr<MultiShardExecution> multi_shard_exe,
                   class RdbLoadContext* load_context, uint32_t origin_idx, bool peer_mode);
  ~DflyShardReplica();

  void Cancel();
  void JoinFlow();

  // Start replica initialized as dfly flow.
  // Sets is_full_sync when successful.
  io::Result<bool> StartSyncFlow(util::fb2::BlockingCounter block, ExecutionState* cntx,
                                 std::optional<LSN>,
                                 std::optional<Replica::LastMasterSyncData> data);

  // Transition into stable state mode as dfly flow.
  std::error_code StartStableSyncFlow(ExecutionState* cntx);

  // Single flow full sync fiber spawned by StartFullSyncFlow.
  void FullSyncDflyFb(std::string eof_token, util::fb2::BlockingCounter block,
                      ExecutionState* cntx);

  // Single flow stable state sync fiber spawned by StartStableSyncFlow.
  void StableSyncDflyReadFb(ExecutionState* cntx);

  void StableSyncDflyAcksFb(ExecutionState* cntx);

  // Return true if the transaction executed successfully. On error,
  // or on context cancellation return false.
  bool ExecuteTx(TransactionData&& tx_data, ExecutionState* cntx);

  uint32_t FlowId() const;

  uint64_t JournalExecutedCount() const {
    return journal_rec_executed_.load(std::memory_order_relaxed);
  }

  uint64_t SetRecordsExecuted(uint64_t value) {
    return journal_rec_executed_ = value;
  }

  // Can be called from any thread.
  void Pause(bool pause);

 private:
  // drakeydb: Phase 3 T6b -- adopts `master_lsn` (an incoming Op::LSN marker's payload) as
  // journal_rec_executed_'s new authoritative value; a no-op outside peer mode. See the .cc
  // definition for the full correctness argument (why this can never run the resume LSN ahead of
  // what was actually applied) and StableSyncDflyReadFb for its one call site.
  void AdoptAuthoritativeLsn(uint64_t master_lsn);

  Service& service_;
  MasterContext master_context_;

  std::optional<base::IoBuf> leftover_buf_;

  util::fb2::EventCount shard_replica_waker_;  // waker for trans_data_queue_

  std::unique_ptr<JournalExecutor> executor_;
  std::unique_ptr<RdbLoader> rdb_loader_;

  // The master instance has a LSN for each journal record. This counts
  // the number of journal records executed in this flow plus the initial
  // journal offset that we received in the transition from full sync
  // to stable sync.
  // Note: This is not 1-to-1 the LSN in the master, because this counts
  // **executed** records, which might be received interleaved when commands
  // run out-of-order on the master instance.
  // Atomic, because JournalExecutedCount() can be called from any thread.
  std::atomic_uint64_t journal_rec_executed_ = 1;

  // drakeydb: Phase 3 T6b -- see the constructor's doc comment for `peer_mode`. Named the same as
  // (but independent of, and a different type than) Replica::peer_mode_ -- both classes already
  // duplicate MasterContext master_context_ the same way; each keeps its own copy of what it
  // needs rather than reaching back into the other.
  bool peer_mode_ = false;

  // drakeydb: Phase 3 T6b fix-round-1 (C1) -- set (peer mode only) by StableSyncDflyReadFb when
  // ExecuteTx returns false while the link is still running (e.g. a local OOM on this replica --
  // facade::DispatchResult::OOM is a normal operational outcome, not a can't-happen; see
  // ExecuteTx's caller). journal_rec_executed_ is deliberately NOT advanced for that entry (see
  // the comment on ExecuteTx's success branch below), so it still correctly names the un-applied
  // entry as "still needed" -- but a LATER, authoritative Op::LSN marker (this task's own
  // gap-correction marker, the fully-filtered-link resolution marker, or the pre-existing
  // periodic heartbeat -- dflycmd.cc enables should_sent_lsn for peer links) would otherwise
  // silently overwrite journal_rec_executed_ past that unresolved entry the moment it next fires,
  // permanently losing it: a reconnect would never re-offer an LSN the counter claims is already
  // resolved. Sticky (never cleared) for this flow's lifetime once set: the single scalar counter
  // this class uses cannot represent "entry K failed but K+1..N succeeded", so once one entry's
  // fate is unknown there is no way back to precise tracking short of a fresh full sync (a new
  // DflyShardReplica, with a fresh journal_rec_executed_ seed from the RDB cut) -- which is
  // exactly what a sufficiently stale journal_rec_executed_ (this flow having stopped advancing
  // it via markers, however far behind the master's true LSN by the time of the next reconnect)
  // naturally triggers. See AdoptAuthoritativeLsn's use of this flag.
  bool apply_failed_ = false;

  util::fb2::Fiber sync_fb_, acks_fb_;
  size_t ack_offs_ = 0;
  int proactor_index_ = -1;
  bool force_ping_ = false;

  std::shared_ptr<MultiShardExecution> multi_shard_exe_;
  uint32_t flow_id_ = UINT32_MAX;  // Flow id if replica acts as a dfly flow.
};

}  // namespace dfly
