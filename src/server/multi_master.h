// Copyright 2026, drakeydb authors.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <absl/container/flat_hash_map.h>

#include <cstdint>
#include <nonstd/expected.hpp>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "facade/facade_types.h"
#include "server/replica_types.h"
#include "util/fibers/synchronization.h"

namespace dfly {

// --active_replica / --multi_master accessors (boot-only flags, declared in multi_master.cc).
bool IsActiveReplica();
bool IsMultiMaster();

// Validates the multi-master flag combination: --multi_master requires --active_replica, and
// --active_replica is incompatible with --cluster_mode, tiering (--tiered_prefix) and
// --experimental_cascaded_partial_sync. Logs and returns false on a bad combination. Called from
// main() before the proactor pool starts (see dfly_main.cc), like the TLS/snapshot validators.
bool ValidateMultiMasterFlags();

struct PeerReplicaOfCmd {
  enum class Kind : uint8_t { kAdd, kRemove, kNoOne };
  Kind kind = Kind::kAdd;
  std::string host;
  uint16_t port = 0;
};
// Active-mode REPLICAOF grammar: "<host> <port>" | "REMOVE <host> <port>" | "NO ONE".
// Slot ranges (cluster fan-in syntax) are rejected: active mode is incompatible with cluster mode.
nonstd::expected<PeerReplicaOfCmd, facade::ErrorReply> ParsePeerReplicaOfArgs(
    facade::ParsedArgs args);

// drakeydb: P4-0 -- LWW quality depends entirely on synchronised clocks, and P4's local-stamp
// repair (max(tick, stored + 1)) deliberately fabricates timestamps up to the skew ahead of
// true time. Both are only acceptable if the skew is observable.
constexpr int64_t kClockSkewWarnMs = 250;

// Signed difference peer - local, in ms. Returns 0 when peer_clock_ms is 0, which is how a
// pre-exchange master (or a plain Redis) reports "no clock" -- never treat that as skew.
int64_t ComputeClockSkewMs(int64_t local_clock_ms, int64_t peer_clock_ms);

// Returns whether an absolute skew reaches the operational warning threshold above.
bool IsClockSkewConcerning(int64_t skew_ms);

// INFO replication block for an active node (see D-8).
std::string RenderPeerReplicationInfo(const std::vector<ReplicaSummary>& peers, bool multi_master,
                                      bool show_peer_lines);

// PeerRegistry maps a peer's uuid to a small, dense "origin index", assigned in the order
// uuids are first seen. The local node always occupies kSelfIdx (0). Indices are assigned
// monotonically and are NEVER reused or reclaimed, even after a peer disconnects: Phase 3 stamps
// every journal entry with its origin index so a multi-master mesh can recognize (and drop)
// entries that originated at itself, and that only works if an index keeps naming the same uuid
// for the lifetime of the process. Accordingly, this class intentionally exposes no removal or
// iteration API.
//
// Contract: uuids are opaque strings to this class -- it does not validate or normalize them.
// Callers are expected to pass an already-normalized uuid (see NormalizeNodeUuid in
// node_identity.h); two spellings that differ only in case are treated as distinct peers.
//
// Thread-safe: every method takes an internal fiber mutex, so a single PeerRegistry may be
// shared across shards/fibers without external synchronization.
class PeerRegistry {
 public:
  static constexpr uint32_t kSelfIdx = 0;

  // Seeds the registry with the local node's uuid at kSelfIdx. Must be called exactly once,
  // before any other method is used. A second call is a programming error and CHECK-fails
  // rather than silently reassigning kSelfIdx or leaving two identities live.
  void Init(std::string_view self_uuid);

  // Returns the origin index for `uuid`, assigning and appending the next monotonic index if
  // this is the first time `uuid` has been seen. Idempotent, including under concurrent callers:
  // exactly one index is ever assigned per distinct uuid.
  //
  // drakeydb: Phase 3 T5 -- when a NEW index is assigned, also records an Op::ORIGIN entry on
  // every shard's journal (a fan-out from the calling fiber to shard_set, done outside this
  // registry's own lock -- see multi_master.cc) announcing "this index == this uuid", so
  // downstream plain-replica consumers can resolve origin_idx tags on this peer's entries. A
  // no-op fan-out (idx assignment still happens) when shard_set is null, e.g. this file's own
  // PeerRegistry/PeerRegistryFiberTest unit tests, which exercise the registry without a shard
  // set.
  uint32_t AddOrGet(std::string_view uuid);

  // Returns the origin index for `uuid` if already registered, or nullopt otherwise. Never
  // assigns an index.
  std::optional<uint32_t> FindIdx(std::string_view uuid) const;

  // Returns the uuid registered at `idx`, or "" if `idx` is out of range (including when called
  // before Init()).
  std::string GetUuid(uint32_t idx) const;

  // Number of registered peers, including self.
  size_t Size() const;

 private:
  mutable util::fb2::Mutex mu_;
  absl::flat_hash_map<std::string, uint32_t> uuid_to_idx_;
  std::vector<std::string> idx_to_uuid_;  // append-only; slot i holds the uuid assigned index i.
};

}  // namespace dfly
