// Copyright 2026, drakeydb authors.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "util/fibers/synchronization.h"

namespace dfly {

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
