// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/node_hash_map.h>

#include <memory>
#include <string>
#include <vector>

#include "server/common_types.h"
#include "util/fibers/synchronization.h"

namespace dfly {

class BlockingController;
class DbSlice;
class EngineShard;

// A Namespace is a way to separate and isolate different databases in a single instance.
// It can be used to allow multiple tenants to use the same server without hacks of using a common
// prefix, or SELECT-ing a different database.
// Each Namespace contains per-shard DbSlice, as well as a BlockingController.
class Namespace {
 public:
  // drakeydb: P4-3 Task 3, review fix round 5 (R1) -- `is_default` must be supplied by the
  // creator (Namespaces::GetOrInsert, the only one) rather than derived here, because a
  // Namespace is constructed while the global `namespaces` registry (common.h) that owns it is
  // still mid-construction: Namespaces::Namespaces() reaches GetOrInsert("") before the global
  // pointer is assigned and before default_namespace_ is set, so neither
  // `namespaces->GetDefaultNamespace()` nor `this == namespaces->default_namespace_` is a legal
  // comparison from inside this constructor. The flag is set before shard_db_slices_ is
  // populated, so each DbSlice's own constructor can read it (DbSlice::DbSlice, db_slice.cc).
  explicit Namespace(bool is_default);

  // True only for the registry's default (empty-named) namespace. Fixed at construction; a
  // namespace is never renamed or promoted.
  bool IsDefault() const {
    return is_default_;
  }

  DbSlice& GetCurrentDbSlice();

  DbSlice& GetDbSlice(ShardId sid);
  BlockingController* GetOrAddBlockingController(EngineShard* shard);
  BlockingController* GetBlockingController(ShardId sid);

 private:
  const bool is_default_;
  std::vector<std::unique_ptr<DbSlice>> shard_db_slices_;
  std::vector<std::unique_ptr<BlockingController>> shard_blocking_controller_;

  friend class Namespaces;
};

// Namespaces is a registry and container for Namespace instances.
// Each Namespace has a unique string name, which identifies it in the store.
// Any attempt to access a non-existing Namespace will first create it, add it to the internal map
// and will then return it.
// It is currently impossible to remove a Namespace after it has been created.
// The default Namespace can be accessed via either GetDefaultNamespace() (which guarantees not to
// yield), or via the GetOrInsert() with an empty string.
// The initialization order of this class with the engine shards is slightly subtle, as they have
// mutual dependencies.
class Namespaces {
 public:
  Namespaces();
  ~Namespaces();

  void Clear() ABSL_LOCKS_EXCLUDED(mu_);  // Thread unsafe, use in tear-down or tests

  Namespace& GetDefaultNamespace() const;  // No locks
  Namespace& GetOrInsert(std::string_view ns) ABSL_LOCKS_EXCLUDED(mu_);

  // Advances a round-robin cursor and returns the next namespace other than skip. Namespace
  // pointers are stable until tear-down, and lookup stays O(1) regardless of tenant count.
  Namespace* GetNext(size_t* cursor, const Namespace* skip) ABSL_LOCKS_EXCLUDED(mu_);

  // Applies to all namespaces and becomes the default for namespaces created later.
  void SetExpiredEventsRecording(bool enable) ABSL_LOCKS_EXCLUDED(mu_);

  // drakeydb: P4-3 Task 3, review fix round 4; wording corrected round 5 (R3) -- idempotently
  // stops the tombstone GC idle task (DbSlice::StopTombstoneGc, db_slice.h) on every
  // (namespace, shard) DbSlice. Mirrors SetExpiredEventsRecording's fan-out shape above.
  //
  // Called from Service::Shutdown (main_service.cc) BEFORE EngineShardSet::PreShutdown. The
  // invariant that ordering serves is NOT "this crash cannot happen"; it is narrower and worth
  // stating exactly, because helio's RemoveOnIdleTask (proactor_base.cc) pops only TRAILING
  // empty slots and never clamps ProactorBase::on_idle_next_:
  //
  //   never remove a TRAILING on-idle task while an EARLIER-registered one is still live.
  //
  // Doing so shrinks the array below a possibly-stale cursor, and a later tick then indexes past
  // the end. The default namespace's GC task satisfies the invariant by construction: it is
  // registered before "defrag" (EngineShardSet::Init builds namespaces at :121, then
  // StartPeriodicHeartbeatFiber registers "defrag" at :130) and removed before it (here, at
  // main_service.cc, ahead of PreShutdown) -- so removing it leaves a HOLE, not a shrink.
  // Non-default namespaces register no GC task at all (round 5, R1: DbSlice's constructor gates
  // on Namespace::IsDefault()), precisely because one created at RUNTIME would land ABOVE
  // "defrag" and this fan-out would then remove a trailing task with live entries beneath it.
  // The fan-out still visits every namespace -- it is a no-op for the ones that never registered
  // (DbSlice::StopTombstoneGc returns early on nullopt) and stays correct if the registration
  // gate ever changes shape.
  void StopTombstoneGc() ABSL_LOCKS_EXCLUDED(mu_);

 private:
  util::fb2::SharedMutex mu_{};
  absl::node_hash_map<std::string, Namespace> namespaces_ ABSL_GUARDED_BY(mu_);
  std::vector<Namespace*> namespace_index_ ABSL_GUARDED_BY(mu_);
  Namespace* default_namespace_ = nullptr;
  // Kept apart from the raw flag, which briefly holds unvalidated input during CONFIG SET.
  bool expired_events_recording_default_ ABSL_GUARDED_BY(mu_) = false;
};

}  // namespace dfly
