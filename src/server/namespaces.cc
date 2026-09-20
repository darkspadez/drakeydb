// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/namespaces.h"

#include "base/flags.h"
#include "base/logging.h"
#include "server/blocking_controller.h"
#include "server/db_slice.h"
#include "server/engine_shard_set.h"

ABSL_DECLARE_FLAG(bool, cache_mode);
ABSL_DECLARE_FLAG(std::string, notify_keyspace_events);

namespace dfly {

using namespace std;

Namespace::Namespace(bool is_default) : is_default_(is_default) {
  // is_default_ is initialized above, before the DbSlices below are built: DbSlice's constructor
  // reads it back via ns->IsDefault() to decide whether to register the tombstone GC idle task
  // (round 5, R1 -- see the declaration, namespaces.h).
  shard_db_slices_.resize(shard_set->size());
  shard_blocking_controller_.resize(shard_set->size());
  shard_set->RunBriefInParallel([&](EngineShard* es) {
    CHECK(es != nullptr);
    ShardId sid = es->shard_id();
    shard_db_slices_[sid] = make_unique<DbSlice>(sid, absl::GetFlag(FLAGS_cache_mode), es, this);
  });
}

DbSlice& Namespace::GetCurrentDbSlice() {
  EngineShard* es = EngineShard::tlocal();
  CHECK(es != nullptr);
  return GetDbSlice(es->shard_id());
}

DbSlice& Namespace::GetDbSlice(ShardId sid) {
  CHECK_LT(sid, shard_db_slices_.size());
  return *shard_db_slices_[sid];
}

BlockingController* Namespace::GetOrAddBlockingController(EngineShard* shard) {
  if (!shard_blocking_controller_[shard->shard_id()]) {
    shard_blocking_controller_[shard->shard_id()] = make_unique<BlockingController>(shard, this);
  }

  return shard_blocking_controller_[shard->shard_id()].get();
}

BlockingController* Namespace::GetBlockingController(ShardId sid) {
  return shard_blocking_controller_[sid].get();
}

Namespaces::Namespaces() {
  {
    // Startup only: CONFIG SET is not reachable yet, the validated flag is safe to read.
    util::fb2::LockGuard guard(mu_);
    expired_events_recording_default_ = !absl::GetFlag(FLAGS_notify_keyspace_events).empty();
  }
  default_namespace_ = &GetOrInsert("");
}

Namespaces::~Namespaces() {
  Clear();
}

void Namespaces::Clear() {
  util::fb2::LockGuard guard(mu_);

  default_namespace_ = nullptr;

  if (namespaces_.empty()) {
    return;
  }

  shard_set->RunBriefInParallel([&](EngineShard* es) {
    CHECK(es != nullptr);
    for (auto& ns : ABSL_TS_UNCHECKED_READ(namespaces_)) {
      ns.second.shard_db_slices_[es->shard_id()].reset();
    }
  });

  namespace_index_.clear();
  namespaces_.clear();
}

Namespace& Namespaces::GetDefaultNamespace() const {
  CHECK(default_namespace_ != nullptr);
  return *default_namespace_;
}

Namespace* Namespaces::GetNext(size_t* cursor, const Namespace* skip) {
  dfly::SharedLock guard(mu_);
  if (namespace_index_.empty())
    return nullptr;

  // At most one entry is skipped, so a populated non-default registry needs no more than two
  // probes. The loop also handles the default-only case without special casing the index.
  for (size_t attempts = 0; attempts < namespace_index_.size(); ++attempts) {
    Namespace* candidate = namespace_index_[(*cursor)++ % namespace_index_.size()];
    if (candidate != skip)
      return candidate;
  }
  return nullptr;
}

void Namespaces::SetExpiredEventsRecording(bool enable) {
  util::fb2::LockGuard guard(mu_);
  expired_events_recording_default_ = enable;
  // mu_ serializes this with namespace creation, which inherits the default.
  shard_set->pool()->AwaitFiberOnAll([&](unsigned, util::ProactorBase*) {
    EngineShard* shard = EngineShard::tlocal();
    if (shard) {
      for (auto& entry : ABSL_TS_UNCHECKED_READ(namespaces_)) {
        entry.second.GetDbSlice(shard->shard_id()).SetExpiredEventsRecording(enable);
      }
    }
  });
}

// drakeydb: P4-3 Task 3, review fix round 4; wording corrected round 5 (R3) -- see the
// declaration (namespaces.h) for the exact on-idle removal-order invariant this serves and why
// only the default namespace ever has a task to stop. Mirrors SetExpiredEventsRecording's
// fan-out above (mu_ serializes this with concurrent namespace creation the same way); does not
// need the lock for anything it writes -- there is no Namespaces-level state to update here --
// but takes it anyway so a namespace cannot be inserted mid-iteration and be missed or read
// half-constructed, exactly the hazard SetExpiredEventsRecording's own comment names.
void Namespaces::StopTombstoneGc() {
  util::fb2::LockGuard guard(mu_);
  shard_set->pool()->AwaitFiberOnAll([&](unsigned, util::ProactorBase*) {
    EngineShard* shard = EngineShard::tlocal();
    if (shard) {
      for (auto& entry : ABSL_TS_UNCHECKED_READ(namespaces_)) {
        entry.second.GetDbSlice(shard->shard_id()).StopTombstoneGc();
      }
    }
  });
}

Namespace& Namespaces::GetOrInsert(std::string_view ns) {
  {
    // Try to look up under a shared lock
    dfly::SharedLock guard(mu_);
    auto it = namespaces_.find(ns);
    if (it != namespaces_.end()) {
      return it->second;
    }
  }

  {
    // Key was not found, so we create create it under unique lock
    util::fb2::LockGuard guard(mu_);
    auto it = namespaces_.find(ns);
    if (it != namespaces_.end()) {
      return it->second;
    }

    // drakeydb: P4-3 Task 3, review fix round 5 (R1) -- try_emplace, not operator[]: Namespace is
    // no longer default-constructible, and the empty name is the ONLY thing that identifies the
    // default namespace at construction time (Namespaces::Namespaces() reaches here via
    // GetOrInsert(""), before default_namespace_ or the global `namespaces` pointer exist).
    Namespace& new_ns = namespaces_.try_emplace(std::string(ns), ns.empty()).first->second;
    // Not published yet (mu_ is held), so plain writes are safe.
    for (ShardId sid = 0; sid < shard_set->size(); ++sid) {
      new_ns.GetDbSlice(sid).SetExpiredEventsRecording(expired_events_recording_default_);
    }
    namespace_index_.push_back(&new_ns);
    return new_ns;
  }
}

}  // namespace dfly
