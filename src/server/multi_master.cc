// Copyright 2026, drakeydb authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "server/multi_master.h"

#include "base/logging.h"

namespace dfly {

void PeerRegistry::Init(std::string_view self_uuid) {
  util::fb2::LockGuard lk(mu_);
  CHECK(idx_to_uuid_.empty()) << "PeerRegistry::Init() must be called exactly once";
  CHECK(uuid_to_idx_.empty()) << "PeerRegistry::Init() must be called exactly once";
  uuid_to_idx_.try_emplace(std::string(self_uuid), kSelfIdx);
  idx_to_uuid_.emplace_back(self_uuid);
}

uint32_t PeerRegistry::AddOrGet(std::string_view uuid) {
  util::fb2::LockGuard lk(mu_);
  auto [it, inserted] = uuid_to_idx_.try_emplace(std::string(uuid), idx_to_uuid_.size());
  if (inserted)
    idx_to_uuid_.emplace_back(uuid);
  return it->second;
}

std::optional<uint32_t> PeerRegistry::FindIdx(std::string_view uuid) const {
  util::fb2::LockGuard lk(mu_);
  auto it = uuid_to_idx_.find(uuid);
  if (it == uuid_to_idx_.end())
    return std::nullopt;
  return it->second;
}

std::string PeerRegistry::GetUuid(uint32_t idx) const {
  util::fb2::LockGuard lk(mu_);
  if (idx >= idx_to_uuid_.size())
    return "";
  return idx_to_uuid_[idx];
}

size_t PeerRegistry::Size() const {
  util::fb2::LockGuard lk(mu_);
  return idx_to_uuid_.size();
}

}  // namespace dfly
