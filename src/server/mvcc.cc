// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/mvcc.h"

#include <xxhash.h>

#include "base/logging.h"  // DCHECK

namespace dfly {

namespace {
// Distinct from LockTag::Fingerprint's seed (tx_base.cc:100) so the two hash spaces cannot be
// confused in a debugger or a log.
constexpr uint64_t kOriginHashSeed = 0x9E3779B97F4A7C15ULL;
}  // namespace

uint64_t NodeUuidHash(std::string_view uuid) {
  return XXH64(uuid.data(), uuid.size(), kOriginHashSeed);
}

MvccStamper* MvccStamper::tlocal() {
  static thread_local MvccStamper stamper;
  return &stamper;
}

void MvccStamper::SetSelfUuid(std::string_view uuid) {
  RegisterOriginHash(0, NodeUuidHash(uuid));  // PeerRegistry::kSelfIdx == 0
}

void MvccStamper::RegisterOriginHash(uint32_t origin_idx, uint64_t hash) {
  if (origin_hash_cache_.size() <= origin_idx)
    origin_hash_cache_.resize(origin_idx + 1, 0);
  origin_hash_cache_[origin_idx] = hash;
}

uint64_t MvccStamper::OriginHash(uint32_t origin_idx) const {
  return origin_idx < origin_hash_cache_.size() ? origin_hash_cache_[origin_idx] : 0;
}

uint64_t MvccStamper::HopStamp(uint64_t now_ms) {
  if (hop_stamp_ == 0 || (hop_stamp_ >> MvccClock::kCounterBits) + kMaxEpochMs < now_ms) {
    if (hop_stamp_ != 0)
      ++stats_.stale_epoch;
    hop_stamp_ = clock_.Next(now_ms);
  }
  return hop_stamp_;
}

void MvccStamper::Arm(DbIndex db_index, std::string_view key) {
  DCHECK(!in_commit_) << "a CommitFn armed a key -- Commit() is mid-iteration over armed_/arena_, "
                         "both of which this call can reallocate, corrupting that iteration";
  const uint32_t off = static_cast<uint32_t>(arena_.size());
  arena_.append(key);
  armed_.push_back(Armed{db_index, off, static_cast<uint32_t>(key.size())});
}

void MvccStamper::Disarm(DbIndex db_index, std::string_view key) {
  for (auto it = armed_.begin(); it != armed_.end(); ++it) {
    if (it->db_index == db_index && ArmedKey(*it) == key) {
      armed_.erase(it);
      return;
    }
  }
}

void MvccStamper::Commit(uint64_t mvcc, uint32_t origin_idx, const CommitFn& fn) {
  if (armed_.empty())
    return;

  // The only production call site mints (HopStamp, which cannot return 0) or forwards an applied
  // write's non-zero author stamp before calling Commit, under the same MvccEnabled() && COMMAND
  // gate. Commit has no clock of its own, so it cannot invent a stamp -- it can only store what it
  // is given, and this DCHECK is the last check that "given" was ever actually true.
  DCHECK(mvcc != 0);
  const MvccStamp stamp{mvcc, OriginHash(origin_idx)};
  in_commit_ = true;
  for (const Armed& a : armed_)
    fn(a.db_index, ArmedKey(a), stamp);
  in_commit_ = false;

  armed_.clear();
  arena_.clear();  // keeps capacity
}

void MvccStamper::EndOfWriteEpoch() {
  stats_.unstamped_writes += armed_.size();
  armed_.clear();
  arena_.clear();
  hop_stamp_ = 0;
}

void MvccStamper::TEST_Reset() {
  clock_ = MvccClock{};
  hop_stamp_ = 0;
  armed_.clear();
  arena_.clear();
  origin_hash_cache_.clear();
  stats_ = Stats{};
  in_commit_ = false;
}

}  // namespace dfly
