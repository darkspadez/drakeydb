// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/mvcc.h"

#include <absl/cleanup/cleanup.h>
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
  DCHECK_EQ(commit_depth_, 0) << "a CommitFn armed a key -- Commit() is mid-iteration over "
                                 "armed_/arena_, both of which this call can reallocate, "
                                 "corrupting that iteration";
  const uint32_t off = static_cast<uint32_t>(arena_.size());
  arena_.append(key);
  armed_.push_back(Armed{db_index, off, static_cast<uint32_t>(key.size())});
}

void MvccStamper::Disarm(DbIndex db_index, std::string_view key) {
  DCHECK_EQ(commit_depth_, 0) << "a CommitFn disarmed a key -- Commit() is mid-iteration over "
                                 "armed_, which erase() would corrupt";
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

  // Both DCHECKs below are scoped to a non-empty commit (above): Commit(0, ...), or a re-entrant
  // Commit() call, against an empty arm list is a harmless no-op that never reaches here.
  DCHECK_EQ(commit_depth_, 0) << "a CommitFn called Commit() re-entrantly -- the inner call's "
                                 "armed_.clear()/arena_.clear() would corrupt the outer call's "
                                 "still-in-progress iteration over the very same containers";
  // The only production call site mints (HopStamp, which cannot return 0) or forwards an applied
  // write's non-zero author stamp, under the same MvccEnabled() && COMMAND gate; Commit has no
  // clock of its own, so it cannot invent a stamp -- it can only store what it is given.
  DCHECK(mvcc != 0);

  const MvccStamp stamp{mvcc, OriginHash(origin_idx)};
  ++commit_depth_;
  // RAII, not a bare decrement after the loop: a throwing fn must still unwind commit_depth_, or
  // one transient failure (e.g. a side-table insert hitting bad_alloc) leaves every subsequent
  // Arm()/Disarm()/Commit() DCHECK-aborting forever.
  absl::Cleanup restore_depth = [this] { --commit_depth_; };
  for (const Armed& a : armed_)
    fn(a.db_index, ArmedKey(a), stamp);

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
  commit_depth_ = 0;
}

}  // namespace dfly
