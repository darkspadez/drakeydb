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
  // RAII, not bare statements after the loop: a throwing fn (SetMvcc's side-table insert can
  // hit bad_alloc) must still unwind commit_depth_ AND clear armed_/arena_, or two things break.
  // First, commit_depth_ stuck positive DCHECK-aborts every subsequent Arm()/Disarm()/Commit()
  // forever. Second -- the parked decision this closes -- a surviving armed_ would be picked up
  // by whatever runs next: EndOfWriteEpoch() would silently over-count mvcc_unstamped_writes for
  // keys this call actually attempted, or -- worse -- a LATER Commit() call within the same
  // epoch (a second RecordEntry from the same callback, e.g. a Lua script issuing more than one
  // write) would stamp these leftover keys with THAT entry's mvcc, misattributing them to a
  // journal entry that never mentioned them. Unconditional clearing means a throw during Commit
  // instead degrades to: keys fn() reached before the throw are stamped (SetMvcc succeeded);
  // keys it never reached keep their pre-call stamp -- silently, NOT counted in
  // stats_.unstamped_writes, since that counter is only touched by EndOfWriteEpoch(), which by
  // then sees an already-empty armed_ (a known, narrow observability gap: a throw here is already
  // an exceptional bad_alloc, and the exception itself propagates out of Commit() uncaught by
  // anything between here and Transaction::RunCallback's tail -- see this task's report). Silent
  // pre-call-stamp survival is still the SAFE direction for the phase's central invariant (stamp
  // does not advance without the entry that would justify it), rather than silently corrupting a
  // future, unrelated commit.
  absl::Cleanup restore_depth = [this] {
    --commit_depth_;
    armed_.clear();
    arena_.clear();  // keeps capacity
  };
  for (const Armed& a : armed_)
    fn(a.db_index, ArmedKey(a), stamp);
}

void MvccStamper::EndOfWriteEpoch() {
  // The fourth mutator of armed_/arena_ (with Arm/Disarm/Commit, all DCHECK'd above): guards
  // against a CommitFn that calls back into EndOfWriteEpoch() while Commit() is mid-iteration,
  // which would clear armed_/arena_ out from under that loop exactly as a nested Arm()/Disarm()/
  // Commit() would (see Commit()'s comment).
  DCHECK_EQ(commit_depth_, 0) << "a CommitFn ended the write epoch -- Commit() is mid-iteration "
                                 "over armed_/arena_, which this call would clear out from under "
                                 "it";
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
