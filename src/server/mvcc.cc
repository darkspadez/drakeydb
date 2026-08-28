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
  const bool clock_stepped_back = hop_stamp_ != 0 && now_ms < hop_started_ms_;
  const bool expired =
      hop_stamp_ != 0 && !clock_stepped_back && now_ms - hop_started_ms_ > kMaxEpochMs;
  if (hop_stamp_ == 0 || clock_stepped_back || expired) {
    if (hop_stamp_ != 0)
      ++stats_.stale_epoch;
    hop_stamp_ = clock_.Next(now_ms);
    hop_started_ms_ = now_ms;
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

// drakeydb: Phase 4, review wave 2 (F1, CRITICAL) -- erases EVERY arm matching (db_index, key),
// not just the first. A key can be armed more than once before it is deleted: e.g. HDEL emptying
// a hash arms it via ExecuteW's own it_res->post_updater.Run() (hset_family.cc), then DeleteHw
// takes a SECOND, independent FindMutable/AutoUpdater on the same still-present key and arms it
// again via its own post_updater.Run() before calling Del -- OpFieldExpire's
// auto_updater.Run()-then-Delete{Set}IfEmpty->DelMutable shape (generic_family.cc) is the same
// pattern on both the HASH and SET branches. The original single-match version (erase the first
// hit, `return`) left one arm behind for a key PerformDeletionAtomic had just erased from `prime`
// and from the mvcc side table; the derived DEL's own journal::RecordEntry->Commit then
// reinserted a side-table entry for that now-nonexistent key, corrupting
// mvcc->size()/prime->size() parity (the DCHECK in DbSlice::OnCbFinishBlocking) --
// reproduced verbatim on `HSET h f v` then `HDEL h f` under --active_replica, which aborts the
// process; see multi_master_test.cc's HdelEmptyingHashDoesNotResurrectAStamp and its two
// FieldExpire siblings for the regression coverage, and final-fix-report.md for the verbatim
// crash this fixes.
//
// Disarm's only caller is PerformDeletionAtomic, invoked once the key is gone for good, so there
// is no scenario where leaving a second arm behind for it is correct -- erasing every match (not
// de-duplicating in Arm) is the fix. This changes nothing about Arm()'s bookkeeping for the
// (harmless) case of a still-live key armed more than once in one callback -- Commit() already
// tolerates that by calling SetMvcc with the same stamp value once per arm -- and only removes
// arms for keys that no longer exist, so stats_.unstamped_writes (only ever incremented by
// EndOfWriteEpoch(), never by Disarm()) is unaffected either way. Already an O(n) scan in the
// worst case (no match), so continuing past the first match instead of returning early costs
// nothing extra asymptotically.
void MvccStamper::Disarm(DbIndex db_index, std::string_view key) {
  DCHECK_EQ(commit_depth_, 0) << "a CommitFn disarmed a key -- Commit() is mid-iteration over "
                                 "armed_, which erase() would corrupt";
  for (auto it = armed_.begin(); it != armed_.end();) {
    if (it->db_index == db_index && ArmedKey(*it) == key) {
      it = armed_.erase(it);
    } else {
      ++it;
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
  // RAII, not bare statements after the loop: Commit is a generic primitive and its callback may
  // throw (the unit suite exercises that contract), even though journal::RecordEntry now supplies
  // an allocation-free SetExistingMvcc callback. A throw must unwind commit_depth_ and clear the
  // arm storage; otherwise later epoch end would over-count attempted keys, or a later Commit
  // could misattribute surviving arms to an unrelated entry.
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
  hop_started_ms_ = 0;
}

void MvccStamper::TEST_Reset() {
  clock_ = MvccClock{};
  hop_stamp_ = 0;
  hop_started_ms_ = 0;
  armed_.clear();
  arena_.clear();
  origin_hash_cache_.clear();
  stats_ = Stats{};
  commit_depth_ = 0;
}

}  // namespace dfly
