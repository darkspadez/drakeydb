// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/mvcc.h"

#include <absl/cleanup/cleanup.h>
#include <xxhash.h>

#include <algorithm>  // std::max

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

// drakeydb: P4-4 Task A5 -- see the declaration (mvcc.h) for the full why. `stored.Mvcc() == 0`
// covers both a genuinely fresh {0,0} slot and an uncommitted placeholder (Mvcc() masks bit 63,
// so PerformDeletionAtomic's {kTombstoneBit, 0} lands here too): neither carries a real prior
// stamp to floor against. `!(incoming < stored)` covers both "incoming is strictly newer" and an
// exact tie -- both commit verbatim, unchanged from before this task.
//
// drakeydb: P4-4 Task A5 -- the `stored.Mvcc() == 0` check is NOT strictly redundant, despite
// this function's only real caller gating on journal::RecordEntry's `applied` flag (`mvcc != 0`
// on the caller-supplied RAW wire value):
// `applied` only guarantees the raw wire mvcc is nonzero, not that its MASKED Mvcc() is -- a
// wire value of EXACTLY `MvccClock::kTombstoneBit` (bit 63 set, every other bit clear) is
// `applied` (nonzero as a raw integer) yet has `incoming.Mvcc() == 0`. If `stored` ALSO has
// `Mvcc() == 0` but a nonzero `origin_hash` (a malformed or placeholder-shaped value that is not
// exactly {0,0}), `incoming < stored` can still be true by origin_hash alone, and without this
// check the function would wrongly take the floor branch and return a corrupted result --
// `stored.Mvcc() | tomb_bit` collapses to just the tombstone bit (or 0), discarding `incoming`'s
// own value entirely -- instead of recognizing there is no real prior stamp to floor against.
// This is a general pure function regardless, exercised directly in mvcc_test.cc without going
// through that caller, so relying on a caller invariant here would make the function's own
// correctness depend on something this file cannot see or enforce -- and, as shown above, this
// particular one does not even hold.
MvccStamp FloorAppliedStamp(const MvccStamp& stored, const MvccStamp& incoming) {
  if (stored.Mvcc() == 0 || !(incoming < stored))
    return incoming;
  // incoming < stored: land one tick below `stored` instead of rewinding to `incoming` verbatim.
  // Never inherit stored's own tombstone bit -- only the NEW operation's (incoming's).
  const uint64_t tomb_bit = incoming.IsTombstone() ? MvccClock::kTombstoneBit : 0;
  if (stored.origin_hash == 0)
    return MvccStamp{(stored.Mvcc() - 1) | tomb_bit, UINT64_MAX};
  return MvccStamp{stored.Mvcc() | tomb_bit, stored.origin_hash - 1};
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

// drakeydb: P4-4 Task A5b -- see the declaration (mvcc.h) for the full why. A pure read over
// armed_ -- no allocation, no mutation of clock_/hop_stamp_/armed_ -- so calling this from
// journal::RecordEntry, alongside its own HopStamp call, costs nothing extra beyond the scan
// itself (armed_ is per-epoch and small, the same bound Arm()'s own fix-round-2 scan relies on).
// `prev_stamp.Mvcc() == 0` excludes both a genuinely fresh {0,0} slot and an uncommitted
// placeholder (Mvcc() masks bit 63, so PerformDeletionAtomic's {kTombstoneBit, 0} lands here too)
// -- neither carries a real prior stamp to floor a local mint against.
uint64_t MvccStamper::LocalMintFloor() const {
  uint64_t floor = 0;
  for (const Armed& a : armed_) {
    const uint64_t prev_mvcc = a.prev_stamp.Mvcc();
    if (prev_mvcc == 0)
      continue;
    // drakeydb: P4-4 Task A5b fix round 1 -- capped at kStampMask: prev_mvcc == kStampMask (the
    // max representable non-tombstone value -- all bits but bit 63) would otherwise overflow
    // prev_mvcc + 1 into EXACTLY kTombstoneBit, which a plain (non-tombstone) arm's Commit() call
    // would then commit verbatim as this key's live stamp -- silently marking a LIVE write as a
    // tombstone (IsTombstone() true) whose Mvcc() masks right back down to 0. A corrupt or
    // hostile peer stamp at exactly that value must not be able to do that; see
    // LocalMintFloorCapsAtStampMaskNeverOverflowingIntoTheTombstoneBit (mvcc_test.cc).
    floor = std::max(floor, std::min(prev_mvcc + 1, MvccClock::kStampMask));
  }
  return floor;
}

void MvccStamper::Arm(DbIndex db_index, std::string_view key, const MvccStamp& prev_stamp) {
  DCHECK_EQ(commit_depth_, 0) << "a CommitFn armed a key -- Commit() is mid-iteration over "
                                 "armed_/arena_, both of which this call can reallocate, "
                                 "corrupting that iteration";
  // drakeydb: P4-4 Task A5 fix round 2 -- see the declaration (mvcc.h) for the why. Gated on the
  // placeholder check first, so the common case (a real prior stamp, or none) never pays this
  // O(armed_.size()) scan; done BEFORE arena_.append() below, so every earlier arm's ArmedKey()
  // is still valid to compare against (arena_ has not been touched yet by this call).
  MvccStamp effective_prev = prev_stamp;
  if (prev_stamp.IsTombstone() && prev_stamp.Mvcc() == 0) {
    for (const Armed& a : armed_) {
      if (a.tombstone && a.db_index == db_index && ArmedKey(a) == key) {
        effective_prev = a.prev_stamp;
        break;
      }
    }
  }
  const uint32_t off = static_cast<uint32_t>(arena_.size());
  arena_.append(key);
  armed_.push_back(
      Armed{db_index, off, static_cast<uint32_t>(key.size()), /*tombstone=*/false, effective_prev});
}

// drakeydb: P4-3 Task 2 -- see the declaration (mvcc.h) for the contract. Identical to Arm()
// above except for the tombstone flag; kept as a separate function (not an Arm(..., bool) overload
// with a default) so every existing Arm() call site -- and its "arms a LIVE key" reading -- stays
// unambiguous at the call site, matching ArmTombstone's own single caller being the one place that
// actually means "arm a deletion".
void MvccStamper::ArmTombstone(DbIndex db_index, std::string_view key,
                               const MvccStamp& prev_stamp) {
  DCHECK_EQ(commit_depth_, 0) << "a CommitFn armed a tombstone -- Commit() is mid-iteration over "
                                 "armed_/arena_, both of which this call can reallocate, "
                                 "corrupting that iteration";
  const uint32_t off = static_cast<uint32_t>(arena_.size());
  arena_.append(key);
  armed_.push_back(
      Armed{db_index, off, static_cast<uint32_t>(key.size()), /*tombstone=*/true, prev_stamp});
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
  // drakeydb: P4-3 Task 2 -- the tombstone-flagged sibling of `stamp` above, sharing the same
  // mvcc/origin_hash and differing only in kTombstoneBit. Computed once per Commit() call (not
  // per-arm) since every tombstone arm in this call shares the same committed mvcc/origin_idx --
  // identical in spirit to `stamp` itself. mvcc is never re-derived or re-minted here: Commit has
  // no clock of its own (see the DCHECK above), so a tombstone arm gets exactly the same mvcc as
  // a plain arm in the same call would, with only the bit differing.
  const MvccStamp tomb_stamp{mvcc | MvccClock::kTombstoneBit, stamp.origin_hash};
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
    fn(a.db_index, ArmedKey(a), a.tombstone ? tomb_stamp : stamp, a.tombstone, a.prev_stamp);
}

// drakeydb: P4-3 Task 11, review ruling I2 -- see the declaration (mvcc.h) for the contract.
// RecordExpiryBlocking (tx_base.cc) is the only caller, and calls this BEFORE its own
// journal::RecordEntry -> Commit(): an expiry's own tombstone must always get a freshly minted
// self stamp, decoupled from whatever ambient (possibly a replicated peer's, possibly old) mvcc/
// origin that later Commit() call would otherwise apply to every currently armed key, including
// this one, if it were still armed by then.
bool MvccStamper::CommitOwnTombstone(DbIndex db_index, std::string_view key, uint64_t now_ms,
                                     const CommitFn& fn) {
  DCHECK_EQ(commit_depth_, 0) << "a CommitFn called CommitOwnTombstone() re-entrantly -- this "
                                 "call erases from armed_ mid-iteration, which would corrupt an "
                                 "outer Commit()/EndOfWriteEpoch() call's own in-progress "
                                 "iteration over the same container";
  for (auto it = armed_.begin(); it != armed_.end(); ++it) {
    if (it->db_index == db_index && it->tombstone && ArmedKey(*it) == key) {
      // drakeydb: P4-4 Task A5b -- spec D3 applies to an expiry's own tombstone too: it must not
      // stamp below the value it deletes (clock skew can otherwise make the freshly-minted
      // HopStamp alone land below `it->prev_stamp`, the value this exact arm is replacing).
      // Floored against THIS arm's own prev only -- never armed_ as a whole -- since this call
      // commits exactly one arm, unlike Commit()'s sweep of every currently-armed key.
      //
      // drakeydb: P4-4 Task A5b fix round 1 -- capped at kStampMask, mirroring LocalMintFloor's
      // own cap (see its comment, mvcc.cc, for the full why): prev_mvcc == kStampMask would
      // otherwise overflow prev_mvcc + 1 into EXACTLY kTombstoneBit, which the `| kTombstoneBit`
      // below is then a no-op on -- the committed tombstone's own Mvcc() would mask right back
      // down to 0, losing every bit of the "strictly newer" guarantee this floor exists to give
      // it. See CommitOwnTombstoneCapsAtStampMaskNeverOverflowingTheMaskedMvccToZero
      // (mvcc_test.cc).
      const uint64_t prev_mvcc = it->prev_stamp.Mvcc();
      const uint64_t floor = prev_mvcc == 0 ? 0 : std::min(prev_mvcc + 1, MvccClock::kStampMask);
      const MvccStamp stamp{std::max(HopStamp(now_ms), floor) | MvccClock::kTombstoneBit,
                            OriginHash(0)};
      ++commit_depth_;
      // RAII, matching Commit()'s own exception-safety contract (fn may throw): erase this one
      // arm and restore commit_depth_ whether or not fn throws. Does not touch arena_ -- same as
      // Disarm() above, an erased Armed entry's slice of arena_ becomes unreferenced garbage,
      // reclaimed wholesale the next time Commit()/EndOfWriteEpoch() clears it.
      absl::Cleanup restore_depth = [this, it] {
        --commit_depth_;
        armed_.erase(it);
      };
      // drakeydb: P4-4 Task A5 fix round 1 -- passes true (this is always a tombstone arm, per
      // the `it->tombstone` check above) and the arm's own prev_stamp through, matching Commit()'s
      // own call above; both harmless here since this stamp is always a fresh self-mint (never an
      // applied write), so RecordExpiryBlocking's fn (tx_base.cc) ignores both arguments entirely.
      fn(db_index, ArmedKey(*it), stamp, /*tombstone=*/true, it->prev_stamp);
      return true;
    }
  }
  return false;
}

void MvccStamper::EndOfWriteEpoch(const EraseFn& erase_fn) {
  // The fourth mutator of armed_/arena_ (with Arm/Disarm/Commit, all DCHECK'd above): guards
  // against a CommitFn that calls back into EndOfWriteEpoch() while Commit() is mid-iteration,
  // which would clear armed_/arena_ out from under that loop exactly as a nested Arm()/Disarm()/
  // Commit() would (see Commit()'s comment).
  DCHECK_EQ(commit_depth_, 0) << "a CommitFn ended the write epoch -- Commit() is mid-iteration "
                                 "over armed_/arena_, which this call would clear out from under "
                                 "it";
  // drakeydb: P4-3 Task 2 review fix (I3) -- commit_depth_ guards this loop's own iteration over
  // armed_/arena_ the same way Commit()'s loop guards itself: erase_fn must not call back into
  // Arm()/Disarm()/Commit()/EndOfWriteEpoch(). RAII, not a bare decrement after the loop, so a
  // throwing erase_fn (EraseMvcc touches a DashTable, which can in principle throw) still leaves
  // commit_depth_ at 0 and armed_/arena_/hop_stamp_ cleared, exactly like Commit()'s own guard.
  ++commit_depth_;
  absl::Cleanup restore_depth = [this] {
    --commit_depth_;
    stats_.unstamped_writes += armed_.size();
    armed_.clear();
    arena_.clear();
    hop_stamp_ = 0;
    hop_started_ms_ = 0;
  };
  for (const Armed& a : armed_) {
    if (a.tombstone)
      erase_fn(a.db_index, ArmedKey(a));
  }
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
