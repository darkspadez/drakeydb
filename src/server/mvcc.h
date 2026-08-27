// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#include "server/common.h"  // DbIndex

namespace dfly {

// drakeydb: Phase 4. A per-shard logical clock in KeyDB's layout (ms << 20 | counter,
// KeyDB/src/server.cpp:7263-7287) so a KeyDB-sourced mvcc-tstamp compares meaningfully against a
// locally minted one during P7 onboarding.
//
// Unlike KeyDB's process-global atomic, this is a plain member of a thread-local stamper: KeyDB
// serialises every write in the process on one cache line, which a shared-nothing server must not.
class MvccClock {
 public:
  static constexpr unsigned kCounterBits = 20;
  static constexpr uint64_t kCounterMask = (1ULL << kCounterBits) - 1;

  // Bit 63 marks a tombstone. Safe indefinitely: ms << 20 with today's ms (~1.77e12, 2^40.7)
  // reaches bit 60, and bit 61 is not reached until ~2079.
  static constexpr uint64_t kTombstoneBit = 1ULL << 63;
  static constexpr uint64_t kStampMask = ~kTombstoneBit;

  // Strictly increasing, always -- including across a backwards NTP step, where it advances one
  // counter tick per call until the wall clock catches up. Counter exhaustion carries into the ms
  // field exactly as KeyDB's fetch_add(1) does.
  uint64_t Next(uint64_t now_ms) {
    const uint64_t cand = now_ms << kCounterBits;
    last_ = (cand > last_) ? cand : last_ + 1;
    return last_;
  }

  uint64_t last() const {
    return last_;
  }

  // Milliseconds this clock currently runs ahead of the wall clock; 0 when in step. Non-zero
  // means this node wins every LWW conflict for that long -- see INFO mvcc_clock_ahead_ms.
  uint64_t AheadMs(uint64_t now_ms) const {
    const uint64_t ms = last_ >> kCounterBits;
    return ms > now_ms ? ms - now_ms : 0;
  }

  void TEST_Set(uint64_t v) {
    last_ = v;
  }

 private:
  uint64_t last_ = 0;
};

// Per-key version. 16 bytes; ordered lexicographically on (mvcc, origin_hash).
//
// origin_hash is what makes the order *total*, and that is load-bearing: KeyDB's merge lets an
// incoming write win an exact tie (KeyDB/src/db.cpp:384), so in a mesh A adopts B's value while B
// adopts A's and they swap permanently. Ties are common rather than rare -- under light load the
// 20-bit counter sits at 0, so two nodes writing in the same millisecond collide.
struct MvccStamp {
  uint64_t packed = 0;       // (tombstone << 63) | (ms << 20) | counter
  uint64_t origin_hash = 0;  // NodeUuidHash of the AUTHOR node's uuid, never the sender's

  uint64_t Mvcc() const {
    return packed & MvccClock::kStampMask;
  }
  bool IsTombstone() const {
    return (packed & MvccClock::kTombstoneBit) != 0;
  }
  uint64_t MsPart() const {
    return Mvcc() >> MvccClock::kCounterBits;
  }
  bool Empty() const {
    return packed == 0 && origin_hash == 0;
  }

  // INVARIANT: a tombstone's mvcc MUST be freshly minted via MvccClock::Next -- it must never
  // reuse the mvcc of the value it deletes. operator< masks bit 63 below precisely because a
  // fresh mvcc is assumed to make (mvcc, origin_hash) unique per write; if a tombstone instead
  // sets bit 63 on the value's existing packed, it becomes order-equivalent to that value (see
  // MvccStampTest.EqualityDistinguishesTombstoneAtEqualMvcc), and merge code written as
  // `if (local < incoming) adopt;` silently drops the delete. operator== below is deliberately
  // NOT tombstone-masked (it compares raw packed), so equality still distinguishes a tombstone
  // from the value at the same mvcc even though ordering does not.
  friend bool operator<(const MvccStamp& a, const MvccStamp& b) {
    // std::tie needs lvalues; Mvcc() returns by value, so make_tuple (which copies) is used
    // instead. Semantics are identical: lexicographic comparison on (Mvcc(), origin_hash).
    return std::make_tuple(a.Mvcc(), a.origin_hash) < std::make_tuple(b.Mvcc(), b.origin_hash);
  }
  friend bool operator==(const MvccStamp& a, const MvccStamp& b) {
    return a.packed == b.packed && a.origin_hash == b.origin_hash;
  }
};

static_assert(sizeof(MvccStamp) == 16, "side-table per-slot cost is computed from this");
static_assert(alignof(MvccStamp) == 8,
              "16-byte packing assumes 8-byte alignment; a consumer "
              "(e.g. the side table) may depend on this");

// Stable across processes, builds and architectures -- the hash is persisted in the RDB and
// compared against values written by other nodes, so std::hash is unusable here.
uint64_t NodeUuidHash(std::string_view uuid);

// Thread-local minting and arm/commit bookkeeping. One per proactor thread, which is the right
// granularity: several DbSlices may share a thread via namespaces, and the clock only has to be
// monotone, never unique.
//
// Contract: DbSlice::PostUpdate ARMS the keys a shard callback touched; journal::RecordEntry
// COMMITS the entry's stamp to every armed key at the moment the entry is emitted; the end of the
// callback DISCARDS whatever is still armed. That is what enforces the phase's central invariant:
//
//   a key's stamp advances if and only if that same stamp is propagated to peers.
//
// Violating it in the "stamp but do not propagate" direction causes permanent, silent divergence:
// the node then rejects peer writes that should have won, forever.
class MvccStamper {
 public:
  using CommitFn = std::function<void(DbIndex, std::string_view, const MvccStamp&)>;

  // Re-mint the hop stamp if the memo is older than this. A missed EndOfWriteEpoch then degrades
  // visibly (stats().stale_epoch) instead of silently reusing an ancient stamp.
  static constexpr uint64_t kMaxEpochMs = 50;

  struct Stats {
    uint64_t unstamped_writes = 0;  // arms discarded with no journal entry -- see INFO
    uint64_t stale_epoch = 0;       // hop memo re-minted by the backstop above
  };

  static MvccStamper* tlocal();

  void SetSelfUuid(std::string_view uuid);
  void RegisterOriginHash(uint32_t origin_idx, uint64_t hash);
  uint64_t OriginHash(uint32_t origin_idx) const;

  // Stable for the whole shard callback: repeated calls with a non-decreasing now_ms return the
  // same value until the memo is older than kMaxEpochMs, or EndOfWriteEpoch() resets it. Takes
  // the time as a parameter -- like MvccClock::Next/AheadMs -- so dfly_transaction never reaches
  // up into dragonfly_lib for GetCurrentTimeMs; callers (Task 7's journal.cc) pass it explicitly.
  uint64_t HopStamp(uint64_t now_ms);

  void Arm(DbIndex db_index, std::string_view key);
  void Disarm(DbIndex db_index, std::string_view key);

  // The caller always supplies a non-zero stamp: the author's freshly minted HopStamp(now_ms), or
  // an applied write's verbatim author stamp -- both under the same MvccEnabled() && COMMAND gate,
  // so Commit itself never mints (DCHECK'd in the .cc). Clears the arm list -- unconditionally,
  // even if fn throws (RAII in the .cc): a surviving armed_ after a partial failure would either
  // let EndOfWriteEpoch() over-count already-attempted keys as unstamped, or, worse, let a LATER
  // Commit() in the same epoch (e.g. a second RecordEntry from one Lua script) stamp these
  // leftover keys with an unrelated entry's mvcc. Decided and documented in Task 7 -- see the .cc.
  //
  // fn must not call Arm(), Disarm(), Commit(), or EndOfWriteEpoch(): this call is mid-iteration
  // over armed_/arena_, and any of the four would corrupt that iteration -- Arm()/a nested
  // Commit() by reallocating arena_ (invalidating the string_view key fn was just handed) and/or
  // armed_ (invalidating the iterator), Disarm()/EndOfWriteEpoch() by erasing from/clearing
  // armed_ out from under it. All four are DCHECK'd in the .cc via commit_depth_.
  void Commit(uint64_t mvcc, uint32_t origin_idx, const CommitFn& fn);

  // The fourth mutator of armed_/arena_ (Task 7 closes the gap: originally not DCHECK'd against
  // commit_depth_ like Arm()/Disarm()/Commit() above). See Commit()'s comment for why a CommitFn
  // calling this would corrupt Commit()'s own in-progress iteration.
  void EndOfWriteEpoch();

  const Stats& stats() const {
    return stats_;
  }
  const MvccClock& clock() const {
    return clock_;
  }

  void TEST_Reset();

 private:
  struct Armed {
    DbIndex db_index;
    uint32_t off;
    uint32_t len;
  };

  std::string_view ArmedKey(const Armed& a) const {
    return std::string_view(arena_.data() + a.off, a.len);
  }

  MvccClock clock_;
  uint64_t hop_stamp_ = 0;
  std::string arena_;  // cleared, never shrunk, so steady-state arming does not allocate
  // std::vector, not absl::InlinedVector: InlinedVector::clear() frees (DeallocateIfAllocated())
  // and reverts to inline storage, so any callback arming more than 4 keys would allocate on the
  // 5th arm and free on every single Commit/EndOfWriteEpoch, forever. std::vector::clear() keeps
  // capacity, matching arena_'s "cleared, never shrunk" discipline. Do not "optimise" this back --
  // InlinedVector is only cheaper for callbacks that arm <=4 keys, and worse for every other one.
  std::vector<Armed> armed_;
  std::vector<uint64_t> origin_hash_cache_;  // dense by origin_idx; index 0 == self
  Stats stats_;
  // >0 while Commit() is mid-iteration over armed_/arena_. Guards Arm(), Disarm(), and a
  // re-entrant Commit() call from a CommitFn -- see Commit()'s comment above for why each would
  // corrupt that iteration. A counter, not a bool: a bool reentrancy flag reset by an inner
  // Commit() call on return would stop guarding Arm()/Disarm() calls still made by the outer,
  // still-running one. RAII'd (absl::Cleanup, in the .cc) around the loop, not a bare decrement
  // after it, so a throwing fn still leaves this at 0 rather than stuck positive forever.
  int commit_depth_ = 0;
};

}  // namespace dfly
