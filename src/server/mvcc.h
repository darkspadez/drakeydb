// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <cstdint>
#include <string_view>
#include <tuple>

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

}  // namespace dfly
