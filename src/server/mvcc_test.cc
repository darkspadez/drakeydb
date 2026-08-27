// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/mvcc.h"

#include <gmock/gmock.h>
#include <xxhash.h>

#include "base/gtest.h"

namespace dfly {

using namespace std;

TEST(MvccClockTest, MsShiftMatchesKeyDbLayout) {
  MvccClock clock;
  const uint64_t s = clock.Next(1'000);
  EXPECT_EQ(s, uint64_t(1'000) << MvccClock::kCounterBits);
  EXPECT_EQ(s >> MvccClock::kCounterBits, 1'000u);
  EXPECT_EQ(s & MvccClock::kCounterMask, 0u);
}

TEST(MvccClockTest, MonotonicWithinSameMs) {
  MvccClock clock;
  uint64_t prev = clock.Next(5'000);
  for (int i = 0; i < 1'000; ++i) {
    const uint64_t cur = clock.Next(5'000);
    EXPECT_GT(cur, prev) << "stamps must be strictly increasing at iteration " << i;
    prev = cur;
  }
  EXPECT_EQ(prev >> MvccClock::kCounterBits, 5'000u) << "1000 ticks must not overflow the ms field";
}

// The NTP test: a backwards wall-clock step must never produce a stamp we already issued.
TEST(MvccClockTest, NeverGoesBackwardsOnClockStep) {
  MvccClock clock;
  const uint64_t base = clock.Next(1'000);
  uint64_t prev = base;
  for (int i = 0; i < 10; ++i) {
    const uint64_t cur = clock.Next(900);  // wall clock jumped back 100 ms
    EXPECT_GT(cur, prev) << "iteration " << i;
    EXPECT_GT(cur, base);
    prev = cur;
  }
}

TEST(MvccClockTest, CounterOverflowCarriesIntoMs) {
  MvccClock clock;
  const uint64_t ms = 7'000;
  clock.TEST_Set((ms << MvccClock::kCounterBits) | MvccClock::kCounterMask);
  const uint64_t next = clock.Next(ms);
  EXPECT_EQ(next, (ms + 1) << MvccClock::kCounterBits)
      << "counter exhaustion must carry into the ms field, not wrap";
}

TEST(MvccClockTest, AheadMsReportsSkew) {
  MvccClock clock;
  clock.Next(2'000);
  EXPECT_EQ(clock.AheadMs(2'000), 0u);
  EXPECT_EQ(clock.AheadMs(1'000), 1'000u) << "clock 1s ahead of a retarded wall clock";
  EXPECT_EQ(clock.AheadMs(3'000), 0u) << "never reports negative";
}

TEST(MvccStampTest, SizeAndAlignment) {
  static_assert(sizeof(MvccStamp) == 16, "the side table's per-slot cost depends on this");
  static_assert(alignof(MvccStamp) == 8);
  EXPECT_EQ(sizeof(MvccStamp), 16u);
}

TEST(MvccStampTest, LexicographicOrderOnMvccThenOrigin) {
  const MvccStamp a{100, 5};
  const MvccStamp b{100, 9};
  const MvccStamp c{101, 1};
  EXPECT_LT(a, b) << "equal mvcc must break the tie on origin_hash";
  EXPECT_LT(b, c) << "mvcc dominates origin_hash";
  EXPECT_FALSE(b < a);
  EXPECT_FALSE(a < a) << "irreflexive";
}

// The tombstone marker rides bit 63 and must not perturb the ordering decision 1 specifies.
TEST(MvccStampTest, TombstoneBitIsMaskedFromComparison) {
  const MvccStamp value{100, 5};
  const MvccStamp tombstone{100 | MvccClock::kTombstoneBit, 5};

  EXPECT_TRUE(tombstone.IsTombstone());
  EXPECT_FALSE(value.IsTombstone());
  EXPECT_EQ(tombstone.Mvcc(), value.Mvcc());
  EXPECT_FALSE(value < tombstone) << "the marker must not make a tombstone win";
  EXPECT_FALSE(tombstone < value) << "...nor lose";

  // Pinning mvcc == 100 on both operands above only observes masking at an exact tie, where an
  // inflated left operand happens to be harmless. Cross-mvcc probes catch an a-side-only masking
  // bug that the tie case cannot: without the mask, a tombstone could become order-equivalent to
  // (neither greater nor less than) a strictly newer or older stamp instead of losing/winning.
  const MvccStamp newer{101, 5};
  EXPECT_LT(tombstone, newer) << "a tombstone must lose to a strictly newer stamp";
  EXPECT_FALSE(newer < tombstone);
  const MvccStamp older{99, 5};
  EXPECT_LT(older, tombstone) << "...and beat a strictly older one";
}

// operator< masks bit 63 so ordering ignores the tombstone marker (above), but operator== does
// not -- it compares raw packed. A tombstone and the value it replaces at the same mvcc are
// therefore order-equivalent yet still distinguishable by equality. See the invariant comment
// above operator< in mvcc.h for why an incoming tombstone must never reuse the value's mvcc.
TEST(MvccStampTest, EqualityDistinguishesTombstoneAtEqualMvcc) {
  const MvccStamp value{100, 5};
  const MvccStamp tombstone{100 | MvccClock::kTombstoneBit, 5};

  EXPECT_FALSE(value == tombstone) << "operator== is not tombstone-masked, unlike operator<";
  EXPECT_FALSE(value < tombstone);
  EXPECT_FALSE(tombstone < value);
}

TEST(MvccStampTest, MsPartIgnoresTombstoneBit) {
  const MvccStamp t{(uint64_t(9'999) << MvccClock::kCounterBits) | MvccClock::kTombstoneBit, 0};
  EXPECT_EQ(t.MsPart(), 9'999u);
}

TEST(NodeUuidHashTest, StableAndDistinct) {
  const string a = "6f1c4c3e-0000-4000-8000-000000000001";
  const string b = "6f1c4c3e-0000-4000-8000-000000000002";
  // Golden value, computed once from the shipped implementation -- not fabricated. A same-process
  // self-comparison (NodeUuidHash(a) == NodeUuidHash(a)) would pass for any pure function,
  // including std::hash, XXH3, or a different seed, so it cannot exercise cross-process/
  // cross-build/cross-architecture stability, which is the property that is actually load-bearing
  // (the hash is persisted in the RDB and compared against values written by other nodes).
  EXPECT_EQ(NodeUuidHash(a), 0x02b4489225d16e46ULL)
      << "the origin hash is persisted in the RDB and compared against values written by "
         "other nodes -- changing the algorithm, the seed, or the byte order silently "
         "diverges every existing snapshot";
  EXPECT_NE(NodeUuidHash(a), NodeUuidHash(b));
  EXPECT_NE(NodeUuidHash(a), 0u) << "0 is reserved for 'no origin'";
  // Must not collide with LockTag::Fingerprint's hash space (tx_base.cc:100), which hashes keys
  // under a different seed (0x1C69B3F74AC4AE35UL) for a different purpose.
  EXPECT_NE(NodeUuidHash(a), XXH64(a.data(), a.size(), 0x1C69B3F74AC4AE35ULL));
}

}  // namespace dfly
