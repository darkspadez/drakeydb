// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/mvcc.h"

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>
#include <xxhash.h>

#include "base/gtest.h"
#include "server/table.h"

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

TEST(MvccTableGeometry, BucketMatchesTheSizingArgument) {
  // 26 (BucketBase<14>) + 14*18 (PrimeKey) + 2 pad + 14*16 (MvccStamp) = 504.
  // If this changes, the ~41 B/key figure and the benchmark's [34,48] band are both stale.
  EXPECT_EQ(DbTable::MvccTable::Segment_t::kBucketSz, 504u);
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
  // inflated left operand happens to be harmless. Below, EXPECT_LT(tombstone, newer) is the probe
  // that actually catches an a-side-only masking bug: with the tombstone as the left ('a') operand
  // and a strictly newer stamp as 'b', an unmasked a.packed (bit 63 set) makes the comparison false
  // when it must be true. EXPECT_LT(older, tombstone) does not catch that same mutation -- older
  // has no tombstone bit to unmask, so its result is unaffected either way.
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

namespace {
// Collects what Commit would have written, so this task needs no DbSlice. A named struct, not a
// std::pair/tuple of (key, stamp): DbSlice::PostUpdate arms per (DbIndex, key), and dropping db on
// the floor here would make every test blind to a Disarm/Commit bug scoped to the wrong db (see
// DisarmIsScopedToTheDbIndex, which regressed exactly this way once already).
struct Recorder {
  struct Write {
    DbIndex db;
    std::string key;
    MvccStamp stamp;
  };
  std::vector<Write> writes;

  MvccStamper::CommitFn Fn() {
    return [this](DbIndex db, std::string_view key, const MvccStamp& st) {
      writes.push_back(Write{db, std::string(key), st});
    };
  }
};

MvccStamper* FreshStamper() {
  MvccStamper* s = MvccStamper::tlocal();
  s->TEST_Reset();
  s->SetSelfUuid("6f1c4c3e-0000-4000-8000-00000000000a");
  return s;
}
}  // namespace

TEST(MvccStamperTest, CommitStampsEveryArmedKey) {
  MvccStamper* s = FreshStamper();
  Recorder rec;
  s->Arm(0, "k1");
  s->Arm(0, "k2");
  s->Commit(4242, /* origin_idx= */ 0, rec.Fn());

  ASSERT_EQ(rec.writes.size(), 2u);
  EXPECT_EQ(rec.writes[0].key, "k1");
  EXPECT_EQ(rec.writes[1].key, "k2");
  EXPECT_EQ(rec.writes[0].stamp.Mvcc(), 4242u);
  EXPECT_EQ(rec.writes[0].stamp.origin_hash, rec.writes[1].stamp.origin_hash);
  EXPECT_EQ(s->stats().unstamped_writes, 0u);
}

TEST(MvccStamperTest, EndOfEpochDropsUncommittedArms) {
  MvccStamper* s = FreshStamper();
  Recorder rec;
  s->Arm(0, "orphan");
  s->EndOfWriteEpoch();
  s->Commit(1, 0, rec.Fn());

  EXPECT_TRUE(rec.writes.empty()) << "an arm with no journal entry must not be stamped";
  EXPECT_EQ(s->stats().unstamped_writes, 1u)
      << "and the drop must be counted -- this is the production canary for read paths "
         "that mutate without journaling";
}

TEST(MvccStamperTest, DisarmRemovesOnlyTheNamedKey) {
  MvccStamper* s = FreshStamper();
  Recorder rec;
  s->Arm(0, "keep");
  s->Arm(0, "drop");
  s->Disarm(0, "drop");
  s->Commit(7, 0, rec.Fn());

  ASSERT_EQ(rec.writes.size(), 1u);
  EXPECT_EQ(rec.writes[0].key, "keep");
}

// Regression coverage: an earlier version of this test recorded only (key, stamp), so it could
// not tell the surviving db=0 arm apart from a wrongly-surviving db=1 one -- both have key "k".
// Disarm(1, "k") must remove the db=1 arm specifically; if Disarm ignored db_index it would erase
// the first key match instead (db=0, armed first), leaving db=1's arm to reach Commit -- and the
// old assertions (size == 1, key == "k") could not tell the two cases apart.
TEST(MvccStamperTest, DisarmIsScopedToTheDbIndex) {
  MvccStamper* s = FreshStamper();
  Recorder rec;
  s->Arm(0, "k");
  s->Arm(1, "k");
  s->Disarm(1, "k");
  s->Commit(7, 0, rec.Fn());

  ASSERT_EQ(rec.writes.size(), 1u);
  EXPECT_EQ(rec.writes[0].key, "k");
  EXPECT_EQ(rec.writes[0].db, 0) << "the surviving arm must be db=0; db=1 was the one "
                                    "Disarm(1, \"k\") was supposed to remove";
}

// The bit-identity mechanism: several entries minted inside one callback share a stamp.
TEST(MvccStamperTest, HopStampIsStableWithinEpochAndAdvancesAfter) {
  MvccStamper* s = FreshStamper();
  // Same now_ms on every "stable" call, and well inside kMaxEpochMs of itself, so the backstop
  // (see HopStampReMintsPastStaleEpochBackstop below) cannot fire and confound this assertion.
  const uint64_t kNow = 10'000;
  const uint64_t first = s->HopStamp(kNow);
  for (int i = 0; i < 5; ++i)
    EXPECT_EQ(s->HopStamp(kNow), first) << "iteration " << i;

  s->EndOfWriteEpoch();
  EXPECT_GT(s->HopStamp(kNow), first);
}

// The stale-epoch backstop: a missed EndOfWriteEpoch must not let a hop stamp be reused forever.
TEST(MvccStamperTest, HopStampReMintsPastStaleEpochBackstop) {
  MvccStamper* s = FreshStamper();
  const uint64_t first = s->HopStamp(10'000);
  const uint64_t later = s->HopStamp(10'000 + MvccStamper::kMaxEpochMs + 1);

  EXPECT_GT(later, first) << "a memo older than kMaxEpochMs must be re-minted, not reused";
  EXPECT_EQ(s->stats().stale_epoch, 1u)
      << "a missed EndOfWriteEpoch must be visible in stats, not silently absorbed";
}

TEST(MvccStamperTest, PeerMvccIsNeverReminted) {
  MvccStamper* s = FreshStamper();
  Recorder rec;
  s->RegisterOriginHash(3, 0xABCDEF);
  s->Arm(0, "k");
  s->Commit(/* mvcc= */ 999, /* origin_idx= */ 3, rec.Fn());

  ASSERT_EQ(rec.writes.size(), 1u);
  EXPECT_EQ(rec.writes[0].stamp.Mvcc(), 999u) << "an applied write keeps the author's stamp "
                                                 "verbatim, or stamps inflate on every hop";
  EXPECT_EQ(rec.writes[0].stamp.origin_hash, 0xABCDEFu) << "and the author's origin, not ours";
}

TEST(MvccStamperTest, SelfOriginIsIndexZero) {
  MvccStamper* s = FreshStamper();
  EXPECT_EQ(s->OriginHash(0), NodeUuidHash("6f1c4c3e-0000-4000-8000-00000000000a"));
}

TEST(MvccStamperTest, ManyArmsDoNotInvalidateEarlierOnes) {
  // Guards the arena implementation: a reallocating buffer must not corrupt earlier (off, len).
  MvccStamper* s = FreshStamper();
  Recorder rec;
  std::vector<std::string> keys;
  for (int i = 0; i < 256; ++i) {
    keys.push_back(absl::StrCat("key-with-a-long-enough-name-to-force-growth-", i));
    s->Arm(0, keys.back());
  }
  s->Commit(5, 0, rec.Fn());

  ASSERT_EQ(rec.writes.size(), 256u);
  for (int i = 0; i < 256; ++i)
    EXPECT_EQ(rec.writes[i].key, keys[i]) << "arm " << i << " was corrupted by later growth";
}

// ---------------------------------------------------------------------------
// commit_depth_ reentrancy guard: fix round 2, findings 1(a) (exception safety) and 1(b)
// (nesting-awareness). DCHECKs active in debug builds only.
// ---------------------------------------------------------------------------

// Hole 1(a): commit_depth_ must unwind via RAII even if fn throws -- e.g. SetMvcc's side-table
// insert hitting bad_alloc, once Task 7 wires it -- or one transient failure leaves every
// subsequent Arm()/Disarm()/Commit() DCHECK-aborting forever.
TEST(MvccStamperTest, CommitDepthRecoversAfterCommitFnThrows) {
  MvccStamper* s = FreshStamper();
  s->Arm(0, "k");
  EXPECT_THROW(
      s->Commit(1, 0, [](DbIndex, std::string_view, const MvccStamp&) { throw std::bad_alloc{}; }),
      std::bad_alloc);

  // If commit_depth_ had leaked at 1 above, this would DCHECK-abort the whole test binary in a
  // debug build -- there is no way to observe a leaked guard other than the process not dying.
  s->Arm(0, "k2");

  // drakeydb: P4-1 Task 7 -- the throw-safety decision parked from Task 4 (see this file's
  // description above and the Commit() comment in mvcc.cc): armed_/arena_ are cleared
  // unconditionally, even when fn throws, not just commit_depth_. "k" (armed before the throwing
  // Commit() above) must not still be sitting in armed_ here -- if it were, this Commit call
  // would wrongly stamp it with mvcc=99, an entry that never mentioned "k".
  Recorder rec;
  s->Commit(99, 0, rec.Fn());
  ASSERT_EQ(rec.writes.size(), 1u)
      << "a throwing Commit() must not leak the pre-throw arm list into a later, unrelated commit";
  EXPECT_EQ(rec.writes[0].key, "k2");
}

#ifndef NDEBUG
// Hole 1(b): a CommitFn that called Commit() again used to clear armed_/arena_ out from under the
// outer call's still-in-progress iteration (UB), and reset the old bool-typed guard to false on
// return, so a later Arm()/Disarm() in the still-running outer loop went uncaught too.
// commit_depth_ closes this: Commit() now DCHECK_EQ(commit_depth_, 0)s at its own entry, so the
// inner call dies before it ever touches armed_ or arena_.
//
// EXPECT_DEBUG_DEATH runs `statement` in-process, without forking or checking for death, when
// NDEBUG is defined (DCHECK is a no-op there) -- and the corruption this guards against is
// genuine UB, so this whole test is compiled only in a debug build, where the forked child dies
// at the DCHECK before doing any damage.
TEST(MvccStamperDeathTest, ReentrantCommitDies) {
  MvccStamper* s = FreshStamper();
  s->Arm(0, "k");
  EXPECT_DEBUG_DEATH(s->Commit(1, 0,
                               [s](DbIndex, std::string_view, const MvccStamp&) {
                                 s->Commit(2, 0,
                                           [](DbIndex, std::string_view, const MvccStamp&) {});
                               }),
                     "re-entrantly");
}

// The hazard Commit()'s own doc comment names first: "fn must not call Arm()".
TEST(MvccStamperDeathTest, ArmFromCommitFnDies) {
  MvccStamper* s = FreshStamper();
  s->Arm(0, "k");
  EXPECT_DEBUG_DEATH(
      s->Commit(1, 0, [s](DbIndex, std::string_view, const MvccStamp&) { s->Arm(0, "reentrant"); }),
      "mid-iteration");
}
#endif  // NDEBUG

}  // namespace dfly
