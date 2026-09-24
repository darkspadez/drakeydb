// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/mvcc.h"

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>
#include <xxhash.h>

#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "base/gtest.h"
#include "common/backed_args.h"
#include "server/multimaster_lww.h"
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

// drakeydb: P4-3 Task 1 -- the merge-LWW decision, pinned before anything in the phase calls it.
// Owner decision, 2026-08-30: ties are won by the STORED side.
TEST(MvccStamp, MergeAcceptsFavoursTheStoredSideOnATie) {
  const MvccStamp a{0x1000, 0xAAAA};
  EXPECT_FALSE(MergeAccepts(a, a));                         // exact tie -> keep stored
  EXPECT_FALSE(MergeAccepts(MvccStamp{}, MvccStamp{}));     // {0,0} vs {0,0} -> keep stored
  EXPECT_TRUE(MergeAccepts(std::nullopt, MvccStamp{}));     // nothing stored -> write
  EXPECT_TRUE(MergeAccepts(MvccStamp{}, a));                // unversioned loses to stamped
  EXPECT_FALSE(MergeAccepts(a, MvccStamp{}));               // stamped beats unversioned
  EXPECT_TRUE(MergeAccepts(a, MvccStamp{0x1000, 0xBBBB}));  // equal mvcc, higher origin wins
}

// drakeydb: P4-3 Task 1 review fix -- the original version of this test used tiny counter-only
// literals (0x2000/0x3000/0x4000, all < 1<<20) for the mvcc, so MsPart() == 0 for every one of
// them: DeadlineMs(ttl) == ttl regardless of whether MsPart() is added at all, and a mutant
// `DeadlineMs() { return ttl_ms; }` still passed. It also never asserted origin_hash on the
// tombstone, so a mutant AsTombstone() that dropped origin_hash still passed too. Both mutations
// are falsified in the fix report. See task-1-report.md for the verbatim before/after.
TEST(MvccStamp, ATombstoneComparesByItsStampNotItsBit) {
  constexpr uint64_t kLiveMs = 2'000;
  constexpr uint64_t kTombMs = 4'321;  // deliberately not round, and not zero after >> kCounterBits
  constexpr uint64_t kLaterMs = 9'000;
  constexpr uint64_t kTombOrigin = 0xDDDDu;  // distinct from every other origin used in this test
  const MvccStamp live{kLiveMs << MvccClock::kCounterBits, 0xAAAA};
  const MvccStamp later{kLaterMs << MvccClock::kCounterBits, 0xAAAA};
  const MvccStamp tomb = MvccStamp{kTombMs << MvccClock::kCounterBits, kTombOrigin}.AsTombstone();

  EXPECT_TRUE(tomb.IsTombstone());
  EXPECT_EQ(tomb.Mvcc(), kTombMs << MvccClock::kCounterBits);  // bit 63 masked out of the value
  EXPECT_EQ(tomb.origin_hash, kTombOrigin) << "AsTombstone must carry origin_hash through as-is";
  EXPECT_FALSE(MergeAccepts(tomb, live));  // newer tombstone beats an older resurrection
  EXPECT_TRUE(MergeAccepts(tomb, later));  // a later write beats the tombstone
  EXPECT_EQ(tomb.DeadlineMs(600'000), kTombMs + 600'000);
}

// ---------------------------------------------------------------------------
// FloorAppliedStamp (P4-4 Task A5): the "just-below" floor for an applied write whose author
// stamp is older than the key's own stored stamp. See mvcc.h's declaration for the full why.
// ---------------------------------------------------------------------------

TEST(FloorAppliedStampTest, IncomingNewerThanStoredCommitsVerbatim) {
  const MvccStamp stored{0x2000, 0xAAAA};
  const MvccStamp incoming{0x3000, 0xBBBB};
  EXPECT_EQ(FloorAppliedStamp(stored, incoming), incoming);
}

TEST(FloorAppliedStampTest, ExactTieCommitsVerbatim) {
  const MvccStamp stored{0x2000, 0xAAAA};
  const MvccStamp incoming{0x2000, 0xAAAA};
  EXPECT_EQ(FloorAppliedStamp(stored, incoming), incoming)
      << "FloorAppliedStamp's own contract is 'incoming >= stored commits verbatim' -- a tie "
         "is unchanged, not floored, and idempotent (re-committing the same stamp is a no-op)";
}

TEST(FloorAppliedStampTest, IncomingOlderFloorsToOneOriginBelowStored) {
  const MvccStamp stored{0x5000, 0xAAAA};
  const MvccStamp incoming{0x1000, 0xFFFF};  // Mvcc() strictly less -> older, regardless of origin
  const MvccStamp got = FloorAppliedStamp(stored, incoming);
  // Whole-stamp equality, not Mvcc()/origin_hash/IsTombstone() separately: pins bit 63 (masked
  // out of Mvcc()) at exactly 0 too, not merely "falsy by whatever IsTombstone() happens to see".
  EXPECT_EQ(got, (MvccStamp{stored.Mvcc(), stored.origin_hash - 1}));
}

TEST(FloorAppliedStampTest, StoredOriginZeroFloorsToOneMvccTickBelowStored) {
  const MvccStamp stored{0x5000, /*origin_hash=*/0};
  const MvccStamp incoming{0x1000, 0xFFFF};
  const MvccStamp got = FloorAppliedStamp(stored, incoming);
  EXPECT_EQ(got, (MvccStamp{stored.Mvcc() - 1, UINT64_MAX}));
}

TEST(FloorAppliedStampTest, FreshStoredSlotCommitsIncomingVerbatim) {
  const MvccStamp incoming{0x1000, 0xABCD};
  EXPECT_EQ(FloorAppliedStamp(MvccStamp{}, incoming), incoming)
      << "a fresh {0,0} slot has no real prior stamp to floor against";
}

TEST(FloorAppliedStampTest, UncommittedPlaceholderStoredCommitsIncomingVerbatim) {
  // PerformDeletionAtomic's own synchronous tombstone placeholder (db_slice.cc).
  const MvccStamp placeholder{MvccClock::kTombstoneBit, 0};
  const MvccStamp incoming{0x1000, 0xABCD};
  EXPECT_EQ(FloorAppliedStamp(placeholder, incoming), incoming)
      << "Mvcc() masks bit 63 -- the placeholder's Mvcc() is 0, same as a genuinely fresh slot";
}

TEST(FloorAppliedStampTest, StoredTombstoneLiveIncomingOlderFloorsWithoutTombstoneBit) {
  const MvccStamp stored = MvccStamp{0x5000, 0xAAAA}.AsTombstone();
  const MvccStamp incoming{0x1000, 0xFFFF};  // live, not a tombstone
  const MvccStamp got = FloorAppliedStamp(stored, incoming);
  // Whole-stamp equality pins bit 63 at exactly 0 -- the floor must never inherit stored's own
  // tombstone bit, only the NEW operation's (incoming's, here unset).
  EXPECT_EQ(got, (MvccStamp{stored.Mvcc(), stored.origin_hash - 1}));
}

TEST(FloorAppliedStampTest, LiveStoredTombstoneIncomingOlderFloorsWithTombstoneBit) {
  const MvccStamp stored{0x5000, 0xAAAA};                              // live
  const MvccStamp incoming = MvccStamp{0x1000, 0xFFFF}.AsTombstone();  // older, but a delete
  const MvccStamp got = FloorAppliedStamp(stored, incoming);
  // Whole-stamp equality pins bit 63 at exactly 1 -- the NEW operation's tombstone bit must
  // survive the floor.
  EXPECT_EQ(got, (MvccStamp{stored.Mvcc(), stored.origin_hash - 1}.AsTombstone()));
}

TEST(FloorAppliedStampTest, FlooredStampOrdersBetweenStoredAndEveryPreviouslyRejectedStamp) {
  const MvccStamp stored{0x5000, 100};
  const MvccStamp incoming{0x1000, 7};
  const MvccStamp floored = FloorAppliedStamp(stored, incoming);

  EXPECT_TRUE(floored < stored) << "a clean copy stamped exactly S must still beat the floor on "
                                   "the next merge, or RMW divergence never heals";

  // Every stamp {stored.Mvcc(), o} with o < floored.origin_hash was ALREADY rejected against
  // `stored` before this write landed (equal Mvcc(), lower origin loses) -- the floor must still
  // beat every one of them, or a previously-dropped stale write starts winning once this RMW's
  // floor replaces `stored` as the key's live stamp.
  const MvccStamp already_rejected{stored.Mvcc(), floored.origin_hash - 1};
  EXPECT_TRUE(already_rejected < floored);

  // Every stamp with a strictly smaller Mvcc() must also lose to the floor.
  const MvccStamp older_ms{stored.Mvcc() - 1, UINT64_MAX};
  EXPECT_TRUE(older_ms < floored);
}

// ---------------------------------------------------------------------------
// ExpiryTombstoneFor (P4-4): the "just-above" tombstone for a value an expiry replaces. See
// mvcc.h's declaration for the full why (never reuses the value's stamp verbatim, never mints a
// fresh wall-clock-derived one; one origin_hash tick above the value instead, mirroring
// FloorAppliedStamp's own one-tick-below floor for the opposite direction).
// ---------------------------------------------------------------------------

TEST(ExpiryTombstoneForTest, AdvancesOriginHashByOneTickLeavingMvccUnchanged) {
  const MvccStamp value{0x5000, 100};
  const MvccStamp got = ExpiryTombstoneFor(value);
  EXPECT_TRUE(got.IsTombstone());
  EXPECT_EQ(got.Mvcc(), value.Mvcc()) << "the mvcc field must be reused, never advanced, in the "
                                         "ordinary (non-overflow) case";
  EXPECT_EQ(got.origin_hash, value.origin_hash + 1);
  // Whole-stamp equality, not the two fields separately: pins bit 63 (masked out of Mvcc()) at
  // exactly 1 too.
  EXPECT_EQ(got, (MvccStamp{value.Mvcc() | MvccClock::kTombstoneBit, value.origin_hash + 1}));
}

TEST(ExpiryTombstoneForTest, IsStrictlyGreaterThanTheValueItReplaces) {
  const MvccStamp value{0x5000, 100};
  EXPECT_TRUE(value < ExpiryTombstoneFor(value))
      << "AsTombstone's own precondition (mvcc.h): a tombstone must order strictly greater than "
         "the value it replaces, or a peer's still-live copy of that value never loses the "
         "compare and the delete silently never applies there";
}

TEST(ExpiryTombstoneForTest, NeverTiesTheValueItReplaces) {
  // A tie here would let a receiver that later re-creates this exact key at this exact
  // {mvcc, origin_hash} pair (a delta RMW re-creating a value on a node that already reaped it,
  // stamping its own re-create verbatim at the author's stamp) tie against this tombstone
  // forever -- MergeAccepts favors the stored side on a tie, so the tombstone would never lose,
  // permanently unrepairable once it is GC'd. operator== is not tombstone-masked (mvcc.h), so
  // this checks the ordering property directly rather than relying on that asymmetry.
  const MvccStamp value{0x5000, 100};
  EXPECT_FALSE(ExpiryTombstoneFor(value) < value);
  EXPECT_FALSE(value == ExpiryTombstoneFor(value));
}

// Falsifying: reverting the `value.origin_hash == UINT64_MAX` branch in ExpiryTombstoneFor
// (mvcc.h) back to an unconditional `{value.Mvcc() | kTombstoneBit, value.origin_hash + 1}` makes
// the EXPECT_EQ below fail -- origin_hash wraps to 0 via unsigned overflow (UINT64_MAX + 1) while
// Mvcc() stays unchanged, instead of Mvcc() advancing by one and origin_hash landing at 0
// deliberately.
TEST(ExpiryTombstoneForTest, CarriesIntoMvccAtOriginHashMax) {
  const MvccStamp value{0x5000, UINT64_MAX};
  const MvccStamp got = ExpiryTombstoneFor(value);
  EXPECT_TRUE(got.IsTombstone());
  EXPECT_EQ(got.Mvcc(), value.Mvcc() + 1)
      << "origin_hash has no room left to advance into -- the carry must land in Mvcc() instead";
  EXPECT_EQ(got.origin_hash, 0u);
  EXPECT_TRUE(value < got) << "must still order strictly greater than value, same as the ordinary "
                              "case";
}

// Falsifying: removing the `std::min(value.Mvcc() + 1, MvccClock::kStampMask)` cap in
// ExpiryTombstoneFor's own origin_hash-overflow branch (mvcc.h) back to a bare `value.Mvcc() + 1`
// makes the EXPECT_EQ below fail -- Mvcc() + 1 overflows into exactly kTombstoneBit, which masks
// right back down to a Mvcc() of 0 (losing the "strictly greater" guarantee this cap exists to
// give it) instead of staying capped at kStampMask.
TEST(ExpiryTombstoneForTest, CapsAtStampMaskOnDoubleOverflow) {
  const MvccStamp value{MvccClock::kStampMask, UINT64_MAX};
  const MvccStamp got = ExpiryTombstoneFor(value);
  EXPECT_TRUE(got.IsTombstone());
  EXPECT_EQ(got.Mvcc(), MvccClock::kStampMask)
      << "must cap at kStampMask, never overflow value.Mvcc() + 1 into exactly kTombstoneBit";
  EXPECT_EQ(got.origin_hash, 0u);
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
    // drakeydb: P4-4 Task A5 -- CommitFn grew a 4th argument (the arm's captured prev_stamp);
    // unused by every test that only cares about the committed stamp itself.
    return [this](DbIndex db, std::string_view key, const MvccStamp& st, bool, const MvccStamp&) {
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

// drakeydb: P4-3 Task 2 review fix (I3) -- EndOfWriteEpoch now requires an EraseFn (mvcc.h) so
// every call site consciously handles rolling back an abandoned tombstone placeholder. Tests that
// don't care about that (nothing armed via ArmTombstone) pass this no-op instead of a DbSlice.
MvccStamper::EraseFn NoopErase() {
  return [](DbIndex, std::string_view) {};
}

// Records what EndOfWriteEpoch's erase_fn was called with, so EndOfEpochRollsBack... below needs
// no DbSlice either -- same reasoning as Recorder above, for Commit.
struct EraseRecorder {
  std::vector<std::pair<DbIndex, std::string>> erased;
  MvccStamper::EraseFn Fn() {
    return [this](DbIndex db, std::string_view key) { erased.emplace_back(db, std::string(key)); };
  }
};
}  // namespace

TEST(MvccStamperTest, CommitStampsEveryArmedKey) {
  MvccStamper* s = FreshStamper();
  Recorder rec;
  s->Arm(0, "k1", MvccStamp{});
  s->Arm(0, "k2", MvccStamp{});
  s->Commit(4242, /* origin_idx= */ 0, rec.Fn());

  ASSERT_EQ(rec.writes.size(), 2u);
  EXPECT_EQ(rec.writes[0].key, "k1");
  EXPECT_EQ(rec.writes[1].key, "k2");
  EXPECT_EQ(rec.writes[0].stamp.Mvcc(), 4242u);
  EXPECT_EQ(rec.writes[0].stamp.origin_hash, rec.writes[1].stamp.origin_hash);
  EXPECT_EQ(s->stats().unstamped_writes, 0u);
}

// drakeydb: P4-3 Task 2 -- Commit() ORs kTombstoneBit into the stamp handed to an ArmTombstone'd
// key's CommitFn call, and ONLY that arm's: a plain Arm() of a different key committed in the
// same call must come out bare. Both share the exact same (mvcc, origin_hash) -- Commit has no
// clock of its own and mints nothing itself, so a tombstone arm's stamp differs from a plain arm
// committed in the same call by that one bit alone.
TEST(MvccStamperTest, CommitMarksOnlyTombstoneArms) {
  MvccStamper* s = FreshStamper();
  s->Arm(0, "live", MvccStamp{});
  s->ArmTombstone(0, "dead", MvccStamp{});
  std::map<std::string, MvccStamp> got;
  s->Commit(0x5000, 0,
            [&](DbIndex, std::string_view k, const MvccStamp& st, bool, const MvccStamp&) {
              got[std::string(k)] = st;
            });
  ASSERT_EQ(got.size(), 2u);
  EXPECT_FALSE(got["live"].IsTombstone());
  EXPECT_TRUE(got["dead"].IsTombstone());
  EXPECT_EQ(got["live"].Mvcc(), got["dead"].Mvcc());  // same epoch stamp, one bit apart
  EXPECT_EQ(got["live"].origin_hash, got["dead"].origin_hash);
}

TEST(MvccStamperTest, EndOfEpochDropsUncommittedArms) {
  MvccStamper* s = FreshStamper();
  Recorder rec;
  s->Arm(0, "orphan", MvccStamp{});
  s->EndOfWriteEpoch(NoopErase());
  s->Commit(1, 0, rec.Fn());

  EXPECT_TRUE(rec.writes.empty()) << "an arm with no journal entry must not be stamped";
  EXPECT_EQ(s->stats().unstamped_writes, 1u)
      << "and the drop must be counted -- this is the production canary for read paths "
         "that mutate without journaling";
}

// drakeydb: P4-3 Task 2 review fix (I3) -- the root fix for a stuck {kTombstoneBit, 0}
// placeholder: PerformDeletionAtomic (db_slice.cc) writes that placeholder synchronously,
// expecting the delete's own journal commit to overwrite it with a real stamp. When that commit
// never happens, EndOfWriteEpoch must roll the placeholder back (erase_fn), not just drop the arm
// silently -- and must do so ONLY for tombstone arms; a plain, uncommitted Arm() has no
// placeholder of its own to undo (EnsureMvcc's zero-authority slot is a legitimate "write is
// pending" marker, not an abandoned one, and is left for a later write or GC to resolve).
TEST(MvccStamperTest, EndOfEpochRollsBackOnlyUncommittedTombstoneArms) {
  MvccStamper* s = FreshStamper();
  s->Arm(0, "live-orphan", MvccStamp{});
  s->ArmTombstone(0, "dead-orphan", MvccStamp{});
  EraseRecorder erase_rec;
  s->EndOfWriteEpoch(erase_rec.Fn());

  ASSERT_EQ(erase_rec.erased.size(), 1u)
      << "only the tombstone arm is an abandoned placeholder that needs rolling back";
  EXPECT_EQ(erase_rec.erased[0].first, 0u);
  EXPECT_EQ(erase_rec.erased[0].second, "dead-orphan");
  EXPECT_EQ(s->stats().unstamped_writes, 2u)
      << "both arms still count toward the canary -- rollback doesn't hide that this happened";
}

TEST(MvccStamperTest, DisarmRemovesOnlyTheNamedKey) {
  MvccStamper* s = FreshStamper();
  Recorder rec;
  s->Arm(0, "keep", MvccStamp{});
  s->Arm(0, "drop", MvccStamp{});
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
  s->Arm(0, "k", MvccStamp{});
  s->Arm(1, "k", MvccStamp{});
  s->Disarm(1, "k");
  s->Commit(7, 0, rec.Fn());

  ASSERT_EQ(rec.writes.size(), 1u);
  EXPECT_EQ(rec.writes[0].key, "k");
  EXPECT_EQ(rec.writes[0].db, 0) << "the surviving arm must be db=0; db=1 was the one "
                                    "Disarm(1, \"k\") was supposed to remove";
}

// drakeydb: the core-mechanism unit test behind
// multi_master_test.cc's HdelEmptyingHashDoesNotResurrectAStamp and its two FieldExpire siblings,
// isolated from any specific command: a key can be armed twice before it is deleted (e.g. HDEL
// emptying a hash -- ExecuteW's own post_updater.Run() arms it once, then DeleteHw takes a
// second, independent FindMutable/AutoUpdater on the same still-present key and arms it again
// before calling Del). Disarm's only caller, PerformDeletionAtomic, calls it exactly once per
// deleted key regardless of how many times that key was armed -- so a single-match Disarm would
// leave one arm behind, which the deleting command's own Commit() would then re-stamp, corrupting
// the mvcc side table with an entry for a key PerformDeletionAtomic had just erased from `prime`
// (reproduced verbatim: `HSET h f v` then `HDEL h f` under --active_replica aborts the process).
//
// Falsifying: restoring the early `return` inside Disarm's erase loop (mvcc.cc) makes rec.writes
// non-empty here (size 1, key "h") instead of empty.
TEST(MvccStamperTest, DisarmRemovesAllArmsForTheSameKey) {
  MvccStamper* s = FreshStamper();
  Recorder rec;
  s->Arm(0, "h", MvccStamp{});
  s->Arm(0, "h", MvccStamp{});
  s->Disarm(0, "h");
  s->Commit(7, 0, rec.Fn());

  EXPECT_TRUE(rec.writes.empty())
      << "a key armed twice before being deleted must not resurface in Commit() after a single "
         "Disarm() call -- the surviving arm would re-stamp a key that no longer exists";
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

  s->EndOfWriteEpoch(NoopErase());
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

TEST(MvccStamperTest, HopStampReMintsWhenWallClockStepsBackward) {
  MvccStamper* s = FreshStamper();
  const uint64_t first = s->HopStamp(10'000);
  const uint64_t after_rollback = s->HopStamp(9'000);

  EXPECT_GT(after_rollback, first)
      << "a clock rollback must end the memoized epoch while MvccClock preserves monotonicity";
  EXPECT_EQ(s->stats().stale_epoch, 1u)
      << "the rollback-triggered remint must be visible through the stale-epoch canary";
}

TEST(MvccStamperTest, PeerMvccIsNeverReminted) {
  MvccStamper* s = FreshStamper();
  Recorder rec;
  s->RegisterOriginHash(3, 0xABCDEF);
  s->Arm(0, "k", MvccStamp{});
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
    s->Arm(0, keys.back(), MvccStamp{});
  }
  s->Commit(5, 0, rec.Fn());

  ASSERT_EQ(rec.writes.size(), 256u);
  for (int i = 0; i < 256; ++i)
    EXPECT_EQ(rec.writes[i].key, keys[i]) << "arm " << i << " was corrupted by later growth";
}

// drakeydb: P4-4 -- CommitOwnTombstone's headline contract: it commits (and removes) ONLY the
// named key's own tombstone arm, independently of whatever an ordinary Commit() call elsewhere in
// the same epoch would use. A sibling PLAIN arm for a different key must survive untouched, so a
// later ordinary Commit() call still sees and correctly stamps it -- this is what lets
// RecordExpiryBlocking (tx_base.cc) give an expiring key's own tombstone a stamp derived from the
// value's own while a sibling key from a replicated multi-key command still gets that command's
// real author stamp from its own, later journal entry.
//
// This exercises the arm's captured prior stamp being EMPTY (a value that never received a
// stamp, or a slot the caller's own lookup found nothing for): with no real value to advance
// from, CommitOwnTombstone erases the slot instead of minting a self stamp -- a reap-time mint
// would exceed a peer's write authored before this node's reap (the same hazard
// ExpiryTombstoneFor itself exists to close), and ExpiryTombstoneFor's own {0,1}|tombstone result
// for a {0,0} input has Mvcc()==0, which TombstoneGcStep/rdb_load both treat as
// PerformDeletionAtomic's own mid-epoch placeholder rather than a genuine committed tombstone --
// installing one would make it immortal. See
// CommitOwnTombstoneAdvancesTheArmsPriorStampByOneOriginHashTick (below) for the ordinary case,
// where a real prior stamp exists and is advanced by one tick rather than erased.
//
// Falsifying: reverting CommitOwnTombstone (mvcc.cc) to mint `HopStamp(now_ms) | kTombstoneBit`
// for this branch (this file's own git history has that exact prior version) makes the
// IsTombstone()/Empty() checks below fail -- the stamp comes back self-originated and freshly
// minted instead of empty (this test's own erase signal).
TEST(MvccStamperTest, CommitOwnTombstoneErasesWhenNoPriorStampExistsAndSparesSiblingArm) {
  MvccStamper* s = FreshStamper();
  s->Arm(0, "sibling", MvccStamp{});
  s->ArmTombstone(0, "victim", MvccStamp{});

  Recorder victim_rec;
  EXPECT_TRUE(s->CommitOwnTombstone(0, "victim", victim_rec.Fn()));
  ASSERT_EQ(victim_rec.writes.size(), 1u);
  EXPECT_EQ(victim_rec.writes[0].key, "victim");
  EXPECT_FALSE(victim_rec.writes[0].stamp.IsTombstone())
      << "no real prior stamp exists to advance from, and a committed Mvcc()==0 tombstone would "
         "be immortal -- the caller must erase the slot instead of installing one";
  EXPECT_TRUE(victim_rec.writes[0].stamp.Empty());

  // The sibling arm must be untouched: a later, ordinary Commit() call (using a DIFFERENT
  // mvcc/origin, standing in for a replicated command's real author stamp) must still see and
  // correctly stamp it, and must NOT see "victim" again (already processed and removed above).
  constexpr uint32_t kPeerIdx = 3;
  s->RegisterOriginHash(kPeerIdx, 0xBEEFu);
  Recorder sibling_rec;
  s->Commit(999, kPeerIdx, sibling_rec.Fn());
  ASSERT_EQ(sibling_rec.writes.size(), 1u)
      << "the victim's tombstone arm must already be gone -- CommitOwnTombstone above must have "
         "removed it, or this ordinary Commit() would see and re-stamp it too";
  EXPECT_EQ(sibling_rec.writes[0].key, "sibling");
  EXPECT_FALSE(sibling_rec.writes[0].stamp.IsTombstone());
  EXPECT_EQ(sibling_rec.writes[0].stamp.Mvcc(), 999u);
  EXPECT_EQ(sibling_rec.writes[0].stamp.origin_hash, 0xBEEFu);
}

// drakeydb: P4-4 -- the ordinary case: when the arm's own captured prior stamp carries real
// authority (Mvcc() != 0), CommitOwnTombstone delegates to ExpiryTombstoneFor (mvcc.h) -- one
// origin_hash tick above that prior stamp -- rather than minting anything. Proven two ways: the
// origin_hash below (0xC0FFEE) is neither this stamper's own self hash nor a value
// HopStamp/OriginHash(0) could ever produce, and now_ms is chosen far enough in the future that a
// fresh mint would produce a strictly LARGER Mvcc() than the arm's own -- so an unwanted mint
// cannot masquerade as a correct result here. The expected value is constructed independently
// (a raw MvccStamp literal), not by calling ExpiryTombstoneFor -- that function has its own,
// separate pure tests (ExpiryTombstoneForTest, above) that pin its behavior the same way.
//
// Falsifying: reverting CommitOwnTombstone (mvcc.cc) to always mint `HopStamp(now_ms) |
// kTombstoneBit` (this test file's own git history has that exact prior version) makes the
// EXPECT_EQ below fail -- the stamp comes back self-originated and freshly minted instead of one
// tick above `prior`.
TEST(MvccStamperTest, CommitOwnTombstoneAdvancesTheArmsPriorStampByOneOriginHashTick) {
  MvccStamper* s = FreshStamper();
  constexpr uint64_t kPriorMvcc = 42;
  constexpr uint64_t kPriorOriginHash = 0xC0FFEEu;
  const MvccStamp prior{kPriorMvcc, kPriorOriginHash};
  s->ArmTombstone(0, "k", prior);

  Recorder rec;
  EXPECT_TRUE(s->CommitOwnTombstone(0, "k", rec.Fn()));
  ASSERT_EQ(rec.writes.size(), 1u);
  EXPECT_EQ(rec.writes[0].stamp,
            (MvccStamp{kPriorMvcc | MvccClock::kTombstoneBit, kPriorOriginHash + 1}))
      << "must advance the value's own stamp by one origin_hash tick, never reuse it verbatim "
         "and never mint a new one";
  EXPECT_EQ(rec.writes[0].stamp.Mvcc(), kPriorMvcc);
  EXPECT_EQ(rec.writes[0].stamp.origin_hash, kPriorOriginHash + 1);
}

// No tombstone arm for the named key -- e.g. TombstonesEnabled() was false, or
// PerformDeletionAtomic degraded to a plain erase at the tombstone cap -- must be a harmless
// no-op: no mint, no call to fn, and any OTHER arm must be left alone for the caller's own
// subsequent ordinary Commit() to handle exactly as if CommitOwnTombstone had never been called.
TEST(MvccStamperTest, CommitOwnTombstoneIsANoopWhenNothingIsArmed) {
  MvccStamper* s = FreshStamper();
  s->Arm(0, "unrelated", MvccStamp{});

  Recorder rec;
  EXPECT_FALSE(s->CommitOwnTombstone(0, "missing", rec.Fn()));
  EXPECT_TRUE(rec.writes.empty());

  Recorder rec2;
  s->Commit(7, 0, rec2.Fn());
  ASSERT_EQ(rec2.writes.size(), 1u) << "the unrelated arm must have survived the no-op call above";
  EXPECT_EQ(rec2.writes[0].key, "unrelated");
}

// A plain (non-tombstone) arm for the exact same key must not be mistaken for a tombstone arm --
// CommitOwnTombstone's contract is specifically "the named key's TOMBSTONE arm", matching
// ArmTombstone alone, never a plain Arm().
TEST(MvccStamperTest, CommitOwnTombstoneIgnoresAPlainArmOfTheSameKey) {
  MvccStamper* s = FreshStamper();
  s->Arm(0, "k", MvccStamp{});

  Recorder rec;
  EXPECT_FALSE(s->CommitOwnTombstone(0, "k", rec.Fn()));
  EXPECT_TRUE(rec.writes.empty());

  Recorder rec2;
  s->Commit(7, 0, rec2.Fn());
  ASSERT_EQ(rec2.writes.size(), 1u) << "the plain arm for \"k\" must still be armed and unstamped";
  EXPECT_EQ(rec2.writes[0].key, "k");
  EXPECT_FALSE(rec2.writes[0].stamp.IsTombstone());
}

// CommitOwnTombstone is scoped by db_index, matching Disarm/Commit's own scoping
// (DisarmIsScopedToTheDbIndex above) -- a tombstone arm for the same key name on a DIFFERENT db
// must not be matched.
TEST(MvccStamperTest, CommitOwnTombstoneIsScopedToTheDbIndex) {
  MvccStamper* s = FreshStamper();
  s->ArmTombstone(1, "k", MvccStamp{});

  Recorder rec;
  EXPECT_FALSE(s->CommitOwnTombstone(0, "k", rec.Fn()));
  EXPECT_TRUE(rec.writes.empty());

  Recorder rec2;
  s->Commit(7, 0, rec2.Fn());
  ASSERT_EQ(rec2.writes.size(), 1u) << "db=1's tombstone arm must still be armed";
  EXPECT_EQ(rec2.writes[0].key, "k");
  EXPECT_TRUE(rec2.writes[0].stamp.IsTombstone());
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
  s->Arm(0, "k", MvccStamp{});
  EXPECT_THROW(s->Commit(1, 0,
                         [](DbIndex, std::string_view, const MvccStamp&, bool, const MvccStamp&) {
                           throw std::bad_alloc{};
                         }),
               std::bad_alloc);

  // If commit_depth_ had leaked at 1 above, this would DCHECK-abort the whole test binary in a
  // debug build -- there is no way to observe a leaked guard other than the process not dying.
  s->Arm(0, "k2", MvccStamp{});

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

// drakeydb: P4-4 Task A5b -- a corrupt or hostile peer stamp with Mvcc() exactly
// MvccClock::kStampMask (the maximum representable non-tombstone value: every bit but bit 63)
// must not let LocalMintFloor overflow prev+1 into precisely kTombstoneBit -- journal.cc's mint
// would then commit that value verbatim for a PLAIN arm, silently marking a live write's stamp as
// a tombstone (IsTombstone() true) whose own Mvcc() masks right back down to 0.
//
// Falsifying: removing the `std::min(prev_mvcc + 1, MvccClock::kStampMask)` cap (mvcc.cc) back to
// a bare `prev_mvcc + 1` reproduces this: LocalMintFloor() comes back one above kStampMask (i.e.
// exactly kTombstoneBit) instead of capped at kStampMask itself.
TEST(MvccStamperTest, LocalMintFloorCapsAtStampMaskNeverOverflowingIntoTheTombstoneBit) {
  MvccStamper* s = FreshStamper();
  s->Arm(0, "k", MvccStamp{MvccClock::kStampMask, 111});
  EXPECT_EQ(s->LocalMintFloor(), MvccClock::kStampMask)
      << "must cap at kStampMask, never overflow prev_mvcc + 1 into exactly kTombstoneBit";
}

// drakeydb: P4-4 -- CommitOwnTombstone delegates its arithmetic entirely to ExpiryTombstoneFor
// (mvcc.h), whose own boundary/overflow cases (origin_hash == UINT64_MAX carrying into Mvcc(),
// capped at kStampMask on a double overflow) are pinned independently by
// ExpiryTombstoneForTest.CarriesIntoMvccAtOriginHashMax and
// ExpiryTombstoneForTest.CapsAtStampMaskOnDoubleOverflow, above. This test instead pins the
// DELEGATION itself at a representative boundary value: CommitOwnTombstone must hand the arm's
// own prior stamp to ExpiryTombstoneFor UNCHANGED (not, say, a copy with origin_hash already
// pre-incremented, or Mvcc() pre-masked), so the result matches an INDEPENDENTLY constructed
// expectation built the same way ExpiryTombstoneForTest does, not by calling ExpiryTombstoneFor.
//
// Falsifying: reverting CommitOwnTombstone (mvcc.cc) to mint `HopStamp(now_ms) | kTombstoneBit`
// unconditionally (the pre-P4-4 version) makes the origin_hash EXPECT_EQ below fail -- it comes
// back as this stamper's own self hash instead of the arm's own 111, advanced by one.
TEST(MvccStamperTest, CommitOwnTombstoneDelegatesToExpiryTombstoneForAtTheStampMaskBoundary) {
  MvccStamper* s = FreshStamper();
  s->ArmTombstone(0, "k", MvccStamp{MvccClock::kStampMask, 111});
  Recorder rec;
  EXPECT_TRUE(s->CommitOwnTombstone(0, "k", rec.Fn()));
  ASSERT_EQ(rec.writes.size(), 1u);
  EXPECT_TRUE(rec.writes[0].stamp.IsTombstone());
  EXPECT_EQ(rec.writes[0].stamp.Mvcc(), MvccClock::kStampMask)
      << "origin_hash (111) has room to advance into, so the carry-into-Mvcc() branch must not "
         "fire here -- Mvcc() must stay unchanged at the boundary value itself";
  EXPECT_EQ(rec.writes[0].stamp.origin_hash, 112u)
      << "the arm's own prior origin_hash (111) must be advanced by exactly one tick, never "
         "reused verbatim and never replaced by this stamper's self hash";
}

// ---------------------------------------------------------------------------
// multimaster_lww.h: the streaming LWW guard's pure decision module (P4-4 Task A1). These tests
// exercise the module in isolation; see peer_replication_test.cc for its callers
// (Transaction::IsLwwGuarded, DflyShardReplica's constructor).
// ---------------------------------------------------------------------------

TEST(MultimasterLwwTest, ClassifyJournaledCommandMatchesEveryTableRow) {
  EXPECT_EQ(ClassifyJournaledCommand("DEL"), LwwClass::kMultiKeySelfGuarded);
  EXPECT_EQ(ClassifyJournaledCommand("GETDEL"), LwwClass::kSingleKey);
  EXPECT_EQ(ClassifyJournaledCommand("GETSET"), LwwClass::kSingleKey);
  EXPECT_EQ(ClassifyJournaledCommand("MSET"), LwwClass::kMultiKeySelfGuarded);
  EXPECT_EQ(ClassifyJournaledCommand("RESTORE"), LwwClass::kSingleKey);
  EXPECT_EQ(ClassifyJournaledCommand("SET"), LwwClass::kSingleKey);
  EXPECT_EQ(ClassifyJournaledCommand("SETNX"), LwwClass::kSingleKey);
}

// drakeydb: P4-4 -- PEXPIREAT and PERSIST are removed from the guarded table entirely (fail-safe:
// an active author never emits either name any more -- OpExpire/OpPersist ship the key's full
// state under SET/RESTORE/DEL instead -- and a non-active sender's PEXPIREAT/PERSIST entries
// carry mvcc 0, which is never guarded regardless of table membership; see LwwGuardActive above).
// Deliberately a SEPARATE test from ClassifyJournaledCommandMatchesEveryTableRow above (not
// folded into it, and not into ClassifyJournaledCommandUnknownNamesAreUnguarded below): unlike an
// unrecognized name, these are two commands the classifier used to recognize and guard, so this
// pins a removal, not an absence.
//
// Falsifying: restoring either entry to kJournaledClasses (multimaster_lww.cc) makes the
// corresponding EXPECT_EQ below observe kSingleKey instead of kUnguarded.
TEST(MultimasterLwwTest, ClassifyJournaledCommandNoLongerGuardsPexpireatOrPersist) {
  EXPECT_EQ(ClassifyJournaledCommand("PEXPIREAT"), LwwClass::kUnguarded);
  EXPECT_EQ(ClassifyJournaledCommand("PERSIST"), LwwClass::kUnguarded);
  EXPECT_EQ(ClassifyJournaledCommand("pexpireat"), LwwClass::kUnguarded);
  EXPECT_EQ(ClassifyJournaledCommand("persist"), LwwClass::kUnguarded);
}

TEST(MultimasterLwwTest, ClassifyJournaledCommandIsCaseInsensitive) {
  EXPECT_EQ(ClassifyJournaledCommand("set"), LwwClass::kSingleKey);
  EXPECT_EQ(ClassifyJournaledCommand("Set"), LwwClass::kSingleKey);
  EXPECT_EQ(ClassifyJournaledCommand("sEtNx"), LwwClass::kSingleKey);
  EXPECT_EQ(ClassifyJournaledCommand("del"), LwwClass::kMultiKeySelfGuarded);
  EXPECT_EQ(ClassifyJournaledCommand("mSeT"), LwwClass::kMultiKeySelfGuarded);
}

TEST(MultimasterLwwTest, ClassifyJournaledCommandUnknownNamesAreUnguarded) {
  for (std::string_view name : {"INCR", "APPEND", "PFADD", "UNLINK", "SORT", ""})
    EXPECT_EQ(ClassifyJournaledCommand(name), LwwClass::kUnguarded) << name;
}

TEST(MultimasterLwwTest, LwwGuardActiveTruthTable) {
  EXPECT_FALSE(LwwGuardActive(/*link_guard=*/false, /*incoming_mvcc=*/0));
  EXPECT_FALSE(LwwGuardActive(/*link_guard=*/false, /*incoming_mvcc=*/42));
  EXPECT_FALSE(LwwGuardActive(/*link_guard=*/true, /*incoming_mvcc=*/0));
  EXPECT_TRUE(LwwGuardActive(/*link_guard=*/true, /*incoming_mvcc=*/42));
}

TEST(MultimasterLwwTest, LwwShouldDropKeyNothingStoredNeverDrops) {
  const MvccStamp incoming{1000, 7};
  EXPECT_FALSE(LwwShouldDropKey(std::nullopt, incoming));
}

TEST(MultimasterLwwTest, LwwShouldDropKeyStoredOlderDoesNotDrop) {
  const std::optional<MvccStamp> stored = MvccStamp{500, 7};
  const MvccStamp incoming{1000, 7};
  EXPECT_FALSE(LwwShouldDropKey(stored, incoming));
}

TEST(MultimasterLwwTest, LwwShouldDropKeyStoredNewerDrops) {
  const std::optional<MvccStamp> stored = MvccStamp{1000, 7};
  const MvccStamp incoming{500, 7};
  EXPECT_TRUE(LwwShouldDropKey(stored, incoming));
}

TEST(MultimasterLwwTest, LwwShouldDropKeyExactTieDrops) {
  const std::optional<MvccStamp> stored = MvccStamp{1000, 7};
  const MvccStamp incoming{1000, 7};
  EXPECT_TRUE(LwwShouldDropKey(stored, incoming)) << "ties favor the stored side";
}

TEST(MultimasterLwwTest, LwwShouldDropKeyEqualMvccHigherIncomingOriginDoesNotDrop) {
  const std::optional<MvccStamp> stored = MvccStamp{1000, 5};
  const MvccStamp incoming{1000, 9};
  EXPECT_FALSE(LwwShouldDropKey(stored, incoming));
}

TEST(MultimasterLwwTest, LwwShouldDropKeyEqualMvccLowerIncomingOriginDrops) {
  const std::optional<MvccStamp> stored = MvccStamp{1000, 9};
  const MvccStamp incoming{1000, 5};
  EXPECT_TRUE(LwwShouldDropKey(stored, incoming));
}

// The tombstone bit (63) must be masked out of the comparison, not compared as part of the raw
// packed value: a stored tombstone with an otherwise OLDER ms would, compared raw, look "newer"
// than an un-tombstoned incoming write purely because bit 63 dominates the integer's magnitude --
// wrongly dropping the incoming write. Masked comparison (Mvcc(), origin_hash) must see it as
// older and keep the incoming write.
TEST(MultimasterLwwTest, LwwShouldDropKeyTombstoneBitIsMaskedNotComparedRaw) {
  constexpr uint64_t kOlderMs = 100, kNewerMs = 200;
  const std::optional<MvccStamp> stored =
      MvccStamp{kOlderMs << MvccClock::kCounterBits, /*origin_hash=*/5}.AsTombstone();
  const MvccStamp incoming{kNewerMs << MvccClock::kCounterBits, /*origin_hash=*/5};
  ASSERT_GT(stored->packed, incoming.packed) << "raw packed comparison must favor `stored` here "
                                                "-- otherwise this test proves nothing";
  EXPECT_FALSE(LwwShouldDropKey(stored, incoming));
}

TEST(MultimasterLwwTest, IncomingStampRegisteredOriginFormsTheStamp) {
  MvccStamper* s = FreshStamper();
  s->RegisterOriginHash(3, 0xABCDEFu);
  const std::optional<MvccStamp> got = IncomingStamp(/*mvcc=*/555, /*origin_idx=*/3);
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(got->Mvcc(), 555u);
  EXPECT_EQ(got->origin_hash, 0xABCDEFu);
}

// mvcc == 0 must short-circuit before the origin lookup: an unregistered origin_idx here must
// not trip IncomingStamp's DCHECK (see IncomingStampUnregisteredOriginDies below).
TEST(MultimasterLwwTest, IncomingStampZeroMvccIsNullopt) {
  FreshStamper();
  EXPECT_FALSE(IncomingStamp(/*mvcc=*/0, /*origin_idx=*/3).has_value());
}

#ifndef NDEBUG
// An unregistered origin with a non-zero mvcc means a caller invoked this before the peer's
// handshake registered its hash -- IncomingStamp DCHECKs that never happens rather than silently
// forming a bogus {mvcc, 0} stamp that would compare wrong for every key.
TEST(MultimasterLwwDeathTest, IncomingStampUnregisteredOriginDies) {
  FreshStamper();
  EXPECT_DEBUG_DEATH(IncomingStamp(/*mvcc=*/1, /*origin_idx=*/99), "no registered hash");
}
#endif  // NDEBUG

// ---------------------------------------------------------------------------
// ApplyLwwRewrites (P4-4 Task A9 for SETNX->SET / RESTORE->+REPLACE, extended below for
// GETSET->SET / GETDEL->DEL): the pre-dispatch rewrite for every guarded single-key command whose
// journaled form reproduces the author's COMMAND rather than the author's RESULT. See executor.cc
// for where this is actually called, gated on LwwGuardActive.
// ---------------------------------------------------------------------------
namespace {

// Builds a BackedArguments the same shape JournalExecutor::Execute receives off the wire.
cmn::BackedArguments MakeArgs(const std::vector<std::string_view>& parts) {
  cmn::BackedArguments args;
  args.Assign(parts.begin(), parts.end(), parts.size());
  return args;
}

// Reads every argument back as a comparable vector<string>, so assertions can equal-compare
// against a literal expected list instead of poking at BackedArguments one index at a time.
std::vector<std::string> ArgsToVec(const cmn::BackedArguments& args) {
  std::vector<std::string> out;
  out.reserve(args.size());
  for (size_t i = 0; i < args.size(); ++i)
    out.emplace_back(args.at(i));
  return out;
}

}  // namespace

TEST(MultimasterLwwTest, ApplyLwwRewritesSetnxBecomesSetArgsPreserved) {
  cmn::BackedArguments args = MakeArgs({"SETNX", "k", "v"});
  EXPECT_TRUE(ApplyLwwRewrites(&args));
  EXPECT_THAT(ArgsToVec(args), ::testing::ElementsAre("SET", "k", "v"));
}

TEST(MultimasterLwwTest, ApplyLwwRewritesSetnxIsCaseInsensitive) {
  cmn::BackedArguments args = MakeArgs({"setnx", "k", "v"});
  EXPECT_TRUE(ApplyLwwRewrites(&args));
  EXPECT_THAT(ArgsToVec(args), ::testing::ElementsAre("SET", "k", "v"));
}

// A SETNX with the wrong arity is dispatch's problem, not this rewrite's -- left untouched so
// dispatch still reports its own arity error, same as an unrewritten SETNX would today.
TEST(MultimasterLwwTest, ApplyLwwRewritesSetnxWrongArityUntouched) {
  for (const std::vector<std::string_view>& parts :
       {std::vector<std::string_view>{"SETNX", "k"},
        std::vector<std::string_view>{"SETNX", "k", "v", "extra"}}) {
    cmn::BackedArguments args = MakeArgs(parts);
    const std::vector<std::string> before = ArgsToVec(args);
    EXPECT_FALSE(ApplyLwwRewrites(&args));
    EXPECT_EQ(ArgsToVec(args), before);
  }
}

TEST(MultimasterLwwTest, ApplyLwwRewritesGetsetBecomesSetArgsPreserved) {
  cmn::BackedArguments args = MakeArgs({"GETSET", "k", "v"});
  EXPECT_TRUE(ApplyLwwRewrites(&args));
  EXPECT_THAT(ArgsToVec(args), ::testing::ElementsAre("SET", "k", "v"));
}

TEST(MultimasterLwwTest, ApplyLwwRewritesGetsetIsCaseInsensitive) {
  cmn::BackedArguments args = MakeArgs({"getset", "k", "v"});
  EXPECT_TRUE(ApplyLwwRewrites(&args));
  EXPECT_THAT(ArgsToVec(args), ::testing::ElementsAre("SET", "k", "v"));
}

TEST(MultimasterLwwTest, ApplyLwwRewritesGetsetWrongArityUntouched) {
  for (const std::vector<std::string_view>& parts :
       {std::vector<std::string_view>{"GETSET", "k"},
        std::vector<std::string_view>{"GETSET", "k", "v", "extra"}}) {
    cmn::BackedArguments args = MakeArgs(parts);
    const std::vector<std::string> before = ArgsToVec(args);
    EXPECT_FALSE(ApplyLwwRewrites(&args));
    EXPECT_EQ(ArgsToVec(args), before);
  }
}

TEST(MultimasterLwwTest, ApplyLwwRewritesGetdelBecomesDelArgsPreserved) {
  cmn::BackedArguments args = MakeArgs({"GETDEL", "k"});
  EXPECT_TRUE(ApplyLwwRewrites(&args));
  EXPECT_THAT(ArgsToVec(args), ::testing::ElementsAre("DEL", "k"));
}

TEST(MultimasterLwwTest, ApplyLwwRewritesGetdelIsCaseInsensitive) {
  cmn::BackedArguments args = MakeArgs({"getdel", "k"});
  EXPECT_TRUE(ApplyLwwRewrites(&args));
  EXPECT_THAT(ArgsToVec(args), ::testing::ElementsAre("DEL", "k"));
}

TEST(MultimasterLwwTest, ApplyLwwRewritesGetdelWrongArityUntouched) {
  for (const std::vector<std::string_view>& parts :
       {std::vector<std::string_view>{"GETDEL"},
        std::vector<std::string_view>{"GETDEL", "k", "extra"}}) {
    cmn::BackedArguments args = MakeArgs(parts);
    const std::vector<std::string> before = ArgsToVec(args);
    EXPECT_FALSE(ApplyLwwRewrites(&args));
    EXPECT_EQ(ArgsToVec(args), before);
  }
}

TEST(MultimasterLwwTest, ApplyLwwRewritesRestoreGainsReplace) {
  cmn::BackedArguments args = MakeArgs({"RESTORE", "k", "0", "payload"});
  EXPECT_TRUE(ApplyLwwRewrites(&args));
  EXPECT_THAT(ArgsToVec(args), ::testing::ElementsAre("RESTORE", "k", "0", "payload", "REPLACE"));
}

// REPLACE, in any case and at any valid position (including after ABSTTL, or after an
// IDLETIME/FREQ value), must be recognized as already present -- appending a second one would
// hand RESTORE's own arg parser a stray extra token.
TEST(MultimasterLwwTest, ApplyLwwRewritesRestoreAlreadyCarryingReplaceUnchanged) {
  const std::vector<std::vector<std::string_view>> cases = {
      {"RESTORE", "k", "0", "payload", "REPLACE"},
      {"RESTORE", "k", "0", "payload", "replace"},
      {"RESTORE", "k", "0", "payload", "ABSTTL", "Replace"},
      {"RESTORE", "k", "0", "payload", "IDLETIME", "5", "REPLACE"},
      {"RESTORE", "k", "0", "payload", "FREQ", "7", "REPLACE"},
  };
  for (const auto& parts : cases) {
    cmn::BackedArguments args = MakeArgs(parts);
    const std::vector<std::string> before = ArgsToVec(args);
    EXPECT_FALSE(ApplyLwwRewrites(&args)) << before.back();
    EXPECT_EQ(ArgsToVec(args), before);
  }
}

// The token right after IDLETIME is that option's VALUE, not another option name -- even when it
// happens to spell "replace", it must stay opaque, and a REAL REPLACE still gets appended.
TEST(MultimasterLwwTest, ApplyLwwRewritesRestoreIdletimeValueLiterallyReplaceStillAppends) {
  cmn::BackedArguments args = MakeArgs({"RESTORE", "k", "0", "payload", "IDLETIME", "replace"});
  EXPECT_TRUE(ApplyLwwRewrites(&args));
  EXPECT_THAT(ArgsToVec(args), ::testing::ElementsAre("RESTORE", "k", "0", "payload", "IDLETIME",
                                                      "replace", "REPLACE"));
}

TEST(MultimasterLwwTest, ApplyLwwRewritesOtherNamesUntouched) {
  const std::vector<std::vector<std::string_view>> cases = {
      {"SET", "k", "v"}, {"GET", "k"}, {"DEL", "k"}, {"MSET", "k1", "v1"}, {"PERSIST", "k"}, {},
  };
  for (const auto& parts : cases) {
    cmn::BackedArguments args = MakeArgs(parts);
    const std::vector<std::string> before = ArgsToVec(args);
    EXPECT_FALSE(ApplyLwwRewrites(&args));
    EXPECT_EQ(ArgsToVec(args), before);
  }
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
  s->Arm(0, "k", MvccStamp{});
  EXPECT_DEBUG_DEATH(
      s->Commit(1, 0,
                [s](DbIndex, std::string_view, const MvccStamp&, bool, const MvccStamp&) {
                  s->Commit(
                      2, 0,
                      [](DbIndex, std::string_view, const MvccStamp&, bool, const MvccStamp&) {});
                }),
      "re-entrantly");
}

// The hazard Commit()'s own doc comment names first: "fn must not call Arm()".
TEST(MvccStamperDeathTest, ArmFromCommitFnDies) {
  MvccStamper* s = FreshStamper();
  s->Arm(0, "k", MvccStamp{});
  EXPECT_DEBUG_DEATH(s->Commit(1, 0,
                               [s](DbIndex, std::string_view, const MvccStamp&, bool,
                                   const MvccStamp&) { s->Arm(0, "reentrant", MvccStamp{}); }),
                     "mid-iteration");
}
#endif  // NDEBUG

}  // namespace dfly
