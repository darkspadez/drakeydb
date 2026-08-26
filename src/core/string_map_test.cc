// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/string_map.h"

#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>
#include <mimalloc.h>

#include <algorithm>
#include <cstddef>
#include <memory_resource>
#include <random>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "base/logging.h"
#include "core/compact_object.h"
#include "core/detail/stateless_allocator.h"
#include "core/page_usage/page_usage_stats.h"

extern "C" {
#include "redis/zmalloc.h"
}

namespace dfly {

using namespace std;

class StringMapTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    auto* tlh = mi_heap_get_backing();
    init_zmalloc_threadlocal(tlh);
    InitTLStatelessAllocMR(PMR_NS::get_default_resource());
  }

  static void TearDownTestSuite() {
    mi_heap_collect(mi_heap_get_backing(), true);

    auto cb_visit = [](const mi_heap_t* heap, const mi_heap_area_t* area, void* block,
                       size_t block_size, void* arg) {
      LOG(ERROR) << "Unfreed allocations: block_size " << block_size
                 << ", allocated: " << area->used * block_size;
      return true;
    };

    mi_heap_visit_blocks(mi_heap_get_backing(), false /* do not visit all blocks*/, cb_visit,
                         nullptr);
  }

  StringMapTest() : mi_alloc_(mi_heap_get_backing()) {
  }

  void SetUp() override {
    sm_.reset(new StringMap(&mi_alloc_));
  }

  void TearDown() override {
    sm_.reset();
    EXPECT_EQ(zmalloc_used_memory_tl, 0);
  }

  MiMemoryResource mi_alloc_;
  std::unique_ptr<StringMap> sm_;
};

TEST_F(StringMapTest, Basic) {
  EXPECT_TRUE(sm_->AddOrUpdate("foo", "bar"));
  EXPECT_TRUE(sm_->Contains("foo"));
  auto it = sm_->Find("foo");
  EXPECT_STREQ("bar", it->second);

  it = sm_->begin();
  EXPECT_STREQ("foo", it->first);
  EXPECT_STREQ("bar", it->second);
  ++it;
  EXPECT_TRUE(it == sm_->end());

  for (const auto& k_v : *sm_) {
    EXPECT_STREQ("foo", k_v.first);
    EXPECT_STREQ("bar", k_v.second);
  }

  size_t sz = sm_->ObjMallocUsed();
  EXPECT_FALSE(sm_->AddOrUpdate("foo", "baraaaaaaaaaaaa2"));
  EXPECT_GT(sm_->ObjMallocUsed(), sz);
  it = sm_->begin();
  EXPECT_STREQ("baraaaaaaaaaaaa2", it->second);

  EXPECT_FALSE(sm_->AddOrSkip("foo", "bar2"));
  EXPECT_STREQ("baraaaaaaaaaaaa2", it->second);
}

TEST_F(StringMapTest, EmptyFind) {
  sm_->Find("bar");
}

TEST_F(StringMapTest, Ttl) {
  EXPECT_TRUE(sm_->AddOrUpdate("bla", "val1", 1));
  EXPECT_FALSE(sm_->AddOrUpdate("bla", "val2", 1));
  sm_->set_time(1);
  EXPECT_TRUE(sm_->AddOrUpdate("bla", "val2", 1));
  EXPECT_EQ(1u, sm_->UpperBoundSize());

  EXPECT_FALSE(sm_->AddOrSkip("bla", "val3", 2));

  // set ttl to 2, meaning that the key will expire at time 3.
  EXPECT_TRUE(sm_->AddOrSkip("bla2", "val3", 2));
  EXPECT_TRUE(sm_->Contains("bla2"));

  sm_->set_time(3);
  auto it = sm_->begin();
  EXPECT_TRUE(it == sm_->end());
}

TEST_F(StringMapTest, IterateExpired) {
  EXPECT_TRUE(sm_->AddOrUpdate("k1", "v1", 1));
  EXPECT_TRUE(sm_->AddOrUpdate("k2", "v2", 1));
  sm_->set_time(1);
  auto it = sm_->begin();
  it += 1;
  EXPECT_EQ(it, sm_->end());
}

TEST_F(StringMapTest, SetFieldExpireHasExpiry) {
  EXPECT_TRUE(sm_->AddOrUpdate("k1", "v1", 5));
  auto k = sm_->Find("k1");
  EXPECT_TRUE(k.HasExpiry());
  EXPECT_EQ(k.ExpiryTime(), 5);
  k.SetExpiryTime(1);
  EXPECT_TRUE(k.HasExpiry());
  EXPECT_EQ(k.ExpiryTime(), 1);
}

TEST_F(StringMapTest, SetFieldExpireNoHasExpiry) {
  EXPECT_TRUE(sm_->AddOrUpdate("k1", "v1"));
  auto k = sm_->Find("k1");
  EXPECT_FALSE(k.HasExpiry());
  k.SetExpiryTime(1);
  EXPECT_TRUE(k.HasExpiry());
  EXPECT_EQ(k.ExpiryTime(), 1);
}

TEST_F(StringMapTest, Bug3973) {
  for (unsigned i = 0; i < 8; i++) {
    EXPECT_TRUE(sm_->AddOrUpdate(to_string(i), "val"));
  }
  for (unsigned i = 0; i < 8; i++) {
    auto k = sm_->Find(to_string(i));
    ASSERT_FALSE(k.HasExpiry());
    k.SetExpiryTime(1);
    EXPECT_EQ(k.ExpiryTime(), 1);
  }
  for (unsigned i = 100; i < 1000; i++) {
    EXPECT_TRUE(sm_->AddOrUpdate(to_string(i), "val"));
  }

  // make sure the first 8 keys have expiry set
  for (unsigned i = 0; i < 8; i++) {
    auto k = sm_->Find(to_string(i));
    ASSERT_TRUE(k.HasExpiry());
    EXPECT_EQ(k.ExpiryTime(), 1);
  }
}

TEST_F(StringMapTest, Bug3984) {
  for (unsigned i = 0; i < 6; i++) {
    EXPECT_TRUE(sm_->AddOrUpdate(to_string(i), "val"));
  }
  for (unsigned i = 0; i < 6; i++) {
    auto k = sm_->Find(to_string(i));
    ASSERT_FALSE(k.HasExpiry());
    k.SetExpiryTime(1);
    EXPECT_EQ(k.ExpiryTime(), 1);
  }

  for (unsigned i = 0; i < 6; i++) {
    EXPECT_FALSE(sm_->AddOrUpdate(to_string(i), "val"));
  }
}

unsigned total_wasted_memory = 0;

TEST_F(StringMapTest, ReallocIfNeeded) {
  auto build_str = [](size_t i) { return to_string(i) + string(131, 'a'); };

  auto count_waste = [](const mi_heap_t* heap, const mi_heap_area_t* area, void* block,
                        size_t block_size, void* arg) {
    size_t used = block_size * area->used;
    total_wasted_memory += area->committed - used;
    return true;
  };

  for (size_t i = 0; i < 10'000; i++)
    sm_->AddOrUpdate(build_str(i), build_str(i + 1), i * 10 + 1);

  for (size_t i = 0; i < 10'000; i++) {
    if (i % 10 == 0)
      continue;
    sm_->Erase(build_str(i));
  }

  mi_heap_collect(mi_heap_get_backing(), true);
  mi_heap_visit_blocks(mi_heap_get_backing(), false, count_waste, nullptr);
  size_t wasted_before = total_wasted_memory;

  size_t underutilized = 0;
  PageUsage page_usage{CollectPageStats::NO, 0.9};
  for (auto it = sm_->begin(); it != sm_->end(); ++it) {
    underutilized += page_usage.IsPageForObjectUnderUtilized(it->first);
    it.ReallocIfNeeded(&page_usage);
  }
  // Check there are underutilized pages
  CHECK_GT(underutilized, 0u);

  total_wasted_memory = 0;
  mi_heap_collect(mi_heap_get_backing(), true);
  mi_heap_visit_blocks(mi_heap_get_backing(), false, count_waste, nullptr);
  size_t wasted_after = total_wasted_memory;

  // Check we waste significanlty less now
  EXPECT_GT(wasted_before, wasted_after * 2);

  EXPECT_EQ(sm_->UpperBoundSize(), 1000);
  for (size_t i = 0; i < 1000; i++)
    EXPECT_EQ(sm_->Find(build_str(i * 10))->second, build_str(i * 10 + 1));
}

TEST_F(StringMapTest, ExpiryChangesSize) {
  sm_->AddOrUpdate("field", "value");
  const size_t old_size = sm_->ObjMallocUsed();

  auto it = sm_->Find("field");
  it.SetExpiryTime(1);

  const size_t new_size = sm_->ObjMallocUsed();
  EXPECT_LT(old_size, new_size);

  sm_->AddOrUpdate("field", "value", 1);
  EXPECT_EQ(new_size, sm_->ObjMallocUsed());
}

TEST_F(StringMapTest, ExpiryWithMaxAndKeepTTL) {
  sm_->AddOrUpdate("field", "value", 100);
  auto k = sm_->Find("field");
  EXPECT_TRUE(k.HasExpiry());
  EXPECT_EQ(k.ExpiryTime(), 100);

  // ttl is copied from prev. if max value is supplied
  sm_->AddOrUpdate("field", "value", UINT32_MAX, true);
  k = sm_->Find("field");
  EXPECT_TRUE(k.HasExpiry());
  EXPECT_EQ(k.ExpiryTime(), 100);

  // max ttl value results in no expiry without keepttl
  sm_->AddOrUpdate("field", "value", UINT32_MAX);
  EXPECT_FALSE(sm_->Find("field").HasExpiry());

  // No prev. expiry, supplied ttl_sec value is used
  sm_->AddOrUpdate("field", "value", 10, true);
  k = sm_->Find("field");
  EXPECT_TRUE(k.HasExpiry());
  EXPECT_EQ(k.ExpiryTime(), 10);

  // object removed while adding due to expiry
  sm_->set_time(11);
  sm_->AddOrUpdate("field", "value", UINT32_MAX, true);
  k = sm_->Find("field");
  EXPECT_FALSE(k.HasExpiry());
}

TEST_F(StringMapTest, ExtractExisting) {
  sm_->AddOrUpdate("f1", "v1");
  sm_->AddOrUpdate("f2", "v2");
  EXPECT_EQ(sm_->UpperBoundSize(), 2u);

  auto entry = sm_->Extract("f1");
  ASSERT_TRUE(entry);

  // Verify the extracted entry has the correct value
  sds val = StringMap::GetValue(static_cast<sds>(entry.get()));
  EXPECT_EQ(string_view(val, sdslen(val)), "v1");

  // Verify it was removed from the map
  EXPECT_EQ(sm_->UpperBoundSize(), 1u);
  EXPECT_FALSE(sm_->Contains("f1"));
  EXPECT_TRUE(sm_->Contains("f2"));
}

TEST_F(StringMapTest, ExtractNonExisting) {
  sm_->AddOrUpdate("f1", "v1");
  auto entry = sm_->Extract("no_such_key");
  EXPECT_FALSE(entry);
  EXPECT_EQ(sm_->UpperBoundSize(), 1u);
}

TEST_F(StringMapTest, AddOrExchangeNew) {
  // Adding a new field returns nullptr (no previous entry)
  auto prev = sm_->AddOrExchange("f1", "v1");
  EXPECT_FALSE(prev);
  EXPECT_TRUE(sm_->Contains("f1"));
  EXPECT_STREQ(sm_->Find("f1")->second, "v1");
}

TEST_F(StringMapTest, AddOrExchangeReplace) {
  sm_->AddOrUpdate("f1", "old_value");
  EXPECT_EQ(sm_->UpperBoundSize(), 1u);

  auto prev = sm_->AddOrExchange("f1", "new_value");
  ASSERT_TRUE(prev);

  // Verify the returned entry has the old value
  sds prev_key = static_cast<sds>(prev.get());
  sds val = StringMap::GetValue(prev_key);
  EXPECT_EQ(string_view(val, sdslen(val)), "old_value");

  // Verify map now has the new value
  EXPECT_STREQ(sm_->Find("f1")->second, "new_value");
  EXPECT_EQ(sm_->UpperBoundSize(), 1u);
}

TEST_F(StringMapTest, AddOrExchangeWithTtl) {
  sm_->AddOrUpdate("f1", "v1", 100);

  auto prev = sm_->AddOrExchange("f1", "v2", 200);
  ASSERT_TRUE(prev);

  sds prev_key = static_cast<sds>(prev.get());
  sds val = StringMap::GetValue(prev_key);
  EXPECT_EQ(string_view(val, sdslen(val)), "v1");

  // Make sure new entry has correct value and ttl
  auto it = sm_->Find("f1");
  EXPECT_STREQ(it->second, "v2");
  EXPECT_TRUE(it.HasExpiry());
  EXPECT_EQ(it.ExpiryTime(), 200u);
}

TEST_F(StringMapTest, RandomPairsUniqueAfterSetExpiryTime) {
  sm_->Reserve(1024);
  for (unsigned i = 0; i < 20; i++) {
    EXPECT_TRUE(sm_->AddOrUpdate(to_string(i), "v"));
  }
  EXPECT_FALSE(sm_->ExpirationUsed());

  for (unsigned i = 0; i < 10; i++) {
    auto it = sm_->Find(to_string(i));
    ASSERT_FALSE(it.HasExpiry());
    it.SetExpiryTime(1);
  }
  // Validate the regression in all build types: DCHECK below is a no-op in
  // release, so RandomPairsUnique could still return 10 keys by chance.
  EXPECT_TRUE(sm_->ExpirationUsed());

  sm_->set_time(2);

  vector<sds> keys, vals;
  sm_->RandomPairsUnique(20, keys, vals, false);
  EXPECT_EQ(keys.size(), 10u);
}

TEST_F(StringMapTest, ExpireCollectChainUaf) {
  // Iterating after SetExpiryTime'd chain-tail entries expire reads through
  // a LinkKey freed by Delete in ExpireIfNeededInternal's loop. Reproduces
  // probabilistically without ASAN; the 32-trial loop raises crash rate.
  for (unsigned trial = 0; trial < 32; trial++) {
    StringMap sm(&mi_alloc_);
    for (unsigned i = 0; i < 20; i++) {
      ASSERT_TRUE(sm.AddOrUpdate(absl::StrCat("t", trial, "_", i), "v"));
    }
    for (unsigned i = 0; i < 10; i++) {
      sm.Find(absl::StrCat("t", trial, "_", i)).SetExpiryTime(1);
    }
    sm.set_time(2);

    unsigned alive = 0;
    for (auto it = sm.begin(); it != sm.end(); ++it) {
      ++alive;
    }
    EXPECT_EQ(alive, 10u);
  }
}

TEST_F(StringMapTest, FindAfterExpiredTailUaf) {
  // Find() on a missing key walks the chain to the tail. If the tail Object
  // has expired, ExpireIfNeeded inside Find2 frees prev's LinkKey, leaving
  // `curr` dangling for the subsequent Equal / curr->Next() reads.
  for (unsigned trial = 0; trial < 32; trial++) {
    StringMap sm(&mi_alloc_);
    for (unsigned i = 0; i < 20; i++) {
      ASSERT_TRUE(sm.AddOrUpdate(absl::StrCat("t", trial, "_", i), "v"));
    }
    for (unsigned i = 0; i < 10; i++) {
      sm.Find(absl::StrCat("t", trial, "_", i)).SetExpiryTime(1);
    }
    sm.set_time(2);

    for (unsigned i = 0; i < 50; i++) {
      sm.Find(absl::StrCat("missing", trial, "_", i));
    }
  }
}

TEST_F(StringMapTest, ExtractMultiple) {
  for (unsigned i = 0; i < 20; i++) {
    sm_->AddOrUpdate(to_string(i), "val" + to_string(i));
  }
  EXPECT_EQ(sm_->UpperBoundSize(), 20u);

  // Extract every other entry
  vector<StringMap::SdsEntry> extracted;
  for (unsigned i = 0; i < 20; i += 2) {
    auto entry = sm_->Extract(to_string(i));
    ASSERT_TRUE(entry);
    extracted.push_back(std::move(entry));
  }

  EXPECT_EQ(sm_->UpperBoundSize(), 10u);

  // Verify remaining entries
  for (unsigned i = 1; i < 20; i += 2) {
    EXPECT_TRUE(sm_->Contains(to_string(i)));
  }
}

// drakeydb: P4-0 Task 2b Important A -- falsifies the exact defect the whole-branch review found
// in fix round 1: a callback-based walk (container_utils::IterateSet/IterateMap, the reaper's
// original mechanism) only ever sees SURVIVING members reaching its callback, but
// DenseSet::IteratorBase::Advance skips ALREADY-EXPIRED entries internally without ever invoking
// it -- so a container whose members had ALL already expired (the canonical HEXPIRE-workload
// case: a large hash, uniform TTL, all elapsed) walked completely unbounded regardless of any
// cap on the callback's own invocation count. ReaperExpireStep must genuinely stop after
// max_slots RAW SLOTS examined, survivors and already-expired-and-skipped alike.
//
// Falsifying: reverting ReaperExpireStep's loop to only count survivors (e.g. incrementing a
// budget counter inside the `if (!ptr.IsEmpty() && ...)` branch instead of once per slot
// unconditionally) reproduces fix round 1's bug -- this test's kBudget-vs-kCount gap would then
// collapse to zero work done per call, the opposite failure, or (with the original
// container_utils-based walk swapped back in) all 5000 members expiring in the single first
// call, which the assertions below catch either way.
TEST_F(StringMapTest, ReaperExpireStepBoundsMassExpiry) {
  constexpr int kCount = 5000;
  for (int i = 0; i < kCount; ++i) {
    ASSERT_TRUE(sm_->AddOrUpdate(absl::StrCat("k", i), "v", 1));
  }
  ASSERT_EQ(sm_->UpperBoundSize(), size_t(kCount));
  sm_->set_time(2);  // every member's TTL (1) has now elapsed -- all of them, at once.

  constexpr uint32_t kBudget = 100;
  bool complete = sm_->ReaperExpireStep(kBudget);

  EXPECT_FALSE(complete) << "a single bounded call must not complete a full pass over a "
                            "container this much larger than its budget";
  // A budget of 100 slots must not let this one call expire anywhere close to all 5000 members
  // -- if it did, the walk is not actually bounded (fix round 1's exact bug).
  EXPECT_GT(sm_->UpperBoundSize(), kCount / 2)
      << "far more than kBudget members were expired in a single bounded call -- the walk is "
         "not actually bounded";
  EXPECT_LT(sm_->UpperBoundSize(), size_t(kCount))
      << "the call examined nothing at all -- the walk made no progress";

  // Repeated calls (the resume cursor) must still reach every member eventually, bounded by a
  // sane number of calls (~kCount/kBudget), not stall on the same prefix forever.
  int calls = 1;
  while (!complete) {
    ASSERT_LT(calls, 500) << "resume cursor is not making progress -- re-walking the same prefix";
    complete = sm_->ReaperExpireStep(kBudget);
    ++calls;
  }
  EXPECT_EQ(sm_->UpperBoundSize(), 0u);
  // ReaperExpireStep reports whether clearing is safe; it does not clear on its own (matching
  // db_slice.cc's actual caller, which only invokes ReaperClearMemberExpiration() when told to).
  ASSERT_TRUE(complete);
  sm_->ReaperClearMemberExpiration();
  EXPECT_FALSE(sm_->ExpirationUsed())
      << "a complete pass finding zero remaining member TTLs must allow clearing the sticky flag";
}

// drakeydb: P4-0 Task 2b, fix round 4 -- falsifies the terminal-node-only chain-walk bug: an
// earlier version of ReaperExpireStep checked HasTtl() exactly once, on whatever node the
// chain-walk loop ended up at after following every `.next` link (the chain's TAIL -- PushFront
// always inserts at the head, so the earliest-ever occupant of a bucket ends up furthest from
// it, at the end of the chain). But SetExpiryTime stamps the TTL bit on whichever DensePtr the
// iterator is CURRENTLY positioned at -- the head slot, or any interior link -- independent of
// where the chain's tail happens to be. A TTL armed on the head or an interior node was
// therefore invisible to that tail-only check.
//
// Getting a field onto a genuine non-tail position isn't just "insert it late": empirically (see
// this test's own development), MOST fields -- even ones inserted very late into a large,
// fairly-loaded table -- land as the sole occupant of their bucket (DenseSet's displacement
// mechanism, FindEmptyAround, resolves most collisions into a nearby empty slot rather than a
// real `.next` chain -- see dense_set.cc). For a lone occupant, head IS the tail, so arming it
// would be trivially caught by even the buggy code, masking whatever this test is trying to
// isolate. This test instead inserts a large field population, then uses Find()'s iterator to
// directly identify fields whose position IS mid-chain (curr_entry_->IsLink() == true, meaning
// something else still follows via `.next` -- i.e. this position is provably not the tail) and
// arms ONLY those -- guaranteeing every armed TTL sits somewhere the buggy tail-only check
// cannot see, rather than hoping insertion order makes it statistically likely.
//
// Falsifying: reverting ReaperExpireStep to check HasTtl() only once, after the chain-walk loop
// (fix round 3's version), reproduces the exact bug this closes -- verified below with the
// falsification's own verbatim output.
//
// Two ReaperExpireStep calls, not one: SetExpiryTime itself now also sets reaper_any_ttl_seen_
// directly (Important A, same round -- see dense_set.cc), independent of any walk, so a single
// call right after arming would pass even with the terminal-node-only bug reintroduced, for the
// wrong reason (the mutation-mirror masking the walk's own miss). The first call below is a
// "wash": it covers the whole table in one shot and, on returning (this function's own contract
// -- see its comment), unconditionally resets reaper_any_ttl_seen_ to false regardless of what
// it found, consuming Important A's side effect. Nothing mutates the container between the two
// calls, so the second call starts with reaper_any_ttl_seen_ genuinely false -- the only way it
// can become true again is the walk itself finding a HasTtl() node, which is exactly what this
// test needs to isolate.
TEST_F(StringMapTest, ReaperExpireStepChecksTtlOnEveryChainNode) {
  constexpr int kCount = 50000;
  for (int i = 0; i < kCount; ++i) {
    ASSERT_TRUE(sm_->AddOrUpdate(absl::StrCat("k", i), "v"));  // no ttl yet
  }
  ASSERT_FALSE(sm_->ExpirationUsed());

  // Scan a large trailing slice, arming ONLY fields whose Find() position is mid-chain (provably
  // non-tail -- see comment above). Skips the (majority) unchained fields entirely: arming one of
  // those would be trivially detected by even the buggy code (head == tail for a lone occupant),
  // which would mask the very thing this test needs to isolate.
  int armed = 0;
  for (int i = kCount - 5000; i < kCount && armed < 25; ++i) {
    auto it = sm_->Find(absl::StrCat("k", i));
    ASSERT_TRUE(it != sm_->end());
    if (!it.DebugCurrIsLink())
      continue;                 // lone occupant of its bucket -- not useful, see comment above.
    it.SetExpiryTime(1000000);  // far in the future -- never due during this test
    ++armed;
  }
  ASSERT_GT(armed, 0) << "no mid-chain field found in this slice to arm -- widen the scan range";
  ASSERT_TRUE(sm_->ExpirationUsed());

  sm_->set_time(1);  // nothing armed above is due; nothing else has a TTL at all.

  // A budget comfortably covering the whole table in a single call, like
  // ReaperExpireStepFullButLiveTtlPassDoesNotClearFlag below -- NOT a bounded-budget multi-call
  // drain like ReaperExpireStepBoundsMassExpiry above: this test's armed TTLs are never due, so
  // reaper_any_ttl_seen_ is (correctly, once fixed) true on every single pass forever; looping
  // "until complete" the way that test does would simply never terminate here.
  sm_->ReaperExpireStep(1000000);  // wash call -- see this test's comment above.

  bool complete = sm_->ReaperExpireStep(1000000);  // the actual call under test.
  // Mirrors db_slice.cc's real caller (gated on `complete`) before asserting on ExpirationUsed()
  // -- see the TruncatedPass/FullButLiveTtl tests above for why that matters for falsifiability:
  // with the terminal-node-only bug, `complete` comes back wrongly true here, this call actually
  // clears the flag, and the assertion below catches it.
  if (complete)
    sm_->ReaperClearMemberExpiration();

  // The actual assertion: a live (not-yet-due) member TTL survived the full pass on a field that
  // is provably not at any chain's tail -- ReaperExpireStep must not have reported this pass
  // clean, so ReaperClearMemberExpiration() above must not have run, and ExpirationUsed() must
  // still be true.
  EXPECT_FALSE(complete)
      << "a live, not-yet-due member TTL survived the walk but was reported as a clean pass";
  EXPECT_TRUE(sm_->ExpirationUsed())
      << "a live member TTL on a non-tail chain node was missed by the walk -- see this test's "
         "comment for why the terminal-node-only bug this falsifies specifically hides here";
  EXPECT_EQ(sm_->UpperBoundSize(), size_t(kCount)) << "nothing should have been expired yet";
}

// drakeydb: P4-0 Task 2b Important C -- falsifies the interaction the coordinator flagged as
// worse than the bug it fixes: clearing HasMemberExpiration()/ExpirationUsed() on a container
// whose UNEXAMINED tail still holds member TTLs would strand those members permanently, since
// with the flag cleared nothing would ever walk this container again. A single call over a
// container far bigger than the budget must report an incomplete pass, and the flag must stay
// set. Falsifiable, not just asserting a fact `ExpirationUsed()` alone can never disprove (fix
// round 3's version of this test only checked ExpirationUsed() without ever calling
// ReaperClearMemberExpiration(), so it would pass even if `complete` came back wrongly true --
// nothing in the test would have acted on that): mirrors db_slice.cc's actual caller, gating
// ReaperClearMemberExpiration() on `complete`, so a wrongly-true `complete` here would actually
// clear the flag and this assertion would catch it.
TEST_F(StringMapTest, ReaperExpireStepTruncatedPassDoesNotClearFlag) {
  constexpr int kCount = 5000;
  for (int i = 0; i < kCount; ++i) {
    ASSERT_TRUE(sm_->AddOrUpdate(absl::StrCat("k", i), "v", 1));
  }
  ASSERT_TRUE(sm_->ExpirationUsed());
  sm_->set_time(2);  // all elapsed, same setup as the bounding test above.

  bool complete = sm_->ReaperExpireStep(100);  // budget far smaller than kCount
  // Mirrors db_slice.cc's actual caller (gated on `complete`) BEFORE asserting anything about
  // `complete` itself -- an EXPECT (not ASSERT) below, and this call, must both run even if
  // `complete` unexpectedly came back true, or the flag-clearing bug this test exists to catch
  // would never actually happen inside the test.
  if (complete)
    sm_->ReaperClearMemberExpiration();
  EXPECT_FALSE(complete);
  EXPECT_TRUE(sm_->ExpirationUsed())
      << "a truncated pass must never look like a safe-to-clear pass";
  EXPECT_GT(sm_->UpperBoundSize(), 0u);
}

// drakeydb: P4-0 Task 2b Important C -- the companion case: a FULL pass (covers every slot) that
// is not CLEAN (some member still carries a live, not-yet-due TTL) must also not clear the flag
// -- "examined everything" alone is not sufficient, only "examined everything and found no
// remaining TTL" is. Falsifies clearing on `end >= entries_.size()` alone, without the
// `!reaper_any_ttl_seen_` conjunct.
TEST_F(StringMapTest, ReaperExpireStepFullButLiveTtlPassDoesNotClearFlag) {
  constexpr int kCount = 50;
  for (int i = 0; i < kCount; ++i) {
    ASSERT_TRUE(sm_->AddOrUpdate(absl::StrCat("k", i), "v", 100));  // far in the future
  }
  ASSERT_TRUE(sm_->ExpirationUsed());
  sm_->set_time(1);  // nothing is due yet.

  // A budget comfortably covering the whole (small) table in one call, so this pass is
  // structurally complete (examines every slot) -- ReaperExpireStep's return value still comes
  // back false, though, because "complete" alone is not "safe to clear": it also requires no
  // remaining member TTL, which this container still has (every one, none yet due).
  bool complete = sm_->ReaperExpireStep(10000);
  // Mirrors db_slice.cc's actual caller (gated on `complete`), run before asserting anything
  // about `complete` itself, so a wrongly-true `complete` actually exercises the flag-clearing
  // bug this test exists to catch instead of the assertion below passing vacuously.
  if (complete)
    sm_->ReaperClearMemberExpiration();
  EXPECT_FALSE(complete)
      << "a complete pass that still found live, not-yet-due member TTLs must not report itself "
         "safe to clear -- those members would never be swept again if it did";
  EXPECT_TRUE(sm_->ExpirationUsed()) << "the flag must still be set; nothing calls "
                                        "ReaperClearMemberExpiration() when complete is false";
  EXPECT_EQ(sm_->UpperBoundSize(), size_t(kCount)) << "nothing should have been expired yet";
}

}  // namespace dfly
