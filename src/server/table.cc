// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/table.h"

#include "base/flags.h"
#include "base/logging.h"
#include "core/top_keys.h"
#include "server/cluster_support.h"
#include "server/multi_master.h"
#include "server/server_state.h"

using namespace std;
namespace dfly {
#define ADD(x) (x) += o.x

// It should be const, but we override this variable in our tests so that they run faster.
unsigned kInitSegmentLog = 3;

void DbTableStats::AddTypeMemoryUsage(unsigned type, int64_t delta) {
  if (type >= memory_usage_by_type.size()) {
    LOG(DFATAL) << "Encountered unknown type when aggregating per-type memory: " << type;
    return;
  }

  DCHECK_GE(obj_memory_usage, memory_usage_by_type[type]);

  if (delta < 0 && memory_usage_by_type[type] < size_t(-delta)) {
#ifdef NDEBUG
    LOG_EVERY_T(ERROR, 1)
#else
    LOG_EVERY_T(FATAL, 1)
#endif
        << "Encountered underflow memory usage when aggregating per-type memory: "
        << memory_usage_by_type[type] << " + " << delta << ", type: " << type;

    // Truncate delta to avoid underflow, but keep the memory usage consistent with the sum of
    // per-type usage.
    delta = -static_cast<int64_t>(memory_usage_by_type[type]);
  }

  obj_memory_usage += delta;
  memory_usage_by_type[type] += delta;
}

DbTableStats& DbTableStats::operator+=(const DbTableStats& o) {
  constexpr size_t kDbSz = sizeof(DbTableStats) - sizeof(memory_usage_by_type);
  // drakeydb: P4-1 Task 5 -- +16 for mvcc_entries/mvcc_tombstones. P4-2 Task 4 -- +8 for
  // mvcc_key_dup_bytes (96 -> 104).
  static_assert(kDbSz == 104);

  ADD(inline_keys);
  ADD(expire_count);
  ADD(member_expire_count);
  ADD(mvcc_entries);
  ADD(mvcc_tombstones);
  ADD(mvcc_key_dup_bytes);
  ADD(obj_memory_usage);
  ADD(tiered_entries);
  ADD(tiered_used_bytes);
  ADD(events.hits);
  ADD(events.misses);
  ADD(events.expired_keys);
  ADD(events.evicted_keys);

  for (size_t i = 0; i < o.memory_usage_by_type.size(); ++i) {
    memory_usage_by_type[i] += o.memory_usage_by_type[i];
  }

  return *this;
}

SlotStats& SlotStats::operator+=(const SlotStats& o) {
  static_assert(sizeof(SlotStats) == 40);

  ADD(key_count);
  ADD(total_reads);
  ADD(total_writes);
  ADD(memory_bytes);
  ADD(tiered_bytes);
  return *this;
}

std::optional<const IntentLock> LockTable::Find(LockTag tag) const {
  LockFp fp = tag.Fingerprint();
  if (auto it = locks_.find(fp); it != locks_.end())
    return it->second;
  return std::nullopt;
}

std::optional<const IntentLock> LockTable::Find(uint64_t fp) const {
  if (auto it = locks_.find(fp); it != locks_.end())
    return it->second;
  return std::nullopt;
}

void LockTable::Release(uint64_t fp, IntentLock::Mode mode) {
  auto it = locks_.find(fp);
  DCHECK(it != locks_.end()) << fp;

  it->second.Release(mode);
  if (it->second.IsFree())
    locks_.erase(it);
}

[[maybe_unused]] constexpr size_t kSzTable = sizeof(DbTable);

DbTable::SampleTopKeys::~SampleTopKeys() {
  delete top_keys;
}

DbTable::SampleUniqueKeys::~SampleUniqueKeys() {
  delete[] dense_hll;
}

// drakeydb: P4-2, final review round 2 (Critical) -- see DbTable::FromPrime's declaration
// (table.h) for why a serializer cannot resolve a bucket's owner from its own pointers alone.
// Namespace-scope thread_local, mirroring snapshot.cc's tl_slice_snapshots: DbTable is a
// per-thread object (it stores thread_index and its destructor DCHECKs it), so a thread-local
// map is the exact right scope -- no lock, no cross-thread visibility question. Entries live and
// die with the DbTable, including one already detached from its DbSlice by a flush and kept alive
// only by a snapshot's captured intrusive_ptr.
thread_local absl::flat_hash_map<const PrimeTable*, DbTable*> tl_prime_owners;

DbTable* DbTable::FromPrime(const PrimeTable* pt) {
  auto it = tl_prime_owners.find(pt);
  return it == tl_prime_owners.end() ? nullptr : it->second;
}

DbTable::DbTable(PMR_NS::memory_resource* mr, DbIndex db_index)
    : prime(kInitSegmentLog, detail::PrimeTablePolicy{}, mr),
      mcflag(0, detail::ExpireTablePolicy{}, mr),
      index(db_index) {
  if (IsActiveReplica())
    mvcc = std::make_unique<MvccTable>(0, detail::ExpireTablePolicy{}, mr);
  if (IsClusterEnabled()) {
    slots_stats.reset(new SlotStats[kMaxSlotNum + 1]);
  }
  thread_index = ServerState::tlocal()->thread_index();
  tl_prime_owners[&prime] = this;
}

DbTable::~DbTable() {
  DCHECK_EQ(thread_index, ServerState::tlocal()->thread_index());
  // Guarded on identity rather than erased blindly: a (DCHECK-violating) destruction on the wrong
  // thread must not evict a live sibling's entry from that thread's map.
  if (auto it = tl_prime_owners.find(&prime); it != tl_prime_owners.end() && it->second == this)
    tl_prime_owners.erase(it);
  delete sample_top_keys;
  delete sample_unique_keys;
}

void DbTable::Clear() {
  prime.Clear();
  mcflag.Clear();
  if (mvcc)
    mvcc->Clear();
  stats = DbTableStats{};
  expire_cursor = PrimeTable::Cursor::end();
  segment_defrag_cursor = PrimeTable::Cursor::end();
  mvcc_defrag_cursor = MvccTable::Cursor::end();
}

// drakeydb: P4-2, final review (Critical) -- see the declaration in table.h for why this lives on
// DbTable. The `if (!mvcc)` test must stay above the GetSlice call.
optional<MvccStamp> DbTable::GetMvcc(const PrimeKey& key) const {
  if (!mvcc)
    return nullopt;
  string scratch;
  auto it = mvcc->Find(key.GetSlice(&scratch));
  if (it.is_done())
    return nullopt;
  return it->second;
}

PrimeIterator DbTable::Launder(PrimeIterator it, string_view key) {
  if (!it.IsOccupied() || it->first != key) {
    it = prime.Find(key);
  }
  return it;
}

}  // namespace dfly
