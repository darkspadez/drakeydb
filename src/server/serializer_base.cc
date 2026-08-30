// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/serializer_base.h"

#include <absl/strings/match.h>
#include <absl/strings/str_join.h>

#include "base/flags.h"
#include "base/logging.h"
#include "redis/redis_aux.h"
#include "server/common_types.h"
#include "server/db_slice.h"
#include "server/engine_shard.h"
#include "server/execution_state.h"
#include "server/journal/journal.h"
#include "server/synchronization.h"
#include "server/table.h"
#include "server/tiered_storage.h"
#include "util/fibers/fibers.h"
#include "util/fibers/stacktrace.h"
#include "util/fibers/synchronization.h"

ABSL_DECLARE_FLAG(bool, serialization_tagged_chunks);

namespace dfly {

void BucketDependencies::Increment(BucketIdentity bucket) {
  auto& counter = deps_[bucket];
  if (!counter)
    counter = std::make_shared<LocalLatch>();
  counter->lock();
}

void BucketDependencies::Decrement(BucketIdentity bucket) {
  auto it = deps_.find(bucket);
  CHECK(it != deps_.end());

  it->second->unlock();
  if (!it->second->IsBlocked())
    deps_.erase(it);

  if (deps_.empty())
    empty_q_.notify_all();
}

void BucketDependencies::Wait(BucketIdentity bucket) const {
  auto it = deps_.find(bucket);
  if (it == deps_.end())
    return;

  auto counter = it->second;  // copy value for address stability
  counter->Wait();
}

void BucketDependencies::WaitEmpty() const {
  util::fb2::NoOpLock lock;
  empty_q_.wait(lock, [&] { return deps_.empty(); });
}

void DelayedEntryHandler::EnqueueOffloaded(BucketIdentity bucket, DbIndex db_index, PrimeKey pk,
                                           const PrimeValue& pv, time_t expire_time,
                                           uint32_t mc_flags, const MvccStamp& mvcc) {
  DCHECK(pv.IsExternal());
  DCHECK(!pv.IsCool());

  auto key = pk.ToString();
  auto future = ReadTieredValue(db_index, key, pv, EngineShard::tlocal()->tiered_storage());
  auto entry = std::make_unique<TieredDelayedEntry>(db_index, std::move(pk), std::move(future),
                                                    expire_time, mc_flags, mvcc);

  deps_.Increment(bucket);
  delayed_entries_.emplace(bucket, std::move(entry));
}

void DelayedEntryHandler::ProcessDelayedEntries(bool force, BucketIdentity flush_bucket,
                                                ExecutionState* cntx) {
  const size_t kMaxDelayedEntries = 512;
  if (delayed_entries_.size() > kMaxDelayedEntries)
    force |= true;

  // Extract ahead because of possible iterator invalidation during suspension (Get/Serialize)
  // if multiple fibers progress on delayed entries
  std::vector<decltype(delayed_entries_)::node_type> targets;

  // Flush all entries of bucket if provided
  if (flush_bucket) {
    auto [it, end] = delayed_entries_.equal_range(flush_bucket);
    while (it != end)
      targets.push_back(delayed_entries_.extract(it++));
  }

  // Serialize the delayed entries that are resolved, or all if force it true
  for (auto it = delayed_entries_.begin(); it != delayed_entries_.end();) {
    if (!force && !it->second->value.IsResolved())
      it++;
    else
      targets.push_back(delayed_entries_.extract(it++));
  }

  // Serialize all targets
  for (auto& target : targets) {
    auto& entry = target.mapped();
    auto value = entry->value.Get();

    if (!value.has_value()) {
      deps_.Decrement(target.key());
      cntx->ReportError(make_error_code(std::errc::io_error),
                        absl::StrCat("Failed to read tiered key: ", entry->key.ToString()));
      return;
    }

    SerializeFetchedEntry(*entry, *value);

    deps_.Decrement(target.key());
  };
}

SerializerBase::SerializerBase(DbSlice* slice, ExecutionState* cntx)
    : DelayedEntryHandler(static_cast<BucketDependencies&>(*this)),
      db_slice_(slice),
      base_cntx_(cntx),
      // Enable stream mutex if tagged chunks are disabled
      stream_mu_(!absl::GetFlag(FLAGS_serialization_tagged_chunks)) {
  DCHECK(base_cntx_);
}

SerializerBase::~SerializerBase() {
}

// drakeydb: P4-2, final review (Critical) -- resolve `pk`'s stamp from the same captured DbTable
// that owns the value. RegisterChangeListener retains that table precisely so point-in-time
// content survives FLUSHALL replacing the live table.
//
// Getting this wrong is not merely lossy. A stamp read from the wrong table is either absent
// (-> {0,0} -> rdb_save.cc's `!mvcc.Empty()` gate omits the opcode, silently stripping
// authority) or, for a key re-created after the flush, FABRICATED: the post-flush write's
// authority pasted onto the pre-flush value -- a {value, stamp} pair no node ever authored, which
// ties bit-identically against a peer's genuine newer value so LWW can never reconcile it.
//
// Only a bucket owned by this consumer's captured table can reach SerializeEntry. Before a flush,
// OnChange sees that same table. After a flush (or database activation), any occupied bucket in the
// replacement table was inserted after snapshot_version_ and ProcessBucket skips it. Likewise,
// FlushChangeToEarlierCallbacks forwards a bucket only when its version is old enough for the
// earlier snapshot; such a bucket belongs to the table both consumers captured. Therefore a
// live/foreign-table fallback is both unnecessary and dangerous: it would make an impossible
// state fabricate authority instead of failing closed. An unknown or null owner yields
// MvccStamp{} -- unstamped, never a guess or dereference.
//
// Cost is unchanged on a non-active node: DbTable::GetMvcc (table.cc) returns on the null side
// table before pk.GetSlice(&scratch) can materialize/allocate anything.
MvccStamp SerializerBase::MvccOf(DbIndex db_index, const PrimeKey& pk,
                                 const PrimeTable* owner) const {
  const DbTable* table = nullptr;
  if (db_index < db_array_.size() && db_array_[db_index] && &db_array_[db_index]->prime == owner) {
    table = db_array_[db_index].get();
  }
  if (table == nullptr)
    return MvccStamp{};
  return table->GetMvcc(pk).value_or(MvccStamp{});
}

void SerializerBase::SerializeEntry(BucketIdentity bucket, DbIndex db_index, const PrimeKey& pk,
                                    const PrimeValue& pv, const PrimeTable* owner) {
  if (pv.IsExternal() && pv.IsCool())
    return SerializeEntry(bucket, db_index, pk, pv.GetCool().record->value, owner);

  time_t expire_time = pk.GetExpireTime();
  // drakeydb: P4-2, final review -- GetMCFlag has the same live-DbSlice shape this line used to
  // have and therefore the same captured-vs-live mismatch for mcflags across a mid-save FLUSHALL.
  // That is pre-existing upstream behavior, deliberately left untouched here and reported
  // upstream separately; do not "fix" it in passing without upstream's sign-off.
  uint32_t mc_flags = pv.HasFlag() ? db_slice_->GetMCFlag(db_index, pk) : 0;
  // drakeydb: P4-2 Task 1 -- MvccStamp{} (unstamped/absent) on a non-active node.
  MvccStamp mvcc = MvccOf(db_index, pk, owner);

  if (pv.IsExternal()) {
    // TODO: we loose the stickiness attribute by cloning like this PrimeKey.
    EnqueueOffloaded(bucket, db_index, PrimeKey{pk.ToString()}, pv, expire_time, mc_flags, mvcc);
  } else {
    std::lock_guard lk{stream_mu_};
    SerializeEntryLocked(db_index, pk, pv, expire_time, mc_flags, mvcc);
  }
}

void SerializerBase::SerializeFetchedEntry(const TieredDelayedEntry& tde, const PrimeValue& pv) {
  std::lock_guard lk{stream_mu_};
  SerializeEntryLocked(tde.dbid, tde.key, pv, tde.expire, tde.mc_flags, tde.mvcc);
}

// Ordering invariant:
//   For any key K, the replica must receive K's baseline value strictly before any journal entry
//   that mutates K.
// RegisterChangeListener registers the DbSlice callback that routes mutations through
//   SerializerBase::OnChange. ConsumeJournalChange runs later on the
//   same fiber, so the baseline is serialized first. stream_mu_ prevents this callback path
//   from interleaving with the traversal fiber's bucket serialization, which may preempt while
//   emitting large values.
void SerializerBase::RegisterChangeListener(bool replication) {
  db_array_ = db_slice_->databases();  // copy pointers to survive flush
  db_slice_->RegisterOnChange(this);
  eventually_consistent_ = replication;
}

void SerializerBase::UnregisterChangeListener() {
  DCHECK(!IsAnyBucketBlocked());
  if (snapshot_version_ > 0)
    if (!db_slice_->UnregisterOnChange(this))
      LOG(DFATAL) << "UnregisterOnChange failed for snapshot_version_ " << snapshot_version_;
}

bool SerializerBase::ProcessBucket(DbIndex db_index, PrimeTable::bucket_iterator it,
                                   bool on_update) {
  // Check if this bucket is stale
  if (it.is_done() || it.GetVersion() >= snapshot_version_) {
    stats_.buckets_skipped++;

    // Update versions for empty buckets to mark that won't visit them anymore
    // TODO: Flush changes to earlier callbacks
    if (it.GetVersion() < snapshot_version_)
      it.SetVersion(snapshot_version_);

    // Force flush all delayed entries in the touched bucket
    if (EngineShard::tlocal()->tiered_storage() != nullptr && on_update)
      ProcessDelayedEntries(false, it.bucket_address(), base_cntx_);

    // Wait for all dependencies to be resolved
    BucketDependencies::Wait(it.bucket_address());
    return false;
  }

  // For non updates (traversal flow), flush change to earlier snapshots and
  // acquire serialization latch.
  // We must make sure that earlier snapshots serialized this bucket before we update its
  // version below.
  if (!on_update)
    db_slice_->FlushChangeToEarlierCallbacks(db_index, DbSlice::Iterator::FromPrime(it),
                                             snapshot_version_);

  // The block above with updating earlier callbacks is not exlusive - check version again
  if (it.GetVersion() >= snapshot_version_)
    return ProcessBucket(db_index, it, on_update);  // for the false path

  // We call it before SerializeBucketLocked because it dchecks on bucket version.
  it.SetVersion(snapshot_version_);
  BucketDependencies::Increment(it.bucket_address());

  stats_.keys_serialized += SerializeBucketLocked(db_index, it, on_update);
  stats_.buckets_serialized++;
  stats_.buckets_on_change += unsigned(on_update);

  BucketDependencies::Decrement(it.bucket_address());

  if (EngineShard::tlocal()->tiered_storage() != nullptr)
    ProcessDelayedEntries(false, on_update ? it.bucket_address() : 0, base_cntx_);

  return true;
}

void SerializerBase::WaitForNoBucketBlocked() const {
  BucketDependencies::WaitEmpty();
}

void SerializerBase::OnChange(DbIndex db_index, const ChangeReq& req) {
  // OnChangeBlocking can be reached almost from any fiber.
  // Specifically, they can be called from the following fibers:
  // 1. shard_queue, SliceSnapshot - trivial.
  // 2. DflyConn/AsyncFiber - connection fiber that runs an inline transaction.
  // 3. l2_queue via pipelining.
  // 4."Debug/Traverse" - DEBUG OBJHIST/UNIQ-STRS delete lazy-expired empty collections.
  // 5. "Dispatched" -  OnAllShards(... { migration->RunSync(); });
  // 6. "shard_stable_sync_read" - on replica

  // We call Process even for up-to-date buckets to ensure all operations (delayed) are finished.
  for (auto it : req.buckets())
    ProcessBucket(db_index, it, true);
}

}  // namespace dfly
