// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/snapshot.h"

#include <absl/strings/str_cat.h>

#include <mutex>

#include "base/cycle_clock.h"
#include "base/flags.h"
#include "base/logging.h"
#include "core/search/base.h"
#include "server/db_slice.h"
#include "server/engine_shard_set.h"
#include "server/execution_state.h"
#include "server/journal/journal.h"
#include "server/multi_master.h"
#include "server/rdb_extensions.h"
#include "server/rdb_save.h"
#include "server/search/global_hnsw_index.h"
#include "server/search/serialization_utils.h"
#include "server/server_state.h"
#include "server/tiered_storage.h"
#include "strings/human_readable.h"
#include "util/fibers/fibers.h"
#include "util/fibers/stacktrace.h"
#include "util/fibers/synchronization.h"

ABSL_FLAG(bool, background_snapshotting, false, "Whether to run snapshot as a background fiber");

ABSL_FLAG(bool, serialize_hnsw_index, false, "Serialize HNSW vector index graph structure");
ABSL_FLAG(bool, serialization_tagged_chunks, true,
          "Allow serializer output to be split into tagged chunks and reassembled by receiver");

namespace dfly {

using namespace std;
using namespace util;
using namespace chrono_literals;

using facade::operator""_KB;

namespace {
thread_local absl::flat_hash_set<SliceSnapshot*> tl_slice_snapshots;

// Controls the chunks size for pushing serialized data. The larger the chunk the more CPU
// it may require (especially with compression), and less responsive the server may be.
constexpr size_t kMinBlobSize = 8_KB;

// drakeydb: P4-3 Task 5 review fix (I2) -- hoisted out of IterateBucketsFb (below) so
// SerializeTombstones can share the identical yield threshold instead of re-deriving it; a
// function-local static (not a namespace-scope `const`) avoids any static-init-order question
// around base::CycleClock::Frequency() -- computed lazily, once, on first call.
uint64_t CyclesPerJiffy() {
  static const uint64_t kCyclesPerJiffy = base::CycleClock::Frequency() >> 16;  // ~15usec.
  return kCyclesPerJiffy;
}

}  // namespace

SliceSnapshot::SliceSnapshot(CompressionMode compression_mode, DbSlice* slice,
                             SnapshotDataConsumerInterface* consumer, ExecutionState* cntx,
                             DflyVersion replica_dfly_version, bool peer_mode)
    : SerializerBase(slice, cntx),
      compression_mode_(compression_mode),
      replica_dfly_version_(replica_dfly_version),
      peer_mode_(peer_mode),
      consumer_(consumer) {
  tl_slice_snapshots.insert(this);
}

SliceSnapshot::~SliceSnapshot() {
  DCHECK(db_slice_->shard_owner()->IsMyThread());
  tl_slice_snapshots.erase(this);
}

size_t SliceSnapshot::GetThreadLocalMemoryUsage() {
  size_t mem = 0;
  for (SliceSnapshot* snapshot : tl_slice_snapshots) {
    mem += snapshot->GetBufferCapacity();
  }
  return mem;
}

bool SliceSnapshot::IsSnaphotInProgress() {
  return !tl_slice_snapshots.empty();
}

void SliceSnapshot::Start(bool stream_journal, SnapshotFlush allow_flush) {
  DCHECK(!snapshot_fb_.IsJoinable());

  use_background_mode_ = absl::GetFlag(FLAGS_background_snapshotting);
  SerializerBase::RegisterChangeListener(stream_journal);

  if (stream_journal) {
    journal_cb_id_ = journal::RegisterConsumer(this);
  }

  size_t flush_threshold = 0;
  RdbSerializer::ConsumeFun consume_fun;
  if (allow_flush == SnapshotFlush::kAllow) {
    flush_threshold = ServerState::tlocal()->serialization_max_chunk_size;
    // The callback receives data directly from the serializer, no need to call back into it.
    if (flush_threshold != 0)
      consume_fun = std::bind_front(&SliceSnapshot::ConsumeBigValueChunk, this);
  }

  bool serialize_index = SaveMode() != dfly::SaveMode::RDB &&
                         absl::GetFlag(FLAGS_serialize_hnsw_index) &&
                         replica_dfly_version_ >= DflyVersion::VER6;

  serializer_ = std::make_unique<RdbSerializer>(compression_mode_, consume_fun, flush_threshold);

  if (allow_flush == SnapshotFlush::kAllow) {
    serializer_->SetTagEntries(absl::GetFlag(FLAGS_serialization_tagged_chunks));
  }

  VLOG(1) << "DbSaver::Start - saving entries with version less than " << snapshot_version_;

  fb2::Fiber::Opts opts{.priority = use_background_mode_ ? fb2::FiberPriority::BACKGROUND
                                                         : fb2::FiberPriority::NORMAL,
                        .name = absl::StrCat("SliceSnapshot-", ProactorBase::me()->GetPoolIndex())};
  snapshot_fb_ = fb2::Fiber(opts, [this, stream_journal, serialize_index] {
    if (serialize_index) {
      // TODO add error processing for index serialization
      SearchSerializer::Serialize(serializer_.get(), db_slice_,
                                  std::bind(&SliceSnapshot::PushSerialized, this, false));
    }
    // drakeydb: P4-3 Task 5 -- unlike serialize_index above (search-specific, version-gated),
    // this runs unconditionally: RDB_OPCODE_DF_TOMBSTONES's own write-gate (IsActiveReplica() +
    // non-empty table, checked inside) decides whether anything is actually emitted.
    this->SerializeTombstones();
    this->IterateBucketsFb(stream_journal);
    UnregisterChangeListener();
    consumer_->Finalize();
    VLOG(1) << "Serialization peak bytes: " << serializer_->GetSerializationPeakBytes();
  });
}

// Called only for replication use-case.
void SliceSnapshot::FinalizeJournalStream(bool cancel) {
  VLOG(1) << "FinalizeJournalStream";
  DCHECK(db_slice_->shard_owner()->IsMyThread());
  if (!journal_cb_id_) {  // Finalize only once.
    // In case of incremental snapshotting in StartIncremental, if an error is encountered,
    // journal_cb_id_ may not be set, but the snapshot fiber is still running.
    snapshot_fb_.JoinIfNeeded();
    return;
  }
  uint32_t cb_id = journal_cb_id_;
  journal_cb_id_ = 0;

  // Wait for serialization to finish in any case.
  snapshot_fb_.JoinIfNeeded();

  journal::UnregisterConsumer(cb_id);
  if (!cancel) {
    // always succeeds because serializer_ flushes to string.
    VLOG(1) << "FinalizeJournalStream lsn: " << journal::GetLsn();
    std::ignore = serializer_->SendJournalOffset(journal::GetLsn());
    PushSerialized(true);
  }
}

// drakeydb: P4-3 Task 5 -- see snapshot.h's doc comment for why this runs from the per-shard
// PROLOGUE (called from Start() above, beside SearchSerializer::Serialize) instead of an
// epilogue: RdbSaver has no per-shard epilogue hook. Ordering relative to IterateBucketsFb below,
// and relative to any concurrent journal blob on this same shard, carries no meaning either way:
// tombstone application is itself LWW-guarded (MergeAccepts, mvcc.h), so whichever of the three
// lands last for a given key resolves the same way regardless of ordering (rdb_load.cc's
// HandleTombstones says the same on the read side) -- UNDER merge_lww_ specifically. That
// guarantee was completed by P4-3 Task 6 (rdb_load.cc's HandleTombstones `install` lambda): before
// it, a tombstone landing against a RESIDENT LIVE key or a resident TOMBSTONE was not yet compared
// via MergeAccepts on the apply side, so the claim held only for the "no prior entry at all" case.
// Under merge_lww_ it now holds unconditionally (review fix I3: an earlier version of this
// sentence overclaimed unconditionally for every loader). A non-merge load (a local RDB file,
// DEBUG LOAD, or a plain Dragonfly replica's full sync) instead keeps D-7's separate,
// simpler rule -- skip a resident live key, otherwise install/overwrite unconditionally -- which
// needs no ordering guarantee either, since it never depends on comparing against the key stream's
// own outcome in the first place.
//
// Write side only -- gated on IsActiveReplica() and a non-empty tombstone table per db (D-7); the
// read side (RdbLoader::HandleTombstones) is unconditional. An inactive node's DbTable::mvcc is
// always null (table.cc), so the per-db `!db->mvcc` check below already makes this a no-op then
// too, but the explicit IsActiveReplica() gate up front makes that first-class rather than
// incidental, and avoids the table.size()-driven work below entirely on the far more common
// (inactive) path.
void SliceSnapshot::SerializeTombstones() {
  if (!IsActiveReplica())
    return;

  // drakeydb: D-10 save-time GC -- a tombstone already past its GC deadline is dropped HERE
  // rather than shipped: nothing on the load side ever re-evaluates a persisted tombstone's
  // deadline against "now" (TombstoneGcStep, db_slice.cc, only walks the LIVE table on an idle
  // tick; a loaded tombstone just sits there until that GC eventually reaps it), so an
  // already-expired one would otherwise ride the RDB and its own TTL semantics indefinitely.
  const uint64_t ttl_ms = absl::GetFlag(FLAGS_multi_master_tombstone_ttl) * 1000;
  const uint64_t now_ms = GetCurrentTimeMs();
  std::string scratch;

  // drakeydb: P4-3 Task 5 review fix (I2) -- bounds this section's peak RAM to one chunk's worth
  // of tombstones, not the whole db's (a whole-db materialize + single trailing push was ~2
  // copies of every tombstone in RAM at the default cap, with no yield/throttle for the entire
  // span). Emits one RDB_OPCODE_DF_TOMBSTONES section PER CHUNK instead of one section for the
  // whole db; the loader (HandleTombstones, rdb_load.cc) already treats any number of sections
  // per db as ordinary repeats of the same opcode, so a db needing multiple chunks is invisible
  // on the read side. Matches SearchSerializer's own HNSW node batching (serialization_utils.cc,
  // kBatchSize = 1000).
  constexpr size_t kChunkSize = 1000;

  for (DbIndex db_indx = 0; db_indx < db_array_.size(); ++db_indx) {
    auto& db = db_array_[db_indx];
    if (!db || !db->mvcc)
      continue;

    std::vector<std::pair<std::string, MvccStamp>> chunk;
    // drakeydb: P4-3 Task 5 review fix (I1) -- SaveTombstoneSection (rdb_save.h/.cc) writes the
    // whole chunk as one all-or-nothing unit; on failure it logs and drops just this chunk
    // (already rolled back out of the buffer by SaveTombstoneSection's own transaction), leaving
    // no partial opcode/payload behind for the next opcode byte to desync against.
    auto flush_chunk = [&] {
      if (chunk.empty())
        return;
      if (auto ec = serializer_->SaveTombstoneSection(db_indx, chunk); ec)
        LOG(ERROR) << "Failed to save tombstone section for db " << db_indx << ": " << ec.message();
      chunk.clear();
      // drakeydb: review fix (I2) -- same push/yield/throttle shape IterateBucketsFb uses after
      // every bucket (below in this file), reused here via the shared CyclesPerJiffy() so this
      // section's flow control matches the bucket path's instead of bypassing it.
      PushSerialized(false);
      if (use_background_mode_) {
        ThisFiber::Yield();
      } else if (ThisFiber::GetRunningTimeCycles() > CyclesPerJiffy()) {
        ThisFiber::Yield();
      }
      ServerState::tlocal()->GetEgressThrottler().Throttle();
    };

    detail::DashCursor cursor;
    do {
      {
        // drakeydb: review fix (I2) -- scoped tightly around the Traverse call only (unlike the
        // original version, which let the guard's scope span the chunk-size check and the
        // potentially-yielding flush_chunk() call below): flush_chunk() may call
        // ThisFiber::Yield()/Throttle(), which must never happen while a FiberAtomicGuard is
        // still live.
        //
        // Mirrors TombstoneGcStep's own guard (db_slice.cc) for the identical reason: a
        // Mvcc() == 0 slot is never a real, committed tombstone -- only a synchronous
        // placeholder PerformDeletionAtomic writes mid-epoch, before the delete's own journal
        // commit overwrites it with a real, minted stamp. Never ship that placeholder: a loaded
        // tombstone with Mvcc() == 0 would be immortal (RdbLoader::HandleTombstones rejects one
        // anyway, but this is the cleaner place to never produce one in the first place).
        FiberAtomicGuard g;
        cursor = db->mvcc->Traverse(cursor, [&](auto it) {
          if (!it->second.IsTombstone() || it->second.Mvcc() == 0)
            return;
          if (it->second.DeadlineMs(ttl_ms) <= now_ms)
            return;
          chunk.emplace_back(std::string(it->first.GetSlice(&scratch)), it->second);
        });
      }
      if (chunk.size() >= kChunkSize)
        flush_chunk();
    } while (cursor);

    flush_chunk();
  }
}

// The algorithm is to go over all the buckets and serialize those with
// version < snapshot_version_. In order to serialize each physical bucket exactly once we update
// bucket version to snapshot_version_ once it has been serialized.
// We handle serialization at physical bucket granularity.
// To further complicate things, Table::Traverse covers a logical bucket that may comprise of
// several physical buckets in dash table. For example, items belonging to logical bucket 0
// can reside in buckets 0,1 and stash buckets 56-59.
// PrimeTable::Traverse guarantees an atomic traversal of a single logical bucket,
// it also guarantees 100% coverage of all items that exists when the traversal started
// and survived until it finished.

// Serializes all the entries with version less than snapshot_version_.
void SliceSnapshot::IterateBucketsFb(bool send_full_sync_cut) {
  for (DbIndex db_indx = 0; db_indx < db_array_.size(); ++db_indx) {
    stats_.keys_total += db_slice_->DbSize(db_indx);
  }

  for (DbIndex snapshot_db_indx = 0; snapshot_db_indx < db_array_.size(); ++snapshot_db_indx) {
    if (!base_cntx_->IsRunning())
      return;

    if (!db_array_[snapshot_db_indx])
      continue;

    PrimeTable* pt = &db_array_[snapshot_db_indx]->prime;
    VLOG(1) << "Start traversing " << pt->size() << " items for index " << snapshot_db_indx;

    do {
      if (!base_cntx_->IsRunning())
        return;

      snapshot_cursor_ = pt->TraverseBuckets(
          snapshot_cursor_,
          [this, snapshot_db_indx](auto it) { ProcessBucket(snapshot_db_indx, it, false); },
          true /* include empty buckets */);

      if (use_background_mode_) {
        // Yielding for background fibers has low overhead if the time slice isn't used up.
        // Do it after every bucket for maximum responsiveness.
        DCHECK(ThisFiber::Priority() == fb2::FiberPriority::BACKGROUND);
        ThisFiber::Yield();
        PushSerialized(false);
      } else {
        if (!PushSerialized(false)) {
          if (!use_background_mode_ && ThisFiber::GetRunningTimeCycles() > CyclesPerJiffy()) {
            ThisFiber::Yield();
          }
        }
      }

      // Suspend the traversal loop if we are exceeding the egress budget, letting
      // high priority writes drain first. Guarantees the loop its reserved share.
      ServerState::tlocal()->GetEgressThrottler().Throttle();
    } while (snapshot_cursor_);

    // Wait for all the outstanding delayed entries and serialize them as well.
    ProcessDelayedEntries(true, 0, base_cntx_);

    PushSerialized(true);
  }  // for (dbindex)

  CHECK(!serialize_bucket_running_);
  if (send_full_sync_cut) {
    CHECK(!serializer_->SendFullSyncCut());
    PushSerialized(true);
  }

  if (VLOG_IS_ON(1)) {
    auto stats = SerializerBase::GetStats();

    // serialized + side_saved must be equal to the total saved.
    VLOG(1) << "Exit SnapshotSerializer total_serialized: " << stats.keys_serialized
            << ", buckets side saved " << stats.buckets_on_change << ", total bucket saved "
            << stats.buckets_serialized << ", journal_saved " << stats_.jounal_changes;
  }
}

unsigned SliceSnapshot::SerializeBucketLocked(DbIndex db_index, PrimeTable::bucket_iterator it,
                                              bool on_update) {
  // traverse physical bucket and write it into string file.
  serialize_bucket_running_ = true;

  unsigned serialized = 0;

  for (it.AdvanceIfNotOccupied(); !it.is_done(); ++it) {
    // Version is already stamped by SerializerBase::ProcessBucket.
    DCHECK_EQ(it.GetVersion(), snapshot_version_);

    ++serialized;

    // might preempt due to big value serialization.
    SerializerBase::SerializeEntry(it.bucket_address(), db_index, it->first, it->second,
                                   &it.owner());
  }

  serialize_bucket_running_ = false;
  return serialized;
}

void SliceSnapshot::SerializeEntryLocked(DbIndex db_index, const PrimeKey& pk, const PrimeValue& pv,
                                         time_t expire, uint32_t mc_flags, const MvccStamp& mvcc) {
  io::Result<uint8_t> res = serializer_->SaveEntry(pk, pv, expire, mc_flags, db_index, mvcc);
  LOG_IF(ERROR, !res.has_value()) << "Serialization error: " << res.error();
  if (res)
    ++type_freq_map_[*res];
}

void SliceSnapshot::HandleFlushData(std::string data) {
  if (data.empty())
    return;

  if (stream_mu_.is_locked()) {
    ++stats_.flushed_under_lock;
  }
  size_t serialized = data.size();
  uint64_t id = rec_id_++;

  if (use_background_mode_) {
    // Yield after possibly long cpu slice due to compression and serialization
    // before possbile suspension of ConsumeData resets the cpu time of the last slice
    if (ThisFiber::Priority() == fb2::FiberPriority::BACKGROUND)
      ThisFiber::Yield();
    // else: This function is invoked from the journal with regular priority as well.
    // TODO: Mavbe Sleep() to provide write backpressure in advance?
  }

  uint64_t running_cycles = ThisFiber::GetRunningTimeCycles();

  fb2::NoOpLock lk;
  // We create a critical section here that ensures that records are pushed in sequential order.
  // As a result, it is not possible for two fiber producers to push concurrently.
  // If A.id = 5, and then B.id = 6, and both are blocked here, it means that last_pushed_id_ < 4.
  // Once last_pushed_id_ = 4, A will be unblocked, while B will wait until A finishes pushing and
  // update last_pushed_id_ to 5.
  seq_cond_.wait(lk, [&] { return id == this->last_pushed_id_ + 1; });

  // Track egress just before the socket write. Attribute it to a high priority (out of order)
  // write when we don't run on the snapshot fiber.
  ServerState::tlocal()->GetEgressThrottler().Record(serialized, !snapshot_fb_.IsActive());

  // Blocking point.
  consumer_->ConsumeData(std::move(data), base_cntx_);

  DCHECK_EQ(last_pushed_id_ + 1, id);
  last_pushed_id_ = id;
  seq_cond_.notify_all();

  if (!use_background_mode_) {
    // serializer_->Flush can be quite slow for large values or due to compression, therefore
    // we counter-balance CPU over-usage by sleeping.
    // We measure running_cycles before the preemption points, because they reset the counter.
    uint64_t sleep_usec = (running_cycles * 1000'000 / base::CycleClock::Frequency()) / 2;
    ThisFiber::SleepFor(chrono::microseconds(std::min<uint64_t>(sleep_usec, 2000ul)));
  }

  VLOG(2) << "Pushed with Serialize() " << serialized;
}

std::error_code SliceSnapshot::ConsumeBigValueChunk(std::string data) {
  if (base_cntx_->IsError())
    return base_cntx_->GetError();

  if (base_cntx_->IsCancelled())
    return std::make_error_code(std::errc::operation_canceled);

  HandleFlushData(std::move(data));
  ++ServerState::tlocal()->stats.big_value_preemptions;
  return {};
}

size_t SliceSnapshot::FlushSerialized() {
  std::string blob = serializer_->Flush(RdbSerializer::FlushState::kFlushEndEntry);

  size_t serialized = blob.size();
  HandleFlushData(std::move(blob));
  return serialized;
}

bool SliceSnapshot::PushSerialized(bool force) {
  if (!force && serializer_->SerializedLen() < kMinBlobSize)
    return false;
  return FlushSerialized();
}

// stream_mu_ prevents expiry/eviction DEL journal entries from interleaving with an
// in-progress SaveEntry for a large value. SaveEntry may yield mid-entry (emitting chunks
// across multiple scheduler turns); expiry paths emit DEL via RecordDelete directly,
// bypassing OnChange. Without the lock, such a DEL could be written between two chunks
// of the same entry, producing an invalid wire format for the downstream consumer.
//
// Note: even if the protocol were extended to support interleaved chunks, the lock would
// still be required semantically: a DEL journal entry must not be applied on the replica
// while the entry's baseline is still being loaded. The delayed deletion queue proposal
// in the design doc addresses this without a shard-wide lock.
//
// Note: for transaction-driven mutations, baseline-before-journal ordering is already
// guaranteed by call order on the mutation fiber (OnChange precedes ConsumeJournalChange);
// stream_mu_ is not needed for that ordering.
void SliceSnapshot::ConsumeJournalChange(const journal::JournalChangeItem& item) {
  // drakeydb: Phase 3 T7b -- apply the SAME peer-echo filter JournalStreamer::ShouldWrite uses
  // for the stable-sync stream (journal::PassesPeerEchoFilter, journal/types.h) to the FULL-SYNC
  // window's concurrent journal blob. Without this, a peer full-syncing (e.g. because its
  // partial-sync backlog was evicted -- see PendingBuf/CleanEntries) would receive every
  // concurrent write inline and unfiltered through this path instead of streamer.cc's: peer-origin
  // writes echoed back toward their author, expiry-flagged DELs, derived DELs (P4-0's
  // kEntryFlagDerived), and Op::ORIGIN entries alike.
  //
  // Gated on peer_mode_ (false unless CreateSyncSession admitted this consumer as a peer of an
  // active node -- see dflycmd.cc's StartFullSyncInThread), so a plain replica's or a local
  // backup snapshot's journal blob stays completely unfiltered, exactly as upstream: filtering a
  // plain replica's stream would be silent data loss, not an echo fix.
  //
  // Checked before taking stream_mu_: a dropped entry never reaches the serializer, so there is
  // nothing for the lock to protect here.
  if (peer_mode_ && !journal::PassesPeerEchoFilter(item.journal_item))
    return;

  std::lock_guard lk{stream_mu_};
  std::ignore = serializer_->WriteJournalEntry(item.journal_item.data);
  ++stats_.jounal_changes;
}

void SliceSnapshot::ThrottleIfNeeded() {
  PushSerialized(false);
}

size_t SliceSnapshot::GetBufferCapacity() const {
  return serializer_ ? serializer_->GetBufferCapacity() : 0;
}

size_t SliceSnapshot::GetTempBuffersSize() const {
  return serializer_ ? serializer_->GetTempBufferSize() : 0;
}

RdbSaver::SnapshotStats SliceSnapshot::GetCurrentSnapshotProgress() const {
  return {SerializerBase::GetStats().keys_serialized, stats_.keys_total};
}

}  // namespace dfly
