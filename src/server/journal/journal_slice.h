// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <boost/circular_buffer.hpp>
#include <boost/circular_buffer/space_optimized.hpp>
#include <cstdint>
#include <shared_mutex>
#include <string_view>

#include "server/journal/types.h"
#include "util/fibers/synchronization.h"

namespace dfly {
namespace journal {

// Journal slice is present for both shards and io threads.
class JournalSlice {
 public:
  JournalSlice();
  ~JournalSlice();

  void Init();

  // This is always the LSN of the *next* journal entry.
  LSN cur_lsn() const {
    return lsn_;
  }

  std::error_code status() const {
    return status_ec_;
  }

  void AddLogRecord(const Entry& entry);

  // Register a callback that will be called every time a new entry is
  // added to the journal.
  // The callback receives the entry and a boolean that indicates whether
  // awaiting (to apply backpressure) is allowed.
  uint32_t RegisterOnChange(JournalConsumerInterface* consumer);
  void UnregisterOnChange(uint32_t);

  unsigned OnChangeCbCount() const {
    return journal_consumers_arr_.size();
  }

  /// Returns whether the journal entry with this LSN is available
  /// from the buffer.
  bool IsLSNInBuffer(LSN lsn) const;
  std::string_view GetEntry(LSN lsn) const;
  // drakeydb: Phase 3 metadata accessor. Returns the full buffered JournalItem -- including
  // origin_idx/entry_flags -- so a later peer-echo filter can inspect them without reparsing
  // `data`. Added alongside GetEntry() (rather than widening it) so GetEntry's existing caller
  // in streamer.cc, outside this task's scope, stays untouched.
  //
  // drakeydb: Phase 3 T5 -- contract (undocumented until now; this task is GetEntryMeta's first
  // caller outside JournalSlice itself, via journal::GetEntryMeta -> JournalStreamer::ShouldWrite
  // / MaybePartialStreamLSNs):
  //  - IsLSNInBuffer(lsn) is a MANDATORY precondition. It is checked only via DCHECK here (like
  //    GetEntry() above), so it is NOT enforced in release builds -- callers must check it
  //    themselves first, exactly as MaybePartialStreamLSNs's `while (... && IsLSNInBuffer(lsn))`
  //    loop guard does.
  //  - The returned reference points INTO the ring buffer and is invalidated by any subsequent
  //    call that mutates it: AddLogRecord's push_back once the buffer is full (overwrites the
  //    oldest slot), CleanEntries's rerase (age/byte-limit eviction), or set_capacity (growth).
  //    Since AddLogRecord can run from another fiber that preempts the caller (e.g. between two
  //    statements, or inside a callback the caller invokes while still holding the reference),
  //    the safe pattern is: take the reference, copy out the fields you need, and let it go out
  //    of scope before doing anything that might preempt -- never store it in a variable that
  //    outlives that copy.
  const JournalItem& GetEntryMeta(LSN lsn) const;
  // SetFlushMode with allow_flush=false is used to disable preemptions during
  // subsequent calls to AddLogRecord.
  // SetFlushMode with allow_flush=true flushes all log records aggregated
  // since the last call with allow_flush=false. This call may preempt.
  // The caller must ensure that no preemptions occur between the initial call
  // with allow_flush=false and the subsequent call with allow_flush=true.
  void SetFlushMode(bool allow_flush);

  size_t GetRingBufferSize() const {
    return ring_buffer_.size();
  }

  size_t GetRingBufferBytes() const {
    return ring_buffer_bytes_;
  }

  void ResetRingBuffer() {
    ring_buffer_.clear();
    ring_buffer_bytes_ = 0;
  }

  void SetStartingLSN(LSN lsn) {
    lsn_ = lsn;
  }

 private:
  void CallOnChange(JournalChangeItem* item);
  void CleanEntries(size_t next_item_bytes, uint64_t now_ms);
  static size_t ItemBytes(const JournalItem& item);

  boost::circular_buffer_space_optimized<JournalItem> ring_buffer_;

  mutable util::fb2::SharedMutex cb_mu_;  // to prevent removing callback during call
  std::list<std::pair<uint32_t, JournalConsumerInterface*>> journal_consumers_arr_;

  LSN lsn_ = 1;

  uint32_t next_cb_id_ = 1;
  std::error_code status_ec_;
  bool enable_journal_flush_ = true;

  uint32_t max_age_ms_ = 0;
  size_t max_bytes_ = 0;

  // drakeydb: cached once in Init() (--active_replica is boot-only), like max_age_ms_/
  // max_bytes_ above, rather than reading the flag on every AddLogRecord call.
  bool extended_framing_ = false;

  size_t ring_buffer_bytes_ = 0;
};

}  // namespace journal
}  // namespace dfly
