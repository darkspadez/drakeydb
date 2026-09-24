// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once
#include "server/journal/types.h"
#include "util/fibers/detail/fiber_interface.h"

namespace dfly {

namespace journal {

void StartInThread();

// Starts the journal at specified LSN
// Also drops the (resets) the partial sync buffers
void StartInThreadAtLsn(LSN lsn);

// Drops the partial-sync buffer for the current shard and invalidates any
// replica's previously-observed LSN; see the definition for details.
void ClearBuffer();

std::error_code Close();

//******* The following functions must be called in the context of the owning shard *********//

unsigned GetCallbackCount();
inline bool HasRegisteredCallbacks() {
  return GetCallbackCount() > 0;
}

bool IsLSNInBuffer(LSN lsn);

std::string_view GetEntry(LSN lsn);
// drakeydb: mirrors JournalSlice::GetEntryMeta -- see journal_slice.h for why this exists
// alongside GetEntry().
const JournalItem& GetEntryMeta(LSN lsn);

LSN GetLsn();
uint32_t RegisterConsumer(JournalConsumerInterface* consumer);
void UnregisterConsumer(uint32_t id);

// drakeydb: Phase 3 -- origin_idx/mvcc/entry_flags default to self/0/none (PeerRegistry::kSelfIdx
// == 0), so every pre-existing call site (PING/DEL/cluster control entries) is unaffected.
// Transaction::LogJournalOnShard is the only caller that passes a non-default origin_idx/mvcc,
// forwarding a transaction's replication-apply origin so entries applied from a peer are tagged
// with that peer's origin instead of self. Two tx_base.cc callers pass a non-default entry_flags:
// RecordExpiryBlocking tags expiry/eviction-triggered DELs with kEntryFlagExpired, and (P4-0)
// RecordDerivedDelete tags a DEL derived from a collection command emptying its key with
// kEntryFlagDerived -- both so a later peer-echo filter (journal::PassesPeerEchoFilter) can
// suppress them.
//
// drakeydb: P4-4 -- a kEntryFlagExpired entry never runs the ordinary per-arm Commit() sweep below
// (journal.cc): RecordExpiryBlocking's own CommitOwnTombstone call (mvcc.cc) already committed
// this key's own tombstone arm, with its own stamp, before this entry was even built, and any
// OTHER key still armed this epoch (a sibling of a multi-key command) is left alone here for that
// command's own, later journal entry to commit instead -- see RecordExpiryBlocking's own comment
// (tx_base.cc) for the full account.
void RecordEntry(TxId txid, Op opcode, DbIndex dbid, std::optional<SlotId> slot,
                 Entry::Payload payload, uint32_t origin_idx = 0, uint64_t mvcc = 0,
                 uint8_t entry_flags = 0);

size_t LsnBufferSize();
size_t LsnBufferBytes();

void SetFlushMode(bool allow_flush);

class DisableFlushGuard {
 public:
  explicit DisableFlushGuard(bool j) : journal_(j) {
    if (journal_ && counter_ == 0) {
      SetFlushMode(false);
    }
    util::fb2::detail::EnterFiberAtomicSection();
    ++counter_;
  }

  ~DisableFlushGuard() {
    util::fb2::detail::LeaveFiberAtomicSection();
    --counter_;
    if (journal_ && counter_ == 0) {
      SetFlushMode(true);  // Restore the state on destruction
    }
  }

  DisableFlushGuard(const DisableFlushGuard&) = delete;
  DisableFlushGuard& operator=(const DisableFlushGuard&) = delete;

 private:
  bool journal_;
  static size_t thread_local counter_;
};

}  // namespace journal
}  // namespace dfly
