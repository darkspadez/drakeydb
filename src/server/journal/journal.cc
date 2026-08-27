// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/journal.h"

#include <new>

#include "base/logging.h"
#include "server/common.h"
#include "server/db_slice.h"
#include "server/engine_shard_set.h"
#include "server/journal/journal_slice.h"
#include "server/mvcc.h"
#include "server/namespaces.h"

namespace dfly {
namespace journal {

using namespace std;
using namespace util;

namespace {

// Active only in shard threads.
thread_local JournalSlice journal_slice;

// drakeydb: Phase 4 -- cached read (JournalSlice::mvcc_enabled_, cached once in Init(), mirroring
// extended_framing_ just below it). NOT a bare IsActiveReplica(): that is an uncached
// absl::GetFlag (multi_master.cc), and RecordEntry below runs once per journal entry -- the
// hottest shared path in the server. P4-0 shipped a fix for this exact defect class (a5345509).
bool MvccEnabled() {
  return journal_slice.mvcc_enabled();
}

}  // namespace

void StartInThread() {
  journal_slice.Init();

  EngineShard* shard = EngineShard::tlocal();
  shard->set_journal(true);
}

void StartInThreadAtLsn(LSN lsn) {
  StartInThread();
  journal_slice.ResetRingBuffer();
  journal_slice.SetStartingLSN(lsn);
}

void ClearBuffer() {
  journal_slice.ResetRingBuffer();
  // Advance LSN so that any stale LSN from a pre-clear replica no longer
  // matches journal::GetLsn(); otherwise the partial-sync fast path in
  // DflyCmd::IsLSNInPartialSyncBuffer would let a reconnecting replica skip
  // full sync even though the buffer is empty.
  journal_slice.SetStartingLSN(journal_slice.cur_lsn() + 1);
}

error_code Close() {
  VLOG(1) << "Journal::Close";

  auto close_cb = [&](auto* shard) {
    journal_slice.ResetRingBuffer();
    shard->set_journal(false);
  };

  shard_set->RunBriefInParallel(close_cb);

  return {};
}

unsigned GetCallbackCount() {
  return journal_slice.OnChangeCbCount();
}

bool IsLSNInBuffer(LSN lsn) {
  return journal_slice.IsLSNInBuffer(lsn);
}

std::string_view GetEntry(LSN lsn) {
  return journal_slice.GetEntry(lsn);
}

// drakeydb: thin hook onto JournalSlice::GetEntryMeta -- see its declaration in journal.h and
// definition in journal_slice.h/.cc for the contract (IsLSNInBuffer precondition, reference
// lifetime tied to the ring buffer).
const JournalItem& GetEntryMeta(LSN lsn) {
  return journal_slice.GetEntryMeta(lsn);
}

uint32_t RegisterConsumer(JournalConsumerInterface* consumer) {
  return journal_slice.RegisterOnChange(consumer);
}

void UnregisterConsumer(uint32_t id) {
  journal_slice.UnregisterOnChange(id);
}

LSN GetLsn() {
  return journal_slice.cur_lsn();
}

void RecordEntry(TxId txid, Op opcode, DbIndex dbid, std::optional<SlotId> slot,
                 Entry::Payload payload, uint32_t origin_idx, uint64_t mvcc, uint8_t entry_flags) {
  Entry entry{txid, opcode, dbid, slot, std::move(payload)};
  // drakeydb: Phase 3 -- stamp origin/mvcc/entry_flags onto the entry; defaults to self/0/none
  // for callers that don't pass them. See journal.h for why this isn't folded into the Entry
  // constructor.
  entry.origin_idx = origin_idx;
  entry.mvcc = mvcc;
  entry.entry_flags = entry_flags;

  // drakeydb: Phase 4 -- mint AFTER entry.mvcc = mvcc above (an assignment from the possibly-zero
  // caller-supplied parameter, which would otherwise clobber a mint placed earlier straight back
  // to 0) and BEFORE AddLogRecord below, so the wire carries this exact value. HopStamp takes
  // now_ms explicitly (Task 4, design point 3): mvcc.cc itself never calls GetCurrentTimeMs(), so
  // the caller -- here, already deep in EngineShard territory -- does. entry.mvcc == 0 is a safe
  // "caller supplied no stamp" test: 0 is unreachable for a real stamp (ms << 20, ms ~ 1.77e12).
  if (MvccEnabled() && opcode == Op::COMMAND && entry.mvcc == 0)
    entry.mvcc = MvccStamper::tlocal()->HopStamp(GetCurrentTimeMs());

  journal_slice.AddLogRecord(entry);

  // drakeydb: Phase 4 -- commit AFTER the entry is durable, so a key is only stamped once its
  // entry has actually joined the journal on its way to peers. entry.mvcc is always non-zero by
  // this point (freshly minted just above, or supplied non-zero by the caller -- an applied
  // write's verbatim author stamp, kept as-is or stamps would inflate on every hop) -- Commit()
  // DCHECKs this and has no clock of its own, so there is no now_ms to pass here. Gating on
  // Op::COMMAND excludes SELECT/PING/LSN/ORIGIN.
  //
  // drakeydb: Phase 4, review fix round 1 (F1) -- the commit target is always
  // GetDefaultNamespace(): the journal has no namespace identity on the wire, and armed_ (mvcc.h)
  // carries no namespace either, so this callback has no way to route a non-default-namespace
  // key to its actual DbSlice even if one were armed. That is exactly why DbSlice::PostUpdate
  // gates arming to the default namespace only (see its comment) -- by the time Commit() runs
  // here, every armed key is guaranteed to belong to the default namespace, so this lookup is
  // correct by construction, not by the (previously wrong) claim that "the journal only ever
  // carries the default namespace" -- ACL-namespaced writes ARE real commands that reach this
  // function; they are simply never armed in the first place. Looked up once, not once per armed
  // key inside the CommitFn, since a single journal entry can arm many keys (e.g. MSET).
  if (MvccEnabled() && opcode == Op::COMMAND) {
    DbSlice& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    // drakeydb: Phase 4, review fix round 1 (F3) -- contained locally, not for the odds (SetMvcc's
    // side-table Insert can only fail via genuine OS allocation failure: DefaultEvictionPolicy's
    // CanGrow always returns true, so maxmemory pressure can't reach it) but for the consequence
    // of letting it escape uncaught: this call runs from LogAutoJournalOnShard, in
    // Transaction::RunCallback's tail, OUTSIDE the try/catch that wraps only the command callback
    // -- an uncaught throw here would skip shard->set_running_tx(nullptr) and permanently wedge
    // the shard (PollExecution and Heartbeat both refuse to proceed while running_tx_ is set;
    // engine_shard.cc). MvccStamper::Commit's own absl::Cleanup has already restored commit_depth_
    // to 0 and cleared armed_/arena_ by the time this catch runs (Task 7's throw-safety decision),
    // so the stamper itself is left consistent -- the affected keys simply stay unstamped, the
    // safe direction for the phase's central invariant.
    try {
      MvccStamper::tlocal()->Commit(
          entry.mvcc, entry.origin_idx,
          [&db_slice](DbIndex db, std::string_view key, const MvccStamp& st) {
            // drakeydb: Phase 4, review fix round 1 (F4) -- string_view overload, not
            // SetMvcc(db, PrimeKey{key}, st): key is already a string_view (it was armed as one
            // in PostUpdate and stored verbatim in MvccStamper's arena), so routing it through a
            // freshly-constructed PrimeKey here cost three allocations per key per write for
            // nothing (PrimeKey{key}'s own allocation for keys > its inline capacity, then
            // PrimeKey::GetSlice back out to a string_view, then Dash re-encoding that string_view
            // a third time) on the hottest shared path in the server. See db_slice.cc's SetMvcc
            // overload comment.
            db_slice.SetMvcc(db, key, st);
          });
    } catch (const std::bad_alloc&) {
      LOG_EVERY_T(ERROR, 1) << "mvcc commit OOM -- key(s) left unstamped";
    }
  }
}

void SetFlushMode(bool allow_flush) {
  journal_slice.SetFlushMode(allow_flush);
}

size_t LsnBufferSize() {
  return journal_slice.GetRingBufferSize();
}

size_t LsnBufferBytes() {
  return journal_slice.GetRingBufferBytes();
}

size_t thread_local DisableFlushGuard::counter_ = 0;

}  // namespace journal
}  // namespace dfly
