// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tx_base.h"

#include <xxhash.h>

#include "base/logging.h"
#include "facade/facade_types.h"
#include "server/cluster/cluster_defs.h"
#include "server/db_slice.h"
#include "server/engine_shard_set.h"
#include "server/journal/journal.h"
#include "server/mvcc.h"
#include "server/namespaces.h"
#include "server/transaction.h"

namespace dfly {

using namespace std;
using Payload = journal::Entry::Payload;

namespace {

bool IsDefaultNamespace(const DbContext& db_cntx) {
  DCHECK(db_cntx.ns != nullptr);
  return db_cntx.ns == &namespaces->GetDefaultNamespace();
}

}  // namespace

unsigned KeyIndex::operator*() const {
  if (bonus)
    return *bonus;
  return start;
}

KeyIndex& KeyIndex::operator++() {
  if (bonus)
    bonus.reset();
  else
    start = std::min(end, start + step);
  return *this;
}

bool KeyIndex::operator!=(const KeyIndex& ki) const {
  return std::tie(start, end, step, bonus) != std::tie(ki.start, ki.end, ki.step, ki.bonus);
}

DbSlice& DbContext::GetDbSlice(ShardId shard_id) const {
  return ns->GetDbSlice(shard_id);
}

DbSlice& OpArgs::GetDbSlice() const {
  return db_cntx.GetDbSlice(shard->shard_id());
}

size_t ShardArgs::Size() const {
  size_t sz = 0;
  for (const auto& s : slice_.second)
    sz += (s.second - s.first);
  return sz;
}

void RecordJournal(const OpArgs& op_args, string_view cmd, const ShardArgs& args, uint32_t unused) {
  DCHECK(op_args.tx);
  VLOG(2) << "Logging command " << cmd << " from txn " << op_args.tx->txid();
  op_args.tx->LogJournalOnShard(Payload(cmd, args));
}

void RecordJournal(const OpArgs& op_args, std::string_view cmd, facade::ArgSlice args,
                   uint32_t unused) {
  DCHECK(op_args.tx);
  VLOG(2) << "Logging command " << cmd << " from txn " << op_args.tx->txid();
  op_args.tx->LogJournalOnShard(Payload(cmd, args));
}

void RecordDelete(DbIndex dbid, string_view key) {
  journal::RecordEntry(0, journal::Op::COMMAND, dbid, KeySlot(key), Payload("DEL", ArgSlice{key}));
}

void RecordDelete(const DbContext& db_cntx, string_view key) {
  if (!IsDefaultNamespace(db_cntx))
    return;

  // drakeydb: Phase 3 -- see the declaration in tx_base.h. origin_idx is one of two non-default
  // journal::RecordEntry args passed here; entry_flags stays 0 (this is never an expiry DEL).
  //
  // drakeydb: Phase 4, review wave 2 (F2, IMPORTANT) -- also forward db_cntx.repl_mvcc, added in
  // Task 6 for exactly this purpose (see its comment, tx_base.h) but left unread until now. A
  // self-originated derived DEL has repl_mvcc == 0, so journal::RecordEntry's "caller supplied no
  // stamp" test (entry.mvcc == 0, journal.cc) is unaffected and this call keeps minting a fresh
  // local HopStamp exactly as before. On an applier, though, repl_mvcc carries the author's own
  // verbatim stamp (Transaction::GetDbContext(), threaded from JournalExecutor::SetApplyMvcc via
  // SetReplOrigin) -- without forwarding it here, this DEL minted a LOCAL stamp instead, so two
  // peers applying the same replicated command that derives this same DEL diverged onto two
  // different stamps for the same key ({H_A, hash(A)} vs {H_B, hash(A)}) instead of converging on
  // one.
  journal::RecordEntry(0, journal::Op::COMMAND, db_cntx.db_index, KeySlot(key),
                       Payload("DEL", ArgSlice{key}), db_cntx.repl_origin_idx, db_cntx.repl_mvcc);
}

void RecordDerivedDelete(const DbContext& db_cntx, string_view key) {
  if (!IsDefaultNamespace(db_cntx))
    return;

  // drakeydb: Phase 4, review wave 2 (F2, IMPORTANT) -- db_cntx.repl_mvcc, not a hardcoded 0; see
  // RecordDelete's comment above for the full argument (identical here, modulo kEntryFlagDerived).
  journal::RecordEntry(0, journal::Op::COMMAND, db_cntx.db_index, KeySlot(key),
                       Payload("DEL", ArgSlice{key}), db_cntx.repl_origin_idx, db_cntx.repl_mvcc,
                       journal::kEntryFlagDerived);
}

void RecordExpiryBlocking(const DbContext& db_cntx, string_view key) {
  if (!IsDefaultNamespace(db_cntx))
    return;

  // drakeydb: P4-4 -- an expiry is always a local decision, so ITS OWN tombstone arm
  // (PerformDeletionAtomic, db_slice.cc) carries a stamp derived from the expired value's OWN
  // pre-deletion stamp -- one origin_hash tick above it (ExpiryTombstoneFor, mvcc.h), never that
  // stamp reused verbatim, never db_cntx.repl_mvcc/repl_origin_idx, and never a freshly minted
  // one. A lazy expiry can fire while applying a peer's command (e.g. a replicated multi-key
  // command whose processing discovers a DIFFERENT, unrelated key already expired -- a single
  // replicated DEL of an already-expired key is the simplest case: FindMutable's lookup expires
  // it via this same function before the DEL's own OpDelV2 ever runs); inheriting that peer's
  // mvcc/origin for the tombstone would be wrong regardless of which stamp ends up on it, since
  // that peer never authored this delete. CommitOwnTombstone (mvcc.cc) does the actual work,
  // reading this exact arm's own captured prior stamp -- see its own comment for what it does
  // when that arm carries no real prior stamp at all (erases the slot rather than tombstoning
  // it). `committed` captures the exact value it wrote (or stays empty in the erase case), for
  // the wire entry just below to reuse rather than re-derive.
  //
  // Wire-safe regardless of what this commits: PassesPeerEchoFilter (journal/types.cc) already
  // drops every kEntryFlagExpired entry before it reaches a mesh peer. Committing (and disarming)
  // this key's own tombstone here, before RecordEntry below, means that entry's own commit logic
  // never sees this arm again -- it is already gone -- so it cannot re-stamp it with a different
  // value. A sibling key armed earlier in the SAME epoch (e.g. a replicated MSET's other pair) is
  // a PLAIN arm, not a tombstone one, so CommitOwnTombstone (which matches only this exact key's
  // own tombstone arm) leaves it untouched -- and RecordEntry below no longer sweeps it either
  // (see that call's own comment), so it survives armed until the enclosing command's own,
  // later journal entry commits it with whatever stamp that entry actually carries.
  MvccStamp committed;
  const bool found_tombstone = MvccStamper::tlocal()->CommitOwnTombstone(
      db_cntx.db_index, key,
      [&committed](DbIndex db, string_view k, const MvccStamp& st, bool has_prior_stamp,
                   const MvccStamp&) {
        // drakeydb: P4-4 Task FW-A1 -- has_prior_stamp false means the expired value never had
        // a real stamp of its own (see CommitOwnTombstone's own comment, mvcc.h): erase the slot
        // rather than install an unsafe Mvcc()==0 tombstone. `committed` stays default (Empty()),
        // which the wire-entry mvcc below already treats the same as "nothing was committed".
        if (has_prior_stamp) {
          committed = st;
          namespaces->GetDefaultNamespace().GetCurrentDbSlice().SetExistingMvcc(db, k, st);
        } else {
          namespaces->GetDefaultNamespace().GetCurrentDbSlice().EraseMvcc(db, k);
        }
      });

  // drakeydb: Phase 3 -- see the declaration in tx_base.h. origin_idx stays default (0 ==
  // kSelfIdx; an expiry is always a local decision); entry_flags carries kEntryFlagExpired.
  //
  // drakeydb: P4-4 -- the wire mvcc is the just-committed tombstone's own masked magnitude
  // (Mvcc() -- never the tombstone bit itself: every other entry's wire mvcc is a bare magnitude
  // too, and each receiver decides locally, from its own arm's ground truth, whether the
  // committed stamp gets the tombstone bit). A plain (non-active-mesh) replica applies this DEL
  // as an ordinary command with that value as its author stamp, so its own floor lands on or
  // under the SAME value this node just committed, rather than minting one of its own. When no
  // tombstone was actually committed above (TombstonesEnabled() is false, or
  // PerformDeletionAtomic degraded to a plain erase at the tombstone cap) there is no such value
  // to reuse, so this falls back to forwarding db_cntx.repl_mvcc verbatim, exactly as every DEL
  // derived from an applied write already does (RecordDelete/RecordDerivedDelete above) --
  // journal::RecordEntry mints a fresh local stamp itself when that is 0 (a self-originated
  // command), the same as it always has.
  //
  // origin_idx stays self-originated because this node's clock made the expiry decision and the
  // resulting DEL must not be echoed to peers. Unlike RecordDelete/RecordDerivedDelete, mvcc and
  // origin_idx are therefore NOT the matching pair from the same source here -- deliberate, not
  // an oversight.
  //
  // The generic per-arm commit inside RecordEntry never runs for a kEntryFlagExpired entry (see
  // that function's own comment, journal.h/.cc): this key's arm is already gone (committed
  // above), and any sibling key still armed this epoch is left for the enclosing command's own,
  // later entry to commit instead.
  journal::RecordEntry(0, journal::Op::COMMAND, db_cntx.db_index, KeySlot(key),
                       Payload("DEL", ArgSlice{key}), /* origin_idx= */ 0,
                       found_tombstone ? committed.Mvcc() : db_cntx.repl_mvcc,
                       journal::kEntryFlagExpired);
}

LockTag::LockTag(std::string_view key) {
  if (LockTagOptions::instance().enabled)
    str_ = LockTagOptions::instance().Tag(key);
  else
    str_ = key;
}

LockFp LockTag::Fingerprint() const {
  return XXH64(str_.data(), str_.size(), 0x1C69B3F74AC4AE35UL);
}

}  // namespace dfly
