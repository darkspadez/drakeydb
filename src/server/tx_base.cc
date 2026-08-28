// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tx_base.h"

#include <xxhash.h>

#include "base/logging.h"
#include "facade/facade_types.h"
#include "server/cluster/cluster_defs.h"
#include "server/engine_shard_set.h"
#include "server/journal/journal.h"
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

  // drakeydb: Phase 3 -- see the declaration in tx_base.h. origin_idx stays default (0 ==
  // kSelfIdx; an expiry is always a local decision); entry_flags carries kEntryFlagExpired.
  //
  // drakeydb: Phase 4, review wave 2 (F4, IMPORTANT) -- mvcc is now db_cntx.repl_mvcc, not a
  // hardcoded 0, but origin_idx deliberately stays kSelfIdx -- these two, unlike RecordDelete/
  // RecordDerivedDelete above, are NOT symmetric here, and that asymmetry is load-bearing, not an
  // oversight:
  //
  // The bug: MvccStamper::Commit(mvcc, origin_idx, fn) (mvcc.cc) stamps EVERY currently-armed
  // key with the SAME stamp, not just the key this call's own payload names. A whole-key lazy
  // expiry firing mid-callback (ExpireIfNeeded, this file) while an applier is still processing
  // an earlier key of the SAME replicated multi-key command (e.g. MSET arms key-by-key via
  // AddOrFind -> FindInternal -> ExpireIfNeeded, string_family.cc) sweeps that earlier,
  // already-armed sibling key into THIS entry's Commit() call, permanently consuming its arm
  // before the MSET's own trailing RecordJournal ever gets a chance to stamp it correctly. Before
  // this fix, that sibling key was left stamped with a freshly LOCAL HopStamp (the applier's own
  // "now") instead of the replicated command's real author stamp.
  //
  // Threading db_cntx.repl_mvcc through closes the numeric-ordering half of this: the sibling key
  // now gets the AUTHOR's real mvcc, which is what operator< compares first (MvccStamp::Mvcc()),
  // so this is what actually protects LWW ordering for that key in the overwhelmingly common
  // case.
  //
  // origin_idx is NOT threaded the same way, because unlike mvcc it is not merely descriptive --
  // journal::PassesPeerEchoFilter (journal/types.cc) branches on it FIRST, before even looking at
  // entry_flags: `if (item.origin_idx != kSelfIdx) { LOG(ERROR) ...; return false; }`. That
  // check exists to catch a node accidentally re-forwarding a peer's entry to a THIRD peer in a
  // full-mesh topology where every node is expected to link directly to every other node.
  // kEntryFlagExpired already independently makes PassesPeerEchoFilter return false a few lines
  // later, so setting origin_idx to db_cntx.repl_origin_idx here would not change whether this
  // DEL reaches a peer (it still wouldn't) -- it would only make it take the noisy,
  // rate-limited-ERROR "foreign-origin entry refused" branch instead of the silent,
  // expiry-specific one, on every node in the mesh, for an entry that was never anomalous. It
  // would also misattribute the expiry decision itself: kSelfIdx here is not a placeholder, it is
  // the true statement that THIS node's own clock decided to reap this key, independent of
  // whatever command is being replayed at that moment -- exactly the KeyDB semantics this
  // function's original comment above documents.
  //
  // Residual, accepted gap: a sibling key swept into this Commit() call ends up with
  // {author's real mvcc, THIS node's own origin_hash} rather than {author's mvcc, author's
  // origin_hash} -- correct on the primary (Mvcc()) ordering key, wrong only on origin_hash,
  // which operator< only consults to break an EXACT mvcc tie (mvcc.h). That already-narrow window
  // (this expiry race, on a non-first key of a multi-key applied command) would have to further
  // coincide with a second peer independently writing the very same key at the very same
  // millisecond-and-counter for the wrong origin_hash to change the outcome. Not closed by this
  // fix; needs Commit() to accept a stamp that differs from the entry it is nested inside, which
  // is a larger change than this pass makes -- flagged here for a future phase rather than risked
  // now.
  journal::RecordEntry(0, journal::Op::COMMAND, db_cntx.db_index, KeySlot(key),
                       Payload("DEL", ArgSlice{key}),
                       /* origin_idx= */ 0, db_cntx.repl_mvcc, journal::kEntryFlagExpired);
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
