// Copyright 2026, drakeydb authors.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <absl/flags/declare.h>

#include <cstdint>
#include <optional>
#include <string_view>

#include "server/mvcc.h"

// drakeydb: P4-4 -- forward declaration only: ApplyLwwRewrites below takes a pointer, so callers
// never need the complete type from this header. Every caller of ApplyLwwRewrites already has
// the complete type transitively (journal/executor.cc via journal/types.h; transaction.h via the
// same); multimaster_lww.cc, which implements the function, includes common/backed_args.h itself.
namespace cmn {
class BackedArguments;
}  // namespace cmn

// drakeydb: P4-4 -- absl flags live at global scope (matching multi_master.h's own
// ABSL_DECLARE_FLAG block), declared here so any caller can read it via absl::GetFlag without
// reaching into multimaster_lww.cc.
ABSL_DECLARE_FLAG(bool, multi_master_stream_lww);

namespace dfly {

// drakeydb: P4-4 -- the streaming LWW guard's pure decision module. FLAGS_multi_master_stream_lww
// is read once per peer link, at flow setup, by DflyShardReplica's constructor (replica.cc);
// LwwGuardActive backs Transaction::IsLwwGuarded() (transaction.h). Nothing yet acts on the
// result -- the veto is task A3.

// A journaled command's guard classification. kUnguarded commands are never LWW-compared; a
// kSingleKey command's OWN journaled write is compared under the key's lock; a
// kMultiKeySelfGuarded command (MSET, DEL) does its own per-key comparison inside its Op function
// rather than being guarded generically by the caller.
enum class LwwClass : uint8_t { kUnguarded, kSingleKey, kMultiKeySelfGuarded };

// drakeydb: P4-4 -- classifies a JOURNALED command name for the streaming LWW guard. Keyed on the
// JOURNALED name, not the client-facing one: the master normalizes SETEX / `SET ... EX` -> SET,
// UNLINK -> DEL, the EXPIRE family -> PEXPIREAT/DEL, MSETNX -> MSET, GAT -> PEXPIREAT/DEL/PERSIST
// (FindKeyAndSetExpiry, string_family.cc -- a different call site from GETEX's own, which picks
// the same three names independently), and a CROSS-SHARD RENAME -> DEL src + RESTORE dst ...
// REPLACE, before journaling, so classifying anything else would silently miss those. A SAME-SHARD
// RENAME/RENAMENX, a same-shard SORT ... STORE, and an exact (non-approximate, non-MAXLEN) XTRIM
// instead revive auto-journal at runtime (Transaction::ReviveAutoJournal) and journal the client's
// own command verbatim under its OWN name (RENAME/RENAMENX/SORT/XTRIM) -- none of those four names
// are in this table, so they all classify kUnguarded; a documented residual of the runtime-revival
// mechanism, not an oversight here. State-carrying RMW results that journal under a
// guarded name (PFMERGE/BITOP -> SET/DEL) are deliberately guarded here: a journaled SET is a
// blind full-state write on the receiver, exactly as droppable as any other SET. Delta-journaled
// RMW (INCR, APPEND, PFADD, ...) is deliberately absent: dropping a delta permanently loses it
// rather than merely reordering it, so those always fall through to kUnguarded. Unknown name ->
// kUnguarded is the fail-safe: an unrecognized name must never be silently guarded. Matches
// case-insensitively (journaled names are upper-case in practice, but callers should not have to
// guarantee it).
LwwClass ClassifyJournaledCommand(std::string_view journaled_name);

// True iff this apply should be LWW-guarded: the link is a guarded peer link AND the entry
// carries a real author stamp. A zero mvcc (classic Redis/KeyDB link, or a DFLY link to a
// non-active node) must NEVER be guarded -- MergeAccepts(stored, {0, h}) is false for every
// stamped key, so guarding it would silently discard the whole stream. This single predicate is
// called by BOTH Transaction::IsLwwGuarded() (A2) and the pre-dispatch SETNX/RESTORE rewrite
// (A9); it must be the only definition of the rule.
constexpr bool LwwGuardActive(bool link_guard, uint64_t incoming_mvcc) {
  return link_guard && incoming_mvcc != 0;
}

// drakeydb: P4-4 -- pre-dispatch rewrite for the two guarded single-key commands whose journaled
// form reproduces the author's COMMAND rather than the author's RESULT: SETNX (conditional on
// non-existence -- applying it verbatim is a silent no-op on a receiver that already holds the
// key) becomes a plain SET, and RESTORE (errors on an existing key without REPLACE, and
// DispatchCommand reports that reply-level error as an applied OK) gets REPLACE injected. Both
// are unconditional name/arg edits with no TOCTOU hazard, so callers run this PRE-dispatch; the
// stamp COMPARE itself still happens inside the transaction, under the key's lock
// (Transaction::ShouldDropForLww). Callers must gate this call on EXACTLY
// LwwGuardActive(link_guard, incoming_mvcc) -- never on the link bit alone, or an unstamped
// (mvcc 0) SETNX would be rewritten into an UNGUARDED blind SET that clobbers the key. Returns
// whether it changed anything.
bool ApplyLwwRewrites(cmn::BackedArguments* args);

// The author's stamp for an incoming replicated write, or nullopt when it cannot be formed
// (origin not registered: MvccStamper::OriginHash returns 0 for an unknown index) -- callers then
// fail OPEN (apply unguarded). DCHECKs origin hash != 0 whenever mvcc != 0.
std::optional<MvccStamp> IncomingStamp(uint64_t mvcc, uint32_t origin_idx);

// Streaming LWW drop decision: EXACTLY !MergeAccepts(stored, incoming) so streaming and
// merge-on-full-sync can never disagree (ties favor the stored side). Calls MergeAccepts
// (mvcc.h) rather than re-implementing the comparison.
inline bool LwwShouldDropKey(const std::optional<MvccStamp>& stored, const MvccStamp& incoming) {
  return !MergeAccepts(stored, incoming);
}

// Records one dropped key: ++ServerState::tlocal()->stats.multimaster_lww_dropped, VLOG(2) with
// cmd + key, and a LOG_EVERY_T(INFO, 60) rollup of the running total. Never a per-drop
// LOG(INFO) -- a conflicting workload drops thousands per second.
void NoteLwwDrop(std::string_view journaled_name, std::string_view key);

}  // namespace dfly
