// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <algorithm>  // std::min
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#include "server/common.h"  // DbIndex

namespace dfly {

// drakeydb: Phase 4. A per-shard logical clock in KeyDB's layout (ms << 20 | counter,
// KeyDB/src/server.cpp:7263-7287) so a KeyDB-sourced mvcc-tstamp compares meaningfully against a
// locally minted one during P7 onboarding.
//
// Unlike KeyDB's process-global atomic, this is a plain member of a thread-local stamper: KeyDB
// serialises every write in the process on one cache line, which a shared-nothing server must not.
class MvccClock {
 public:
  static constexpr unsigned kCounterBits = 20;
  static constexpr uint64_t kCounterMask = (1ULL << kCounterBits) - 1;

  // Bit 63 marks a tombstone. Safe indefinitely: ms << 20 with today's ms (~1.77e12, 2^40.7)
  // reaches bit 60, and bit 61 is not reached until ~2079.
  static constexpr uint64_t kTombstoneBit = 1ULL << 63;
  static constexpr uint64_t kStampMask = ~kTombstoneBit;

  // Strictly increasing, always -- including across a backwards NTP step, where it advances one
  // counter tick per call until the wall clock catches up. Counter exhaustion carries into the ms
  // field exactly as KeyDB's fetch_add(1) does.
  uint64_t Next(uint64_t now_ms) {
    const uint64_t cand = now_ms << kCounterBits;
    last_ = (cand > last_) ? cand : last_ + 1;
    return last_;
  }

  uint64_t last() const {
    return last_;
  }

  // Milliseconds this clock currently runs ahead of the wall clock; 0 when in step. Non-zero
  // means this node wins every LWW conflict for that long -- see INFO mvcc_clock_ahead_ms.
  uint64_t AheadMs(uint64_t now_ms) const {
    const uint64_t ms = last_ >> kCounterBits;
    return ms > now_ms ? ms - now_ms : 0;
  }

  void TEST_Set(uint64_t v) {
    last_ = v;
  }

 private:
  uint64_t last_ = 0;
};

// Per-key version. 16 bytes; ordered lexicographically on (mvcc, origin_hash).
//
// origin_hash is what makes the order *total*, and that is load-bearing: KeyDB's merge lets an
// incoming write win an exact tie (KeyDB/src/db.cpp:384), so in a mesh A adopts B's value while B
// adopts A's and they swap permanently. Ties are common rather than rare -- under light load the
// 20-bit counter sits at 0, so two nodes writing in the same millisecond collide.
struct MvccStamp {
  uint64_t packed = 0;       // (tombstone << 63) | (ms << 20) | counter
  uint64_t origin_hash = 0;  // NodeUuidHash of the AUTHOR node's uuid, never the sender's

  uint64_t Mvcc() const {
    return packed & MvccClock::kStampMask;
  }
  bool IsTombstone() const {
    return (packed & MvccClock::kTombstoneBit) != 0;
  }
  uint64_t MsPart() const {
    return Mvcc() >> MvccClock::kCounterBits;
  }
  bool Empty() const {
    return packed == 0 && origin_hash == 0;
  }
  // Same origin_hash, packed | kTombstoneBit. This is the ONLY place that may set the tombstone
  // bit -- it exists for the RDB LOAD path, which reconstructs a tombstone from persisted bytes.
  // The live delete path sets the bit inside MvccStamper::Commit instead (drakeydb P4-3); that is
  // a second call site by necessity (Commit mints/forwards the stamp, this method does not), not
  // a duplication of this logic.
  //
  // PRECONDITION: `this` must already be strictly greater than the stamp of the value being
  // deleted, never that value's own stamp reused verbatim -- operator< masks bit 63 (see the
  // INVARIANT comment below), so a reused stamp is order-equivalent to the value it replaces,
  // MergeAccepts ties on it, and ties favor the stored side: the delete then silently never
  // applies on a peer that still holds the live value, and (worse) a receiver that later
  // re-creates the exact same key at the exact same {mvcc, origin_hash} pair -- reachable via a
  // delta RMW applied on a node that already reaped the key -- ties against this tombstone
  // forever, permanently unrepairable once it is GC'd. Every caller mints or advances a stamp
  // strictly greater than the value's before calling this: MvccStamper::Commit's own commit-time
  // bit-set for an ordinary (non-expiry) delete, and ExpiryTombstoneFor (below) for every expiry
  // tombstone -- local (MvccStamper::CommitOwnTombstone), the member-expiry reaper
  // (DbSlice::DeleteReapedContainer, db_slice.cc), and a merge load's synthetic tombstone for an
  // already-expired incoming key (RdbLoader::ApplyMergeTombstoneOnShard's caller, rdb_load.cc)
  // alike. Never call this directly on a stamp unmodified from the value it replaces.
  MvccStamp AsTombstone() const {
    return MvccStamp{packed | MvccClock::kTombstoneBit, origin_hash};
  }
  // The GC deadline for a tombstone, derived on demand rather than stored: no third field, no
  // size growth (D-10). Callers pass the ttl; this type has no notion of "now".
  uint64_t DeadlineMs(uint64_t ttl_ms) const {
    return MsPart() + ttl_ms;
  }
  // INVARIANT: a tombstone must be strictly greater, in the (mvcc, origin_hash) lexicographic
  // order operator< implements below, than the value it replaces -- landing EXACTLY on that value
  // (advancing neither field) makes the tombstone order-equivalent to it (see
  // MvccStampTest.EqualityDistinguishesTombstoneAtEqualMvcc), and merge code written as
  // `if (local < incoming) adopt;` silently drops the delete. Two DIFFERENT, both sanctioned, ways
  // satisfy this: an ordinary (non-expiry) delete's own commit-time bit-set mints a genuinely
  // fresh mvcc via MvccClock::Next, never reusing the value's own; an expiry's own tombstone
  // (`ExpiryTombstoneFor`, below) instead reuses the value's mvcc UNCHANGED and advances
  // origin_hash by exactly one tick -- still strictly greater by the SAME lexicographic order,
  // just moving the other coordinate. Which field moved is not what merge code compares; only
  // "strictly greater" is. operator== below is deliberately NOT tombstone-masked (it compares raw
  // packed), so equality still distinguishes a tombstone from the value at the same mvcc even
  // though ordering does not.
  friend bool operator<(const MvccStamp& a, const MvccStamp& b) {
    // std::tie needs lvalues; Mvcc() returns by value, so make_tuple (which copies) is used
    // instead. Semantics are identical: lexicographic comparison on (Mvcc(), origin_hash).
    return std::make_tuple(a.Mvcc(), a.origin_hash) < std::make_tuple(b.Mvcc(), b.origin_hash);
  }
  friend bool operator==(const MvccStamp& a, const MvccStamp& b) {
    return a.packed == b.packed && a.origin_hash == b.origin_hash;
  }
};

static_assert(sizeof(MvccStamp) == 16, "side-table per-slot cost is computed from this");
static_assert(alignof(MvccStamp) == 8,
              "16-byte packing assumes 8-byte alignment; a consumer "
              "(e.g. the side table) may depend on this");

// The merge-LWW decision: true when `incoming` should be written over `stored`. Shared verbatim
// by the key path and the tombstone-apply path (drakeydb P4-3) so both compare the same way.
//
// Owner decision, 2026-08-30: ties are won by the STORED side -- an incoming write is accepted
// only when it is strictly newer, never on equality, so a retried or duplicated apply can never
// churn a key that already holds that exact stamp.
//
// This compares via operator< (masks bit 63, see the INVARIANT comment above), not operator==
// (raw packed, so a tombstone never compares equal to the live stamp it replaced). That asymmetry
// is deliberate and load-bearing: it is what lets a tombstone and a later resurrection order
// correctly against each other here, while still being distinguishable elsewhere by equality.
inline bool MergeAccepts(const std::optional<MvccStamp>& stored, const MvccStamp& incoming) {
  return !stored.has_value() || *stored < incoming;
}

// drakeydb: P4-4 -- the "just-above" tombstone for a value an expiry replaces, never a value's
// own stamp reused verbatim (AsTombstone's own precondition, above) and never a freshly minted,
// wall-clock-derived one either. One shared rule for every expiry tombstone in the phase: local
// lazy/active expiry (MvccStamper::CommitOwnTombstone, below), the member-expiry reaper
// (DbSlice::DeleteReapedContainer, db_slice.cc), and a merge load's synthetic tombstone for an
// already-expired incoming key (RdbLoader::ApplyMergeTombstoneOnShard's caller, rdb_load.cc) all
// call this, so the three can never drift onto different tombstone stamps for what is logically
// the same kind of event.
//
// `{value.Mvcc(), value.origin_hash + 1}`, tombstone bit set -- one origin_hash tick above
// `value`, mirroring FloorAppliedStamp's one-tick-BELOW landing above for the opposite direction.
// Strictly greater than `value` (satisfies AsTombstone's own precondition: this is genuinely new
// authority, not `value`'s own stamp reused), yet the smallest such stamp, so it costs nothing
// against a later write with real authority of its own (a guarded apply, or a later local write):
// nothing with legitimate authority over this key can land strictly between `value` and this
// tombstone, since no author ever mints (or is floored/advanced to) a stamp that exactly
// reproduces a stamp that already exists here.
//
// Landing exactly ON `value` instead (order-equivalent to it, reusing `value`'s own stamp
// verbatim) was tried and rejected: MergeAccepts ties favor the stored side, so a receiver that
// re-creates this exact key at this exact {mvcc, origin_hash} pair -- reachable via a delta RMW
// (INCR/APPEND/...) applied on a node that already reaped the key, which stamps its own re-create
// verbatim at the author's stamp with no floor of its own to raise it -- would tie against this
// tombstone forever, permanently unrepairable once the tombstone itself is GC'd. One tick above
// forecloses that tie entirely, at essentially no cost: the only stamp this excludes that
// order-equivalence would have included is `value` itself, which nothing legitimately re-mints.
//
// `value.origin_hash == UINT64_MAX` carries into Mvcc() instead of overflowing origin_hash to 0
// (which would REGRESS the ordering, not advance it) -- capped at kStampMask, mirroring
// LocalMintFloor's own cap (mvcc.cc) for the identical overflow-into-kTombstoneBit hazard: see
// ExpiryTombstoneForCarriesIntoMvccAtOriginHashMax and
// ExpiryTombstoneForCapsAtStampMaskOnDoubleOverflow (mvcc_test.cc).
inline MvccStamp ExpiryTombstoneFor(const MvccStamp& value) {
  if (value.origin_hash == UINT64_MAX) {
    return MvccStamp{std::min(value.Mvcc() + 1, MvccClock::kStampMask) | MvccClock::kTombstoneBit,
                     0};
  }
  return MvccStamp{value.Mvcc() | MvccClock::kTombstoneBit, value.origin_hash + 1};
}

// drakeydb: P4-4 Task A5 -- the "just-below" stamp floor for an APPLIED write (a caller-supplied,
// non-zero author mvcc committed by journal::RecordEntry, never a local mint) whose `incoming`
// stamp is OLDER than the key's own current stamp `stored`. Guarded writes (Task A3) only ever
// apply when incoming > stored, so verbatim is correct for them -- this floor only matters for an
// UNGUARDED applied write (a delta RMW like INCR/APPEND, or any apply on a plain, non-active-peer
// replica), which can legitimately carry an author stamp older than what this node already has,
// because that author authored it before it ever saw `stored`.
//
// Committing `incoming` verbatim there would REWIND the key's stamp, after which a stale guarded
// write whose stamp sits between the rewound value and `stored` wrongly wins the next comparison.
// `max(stored, incoming)` was rejected too: it gives the dirty result (stored's value, mutated by
// the unapplied RMW) the SAME stamp as a clean copy of `stored` alone, and since MergeAccepts
// favors ties to the stored side, a later merge-on-full-sync can never repair that divergence.
//
// Landing one origin_hash below `stored` (or one mvcc tick below it, at origin_hash 0) keeps two
// properties `max()` could not: the result is still strictly LESS than `stored`, so a clean copy
// stamped exactly `stored` beats it on the next merge and heals the divergence; and it still
// rejects every stamp `stored` itself would have rejected, EXCEPT `stored` itself -- a tie always
// rejects itself, but the floor must not, or the clean copy above could never win and heal the
// divergence -- so no other previously-dropped stale write starts winning either. This also means
// the floor never RAISES the key's stamp above `stored`: it cannot make a later LOCAL write (which
// floors its own mint at least one tick above whatever is currently stored, capped at the stamp
// mask, via `LocalMintFloor` below, rather than minting purely from the wall clock) compare any
// worse against a peer than `stored` itself already would have. `stored.origin_hash`
// has only so much room below it for this to keep working against a torrent of applied writes at
// the exact same `stored.Mvcc()`, though -- a bound this phase accepts.
//
// `stored.Mvcc() == 0` (a fresh {0,0} slot, or an uncommitted placeholder -- Mvcc() masks bit 63,
// so this also covers PerformDeletionAtomic's {kTombstoneBit, 0}) has no real prior stamp to floor
// against, so `incoming` always wins verbatim, exactly as it always has. The floored stamp never
// inherits `stored`'s own tombstone bit -- only the NEW operation's (`incoming`'s).
MvccStamp FloorAppliedStamp(const MvccStamp& stored, const MvccStamp& incoming);

// Stable across processes, builds and architectures -- the hash is persisted in the RDB and
// compared against values written by other nodes, so std::hash is unusable here.
uint64_t NodeUuidHash(std::string_view uuid);

// Thread-local minting and arm/commit bookkeeping. One per proactor thread, which is the right
// granularity: several DbSlices may share a thread via namespaces, and the clock only has to be
// monotone, never unique.
//
// Contract: DbSlice::PostUpdate ARMS the keys a shard callback touched; journal::RecordEntry
// COMMITS the entry's stamp to every armed key at the moment the entry is emitted; the end of the
// callback DISCARDS whatever is still armed. That is what enforces the phase's central invariant:
//
//   a key's stamp advances if and only if that same stamp is propagated to peers.
//
// Violating it in the "stamp but do not propagate" direction causes permanent, silent divergence:
// the node then rejects peer writes that should have won, forever.
//
// drakeydb: P4-4 Task A5 -- for an APPLIED write specifically, journal::RecordEntry no longer
// commits the propagated entry's stamp verbatim: FloorAppliedStamp (mvcc.h) may floor it below
// what the wire entry itself carries, when the author's stamp is older than the key's own. The
// invariant above is deliberately relaxed for that one case -- the STORED stamp can end up
// strictly less than the PROPAGATED one -- because committing verbatim there would rewind the
// key's stamp instead, which is the worse failure (see FloorAppliedStamp's own declaration).
class MvccStamper {
 public:
  // drakeydb: P4-4 Task A5 fix round 1 -- the 4th parameter is true iff this arm is a tombstone
  // (ArmTombstone'd, never a plain Arm()'d one) -- ground truth set by the SERVER's own code, at
  // the call site that armed it, never influenced by a caller-supplied wire mvcc. A caller that
  // needs to know whether this commit is logically a delete (e.g. to decide the final stamp's own
  // tombstone bit) must read THIS, never the 3rd parameter's own IsTombstone(): that stamp's mvcc
  // field is the author's wire-supplied value for an applied write, unsanitized, so it could in
  // principle carry a stray bit 63 for an ordinary (non-delete) write. The 5th parameter is the
  // arm's own captured PRE-mutation stamp: Arm()'s own 3rd argument for a plain arm (from
  // DbSlice::EnsureMvcc's return, PostUpdate) or ArmTombstone's 3rd argument for a tombstone arm
  // (from PerformDeletionAtomic's own pre-delete capture) -- both are captured at ARM time,
  // because by commit time the slot itself may no longer hold it (EnsureMvcc's own
  // tombstone-clearing branch, or PerformDeletionAtomic's placeholder write, may already have
  // overwritten it, possibly well before Commit() ever runs).
  using CommitFn =
      std::function<void(DbIndex, std::string_view, const MvccStamp&, bool, const MvccStamp&)>;
  // drakeydb: P4-3 Task 2 review fix (I3) -- invoked by EndOfWriteEpoch below, once per arm that
  // is STILL armed (uncommitted) at epoch end AND was armed via ArmTombstone (never a plain
  // Arm()). PerformDeletionAtomic (db_slice.cc) writes a synchronous tombstone placeholder to
  // keep the dense invariant intact the instant a key leaves `prime`, expecting the delete's own
  // journal commit to overwrite that placeholder with a real, minted stamp -- but that commit can
  // fail to happen (a non-default namespace, an auto-journaled command that returns non-OK, a
  // NO_AUTOJOURNAL branch that never reaches its own RecordJournal, and others this type has no
  // way to enumerate). Left alone, the abandoned placeholder ({kTombstoneBit, 0} -- Mvcc()==0,
  // origin_hash==0, the strict minimum of operator<) stays in the table forever and can never win
  // a future merge comparison: a silently-lost delete. erase_fn lets the caller (which owns the
  // DbSlice this arm's db_index names) undo that placeholder generically, without this type
  // needing to know anything about DbSlice or enumerate the paths that can orphan an arm.
  using EraseFn = std::function<void(DbIndex, std::string_view)>;

  // Re-mint the hop stamp if the memo is older than this. A missed EndOfWriteEpoch then degrades
  // visibly (stats().stale_epoch) instead of silently reusing an ancient stamp.
  static constexpr uint64_t kMaxEpochMs = 50;

  struct Stats {
    uint64_t unstamped_writes = 0;  // arms discarded with no journal entry -- see INFO
    uint64_t stale_epoch = 0;       // hop memo re-minted by the backstop above
  };

  static MvccStamper* tlocal();

  void SetSelfUuid(std::string_view uuid);
  void RegisterOriginHash(uint32_t origin_idx, uint64_t hash);
  uint64_t OriginHash(uint32_t origin_idx) const;

  // Stable for the whole shard callback: repeated calls return the same value until the memo is
  // older than kMaxEpochMs, the wall clock moves backward, or EndOfWriteEpoch() resets it. Memo
  // age is tracked separately from the logical stamp: MvccClock may deliberately ratchet its
  // stamp ahead of wall time. Takes time as a parameter -- like MvccClock::Next/AheadMs -- so
  // dfly_transaction never reaches up into dragonfly_lib for GetCurrentTimeMs; callers (Task 7's
  // journal.cc) pass it explicitly.
  uint64_t HopStamp(uint64_t now_ms);

  // drakeydb: P4-4 Task A5b -- spec D3: "Local stamp = max(clock_tick(), stored.mvcc + 1)". A
  // node whose wall clock trails a peer's already-observed stamp for a key otherwise mints a
  // causally-LATER local write BELOW that stamp -- the client sees OK, and every peer running the
  // streaming LWW guard silently drops the write as stale. This is a pure read of the arms already
  // pending at mint time: Arm()/ArmTombstone() ran first, so every arm THIS entry itself placed is
  // already in armed_ by the time journal::RecordEntry (journal.cc) calls this, ahead of that same
  // statement's own AddLogRecord call. (Its order relative to that statement's own HopStamp() call
  // is unspecified by C++ -- both are arguments to one std::max -- but harmless: this has no side
  // effects, so calling it before or after HopStamp's memo update changes nothing observable.)
  // Never a per-shard clock ratchet -- MvccClock itself is untouched. Deliberately NOT folded into
  // MvccClock::Next: ratcheting the shared clock would let one fast/skewed peer's write poison
  // every LATER, unrelated local mint on this shard too, which the spec explicitly rejects.
  //
  // Returns the highest (prev_stamp.Mvcc() + 1) among currently-armed keys that carry a real
  // prior stamp (Mvcc() != 0 -- a fresh {0,0} slot or an uncommitted placeholder has none to floor
  // against, same check as FloorAppliedStamp's), or 0 if none does -- a no-op floor, so
  // std::max(HopStamp(now), LocalMintFloor()) degrades to a bare HopStamp exactly as before D3.
  // Capped at MvccClock::kStampMask (P4-4 Task A5b fix round 1): a corrupt or hostile peer stamp
  // with Mvcc() exactly kStampMask would otherwise overflow prev+1 into precisely kTombstoneBit,
  // silently marking a LIVE write's stamp as a tombstone (Mvcc() masks bit 63 back to 0).
  uint64_t LocalMintFloor() const;

  // May be called more than once for the same (db_index, key) within one callback -- a command
  // can take a second, independent FindMutable/AutoUpdater on a key it is about to delete (e.g.
  // DeleteHw, DelMutable's other callers). Duplicates are harmless for an ORDINARY plain arm:
  // Commit() below just calls its CommitFn once per arm with the same stamp.
  //
  // KNOWN RESIDUAL, deliberately parked, not fixed: if a key is armed as a PLAIN arm more than
  // once in the same callback, and the FIRST such arm's own EnsureMvcc call cleared a tombstone
  // -- whether a same-callback pending ArmTombstone's placeholder (fix round 2's inheritance
  // case, see Arm()'s own comment below) or an ALREADY-COMMITTED tombstone left over from an
  // earlier, separate command (e.g. an unguarded applied `MSET k a k b` touching the same
  // tombstoned key twice: the first pair's EnsureMvcc clears it and correctly inherits/captures
  // its true prior stamp, the second pair's own EnsureMvcc call then sees the already-cleared
  // {0,0}) -- only that FIRST arm's prev_stamp reflects a real prior stamp. Every SUBSEQUENT
  // plain arm for the same key in the same callback sees the already-cleared {0,0}, floors
  // (verbatim) against it, and Commit() processing arms in registration order means this later,
  // wrong commit silently overwrites the first arm's correct floor.
  //
  // drakeydb: P4-4 Task A5 fix round 1 -- `prev_stamp` is this key's stamp from BEFORE this
  // write, mandatory (no default -- a caller that skipped capturing it would silently commit the
  // author's stamp verbatim, exactly the bug this task exists to close): DbSlice::PostUpdate's
  // own EnsureMvcc call returns it and passes it straight through. There is no live-table
  // fallback for a caller that has none -- EnsureMvcc's tombstone-clearing branch (db_slice.cc)
  // can overwrite the slot synchronously, well before Commit() ever runs, so the slot itself is
  // not a reliable place to look this up later.
  //
  // drakeydb: P4-4 Task A5 fix round 2 -- if `prev_stamp` is itself an uncommitted placeholder
  // (IsTombstone() && Mvcc() == 0 -- PerformDeletionAtomic's own synchronous stand-in,
  // db_slice.cc), this key was ArmTombstone'd earlier in THIS SAME callback (a delete-then-
  // recreate of the same key in one command, e.g. Renamer::DeserializeDest deleting an existing
  // dest then recreating it -- RenameOntoAnExistingDestSelfCorrectsToALiveStamp). EnsureMvcc's own
  // tombstone-clearing branch has no way to recover that earlier arm's true pre-delete stamp --
  // it only ever sees PerformDeletionAtomic's placeholder, which has already overwritten it -- so
  // this call searches armed_ for a still-pending tombstone arm on the exact same (db_index, key)
  // and inherits ITS prev_stamp instead. Scoped to the placeholder case alone (checked before the
  // search, never unconditionally): the common path -- a plain write with a real prior stamp, or
  // none at all -- stays O(1) and allocation-free, and only ever pays this O(armed_.size()) scan
  // on the rare delete-then-recreate-in-one-entry shape armed_ (per-epoch, small) is built for.
  void Arm(DbIndex db_index, std::string_view key, const MvccStamp& prev_stamp);
  // drakeydb: P4-3 Task 2 -- identical bookkeeping to Arm above, except the recorded Armed entry
  // carries tombstone=true, so Commit() ORs kTombstoneBit into the stamp it hands this key's
  // CommitFn (only for this arm; a plain Arm() of a different key in the same Commit() call is
  // unaffected). PerformDeletionAtomic (db_slice.cc) is the only caller: a kExplicit or kExpired
  // (P4-3 Task 11) delete re-arms the key it just Disarm()'d as a tombstone instead of erasing its
  // side-table slot outright -- kEvicted/kSlotFlush still erase (see DeleteReason's comment,
  // db_slice.h, for why). Unlike Arm(), never needs a preceding EnsureMvcc: the dense invariant
  // (mvcc->size() - mvcc_tombstones == prime.size(), db_slice.cc) guarantees a live prime key --
  // the only kind PerformDeletionAtomic ever deletes -- already owns an mvcc slot.
  // drakeydb: P4-4 Task A5 -- `prev_stamp` is this key's PRE-delete stamp, captured by the caller
  // (PerformDeletionAtomic, db_slice.cc) BEFORE it overwrites the slot with the synchronous
  // tombstone placeholder -- by the time Commit() below runs, the slot no longer holds it, so it
  // must be carried on the arm itself for Commit()'s caller to floor an applied delete against.
  // drakeydb: P4-4 Task A5 fix round 1 -- no default (was `= MvccStamp{}`): a defaulted
  // parameter let a caller silently fall back to "no real prior stamp" (verbatim commit) without
  // ever noticing it skipped the capture. Every call site now supplies one explicitly, including
  // every test in mvcc_test.cc that does not otherwise care about the floor (passing `MvccStamp{}`
  // there by name, not by omission).
  void ArmTombstone(DbIndex db_index, std::string_view key, const MvccStamp& prev_stamp);
  // Erases EVERY arm matching (db_index, key), not just the first -- see the .cc for why a
  // single-match version corrupted the mvcc side table (review wave 2, F1, CRITICAL).
  void Disarm(DbIndex db_index, std::string_view key);

  // The caller always supplies a non-zero stamp: the author's freshly minted HopStamp(now_ms), or
  // an applied write's verbatim author stamp -- both under the same MvccEnabled() && COMMAND gate,
  // so Commit itself never mints (DCHECK'd in the .cc). For an ArmTombstone'd key, the stamp
  // handed to that key's CommitFn call has kTombstoneBit OR'd in (mvcc itself is untouched, so a
  // plain Arm() of a different key in the same call still gets the bare value) -- this is the
  // ONLY place the live delete path sets that bit; see AsTombstone()'s comment above for why the
  // RDB load path (a later task) uses a separate method instead of this one setting it twice.
  // drakeydb: P4-4 Task A5 fix round 1 -- fn's 4th argument is that arm's own Armed::tombstone
  // (ground truth), its 5th is that arm's own Armed::prev_stamp, both passed through verbatim --
  // see CommitFn's own comment for what each means and when it matters.
  // Clears the arm list -- unconditionally,
  // even if fn throws (RAII in the .cc): a surviving armed_ after a partial failure would either
  // let EndOfWriteEpoch() over-count already-attempted keys as unstamped, or, worse, let a LATER
  // Commit() in the same epoch (e.g. a second RecordEntry from one Lua script) stamp these
  // leftover keys with an unrelated entry's mvcc. Decided and documented in Task 7 -- see the .cc.
  //
  // fn must not call Arm(), Disarm(), Commit(), or EndOfWriteEpoch(): this call is mid-iteration
  // over armed_/arena_, and any of the four would corrupt that iteration -- Arm()/a nested
  // Commit() by reallocating arena_ (invalidating the string_view key fn was just handed) and/or
  // armed_ (invalidating the iterator), Disarm()/EndOfWriteEpoch() by erasing from/clearing
  // armed_ out from under it. All four are DCHECK'd in the .cc via commit_depth_.
  void Commit(uint64_t mvcc, uint32_t origin_idx, const CommitFn& fn);

  // drakeydb: P4-4 -- commits ONLY the tombstone arm for (db_index, key), if one is currently
  // armed, independently of whatever the epoch's ordinary Commit() call above would otherwise
  // apply to EVERY armed key. An expiry is always a local decision: its own tombstone is
  // `ExpiryTombstoneFor` (above) applied to that arm's own captured prev_stamp -- one tick above
  // the expired value's own pre-deletion stamp -- never an applied peer command's mvcc/origin, and
  // never a freshly minted, wall-clock-derived one, even when a sibling key armed earlier in the
  // SAME epoch (e.g. a replicated multi-key command's other pair) correctly must retain that
  // peer's stamp -- which happens via the enclosing command's own, LATER journal entry and its own
  // Commit() call, never this one: RecordExpiryBlocking (tx_base.cc) is the only caller, and calls
  // this BEFORE its own journal::RecordEntry, so this key's arm (if any) has already been removed
  // and stamped here by the time that entry's own commit logic runs, and a sibling's arm is left
  // untouched by this call for that later entry to pick up.
  //
  // Falls back to a freshly minted self stamp (HopStamp(now_ms)) only when that arm's own
  // prev_stamp carries no real authority (Mvcc() == 0: a value that never received a stamp, or a
  // slot PerformDeletionAtomic's own GetMvcc call found nothing for) -- `ExpiryTombstoneFor` has
  // nothing to advance in that case. Looks up this node's own (kSelfIdx) origin hash only in that
  // fallback, and only if a matching tombstone arm is found at all -- avoids the lookup (and, in
  // the fallback branch, advancing the shared per-thread clock) on every expiry when nothing is
  // armed (TombstonesEnabled() is false, the delete was outside the default namespace, or
  // PerformDeletionAtomic degraded to a plain erase at the tombstone cap). Returns true if it
  // found and committed (removing) that arm; false otherwise -- the caller has no narrower way to
  // know which of those cases applies, and does not need to: either way, its own subsequent
  // journal entry is unaffected.
  //
  // Same reentrancy contract as Commit()/EndOfWriteEpoch() above: fn must not call Arm()/Disarm()/
  // Commit()/EndOfWriteEpoch()/this method itself -- DCHECK'd the same way, via commit_depth_.
  bool CommitOwnTombstone(DbIndex db_index, std::string_view key, uint64_t now_ms,
                          const CommitFn& fn);

  // The fourth mutator of armed_/arena_ (Task 7 closes the gap: originally not DCHECK'd against
  // commit_depth_ like Arm()/Disarm()/Commit() above). See Commit()'s comment for why a CommitFn
  // calling this would corrupt Commit()'s own in-progress iteration.
  //
  // drakeydb: P4-3 Task 2 review fix (I3) -- calls erase_fn (see its own comment above) for every
  // still-armed tombstone arm, BEFORE armed_/arena_ are cleared (so ArmedKey is still valid
  // inside erase_fn), then clears armed_/arena_/hop_stamp_ exactly as before. No default: every
  // call site must consciously supply a rollback, or a tombstone-earning delete on that path
  // degrades from "no peer learns about this delete" (already the accepted, bounded-risk fallback
  // at the cap) to "no peer ever CAN learn about this delete, permanently" -- worse, and silent.
  // erase_fn must not call Arm()/Disarm()/Commit()/EndOfWriteEpoch(): this call is mid-iteration
  // over armed_/arena_ for the same reason Commit()'s fn may not, above -- DCHECK'd the same way.
  void EndOfWriteEpoch(const EraseFn& erase_fn);

  const Stats& stats() const {
    return stats_;
  }
  const MvccClock& clock() const {
    return clock_;
  }

  void TEST_Reset();

 private:
  struct Armed {
    DbIndex db_index;
    uint32_t off;
    uint32_t len;
    // drakeydb: P4-3 Task 2 -- true for an ArmTombstone()'d entry; Commit() ORs kTombstoneBit
    // into the stamp it hands this arm's CommitFn call, and only this arm's.
    //
    // No default member initializer, deliberately (P4-4 Task A5 fix round 4): both Arm() and
    // ArmTombstone() (mvcc.cc) already fully specify all 5 fields of every Armed{} they
    // construct, so nothing needs one -- and giving it one would SUPPRESS
    // -Wmissing-field-initializers (-Wextra, -Werror in CI) for a FUTURE aggregate-init that
    // forgot a field, silently defaulting `tombstone` to false and `prev_stamp` (below) to
    // "nothing to floor against" instead of failing the build loudly. See prev_stamp's own
    // comment below for the identical reasoning.
    bool tombstone;
    // drakeydb: P4-4 Task A5 -- this arm's captured pre-mutation stamp: ArmTombstone's own 3rd
    // argument for a tombstone arm (PerformDeletionAtomic's pre-delete capture, db_slice.cc), or
    // Arm()'s own 3rd argument for a plain arm (DbSlice::EnsureMvcc's return, PostUpdate --
    // possibly itself replaced by an inherited tombstone arm's own prev_stamp, fix round 2, see
    // Arm()'s own comment). Handed to Commit()'s CommitFn verbatim as that call's 5TH argument;
    // the journal.cc lambda reads it for BOTH arm kinds, never a live table lookup (fix round 1).
    // No default member initializer either, for the same reason `tombstone` above has none.
    MvccStamp prev_stamp;
  };

  std::string_view ArmedKey(const Armed& a) const {
    return std::string_view(arena_.data() + a.off, a.len);
  }

  MvccClock clock_;
  uint64_t hop_stamp_ = 0;
  uint64_t hop_started_ms_ = 0;  // wall-time observation used only to age hop_stamp_
  std::string arena_;            // cleared, never shrunk, so steady-state arming does not allocate
  // std::vector, not absl::InlinedVector: InlinedVector::clear() frees (DeallocateIfAllocated())
  // and reverts to inline storage, so any callback arming more than 4 keys would allocate on the
  // 5th arm and free on every single Commit/EndOfWriteEpoch, forever. std::vector::clear() keeps
  // capacity, matching arena_'s "cleared, never shrunk" discipline. Do not "optimise" this back --
  // InlinedVector is only cheaper for callbacks that arm <=4 keys, and worse for every other one.
  std::vector<Armed> armed_;
  std::vector<uint64_t> origin_hash_cache_;  // dense by origin_idx; index 0 == self
  Stats stats_;
  // >0 while Commit() is mid-iteration over armed_/arena_. Guards Arm(), Disarm(), and a
  // re-entrant Commit() call from a CommitFn -- see Commit()'s comment above for why each would
  // corrupt that iteration. A counter, not a bool: a bool reentrancy flag reset by an inner
  // Commit() call on return would stop guarding Arm()/Disarm() calls still made by the outer,
  // still-running one. RAII'd (absl::Cleanup, in the .cc) around the loop, not a bare decrement
  // after it, so a throwing fn still leaves this at 0 rather than stuck positive forever.
  int commit_depth_ = 0;
};

}  // namespace dfly
