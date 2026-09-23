// Copyright 2026, drakeydb authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "server/multi_master.h"

#include <limits>

#include "absl/flags/declare.h"
#include "absl/flags/flag.h"
#include "absl/strings/str_cat.h"
#include "base/logging.h"
#include "facade/cmd_arg_parser.h"
#include "server/engine_shard_set.h"
#include "server/journal/journal.h"
#include "server/multimaster_lww.h"  // drakeydb: P4-4 -- for FLAGS_multi_master_stream_lww

ABSL_FLAG(bool, active_replica, false,
          "drakeydb: stay a writable master while replicating from the masters given to "
          "REPLICAOF / --replicaof (KeyDB active-replica). Boot-only.");
ABSL_FLAG(bool, multi_master, false,
          "drakeydb: let REPLICAOF attach several masters at once (fan-in). Requires "
          "--active_replica (KeyDB multi-master). Boot-only.");
// drakeydb: P4-3 Task 2 -- an explicit DEL keeps its mvcc side-table slot (with kTombstoneBit
// set) instead of erasing it, so a peer's stale snapshot cannot resurrect a key we deleted.
// kEvicted/kSlotFlush are unaffected: they always erase (see PerformDeletionAtomic, db_slice.cc).
// kExpired joined kExplicit in P4-3 Task 11, once ExpireIfNeeded/DeleteReapedContainer were
// reordered to journal after deleting -- see DeleteReason's comment (db_slice.h) for the history.
// Seconds, not ms, to match
// --tombstone_ttl-style operator-facing flags elsewhere; a tombstone's own GC deadline math
// (MvccStamp::DeadlineMs) takes ms, so callers convert.
ABSL_FLAG(uint64_t, multi_master_tombstone_ttl, 600,
          "drakeydb: seconds a tombstone (a deleted key's stamp, kept instead of erased) is "
          "retained before DbSlice::TombstoneGcStep's idle-task GC reclaims it. This value also "
          "gates TombstonesEnabled(): 0 disables tombstoning entirely -- every delete erases its "
          "slot immediately, same as kEvicted/kSlotFlush -- and the GC step itself no-ops.");
// drakeydb: P4-3 Task 8, controller fix (I1) -- "per-shard" was imprecise: table->stats
// (db_slice.cc's cap check) lives on a DbTable, one per (SELECT-able database, shard) pair, not
// one per shard overall. A node with N databases can therefore hold up to N times this many live
// tombstones on a single shard -- one independent cap per db index.
ABSL_FLAG(uint64_t, multi_master_max_tombstones, 1000000,
          "drakeydb: cap on live tombstones per (database, shard) pair -- NOT a single per-shard "
          "total: a node with N SELECT-able databases can hold up to N times this many on one "
          "shard. A delete that would push its own (database, shard) pair's tombstone count past "
          "this degrades to an erase instead (counted in mvcc_tombstones_dropped), trading "
          "resurrection risk for bounded memory on a shard that never gets an idle moment for GC "
          "to catch up.");
ABSL_FLAG(uint32_t, multi_master_tombstone_gc_budget, 64,
          "drakeydb: mvcc side-table buckets DbSlice::TombstoneGcStep (db_slice.cc) visits per "
          "idle-task GC tick when reclaiming tombstones older than "
          "--multi_master_tombstone_ttl. Bounds one tick's total work, spread across however "
          "many SELECT-able databases still need visiting -- not a per-database allowance.");
ABSL_DECLARE_FLAG(std::string, cluster_mode);
ABSL_DECLARE_FLAG(std::string, tiered_prefix);
ABSL_DECLARE_FLAG(bool, experimental_cascaded_partial_sync);
ABSL_DECLARE_FLAG(bool, cache_mode);

namespace dfly {

bool IsActiveReplica() {
  return absl::GetFlag(FLAGS_active_replica);
}

bool IsMultiMaster() {
  return absl::GetFlag(FLAGS_multi_master);
}

bool TombstonesEnabled() {
  return absl::GetFlag(FLAGS_multi_master_tombstone_ttl) != 0;
}

bool ValidateMultiMasterFlags() {
  const bool active = absl::GetFlag(FLAGS_active_replica);
  if (absl::GetFlag(FLAGS_multi_master) && !active) {
    LOG(ERROR) << "--multi_master requires --active_replica";
    return false;
  }
  if (!active)
    return true;
  if (!absl::GetFlag(FLAGS_cluster_mode).empty()) {
    LOG(ERROR) << "--active_replica is incompatible with --cluster_mode";
    return false;
  }
  if (!absl::GetFlag(FLAGS_tiered_prefix).empty()) {
    LOG(ERROR) << "--active_replica is incompatible with tiering (--tiered_prefix)";
    return false;
  }
  if (absl::GetFlag(FLAGS_experimental_cascaded_partial_sync)) {
    LOG(ERROR) << "--active_replica is incompatible with --experimental_cascaded_partial_sync";
    return false;
  }
  // drakeydb: P4-3 Task 3 -- guards Task 1 review Finding 6 (task-1-review.md, carried forward by
  // progress.md): a negative value reaching this uint64_t flag (e.g. built from a signed source
  // upstream of absl's own flag parser, which already rejects a literal "-1" on the command line)
  // sign-converts to a value near UINT64_MAX. TombstoneGcStep (db_slice.cc) multiplies this by
  // 1000 for MvccStamp::DeadlineMs's ms domain and adds MsPart() (mvcc.h); with a wrapped ttl that
  // sum overflows uint64 and wraps to a deadline in the PAST, so the very first GC tick would reap
  // every live tombstone, silently reopening the resurrection window Task 2 closed.
  // kMaxTombstoneTtlSeconds is far beyond any real deployment's needs (~136 years) but comfortably
  // below the region where that overflow becomes reachable, so no legitimate operator value can
  // ever trip it.
  constexpr uint64_t kMaxTombstoneTtlSeconds = std::numeric_limits<uint32_t>::max();
  if (absl::GetFlag(FLAGS_multi_master_tombstone_ttl) > kMaxTombstoneTtlSeconds) {
    LOG(ERROR) << "--multi_master_tombstone_ttl ("
               << absl::GetFlag(FLAGS_multi_master_tombstone_ttl)
               << "s) is implausibly large -- likely a sign-converted negative value; refusing "
                  "to start";
    return false;
  }
  // drakeydb: P4-3 Task 3, review fix (C1), Critical -- 0 buckets/tick can never make progress,
  // so DbSlice::TombstoneGcStep (db_slice.cc) treats it identically to "GC has nothing to do" and
  // no-ops -- but before that fix, 0 was indistinguishable from "ran out of budget mid-lap", and
  // the on-idle wrapper (the DbSlice constructor) maps that into the proactor's highest
  // re-scheduling frequency with no backoff: reproduced directly, this returned true on 5/5
  // consecutive calls while doing zero work, which pegs a shard's core at 100% forever. Reject at
  // boot so a misconfigured value never reaches that path; TombstoneGcStep's own guard remains as
  // a backstop for a value set after boot (e.g. a future CONFIG SET, or a test's absl::SetFlag).
  if (absl::GetFlag(FLAGS_multi_master_tombstone_gc_budget) == 0) {
    LOG(ERROR) << "--multi_master_tombstone_gc_budget must be >= 1 (0 buckets/tick can never "
                  "reclaim a tombstone, and is treated as a misconfiguration, not a valid "
                  "'never run' setting -- use --multi_master_tombstone_ttl=0 for that)";
    return false;
  }
  // drakeydb: P4-3 Task 2 -- not a validation failure (both flags are independently legal), but a
  // resurrection-semantics gotcha worth surfacing at boot: --cache_mode evicts under memory
  // pressure, and eviction (kEvicted, PerformDeletionAtomic) deliberately writes no tombstone --
  // the peer's copy is authoritative, so resurrection on that peer's next full sync is the whole
  // point. An explicit DEL (or, since P4-3 Task 11, an expiry) on the SAME key still tombstones
  // normally; only the memory-pressure-evicted keys are a deliberate design choice, not a gap.
  if (TombstonesEnabled() && absl::GetFlag(FLAGS_cache_mode)) {
    LOG(WARNING) << "--active_replica with --cache_mode: eviction cannot free tombstones and "
                    "evicted keys deliberately get none (kEvicted is a capacity decision, not a "
                    "deletion), so a key evicted here for memory pressure can be resurrected by a "
                    "peer's full sync -- an explicit DEL on this node still tombstones and is "
                    "immune to that";
  }
  // drakeydb: P4-4 -- this warning used to say every stable-sync apply stayed arrival-order
  // "until P4-4", with no flag to change that. P4-4 added --multi_master_stream_lww (default
  // true): a guarded command's own replicated write (SET, SETNX, GETSET, GETDEL, PEXPIREAT,
  // PERSIST, RESTORE, and MSET/DEL's own per-key split -- see docs/multi-master.md for the full
  // table) is now LWW-compared against the local stamp on stream, the same rule full-sync merge
  // already used (ties favor the stored side). Only turning that flag off brings back the old
  // arrival-order behavior for those commands; full-sync merge LWW (MergeAccepts, mvcc.h) is
  // unconditional either way and does not read this flag.
  LOG(WARNING) << "--active_replica: known limitations -- a local read (e.g. HTTL) can lazily "
                  "expire and delete a peer's not-yet-expired key under clock skew; full-sync "
                  "merge is last-write-wins per key since P4-3 (ties favor the stored side; a "
                  "classic-protocol peer's unstamped keys use an approximate snapshot-time "
                  "authority and can resurrect an older delete -- see docs/multi-master.md); "
                  "delta-journaled RMW commands (INCR, APPEND, ...) always resolve by arrival "
                  "order, never LWW-compared -- dropping a delta would lose it outright, not "
                  "merely reorder it";
  if (!absl::GetFlag(FLAGS_multi_master_stream_lww)) {
    LOG(WARNING) << "--multi_master_stream_lww=false: streamed peer writes for otherwise-guarded "
                    "commands apply in plain arrival order, same as every stable-sync apply "
                    "before P4-4 -- a peer's write can still overwrite a newer local write if it "
                    "simply arrives later. Full-sync merge LWW (see above) is unaffected and "
                    "stays on regardless";
  }
  return true;
}

nonstd::expected<PeerReplicaOfCmd, facade::ErrorReply> ParsePeerReplicaOfArgs(
    facade::ParsedArgs args) {
  PeerReplicaOfCmd cmd;
  facade::CmdArgParser parser(args);
  if (parser.Check("NO")) {
    parser.ExpectTag("ONE");
    cmd.kind = PeerReplicaOfCmd::Kind::kNoOne;
  } else {
    if (parser.Check("REMOVE"))
      cmd.kind = PeerReplicaOfCmd::Kind::kRemove;
    cmd.host = parser.Next<std::string>();
    cmd.port = parser.Next<facade::Positive<uint16_t>>("port is out of range");
    if (auto err = parser.TakeError(); err)
      return nonstd::make_unexpected(facade::ErrorReply("port is out of range"));
  }
  if (parser.HasNext())
    return nonstd::make_unexpected(
        facade::ErrorReply("slot ranges are not supported in active-replica mode"));
  if (auto err = parser.TakeError(); err)
    return nonstd::make_unexpected(err.MakeReply());
  return cmd;
}

int64_t ComputeClockSkewMs(int64_t local_clock_ms, int64_t peer_clock_ms) {
  if (peer_clock_ms == 0)
    return 0;
  return peer_clock_ms - local_clock_ms;
}

bool IsClockSkewConcerning(int64_t skew_ms) {
  return skew_ms >= kClockSkewWarnMs || skew_ms <= -kClockSkewWarnMs;
}

std::string RenderPeerReplicationInfo(const std::vector<ReplicaSummary>& peers, bool multi_master,
                                      bool show_peer_lines) {
  std::string out = absl::StrCat("active_replica:1\r\nmulti_master:", multi_master ? 1 : 0,
                                 "\r\nconnected_masters:", peers.size(), "\r\n");
  if (!show_peer_lines)
    return out;
  for (size_t i = 0; i < peers.size(); ++i) {
    const ReplicaSummary& p = peers[i];
    absl::StrAppend(&out, "master", i, ":host=", p.host, ",port=", p.port,
                    ",link_status=", p.master_link_established ? "up" : "down",
                    ",last_io_seconds_ago=", p.master_last_io_sec,
                    ",sync_in_progress=", p.full_sync_in_progress ? 1 : 0);
    if (!p.master_node_uuid.empty())
      absl::StrAppend(&out, ",node_uuid=", p.master_node_uuid);
    // drakeydb: P4-0 -- unconditional (unlike node_uuid above): ComputeClockSkewMs's 0 default
    // is itself the meaningful "no clock sample yet" value, so there is no separate "absent"
    // case to omit the field for.
    absl::StrAppend(&out, ",clock_skew_ms=", p.clock_skew_ms);
    absl::StrAppend(&out, "\r\n");
  }
  return out;
}

void PeerRegistry::Init(std::string_view self_uuid) {
  util::fb2::LockGuard lk(mu_);
  CHECK(idx_to_uuid_.empty()) << "PeerRegistry::Init() must be called exactly once";
  CHECK(uuid_to_idx_.empty()) << "PeerRegistry::Init() must be called exactly once";
  uuid_to_idx_.try_emplace(std::string(self_uuid), kSelfIdx);
  idx_to_uuid_.emplace_back(self_uuid);
}

uint32_t PeerRegistry::AddOrGet(std::string_view uuid) {
  uint32_t idx;
  bool inserted;
  {
    util::fb2::LockGuard lk(mu_);
    auto [it, ins] = uuid_to_idx_.try_emplace(std::string(uuid), idx_to_uuid_.size());
    if (ins)
      idx_to_uuid_.emplace_back(uuid);
    idx = it->second;
    inserted = ins;
  }  // drakeydb: Phase 3 T5 -- lock released before the shard fan-out below.

  // drakeydb: Phase 3 T5 -- a newly-discovered peer gets announced on every shard's journal as an
  // Op::ORIGIN entry ("this index == this uuid"), so downstream plain-replica consumers can later
  // resolve origin_idx tags on entries this peer authored (a mesh peer doesn't need this relayed:
  // it discovers every other node directly, so JournalStreamer::ShouldWrite drops Op::ORIGIN for
  // peer consumers -- see streamer.cc).
  //
  // Emitted via journal::RecordEntry -- a real per-shard journal record occupying a real LSN slot
  // -- rather than written directly onto a streamer's socket the way Op::LSN is. This is load
  // bearing: replica.cc's journal_rec_executed_ accounting counts Op::ORIGIN like any other
  // opcode, and TransactionReader::NextTxData advances its own lsn_ for every opcode except
  // Op::LSN with a DCHECK_EQ tying the two together -- writing ORIGIN any other way would desync
  // that counter the moment a peer link starts emitting these.
  //
  // Deliberately outside the lock above: the fan-out below dispatches onto every shard's own
  // fiber and blocks this fiber until they all complete, and AddOrGet can itself be called
  // concurrently from several fibers (see PeerRegistryFiberTest) -- holding mu_ across that
  // fan-out would serialize unrelated AddOrGet/FindIdx/GetUuid calls behind it for no reason.
  //
  // shard_set is null in this file's own PeerRegistry/PeerRegistryFiberTest unit tests, which
  // exercise the registry without standing up a shard set; skip the fan-out there, the same way
  // journal_slice.cc's GetPerShardBacklogMaxBytes() treats a null shard_set.
  if (inserted && shard_set) {
    using Payload = journal::Entry::Payload;
    std::string uuid_copy(uuid);
    // drakeydb: fix-round-1 -- RunBriefInParallel (via DispatchBrief) contractually requires
    // `func` not to preempt (engine_shard_set.h:65,71). journal::RecordEntry -> AddLogRecord ->
    // CallOnChange -> JournalStreamer::ThrottleIfNeeded can block on waker_.await_until
    // (streamer.cc) whenever a registered consumer is stalled -- exactly the replication
    // back-pressure a newly joining peer tends to cause. RunBlockingInParallel (via Dispatch,
    // which spawns a real fiber per shard) is the preemption-safe fan-out primitive.
    shard_set->RunBlockingInParallel([&](EngineShard* shard) {
      if (shard->journal()) {
        Payload payload;
        payload.cmd = uuid_copy;
        journal::RecordEntry(/*txid=*/0, journal::Op::ORIGIN, /*dbid=*/0, /*slot=*/std::nullopt,
                             payload, /*origin_idx=*/idx);
      }
    });
  }
  return idx;
}

std::optional<uint32_t> PeerRegistry::FindIdx(std::string_view uuid) const {
  util::fb2::LockGuard lk(mu_);
  auto it = uuid_to_idx_.find(uuid);
  if (it == uuid_to_idx_.end())
    return std::nullopt;
  return it->second;
}

std::string PeerRegistry::GetUuid(uint32_t idx) const {
  util::fb2::LockGuard lk(mu_);
  if (idx >= idx_to_uuid_.size())
    return "";
  return idx_to_uuid_[idx];
}

size_t PeerRegistry::Size() const {
  util::fb2::LockGuard lk(mu_);
  return idx_to_uuid_.size();
}

}  // namespace dfly
