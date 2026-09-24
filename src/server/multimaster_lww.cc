// Copyright 2026, drakeydb authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "server/multimaster_lww.h"

#include <absl/flags/flag.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>

#include <algorithm>
#include <iterator>
#include <string>
#include <vector>

#include "base/logging.h"
#include "common/backed_args.h"
#include "server/server_state.h"

ABSL_FLAG(bool, multi_master_stream_lww, true,
          "drakeydb: on an active-replica peer link, compare each replicated guarded write "
          "against the local MVCC stamp and drop it if it is not strictly newer (LWW); false "
          "restores arrival order for streamed writes only -- merge-on-full-sync LWW stays on "
          "regardless.");

namespace dfly {

namespace {

struct ClassifiedCommand {
  std::string_view name;
  LwwClass klass;
};

// drakeydb: P4-4 -- sorted by name (all entries already upper-case) for ClassifyJournaledCommand's
// binary search below; TableIsSorted's static_assert enforces this as rows are added. See the
// declaration (multimaster_lww.h) for the table's full contract.
constexpr ClassifiedCommand kJournaledClasses[] = {
    {"DEL", LwwClass::kMultiKeySelfGuarded}, {"GETDEL", LwwClass::kSingleKey},
    {"GETSET", LwwClass::kSingleKey},        {"MSET", LwwClass::kMultiKeySelfGuarded},
    {"RESTORE", LwwClass::kSingleKey},       {"SET", LwwClass::kSingleKey},
    {"SETNX", LwwClass::kSingleKey},
};

constexpr bool TableIsSorted() {
  for (std::size_t i = 1; i < std::size(kJournaledClasses); ++i) {
    if (!(kJournaledClasses[i - 1].name < kJournaledClasses[i].name))
      return false;
  }
  return true;
}
static_assert(TableIsSorted(), "kJournaledClasses must stay sorted by name for binary search");

}  // namespace

LwwClass ClassifyJournaledCommand(std::string_view journaled_name) {
  const std::string upper = absl::AsciiStrToUpper(journaled_name);
  const auto it = std::lower_bound(
      std::begin(kJournaledClasses), std::end(kJournaledClasses), std::string_view(upper),
      [](const ClassifiedCommand& c, std::string_view name) { return c.name < name; });
  if (it != std::end(kJournaledClasses) && it->name == upper)
    return it->klass;
  return LwwClass::kUnguarded;
}

bool ApplyLwwRewrites(cmn::BackedArguments* args) {
  if (args->empty())
    return false;

  const std::string_view name = args->Front();

  if (absl::EqualsIgnoreCase(name, "SETNX")) {
    // drakeydb: P4-4 -- exactly 3 args (SETNX key value) or leave untouched; a malformed SETNX
    // is dispatch's problem, not this rewrite's.
    if (args->size() != 3)
      return false;
    const std::vector<std::string> rewritten{"SET", std::string(args->at(1)),
                                             std::string(args->at(2))};
    // BackedArguments has no in-place rename -- Assign rebuilds storage_/offsets_ from scratch.
    args->Assign(rewritten.begin(), rewritten.end(), rewritten.size());
    return true;
  }

  if (absl::EqualsIgnoreCase(name, "GETSET")) {
    // drakeydb: P4-4 -- GETSET key value always replicates as GETSET's own verbatim command
    // (auto-journaled: GETSET is CO::JOURNALED, not NO_AUTOJOURNAL). Replaying it verbatim on a
    // receiver re-runs GETSET's OWN read-modify-write, which requires the existing key (if any) to
    // already be a string -- a receiver holding a DIFFERENT type for this key gets WRONGTYPE and
    // applies nothing, even though the per-key veto already decided this write should win. Rewrite
    // to a plain SET, exactly like SETNX->SET above: a blind full-state write, same as every other
    // guarded single-key command's applied form.
    if (args->size() != 3)
      return false;
    const std::vector<std::string> rewritten{"SET", std::string(args->at(1)),
                                             std::string(args->at(2))};
    args->Assign(rewritten.begin(), rewritten.end(), rewritten.size());
    return true;
  }

  if (absl::EqualsIgnoreCase(name, "GETDEL")) {
    // drakeydb: P4-4 -- same reasoning as GETSET above: GETDEL key auto-journals verbatim
    // (CO::JOURNALED), and replaying it re-runs GETDEL's own type-checked read-then-delete, which
    // WRONGTYPEs (and deletes nothing) against a receiver holding a different type. Rewrite to a
    // plain DEL, exactly the write GETDEL's own author-side effect already was.
    if (args->size() != 2)
      return false;
    const std::vector<std::string> rewritten{"DEL", std::string(args->at(1))};
    args->Assign(rewritten.begin(), rewritten.end(), rewritten.size());
    return true;
  }

  if (absl::EqualsIgnoreCase(name, "RESTORE")) {
    // RESTORE key ttl serialized-value [REPLACE] [ABSTTL] [IDLETIME seconds] [FREQ frequency].
    // A short/malformed RESTORE (arity < 4) is dispatch's problem, same as SETNX above.
    if (args->size() < 4)
      return false;
    for (size_t i = 4; i < args->size(); ++i) {
      const std::string_view opt = args->at(i);
      if (absl::EqualsIgnoreCase(opt, "REPLACE"))
        return false;  // already present, anywhere, in any case -- nothing to do
      if (absl::EqualsIgnoreCase(opt, "IDLETIME") || absl::EqualsIgnoreCase(opt, "FREQ"))
        ++i;  // the next token is that option's VALUE, not another option name -- skip it
    }
    args->PushArg("REPLACE");
    return true;
  }

  return false;
}

std::optional<MvccStamp> IncomingStamp(uint64_t mvcc, uint32_t origin_idx) {
  if (mvcc == 0)
    return std::nullopt;
  const uint64_t hash = MvccStamper::tlocal()->OriginHash(origin_idx);
  // drakeydb: P4-4 -- a real (non-zero) mvcc must always name a registered origin; an
  // unregistered origin here means a caller invoked this before the peer's handshake finished
  // registering its hash, which should never happen on a real apply path. DCHECK surfaces that
  // as a hard failure in debug builds; a release build still fails open below (hash == 0 ->
  // nullopt) rather than forming a bogus {mvcc, 0} stamp that would compare wrong for every key.
  DCHECK(hash != 0) << "origin_idx " << origin_idx
                    << " has no registered hash but incoming mvcc != 0";
  if (hash == 0)
    return std::nullopt;
  return MvccStamp{mvcc, hash};
}

void NoteLwwDrop(std::string_view journaled_name, std::string_view key) {
  ServerState::Stats& stats = ServerState::tlocal()->stats;
  ++stats.multimaster_lww_dropped;
  VLOG(2) << "multi-master LWW guard dropped " << journaled_name << " on key " << key;
  // stats.multimaster_lww_dropped is this SHARD/proactor thread's own counter (ServerState is
  // thread-local); this rollup is per-thread too, not the cross-shard sum INFO replication and
  // the Prometheus counter report (ServerState::Stats::Add, server_state.cc).
  LOG_EVERY_T(INFO, 60) << "multi-master LWW guard has dropped " << stats.multimaster_lww_dropped
                        << " replicated writes so far on this shard";
}

}  // namespace dfly
