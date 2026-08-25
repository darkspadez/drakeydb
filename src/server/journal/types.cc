// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/types.h"

#include <absl/strings/str_join.h>

#include "base/logging.h"
#include "server/multi_master.h"  // drakeydb: Phase 3 T7b -- PeerRegistry::kSelfIdx, see below.

namespace dfly::journal {

using namespace std;

void AppendPrefix(string_view cmd, string* dest) {
  absl::StrAppend(dest, ", cmd='");
  absl::StrAppend(dest, cmd);
  absl::StrAppend(dest, "', args=[");
}

void AppendSuffix(string* dest) {
  if (dest->back() == ',')
    dest->pop_back();
  absl::StrAppend(dest, "]");
}

string Entry::ToString() const {
  string rv = absl::StrCat("{op=", opcode, ", dbid=", dbid);

  if (HasPayload()) {
    AppendPrefix(payload.cmd, &rv);
    for (string_view arg : base::it::Wrap(cmn::kToSV, payload.args))
      absl::StrAppend(&rv, "'", cmn::ToSV(arg), "',");
    AppendSuffix(&rv);
  } else {
    absl::StrAppend(&rv, ", empty");
  }

  rv += "}";
  return rv;
}

string ParsedEntry::ToString() const {
  return absl::StrCat("{op=", opcode, ", dbid=", dbid, ", cmd='")  //
         + absl::StrJoin(cmd.view(), " ") + "'}";
}

// drakeydb: Phase 3 T7b -- see the doc comment in types.h. Shared verbatim by
// JournalStreamer::ShouldWrite and SliceSnapshot::ConsumeJournalChange.
bool PassesPeerEchoFilter(const JournalItem& item) {
  if (item.origin_idx != PeerRegistry::kSelfIdx) {
    LOG_EVERY_T(ERROR, 60) << "Refusing to forward foreign-origin journal entry on peer stream: "
                           << "origin_idx=" << item.origin_idx
                           << ", opcode=" << static_cast<unsigned>(item.opcode);
    return false;
  }

  if (item.entry_flags & kEntryFlagExpired)
    return false;

  if (item.entry_flags & kEntryFlagDerived)
    return false;

  if (item.opcode == Op::ORIGIN)
    return false;

  return true;
}

}  // namespace dfly::journal
