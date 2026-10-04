// Copyright 2026, drakeydb authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "server/classic_replay.h"

#include <absl/strings/match.h>
#include <absl/strings/str_split.h>

namespace dfly {

using namespace std;

CapaReply ParseCapaReply(string_view simple_string) {
  CapaReply reply;
  if (!absl::ConsumePrefix(&simple_string, "OK"))
    return reply;
  // "OKAY" or "OKactive-replica" is not an OK followed by words.
  if (!simple_string.empty() && simple_string.front() != ' ')
    return reply;

  reply.ok = true;
  for (string_view word : absl::StrSplit(simple_string, ' ', absl::SkipEmpty())) {
    if (word == "active-replica")
      reply.active_replica = true;
    else if (word == "keydb-fastsync-save")
      reply.keydb_fastsync_save = true;
  }
  return reply;
}

}  // namespace dfly
