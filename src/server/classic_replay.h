// Copyright 2026, drakeydb authors.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <string_view>

namespace dfly {

// What a classic (Redis-protocol) master said in reply to a `REPLCONF capa ...` command. A stock
// master answers `+OK`; an active KeyDB appends capability words to every capa reply
// (`+OK active-replica`, possibly followed by `keydb-fastsync-save`).
struct CapaReply {
  bool ok = false;
  bool active_replica = false;
  bool keydb_fastsync_save = false;
};

// Parses the simple string of a capa reply, i.e. "OK" or "OK <word> ...". `ok` is set iff the first
// token is exactly "OK" (case-sensitive, as before the suffix was tolerated); the words after it
// are separated by spaces and unknown ones are ignored.
CapaReply ParseCapaReply(std::string_view simple_string);

}  // namespace dfly
