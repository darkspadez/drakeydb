// Copyright 2026, drakeydb authors.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <string>
#include <string_view>

#include "io/io.h"

namespace dfly {

// Fork replication protocol version, sent via REPLCONF CLIENT-VERSION starting in P2/P3.
// Deliberately a plain unsigned, NOT a DflyVersion: the enum's value range is 0..7, so
// DflyVersion(65) is UB, and sending 65 today would flip every master-side `>= VER6` gate
// (rdb_save.cc:1782, snapshot.cc:101) against peers that don't support those features.
inline constexpr unsigned kDrakeydbReplVersion = 65;

inline constexpr char kNodeUuidFileName[] = "drakeydb.uuid";

// Returns a canonical lowercase RFC-4122 v4 uuid.
std::string GenerateNodeUuid();

// 36 chars, dashes at 8/13/18/23, hex elsewhere; case-insensitive (KeyDB uuid_parse parity).
bool IsValidNodeUuid(std::string_view uuid);

// Precondition: IsValidNodeUuid(uuid). Returns the lowercase canonical form.
std::string NormalizeNodeUuid(std::string_view uuid);

// Parses the master's REPLCONF UUID success reply: "<uuid>" (KeyDB) or "<uuid> <ms>" (drakeydb).
// Extra tokens are ignored for forward compatibility. Returns false on a malformed reply
// (caller treats that as fatal). *master_ms is 0 when the master sent no clock.
bool ParseReplconfUuidReply(std::string_view reply, std::string* master_uuid, uint64_t* master_ms);

}  // namespace dfly
