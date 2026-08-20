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

}  // namespace dfly
