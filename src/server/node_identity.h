// Copyright 2026, drakeydb authors.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "io/io.h"

namespace dfly {

// Fork replication protocol version, sent via REPLCONF DRAKEY-VERSION (replica.cc's
// ConfigureDflyMaster, P3 T6) -- never via REPLCONF CLIENT-VERSION, which stays at
// DflyVersion::CURRENT_VER. Deliberately a plain unsigned, NOT a DflyVersion: the enum's value
// range is 0..7, so DflyVersion(65) is UB, and sending 65 via CLIENT-VERSION would flip every
// master-side `>= VER6` gate (rdb_save.cc:1782, snapshot.cc:101) against peers that don't
// support those features.
inline constexpr unsigned kDrakeydbReplVersion = 65;

inline constexpr char kNodeUuidFileName[] = "drakeydb.uuid";

// Returns a canonical lowercase RFC-4122 v4 uuid.
std::string GenerateNodeUuid();

// 36 chars, dashes at 8/13/18/23, hex elsewhere; case-insensitive (KeyDB uuid_parse parity).
bool IsValidNodeUuid(std::string_view uuid);

// Precondition: IsValidNodeUuid(uuid). Returns the lowercase canonical form.
std::string NormalizeNodeUuid(std::string_view uuid);

enum class ReplconfUuidReplyStatus : uint8_t {
  kSuccess,
  kUnsupported,
  kMalformed,
};

// Parses the master's REPLCONF UUID string reply: "<uuid>" (KeyDB), "<uuid> <ms>" (drakeydb),
// or exact "OK" from a master that ignores unsupported REPLCONF options. Extra tokens after the
// clock are ignored for forward compatibility. On success, *master_uuid is the normalized uuid
// and *master_ms is 0 when the master sent no clock. On any other status, neither out-param is
// modified.
ReplconfUuidReplyStatus ParseReplconfUuidReply(std::string_view reply, std::string* master_uuid,
                                               uint64_t* master_ms);

// The node's persistent identity, as loaded or created for this boot.
struct NodeIdentity {
  std::string uuid;
  // True when `uuid` is not backed by the identity file: it came from a --node_uuid override,
  // `dir` is a cloud path, or the file could not be persisted. An ephemeral identity does not
  // survive a restart.
  bool ephemeral = false;
};

// Loads the node's uuid from `<dir>/kNodeUuidFileName`, or generates and persists a new one on
// first boot. `override_uuid`, when non-empty, must already be a valid uuid; it is normalized
// and returned as an ephemeral identity without touching the file. Returns an error for an
// invalid override, a corrupt identity file, or any file read error other than "missing".
io::Result<NodeIdentity> LoadOrCreateNodeIdentity(std::string_view dir,
                                                  std::string_view override_uuid);

// Convenience wrapper for ServerFamily::Init(): resolves the --node_uuid override flag and
// exits the process on failure.
NodeIdentity InitNodeIdentityOrExit(std::string_view dir);

}  // namespace dfly
