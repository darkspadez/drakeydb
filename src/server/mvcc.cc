// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/mvcc.h"

#include <xxhash.h>

namespace dfly {

namespace {
// Distinct from LockTag::Fingerprint's seed (tx_base.cc:100) so the two hash spaces cannot be
// confused in a debugger or a log.
constexpr uint64_t kOriginHashSeed = 0x9E3779B97F4A7C15ULL;
}  // namespace

uint64_t NodeUuidHash(std::string_view uuid) {
  return XXH64(uuid.data(), uuid.size(), kOriginHashSeed);
}

}  // namespace dfly
