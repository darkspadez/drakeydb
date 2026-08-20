// Copyright 2026, drakeydb authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "server/node_identity.h"

#include <absl/random/random.h>
#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>

#include "base/logging.h"
#include "core/detail/gen_utils.h"

namespace dfly {

std::string GenerateNodeUuid() {
  absl::InsecureBitGen gen;
  std::string hex = GetRandomHex(gen, 32);
  hex[12] = '4';                                 // version 4
  hex[16] = "89ab"[absl::Uniform(gen, 0u, 4u)];  // RFC-4122 variant
  return absl::StrCat(hex.substr(0, 8), "-", hex.substr(8, 4), "-", hex.substr(12, 4), "-",
                      hex.substr(16, 4), "-", hex.substr(20, 12));
}

bool IsValidNodeUuid(std::string_view uuid) {
  if (uuid.size() != 36)
    return false;
  for (size_t i = 0; i < uuid.size(); ++i) {
    if (i == 8 || i == 13 || i == 18 || i == 23) {
      if (uuid[i] != '-')
        return false;
    } else if (!absl::ascii_isxdigit(static_cast<unsigned char>(uuid[i]))) {
      return false;
    }
  }
  return true;
}

std::string NormalizeNodeUuid(std::string_view uuid) {
  DCHECK(IsValidNodeUuid(uuid));
  return absl::AsciiStrToLower(uuid);
}

}  // namespace dfly
