// Copyright 2026, drakeydb authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "server/multi_master.h"

#include "base/gtest.h"
#include "server/node_identity.h"

namespace dfly {

TEST(NodeIdentity, VersionConstant) {
  EXPECT_EQ(65u, kDrakeydbReplVersion);
}

}  // namespace dfly
