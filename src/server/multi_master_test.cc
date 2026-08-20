// Copyright 2026, drakeydb authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "server/multi_master.h"

#include <absl/container/flat_hash_set.h>
#include <absl/strings/ascii.h>

#include "base/gtest.h"
#include "server/node_identity.h"

namespace dfly {

TEST(NodeIdentity, VersionConstant) {
  EXPECT_EQ(65u, kDrakeydbReplVersion);
}

TEST(NodeUuid, GenerateIsValidV4) {
  absl::flat_hash_set<std::string> seen;
  for (int i = 0; i < 1000; ++i) {
    std::string u = GenerateNodeUuid();
    ASSERT_TRUE(IsValidNodeUuid(u)) << u;
    ASSERT_EQ(u, absl::AsciiStrToLower(u));
    ASSERT_EQ('4', u[14]);
    ASSERT_TRUE(u[19] == '8' || u[19] == '9' || u[19] == 'a' || u[19] == 'b') << u;
    seen.insert(u);
  }
  EXPECT_EQ(1000u, seen.size());
}

TEST(NodeUuid, ValidateRejectsMalformed) {
  EXPECT_TRUE(IsValidNodeUuid("01234567-89ab-4cde-8f01-23456789abcd"));
  EXPECT_TRUE(IsValidNodeUuid("01234567-89AB-4CDE-8F01-23456789ABCD"));  // mixed case ok
  EXPECT_FALSE(IsValidNodeUuid(""));
  EXPECT_FALSE(IsValidNodeUuid("01234567-89ab-4cde-8f01-23456789abc"));    // 35
  EXPECT_FALSE(IsValidNodeUuid("01234567-89ab-4cde-8f01-23456789abcde"));  // 37
  EXPECT_FALSE(IsValidNodeUuid("012345678-9ab-4cde-8f01-23456789abcd"));   // dash misplaced
  EXPECT_FALSE(IsValidNodeUuid("0123456g-89ab-4cde-8f01-23456789abcd"));   // non-hex
  EXPECT_EQ("01234567-89ab-4cde-8f01-23456789abcd",
            NormalizeNodeUuid("01234567-89AB-4CDE-8F01-23456789ABCD"));
}

TEST(ReplconfUuidReply, ParsesKeydbBareUuid) {
  std::string uuid;
  uint64_t ms = 12345;
  ASSERT_TRUE(ParseReplconfUuidReply("01234567-89ab-4cde-8f01-23456789abcd", &uuid, &ms));
  EXPECT_EQ("01234567-89ab-4cde-8f01-23456789abcd", uuid);
  EXPECT_EQ(0u, ms);  // KeyDB sends no clock
}

TEST(ReplconfUuidReply, ParsesDrakeydbUuidWithMs) {
  std::string uuid;
  uint64_t ms = 0;
  ASSERT_TRUE(
      ParseReplconfUuidReply("01234567-89AB-4cde-8f01-23456789abcd 1755600000000", &uuid, &ms));
  EXPECT_EQ("01234567-89ab-4cde-8f01-23456789abcd", uuid);  // normalized
  EXPECT_EQ(1755600000000u, ms);
}

TEST(ReplconfUuidReply, MalformedRejectedExtraTokensIgnored) {
  std::string uuid;
  uint64_t ms;
  EXPECT_FALSE(ParseReplconfUuidReply("", &uuid, &ms));
  EXPECT_FALSE(ParseReplconfUuidReply("OK", &uuid, &ms));
  EXPECT_FALSE(ParseReplconfUuidReply("not-a-uuid 123", &uuid, &ms));
  EXPECT_FALSE(ParseReplconfUuidReply("01234567-89ab-4cde-8f01-23456789abcd notanum", &uuid, &ms));
  // Forward compat: a third token from a future master is ignored.
  EXPECT_TRUE(ParseReplconfUuidReply("01234567-89ab-4cde-8f01-23456789abcd 5 future", &uuid, &ms));
  EXPECT_EQ(5u, ms);
}

}  // namespace dfly
