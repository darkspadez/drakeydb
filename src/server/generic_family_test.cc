// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/generic_family.h"

#include <absl/cleanup/cleanup.h>

extern "C" {
#include "redis/rdb.h"
}

#include "base/flags.h"
#include "base/gtest.h"
#include "base/logging.h"
#include "facade/facade_test.h"
#include "server/channel_store.h"
#include "server/conn_context.h"
#include "server/container_utils.h"
#include "server/engine_shard_set.h"
#include "server/test_utils.h"
#include "server/transaction.h"

ABSL_DECLARE_FLAG(bool, multi_exec_squash);
ABSL_DECLARE_FLAG(uint32_t, container_iteration_yield_interval_usec);
ABSL_DECLARE_FLAG(std::string, notify_keyspace_events);

using namespace testing;
using namespace std;
using namespace util;
using absl::StrCat;

namespace dfly {

class GenericFamilyTest : public BaseFamilyTest {};

TEST_F(GenericFamilyTest, Expire) {
  Run({"set", "key", "val"});

  // sideqik expiry limit
  auto resp = Run({"expire", "key", absl::StrCat(5 * 365 * 24 * 3600)});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"expire", "key", "1"});
  EXPECT_THAT(resp, IntArg(1));
  AdvanceTime(1000);
  resp = Run({"get", "key"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));

  Run({"set", "key", "val"});
  resp = Run({"pexpireat", "key", absl::StrCat(TEST_current_time_ms + 2000)});
  EXPECT_THAT(resp, IntArg(1));

  // override
  resp = Run({"pexpireat", "key", absl::StrCat(TEST_current_time_ms + 3000)});
  EXPECT_THAT(resp, IntArg(1));

  AdvanceTime(2999);
  resp = Run({"get", "key"});
  EXPECT_THAT(resp, "val");

  AdvanceTime(1);
  resp = Run({"get", "key"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));

  // pexpire test
  Run({"set", "key", "val"});
  resp = Run({"pexpire", "key", absl::StrCat(2000)});
  EXPECT_THAT(resp, IntArg(1));

  // expire time override
  resp = Run({"pexpire", "key", absl::StrCat(3000)});
  EXPECT_THAT(resp, IntArg(1));

  AdvanceTime(2999);
  resp = Run({"get", "key"});
  EXPECT_THAT(resp, "val");

  AdvanceTime(1);
  resp = Run({"get", "key"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));
}

TEST_F(GenericFamilyTest, ExpireCornerCases) {
  // EXPIRE / PEXPIRE with non-positive TTL deletes the key immediately and reports success.
  Run({"set", "key", "val"});
  EXPECT_THAT(Run({"expire", "key", "-1"}), IntArg(1));
  EXPECT_THAT(Run({"exists", "key"}), IntArg(0));

  Run({"set", "key", "val"});
  EXPECT_THAT(Run({"expire", "key", "0"}), IntArg(1));
  EXPECT_THAT(Run({"exists", "key"}), IntArg(0));

  Run({"set", "key", "val"});
  EXPECT_THAT(Run({"expire", "key", "-100"}), IntArg(1));
  EXPECT_THAT(Run({"exists", "key"}), IntArg(0));

  Run({"set", "key", "val"});
  EXPECT_THAT(Run({"pexpire", "key", "-1"}), IntArg(1));
  EXPECT_THAT(Run({"exists", "key"}), IntArg(0));

  Run({"set", "key", "val"});
  EXPECT_THAT(Run({"pexpire", "key", "0"}), IntArg(1));
  EXPECT_THAT(Run({"exists", "key"}), IntArg(0));

  // EXPIREAT / PEXPIREAT with a past absolute timestamp (including 0 and negatives) deletes
  // the key.
  Run({"set", "key", "val"});
  EXPECT_THAT(Run({"expireat", "key", "0"}), IntArg(1));
  EXPECT_THAT(Run({"exists", "key"}), IntArg(0));

  Run({"set", "key", "val"});
  EXPECT_THAT(Run({"expireat", "key", "-100"}), IntArg(1));
  EXPECT_THAT(Run({"exists", "key"}), IntArg(0));

  Run({"set", "key", "val"});
  EXPECT_THAT(Run({"pexpireat", "key", "0"}), IntArg(1));
  EXPECT_THAT(Run({"exists", "key"}), IntArg(0));

  Run({"set", "key", "val"});
  EXPECT_THAT(Run({"pexpireat", "key", "-1"}), IntArg(1));
  EXPECT_THAT(Run({"exists", "key"}), IntArg(0));

  // Huge absolute timestamps overflow the kMaxExpireDeadlineMs cap and surface OUT_OF_RANGE.
  Run({"set", "key", "val"});
  EXPECT_THAT(Run({"expireat", "key", absl::StrCat(INT64_MAX)}), ErrArg("expiry is out of range"));
  EXPECT_THAT(Run({"exists", "key"}), IntArg(1));

  EXPECT_THAT(Run({"pexpireat", "key", absl::StrCat(INT64_MAX)}), ErrArg("expiry is out of range"));
  EXPECT_THAT(Run({"exists", "key"}), IntArg(1));

  // Huge relative TTLs are silently capped to kMaxExpireDeadlineSec (~8.5 years).
  EXPECT_THAT(Run({"expire", "key", "99999999999"}), IntArg(1));
  EXPECT_EQ(CheckedInt({"ttl", "key"}), kMaxExpireDeadlineSec);

  EXPECT_THAT(Run({"pexpire", "key", absl::StrCat(int64_t{kMaxExpireDeadlineMs} * 10)}), IntArg(1));
  EXPECT_EQ(CheckedInt({"pttl", "key"}), kMaxExpireDeadlineMs);

  // Expire commands on a missing key return 0 regardless of the TTL value.
  EXPECT_THAT(Run({"del", "missing"}), IntArg(0));
  EXPECT_THAT(Run({"expire", "missing", "5"}), IntArg(0));
  EXPECT_THAT(Run({"expire", "missing", "-1"}), IntArg(0));
  EXPECT_THAT(Run({"expireat", "missing", "0"}), IntArg(0));
  EXPECT_THAT(Run({"pexpireat", "missing", "0"}), IntArg(0));
}

TEST_F(GenericFamilyTest, ExpireOptions) {
  // NX and XX are mutually exclusive
  Run({"set", "key", "val"});
  auto resp = Run({"expire", "key", "3600", "NX", "XX"});
  ASSERT_THAT(resp, ErrArg("NX and XX options at the same time are not compatible"));

  // GT and LT are mutually exclusive
  resp = Run({"expire", "key", "3600", "GT", "LT"});
  ASSERT_THAT(resp, ErrArg("GT and LT options at the same time are not compatible"));

  // Duplicate flags are tolerated (idempotent), like Redis.
  resp = Run({"expire", "key", "3600", "NX", "NX"});
  ASSERT_THAT(resp, IntArg(1));
  Run({"persist", "key"});

  // Unknown option -> error naming the offending token.
  resp = Run({"expire", "key", "3600", "FOO"});
  ASSERT_THAT(resp, ErrArg("Unsupported option: FOO"));

  // NX option should be added since there is no expiry
  resp = Run({"expire", "key", "3600", "NX"});
  EXPECT_THAT(resp, IntArg(1));
  resp = Run({"ttl", "key"});
  EXPECT_THAT(resp.GetInt(), 3600);

  // running again with NX option, should not change expiry
  resp = Run({"expire", "key", "42", "NX"});
  EXPECT_THAT(resp, IntArg(0));

  // given a key with no expiry
  Run({"set", "key2", "val"});
  resp = Run({"expire", "key2", "404", "XX"});
  // XX does not apply expiry since key has no existing expiry
  EXPECT_THAT(resp, IntArg(0));
  resp = Run({"ttl", "key2"});
  EXPECT_THAT(resp.GetInt(), -1);

  // GT does not apply since key has no "inf" expiry
  resp = Run({"expire", "key2", "404", "GT"});
  EXPECT_THAT(resp, IntArg(0));
  resp = Run({"ttl", "key2"});
  EXPECT_THAT(resp.GetInt(), -1);

  // LT applies
  resp = Run({"expire", "key2", "404", "LT"});
  EXPECT_THAT(resp, IntArg(1));
  resp = Run({"ttl", "key2"});
  EXPECT_THAT(resp.GetInt(), 404);

  Run({"persist", "key"});

  // set expiry to 101
  resp = Run({"expire", "key", "101"});
  EXPECT_THAT(resp, IntArg(1));

  // GT should not apply expiry since new is not greater than the current one
  resp = Run({"expire", "key", "100", "GT"});
  EXPECT_THAT(resp, IntArg(0));
  resp = Run({"ttl", "key"});
  EXPECT_THAT(resp.GetInt(), 101);

  // GT should apply expiry since new is greater than the current one
  resp = Run({"expire", "key", "102", "GT"});
  EXPECT_THAT(resp, IntArg(1));
  resp = Run({"ttl", "key"});
  EXPECT_THAT(resp.GetInt(), 102);

  // GT should not apply since expiry is smaller than current
  resp = Run({"expire", "key", "101", "GT"});
  EXPECT_THAT(resp, IntArg(0));
  resp = Run({"ttl", "key"});
  EXPECT_THAT(resp.GetInt(), 102);

  // LT should apply new expiry is smaller than current
  resp = Run({"expire", "key", "101", "LT"});
  EXPECT_THAT(resp, IntArg(1));
  resp = Run({"ttl", "key"});
  EXPECT_THAT(resp.GetInt(), 101);

  resp = Run({"expire", "key", "102", "LT"});
  EXPECT_THAT(resp, IntArg(0));
  resp = Run({"ttl", "key"});
  EXPECT_THAT(resp.GetInt(), 101);

  // NX with GT, first sets expiry, updates only to larger values
  Run({"persist", "key"});
  Run({"expire", "key", "5", "NX", "GT"});
  EXPECT_THAT(Run({"ttl", "key"}), IntArg(5));

  Run({"expire", "key", "3", "NX", "GT"});
  EXPECT_THAT(Run({"ttl", "key"}), IntArg(5));

  Run({"expire", "key", "7", "NX", "GT"});
  EXPECT_THAT(Run({"ttl", "key"}), IntArg(7));
}

TEST_F(GenericFamilyTest, ExpireAtOptions) {
  auto test_time_ms = TEST_current_time_ms;
  auto time_s = (test_time_ms + 500) / 1000;
  auto test_time_s = time_s;

  Run({"set", "key", "val"});
  // NX and XX are mutually exclusive
  auto resp = Run({"expireat", "key", "3600", "NX", "XX"});
  ASSERT_THAT(resp, ErrArg("NX and XX options at the same time are not compatible"));

  // GT and LT are mutually exclusive
  resp = Run({"expireat", "key", "3600", "GT", "LT"});
  ASSERT_THAT(resp, ErrArg("GT and LT options at the same time are not compatible"));

  // NX option should be added since there is no expiry
  test_time_s = time_s + 5;
  resp = Run({"expireat", "key", absl::StrCat(test_time_s), "NX"});
  EXPECT_THAT(resp, IntArg(1));
  EXPECT_EQ(test_time_s, CheckedInt({"EXPIRETIME", "key"}));

  // running again with NX option, should not change expiry
  test_time_s = time_s + 9;
  resp = Run({"expireat", "key", absl::StrCat(test_time_s), "NX"});
  EXPECT_THAT(resp, IntArg(0));

  // NX option with expired time is not accepted and so it doesn't delete the value
  resp = Run({"expireat", "key", absl::StrCat(TEST_current_time_ms / 1000 - 10), "NX"});
  EXPECT_THAT(resp, IntArg(0));
  EXPECT_THAT(Run({"exists", "key"}), IntArg(1));

  // given a key with no expiry
  Run({"set", "key2", "val"});
  test_time_s = time_s + 9;
  resp = Run({"expireat", "key2", absl::StrCat(test_time_s), "XX"});
  // XX does not apply expiry since key has no existing expiry
  EXPECT_THAT(resp, IntArg(0));
  resp = Run({"ttl", "key2"});
  EXPECT_THAT(resp.GetInt(), -1);

  // set expiry to 101
  test_time_s = time_s + 101;
  resp = Run({"expireat", "key", absl::StrCat(test_time_s)});
  EXPECT_THAT(resp, IntArg(1));

  // GT should not apply expiry since new is not greater than the current one
  auto less_test_time_s = time_s + 99;
  resp = Run({"expireat", "key", absl::StrCat(less_test_time_s), "GT"});
  EXPECT_THAT(resp, IntArg(0));
  EXPECT_EQ(test_time_s, CheckedInt({"EXPIRETIME", "key"}));

  // GT should apply expiry since new is greater than the current one
  test_time_s = time_s + 105;
  resp = Run({"expireat", "key", absl::StrCat(test_time_s), "GT"});
  EXPECT_THAT(resp, IntArg(1));
  EXPECT_EQ(test_time_s, CheckedInt({"EXPIRETIME", "key"}));

  // LT should apply new expiry is smaller than current
  test_time_s = time_s + 101;
  resp = Run({"expireat", "key", absl::StrCat(test_time_s), "LT"});
  EXPECT_THAT(resp, IntArg(1));
  EXPECT_EQ(test_time_s, CheckedInt({"EXPIRETIME", "key"}));

  // LT should not apply expiry since new is not lesser than the current one
  auto gt_test_time_s = time_s + 102;
  resp = Run({"expireat", "key", absl::StrCat(gt_test_time_s), "LT"});
  EXPECT_THAT(resp, IntArg(0));
  EXPECT_EQ(test_time_s, CheckedInt({"EXPIRETIME", "key"}));
}

TEST_F(GenericFamilyTest, PExpireOptions) {
  // NX and XX are mutually exclusive
  Run({"set", "key", "val"});
  auto resp = Run({"pexpire", "key", "3600", "NX", "XX"});
  ASSERT_THAT(resp, ErrArg("NX and XX options at the same time are not compatible"));

  // GT and LT are mutually exclusive
  resp = Run({"pexpire", "key", "3600", "GT", "LT"});
  ASSERT_THAT(resp, ErrArg("GT and LT options at the same time are not compatible"));

  // NX option should be added since there is no expiry
  resp = Run({"pexpire", "key", "3600000", "NX"});
  EXPECT_THAT(resp, IntArg(1));
  resp = Run({"pttl", "key"});
  EXPECT_THAT(resp.GetInt(), 3600000);

  // running again with NX option, should not change expiry
  resp = Run({"pexpire", "key", "42", "NX"});
  EXPECT_THAT(resp, IntArg(0));

  // given a key with no expiry
  Run({"set", "key2", "val"});
  resp = Run({"pexpire", "key2", "404", "XX"});
  // XX does not apply expiry since key has no existing expiry
  EXPECT_THAT(resp, IntArg(0));
  resp = Run({"pttl", "key2"});
  EXPECT_THAT(resp.GetInt(), -1);

  // set expiry to 101
  resp = Run({"pexpire", "key", "101000"});
  EXPECT_THAT(resp, IntArg(1));

  // GT should not apply expiry since new is not greater than the current one
  resp = Run({"pexpire", "key", "100000", "GT"});
  EXPECT_THAT(resp, IntArg(0));
  resp = Run({"pttl", "key"});
  EXPECT_THAT(resp.GetInt(), 101000);

  // GT should apply expiry since new is greater than the current one
  resp = Run({"pexpire", "key", "102000", "GT"});
  EXPECT_THAT(resp, IntArg(1));
  resp = Run({"pttl", "key"});
  EXPECT_THAT(resp.GetInt(), 102000);

  // GT should not apply since expiry is smaller than current
  resp = Run({"pexpire", "key", "101000", "GT"});
  EXPECT_THAT(resp, IntArg(0));
  resp = Run({"pttl", "key"});
  EXPECT_THAT(resp.GetInt(), 102000);

  // LT should apply new expiry is smaller than current
  resp = Run({"pexpire", "key", "101000", "LT"});
  EXPECT_THAT(resp, IntArg(1));
  resp = Run({"pttl", "key"});
  EXPECT_THAT(resp.GetInt(), 101000);

  // LT should not apply since expiry is greater than current
  resp = Run({"pexpire", "key", "102000", "LT"});
  EXPECT_THAT(resp, IntArg(0));
  resp = Run({"pttl", "key"});
  EXPECT_THAT(resp.GetInt(), 101000);
}

TEST_F(GenericFamilyTest, PExpireAtOptions) {
  auto test_time_ms = TEST_current_time_ms;
  Run({"set", "key", "val"});
  // NX and XX are mutually exclusive
  auto resp = Run({"pexpireat", "key", "3600", "NX", "XX"});
  ASSERT_THAT(resp, ErrArg("NX and XX options at the same time are not compatible"));

  // GT and LT are mutually exclusive
  resp = Run({"pexpireat", "key", "3600", "GT", "LT"});
  ASSERT_THAT(resp, ErrArg("GT and LT options at the same time are not compatible"));

  // NX option should be added since there is no expiry
  test_time_ms = TEST_current_time_ms + 3600;
  resp = Run({"pexpireat", "key", absl::StrCat(test_time_ms), "NX"});
  EXPECT_THAT(resp, IntArg(1));
  EXPECT_EQ(test_time_ms, CheckedInt({"PEXPIRETIME", "key"}));

  // running again with NX option, should not change expiry
  test_time_ms = TEST_current_time_ms + 42000;
  resp = Run({"pexpireat", "key", absl::StrCat(test_time_ms), "NX"});
  EXPECT_THAT(resp, IntArg(0));

  // given a key with no expiry
  Run({"set", "key2", "val"});
  test_time_ms = TEST_current_time_ms + 404;
  resp = Run({"pexpireat", "key2", absl::StrCat(test_time_ms), "XX"});
  // XX does not apply expiry since key has no existing expiry
  EXPECT_THAT(resp, IntArg(0));
  resp = Run({"ttl", "key2"});
  EXPECT_THAT(resp.GetInt(), -1);

  // set expiry to 101
  test_time_ms = TEST_current_time_ms + 101;
  resp = Run({"pexpireat", "key", absl::StrCat(test_time_ms)});
  EXPECT_THAT(resp, IntArg(1));

  // GT should not apply expiry since new is not greater than the current one
  auto less_test_time_ms = TEST_current_time_ms + 100;
  resp = Run({"pexpireat", "key", absl::StrCat(less_test_time_ms), "GT"});
  EXPECT_THAT(resp, IntArg(0));
  EXPECT_EQ(test_time_ms, CheckedInt({"PEXPIRETIME", "key"}));

  // GT should apply expiry since new is greater than the current one
  test_time_ms = TEST_current_time_ms + 105;
  resp = Run({"pexpireat", "key", absl::StrCat(test_time_ms), "GT"});
  EXPECT_THAT(resp, IntArg(1));
  EXPECT_EQ(test_time_ms, CheckedInt({"PEXPIRETIME", "key"}));

  // LT should apply new expiry is smaller than current
  test_time_ms = TEST_current_time_ms + 101;
  resp = Run({"pexpireat", "key", absl::StrCat(test_time_ms), "LT"});
  EXPECT_THAT(resp, IntArg(1));
  EXPECT_EQ(test_time_ms, CheckedInt({"PEXPIRETIME", "key"}));

  // LT should not apply expiry since new is not lesser than the current one
  auto gt_test_time_ms = TEST_current_time_ms + 102;
  resp = Run({"pexpireat", "key", absl::StrCat(gt_test_time_ms), "LT"});
  EXPECT_THAT(resp, IntArg(0));
  EXPECT_EQ(test_time_ms, CheckedInt({"PEXPIRETIME", "key"}));
}

TEST_F(GenericFamilyTest, Del) {
  for (size_t i = 0; i < 1000; ++i) {
    Run({"set", StrCat("foo", i), "1"});
    Run({"set", StrCat("bar", i), "1"});
  }

  ASSERT_EQ(2000, CheckedInt({"dbsize"}));

  auto exist_fb = pp_->at(0)->LaunchFiber([&] {
    for (size_t i = 0; i < 1000; ++i) {
      int64_t resp = CheckedInt({"exists", StrCat("foo", i), StrCat("bar", i)});
      ASSERT_TRUE(2 == resp || resp == 0) << resp << " " << i;
    }
  });

  auto del_fb = pp_->at(2)->LaunchFiber([&] {
    for (size_t i = 0; i < 1000; ++i) {
      auto resp = CheckedInt({"del", StrCat("foo", i), StrCat("bar", i)});
      ASSERT_EQ(2, resp);
    }
  });

  exist_fb.Join();
  del_fb.Join();

  Run({"setex", "k1", "10", "bar"});
  Run({"del", "k1"});
}

TEST_F(GenericFamilyTest, TTL) {
  EXPECT_EQ(-2, CheckedInt({"ttl", "foo"}));
  EXPECT_EQ(-2, CheckedInt({"pttl", "foo"}));
  Run({"set", "foo", "bar"});
  EXPECT_EQ(-1, CheckedInt({"ttl", "foo"}));
  EXPECT_EQ(-1, CheckedInt({"pttl", "foo"}));
}

TEST_F(GenericFamilyTest, Exists) {
  Run({"mset", "x", "0", "y", "1"});
  auto resp = Run({"exists", "x", "y", "x"});
  EXPECT_THAT(resp, IntArg(3));
}

TEST_F(GenericFamilyTest, Touch) {
  RespExpr resp;

  Run({"mset", "x", "0", "y", "1"});
  resp = Run({"touch", "x", "y", "x"});
  EXPECT_THAT(resp, IntArg(3));

  resp = Run({"touch", "z", "x", "w"});
  EXPECT_THAT(resp, IntArg(1));
}

TEST_F(GenericFamilyTest, Rename) {
  RespExpr resp;
  string b_val(32, 'b');
  string x_val(32, 'x');

  resp = Run({"mset", "x", x_val, "b", b_val});
  ASSERT_EQ(resp, "OK");
  ASSERT_EQ(2, last_cmd_dbg_info_.shards_count);

  resp = Run({"rename", "z", "b"});
  ASSERT_THAT(resp, ErrArg("no such key"));

  resp = Run({"rename", "x", "b"});
  ASSERT_EQ(resp, "OK");

  int64_t val = CheckedInt({"get", "x"});
  ASSERT_EQ(kint64min, val);  // does not exist

  ASSERT_EQ(x_val, Run({"get", "b"}));  // swapped.

  EXPECT_EQ(CheckedInt({"exists", "x", "b"}), 1);

  const char* keys[2] = {"b", "x"};
  auto ren_fb = pp_->at(0)->LaunchFiber([&] {
    for (size_t i = 0; i < 200; ++i) {
      int j = i % 2;
      auto resp = Run({"rename", keys[j], keys[1 - j]});
      ASSERT_EQ(resp, "OK");
    }
  });

  auto exist_fb = pp_->at(2)->LaunchFiber([&] {
    for (size_t i = 0; i < 300; ++i) {
      int64_t resp = CheckedInt({"exists", "x", "b"});
      ASSERT_EQ(1, resp);
    }
  });

  exist_fb.Join();
  ren_fb.Join();
}

TEST_F(GenericFamilyTest, RenameList) {
  for (string_view dest : {"b", "y", "z"}) {
    EXPECT_EQ(1, CheckedInt({"lpush", "x", "elem"}));
    Metrics metrics = GetMetrics();

    size_t list_usage = metrics.db_stats[0].memory_usage_by_type[OBJ_LIST];
    size_t string_usage = metrics.db_stats[0].memory_usage_by_type[OBJ_STRING];
    ASSERT_GT(list_usage, 0);
    ASSERT_EQ(string_usage, 0);

    auto resp = Run({"rename", "x", dest});
    ASSERT_EQ(resp, "OK");
    if (dest == "b") {
      ASSERT_EQ(2, last_cmd_dbg_info_.shards_count);
    } else {
      ASSERT_EQ(1, last_cmd_dbg_info_.shards_count);
    }

    metrics = GetMetrics();
    size_t list_usage_after = metrics.db_stats[0].memory_usage_by_type[OBJ_LIST];
    string_usage = metrics.db_stats[0].memory_usage_by_type[OBJ_STRING];
    ASSERT_EQ(list_usage_after, list_usage);
    ASSERT_EQ(string_usage, 0);

    EXPECT_EQ(0, CheckedInt({"del", "x"}));
    EXPECT_EQ(1, CheckedInt({"del", dest}));
  }
}

TEST_F(GenericFamilyTest, RenameBinary) {
  const char kKey1[] = "\x01\x02\x03\x04";
  const char kKey2[] = "\x05\x06\x07\x08";

  Run({"set", kKey1, "bar"});
  Run({"rename", kKey1, kKey2});
  EXPECT_THAT(Run({"get", kKey1}), ArgType(RespExpr::NIL));
  EXPECT_EQ(Run({"get", kKey2}), "bar");
}

TEST_F(GenericFamilyTest, RenameNx) {
  // Set two keys
  string b_val(32, 'b');
  string x_val(32, 'x');
  Run({"mset", "x", x_val, "b", b_val});

  ASSERT_THAT(Run({"renamenx", "z", "b"}), ErrArg("no such key"));
  ASSERT_THAT(Run({"renamenx", "x", "b"}), IntArg(0));  // b already exists
  ASSERT_THAT(Run({"renamenx", "x", "y"}), IntArg(1));
  ASSERT_EQ(Run({"get", "y"}), x_val);
  ASSERT_THAT(Run({"renamenx", "y", "y"}), IntArg(0));
}

TEST_F(GenericFamilyTest, RenameSameName) {
  const char kKey[] = "key";

  ASSERT_THAT(Run({"rename", kKey, kKey}), ErrArg("no such key"));

  ASSERT_EQ(Run({"set", kKey, "value"}), "OK");
  EXPECT_EQ(Run({"rename", kKey, kKey}), "OK");
}

TEST_F(GenericFamilyTest, RenameSameShard) {
  num_threads_ = 1;
  ResetService();

  ASSERT_EQ(Run({"set", "x", "value"}), "OK");
  ASSERT_EQ(Run({"set", "y", "value"}), "OK");
  EXPECT_EQ(Run({"rename", "x", "y"}), "OK");
}

TEST_F(GenericFamilyTest, RenameCmsNanCrash) {
  num_threads_ = 2;
  ResetService();

  // With 2 shards (XXH64 seed 120577240643): myset -> shard 0, dst -> shard 1.
  // NaN must be rejected: !(NaN > 0 && NaN < 1) is true for both checks.
  // Before the fix, NaN bypassed <= 0 / >= 1 guards (NaN comparisons are always
  // false), creating a CMS with width=0 and depth=0.  The subsequent cross-shard
  // RENAME triggered Renamer::FinalizeRename -> DeserializeDest -> ReadCMS,
  // which rejected width==0 -> INVALID_VALUE -> DFATAL (SIGABRT in debug builds).
  EXPECT_THAT(Run({"cms.initbyprob", "myset", "NaN", "NaN"}), ErrArg("between 0 and 1"));
  EXPECT_THAT(Run({"exists", "myset"}), IntArg(0));
  EXPECT_THAT(Run({"rename", "myset", "dst"}), ErrArg("no such key"));
}

TEST_F(GenericFamilyTest, Stick) {
  // check stick returns zero on non-existent keys
  ASSERT_THAT(Run({"stick", "a", "b"}), IntArg(0));

  for (auto key : {"a", "b", "c", "d"}) {
    Run({"set", key, "."});
  }

  // check stick is applied only once
  ASSERT_THAT(Run({"stick", "a", "b"}), IntArg(2));
  ASSERT_THAT(Run({"stick", "a", "b"}), IntArg(0));
  ASSERT_THAT(Run({"stick", "a", "c"}), IntArg(1));
  ASSERT_THAT(Run({"stick", "b", "d"}), IntArg(1));
  ASSERT_THAT(Run({"stick", "c", "d"}), IntArg(0));

  // check stickyness persists during writes
  Run({"set", "a", "new"});
  ASSERT_THAT(Run({"stick", "a"}), IntArg(0));
  Run({"append", "a", "-value"});
  ASSERT_THAT(Run({"stick", "a"}), IntArg(0));

  // check rename persists stickyness
  Run({"rename", "a", "k"});
  ASSERT_THAT(Run({"stick", "k"}), IntArg(0));

  // check rename persists stickyness on multiple shards
  Run({"del", "b"});
  string b_val(32, 'b');
  string x_val(32, 'x');
  Run({"mset", "b", b_val, "x", x_val});
  ASSERT_EQ(2, last_cmd_dbg_info_.shards_count);
  Run({"stick", "x"});
  Run({"rename", "x", "b"});
  ASSERT_THAT(Run({"stick", "b"}), IntArg(0));
}

TEST_F(GenericFamilyTest, Move) {
  // Check MOVE returns 0 on non-existent keys
  ASSERT_THAT(Run({"move", "a", "1"}), IntArg(0));

  // Check MOVE catches non-existent database indices
  ASSERT_THAT(Run({"move", "a", "-1"}), ArgType(RespExpr::ERROR));
  ASSERT_THAT(Run({"move", "a", "100500"}), ArgType(RespExpr::ERROR));

  // Check MOVE moves value & expiry & stickyness
  Run({"set", "a", "test"});
  Run({"expire", "a", "1000"});
  Run({"stick", "a"});
  ASSERT_THAT(Run({"move", "a", "1"}), IntArg(1));
  Run({"select", "1"});
  ASSERT_THAT(Run({"get", "a"}), "test");
  ASSERT_THAT(Run({"ttl", "a"}), testing::Not(IntArg(-1)));
  ASSERT_THAT(Run({"stick", "a"}), IntArg(0));

  // Check MOVE doesn't move if key exists
  Run({"select", "1"});
  Run({"set", "a", "test"});
  Run({"select", "0"});
  Run({"set", "a", "another test"});
  ASSERT_THAT(Run({"move", "a", "1"}), IntArg(0));  // exists from test case above
  Run({"select", "1"});
  ASSERT_THAT(Run({"get", "a"}), "test");

  // Check MOVE awakes blocking operations
  auto fb_blpop = pp_->at(0)->LaunchFiber(Launch::dispatch, [&] {
    Run({"select", "1"});
    auto resp = Run({"blpop", "l", "0"});
    ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
    EXPECT_THAT(resp.GetVec(), ElementsAre("l", "TestItem"));
  });

  WaitUntilLocked(1, "l");

  pp_->at(1)->Await([&] {
    Run({"select", "0"});
    Run({"lpush", "l", "TestItem"});
    Run({"move", "l", "1"});
  });

  fb_blpop.Join();
}

TEST_F(GenericFamilyTest, MoveUpdatesMemoryAccounting) {
  EXPECT_EQ(1, CheckedInt({"lpush", "list", "elem"}));

  Metrics metrics = GetMetrics();
  size_t list_usage = metrics.db_stats[0].memory_usage_by_type[OBJ_LIST];
  ASSERT_GT(list_usage, 0);

  EXPECT_THAT(Run({"move", "list", "1"}), IntArg(1));

  metrics = GetMetrics();
  EXPECT_EQ(metrics.db_stats[0].memory_usage_by_type[OBJ_LIST], 0u);
  EXPECT_EQ(metrics.db_stats[1].memory_usage_by_type[OBJ_LIST], list_usage);
}

using testing::AnyOf;
using testing::Each;
using testing::StartsWith;

TEST_F(GenericFamilyTest, Scan) {
  for (unsigned i = 0; i < 10; ++i)
    Run({"set", absl::StrCat("key", i), "bar"});

  for (unsigned i = 0; i < 10; ++i)
    Run({"set", absl::StrCat("str", i), "bar"});

  for (unsigned i = 0; i < 10; ++i)
    Run({"sadd", absl::StrCat("set", i), "bar"});

  for (unsigned i = 0; i < 10; ++i)
    Run({"zadd", absl::StrCat("zset", i), "0", "bar"});

  auto resp = Run({"scan", "0", "count", "20", "type", "string"});
  EXPECT_THAT(resp, ArrLen(2));
  auto vec = StrArray(resp.GetVec()[1]);
  EXPECT_GT(vec.size(), 10);
  EXPECT_THAT(vec, Each(AnyOf(StartsWith("str"), StartsWith("key"))));

  resp = Run({"scan", "0", "count", "20", "match", "zset*"});
  vec = StrArray(resp.GetVec()[1]);
  EXPECT_EQ(10, vec.size());
  EXPECT_THAT(vec, Each(StartsWith("zset")));

  EXPECT_THAT(Run({"scan", "0", "count"}), ErrArg("syntax error"));
  EXPECT_THAT(Run({"scan", "0", "count", "not-a-number"}), ErrArg("value is not an integer"));
  EXPECT_THAT(Run({"scan", "0", "type", "not-a-type"}), ErrArg("syntax error"));
  EXPECT_THAT(Run({"scan", "0", "NOVALUES"}), ErrArg("syntax error"));

  // COUNT is a size_t hint: values above UINT32_MAX must still parse.
  resp = Run({"scan", "0", "count", "5000000000"});
  EXPECT_THAT(resp, ArrLen(2));

  Run({"flushdb"});

  Run({"set", "", "foo"});
  Run({"set", "bar", "1"});
  resp = Run({"keys", "*"});
  EXPECT_THAT(resp, RespArray(ElementsAre("bar", "")));
  resp = Run({"keys", ""});
  EXPECT_THAT(resp, RespElementsAre(""));
}

TEST_F(GenericFamilyTest, ScanWithAttr) {
  Run({"set", "hello", "world"});
  Run({"set", "foo", "bar"});

  Run({"expire", "hello", "1000"});

  auto resp = Run({"scan", "0", "attr", "v"});
  auto vec = StrArray(resp.GetVec()[1]);
  ASSERT_EQ(1, vec.size());
  EXPECT_EQ(vec[0], "hello");

  resp = Run({"scan", "0", "attr", "p"});
  vec = StrArray(resp.GetVec()[1]);
  ASSERT_EQ(1, vec.size());
  EXPECT_EQ(vec[0], "foo");

  // before run get "foo", scan with a attr should return "hello", because set "hello" expire before
  resp = Run({"scan", "0", "attr", "a"});
  vec = StrArray(resp.GetVec()[1]);
  ASSERT_EQ(1, vec.size());
  EXPECT_EQ(vec[0], "hello");

  // before run get "foo", scan with a attr should return "foo"
  resp = Run({"scan", "0", "attr", "u"});
  vec = StrArray(resp.GetVec()[1]);
  ASSERT_EQ(1, vec.size());
  EXPECT_EQ(vec[0], "foo");

  ASSERT_THAT(Run({"get", "foo"}), "bar");

  // after run get "foo", scan with a attr should return "foo" and "hello"
  resp = Run({"scan", "0", "attr", "a"});
  vec = StrArray(resp.GetVec()[1]);
  ASSERT_EQ(2, vec.size());

  // after run get "foo", scan with a attr should return empty set
  resp = Run({"scan", "0", "attr", "u"});
  vec = StrArray(resp.GetVec()[1]);
  ASSERT_EQ(0, vec.size());
}

TEST_F(GenericFamilyTest, ScanMallocSize) {
  Run({"set", "k1", string(1000, 'a')});
  Run({"set", "k2", string(500, 'b')});
  Run({"set", "k3", string(15, 'c')});

  auto resp = Run({"scan", "0", "MINMSZ", "15"});
  EXPECT_THAT(resp.GetVec()[1], RespArray(UnorderedElementsAre("k1", "k2")));
  resp = Run({"scan", "0", "MINMSZ", "500"});
  EXPECT_THAT(resp.GetVec()[1], RespArray(UnorderedElementsAre("k1")));
}

TEST_F(GenericFamilyTest, Sort) {
  // Test list sort with params
  Run({"del", "list-1"});
  Run({"lpush", "list-1", "3.5", "1.2", "10.1", "2.20", "200"});
  // numeric
  ASSERT_THAT(Run({"sort", "list-1"}).GetVec(), ElementsAre("1.2", "2.20", "3.5", "10.1", "200"));
  // string
  ASSERT_THAT(Run({"sort", "list-1", "ALPHA"}).GetVec(),
              ElementsAre("1.2", "10.1", "2.20", "200", "3.5"));
  // desc numeric
  ASSERT_THAT(Run({"sort", "list-1", "DESC"}).GetVec(),
              ElementsAre("200", "10.1", "3.5", "2.20", "1.2"));
  // desc strig
  ASSERT_THAT(Run({"sort", "list-1", "DESC", "ALPHA"}).GetVec(),
              ElementsAre("3.5", "200", "2.20", "10.1", "1.2"));
  // ASC/DESC are not mutually exclusive — last one wins (matches Redis behavior).
  ASSERT_THAT(Run({"sort", "list-1", "DESC", "ASC"}).GetVec(),
              ElementsAre("1.2", "2.20", "3.5", "10.1", "200"));
  ASSERT_THAT(Run({"sort", "list-1", "ASC", "DESC"}).GetVec(),
              ElementsAre("200", "10.1", "3.5", "2.20", "1.2"));
  // limits
  ASSERT_THAT(Run({"sort", "list-1", "LIMIT", "0", "5"}).GetVec(),
              ElementsAre("1.2", "2.20", "3.5", "10.1", "200"));
  ASSERT_THAT(Run({"sort", "list-1", "LIMIT", "0", "10"}).GetVec(),
              ElementsAre("1.2", "2.20", "3.5", "10.1", "200"));
  ASSERT_THAT(Run({"sort", "list-1", "LIMIT", "2", "2"}).GetVec(), ElementsAre("3.5", "10.1"));
  ASSERT_THAT(Run({"sort", "list-1", "LIMIT", "1", "1"}), RespElementsAre("2.20"));
  ASSERT_THAT(Run({"sort", "list-1", "LIMIT", "4", "2"}), RespElementsAre("200"));
  ASSERT_THAT(Run({"sort", "list-1", "LIMIT", "5", "2"}), ArrLen(0));
  // limits desc
  ASSERT_THAT(Run({"sort", "list-1", "DESC", "LIMIT", "0", "5"}).GetVec(),
              ElementsAre("200", "10.1", "3.5", "2.20", "1.2"));
  ASSERT_THAT(Run({"sort", "list-1", "DESC", "LIMIT", "2", "2"}).GetVec(),
              ElementsAre("3.5", "2.20"));
  ASSERT_THAT(Run({"sort", "list-1", "DESC", "LIMIT", "1", "1"}), RespElementsAre("10.1"));
  ASSERT_THAT(Run({"sort", "list-1", "DESC", "LIMIT", "5", "2"}), ArrLen(0));

  // Test set sort
  Run({"del", "set-1"});
  Run({"sadd", "set-1", "5.3", "4.4", "60", "99.9", "100", "9"});
  ASSERT_THAT(Run({"sort", "set-1"}).GetVec(), ElementsAre("4.4", "5.3", "9", "60", "99.9", "100"));
  ASSERT_THAT(Run({"sort", "set-1", "ALPHA"}).GetVec(),
              ElementsAre("100", "4.4", "5.3", "60", "9", "99.9"));
  ASSERT_THAT(Run({"sort", "set-1", "DESC"}).GetVec(),
              ElementsAre("100", "99.9", "60", "9", "5.3", "4.4"));
  ASSERT_THAT(Run({"sort", "set-1", "DESC", "ALPHA"}).GetVec(),
              ElementsAre("99.9", "9", "60", "5.3", "4.4", "100"));

  // Test intset sort
  Run({"del", "intset-1"});
  Run({"sadd", "intset-1", "5", "4", "3", "2", "1"});
  ASSERT_THAT(Run({"sort", "intset-1"}).GetVec(), ElementsAre("1", "2", "3", "4", "5"));

  // Test sorted set sort
  Run({"del", "zset-1"});
  Run({"zadd", "zset-1", "0", "3.3", "0", "30.1", "0", "8.2"});
  ASSERT_THAT(Run({"sort", "zset-1"}).GetVec(), ElementsAre("3.3", "8.2", "30.1"));
  ASSERT_THAT(Run({"sort", "zset-1", "ALPHA"}).GetVec(), ElementsAre("3.3", "30.1", "8.2"));
  ASSERT_THAT(Run({"sort", "zset-1", "DESC"}).GetVec(), ElementsAre("30.1", "8.2", "3.3"));
  ASSERT_THAT(Run({"sort", "zset-1", "DESC", "ALPHA"}).GetVec(), ElementsAre("8.2", "30.1", "3.3"));

  // Test sort with non existent key
  Run({"del", "list-2"});
  ASSERT_THAT(Run({"sort", "list-2"}), ArrLen(0));

  // Test not convertible to double
  Run({"lpush", "list-2", "NOTADOUBLE"});
  ASSERT_THAT(Run({"sort", "list-2"}), ErrArg("One or more scores can't be converted into double"));

  Run({"set", "foo", "bar"});
  ASSERT_THAT(Run({"sort", "foo"}), ErrArg("WRONGTYPE "));

  Run({"rpush", "list-3", ""});
  ASSERT_THAT(Run({"sort", "list-3"}), RespElementsAre(""));

  Run({"rpush", "list-3", "2", "0", "", "-0.14", "0.12", "-0", "-123123", "7654"});
  ASSERT_THAT(Run({"sort", "list-3"}).GetVec(),
              ElementsAre("-123123", "-0.14", "", "", "-0", "0", "0.12", "2", "7654"));

  Run({"rpush", "NANvalue", "nan"});
  ASSERT_THAT(Run({"sort", "NANvalue"}),
              ErrArg("One or more scores can't be converted into double"));
}

TEST_F(GenericFamilyTest, SortBug3636) {
  Run({"RPUSH", "foo", "1.100000023841858", "1.100000023841858", "1.100000023841858", "-15710",
       "1.100000023841858", "1.100000023841858", "1.100000023841858", "-15710", "-15710",
       "1.100000023841858", "-15710", "-15710", "-15710", "-15710", "1.100000023841858", "-15710",
       "-15710"});
  auto resp = Run({"SORT", "foo", "desc", "alpha"});
  ASSERT_THAT(resp, ArrLen(17));
}

TEST_F(GenericFamilyTest, SortStore) {
  // Test list sort with params
  Run({"del", "list-1"});
  Run({"del", "list-2"});
  Run({"lpush", "list-1", "3.5", "1.2", "10.1", "2.20", "200"});
  // numeric
  auto resp = Run({"sort", "list-1", "store", "list-2"});
  EXPECT_EQ(5, resp.GetInt());
  ASSERT_THAT(Run({"lrange", "list-2", "0", "-1"}).GetVec(),
              ElementsAre("1.2", "2.20", "3.5", "10.1", "200"));

  // string
  resp = Run({"sort", "list-1", "ALPHA", "store", "list-2"});
  EXPECT_EQ(5, resp.GetInt());
  ASSERT_THAT(Run({"lrange", "list-2", "0", "-1"}).GetVec(),
              ElementsAre("1.2", "10.1", "2.20", "200", "3.5"));

  // desc numeric
  resp = Run({"sort", "list-1", "DESC", "store", "list-2"});
  EXPECT_EQ(5, resp.GetInt());
  ASSERT_THAT(Run({"lrange", "list-2", "0", "-1"}).GetVec(),
              ElementsAre("200", "10.1", "3.5", "2.20", "1.2"));

  // desc string
  resp = Run({"sort", "list-1", "ALPHA", "DESC", "store", "list-2"});
  EXPECT_EQ(5, resp.GetInt());
  ASSERT_THAT(Run({"lrange", "list-2", "0", "-1"}).GetVec(),
              ElementsAre("3.5", "200", "2.20", "10.1", "1.2"));

  // limits
  resp = Run({"sort", "list-1", "LIMIT", "0", "5", "store", "list-2"});
  EXPECT_EQ(5, resp.GetInt());
  ASSERT_THAT(Run({"lrange", "list-2", "0", "-1"}).GetVec(),
              ElementsAre("1.2", "2.20", "3.5", "10.1", "200"));
  resp = Run({"sort", "list-1", "LIMIT", "0", "10", "store", "list-2"});
  EXPECT_EQ(5, resp.GetInt());
  ASSERT_THAT(Run({"lrange", "list-2", "0", "-1"}).GetVec(),
              ElementsAre("1.2", "2.20", "3.5", "10.1", "200"));
  resp = Run({"sort", "list-1", "LIMIT", "2", "2", "store", "list-2"});
  EXPECT_EQ(2, resp.GetInt());
  ASSERT_THAT(Run({"lrange", "list-2", "0", "-1"}).GetVec(), ElementsAre("3.5", "10.1"));
  resp = Run({"sort", "list-1", "LIMIT", "1", "1", "store", "list-2"});
  EXPECT_EQ(1, resp.GetInt());
  ASSERT_THAT(Run({"lrange", "list-2", "0", "-1"}), RespElementsAre("2.20"));
  resp = Run({"sort", "list-1", "LIMIT", "4", "2", "store", "list-2"});
  EXPECT_EQ(1, resp.GetInt());
  ASSERT_THAT(Run({"lrange", "list-2", "0", "-1"}), RespElementsAre("200"));
  resp = Run({"sort", "list-1", "LIMIT", "5", "2", "store", "list-2"});
  EXPECT_EQ(0, resp.GetInt());
  ASSERT_THAT(Run({"lrange", "list-2", "0", "-1"}), ArrLen(0));

  // Test set sort
  Run({"del", "set-1"});
  Run({"del", "list-3"});
  Run({"sadd", "set-1", "5.3", "4.4", "60", "99.9", "100", "9"});
  resp = Run({"sort", "set-1", "store", "list-3"});
  EXPECT_EQ(6, resp.GetInt());
  ASSERT_THAT(Run({"lrange", "list-3", "0", "-1"}).GetVec(),
              ElementsAre("4.4", "5.3", "9", "60", "99.9", "100"));

  // Test sorted set sort
  Run({"del", "zset-1"});
  Run({"del", "list-4"});
  Run({"zadd", "zset-1", "0", "3.3", "0", "30.1", "0", "8.2"});
  resp = Run({"sort", "zset-1", "store", "list-4"});
  EXPECT_EQ(3, resp.GetInt());
  ASSERT_THAT(Run({"lrange", "list-4", "0", "-1"}).GetVec(), ElementsAre("3.3", "8.2", "30.1"));

  // Same key overwrite.
  Run({"del", "list-1"});
  Run({"del", "list-2"});
  Run({"lpush", "list-1", "3.5", "1.2", "10.1", "2.20", "200"});
  resp = Run({"sort", "list-1", "store", "list-1"});
  EXPECT_EQ(5, resp.GetInt());
  ASSERT_THAT(Run({"lrange", "list-1", "0", "-1"}).GetVec(),
              ElementsAre("1.2", "2.20", "3.5", "10.1", "200"));

  // Check that the keys should not expire after some time.
  Run({"del", "list-1"});
  Run({"del", "list-2"});
  Run({"lpush", "list-1", "3.5", "1.2", "10.1", "2.20", "200"});
  Run({"sort", "list-1", "store", "list-2"});
  AdvanceTime(5000);
  ASSERT_THAT(Run({"lrange", "list-2", "0", "-1"}).GetVec(),
              ElementsAre("1.2", "2.20", "3.5", "10.1", "200"));
}

// Regression test for SORT ... STORE with empty result must delete destination key,
// not leave an empty list which crashes SAVE (DFATAL in rdb_save.cc).
TEST_F(GenericFamilyTest, SortStoreEmptyResult) {
  Run({"lpush", "list-src", "3", "1", "2"});

  // LIMIT offset beyond list length -> empty result
  auto resp = Run({"sort", "list-src", "LIMIT", "10", "5", "store", "dest"});
  EXPECT_EQ(0, resp.GetInt());
  EXPECT_EQ(0, Run({"exists", "dest"}).GetInt()) << "empty SORT STORE must not leave a key";

  // LIMIT count=0 -> empty result
  Run({"set", "dest", "old"});  // pre-existing key should be deleted
  resp = Run({"sort", "list-src", "LIMIT", "0", "0", "store", "dest"});
  EXPECT_EQ(0, resp.GetInt());
  EXPECT_EQ(0, Run({"exists", "dest"}).GetInt()) << "empty SORT STORE must delete existing key";
}

TEST_F(GenericFamilyTest, SortStoreResetsExpiry) {
  // SORT set STORE dest, where dest has an expiry — dest expiry must be cleared.
  Run({"del", "src", "dest"});
  Run({"sadd", "src", "3", "1", "2"});
  Run({"sadd", "dest", "old"});
  Run({"expire", "dest", "100"});
  EXPECT_GT(Run({"ttl", "dest"}).GetInt(), 0);

  auto resp = Run({"sort", "src", "store", "dest"});
  EXPECT_EQ(3, resp.GetInt());
  // Destination must have no expiry after SORT STORE overwrites it.
  EXPECT_EQ(-1, Run({"ttl", "dest"}).GetInt());
  ASSERT_THAT(Run({"lrange", "dest", "0", "-1"}).GetVec(), ElementsAre("1", "2", "3"));

  // SORT src STORE src (same key), src has an expiry — must not crash and must clear expiry.
  Run({"del", "myset"});
  Run({"sadd", "myset", "c", "a", "b"});
  Run({"expire", "myset", "100"});
  EXPECT_GT(Run({"ttl", "myset"}).GetInt(), 0);

  resp = Run({"sort", "myset", "ALPHA", "store", "myset"});
  EXPECT_EQ(3, resp.GetInt());
  EXPECT_EQ(-1, Run({"ttl", "myset"}).GetInt());
  ASSERT_THAT(Run({"lrange", "myset", "0", "-1"}).GetVec(), ElementsAre("a", "b", "c"));
}

TEST_F(GenericFamilyTest, Sort_RO) {
  // Test list sort with params
  Run({"del", "list-1"});
  Run({"lpush", "list-1", "3.5", "1.2", "10.1", "2.20", "200"});
  // numeric
  ASSERT_THAT(Run({"sort_ro", "list-1"}).GetVec(),
              ElementsAre("1.2", "2.20", "3.5", "10.1", "200"));
  // string
  ASSERT_THAT(Run({"sort_ro", "list-1", "ALPHA"}).GetVec(),
              ElementsAre("1.2", "10.1", "2.20", "200", "3.5"));
  // desc numeric
  ASSERT_THAT(Run({"sort_ro", "list-1", "DESC"}).GetVec(),
              ElementsAre("200", "10.1", "3.5", "2.20", "1.2"));
  // desc strig
  ASSERT_THAT(Run({"sort_ro", "list-1", "DESC", "ALPHA"}).GetVec(),
              ElementsAre("3.5", "200", "2.20", "10.1", "1.2"));
  // limits
  ASSERT_THAT(Run({"sort_ro", "list-1", "LIMIT", "0", "5"}).GetVec(),
              ElementsAre("1.2", "2.20", "3.5", "10.1", "200"));
  ASSERT_THAT(Run({"sort_ro", "list-1", "LIMIT", "0", "10"}).GetVec(),
              ElementsAre("1.2", "2.20", "3.5", "10.1", "200"));
  ASSERT_THAT(Run({"sort_ro", "list-1", "LIMIT", "2", "2"}).GetVec(), ElementsAre("3.5", "10.1"));
  ASSERT_THAT(Run({"sort_ro", "list-1", "LIMIT", "1", "1"}), RespElementsAre("2.20"));
  ASSERT_THAT(Run({"sort_ro", "list-1", "LIMIT", "4", "2"}), RespElementsAre("200"));
  ASSERT_THAT(Run({"sort_ro", "list-1", "LIMIT", "5", "2"}), ArrLen(0));
  // limits desc
  ASSERT_THAT(Run({"sort_ro", "list-1", "DESC", "LIMIT", "0", "5"}).GetVec(),
              ElementsAre("200", "10.1", "3.5", "2.20", "1.2"));
  ASSERT_THAT(Run({"sort_ro", "list-1", "DESC", "LIMIT", "2", "2"}).GetVec(),
              ElementsAre("3.5", "2.20"));
  ASSERT_THAT(Run({"sort_ro", "list-1", "DESC", "LIMIT", "1", "1"}), RespElementsAre("10.1"));
  ASSERT_THAT(Run({"sort_ro", "list-1", "DESC", "LIMIT", "5", "2"}), ArrLen(0));

  // Test set sort
  Run({"del", "set-1"});
  Run({"sadd", "set-1", "5.3", "4.4", "60", "99.9", "100", "9"});
  ASSERT_THAT(Run({"sort_ro", "set-1"}).GetVec(),
              ElementsAre("4.4", "5.3", "9", "60", "99.9", "100"));
  ASSERT_THAT(Run({"sort_ro", "set-1", "ALPHA"}).GetVec(),
              ElementsAre("100", "4.4", "5.3", "60", "9", "99.9"));
  ASSERT_THAT(Run({"sort_ro", "set-1", "DESC"}).GetVec(),
              ElementsAre("100", "99.9", "60", "9", "5.3", "4.4"));
  ASSERT_THAT(Run({"sort_ro", "set-1", "DESC", "ALPHA"}).GetVec(),
              ElementsAre("99.9", "9", "60", "5.3", "4.4", "100"));

  // Test intset sort
  Run({"del", "intset-1"});
  Run({"sadd", "intset-1", "5", "4", "3", "2", "1"});
  ASSERT_THAT(Run({"sort_ro", "intset-1"}).GetVec(), ElementsAre("1", "2", "3", "4", "5"));

  // Test sorted set sort
  Run({"del", "zset-1"});
  Run({"zadd", "zset-1", "0", "3.3", "0", "30.1", "0", "8.2"});
  ASSERT_THAT(Run({"sort_ro", "zset-1"}).GetVec(), ElementsAre("3.3", "8.2", "30.1"));
  ASSERT_THAT(Run({"sort_ro", "zset-1", "ALPHA"}).GetVec(), ElementsAre("3.3", "30.1", "8.2"));
  ASSERT_THAT(Run({"sort_ro", "zset-1", "DESC"}).GetVec(), ElementsAre("30.1", "8.2", "3.3"));
  ASSERT_THAT(Run({"sort_ro", "zset-1", "DESC", "ALPHA"}).GetVec(),
              ElementsAre("8.2", "30.1", "3.3"));

  // Test sort with non existent key
  Run({"del", "list-2"});
  ASSERT_THAT(Run({"sort_ro", "list-2"}), ArrLen(0));

  // Test not convertible to double
  Run({"lpush", "list-2", "NOTADOUBLE"});
  ASSERT_THAT(Run({"sort_ro", "list-2"}),
              ErrArg("One or more scores can't be converted into double"));

  Run({"set", "foo", "bar"});
  ASSERT_THAT(Run({"sort_ro", "foo"}), ErrArg("WRONGTYPE "));

  Run({"rpush", "list-3", ""});
  ASSERT_THAT(Run({"sort_ro", "list-3"}), RespElementsAre(""));

  Run({"rpush", "list-3", "2", "0", "", "-0.14", "0.12", "-0", "-123123", "7654"});
  ASSERT_THAT(Run({"sort_ro", "list-3"}).GetVec(),
              ElementsAre("-123123", "-0.14", "", "", "-0", "0", "0.12", "2", "7654"));

  Run({"rpush", "NANvalue", "nan"});
  ASSERT_THAT(Run({"sort_ro", "NANvalue"}),
              ErrArg("One or more scores can't be converted into double"));

  // Test store option should not work
  ASSERT_THAT(Run({"sort_ro", "list-1", "store", "list-2"}), ErrArg("syntax error"));
}

TEST_F(GenericFamilyTest, SortROBug3636) {
  Run({"RPUSH", "foo", "1.100000023841858", "1.100000023841858", "1.100000023841858", "-15710",
       "1.100000023841858", "1.100000023841858", "1.100000023841858", "-15710", "-15710",
       "1.100000023841858", "-15710", "-15710", "-15710", "-15710", "1.100000023841858", "-15710",
       "-15710"});
  auto resp = Run({"SORT_RO", "foo", "desc", "alpha"});
  ASSERT_THAT(resp, ArrLen(17));
}

TEST_F(GenericFamilyTest, TimeNoKeys) {
  auto resp = Run({"time"});
  EXPECT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp.GetVec()[0], ArgType(RespExpr::INT64));
  EXPECT_THAT(resp.GetVec()[1], ArgType(RespExpr::INT64));

  // Check that time is the same inside a transaction.
  Run({"multi"});
  Run({"time"});
  usleep(2000);
  Run({"time"});
  resp = Run({"exec"});

  EXPECT_THAT(resp, RespArray(ElementsAre(RespArray(ElementsAre(Not(IntArg(0)), _)),
                                          RespArray(ElementsAre(Not(IntArg(0)), _)))));

  for (int i = 0; i < 2; ++i) {
    int64_t val0 = get<int64_t>(resp.GetVec()[0].GetVec()[i].u);
    int64_t val1 = get<int64_t>(resp.GetVec()[1].GetVec()[i].u);
    EXPECT_EQ(val0, val1);
  }
}

TEST_F(GenericFamilyTest, TimeWithKeys) {
  auto resp = Run({"time"});
  EXPECT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp.GetVec()[0], ArgType(RespExpr::INT64));
  EXPECT_THAT(resp.GetVec()[1], ArgType(RespExpr::INT64));

  // Check that time is the same inside a transaction.
  Run({"multi"});
  Run({"time"});
  usleep(2000);
  Run({"time"});
  Run({"get", "x"});
  resp = Run({"exec"});

  EXPECT_THAT(resp, RespArray(ElementsAre(RespArray(ElementsAre(Not(IntArg(0)), _)),
                                          RespArray(ElementsAre(Not(IntArg(0)), _)), _)));

  for (int i = 0; i < 2; ++i) {
    int64_t val0 = get<int64_t>(resp.GetVec()[0].GetVec()[i].u);
    int64_t val1 = get<int64_t>(resp.GetVec()[1].GetVec()[i].u);
    EXPECT_EQ(val0, val1);
  }
}

TEST_F(GenericFamilyTest, Persist) {
  auto resp = Run({"set", "mykey", "somevalue"});
  EXPECT_EQ(resp, "OK");
  // Key without expiration time - return 0
  EXPECT_EQ(0, CheckedInt({"persist", "mykey"}));
  EXPECT_EQ(-1, CheckedInt({"TTL", "mykey"}));
  // set expiration time and try again
  resp = Run({"EXPIRE", "mykey", "10"});
  EXPECT_EQ(10, CheckedInt({"TTL", "mykey"}));
  EXPECT_EQ(1, CheckedInt({"persist", "mykey"}));
  EXPECT_EQ(-1, CheckedInt({"TTL", "mykey"}));
  // persist on key that does not exist should also return 0
  EXPECT_EQ(0, CheckedInt({"persist", "keythatdoesnotexist"}));
}

TEST_F(GenericFamilyTest, Dump) {
  ASSERT_EQ(RDB_SER_VERSION, 9);
  uint8_t EXPECTED_STRING_DUMP[13] = {0x00, 0xc0, 0x13, 0x09, 0x00, 0x23, 0x13,
                                      0x6f, 0x4d, 0x68, 0xf6, 0x35, 0x6e};
  uint8_t EXPECTED_HASH_DUMP[] = {0x10, 0xc,  0xc,  0x0,  0x0, 0x0,  0x2,  0x0,
                                  0x13, 0x1,  0xc4, 0xd2, 0x2, 0xff, 0x9,  0x0,
                                  0x68, 0x4d, 0x73, 0xa4, 0xf, 0x23, 0x4f, 0xc7};

  uint8_t EXPECTED_LIST_DUMP[] = {0x12, 0x01, 0x02, '\t', '\t', 0x00, 0x00, 0x00,
                                  0x01, 0x00, 0x14, 0x01, 0xff, '\t', 0x00, 0xfb,
                                  0xbd, 0x36, 0xf8, 0xb4, 't',  '%',  ';'};

  // Check string dump
  auto resp = Run({"set", "z", "19"});
  EXPECT_EQ(resp, "OK");
  resp = Run({"dump", "z"});
  auto dump = resp.GetBuf();
  ASSERT_EQ(ToSV(dump), ToSV(EXPECTED_STRING_DUMP));

  // Check list dump
  EXPECT_EQ(1, CheckedInt({"rpush", "l", "20"}));
  resp = Run({"dump", "l"});
  dump = resp.GetBuf();
  ASSERT_EQ(ToSV(dump), ToSV(EXPECTED_LIST_DUMP)) << absl::CHexEscape(resp.GetString());

  // Check for hash dump
  EXPECT_EQ(1, CheckedInt({"hset", "z2", "19", "1234"}));
  resp = Run({"dump", "z2"});
  dump = resp.GetBuf();
  ASSERT_EQ(ToSV(dump), ToSV(EXPECTED_HASH_DUMP));

  // Check that when running with none existing key we're getting nil
  resp = Run({"dump", "foo"});
  EXPECT_EQ(resp.type, RespExpr::NIL);
}

TEST_F(GenericFamilyTest, Restore) {
  using std::chrono::duration_cast;
  using std::chrono::milliseconds;
  using std::chrono::seconds;
  using std::chrono::system_clock;

  // redis 6 with RDB_VERSION 9
  uint8_t STRING_DUMP_REDIS[] = {0x00, 0xc1, 0xd2, 0x04, 0x09, 0x00, 0xd0,
                                 0x75, 0x59, 0x6d, 0x10, 0x04, 0x3f, 0x5c};
  auto resp = Run({"set", "exiting-key", "1234"});
  EXPECT_EQ(resp, "OK");

  // try to restore into existing key - this should fail. We should get BUSYKEY error
  ASSERT_THAT(Run({"restore", "exiting-key", "0", ToSV(STRING_DUMP_REDIS)}),
              ErrArg("BUSYKEY Target key name already exists."));

  // Try restore while setting expiration into the past
  // note that value for expiration is just some valid unix time stamp from the pass
  resp = Run(
      {"restore", "exiting-key", "1665476212900", ToSV(STRING_DUMP_REDIS), "ABSTTL", "REPLACE"});
  ASSERT_EQ(resp, "OK");
  resp = Run({"get", "exiting-key"});
  EXPECT_EQ(resp.type, RespExpr::NIL);  // it was deleted as a result of restore action

  // Test for string that we can successfully load the dumped data and read it back
  resp = Run({"restore", "new-key", "0", ToSV(STRING_DUMP_REDIS)});
  EXPECT_EQ(resp, "OK");
  resp = Run({"get", "new-key"});
  EXPECT_EQ("1234", resp);
  resp = Run({"dump", "new-key"});
  auto dump = resp.GetBuf();
  ASSERT_EQ(ToSV(dump), ToSV(STRING_DUMP_REDIS));

  // test for list
  EXPECT_EQ(1, CheckedInt({"rpush", "orig-list", "20"}));
  resp = Run({"dump", "orig-list"});
  dump = resp.GetBuf();
  resp = Run({"restore", "new-list", "10", ToSV(dump)});
  EXPECT_EQ(resp, "OK");
  resp = Run({"lpop", "new-list"});
  EXPECT_EQ("20", resp);

  // run with hash type
  EXPECT_EQ(1, CheckedInt({"hset", "orig-hash", "123", "45678"}));
  resp = Run({"dump", "orig-hash"});
  dump = resp.GetBuf();
  resp = Run({"restore", "new-hash", "1", ToSV(dump)});
  EXPECT_EQ(resp, "OK");
  EXPECT_EQ(1, CheckedInt({"hexists", "new-hash", "123"}));

  // test with replace and no TTL
  resp = Run({"set", "string-key", "hello world"});
  EXPECT_EQ(resp, "OK");
  resp = Run({"dump", "string-key"});
  dump = resp.GetBuf();
  // this will change the value from "hello world" to "1234"
  resp = Run({"restore", "string-key", "7000", ToSV(STRING_DUMP_REDIS), "REPLACE"});
  resp = Run({"get", "string-key"});
  EXPECT_EQ("1234", resp);
  // check TTL validity
  EXPECT_EQ(CheckedInt({"pttl", "string-key"}), 7000);

  // Make check about ttl with abs time, restoring back to "hello world"
  resp = Run({"restore", "string-key", absl::StrCat(TEST_current_time_ms + 2000), ToSV(dump),
              "ABSTTL", "REPLACE"});
  resp = Run({"get", "string-key"});
  EXPECT_EQ("hello world", resp);
  EXPECT_EQ(CheckedInt({"pttl", "string-key"}), 2000);

  // Last but not least - just make sure that we are good without TTL as well
  resp = Run({"restore", "string-key", "0", ToSV(STRING_DUMP_REDIS), "REPLACE"});
  resp = Run({"get", "string-key"});
  EXPECT_EQ("1234", resp);
  EXPECT_EQ(CheckedInt({"ttl", "string-key"}), -1);

  // The following set was created in Redis 7 with rdb version 11 and it's listpack encoded.
  // We should be able to read it and convert it to our own format DenseSet or HT
  // sadd myset "acme"
  // dump myset
  uint8_t SET_LISTPACK_DUMP[] = {0x14, 0x0D, 0x0D, 0x00, 0x00, 0x00, 0x01, 0x00, 0x84,
                                 0x61, 0x63, 0x6D, 0x65, 0x05, 0xff, 0x0b, 0x00, 0xc1,
                                 0x37, 0x5c, 0xe5, 0xe2, 0xc0, 0xdd, 0x27};
  resp = Run({"restore", "listpack-set", "0", ToSV(SET_LISTPACK_DUMP)});
  resp = Run({"sismember", "listpack-set", "acme"});
  EXPECT_EQ(true, resp.GetInt().has_value());
  EXPECT_EQ(1, resp.GetInt());

  // The following zset was created in Redis 7 with rdb version 11 and it's listpack encoded.
  // zadd my-zset 1 "elon"
  // dump my-zset
  uint8_t ZSET_LISTPACK_DUMP[] = {0x11, 0x0f, 0x0f, 0x00, 0x00, 0x00, 0x02, 0x00, 0x84,
                                  0x65, 0x6c, 0x6f, 0x6e, 0x05, 0x01, 0x01, 0xff, 0x0b,
                                  0x00, 0xc8, 0x01, 0x2c, 0xad, 0xd9, 0xa3, 0x99, 0x5e};

  resp = Run({"restore", "my-zset", "0", ToSV(ZSET_LISTPACK_DUMP)});
  EXPECT_EQ(resp.GetString(), "OK");
  resp = Run({"zrange", "my-zset", "0", "-1"});
  EXPECT_THAT(resp, RespElementsAre("elon"));

  // corrupt the dump file but keep the crc correct.
  ZSET_LISTPACK_DUMP[0] = 0x12;
  uint8_t crc64[8] = {0x4e, 0xa3, 0x4c, 0x89, 0xc4, 0x8b, 0xd9, 0xe4};
  memcpy(ZSET_LISTPACK_DUMP + 19, crc64, 8);
  resp = Run({"restore", "invalid", "0", ToSV(ZSET_LISTPACK_DUMP)});
  EXPECT_THAT(resp, ErrArg("ERR Bad data format"));
}

// A crafted RESTORE payload with a valid listpack header but an interior 32-bit string entry
// of declared length 0x7fffffff must be rejected. The trailing read triggers the deferred
// crash for types that store the listpack as-is.

TEST_F(GenericFamilyTest, RestoreOobSetListpack) {
  uint8_t payload[] = {0x14, 0x0c, 0x0c, 0x00, 0x00, 0x00, 0x01, 0x00, 0xf0, 0xff, 0xff, 0xff,
                       0x7f, 0xff, 0x0b, 0x00, 0xdf, 0x34, 0x52, 0xe8, 0xed, 0x1f, 0xfe, 0x61};
  EXPECT_THAT(Run({"restore", "pwn", "0", ToSV(payload)}), ErrArg("ERR Bad data format"));
  Run({"smembers", "pwn"});
}

TEST_F(GenericFamilyTest, RestoreOobHashListpack) {
  uint8_t payload[] = {0x10, 0x0c, 0x0c, 0x00, 0x00, 0x00, 0x02, 0x00, 0xf0, 0xff, 0xff, 0xff,
                       0x7f, 0xff, 0x0b, 0x00, 0xed, 0x55, 0x88, 0x24, 0xae, 0xbc, 0xbd, 0xa0};
  EXPECT_THAT(Run({"restore", "pwn", "0", ToSV(payload)}), ErrArg("ERR Bad data format"));
  Run({"hgetall", "pwn"});
}

TEST_F(GenericFamilyTest, RestoreOobZsetListpack) {
  uint8_t payload[] = {0x11, 0x0c, 0x0c, 0x00, 0x00, 0x00, 0x02, 0x00, 0xf0, 0xff, 0xff, 0xff,
                       0x7f, 0xff, 0x0b, 0x00, 0xc1, 0xf6, 0xd5, 0x74, 0xd3, 0x02, 0x6a, 0x79};
  EXPECT_THAT(Run({"restore", "pwn", "0", ToSV(payload)}), ErrArg("ERR Bad data format"));
  Run({"zrange", "pwn", "0", "-1"});
}

TEST_F(GenericFamilyTest, RestoreOobListQuicklist) {
  uint8_t payload[] = {0x12, 0x01, 0x02, 0x0c, 0x0c, 0x00, 0x00, 0x00, 0x01,
                       0x00, 0xf0, 0xff, 0xff, 0xff, 0x7f, 0xff, 0x0b, 0x00,
                       0xc2, 0x3d, 0x24, 0x8b, 0xeb, 0x8c, 0x05, 0x25};
  EXPECT_THAT(Run({"restore", "pwn", "0", ToSV(payload)}), ErrArg("ERR Bad data format"));
  Run({"lrange", "pwn", "0", "-1"});
}

TEST_F(GenericFamilyTest, RestoreOobStreamListpack) {
  uint8_t payload[] = {0x0f, 0x01, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1d, 0x1d, 0x00, 0x00, 0x00, 0x09, 0x00,
                       0x01, 0x01, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01, 0x00,
                       0x01, 0x01, 0x01, 0xf0, 0xff, 0xff, 0xff, 0x7f, 0x05, 0xff, 0x01, 0x00, 0x00,
                       0x00, 0x0b, 0x00, 0x22, 0x31, 0x24, 0xa4, 0x6a, 0x6d, 0x9d, 0x7f};
  EXPECT_THAT(Run({"restore", "pwn", "0", ToSV(payload)}), ErrArg("ERR Bad data format"));
  Run({"xrange", "pwn", "-", "+"});
}

TEST_F(GenericFamilyTest, Info) {
  InitWithDbFilename();  // Needed for `save`

  auto get_rdb_changes_since_last_save = [](const string& str) -> size_t {
    const string matcher = "rdb_changes_since_last_success_save:";
    const auto pos = str.find(matcher) + matcher.size();
    const auto sub = str.substr(pos, 1);
    return atoi(sub.c_str());
  };

  EXPECT_EQ(Run({"set", "k", "1"}), "OK");
  auto resp = Run({"info", "persistence"});
  EXPECT_EQ(1, get_rdb_changes_since_last_save(resp.GetString()));

  EXPECT_EQ(Run({"set", "k", "1"}), "OK");
  resp = Run({"info", "persistence"});
  EXPECT_EQ(2, get_rdb_changes_since_last_save(resp.GetString()));

  EXPECT_EQ(Run({"set", "k2", "2"}), "OK");
  resp = Run({"info", "persistence"});
  EXPECT_EQ(3, get_rdb_changes_since_last_save(resp.GetString()));

  EXPECT_EQ(Run({"save"}), "OK");
  resp = Run({"info", "persistence"});
  EXPECT_EQ(0, get_rdb_changes_since_last_save(resp.GetString()));

  EXPECT_EQ(Run({"set", "k2", "2"}), "OK");
  resp = Run({"info", "persistence"});
  EXPECT_EQ(1, get_rdb_changes_since_last_save(resp.GetString()));

  EXPECT_EQ(Run({"bgsave"}), "OK");
  bool cond = WaitUntilCondition(
      [&]() {
        resp = Run({"info", "persistence"});
        return get_rdb_changes_since_last_save(resp.GetString()) == 0;
      },
      500ms);
  EXPECT_TRUE(cond);

  EXPECT_EQ(Run({"set", "k3", "3"}), "OK");
  resp = Run({"info", "persistence"});
  EXPECT_EQ(1, get_rdb_changes_since_last_save(resp.GetString()));

  EXPECT_THAT(Run({"del", "k3"}), IntArg(1));
  resp = Run({"info", "persistence"});
  EXPECT_EQ(2, get_rdb_changes_since_last_save(resp.GetString()));
}

TEST_F(GenericFamilyTest, BgSaveSchedule) {
  InitWithDbFilename();

  auto changes_since_save = [](const string& info) -> int {
    const string m = "rdb_changes_since_last_success_save:";
    auto pos = info.find(m);
    if (pos == string::npos)
      return -1;
    pos += m.size();
    auto end = info.find('\r', pos);
    return atoi(info.substr(pos, end - pos).c_str());
  };

  auto bgsave_and_wait = [&](const std::vector<std::string_view>& cmd) {
    EXPECT_EQ(Run({"set", "k", "1"}), "OK");
    EXPECT_EQ(Run(cmd), "OK");
    EXPECT_TRUE(WaitUntilCondition(
        [&] {
          return changes_since_save(Run({"info", "persistence"}).GetString()) == 0;
        },
        500ms));
  };

  bgsave_and_wait({"bgsave", "SCHEDULE"});
  bgsave_and_wait({"bgsave", "schedule"});
  bgsave_and_wait({"bgsave", "SCHEDULE", "DF"});

  // SAVE must still reject the subcommand.
  EXPECT_THAT(Run({"save", "SCHEDULE"}), ErrArg("Unknown subcommand"));
}

TEST_F(GenericFamilyTest, FieldTtl) {
  TEST_current_time_ms = kMemberExpiryBase * 1000;  // to reset to test time.
  EXPECT_THAT(Run({"saddex", "key", "1", "val1"}), IntArg(1));
  EXPECT_THAT(Run({"saddex", "key", "2", "val2"}), IntArg(1));
  EXPECT_THAT(Run({"sadd", "key", "val3"}), IntArg(1));

  EXPECT_EQ(-2, CheckedInt({"fieldttl", "nokey", "val1"}));  // key not found
  EXPECT_EQ(-3, CheckedInt({"fieldttl", "key", "bar"}));     // field not found
  EXPECT_EQ(1, CheckedInt({"fieldttl", "key", "val1"}));
  EXPECT_EQ(2, CheckedInt({"fieldttl", "key", "val2"}));
  EXPECT_EQ(-1, CheckedInt({"fieldttl", "key", "val3"}));

  AdvanceTime(1100);
  EXPECT_EQ(-3, CheckedInt({"fieldttl", "key", "val1"}));
  EXPECT_EQ(1, CheckedInt({"fieldttl", "key", "val2"}));

  Run({"set", "str", "val"});
  EXPECT_THAT(Run({"fieldttl", "str", "bar"}), ErrArg("wrong"));

  EXPECT_EQ(2, CheckedInt({"HSETEX", "k2", "1", "f1", "v1", "f2", "v2"}));
  EXPECT_EQ(1, CheckedInt({"HSET", "k2", "f3", "v3"}));

  EXPECT_EQ(1, CheckedInt({"fieldttl", "k2", "f1"}));
  EXPECT_EQ(-1, CheckedInt({"fieldttl", "k2", "f3"}));
  EXPECT_EQ(-3, CheckedInt({"fieldttl", "k2", "f4"}));
}

TEST_F(GenericFamilyTest, RandomKey) {
  auto resp = Run({"randomkey"});
  EXPECT_EQ(resp.type, RespExpr::NIL);

  resp = Run({"set", "k1", "1"});
  EXPECT_EQ(Run({"randomkey"}), "k1");
}

TEST_F(GenericFamilyTest, JsonType) {
  auto resp = Run({"json.set", "json", "$", R"({"example":"value"})"});
  EXPECT_EQ(resp, "OK");

  resp = Run({"type", "json"});
  EXPECT_EQ(resp, "ReJSON-RL") << "For the Redis GUI the register of the JSON type is important. "
                                  "See https://github.com/dragonflydb/dragonfly/issues/3386";

  // Test json type lowercase works for the SCAN commmand
  resp = Run({"scan", "0", "type", "rejson-rl"});
  EXPECT_THAT(resp, ArrLen(2));
  auto vec = StrArray(resp.GetVec()[1]);
  ASSERT_THAT(vec, ElementsAre("json"));
}

TEST_F(GenericFamilyTest, FieldExpireSet) {
  Run({"SADD", "key", "a", "b", "c"});
  AdvanceTime(2'000);
  EXPECT_THAT(Run({"FIELDEXPIRE", "key", "10", "a", "b", "c"}),
              RespArray(ElementsAre(IntArg(1), IntArg(1), IntArg(1))));
  EXPECT_EQ(10, CheckedInt({"fieldttl", "key", "a"}));
  AdvanceTime(10'000);
  EXPECT_THAT(Run({"SMEMBERS", "key"}), RespArray(ElementsAre()));
}

TEST_F(GenericFamilyTest, FieldExpireHset) {
  for (int i = 0; i < 3; ++i) {
    EXPECT_EQ(CheckedInt({"HSET", "key", absl::StrCat("k", i), "v"}), 1);
  }
  AdvanceTime(2'000);
  EXPECT_THAT(Run({"FIELDEXPIRE", "key", "10", "k0", "k1", "k2"}),
              RespArray(ElementsAre(IntArg(1), IntArg(1), IntArg(1))));
  EXPECT_EQ(10, CheckedInt({"fieldttl", "key", "k0"}));
  AdvanceTime(10'000);
  EXPECT_THAT(Run({"HGETALL", "key"}), RespArray(ElementsAre()));
}

TEST_F(GenericFamilyTest, FieldExpireNoSuchField) {
  EXPECT_EQ(CheckedInt({"SADD", "key", "a"}), 1);
  EXPECT_EQ(CheckedInt({"HSET", "key2", "k0", "v0"}), 1);
  EXPECT_THAT(Run({"FIELDEXPIRE", "key", "10", "a", "b"}),
              RespArray(ElementsAre(IntArg(1), IntArg(-2))));
  EXPECT_THAT(Run({"FIELDEXPIRE", "key2", "10", "k0", "b"}),
              RespArray(ElementsAre(IntArg(1), IntArg(-2))));
}

TEST_F(GenericFamilyTest, FieldExpireNoSuchKey) {
  EXPECT_THAT(Run({"FIELDEXPIRE", "key", "10", "a", "b"}),
              RespArray(ElementsAre(IntArg(-2), IntArg(-2))));
}

TEST_F(GenericFamilyTest, IterateMapSetStaleTimeZombie) {
  for (int i = 0; i < 64; ++i) {
    Run({"HSETEX", "hkey", "1", absl::StrCat("f", i), "v"});
    Run({"SADDEX", "skey", "1", absl::StrCat("m", i)});
  }

  AdvanceTime(2000);

  Run({"HGET", "hkey", "f0"});
  Run({"SISMEMBER", "skey", "m0"});

  Run({"DEBUG", "OBJHIST"});

  EXPECT_EQ(0, CheckedInt({"EXISTS", "hkey"}));
  EXPECT_EQ(0, CheckedInt({"EXISTS", "skey"}));
}

TEST_F(GenericFamilyTest, DebugUniqStrsDeletesEmptyContainers) {
  for (int i = 0; i < 64; ++i) {
    Run({"HSETEX", "hkey", "1", absl::StrCat("f", i), "v"});
    Run({"SADDEX", "skey", "1", absl::StrCat("m", i)});
  }

  AdvanceTime(2000);

  Run({"HGET", "hkey", "f0"});
  Run({"SISMEMBER", "skey", "m0"});
  Run({"DEBUG", "UNIQ-STRS"});

  EXPECT_EQ(0, CheckedInt({"EXISTS", "hkey"}));
  EXPECT_EQ(0, CheckedInt({"EXISTS", "skey"}));
}

// Regression: TraverseAllEntries walks every DB but the callback used the
// connection-selected DB for deletion.  A zombie in DB 1 could cause a
// same-named non-empty key in DB 0 to be deleted incorrectly.
TEST_F(GenericFamilyTest, DebugObjHistMultiDbCorrectDb) {
  // Live hash in DB 0 under the same key name as the zombie in DB 1.
  Run({"SELECT", "0"});
  Run({"HSET", "shared", "alive", "value"});

  Run({"SELECT", "1"});
  for (int i = 0; i < 64; ++i) {
    Run({"HSETEX", "shared", "1", absl::StrCat("f", i), "v"});
  }

  AdvanceTime(2000);

  Run({"HGET", "shared", "f0"});  // enable lazy expiry on DB 1's hash
  Run({"DEBUG", "OBJHIST"});

  // Zombie in DB 1 must be deleted.
  EXPECT_EQ(0, CheckedInt({"EXISTS", "shared"}));

  // Live hash in DB 0 must survive.
  Run({"SELECT", "0"});
  EXPECT_EQ(1, CheckedInt({"EXISTS", "shared"}));
  EXPECT_EQ("value", Run({"HGET", "shared", "alive"}));
}

TEST_F(GenericFamilyTest, ZInterStoreDeletesEmptySet) {
  for (int i = 0; i < 20; ++i) {
    Run({"SADDEX", "skey", "1", absl::StrCat("m", i)});
  }
  // ZINTERSTORE needs at least one non-empty input to reach ScoreMapFromSet.
  Run({"ZADD", "zkey", "1", "m0"});
  EXPECT_EQ(1, CheckedInt({"EXISTS", "skey"}));

  AdvanceTime(2000);

  // ZINTERSTORE iterates skey via IterateSet (inside ScoreMapFromSet),
  // triggering lazy expiry.  The empty set must be cleaned up.
  Run({"ZINTERSTORE", "zdest", "2", "skey", "zkey"});

  EXPECT_EQ(0, CheckedInt({"EXISTS", "skey"}));
}

TEST_F(GenericFamilyTest, ZUnionStoreDeletesEmptySet) {
  for (int i = 0; i < 20; ++i) {
    Run({"SADDEX", "skey", "1", absl::StrCat("m", i)});
  }
  EXPECT_EQ(1, CheckedInt({"EXISTS", "skey"}));

  AdvanceTime(2000);

  // ZUNIONSTORE iterates skey via IterateSet (inside ScoreMapFromSet),
  // triggering lazy expiry.  The empty set must be cleaned up.
  Run({"ZUNIONSTORE", "zdest", "1", "skey"});

  EXPECT_EQ(0, CheckedInt({"EXISTS", "skey"}));
}

// Iterator invalidation: deleting one input set must not break iteration over
// the remaining ones in UnionShardKeysWithScore.
TEST_F(GenericFamilyTest, ZUnionStoreMultipleEmptySets) {
  for (int i = 0; i < 20; ++i) {
    Run({"SADDEX", "s1", "1", absl::StrCat("a", i)});
    Run({"SADDEX", "s2", "1", absl::StrCat("b", i)});
    Run({"SADDEX", "s3", "1", absl::StrCat("c", i)});
  }

  AdvanceTime(2000);

  Run({"ZUNIONSTORE", "zdest", "3", "s1", "s2", "s3"});

  EXPECT_EQ(0, CheckedInt({"EXISTS", "s1"}));
  EXPECT_EQ(0, CheckedInt({"EXISTS", "s2"}));
  EXPECT_EQ(0, CheckedInt({"EXISTS", "s3"}));
}

TEST_F(GenericFamilyTest, SortByPatternDeletesEmptySet) {
  for (int i = 0; i < 20; ++i) {
    Run({"SADDEX", "skey", "1", absl::StrCat("m", i)});
  }
  EXPECT_EQ(1, CheckedInt({"EXISTS", "skey"}));

  AdvanceTime(2000);

  // SORT BY nosort iterates the set, triggering lazy member expiry.
  // The empty set must be cleaned up on its own — no prior command touching
  // the set is needed.
  Run({"SORT", "skey", "BY", "nosort"});

  EXPECT_EQ(0, CheckedInt({"EXISTS", "skey"}));
}

// Regression: OpFieldExpire for hashes calls SetFieldsExpireTime which triggers
// lazy field expiry via StringMap::Find(), but does not call DeleteIfEmpty
// afterward.  When all fields have expired, the hash remains in the DB with
// Size()==0.  A subsequent SAVE hits the DFATAL in SaveEntry.
TEST_F(GenericFamilyTest, FieldExpireHashDeletesEmptyHash) {
  // Create a hash with a short field-level TTL.
  Run({"HSETEX", "key", "1", "f1", "v1"});
  EXPECT_EQ(1, CheckedInt({"EXISTS", "key"}));

  AdvanceTime(2000);

  // FIELDEXPIRE on the already-expired field triggers lazy expiry via Find()
  // inside UpdateTTL.  Without the fix the hash remains as a zombie key.
  Run({"FIELDEXPIRE", "key", "5", "f1"});

  // The key must have been removed.
  EXPECT_EQ(0, CheckedInt({"EXISTS", "key"}));
}

// SHRINK calls set_time() then DenseSet::Shrink() which expires entries during
// bucket compaction.  If all entries expire, the key must be deleted.
TEST_F(GenericFamilyTest, ShrinkDeletesEmptyContainer) {
  for (int i = 0; i < 128; ++i) {
    Run({"HSETEX", "hkey", "1", absl::StrCat("f", i), "v"});
  }
  for (int i = 4; i < 128; ++i) {
    Run({"HDEL", "hkey", absl::StrCat("f", i)});
  }

  for (int i = 0; i < 128; ++i) {
    Run({"SADDEX", "skey", "1", absl::StrCat("m", i)});
  }
  for (int i = 4; i < 128; ++i) {
    Run({"SREM", "skey", absl::StrCat("m", i)});
  }

  AdvanceTime(2000);

  Run({"SHRINK", "hkey"});
  Run({"SHRINK", "skey"});

  EXPECT_EQ(0, CheckedInt({"EXISTS", "hkey"}));
  EXPECT_EQ(0, CheckedInt({"EXISTS", "skey"}));
}

TEST_F(GenericFamilyTest, ExpireTime) {
  EXPECT_EQ(-2, CheckedInt({"EXPIRETIME", "foo"}));
  EXPECT_EQ(-2, CheckedInt({"PEXPIRETIME", "foo"}));
  Run({"set", "foo", "bar"});
  EXPECT_EQ(-1, CheckedInt({"EXPIRETIME", "foo"}));
  EXPECT_EQ(-1, CheckedInt({"PEXPIRETIME", "foo"}));

  // set expiry
  uint64_t expire_time_in_ms = TEST_current_time_ms + 5000;
  uint64_t expire_time_in_seconds = (expire_time_in_ms + 500) / 1000;
  Run({"pexpireat", "foo", absl::StrCat(expire_time_in_ms)});
  EXPECT_EQ(expire_time_in_seconds, CheckedInt({"EXPIRETIME", "foo"}));
  EXPECT_EQ(expire_time_in_ms, CheckedInt({"PEXPIRETIME", "foo"}));
}

TEST_F(GenericFamilyTest, SortDeletesEmptySet) {
  for (int i = 0; i < 20; ++i) {
    Run({"SADDEX", "skey", "1", absl::StrCat("m", i)});
  }

  AdvanceTime(2000);

  Run({"SISMEMBER", "skey", "m0"});
  // SISMEMBER must not delete the key by itself — SORT is the one that should clean up.
  EXPECT_EQ(1, CheckedInt({"EXISTS", "skey"}));

  Run({"SORT", "skey"});

  EXPECT_EQ(0, CheckedInt({"EXISTS", "skey"}));
}

TEST_F(GenericFamilyTest, RestoreOOM) {
  max_memory_limit = 20000000;
  Run({"set", "src", string(5000, 'x')});
  auto resp = Run({"dump", "src"});

  string dump = resp.GetString();

  // Let Dragonfly propagate max_memory_limit to shards. It does not have to be precise,
  // the loop should have enough time for the internal processes to progress.
  usleep(10000);
  unsigned i = 0;
  for (; i < 10000; ++i) {
    resp = Run({"restore", absl::StrCat("dst", i), "0", dump});
    if (resp != "OK")
      break;
  }
  ASSERT_LT(i, 10000);
  EXPECT_THAT(resp, ErrArg("Out of memory"));
}

TEST_F(GenericFamilyTest, Bug4466) {
  auto resp = Run({"SCAN", "9223372036854775808"});  // an invalid cursor should not crash us.
  EXPECT_THAT(resp, RespElementsAre("0", RespElementsAre()));
}

TEST_F(GenericFamilyTest, Unlink) {
  for (unsigned i = 0; i < 1000; ++i) {
    unsigned start = i * 10;
    vector<string> cmd = {"SADD", "s1"};
    for (unsigned j = 0; j < 10; ++j) {
      cmd.push_back(absl::StrCat("f", start + j));
    }
    auto resp = Run(absl::MakeSpan(cmd));
    ASSERT_THAT(resp, IntArg(10));
    cmd[1] = "s2";
    resp = Run(absl::MakeSpan(cmd));
    ASSERT_THAT(resp, IntArg(10));
  }
  auto resp = Run({"unlink", "s1", "s2"});
  EXPECT_THAT(resp, IntArg(2));
}

TEST_F(GenericFamilyTest, Copy) {
  RespExpr resp;
  string b_val(32, 'b');
  string x_val(32, 'x');

  resp = Run({"mset", "x", x_val, "b", b_val});
  ASSERT_EQ(resp, "OK");
  ASSERT_EQ(2, last_cmd_dbg_info_.shards_count);

  resp = Run({"COPY", "z", "b"});
  ASSERT_THAT(resp, IntArg(0));

  resp = Run({"COPY", "b", "c"});
  ASSERT_THAT(resp, IntArg(1));
  ASSERT_EQ(b_val, Run({"get", "c"}));

  resp = Run({"COPY", "x", "b", "REPLACE"});
  ASSERT_THAT(resp, IntArg(1));

  ASSERT_EQ(x_val, Run({"get", "x"}));
  ASSERT_EQ(x_val, Run({"get", "b"}));
  EXPECT_EQ(CheckedInt({"exists", "x", "b"}), 2);

  const char* keys[2] = {"b", "x"};
  auto ren_fb = pp_->at(0)->LaunchFiber([&] {
    for (size_t i = 0; i < 200; ++i) {
      int j = i % 2;
      auto resp = Run({"COPY", keys[j], keys[1 - j], "REPLACE"});
      ASSERT_THAT(resp, IntArg(1));
    }
  });

  auto exist_fb = pp_->at(2)->LaunchFiber([&] {
    for (size_t i = 0; i < 300; ++i) {
      int64_t resp = CheckedInt({"exists", "x", "b"});
      ASSERT_EQ(2, resp);
    }
  });

  exist_fb.Join();
  ren_fb.Join();
}

TEST_F(GenericFamilyTest, CopyNonString) {
  EXPECT_EQ(1, CheckedInt({"lpush", "x", "elem"}));
  auto resp = Run({"COPY", "x", "b"});
  ASSERT_THAT(resp, IntArg(1));
  ASSERT_EQ(2, last_cmd_dbg_info_.shards_count);

  EXPECT_EQ(1, CheckedInt({"del", "x"}));
  EXPECT_EQ(1, CheckedInt({"del", "b"}));
}

TEST_F(GenericFamilyTest, CopyBinary) {
  const char kKey1[] = "\x01\x02\x03\x04";
  const char kKey2[] = "\x05\x06\x07\x08";

  Run({"set", kKey1, "bar"});
  Run({"COPY", kKey1, kKey2});
  EXPECT_EQ(Run({"get", kKey1}), "bar");
  EXPECT_EQ(Run({"get", kKey2}), "bar");
}

TEST_F(GenericFamilyTest, CopyTTL) {
  Run({"setex", "k1", "10", "bar"});

  ASSERT_THAT(Run({"COPY", "k1", "k2"}), IntArg(1));
  EXPECT_THAT(Run({"ttl", "k2"}), 10);
}

TEST_F(GenericFamilyTest, CopySameName) {
  ASSERT_THAT(Run({"COPY", "k1", "k1"}), ErrArg("source and destination objects are the same"));

  ASSERT_EQ(Run({"set", "k1", "v"}), "OK");
  ASSERT_THAT(Run({"COPY", "k1", "k1"}), ErrArg("source and destination objects are the same"));
}

TEST_F(GenericFamilyTest, CopyToDB) {
  // we don't support DB arg for now
  ASSERT_THAT(Run({"COPY", "k1", "k1", "DB", "SOME_DB"}), ErrArg("syntax error"));
}

TEST_F(GenericFamilyTest, CopyKeyExists) {
  Run({"set", "source", "value1"});
  Run({"set", "destination", "value2"});

  ASSERT_THAT(Run({"COPY", "source", "destination"}), IntArg(0));

  EXPECT_EQ(Run({"get", "destination"}), "value2");
  EXPECT_EQ(Run({"get", "source"}), "value1");

  ASSERT_THAT(Run({"COPY", "source", "destination", "REPLACE"}), IntArg(1));
  EXPECT_EQ(Run({"get", "destination"}), "value1");
}

TEST_F(GenericFamilyTest, HashFieldExpiryDuringDeserialize) {
  Run({"HSETEX", "src", "1", "field1", "value1"});

  // Advance time past field TTL - now field is expired
  AdvanceTime(2000);

  Run({"RENAME", "src", "dst"});
}

TEST_F(GenericFamilyTest, SortNegativeLimit) {
  Run({"lpush", "list-neg", "1", "2", "3", "4", "5"});

  // Negative offset
  auto resp = Run({"sort", "list-neg", "LIMIT", "-1", "2"});
  ASSERT_THAT(resp, ErrArg("value is not an integer"));

  // Negative limit
  resp = Run({"sort", "list-neg", "LIMIT", "0", "-1"});
  ASSERT_THAT(resp, ErrArg("value is not an integer"));

  // Both negative
  resp = Run({"sort", "list-neg", "LIMIT", "-1", "-1"});
  ASSERT_THAT(resp, ErrArg("value is not an integer"));
}

TEST_F(GenericFamilyTest, SortBy) {
  Run({"del", "list-1"});
  Run({"lpush", "list-1", "1", "2", "3"});
  Run({"set", "w_1", "30"});
  Run({"set", "w_2", "20"});
  Run({"set", "w_3", "10"});

  // standard sort
  auto resp = Run({"sort", "list-1", "BY", "w_*"});
  ASSERT_THAT(resp, RespElementsAre("3", "2", "1"));

  // desc
  ASSERT_THAT(Run({"sort", "list-1", "BY", "w_*", "DESC"}), RespElementsAre("1", "2", "3"));

  // alpha
  Run({"set", "s_1", "c"});
  Run({"set", "s_2", "b"});
  Run({"set", "s_3", "a"});
  ASSERT_THAT(Run({"sort", "list-1", "BY", "s_*", "ALPHA"}), RespElementsAre("3", "2", "1"));

  // nosort, lpush reverses order, so 3, 2, 1 is insertion order (or close to it)
  ASSERT_THAT(Run({"sort", "list-1", "BY", "nosort"}), RespElementsAre("3", "2", "1"));

  // missing keys -> 0
  Run({"del", "w_1"});
  ASSERT_THAT(Run({"sort", "list-1", "BY", "w_*"}), RespElementsAre("1", "3", "2"));  // 0, 10, 20

  // BY pattern with LIMIT - test pagination works correctly
  Run({"set", "w_1", "30"});  // restore w_1
  // Sorted order: 3 (w_3=10), 2 (w_2=20), 1 (w_1=30). LIMIT 1 2 skips first, returns next 2
  ASSERT_THAT(Run({"sort", "list-1", "BY", "w_*", "LIMIT", "1", "2"}), RespElementsAre("2", "1"));
  // multiple asterisks should result in syntax error
  ASSERT_THAT(Run({"sort", "list-1", "BY", "w_*_*"}), ErrArg("syntax error"));
}

TEST_F(GenericFamilyTest, SortGet) {
  // Setup test data
  Run({"del", "mylist"});
  Run({"lpush", "mylist", "1", "2", "3"});
  Run({"set", "obj_1", "first"});
  Run({"set", "obj_2", "second"});
  Run({"set", "obj_3", "third"});
  Run({"set", "weight_1", "30"});
  Run({"set", "weight_2", "20"});
  Run({"set", "weight_3", "10"});

  // Test 1: Basic GET with single pattern (sorted numerically: 1,2,3)
  auto resp = Run({"sort", "mylist", "GET", "obj_*"});
  ASSERT_THAT(resp, RespElementsAre("first", "second", "third"));

  // Test 2: GET with special # pattern (returns element itself, sorted: 1,2,3)
  resp = Run({"sort", "mylist", "GET", "#"});
  ASSERT_THAT(resp, RespElementsAre("1", "2", "3"));

  // Test 3: Multiple GET patterns
  resp = Run({"sort", "mylist", "GET", "#", "GET", "obj_*"});
  ASSERT_THAT(resp, RespElementsAre("1", "first", "2", "second", "3", "third"));

  // Test 4: GET with BY pattern (sorted by weight: 3(10), 2(20), 1(30))
  resp = Run({"sort", "mylist", "BY", "weight_*", "GET", "obj_*"});
  ASSERT_THAT(resp, RespElementsAre("third", "second", "first"));

  // Test 5: Multiple GET patterns with BY
  resp = Run({"sort", "mylist", "BY", "weight_*", "GET", "#", "GET", "obj_*"});
  ASSERT_THAT(resp, RespElementsAre("3", "third", "2", "second", "1", "first"));

  // Test 6: GET with missing keys (should return empty strings, sorted: 1,2,3)
  Run({"del", "obj_2"});
  resp = Run({"sort", "mylist", "GET", "obj_*"});
  ASSERT_THAT(resp, RespElementsAre("first", "", "third"));

  // Restore obj_2 for further tests
  Run({"set", "obj_2", "second"});

  // Test 7: GET with DESC (sorted DESC: 3,2,1)
  resp = Run({"sort", "mylist", "DESC", "GET", "obj_*"});
  ASSERT_THAT(resp, RespElementsAre("third", "second", "first"));

  // Test 8: GET with ALPHA
  Run({"del", "strlist"});
  Run({"lpush", "strlist", "c", "b", "a"});
  Run({"set", "obj_a", "alpha"});
  Run({"set", "obj_b", "beta"});
  Run({"set", "obj_c", "gamma"});
  resp = Run({"sort", "strlist", "ALPHA", "GET", "obj_*"});
  ASSERT_THAT(resp, RespElementsAre("alpha", "beta", "gamma"));

  // Test 9: GET with LIMIT
  resp = Run({"sort", "mylist", "GET", "#", "GET", "obj_*", "LIMIT", "1", "2"});
  ASSERT_THAT(resp, RespElementsAre("2", "second", "3", "third"));

  // Test 10: GET with STORE
  resp = Run({"sort", "mylist", "GET", "#", "GET", "obj_*", "STORE", "result"});
  ASSERT_THAT(resp, IntArg(6));  // 3 elements * 2 GET patterns = 6 stored values
  resp = Run({"lrange", "result", "0", "-1"});
  ASSERT_THAT(resp, RespElementsAre("1", "first", "2", "second", "3", "third"));

  // Test 11: GET with BY nosort
  resp = Run({"sort", "mylist", "BY", "nosort", "GET", "obj_*"});
  ASSERT_THAT(resp, RespElementsAre("third", "second", "first"));  // insertion order

  // Test 12: GET pattern validation (multiple asterisks should error)
  ASSERT_THAT(Run({"sort", "mylist", "GET", "obj_*_*"}), ErrArg("syntax error"));

  // Test 13: GET with empty list
  Run({"del", "emptylist"});
  Run({"lpush", "emptylist", "placeholder"});
  Run({"lpop", "emptylist"});
  resp = Run({"sort", "emptylist", "GET", "obj_*"});
  ASSERT_THAT(resp, ArrLen(0));

  // Test 14: GET with literal pattern (no asterisk)
  Run({"set", "fixed_key", "fixed_value"});
  resp = Run({"sort", "mylist", "GET", "fixed_key"});
  ASSERT_THAT(resp, RespElementsAre("fixed_value", "fixed_value", "fixed_value"));

  // Test 15: SORT_RO with GET
  resp = Run({"sort_ro", "mylist", "GET", "#", "GET", "obj_*"});
  ASSERT_THAT(resp, RespElementsAre("1", "first", "2", "second", "3", "third"));
}

TEST_F(GenericFamilyTest, Delex) {
  // DELEX without condition behaves like DEL
  Run({"set", "key1", "value1"});
  EXPECT_EQ(1, CheckedInt({"delex", "key1"}));
  EXPECT_THAT(Run({"get", "key1"}), ArgType(RespExpr::NIL));

  // DELEX on non-existent key returns 0
  EXPECT_EQ(0, CheckedInt({"delex", "nonexistent"}));

  // DELEX IFEQ deletes when values match
  Run({"set", "key2", "value2"});
  EXPECT_EQ(1, CheckedInt({"delex", "key2", "IFEQ", "value2"}));
  EXPECT_THAT(Run({"get", "key2"}), ArgType(RespExpr::NIL));

  // DELEX IFEQ does not delete when values differ
  Run({"set", "key3", "value3"});
  EXPECT_EQ(0, CheckedInt({"delex", "key3", "IFEQ", "wrongvalue"}));
  EXPECT_EQ(Run({"get", "key3"}), "value3");

  // DELEX IFNE deletes when values differ
  Run({"set", "key4", "value4"});
  EXPECT_EQ(1, CheckedInt({"delex", "key4", "IFNE", "differentvalue"}));
  EXPECT_THAT(Run({"get", "key4"}), ArgType(RespExpr::NIL));

  // DELEX IFNE does not delete when values match
  Run({"set", "key5", "value5"});
  EXPECT_EQ(0, CheckedInt({"delex", "key5", "IFNE", "value5"}));
  EXPECT_EQ(Run({"get", "key5"}), "value5");

  // DELEX IFDEQ tests - get digest first and use it
  Run({"set", "key6", "value6"});
  auto digest = Run({"digest", "key6"});
  string_view digest_str = ToSV(digest.GetBuf());
  EXPECT_EQ(1, CheckedInt({"delex", "key6", "IFDEQ", string(digest_str)}));
  EXPECT_THAT(Run({"get", "key6"}), ArgType(RespExpr::NIL));

  // DELEX IFDEQ does not delete when digests differ
  Run({"set", "key7", "value7"});
  EXPECT_EQ(0, CheckedInt({"delex", "key7", "IFDEQ", "0000000000000000"}));
  EXPECT_EQ(Run({"get", "key7"}), "value7");

  // DELEX IFDNE deletes when digests differ
  Run({"set", "key8", "value8"});
  EXPECT_EQ(1, CheckedInt({"delex", "key8", "IFDNE", "0000000000000000"}));
  EXPECT_THAT(Run({"get", "key8"}), ArgType(RespExpr::NIL));

  // DELEX IFDNE does not delete when digests match
  Run({"set", "key9", "value9"});
  auto digest9 = Run({"digest", "key9"});
  string_view digest9_str = ToSV(digest9.GetBuf());
  EXPECT_EQ(0, CheckedInt({"delex", "key9", "IFDNE", string(digest9_str)}));
  EXPECT_EQ(Run({"get", "key9"}), "value9");

  Run({"lpush", "list1", "item"});
  EXPECT_THAT(Run({"delex", "list1", "IFEQ", "item"}), ErrArg("WRONGTYPE"));

  // DELEX with invalid option returns syntax error
  Run({"set", "key10", "value10"});
  EXPECT_THAT(Run({"delex", "key10", "INVALID", "value"}), ErrArg("Unknown subcommand"));

  // DELEX with too many arguments returns error
  EXPECT_THAT(Run({"delex", "key", "IFEQ", "val", "extra"}), ErrArg("wrong number of arguments"));

  EXPECT_THAT(Run({"delex", "key11", "randomarg"}), ErrArg("wrong number of arguments"));
  EXPECT_THAT(Run({"delex", "key12", "IFEQ"}), ErrArg("wrong number of arguments"));
  EXPECT_THAT(Run({"delex", "key13", "xyz"}), ErrArg("wrong number of arguments"));
}

TEST_F(GenericFamilyTest, Rm) {
  // Basic: RM 0 on empty db returns [0, 0]
  auto resp = Run({"rm", "0"});
  ASSERT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp.GetVec()[0], "0");
  EXPECT_THAT(resp.GetVec()[1], IntArg(0));

  // With MATCH arg — still parses OK
  resp = Run({"rm", "0", "match", "foo*"});
  ASSERT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp.GetVec()[1], IntArg(0));

  // With TYPE arg — still parses OK
  resp = Run({"rm", "0", "type", "string"});
  ASSERT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp.GetVec()[1], IntArg(0));

  // With COUNT arg — still parses OK
  resp = Run({"rm", "0", "match", "foo*", "count", "100"});
  ASSERT_THAT(resp, ArrLen(2));

  // Invalid cursor → error
  resp = Run({"rm", "notanumber"});
  EXPECT_THAT(resp, ErrArg("invalid cursor"));

  // Invalid options → syntax error
  resp = Run({"rm", "0", "badopt"});
  EXPECT_THAT(resp, ErrArg("syntax"));
}

TEST_F(GenericFamilyTest, RmDeletesMatchingKeys) {
  for (int i = 0; i < 10; ++i)
    Run({"set", absl::StrCat("foo", i), "val"});
  for (int i = 0; i < 5; ++i)
    Run({"set", absl::StrCat("bar", i), "val"});

  // Delete all foo* keys by iterating until cursor returns 0
  uint32_t total_deleted = 0;
  uint64_t cursor = 0;
  do {
    auto resp = Run({"rm", absl::StrCat(cursor), "match", "foo*", "count", "100"});
    ASSERT_THAT(resp, ArrLen(2));
    ASSERT_TRUE(absl::SimpleAtoi(resp.GetVec()[0].GetString(), &cursor));
    total_deleted += resp.GetVec()[1].GetInt().value();
  } while (cursor != 0);

  EXPECT_EQ(total_deleted, 10u);

  // foo* keys are gone, bar* keys remain
  EXPECT_EQ(Run({"exists", "foo0"}), 0);
  EXPECT_EQ(Run({"exists", "bar0"}), 1);
  EXPECT_EQ(Run({"dbsize"}), 5);
}

// Verifies that long-running container iteration is yielding.
// This test uses a sorted set iteration path, but the same yielding
// behavior is expected for other containers (SET, HASH, LIST) with non
// listpack encodings.
TEST_F(GenericFamilyTest, ContainerIterationYields) {
  // Build a large sorted set that will be encoded as SKIPLIST.
  constexpr int N = 500'000;
  for (int start = 0; start < N; start += 1'000) {
    std::vector<std::string> args = {"zadd", "zs"};
    for (int i = start; i < start + 1000; i++) {
      args.push_back(StrCat(i));
      args.push_back(StrCat("m:", i));
    }
    Run(absl::Span<const std::string>(args));
  }

  // Run IterateSortedSet directly on the shard and verify that the
  // long-running iteration path triggers fiber preemption.
  uint64_t delta = 0;
  ShardId sid = Shard("zs", shard_set->size());
  shard_set->Await(sid, [&delta] {
    auto& db = namespaces->GetDefaultNamespace().GetDbSlice(EngineShard::tlocal()->shard_id());
    auto* table = db.GetDBTable(0);
    auto it = table->prime.Find(string_view{"zs"});
    ASSERT_EQ(it->second.Encoding(), OBJ_ENCODING_SKIPLIST);
    uint64_t before = ThisFiber::GetPreemptCount();
    container_utils::IterateSortedSet(it->second,
                                      [](container_utils::ContainerEntry, double) { return true; });
    delta = ThisFiber::GetPreemptCount() - before;
  });

  // Iteration should yield at least once during long-running execution.
  EXPECT_GT(delta, 0);
}

// Verifies that a yielding container iteration is protected by its read transaction.
// While a read transaction iterates over a large list and yields, a concurrent
// fiber performs RPUSH operations. The key must not be modified while the read
// transaction is running.
TEST_F(GenericFamilyTest, ConcurrentWritesDuringContainerYield) {
  absl::FlagSaver fs;
  absl::SetFlag(&FLAGS_container_iteration_yield_interval_usec, 1);

  constexpr int kListSize = 75'000;
  constexpr int kIterations = 50;
  constexpr int kPushIterations = 100'000;
  constexpr int kWriterFibersPerShard = 8;
  const char* key = "list1";
  constexpr int kBatch = 1'000;

  vector<string> batch_args = {"RPUSH", key};
  batch_args.insert(batch_args.end(), kBatch, "A");
  for (int start = 0; start < kListSize; start += kBatch) {
    Run(absl::Span<const string>(batch_args));
  }

  atomic_bool done{false};

  // Run the iterator through a read-only transaction so the size check happens
  // while the key's shared lock is held, including across iteration yields. Calling
  // LRANGE directly would not let the test atomically observe whether the key changed
  // during the command, so this uses the same transaction shape and checks the QList
  // size inside the transaction callback.
  auto verify_yielding_iteration_is_protected = [&] {
    static CommandId cid{"CONTAINER_ITERATION", CO::READONLY, 1, 1, 1};
    boost::intrusive_ptr<Transaction> tx(new Transaction{&cid});

    CmdArgVec args{key};
    OpStatus init_status = tx->InitByArgs(&namespaces->GetDefaultNamespace(), 0, CmdArgList{args});
    CHECK_EQ(init_status, OpStatus::OK);

    auto cb = [&](Transaction* tx, EngineShard* shard) -> OpResult<void> {
      auto op_args = tx->GetOpArgs(shard);
      auto res = op_args.GetDbSlice().FindReadOnly(op_args.db_cntx, key, OBJ_LIST);
      CHECK(res);
      const auto& it = res.value();
      EXPECT_EQ(it->second.Encoding(), kEncodingQL2);
      // Compare size before/after iteration to verify that the key was not modified.
      size_t size_before = it->second.Size();
      uint64_t preempt_before = ThisFiber::GetPreemptCount();
      container_utils::IterateList(it->second,
                                   [](container_utils::ContainerEntry) { return true; });
      uint64_t preempt_delta = ThisFiber::GetPreemptCount() - preempt_before;
      EXPECT_EQ(size_before, it->second.Size());
      EXPECT_GT(preempt_delta, 0);
      return OpStatus::OK;
    };

    OpResult<void> result = tx->ScheduleSingleHopT(std::move(cb));
    EXPECT_EQ(result.status(), OpStatus::OK);
  };

  auto reader = pp_->at(0)->LaunchFiber([&] {
    for (int i = 0; i < kIterations; i++) {
      verify_yielding_iteration_is_protected();
    }
    done.store(true);
  });

  vector<Fiber> writer_fibers;
  writer_fibers.reserve(2 * kWriterFibersPerShard);

  auto launch_writers = [&](unsigned proactor_index, string_view value) {
    for (int fiber_index = 0; fiber_index < kWriterFibersPerShard; fiber_index++) {
      writer_fibers.push_back(
          pp_->at(proactor_index)->LaunchFiber([&, proactor_index, fiber_index, value] {
            string id = StrCat("shard_", proactor_index, "_", fiber_index);
            for (int i = 0; i < kPushIterations && !done.load(); i++) {
              Run(id, {"RPUSH", key, value});
            }
          }));
    }
  };

  // Writers in same shard.
  launch_writers(0, "B");

  // Writers from different shard.
  launch_writers(1, "C");

  reader.Join();
  for (Fiber& writer : writer_fibers) {
    writer.Join();
  }
}

// Regression test for SORT BY nosort STORE inside MULTI/EXEC does a
// non-concluding Execute() hop to fetch elements, then falls through to the
// unsorted reply path without a concluding hop or Conclude().
// This leaves the parent transaction as continuation_trans_ on the shard.
// When the EXEC transaction is later destroyed, continuation_trans_ becomes
// a dangling pointer, crashing in DisarmInShard() on the next EXEC.
TEST_F(GenericFamilyTest, SortByNosortStoreInMulti) {
  absl::FlagSaver fs;
  absl::SetFlag(&FLAGS_multi_exec_squash, true);

  Run({"lpush", "mylist", "c", "b", "a"});

  // SORT BY nosort STORE goes through ExecuteStandalone (multi-key, possibly
  // cross-shard) and calls Execute(fetch_cb, conclude=false) on the parent
  // EXEC transaction but never concludes it.
  for (int i = 0; i < 10; ++i) {
    Run({"multi"});
    Run({"set", "x", StrCat(i)});
    Run({"sort", "mylist", "BY", "nosort", "STORE", "dest"});
    auto resp = Run({"exec"});
    ASSERT_THAT(resp, ArrLen(2));
  }

  // Verify the store actually happened and the server is healthy.
  EXPECT_THAT(Run({"lrange", "dest", "0", "-1"}), ArrLen(3));
  EXPECT_EQ(Run({"get", "x"}), "9");
}

// Regression test for https://github.com/dragonflydb/dragonfly/issues/7052
// Heartbeat-driven key expiry must not call SendMessages inside a fiber-atomic section.
TEST_F(GenericFamilyTest, KeyspaceNotificationNoAtomicSectionOnExpiry) {
  const string prev_notify =
      Run({"CONFIG", "GET", "notify_keyspace_events"}).GetVec()[1].GetString();
  Run({"CONFIG", "SET", "notify_keyspace_events", "EX"});
  absl::Cleanup restore_notify = [&] {
    Run({"CONFIG", "SET", "notify_keyspace_events", prev_notify});
  };

  single_response_ = false;
  auto sub_resp = pp_->at(1)->Await([&] { return Run({"subscribe", "__keyevent@0__:expired"}); });
  ASSERT_THAT(sub_resp, ArrLen(3));

  Run({"set", "mykey", "myval"});
  Run({"expire", "mykey", "1"});
  AdvanceTime(1100);

  // Drive expiry the same way the heartbeat does: DeleteExpiredStep returns events,
  // which are sent outside the atomic section (see issue #7052).
  shard_set->RunBriefInParallel([](EngineShard* shard) {
    DbSlice& db_slice = namespaces->GetDefaultNamespace().GetDbSlice(shard->shard_id());
    DbContext db_cntx{&namespaces->GetDefaultNamespace(), 0, TEST_current_time_ms};
    DbSlice::DeleteExpiredStats stats = db_slice.DeleteExpiredStep(db_cntx, 100);
    if (!stats.key_events.empty())
      channel_store->SendMessages("__keyevent@0__:expired", stats.key_events, false);
  });

  // Flush all async dispatch callbacks so the subscriber's thread receives the message.
  pp_->AwaitFiberOnAll([](util::ProactorBase*) {});

  ASSERT_EQ(1u, SubscriberMessagesLen("IO1"));
  const auto& msg = GetPublishedMessage("IO1", 0);
  EXPECT_EQ("__keyevent@0__:expired", msg.channel);
  EXPECT_EQ("mykey", msg.message);
}

// A rejected CONFIG SET must not leave the invalid value observable: CONFIG GET has to keep
// returning the previous value (a poisoned raw flag would also crash later flag consumers).
TEST_F(GenericFamilyTest, ConfigSetRejectedValueRollsBack) {
  auto initial = Run({"CONFIG", "GET", "notify_keyspace_events"});
  ASSERT_THAT(initial, ArrLen(2));
  const string prev = initial.GetVec()[1].GetString();

  EXPECT_THAT(Run({"CONFIG", "SET", "notify_keyspace_events", "nonsense"}),
              ErrArg("CONFIG SET failed"));
  auto resp = Run({"CONFIG", "GET", "notify_keyspace_events"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("notify_keyspace_events", prev));
}

// CONFIG SET notify_keyspace_events must apply to every namespace, not only the default one,
// and namespaces created afterwards must inherit the current setting.
TEST_F(GenericFamilyTest, ConfigNotifyAppliesToAllNamespaces) {
  Namespace* ns = pp_->at(0)->Await([] { return &namespaces->GetOrInsert("ns1"); });

  const string prev_notify =
      Run({"CONFIG", "GET", "notify_keyspace_events"}).GetVec()[1].GetString();
  Run({"CONFIG", "SET", "notify_keyspace_events", "EX"});
  // Scoped so a fatal assertion below cannot leak the setting into other tests.
  absl::Cleanup restore_notify = [&] {
    Run({"CONFIG", "SET", "notify_keyspace_events", prev_notify});
  };

  for (unsigned i = 0; i < shard_set->size(); ++i) {
    EXPECT_TRUE(ns->GetDbSlice(i).IsExpiredEventsRecording()) << i;
  }

  Namespace* late_ns = pp_->at(0)->Await([] { return &namespaces->GetOrInsert("ns2"); });
  for (unsigned i = 0; i < shard_set->size(); ++i) {
    EXPECT_TRUE(late_ns->GetDbSlice(i).IsExpiredEventsRecording()) << i;
  }

  Run({"CONFIG", "SET", "notify_keyspace_events", ""});
  for (unsigned i = 0; i < shard_set->size(); ++i) {
    EXPECT_FALSE(ns->GetDbSlice(i).IsExpiredEventsRecording()) << i;
    EXPECT_FALSE(late_ns->GetDbSlice(i).IsExpiredEventsRecording()) << i;
  }
}

// EXPIRE with an already-past time deleted the key silently, so a subscriber waiting for the
// expired event blocked forever.
TEST_F(GenericFamilyTest, ExpirePastEmitsExpiredEvent) {
  const string prev_notify =
      Run({"CONFIG", "GET", "notify_keyspace_events"}).GetVec()[1].GetString();
  Run({"CONFIG", "SET", "notify_keyspace_events", "EX"});
  absl::Cleanup restore_notify = [&] {
    Run({"CONFIG", "SET", "notify_keyspace_events", prev_notify});
  };

  Run({"SET", "foo", "1"});

  single_response_ = false;
  auto sub_resp = pp_->at(1)->Await([&] { return Run({"subscribe", "__keyevent@0__:expired"}); });
  ASSERT_THAT(sub_resp, ArrLen(3));

  EXPECT_THAT(Run({"EXPIRE", "foo", "-1"}), IntArg(1));
  EXPECT_THAT(Run({"GET", "foo"}), ArgType(RespExpr::NIL));

  pp_->AwaitFiberOnAll([](util::ProactorBase*) {});  // flush pending publishes
  ASSERT_EQ(1u, SubscriberMessagesLen("IO1"));
  const auto& msg = GetPublishedMessage("IO1", 0);
  EXPECT_EQ("__keyevent@0__:expired", msg.channel);
  EXPECT_EQ("foo", msg.message);
  EXPECT_GE(GetMetrics().events.expired_keys, 1u);
}

// drakeydb: P7-1 (decision 32) -- `SORT <source that cannot be sorted> STORE <dst>`. A fork
// regression from P4-0 made a shard callback return the failure as the hop's status, and on a
// multi-shard transaction RunCallback CHECK-fails on any hop status but OK: a missing, wrong-type
// or non-numeric source plus a destination on another shard aborted the server. With one shard
// there was no abort, but a missing source replied an empty array and left the destination stale
// where Redis and KeyDB delete it and reply 0 (sort.cpp: an empty result deletes the STORE key;
// a wrong-type or unparsable source is an error raised before the destination is touched).
//
// A crash takes the whole test binary down, so the tests below fail by aborting as well as by
// their assertions. They need two shards and place the destination on the source's shard and on
// the other one, for each way SORT reaches its source (the sorted fetch, BY nosort, BY pattern).
namespace {

// A key named `prefix<i>` on the same shard as `src` or on a different one, under this process's
// shard count.
string SortStoreDstKey(string_view prefix, string_view src, bool same_shard) {
  const ShardId src_sid = Shard(src, shard_set->size());
  for (int i = 0;; ++i) {
    string candidate = StrCat(prefix, i);
    if ((Shard(candidate, shard_set->size()) == src_sid) == same_shard)
      return candidate;
    CHECK_LT(i, 10000) << "no '" << prefix << "' key on the wanted shard";
  }
}

// The options between the source and STORE, one per code path of SortGeneric that reads the source.
const vector<vector<string>> kSortStoreVariants = {
    {},                            // sorted fetch, numeric
    {"ALPHA"},                     // sorted fetch, lexicographic
    {"DESC", "LIMIT", "0", "2"},   // sorted fetch with bounds
    {"GET", "#"},                  // sorted fetch with a GET pattern
    {"BY", "nosort"},              // unsorted fetch
    {"BY", "nosort", "GET", "#"},  // unsorted fetch with a GET pattern
    {"BY", "weight_*"},            // BY pattern: unsorted fetch, then external keys
};

vector<string> SortStoreCommand(string_view src, const vector<string>& variant, string_view dst) {
  vector<string> cmd{"SORT", string(src)};
  cmd.insert(cmd.end(), variant.begin(), variant.end());
  cmd.insert(cmd.end(), {"STORE", string(dst)});
  return cmd;
}

}  // namespace

// The source does not exist: the destination is deleted, whatever its type or TTL, and the reply
// is 0. Before: an abort across shards, an empty array and a stale destination within one.
TEST_F(GenericFamilyTest, SortStoreOfMissingSourceDeletesDestination) {
  ASSERT_GT(shard_set->size(), 1u) << "the test needs more than one shard";
  const string src = "sort-missing-src";

  int round = 0;
  for (bool same_shard : {true, false}) {
    for (const auto& variant : kSortStoreVariants) {
      const string dst = SortStoreDstKey("sort-missing-dst", src, same_shard);
      SCOPED_TRACE(StrCat(same_shard ? "same shard" : "other shard", ", variant ",
                          absl::StrJoin(variant, " ")));
      const auto cmd = SortStoreCommand(src, variant, dst);

      // The destination exists, a list or a string by turns, with a TTL: all of it goes.
      if (round++ % 2 == 0)
        Run({"rpush", dst, "stale"});
      else
        Run({"set", dst, "stale"});
      Run({"expire", dst, "1000"});
      EXPECT_THAT(Run(cmd), IntArg(0));
      EXPECT_THAT(Run({"exists", dst}), IntArg(0));
      EXPECT_THAT(Run({"ttl", dst}), IntArg(-2));

      // Nothing to delete the second time: the same reply, and no key appears.
      EXPECT_THAT(Run(cmd), IntArg(0));
      EXPECT_THAT(Run({"exists", dst}), IntArg(0));
      EXPECT_THAT(Run({"exists", src}), IntArg(0));
    }
  }
  EXPECT_THAT(Run({"dbsize"}), IntArg(0)) << "no key was left behind";
}

// The source is an existing key of a type SORT cannot read: WRONGTYPE, the destination untouched.
TEST_F(GenericFamilyTest, SortStoreOfWrongTypeSourceKeepsDestination) {
  ASSERT_GT(shard_set->size(), 1u) << "the test needs more than one shard";
  const string src = "sort-wrongtype-src";

  for (string_view type : {"string", "hash"}) {
    if (type == "string")
      Run({"set", src, "x"});
    else
      Run({"hset", src, "f", "v"});

    for (bool same_shard : {true, false}) {
      for (const auto& variant : kSortStoreVariants) {
        const string dst = SortStoreDstKey("sort-wrongtype-dst", src, same_shard);
        SCOPED_TRACE(StrCat(type, " source, ", same_shard ? "same shard" : "other shard",
                            ", variant ", absl::StrJoin(variant, " ")));

        Run({"del", dst});
        Run({"rpush", dst, "kept", "too"});
        EXPECT_THAT(Run(SortStoreCommand(src, variant, dst)), ErrArg("WRONGTYPE"));
        EXPECT_THAT(Run({"lrange", dst, "0", "-1"}), RespElementsAre("kept", "too"));
      }
    }
    Run({"del", src});
  }
}

// The source holds elements a numeric SORT cannot convert: the error, the destination untouched.
// The same abort as above for a destination on another shard (the hop result was
// INVALID_NUMERIC_RESULT). ALPHA and BY nosort do not convert anything and succeed.
TEST_F(GenericFamilyTest, SortStoreOfUnparsableSourceKeepsDestination) {
  ASSERT_GT(shard_set->size(), 1u) << "the test needs more than one shard";
  const string src = "sort-unparsable-src";
  Run({"rpush", src, "not", "numbers"});

  const vector<vector<string>> variants = {{}, {"DESC", "LIMIT", "0", "2"}, {"GET", "#"}};
  for (bool same_shard : {true, false}) {
    for (const auto& variant : variants) {
      const string dst = SortStoreDstKey("sort-unparsable-dst", src, same_shard);
      SCOPED_TRACE(StrCat(same_shard ? "same shard" : "other shard", ", variant ",
                          absl::StrJoin(variant, " ")));

      Run({"del", dst});
      Run({"rpush", dst, "kept"});
      EXPECT_THAT(Run(SortStoreCommand(src, variant, dst)), ErrArg("can't be converted"));
      EXPECT_THAT(Run({"lrange", dst, "0", "-1"}), RespElementsAre("kept"));
      EXPECT_THAT(Run({"lrange", src, "0", "-1"}), RespElementsAre("not", "numbers"));
    }
  }
}

// A source whose own lazy member expiry empties it reaches the same STORE of nothing on the
// unsorted path, which used to return an empty array and leave the destination alone: a set whose
// every member is expired is a missing source, and the destination goes, like on the sorted path.
TEST_F(GenericFamilyTest, SortStoreOfFullyExpiredSetDeletesDestination) {
  ASSERT_GT(shard_set->size(), 1u) << "the test needs more than one shard";
  const string src = "sort-expired-src";

  for (bool same_shard : {true, false}) {
    for (const auto& variant : kSortStoreVariants) {
      const string dst = SortStoreDstKey("sort-expired-dst", src, same_shard);
      SCOPED_TRACE(StrCat(same_shard ? "same shard" : "other shard", ", variant ",
                          absl::StrJoin(variant, " ")));

      ASSERT_THAT(Run({"sadd", src, "m"}), IntArg(1));
      ASSERT_THAT(Run({"fieldexpire", src, "1", "m"}), ArrLen(1));
      Run({"rpush", dst, "stale"});
      AdvanceTime(1100);

      EXPECT_THAT(Run(SortStoreCommand(src, variant, dst)), IntArg(0));
      EXPECT_THAT(Run({"exists", src}), IntArg(0));
      EXPECT_THAT(Run({"exists", dst}), IntArg(0));
    }
  }
}

// drakeydb: P7-1 (decision 34) -- SORT orders as Redis and KeyDB do, for every caller
// (ISSUE-REGISTER D-34). KeyDB replicates `SORT .. STORE` verbatim, so a replica that orders
// differently ends up with the same members in another order, silently. The rules, each from
// KeyDB's sort.cpp:
//   - a numeric tie under BY breaks on the element, not on the weight (:153-156);
//   - under ALPHA BY a missing weight sorts before every present one, the empty string included
//     (:160-168);
//   - BY nosort on a SET that is stored, or runs in a script, sorts ALPHA by the element
//     (:298-308);
//   - BY nosort on a LIST or a ZSET walks it from the tail under DESC, and LIMIT counts along that
//     walk (:356-382, :401-430).
//
// Every expected list below is what real KeyDB v6.3.4 answers to the same data and command, and
// Redis 7.0.15 answers the same to all of them: the reply, the list STORE leaves and, for the
// nosort tables, both inside EVAL; SORT_RO's replies (KeyDB 6.3.4 has no SORT_RO) were checked on
// Redis alone. They were taken from the servers, not derived by hand. The one exception is the
// table of ALPHA BY ties, which says so.
namespace {

struct SortOrderCase {
  string source;
  vector<string> options;  // between the source key and STORE
  vector<string> expected;
};

string SortOrderCaseName(const SortOrderCase& c) {
  return StrCat("SORT ", c.source, " ", absl::StrJoin(c.options, " "));
}

const vector<SortOrderCase> kByTieCases = {
    {"s", {"BY", "w_*"}, {"b", "c", "d", "e", "f", "g", "h", "i", "j", "a"}},
    {"s", {"BY", "w_*", "DESC"}, {"a", "j", "i", "h", "g", "f", "e", "d", "c", "b"}},
    {"s", {"BY", "w_*", "LIMIT", "1", "3"}, {"c", "d", "e"}},
    {"s", {"BY", "w_*", "DESC", "LIMIT", "2", "4"}, {"i", "h", "g", "f"}},
    {"s", {"BY", "w_*", "LIMIT", "5", "100"}, {"g", "h", "i", "j", "a"}},
    {"s", {"BY", "nokey_*"}, {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}},
    {"s", {"BY", "nokey_*", "DESC"}, {"j", "i", "h", "g", "f", "e", "d", "c", "b", "a"}},
    {"l", {"BY", "lw_*"}, {"r", "t", "u", "w", "y", "q", "e", "i", "o", "p"}},
    {"l", {"BY", "lw_*", "DESC"}, {"p", "o", "i", "e", "q", "y", "w", "u", "t", "r"}},
    {"sm", {"BY", "nw_*"}, {"c", "a", "b", "d"}},
    {"sm", {"BY", "nw_*", "DESC"}, {"d", "b", "a", "c"}},
    {"ln", {"BY", "nokey_*"}, {"1", "3", "3", "5", "5", "5", "7", "9"}},
    {"ln", {"BY", "nokey_*", "DESC"}, {"9", "7", "5", "5", "5", "3", "3", "1"}},
    {"s", {"BY", "w_*", "GET", "#", "GET", "h_*"}, {"b",  "Hb", "c",  "Hc", "d",  "Hd", "e",
                                                    "He", "f",  "Hf", "g",  "Hg", "h",  "Hh",
                                                    "i",  "Hi", "j",  "Hj", "a",  "Ha"}},
    {"s", {"BY", "w_*", "DESC", "LIMIT", "1", "2", "GET", "#"}, {"j", "i"}},
};

const vector<SortOrderCase> kAlphaByCases = {
    {"sa", {"BY", "aw_*", "ALPHA"}, {"c", "b", "f", "d", "a", "e"}},
    {"sa", {"BY", "aw_*", "ALPHA", "DESC"}, {"e", "a", "d", "f", "b", "c"}},
    {"sa", {"BY", "aw_*", "ALPHA", "LIMIT", "1", "3"}, {"b", "f", "d"}},
    {"sa", {"BY", "aw_*", "ALPHA", "DESC", "LIMIT", "1", "3"}, {"a", "d", "f"}},
    {"sa", {"BY", "aw_*", "ALPHA", "GET", "#"}, {"c", "b", "f", "d", "a", "e"}},
    {"sw", {"BY", "ww_*", "ALPHA"}, {"x", "y", "z"}},
    {"sw", {"BY", "ww_*", "ALPHA", "DESC"}, {"z", "y", "x"}},
};

const vector<SortOrderCase> kNosortSetCases = {
    {"s", {"BY", "nosort"}, {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}},
    {"s", {"BY", "nosort", "DESC"}, {"j", "i", "h", "g", "f", "e", "d", "c", "b", "a"}},
    {"s", {"BY", "nosort", "LIMIT", "0", "3"}, {"a", "b", "c"}},
    {"s", {"BY", "nosort", "DESC", "LIMIT", "1", "2"}, {"i", "h"}},
    {"s", {"BY", "nosort", "GET", "#", "GET", "h_*"}, {"a",  "Ha", "b",  "Hb", "c",  "Hc", "d",
                                                       "Hd", "e",  "He", "f",  "Hf", "g",  "Hg",
                                                       "h",  "Hh", "i",  "Hi", "j",  "Hj"}},
    {"sn", {"BY", "nosort"}, {"1", "10", "2", "3", "4", "5", "6", "7", "8", "9"}},
    {"sn", {"BY", "nosort", "DESC"}, {"9", "8", "7", "6", "5", "4", "3", "2", "10", "1"}},
};

const vector<SortOrderCase> kNosortWalkCases = {
    {"zl", {"BY", "nosort"}, {"x", "y", "z", "w", "v"}},
    {"zl", {"BY", "nosort", "DESC"}, {"v", "w", "z", "y", "x"}},
    {"zl", {"BY", "nosort", "LIMIT", "1", "2"}, {"y", "z"}},
    {"zl", {"BY", "nosort", "DESC", "LIMIT", "1", "2"}, {"w", "z"}},
    {"zl", {"BY", "nosort", "DESC", "LIMIT", "3", "10"}, {"y", "x"}},
    {"zl", {"BY", "nosort", "DESC", "LIMIT", "9", "2"}, {}},
    {"zl", {"BY", "nosort", "DESC", "GET", "#"}, {"v", "w", "z", "y", "x"}},
    {"zl", {"BY", "nosort", "GET", "#"}, {"x", "y", "z", "w", "v"}},
    {"z", {"BY", "nosort"}, {"b", "c", "a", "e", "d"}},
    {"z", {"BY", "nosort", "DESC"}, {"d", "e", "a", "c", "b"}},
    {"z", {"BY", "nosort", "LIMIT", "1", "2"}, {"c", "a"}},
    {"z", {"BY", "nosort", "DESC", "LIMIT", "1", "2"}, {"e", "a"}},
    {"z", {"BY", "nosort", "DESC", "LIMIT", "3", "10"}, {"c", "b"}},
    {"z", {"BY", "nosort", "DESC", "LIMIT", "9", "2"}, {}},
    {"z", {"BY", "nosort", "DESC", "GET", "#"}, {"d", "e", "a", "c", "b"}},
    {"z", {"BY", "nosort", "GET", "#"}, {"b", "c", "a", "e", "d"}},
};

const vector<SortOrderCase> kWideLimitCases = {
    {"ln", {"LIMIT", "1", "4294967295"}, {"3", "3", "5", "5", "5", "7", "9"}},
    {"ln", {"LIMIT", "4294967295", "1"}, {}},
    {"zl", {"BY", "nosort", "LIMIT", "1", "4294967295"}, {"y", "z", "w", "v"}},
    {"zl", {"BY", "nosort", "DESC", "LIMIT", "1", "4294967295"}, {"w", "z", "y", "x"}},
    {"s", {"BY", "w_*", "LIMIT", "1", "4294967295"}, {"c", "d", "e", "f", "g", "h", "i", "j", "a"}},
    {"s", {"BY", "w_*", "DESC", "LIMIT", "4294967295", "4294967295"}, {}},
};

// What ALPHA BY does with equal weights is the one order a replica of KeyDB cannot reproduce: KeyDB
// leaves such elements in the order its own sort received them (a list's order, a set's hash
// order, which differs between two KeyDB processes), so these lists are drakeydb's rule, not
// KeyDB's: a tie breaks on the element, bytewise, so drakeydb's own replicas and peers agree.
// KeyDB answers `SORT tl BY nokey_* ALPHA` below with b a b a, in the list's order.
const vector<SortOrderCase> kAlphaByTieCases = {
    {"ts", {"BY", "tw_*", "ALPHA"}, {"a", "b", "c", "d"}},
    {"ts", {"BY", "tw_*", "ALPHA", "DESC"}, {"d", "c", "b", "a"}},
    {"tm", {"BY", "nokey_*", "ALPHA"}, {"a", "b", "c", "d"}},
    {"tm", {"BY", "nokey_*", "ALPHA", "DESC"}, {"d", "c", "b", "a"}},
    {"tl", {"BY", "nokey_*", "ALPHA"}, {"a", "a", "b", "b"}},
};

}  // namespace

class GenericSortOrderTest : public GenericFamilyTest {
 protected:
  void LoadData();
  void ExpectReply(const SortOrderCase& c);
  void ExpectStore(const SortOrderCase& c);
  void ExpectScript(const SortOrderCase& c);
};

// The data the tables above were taken on (the same commands ran against KeyDB and Redis).
void GenericSortOrderTest::LoadData() {
  // s: a SET of letters; every letter weighs 1 (w_*), except a, which weighs 2.
  Run({"sadd", "s", "j", "i", "h", "g", "f", "e", "d", "c", "b", "a"});
  for (string_view ch : {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}) {
    Run({"set", StrCat("w_", ch), ch == "a" ? "2" : "1"});
    Run({"set", StrCat("h_", ch), StrCat("H", ch)});
  }
  Run({"sadd", "sn", "5", "3", "9", "1", "7", "2", "8", "4", "6", "10"});
  // l: a LIST whose elements mostly have no weight (lw_*).
  Run({"rpush", "l", "q", "w", "e", "r", "t", "y", "u", "i", "o", "p"});
  for (string_view ch : {"e", "i", "o", "p"})
    Run({"set", StrCat("lw_", ch), "3"});
  Run({"set", "lw_q", "1"});
  Run({"rpush", "ln", "5", "3", "9", "1", "7", "5", "5", "3"});
  // sm: a and d weigh 0, b has no weight (also 0), c weighs -1.
  Run({"sadd", "sm", "d", "c", "b", "a"});
  Run({"set", "nw_a", "0"});
  Run({"set", "nw_c", "-1"});
  Run({"set", "nw_d", "0"});
  // z: a ZSET whose rank order (b c a e d) is not the members' order; zl: a LIST out of order.
  Run({"zadd", "z", "3", "a", "1", "b", "2", "c", "5", "d", "4", "e"});
  Run({"rpush", "zl", "x", "y", "z", "w", "v"});
  // sa: ALPHA BY weights aw_*: c has none, b has the empty string, the others are distinct.
  Run({"sadd", "sa", "f", "e", "d", "c", "b", "a"});
  Run({"set", "aw_a", "m"});
  Run({"set", "aw_b", ""});
  Run({"set", "aw_d", "b"});
  Run({"set", "aw_e", "z"});
  Run({"set", "aw_f", "a"});
  // sw: x's weight key is a list (not a string, so missing), y's is empty, z's is "a".
  Run({"sadd", "sw", "z", "y", "x"});
  Run({"rpush", "ww_x", "a"});
  Run({"set", "ww_y", ""});
  Run({"set", "ww_z", "a"});
  // The ALPHA BY ties: equal weights (ts), no weights at all (tm), a list with duplicates (tl).
  Run({"sadd", "ts", "d", "c", "b", "a"});
  for (string_view ch : {"a", "b", "c"})
    Run({"set", StrCat("tw_", ch), "x"});
  Run({"set", "tw_d", "y"});
  Run({"sadd", "tm", "d", "c", "b", "a"});
  Run({"rpush", "tl", "b", "a", "b", "a"});
}

// SORT and SORT_RO reply `expected`, in this order.
void GenericSortOrderTest::ExpectReply(const SortOrderCase& c) {
  for (string_view name : {"SORT", "SORT_RO"}) {
    vector<string> cmd{string(name), c.source};
    cmd.insert(cmd.end(), c.options.begin(), c.options.end());
    EXPECT_THAT(Run(cmd), RespArray(ElementsAreArray(c.expected))) << name;
  }
}

// STORE leaves `expected` in a list at dst, whether dst is on the source's shard or on another one,
// over a dst that held something else (an empty result deletes it, as Redis does).
void GenericSortOrderTest::ExpectStore(const SortOrderCase& c) {
  ASSERT_GT(shard_set->size(), 1u) << "the test needs more than one shard";
  for (bool same_shard : {true, false}) {
    const string dst = SortStoreDstKey("sort-order-dst", c.source, same_shard);
    SCOPED_TRACE(StrCat("STORE on ", same_shard ? "the source's shard" : "another shard"));

    Run({"set", dst, "stale"});
    EXPECT_THAT(Run(SortStoreCommand(c.source, c.options, dst)), IntArg(c.expected.size()));
    if (c.expected.empty())
      EXPECT_THAT(Run({"exists", dst}), IntArg(0));
    else
      EXPECT_THAT(Run({"lrange", dst, "0", "-1"}), RespArray(ElementsAreArray(c.expected)));
  }
}

// The same inside EVAL, where a SET under BY nosort is sorted even without STORE: the reply of
// SORT and of SORT_RO, and the list STORE leaves.
void GenericSortOrderTest::ExpectScript(const SortOrderCase& c) {
  const string dst = SortStoreDstKey("sort-order-script-dst", c.source, false);
  for (string_view name : {"SORT", "SORT_RO"}) {
    vector<string> eval{"EVAL", StrCat("return redis.call('", name, "', KEYS[1], unpack(ARGV))"),
                        "2", c.source, dst};
    eval.insert(eval.end(), c.options.begin(), c.options.end());
    EXPECT_THAT(Run(eval), RespArray(ElementsAreArray(c.expected))) << name;
  }

  vector<string> eval{"EVAL", "return redis.call('SORT', KEYS[1], unpack(ARGV))", "2", c.source,
                      dst};
  eval.insert(eval.end(), c.options.begin(), c.options.end());
  eval.insert(eval.end(), {"STORE", dst});
  Run({"set", dst, "stale"});
  EXPECT_THAT(Run(eval), IntArg(c.expected.size()));
  if (c.expected.empty())
    EXPECT_THAT(Run({"exists", dst}), IntArg(0));
  else
    EXPECT_THAT(Run({"lrange", dst, "0", "-1"}), RespArray(ElementsAreArray(c.expected)));
}

// Rule 1: two elements with the same BY weight, or none (a missing key weighs 0), come out in the
// elements' order, ASC and DESC alike, under LIMIT and into STORE. Before, they came out in the
// order the source held them in: a set's iteration order, which a replica does not share.
TEST_F(GenericSortOrderTest, TiedByWeightsBreakOnTheElement) {
  LoadData();
  for (const SortOrderCase& c : kByTieCases) {
    SCOPED_TRACE(SortOrderCaseName(c));
    ExpectReply(c);
    ExpectStore(c);
  }
}

// Rule 2: under ALPHA BY a weight key that does not exist, or is not a string, sorts before every
// weight that does, the empty string included, and DESC puts it last.
TEST_F(GenericSortOrderTest, AlphaByPutsAMissingWeightFirst) {
  LoadData();
  for (const SortOrderCase& c : kAlphaByCases) {
    SCOPED_TRACE(SortOrderCaseName(c));
    ExpectReply(c);
    ExpectStore(c);
  }
}

// What KeyDB leaves undetermined (ALPHA BY ties) is deterministic here: see the table above.
TEST_F(GenericSortOrderTest, AlphaByTiesBreakOnTheElement) {
  LoadData();
  for (const SortOrderCase& c : kAlphaByTieCases) {
    SCOPED_TRACE(SortOrderCaseName(c));
    ExpectReply(c);
    ExpectStore(c);
  }
}

// Rule 3: BY nosort on a SET that is stored, or sorted inside a script, comes out ALPHA by the
// element (note "1" "10" "2" for the intset), with GET, DESC and LIMIT applied after the sort.
TEST_F(GenericSortOrderTest, NosortSetThatIsStoredOrScriptedIsSortedAlpha) {
  LoadData();
  for (const SortOrderCase& c : kNosortSetCases) {
    SCOPED_TRACE(SortOrderCaseName(c));
    ExpectStore(c);
    ExpectScript(c);
  }

  // Inside MULTI a STORE is stored all the same.
  Run({"multi"});
  Run({"sort", "s", "BY", "nosort", "STORE", "multi-dst"});
  ASSERT_THAT(Run({"exec"}), RespElementsAre(IntArg(10)));
  EXPECT_THAT(Run({"lrange", "multi-dst", "0", "-1"}),
              RespElementsAre("a", "b", "c", "d", "e", "f", "g", "h", "i", "j"));
}

// A SET under BY nosort that is neither stored nor scripted keeps its iteration order, which no
// one defines (Redis and KeyDB leave it open too): the members, each once, and LIMIT cuts that
// walk. This is not an order to assert; it pins that the plain reply is not sorted away or lost.
TEST_F(GenericSortOrderTest, NosortSetReplyKeepsItsMembers) {
  LoadData();
  const vector<string> members{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"};

  for (string_view name : {"SORT", "SORT_RO"}) {
    EXPECT_THAT(Run({name, "s", "BY", "nosort"}), RespArray(UnorderedElementsAreArray(members)));

    auto resp = Run({name, "s", "BY", "nosort", "LIMIT", "2", "3"});
    ASSERT_THAT(resp, ArrLen(3));
    set<string> distinct;
    for (const RespExpr& e : resp.GetVec())
      distinct.insert(e.GetString());
    EXPECT_EQ(distinct.size(), 3u);
    EXPECT_TRUE(all_of(distinct.begin(), distinct.end(), [&](const string& m) {
      return find(members.begin(), members.end(), m) != members.end();
    }));
  }

  Run({"multi"});
  Run({"sort", "s", "BY", "nosort"});
  auto resp = Run({"exec"});
  ASSERT_THAT(resp, ArrLen(1));
  EXPECT_THAT(resp.GetVec()[0], RespArray(UnorderedElementsAreArray(members)));
}

// Rule 4: BY nosort on a LIST or a ZSET walks it from the tail under DESC, in the reply and into
// STORE, and LIMIT counts along that walk. ASC is the native order, as before.
TEST_F(GenericSortOrderTest, NosortListAndZsetHonourDesc) {
  LoadData();
  for (const SortOrderCase& c : kNosortWalkCases) {
    SCOPED_TRACE(SortOrderCaseName(c));
    ExpectReply(c);
    ExpectStore(c);
    ExpectScript(c);
  }
}

// `LIMIT 1 4294967295` is a valid count in Redis and KeyDB (everything from the offset on), and
// summed with the offset it wrapped around a uint32: the range ended before it began, which
// crashed the server (BY forms) or replied garbage (plain form). Found while checking decision 34's
// forms against KeyDB.
TEST_F(GenericSortOrderTest, LimitCountBeyondUint32DoesNotOverflow) {
  LoadData();
  for (const SortOrderCase& c : kWideLimitCases) {
    SCOPED_TRACE(SortOrderCaseName(c));
    ExpectReply(c);
    ExpectStore(c);
  }
}

}  // namespace dfly
