// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include <gmock/gmock.h>

extern "C" {
#include "redis/crc64.h"
#include "redis/listpack.h"
#include "redis/redis_aux.h"
#include "redis/stream.h"
#include "redis/zmalloc.h"
}

#include <absl/cleanup/cleanup.h>
#include <absl/flags/reflection.h>
#include <mimalloc.h>

#include <algorithm>

#include "base/flags.h"
#include "base/gtest.h"
#include "base/logging.h"
#include "core/bloom.h"
#include "core/cuckoo.h"
#include "core/intent_lock.h"
#include "facade/facade_test.h"  // needed to find operator== for RespExpr.
#include "io/file.h"
#include "io/file_util.h"
#include "server/engine_shard_set.h"
#include "server/journal/journal.h"
#include "server/journal/serializer.h"
#include "server/journal/types.h"
#include "server/multi_master.h"
#include "server/rdb_extensions.h"
#include "server/rdb_load.h"
#include "server/rdb_save.h"
#include "server/serializer_commons.h"
#include "server/server_state.h"
#include "server/snapshot.h"
#include "server/test_utils.h"
#include "strings/human_readable.h"

namespace rng = std::ranges;

using namespace testing;
using namespace std;
using namespace util;
using namespace facade;
using absl::SetFlag;
using absl::StrCat;

ABSL_DECLARE_FLAG(int32, list_compress_depth);
ABSL_DECLARE_FLAG(int32, list_max_listpack_size);
ABSL_DECLARE_FLAG(dfly::CompressionMode, compression_mode);
ABSL_DECLARE_FLAG(bool, rdb_ignore_expiry);
ABSL_DECLARE_FLAG(uint32_t, num_shards);
ABSL_DECLARE_FLAG(bool, rdb_sbf_chunked);
ABSL_DECLARE_FLAG(bool, serialize_hnsw_index);
ABSL_DECLARE_FLAG(bool, deserialize_hnsw_index);
ABSL_DECLARE_FLAG(std::string, dbfilename);
ABSL_DECLARE_FLAG(bool, active_replica);

namespace {

uint64_t EncodeModuleId(std::string_view name, int ver) {
  CHECK_LE(name.size(), 9u) << "Module names are encoded in at most 9 chars";
  constexpr std::string_view kCharset =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
  uint64_t bits = 0;
  for (char c : name) {
    size_t idx = kCharset.find(c);
    bits = (bits << 6) | idx;
  }
  return (bits << 10) | static_cast<uint64_t>(ver);
}

}  // namespace

namespace dfly {

static const auto kMatchNil = ArgType(RespExpr::NIL);

class RdbTest : public BaseFamilyTest {
 protected:
  void SetUp();

  io::FileSource GetSource(string name);

  std::error_code LoadRdb(const string& filename) {
    return pp_->at(0)->Await([&] {
      io::FileSource fs = GetSource(filename);

      RdbLoadContext load_context;
      RdbLoader loader(service_.get(), &load_context);
      return loader.Load(&fs);
    });
  }
};

void RdbTest::SetUp() {
  // Setting max_memory_limit must be before calling  InitWithDbFilename
  max_memory_limit = 40000000;
  absl::SetFlag(&FLAGS_serialize_hnsw_index, true);
  absl::SetFlag(&FLAGS_deserialize_hnsw_index, true);
  InitWithDbFilename();
  CHECK_EQ(zmalloc_used_memory_tl, 0);
}

inline const uint8_t* to_byte(const void* s) {
  return reinterpret_cast<const uint8_t*>(s);
}

io::FileSource RdbTest::GetSource(string name) {
  string rdb_file = base::ProgramRunfile("testdata/" + name);
  auto open_res = io::OpenRead(rdb_file, io::ReadonlyFile::Options{});
  CHECK(open_res) << rdb_file;

  return io::FileSource(*open_res);
}

static string FloatToBytes(float f) {
  return string(reinterpret_cast<const char*>(&f), sizeof(float));
}

TEST_F(RdbTest, SnapshotIdTest) {
  absl::SetFlag(&FLAGS_num_shards, num_threads_);
  ResetService();

  EXPECT_EQ(Run({"mset", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11"}), "OK");

  Run({"save", "df", "test_dump"});

  absl::SetFlag(&FLAGS_num_shards, num_threads_ - 1);
  ResetService();

  EXPECT_EQ(Run({"mset", "test1", "val1", "test2", "val2"}), "OK");

  Run({"save", "df", "test_dump"});

  ResetService();

  EXPECT_EQ(Run({"dfly", "load", "test_dump-summary.dfs"}), "OK");

  auto resp = Run({"keys", "*"});
  EXPECT_THAT(resp.GetVec(), UnorderedElementsAre("test1", "test2"));
}

TEST_F(RdbTest, Crc) {
  std::string_view s{"TEST"};

  uint64_t c = crc64(0, to_byte(s.data()), s.size());
  ASSERT_NE(c, 0);

  uint64_t c2 = crc64(c, to_byte(s.data()), s.size());
  EXPECT_NE(c, c2);

  uint64_t c3 = crc64(c, to_byte(&c), sizeof(c));
  EXPECT_EQ(c3, 0);

  s = "COOLTEST";
  c = crc64(0, to_byte(s.data()), 8);
  c2 = crc64(0, to_byte(s.data()), 4);
  c3 = crc64(c2, to_byte(s.data() + 4), 4);
  EXPECT_EQ(c, c3);

  c2 = crc64(0, to_byte(s.data() + 4), 4);
  c3 = crc64(c2, to_byte(s.data()), 4);
  EXPECT_NE(c, c3);
}

TEST_F(RdbTest, LoadEmpty) {
  auto ec = LoadRdb("empty.rdb");
  ASSERT_FALSE(ec) << ec;
}

TEST_F(RdbTest, LoadSmall6) {
  // The rdb file contians keys that already expired, we want to continue loading them in this test.
  absl::FlagSaver fs;
  SetTestFlag("rdb_ignore_expiry", "true");

  auto ec = LoadRdb("redis6_small.rdb");

  ASSERT_FALSE(ec) << ec.message();

  auto resp = Run({"scan", "0"});

  ASSERT_THAT(resp, ArrLen(2));
  EXPECT_THAT(StrArray(resp.GetVec()[1]),
              UnorderedElementsAre("list1", "hset_zl", "list2", "zset_sl", "intset", "set1",
                                   "zset_zl", "hset_ht", "intkey", "strkey"));
  EXPECT_THAT(Run({"get", "intkey"}), "1234567");
  EXPECT_THAT(Run({"get", "strkey"}), "abcdefghjjjjjjjjjj");

  resp = Run({"smembers", "intset"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(),
              UnorderedElementsAre("111", "222", "1234", "3333", "4444", "67899", "76554"));

  // TODO: when we implement PEXPIRETIME we will be able to do it directly.
  int ttl = CheckedInt({"ttl", "set1"});    // should expire at 1747008000.
  EXPECT_GT(ttl + time(NULL), 1747007000);  // left 1000 seconds margin in case the clock is off.

  Run({"select", "1"});
  ASSERT_EQ(10, CheckedInt({"dbsize"}));
  ASSERT_EQ(128, CheckedInt({"strlen", "longggggggggggggggkeyyyyyyyyyyyyy:9"}));
  resp = Run({"script", "exists", "4ca238f611c9d0ae4e9a75a5dbac22aedc379801",
              "282297a0228f48cd3fc6a55de6316f31422f5d17"});
  ASSERT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp.GetVec(), ElementsAre(IntArg(1), IntArg(1)));
}

TEST_F(RdbTest, Stream) {
  auto ec = LoadRdb("redis6_stream.rdb");

  ASSERT_FALSE(ec) << ec.message();

  auto resp = Run({"type", "key:10"});
  EXPECT_EQ(resp, "stream");

  resp = Run({"xinfo", "groups", "key:0"});
  EXPECT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp.GetVec()[0],
              RespElementsAre("name", "g1", "consumers", 0, "pending", 0, "last-delivered-id",
                              "1655444851524-3", "entries-read", 128, "lag", 0));
  EXPECT_THAT(resp.GetVec()[1],
              RespElementsAre("name", "g2", "consumers", 1, "pending", 0, "last-delivered-id",
                              "1655444851523-1", "entries-read", kMatchNil, "lag", kMatchNil));

  resp = Run({"xinfo", "groups", "key:1"});  // test dereferences array of size 1
  EXPECT_THAT(resp,
              RespElementsAre(RespElementsAre("name", "g2", "consumers", IntArg(0), "pending",
                                              IntArg(0), "last-delivered-id", "1655444851523-1",
                                              "entries-read", kMatchNil, "lag", kMatchNil)));

  resp = Run({"xinfo", "groups", "key:2"});
  EXPECT_THAT(resp, ArrLen(0));

  Run({"save"});
}

TEST_F(RdbTest, ComressionModeSaveDragonflyAndReload) {
  Run({"debug", "populate", "50000"});
  ASSERT_EQ(50000, CheckedInt({"dbsize"}));
  // Check keys inserted are lower than 50,000.
  auto resp = Run({"keys", "key:[5-9][0-9][0-9][0-9][0-9]*"});
  EXPECT_EQ(resp.GetVec().size(), 0);

  for (auto mode : {CompressionMode::NONE, CompressionMode::SINGLE_ENTRY,
                    CompressionMode::MULTI_ENTRY_ZSTD, CompressionMode::MULTI_ENTRY_LZ4}) {
    SetFlag(&FLAGS_compression_mode, mode);
    RespExpr resp = Run({"save", "df"});
    ASSERT_EQ(resp, "OK");

    if (mode == CompressionMode::MULTI_ENTRY_ZSTD || mode == CompressionMode::MULTI_ENTRY_LZ4) {
      EXPECT_GE(GetMetrics().coordinator_stats.compressed_blobs, 1);
    }

    auto save_info = service_->server_family().GetLastSaveInfo();
    resp = Run({"dfly", "load", save_info.file_name});
    ASSERT_EQ(resp, "OK");
    ASSERT_EQ(50000, CheckedInt({"dbsize"}));
  }
}

TEST_F(RdbTest, RdbLoaderOnReadCompressedDataShouldNotEnterEnsureReadFlow) {
  SetFlag(&FLAGS_compression_mode, CompressionMode::MULTI_ENTRY_ZSTD);
  for (int i = 0; i < 1000; ++i) {
    Run({"set", StrCat(i), "1"});
  }
  RespExpr resp = Run({"save", "df"});
  ASSERT_EQ(resp, "OK");

  auto save_info = service_->server_family().GetLastSaveInfo();
  resp = Run({"dfly", "load", save_info.file_name});
  ASSERT_EQ(resp, "OK");
}

TEST_F(RdbTest, SaveLoadSticky) {
  Run({"set", "a", "1"});
  Run({"set", "b", "2"});
  Run({"set", "c", "3"});
  Run({"stick", "a", "b"});
  RespExpr resp = Run({"save", "df"});
  ASSERT_EQ(resp, "OK");

  resp = Run({"debug", "reload"});
  ASSERT_EQ(resp, "OK");
  EXPECT_THAT(Run({"get", "a"}), "1");
  EXPECT_THAT(Run({"get", "b"}), "2");
  EXPECT_THAT(Run({"get", "c"}), "3");
  EXPECT_THAT(Run({"stick", "a", "b"}), IntArg(0));
  EXPECT_THAT(Run({"stick", "c"}), IntArg(1));
}

TEST_F(RdbTest, ReloadSetSmallStringBug) {
  auto str = absl::StrCat(std::string(32, 'X'));
  Run({"set", "small_key", str});
  auto resp = Run({"debug", "reload"});
  ASSERT_EQ(resp, "OK");
}

TEST_F(RdbTest, Reload) {
  absl::FlagSaver fs;

  SetFlag(&FLAGS_list_compress_depth, 1);
  SetFlag(&FLAGS_list_max_listpack_size, 1);  // limit listpack to a single element.

  Run({"set", "string_key", "val"});
  Run({"set", "large_key", string(511, 'L')});
  Run({"set", "huge_key", string((1 << 17) - 10, 'H')});

  Run({"sadd", "set_key1", "val1", "val2"});
  Run({"sadd", "intset_key", "1", "2", "3"});
  Run({"hset", "small_hset", "field1", "val1", "field2", "val2"});
  Run({"hset", "large_hset", "field1", string(510, 'V'), string(120, 'F'), "val2"});

  Run({"rpush", "list_key1", "val", "val2"});
  Run({"rpush", "list_key2", "head", string(511, 'a'), string(500, 'b'), "tail"});

  Run({"zadd", "zs1", "1.1", "a", "-1.1", "b"});
  Run({"zadd", "zs2", "1.1", string(510, 'a'), "-1.1", string(502, 'b')});

  Run({"hset", "large_keyname", string(240, 'X'), "-5"});
  Run({"hset", "large_keyname", string(240, 'Y'), "-500"});
  Run({"hset", "large_keyname", string(240, 'Z'), "-50000"});

  auto resp = Run({"debug", "reload"});
  ASSERT_EQ(resp, "OK");

  EXPECT_EQ(2, CheckedInt({"scard", "set_key1"}));
  EXPECT_EQ(3, CheckedInt({"scard", "intset_key"}));
  EXPECT_EQ(2, CheckedInt({"hlen", "small_hset"}));
  EXPECT_EQ(2, CheckedInt({"hlen", "large_hset"}));
  EXPECT_EQ(4, CheckedInt({"LLEN", "list_key2"}));
  EXPECT_EQ(2, CheckedInt({"ZCARD", "zs1"}));
  EXPECT_EQ(2, CheckedInt({"ZCARD", "zs2"}));

  EXPECT_EQ(-5, CheckedInt({"hget", "large_keyname", string(240, 'X')}));
  EXPECT_EQ(-500, CheckedInt({"hget", "large_keyname", string(240, 'Y')}));
  EXPECT_EQ(-50000, CheckedInt({"hget", "large_keyname", string(240, 'Z')}));
}

TEST_F(RdbTest, ReloadTtl) {
  Run({"set", "key", "val"});
  Run({"expire", "key", "1000"});
  Run({"debug", "reload"});
  EXPECT_LT(990, CheckedInt({"ttl", "key"}));
}

TEST_F(RdbTest, ReloadExpired) {
  Run({"set", "key", "val"});
  Run({"expire", "key", "2"});
  RespExpr resp = Run({"save", "df"});
  ASSERT_EQ(resp, "OK");
  auto save_info = service_->server_family().GetLastSaveInfo();
  AdvanceTime(2000);
  resp = Run({"dfly", "load", save_info.file_name});
  ASSERT_EQ(resp, "OK");
  resp = Run({"get", "key"});
  ASSERT_THAT(resp, ArgType(RespExpr::NIL));
}

TEST_F(RdbTest, HashmapExpiry) {
  // Add non-expiring elements
  Run({"hset", "key", "key1", "val1", "key2", "val2"});
  Run({"debug", "reload"});
  EXPECT_THAT(Run({"hgetall", "key"}),
              RespArray(UnorderedElementsAre("key1", "val1", "key2", "val2")));

  // Add expiring elements
  Run({"hsetex", "key", "5", "key3", "val3", "key4", "val4"});
  Run({"debug", "reload"});  // Reload before expiration
  EXPECT_THAT(Run({"hgetall", "key"}),
              RespArray(UnorderedElementsAre("key1", "val1", "key2", "val2", "key3", "val3", "key4",
                                             "val4")));
  AdvanceTime(10'000);
  EXPECT_THAT(Run({"hgetall", "key"}),
              RespArray(UnorderedElementsAre("key1", "val1", "key2", "val2")));

  Run({"hsetex", "key", "5", "key5", "val5", "key6", "val6"});
  EXPECT_THAT(Run({"hgetall", "key"}),
              RespArray(UnorderedElementsAre("key1", "val1", "key2", "val2", "key5", "val5", "key6",
                                             "val6")));
  AdvanceTime(10'000);
  Run({"debug", "reload"});  // Reload after expiration
  EXPECT_THAT(Run({"hgetall", "key"}),
              RespArray(UnorderedElementsAre("key1", "val1", "key2", "val2")));
}

TEST_F(RdbTest, SaveLoadExpiredValuesHmap) {
  // Add expiring elements
  Run({"hsetex", "hkey", "1", "key3", "val3", "key4", "val4"});

  RespExpr resp = Run({"TYPE", "hkey"});
  ASSERT_EQ(resp, "hash");

  AdvanceTime(10'000);
  resp = Run({"save", "RDB"});
  ASSERT_EQ(resp, "OK");

  resp = Run({"TYPE", "hkey"});
  ASSERT_EQ(resp, "hash");

  Run({"debug", "reload"});

  resp = Run({"TYPE", "hkey"});
  ASSERT_EQ(resp, "none");
}

TEST_F(RdbTest, SaveLoadExpiredValuesHugeHmap) {
  constexpr auto keys_num = 10000;
  for (int i = 0; i < keys_num; ++i) {
    Run({"hsetex", "hkey", "1", absl::StrCat("key", i), "val"});
  }

  ASSERT_EQ(keys_num, CheckedInt({"hlen", "hkey"}));

  AdvanceTime(10'000);

  Run({"debug", "reload"});

  ASSERT_EQ(Run({"TYPE", "hkey"}), "none");

  // with one value that isn't expired
  for (int i = 0; i < keys_num; ++i) {
    Run({"hsetex", "hkey", "1", absl::StrCat("key", i), "val"});
  }

  Run({"hset", "hkey", base::RandStr(20), "val"});

  ASSERT_EQ(keys_num + 1, CheckedInt({"hlen", "hkey"}));

  AdvanceTime(10'000);

  Run({"debug", "reload"});

  ASSERT_EQ(1, CheckedInt({"hlen", "hkey"}));
}

TEST_F(RdbTest, SaveLoadExpiredValuesSSet) {
  // Add expiring elements
  Run({"saddex", "skey", "1", "key3", "key4"});

  RespExpr resp = Run({"TYPE", "skey"});
  ASSERT_EQ(resp, "set");

  AdvanceTime(10'000);
  resp = Run({"save", "RDB"});
  ASSERT_EQ(resp, "OK");

  resp = Run({"TYPE", "skey"});
  ASSERT_EQ(resp, "set");

  Run({"debug", "reload"});

  resp = Run({"TYPE", "skey"});
  ASSERT_EQ(resp, "none");
}

TEST_F(RdbTest, SaveLoadExpiredValuesHugeSet) {
  constexpr auto keys_num = 10000;
  for (int i = 0; i < keys_num; ++i) {
    Run({"saddex", "skey", "1", absl::StrCat("key", i)});
  }

  ASSERT_EQ(keys_num, CheckedInt({"scard", "skey"}));

  AdvanceTime(10'000);

  Run({"debug", "reload"});

  ASSERT_EQ(Run({"TYPE", "skey"}), "none");

  // with one value that isn't expired
  for (int i = 0; i < keys_num; ++i) {
    Run({"saddex", "skey", "1", absl::StrCat("key", i)});
  }
  Run({"sadd", "skey", base::RandStr(20)});

  ASSERT_EQ(keys_num + 1, CheckedInt({"scard", "skey"}));

  AdvanceTime(10'000);

  Run({"debug", "reload"});

  ASSERT_EQ(1, CheckedInt({"scard", "skey"}));
}

TEST_F(RdbTest, SetExpiry) {
  // Add non-expiring elements
  Run({"sadd", "key", "key1", "key2"});
  Run({"debug", "reload"});
  EXPECT_THAT(Run({"smembers", "key"}), RespArray(UnorderedElementsAre("key1", "key2")));

  // Add expiring elements
  Run({"saddex", "key", "5", "key3", "key4"});
  Run({"debug", "reload"});  // Reload before expiration
  EXPECT_THAT(Run({"smembers", "key"}),
              RespArray(UnorderedElementsAre("key1", "key2", "key3", "key4")));
  AdvanceTime(10'000);
  EXPECT_THAT(Run({"smembers", "key"}), RespArray(UnorderedElementsAre("key1", "key2")));

  Run({"saddex", "key", "5", "key5", "key6"});
  EXPECT_THAT(Run({"smembers", "key"}),
              RespArray(UnorderedElementsAre("key1", "key2", "key5", "key6")));
  AdvanceTime(10'000);
  Run({"debug", "reload"});  // Reload after expiration
  EXPECT_THAT(Run({"smembers", "key"}), RespArray(UnorderedElementsAre("key1", "key2")));
}

// Tests that integer elements in sets with expiry are not corrupted during RDB load.
// This test covers the bug where ToSV() internal buffer was being reused,
// causing string corruption when loading integer elements.
TEST_F(RdbTest, SetExpiryInteger) {
  // Add integer elements with expiry - integers trigger ToSV() buffer reuse
  Run({"saddex", "s1", "10", "1", "2", "3", "12345", "67890"});

  // Verify elements are added correctly
  EXPECT_EQ(5, CheckedInt({"scard", "s1"}));
  EXPECT_THAT(Run({"smembers", "s1"}),
              RespArray(UnorderedElementsAre("1", "2", "3", "12345", "67890")));

  // Reload from RDB - this would trigger the corruption bug
  Run({"debug", "reload"});

  // Verify integers were loaded correctly without corruption
  EXPECT_EQ(5, CheckedInt({"scard", "s1"}));
  EXPECT_THAT(Run({"smembers", "s1"}),
              RespArray(UnorderedElementsAre("1", "2", "3", "12345", "67890")));

  // Verify all elements are actually in the set (no duplicates from corruption)
  EXPECT_THAT(Run({"sismember", "s1", "1"}), IntArg(1));
  EXPECT_THAT(Run({"sismember", "s1", "2"}), IntArg(1));
  EXPECT_THAT(Run({"sismember", "s1", "3"}), IntArg(1));
  EXPECT_THAT(Run({"sismember", "s1", "12345"}), IntArg(1));
  EXPECT_THAT(Run({"sismember", "s1", "67890"}), IntArg(1));
}

TEST_F(RdbTest, SaveFlush) {
  Run({"debug", "populate", "500000"});

  auto save_fb = pp_->at(1)->LaunchFiber([&] {
    RespExpr resp = Run({"save"});
    ASSERT_EQ(resp, "OK");
  });

  do {
    usleep(10);
  } while (!service_->server_family().TEST_IsSaving());

  Run({"flushdb"});
  save_fb.Join();
  auto save_info = service_->server_family().GetLastSaveInfo();
  ASSERT_EQ(1, save_info.freq_map.size());
  auto& k_v = save_info.freq_map.front();
  EXPECT_EQ("string", k_v.first);
  EXPECT_EQ(500000, k_v.second);
}

TEST_F(RdbTest, SaveManyDbs) {
  Run({"debug", "populate", "50000"});
  pp_->at(1)->Await([&] {
    Run({"select", "1"});
    Run({"debug", "populate", "10000"});
  });

  auto metrics = GetMetrics();
  ASSERT_EQ(2, metrics.db_stats.size());
  EXPECT_EQ(50000, metrics.db_stats[0].key_count);
  EXPECT_EQ(10000, metrics.db_stats[1].key_count);

  auto save_fb = pp_->at(0)->LaunchFiber([&] {
    RespExpr resp = Run({"save"});
    ASSERT_EQ(resp, "OK");
  });

  do {
    usleep(10);
  } while (!service_->server_family().TEST_IsSaving());

  pp_->at(1)->Await([&] {
    Run({"select", "1"});
    for (unsigned i = 0; i < 1000; ++i) {
      Run({"set", StrCat("abc", i), "bar"});
    }
  });

  save_fb.Join();

  auto save_info = service_->server_family().GetLastSaveInfo();
  ASSERT_EQ(1, save_info.freq_map.size());
  auto& k_v = save_info.freq_map.front();

  EXPECT_EQ("string", k_v.first);
  EXPECT_EQ(60000, k_v.second);
  auto resp = Run({"debug", "reload", "NOSAVE"});
  EXPECT_EQ(resp, "OK");

  metrics = GetMetrics();
  ASSERT_EQ(2, metrics.db_stats.size());
  EXPECT_EQ(50000, metrics.db_stats[0].key_count);
  EXPECT_EQ(10000, metrics.db_stats[1].key_count);
  if (metrics.db_stats[1].key_count != 10000) {
    Run({"select", "1"});
    resp = Run({"scan", "0", "match", "ab*"});
    StringVec vec = StrArray(resp.GetVec()[1]);
    for (const auto& s : vec) {
      LOG(ERROR) << "Bad key: " << s;
    }
  }
}

TEST_F(RdbTest, HMapBugs) {
  // Force kEncodingStrMap2 encoding.
  server.max_map_field_len = 0;
  Run({"hset", "hmap1", "key1", "val", "key2", "val2"});
  Run({"hset", "hmap2", "key1", string(690557, 'a')});

  server.max_map_field_len = 32;
  Run({"debug", "reload"});
  EXPECT_EQ(2, CheckedInt({"hlen", "hmap1"}));
}

TEST_F(RdbTest, Issue1305) {
  /***************
   * The code below crashes because of the weird listpack API that assumes that lpInsert
   * pointers are null then it should do deletion :(. See lpInsert comments for more info.

     uint8_t* lp = lpNew(128);
     lpAppend(lp, NULL, 0);
     lpFree(lp);

  */

  // Force kEncodingStrMap2 encoding.
  server.max_map_field_len = 0;
  Run({"hset", "hmap", "key1", "val", "key2", ""});

  server.max_map_field_len = 32;
  Run({"debug", "reload"});
  EXPECT_EQ(2, CheckedInt({"hlen", "hmap"}));
}

TEST_F(RdbTest, JsonTest) {
  string_view data[] = {
      R"({"a":1})"sv,                          //
      R"([1,2,3,4,5,6])"sv,                    //
      R"({"a":1.0,"b":[1,2],"c":"value"})"sv,  //
      R"({"a":{"a":{"a":{"a":1}}}})"sv         //
  };

  for (auto test : data) {
    Run({"json.set", "doc", "$", test});
    auto dump = Run({"dump", "doc"});
    Run({"del", "doc"});
    Run({"restore", "doc", "0", facade::ToSV(dump.GetBuf())});
    auto res = Run({"json.get", "doc"});
    ASSERT_EQ(res, test);
  }
}

// hll.rdb has 2 keys: "key-dense" and "key-sparse", both are HLL with a single added value "1".
class HllRdbTest : public RdbTest, public testing::WithParamInterface<string> {};

TEST_P(HllRdbTest, Hll) {
  LOG(INFO) << " max memory: " << max_memory_limit
            << " used_mem_current: " << used_mem_current.load();
  auto ec = LoadRdb("hll.rdb");

  ASSERT_FALSE(ec) << ec.message();

  EXPECT_EQ(CheckedInt({"pfcount", GetParam()}), 1);

  EXPECT_EQ(CheckedInt({"pfcount", GetParam(), "non-existing"}), 1);

  EXPECT_EQ(CheckedInt({"pfadd", "key2", "2"}), 1);
  EXPECT_EQ(CheckedInt({"pfcount", GetParam(), "key2"}), 2);

  EXPECT_EQ(CheckedInt({"pfadd", GetParam(), "2"}), 1);
  EXPECT_EQ(CheckedInt({"pfcount", GetParam()}), 2);

  EXPECT_EQ(Run({"pfmerge", "key3", GetParam(), "key2"}), "OK");
  EXPECT_EQ(CheckedInt({"pfcount", "key3"}), 2);
}

INSTANTIATE_TEST_SUITE_P(HllRdbTest, HllRdbTest, Values("key-sparse", "key-dense"));

TEST_F(RdbTest, LoadSmall7) {
  // Contains 3 keys
  // 1. A list called my-list encoded as RDB_TYPE_LIST_QUICKLIST_2
  // 2. A hashtable called my-hset encoded as RDB_TYPE_HASH_LISTPACK
  // 3. A set called my-set encoded as RDB_TYPE_SET_LISTPACK
  // 4. A zset called my-zset encoded as RDB_TYPE_ZSET_LISTPACK
  auto ec = LoadRdb("redis7_small.rdb");

  ASSERT_FALSE(ec) << ec.message();

  auto resp = Run({"scan", "0"});

  ASSERT_THAT(resp, ArrLen(2));

  EXPECT_THAT(StrArray(resp.GetVec()[1]),
              UnorderedElementsAre("my-set", "my-hset", "my-list", "zset"));

  resp = Run({"smembers", "my-set"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), UnorderedElementsAre("redis", "acme"));

  resp = Run({"hgetall", "my-hset"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), UnorderedElementsAre("acme", "44", "field", "22"));

  resp = Run({"lrange", "my-list", "0", "-1"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), UnorderedElementsAre("list1", "list2"));

  resp = Run({"zrange", "zset", "0", "-1"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), ElementsAre("einstein", "schrodinger"));
}

TEST_F(RdbTest, RedisJson) {
  // RDB file generated via:
  // ./redis-server --save "" --appendonly no --loadmodule ../lib/rejson.so
  // and then:
  // JSON.SET json-str $ '"hello"'
  // JSON.SET json-arr $ "[1, true, \"hello\", 3.14]"
  // JSON.SET json-obj $
  // '{"company":"DragonflyDB","product":"Dragonfly","website":"https://dragondlydb.io","years-active":[2021,2022,2023,2024,"and
  // more!"]}'
  auto ec = LoadRdb("redis_json.rdb");

  ASSERT_FALSE(ec) << ec.message();

  EXPECT_EQ(Run({"JSON.GET", "json-str"}), "\"hello\"");
  EXPECT_EQ(Run({"JSON.GET", "json-arr"}), "[1,true,\"hello\",3.14]");
  EXPECT_EQ(Run({"JSON.GET", "json-obj"}),
            "{\"company\":\"DragonflyDB\",\"product\":\"Dragonfly\",\"website\":\"https://"
            "dragondlydb.io\",\"years-active\":[2021,2022,2023,2024,\"and more!\"]}");
}

TEST_F(RdbTest, SBF) {
  EXPECT_THAT(Run({"BF.ADD", "k", "1"}), IntArg(1));
  Run({"debug", "reload"});
  EXPECT_EQ(Run({"type", "k"}), "MBbloom--");
  EXPECT_THAT(Run({"BF.EXISTS", "k", "1"}), IntArg(1));
}

TEST_F(RdbTest, SBFLargeFilterChunking) {
  max_memory_limit = 200000000;

  // Using this set of parameters for the BF.RESERVE command resulted in a
  // filter size large enough to require chunking (> 64 MB).
  const double error_rate = 0.001;
  const size_t capacity = 50'000'000;
  const size_t num_items = 100;

  size_t collisions = 0;

  Run({"BF.RESERVE", "large_key", std::to_string(error_rate), std::to_string(capacity)});
  for (size_t i = 0; i < num_items; i++) {
    auto res = Run({"BF.ADD", "large_key", absl::StrCat("item", i)});
    if (*res.GetInt() == 0)
      collisions++;
  }
  EXPECT_LT(static_cast<double>(collisions) / num_items, error_rate);

  Run({"debug", "reload"});
  EXPECT_EQ(Run({"type", "large_key"}), "MBbloom--");

  for (size_t i = 0; i < num_items; i++) {
    EXPECT_THAT(Run({"BF.EXISTS", "large_key", absl::StrCat("item", i)}), IntArg(1));
  }
}

TEST_F(RdbTest, RestoreSearchIndexNameStartingWithColon) {
  // Create an index with a name that starts with ':' and add a sample document
  EXPECT_EQ(Run({"FT.CREATE", ":Order:index", "ON", "HASH", "PREFIX", "1", ":Order:", "SCHEMA",
                 "customer_name", "AS", "customer_name", "TEXT", "status", "AS", "status", "TAG"}),
            "OK");

  EXPECT_THAT(Run({"HSET", ":Order:1", "customer_name", "John", "status", "new"}), IntArg(2));

  // Save and reload to ensure the index definition is persisted and restored
  EXPECT_EQ(Run({"save", "df"}), "OK");
  EXPECT_EQ(Run({"debug", "reload"}), "OK");

  // Verify a basic search works on the restored index
  auto search = Run({"FT.SEARCH", ":Order:index", "John"});
  ASSERT_THAT(search, ArgType(RespExpr::ARRAY));
  const auto& v = search.GetVec();
  ASSERT_FALSE(v.empty());
  EXPECT_THAT(v.front(), IntArg(1));
}

// Parametrized test for RestoreVectorSearchIndexHnsw with varying document counts
class HnswRestoreTest : public RdbTest, public testing::WithParamInterface<int> {};

TEST_P(HnswRestoreTest, RestoreVectorSearchIndexHnsw) {
  int num_docs = GetParam();

  EXPECT_EQ(
      Run({"FT.CREATE", "only_vec_idx", "ON", "HASH", "PREFIX", "1", "doc:", "SCHEMA", "embedding",
           "VECTOR", "HNSW", "6", "TYPE", "FLOAT32", "DIM", "2", "DISTANCE_METRIC", "L2"}),
      "OK");

  EXPECT_EQ(Run({"FT.CREATE", "vec_idx", "ON",   "HASH",      "PREFIX",          "1",    "doc:",
                 "SCHEMA",    "name",    "TEXT", "embedding", "VECTOR",          "HNSW", "6",
                 "TYPE",      "FLOAT32", "DIM",  "2",         "DISTANCE_METRIC", "L2"}),
            "OK");

  // Insert documents with incrementing vectors
  for (int i = 1; i <= num_docs; ++i) {
    float x = static_cast<float>(i * 2 - 1);
    float y = static_cast<float>(i * 2);
    Run({"HSET", StrCat("doc:", i), "name", StrCat("doc", i), "embedding",
         StrCat(FloatToBytes(x), FloatToBytes(y))});
  }

  LOG(INFO) << "Created " << num_docs << " documents with vector embeddings";

  EXPECT_EQ(Run({"save", "df"}), "OK");
  auto save_info = service_->server_family().GetLastSaveInfo();

  // Reload from the saved file - this should restore the HNSW index, not rebuild it
  // Look for "Restored HNSW index" in logs to verify restoration vs rebuild
  LOG(INFO) << "Reloading from " << save_info.file_name << " - expecting HNSW index restoration";
  EXPECT_EQ(Run({"dfly", "load", save_info.file_name}), "OK");

  // Wait for async index building to complete on both indices
  auto is_indexing_done = [this](string_view idx_name) {
    auto resp = Run({"FT.INFO", idx_name});
    auto arr = resp.GetVec();
    auto it = rng::find_if(arr, [](const auto& e) { return e == "indexing"; });
    return it != arr.end() && (++it)->GetInt() == 0;
  };

  ASSERT_TRUE(WaitUntilCondition([&] { return is_indexing_done("vec_idx"); },
                                 std::chrono::milliseconds(10000)));
  ASSERT_TRUE(WaitUntilCondition([&] { return is_indexing_done("only_vec_idx"); },
                                 std::chrono::milliseconds(10000)));

  // Verify text search still works on the restored index
  auto search = Run({"FT.SEARCH", "vec_idx", "doc1"});
  ASSERT_THAT(search, ArgType(RespExpr::ARRAY));
  const auto& v = search.GetVec();
  ASSERT_FALSE(v.empty());
  EXPECT_THAT(v.front(), IntArg(1));

  // Verify KNN vector search works on the restored index
  // Query vector close to (1.0, 2.0) should find doc:1 as nearest
  string query_vec = StrCat(FloatToBytes(1.1f), FloatToBytes(2.1f));
  auto knn_search = Run({"FT.SEARCH", "vec_idx", "*=>[KNN 2 @embedding $vec]", "PARAMS", "2", "vec",
                         query_vec, "RETURN", "1", "name"});
  ASSERT_THAT(knn_search, ArgType(RespExpr::ARRAY));
  EXPECT_GE(knn_search.GetVec().front().GetInt(), 1);

  // The same check for another index with only vector field
  knn_search = Run({"FT.SEARCH", "only_vec_idx", "*=>[KNN 2 @embedding $vec]", "PARAMS", "2", "vec",
                    query_vec, "RETURN", "1", "name"});
  ASSERT_THAT(knn_search, ArgType(RespExpr::ARRAY));
  EXPECT_GE(knn_search.GetVec().front().GetInt(), 1);

  // Verify total document count matches
  EXPECT_EQ(CheckedInt({"dbsize"}), num_docs);

  LOG(INFO) << "Successfully verified HNSW index restoration with " << num_docs << " documents";
}

INSTANTIATE_TEST_SUITE_P(HnswRestoreTest, HnswRestoreTest, Values(5, 50, 500, 1000),
                         [](const testing::TestParamInfo<int>& info) {
                           return StrCat("Docs", info.param);
                         });

TEST_F(RdbTest, DflyLoadAppend) {
  // Create an RDB with (k1,1) value in it saved as `filename`
  EXPECT_EQ(Run({"set", "k1", "1"}), "OK");
  EXPECT_EQ(Run({"save", "df"}), "OK");
  string filename = service_->server_family().GetLastSaveInfo().file_name;

  // Without APPEND option - db should be flushed
  EXPECT_EQ(Run({"set", "k1", "TO-BE-FLUSHED"}), "OK");
  EXPECT_EQ(Run({"set", "k2", "TO-BE-FLUSHED"}), "OK");
  EXPECT_EQ(Run({"dfly", "load", filename}), "OK");
  EXPECT_THAT(Run({"dbsize"}), IntArg(1));
  EXPECT_EQ(Run({"get", "k1"}), "1");

  // With APPEND option - db shouldn't be flushed, but k1 should be overridden
  EXPECT_EQ(Run({"set", "k1", "TO-BE-OVERRIDDEN"}), "OK");
  EXPECT_EQ(Run({"set", "k2", "2"}), "OK");
  EXPECT_EQ(Run({"dfly", "load", filename, "append"}), "OK");
  EXPECT_THAT(Run({"dbsize"}), IntArg(2));
  EXPECT_EQ(Run({"get", "k1"}), "1");
  EXPECT_EQ(Run({"get", "k2"}), "2");
}

// Tests loading a huge set, where the set is loaded in multiple partial reads.
TEST_F(RdbTest, LoadHugeSet) {
  // Add 2 sets with 100k elements each (note must have more than kMaxBlobLen
  // elements to test partial reads).
  Run({"debug", "populate", "2", "test", "100", "rand", "type", "set", "elements", "100000"});
  ASSERT_EQ(100000, CheckedInt({"scard", "test:0"}));
  ASSERT_EQ(100000, CheckedInt({"scard", "test:1"}));

  RespExpr resp = Run({"save", "df"});
  ASSERT_EQ(resp, "OK");

  auto save_info = service_->server_family().GetLastSaveInfo();
  resp = Run({"dfly", "load", save_info.file_name});
  ASSERT_EQ(resp, "OK");

  ASSERT_EQ(100000, CheckedInt({"scard", "test:0"}));
  ASSERT_EQ(100000, CheckedInt({"scard", "test:1"}));
  auto metrics = GetMetrics();
  EXPECT_GT(metrics.db_stats[0].obj_memory_usage, 24'000'000u);
}

// Tests loading a huge hmap, where the map is loaded in multiple partial
// reads.
TEST_F(RdbTest, LoadHugeHMap) {
  // Add 2 sets with 100k elements each (note must have more than kMaxBlobLen
  // elements to test partial reads).
  Run({"debug", "populate", "2", "test", "100", "rand", "type", "hash", "elements", "100000"});
  ASSERT_EQ(100000, CheckedInt({"hlen", "test:0"}));
  ASSERT_EQ(100000, CheckedInt({"hlen", "test:1"}));

  RespExpr resp = Run({"save", "df"});
  ASSERT_EQ(resp, "OK");

  auto save_info = service_->server_family().GetLastSaveInfo();
  resp = Run({"dfly", "load", save_info.file_name});
  ASSERT_EQ(resp, "OK");

  ASSERT_EQ(100000, CheckedInt({"hlen", "test:0"}));
  ASSERT_EQ(100000, CheckedInt({"hlen", "test:1"}));
  auto metrics = GetMetrics();
  EXPECT_GT(metrics.db_stats[0].obj_memory_usage, 29'000'000u);
}

// Tests loading a huge zset, where the zset is loaded in multiple partial
// reads.
TEST_F(RdbTest, LoadHugeZSet) {
  // Add 2 sets with 100k elements each (note must have more than kMaxBlobLen
  // elements to test partial reads).
  Run({"debug", "populate", "2", "test", "100", "rand", "type", "zset", "elements", "100000"});
  ASSERT_EQ(100000, CheckedInt({"zcard", "test:0"}));
  ASSERT_EQ(100000, CheckedInt({"zcard", "test:1"}));

  RespExpr resp = Run({"save", "df"});
  ASSERT_EQ(resp, "OK");

  auto save_info = service_->server_family().GetLastSaveInfo();
  resp = Run({"dfly", "load", save_info.file_name});
  ASSERT_EQ(resp, "OK");

  ASSERT_EQ(100000, CheckedInt({"zcard", "test:0"}));
  ASSERT_EQ(100000, CheckedInt({"zcard", "test:1"}));
  auto metrics = GetMetrics();
  EXPECT_GT(metrics.db_stats[0].obj_memory_usage, 26'000'000u);
}

// Tests loading a huge list, where the list is loaded in multiple partial
// reads.
TEST_F(RdbTest, LoadHugeList) {
  // Add 2 lists with 100k elements each (note must have more than 512*8Kb
  // elements to test partial reads).
  Run({"debug", "populate", "2", "test", "100", "rand", "type", "list", "elements", "100000"});
  ASSERT_EQ(100000, CheckedInt({"llen", "test:0"}));
  ASSERT_EQ(100000, CheckedInt({"llen", "test:1"}));

  RespExpr resp = Run({"save", "df"});
  ASSERT_EQ(resp, "OK");

  auto save_info = service_->server_family().GetLastSaveInfo();
  resp = Run({"dfly", "load", save_info.file_name});
  ASSERT_EQ(resp, "OK");

  ASSERT_EQ(100000, CheckedInt({"llen", "test:0"}));
  ASSERT_EQ(100000, CheckedInt({"llen", "test:1"}));
  auto metrics = GetMetrics();
  EXPECT_GT(metrics.db_stats[0].obj_memory_usage, 20'000'000u);
}

// Tests loading a huge stream, where the stream is loaded in multiple partial
// reads.
TEST_F(RdbTest, LoadHugeStream) {
  TEST_current_time_ms = 1000;

  // Add a huge stream (test:0) with 2000 entries, and 4 1k elements per entry
  // (note must be more than 512*4kb elements to test partial reads).
  // We add 2000 entries to the stream to ensure that the stream, because populate stream
  // adds only a single entry at a time, with multiple elements in it.

  Run({"debug", "populate", "1", "test", "2000", "rand", "type", "stream", "elements", "8000"});

  ASSERT_EQ(2000, CheckedInt({"xlen", "test:0"}));
  Run({"XGROUP", "CREATE", "test:0", "grp1", "0"});
  Run({"XGROUP", "CREATE", "test:0", "grp2", "0"});
  Run({"XREADGROUP", "GROUP", "grp1", "Alice", "COUNT", "1", "STREAMS", "test:0", ">"});
  Run({"XREADGROUP", "GROUP", "grp2", "Alice", "COUNT", "1", "STREAMS", "test:0", ">"});

  auto resp = Run({"xinfo", "stream", "test:0"});

  EXPECT_THAT(
      resp, RespElementsAre("length", 2000, "radix-tree-keys", 2000, "radix-tree-nodes", 2010,
                            "last-generated-id", "1000-1999", "max-deleted-entry-id", "0-0",
                            "entries-added", 2000, "recorded-first-entry-id", "1000-0", "groups", 2,
                            "first-entry", ArrLen(2), "last-entry", ArrLen(2)));

  resp = Run({"save", "df"});
  ASSERT_EQ(resp, "OK");

  auto save_info = service_->server_family().GetLastSaveInfo();
  resp = Run({"dfly", "load", save_info.file_name});
  ASSERT_EQ(resp, "OK");

  ASSERT_EQ(2000, CheckedInt({"xlen", "test:0"}));
  resp = Run({"xinfo", "stream", "test:0"});
  EXPECT_THAT(
      resp, RespElementsAre("length", 2000, "radix-tree-keys", 2000, "radix-tree-nodes", 2010,
                            "last-generated-id", "1000-1999", "max-deleted-entry-id", "0-0",
                            "entries-added", 2000, "recorded-first-entry-id", "1000-0", "groups", 2,
                            "first-entry", ArrLen(2), "last-entry", ArrLen(2)));
  resp = Run({"xinfo", "groups", "test:0"});
  EXPECT_THAT(resp, RespElementsAre(RespElementsAre("name", "grp1", "consumers", 1, "pending", 1,
                                                    "last-delivered-id", "1000-0", "entries-read",
                                                    1, "lag", 1999),
                                    _));
}

TEST_F(RdbTest, LoadStream2) {
  auto ec = LoadRdb("RDB_TYPE_STREAM_LISTPACKS_2.rdb");
  ASSERT_FALSE(ec) << ec.message();
  auto res = Run({"XINFO", "STREAM", "mystream"});
  ASSERT_THAT(res.GetVec(),
              ElementsAre("length", 2, "radix-tree-keys", 1, "radix-tree-nodes", 2,
                          "last-generated-id", "1732613360686-0", "max-deleted-entry-id", "0-0",
                          "entries-added", 2, "recorded-first-entry-id", "1732613352350-0",
                          "groups", 1, "first-entry", RespElementsAre("1732613352350-0", _),
                          "last-entry", RespElementsAre("1732613360686-0", _)));
}

TEST_F(RdbTest, LoadStream3) {
  auto ec = LoadRdb("RDB_TYPE_STREAM_LISTPACKS_3.rdb");
  ASSERT_FALSE(ec) << ec.message();
  auto res = Run({"XINFO", "STREAM", "mystream"});
  ASSERT_THAT(
      res.GetVec(),
      ElementsAre("length", 2, "radix-tree-keys", 1, "radix-tree-nodes", 2, "last-generated-id",
                  "1732614679549-0", "max-deleted-entry-id", "0-0", "entries-added", 2,
                  "recorded-first-entry-id", "1732614676541-0", "groups", 1, "first-entry",
                  ArgType(RespExpr::ARRAY), "last-entry", ArgType(RespExpr::ARRAY)));
}

TEST_F(RdbTest, SnapshotTooBig) {
  // Run({"debug", "populate", "10000", "foo", "1000"});
  //  usleep(5000);  // let the stats to sync
  max_memory_limit = 100000;
  used_mem_current = 1000000;
  auto resp = Run({"debug", "reload"});
  ASSERT_THAT(resp, ErrArg("Out of memory"));
}

TEST_F(RdbTest, HugeKeyIssue4497) {
  absl::FlagSaver fs;
  SetTestFlag("cache_mode", "true");
  ResetService();

  EXPECT_EQ(Run({"flushall"}), "OK");
  EXPECT_EQ(Run({"debug", "populate", "1", "k", "1000", "rand", "type", "set", "elements", "5000"}),
            "OK");
  EXPECT_EQ(Run({"save", "rdb", "hugekey.rdb"}), "OK");
  EXPECT_EQ(Run({"dfly", "load", "hugekey.rdb"}), "OK");
  EXPECT_EQ(Run({"flushall"}), "OK");
}

TEST_F(RdbTest, HugeKeyIssue4554) {
  absl::FlagSaver fs;
  SetTestFlag("cache_mode", "true");
  // We need to stress one flow/shard such that the others finish early. Lock on hashtags allows
  // that.
  SetTestFlag("lock_on_hashtags", "true");
  ResetService();

  EXPECT_EQ(
      Run({"debug", "populate", "20", "{tmp}", "20", "rand", "type", "set", "elements", "10000"}),
      "OK");
  EXPECT_EQ(Run({"save", "df", "hugekey"}), "OK");
  EXPECT_EQ(Run({"dfly", "load", "hugekey-summary.dfs"}), "OK");
  EXPECT_EQ(Run({"flushall"}), "OK");
}

// ignore_expiry.rdb contains 2 keys which are expired keys
// this test case verifies wheather rdb_ignore_expiry flag is working as expected.
TEST_F(RdbTest, RDBIgnoreExpiryFlag) {
  absl::FlagSaver fs;

  SetTestFlag("rdb_ignore_expiry", "true");
  auto ec = LoadRdb("ignore_expiry.rdb");

  ASSERT_FALSE(ec) << ec.message();

  auto resp = Run({"scan", "0"});

  ASSERT_THAT(resp, ArrLen(2));

  EXPECT_THAT(StrArray(resp.GetVec()[1]), UnorderedElementsAre("test", "test2"));

  EXPECT_THAT(Run({"get", "test"}), "expkey");
  EXPECT_THAT(Run({"get", "test2"}), "expkey");

  int ttl = CheckedInt({"ttl", "test"});  // should ignore expiry for key
  EXPECT_EQ(ttl, -1);

  int ttl2 = CheckedInt({"ttl", "test2"});  // should ignore expiry for key
  EXPECT_EQ(ttl2, -1);
}

TEST_F(RdbTest, CmsSerialization) {
  Run("cms.initbydim cms 1000 5");
  Run("cms.incrby cms foo 5 bar 3 baz 9");

  auto resp = Run("cms.query cms foo bar baz");
  EXPECT_THAT(resp, RespArray(ElementsAre(IntArg(5), IntArg(3), IntArg(9))));

  Run("save df cms");
  Run("flushall");
  EXPECT_EQ(Run("dfly load cms-summary.dfs"), "OK");

  resp = Run("cms.query cms foo bar baz");
  EXPECT_THAT(resp, RespArray(ElementsAre(IntArg(5), IntArg(3), IntArg(9))));
}

// Tests basic TOPK save/load: verifies that top-k heap items are correctly serialized
// and restored, maintaining their frequency-based ordering.
// Uses TOPK.INCRBY with large increments to ensure deterministic counts despite
// the stochastic HeavyKeeper decay (decay^count ≈ 0 for large counts).
TEST_F(RdbTest, TopkSerializationBasic) {
  Run({"TOPK.RESERVE", "topk_small", "3", "50", "7", "0.9"});
  Run({"TOPK.INCRBY", "topk_small", "foo", "300", "bar", "200", "baz", "400"});

  auto resp = Run({"TOPK.LIST", "topk_small"});
  EXPECT_THAT(resp, RespArray(ElementsAre("baz", "foo", "bar")));

  Run({"debug", "reload"});

  resp = Run({"TOPK.LIST", "topk_small"});
  EXPECT_THAT(resp, RespArray(ElementsAre("baz", "foo", "bar")));
}

// Tests that the Count-Min Sketch counter array is correctly serialized:
// verifies that existing counters suppress colliding items correctly after load.
TEST_F(RdbTest, TopkSerializationCounterArrayIntegrity) {
  Run({"TOPK.RESERVE", "topk_counters", "5", "100", "5", "0.9"});
  Run({"TOPK.INCRBY", "topk_counters", "alpha", "300", "beta", "200"});

  Run({"debug", "reload"});

  // Verify counts are preserved via TOPK.COUNT, which reads the counter array directly.
  // If counters weren't restored, these would return 0 (or wrong values).
  // TOPK.COUNT returns an array with one element per queried item.
  auto counts = Run({"TOPK.COUNT", "topk_counters", "alpha", "beta"});
  ASSERT_THAT(counts, ArrLen(2));
  int64_t alpha_count = counts.GetVec()[0].GetInt().value_or(0);
  int64_t beta_count = counts.GetVec()[1].GetInt().value_or(0);
  EXPECT_GE(alpha_count, 1);
  EXPECT_GE(beta_count, 1);
  EXPECT_GT(alpha_count, beta_count);

  // Also verify items are still in the heap (heap restoration).
  EXPECT_THAT(Run({"TOPK.QUERY", "topk_counters", "alpha", "beta"}),
              RespArray(ElementsAre(IntArg(1), IntArg(1))));

  auto resp = Run({"TOPK.LIST", "topk_counters"});
  EXPECT_THAT(resp, RespArray(ElementsAre("alpha", "beta")));
}

// Tests that K parameter (max heap size) is preserved after serialization:
// verifies list size stays at K=3 and eviction works correctly after load.
TEST_F(RdbTest, TopkSerializationParametersPreserved) {
  Run({"TOPK.RESERVE", "topk_params", "3", "64", "4", "0.95"});
  Run({"TOPK.INCRBY", "topk_params", "a", "100", "b", "200", "c", "300"});

  Run({"debug", "reload"});

  auto before = Run({"TOPK.LIST", "topk_params"});
  ASSERT_THAT(before, ArrLen(3));  // K=3 must be enforced

  // Add a new item heavily. It should evict the lowest item, maintaining K=3.
  Run({"TOPK.INCRBY", "topk_params", "z", "1000"});

  auto after = Run({"TOPK.LIST", "topk_params"});
  ASSERT_THAT(after, ArrLen(3));
  EXPECT_EQ(after.GetVec().front(), "z");  // 'z' should be the new king
}

// Tests serialization of heap-allocated strings (bypass SSO) to verify correct
// memory handling for string pointers in the min-heap.
TEST_F(RdbTest, TopkSerializationExtensive) {
  Run({"TOPK.RESERVE", "topk_large", "10", "128", "5", "0.9"});

  // Bypass SSO (Small String Optimization) to test memory pointers
  std::string long_str1(50, 'A');
  std::string long_str2(60, 'B');
  std::string long_str3(70, 'C');

  // Use INCRBY with large values to ensure deterministic counts
  Run({"TOPK.INCRBY", "topk_large", long_str1, "500"});
  Run({"TOPK.INCRBY", "topk_large", long_str2, "300"});
  Run({"TOPK.INCRBY", "topk_large", long_str3, "700"});

  Run({"debug", "reload"});

  auto resp = Run({"TOPK.LIST", "topk_large"});
  EXPECT_THAT(resp, RespArray(ElementsAre(long_str3, long_str1, long_str2)));
}

// Tests that empty TOPK (zero items in heap) can be saved and loaded correctly:
// validates TagAllowsEmptyValue() and ensures structure remains functional after load.
TEST_F(RdbTest, TopkSerializationEmptyEdgeCase) {
  Run({"TOPK.RESERVE", "topk_empty", "5", "50", "3", "0.9"});

  Run({"debug", "reload"});

  auto resp = Run({"TOPK.LIST", "topk_empty"});
  EXPECT_THAT(resp, ArrLen(0));

  // After loading an empty TOPK, adding items must work correctly.
  Run({"TOPK.INCRBY", "topk_empty", "new_item", "100"});
  resp = Run({"TOPK.LIST", "topk_empty"});
  EXPECT_THAT(resp, RespElementsAre("new_item"));
}

// Tests that the decay parameter (double) is correctly serialized using SaveBinaryDouble/
// FetchBinaryDouble: critical test for the strict aliasing fix (no reinterpret_cast).
TEST_F(RdbTest, TopkSerializationDecayParameter) {
  // Create TOPK with extreme decay values to ensure the double serialization works
  Run({"TOPK.RESERVE", "topk_decay_low", "5", "50", "3", "0.1"});     // Very aggressive decay
  Run({"TOPK.RESERVE", "topk_decay_high", "5", "50", "3", "0.999"});  // Minimal decay

  // Use INCRBY with large values to ensure deterministic counts
  Run({"TOPK.INCRBY", "topk_decay_low", "item1", "500", "item2", "300"});
  Run({"TOPK.INCRBY", "topk_decay_high", "item3", "500", "item4", "300"});

  Run({"debug", "reload"});

  // Verify both TOPKs loaded successfully and maintain their items
  auto resp1 = Run({"TOPK.LIST", "topk_decay_low"});
  EXPECT_THAT(resp1, RespArray(ElementsAre("item1", "item2")));

  auto resp2 = Run({"TOPK.LIST", "topk_decay_high"});
  EXPECT_THAT(resp2, RespArray(ElementsAre("item3", "item4")));
}

void AssertTaggedData(std::string_view blob, std::string_view expected, uint32_t expected_id = 1) {
  using namespace absl::little_endian;

  ASSERT_EQ(blob.size(), MemBufController::kHeaderSize + expected.size());
  EXPECT_EQ(static_cast<uint8_t>(blob[0]), RDB_OPCODE_TAGGED_CHUNK);

  auto id = Load32(reinterpret_cast<const uint8_t*>(blob.data()) + 1);
  auto len = Load32(reinterpret_cast<const uint8_t*>(blob.data()) + 5);

  EXPECT_EQ(id, expected_id);
  EXPECT_EQ(len, expected.size());
  EXPECT_EQ(blob.substr(MemBufController::kHeaderSize), expected);
}

class MemBufControllerTest : public Test {
 protected:
  MemBufController controller_;

  bool HasSplitEntries() const {
    return !controller_.split_entries_.empty();
  }

  std::string Flush() {
    const auto blob = controller_.BuildBlob();
    EXPECT_EQ(controller_.FlushableSize(), 0);
    return blob;
  }

  void Write(std::string_view s) {
    controller_.Buffer()->WriteAndCommit(s.data(), s.size());
  }

  void AssertDefaultState() {
    EXPECT_EQ(controller_.active_id_, 0u);
    EXPECT_EQ(controller_.Buffer(), &controller_.buffer_);
  }

  void MarkMidFlush() {
    controller_.MarkEntrySplit();
    EXPECT_TRUE(controller_.split_entries_.contains(controller_.active_id_));
  }

  MemBufController::EntryId SplitAndSuspend(std::string_view payload, uint32_t expected_id) {
    controller_.StartEntry();
    EXPECT_EQ(controller_.active_id_, expected_id);
    Write(payload);
    MarkMidFlush();
    AssertTaggedData(Flush(), payload, expected_id);

    const auto saved_id = controller_.SaveStateBeforeConsume();
    EXPECT_EQ(saved_id, expected_id);
    AssertDefaultState();
    EXPECT_EQ(controller_.FlushableSize(), 0);
    return saved_id;
  }

  void Restore(MemBufController::EntryId id) {
    controller_.RestoreStateAfterConsume(id);
    EXPECT_EQ(controller_.active_id_, id);
  }

  void WriteEntry(std::string_view data, bool save_successful = true) {
    controller_.StartEntry();
    Write(data);
    controller_.FinishEntry(save_successful);
  }
};

TEST_F(MemBufControllerTest, TaggedData) {
  controller_.SetTagEntries(true);

  constexpr std::string_view data = "a_a_a_";
  const auto saved_id = SplitAndSuspend(data, 1);
  EXPECT_TRUE(HasSplitEntries());

  Write("a");
  Restore(saved_id);
  ASSERT_EQ(controller_.FlushableSize(), 1);

  Write("b");
  ASSERT_EQ(controller_.FlushableSize(), 2);
  controller_.FinishEntry(true);
  EXPECT_FALSE(HasSplitEntries());

  const std::string blob = Flush();

  ASSERT_EQ(blob.size(), MemBufController::kHeaderSize + 2);
  ASSERT_EQ(blob[0], 'a');
  AssertTaggedData(blob.substr(1), "b");
}

TEST_F(MemBufControllerTest, NestedInterleaving) {
  controller_.SetTagEntries(true);

  const auto saved_id_a = SplitAndSuspend("aaa", 1);
  const auto saved_id_b = SplitAndSuspend("bbb", 2);

  controller_.StartEntry();
  Write("ccc");
  controller_.FinishEntry(true);
  AssertDefaultState();

  EXPECT_EQ(controller_.FlushableSize(), 3);

  EXPECT_EQ(Flush(), "ccc");

  Restore(saved_id_b);
  Write("x");
  controller_.FinishEntry(true);

  AssertTaggedData(Flush(), "x", 2);

  Restore(saved_id_a);
  Write("y");
  controller_.FinishEntry(true);
  EXPECT_FALSE(HasSplitEntries());

  AssertTaggedData(Flush(), "y");
}

TEST_F(MemBufControllerTest, BuildBlobEdgeCases) {
  controller_.SetTagEntries(true);

  Write("p");
  controller_.StartEntry();
  Write("x");
  MarkMidFlush();

  const std::string blob = Flush();
  ASSERT_FALSE(blob.empty());
  EXPECT_EQ(blob[0], 'p');
  AssertTaggedData(blob.substr(1), "x");

  controller_.FinishEntry(true);
  AssertDefaultState();
}

TEST_F(MemBufControllerTest, UnsplitEntry) {
  controller_.SetTagEntries(true);

  controller_.StartEntry();
  Write("hello");
  controller_.FinishEntry(true);
  AssertDefaultState();

  EXPECT_EQ(controller_.FlushableSize(), 5);
  EXPECT_EQ(Flush(), "hello");
}

TEST_F(MemBufControllerTest, TaggingDisabled) {
  controller_.StartEntry();
  Write("abc");
  MarkMidFlush();

  EXPECT_EQ(Flush(), "abc");

  const auto saved_id = controller_.SaveStateBeforeConsume();
  Restore(saved_id);

  Write("def");
  controller_.FinishEntry(true);

  EXPECT_EQ(Flush(), "def");
}

TEST_F(MemBufControllerTest, RollbackPartialEntry) {
  for (const auto state : {true, false}) {
    controller_.SetTagEntries(state);

    WriteEntry("hello", true);
    WriteEntry("world", false);

    EXPECT_EQ(Flush(), "hello");

    // empty buffer case
    WriteEntry("a", false);

    EXPECT_EQ(controller_.FlushableSize(), 0);
    EXPECT_EQ(Flush(), "");

    // next write works as expected ie no state corruption
    WriteEntry("abc", true);
    EXPECT_EQ(Flush(), "abc");
  }
}

TEST_F(MemBufControllerTest, RollbackPartialEntrySplit) {
  controller_.SetTagEntries(true);
  auto entry = SplitAndSuspend("abc", 1);
  EXPECT_TRUE(HasSplitEntries());
  Restore(entry);
  Write("bbb");

  controller_.FinishEntry(false);
  EXPECT_FALSE(HasSplitEntries());
  EXPECT_EQ(controller_.FlushableSize(), 0);
  EXPECT_EQ(Flush(), "");
}

TEST_F(MemBufControllerTest, RollbackOnSuspendedEntry) {
  controller_.SetTagEntries(true);

  const auto id_a = SplitAndSuspend("aaa", 1);

  // a failed entry written
  WriteEntry("bbb", false);

  // controller still has aaa in map
  EXPECT_TRUE(HasSplitEntries());

  Restore(id_a);
  Write("a_tail");
  controller_.FinishEntry(true);

  AssertTaggedData(Flush(), "a_tail", 1);
  EXPECT_FALSE(HasSplitEntries());
}

namespace {

// drakeydb: P4-2 Task 2, review round 1 (Important, finding 2) -- duplicated from
// journal_test.cc's own file-local ScopedLogCapture (same reasoning as WrapInRdbForTest in
// peer_replication_test.cc: the original is anonymous-namespace-scoped there, so a second TU
// needing the same capability copies it rather than promoting it to a shared header for one
// caller). Registers as a real glog/absl-log sink for the scope's lifetime and records every
// message's text verbatim, so a test can assert on the exact log output a code path produces
// (or, as here, does NOT produce) without guessing at log levels or destinations.
#ifdef USE_ABSL_LOG
class ScopedLogCapture : public absl::LogSink {
 public:
  ScopedLogCapture() {
    absl::AddLogSink(this);
  }
  ~ScopedLogCapture() override {
    absl::RemoveLogSink(this);
  }
  void Send(const absl::LogEntry& entry) override {
    logs.emplace_back(entry.text_message());
  }

  std::vector<std::string> logs;
};
#else
class ScopedLogCapture : public google::LogSink {
 public:
  ScopedLogCapture() {
    google::AddLogSink(this);
  }
  ~ScopedLogCapture() override {
    google::RemoveLogSink(this);
  }
  void send(google::LogSeverity severity, const char* full_filename, const char* base_filename,
            int line, const struct tm* tm_time, const char* message, size_t message_len) override {
    logs.emplace_back(message, message_len);
  }

  std::vector<std::string> logs;
};
#endif

// Wraps string in rdb version, eof, checksum, etc so it can be fed to a loader
std::string WrapInRdb(std::string_view body) {
  std::string out = absl::StrFormat("REDIS%04d", RDB_SER_VERSION);
  out.append(body);
  out.push_back(static_cast<char>(RDB_OPCODE_EOF));
  constexpr uint8_t checksum[8] = {};
  out.append(reinterpret_cast<const char*>(checksum), sizeof(checksum));
  return out;
}

std::error_code LoadRdbData(Service* service, const std::string& rdb,
                            std::optional<uint64_t> journal_offset = std::nullopt) {
  io::BytesSource src{io::Buffer(rdb)};
  RdbLoadContext load_context;
  RdbLoader loader(service, &load_context);
  auto ec = loader.Load(&src);
  EXPECT_EQ(loader.journal_offset(), journal_offset);
  return ec;
}

void AppendLen(std::string* out, uint64_t len) {
  uint8_t buf[9];
  const auto sz = WritePackedUInt(len, {buf, sizeof(buf)});
  out->append(reinterpret_cast<const char*>(buf), sz);
}

void AppendString(std::string* out, std::string_view s) {
  AppendLen(out, s.size());
  out->append(s);
}

void AddKV(std::string* out, std::string_view key, std::string_view val) {
  AppendString(out, key);
  AppendString(out, val);
}

std::string MakeTaggedChunk(uint32_t id, std::string_view payload) {
  std::string out;
  out.push_back(static_cast<char>(RDB_OPCODE_TAGGED_CHUNK));

  uint8_t header[8];
  absl::little_endian::Store32(header, id);
  absl::little_endian::Store32(header + 4, payload.size());
  out.append(reinterpret_cast<const char*>(header), sizeof(header));

  out.append(payload);
  return out;
}

void AppendBinaryDouble(std::string* out, double val) {
  uint64_t bits;
  memcpy(&bits, &val, sizeof(bits));

  uint8_t buf[8];
  absl::little_endian::Store64(buf, bits);
  out->append(reinterpret_cast<const char*>(buf), sizeof(buf));
}

// drakeydb: P4-3 Task 5 -- hand-builds an RDB_OPCODE_DF_TOMBSTONES section byte-for-byte per
// rdb_extensions.h's format: [db_index][count][count x {key, packed, origin_hash}], with
// {packed, origin_hash} as 16 raw LE bytes, exactly like EmitsOpcodeOnlyForTheStampedKey (above)
// hand-builds RDB_OPCODE_DF_MVCC's own 17-byte block. Used to drive RdbLoader::HandleTombstones
// directly with well-formed AND deliberately malformed entries, the same way that test drives its
// read-side counterpart.
std::string BuildTombstoneSection(DbIndex db_index,
                                  const std::vector<std::pair<std::string, MvccStamp>>& entries) {
  std::string out;
  out.push_back(static_cast<char>(RDB_OPCODE_DF_TOMBSTONES));
  AppendLen(&out, db_index);
  AppendLen(&out, entries.size());
  for (const auto& [key, stamp] : entries) {
    AppendString(&out, key);
    uint8_t buf[16];
    absl::little_endian::Store64(buf, stamp.packed);
    absl::little_endian::Store64(buf + 8, stamp.origin_hash);
    out.append(reinterpret_cast<const char*>(buf), sizeof(buf));
  }
  return out;
}

// Drives interleaving SaveEntry calls from the serializer's consume callback to exercise
// tagged-chunk framing when a serialization is preempted mid-entry.
struct InterleaveHarness {
  struct Pending {
    std::string key;
    const PrimeValue* value;
  };

  std::vector<Pending> queued;
  size_t next = 0;
  std::string body;
  RdbSerializer* serializer = nullptr;
  std::optional<uint64_t> last_journal_offset;

  void AddKey(std::string_view key, DbContext& ctx) {
    auto& db = ctx.GetDbSlice(0);
    auto it = db.FindReadOnly(ctx, key, OBJ_HASH);
    ASSERT_TRUE(it.ok());
    queued.push_back(Pending{std::string{key}, &it.value()->second});
  }

  // Each invocation appends the supplied blob to the body, then injects a SaveEntry for the next
  // queued key (sandwiched between a journal offset and a journal entry) to force interleaved
  // tagged chunks. Passed to serializer as consume_fun_, so the blob is the flushed data
  // accumulated in serializer.
  std::error_code operator()(std::string blob) {
    body += blob;
    if (next >= queued.size())
      return {};

    uint64_t offset = last_journal_offset.value_or(0) + 100;
    last_journal_offset = offset;
    EXPECT_FALSE(serializer->SendJournalOffset(offset));

    const auto& entry = queued[next++];
    // SaveEntry calls get preempted (not by fiber but call stack) every time consume_fun_ is
    // called. So the call stack looks like: SaveEntry(A) -> consume_fun_ -> SaveEntry(B) ->
    // consume_fun_ -> ... The last entry in queue (next == queued size) does not add anything, it
    // simply returns until the entry is completed. From that point on all entries simply flush
    // repeatedly until completed, moving down the stack.
    EXPECT_TRUE(
        serializer->SaveEntry(PrimeKey{entry.key}, *entry.value, 0, 0, 0, MvccStamp{}).has_value());

    io::StringSink sink;
    JournalWriter writer(&sink);
    writer.Write(journal::Entry{journal::Op::PING, 0, std::nullopt});
    EXPECT_FALSE(serializer->WriteJournalEntry(std::move(sink).str()));
    return {};
  }
};

}  // namespace

// The following are tests that directly feed byte data to loader to exercise chunk loading.
// Some of these will become redundant once the saver starts sending chunked data, so instead of
// hand-crafting data we will be able to load from the db directly.

TEST_F(RdbTest, InterleavedLoad) {
  // must have >1 shards for non inlined path check. find a key that lands in shard 1 by hashing, to
  // test non inlined obj. creation
  ASSERT_GT(shard_set->size(), 1u);
  std::string key;
  for (unsigned i = 0; i < 1000; ++i) {
    key = StrCat("x", i);
    if (Shard(key, shard_set->size()) == 1)
      break;
  }
  ASSERT_EQ(Shard(key, shard_set->size()), 1u);

  std::string a1;
  // hash chunk 1
  a1.push_back(RDB_TYPE_HASH);
  AppendString(&a1, key);
  AppendLen(&a1, 2);
  AddKV(&a1, "f1", "v1");

  // string
  std::string b;
  b.push_back(RDB_TYPE_STRING);
  AppendString(&b, "b");
  AppendString(&b, "plain");

  // hash chunk 2
  std::string a2;
  AddKV(&a2, "f2", "v2");

  std::string body;
  // chunk for db 0
  body += MakeTaggedChunk(1, a1);
  // simple string b=plain
  body += b;
  body.push_back(static_cast<char>(RDB_OPCODE_SELECTDB));
  // switch to db 1
  AppendLen(&body, 1);
  // back to chunk for db 0
  body += MakeTaggedChunk(1, a2);

  auto ec = pp_->at(0)->Await([&] { return LoadRdbData(service_.get(), WrapInRdb(body)); });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_EQ(Run({"SELECT", "0"}), "OK");
  EXPECT_EQ(Run({"HGET", key, "f1"}), "v1");
  EXPECT_EQ(Run({"HGET", key, "f2"}), "v2");
  EXPECT_EQ(Run({"GET", "b"}), "plain");

  EXPECT_EQ(Run({"SELECT", "1"}), "OK");
  EXPECT_THAT(Run({"EXISTS", key}), IntArg(0));
  EXPECT_EQ(Run({"SELECT", "0"}), "OK");
}

TEST_F(RdbTest, EofWithPendingChunkState) {
  // will be skipped
  std::string a1;
  a1.push_back(RDB_TYPE_HASH);
  AppendString(&a1, "partial_hash");
  AppendLen(&a1, 2);
  AddKV(&a1, "f1", "v1");

  // will survive
  std::string b;
  b.push_back(RDB_TYPE_STRING);
  AppendString(&b, "complete_key");
  AppendString(&b, "hello");

  std::string body;
  body += MakeTaggedChunk(1, a1);
  body += b;

  const auto ec = pp_->at(0)->Await([&] { return LoadRdbData(service_.get(), WrapInRdb(body)); });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_THAT(Run({"EXISTS", "partial_hash"}), IntArg(0));
  EXPECT_EQ(Run({"GET", "complete_key"}), "hello");
}

TEST_F(RdbTest, SplitSBF) {
  // this test creates two filter SBF, then splits one of the filters. Since in sbf loading there
  // are two layers of possible splits, intra-filter and inter-filter, this test exercises both
  // splits. A plain string is also added between the split filter.

  // Creates filter in db to copy the fields from
  auto resp = Run({"BF.RESERVE", "bf_src", "0.01", "10"});
  EXPECT_EQ(resp, "OK");
  for (size_t i = 0; i < 50; ++i) {
    resp = Run({"BF.ADD", "bf_src", StrCat("item", i)});
    EXPECT_THAT(resp, AnyOf(0, 1));
  }

  std::string first;
  std::string blob1;

  // split the blob of the second filter into three chunks. this exercises the loader path where we
  // first try to load the incomplete filter, and return early before that finishes
  constexpr size_t kFirstSplit = 17;
  constexpr size_t kSecondSplit = 13;

  pp_->at(0)->Await([&] {
    const DbContext ctx{&namespaces->GetDefaultNamespace(), 0, GetCurrentTimeMs()};
    const auto& db = ctx.GetDbSlice(0);
    auto it = db.FindReadOnly(ctx, "bf_src", OBJ_SBF);
    ASSERT_TRUE(it.ok());

    const SBF* sbf = it.value()->second.GetSBF();
    ASSERT_GE(sbf->num_filters(), 2);

    const std::string blob0{sbf->data(0)};

    blob1 = std::string{sbf->data(1)};
    ASSERT_GT(blob1.size(), kFirstSplit + kSecondSplit);

    first.push_back(RDB_TYPE_SBF2);
    // brand new key whose shape is copied off bf_src
    AppendString(&first, "bf_loaded");
    AppendLen(&first, 0);
    AppendBinaryDouble(&first, sbf->grow_factor());
    AppendBinaryDouble(&first, sbf->fp_probability());
    AppendLen(&first, sbf->prev_size());
    AppendLen(&first, sbf->current_size());
    AppendLen(&first, sbf->max_capacity());
    AppendLen(&first, sbf->num_filters());

    AppendLen(&first, sbf->hashfunc_cnt(0));
    // total size of blob0
    AppendLen(&first, blob0.size());
    // this chunk size (all of blob0 is fit in one chunk)
    AppendLen(&first, blob0.size());
    first.append(blob0);

    AppendLen(&first, sbf->hashfunc_cnt(1));
    // total size of blob1
    AppendLen(&first, blob1.size());
    // only 17 bytes from blob1 in this chunk
    AppendLen(&first, kFirstSplit);
    first.append(blob1.data(), kFirstSplit);
  });

  // add this plain string between chunks of blob1 filter
  std::string plain;
  plain.push_back(RDB_TYPE_STRING);
  AppendString(&plain, "plain_key");
  AppendString(&plain, "plain_val");

  // p2 of blob1
  std::string second;
  AppendLen(&second, kSecondSplit);
  second.append(blob1.data() + kFirstSplit, kSecondSplit);

  // p3 of blob1
  std::string third;
  constexpr auto kPrefixConsumed = kFirstSplit + kSecondSplit;
  AppendLen(&third, blob1.size() - kPrefixConsumed);
  third.append(blob1.data() + kPrefixConsumed, blob1.size() - kPrefixConsumed);

  std::string body;
  body += MakeTaggedChunk(1, first);
  body += plain;
  body += MakeTaggedChunk(1, second);
  body += MakeTaggedChunk(1, third);

  EXPECT_EQ(Run({"FLUSHALL"}), "OK");

  auto ec = pp_->at(0)->Await([&] { return LoadRdbData(service_.get(), WrapInRdb(body)); });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_EQ(Run({"TYPE", "bf_loaded"}), "MBbloom--");
  EXPECT_EQ(Run({"GET", "plain_key"}), "plain_val");

  for (size_t i = 0; i < 50; ++i) {
    EXPECT_THAT(Run({"BF.EXISTS", "bf_loaded", StrCat("item", i)}), IntArg(1));
  }
}

TEST_F(RdbTest, SplitCuckoo) {
  auto resp = Run("cf.reserve cf_src 4 expansion 2");
  EXPECT_EQ(resp, "OK");
  for (size_t i = 0; i < 100; ++i) {
    resp = Run(StrCat("cf.add cf_src item", i));
    EXPECT_THAT(resp, IntArg(1));
  }

  std::string first;
  std::string last_blob;

  // split the blob of the last filter into three chunks.
  constexpr size_t kFirstSplit = 17;
  constexpr size_t kSecondSplit = 13;

  pp_->at(0)->Await([&] {
    const DbContext ctx{&namespaces->GetDefaultNamespace(), 0, GetCurrentTimeMs()};
    const auto& db = ctx.GetDbSlice(0);
    auto it = db.FindReadOnly(ctx, "cf_src", OBJ_CUCKOOFILTER);
    ASSERT_TRUE(it.ok());

    const CuckooFilter* cf = it.value()->second.GetCuckooFilter();
    ASSERT_GE(cf->NumFilters(), 2u);
    const size_t num_filters = cf->NumFilters();

    last_blob = std::string{cf->FilterBytes(num_filters - 1)};
    ASSERT_GT(last_blob.size(), kFirstSplit + kSecondSplit);

    first.push_back(RDB_TYPE_CUCKOO);
    // brand new key whose shape is copied off cf_src
    AppendString(&first, "cf_loaded");
    AppendLen(&first, cf->SlotsPerBucket());
    AppendLen(&first, cf->MaxIterations());
    AppendLen(&first, cf->Expansion());
    AppendLen(&first, cf->NumBuckets());
    AppendLen(&first, cf->NumItems());
    AppendLen(&first, cf->NumDeletes());
    AppendLen(&first, num_filters);

    // every filter but the last is written whole, in a single chunk
    for (size_t i = 0; i + 1 < num_filters; ++i) {
      const std::string blob{cf->FilterBytes(i)};
      AppendLen(&first, blob.size());
      AppendLen(&first, blob.size());
      first.append(blob);
    }

    // total size of the last filter's blob
    AppendLen(&first, last_blob.size());
    // only kFirstSplit bytes of it in this chunk
    AppendLen(&first, kFirstSplit);
    first.append(last_blob.data(), kFirstSplit);
  });

  // add this plain string between chunks of the split filter
  std::string plain;
  plain.push_back(RDB_TYPE_STRING);
  AppendString(&plain, "plain_key");
  AppendString(&plain, "plain_val");

  // p2 of last_blob
  std::string second;
  AppendLen(&second, kSecondSplit);
  second.append(last_blob.data() + kFirstSplit, kSecondSplit);

  // p3 of last_blob
  std::string third;
  constexpr auto kPrefixConsumed = kFirstSplit + kSecondSplit;
  AppendLen(&third, last_blob.size() - kPrefixConsumed);
  third.append(last_blob.data() + kPrefixConsumed, last_blob.size() - kPrefixConsumed);

  std::string body;
  body += MakeTaggedChunk(1, first);
  body += plain;
  body += MakeTaggedChunk(1, second);
  body += MakeTaggedChunk(1, third);

  EXPECT_EQ(Run("flushall"), "OK");

  auto ec = pp_->at(0)->Await([&] { return LoadRdbData(service_.get(), WrapInRdb(body)); });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_EQ(Run("type cf_loaded"), "MBbloomCF");
  EXPECT_EQ(Run("get plain_key"), "plain_val");

  for (size_t i = 0; i < 100; ++i) {
    EXPECT_THAT(Run(StrCat("cf.exists cf_loaded item", i)), IntArg(1));
  }
}

TEST_F(RdbTest, TaggedInterleavedRoundTrip) {
  absl::FlagSaver fs;
  SetTestFlag("cache_mode", "false");
  SetTestFlag("num_shards", "1");
  SetTestFlag("serialization_tagged_chunks", "true");
  ResetService();

  // create hset named key, then fill it with count fields each with 128 char long string
  auto fill_hash = [&](std::string_view key, int count, char ch) {
    for (int i = 0; i < count; ++i) {
      auto res = Run({"HSET", key, StrCat("field:", i), std::string(128, ch)});
      EXPECT_THAT(res, IntArg(1));
    }
  };

  // Some hashes have many more fields to make them flush mid-entry during serialization
  auto num_fields_in_hash_set = [](std::string s) {
    if ((s[0] - 'A') % 3 == 2)
      return 4;
    return 200;
  };

  // note: going to Z causes stack size issues because of the recursive nature of the harness
  constexpr auto from = 'A';
  constexpr auto to = 'F';
  for (auto ch = from; ch <= to; ++ch) {
    std::string s{ch};
    fill_hash(s, num_fields_in_hash_set(s), ch);
  }

  std::string body;
  std::optional<uint64_t> last_journal_offset;

  pp_->at(0)->Await([&] {
    DbContext ctx{&namespaces->GetDefaultNamespace(), 0, GetCurrentTimeMs()};

    InterleaveHarness harness;
    // Queue up B..F to be injected when A yields mid-SaveEntry through PushToConsumerIfNeeded.
    for (auto ch = 'B'; ch <= to; ++ch) {
      std::string s{ch};
      harness.AddKey(s, ctx);
    }

    RdbSerializer serializer(
        CompressionMode::NONE,
        [&](std::string blob) -> std::error_code {
          harness(std::move(blob));
          return {};
        },
        256);

    harness.serializer = &serializer;
    serializer.SetTagEntries(true);

    auto& db = ctx.GetDbSlice(0);
    auto it = db.FindReadOnly(ctx, "A", OBJ_HASH);
    ASSERT_TRUE(it.ok());

    ASSERT_TRUE(
        serializer.SaveEntry(PrimeKey{"A"}, it.value()->second, 0, 0, 0, MvccStamp{}).has_value());

    if (auto tail = serializer.Flush(RdbSerializer::FlushState::kFlushEndEntry); !tail.empty())
      harness.body += tail;

    body = std::move(harness.body);
    last_journal_offset = harness.last_journal_offset;
  });

  EXPECT_EQ(Run({"FLUSHALL"}), "OK");

  auto ec = pp_->at(0)->Await(
      [&] { return LoadRdbData(service_.get(), WrapInRdb(body), last_journal_offset); });
  ASSERT_FALSE(ec) << ec.message();

  auto verify_hash = [&](std::string_view key, int count, char ch) {
    EXPECT_EQ(CheckedInt({"HLEN", std::string{key}}), count);
    for (int i = 0; i < count; ++i) {
      EXPECT_EQ(Run({"HGET", std::string{key}, StrCat("field:", i)}), std::string(128, ch));
    }
  };

  for (auto ch = from; ch <= to; ++ch) {
    std::string s{ch};
    verify_hash(s, num_fields_in_hash_set(s), ch);
  }
}

std::string MakeJournalDel(std::string_view key) {
  io::StringSink sink;
  JournalWriter writer(&sink);
  writer.Write(journal::Entry{1, journal::Op::COMMAND, 0, std::nullopt,
                              journal::Entry::Payload("DEL", ArgSlice{key})});

  RdbSerializer serializer(CompressionMode::NONE);
  CHECK(!serializer.WriteJournalEntry(std::move(sink).str()));
  return serializer.Flush(RdbSerializer::FlushState::kFlushEndEntry);
}

TEST_F(RdbTest, JournalDelWaitsForShardLoads) {
  ASSERT_GT(shard_set->size(), 1u);

  std::string key;
  for (unsigned i = 0; i < 1000; ++i) {
    key = StrCat("journal-del-barrier-", i);
    if (Shard(key, shard_set->size()) != 0)
      break;
  }
  ASSERT_NE(Shard(key, shard_set->size()), 0u);
  const ShardId sid = Shard(key, shard_set->size());

  // Priming key on same shard as key so that it can schedule before the RDB load callback.
  std::string priming_key;
  for (unsigned i = 0; i < 1000; ++i) {
    priming_key = StrCat("journal-del-prime-", i);
    if (Shard(priming_key, shard_set->size()) == sid)
      break;
  }
  ASSERT_EQ(Shard(priming_key, shard_set->size()), sid);

  EXPECT_EQ(Run({"FLUSHALL"}), "OK");

  std::atomic_bool release_shard_queue{false};

  // block sid task queue for 50ms. releaser will unlock this after 50ms
  shard_set->Add(sid, [&] {
    while (!release_shard_queue.load(std::memory_order_relaxed)) {
      ThisFiber::SleepFor(chrono::milliseconds(1));
    }
  });

  const auto ec = pp_->at(0)->Await([&] {
    Fiber releaser([&] {
      ThisFiber::SleepFor(chrono::milliseconds(50));
      release_shard_queue.store(true, std::memory_order_relaxed);
    });

    // Run priming key so that transaction scheduling is already in the shard set task queue by the
    // time delete runs.
    Fiber scheduler_primer([&] { EXPECT_EQ(Run({"SET", priming_key, "1"}), "OK"); });
    ThisFiber::SleepFor(chrono::milliseconds(10));

    std::string entry;
    entry.push_back(RDB_TYPE_STRING);
    AppendString(&entry, key);
    AppendString(&entry, "baseline");

    // create artificial rdb
    // one entry key -> baseline
    // one delete journal entry for same key
    std::string body;

    // this entry will be added to task set after the blocked entry from
    // src/server/rdb_load.cc:2824
    body += entry;
    // this entry will be executed directly in the already running batch without going to task queue
    // task queue will look like this for sid
    // 1. 50ms blocker
    // 2. schedule batch in shard (which will run SET priming_key and then DEL key in same batch)
    // 3. then finally loader callback which creates the key
    body += MakeJournalDel(key);

    const std::string rdb = WrapInRdb(body);
    io::BytesSource src{io::Buffer(rdb)};
    RdbLoadContext load_context;
    RdbLoader loader(service_.get(), &load_context);

    const auto ec_ = loader.Load(&src);
    EXPECT_EQ(loader.journal_offset(), std::nullopt);
    scheduler_primer.Join();
    releaser.Join();
    shard_set->Await(sid, [] {});
    return ec_;
  });

  ASSERT_FALSE(ec) << ec.message();

  EXPECT_THAT(Run({"GET", key}), ArgType(RespExpr::NIL));
}

// drakeydb: Phase 3 T7b -- a peer's full sync must filter its CONCURRENT journal blob (writes
// racing the snapshot) exactly like JournalStreamer::ShouldWrite filters the stable-sync stream
// (journal::PassesPeerEchoFilter, journal/types.h); a plain replica's full sync must keep
// receiving that blob completely unfiltered -- filtering it would be silent data loss. Drives the
// REAL send side (RdbSaver -> SliceSnapshot::ConsumeJournalChange, snapshot.cc) via
// journal::RecordEntry -- not a reimplementation of it -- capturing a peer-mode and a
// plain-replica full sync of the same three concurrent writes, then observes the filtering effect
// through the REAL receive side (a fresh RdbLoader) rather than hand-decoding the wire bytes.
//
// Op::ORIGIN's own drop condition is intentionally NOT exercised here (it would need
// --active_replica live before this fixture's shard set initializes, purely so the entry's WIRE
// BYTES satisfy an unrelated DCHECK on extended framing -- the filter DECISION itself reads only
// the JournalItem struct fields journal::RecordEntry populates directly, never the wire bytes);
// PassesPeerEchoFilterTest (journal_test.cc) covers that condition exhaustively on the shared
// predicate directly, without needing that machinery.
//
// Falsifying (verified by hand -- see task-7b-report.md): gating the filter out of
// SliceSnapshot::ConsumeJournalChange entirely (reverting to an unconditional
// serializer_->WriteJournalEntry(...), no peer_mode check) makes the peer_mode=true load ALSO
// show "foreignkey"=="foreignval" and "expirykey"==NIL -- identical to the plain-replica load; the
// two capture results become indistinguishable, and the two EXPECT lines that currently fail
// (foreignkey, expirykey) both flip. Reverting only RdbSaver::Impl::CreateSliceSnapshot's
// peer_mode forwarding (passing a hard-coded false into SliceSnapshot's constructor there instead
// of peer_mode_) reproduces the identical failure for the peer_mode=true case alone, proving the
// plumbing itself -- not just the shared predicate, which PassesPeerEchoFilterTest covers on its
// own -- is load-bearing here.
TEST_F(RdbTest, PeerFullSyncFiltersConcurrentJournalPlainReplicaUnaffected) {
  auto capture = [&](bool peer_mode) {
    io::StringSink sink;
    return pp_->at(0)->Await([&]() -> std::string {
      // drakeydb: Phase 3 T7b -- journal::RecordEntry below DCHECKs the ring buffer has a
      // non-zero capacity, which is only true once journaling has actually been engaged on this
      // shard (normally the first time a replica connects; a fresh BaseFamilyTest boot never
      // does that on its own). journal::StartInThread's Init() call is idempotent (an early
      // return once already initialized), so calling it here on every capture() invocation is
      // harmless on the second call.
      journal::StartInThread();

      RdbSaver saver(&sink, SaveMode::SINGLE_SHARD_WITH_SUMMARY, /*align_writes=*/false, "",
                     DflyVersion::CURRENT_VER, peer_mode);
      ExecutionState cntx;
      EngineShard* shard = EngineShard::tlocal();
      CHECK(!saver.SaveHeader(RdbSaver::GetGlobalData(service_.get(), true)));

      // drakeydb: Phase 3 T7b -- DbSlice::RegisterOnChange (SliceSnapshot::Start's own
      // RegisterChangeListener call, inside StartSnapshotInShard below) DCHECKs the shard's
      // intent lock is held: in production, DFLY SYNC is a GLOBAL_TRANS command, so ordinary
      // command scheduling already holds it for the whole StartFullSyncInThread call (see
      // DflyCmd::Sync's Transaction::Guard, which is a SEPARATE, additional expiry-disabling
      // mechanism -- not what holds this lock). This test drives RdbSaver directly, off the
      // command-dispatch path, so it must satisfy that same precondition explicitly.
      shard->shard_lock()->Acquire(IntentLock::EXCLUSIVE);
      saver.StartSnapshotInShard(/*stream_journal=*/true, &cntx, shard);

      // Self-origin write: passes the filter for both a peer and a plain replica.
      array<string_view, 2> self_kv{"selfkey", "selfval"};
      journal::RecordEntry(0, journal::Op::COMMAND, 0, std::nullopt,
                           journal::Entry::Payload{"SET", ArgSlice{self_kv.data(), self_kv.size()}},
                           PeerRegistry::kSelfIdx);

      // Foreign-origin write: dropped for a peer, kept for a plain replica.
      constexpr uint32_t kPeerIdx = 9;  // some peer's PeerRegistry index; != kSelfIdx.
      array<string_view, 2> foreign_kv{"foreignkey", "foreignval"};
      journal::RecordEntry(
          0, journal::Op::COMMAND, 0, std::nullopt,
          journal::Entry::Payload{"SET", ArgSlice{foreign_kv.data(), foreign_kv.size()}}, kPeerIdx);

      // Self-origin, expiry-flagged DEL: dropped for a peer, kept for a plain replica.
      array<string_view, 1> del_key{"expirykey"};
      journal::RecordEntry(0, journal::Op::COMMAND, 0, std::nullopt,
                           journal::Entry::Payload{"DEL", ArgSlice{del_key.data(), del_key.size()}},
                           PeerRegistry::kSelfIdx, /*mvcc=*/0, journal::kEntryFlagExpired);

      CHECK(!saver.StopFullSyncInShard(shard));
      shard->shard_lock()->Release(IntentLock::EXCLUSIVE);
      return std::move(sink).str();
    });
  };

  std::string peer_bytes = capture(/*peer_mode=*/true);
  std::string full_bytes = capture(/*peer_mode=*/false);

  auto load_and_check = [&](const std::string& bytes) {
    ASSERT_EQ(Run({"FLUSHALL"}), "OK");
    ASSERT_EQ(Run({"SET", "expirykey", "baseline"}), "OK");
    io::BytesSource src{io::Buffer(bytes)};
    RdbLoadContext load_context;
    auto ec = pp_->at(0)->Await([&] {
      RdbLoader loader(service_.get(), &load_context);
      return loader.Load(&src);
    });
    ASSERT_FALSE(ec) << ec.message();
  };

  load_and_check(peer_bytes);
  EXPECT_EQ(Run({"GET", "selfkey"}), "selfval");
  EXPECT_THAT(Run({"GET", "foreignkey"}), ArgType(RespExpr::NIL));
  EXPECT_EQ(Run({"GET", "expirykey"}), "baseline");

  load_and_check(full_bytes);
  EXPECT_EQ(Run({"GET", "selfkey"}), "selfval");
  EXPECT_EQ(Run({"GET", "foreignkey"}), "foreignval");
  EXPECT_THAT(Run({"GET", "expirykey"}), ArgType(RespExpr::NIL));
}

// Test that an unsupported module type is skipped, and that the keys before and after it are
// loaded correctly.
TEST_F(RdbTest, ModuleUnsupportedTypeSkipped) {
  std::string body;

  body.push_back(RDB_TYPE_STRING);
  AddKV(&body, "key_before", "val_before");

  body.push_back(RDB_TYPE_MODULE_2);
  AppendString(&body, "key_with_unsupported_module");
  AppendLen(&body, EncodeModuleId("invalid", 1));
  AppendLen(&body, RDB_MODULE_OPCODE_STRING);
  AppendString(&body, "value");
  AppendLen(&body, RDB_MODULE_OPCODE_EOF);

  body.push_back(RDB_TYPE_STRING);
  AddKV(&body, "key_after", "val_after");

  auto ec = pp_->at(0)->Await([&] { return LoadRdbData(service_.get(), WrapInRdb(body)); });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_EQ(Run({"GET", "key_before"}), "val_before");
  EXPECT_THAT(Run({"EXISTS", "key_with_unsupported_module"}), IntArg(0));
  EXPECT_EQ(Run({"GET", "key_after"}), "val_after");
}

// Every global-PEL entry of a loaded consumer group must be owned by exactly one
// consumer. A crafted stream that violates this must be rejected: two consumers
// sharing one entry leave a NACK double-freed on consumer deletion, and an
// unclaimed entry leaves nack->consumer == nullptr for XACK/XCLAIM to dereference.
TEST_F(RdbTest, RestoreStreamConsumerGroupCorruption) {
  auto u64le = [](std::string* out, uint64_t v) {
    uint8_t b[8];
    absl::little_endian::Store64(b, v);
    out->append(reinterpret_cast<const char*>(b), sizeof(b));
  };

  struct Consumer {
    std::string name;
    std::vector<std::string> pel;
  };

  // Builds a DUMP payload for a stream with an empty body and a single consumer
  // group whose global PEL and consumers are as specified.
  auto build = [&](const std::vector<std::string>& global_pel,
                   const std::vector<Consumer>& consumers) {
    std::string p;
    p.push_back(RDB_TYPE_STREAM_LISTPACKS);
    AppendLen(&p, 0);  // listpack node count: empty stream body
    AppendLen(&p, 0);  // stream_len
    AppendLen(&p, 0);  // last_id.ms
    AppendLen(&p, 0);  // last_id.seq

    AppendLen(&p, 1);       // one consumer group
    AppendString(&p, "g");  // group name
    AppendLen(&p, 0);       // group last_id.ms
    AppendLen(&p, 0);       // group last_id.seq

    AppendLen(&p, global_pel.size());
    for (const auto& id : global_pel) {
      p.append(id);
      u64le(&p, 0);      // delivery_time
      AppendLen(&p, 0);  // delivery_count
    }

    AppendLen(&p, consumers.size());
    for (const auto& c : consumers) {
      AppendString(&p, c.name);
      u64le(&p, 0);  // seen_time
      AppendLen(&p, c.pel.size());
      for (const auto& id : c.pel)
        p.append(id);
    }

    // DUMP footer: 2-byte version, then CRC64 over everything preceding it.
    uint8_t ver[2];
    absl::little_endian::Store16(ver, RDB_SER_VERSION);
    p.append(reinterpret_cast<const char*>(ver), sizeof(ver));
    uint64_t cs = crc64(0, reinterpret_cast<const uint8_t*>(p.data()), p.size());
    u64le(&p, cs);
    return p;
  };

  const std::string x(16, 'A');
  const std::string y(16, 'B');

  // Two consumers claim the same global-PEL entry: accepted by a vulnerable
  // loader, then a double-free when both consumers are deleted.
  EXPECT_THAT(Run({"RESTORE", "shared", "0", build({x}, {{"c1", {x}}, {"c2", {x}}})}),
              ErrArg("Bad data format"));
  EXPECT_THAT(Run({"EXISTS", "shared"}), IntArg(0));

  // A global-PEL entry that no consumer claims: accepted by a vulnerable loader,
  // leaving nack->consumer == nullptr.
  EXPECT_THAT(Run({"RESTORE", "unclaimed", "0", build({x}, {{"c1", {}}})}),
              ErrArg("Bad data format"));
  EXPECT_THAT(Run({"EXISTS", "unclaimed"}), IntArg(0));

  // One consumer lists the same id twice (duplicate consumer-PEL insert).
  EXPECT_THAT(Run({"RESTORE", "dup", "0", build({x}, {{"c1", {x, x}}})}),
              ErrArg("Bad data format"));
  EXPECT_THAT(Run({"EXISTS", "dup"}), IntArg(0));

  // Positive controls: well-formed groups must still load.
  EXPECT_EQ(Run({"RESTORE", "ok1", "0", build({x}, {{"c1", {x}}})}), "OK");
  EXPECT_THAT(Run({"EXISTS", "ok1"}), IntArg(1));
  EXPECT_EQ(Run({"RESTORE", "ok2", "0", build({x, y}, {{"c1", {x}}, {"c2", {y}}})}), "OK");
  EXPECT_THAT(Run({"EXISTS", "ok2"}), IntArg(1));
}

// A master entry declaring more fields than the listpack holds makes stream iteration walk
// lpNext past the end and crash; RESTORE must reject the inconsistent stream.
TEST_F(RdbTest, RestoreStreamListpackMasterFieldsOverflow) {
  auto u64le = [](std::string* out, uint64_t v) {
    uint8_t b[8];
    absl::little_endian::Store64(b, v);
    out->append(reinterpret_cast<const char*>(b), sizeof(b));
  };

  // A one-node stream DUMP whose master entry declares the given counts. An optional record with
  // no fields can be added to test consistency between deleted_count and the record flags.
  auto build = [&](int64_t count, int64_t deleted, int64_t num_master_fields,
                   std::optional<int64_t> record_flags = std::nullopt) {
    uint8_t* lp = lpNew(0);
    lp = lpAppendInteger(lp, count);              // valid entry count
    lp = lpAppendInteger(lp, deleted);            // deleted count
    lp = lpAppendInteger(lp, num_master_fields);  // master fields (untrusted)
    lp = lpAppendInteger(lp, 0);                  // terminator
    if (record_flags) {
      CHECK_EQ(num_master_fields, 0);
      lp = lpAppendInteger(lp, *record_flags);
      lp = lpAppendInteger(lp, 0);  // entry ID milliseconds delta
      lp = lpAppendInteger(lp, 0);  // entry ID sequence delta
      lp = lpAppendInteger(lp, 3);  // flags + two ID deltas
    }
    std::string lp_blob(reinterpret_cast<const char*>(lp), lpBytes(lp));
    lpFree(lp);

    std::string p;
    p.push_back(RDB_TYPE_STREAM_LISTPACKS);
    AppendLen(&p, 1);                           // one listpack node
    AppendString(&p, std::string(16, '\x01'));  // 16-byte master ID (sizeof(streamID))
    AppendString(&p, lp_blob);                  // the crafted master-entry listpack
    AppendLen(&p, 0);                           // stream_len
    AppendLen(&p, 0);                           // last_id.ms
    AppendLen(&p, 0);                           // last_id.seq
    AppendLen(&p, 0);                           // consumer-group count

    uint8_t ver[2];
    absl::little_endian::Store16(ver, RDB_SER_VERSION);
    p.append(reinterpret_cast<const char*>(ver), sizeof(ver));
    u64le(&p, crc64(0, reinterpret_cast<const uint8_t*>(p.data()), p.size()));
    return p;
  };

  // Control: truthful counts must still load.
  EXPECT_EQ(Run({"RESTORE", "safe", "0", build(0, 0, 0)}), "OK");
  EXPECT_THAT(Run({"EXISTS", "safe"}), IntArg(1));

  // Inflated master-fields count: crashes a vulnerable loader in streamGetEdgeID, rejected here.
  EXPECT_THAT(Run({"RESTORE", "overflow", "0", build(0, 0, 1000)}), ErrArg("Bad data format"));
  EXPECT_THAT(Run({"EXISTS", "overflow"}), IntArg(0));

  // valid + deleted counts whose signed sum overflows; must not wrap negative and skip the walk.
  EXPECT_THAT(Run({"RESTORE", "sumovf", "0", build(INT64_MAX, 1, 0)}), ErrArg("Bad data format"));
  EXPECT_THAT(Run({"EXISTS", "sumovf"}), IntArg(0));

  // The number of records carrying the DELETED flag must match deleted_count in the master entry.
  EXPECT_THAT(Run({"RESTORE", "deleted-mismatch", "0",
                   build(1, 0, 0, STREAM_ITEM_FLAG_SAMEFIELDS | STREAM_ITEM_FLAG_DELETED)}),
              ErrArg("Bad data format"));
  EXPECT_THAT(Run({"EXISTS", "deleted-mismatch"}), IntArg(0));

  // Real streams must still round-trip, exercising both SAMEFIELDS and full records.
  Run({"XADD", "s", "1-1", "a", "1", "b", "2"});  // master fields {a, b}
  Run({"XADD", "s", "2-1", "a", "3", "b", "4"});  // same fields -> SAMEFIELDS
  Run({"XADD", "s", "3-1", "c", "5"});            // different fields -> full record
  auto dump = Run({"DUMP", "s"});
  Run({"DEL", "s"});
  EXPECT_EQ(Run({"RESTORE", "s", "0", dump.GetString()}), "OK");
  EXPECT_THAT(Run({"XLEN", "s"}), IntArg(3));
}

// An early EOF with trailing bytes passes the non-deep lpValidateIntegrity but must be
// rejected by the walk; otherwise reverse iteration (lpLast/lpPrev) reads the trailing bytes
// out of bounds. Exercised via the non-deep full-RDB load path.
TEST_F(RdbTest, LoadStreamListpackEarlyEof) {
  uint8_t* lp = lpNew(0);
  lp = lpAppendInteger(lp, 0);  // count
  lp = lpAppendInteger(lp, 0);  // deleted
  lp = lpAppendInteger(lp, 0);  // num master fields
  lp = lpAppendInteger(lp, 0);  // terminator
  std::string blob(reinterpret_cast<const char*>(lp), lpBytes(lp));
  lpFree(lp);

  // The trailing LP_EOF becomes an early EOF once junk and a new final EOF follow it; patch
  // the header total-bytes to the enlarged size so lpValidateIntegrity still accepts it.
  blob.append(4, '\x7f');
  blob.push_back('\xff');
  absl::little_endian::Store32(reinterpret_cast<uint8_t*>(blob.data()), blob.size());

  std::string body;
  body.push_back(RDB_TYPE_STREAM_LISTPACKS);
  AppendString(&body, "earlyeof");               // key
  AppendLen(&body, 1);                           // one listpack node
  AppendString(&body, std::string(16, '\x01'));  // 16-byte master ID
  AppendString(&body, blob);                     // the early-EOF listpack
  AppendLen(&body, 0);                           // stream_len
  AppendLen(&body, 0);                           // last_id.ms
  AppendLen(&body, 0);                           // last_id.seq
  AppendLen(&body, 0);                           // consumer-group count

  // Skipping the corrupt key is non-fatal; the point is that it is never stored.
  std::ignore = pp_->at(0)->Await([&] { return LoadRdbData(service_.get(), WrapInRdb(body)); });
  EXPECT_THAT(Run({"EXISTS", "earlyeof"}), IntArg(0));
}

// Integration test for snapshot egress throttling (--snapshot_egress_limit_bytes).
// Bandwidth limiting is inherently time-based, so this test runs a snapshot large enough
// that the throttled run takes a few seconds. It asserts two robust properties:
//   1. The effective egress rate does not exceed the configured limit (the core guarantee).
//   2. The limit - not inherent serialization cost - is what slows the save down.
TEST_F(RdbTest, SnapshotEgressThrottle) {
  absl::FlagSaver fs;
  // Disable compression so the on-disk size equals the tracked egress and serialization
  // stays cheap, ensuring the throttle (not CPU) dominates the timing.
  SetFlag(&FLAGS_compression_mode, CompressionMode::NONE);

  // ~20MB spread across shards.
  Run({"debug", "populate", "20000", "key", "1000"});

  auto save_and_measure = [&]() -> std::pair<double, size_t> {
    int64_t start = absl::GetCurrentTimeNanos();
    RespExpr resp = Run({"save", "rdb"});
    CHECK_EQ(resp, "OK");
    double secs = double(absl::GetCurrentTimeNanos() - start) / 1e9;
    auto files = io::StatFiles(absl::StrCat(absl::GetFlag(FLAGS_dbfilename), "*"));
    CHECK(files) << files.error().message();
    size_t total = 0;
    for (const auto& f : *files)
      total += f.size;
    return {secs, total};
  };

  // Baseline save with no limit to measure the machine's serialization capacity.
  Run({"config", "set", "snapshot_egress_limit_bytes", "0"});
  auto [t_base, bytes] = save_and_measure();
  ASSERT_GT(bytes, 1u << 20) << "populated dataset too small to test throttling";

  // The limit is per-shard-thread, so throttle each shard to a small fraction of the machine's
  // measured per-shard capacity. That way the limit - not CPU - is the bottleneck on any machine
  // (including slow ASAN/CI). Aim for a run of at least a couple of seconds so several sliding
  // windows elapse. Pacing makes each shard's average rate converge to the limit, so the
  // aggregate rate converges to limit * num_shards and the expected duration is ~target_sec.
  uint64_t shards = shard_set->size();
  double target_sec = std::max(2.0, t_base * 8);
  uint64_t limit = uint64_t(bytes / shards / target_sec);
  Run({"config", "set", "snapshot_egress_limit_bytes", absl::StrCat(limit)});
  auto [t_lim, bytes2] = save_and_measure();

  double rate = double(bytes2) / t_lim;
  double per_shard_rate = rate / shards;
  LOG(INFO) << "egress throttle: bytes=" << bytes2 << " shards=" << shards << " limit=" << limit
            << "B/s t_base=" << t_base << "s t_lim=" << t_lim << "s rate=" << uint64_t(rate)
            << "B/s per_shard_rate=" << uint64_t(per_shard_rate) << "B/s";

  // Core guarantee: each shard's achieved egress rate stays at/below the limit. The only slack is
  // the one-window initial burst plus per-bucket overshoot, comfortably within 1.6x.
  EXPECT_LE(per_shard_rate, limit * 1.6) << "egress exceeded the configured per-shard limit";

  // Sanity: the slowdown is caused by the limit, not by inherent save cost.
  EXPECT_GT(t_lim, t_base * 3);
}

TEST_F(RdbTest, EofWithRemoteShardChunksPending) {
  // This test creates a key whose RDB chunk is dispatched to a remote shard (not the shard
  // driving the load), and simulates the source stream ending (EOF) before all of that chunk's
  // promised elements arrive. This exercises the case where a remote-shard chunk is left
  // incomplete/pending when EOF is hit, verifying the loader neither errors out nor leaves a
  // partially-built key behind.
  ASSERT_GT(shard_set->size(), 1);  // need >1 shard so we can pick a key on a non-zero shard

  std::string key;
  ShardId sid = 0;

  for (auto i = 0; i < 1000; ++i) {
    key = absl::StrCat("uc-", i);
    sid = Shard(key, shard_set->size());
    if (sid > 0)
      break;
  }
  ASSERT_GT(sid, 0);

  std::string chunk;
  chunk.push_back(RDB_TYPE_HASH);
  AppendString(&chunk, key);
  AppendLen(&chunk, 2);  // promise 2 fields, only 1 will follow
  AddKV(&chunk, "field", "v1");

  const std::string body = MakeTaggedChunk(1, chunk);

  const auto ec = pp_->at(0)->Await([&] { return LoadRdbData(service_.get(), WrapInRdb(body)); });
  ASSERT_FALSE(ec) << ec.message();  // EOF with pending remote chunk must not surface as an error

  // Key was never fully loaded before EOF, so it must not exist.
  EXPECT_EQ(Run({"EXISTS", key}), 0);
}

// drakeydb: P4-2 Task 1 -- pins num_shards=1 (RdbTest's default num_threads_ == 3 gives 2 shards)
// so a single-shard capture is guaranteed to see every key in this fixture's tests, regardless of
// hash placement. FlagSaver restores active_replica/num_shards for every later RdbTest case in
// this binary (gtest runs the whole file in one process).
class RdbMvccTest : public RdbTest {
 protected:
  RdbMvccTest() {
    absl::SetFlag(&FLAGS_active_replica, true);
    num_threads_ = 1;
    absl::SetFlag(&FLAGS_num_shards, 1);
  }

  absl::FlagSaver saver_;
};

// drakeydb: P4-2 Task 1 -- proves RDB_OPCODE_DF_MVCC (221 / 0xDD) is emitted exactly once per
// stamped key, immediately before that key's type byte, and never for a key whose side-table
// stamp is zero (unstamped/absent). Drives a REAL single-shard-with-summary RdbSaver capture over
// a REAL DbSlice -- the same SaveHeader/StartSnapshotInShard sequence
// PeerFullSyncFiltersConcurrentJournalPlainReplicaUnaffected above uses for its full-sync capture,
// minus journal streaming (this is a plain point-in-time snapshot, not a stable-sync stream) --
// so the opcode's SaveEntry/SerializeEntry/GetMvcc plumbing is exercised end to end, not
// hand-simulated. Byte-scanned rather than round-tripped through a loader: Task 2 (the read side)
// is a separate, not-yet-landed task, and an unrecognized opcode 221 would fail a loader round
// trip for the wrong reason.
TEST_F(RdbMvccTest, EmitsOpcodeOnlyForTheStampedKey) {
  ASSERT_TRUE(IsActiveReplica());
  ASSERT_EQ(Run({"set", "k0", "v0"}), "OK");
  ASSERT_EQ(Run({"set", "k1", "v1"}), "OK");

  const MvccStamp kStamp{0x0123456789ABCDEFULL, 0xFEDCBA9876543210ULL};
  shard_set->Await(0, [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    // Overwrites whatever the real write path minted (active mode stamps every write): k1 gets an
    // exact, recognizable stamp, and k0 is forced back to zero -- "unstamped" -- which is also
    // what an unversioned key looks like after a plain SET under a not-yet-active P4-1 node.
    db_slice.SetMvcc(0, std::string_view{"k1"}, kStamp);
    db_slice.SetMvcc(0, std::string_view{"k0"}, MvccStamp{});
  });

  io::StringSink sink;
  std::string bytes = pp_->at(0)->Await([&]() -> std::string {
    RdbSaver saver(&sink, SaveMode::SINGLE_SHARD_WITH_SUMMARY, /*align_writes=*/false, "",
                   DflyVersion::CURRENT_VER);
    ExecutionState cntx;
    EngineShard* shard = EngineShard::tlocal();
    CHECK(!saver.SaveHeader(RdbSaver::GetGlobalData(service_.get(), true)));

    // DbSlice::RegisterOnChange (inside StartSnapshotInShard's SliceSnapshot::Start) DCHECKs the
    // shard's intent lock is held; ordinary command dispatch holds it via transaction scheduling,
    // but this test drives RdbSaver directly, off that path.
    shard->shard_lock()->Acquire(IntentLock::EXCLUSIVE);
    saver.StartSnapshotInShard(/*stream_journal=*/false, &cntx, shard);
    CHECK(!saver.WaitSnapshotInShard(shard));
    shard->shard_lock()->Release(IntentLock::EXCLUSIVE);
    return std::move(sink).str();
  });

  std::string key0_encoded, key1_encoded;
  AppendString(&key0_encoded, "k0");
  AppendString(&key1_encoded, "k1");
  ASSERT_NE(bytes.find(key0_encoded), std::string::npos) << "k0 must have been serialized";
  ASSERT_NE(bytes.find(key1_encoded), std::string::npos) << "k1 must have been serialized";

  // Positive coverage for SaveAux's breadcrumb: active mode must actually write it. (The gating
  // test below only proves it is ABSENT when inactive; without this, deleting the
  // SaveAuxFieldStrStr call entirely would leave the whole suite green.)
  EXPECT_NE(bytes.find("drakeydb-mvcc"), std::string::npos)
      << "active mode must emit the drakeydb-mvcc aux breadcrumb";

  uint8_t block[17] = {0xDD};
  absl::little_endian::Store64(block + 1, kStamp.packed);
  absl::little_endian::Store64(block + 9, kStamp.origin_hash);
  std::string_view mvcc_block(reinterpret_cast<const char*>(block), sizeof(block));

  size_t pos = bytes.find(mvcc_block);
  ASSERT_NE(pos, std::string::npos) << "expected a 0xDD opcode block carrying k1's exact stamp";

  // The block must sit immediately before k1's type byte: exactly one byte (the RDB type), then
  // k1's own key encoding.
  size_t after_block = pos + mvcc_block.size();
  ASSERT_LE(after_block + 1 + key1_encoded.size(), bytes.size());
  EXPECT_EQ(bytes.substr(after_block + 1, key1_encoded.size()), key1_encoded)
      << "0xDD opcode block must be positioned before k1's type byte";

  // 0xDD's first (and only legitimate) occurrence in the whole buffer is k1's block above -- so no
  // 0xDD opcode precedes k0 (its stamp is zero == unstamped == absent), and the block does not
  // appear a second time anywhere else either.
  EXPECT_EQ(bytes.find(static_cast<char>(0xDD)), pos)
      << "0xDD must not appear anywhere before k1's opcode block (e.g., preceding k0)";
  EXPECT_EQ(bytes.find(static_cast<char>(0xDD), pos + 1), std::string::npos)
      << "0xDD must not appear a second time anywhere in the buffer";
}

// drakeydb: P4-2 Task 1 -- the write side is active-only (spec D-7, "the single most important
// compatibility rule in the phase"): with --active_replica off, neither the RDB_OPCODE_DF_MVCC
// byte nor its "drakeydb-mvcc" breadcrumb aux field may appear, even though both keys below get a
// real (non-mvcc-table-backed) write. Uses plain RdbTest -- inactive is upstream's default, so no
// extra fixture scaffolding is needed.
TEST_F(RdbTest, NoMvccOpcodeOrAuxWhenInactive) {
  ASSERT_FALSE(IsActiveReplica());
  ASSERT_EQ(Run({"set", "k0", "v0"}), "OK");
  ASSERT_EQ(Run({"set", "k1", "v1"}), "OK");

  io::StringSink sink;
  std::string bytes = pp_->at(0)->Await([&]() -> std::string {
    RdbSaver saver(&sink, SaveMode::SINGLE_SHARD_WITH_SUMMARY, /*align_writes=*/false, "",
                   DflyVersion::CURRENT_VER);
    ExecutionState cntx;
    EngineShard* shard = EngineShard::tlocal();
    CHECK(!saver.SaveHeader(RdbSaver::GetGlobalData(service_.get(), true)));

    shard->shard_lock()->Acquire(IntentLock::EXCLUSIVE);
    saver.StartSnapshotInShard(/*stream_journal=*/false, &cntx, shard);
    CHECK(!saver.WaitSnapshotInShard(shard));
    shard->shard_lock()->Release(IntentLock::EXCLUSIVE);
    return std::move(sink).str();
  });

  EXPECT_EQ(bytes.find("drakeydb-mvcc"), std::string::npos);
  EXPECT_EQ(bytes.find(static_cast<char>(0xDD)), std::string::npos);
}

// drakeydb: P4-2 Task 2 -- the read-side counterpart to EmitsOpcodeOnlyForTheStampedKey above:
// hand-builds the exact 17-byte block that test proved the saver emits (0xDD, then the stamp's
// packed/origin_hash as raw LE uint64s) immediately before a key's type byte, feeds it through a
// REAL RdbLoader (WrapInRdb/LoadRdbData -- the same helpers InterleavedLoad and friends use
// earlier in this file), and asserts the loaded stamp equals the original bytes exactly: never
// re-minted, installed verbatim. This is the invariant P4-2 exists for -- "a key's stamp advances
// iff that same stamp is propagated" -- checked here for the load-as-propagation-by-snapshot
// direction specifically.
TEST_F(RdbMvccTest, LoadInstallsThePersistedStampVerbatim) {
  ASSERT_TRUE(IsActiveReplica());

  const MvccStamp kStamp{0x0123456789ABCDEFULL, 0xFEDCBA9876543210ULL};
  std::string body;
  uint8_t block[17] = {0xDD};
  absl::little_endian::Store64(block + 1, kStamp.packed);
  absl::little_endian::Store64(block + 9, kStamp.origin_hash);
  body.append(reinterpret_cast<const char*>(block), sizeof(block));
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "k1");
  AppendString(&body, "v1");

  auto ec = pp_->at(0)->Await([&] { return LoadRdbData(service_.get(), WrapInRdb(body)); });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_EQ(Run({"get", "k1"}), "v1");

  std::optional<MvccStamp> got;
  shard_set->Await(0, [&] {
    got = namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, std::string_view{"k1"});
  });
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(*got, kStamp)
      << "a loaded key's stamp must equal the persisted RDB_OPCODE_DF_MVCC bytes exactly, "
         "never re-minted by the loader";
}

// drakeydb: P4-2 Task 2 -- the counterpart to NoMvccOpcodeOrAuxWhenInactive above: a body with no
// RDB_OPCODE_DF_MVCC record before the key's type byte is byte-for-byte what that test proved an
// inactive save produces (also what an unstamped/absent key looks like on an active save, e.g.
// k0 in EmitsOpcodeOnlyForTheStampedKey -- the loader cannot tell, and D-7 says it must not try:
// the read is unconditional on the record's presence, never on the local node's own
// active-ness). Loaded here by an ACTIVE node specifically, so the mvcc table actually exists and
// this proves the {0,0} fallback is an explicit, dense slot -- not merely "no crash" -- exactly
// like the unconditional SetMvcc call for a has_mc_flags-less key already is for mc_flags.
TEST_F(RdbMvccTest, LoadFallsBackToZeroStampWhenOpcodeAbsent) {
  ASSERT_TRUE(IsActiveReplica());

  std::string body;
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "k0");
  AppendString(&body, "v0");

  auto ec = pp_->at(0)->Await([&] { return LoadRdbData(service_.get(), WrapInRdb(body)); });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_EQ(Run({"get", "k0"}), "v0");

  std::optional<MvccStamp> got;
  shard_set->Await(0, [&] {
    got = namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, std::string_view{"k0"});
  });
  ASSERT_TRUE(got.has_value()) << "an active loader must still leave a dense {0,0} slot";
  EXPECT_TRUE(got->Empty())
      << "no RDB_OPCODE_DF_MVCC record for this key (e.g. a snapshot produced by a node with "
         "--active_replica off) must fall back to {0,0}, D-7's unversioned default -- the "
         "fallback must survive Task 2's new opcode-aware path";
}

// drakeydb: P4-3 Task 5 -- the main round-trip proof: two real deletes (via the live DEL path,
// PerformDeletionAtomic, not hand-built bytes) mint two real tombstones; a real `debug reload`
// (save into an in-memory RDB, flush, reload -- the same mechanism
// ActiveReloadDoesNotWarnOnRecognizedMvccAux above already relies on) must ship
// RDB_OPCODE_DF_TOMBSTONES and re-install both, verbatim, into the freshly reloaded (still
// active) instance. This is also the falsification target for this task: temporarily
// short-circuiting SliceSnapshot::SerializeTombstones (snapshot.cc) to a no-op makes this test
// fail (see task-5-report.md for the verbatim failure text).
TEST_F(RdbMvccTest, TombstonesSurviveDebugReload) {
  ASSERT_TRUE(IsActiveReplica());
  ASSERT_EQ(Run({"set", "k1", "v1"}), "OK");
  ASSERT_EQ(Run({"set", "k2", "v2"}), "OK");
  ASSERT_THAT(Run({"del", "k1", "k2"}), IntArg(2));

  std::optional<MvccStamp> stamp1_before, stamp2_before;
  size_t tombstones_before = 0;
  shard_set->Await(0, [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    stamp1_before = db_slice.GetMvcc(0, std::string_view{"k1"});
    stamp2_before = db_slice.GetMvcc(0, std::string_view{"k2"});
    tombstones_before = db_slice.GetDBTable(0)->stats.mvcc_tombstones;
  });
  ASSERT_TRUE(stamp1_before.has_value());
  ASSERT_TRUE(stamp1_before->IsTombstone());
  ASSERT_TRUE(stamp2_before.has_value());
  ASSERT_TRUE(stamp2_before->IsTombstone());
  ASSERT_EQ(tombstones_before, 2u);

  ASSERT_EQ(Run({"debug", "reload"}), "OK");

  EXPECT_THAT(Run({"exists", "k1"}), IntArg(0));
  EXPECT_THAT(Run({"exists", "k2"}), IntArg(0));

  std::optional<MvccStamp> stamp1_after, stamp2_after;
  size_t tombstones_after = 0;
  shard_set->Await(0, [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    stamp1_after = db_slice.GetMvcc(0, std::string_view{"k1"});
    stamp2_after = db_slice.GetMvcc(0, std::string_view{"k2"});
    tombstones_after = db_slice.GetDBTable(0)->stats.mvcc_tombstones;
  });

  ASSERT_TRUE(stamp1_after.has_value()) << "k1's tombstone must survive the reload";
  EXPECT_EQ(*stamp1_after, *stamp1_before)
      << "packed (bit 63 included) and origin_hash must round-trip exactly";
  EXPECT_TRUE(stamp1_after->IsTombstone());

  ASSERT_TRUE(stamp2_after.has_value()) << "k2's tombstone must survive the reload";
  EXPECT_EQ(*stamp2_after, *stamp2_before);
  EXPECT_TRUE(stamp2_after->IsTombstone());

  EXPECT_EQ(tombstones_after, tombstones_before);
}

// drakeydb: P4-3 Task 5, D-10 -- a tombstone already past its GC deadline at save time must be
// dropped from the file entirely, not merely re-persisted for TombstoneGcStep to reclaim later.
// Setting the ttl flag to 0 AFTER the delete (but before reload) is a deterministic way to force
// "already expired" without a sleep: DeadlineMs(0) == MsPart(), which is always <= the reload's
// own "now" for a tombstone minted moments earlier as part of this same test. RdbMvccTest's own
// absl::FlagSaver member restores the flag afterward.
TEST_F(RdbMvccTest, SaveTimeGcDropsExpiredTombstone) {
  ASSERT_TRUE(IsActiveReplica());
  ASSERT_EQ(Run({"set", "k1", "v1"}), "OK");
  ASSERT_THAT(Run({"del", "k1"}), IntArg(1));

  std::optional<MvccStamp> before;
  shard_set->Await(0, [&] {
    before =
        namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, std::string_view{"k1"});
  });
  ASSERT_TRUE(before.has_value());
  ASSERT_TRUE(before->IsTombstone());

  absl::SetFlag(&FLAGS_multi_master_tombstone_ttl, 0);

  ASSERT_EQ(Run({"debug", "reload"}), "OK");

  std::optional<MvccStamp> after;
  shard_set->Await(0, [&] {
    after =
        namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, std::string_view{"k1"});
  });
  EXPECT_FALSE(after.has_value())
      << "an already-expired tombstone must not ride the RDB at all -- debug reload flushes the "
         "live dataset first, so a surviving slot here can only have come from the file";
}

// drakeydb: P4-3 Task 8, controller fix (I4) -- TDD pin for a code defect the review caught: the
// LOAD side had NO --multi_master_tombstone_ttl=0 gate on a non-merge tombstone install (this
// install lambda's final, unconditional SetTombstone call, rdb_load.cc), while TombstoneGcStep
// (db_slice.cc) unconditionally no-ops at ttl=0 -- an active node booted with
// --multi_master_tombstone_ttl=0 that loads a file carrying a persisted tombstone section (e.g.
// its own prior SAVE, taken while the ttl was still nonzero) would install an IMMORTAL tombstone:
// never reapable, and permanently authoritative over any future merge for that key -- the exact
// failure class Task 3's Mvcc()==0 rejection (this file's own tests, above) and this function's
// resident_live guard both exist to prevent, reached here via a third route neither covers.
// SAVE happens first, while ttl is still nonzero, so the tombstone is genuinely written to the
// file (contrast SaveTimeGcDropsExpiredTombstone above, which tests the SAVE-time drop this test
// must NOT trigger); ttl then flips to 0; `DEBUG RELOAD NOSAVE` reloads from that already-written
// file under the new, disabled setting -- the only way to exercise the LOAD-time gate
// independently of the SAVE-time one.
TEST_F(RdbMvccTest, LoadSkipsTombstoneInstallWhenTombstoningDisabled) {
  ASSERT_TRUE(IsActiveReplica());
  ASSERT_EQ(Run({"set", "k1", "v1"}), "OK");
  ASSERT_THAT(Run({"del", "k1"}), IntArg(1));

  std::optional<MvccStamp> before;
  shard_set->Await(0, [&] {
    before =
        namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, std::string_view{"k1"});
  });
  ASSERT_TRUE(before.has_value());
  ASSERT_TRUE(before->IsTombstone());

  ASSERT_EQ(Run({"save", "df"}), "OK");  // tombstone written to disk while ttl is still nonzero

  absl::SetFlag(&FLAGS_multi_master_tombstone_ttl, 0);

  ASSERT_EQ(Run({"debug", "reload", "NOSAVE"}), "OK");  // reload from disk under ttl=0

  std::optional<MvccStamp> after;
  size_t tombstones_after = 0;
  shard_set->Await(0, [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    after = db_slice.GetMvcc(0, std::string_view{"k1"});
    tombstones_after = db_slice.GetDBTable(0)->stats.mvcc_tombstones;
  });
  EXPECT_FALSE(after.has_value())
      << "tombstoning is disabled (ttl=0) on this node -- installing this tombstone from the file "
         "would create one TombstoneGcStep can never reap (it unconditionally no-ops at ttl=0)";
  EXPECT_EQ(tombstones_after, 0u);
}

// drakeydb: P4-3 Task 5 review fix (I2) -- SerializeTombstones (snapshot.cc) now chunks a db's
// tombstones into bounded RDB_OPCODE_DF_TOMBSTONES sections (kChunkSize = 1000) instead of
// materializing and emitting the whole db in one section, to bound peak memory and let the
// shard fiber yield/throttle between chunks. This creates more tombstones than one chunk holds
// (comfortably above kChunkSize, tolerant of that constant changing slightly) for a single db on
// a single shard (RdbMvccTest pins one shard), forcing the save side to emit -- and the load
// side to correctly accumulate -- more than one section for the same db. A bug that dropped or
// overwrote entries past the first chunk would fail this round trip, not just "look chunky" in
// the wire bytes.
TEST_F(RdbMvccTest, SurvivesMultipleTombstoneSectionsForOneDb) {
  ASSERT_TRUE(IsActiveReplica());
  constexpr int kNumKeys = 1500;

  for (int i = 0; i < kNumKeys; ++i) {
    std::string key = absl::StrCat("multi-", i);
    ASSERT_EQ(Run({"set", key, "v"}), "OK");
    ASSERT_THAT(Run({"del", key}), IntArg(1));
  }

  size_t tombstones_before = 0;
  shard_set->Await(0, [&] {
    tombstones_before =
        namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetDBTable(0)->stats.mvcc_tombstones;
  });
  ASSERT_EQ(tombstones_before, static_cast<size_t>(kNumKeys));

  ASSERT_EQ(Run({"debug", "reload"}), "OK");

  size_t tombstones_after = 0;
  int missing = 0;
  shard_set->Await(0, [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    tombstones_after = db_slice.GetDBTable(0)->stats.mvcc_tombstones;
    for (int i = 0; i < kNumKeys; ++i) {
      auto got = db_slice.GetMvcc(0, std::string_view{absl::StrCat("multi-", i)});
      if (!got.has_value() || !got->IsTombstone())
        ++missing;
    }
  });
  EXPECT_EQ(tombstones_after, tombstones_before);
  EXPECT_EQ(missing, 0) << "every tombstone across multiple sections for the same db must "
                           "survive the round trip";
}

// drakeydb: P4-3 Task 5 -- D-7's read-unconditional counterpart to
// NoMvccOpcodeOrAuxWhenInactive/LoadConsumesAndDiscardsMvccRecordWhenInactive above, for the new
// opcode: an inactive loader must still fully consume a well-formed RDB_OPCODE_DF_TOMBSTONES
// section (proved by k2 loading correctly right after it -- if the section were mis-consumed the
// stream would desync here exactly like those tests' own k2), while installing nothing (an
// inactive node's DbTable::mvcc is always null, table.cc).
TEST_F(RdbTest, LoadConsumesAndDiscardsTombstoneSectionWhenInactive) {
  ASSERT_FALSE(IsActiveReplica());

  const MvccStamp kStamp{MvccClock::kTombstoneBit | (0x0123456789ULL << MvccClock::kCounterBits),
                         0xFEDCBA9876543210ULL};
  std::string body = BuildTombstoneSection(0, {{"ghost", kStamp}});
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "k1");
  AppendString(&body, "v1");
  // No separator between the tombstone section and k2 either: proves the section's byte count
  // (not just its opcode) was consumed precisely.
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "k2");
  AppendString(&body, "v2");

  auto ec = pp_->at(0)->Await([&] { return LoadRdbData(service_.get(), WrapInRdb(body)); });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_EQ(Run({"get", "k1"}), "v1");
  EXPECT_EQ(Run({"get", "k2"}), "v2");

  std::optional<MvccStamp> got;
  shard_set->Await(Shard("ghost", shard_set->size()), [&] {
    got =
        namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, std::string_view{"ghost"});
  });
  EXPECT_FALSE(got.has_value())
      << "an inactive node must never install a tombstone -- GetMvcc must return nullopt, "
         "proving SetTombstone's own `if (!db.mvcc) return;` guard (db_slice.cc) discarded it";
}

// drakeydb: P4-3 Task 5 -- carried contract from Task 3 (mvcc.h/db_slice.cc's TombstoneGcStep):
// a persisted tombstone whose masked Mvcc() is 0 would reconstruct into the exact placeholder
// value TombstoneGcStep's own reap predicate depends on never reaping -- installing it here would
// make it immortal. The loader must reject it instead, loudly but without failing the load.
TEST_F(RdbMvccTest, RejectsPersistedTombstoneWithZeroMvcc) {
  ASSERT_TRUE(IsActiveReplica());
  ScopedLogCapture log_capture;

  const MvccStamp kZeroMvcc{MvccClock::kTombstoneBit, 0xDEADBEEFULL};  // Mvcc() == 0, bit 63 set.
  std::string body = BuildTombstoneSection(0, {{"ghost", kZeroMvcc}});
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "k2");
  AppendString(&body, "v2");

  auto ec = pp_->at(0)->Await([&] { return LoadRdbData(service_.get(), WrapInRdb(body)); });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_EQ(Run({"get", "k2"}), "v2") << "the section's bytes must still be fully consumed";

  std::optional<MvccStamp> got;
  shard_set->Await(Shard("ghost", shard_set->size()), [&] {
    got =
        namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, std::string_view{"ghost"});
  });
  EXPECT_FALSE(got.has_value()) << "a Mvcc()==0 tombstone must never be installed";

  bool warned = std::any_of(log_capture.logs.begin(), log_capture.logs.end(), [](const auto& l) {
    return l.find("Mvcc() == 0") != std::string::npos && l.find("ghost") != std::string::npos;
  });
  EXPECT_TRUE(warned) << "expected a WARNING naming the rejected key and the reason";
}

// drakeydb: P4-3 Task 5 -- carried contract from Task 3: a persisted "tombstone" with bit 63
// clear is not a tombstone at all -- a malformed file (our own saver never produces this, since
// SerializeTombstones only ever collects slots that already satisfy IsTombstone()). Reject it
// rather than installing a live-looking stamp via SetTombstone, which would corrupt
// mvcc_tombstones' accounting.
TEST_F(RdbMvccTest, RejectsPersistedTombstoneMissingTombstoneBit) {
  ASSERT_TRUE(IsActiveReplica());
  ScopedLogCapture log_capture;

  const MvccStamp kLiveLooking{0x0123456789ABCDEFULL & MvccClock::kStampMask, 0xBEEF};
  ASSERT_FALSE(kLiveLooking.IsTombstone());
  std::string body = BuildTombstoneSection(0, {{"ghost", kLiveLooking}});
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "k2");
  AppendString(&body, "v2");

  auto ec = pp_->at(0)->Await([&] { return LoadRdbData(service_.get(), WrapInRdb(body)); });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_EQ(Run({"get", "k2"}), "v2") << "the section's bytes must still be fully consumed";

  std::optional<MvccStamp> got;
  shard_set->Await(Shard("ghost", shard_set->size()), [&] {
    got =
        namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, std::string_view{"ghost"});
  });
  EXPECT_FALSE(got.has_value()) << "a non-tombstone entry in this section must never be installed";

  bool warned = std::any_of(log_capture.logs.begin(), log_capture.logs.end(), [](const auto& l) {
    return l.find("bit 63 clear") != std::string::npos && l.find("ghost") != std::string::npos;
  });
  EXPECT_TRUE(warned) << "expected a WARNING naming the rejected key and the reason";
}

// drakeydb: P4-3 Task 5 review fix (I4) -- renamed and rewritten from
// RejectsPersistedTombstoneForKeyLiveInSameFile: that name and framing were wrong. This section
// is a PROLOGUE, parsed and (if active) installed before this shard's own key stream even
// begins, so the guard being tested here can only ever see a key that was RESIDENT on this node
// BEFORE the file started loading -- never a later key from the same file (see the corrected
// comment on this reject case in rdb_load.cc for the full account, including why our own
// saver's legitimate same-file dual-emission -- delete-then-recreate straddling the snapshot --
// is a different, self-repairing case this guard never even observes). "dup" is made resident
// via a real SET BEFORE LoadRdbData runs, independent of anything the loaded body itself
// contains -- the body below carries only a well-formed tombstone section for "dup" (no live
// record for it at all) plus a trailing key to prove the section's bytes are still fully
// consumed. Safe choice (documented in rdb_load.cc): keep the resident value, skip the
// tombstone.
TEST_F(RdbMvccTest, RejectsPersistedTombstoneForResidentLiveKey) {
  ASSERT_TRUE(IsActiveReplica());
  ScopedLogCapture log_capture;

  ASSERT_EQ(Run({"set", "dup", "livevalue"}), "OK");
  std::optional<MvccStamp> before;
  shard_set->Await(Shard("dup", shard_set->size()), [&] {
    before =
        namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, std::string_view{"dup"});
  });
  ASSERT_TRUE(before.has_value());
  ASSERT_FALSE(before->IsTombstone());

  const MvccStamp kWellFormed{MvccClock::kTombstoneBit | (12345ULL << MvccClock::kCounterBits),
                              0xAAAA};
  std::string body = BuildTombstoneSection(0, {{"dup", kWellFormed}});
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "after");
  AppendString(&body, "afterval");

  auto ec = pp_->at(0)->Await([&] { return LoadRdbData(service_.get(), WrapInRdb(body)); });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_EQ(Run({"get", "dup"}), "livevalue") << "the resident value must survive untouched";
  EXPECT_EQ(Run({"get", "after"}), "afterval") << "bytes after the section must still parse";

  std::optional<MvccStamp> after;
  shard_set->Await(Shard("dup", shard_set->size()), [&] {
    after =
        namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, std::string_view{"dup"});
  });
  ASSERT_TRUE(after.has_value()) << "an active node keeps a dense slot for a resident live key";
  EXPECT_EQ(*after, *before)
      << "the resident key's own stamp must survive untouched -- the tombstone must never have "
         "been installed over it";

  bool warned = std::any_of(log_capture.logs.begin(), log_capture.logs.end(), [](const auto& l) {
    return l.find("resident") != std::string::npos && l.find("dup") != std::string::npos;
  });
  EXPECT_TRUE(warned) << "expected a WARNING naming the rejected key and the reason";
}

// drakeydb: P4-3 Task 6 -- the delete half of the resurrection fix: applying a peer's WINNING
// tombstone must delete a resident LIVE key, not merely decline to (re)write it (Task 5 stopped at
// "skip and warn" for exactly this case -- see the I4 comment on that branch in rdb_load.cc, which
// explicitly defers to this task). Peer B deleted "k" at t=0x2000 while this node was down holding
// "k"@0x1000; B's snapshot carries a tombstone for "k" and no "k" key at all, so this tombstone
// record is the ONLY way this node ever learns about the delete -- the resurrection hole Task 6
// closes.
TEST_F(RdbMvccTest, MergeLwwWinningTombstoneDeletesResidentLiveKey) {
  ASSERT_TRUE(IsActiveReplica());
  constexpr uint64_t kPeerHash = 0xFEDCBA9876543210ULL;
  constexpr uint64_t kSelfHash = 0x1122334455667788ULL;
  const MvccStamp kResidentStamp{0x1000, kSelfHash};
  const MvccStamp kIncomingTombstone = MvccStamp{0x2000, kPeerHash}.AsTombstone();

  ASSERT_EQ(Run({"set", "k", "resident_v"}), "OK");
  shard_set->Await(0, [&] {
    namespaces->GetDefaultNamespace().GetCurrentDbSlice().SetMvcc(0, std::string_view{"k"},
                                                                  kResidentStamp);
  });

  std::string body = BuildTombstoneSection(0, {{"k", kIncomingTombstone}});
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "after");
  AppendString(&body, "afterval");

  const std::string rdb = WrapInRdb(body);
  io::BytesSource src{io::Buffer(rdb)};
  RdbLoadContext load_context;
  auto ec = pp_->at(0)->Await([&]() -> std::error_code {
    RdbLoader loader(service_.get(), &load_context);
    loader.SetMergeLww(true, kPeerHash);
    return loader.Load(&src);
  });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_THAT(Run({"get", "k"}), kMatchNil) << "a winning peer tombstone must delete our live key";
  EXPECT_EQ(Run({"get", "after"}), "afterval") << "bytes after the section must still parse";

  std::optional<MvccStamp> got;
  size_t mismatches = 0;
  shard_set->Await(0, [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    got = db_slice.GetMvcc(0, std::string_view{"k"});
    mismatches = db_slice.TEST_VerifyMvccTable(0);
  });
  ASSERT_TRUE(got.has_value()) << "the tombstone must be installed with the peer's stamp";
  EXPECT_EQ(*got, kIncomingTombstone)
      << "no leftover {kTombstoneBit, 0} placeholder from the delete -- the peer's real stamp "
         "must have overwritten it";
  EXPECT_TRUE(got->IsTombstone());
  EXPECT_EQ(mismatches, 0u) << "dense invariant must hold after the delete";
}

// The guard: the same resident key, but the incoming tombstone is OLDER than the resident value's
// own stamp. MergeAccepts must reject it -- the apply is itself LWW-guarded -- leaving both the
// value and its stamp completely untouched, and no tombstone installed at all.
TEST_F(RdbMvccTest, MergeLwwStaleTombstoneLeavesResidentLiveKeyIntact) {
  ASSERT_TRUE(IsActiveReplica());
  constexpr uint64_t kPeerHash = 0xFEDCBA9876543210ULL;
  constexpr uint64_t kSelfHash = 0x1122334455667788ULL;
  const MvccStamp kResidentStamp{0x1000, kSelfHash};
  const MvccStamp kIncomingTombstone = MvccStamp{0x0500, kPeerHash}.AsTombstone();  // stale

  ASSERT_EQ(Run({"set", "k", "resident_v"}), "OK");
  shard_set->Await(0, [&] {
    namespaces->GetDefaultNamespace().GetCurrentDbSlice().SetMvcc(0, std::string_view{"k"},
                                                                  kResidentStamp);
  });

  std::string body = BuildTombstoneSection(0, {{"k", kIncomingTombstone}});
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "after");
  AppendString(&body, "afterval");

  const std::string rdb = WrapInRdb(body);
  io::BytesSource src{io::Buffer(rdb)};
  RdbLoadContext load_context;
  auto ec = pp_->at(0)->Await([&]() -> std::error_code {
    RdbLoader loader(service_.get(), &load_context);
    loader.SetMergeLww(true, kPeerHash);
    return loader.Load(&src);
  });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_EQ(Run({"get", "k"}), "resident_v")
      << "a stale peer tombstone must never delete a newer resident value";
  EXPECT_EQ(Run({"get", "after"}), "afterval") << "bytes after the section must still parse";

  std::optional<MvccStamp> got;
  size_t mismatches = 0;
  shard_set->Await(0, [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    got = db_slice.GetMvcc(0, std::string_view{"k"});
    mismatches = db_slice.TEST_VerifyMvccTable(0);
  });
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(*got, kResidentStamp) << "the resident stamp must survive untouched";
  EXPECT_FALSE(got->IsTombstone());
  EXPECT_EQ(mismatches, 0u) << "dense invariant must hold: the rejected tombstone touched nothing";
}

// The second guarded path: a resident TOMBSTONE, not a live key. SetTombstone alone overwrites
// unconditionally, so without this guard a stale persisted tombstone would regress a newer
// resident tombstone's own stamp -- and with it, its GC deadline (DeadlineMs is derived from
// MsPart(), mvcc.h) -- silently reviving a shorter TTL than the one already committed to.
TEST_F(RdbMvccTest, MergeLwwOlderIncomingTombstoneDoesNotRegressResidentTombstone) {
  ASSERT_TRUE(IsActiveReplica());
  constexpr uint64_t kPeerHash = 0xFEDCBA9876543210ULL;
  constexpr uint64_t kSelfHash = 0x1122334455667788ULL;
  const MvccStamp kResidentTombstone = MvccStamp{0x2000, kSelfHash}.AsTombstone();
  const MvccStamp kIncomingTombstone = MvccStamp{0x1000, kPeerHash}.AsTombstone();  // older

  shard_set->Await(0, [&] {
    namespaces->GetDefaultNamespace().GetCurrentDbSlice().SetTombstone(0, std::string_view{"k"},
                                                                       kResidentTombstone);
  });

  std::string body = BuildTombstoneSection(0, {{"k", kIncomingTombstone}});
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "after");
  AppendString(&body, "afterval");

  const std::string rdb = WrapInRdb(body);
  io::BytesSource src{io::Buffer(rdb)};
  RdbLoadContext load_context;
  auto ec = pp_->at(0)->Await([&]() -> std::error_code {
    RdbLoader loader(service_.get(), &load_context);
    loader.SetMergeLww(true, kPeerHash);
    return loader.Load(&src);
  });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_EQ(Run({"get", "after"}), "afterval") << "bytes after the section must still parse";

  std::optional<MvccStamp> got;
  size_t mismatches = 0;
  shard_set->Await(0, [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    got = db_slice.GetMvcc(0, std::string_view{"k"});
    mismatches = db_slice.TEST_VerifyMvccTable(0);
  });
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(*got, kResidentTombstone)
      << "an older incoming tombstone must never regress a newer resident tombstone's stamp";
  EXPECT_TRUE(got->IsTombstone());
  EXPECT_EQ(mismatches, 0u) << "dense invariant must hold";
}

TEST_F(RdbMvccTest, MergeLwwNewerIncomingTombstoneUpdatesResidentTombstone) {
  ASSERT_TRUE(IsActiveReplica());
  constexpr uint64_t kPeerHash = 0xFEDCBA9876543210ULL;
  constexpr uint64_t kSelfHash = 0x1122334455667788ULL;
  const MvccStamp kResidentTombstone = MvccStamp{0x1000, kSelfHash}.AsTombstone();
  const MvccStamp kIncomingTombstone = MvccStamp{0x3000, kPeerHash}.AsTombstone();  // newer

  size_t tombstones_before = 0;
  shard_set->Await(0, [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    db_slice.SetTombstone(0, std::string_view{"k"}, kResidentTombstone);
    tombstones_before = db_slice.MutableStats(0)->mvcc_tombstones;
  });
  ASSERT_GT(tombstones_before, 0u);

  std::string body = BuildTombstoneSection(0, {{"k", kIncomingTombstone}});
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "after");
  AppendString(&body, "afterval");

  const std::string rdb = WrapInRdb(body);
  io::BytesSource src{io::Buffer(rdb)};
  RdbLoadContext load_context;
  auto ec = pp_->at(0)->Await([&]() -> std::error_code {
    RdbLoader loader(service_.get(), &load_context);
    loader.SetMergeLww(true, kPeerHash);
    return loader.Load(&src);
  });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_EQ(Run({"get", "after"}), "afterval") << "bytes after the section must still parse";

  std::optional<MvccStamp> got;
  size_t tombstones_after = 0, mismatches = 0;
  shard_set->Await(0, [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    got = db_slice.GetMvcc(0, std::string_view{"k"});
    tombstones_after = db_slice.MutableStats(0)->mvcc_tombstones;
    mismatches = db_slice.TEST_VerifyMvccTable(0);
  });
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(*got, kIncomingTombstone) << "a newer incoming tombstone must update the resident one";
  EXPECT_TRUE(got->IsTombstone());
  EXPECT_EQ(tombstones_after, tombstones_before)
      << "overwriting one tombstone with another must not change the tombstone count";
  EXPECT_EQ(mismatches, 0u) << "dense invariant must hold";
}

// Non-merge control: Task 5's plain (skip-and-warn) behavior for a resident live key is untouched
// by this task. Without SetMergeLww, the incoming tombstone's relative age is irrelevant -- Task
// 5's branch never compares stamps at all, so even a "newer" tombstone must still be skipped.
TEST_F(RdbMvccTest, WithoutMergeLwwNewerTombstoneStillSkipsResidentLiveKey) {
  ASSERT_TRUE(IsActiveReplica());
  constexpr uint64_t kPeerHash = 0xFEDCBA9876543210ULL;
  constexpr uint64_t kSelfHash = 0x1122334455667788ULL;
  const MvccStamp kResidentStamp{0x1000, kSelfHash};
  const MvccStamp kIncomingTombstone = MvccStamp{0x2000, kPeerHash}.AsTombstone();  // "newer"

  ASSERT_EQ(Run({"set", "k", "resident_v"}), "OK");
  shard_set->Await(0, [&] {
    namespaces->GetDefaultNamespace().GetCurrentDbSlice().SetMvcc(0, std::string_view{"k"},
                                                                  kResidentStamp);
  });

  std::string body = BuildTombstoneSection(0, {{"k", kIncomingTombstone}});
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "after");
  AppendString(&body, "afterval");

  auto ec = pp_->at(0)->Await([&] { return LoadRdbData(service_.get(), WrapInRdb(body)); });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_EQ(Run({"get", "k"}), "resident_v")
      << "without merge-LWW, a resident live key must never be deleted by a tombstone record, "
         "regardless of relative stamp age";

  std::optional<MvccStamp> got;
  shard_set->Await(0, [&] {
    got = namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, std::string_view{"k"});
  });
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(*got, kResidentStamp);
}

// Non-merge control, second guarded path: without SetMergeLww, SetTombstone's own pre-existing
// unconditional overwrite is exactly Task 5's behavior -- an OLDER incoming tombstone still
// regresses a newer resident tombstone's stamp. This is the plain-replica/local-load compatibility
// row the merge guard above must never touch (D-7).
TEST_F(RdbMvccTest, WithoutMergeLwwOlderTombstoneStillRegressesResidentTombstone) {
  ASSERT_TRUE(IsActiveReplica());
  constexpr uint64_t kPeerHash = 0xFEDCBA9876543210ULL;
  constexpr uint64_t kSelfHash = 0x1122334455667788ULL;
  const MvccStamp kResidentTombstone = MvccStamp{0x2000, kSelfHash}.AsTombstone();
  const MvccStamp kIncomingTombstone = MvccStamp{0x1000, kPeerHash}.AsTombstone();  // older

  shard_set->Await(0, [&] {
    namespaces->GetDefaultNamespace().GetCurrentDbSlice().SetTombstone(0, std::string_view{"k"},
                                                                       kResidentTombstone);
  });

  std::string body = BuildTombstoneSection(0, {{"k", kIncomingTombstone}});
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "after");
  AppendString(&body, "afterval");

  auto ec = pp_->at(0)->Await([&] { return LoadRdbData(service_.get(), WrapInRdb(body)); });
  ASSERT_FALSE(ec) << ec.message();

  std::optional<MvccStamp> got;
  shard_set->Await(0, [&] {
    got = namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, std::string_view{"k"});
  });
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(*got, kIncomingTombstone)
      << "without merge-LWW, SetTombstone's pre-existing unconditional overwrite is unchanged";
}

// drakeydb: P4-3 Task 6, review fix I1 (Important) -- the race itself, forced deterministically,
// for the DELETE path this time (MergeLwwRaceConcurrentWriteDuringYieldBeatsStaleSnapshot below
// already covers Task 4's key-write path). Before this fix, the merge-apply delete branch called
// Del() directly on an already-found PrimeIterator with no yield at all, on the theory that the
// idle-task eviction sweep does the same -- wrong precedent (review fix M1): that sweep runs
// inside its own FiberAtomicGuard, which this one-shot loader path has none of. Without a yield
// point, a SliceSnapshot fiber genuinely registered and suspended mid-SerializeEntry on this exact
// bucket (Task 4's own Hazard 1 finding: a BGSAVE/full-sync snapshot CAN be registered while a
// LOAD's is_replicating apply runs on this same shard thread) could have its PrimeValue freed out
// from under it by this delete's prime.Erase. The fix routes the resident-live-key branch through
// FindMutable first, exactly like AddOrFind's own found-bucket branch, so a registered
// ChangeConsumerInterface gets the same chance to run first. This test reproduces the same
// interleaving mechanism MergeLwwRaceConcurrentWriteDuringYieldBeatsStaleSnapshot below uses
// (BlockFirstCallChangeConsumer, a plain ChangeConsumerInterface that blocks on its FIRST OnChange
// call and passes every later one straight through) but drives it through the TOMBSTONE apply path
// instead of the key-write path: the loader's own FindMutable call is OnChange's call #1 (blocks,
// strictly before Del() runs), and a concurrent SET's own FindMutable->PreUpdateBlocking is call #2
// (passes straight through). No sleeps, no timing assumptions -- WaitEntered()/Release() are a
// util::fb2::Done pair, so the interleaving is exact every run.
TEST_F(RdbMvccTest, MergeLwwRaceRegisteredSnapshotDuringDeleteYieldBeatsStaleTombstone) {
  ASSERT_TRUE(IsActiveReplica());
  constexpr uint64_t kPeerHash = 0xFEDCBA9876543210ULL;
  constexpr uint64_t kSelfHash = 0x1122334455667788ULL;
  const MvccStamp kResidentStamp{0x1000, kSelfHash};
  // Beats kResidentStamp at the fast-path pre-check, BEFORE FindMutable's yield -- the whole
  // point is that this tombstone must still lose once a fresher write lands during that yield.
  const MvccStamp kIncomingTombstone = MvccStamp{0x2000, kPeerHash}.AsTombstone();

  ASSERT_EQ(Run({"set", "k", "resident_v"}), "OK");
  shard_set->Await(0, [&] {
    namespaces->GetDefaultNamespace().GetCurrentDbSlice().SetMvcc(0, std::string_view{"k"},
                                                                  kResidentStamp);
  });

  // No trailing key after the tombstone section here (unlike this file's other tombstone tests):
  // a fresh key loaded via the regular key stream would itself hit AddOrFindInternal's INSERT
  // branch, which also calls CallChangeCallbacks whenever change_cb_ is non-empty (db_slice.cc) --
  // exactly the same OnChange this test's own consumer reacts to. With a second, unrelated trigger
  // in the stream, "call #1" could come from loading that key instead of from this test's actual
  // target (the tombstone section's own resident-live-key delete), making the interleaving this
  // test exists to force ambiguous. The tombstone section alone is enough to prove the point.
  const std::string rdb = WrapInRdb(BuildTombstoneSection(0, {{"k", kIncomingTombstone}}));

  class BlockFirstCallChangeConsumer final : public DbSlice::ChangeConsumerInterface {
   public:
    void OnChange(DbIndex, const ChangeReq&) override {
      if (calls_++ == 0) {
        entered_.Notify();
        release_.Wait();
      }
    }
    void WaitEntered() {
      entered_.Wait();
    }
    void Release() {
      release_.Notify();
    }

   private:
    int calls_ = 0;
    util::fb2::Done entered_;
    util::fb2::Done release_;
  } consumer;

  io::BytesSource src{io::Buffer(rdb)};
  RdbLoadContext load_context;
  std::error_code load_ec;

  util::fb2::Fiber loader_fiber = pp_->at(0)->LaunchFiber([&] {
    EngineShard* shard = EngineShard::tlocal();
    DbSlice& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    shard->shard_lock()->Acquire(IntentLock::EXCLUSIVE);
    db_slice.RegisterOnChange(&consumer);
    shard->shard_lock()->Release(IntentLock::EXCLUSIVE);

    RdbLoader loader(service_.get(), &load_context);
    loader.SetMergeLww(true, kPeerHash);
    load_ec = loader.Load(&src);

    db_slice.UnregisterOnChange(&consumer);
  });

  // Blocks THIS (the test's own) fiber until the loader fiber is genuinely parked inside
  // OnChange's call #1 -- i.e. mid-FindMutable's PreUpdateBlocking, strictly before Del() has
  // touched anything.
  consumer.WaitEntered();

  // The concurrent write: a plain SET on the very same key, from a completely independent path,
  // mints and commits a real, fresh HopStamp -- standing in for "another peer's stable-sync apply
  // landing on this shard thread during LOADING" per main_service.cc's is_replicating admission
  // rule. Its own FindMutable->PreUpdateBlocking call is OnChange's call #2 above, which passes
  // straight through, so this completes normally.
  ASSERT_EQ(Run({"set", "k", "concurrent_v"}), "OK");

  consumer.Release();
  loader_fiber.Join();

  ASSERT_FALSE(load_ec) << load_ec.message();

  EXPECT_EQ(Run({"get", "k"}), "concurrent_v")
      << "the write that landed during FindMutable's own yield must win, even though the "
         "loader's incoming tombstone passed the earlier, non-authoritative fast-path pre-check";

  std::optional<MvccStamp> got;
  size_t mismatches = 0;
  shard_set->Await(0, [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    got = db_slice.GetMvcc(0, std::string_view{"k"});
    mismatches = db_slice.TEST_VerifyMvccTable(0);
  });
  ASSERT_TRUE(got.has_value());
  EXPECT_FALSE(got->IsTombstone())
      << "the stale incoming tombstone must not have been installed over the concurrent write's "
         "own, freshly-minted (live) stamp";
  EXPECT_NE(*got, kIncomingTombstone);
  EXPECT_EQ(mismatches, 0u) << "dense invariant must hold: the rejected delete touched nothing";
}

// drakeydb: P4-3 Task 6, review fix I2 (Important) -- with tombstones disabled node-wide
// (--multi_master_tombstone_ttl=0, TombstonesEnabled() false, multi_master.cc), a winning peer
// tombstone must still DELETE the resident live key (the peer's delete authority is real and must
// not be silently ignored) but must install NO tombstone afterward: PerformDeletionAtomic
// (db_slice.cc) itself refuses to arm one under this policy for a local delete, and blindly
// installing one anyway here would create an entry TombstoneGcStep can never reap (it no-ops
// entirely whenever !TombstonesEnabled()) -- immortal, the same failure class the Mvcc()==0
// rejection (above in HandleTombstones) exists to prevent, just reached a different way.
TEST_F(RdbMvccTest, MergeLwwWinningTombstoneDeletesLiveKeyButInstallsNoneWhenDisabled) {
  ASSERT_TRUE(IsActiveReplica());
  absl::SetFlag(&FLAGS_multi_master_tombstone_ttl, 0);
  ASSERT_FALSE(TombstonesEnabled());

  constexpr uint64_t kPeerHash = 0xFEDCBA9876543210ULL;
  constexpr uint64_t kSelfHash = 0x1122334455667788ULL;
  const MvccStamp kResidentStamp{0x1000, kSelfHash};
  const MvccStamp kIncomingTombstone = MvccStamp{0x2000, kPeerHash}.AsTombstone();

  ASSERT_EQ(Run({"set", "k", "resident_v"}), "OK");
  shard_set->Await(0, [&] {
    namespaces->GetDefaultNamespace().GetCurrentDbSlice().SetMvcc(0, std::string_view{"k"},
                                                                  kResidentStamp);
  });

  std::string body = BuildTombstoneSection(0, {{"k", kIncomingTombstone}});
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "after");
  AppendString(&body, "afterval");

  const std::string rdb = WrapInRdb(body);
  io::BytesSource src{io::Buffer(rdb)};
  RdbLoadContext load_context;
  auto ec = pp_->at(0)->Await([&]() -> std::error_code {
    RdbLoader loader(service_.get(), &load_context);
    loader.SetMergeLww(true, kPeerHash);
    return loader.Load(&src);
  });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_THAT(Run({"get", "k"}), kMatchNil)
      << "the peer's delete authority must still apply even though tombstoning is disabled";
  EXPECT_EQ(Run({"get", "after"}), "afterval") << "bytes after the section must still parse";

  std::optional<MvccStamp> got;
  size_t mismatches = 0;
  shard_set->Await(0, [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    got = db_slice.GetMvcc(0, std::string_view{"k"});
    mismatches = db_slice.TEST_VerifyMvccTable(0);
  });
  EXPECT_FALSE(got.has_value())
      << "no tombstone may be installed while --multi_master_tombstone_ttl=0 -- one would be "
         "immortal, since TombstoneGcStep never reaps anything under this policy";
  EXPECT_EQ(mismatches, 0u) << "dense invariant must hold after a plain (non-tombstoning) erase";
}

// drakeydb: P4-3 Task 6, review fix I2 (Important) -- the second half: at the per-(database,
// shard) tombstone cap (--multi_master_max_tombstones, which caps each DbTable's own count, not a
// shard overall -- see the flag's help in multi_master.cc), PerformDeletionAtomic degrades a
// kExplicit delete to a
// plain erase (counted in mvcc_tombstones_dropped) instead of arming a tombstone placeholder --
// the merge-apply path must read and respect that exact decision instead of installing the peer's
// tombstone anyway. max_tombstones=0 makes every delete hit the cap trivially (mvcc_tombstones(0)
// < max(0) is never true), without needing to pre-populate the table with real tombstones first.
TEST_F(RdbMvccTest, MergeLwwWinningTombstoneDeletesLiveKeyButInstallsNoneAtCap) {
  ASSERT_TRUE(IsActiveReplica());
  absl::SetFlag(&FLAGS_multi_master_max_tombstones, 0);

  constexpr uint64_t kPeerHash = 0xFEDCBA9876543210ULL;
  constexpr uint64_t kSelfHash = 0x1122334455667788ULL;
  const MvccStamp kResidentStamp{0x1000, kSelfHash};
  const MvccStamp kIncomingTombstone = MvccStamp{0x2000, kPeerHash}.AsTombstone();

  ASSERT_EQ(Run({"set", "k", "resident_v"}), "OK");
  size_t dropped_before = 0;
  shard_set->Await(0, [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    db_slice.SetMvcc(0, std::string_view{"k"}, kResidentStamp);
    dropped_before = db_slice.MutableStats(0)->mvcc_tombstones_dropped;
  });

  std::string body = BuildTombstoneSection(0, {{"k", kIncomingTombstone}});
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "after");
  AppendString(&body, "afterval");

  const std::string rdb = WrapInRdb(body);
  io::BytesSource src{io::Buffer(rdb)};
  RdbLoadContext load_context;
  auto ec = pp_->at(0)->Await([&]() -> std::error_code {
    RdbLoader loader(service_.get(), &load_context);
    loader.SetMergeLww(true, kPeerHash);
    return loader.Load(&src);
  });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_THAT(Run({"get", "k"}), kMatchNil)
      << "the peer's delete authority must still apply even at the tombstone cap";
  EXPECT_EQ(Run({"get", "after"}), "afterval") << "bytes after the section must still parse";

  std::optional<MvccStamp> got;
  size_t dropped_after = 0, mismatches = 0;
  shard_set->Await(0, [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    got = db_slice.GetMvcc(0, std::string_view{"k"});
    dropped_after = db_slice.MutableStats(0)->mvcc_tombstones_dropped;
    mismatches = db_slice.TEST_VerifyMvccTable(0);
  });
  EXPECT_FALSE(got.has_value())
      << "no tombstone may be installed once PerformDeletionAtomic itself degraded to a plain "
         "erase at the cap";
  EXPECT_EQ(dropped_after, dropped_before + 1)
      << "the cap-induced degradation must still be counted, exactly as a local delete's would be";
  EXPECT_EQ(mismatches, 0u) << "dense invariant must hold after a plain (capped-out) erase";
}

// drakeydb: P4-3 Task 5, D-7 -- the write-side counterpart to NoMvccOpcodeOrAuxWhenInactive
// above: an --active_replica off save must never emit RDB_OPCODE_DF_TOMBSTONES (byte 225 / 0xE1),
// even though DEL below performs a real delete.
TEST_F(RdbTest, NoTombstoneOpcodeWhenInactive) {
  ASSERT_FALSE(IsActiveReplica());
  ASSERT_EQ(Run({"set", "k0", "v0"}), "OK");
  ASSERT_THAT(Run({"del", "k0"}), IntArg(1));

  io::StringSink sink;
  std::string bytes = pp_->at(0)->Await([&]() -> std::string {
    RdbSaver saver(&sink, SaveMode::SINGLE_SHARD_WITH_SUMMARY, /*align_writes=*/false, "",
                   DflyVersion::CURRENT_VER);
    ExecutionState cntx;
    EngineShard* shard = EngineShard::tlocal();
    CHECK(!saver.SaveHeader(RdbSaver::GetGlobalData(service_.get(), true)));

    shard->shard_lock()->Acquire(IntentLock::EXCLUSIVE);
    saver.StartSnapshotInShard(/*stream_journal=*/false, &cntx, shard);
    CHECK(!saver.WaitSnapshotInShard(shard));
    shard->shard_lock()->Release(IntentLock::EXCLUSIVE);
    return std::move(sink).str();
  });

  EXPECT_EQ(bytes.find(static_cast<char>(RDB_OPCODE_DF_TOMBSTONES)), std::string::npos);
}

// drakeydb: P4-3 Task 5 review fix (I5) -- every tombstone test above uses RdbMvccTest, whose
// constructor pins num_threads_ = 1 and FLAGS_num_shards = 1: with exactly one proactor and one
// shard, a loader fiber can only ever run ON that same shard's own thread, so
// HandleTombstones's `if (EngineShard::tlocal() ... == sid) install(); else shard_set->Add(...)`
// split (rdb_load.cc) always takes the inlined branch -- the shard_set->Add cross-shard dispatch
// path had zero coverage. This fixture deliberately leaves BaseFamilyTest's own default
// (num_threads_ = 3, FLAGS_num_shards derived to 2) untouched, and drives the round trip through
// a REAL `debug reload` (ServerFamily::Load, server_family.cc): that path assigns each shard's
// own RDB file to a loader fiber via `pool.GetNextProactor()` -- a plain round robin over ALL
// proactors, not shard-aware -- so with 2 shards spread over 3 threads, at least one file's
// loader fiber is virtually certain to run on a thread that is NOT the shard that data belongs
// to, forcing the cross-shard dispatch this test exists to cover.
class RdbMvccMultiShardTest : public RdbTest {
 protected:
  RdbMvccMultiShardTest() {
    absl::SetFlag(&FLAGS_active_replica, true);
  }

  absl::FlagSaver saver_;
};

TEST_F(RdbMvccMultiShardTest, TombstonesRouteToOwningShardOnReload) {
  ASSERT_TRUE(IsActiveReplica());
  ASSERT_GT(shard_set->size(), 1u) << "this test requires more than one shard to be meaningful";

  // Shard() is a deterministic hash -- search for two keys landing on two different shards
  // rather than assuming particular literals stay stable across any future hash change.
  std::string key_a, key_b;
  ShardId shard_a = 0, shard_b = 0;
  for (int i = 0; i < 10000 && key_b.empty(); ++i) {
    std::string candidate = absl::StrCat("mk", i);
    ShardId sid = Shard(candidate, shard_set->size());
    if (key_a.empty()) {
      key_a = candidate;
      shard_a = sid;
    } else if (sid != shard_a) {
      key_b = candidate;
      shard_b = sid;
    }
  }
  ASSERT_FALSE(key_b.empty()) << "could not find two keys hashing to different shards";
  ASSERT_NE(shard_a, shard_b);

  ASSERT_EQ(Run({"set", key_a, "va"}), "OK");
  ASSERT_EQ(Run({"set", key_b, "vb"}), "OK");
  ASSERT_THAT(Run({"del", key_a, key_b}), IntArg(2));

  std::optional<MvccStamp> stamp_a_before, stamp_b_before;
  shard_set->Await(shard_a, [&] {
    stamp_a_before =
        namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, std::string_view{key_a});
  });
  shard_set->Await(shard_b, [&] {
    stamp_b_before =
        namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, std::string_view{key_b});
  });
  ASSERT_TRUE(stamp_a_before.has_value());
  ASSERT_TRUE(stamp_a_before->IsTombstone());
  ASSERT_TRUE(stamp_b_before.has_value());
  ASSERT_TRUE(stamp_b_before->IsTombstone());

  ASSERT_EQ(Run({"debug", "reload"}), "OK");

  EXPECT_THAT(Run({"exists", key_a}), IntArg(0));
  EXPECT_THAT(Run({"exists", key_b}), IntArg(0));

  std::optional<MvccStamp> stamp_a_after, stamp_b_after;
  size_t tombstones_a = 0, tombstones_b = 0;
  shard_set->Await(shard_a, [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    stamp_a_after = db_slice.GetMvcc(0, std::string_view{key_a});
    tombstones_a = db_slice.GetDBTable(0)->stats.mvcc_tombstones;
  });
  shard_set->Await(shard_b, [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    stamp_b_after = db_slice.GetMvcc(0, std::string_view{key_b});
    tombstones_b = db_slice.GetDBTable(0)->stats.mvcc_tombstones;
  });

  ASSERT_TRUE(stamp_a_after.has_value())
      << "key_a's tombstone must have routed to its owning shard (" << shard_a << ")";
  EXPECT_EQ(*stamp_a_after, *stamp_a_before);
  EXPECT_GE(tombstones_a, 1u);

  ASSERT_TRUE(stamp_b_after.has_value())
      << "key_b's tombstone must have routed to its owning shard (" << shard_b << ")";
  EXPECT_EQ(*stamp_b_after, *stamp_b_before);
  EXPECT_GE(tombstones_b, 1u);
}

// drakeydb: P4-3 Task 6 -- the merge-LWW delete-or-guard path (rdb_load.cc's HandleTombstones
// `install` lambda, `if (merge_lww)` branch) must also work when the loader fiber parsing this
// file is NOT running on the target key's owning shard thread -- the same cross-shard dispatch
// concern TombstonesRouteToOwningShardOnReload above exists to cover for the non-merge path (see
// that fixture's own I5 review-fix comment). Deliberately picks a parsing proactor whose own
// EngineShard (if it has one at all) differs from "k"'s owning shard, so `install`'s
// `shard_set->Add(sid, ...)` cross-shard branch -- not the inlined same-thread branch -- is the one
// that runs the actual GetMvcc/Del/SetTombstone sequence for this test.
TEST_F(RdbMvccMultiShardTest, MergeLwwWinningTombstoneDeletesResidentLiveKeyOnNonParsingShard) {
  ASSERT_TRUE(IsActiveReplica());
  ASSERT_GT(shard_set->size(), 1u) << "this test requires more than one shard to be meaningful";

  constexpr uint64_t kPeerHash = 0xFEDCBA9876543210ULL;
  constexpr uint64_t kSelfHash = 0x1122334455667788ULL;
  const MvccStamp kResidentStamp{0x1000, kSelfHash};
  const MvccStamp kIncomingTombstone = MvccStamp{0x2000, kPeerHash}.AsTombstone();

  ASSERT_EQ(Run({"set", "k", "resident_v"}), "OK");
  const ShardId target_shard = Shard("k", shard_set->size());
  shard_set->Await(target_shard, [&] {
    namespaces->GetDefaultNamespace().GetCurrentDbSlice().SetMvcc(0, std::string_view{"k"},
                                                                  kResidentStamp);
  });

  // Probe every proactor's own EngineShard (nullptr on a proactor with no shard, e.g. the third
  // thread with the fixture's default num_threads_=3/num_shards=2 -- see the comment on
  // RdbMvccMultiShardTest above) and pick one that does not own target_shard.
  int parsing_proactor = -1;
  for (size_t i = 0; i < pp_->size(); ++i) {
    std::optional<ShardId> owner = pp_->at(i)->Await([]() -> std::optional<ShardId> {
      EngineShard* es = EngineShard::tlocal();
      if (es == nullptr)
        return std::nullopt;
      return es->shard_id();
    });
    if (!owner.has_value() || *owner != target_shard) {
      parsing_proactor = static_cast<int>(i);
      break;
    }
  }
  ASSERT_GE(parsing_proactor, 0)
      << "need a proactor whose own shard (if any) differs from target_shard for this test to be "
         "meaningful";

  std::string body = BuildTombstoneSection(0, {{"k", kIncomingTombstone}});
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "after");
  AppendString(&body, "afterval");

  const std::string rdb = WrapInRdb(body);
  io::BytesSource src{io::Buffer(rdb)};
  RdbLoadContext load_context;
  auto ec = pp_->at(parsing_proactor)->Await([&]() -> std::error_code {
    RdbLoader loader(service_.get(), &load_context);
    loader.SetMergeLww(true, kPeerHash);
    return loader.Load(&src);
  });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_THAT(Run({"get", "k"}), kMatchNil)
      << "a winning peer tombstone must delete our live key even when parsed on another shard's "
         "thread";
  EXPECT_EQ(Run({"get", "after"}), "afterval") << "bytes after the section must still parse";

  std::optional<MvccStamp> got;
  size_t mismatches = 0;
  shard_set->Await(target_shard, [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    got = db_slice.GetMvcc(0, std::string_view{"k"});
    mismatches = db_slice.TEST_VerifyMvccTable(0);
  });
  ASSERT_TRUE(got.has_value()) << "the tombstone must have routed to its owning shard ("
                               << target_shard << ")";
  EXPECT_EQ(*got, kIncomingTombstone);
  EXPECT_TRUE(got->IsTombstone());
  EXPECT_EQ(mismatches, 0u) << "dense invariant must hold on the owning shard after the delete";
}

// drakeydb: P4-3 Task 4 -- merge-LWW on the full-sync load path. A peer-mode full sync must merge
// into this node's own dataset via a real MvccStamp compare (mvcc.h's MergeAccepts), not blindly
// overwrite a resident value the way every OTHER loader still does (the guard test at the bottom
// of this group proves that). This first case: the incoming snapshot's stamp is STALE relative to
// the resident value, so SetMergeLww(true, ...) must reject the incoming write outright -- both
// the resident VALUE and its STAMP must survive completely untouched.
TEST_F(RdbMvccTest, MergeLwwRejectsStaleIncomingLeavingResidentValueAndStampUntouched) {
  ASSERT_TRUE(IsActiveReplica());
  constexpr uint64_t kPeerHash = 0xFEDCBA9876543210ULL;
  constexpr uint64_t kSelfHash = 0x1122334455667788ULL;
  const MvccStamp kResidentStamp{0x2000, kSelfHash};
  const MvccStamp kIncomingStamp{0x1000, kPeerHash};  // Mvcc() 0x1000 < 0x2000: strictly older

  ASSERT_EQ(Run({"set", "k", "resident_v"}), "OK");
  shard_set->Await(0, [&] {
    namespaces->GetDefaultNamespace().GetCurrentDbSlice().SetMvcc(0, std::string_view{"k"},
                                                                  kResidentStamp);
  });

  std::string body;
  uint8_t block[17] = {RDB_OPCODE_DF_MVCC};
  absl::little_endian::Store64(block + 1, kIncomingStamp.packed);
  absl::little_endian::Store64(block + 9, kIncomingStamp.origin_hash);
  body.append(reinterpret_cast<const char*>(block), sizeof(block));
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "k");
  AppendString(&body, "incoming_v");

  const std::string rdb = WrapInRdb(body);
  io::BytesSource src{io::Buffer(rdb)};
  RdbLoadContext load_context;
  auto ec = pp_->at(0)->Await([&]() -> std::error_code {
    RdbLoader loader(service_.get(), &load_context);
    loader.SetMergeLww(true, kPeerHash);
    return loader.Load(&src);
  });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_EQ(Run({"get", "k"}), "resident_v")
      << "a stale incoming snapshot value must never overwrite a newer resident value";

  std::optional<MvccStamp> got;
  shard_set->Await(0, [&] {
    got = namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, std::string_view{"k"});
  });
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(*got, kResidentStamp)
      << "the resident stamp must also survive untouched -- MergeAccepts rejected the whole write "
         "before AddOrUpdate/SetMvcc ever ran";
}

// The reverse of the case above: the incoming snapshot's stamp is NEWER than the resident one, so
// SetMergeLww(true, ...) must accept it -- both the VALUE and the STAMP get installed from the
// snapshot, exactly like the pre-Task-4 unconditional path already did for every key.
TEST_F(RdbMvccTest, MergeLwwAcceptsNewerIncomingInstallingValueAndStamp) {
  ASSERT_TRUE(IsActiveReplica());
  constexpr uint64_t kPeerHash = 0xFEDCBA9876543210ULL;
  constexpr uint64_t kSelfHash = 0x1122334455667788ULL;
  const MvccStamp kResidentStamp{0x2000, kSelfHash};
  const MvccStamp kIncomingStamp{0x3000, kPeerHash};  // Mvcc() 0x3000 > 0x2000: strictly newer

  ASSERT_EQ(Run({"set", "k", "resident_v"}), "OK");
  shard_set->Await(0, [&] {
    namespaces->GetDefaultNamespace().GetCurrentDbSlice().SetMvcc(0, std::string_view{"k"},
                                                                  kResidentStamp);
  });

  std::string body;
  uint8_t block[17] = {RDB_OPCODE_DF_MVCC};
  absl::little_endian::Store64(block + 1, kIncomingStamp.packed);
  absl::little_endian::Store64(block + 9, kIncomingStamp.origin_hash);
  body.append(reinterpret_cast<const char*>(block), sizeof(block));
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "k");
  AppendString(&body, "incoming_v");

  const std::string rdb = WrapInRdb(body);
  io::BytesSource src{io::Buffer(rdb)};
  RdbLoadContext load_context;
  auto ec = pp_->at(0)->Await([&]() -> std::error_code {
    RdbLoader loader(service_.get(), &load_context);
    loader.SetMergeLww(true, kPeerHash);
    return loader.Load(&src);
  });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_EQ(Run({"get", "k"}), "incoming_v");

  std::optional<MvccStamp> got;
  shard_set->Await(0, [&] {
    got = namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, std::string_view{"k"});
  });
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(*got, kIncomingStamp);
}

// ---- P4-3 final fix wave (F-1): an ALREADY-EXPIRED incoming key on a merge load ----------------
//
// These four pin the adversarial review's refutation. Before the fix, an incoming key whose
// whole-key TTL had already elapsed was dropped by an early return that ran BEFORE the entire
// merge block -- no MergeAccepts compare, no delete of the resident value, no SetMvcc -- so a
// strictly OLDER resident value AND its stamp both survived a strictly NEWER peer write, and
// nothing ever repaired it (the sender's tombstone is not in the opcode-225 section, which is
// emitted from the snapshot PROLOGUE before the key expired, and the sender's expiry DEL is
// dropped from every peer link by PassesPeerEchoFilter's kEntryFlagExpired test). Live-reproduced
// at 5/40 keys permanently divergent.
//
// CONTROLLER RULING: on a merge load, an already-expired incoming key is the peer's DELETE of that
// key, applied through the same Task 6 tombstone path as an opcode-225 record.
//
// Both loader gates are covered, because which one fires depends on ServerState::is_master:
//   * is_master TRUE  -> RdbLoader::ShouldDiscardKey drops it on the PARSING fiber. This is the
//     production case: ServerFamily::ReplicaOfInternal short-circuits to ReplicaOfActive whenever
//     IsActiveReplica() (server_family.cc), so an active node NEVER calls
//     SetMasterFlagOnAllThreads(false) and stays is_master==true for the whole peer full sync.
//   * is_master FALSE -> ShouldDiscardKey passes and CreateObjectOnShard's own gate drops it.
//
// Each builds the exact wire bytes SaveEntry (rdb_save.cc) emits, in SaveEntry's own order:
// RDB_OPCODE_EXPIRETIME_MS, RDB_OPCODE_DF_MVCC, type byte, key, value.

namespace {

// EXPIRETIME_MS = 1000 (1970-01-01T00:00:01Z -- long elapsed under any real clock), then the
// stamp, then the string record. Mirrors RdbSerializer::SaveEntry's emit order exactly.
std::string BuildExpiredStringEntry(std::string_view key, std::string_view value,
                                    const MvccStamp& stamp, uint64_t expire_ms = 1000) {
  std::string body;
  uint8_t exp[9] = {RDB_OPCODE_EXPIRETIME_MS};
  absl::little_endian::Store64(exp + 1, expire_ms);
  body.append(reinterpret_cast<const char*>(exp), sizeof(exp));

  uint8_t block[17] = {RDB_OPCODE_DF_MVCC};
  absl::little_endian::Store64(block + 1, stamp.packed);
  absl::little_endian::Store64(block + 9, stamp.origin_hash);
  body.append(reinterpret_cast<const char*>(block), sizeof(block));

  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, key);
  AppendString(&body, value);
  return body;
}

}  // namespace

// The is_master==false route: ShouldDiscardKey passes the key through and CreateObjectOnShard's
// own already-expired gate is what must now treat it as the peer's delete.
TEST_F(RdbMvccTest, MergeLwwExpiredIncomingDeletesStaleResidentAsReplica) {
  ASSERT_TRUE(IsActiveReplica());
  constexpr uint64_t kPeerHash = 0xFEDCBA9876543210ULL;
  constexpr uint64_t kSelfHash = 0x1122334455667788ULL;
  const MvccStamp kResidentStamp{0x2000, kSelfHash};
  // Vastly newer than the resident stamp -- the peer's SET that replaced our value, then expired.
  const MvccStamp kIncomingStamp{0x0030000000000000ULL, kPeerHash};

  ASSERT_EQ(Run({"set", "k", "resident_v"}), "OK");
  shard_set->Await(0, [&] {
    namespaces->GetDefaultNamespace().GetCurrentDbSlice().SetMvcc(0, std::string_view{"k"},
                                                                  kResidentStamp);
  });

  std::string body = BuildExpiredStringEntry("k", "incoming_v", kIncomingStamp);
  const std::string rdb = WrapInRdb(body);
  io::BytesSource src{io::Buffer(rdb)};
  RdbLoadContext load_context;
  // The real peer-mode condition for this route. Restored before the test returns so no later
  // case in this binary inherits it.
  shard_set->pool()->AwaitBrief([](unsigned, auto*) { ServerState::tlocal()->is_master = false; });
  absl::Cleanup restore_master = [] {
    shard_set->pool()->AwaitBrief([](unsigned, auto*) { ServerState::tlocal()->is_master = true; });
  };
  auto ec = pp_->at(0)->Await([&]() -> std::error_code {
    RdbLoader loader(service_.get(), &load_context);
    loader.SetMergeLww(true, kPeerHash);
    return loader.Load(&src);
  });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_THAT(Run({"get", "k"}), kMatchNil)
      << "an already-expired incoming key is the peer's DELETE: our strictly older value must go";

  std::optional<MvccStamp> got;
  size_t mismatches = 0;
  shard_set->Await(0, [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    got = db_slice.GetMvcc(0, std::string_view{"k"});
    mismatches = db_slice.TEST_VerifyMvccTable(0);
  });
  ASSERT_TRUE(got.has_value()) << "the synthetic tombstone must be installed";
  EXPECT_EQ(*got, kIncomingStamp.AsTombstone())
      << "the tombstone carries the peer's OWN stamp with bit 63 set -- never our stale one, and "
         "never a locally minted one";
  EXPECT_TRUE(got->IsTombstone());
  EXPECT_EQ(mismatches, 0u) << "dense invariant must hold after the delete";
}

// The is_master==true route -- the production one for an active node (see the block comment
// above): ShouldDiscardKey must no longer discard under merge_lww_, so the item reaches the shard.
TEST_F(RdbMvccTest, MergeLwwExpiredIncomingDeletesStaleResidentAsMaster) {
  ASSERT_TRUE(IsActiveReplica());
  // ServerState is per-proactor; the gtest thread has none, so this must be read on a proactor.
  bool master_everywhere = true;
  shard_set->pool()->AwaitBrief([&](unsigned, auto*) {
    if (!ServerState::tlocal()->is_master)
      master_everywhere = false;
  });
  ASSERT_TRUE(master_everywhere) << "this fixture must start out as a master on every thread";
  constexpr uint64_t kPeerHash = 0xFEDCBA9876543210ULL;
  constexpr uint64_t kSelfHash = 0x1122334455667788ULL;
  const MvccStamp kResidentStamp{0x2000, kSelfHash};
  const MvccStamp kIncomingStamp{0x0030000000000000ULL, kPeerHash};

  ASSERT_EQ(Run({"set", "k", "resident_v"}), "OK");
  shard_set->Await(0, [&] {
    namespaces->GetDefaultNamespace().GetCurrentDbSlice().SetMvcc(0, std::string_view{"k"},
                                                                  kResidentStamp);
  });

  std::string body = BuildExpiredStringEntry("k", "incoming_v", kIncomingStamp);
  // Bytes after the expired entry must still parse: the item is no longer skipped during parsing,
  // so the reader's position handling changes on this route specifically.
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "after");
  AppendString(&body, "afterval");

  const std::string rdb = WrapInRdb(body);
  io::BytesSource src{io::Buffer(rdb)};
  RdbLoadContext load_context;
  auto ec = pp_->at(0)->Await([&]() -> std::error_code {
    RdbLoader loader(service_.get(), &load_context);
    loader.SetMergeLww(true, kPeerHash);
    return loader.Load(&src);
  });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_THAT(Run({"get", "k"}), kMatchNil)
      << "ShouldDiscardKey must not swallow the peer's delete on an active node";
  EXPECT_EQ(Run({"get", "after"}), "afterval") << "bytes after the expired entry must still parse";

  std::optional<MvccStamp> got;
  size_t mismatches = 0;
  shard_set->Await(0, [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    got = db_slice.GetMvcc(0, std::string_view{"k"});
    mismatches = db_slice.TEST_VerifyMvccTable(0);
  });
  ASSERT_TRUE(got.has_value()) << "the synthetic tombstone must be installed";
  EXPECT_EQ(*got, kIncomingStamp.AsTombstone());
  EXPECT_TRUE(got->IsTombstone());
  EXPECT_EQ(mismatches, 0u) << "dense invariant must hold after the delete";
}

// The guard, so the fix above is a DECISION and not a new unconditional delete: the same expired
// incoming key, but with a stamp STRICTLY OLDER than the resident one. The synthetic tombstone
// must lose the MergeAccepts compare and leave both the resident value and its stamp untouched.
TEST_F(RdbMvccTest, MergeLwwStaleExpiredIncomingLeavesResidentValueAndStampUntouched) {
  ASSERT_TRUE(IsActiveReplica());
  constexpr uint64_t kPeerHash = 0xFEDCBA9876543210ULL;
  constexpr uint64_t kSelfHash = 0x1122334455667788ULL;
  const MvccStamp kResidentStamp{0x2000, kSelfHash};
  const MvccStamp kIncomingStamp{0x1000, kPeerHash};  // strictly older than the resident

  ASSERT_EQ(Run({"set", "k", "resident_v"}), "OK");
  shard_set->Await(0, [&] {
    namespaces->GetDefaultNamespace().GetCurrentDbSlice().SetMvcc(0, std::string_view{"k"},
                                                                  kResidentStamp);
  });

  std::string body = BuildExpiredStringEntry("k", "incoming_v", kIncomingStamp);
  const std::string rdb = WrapInRdb(body);
  io::BytesSource src{io::Buffer(rdb)};
  RdbLoadContext load_context;
  auto ec = pp_->at(0)->Await([&]() -> std::error_code {
    RdbLoader loader(service_.get(), &load_context);
    loader.SetMergeLww(true, kPeerHash);
    return loader.Load(&src);
  });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_EQ(Run({"get", "k"}), "resident_v")
      << "a peer delete older than our own value must never win";

  std::optional<MvccStamp> got;
  size_t mismatches = 0;
  shard_set->Await(0, [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    got = db_slice.GetMvcc(0, std::string_view{"k"});
    mismatches = db_slice.TEST_VerifyMvccTable(0);
  });
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(*got, kResidentStamp) << "the resident stamp must survive untouched too";
  EXPECT_FALSE(got->IsTombstone());
  EXPECT_EQ(mismatches, 0u);
}

// The non-merge control, BOTH routes. Without SetMergeLww the pre-existing, upstream-verbatim
// behavior must be completely unchanged: an already-expired incoming key is silently dropped and
// whatever this node already held is left exactly as it was. This is what a plain Dragonfly
// replica's full sync, a local RDB file load and DEBUG LOAD all do, and it is the guarantee the
// F-1 fix must not disturb -- restoring the unconditional early return makes the three tests above
// fail while this one keeps passing, which is precisely the falsification split we want.
TEST_F(RdbMvccTest, WithoutMergeLwwExpiredIncomingIsDroppedLeavingResidentIntact) {
  ASSERT_TRUE(IsActiveReplica());
  constexpr uint64_t kPeerHash = 0xFEDCBA9876543210ULL;
  constexpr uint64_t kSelfHash = 0x1122334455667788ULL;
  const MvccStamp kResidentStamp{0x2000, kSelfHash};
  const MvccStamp kIncomingStamp{0x0030000000000000ULL, kPeerHash};  // "newer" -- irrelevant here

  ASSERT_EQ(Run({"set", "k", "resident_v"}), "OK");
  shard_set->Await(0, [&] {
    namespaces->GetDefaultNamespace().GetCurrentDbSlice().SetMvcc(0, std::string_view{"k"},
                                                                  kResidentStamp);
  });

  const std::string body = BuildExpiredStringEntry("k", "incoming_v", kIncomingStamp);

  auto check_intact = [&](const char* route) {
    EXPECT_EQ(Run({"get", "k"}), "resident_v")
        << route << ": a non-merge load must keep dropping an expired key, touching nothing";
    std::optional<MvccStamp> got;
    size_t mismatches = 0;
    shard_set->Await(0, [&] {
      auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
      got = db_slice.GetMvcc(0, std::string_view{"k"});
      mismatches = db_slice.TEST_VerifyMvccTable(0);
    });
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(*got, kResidentStamp) << route;
    EXPECT_FALSE(got->IsTombstone()) << route;
    EXPECT_EQ(mismatches, 0u) << route;
  };

  // Route 1: is_master true -- ShouldDiscardKey's gate.
  auto ec = pp_->at(0)->Await([&] { return LoadRdbData(service_.get(), WrapInRdb(body)); });
  ASSERT_FALSE(ec) << ec.message();
  check_intact("is_master=true (ShouldDiscardKey)");

  // Route 2: is_master false -- CreateObjectOnShard's gate.
  shard_set->pool()->AwaitBrief([](unsigned, auto*) { ServerState::tlocal()->is_master = false; });
  absl::Cleanup restore_master = [] {
    shard_set->pool()->AwaitBrief([](unsigned, auto*) { ServerState::tlocal()->is_master = true; });
  };
  ec = pp_->at(0)->Await([&] { return LoadRdbData(service_.get(), WrapInRdb(body)); });
  ASSERT_FALSE(ec) << ec.message();
  check_intact("is_master=false (CreateObjectOnShard)");
}

// Owner decision, 2026-08-30 (mvcc.h): ties are won by the STORED side. An incoming record whose
// stamp is byte-identical to the resident one must never churn the key -- the two carry different
// VALUES here specifically so this test can tell "the tie was resolved by MvccStamp equality"
// (operator< never returns true for equal stamps, mvcc.h) apart from "the value happened to match
// already".
TEST_F(RdbMvccTest, MergeLwwExactTieKeepsResidentValue) {
  ASSERT_TRUE(IsActiveReplica());
  constexpr uint64_t kPeerHash = 0xFEDCBA9876543210ULL;
  const MvccStamp kTiedStamp{0x2000, kPeerHash};

  ASSERT_EQ(Run({"set", "k", "resident_v"}), "OK");
  shard_set->Await(0, [&] {
    namespaces->GetDefaultNamespace().GetCurrentDbSlice().SetMvcc(0, std::string_view{"k"},
                                                                  kTiedStamp);
  });

  std::string body;
  uint8_t block[17] = {RDB_OPCODE_DF_MVCC};
  absl::little_endian::Store64(block + 1, kTiedStamp.packed);
  absl::little_endian::Store64(block + 9, kTiedStamp.origin_hash);
  body.append(reinterpret_cast<const char*>(block), sizeof(block));
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "k");
  AppendString(&body, "incoming_v_must_not_apply");

  const std::string rdb = WrapInRdb(body);
  io::BytesSource src{io::Buffer(rdb)};
  RdbLoadContext load_context;
  auto ec = pp_->at(0)->Await([&]() -> std::error_code {
    RdbLoader loader(service_.get(), &load_context);
    loader.SetMergeLww(true, kPeerHash);
    return loader.Load(&src);
  });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_EQ(Run({"get", "k"}), "resident_v") << "an exact stamp tie must favor the STORED side";

  std::optional<MvccStamp> got;
  shard_set->Await(0, [&] {
    got = namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, std::string_view{"k"});
  });
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(*got, kTiedStamp);
}

// The guard: the exact same stale-incoming-vs-newer-resident snapshot as
// MergeLwwRejectsStaleIncomingLeavingResidentValueAndStampUntouched above, but loaded WITHOUT ever
// calling SetMergeLww -- proving the merge compare is opt-in, not a global behavior change. This
// is what a plain Dragonfly replica's full sync and DEBUG LOAD/restore still do (neither call site
// SetOverrideExistingKeys sits at is touched by this task): last-loaded-wins, exactly as before.
TEST_F(RdbMvccTest, WithoutMergeLwwStaleSnapshotStillOverwrites) {
  ASSERT_TRUE(IsActiveReplica());
  constexpr uint64_t kPeerHash = 0xFEDCBA9876543210ULL;
  constexpr uint64_t kSelfHash = 0x1122334455667788ULL;
  const MvccStamp kResidentStamp{0x2000, kSelfHash};
  const MvccStamp kIncomingStamp{0x1000, kPeerHash};  // "stale" only matters under merge-LWW

  ASSERT_EQ(Run({"set", "k", "resident_v"}), "OK");
  shard_set->Await(0, [&] {
    namespaces->GetDefaultNamespace().GetCurrentDbSlice().SetMvcc(0, std::string_view{"k"},
                                                                  kResidentStamp);
  });

  std::string body;
  uint8_t block[17] = {RDB_OPCODE_DF_MVCC};
  absl::little_endian::Store64(block + 1, kIncomingStamp.packed);
  absl::little_endian::Store64(block + 9, kIncomingStamp.origin_hash);
  body.append(reinterpret_cast<const char*>(block), sizeof(block));
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "k");
  AppendString(&body, "incoming_v");

  // No SetMergeLww call anywhere below: merge_lww_ stays at its default false.
  auto ec = pp_->at(0)->Await([&] { return LoadRdbData(service_.get(), WrapInRdb(body)); });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_EQ(Run({"get", "k"}), "incoming_v")
      << "without SetMergeLww, the loader must keep overwriting verbatim (plain replicas, DEBUG "
         "LOAD/restore)";

  std::optional<MvccStamp> got;
  shard_set->Await(0, [&] {
    got = namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, std::string_view{"k"});
  });
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(*got, kIncomingStamp);
}

// The resurrection guard. A resident TOMBSTONE (a delete this node already committed -- Task
// 2/11) is NEWER than the incoming snapshot's value, so MergeAccepts -- which compares via
// operator< (masks bit 63, mvcc.h) -- must reject the incoming write the same way it would reject
// a stale live value: the deleted key must not resurrect, and the tombstone itself (both its
// IsTombstone() flag and its exact stamp) must survive untouched. Installs the tombstone directly
// via DbSlice::SetTombstone rather than an actual DEL, so the test controls its stamp precisely
// without depending on Task 2/11's own delete-path plumbing.
TEST_F(RdbMvccTest, MergeLwwTombstoneNewerThanIncomingGuardsAgainstResurrection) {
  ASSERT_TRUE(IsActiveReplica());
  constexpr uint64_t kPeerHash = 0xFEDCBA9876543210ULL;
  constexpr uint64_t kSelfHash = 0x1122334455667788ULL;
  const MvccStamp kTombstoneStamp = MvccStamp{0x5000, kSelfHash}.AsTombstone();
  const MvccStamp kIncomingStamp{0x1000, kPeerHash};  // older than the tombstone's own Mvcc()

  shard_set->Await(0, [&] {
    namespaces->GetDefaultNamespace().GetCurrentDbSlice().SetTombstone(0, std::string_view{"k"},
                                                                       kTombstoneStamp);
  });

  std::string body;
  uint8_t block[17] = {RDB_OPCODE_DF_MVCC};
  absl::little_endian::Store64(block + 1, kIncomingStamp.packed);
  absl::little_endian::Store64(block + 9, kIncomingStamp.origin_hash);
  body.append(reinterpret_cast<const char*>(block), sizeof(block));
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "k");
  AppendString(&body, "incoming_v");

  const std::string rdb = WrapInRdb(body);
  io::BytesSource src{io::Buffer(rdb)};
  RdbLoadContext load_context;
  auto ec = pp_->at(0)->Await([&]() -> std::error_code {
    RdbLoader loader(service_.get(), &load_context);
    loader.SetMergeLww(true, kPeerHash);
    return loader.Load(&src);
  });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_THAT(Run({"get", "k"}), kMatchNil) << "a stale incoming value must not resurrect a key "
                                               "this node already tombstoned";

  std::optional<MvccStamp> got;
  size_t mismatches = 0;
  shard_set->Await(0, [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    got = db_slice.GetMvcc(0, std::string_view{"k"});
    mismatches = db_slice.TEST_VerifyMvccTable(0);
  });
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(*got, kTombstoneStamp) << "the tombstone must survive exactly as installed";
  EXPECT_TRUE(got->IsTombstone());
  EXPECT_EQ(mismatches, 0u) << "dense invariant must hold: the rejected write touched nothing";
}

// The other direction: a resident TOMBSTONE OLDER than the incoming value. MergeAccepts must
// accept the incoming write -- the key resurrects with the snapshot's value -- and
// DbSlice::SetMvcc's own bookkeeping (the live-key setter Task 4 calls unconditionally on accept)
// must clear the tombstone flag and release its mvcc_tombstones credit as a side effect, with no
// extra code needed in CreateObjectOnShard for this: SetMvcc masks bit 63 on every write and
// decrements mvcc_tombstones whenever it overwrites a slot that was one (db_slice.cc, verified by
// reading SetMvcc, not assumed).
TEST_F(RdbMvccTest, MergeLwwOlderTombstoneLosesToIncomingAndClearsCleanly) {
  ASSERT_TRUE(IsActiveReplica());
  constexpr uint64_t kPeerHash = 0xFEDCBA9876543210ULL;
  constexpr uint64_t kSelfHash = 0x1122334455667788ULL;
  const MvccStamp kTombstoneStamp = MvccStamp{0x1000, kSelfHash}.AsTombstone();
  const MvccStamp kIncomingStamp{0x5000, kPeerHash};  // newer than the tombstone's own Mvcc()

  size_t tombstones_before = 0;
  shard_set->Await(0, [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    db_slice.SetTombstone(0, std::string_view{"k"}, kTombstoneStamp);
    tombstones_before = db_slice.MutableStats(0)->mvcc_tombstones;
  });
  ASSERT_GT(tombstones_before, 0u);

  std::string body;
  uint8_t block[17] = {RDB_OPCODE_DF_MVCC};
  absl::little_endian::Store64(block + 1, kIncomingStamp.packed);
  absl::little_endian::Store64(block + 9, kIncomingStamp.origin_hash);
  body.append(reinterpret_cast<const char*>(block), sizeof(block));
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "k");
  AppendString(&body, "incoming_v");

  const std::string rdb = WrapInRdb(body);
  io::BytesSource src{io::Buffer(rdb)};
  RdbLoadContext load_context;
  auto ec = pp_->at(0)->Await([&]() -> std::error_code {
    RdbLoader loader(service_.get(), &load_context);
    loader.SetMergeLww(true, kPeerHash);
    return loader.Load(&src);
  });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_EQ(Run({"get", "k"}), "incoming_v")
      << "a newer incoming value must resurrect a key whose tombstone is older than it";

  std::optional<MvccStamp> got;
  size_t tombstones_after = 0, mismatches = 0;
  shard_set->Await(0, [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    got = db_slice.GetMvcc(0, std::string_view{"k"});
    tombstones_after = db_slice.MutableStats(0)->mvcc_tombstones;
    mismatches = db_slice.TEST_VerifyMvccTable(0);
  });
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(*got, kIncomingStamp) << "the installed stamp must be the incoming one, tombstone bit "
                                     "cleared by SetMvcc's own masking";
  EXPECT_FALSE(got->IsTombstone());
  EXPECT_EQ(tombstones_after, tombstones_before - 1)
      << "SetMvcc must release the tombstone's mvcc_tombstones credit when it overwrites the slot";
  EXPECT_EQ(mismatches, 0u) << "dense invariant must hold after the tombstone clears";
}

// drakeydb: P4-3 Task 4, Hazard 1 fix -- the race itself, forced deterministically rather than
// hoped for. AddOrFindInternal's PreUpdateBlocking (db_slice.cc) synchronously calls every
// registered DbSlice::ChangeConsumerInterface -- the exact mechanism SerializerBase::OnChange uses
// to block a concurrent mutator while a BGSAVE/full-sync SliceSnapshot is serializing a bucket
// (Hazard 1, task-4-report.md). A plain ChangeConsumerInterface that blocks on its FIRST call and
// passes every later call straight through reproduces that same contention without needing a real
// SliceSnapshot, large values, or SerializerBase::stream_mu_: the loader's own AddOrFind call is
// call #1 (it blocks here, mid-PreUpdateBlocking, strictly before any mutation); a plain
// concurrent SET's own FindMutable->PreUpdateBlocking is call #2 (passes straight through,
// completing normally). No sleeps, no timing assumptions: WaitEntered()/Release() are a
// util::fb2::Done pair, so the interleaving is exact every run.
//
// The loader runs on its own joinable fiber (LaunchFiber, not Await) specifically so this test's
// own (calling) fiber is free to run the concurrent SET and call Release() while the loader fiber
// is genuinely parked inside OnChange -- Await would block the calling fiber for the loader's
// entire Load() call, leaving no fiber free to drive the "concurrent" side at all.
TEST_F(RdbMvccTest, MergeLwwRaceConcurrentWriteDuringYieldBeatsStaleSnapshot) {
  ASSERT_TRUE(IsActiveReplica());
  constexpr uint64_t kPeerHash = 0xFEDCBA9876543210ULL;
  constexpr uint64_t kSelfHash = 0x1122334455667788ULL;
  const MvccStamp kResidentStamp{0x2000, kSelfHash};
  // Beats kResidentStamp at the fast-path pre-check, BEFORE AddOrFind's yield -- the whole point
  // is that this stamp must still lose once a fresher write lands during that yield.
  const MvccStamp kIncomingStamp{0x3000, kPeerHash};

  ASSERT_EQ(Run({"set", "k", "resident_v"}), "OK");
  shard_set->Await(0, [&] {
    namespaces->GetDefaultNamespace().GetCurrentDbSlice().SetMvcc(0, std::string_view{"k"},
                                                                  kResidentStamp);
  });

  std::string body;
  uint8_t block[17] = {RDB_OPCODE_DF_MVCC};
  absl::little_endian::Store64(block + 1, kIncomingStamp.packed);
  absl::little_endian::Store64(block + 9, kIncomingStamp.origin_hash);
  body.append(reinterpret_cast<const char*>(block), sizeof(block));
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "k");
  AppendString(&body, "stale_incoming_v");
  const std::string rdb = WrapInRdb(body);

  class BlockFirstCallChangeConsumer final : public DbSlice::ChangeConsumerInterface {
   public:
    void OnChange(DbIndex, const ChangeReq&) override {
      if (calls_++ == 0) {
        entered_.Notify();
        release_.Wait();
      }
    }
    void WaitEntered() {
      entered_.Wait();
    }
    void Release() {
      release_.Notify();
    }

   private:
    int calls_ = 0;
    util::fb2::Done entered_;
    util::fb2::Done release_;
  } consumer;

  io::BytesSource src{io::Buffer(rdb)};
  RdbLoadContext load_context;
  std::error_code load_ec;

  util::fb2::Fiber loader_fiber = pp_->at(0)->LaunchFiber([&] {
    EngineShard* shard = EngineShard::tlocal();
    DbSlice& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    // RegisterOnChange DCHECKs the shard's intent lock is held (see #7153); the lock is released
    // again immediately after -- production only ever needs it held for the registering call
    // itself (e.g. a DFLY SYNC command's own transaction), never for as long as the consumer stays
    // registered.
    shard->shard_lock()->Acquire(IntentLock::EXCLUSIVE);
    db_slice.RegisterOnChange(&consumer);
    shard->shard_lock()->Release(IntentLock::EXCLUSIVE);

    RdbLoader loader(service_.get(), &load_context);
    loader.SetMergeLww(true, kPeerHash);
    load_ec = loader.Load(&src);

    db_slice.UnregisterOnChange(&consumer);
  });

  // Blocks THIS (the test's own) fiber until the loader fiber is genuinely parked inside
  // OnChange's call #1 -- i.e. mid-PreUpdateBlocking, strictly before AddOrFind has mutated
  // anything.
  consumer.WaitEntered();

  // The concurrent write: a plain SET on the very same key, from a completely independent path,
  // mints and commits a real, fresh HopStamp -- standing in for "another peer's stable-sync apply
  // landing on this shard thread during LOADING" per main_service.cc's is_replicating admission
  // rule that motivates Hazard 1 in the first place. Its own FindMutable->PreUpdateBlocking call
  // is OnChange's call #2 above, which passes straight through, so this completes normally.
  ASSERT_EQ(Run({"set", "k", "concurrent_v"}), "OK");

  consumer.Release();
  loader_fiber.Join();

  ASSERT_FALSE(load_ec) << load_ec.message();

  EXPECT_EQ(Run({"get", "k"}), "concurrent_v")
      << "the write that landed during AddOrFind's own yield must win, even though the loader's "
         "incoming snapshot stamp passed the earlier, non-authoritative fast-path pre-check";

  std::optional<MvccStamp> got;
  shard_set->Await(0, [&] {
    got = namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, std::string_view{"k"});
  });
  ASSERT_TRUE(got.has_value());
  EXPECT_NE(*got, kIncomingStamp)
      << "the stale incoming stamp must not have been installed over the concurrent write's own, "
         "freshly-minted stamp";
}

// drakeydb: P4-3 Task 4, Hazard 1 fix -- the `is_new && reject` rollback path, driven directly
// against the real DbSlice API rather than through a second live race (the only way to reach it is
// itself a race -- see RollbackFreshInsert's own doc comment, db_slice.h -- so this test
// constructs the POST-YIELD state by hand: it calls the exact same sequence CreateObjectOnShard
// runs, in the exact same order, just without an actual concurrent fiber in between). Proves
// RollbackFreshInsert leaves no prime entry and an intact dense invariant.
TEST_F(RdbMvccTest, MergeLwwRejectedFreshInsertRollsBackCleanly) {
  ASSERT_TRUE(IsActiveReplica());
  constexpr uint64_t kPeerHash = 0xFEDCBA9876543210ULL;
  constexpr uint64_t kSelfHash = 0x1122334455667788ULL;
  const MvccStamp kIncomingStamp{0x1000, kPeerHash};
  // Newer than kIncomingStamp -- simulates a concurrent apply tombstoning this exact, previously
  // absent key during AddOrFind's own yield, the only way a FRESH insert can be rejected.
  const MvccStamp kTombstoneStamp = MvccStamp{0x5000, kSelfHash}.AsTombstone();

  shard_set->Await(0, [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    ASSERT_FALSE(db_slice.GetMvcc(0, std::string_view{"k"}).has_value())
        << "key must start absent for this to exercise the insert branch";

    DbContext db_cntx{&namespaces->GetDefaultNamespace(), 0, 0};
    // The exact call CreateObjectOnShard makes (rdb_load.cc): AddOrFind, not AddOrUpdate.
    auto op_res = db_slice.AddOrFind(db_cntx, std::string_view{"k"}, std::nullopt);
    ASSERT_TRUE(op_res.ok());
    DbSlice::ItAndUpdater& updater = *op_res;
    ASSERT_TRUE(updater.is_new);
    ASSERT_EQ(updater.it->second.MallocUsed(), 0u);

    // Stand-in for "a concurrent apply tombstoned this key during the yield above".
    db_slice.SetTombstone(0, std::string_view{"k"}, kTombstoneStamp);

    // The exact decision CreateObjectOnShard runs after AddOrFind returns.
    ASSERT_FALSE(MergeAccepts(db_slice.GetMvcc(0, std::string_view{"k"}), kIncomingStamp));
    // drakeydb: fix round 1 (I1) -- RollbackFreshInsert now takes the whole ItAndUpdater and
    // cancels the AutoUpdater itself; the caller no longer calls post_updater.Cancel() separately.
    db_slice.RollbackFreshInsert(0, updater);
  });

  EXPECT_THAT(Run({"get", "k"}), kMatchNil)
      << "the rolled-back insert must leave no prime entry behind";

  std::optional<MvccStamp> got;
  size_t mismatches = 0;
  shard_set->Await(0, [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    got = db_slice.GetMvcc(0, std::string_view{"k"});
    mismatches = db_slice.TEST_VerifyMvccTable(0);
  });
  ASSERT_TRUE(got.has_value()) << "the tombstone itself must be untouched by the rollback";
  EXPECT_EQ(*got, kTombstoneStamp);
  EXPECT_EQ(mismatches, 0u)
      << "dense invariant must hold: RollbackFreshInsert's own DCHECK plus this O(size) check";
}

// drakeydb: fix round 1 (I1) -- RollbackFreshInsert's own CHECK(it_updater.is_new) is what stands
// between a correct rollback and silently erasing a live key in a release build (see its doc
// comment, db_slice.h/.cc): MallocUsed() == 0 alone cannot tell a fresh, still-empty insert apart
// from a resident inline/INT-encoded value -- `SET k 5` also reports 0. Proves the guard actually
// fires on exactly that non-fresh case. CHECK, unlike DCHECK, is never compiled out under NDEBUG,
// so this death test needs no #ifndef NDEBUG guard. "threadsafe" death-test style sidesteps the
// classic fork()-in-a-multithreaded-process hazard this fixture's own proactor/shard thread would
// otherwise create for the default fork-only style -- the same workaround helio uses for its one
// surviving fiber-context death test (helio/util/fibers/fibers_test.cc,
// PersistentWaiterStarvationDCheck); two earlier attempts there without it were disabled outright
// (`#if 0`) for exactly this class of flakiness.
//
// Falsification (fix round 1): reverted the guard from CHECK to DCHECK, rebuilt in release
// (-DNDEBUG), and confirmed this test failed to observe a crash -- RollbackFreshInsert instead
// silently erased the live key, exactly I1's failure scenario. Restored to CHECK; verbatim output
// recorded in task-4-report.md.
TEST_F(RdbMvccTest, RollbackFreshInsertRefusesNonFreshEntry) {
  ASSERT_TRUE(IsActiveReplica());
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  // Resident, inline/INT-encoded -- MallocUsed() == 0, same as AddOrFindInternal's fresh, still-
  // empty PrimeValue{}, which is exactly the confusion this guard exists to resolve.
  ASSERT_EQ(Run({"set", "k", "5"}), "OK");

  EXPECT_DEATH(
      {
        shard_set->Await(0, [&] {
          auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
          DbContext db_cntx{&namespaces->GetDefaultNamespace(), 0, 0};
          auto op_res = db_slice.AddOrFind(db_cntx, std::string_view{"k"}, std::nullopt);
          CHECK(op_res.ok());
          DbSlice::ItAndUpdater& updater = *op_res;
          CHECK(!updater.is_new) << "k is resident: AddOrFind must find it, not insert";
          db_slice.RollbackFreshInsert(0, updater);
        });
      },
      "Check failed: it_updater.is_new");
}

// drakeydb: P4-2 Task 2, review round 1 (Important, finding 1) -- covers D-7's "parses, discards"
// compatibility row, which neither test above exercises: both use the ACTIVE RdbMvccTest
// fixture, and the pytest plain-replica gate
// (test_plain_replica_of_active_node_gets_full_unfiltered_stream) attaches before any writes, so
// its own full-sync stream never carries a live 0xDD record either. A loader bug that skips the
// opcode's 16 bytes WITHOUT consuming them (e.g. an errantly added `if (IsActiveReplica())` guard
// around the fetch, not just around the eventual SetMvcc apply) would desync the byte stream --
// the type byte of the NEXT record would be misread as the low byte of the abandoned stamp -- and
// every real non-active consumer of an active node's snapshot (a plain replica's full sync, or
// this exact scenario relayed through a peer mesh) would silently corrupt or fail to load
// everything after the first stamped key. Nothing in the suite before this test could have caught
// that: it is the first test in the file to put a live RDB_OPCODE_DF_MVCC record in front of an
// INACTIVE loader with more data after it.
//
// Plain RdbTest (inactive is upstream's default, matching NoMvccOpcodeOrAuxWhenInactive above) --
// k2 immediately follows k1's record with no opcode of its own, so it can only parse correctly if
// the loader consumed exactly 16 bytes for k1's record, no more, no less.
TEST_F(RdbTest, LoadConsumesAndDiscardsMvccRecordWhenInactive) {
  ASSERT_FALSE(IsActiveReplica());

  const MvccStamp kStamp{0x0123456789ABCDEFULL, 0xFEDCBA9876543210ULL};
  std::string body;
  uint8_t block[17] = {0xDD};
  absl::little_endian::Store64(block + 1, kStamp.packed);
  absl::little_endian::Store64(block + 9, kStamp.origin_hash);
  body.append(reinterpret_cast<const char*>(block), sizeof(block));
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "k1");
  AppendString(&body, "v1");
  // No opcode before k2: if the record above were not fully (and only) consumed, the loader
  // would desync right here.
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "k2");
  AppendString(&body, "v2");

  auto ec = pp_->at(0)->Await([&] { return LoadRdbData(service_.get(), WrapInRdb(body)); });
  ASSERT_FALSE(ec) << ec.message();

  // "Parses": both keys loaded with their correct values -- proof the record's 16 bytes were
  // fully consumed rather than skipped or partially consumed.
  EXPECT_EQ(Run({"get", "k1"}), "v1");
  EXPECT_EQ(Run({"get", "k2"}), "v2");

  // "Discards": an inactive node never allocates the mvcc side table (DbTable::DbTable,
  // table.cc), so GetMvcc must return nullopt for k1 -- not the stamp the record carried, and
  // not {0,0} either.
  std::optional<MvccStamp> got;
  shard_set->Await(Shard("k1", shard_set->size()), [&] {
    got = namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, std::string_view{"k1"});
  });
  EXPECT_FALSE(got.has_value())
      << "an inactive node must never install a stamp -- GetMvcc must return nullopt, proving "
         "SetMvcc's own `if (!db.mvcc) return;` guard (db_slice.cc) discarded it";
}

// drakeydb: P4-2 Task 2, review round 1 (Important, finding 2) -- rdb_save.cc's SaveAux writes
// the "drakeydb-mvcc" breadcrumb unconditionally on every active-mode save (SaveAuxFieldStrStr,
// gated on IsActiveReplica()); before HandleAux recognized it, every single active-mode load --
// including this fixture's own SAVE-then-LOAD via `debug reload` -- logged "Unrecognized RDB AUX
// field: 'drakeydb-mvcc'" once per shard file, inverting the warning's purpose (it exists to flag
// a FOREIGN binary's genuinely unknown aux fields, not drakeydb's own recognized one).
//
// Real glog/absl-log capture (ScopedLogCapture above), not a code-inspection stand-in: this repo
// already has a working, in-tree precedent for exactly this (journal_test.cc's
// PassesPeerEchoFilterTest.DropsForeignOriginExpiryDelAndOriginOpcodeOnly, which asserts a
// rate-limited log line's exact count the same way), so there was no reason to fall back to a
// weaker check here.
TEST_F(RdbMvccTest, ActiveReloadDoesNotWarnOnRecognizedMvccAux) {
  ASSERT_TRUE(IsActiveReplica());
  ScopedLogCapture log_capture;

  ASSERT_EQ(Run({"set", "k", "v"}), "OK");
  ASSERT_EQ(Run({"debug", "reload"}), "OK");

  for (const auto& log : log_capture.logs) {
    EXPECT_EQ(log.find("Unrecognized RDB AUX field: 'drakeydb-mvcc'"), std::string::npos)
        << "the recognized drakeydb-mvcc breadcrumb must not trigger the foreign-aux warning: "
        << log;
  }
}

// drakeydb: P4-2 Task 3 -- proves RdbLoader::HandleAux recognizes KeyDB's "mvcc-tstamp" aux
// (fActiveReplica's rdbSaveAuxFieldStrStr, KeyDB/src/rdb.cpp:1164-1168 -- KeyDB source, not
// guessed) and installs it as a stamp with the SAME packed layout
// LoadInstallsThePersistedStampVerbatim above already proved for our own RDB_OPCODE_DF_MVCC
// opcode, but with origin_hash coming from SetLoadOriginHash (the link) instead of the file:
// unlike our own opcode, KeyDB's aux is a bare decimal counter with no author identity of its own
// (D-7). k2 has no preceding aux at all -- not even an unrelated one -- which is the one-shot
// semantics the brief calls out: ObjSettings::Reset() (rdb_load.cc, called after every
// LoadKeyValPair) means k1's aux can never leak onto k2, exactly like has_mc_flags's existing
// one-shot contract for the DF_MASK opcode.
TEST_F(RdbMvccTest, LoadsKeyDbMvccTstampAuxWithLinkOriginHash) {
  ASSERT_TRUE(IsActiveReplica());

  const MvccStamp kStamp{0x0123456789ABCDEFULL, 0xFEDCBA9876543210ULL};

  std::string body;
  body.push_back(static_cast<char>(RDB_OPCODE_AUX));
  AppendString(&body, "mvcc-tstamp");
  AppendString(&body, "81985529216486895");  // decimal(0x0123456789ABCDEF), KeyDB's %PRIu64
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "k1");
  AppendString(&body, "v1");
  // No aux at all precedes k2 -- proves one-shot semantics, not merely that the branch parses.
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "k2");
  AppendString(&body, "v2");

  const std::string rdb = WrapInRdb(body);
  io::BytesSource src{io::Buffer(rdb)};
  RdbLoadContext load_context;
  auto ec = pp_->at(0)->Await([&]() -> std::error_code {
    RdbLoader loader(service_.get(), &load_context);
    loader.SetLoadOriginHash(kStamp.origin_hash);
    return loader.Load(&src);
  });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_EQ(Run({"get", "k1"}), "v1");
  EXPECT_EQ(Run({"get", "k2"}), "v2");

  std::optional<MvccStamp> got1, got2;
  shard_set->Await(0, [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    got1 = db_slice.GetMvcc(0, std::string_view{"k1"});
    got2 = db_slice.GetMvcc(0, std::string_view{"k2"});
  });
  ASSERT_TRUE(got1.has_value());
  EXPECT_EQ(*got1, kStamp)
      << "k1's stamp must combine the aux's raw counter with the link's origin hash";

  ASSERT_TRUE(got2.has_value()) << "an active loader must still leave a dense {0,0} slot";
  EXPECT_TRUE(got2->Empty())
      << "k2 has no preceding aux -- one-shot semantics must not leak k1's stamp onto it";
}

// A local KeyDB RDB file carries a logical timestamp but no authenticated author identity. The
// loader must not turn that into durable {mvcc,0} authority, which would later be re-emitted as a
// real DF_MVCC record. Live KeyDB replication sets a non-zero link origin and is covered above.
TEST_F(RdbMvccTest, LocalKeyDbMvccTstampWithoutOriginLoadsUnstamped) {
  ASSERT_TRUE(IsActiveReplica());
  ScopedLogCapture log_capture;

  std::string body;
  body.push_back(static_cast<char>(RDB_OPCODE_AUX));
  AppendString(&body, "mvcc-tstamp");
  AppendString(&body, "81985529216486895");
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "k1");
  AppendString(&body, "v1");

  const std::string rdb = WrapInRdb(body);
  io::BytesSource src{io::Buffer(rdb)};
  RdbLoadContext load_context;
  auto ec = pp_->at(0)->Await([&]() -> std::error_code {
    RdbLoader loader(service_.get(), &load_context);  // no SetLoadOriginHash: local file load
    return loader.Load(&src);
  });
  ASSERT_FALSE(ec) << ec.message();

  std::optional<MvccStamp> got;
  shard_set->Await(0, [&] {
    got = namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, std::string_view{"k1"});
  });
  ASSERT_TRUE(got.has_value()) << "an active loader must still leave a dense slot";
  EXPECT_TRUE(got->Empty())
      << "a local KeyDB import has no authenticated author and must remain unstamped";

  bool warned = false;
  for (const auto& log : log_capture.logs) {
    warned |= log.find("without an authenticated sender origin") != std::string::npos;
  }
  EXPECT_TRUE(warned)
      << "an active local import must explain why its otherwise-valid timestamp was ignored";
}

// drakeydb: P4-2 Task 3, review round 1 (Important, finding 1) -- KeyDB stamps a key it has no
// valid mvcc for (e.g. one synced in from a plain-Redis master) with OBJ_MVCC_INVALID,
// 0xFFFFFFFFFFFFFFFF (KeyDB/src/server.h:958), not a real timestamp. Installed verbatim, that
// value sets drakeydb's tombstone bit (bit 63, mvcc.h) and yields the maximum possible mvcc -- a
// phantom tombstone that would win every LWW merge forever. Controller ruling overrode the
// brief's original verbatim-install snippet for this one case: any parsed value with bit 63 set
// is unrepresentable as a genuine KeyDB timestamp (their ms << 20 layout does not reach bit 63
// until roughly the year 280000 AD) and must be treated as unstamped. k_invalid proves the
// sentinel is rejected (loads {0,0}, not a tombstone); k_valid -- a second key in the SAME
// stream, given an ordinary high-but-representable stamp -- proves the bit-63 guard does not
// overreach and reject legitimate stamps in general.
TEST_F(RdbMvccTest, TreatsKeyDbObjMvccInvalidSentinelAsUnstamped) {
  ASSERT_TRUE(IsActiveReplica());
  ScopedLogCapture log_capture;

  const MvccStamp kStamp{0x0123456789ABCDEFULL, 0xFEDCBA9876543210ULL};

  std::string body;
  body.push_back(static_cast<char>(RDB_OPCODE_AUX));
  AppendString(&body, "mvcc-tstamp");
  AppendString(&body, "18446744073709551615");  // decimal(0xFFFFFFFFFFFFFFFF) == OBJ_MVCC_INVALID
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "k_invalid");
  AppendString(&body, "v1");
  body.push_back(static_cast<char>(RDB_OPCODE_AUX));
  AppendString(&body, "mvcc-tstamp");
  AppendString(&body, "81985529216486895");  // decimal(0x0123456789ABCDEF), an ordinary stamp
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "k_valid");
  AppendString(&body, "v2");

  const std::string rdb = WrapInRdb(body);
  io::BytesSource src{io::Buffer(rdb)};
  RdbLoadContext load_context;
  auto ec = pp_->at(0)->Await([&]() -> std::error_code {
    RdbLoader loader(service_.get(), &load_context);
    loader.SetLoadOriginHash(kStamp.origin_hash);
    return loader.Load(&src);
  });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_EQ(Run({"get", "k_invalid"}), "v1");
  EXPECT_EQ(Run({"get", "k_valid"}), "v2");

  std::optional<MvccStamp> got_invalid, got_valid;
  shard_set->Await(0, [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    got_invalid = db_slice.GetMvcc(0, std::string_view{"k_invalid"});
    got_valid = db_slice.GetMvcc(0, std::string_view{"k_valid"});
  });
  ASSERT_TRUE(got_invalid.has_value());
  EXPECT_TRUE(got_invalid->Empty())
      << "KeyDB's OBJ_MVCC_INVALID sentinel (all bits set) must never install as a stamp -- "
         "verbatim it would set drakeydb's tombstone bit and become a phantom tombstone that "
         "wins every LWW merge forever";

  ASSERT_TRUE(got_valid.has_value());
  EXPECT_EQ(*got_valid, kStamp) << "an ordinary valid stamp in the same stream must still install";

  bool warned = false;
  for (const auto& log : log_capture.logs) {
    if (log.find("18446744073709551615") != std::string::npos) {
      warned = true;
    }
  }
  EXPECT_TRUE(warned) << "the OBJ_MVCC_INVALID sentinel must be warned about, naming the value";
}

// drakeydb: P4-2, final review (Minor) -- a KeyDB "mvcc-tstamp" aux whose value is "0" parses
// cleanly (SimpleAtoi ok, bit 63 clear), so before the fix it installed {packed=0,
// origin_hash=load_origin_hash_} with has_mvcc=true. MvccStamp::Empty() requires BOTH fields zero
// (mvcc.h), so that is a NON-empty stamp minted from an aux that carried no authority whatsoever
// -- and, being non-empty, rdb_save.cc's `!mvcc.Empty()` gate re-emits it as a real
// RDB_OPCODE_DF_MVCC record on every subsequent save, laundering "unknown" into "authored by the
// link we happened to load from". Same never-fabricate-authority principle as the bit-63 sentinel
// guard above, at the other end of the range. k_zero must land unstamped; k_one -- the smallest
// possible NON-zero value, in the same stream -- proves the guard tests for zero exactly and does
// not overreach into small legitimate stamps.
TEST_F(RdbMvccTest, TreatsZeroKeyDbMvccTstampAuxAsUnstamped) {
  ASSERT_TRUE(IsActiveReplica());

  constexpr uint64_t kLinkOrigin = 0xFEDCBA9876543210ULL;

  std::string body;
  body.push_back(static_cast<char>(RDB_OPCODE_AUX));
  AppendString(&body, "mvcc-tstamp");
  AppendString(&body, "0");
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "k_zero");
  AppendString(&body, "v1");
  body.push_back(static_cast<char>(RDB_OPCODE_AUX));
  AppendString(&body, "mvcc-tstamp");
  AppendString(&body, "1");
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "k_one");
  AppendString(&body, "v2");

  const std::string rdb = WrapInRdb(body);
  io::BytesSource src{io::Buffer(rdb)};
  RdbLoadContext load_context;
  auto ec = pp_->at(0)->Await([&]() -> std::error_code {
    RdbLoader loader(service_.get(), &load_context);
    loader.SetLoadOriginHash(kLinkOrigin);
    return loader.Load(&src);
  });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_EQ(Run({"get", "k_zero"}), "v1");
  EXPECT_EQ(Run({"get", "k_one"}), "v2");

  std::optional<MvccStamp> got_zero, got_one;
  shard_set->Await(0, [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    got_zero = db_slice.GetMvcc(0, std::string_view{"k_zero"});
    got_one = db_slice.GetMvcc(0, std::string_view{"k_one"});
  });
  ASSERT_TRUE(got_zero.has_value()) << "an active loader must still leave a dense slot";
  EXPECT_TRUE(got_zero->Empty())
      << "an mvcc-tstamp aux of \"0\" carries no authority, so it must load UNSTAMPED ({0,0}); "
         "installing {0, link_origin_hash} mints a non-empty stamp the file never contained and "
         "re-emits it on every later save";

  ASSERT_TRUE(got_one.has_value());
  EXPECT_EQ(*got_one, (MvccStamp{1, kLinkOrigin}))
      << "the zero guard must not overreach: the smallest non-zero value must still install";
}

// drakeydb: P4-2 Task 3, review round 1 (Important, finding 2) -- the brief's "malformed value
// must warn and load the key unstamped, never fail the load" requirement had zero coverage.
// Real log capture (ScopedLogCapture, used identically by
// ActiveReloadDoesNotWarnOnRecognizedMvccAux above), not a code-inspection stand-in.
TEST_F(RdbMvccTest, WarnsAndLoadsUnstampedOnMalformedMvccTstampAux) {
  ASSERT_TRUE(IsActiveReplica());
  ScopedLogCapture log_capture;

  std::string body;
  body.push_back(static_cast<char>(RDB_OPCODE_AUX));
  AppendString(&body, "mvcc-tstamp");
  AppendString(&body, "not-a-number");
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "k1");
  AppendString(&body, "v1");

  const std::string rdb = WrapInRdb(body);
  io::BytesSource src{io::Buffer(rdb)};
  RdbLoadContext load_context;
  auto ec = pp_->at(0)->Await([&]() -> std::error_code {
    RdbLoader loader(service_.get(), &load_context);
    return loader.Load(&src);
  });
  ASSERT_FALSE(ec) << ec.message() << " -- a malformed mvcc-tstamp aux must never fail the load";

  EXPECT_EQ(Run({"get", "k1"}), "v1");

  std::optional<MvccStamp> got;
  shard_set->Await(0, [&] {
    got = namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, std::string_view{"k1"});
  });
  ASSERT_TRUE(got.has_value());
  EXPECT_TRUE(got->Empty())
      << "a malformed mvcc-tstamp aux must load the key unstamped ({0,0}), not fail the load";

  bool warned = false;
  for (const auto& log : log_capture.logs) {
    if (log.find("Ignoring malformed mvcc-tstamp aux") != std::string::npos &&
        log.find("not-a-number") != std::string::npos) {
      warned = true;
    }
  }
  EXPECT_TRUE(warned) << "a malformed mvcc-tstamp aux must be warned about, naming the value";
}

// drakeydb: P4-2, final review (Critical) -- regression coverage for the stamp-source bug the
// adversarial pass proved live (adversarial-review.md): SerializerBase::SerializeEntry read each
// key's stamp through the LIVE DbSlice while the value it serialized came from the pointer array
// captured at snapshot start. A FLUSHALL mid-save makes those two different DbTable objects, so
// the pair the RDB recorded was either stamp-less ({0,0}, 74.9% of sampled keys in the live
// repro) or -- for a key re-created after the flush -- the pre-flush VALUE married to the
// post-flush write's STAMP: authority no node ever issued for that value, and bit-identical to a
// peer's genuine newer write, so LWW can never reconcile it.
//
// This probe drives the real SerializerBase pipeline (RegisterChangeListener -> ProcessBucket ->
// SerializeBucketLocked -> SerializeEntry -> MvccOf) and stubs only the output sink, so the
// {value, stamp} pair the serializer produced is directly observable.
namespace {

class CapturedTableProbe : public SerializerBase {
 public:
  struct Emitted {
    std::string value;
    MvccStamp mvcc;
  };

  CapturedTableProbe(DbSlice* slice, ExecutionState* cntx) : SerializerBase(slice, cntx) {
  }

  using SerializerBase::MvccOf;
  using SerializerBase::RegisterChangeListener;
  using SerializerBase::UnregisterChangeListener;

  // Mirrors SliceSnapshot::IterateBucketsFb (snapshot.cc): the traversal flow walks the CAPTURED
  // tables, never the live ones, and reports on_update=false.
  void TraverseCaptured() {
    for (DbIndex i = 0; i < db_array_.size(); ++i) {
      if (!db_array_[i])
        continue;
      PrimeTable* pt = &db_array_[i]->prime;
      PrimeTable::Cursor cursor;
      do {
        cursor = pt->TraverseBuckets(
            cursor, [&](PrimeTable::bucket_iterator it) { ProcessBucket(i, it, false); });
      } while (cursor);
    }
  }

  const PrimeTable* captured_prime(DbIndex db_index) const {
    return db_index < db_array_.size() && db_array_[db_index] ? &db_array_[db_index]->prime
                                                              : nullptr;
  }

  absl::flat_hash_map<std::string, Emitted> emitted;

 private:
  unsigned SerializeBucketLocked(DbIndex db_index, PrimeTable::bucket_iterator it,
                                 bool on_update) override {
    unsigned n = 0;
    for (it.AdvanceIfNotOccupied(); !it.is_done(); ++it, ++n)
      SerializerBase::SerializeEntry(it.bucket_address(), db_index, it->first, it->second,
                                     &it.owner());
    return n;
  }

  void SerializeEntryLocked(DbIndex db_index, const PrimeKey& pk, const PrimeValue& pv,
                            time_t expire, uint32_t mc_flags, const MvccStamp& mvcc) override {
    std::string scratch;
    emitted[std::string(pk.GetSlice(&scratch))] = Emitted{pv.ToString(), mvcc};
  }
};

// Accumulates every chunk a SliceSnapshot pushes, so the raw RDB bytes can be scanned.
class RecordingSnapshotConsumer : public SliceSnapshot::SnapshotDataConsumerInterface {
 public:
  void ConsumeData(std::string data, ExecutionState*) override {
    bytes.append(data);
  }
  void Finalize() override {
  }

  std::string bytes;
};

}  // namespace

TEST_F(RdbMvccTest, SerializerReadsStampsFromTheCapturedTableNotTheLiveOne) {
  ASSERT_TRUE(IsActiveReplica());

  constexpr int kKeys = 8;
  for (int i = 0; i < kKeys; ++i)
    ASSERT_EQ(Run({"set", StrCat("k", i), StrCat("v", i)}), "OK");

  // Exact, recognizable pre-flush stamps -- one per key -- so a lost stamp ({0,0}) and a
  // fabricated one (the post-flush stamp below) are distinguishable from each other.
  std::vector<MvccStamp> pre(kKeys);
  for (int i = 0; i < kKeys; ++i)
    pre[i] = MvccStamp{0x1000ULL + i, 0xAAAA0000ULL + i};
  shard_set->Await(0, [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    for (int i = 0; i < kKeys; ++i)
      db_slice.SetMvcc(0, std::string_view{StrCat("k", i)}, pre[i]);
  });

  ExecutionState cntx;
  std::optional<CapturedTableProbe> probe;

  pp_->at(0)->Await([&] {
    EngineShard* shard = EngineShard::tlocal();
    DbSlice& db_slice = namespaces->GetDefaultNamespace().GetDbSlice(shard->shard_id());
    probe.emplace(&db_slice, &cntx);
    // DbSlice::RegisterOnChange DCHECKs the shard's intent lock is held; command dispatch holds
    // it in production, this test drives the serializer directly.
    shard->shard_lock()->Acquire(IntentLock::EXCLUSIVE);
    probe->RegisterChangeListener(/*replication=*/false);  // captures db_array_
    shard->shard_lock()->Release(IntentLock::EXCLUSIVE);
  });

  // The flush lands strictly between "capture" and "traverse": the live array is swapped for
  // fresh DbTables whose mvcc side table is brand new and EMPTY, while the captured intrusive_ptrs
  // keep the old tables (values AND stamps) alive.
  ASSERT_EQ(Run({"flushall"}), "OK");

  // k0 is re-created after the flush and given a stamp of its own -- the value the buggy code
  // pasted onto k0's PRE-flush value.
  const MvccStamp kPostFlush{0x9999ULL, 0xBBBB0000ULL};
  ASSERT_EQ(Run({"set", "k0", "NEWVAL"}), "OK");
  shard_set->Await(0, [&] {
    namespaces->GetDefaultNamespace().GetCurrentDbSlice().SetMvcc(0, std::string_view{"k0"},
                                                                  kPostFlush);
  });

  MvccStamp captured_branch{}, foreign_branch{}, unregistered{};
  pp_->at(0)->Await([&] {
    EngineShard* shard = EngineShard::tlocal();
    DbSlice& db_slice = namespaces->GetDefaultNamespace().GetDbSlice(shard->shard_id());

    probe->TraverseCaptured();
    PrimeKey pk{"k0"};
    const PrimeTable* captured_pt = probe->captured_prime(0);
    const PrimeTable* live_pt = &db_slice.GetDBTable(0)->prime;
    ASSERT_NE(captured_pt, live_pt) << "the flush must have replaced the table";

    captured_branch = probe->MvccOf(0, pk, captured_pt);
    foreign_branch = probe->MvccOf(0, pk, live_pt);
    // An owner this thread knows nothing about (and RestoreStreamer's cancelled-before-Start
    // case, where db_array_ is empty -- streamer.cc): unstamped, never a null deref.
    CapturedTableProbe never_registered(&db_slice, &cntx);
    unregistered = never_registered.MvccOf(0, pk, nullptr);

    probe->UnregisterChangeListener();
  });

  auto emitted = probe->emitted;
  pp_->at(0)->Await([&] { probe.reset(); });

  ASSERT_EQ(emitted.size(), size_t(kKeys))
      << "the traversal must still see the captured point-in-time content after the flush";
  for (int i = 0; i < kKeys; ++i) {
    const std::string key = StrCat("k", i);
    ASSERT_TRUE(emitted.contains(key)) << key << " was not serialized";
    EXPECT_EQ(emitted[key].value, StrCat("v", i)) << key
                                                  << ": value must come from the captured "
                                                     "table (point-in-time semantics)";
    EXPECT_EQ(emitted[key].mvcc, pre[i])
        << key
        << ": the stamp must come from the SAME table as the value. A zero stamp here is "
           "the stamp-loss half of the bug (the live table's side table is empty after the "
           "flush); anything else is fabrication.";
  }
  EXPECT_NE(emitted["k0"].mvcc, kPostFlush)
      << "k0 was serialized with its PRE-flush value but its POST-flush stamp -- a {value, stamp} "
         "pair no node ever authored, which ties bit-identically against a peer's genuine newer "
         "value and can never LWW-reconcile";

  EXPECT_EQ(captured_branch, pre[0])
      << "a bucket owned by the captured table must resolve against the captured table";
  EXPECT_TRUE(foreign_branch.Empty())
      << "a post-snapshot live owner must not lend its stamp to captured content";
  EXPECT_TRUE(unregistered.Empty()) << "an unknown owner must be unstamped, not a deref";
}

// drakeydb: P4-2, final review (Critical) -- the same invariant through the REAL SliceSnapshot,
// so snapshot.cc's on_update plumbing (not just SerializerBase's own resolution) is pinned: a
// mid-save FLUSHALL must not strip the DF_MVCC record off the point-in-time bytes.
TEST_F(RdbMvccTest, SnapshotKeepsTheMvccOpcodeAcrossAMidSaveFlush) {
  ASSERT_TRUE(IsActiveReplica());
  ASSERT_EQ(Run({"set", "k1", "v1"}), "OK");

  const MvccStamp kStamp{0x0123456789ABCDEFULL, 0xFEDCBA9876543210ULL};
  shard_set->Await(0, [&] {
    namespaces->GetDefaultNamespace().GetCurrentDbSlice().SetMvcc(0, std::string_view{"k1"},
                                                                  kStamp);
  });

  RecordingSnapshotConsumer consumer;
  ExecutionState cntx;
  uint64_t serialized_at_swap = 0;
  size_t live_size_after_flush = 0;

  pp_->at(0)->Await([&] {
    EngineShard* shard = EngineShard::tlocal();
    DbSlice& db_slice = namespaces->GetDefaultNamespace().GetDbSlice(shard->shard_id());
    shard->shard_lock()->Acquire(IntentLock::EXCLUSIVE);

    SliceSnapshot snapshot(CompressionMode::NONE, &db_slice, &consumer, &cntx,
                           DflyVersion::CURRENT_VER);
    // kDisallow keeps the output un-chunked and untagged so it can be byte-scanned below.
    snapshot.Start(/*stream_journal=*/false, SliceSnapshot::SnapshotFlush::kDisallow);

    // Start() captures db_array_ and queues the traversal fiber; fb2 fibers are cooperative, so
    // nothing runs until this fiber yields. FlushDbIndexes swaps the tables synchronously and
    // hands back the deallocation fiber, so at this point the swap is done and -- asserted below
    // -- not one key has been serialized yet. Every key in the output therefore came out of the
    // captured table AFTER the live one was replaced: the exact window the bug lived in.
    fb2::Fiber flush_fb = db_slice.FlushDb(0);
    serialized_at_swap = snapshot.GetStats().keys_serialized;
    live_size_after_flush = db_slice.DbSize(0);
    flush_fb.Join();

    snapshot.WaitSnapshotting();
    shard->shard_lock()->Release(IntentLock::EXCLUSIVE);
  });

  ASSERT_EQ(serialized_at_swap, 0u)
      << "guard against a vacuous pass: the snapshot must not have serialized anything before the "
         "flush swapped the tables";
  ASSERT_EQ(live_size_after_flush, 0u) << "guard against a vacuous pass: the flush must have run";

  std::string key1_encoded;
  AppendString(&key1_encoded, "k1");
  ASSERT_NE(consumer.bytes.find(key1_encoded), std::string::npos)
      << "the captured point-in-time content must still be serialized after the flush";

  uint8_t block[17] = {RDB_OPCODE_DF_MVCC};
  absl::little_endian::Store64(block + 1, kStamp.packed);
  absl::little_endian::Store64(block + 9, kStamp.origin_hash);
  std::string_view mvcc_block(reinterpret_cast<const char*>(block), sizeof(block));
  EXPECT_NE(consumer.bytes.find(mvcc_block), std::string::npos)
      << "k1's stamp was dropped (or altered) because it was looked up in the post-flush LIVE "
         "table instead of the captured one the value came from";
}

// drakeydb: P4-3 Task 13 -- replaces Task 12's withdrawn unconditional-override rule
// (task-12-report.md; controller reversal, task-13-report.md). `!item->has_mvcc` captured far
// more than "a peer that cannot stamp": it also matched keys P4-2 deliberately downgraded so they
// would LOSE (KeyDB's OBJ_MVCC_INVALID sentinel, a zero mvcc-tstamp, a malformed aux, or one
// without an authenticated origin), which then won unconditionally instead -- and on the DFLY
// multi-shard protocol, SaveEntry omits RDB_OPCODE_DF_MVCC outright for a {0,0} stamp, so an
// unversioned drakeydb peer (or a whole non-active drakeydb master) would override this node's
// ENTIRE resident dataset, a direct D-7 violation (see MergeLwwDflyUnstampedIncomingLosesTo
// StampedResident below, pinning exactly that). The replacement: on a CLASSIC-PSYNC link only
// (SetMergeLww's third argument), an unstamped key is stamped from the snapshot's own "ctime" aux
// (HandleAux) -- clamped to this node's own `now`, so a peer can never claim a future time -- and
// then runs through the ORDINARY MergeAccepts compare, never bypassed. This test: a resident key
// written 5 real seconds AFTER the snapshot's ctime must still win against an unstamped incoming
// key from that snapshot.
TEST_F(RdbMvccTest, MergeLwwClassicUnstampedIncomingLosesToResidentWrittenAfterCtime) {
  ASSERT_TRUE(IsActiveReplica());
  constexpr uint64_t kPeerHash = 0xFEDCBA9876543210ULL;
  constexpr uint64_t kSelfHash = 0x1122334455667788ULL;
  // Deliberately old and far below any real "now" in this process's lifetime, so `min(ctime, now)`
  // in the implementation is a no-op here -- this test isolates the ctime-vs-resident compare
  // itself, not the future-clamp (see MergeLwwClassicFutureCtimeClampedToNow below for that).
  const int64_t kCtimeSec = 1'000'000;
  const uint64_t kCtimeMs = static_cast<uint64_t>(kCtimeSec) * 1000;
  const MvccStamp kResidentStamp{(kCtimeMs + 5000) << MvccClock::kCounterBits, kSelfHash};

  ASSERT_EQ(Run({"set", "k", "resident_v"}), "OK");
  shard_set->Await(0, [&] {
    namespaces->GetDefaultNamespace().GetCurrentDbSlice().SetMvcc(0, std::string_view{"k"},
                                                                  kResidentStamp);
  });

  std::string body;
  body.push_back(static_cast<char>(RDB_OPCODE_AUX));
  AppendString(&body, "ctime");
  AppendString(&body, absl::StrCat(kCtimeSec));
  body.push_back(RDB_TYPE_STRING);  // no RDB_OPCODE_DF_MVCC block: unstamped, like a real Redis RDB
  AppendString(&body, "k");
  AppendString(&body, "incoming_v");

  const std::string rdb = WrapInRdb(body);
  io::BytesSource src{io::Buffer(rdb)};
  RdbLoadContext load_context;
  auto ec = pp_->at(0)->Await([&]() -> std::error_code {
    RdbLoader loader(service_.get(), &load_context);
    loader.SetMergeLww(true, kPeerHash, /*classic_protocol=*/true);
    return loader.Load(&src);
  });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_EQ(Run({"get", "k"}), "resident_v")
      << "a resident value written AFTER the snapshot's ctime must survive an unstamped incoming "
         "key from that snapshot";

  std::optional<MvccStamp> got;
  shard_set->Await(0, [&] {
    got = namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, std::string_view{"k"});
  });
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(*got, kResidentStamp) << "the resident stamp must also survive untouched";
}

// The mirror of the case above: a resident key written 5 seconds BEFORE the snapshot's ctime must
// lose to the unstamped incoming key, which is stamped with ctime's (later) authority.
TEST_F(RdbMvccTest, MergeLwwClassicUnstampedIncomingWinsAgainstResidentWrittenBeforeCtime) {
  ASSERT_TRUE(IsActiveReplica());
  constexpr uint64_t kPeerHash = 0xFEDCBA9876543210ULL;
  constexpr uint64_t kSelfHash = 0x1122334455667788ULL;
  const int64_t kCtimeSec = 1'000'000;
  const uint64_t kCtimeMs = static_cast<uint64_t>(kCtimeSec) * 1000;
  const MvccStamp kResidentStamp{(kCtimeMs - 5000) << MvccClock::kCounterBits, kSelfHash};

  ASSERT_EQ(Run({"set", "k", "resident_v"}), "OK");
  shard_set->Await(0, [&] {
    namespaces->GetDefaultNamespace().GetCurrentDbSlice().SetMvcc(0, std::string_view{"k"},
                                                                  kResidentStamp);
  });

  std::string body;
  body.push_back(static_cast<char>(RDB_OPCODE_AUX));
  AppendString(&body, "ctime");
  AppendString(&body, absl::StrCat(kCtimeSec));
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "k");
  AppendString(&body, "incoming_v");

  const std::string rdb = WrapInRdb(body);
  io::BytesSource src{io::Buffer(rdb)};
  RdbLoadContext load_context;
  auto ec = pp_->at(0)->Await([&]() -> std::error_code {
    RdbLoader loader(service_.get(), &load_context);
    loader.SetMergeLww(true, kPeerHash, /*classic_protocol=*/true);
    return loader.Load(&src);
  });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_EQ(Run({"get", "k"}), "incoming_v")
      << "an unstamped incoming key must win against a resident value written BEFORE the "
         "snapshot's ctime";

  std::optional<MvccStamp> got;
  shard_set->Await(0, [&] {
    got = namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, std::string_view{"k"});
  });
  ASSERT_TRUE(got.has_value());
  // +999: ctime is 1-second granularity: CreateObjectOnShard (rdb_load.cc) assumes the latest
  // millisecond consistent with the truncated value -- see that function's own comment (post-
  // review fix) for why the literal start-of-second value broke the same-real-second race this
  // rule exists to resolve in the first place.
  EXPECT_EQ(got->Mvcc(), (kCtimeMs + 999) << MvccClock::kCounterBits)
      << "the installed stamp must carry ctime's own ms value (rounded to the latest ms within "
         "its 1-second granularity), not a fresh HopStamp -- Task 12's withdrawn rule minted one, "
         "Task 13 does not";
  EXPECT_EQ(got->origin_hash, kPeerHash);
}

// drakeydb: P4-3 Task 13, re-review fix -- pins the D-8 exposure this rule NARROWS but does not
// ELIMINATE (see CreateObjectOnShard's own comment, rdb_load.cc, for the corrected account: an
// earlier version of that comment claimed the ctime rule stops a classic peer from resurrecting
// locally tombstoned keys; it does not). GetMvcc's stored side includes tombstones, and
// MergeAccepts masks bit 63 -- so an unstamped classic key's ctime-derived stamp beats ANY
// resident tombstone strictly OLDER than min(ctime+999, now), same as it would beat any other
// resident value that old. A classic-PSYNC peer carries no per-key write time at all, only this
// one whole-snapshot timestamp, so there is no way to distinguish "this peer's copy is a
// legitimate rewrite made after our delete" from "this is just the peer's stale pre-delete copy"
// -- the resurrection risk this test pins is real, not a test artifact (the reviewer confirmed it
// empirically against a live server: delete a key locally, wait, full-sync from a peer whose own
// copy predates the local delete -- the tombstone is replaced, 3/3 runs).
//
// Paired with MergeLwwClassicUnstampedIncomingRespectsTombstoneNewerThanCtime below so neither
// test is vacuous against a stub that always accepts or always rejects: a stub that always
// REJECTS the incoming key would fail THIS test (the tombstone would wrongly survive); a stub
// that always ACCEPTS it would fail the other one (the newer tombstone would wrongly be cleared).
TEST_F(RdbMvccTest, MergeLwwClassicUnstampedIncomingResurrectsTombstoneOlderThanCtime) {
  ASSERT_TRUE(IsActiveReplica());
  constexpr uint64_t kPeerHash = 0xFEDCBA9876543210ULL;
  constexpr uint64_t kSelfHash = 0x1122334455667788ULL;
  const int64_t kCtimeSec = 1'000'000;
  const uint64_t kCtimeMs = static_cast<uint64_t>(kCtimeSec) * 1000;
  // Strictly older than min(ctime+999, now): the local delete happened well before the snapshot's
  // own ctime, so it carries no information the classic peer's snapshot could have "seen".
  const MvccStamp kResidentTombstone =
      MvccStamp{(kCtimeMs - 5000) << MvccClock::kCounterBits, kSelfHash}.AsTombstone();

  shard_set->Await(0, [&] {
    namespaces->GetDefaultNamespace().GetCurrentDbSlice().SetTombstone(0, std::string_view{"k"},
                                                                       kResidentTombstone);
  });

  std::string body;
  body.push_back(static_cast<char>(RDB_OPCODE_AUX));
  AppendString(&body, "ctime");
  AppendString(&body, absl::StrCat(kCtimeSec));
  body.push_back(RDB_TYPE_STRING);  // unstamped, like a real Redis RDB
  AppendString(&body, "k");
  AppendString(&body, "incoming_v");

  const std::string rdb = WrapInRdb(body);
  io::BytesSource src{io::Buffer(rdb)};
  RdbLoadContext load_context;
  auto ec = pp_->at(0)->Await([&]() -> std::error_code {
    RdbLoader loader(service_.get(), &load_context);
    loader.SetMergeLww(true, kPeerHash, /*classic_protocol=*/true);
    return loader.Load(&src);
  });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_EQ(Run({"get", "k"}), "incoming_v")
      << "a resident tombstone OLDER than the snapshot's (clamped) ctime must be beaten by an "
         "unstamped classic key -- this is the D-8 exposure the ctime rule narrows but does not "
         "eliminate, not a bug in this specific test";

  std::optional<MvccStamp> got;
  shard_set->Await(0, [&] {
    got = namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, std::string_view{"k"});
  });
  ASSERT_TRUE(got.has_value());
  EXPECT_FALSE(got->IsTombstone()) << "the tombstone must have been cleared, not just out-raced";
}

// The converse of the case above: a resident tombstone NEWER than min(ctime+999, now) must survive
// -- D-8's guard still closes the gap for anything deleted AFTER the peer's own fork point, which
// is the scenario test_active_replica_merges_redis_full_sync_via_synthetic_uuid (multimaster_test)
// and MergeLwwClassicUnstampedIncomingLosesToResidentWrittenAfterCtime above both already exercise
// for a resident LIVE value; this test is the same guarantee for a resident TOMBSTONE specifically.
TEST_F(RdbMvccTest, MergeLwwClassicUnstampedIncomingRespectsTombstoneNewerThanCtime) {
  ASSERT_TRUE(IsActiveReplica());
  constexpr uint64_t kPeerHash = 0xFEDCBA9876543210ULL;
  constexpr uint64_t kSelfHash = 0x1122334455667788ULL;
  const int64_t kCtimeSec = 1'000'000;
  const uint64_t kCtimeMs = static_cast<uint64_t>(kCtimeSec) * 1000;
  // Strictly newer than min(ctime+999, now) == ctime+999 here (ctime is far below any real "now"
  // in this process's lifetime, so the clamp is a no-op): the local delete happened AFTER the
  // snapshot was taken.
  const MvccStamp kResidentTombstone =
      MvccStamp{(kCtimeMs + 999 + 5000) << MvccClock::kCounterBits, kSelfHash}.AsTombstone();

  shard_set->Await(0, [&] {
    namespaces->GetDefaultNamespace().GetCurrentDbSlice().SetTombstone(0, std::string_view{"k"},
                                                                       kResidentTombstone);
  });

  std::string body;
  body.push_back(static_cast<char>(RDB_OPCODE_AUX));
  AppendString(&body, "ctime");
  AppendString(&body, absl::StrCat(kCtimeSec));
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "k");
  AppendString(&body, "incoming_v");

  const std::string rdb = WrapInRdb(body);
  io::BytesSource src{io::Buffer(rdb)};
  RdbLoadContext load_context;
  auto ec = pp_->at(0)->Await([&]() -> std::error_code {
    RdbLoader loader(service_.get(), &load_context);
    loader.SetMergeLww(true, kPeerHash, /*classic_protocol=*/true);
    return loader.Load(&src);
  });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_THAT(Run({"get", "k"}), kMatchNil)
      << "a resident tombstone NEWER than the snapshot's (clamped) ctime must survive -- the "
         "unstamped classic key must lose to it, exactly like it would lose to any other "
         "resident value that new";

  std::optional<MvccStamp> got;
  shard_set->Await(0, [&] {
    got = namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, std::string_view{"k"});
  });
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(*got, kResidentTombstone) << "the resident tombstone must survive untouched";
}

// drakeydb: P4-3 Task 13 -- C2 pinned: on the DFLY multi-shard protocol (classic_protocol left at
// its default, false), an unstamped incoming key keeps D-7's {0,0} fallback and therefore still
// loses to ANY stamped resident value, exactly as it did before Task 12 ever existed. This is the
// scenario Task 12's withdrawn rule broke silently: SaveEntry (snapshot.cc) never emits
// RDB_OPCODE_DF_MVCC for a {0,0} stamp, so an unversioned drakeydb peer (or a whole non-active
// drakeydb master, all of whose keys are unstamped) must never be able to override this node's
// resident dataset on this link type.
TEST_F(RdbMvccTest, MergeLwwDflyUnstampedIncomingLosesToStampedResident) {
  ASSERT_TRUE(IsActiveReplica());
  constexpr uint64_t kPeerHash = 0xFEDCBA9876543210ULL;
  constexpr uint64_t kSelfHash = 0x1122334455667788ULL;
  const MvccStamp kResidentStamp{0x1000, kSelfHash};

  ASSERT_EQ(Run({"set", "k", "resident_v"}), "OK");
  shard_set->Await(0, [&] {
    namespaces->GetDefaultNamespace().GetCurrentDbSlice().SetMvcc(0, std::string_view{"k"},
                                                                  kResidentStamp);
  });

  std::string body;
  body.push_back(RDB_TYPE_STRING);  // unstamped: no RDB_OPCODE_DF_MVCC block
  AppendString(&body, "k");
  AppendString(&body, "incoming_v");

  const std::string rdb = WrapInRdb(body);
  io::BytesSource src{io::Buffer(rdb)};
  RdbLoadContext load_context;
  auto ec = pp_->at(0)->Await([&]() -> std::error_code {
    RdbLoader loader(service_.get(), &load_context);
    loader.SetMergeLww(true, kPeerHash);  // classic_protocol defaults false: the DFLY link
    return loader.Load(&src);
  });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_EQ(Run({"get", "k"}), "resident_v")
      << "C2: an unstamped key on the DFLY link must never override a stamped resident value";

  std::optional<MvccStamp> got;
  shard_set->Await(0, [&] {
    got = namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, std::string_view{"k"});
  });
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(*got, kResidentStamp);
}

// drakeydb: P4-3 Task 13 -- C1 pinned: a KeyDB OBJ_MVCC_INVALID sentinel (mvcc-tstamp aux value
// 2^64-1, KeyDB/src/server.h:958) is deliberately treated as unstamped by HandleAux's own
// mvcc-tstamp branch (rdb_load.cc, P4-2 Task 3) -- Task 12's withdrawn rule then let it win
// unconditionally (+infinity authority) instead of the "no valid mvcc" it actually represents.
// Under Task 13's ctime rule, it gets the SAME ctime authority as any other unstamped key on this
// classic link -- so it still loses to a resident value written after ctime, exactly like
// MergeLwwClassicUnstampedIncomingLosesToResidentWrittenAfterCtime above.
TEST_F(RdbMvccTest, MergeLwwClassicKeyDbInvalidMvccGetsCtimeAuthorityNotInfinity) {
  ASSERT_TRUE(IsActiveReplica());
  constexpr uint64_t kPeerHash = 0xFEDCBA9876543210ULL;
  constexpr uint64_t kSelfHash = 0x1122334455667788ULL;
  const int64_t kCtimeSec = 1'000'000;
  const uint64_t kCtimeMs = static_cast<uint64_t>(kCtimeSec) * 1000;
  const MvccStamp kResidentStamp{(kCtimeMs + 5000) << MvccClock::kCounterBits, kSelfHash};

  ASSERT_EQ(Run({"set", "k", "resident_v"}), "OK");
  shard_set->Await(0, [&] {
    namespaces->GetDefaultNamespace().GetCurrentDbSlice().SetMvcc(0, std::string_view{"k"},
                                                                  kResidentStamp);
  });

  std::string body;
  body.push_back(static_cast<char>(RDB_OPCODE_AUX));
  AppendString(&body, "ctime");
  AppendString(&body, absl::StrCat(kCtimeSec));
  body.push_back(static_cast<char>(RDB_OPCODE_AUX));
  AppendString(&body, "mvcc-tstamp");
  AppendString(&body, "18446744073709551615");  // 2^64-1, KeyDB's OBJ_MVCC_INVALID
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "k");
  AppendString(&body, "incoming_v");

  const std::string rdb = WrapInRdb(body);
  io::BytesSource src{io::Buffer(rdb)};
  RdbLoadContext load_context;
  auto ec = pp_->at(0)->Await([&]() -> std::error_code {
    RdbLoader loader(service_.get(), &load_context);
    loader.SetMergeLww(true, kPeerHash, /*classic_protocol=*/true);
    return loader.Load(&src);
  });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_EQ(Run({"get", "k"}), "resident_v")
      << "C1: OBJ_MVCC_INVALID must get ctime authority, not +infinity -- it must still lose to a "
         "resident value written after ctime";

  std::optional<MvccStamp> got;
  shard_set->Await(0, [&] {
    got = namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, std::string_view{"k"});
  });
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(*got, kResidentStamp);
}

// drakeydb: P4-3 Task 13 -- the future-ctime clamp: a snapshot claiming a ctime in the future must
// never be believed literally (a peer's clock is not this node's authority) -- clamped to this
// node's own `now` instead, per `min(ctime_ms, now_ms)` in CreateObjectOnShard (rdb_load.cc). Uses
// an ABSENT key (no resident conflict) purely to observe the installed stamp's value directly.
TEST_F(RdbMvccTest, MergeLwwClassicFutureCtimeClampedToNow) {
  ASSERT_TRUE(IsActiveReplica());
  constexpr uint64_t kPeerHash = 0xFEDCBA9876543210ULL;
  const int64_t kFutureCtimeSec =
      static_cast<int64_t>(time(nullptr)) + 1'000'000;  // ~11.5 days out

  std::string body;
  body.push_back(static_cast<char>(RDB_OPCODE_AUX));
  AppendString(&body, "ctime");
  AppendString(&body, absl::StrCat(kFutureCtimeSec));
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "k");
  AppendString(&body, "incoming_v");

  const std::string rdb = WrapInRdb(body);
  io::BytesSource src{io::Buffer(rdb)};
  RdbLoadContext load_context;
  auto ec = pp_->at(0)->Await([&]() -> std::error_code {
    RdbLoader loader(service_.get(), &load_context);
    loader.SetMergeLww(true, kPeerHash, /*classic_protocol=*/true);
    return loader.Load(&src);
  });
  ASSERT_FALSE(ec) << ec.message();
  const uint64_t after_load_ms = GetCurrentTimeMs();

  EXPECT_EQ(Run({"get", "k"}), "incoming_v");

  std::optional<MvccStamp> got;
  shard_set->Await(0, [&] {
    got = namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, std::string_view{"k"});
  });
  ASSERT_TRUE(got.has_value());
  EXPECT_LE(got->MsPart(), after_load_ms)
      << "a future ctime must be clamped to this node's own `now`, never believed literally";
}

// drakeydb: P4-3 Task 13 -- merge_origin_hash_ == 0 should never happen on a real peer link
// (SetMergeLww's only caller, replica.cc, always passes peer_origin_hash_, which the ownership
// comment there notes stays "unknown origin" (0) only for a non-peer Replica -- a path that never
// sets merge_lww_ true at all), but the ctime-derived stamp must not silently carry {mvcc, 0} in
// that case either -- indistinguishable from a genuine self origin, misattributing the write.
// Falls back to this node's own origin hash (PeerRegistry::kSelfIdx) instead.
TEST_F(RdbMvccTest, MergeLwwClassicUnstampedIncomingWithZeroOriginHashFallsBackToSelfOrigin) {
  ASSERT_TRUE(IsActiveReplica());
  ASSERT_EQ(Run({"set", "control", "v"}), "OK");
  std::optional<MvccStamp> control_stamp;
  shard_set->Await(0, [&] {
    control_stamp = namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(
        0, std::string_view{"control"});
  });
  ASSERT_TRUE(control_stamp.has_value());
  const uint64_t self_origin_hash = control_stamp->origin_hash;
  ASSERT_NE(self_origin_hash, 0u) << "guard against a vacuous pass";

  std::string body;
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "k");
  AppendString(&body, "incoming_v");

  const std::string rdb = WrapInRdb(body);
  io::BytesSource src{io::Buffer(rdb)};
  RdbLoadContext load_context;
  auto ec = pp_->at(0)->Await([&]() -> std::error_code {
    RdbLoader loader(service_.get(), &load_context);
    loader.SetMergeLww(true, /*sender_origin_hash=*/0, /*classic_protocol=*/true);
    return loader.Load(&src);
  });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_EQ(Run({"get", "k"}), "incoming_v");

  std::optional<MvccStamp> got;
  shard_set->Await(0, [&] {
    got = namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, std::string_view{"k"});
  });
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(got->origin_hash, self_origin_hash)
      << "merge_origin_hash_ == 0 must fall back to this node's own origin hash, not write 0";
}

// drakeydb: P4-3 Task 12 carried item (b) -- FindMutable's own ExpireIfNeeded call (inside the
// merge-apply's delete branch, rdb_load.cc, HandleTombstones) can lazily expire an already-expired
// RESIDENT key itself before this callback ever gets to decide anything about the peer's own
// incoming tombstone. That lazy expiry goes through PerformDeletionAtomic's kExpired branch (Task
// 11), which ArmTombstone's the key exactly like a kExplicit delete would -- expecting
// RecordExpiryBlocking's own journal Commit() to consume that arm. That only happens when this
// shard's journal is actually running (ExpireIfNeeded's own `if (journal && journal_expiry)`
// gate, db_slice.cc); an active-replica node's journal is normally always on from boot
// (server_family.cc), but this test forces it off on shard 0 specifically to reach the narrower
// case the brief calls out -- without an explicit Disarm on every path out of the merge-apply
// block, that arm sits pending until some LATER, unrelated command's epoch end
// (Transaction::RunCallback, transaction.cc) rolls it back via RollbackUncommittedTombstone --
// erasing whatever this callback itself installed at that exact mvcc slot in the meantime, which
// in this scenario is the peer's own real tombstone. See task-12-report.md for the verbatim
// falsification (removing the Disarm call in the "no live key" fallthrough reproduces this:
// `got_after` comes back empty instead of the peer's tombstone).
TEST_F(RdbMvccTest, MergeLwwTombstoneOnLazilyExpiredResidentKeySurvivesLaterEpochEnd) {
  ASSERT_TRUE(IsActiveReplica());
  constexpr uint64_t kPeerHash = 0xFEDCBA9876543210ULL;
  constexpr uint64_t kSelfHash = 0x1122334455667788ULL;
  const MvccStamp kIncomingTombstone = MvccStamp{0x2000, kPeerHash}.AsTombstone();

  // A whole-key TTL that has already elapsed by the time the load below runs FindMutable on it,
  // but nothing has touched it since, so it is still physically present -- ExpireIfNeeded discovers
  // this lazily (same recipe as multi_master_test.cc's
  // ExpiryMidMultiKeyAppliedWriteKeepsSiblingAuthorMvcc for the analogous applied-command case).
  ASSERT_EQ(Run({"set", "k", "resident_v", "px", "10"}), "OK");
  shard_set->Await(0, [&] {
    namespaces->GetDefaultNamespace().GetCurrentDbSlice().SetMvcc(0, std::string_view{"k"},
                                                                  MvccStamp{0x1000, kSelfHash});
    // Force this shard's journal off, so ExpireIfNeeded's lazy expiry below skips
    // RecordExpiryBlocking entirely (its own `journal && journal_expiry` gate, db_slice.cc) and
    // leaves the kExpired delete's tombstone ARM genuinely uncommitted -- the exact precondition
    // this test exists to exercise. Does not affect mvcc_enabled()/the epoch-end mechanism below,
    // which is gated on that alone, not on journal state.
    EngineShard::tlocal()->set_journal(false);
  });
  AdvanceTime(50);

  std::string body = BuildTombstoneSection(0, {{"k", kIncomingTombstone}});
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "after");
  AppendString(&body, "afterval");

  const std::string rdb = WrapInRdb(body);
  io::BytesSource src{io::Buffer(rdb)};
  RdbLoadContext load_context;
  auto ec = pp_->at(0)->Await([&]() -> std::error_code {
    RdbLoader loader(service_.get(), &load_context);
    loader.SetMergeLww(true, kPeerHash);
    return loader.Load(&src);
  });
  ASSERT_FALSE(ec) << ec.message();

  // Read the mvcc slot directly -- no command dispatch, hence no Transaction::RunCallback epoch
  // end yet -- to pin down that the peer's tombstone landed correctly immediately after load,
  // before any later epoch-end machinery has had a chance to run at all.
  std::optional<MvccStamp> got_right_after_load;
  shard_set->Await(0, [&] {
    got_right_after_load =
        namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, std::string_view{"k"});
  });
  ASSERT_TRUE(got_right_after_load.has_value())
      << "the peer's tombstone must be installed right after load";
  EXPECT_EQ(*got_right_after_load, kIncomingTombstone);

  // Any ordinary command reaches Transaction::RunCallback's epoch-end cleanup (transaction.cc),
  // which rolls back any tombstone arm still pending from before -- including this GET itself,
  // the very first command dispatched since the load. If the loader's own Disarm call
  // (rdb_load.cc) were skipped, this is exactly where the peer's tombstone would get silently
  // erased (confirmed by the falsification in task-12-report.md: removing that Disarm call makes
  // `got_after_one_epoch` below come back empty, not merely the assertion above).
  EXPECT_THAT(Run({"get", "k"}), kMatchNil);

  std::optional<MvccStamp> got_after_one_epoch;
  shard_set->Await(0, [&] {
    got_after_one_epoch =
        namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, std::string_view{"k"});
  });
  ASSERT_TRUE(got_after_one_epoch.has_value())
      << "the peer's tombstone must survive the first command dispatched after load";
  EXPECT_EQ(*got_after_one_epoch, kIncomingTombstone);

  // A second, unrelated command's epoch end must not disturb it either -- not a one-off survival.
  EXPECT_THAT(Run({"get", "unrelated"}), kMatchNil);

  std::optional<MvccStamp> got_after_second_epoch;
  size_t mismatches = 0;
  shard_set->Await(0, [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    got_after_second_epoch = db_slice.GetMvcc(0, std::string_view{"k"});
    mismatches = db_slice.TEST_VerifyMvccTable(0);
  });
  ASSERT_TRUE(got_after_second_epoch.has_value())
      << "the peer's tombstone must survive a second, unrelated command's epoch end too -- an "
         "orphaned expiry arm from FindMutable's own lazy expiry must not roll it back";
  EXPECT_EQ(*got_after_second_epoch, kIncomingTombstone);
  EXPECT_EQ(mismatches, 0u);
}

// drakeydb: P4-3 Task 12 carried item (c) -- the "no live key" tombstone install path (the
// fallthrough at the end of the merge-apply block, rdb_load.cc HandleTombstones) previously
// honoured TombstonesEnabled() alone, unlike the delete branch just above it (and
// PerformDeletionAtomic itself, db_slice.cc), which also caps at --multi_master_max_tombstones.
// Neither existing policy test (MergeLwwWinningTombstoneDeletesLiveKeyButInstallsNoneWhenDisabled/
// AtCap, Task 6) exercises this branch: both use a RESIDENT key, which always returns from the
// delete branch above and never reaches this one -- an ABSENT key is required instead.
TEST_F(RdbMvccTest, MergeLwwTombstoneForAbsentKeyInstallsNoneWhenDisabled) {
  ASSERT_TRUE(IsActiveReplica());
  absl::SetFlag(&FLAGS_multi_master_tombstone_ttl, 0);
  ASSERT_FALSE(TombstonesEnabled());

  constexpr uint64_t kPeerHash = 0xFEDCBA9876543210ULL;
  const MvccStamp kIncomingTombstone = MvccStamp{0x2000, kPeerHash}.AsTombstone();

  std::string body = BuildTombstoneSection(0, {{"ghost", kIncomingTombstone}});
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "after");
  AppendString(&body, "afterval");

  const std::string rdb = WrapInRdb(body);
  io::BytesSource src{io::Buffer(rdb)};
  RdbLoadContext load_context;
  auto ec = pp_->at(0)->Await([&]() -> std::error_code {
    RdbLoader loader(service_.get(), &load_context);
    loader.SetMergeLww(true, kPeerHash);
    return loader.Load(&src);
  });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_THAT(Run({"get", "ghost"}), kMatchNil);
  EXPECT_EQ(Run({"get", "after"}), "afterval") << "bytes after the section must still parse";

  std::optional<MvccStamp> got;
  size_t mismatches = 0;
  shard_set->Await(0, [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    got = db_slice.GetMvcc(0, std::string_view{"ghost"});
    mismatches = db_slice.TEST_VerifyMvccTable(0);
  });
  EXPECT_FALSE(got.has_value())
      << "no tombstone may be installed for an absent key while --multi_master_tombstone_ttl=0 -- "
         "one would be immortal, since TombstoneGcStep never reaps anything under this policy";
  EXPECT_EQ(mismatches, 0u) << "dense invariant must hold when nothing was installed";
}

// The cap-side counterpart to the disabled-policy test above, mirroring
// MergeLwwWinningTombstoneDeletesLiveKeyButInstallsNoneAtCap's recipe for the delete branch.
TEST_F(RdbMvccTest, MergeLwwTombstoneForAbsentKeyInstallsNoneAtCap) {
  ASSERT_TRUE(IsActiveReplica());
  absl::SetFlag(&FLAGS_multi_master_max_tombstones, 0);

  constexpr uint64_t kPeerHash = 0xFEDCBA9876543210ULL;
  const MvccStamp kIncomingTombstone = MvccStamp{0x2000, kPeerHash}.AsTombstone();

  size_t dropped_before = 0;
  shard_set->Await(0, [&] {
    dropped_before = namespaces->GetDefaultNamespace()
                         .GetCurrentDbSlice()
                         .MutableStats(0)
                         ->mvcc_tombstones_dropped;
  });

  std::string body = BuildTombstoneSection(0, {{"ghost", kIncomingTombstone}});
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "after");
  AppendString(&body, "afterval");

  const std::string rdb = WrapInRdb(body);
  io::BytesSource src{io::Buffer(rdb)};
  RdbLoadContext load_context;
  auto ec = pp_->at(0)->Await([&]() -> std::error_code {
    RdbLoader loader(service_.get(), &load_context);
    loader.SetMergeLww(true, kPeerHash);
    return loader.Load(&src);
  });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_THAT(Run({"get", "ghost"}), kMatchNil);
  EXPECT_EQ(Run({"get", "after"}), "afterval") << "bytes after the section must still parse";

  std::optional<MvccStamp> got;
  size_t dropped_after = 0, mismatches = 0;
  shard_set->Await(0, [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    got = db_slice.GetMvcc(0, std::string_view{"ghost"});
    dropped_after = db_slice.MutableStats(0)->mvcc_tombstones_dropped;
    mismatches = db_slice.TEST_VerifyMvccTable(0);
  });
  EXPECT_FALSE(got.has_value())
      << "no tombstone may be installed for an absent key once the per-(db, shard) cap "
         "(--multi_master_max_tombstones) is reached";
  EXPECT_EQ(dropped_after, dropped_before + 1)
      << "the cap-induced degradation must be counted here too, exactly like the delete path's";
  EXPECT_EQ(mismatches, 0u) << "dense invariant must hold when nothing was installed";
}

// drakeydb: P4-3 Task 13 review fix (I3, Important) -- MvccStamper::tlocal() is a per-SHARD-THREAD
// singleton, not per-callback: the tombstone-install lambda (HandleTombstones, rdb_load.cc) is
// dispatched via shard_set->Add, and an ordinary command can yield between its own arm
// (PostUpdate::Arm() or PerformDeletionAtomic's ArmTombstone) and its eventual journal Commit()
// (e.g. RecordJournal blocking on a slow replica or a full ring buffer). An earlier version of the
// "no live key" fallthrough's fix (Task 12 carried item (b)) Disarmed UNCONDITIONALLY there, even
// when found_mutable is false -- meaning this callback never itself called FindMutable and so
// could not possibly have armed anything of its own for this key. That unconditional Disarm could
// instead steal a DIFFERENT, concurrent fiber's still-pending, legitimate arm for the SAME key
// name, silently orphaning ITS eventual Commit() (a real, minted stamp would then never overwrite
// the {kTombstoneBit,0} placeholder that fiber's own delete left -- an immortal, unreapable
// tombstone). Constructed directly here rather than via real fiber timing: arms a tombstone for
// "ghost" and writes the matching placeholder PerformDeletionAtomic would have left for a
// concurrent, not-yet-committed DEL, THEN loads a peer tombstone for that same (still logically
// absent-live) key through the merge fallthrough. CommitOwnTombstone (mvcc.h) below finds and
// consumes the concurrent arm, returning true, iff the loader's own Disarm call was correctly
// scoped to found_mutable -- see task-13-report.md for the verbatim falsification (reverting the
// scoping makes this return false).
TEST_F(RdbMvccTest, MergeLwwTombstoneInstallForAbsentKeyDoesNotStealConcurrentArm) {
  ASSERT_TRUE(IsActiveReplica());
  constexpr uint64_t kPeerHash = 0xFEDCBA9876543210ULL;
  const MvccStamp kIncomingTombstone = MvccStamp{0x2000, kPeerHash}.AsTombstone();

  shard_set->Await(0, [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    // Mirrors PerformDeletionAtomic's own synchronous placeholder write + arm (db_slice.cc) for a
    // concurrent DEL of "ghost" that has not yet reached its own journal Commit().
    db_slice.SetTombstone(0, std::string_view{"ghost"}, MvccStamp{MvccClock::kTombstoneBit, 0});
    MvccStamper::tlocal()->ArmTombstone(0, std::string_view{"ghost"});
  });

  std::string body = BuildTombstoneSection(0, {{"ghost", kIncomingTombstone}});
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "after");
  AppendString(&body, "afterval");

  const std::string rdb = WrapInRdb(body);
  io::BytesSource src{io::Buffer(rdb)};
  RdbLoadContext load_context;
  auto ec = pp_->at(0)->Await([&]() -> std::error_code {
    RdbLoader loader(service_.get(), &load_context);
    loader.SetMergeLww(true, kPeerHash);
    return loader.Load(&src);
  });
  ASSERT_FALSE(ec) << ec.message();

  // Checked via shard_set->Await, BEFORE any command is dispatched: a real command would itself
  // reach Transaction::RunCallback's own epoch end, which -- entirely correctly -- rolls back any
  // arm still pending at that point (this test's synthetic "concurrent fiber" never goes on to
  // commit its own arm, unlike a real one eventually would). This block must run first so it
  // observes the state right after the loader's own install, not after that unrelated rollback.
  std::optional<MvccStamp> got;
  bool arm_survived = false;
  shard_set->Await(0, [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    got = db_slice.GetMvcc(0, std::string_view{"ghost"});
    arm_survived = MvccStamper::tlocal()->CommitOwnTombstone(
        0, std::string_view{"ghost"}, GetCurrentTimeMs(),
        [](DbIndex, std::string_view, const MvccStamp&) {});
  });
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(*got, kIncomingTombstone)
      << "the peer's real stamp must still have been installed over the placeholder";
  EXPECT_TRUE(arm_survived)
      << "the concurrent fiber's own pending tombstone arm for the SAME key must survive this "
         "callback's install -- an unconditional Disarm here would silently steal it";

  EXPECT_EQ(Run({"get", "after"}), "afterval") << "bytes after the section must still parse";
}

// drakeydb: P4-3 Task 13 review fix (I4, Important) -- the per-(database, shard) tombstone cap
// (--multi_master_max_tombstones) must gate only an install that would actually GROW the table:
// SetTombstone's own Insert-or-overwrite (db_slice.cc) does NOT increment mvcc_tombstones when the
// slot it is updating is ALREADY a tombstone. An earlier version of this fix applied the cap
// unconditionally, so updating an already-tombstoned slot to a NEWER, MergeAccepts-approved stamp
// was wrongly refused at the cap, spuriously counted as a drop, AND left the OLDER, weaker
// tombstone stamp resident -- letting a THIRD peer's later write, correctly losing against the
// newer stamp this callback was trying to install, wrongly win against the stale one instead (an
// intermediate resurrection). max_tombstones=0 makes the cap trivially refuse any GROWING install,
// without needing to pre-populate the table with real tombstones to reach it.
TEST_F(RdbMvccTest, MergeLwwTombstoneUpdateForAbsentKeyIgnoresCapWhenNoGrowth) {
  ASSERT_TRUE(IsActiveReplica());
  absl::SetFlag(&FLAGS_multi_master_max_tombstones, 0);

  constexpr uint64_t kPeerHash = 0xFEDCBA9876543210ULL;
  const MvccStamp kOldTombstone = MvccStamp{0x1000, kPeerHash}.AsTombstone();
  const MvccStamp kNewTombstone = MvccStamp{0x2000, kPeerHash}.AsTombstone();

  // Pre-install an OLDER tombstone directly (SetTombstone has no cap of its own -- the cap lives
  // only at call sites) so "ghost" already occupies a tombstone slot before the merge fallthrough
  // ever consults the cap for it: installing a NEWER stamp over it does not grow mvcc_tombstones.
  shard_set->Await(0, [&] {
    namespaces->GetDefaultNamespace().GetCurrentDbSlice().SetTombstone(0, std::string_view{"ghost"},
                                                                       kOldTombstone);
  });

  std::string body = BuildTombstoneSection(0, {{"ghost", kNewTombstone}});
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "after");
  AppendString(&body, "afterval");

  const std::string rdb = WrapInRdb(body);
  io::BytesSource src{io::Buffer(rdb)};
  RdbLoadContext load_context;
  auto ec = pp_->at(0)->Await([&]() -> std::error_code {
    RdbLoader loader(service_.get(), &load_context);
    loader.SetMergeLww(true, kPeerHash);
    return loader.Load(&src);
  });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_EQ(Run({"get", "after"}), "afterval") << "bytes after the section must still parse";

  std::optional<MvccStamp> got;
  shard_set->Await(0, [&] {
    got =
        namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, std::string_view{"ghost"});
  });
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(*got, kNewTombstone)
      << "updating an ALREADY-tombstoned slot does not grow mvcc_tombstones, so the "
         "per-(database, shard) cap must not block it even at max_tombstones=0 -- a stale stamp "
         "here would let a later, intermediate resurrection win";
}

// drakeydb: P4-3 final fix wave (I-1) -- the NON-merge tombstone install (HandleTombstones'
// fallthrough for a loader that never called SetMergeLww) honoured --multi_master_tombstone_ttl
// but NOT --multi_master_max_tombstones, unlike its merge twin above and unlike
// PerformDeletionAtomic itself (db_slice.cc). That path is the ORDINARY restart-from-own-RDB one:
// a node whose file carries more tombstones than the cap currently allows would restore all of
// them and keep them (TombstoneGcStep reaps on age, never down to the cap).
//
// cap = 1, two tombstones for keys this node does not hold: the first install grows
// mvcc_tombstones from 0 to 1 and is under the cap; the second would grow it to 2 and must be
// refused and counted. No SetMergeLww call anywhere -- this is the plain loader.
TEST_F(RdbMvccTest, WithoutMergeLwwTombstoneInstallHonorsCapForAbsentKeys) {
  ASSERT_TRUE(IsActiveReplica());
  absl::SetFlag(&FLAGS_multi_master_max_tombstones, 1);

  constexpr uint64_t kPeerHash = 0xFEDCBA9876543210ULL;
  const MvccStamp kFirst = MvccStamp{0x1000, kPeerHash}.AsTombstone();
  const MvccStamp kSecond = MvccStamp{0x2000, kPeerHash}.AsTombstone();

  size_t dropped_before = 0, tombstones_before = 0;
  shard_set->Await(0, [&] {
    auto* stats = namespaces->GetDefaultNamespace().GetCurrentDbSlice().MutableStats(0);
    dropped_before = stats->mvcc_tombstones_dropped;
    tombstones_before = stats->mvcc_tombstones;
  });
  ASSERT_EQ(tombstones_before, 0u) << "this fixture must start with an empty tombstone table";

  // Section order is the file's order, and this fixture pins a single shard, so "ghost1" is
  // installed first and "ghost2" is the one that hits the cap.
  std::string body = BuildTombstoneSection(0, {{"ghost1", kFirst}, {"ghost2", kSecond}});
  body.push_back(RDB_TYPE_STRING);
  AppendString(&body, "after");
  AppendString(&body, "afterval");

  // No SetMergeLww: merge_lww_ stays false, so this exercises the non-merge install path.
  auto ec = pp_->at(0)->Await([&] { return LoadRdbData(service_.get(), WrapInRdb(body)); });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_EQ(Run({"get", "after"}), "afterval") << "bytes after the section must still parse";

  std::optional<MvccStamp> first, second;
  size_t dropped_after = 0, tombstones_after = 0, mismatches = 0;
  shard_set->Await(0, [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    first = db_slice.GetMvcc(0, std::string_view{"ghost1"});
    second = db_slice.GetMvcc(0, std::string_view{"ghost2"});
    auto* stats = db_slice.MutableStats(0);
    dropped_after = stats->mvcc_tombstones_dropped;
    tombstones_after = stats->mvcc_tombstones;
    mismatches = db_slice.TEST_VerifyMvccTable(0);
  });

  ASSERT_TRUE(first.has_value()) << "the first tombstone is under the cap and must install";
  EXPECT_EQ(*first, kFirst);
  EXPECT_FALSE(second.has_value())
      << "the second would push this (database, shard) pair over --multi_master_max_tombstones "
         "and must be refused, exactly as the merge twin and a live delete both do";
  EXPECT_EQ(tombstones_after, 1u);
  EXPECT_EQ(dropped_after, dropped_before + 1)
      << "the at-cap drop must be counted in mvcc_tombstones_dropped (DEBUG MVCC / INFO), not "
         "silently discarded";
  EXPECT_EQ(mismatches, 0u) << "dense invariant must hold";
}

}  // namespace dfly
