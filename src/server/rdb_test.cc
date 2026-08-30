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

#include <absl/flags/reflection.h>
#include <mimalloc.h>

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
// -- and, being non-empty, rdb_save.cc's `!mvcc.IsZero()` gate re-emits it as a real
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

  size_t captured_db_count() const {
    return db_array_.size();
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

  size_t captured_dbs = 0;
  MvccStamp captured_branch{}, live_branch{}, out_of_range{}, unregistered{};
  pp_->at(0)->Await([&] {
    EngineShard* shard = EngineShard::tlocal();
    DbSlice& db_slice = namespaces->GetDefaultNamespace().GetDbSlice(shard->shard_id());

    probe->TraverseCaptured();
    captured_dbs = probe->captured_db_count();

    PrimeKey pk{"k0"};
    const PrimeTable* captured_pt = probe->captured_prime(0);
    const PrimeTable* live_pt = &db_slice.GetDBTable(0)->prime;
    ASSERT_NE(captured_pt, live_pt) << "the flush must have replaced the table";

    captured_branch = probe->MvccOf(0, pk, captured_pt);
    live_branch = probe->MvccOf(0, pk, live_pt);
    // db_index is only a fast-path hint now: with a db index beyond the captured array
    // (DbSlice::ActivateDb can grow the live one after a snapshot starts) the owner still
    // resolves, and it must never be an out-of-bounds read.
    out_of_range = probe->MvccOf(static_cast<DbIndex>(captured_dbs + 5), pk, captured_pt);

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
  EXPECT_EQ(live_branch, kPostFlush)
      << "a bucket owned by the live table must resolve against the live table -- pinning both "
         "tables onto one fixed choice mis-stamps the other flow instead";
  EXPECT_EQ(out_of_range, pre[0])
      << "db_index is a fast-path hint only: an out-of-range index must still resolve by owner, "
         "and must never be an out-of-bounds read";
  EXPECT_TRUE(unregistered.Empty()) << "an unknown owner must be unstamped, not a deref";
}

// drakeydb: P4-2, final review round 2 (Critical) -- the third route, which no single-consumer
// test can see. SerializerBase::ProcessBucket's TRAVERSAL flow calls
// DbSlice::FlushChangeToEarlierCallbacks with an iterator into its OWN captured table, and that
// dispatches cb->OnChange(db_ind, ChangeReq{...}) to every EARLIER-registered consumer.
// ChangeReq is a PrimeTable::BucketSet carrying only `owner_` (dash.h), and DbSlice::Iterator's
// LaunderIfNeeded re-finds through `it_.owner()` (db_slice.h), so those buckets stay in the LATER
// consumer's captured table while the EARLIER consumer serializes them. An earlier consumer that
// resolved stamps by "am I in the OnChange flow?" would read the LIVE table for buckets that live
// in neither its own captured table nor the live one -- stripping stamps to {0,0} and, for a key
// re-created after the flush, fabricating a pre-flush-value/post-flush-stamp pair.
//
// Reachable in production whenever two consumers are registered at once (a BGSAVE via
// save_stages_controller.cc plus a replica full sync via dflycmd.cc, or two replicas full-syncing)
// and a FLUSHALL lands mid-save. FlushChangeToEarlierCallbacks short-circuits on
// `cb->snapshot_version_ == upper_bound`, which is why one consumer alone never triggers it.
//
// Deterministic here: A registers, B registers, FLUSHALL, then B traverses. No fibers race; the
// dispatch to A happens synchronously inside B's ProcessBucket.
TEST_F(RdbMvccTest, EarlierConsumerStampsForeignBucketsFromTheOwningTable) {
  ASSERT_TRUE(IsActiveReplica());

  constexpr int kKeys = 8;
  for (int i = 0; i < kKeys; ++i)
    ASSERT_EQ(Run({"set", StrCat("k", i), StrCat("v", i)}), "OK");

  std::vector<MvccStamp> pre(kKeys);
  for (int i = 0; i < kKeys; ++i)
    pre[i] = MvccStamp{0x2000ULL + i, 0xCCCC0000ULL + i};
  shard_set->Await(0, [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    for (int i = 0; i < kKeys; ++i)
      db_slice.SetMvcc(0, std::string_view{StrCat("k", i)}, pre[i]);
  });

  ExecutionState cntx;
  std::optional<CapturedTableProbe> earlier, later;

  pp_->at(0)->Await([&] {
    EngineShard* shard = EngineShard::tlocal();
    DbSlice& db_slice = namespaces->GetDefaultNamespace().GetDbSlice(shard->shard_id());
    earlier.emplace(&db_slice, &cntx);
    later.emplace(&db_slice, &cntx);
    shard->shard_lock()->Acquire(IntentLock::EXCLUSIVE);
    // Registration order is what makes `earlier` the target of FlushChangeToEarlierCallbacks:
    // DbSlice::RegisterOnChange appends to change_cb_ and hands out an increasing
    // snapshot_version_. Both capture the SAME table here -- the situation the reviewer hit.
    earlier->RegisterChangeListener(/*replication=*/false);
    later->RegisterChangeListener(/*replication=*/false);
    shard->shard_lock()->Release(IntentLock::EXCLUSIVE);
  });

  ASSERT_EQ(Run({"flushall"}), "OK");

  const MvccStamp kPostFlush{0x7777ULL, 0xDDDD0000ULL};
  ASSERT_EQ(Run({"set", "k0", "NEWVAL"}), "OK");
  shard_set->Await(0, [&] {
    namespaces->GetDefaultNamespace().GetCurrentDbSlice().SetMvcc(0, std::string_view{"k0"},
                                                                  kPostFlush);
  });

  pp_->at(0)->Await([&] {
    // `later`'s traversal walks the captured table and, per bucket, hands that same bucket to
    // `earlier` through FlushChangeToEarlierCallbacks before stamping its own version on it.
    later->TraverseCaptured();
    earlier->UnregisterChangeListener();
    later->UnregisterChangeListener();
  });

  auto from_earlier = earlier->emitted;
  auto from_later = later->emitted;
  pp_->at(0)->Await([&] {
    earlier.reset();
    later.reset();
  });

  ASSERT_EQ(from_earlier.size(), size_t(kKeys))
      << "guard against a vacuous pass: the earlier consumer must actually have been dispatched "
         "the later consumer's captured buckets (FlushChangeToEarlierCallbacks)";
  ASSERT_EQ(from_later.size(), size_t(kKeys));

  for (int i = 0; i < kKeys; ++i) {
    const std::string key = StrCat("k", i);
    ASSERT_TRUE(from_earlier.contains(key)) << key << " was not serialized by the earlier consumer";
    EXPECT_EQ(from_earlier[key].value, StrCat("v", i));
    EXPECT_EQ(from_earlier[key].mvcc, pre[i])
        << key
        << ": the earlier consumer received a bucket owned by the LATER consumer's "
           "captured table; its stamp must come from that same table. {0,0} here is the "
           "stamp-loss half of the bug; anything else is fabrication.";
    EXPECT_EQ(from_later[key].mvcc, pre[i]) << key
                                            << ": the traversing consumer must be correct "
                                               "too";
  }
  EXPECT_NE(from_earlier["k0"].mvcc, kPostFlush)
      << "k0 was serialized with its PRE-flush value but its POST-flush stamp -- authority no "
         "node ever issued for that value, bit-identical to a peer's genuine newer write";
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

}  // namespace dfly
