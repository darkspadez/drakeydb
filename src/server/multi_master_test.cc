// Copyright 2026, drakeydb authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "server/multi_master.h"

#include <absl/cleanup/cleanup.h>
#include <absl/container/flat_hash_set.h>
#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <vector>

#include "absl/flags/declare.h"
#include "absl/flags/flag.h"
#include "absl/flags/reflection.h"
#include "base/gtest.h"
#include "facade/facade_test.h"
#include "facade/reply_builder.h"
#include "io/file_util.h"
#include "server/dflycmd.h"
#include "server/engine_shard_set.h"
#include "server/journal/executor.h"
#include "server/journal/journal.h"
#include "server/journal/serializer.h"
#include "server/journal/types.h"
#include "server/node_identity.h"
#include "server/server_family.h"
#include "server/snapshot.h"
#include "server/test_utils.h"
#include "util/fibers/fibers.h"
#include "util/fibers/pool.h"

ABSL_DECLARE_FLAG(bool, force_epoll);
ABSL_DECLARE_FLAG(std::string, dir);
ABSL_DECLARE_FLAG(bool, active_replica);
ABSL_DECLARE_FLAG(bool, multi_master);
ABSL_DECLARE_FLAG(std::string, cluster_mode);
ABSL_DECLARE_FLAG(std::string, tiered_prefix);
ABSL_DECLARE_FLAG(bool, experimental_cascaded_partial_sync);
ABSL_DECLARE_FLAG(uint32_t, num_shards);
ABSL_DECLARE_FLAG(uint32_t, reaper_member_walk_budget);

namespace dfly {

namespace {

// Test-only helper: writes `content` verbatim to `path` using std IO (tests may use std IO).
bool WriteStringToFileForTest(const std::string& path, std::string_view content) {
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  out << content;
  return out.good();
}

}  // namespace

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
  ASSERT_EQ(ReplconfUuidReplyStatus::kSuccess,
            ParseReplconfUuidReply("01234567-89ab-4cde-8f01-23456789abcd", &uuid, &ms));
  EXPECT_EQ("01234567-89ab-4cde-8f01-23456789abcd", uuid);
  EXPECT_EQ(0u, ms);  // KeyDB sends no clock
}

TEST(ReplconfUuidReply, ParsesDrakeydbUuidWithMs) {
  std::string uuid;
  uint64_t ms = 0;
  ASSERT_EQ(
      ReplconfUuidReplyStatus::kSuccess,
      ParseReplconfUuidReply("01234567-89AB-4cde-8f01-23456789abcd 1755600000000", &uuid, &ms));
  EXPECT_EQ("01234567-89ab-4cde-8f01-23456789abcd", uuid);  // normalized
  EXPECT_EQ(1755600000000u, ms);
}

TEST(ReplconfUuidReply, UnsupportedOkMustBeExact) {
  std::string uuid = "sentinel";
  uint64_t ms = 4242;
  EXPECT_EQ(ReplconfUuidReplyStatus::kUnsupported, ParseReplconfUuidReply("OK", &uuid, &ms));
  EXPECT_EQ(ReplconfUuidReplyStatus::kMalformed, ParseReplconfUuidReply("ok", &uuid, &ms));
  EXPECT_EQ(ReplconfUuidReplyStatus::kMalformed, ParseReplconfUuidReply("OK ", &uuid, &ms));
  EXPECT_EQ(ReplconfUuidReplyStatus::kMalformed, ParseReplconfUuidReply("OK extra", &uuid, &ms));
  EXPECT_EQ("sentinel", uuid);
  EXPECT_EQ(4242u, ms);
}

TEST(ReplconfUuidReply, MalformedRejectedExtraTokensIgnored) {
  std::string uuid;
  uint64_t ms = 0;
  EXPECT_EQ(ReplconfUuidReplyStatus::kMalformed, ParseReplconfUuidReply("", &uuid, &ms));
  EXPECT_EQ(ReplconfUuidReplyStatus::kMalformed,
            ParseReplconfUuidReply("not-a-uuid 123", &uuid, &ms));
  EXPECT_EQ(ReplconfUuidReplyStatus::kMalformed,
            ParseReplconfUuidReply("01234567-89ab-4cde-8f01-23456789abcd notanum", &uuid, &ms));
  // Forward compat: a third token from a future master is ignored.
  EXPECT_EQ(ReplconfUuidReplyStatus::kSuccess,
            ParseReplconfUuidReply("01234567-89ab-4cde-8f01-23456789abcd 5 future", &uuid, &ms));
  EXPECT_EQ(5u, ms);
}

TEST(ReplconfUuidReply, FailureLeavesOutParamsUntouched) {
  std::string uuid = "sentinel";
  uint64_t ms = 4242;
  EXPECT_EQ(ReplconfUuidReplyStatus::kMalformed,
            ParseReplconfUuidReply("01234567-89ab-4cde-8f01-23456789abcd notanum", &uuid, &ms));
  EXPECT_EQ("sentinel", uuid);
  EXPECT_EQ(4242u, ms);
  // Overflow used to write UINT64_MAX through the out-param before returning false.
  EXPECT_EQ(ReplconfUuidReplyStatus::kMalformed,
            ParseReplconfUuidReply("01234567-89ab-4cde-8f01-23456789abcd 99999999999999999999999",
                                   &uuid, &ms));
  EXPECT_EQ("sentinel", uuid);
  EXPECT_EQ(4242u, ms);
}

TEST(NodeIdentityFile, CreatesAndPersists) {
  std::string dir = base::GetTestTempPath("nid_create");
  std::filesystem::remove_all(dir);
  auto id1 = LoadOrCreateNodeIdentity(dir, "");
  ASSERT_TRUE(id1);
  EXPECT_FALSE(id1->ephemeral);
  EXPECT_TRUE(IsValidNodeUuid(id1->uuid));
  auto content = io::ReadFileToString(dir + "/drakeydb.uuid");
  ASSERT_TRUE(content);
  EXPECT_EQ(absl::StrCat(id1->uuid, "\n"), *content);
  size_t temp_count = 0;
  for (const auto& entry : std::filesystem::directory_iterator(dir)) {
    temp_count += entry.path().filename().string().starts_with("drakeydb.uuid.tmp.");
  }
  EXPECT_EQ(0u, temp_count);
  auto id2 = LoadOrCreateNodeIdentity(dir, "");  // reread returns the same identity
  ASSERT_TRUE(id2);
  EXPECT_EQ(id1->uuid, id2->uuid);
}

TEST(NodeIdentityFile, CorruptFileFails) {
  std::string dir = base::GetTestTempPath("nid_corrupt");
  std::filesystem::create_directories(dir);
  ASSERT_TRUE(WriteStringToFileForTest(dir + "/drakeydb.uuid", "garbage-not-a-uuid\n"));
  EXPECT_FALSE(LoadOrCreateNodeIdentity(dir, ""));
  ASSERT_TRUE(WriteStringToFileForTest(dir + "/drakeydb.uuid",
                                       "01234567-89ab-4cde-8f01-23456789abc\n"));  // 35 chars
  EXPECT_FALSE(LoadOrCreateNodeIdentity(dir, ""));
}

TEST(NodeIdentityFile, ExistingFileWhitespaceAndMixedCaseNormalized) {
  std::string dir = base::GetTestTempPath("nid_whitespace");
  std::filesystem::create_directories(dir);
  ASSERT_TRUE(WriteStringToFileForTest(dir + "/drakeydb.uuid",
                                       "  01234567-89AB-4CDE-8F01-23456789ABCD \n"));
  auto id = LoadOrCreateNodeIdentity(dir, "");
  ASSERT_TRUE(id);
  EXPECT_FALSE(id->ephemeral);
  EXPECT_EQ("01234567-89ab-4cde-8f01-23456789abcd", id->uuid);
}

TEST(NodeIdentityFile, CloudDirIsEphemeral) {
  auto id = LoadOrCreateNodeIdentity("s3://bucket/prefix", "");
  ASSERT_TRUE(id);
  EXPECT_TRUE(id->ephemeral);
  EXPECT_TRUE(IsValidNodeUuid(id->uuid));
}

TEST(NodeIdentityFile, UnwritableDirIsEphemeral) {
  if (geteuid() == 0)
    GTEST_SKIP() << "root ignores permission bits";
  std::string dir = base::GetTestTempPath("nid_ro");
  std::filesystem::create_directories(dir);
  chmod(dir.c_str(), 0500);
  auto id = LoadOrCreateNodeIdentity(dir, "");
  chmod(dir.c_str(), 0700);  // restore so the temp tree can be cleaned
  ASSERT_TRUE(id);
  EXPECT_TRUE(id->ephemeral);
}

TEST(NodeIdentityFile, OverrideIsEphemeralAndNeverPersisted) {
  std::string dir = base::GetTestTempPath("nid_override");
  std::filesystem::remove_all(dir);
  auto id = LoadOrCreateNodeIdentity(dir, "01234567-89AB-4cde-8f01-23456789abcd");
  ASSERT_TRUE(id);
  EXPECT_TRUE(id->ephemeral);
  EXPECT_EQ("01234567-89ab-4cde-8f01-23456789abcd", id->uuid);  // normalized
  EXPECT_FALSE(std::filesystem::exists(dir + "/drakeydb.uuid"));
  EXPECT_FALSE(LoadOrCreateNodeIdentity(dir, "nonsense"));  // invalid override -> error
}

TEST(NodeIdentityFile, CreatesMissingDir) {
  std::string dir = base::GetTestTempPath("nid_missing_dir");
  std::filesystem::remove_all(dir);
  std::string sub = dir + "/a/b";
  auto id = LoadOrCreateNodeIdentity(sub, "");
  ASSERT_TRUE(id);
  EXPECT_FALSE(id->ephemeral);
  EXPECT_TRUE(std::filesystem::exists(sub + "/drakeydb.uuid"));
}

TEST(NodeIdentityFile, EmptyDirPersistsIntoCwd) {
  namespace fs = std::filesystem;
  std::string dir = base::GetTestTempPath("nid_cwd");
  fs::remove_all(dir);
  fs::create_directories(dir);
  fs::path saved_cwd = fs::current_path();
  fs::current_path(dir);
  auto id = LoadOrCreateNodeIdentity("", "");
  bool file_exists = fs::exists("drakeydb.uuid");
  fs::current_path(saved_cwd);  // restore BEFORE asserting
  ASSERT_TRUE(id);
  EXPECT_FALSE(id->ephemeral);
  EXPECT_TRUE(file_exists);
}

TEST(PeerRegistry, SelfIsZeroAndAddOrGetIsMonotonicIdempotent) {
  PeerRegistry reg;
  reg.Init("01234567-89ab-4cde-8f01-000000000000");
  EXPECT_EQ(PeerRegistry::kSelfIdx, reg.FindIdx("01234567-89ab-4cde-8f01-000000000000"));
  EXPECT_EQ("01234567-89ab-4cde-8f01-000000000000", reg.GetUuid(0));
  EXPECT_EQ(1u, reg.Size());
  uint32_t a = reg.AddOrGet("01234567-89ab-4cde-8f01-000000000001");
  uint32_t b = reg.AddOrGet("01234567-89ab-4cde-8f01-000000000002");
  EXPECT_EQ(1u, a);
  EXPECT_EQ(2u, b);
  EXPECT_EQ(a, reg.AddOrGet("01234567-89ab-4cde-8f01-000000000001"));  // idempotent
  EXPECT_EQ("01234567-89ab-4cde-8f01-000000000002", reg.GetUuid(b));
  EXPECT_EQ(std::nullopt, reg.FindIdx("01234567-89ab-4cde-8f01-0000000000ff"));
  EXPECT_EQ("", reg.GetUuid(99));
  EXPECT_EQ(3u, reg.Size());
}

TEST(MultiMasterFlags, DefaultsAreValidAndOff) {
  absl::FlagSaver saver;
  EXPECT_FALSE(IsActiveReplica());
  EXPECT_FALSE(IsMultiMaster());
  EXPECT_TRUE(ValidateMultiMasterFlags());
}

TEST(MultiMasterFlags, MultiMasterRequiresActiveReplica) {
  absl::FlagSaver saver;
  absl::SetFlag(&FLAGS_multi_master, true);
  EXPECT_FALSE(ValidateMultiMasterFlags());
  absl::SetFlag(&FLAGS_active_replica, true);
  EXPECT_TRUE(ValidateMultiMasterFlags());
  EXPECT_TRUE(IsActiveReplica());
  EXPECT_TRUE(IsMultiMaster());
}

TEST(MultiMasterFlags, ActiveReplicaRejectsIncompatibleFlags) {
  absl::FlagSaver saver;
  absl::SetFlag(&FLAGS_active_replica, true);
  absl::SetFlag(&FLAGS_cluster_mode, "emulated");
  EXPECT_FALSE(ValidateMultiMasterFlags());
  absl::SetFlag(&FLAGS_cluster_mode, "");
  absl::SetFlag(&FLAGS_tiered_prefix, "/tmp/x");
  EXPECT_FALSE(ValidateMultiMasterFlags());
  absl::SetFlag(&FLAGS_tiered_prefix, "");
  absl::SetFlag(&FLAGS_experimental_cascaded_partial_sync, true);
  EXPECT_FALSE(ValidateMultiMasterFlags());
  absl::SetFlag(&FLAGS_experimental_cascaded_partial_sync, false);
  EXPECT_TRUE(ValidateMultiMasterFlags());
}

namespace {
nonstd::expected<PeerReplicaOfCmd, facade::ErrorReply> ParsePeer(std::vector<std::string> words) {
  CmdArgVec vec;
  for (auto& w : words)
    vec.emplace_back(w);
  CmdArgList list = absl::MakeSpan(vec);
  return ParsePeerReplicaOfArgs(list);
}
}  // namespace

TEST(PeerReplicaOfArgs, ParsesAddRemoveAndNoOne) {
  auto add = ParsePeer({"localhost", "6379"});
  ASSERT_TRUE(add.has_value());
  EXPECT_EQ(PeerReplicaOfCmd::Kind::kAdd, add->kind);
  EXPECT_EQ("localhost", add->host);
  EXPECT_EQ(6379, add->port);

  auto rem = ParsePeer({"REMOVE", "10.0.0.7", "7000"});
  ASSERT_TRUE(rem.has_value());
  EXPECT_EQ(PeerReplicaOfCmd::Kind::kRemove, rem->kind);
  EXPECT_EQ("10.0.0.7", rem->host);
  EXPECT_EQ(7000, rem->port);

  auto none = ParsePeer({"NO", "ONE"});
  ASSERT_TRUE(none.has_value());
  EXPECT_EQ(PeerReplicaOfCmd::Kind::kNoOne, none->kind);
  auto none_lc = ParsePeer({"no", "one"});
  ASSERT_TRUE(none_lc.has_value());
}

TEST(PeerReplicaOfArgs, RejectsBadForms) {
  EXPECT_FALSE(ParsePeer({"localhost"}).has_value());
  EXPECT_FALSE(ParsePeer({"localhost", "0"}).has_value());
  EXPECT_FALSE(ParsePeer({"localhost", "70000"}).has_value());
  EXPECT_FALSE(ParsePeer({"localhost", "abc"}).has_value());
  EXPECT_FALSE(ParsePeer({"REMOVE", "localhost"}).has_value());
  EXPECT_FALSE(ParsePeer({"localhost", "6379", "0", "100"}).has_value());  // slot range
  EXPECT_FALSE(ParsePeer({"NO"}).has_value());
}

TEST(PeerReplicationInfo, RendersCountsAndPeerLines) {
  ReplicaSummary up{};
  up.host = "localhost";
  up.port = 7001;
  up.master_link_established = true;
  up.full_sync_in_progress = false;
  up.master_last_io_sec = 3;
  up.master_node_uuid = "01234567-89ab-4cde-8f01-23456789abcd";
  up.clock_skew_ms = 42;
  ReplicaSummary down{};
  down.host = "10.0.0.9";
  down.port = 7002;
  down.master_link_established = false;
  down.full_sync_in_progress = true;
  down.master_last_io_sec = 0;
  down.clock_skew_ms = -17;
  std::string s = RenderPeerReplicationInfo({up, down}, true, true);
  EXPECT_EQ(
      "active_replica:1\r\nmulti_master:1\r\nconnected_masters:2\r\n"
      "master0:host=localhost,port=7001,link_status=up,last_io_seconds_ago=3,"
      "sync_in_progress=0,node_uuid=01234567-89ab-4cde-8f01-23456789abcd,"
      "clock_skew_ms=42\r\n"
      "master1:host=10.0.0.9,port=7002,link_status=down,last_io_seconds_ago=0,"
      "sync_in_progress=1,clock_skew_ms=-17\r\n",
      s);
  EXPECT_EQ("active_replica:1\r\nmulti_master:0\r\nconnected_masters:2\r\n",
            RenderPeerReplicationInfo({up, down}, false, false));
  EXPECT_EQ("active_replica:1\r\nmulti_master:0\r\nconnected_masters:0\r\n",
            RenderPeerReplicationInfo({}, false, true));
}

TEST(ClockSkew, ComputesSignedSkewAndThreshold) {
  // Peer's clock ahead of ours -> positive skew.
  EXPECT_EQ(ComputeClockSkewMs(/* local_ms= */ 1'000, /* peer_ms= */ 1'500), 500);
  // Peer behind -> negative.
  EXPECT_EQ(ComputeClockSkewMs(/* local_ms= */ 1'500, /* peer_ms= */ 1'000), -500);
  // Absent peer clock (a pre-exchange master) -> no skew, never a warning.
  EXPECT_EQ(ComputeClockSkewMs(/* local_ms= */ 1'500, /* peer_ms= */ 0), 0);

  EXPECT_FALSE(IsClockSkewConcerning(0));
  EXPECT_FALSE(IsClockSkewConcerning(-kClockSkewWarnMs + 1));
  EXPECT_FALSE(IsClockSkewConcerning(kClockSkewWarnMs - 1));
  EXPECT_TRUE(IsClockSkewConcerning(kClockSkewWarnMs));
  EXPECT_TRUE(IsClockSkewConcerning(-kClockSkewWarnMs)) << "skew is concerning in both directions";
}

// Launch::post-constructed fibers only get queued (AddReady) on the constructing thread's
// scheduler; they don't start running until that thread yields (e.g. at Join()). And
// util::fb2::Mutex::lock()'s uncontended fast path never suspends. So fibers built directly on
// the bare gtest thread never actually interleave here: the first Join() runs fiber 0 to
// completion (always uncontended), then fiber 1, etc. -- strictly sequential, not a test of
// concurrent access. A real concurrency test needs fibers on distinct proactor threads, where
// util::fb2::Mutex's base::SpinLock-guarded owner_ can genuinely be contended.
class PeerRegistryFiberTest : public ::testing::Test {
 protected:
  void SetUp() override {
#ifdef __linux__
    if (absl::GetFlag(FLAGS_force_epoll)) {
      pp_.reset(util::fb2::Pool::Epoll(2));
    } else {
      pp_.reset(util::fb2::Pool::IOUring(16, 2));
    }
#else
    pp_.reset(util::fb2::Pool::Epoll(2));
#endif
    pp_->Run();
  }
  void TearDown() override {
    pp_->Stop();
    pp_.reset();
  }
  std::unique_ptr<util::ProactorPool> pp_;
};

TEST_F(PeerRegistryFiberTest, ConcurrentAddersDontDuplicate) {
  PeerRegistry reg;
  reg.Init(GenerateNodeUuid());
  std::vector<std::string> uuids;
  for (int i = 0; i < 16; ++i)
    uuids.push_back(GenerateNodeUuid());
  std::vector<util::fb2::Fiber> fibers;
  for (unsigned f = 0; f < 8; ++f) {
    fibers.emplace_back(pp_->at(f % pp_->size())->LaunchFiber(util::fb2::Launch::post, [&] {
      for (const auto& u : uuids)
        reg.AddOrGet(u);
    }));
  }
  for (auto& fb : fibers)
    fb.Join();
  EXPECT_EQ(1 + uuids.size(), reg.Size());
  for (const auto& u : uuids)
    EXPECT_EQ(u, reg.GetUuid(*reg.FindIdx(u)));  // round-trip: catches index aliasing too.
}

// drakeydb: P3 T9 (e). Same distinct-proactor-threads rationale as PeerRegistryFiberTest above
// (its own comment explains why fibers sharing one thread, or built on the bare gtest thread,
// never actually interleave): LoadOrCreateNodeIdentity's mkstemp/write/fsync/link sequence is
// raw, non-fiber-aware blocking syscalls, so genuinely racing it needs one racer fiber per real
// OS thread.
class NodeIdentityFiberTest : public ::testing::Test {
 protected:
  void SetUp() override {
#ifdef __linux__
    if (absl::GetFlag(FLAGS_force_epoll)) {
      pp_.reset(util::fb2::Pool::Epoll(kRacers));
    } else {
      pp_.reset(util::fb2::Pool::IOUring(16, kRacers));
    }
#else
    pp_.reset(util::fb2::Pool::Epoll(kRacers));
#endif
    pp_->Run();
  }
  void TearDown() override {
    pp_->Stop();
    pp_.reset();
  }

  static constexpr unsigned kRacers = 8;
  std::unique_ptr<util::ProactorPool> pp_;
};

// drakeydb: P3 T9 (e) acceptance case. kRacers processes booting concurrently against the same
// empty --dir must converge on ONE uuid -- matching what actually ends up on disk -- never each
// keep the different uuid it locally generated. Before the fix, PersistUuid published via
// mkstemp()+rename(), which always overwrites: every racer that saw "no such file" on its
// initial read would go on to generate its own uuid and successfully rename() it over whatever
// the last writer left, so racers' in-memory NodeIdentity::uuid values could differ from each
// other and from whatever uuid ultimately ended up on disk -- a restart would then load a uuid
// that does not match the identity this boot ran with.
//
// Falsifying: reverting PersistUuid to mkstemp+rename (no link()/EEXIST/adopt path) makes this
// test fail with racer uuids that diverge from each other and/or from the file on disk --
// observed failure text captured in task-9-report.md.
TEST_F(NodeIdentityFiberTest, ConcurrentBootConvergesOnSameUuid) {
  std::string dir = base::GetTestTempPath("nid_race");
  std::filesystem::remove_all(dir);

  std::vector<io::Result<NodeIdentity>> results(kRacers);
  std::vector<util::fb2::Fiber> fibers;
  for (unsigned i = 0; i < kRacers; ++i) {
    fibers.emplace_back(pp_->at(i % pp_->size())->LaunchFiber(util::fb2::Launch::post, [&, i] {
      results[i] = LoadOrCreateNodeIdentity(dir, "");
    }));
  }
  for (auto& fb : fibers)
    fb.Join();

  for (unsigned i = 0; i < kRacers; ++i) {
    ASSERT_TRUE(results[i]) << "racer " << i;
    EXPECT_FALSE(results[i]->ephemeral) << "racer " << i;
  }
  const std::string expected_uuid = results[0]->uuid;
  ASSERT_TRUE(IsValidNodeUuid(expected_uuid));
  for (unsigned i = 1; i < kRacers; ++i)
    EXPECT_EQ(expected_uuid, results[i]->uuid) << "racer " << i << " diverged from racer 0";

  auto on_disk = io::ReadFileToString(dir + "/drakeydb.uuid");
  ASSERT_TRUE(on_disk);
  EXPECT_EQ(absl::StrCat(expected_uuid, "\n"), *on_disk);
}

// Gives this fixture's boot its own private --dir so its identity file never collides with
// another test's, and restores the global --dir flag on teardown (saver_ is the fixture's only
// data member, so it is constructed -- capturing the pre-test value -- before the constructor
// body below runs, and destroyed, restoring that value, after TearDown()).
class MultiMasterFamilyTest : public BaseFamilyTest {
 protected:
  MultiMasterFamilyTest() {
    absl::SetFlag(&FLAGS_dir, base::GetTestTempPath("mm_family"));
  }

  // Explicit TearDown (not a trailing assignment at the end of each test body) so a frozen
  // TEST_current_time_ms is restored even if a test fails or throws mid-body. gtest runs the
  // whole binary in one process, so a leaked frozen clock would silently corrupt every later
  // BaseFamilyTest case that follows -- same leak shape saver_ already guards against for --dir.
  void TearDown() override {
    TEST_current_time_ms = 0;
    BaseFamilyTest::TearDown();
  }

  absl::FlagSaver saver_;
};

TEST_F(MultiMasterFamilyTest, InfoReplicationHasNodeUuid) {
  auto resp = Run({"info", "replication"});
  std::string info{ToSV(resp.GetBuf())};
  // Must not match inside "master_node_uuid:" (which also contains "node_uuid:" as a substring) --
  // anchor on the preceding newline so this only matches the standalone "node_uuid:" field.
  size_t pos = info.find("\nnode_uuid:");
  ASSERT_NE(std::string::npos, pos);
  std::string uuid = info.substr(pos + 11, 36);
  EXPECT_TRUE(IsValidNodeUuid(uuid)) << uuid;
}

TEST_F(MultiMasterFamilyTest, ReplconfUuidRepliesOwnUuidAndMs) {
  TEST_current_time_ms = 1755600000000;
  auto resp = Run({"replconf", "uuid", "01234567-89AB-4cde-8f01-23456789abcd"});
  std::string reply{ToSV(resp.GetBuf())};
  std::string uuid;
  uint64_t ms = 0;
  ASSERT_EQ(ReplconfUuidReplyStatus::kSuccess, ParseReplconfUuidReply(reply, &uuid, &ms)) << reply;
  EXPECT_TRUE(IsValidNodeUuid(uuid));
  EXPECT_EQ(1755600000000u, ms);
  // Second send with a different uuid also succeeds (idempotent handling).
  auto resp2 = Run({"replconf", "uuid", "01234567-89ab-4cde-8f01-000000000002"});
  ASSERT_EQ(ReplconfUuidReplyStatus::kSuccess,
            ParseReplconfUuidReply(std::string{ToSV(resp2.GetBuf())}, &uuid, &ms));
}

TEST_F(MultiMasterFamilyTest, ReplconfUuidInvalidRejected) {
  auto resp = Run({"replconf", "uuid", "not-a-uuid"});
  EXPECT_THAT(resp, ErrArg("Invalid UUID"));
}

// Boots with --active_replica and --multi_master on, on top of MultiMasterFamilyTest's private
// --dir (base's saver_ restores both flags on TearDown, same as it already does for --dir).
class ActiveReplicaFamilyTest : public MultiMasterFamilyTest {
 protected:
  ActiveReplicaFamilyTest() {
    absl::SetFlag(&FLAGS_active_replica, true);
    absl::SetFlag(&FLAGS_multi_master, true);
  }
};

TEST_F(ActiveReplicaFamilyTest, ReplconfRefusedWhileActive) {
  // drakeydb: P3 T7 moved the admission gate from a blanket top-of-function refusal to the CAPA
  // dragonfly case alone, so earlier handshake steps (like LISTENING-PORT) now succeed
  // unconditionally -- Greet() must be allowed to run to completion so the admission check can
  // see what it sent. Only CAPA dragonfly -- with no DRAKEY-VERSION ever presented on this
  // connection -- is still refused, with the same text the old blanket check used.
  auto resp = Run({"replconf", "listening-port", "1"});
  EXPECT_EQ("OK", resp);
  resp = Run({"replconf", "capa", "dragonfly"});
  EXPECT_THAT(resp, ErrArg("active-replica"));
}

TEST_F(ActiveReplicaFamilyTest, ReplTakeoverRefusedWhileActive) {
  EXPECT_THAT(Run({"repltakeover", "1"}), ErrArg("active-replica"));
}

// drakeydb: P3 T7 -- master-side peer admission. Each test below drives the same handshake
// sequence Greet() uses (REPLCONF UUID / DRAKEY-VERSION / PEER, strictly before CAPA dragonfly)
// directly through Run(), one REPLCONF invocation per pair exactly as the real wire handshake
// does it, and inspects both the final CAPA dragonfly reply -- an array on admission, an error
// otherwise -- and the resulting DflyCmd::ReplicaInfo::IsPeer(), via the public
// GetReplicaInfoSnapshot(), so a peer admission and a plain-replica admission are distinguished
// by more than "some array came back" -- each row asserts the one property that differs between
// them, not just the property they share.
TEST_F(ActiveReplicaFamilyTest, ReplconfAdmitsPeerWithValidForeignUuid) {
  const std::string kForeignUuid = "01234567-89ab-4cde-8f01-23456789abcd";
  Run({"replconf", "uuid", kForeignUuid});
  EXPECT_EQ("OK", Run({"replconf", "drakey-version", absl::StrCat(kDrakeydbReplVersion)}));
  EXPECT_EQ("OK", Run({"replconf", "peer", "1"}));
  auto resp = Run({"replconf", "capa", "dragonfly"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type) << "expected peer admission to succeed: " << resp;
  // Not StrArray(): the reply mixes strings with two integer elements (flow_count, version) --
  // StrArray()'s GetBuf() call on those would throw (wrong std::variant alternative).
  EXPECT_EQ(5u, resp.GetVec().size());

  auto infos = service_->server_family().GetDflyCmd()->GetReplicaInfoSnapshot();
  ASSERT_EQ(1u, infos.size());
  EXPECT_TRUE(infos[0]->IsPeer()) << "PEER 1 was requested; ReplicaInfo must record it as a peer";
  EXPECT_EQ(kForeignUuid, infos[0]->GetNodeUuid());
}

TEST_F(ActiveReplicaFamilyTest, ReplconfAdmitsPlainReplicaWithoutPeerFlag) {
  const std::string kForeignUuid = "01234567-89ab-4cde-8f01-23456789abce";
  Run({"replconf", "uuid", kForeignUuid});
  EXPECT_EQ("OK", Run({"replconf", "drakey-version", absl::StrCat(kDrakeydbReplVersion)}));
  // No REPLCONF PEER sent -- this consumer never asked for peer mode.
  auto resp = Run({"replconf", "capa", "dragonfly"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type) << "expected plain-replica admission to succeed: " << resp;
  // Not StrArray(): the reply mixes strings with two integer elements (flow_count, version) --
  // StrArray()'s GetBuf() call on those would throw (wrong std::variant alternative).
  EXPECT_EQ(5u, resp.GetVec().size());

  auto infos = service_->server_family().GetDflyCmd()->GetReplicaInfoSnapshot();
  ASSERT_EQ(1u, infos.size());
  EXPECT_FALSE(infos[0]->IsPeer()) << "PEER was never sent; ReplicaInfo must not record a peer";
}

TEST_F(ActiveReplicaFamilyTest, ReplconfRefusesPeerWithoutUuid) {
  EXPECT_EQ("OK", Run({"replconf", "drakey-version", absl::StrCat(kDrakeydbReplVersion)}));
  EXPECT_EQ("OK", Run({"replconf", "peer", "1"}));
  // No REPLCONF UUID sent -- PEER 1 alone is not a valid identity.
  auto resp = Run({"replconf", "capa", "dragonfly"});
  EXPECT_THAT(resp, ErrArg("REPLCONF PEER requires a valid REPLCONF UUID identity"));
}

TEST_F(ActiveReplicaFamilyTest, ReplconfAdmitsPlainReplicaWithOwnUuid) {
  // A plain consumer receives a full stream and cannot echo writes back, so UUID identity checks
  // are peer-only. A restored/cloned read-only replica may legitimately present this UUID.
  std::string info{ToSV(Run({"info", "replication"}).GetBuf())};
  size_t pos = info.find("\nnode_uuid:");
  ASSERT_NE(std::string::npos, pos);
  std::string own_uuid = info.substr(pos + 11, 36);

  Run({"replconf", "uuid", own_uuid});
  EXPECT_EQ("OK", Run({"replconf", "drakey-version", absl::StrCat(kDrakeydbReplVersion)}));
  auto resp = Run({"replconf", "capa", "dragonfly"});
  ASSERT_EQ(RespExpr::ARRAY, resp.type) << "expected plain-replica admission to succeed: " << resp;

  auto infos = service_->server_family().GetDflyCmd()->GetReplicaInfoSnapshot();
  ASSERT_EQ(1u, infos.size());
  EXPECT_FALSE(infos[0]->IsPeer());
}

TEST_F(ActiveReplicaFamilyTest, ReplconfRefusesPeerWithOwnUuid) {
  std::string info{ToSV(Run({"info", "replication"}).GetBuf())};
  size_t pos = info.find("\nnode_uuid:");
  ASSERT_NE(std::string::npos, pos);
  std::string own_uuid = info.substr(pos + 11, 36);

  Run({"replconf", "uuid", own_uuid});
  EXPECT_EQ("OK", Run({"replconf", "drakey-version", absl::StrCat(kDrakeydbReplVersion)}));
  EXPECT_EQ("OK", Run({"replconf", "peer", "1"}));
  auto resp = Run({"replconf", "capa", "dragonfly"});
  EXPECT_THAT(resp, ErrArg("Refusing to admit a consumer presenting this node's own uuid"));
}

TEST_F(ActiveReplicaFamilyTest, InfoShowsActiveFieldsAndStaysMaster) {
  auto resp = Run({"info", "replication"});
  std::string info{ToSV(resp.GetBuf())};
  EXPECT_NE(std::string::npos, info.find("role:master\r\n"));
  EXPECT_NE(std::string::npos, info.find("active_replica:1\r\n"));
  EXPECT_NE(std::string::npos, info.find("multi_master:1\r\n"));
  EXPECT_NE(std::string::npos, info.find("connected_masters:0\r\n"));
  EXPECT_EQ(std::string::npos, info.find("master_host:"));
}

TEST_F(ActiveReplicaFamilyTest, ReplicaOfGrammarAndNoPeersPaths) {
  EXPECT_THAT(Run({"replicaof", "remove", "localhost", "1"}), ErrArg("Not attached"));
  EXPECT_EQ("OK", Run({"replicaof", "no", "one"}));
  EXPECT_THAT(Run({"replicaof", "localhost", "6379", "0", "100"}), ErrArg("slot ranges"));
  // Unresolvable peer: error, nothing attached, still a writable master. Use a syntactically
  // invalid hostname so this unit test exercises manager cleanup without depending on DNS or a
  // listening socket; live peer connections are covered by the integration suite.
  EXPECT_THAT(Run({"replicaof", "invalid host", "1"}), ErrArg("replication cancelled"));
  std::string info{ToSV(Run({"info", "replication"}).GetBuf())};
  EXPECT_NE(std::string::npos, info.find("connected_masters:0\r\n"));
  EXPECT_EQ("OK", Run({"set", "k", "v"}));
}

TEST_F(MultiMasterFamilyTest, NonActiveInfoHasNoActiveFields) {
  std::string info{ToSV(Run({"info", "replication"}).GetBuf())};
  EXPECT_EQ(std::string::npos, info.find("active_replica:"));
  EXPECT_EQ(std::string::npos, info.find("connected_masters:"));
}

// drakeydb: P4-1 Task 5 -- the side table on DbTable. Storage only; nothing writes to it outside
// these tests until Task 7. --active_replica is boot-only (DbTable reads it at construction), so
// the flag must be set before BaseFamilyTest::SetUp() runs, not in the constructor/TearDown body.
class MvccStoreTest : public BaseFamilyTest {
 protected:
  void SetUp() override {
    absl::SetFlag(&FLAGS_active_replica, true);
    BaseFamilyTest::SetUp();
    // drakeydb: P4-1 Task 7 -- LogAutoJournalOnShard and every command-specific RecordJournal
    // call site (e.g. SetCmd::RecordJournal) gate on shard->journal() before ever reaching
    // journal::RecordEntry, and the journal is off by default in tests (production only starts
    // it via the DFLY FLOW replica handshake). Without this, RecordEntry -- and so Arm/Commit --
    // never runs for a plain SET/LPUSH, and StampOf() returns nullopt for every write in this
    // fixture. Broadcast to every shard (this fixture does not pin num_shards=1, unlike
    // OriginJournalFamilyTest below).
    shard_set->RunBriefInParallel([](auto*) { journal::StartInThread(); });
  }
  void TearDown() override {
    BaseFamilyTest::TearDown();
    absl::SetFlag(&FLAGS_active_replica, false);
  }

  // drakeydb: P4-1 Task 7 -- shard-hops to read back the stamp arm/commit left (or didn't leave)
  // on a key, the same way production code would via DbSlice::GetMvcc.
  std::optional<MvccStamp> StampOf(std::string_view key) {
    std::optional<MvccStamp> out;
    shard_set->Await(Shard(key, shard_set->size()), [&] {
      out = namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, key);
    });
    return out;
  }
};

TEST_F(MvccStoreTest, SideTableIsAllocatedInActiveMode) {
  Run({"set", "k", "v"});
  EXPECT_GT(GetMetrics().db_stats[0].mvcc_table_bytes, 0u)
      << "active mode must allocate the side table";
}

TEST_F(MvccStoreTest, RoundTripsAStamp) {
  Run({"set", "k", "v"});
  auto& shard_set_ref = *shard_set;
  const MvccStamp written{12345, 999};
  shard_set_ref.Await(Shard("k", shard_set_ref.size()), [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    PrimeKey pk{"k"};
    db_slice.SetMvcc(0, pk, written);
  });

  std::optional<MvccStamp> got;
  shard_set_ref.Await(Shard("k", shard_set_ref.size()), [&] {
    got = namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, "k");
  });
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(*got, written);
}

// drakeydb: P4-1 Task 5, fix round 1 -- EraseMvcc and mvcc_enabled() had no caller anywhere in
// the tree (production or test), so a mutation to either -- e.g. EraseMvcc silently becoming a
// no-op, or mvcc_enabled_ getting stuck false -- would go undetected. This closes both.
TEST_F(MvccStoreTest, EraseRemovesTheStampAndDecrementsCount) {
  Run({"set", "k", "v"});
  auto& shard_set_ref = *shard_set;
  const ShardId sid = Shard("k", shard_set_ref.size());
  const MvccStamp written{777, 111};

  shard_set_ref.Await(sid, [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    ASSERT_TRUE(db_slice.mvcc_enabled());
    PrimeKey pk{"k"};
    db_slice.SetMvcc(0, pk, written);
  });
  ASSERT_EQ(GetMetrics().db_stats[0].mvcc_entries, 1u);

  std::optional<MvccStamp> got;
  shard_set_ref.Await(
      sid, [&] { got = namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, "k"); });
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(*got, written);

  shard_set_ref.Await(sid, [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    PrimeKey pk{"k"};
    db_slice.EraseMvcc(0, pk);
  });

  shard_set_ref.Await(
      sid, [&] { got = namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, "k"); });
  EXPECT_FALSE(got.has_value()) << "EraseMvcc must remove the stamp";
  EXPECT_EQ(GetMetrics().db_stats[0].mvcc_entries, 0u) << "EraseMvcc must decrement mvcc_entries";
}

TEST_F(MvccStoreTest, AbsentKeyReturnsNullopt) {
  std::optional<MvccStamp> got;
  shard_set->Await(Shard("nope", shard_set->size()), [&] {
    got = namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, "nope");
  });
  EXPECT_FALSE(got.has_value());
}

TEST_F(MvccStoreTest, FlushAllDropsTheSideTable) {
  Run({"set", "k", "v"});
  shard_set->Await(Shard("k", shard_set->size()), [&] {
    PrimeKey pk{"k"};
    namespaces->GetDefaultNamespace().GetCurrentDbSlice().SetMvcc(0, pk, MvccStamp{1, 1});
  });
  // Falsification note (P4-1 Task 5): the brief's original body asserted only the post-flush
  // count, which stays 0 whether or not SetMvcc ever wrote anything -- vacuous against a no-op
  // SetMvcc. This pre-flush check closes that gap.
  ASSERT_EQ(GetMetrics().db_stats[0].mvcc_entries, 1u);
  Run({"flushall"});
  EXPECT_EQ(GetMetrics().db_stats[0].mvcc_entries, 0u);
  // Fix round 1: the counter alone doesn't pin the table itself -- FlushDbIndexes replaces the
  // whole DbTable, which resets DbTableStats (and so mvcc_entries) regardless of whether the side
  // table was actually dropped. Check the table directly too.
  std::optional<MvccStamp> got;
  shard_set->Await(Shard("k", shard_set->size()), [&] {
    got = namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, "k");
  });
  EXPECT_FALSE(got.has_value()) << "the side table itself, not just its counter, must be dropped";
}

// drakeydb: P4-1 Task 5, fix round 1 -- DbSlice::mvcc_table_memory() (unlike table_memory_, which
// takes a live delta on every write, e.g. AddOrFind's `table_memory_ += table_increase;` at
// db_slice.cc:946) is computed on demand from DbTable::mvcc_table_memory() rather than
// accumulated, precisely so it cannot silently stop tracking real growth or underflow on flush.
// This is the test that would have caught the original accumulator design being wrong: it
// requires the value to actually move.
TEST_F(MvccStoreTest, TableMemoryGrowsWithWritesAndDropsOnFlush) {
  Run({"set", "k", "v"});
  auto& shard_set_ref = *shard_set;
  const ShardId sid = Shard("k", shard_set_ref.size());

  size_t before = 0;
  shard_set_ref.Await(sid, [&] {
    before = namespaces->GetDefaultNamespace().GetCurrentDbSlice().mvcc_table_memory();
  });

  // Enough distinct keys to force the side table's DashTable past its initial single-segment
  // capacity (kMaxSize = (56 + 4) * 14 = 840 slots) and actually grow, not just accept a write
  // that fits in already-allocated space.
  shard_set_ref.Await(sid, [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    for (int i = 0; i < 2000; ++i) {
      PrimeKey pk{absl::StrCat("mvcc-mem-", i)};
      db_slice.SetMvcc(0, pk, MvccStamp{uint64_t(i) + 1, 1});
    }
  });

  size_t after = 0;
  shard_set_ref.Await(sid, [&] {
    after = namespaces->GetDefaultNamespace().GetCurrentDbSlice().mvcc_table_memory();
  });
  EXPECT_GT(after, before) << "mvcc_table_memory() must grow as the side table fills";

  Run({"flushall"});

  size_t post_flush = 0;
  shard_set_ref.Await(sid, [&] {
    post_flush = namespaces->GetDefaultNamespace().GetCurrentDbSlice().mvcc_table_memory();
  });
  EXPECT_EQ(post_flush, before)
      << "flush must drop the grown side table back to a freshly-allocated table's baseline";
}

// drakeydb: P4-1 Task 7 -- the landmine test. LPUSH is auto-journaled AND mutates in place, so it
// never calls AddOrUpdate. It is the command that fails if EndOfWriteEpoch is placed at
// transaction.cc's OnCbFinishBlocking call instead of after LogAutoJournalOnShard -- see the
// landmine section of task-7-brief.md and this task's report for the falsification run.
TEST_F(MvccStoreTest, AutoJournaledCommandStampsKey) {
  Run({"lpush", "mylist", "a"});
  auto st = StampOf("mylist");
  ASSERT_TRUE(st.has_value()) << "auto-journaled write left no stamp -- check that "
                                 "EndOfWriteEpoch runs AFTER LogAutoJournalOnShard";
  EXPECT_GT(st->Mvcc(), 0u);
  EXPECT_NE(st->origin_hash, 0u) << "self origin hash must be seeded at boot";
}

TEST_F(MvccStoreTest, NoAutoJournalCommandStampsKey) {
  Run({"set", "k", "v"});  // SET is NO_AUTOJOURNAL and journals via SetCmd::RecordJournal
  ASSERT_TRUE(StampOf("k").has_value());
}

// drakeydb: review fix round 1 (minor) -- hardened against a regression that drops stamping
// entirely: StampOf's optional is checked before every dereference now, so such a regression
// fails cleanly here instead of reading an uninitialized MvccStamp (undefined behavior, and the
// exact shape of the bug this task's own initial run of this fixture hit before the journal was
// started -- see this task's report).
TEST_F(MvccStoreTest, StampsAreStrictlyIncreasingAcrossWrites) {
  Run({"set", "k", "v1"});
  auto first_stamp = StampOf("k");
  ASSERT_TRUE(first_stamp.has_value());
  const uint64_t first = first_stamp->Mvcc();
  for (int i = 0; i < 50; ++i) {
    Run({"set", "k", absl::StrCat("v", i)});
    auto cur_stamp = StampOf("k");
    ASSERT_TRUE(cur_stamp.has_value()) << "iteration " << i;
    const uint64_t cur = cur_stamp->Mvcc();
    EXPECT_GT(cur, first) << "iteration " << i;
  }
}

TEST_F(MvccStoreTest, MultiKeySameShardCommandSharesOneStamp) {
  // drakeydb: falsification note -- the brief's comment here claimed "the single-shard test
  // fixture"; MvccStoreTest does not pin num_shards (BaseFamilyTest's default is num_threads_ - 1
  // = 2 shards here). "k1"/"k2" simply hash to the same one of those shards, verified empirically
  // (this test passes reliably run in isolation, and MvccStoreTest's TEST_F's are declared before
  // any fixture in this file that changes FLAGS_num_shards, so no earlier test can perturb it).
  Run({"mset", "k1", "v1", "k2", "v2"});
  auto a = StampOf("k1");
  auto b = StampOf("k2");
  ASSERT_TRUE(a.has_value() && b.has_value());
  EXPECT_EQ(a->Mvcc(), b->Mvcc()) << "one shard callback mints one stamp";
}

// The over-stamp cases. Each must leave the stamp untouched and bump the counter.
TEST_F(MvccStoreTest, ReadOnlyGetExDoesNotStampKey) {
  Run({"set", "k", "v"});
  auto before_stamp = StampOf("k");
  ASSERT_TRUE(before_stamp.has_value());
  const MvccStamp before = *before_stamp;
  Run({"getex", "k"});  // no expiry option -> mutates nothing, journals nothing
  auto after_stamp = StampOf("k");
  ASSERT_TRUE(after_stamp.has_value());
  EXPECT_EQ(*after_stamp, before)
      << "a read that runs an AutoUpdater must not advance the stamp -- if it does, this node "
         "silently rejects peer writes that should have won";
}

TEST_F(MvccStoreTest, SkippedExpireDoesNotStampKey) {
  Run({"set", "k", "v"});
  auto before_stamp = StampOf("k");
  ASSERT_TRUE(before_stamp.has_value());
  const MvccStamp before = *before_stamp;
  EXPECT_EQ(0, CheckedInt({"expire", "k", "100", "XX"}));  // no TTL set -> predicate fails
  auto after_stamp = StampOf("k");
  ASSERT_TRUE(after_stamp.has_value());
  EXPECT_EQ(*after_stamp, before);
}

// drakeydb: P4-1 Task 7, Step 6 -- IsOmittableWrite's redundant-write optimisation arms but never
// commits (no journal entry is emitted for the omitted write), so under active mode it must be
// disabled outright or the key ends up permanently unstamped.
//
// Falsification note: the brief's original body (`debug populate` then a plain `set`) never
// registers a change consumer, so IsOmittableWrite's `change_cb_.size() == 1` branch never
// engages and omission never actually happens -- confirmed by temporarily disabling this task's
// `if (mvcc_enabled_) return false;` gate in IsOmittableWrite (db_slice.cc) and re-running: the
// test still passed. Strengthened to genuinely trigger the omission path IsOmittableWrite
// targets, mirroring the existing RegisterOnChange-under-shard_lock pattern used by
// ReaperJournalFamilyTest.ReaperDeleteBypassesChangeCallbacks above: a real eventually-consistent
// change consumer plus a real journal consumer, registered before "during" (a fresh key, so its
// bucket version predates the registration) is ever written.
TEST_F(MvccStoreTest, WritesDuringSnapshotAreStillStamped) {
  class NoopChangeConsumer final : public DbSlice::ChangeConsumerInterface {
   public:
    void OnChange(DbIndex, const ChangeReq&) override {
    }
  } change_consumer;
  change_consumer.eventually_consistent_ = true;  // the flavor of snapshot IsOmittableWrite gates

  class NoopJournalConsumer final : public journal::JournalConsumerInterface {
   public:
    void ConsumeJournalChange(const journal::JournalChangeItem&) override {
    }
    void ThrottleIfNeeded() override {
    }
  } journal_consumer;
  uint32_t journal_consumer_id = 0;

  const ShardId sid = Shard("during", shard_set->size());
  shard_set->Await(sid, [&] {
    EngineShard* shard = EngineShard::tlocal();
    DbSlice& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    journal_consumer_id = journal::RegisterConsumer(&journal_consumer);

    // RegisterOnChange DCHECKs the shard's intent lock is held (see #7153) and stamps
    // snapshot_version_ = NextVersion() -- higher than any bucket untouched since this call,
    // "during"'s included, since it has not been written yet.
    shard->shard_lock()->Acquire(IntentLock::EXCLUSIVE);
    db_slice.RegisterOnChange(&change_consumer);
    shard->shard_lock()->Release(IntentLock::EXCLUSIVE);
  });

  // A write to a key whose bucket predates the registration above, with exactly one
  // eventually-consistent change consumer and one journal consumer registered: precisely the
  // shape IsOmittableWrite requires before it will omit the journal write.
  Run({"set", "during", "v"});

  shard_set->Await(sid, [&] {
    DbSlice& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    EXPECT_TRUE(db_slice.UnregisterOnChange(&change_consumer));
    journal::UnregisterConsumer(journal_consumer_id);
  });

  ASSERT_TRUE(StampOf("during").has_value())
      << "a journal-omitted write must still be stamped, or the key is permanently unstamped";
}

// drakeydb: P4-1 Task 7, Step 10 -- a pure write workload (no reads-that-mutate, no
// not-satisfied-predicate skips) must never discard an armed key: every arm this workload creates
// must reach a commit. mvcc_unstamped_writes only counts arms EndOfWriteEpoch finds still armed,
// so this is read through a shard hop rather than via INFO (that wiring is Task 11's job).
TEST_F(MvccStoreTest, PureWriteWorkloadLeavesNoUnstampedWrites) {
  for (int i = 0; i < 200; ++i)
    Run({"set", absl::StrCat("k", i), "v"});

  // Summed across every shard, not just shard 0: 200 distinct keys spread across every shard
  // this fixture runs (BaseFamilyTest's default is more than one), and a bug could plausibly
  // manifest on only one of them.
  std::atomic<uint64_t> unstamped{0};
  shard_set->pool()->AwaitBrief(
      [&](unsigned, auto*) { unstamped += MvccStamper::tlocal()->stats().unstamped_writes; });
  EXPECT_EQ(unstamped.load(), 0u) << "a pure write workload must never discard an arm";
}

// drakeydb: review fix round 1 (F1) -- a write through a non-default namespace must leave no
// stamp anywhere: not in its own namespace's side table (never propagated to any peer, so under
// "a key's stamp advances iff that same stamp is propagated" it must never advance), and not as a
// phantom stamp on a same-named key in the DEFAULT namespace's side table either --
// journal::RecordEntry's commit callback always targets GetDefaultNamespace() (armed_ carries no
// namespace, see DbSlice::PostUpdate's comment), so an unguarded arm from a non-default-namespace
// write would stamp that unrelated, never-written default-namespace key instead.
//
// In production, ns1 is reached via ServerFamily::DoAuth (server_family.cc:2109-2120), which sets
// `cntx->ns = &namespaces->GetOrInsert(cred.ns)` for an ACL user carrying a NAMESPACE: directive
// (acl_family.cc's MaybeParseNamespace). That chain is exercised here via RunViaNamespace, not a
// real ACL SETUSER+AUTH round trip: BaseFamilyTest::Run(id, slice) (test_utils.cc) unconditionally
// resets the dispatching connection's `context->ns` back to the default namespace before every
// single command, discovered the hard way while first writing this test with AUTH -- an ACL
// NAMESPACE:-scoped SET still landed (and got legitimately stamped) in the default namespace,
// which momentarily looked exactly like an F1 regression until DIAG prints traced it to Run()'s
// own reset, confirmed by checking the stored ACL credential directly
// (ServerState::tlocal()->user_registry->GetCredentials("nsuser").ns == "NS1", i.e. the ACL layer
// itself was never the problem). RunViaNamespace (test_utils.h/.cc) mirrors Run(id, slice)'s
// dispatch with the namespace as a parameter instead of hardcoded, added specifically to route
// around that reset -- it is NOT verbatim (see its own declaration comment in test_utils.h for the
// two omissions, both harmless for this test's plain SET/GET calls) -- it still goes through the
// real Transaction/PostUpdate/LogAutoJournalOnShard path via DispatchCommand, only the mechanism
// for reaching a non-default `cntx->ns` differs from production's ACL path (the ACL layer's own
// correctness -- MaybeParseNamespace, DoAuth -- is pre-existing, unchanged by this task, and
// independently confirmed working above).
//
// Falsification: temporarily changing PostUpdate's `ns_ == &namespaces->GetDefaultNamespace()`
// gate to unconditionally arm (the pre-fix-round-1 behavior) makes the LAST assertion below
// (`EXPECT_EQ(*after, *original)`) fail -- that is what exercises the shipped gate, since
// journal.cc's commit callback is only ever reachable with the default namespace's DbSlice
// (`namespaces->GetDefaultNamespace().GetCurrentDbSlice()`, unconditional), so an unguarded arm
// stamps the phantom key there, which this assertion catches. The earlier
// `EXPECT_FALSE(ns1_stamp.has_value())` assertion does NOT fail under that same mutation and does
// not exercise the shipped gate at all: SetMvcc is never called against ns1's own DbSlice by any
// code path in this binary, gate present or not, so ns1_stamp is always empty regardless of what
// PostUpdate does. It is not vacuous -- ns1's DbTable really does allocate an mvcc table
// (table.cc's constructor gates on the global IsActiveReplica(), not on namespace) -- and it does
// guard a plausible alternative fix this task did not take (routing the commit back to whichever
// namespace originally armed the key, instead of always targeting the default namespace). See
// this task's report for the verbatim falsification run.
TEST_F(MvccStoreTest, NonDefaultNamespaceWriteLeavesNoStampAnywhere) {
  // A key of the same name, already stamped in the default namespace, to prove the ns1 write
  // below does not perturb it.
  ASSERT_EQ(Run({"set", "shared", "v0"}), "OK");
  auto original = StampOf("shared");
  ASSERT_TRUE(original.has_value());

  Namespace& ns1 = namespaces->GetOrInsert("ns1");
  ASSERT_EQ(RunViaNamespace(&ns1, {"set", "shared", "v1"}), "OK");

  const ShardId sid = Shard("shared", shard_set->size());
  std::optional<MvccStamp> ns1_stamp;
  shard_set->Await(sid, [&] { ns1_stamp = ns1.GetDbSlice(sid).GetMvcc(0, "shared"); });
  EXPECT_FALSE(ns1_stamp.has_value())
      << "a non-default-namespace write must never be stamped -- it is never propagated to any "
         "peer, so under the phase invariant it must never advance a stamp either";

  ASSERT_EQ(RunViaNamespace(&ns1, {"get", "shared"}), "v1")
      << "sanity: the write must have actually landed in ns1's own table, or an unstamped ns1 "
         "key proves nothing";

  auto after = StampOf("shared");
  ASSERT_TRUE(after.has_value());
  EXPECT_EQ(*after, *original)
      << "the ns1 write must not perturb the DEFAULT namespace's same-named key -- "
         "journal::RecordEntry's commit callback always targets GetDefaultNamespace(), so an "
         "unguarded arm would stamp this unrelated, never-written key instead";
}

// The "off means byte-identical to upstream" guard.
TEST_F(BaseFamilyTest, NonActiveModeAllocatesNoMvccTable) {
  Run({"set", "k", "v"});
  EXPECT_EQ(GetMetrics().db_stats[0].mvcc_table_bytes, 0u)
      << "a non-active node must pay nothing for MVCC";
}

namespace {
// drakeydb: Phase 3 T3 -- captures the origin_idx of every COMMAND journal entry seen on the
// shard this consumer is registered on, via JournalSlice::AddLogRecord -> CallOnChange (see
// journal_slice.cc). That's populated straight from Entry::origin_idx regardless of wire
// framing, so this works without enabling active-replica/extended framing.
class OriginCapturingConsumer : public journal::JournalConsumerInterface {
 public:
  void ConsumeJournalChange(const journal::JournalChangeItem& item) override {
    origins.push_back(item.journal_item.origin_idx);
  }
  void ThrottleIfNeeded() override {
  }

  std::vector<uint32_t> origins;
};
}  // namespace

// Pins the test to a single shard on a single thread so a journal consumer registered on shard 0
// is guaranteed to observe every command this fixture runs, regardless of key hashing. saver_
// (inherited from MultiMasterFamilyTest) restores FLAGS_num_shards on teardown.
class OriginJournalFamilyTest : public MultiMasterFamilyTest {
 protected:
  OriginJournalFamilyTest() {
    num_threads_ = 1;
    absl::SetFlag(&FLAGS_num_shards, 1);
  }
};

// drakeydb: Phase 3 T3 acceptance case. Proves the apply-origin plumbing end to end: a command
// dispatched through a JournalExecutor with SetApplyOrigin(k) must produce a journal entry
// carrying origin_idx == k, while a normal client-issued command must still produce kSelfIdx.
// Falsifying: reverting the PrepareTransaction hook, Transaction::SetReplOrigin/
// LogJournalOnShard/RecordEntry threading, or JournalExecutor::SetApplyOrigin makes every
// entry -- including the peer-applied one -- come back as kSelfIdx, failing the second check.
TEST_F(OriginJournalFamilyTest, ApplyOriginTagsJournalEntries) {
  OriginCapturingConsumer consumer;
  uint32_t consumer_id = 0;
  pp_->at(0)
      ->LaunchFiber([&] {
        journal::StartInThread();
        consumer_id = journal::RegisterConsumer(&consumer);
      })
      .Join();

  // A normal client-issued command journals under kSelfIdx (self).
  EXPECT_EQ("OK", Run({"set", "client-key", "v1"}));
  ASSERT_FALSE(consumer.origins.empty());
  EXPECT_EQ(PeerRegistry::kSelfIdx, consumer.origins.back());

  // A command applied through a JournalExecutor with SetApplyOrigin(k) journals under k, as if
  // it were being replicated in from peer `k` (real peer wiring is task T6; this only proves the
  // plumbing carries the value through). Constructed and driven on a regular fiber on shard 0's
  // own proactor thread, matching how every production caller (rdb_load.cc,
  // incoming_slot_migration.cc) always uses JournalExecutor from a fiber already running on a
  // shard/proactor thread: Execute() calls into Service::DispatchCommand, which needs
  // ServerState::tlocal() to resolve on the calling thread. Deliberately NOT shard_set->Await --
  // that runs the callback directly on shard 0's own TxQueue-processing fiber, which self-
  // deadlocks the moment a dispatched command needs that same queue to schedule a hop (see
  // SquashedStubInheritsParentOrigin below, which hit exactly that).
  constexpr uint32_t kPeerOrigin = 7;
  facade::DispatchResult dispatch_result = facade::DispatchResult::ERROR;
  pp_->at(0)
      ->LaunchFiber([&] {
        JournalExecutor executor(service_.get());
        executor.SetApplyOrigin(kPeerOrigin);

        journal::ParsedEntry::CmdData cmd_data;
        std::vector<std::string> parts{"SET", "peer-key", "v2"};
        cmd_data.Assign(parts.begin(), parts.end(), parts.size());
        dispatch_result = executor.Execute(0, cmd_data);
      })
      .Join();
  EXPECT_EQ(facade::DispatchResult::OK, dispatch_result);
  ASSERT_FALSE(consumer.origins.empty());
  EXPECT_EQ(kPeerOrigin, consumer.origins.back());

  pp_->at(0)->LaunchFiber([&] { journal::UnregisterConsumer(consumer_id); }).Join();
}

// drakeydb: Phase 3 fix-round-1 -- the first acceptance test above never squashes, so it never
// exercises Transaction's parent/shard_id/slot_id constructor (transaction.cc), which is what
// makes a SQUASHED_STUB inherit its parent's apply-origin. This test forces exactly that with a
// real MULTI/EXEC (two transactional writes, no eval/global command, so DeduceExecMode picks
// LOCK_AHEAD): EXEC's default multi_exec_squash=true path builds one SQUASHED_STUB Transaction
// per shard via multi_command_squasher.cc's atomic branch (`new Transaction{cntx_->transaction,
// sid, nullopt}`, the same ctor the single-shard-EVAL fast path also uses), and its squashed
// commands run through SquashedHopCb -> Service::InvokeCmd directly -- NOT through
// PrepareTransaction again. That distinction matters: an EVAL script's redis.call *does* go
// back through DispatchCommand/PrepareTransaction for each call (verified empirically -- an
// EVAL-based version of this test kept passing even with the ctor's inheritance lines deleted,
// because PrepareTransaction's hook alone was silently covering for it), so only a path that
// bypasses PrepareTransaction -- like this one -- actually isolates the ctor's contribution.
// Falsifying: deleting the two inheritance lines at the end of that constructor makes the stub
// default to kSelfIdx, so both LPUSHes come back origin 0 instead of kPeerOrigin.
TEST_F(OriginJournalFamilyTest, SquashedStubInheritsParentOrigin) {
  OriginCapturingConsumer consumer;
  uint32_t consumer_id = 0;
  pp_->at(0)
      ->LaunchFiber([&] {
        journal::StartInThread();
        consumer_id = journal::RegisterConsumer(&consumer);
      })
      .Join();

  // Instrument LPUSH to directly confirm each squashed invocation actually ran on a
  // SQUASHED_STUB transaction (same technique as MultiTest.SquashedCallbackBadAlloc in
  // multi_test.cc) -- without this, a broken squash setup could silently fall back to running
  // each command on the (already origin-correct via PrepareTransaction) top-level EXEC
  // transaction, and this test would pass vacuously. A fresh Service/CommandRegistry is created
  // per test via ResetService(), so this handler replacement doesn't leak across tests.
  static std::atomic<int> stub_hits{0};
  static std::atomic<int> total_hits{0};
  stub_hits = 0;
  total_hits = 0;
  // Real command handlers detect stub-ness (and trigger auto-journal, via RunCallback wrapping
  // the shard callback below) by scheduling a hop, not by reading cmd_cntx->tx() directly -- see
  // MultiTest.SquashedCallbackBadAlloc in multi_test.cc for the same technique.
  auto handler = [](facade::CmdArgParser, CommandContext* cmd_cntx) {
    auto cb = [](Transaction* t, EngineShard*) -> OpResult<long> {
      total_hits.fetch_add(1);
      if (t->IsSquashedStub())
        stub_hits.fetch_add(1);
      return 1L;
    };
    OpResult<long> res = cmd_cntx->tx()->ScheduleSingleHopT(cb);
    auto* rb = cmd_cntx->rb();
    if (res)
      rb->SendLong(*res);
    else
      rb->SendError(res.status());
  };
  std::move(*service_->mutable_registry()->Find("LPUSH")).SetHandler(handler);

  constexpr uint32_t kPeerOrigin = 9;
  facade::DispatchResult multi_res = facade::DispatchResult::ERROR;
  facade::DispatchResult a_res = facade::DispatchResult::ERROR;
  facade::DispatchResult b_res = facade::DispatchResult::ERROR;
  facade::DispatchResult exec_res = facade::DispatchResult::ERROR;
  // Run on a regular fiber on shard 0's proactor thread (not shard_set->Await, which runs
  // directly on shard 0's own TxQueue-processing fiber and would self-deadlock the moment EXEC
  // needs that same queue to schedule the squashed hop). A plain fiber mirrors how a real
  // connection dispatches.
  pp_->at(0)
      ->LaunchFiber([&] {
        JournalExecutor executor(service_.get());
        executor.SetApplyOrigin(kPeerOrigin);

        auto dispatch = [&](std::vector<std::string> parts) {
          journal::ParsedEntry::CmdData cmd_data;
          cmd_data.Assign(parts.begin(), parts.end(), parts.size());
          return executor.Execute(0, cmd_data);
        };
        multi_res = dispatch({"MULTI"});
        a_res = dispatch({"LPUSH", "squash-a", "v1"});
        b_res = dispatch({"LPUSH", "squash-b", "v2"});
        exec_res = dispatch({"EXEC"});
      })
      .Join();
  EXPECT_EQ(facade::DispatchResult::OK, multi_res);
  EXPECT_EQ(facade::DispatchResult::OK, a_res);
  EXPECT_EQ(facade::DispatchResult::OK, b_res);
  EXPECT_EQ(facade::DispatchResult::OK, exec_res);

  // Guard against a vacuous pass: both LPUSHes must have actually executed on squashed stubs.
  EXPECT_EQ(2, total_hits.load());
  EXPECT_EQ(2, stub_hits.load());

  ASSERT_GE(consumer.origins.size(), 2u);
  for (uint32_t origin : consumer.origins) {
    EXPECT_EQ(kPeerOrigin, origin);
  }

  pp_->at(0)->LaunchFiber([&] { journal::UnregisterConsumer(consumer_id); }).Join();
}

namespace {
// drakeydb: Phase 3 T4 -- like OriginCapturingConsumer above, but also captures entry_flags and
// the command name, so a test can isolate the "DEL" entries among a mix of setup commands
// instead of assuming a DEL is the only or the last entry seen.
struct CapturedEntry {
  std::string cmd;
  uint32_t origin_idx;
  uint8_t entry_flags;
};

class OriginFlagCapturingConsumer : public journal::JournalConsumerInterface {
 public:
  void ConsumeJournalChange(const journal::JournalChangeItem& item) override {
    entries.push_back(
        {std::string(item.cmd), item.journal_item.origin_idx, item.journal_item.entry_flags});
  }
  void ThrottleIfNeeded() override {
  }

  std::vector<CapturedEntry> entries;
};

// Returns the last captured entry with cmd == "DEL", or nullptr if none was seen.
const CapturedEntry* LastDel(const std::vector<CapturedEntry>& entries) {
  for (auto it = entries.rbegin(); it != entries.rend(); ++it) {
    if (it->cmd == "DEL")
      return &*it;
  }
  return nullptr;
}
}  // namespace

// drakeydb: Phase 3 T4 acceptance case. A plain user-issued DEL is an ordinary auto-journaled
// command (Transaction::LogJournalOnShard) and must carry neither the expiry flag nor a
// non-self origin. A key that outlives its TTL and is then accessed is instead deleted via
// db_slice.cc's ExpireIfNeeded -> RecordExpiryBlocking (tx_base.h/.cc) -- a completely different
// path from the user DEL above -- and must carry journal::kEntryFlagExpired with origin
// kSelfIdx: an expiry is always a local decision, never the replay of a peer's command.
// Falsifying: dropping the entry_flags plumbing in RecordExpiryBlocking (tx_base.cc) makes the
// second DEL come back with entry_flags == 0, indistinguishable from the first.
TEST_F(OriginJournalFamilyTest, ExpiredKeyDelCarriesExpiryFlagUserDelDoesNot) {
  OriginFlagCapturingConsumer consumer;
  uint32_t consumer_id = 0;
  pp_->at(0)
      ->LaunchFiber([&] {
        journal::StartInThread();
        consumer_id = journal::RegisterConsumer(&consumer);
      })
      .Join();

  Run({"set", "plain-key", "v"});
  Run({"del", "plain-key"});
  EXPECT_EQ(Run({"exists", "plain-key"}).GetInt(), 0);

  Run({"set", "expiring-key", "v", "PX", "1"});
  AdvanceTime(2);
  Run({"get", "expiring-key"});  // triggers lazy ExpireIfNeeded on access.
  EXPECT_EQ(Run({"exists", "expiring-key"}).GetInt(), 0);

  pp_->at(0)->LaunchFiber([&] { journal::UnregisterConsumer(consumer_id); }).Join();

  std::vector<CapturedEntry> dels;
  for (const auto& e : consumer.entries) {
    if (e.cmd == "DEL")
      dels.push_back(e);
  }
  ASSERT_EQ(2u, dels.size());

  EXPECT_EQ(0, dels[0].entry_flags);
  EXPECT_EQ(PeerRegistry::kSelfIdx, dels[0].origin_idx);

  EXPECT_TRUE(dels[1].entry_flags & journal::kEntryFlagExpired);
  EXPECT_EQ(PeerRegistry::kSelfIdx, dels[1].origin_idx);
}

// drakeydb: Phase 3 T4 acceptance case. A DEL derived from a collection command emptying its key
// -- here, HTTL discovering a hash field's TTL has lazily expired, leaving the hash empty, via
// HSetFamily::DeleteIfEmpty (hset_family.cc) -- must inherit the causing transaction's origin
// instead of always being attributed to this node. Falsifying: reverting
// DbContext::repl_origin_idx (tx_base.h), Transaction::GetDbContext's propagation of it
// (transaction.h), or the DeleteIfEmpty call site's switch to the DbContext-aware RecordDelete
// overload (hset_family.cc) makes the derived DEL come back kSelfIdx even though the causing
// HTTL ran under kPeerOrigin.
TEST_F(OriginJournalFamilyTest, DerivedDeleteInheritsCausingTransactionOrigin) {
  OriginFlagCapturingConsumer consumer;
  uint32_t consumer_id = 0;
  pp_->at(0)
      ->LaunchFiber([&] {
        journal::StartInThread();
        consumer_id = journal::RegisterConsumer(&consumer);
      })
      .Join();

  // Set up a hash with one field carrying a short TTL, as an ordinary self-originated client
  // command -- the setup's own origin is irrelevant to what this test checks.
  EXPECT_EQ(Run({"hset", "h", "f", "v"}).GetInt(), 1);
  Run({"hexpire", "h", "1", "FIELDS", "1", "f"});
  AdvanceTime(1100);

  // Dispatch HTTL -- which lazily discovers the field expired and, finding the hash now empty,
  // calls HSetFamily::DeleteIfEmpty -- through a JournalExecutor tagged as peer kPeerOrigin, as
  // if this were a peer's own read triggering its own lazy cleanup (real peer wiring is T6;
  // like the T3 tests above, this only proves the plumbing carries the value through).
  // Dispatched via a plain fiber on shard 0's own proactor thread, not shard_set->Await, which
  // runs directly on shard 0's own TxQueue-processing fiber and would self-deadlock the moment
  // HTTL's ScheduleSingleHopT needs that same queue to schedule its hop (see
  // SquashedStubInheritsParentOrigin above, which hit exactly that).
  constexpr uint32_t kPeerOrigin = 5;
  facade::DispatchResult dispatch_result = facade::DispatchResult::ERROR;
  pp_->at(0)
      ->LaunchFiber([&] {
        JournalExecutor executor(service_.get());
        executor.SetApplyOrigin(kPeerOrigin);

        journal::ParsedEntry::CmdData cmd_data;
        std::vector<std::string> parts{"HTTL", "h", "FIELDS", "1", "f"};
        cmd_data.Assign(parts.begin(), parts.end(), parts.size());
        dispatch_result = executor.Execute(0, cmd_data);
      })
      .Join();
  EXPECT_EQ(facade::DispatchResult::OK, dispatch_result);

  pp_->at(0)->LaunchFiber([&] { journal::UnregisterConsumer(consumer_id); }).Join();

  // Guard against a vacuous pass: the hash must have actually been cleaned up.
  EXPECT_EQ(Run({"exists", "h"}).GetInt(), 0);

  const CapturedEntry* del = LastDel(consumer.entries);
  ASSERT_NE(nullptr, del);
  EXPECT_EQ(kPeerOrigin, del->origin_idx);
  // drakeydb: P4-0 -- HSetFamily::DeleteIfEmpty now records this DEL via RecordDerivedDelete,
  // which sets kEntryFlagDerived (see journal/types.h) so PassesPeerEchoFilter keeps it off
  // mesh-peer links; it is still not an expiry DEL (that flag is reserved for the whole-key
  // TTL path, RecordExpiryBlocking).
  EXPECT_TRUE(del->entry_flags & journal::kEntryFlagDerived);
  EXPECT_FALSE(del->entry_flags & journal::kEntryFlagExpired);
}

// HMapWrap's generic read wrapper has a separate empty-hash deletion path from
// HSetFamily::DeleteIfEmpty. It must carry the same derived flag now that the reaper guarantees
// independent convergence in every namespace and DB.
TEST_F(OriginJournalFamilyTest, HMapWrapDeleteCarriesDerivedFlag) {
  OriginFlagCapturingConsumer consumer;
  uint32_t consumer_id = 0;
  pp_->at(0)
      ->LaunchFiber([&] {
        journal::StartInThread();
        consumer_id = journal::RegisterConsumer(&consumer);
      })
      .Join();

  EXPECT_EQ(Run({"hset", "hmap-wrap", "field", "value"}).GetInt(), 1);
  Run({"hexpire", "hmap-wrap", "1", "FIELDS", "1", "field"});
  AdvanceTime(1100);

  consumer.entries.clear();
  EXPECT_THAT(Run({"hget", "hmap-wrap", "field"}), ArgType(RespExpr::NIL));

  pp_->at(0)->LaunchFiber([&] { journal::UnregisterConsumer(consumer_id); }).Join();

  EXPECT_EQ(Run({"exists", "hmap-wrap"}).GetInt(), 0);
  const CapturedEntry* del = LastDel(consumer.entries);
  ASSERT_NE(del, nullptr);
  EXPECT_TRUE(del->entry_flags & journal::kEntryFlagDerived);
}

// HSETEX FNX/FXX is the HMapWrap exception: a peer with a lagging member-expiry clock can choose
// the opposite conditional branch. Its explicit DEL must therefore reach the peer before the
// verbatim HSETEX entry instead of relying on eventual reaping to repair a skipped conditional.
TEST_F(OriginJournalFamilyTest, HSetExConditionalDeleteStaysForwarded) {
  OriginFlagCapturingConsumer consumer;
  uint32_t consumer_id = 0;
  pp_->at(0)
      ->LaunchFiber([&] {
        journal::StartInThread();
        consumer_id = journal::RegisterConsumer(&consumer);
      })
      .Join();

  EXPECT_EQ(Run({"hset", "hsetex-condition", "field", "old"}).GetInt(), 1);
  Run({"hexpire", "hsetex-condition", "1", "FIELDS", "1", "field"});
  AdvanceTime(1100);

  consumer.entries.clear();
  EXPECT_THAT(Run({"hsetex", "hsetex-condition", "FNX", "FIELDS", "1", "field", "new"}), IntArg(1));

  pp_->at(0)->LaunchFiber([&] { journal::UnregisterConsumer(consumer_id); }).Join();

  EXPECT_EQ(Run({"hget", "hsetex-condition", "field"}), "new");
  const CapturedEntry* del = LastDel(consumer.entries);
  ASSERT_NE(del, nullptr);
  EXPECT_FALSE(del->entry_flags & journal::kEntryFlagDerived);
  auto del_it = std::ranges::find_if(consumer.entries,
                                     [](const CapturedEntry& entry) { return entry.cmd == "DEL"; });
  auto hsetex_it = std::ranges::find_if(
      consumer.entries, [](const CapturedEntry& entry) { return entry.cmd == "HSETEX"; });
  ASSERT_NE(hsetex_it, consumer.entries.end());
  EXPECT_LT(del_it, hsetex_it);
}

// drakeydb: P4-0 acceptance case. A DEL derived from a collection command emptying its key must
// carry kEntryFlagDerived so PassesPeerEchoFilter (journal/types.cc) keeps it off mesh-peer
// links -- see docs/PLAN.md's Phase 4 section for why every one of the ~25 DeleteIfEmpty/
// DeleteSetIfEmpty call sites (hset_family.cc/set_family.cc/generic_family.cc/zset_family.cc/
// debugcmd.cc/search/doc_accessors.cc) is safe to suppress this way, and for the two exceptions.
//
// Deliberately triggered via FIELDEXPIRE/FIELDTTL, not SREM: SREM's own OpRem (set_family.cc)
// deletes an emptied set directly (db_slice.Del) and journals only "SREM" -- it never calls
// SetFamily::DeleteSetIfEmpty, so it cannot exercise this code path at all. FIELDTTL, on the
// other hand, lazily discovers the member-TTL'd field already expired (via
// SetFamily::FieldExpireTime -> GetExpiry) and, finding the set now empty, calls
// SetFamily::DeleteSetIfEmpty (set_family.cc) -- a real, shipped read path (the same one HTTL
// exercises for hashes in DerivedDeleteInheritsCausingTransactionOrigin above).
//
// Falsifying: reverting the RecordDerivedDelete switch at set_family.cc's DeleteSetIfEmpty makes
// the derived DEL come back with entry_flags == 0 -- verified by hand during development.
TEST_F(OriginJournalFamilyTest, EmptiedCollectionDeleteCarriesDerivedFlag) {
  OriginFlagCapturingConsumer consumer;
  uint32_t consumer_id = 0;
  pp_->at(0)
      ->LaunchFiber([&] {
        journal::StartInThread();
        consumer_id = journal::RegisterConsumer(&consumer);
      })
      .Join();

  // A set with one member carrying a short TTL, set up as ordinary self-originated client
  // commands. FIELDTTL below lazily discovers the member expired and, finding the set now
  // empty, calls SetFamily::DeleteSetIfEmpty.
  EXPECT_EQ(Run({"sadd", "s", "m"}).GetInt(), 1);
  Run({"fieldexpire", "s", "1", "m"});
  AdvanceTime(1100);
  Run({"fieldttl", "s", "m"});

  pp_->at(0)->LaunchFiber([&] { journal::UnregisterConsumer(consumer_id); }).Join();

  // Guard against a vacuous pass: the set must have actually been cleaned up.
  EXPECT_EQ(Run({"exists", "s"}).GetInt(), 0);

  const CapturedEntry* del = LastDel(consumer.entries);
  ASSERT_NE(nullptr, del);
  EXPECT_TRUE(del->entry_flags & journal::kEntryFlagDerived)
      << "derived DEL must be flagged or the peer filter forwards it";
  EXPECT_FALSE(del->entry_flags & journal::kEntryFlagExpired)
      << "a FIELDTTL-caused empty is not a whole-key expiry";
}

// drakeydb: P4-0 fix-round-1 -- the counterpart to the acceptance case above: OpFieldExpire
// (generic_family.cc) passes derived=false to DeleteSetIfEmpty/DeleteIfEmpty, because unlike
// FIELDTTL (or any of the other 23 call sites), its own replay on a peer is clock-dependent -- a
// lagging peer can *arm* an already-expired member's new TTL instead of also discovering it
// expired, so key_deleted stays false there and the partial-expiry compensation just above that
// call site never fires either (every field "succeeded"). Nothing on that peer converges it
// without the forwarded, non-suppressed DEL this test pins. Same helper
// (SetFamily::DeleteSetIfEmpty), a different caller, a different flag -- this test and the one
// above are the point.
//
// Falsifying: reverting either `false` argument at generic_family.cc's OpFieldExpire call sites
// back to the default makes this DEL come back flagged kEntryFlagDerived -- verified by hand
// during development.
TEST_F(OriginJournalFamilyTest, FieldExpireCausedDeleteIsNotFlaggedDerived) {
  OriginFlagCapturingConsumer consumer;
  uint32_t consumer_id = 0;
  pp_->at(0)
      ->LaunchFiber([&] {
        journal::StartInThread();
        consumer_id = journal::RegisterConsumer(&consumer);
      })
      .Join();

  // A set with one member whose TTL has already elapsed. Re-probing it via FIELDEXPIRE (not
  // FIELDTTL) causes SetFieldsExpireTime to discover it lazily expired and flush it while trying
  // to re-arm it -- the set empties -> SetFamily::DeleteSetIfEmpty derives a DEL, this time
  // through the derived=false path.
  EXPECT_EQ(Run({"sadd", "feset", "m"}).GetInt(), 1);
  Run({"fieldexpire", "feset", "1", "m"});
  AdvanceTime(1100);
  Run({"fieldexpire", "feset", "1", "m"});

  pp_->at(0)->LaunchFiber([&] { journal::UnregisterConsumer(consumer_id); }).Join();

  // Guard against a vacuous pass: the set must have actually been cleaned up.
  EXPECT_EQ(Run({"exists", "feset"}).GetInt(), 0);

  const CapturedEntry* del = LastDel(consumer.entries);
  ASSERT_NE(nullptr, del);
  EXPECT_FALSE(del->entry_flags & journal::kEntryFlagDerived)
      << "FIELDEXPIRE's own derived DEL must reach peers -- its replay is clock-dependent";
  EXPECT_FALSE(del->entry_flags & journal::kEntryFlagExpired)
      << "a FIELDEXPIRE-caused empty is not a whole-key expiry";
}

// drakeydb: P4-0 fix-wave -- SORT is the same defect class as FIELDEXPIRE above, caught by an
// adversarial review pass: SORT (CO::JOURNALED, no NO_AUTOJOURNAL, generic_family.cc) auto-
// journals verbatim just like FIELDEXPIRE, so OpFetchContainerElements/OpFetchSortEntries'
// derived DEL must also reach peers -- same hazard, same fix (WillAutoJournalVerbatim,
// generic_family.cc, keyed off the transaction's own CommandId, not a hardcoded name). SORT_RO
// shares those exact call sites but is CO::READONLY and never auto-journals, so it must keep the
// suppressed default -- this is the "cannot be a literal false" requirement the predicate exists
// for. One consumer registration spans both halves; LastDel isolates each half's own DEL because
// the two halves use disjoint keys run strictly in sequence.
//
// Falsifying: hardcoding WillAutoJournalVerbatim to always return false (or reverting either
// SORT call site's `!WillAutoJournalVerbatim(...)` back to the derived=true default) makes
// SORT's DEL come back flagged kEntryFlagDerived -- verified by hand during development.
TEST_F(OriginJournalFamilyTest, SortDerivedDeleteReachesPeersButSortRoStaysSuppressed) {
  OriginFlagCapturingConsumer consumer;
  uint32_t consumer_id = 0;
  pp_->at(0)
      ->LaunchFiber([&] {
        journal::StartInThread();
        consumer_id = journal::RegisterConsumer(&consumer);
      })
      .Join();

  // A set with one member whose TTL has already elapsed. "SORT ... BY nosort STORE" forces
  // OpFetchContainerElements to run (the fetch_unsorted branch), which lazily discovers "m"
  // expired and, finding the set now empty, calls SetFamily::DeleteSetIfEmpty through SORT's own
  // auto-journaling path.
  EXPECT_EQ(Run({"sadd", "sort-s", "m"}).GetInt(), 1);
  Run({"fieldexpire", "sort-s", "1", "m"});
  AdvanceTime(1100);
  Run({"sort", "sort-s", "by", "nosort", "store", "sort-dest"});

  EXPECT_EQ(Run({"exists", "sort-s"}).GetInt(), 0);
  const CapturedEntry* sort_del = LastDel(consumer.entries);
  ASSERT_NE(nullptr, sort_del);
  EXPECT_FALSE(sort_del->entry_flags & journal::kEntryFlagDerived)
      << "SORT auto-journals verbatim, so its derived DEL must reach peers";

  // Same call sites, SORT_RO this time (a fresh key -- the SORT above already deleted sort-s):
  // SORT_RO never auto-journals, so it must keep the suppressed default.
  EXPECT_EQ(Run({"sadd", "sortro-s", "m"}).GetInt(), 1);
  Run({"fieldexpire", "sortro-s", "1", "m"});
  AdvanceTime(1100);
  Run({"sort_ro", "sortro-s", "by", "nosort"});

  EXPECT_EQ(Run({"exists", "sortro-s"}).GetInt(), 0);
  const CapturedEntry* sortro_del = LastDel(consumer.entries);
  ASSERT_NE(nullptr, sortro_del);
  EXPECT_TRUE(sortro_del->entry_flags & journal::kEntryFlagDerived)
      << "SORT_RO never auto-journals, so its derived DEL must stay suppressed";

  pp_->at(0)->LaunchFiber([&] { journal::UnregisterConsumer(consumer_id); }).Join();
}

// Partial lazy expiry must be journaled as SREM before SORT itself. Otherwise a peer whose clock
// has not expired the same member computes a different STORE destination when replaying SORT.
// Exercise both source-fetch implementations: the ordinary sorted path and BY nosort.
TEST_F(OriginJournalFamilyTest, SortPartialExpiryJournalsSourceEffectBeforeDestinationEffect) {
  OriginFlagCapturingConsumer consumer;
  uint32_t consumer_id = 0;
  pp_->at(0)
      ->LaunchFiber([&] {
        journal::StartInThread();
        consumer_id = journal::RegisterConsumer(&consumer);
      })
      .Join();

  auto exercise = [&](string_view key, string_view dest, bool no_sort) {
    EXPECT_EQ(Run({"sadd", key, "1", "2"}).GetInt(), 2);
    Run({"fieldexpire", key, "1", "1"});
    AdvanceTime(1100);

    consumer.entries.clear();
    if (no_sort)
      Run({"sort", key, "by", "nosort", "store", dest});
    else
      Run({"sort", key, "store", dest});

    EXPECT_EQ(Run({"scard", key}).GetInt(), 1);
    EXPECT_EQ(Run({"sismember", key, "2"}).GetInt(), 1);
    EXPECT_EQ(Run({"llen", dest}).GetInt(), 1);
    EXPECT_EQ(Run({"lindex", dest, "0"}), "2");

    auto srem = std::ranges::find_if(
        consumer.entries, [](const CapturedEntry& entry) { return entry.cmd == "SREM"; });
    auto sort = std::ranges::find_if(
        consumer.entries, [](const CapturedEntry& entry) { return entry.cmd == "SORT"; });
    ASSERT_NE(srem, consumer.entries.end()) << "partial expiry must emit SREM";
    ASSERT_NE(sort, consumer.entries.end()) << "SORT must still auto-journal";
    EXPECT_LT(srem, sort) << "the peer must remove expired source members before replaying SORT";
  };

  exercise("sort-partial", "sort-partial-dest", false);
  exercise("sort-nosort-partial", "sort-nosort-partial-dest", true);

  EXPECT_EQ(Run({"sadd", "sort-error-partial", "expired", "not-a-number"}).GetInt(), 2);
  Run({"fieldexpire", "sort-error-partial", "1", "expired"});
  AdvanceTime(1100);
  consumer.entries.clear();

  EXPECT_THAT(Run({"sort", "sort-error-partial"}), ErrArg("can't be converted into double"));
  EXPECT_THAT(Run({"smembers", "sort-error-partial"}), RespElementsAre("not-a-number"));
  auto srem = std::ranges::find_if(consumer.entries,
                                   [](const CapturedEntry& entry) { return entry.cmd == "SREM"; });
  auto sort = std::ranges::find_if(consumer.entries,
                                   [](const CapturedEntry& entry) { return entry.cmd == "SORT"; });
  ASSERT_NE(srem, consumer.entries.end())
      << "an errored SORT must still replicate the lazy source expiry it performed";
  EXPECT_EQ(sort, consumer.entries.end()) << "the failed SORT itself must not be journaled";

  pp_->at(0)->LaunchFiber([&] { journal::UnregisterConsumer(consumer_id); }).Join();
}

// drakeydb: P4-0 -- the counterpart to the acceptance case above: an ordinary client-issued DEL
// (never touching DeleteIfEmpty/DeleteSetIfEmpty at all) must NOT carry kEntryFlagDerived, or a
// mesh peer would silently stop receiving real user deletes.
TEST_F(OriginJournalFamilyTest, UserIssuedDeleteIsNotFlaggedDerived) {
  OriginFlagCapturingConsumer consumer;
  uint32_t consumer_id = 0;
  pp_->at(0)
      ->LaunchFiber([&] {
        journal::StartInThread();
        consumer_id = journal::RegisterConsumer(&consumer);
      })
      .Join();

  Run({"set", "k", "v"});
  Run({"del", "k"});

  pp_->at(0)->LaunchFiber([&] { journal::UnregisterConsumer(consumer_id); }).Join();

  const CapturedEntry* del = LastDel(consumer.entries);
  ASSERT_NE(nullptr, del);
  EXPECT_FALSE(del->entry_flags & journal::kEntryFlagDerived)
      << "a client DEL must still reach peers";
}

namespace {
// drakeydb: Phase 3 T5 -- captures opcode/origin_idx/cmd/shard_id and the serialized ORIGIN index
// for every journal entry seen.
// `cmd` doubles as the announced uuid for an Op::ORIGIN entry: JournalSlice::AddLogRecord sets
// JournalChangeItem::cmd from Entry::payload.cmd unconditionally (not gated on opcode), and
// PeerRegistry::AddOrGet's ORIGIN emission puts the uuid in exactly that field (see
// multi_master.cc) -- so this needs no wire-level decoding, unlike journal_test.cc's streamer
// tests, which observe already-serialized bytes off a captured socket instead of live
// JournalChangeItems.
//
// drakeydb: fix-round-1 -- AddOrGet's fan-out (RunBlockingInParallel) dispatches one fiber per
// shard, each on that shard's own thread, so ConsumeJournalChange can fire on this SAME consumer
// instance concurrently from multiple threads. mu_ makes that safe; shard_id is captured via
// EngineShard::tlocal() inside ConsumeJournalChange, which runs on the recording shard's own
// thread, so it always names the shard that actually produced the entry.
struct OriginOpcodeEntry {
  journal::Op opcode;
  uint32_t origin_idx;
  uint32_t wire_origin_idx;
  std::string cmd;
  ShardId shard_id;
};

class OriginOpcodeCapturingConsumer : public journal::JournalConsumerInterface {
 public:
  void ConsumeJournalChange(const journal::JournalChangeItem& item) override {
    io::BytesSource source{item.journal_item.data};
    JournalReader reader{&source, 0};
    journal::ParsedEntry parsed;
    CHECK(!reader.ReadEntry(&parsed));
    CHECK(parsed.opcode == journal::Op::ORIGIN);

    util::fb2::LockGuard lk(mu_);
    entries.push_back({item.journal_item.opcode, item.journal_item.origin_idx, parsed.origin_idx,
                       std::string(item.cmd), EngineShard::tlocal()->shard_id()});
  }
  void ThrottleIfNeeded() override {
  }

  util::fb2::Mutex mu_;
  std::vector<OriginOpcodeEntry> entries;  // guarded by mu_
};
}  // namespace

// Uses MultiMasterFamilyTest's (== BaseFamilyTest's) default shard count directly, deliberately
// NOT OriginJournalFamilyTest's num_threads_=1/num_shards=1 pin: this test needs more than one
// shard to be meaningful, and (unlike the T3/T4 tests sharing OriginJournalFamilyTest) it never
// dispatches a keyed Redis command whose shard routing needs to be predictable.
class MultiShardOriginJournalFamilyTest : public MultiMasterFamilyTest {};

// drakeydb: Phase 3 T5 acceptance case for PeerRegistry::AddOrGet's journal fan-out, strengthened
// in fix-round-1 to actually exercise "every shard's journal" (not just shard 0) and the
// cross-proactor RunBlockingInParallel dispatch it now uses.
//
// Falsifying:
//  - Removing the fan-out (or gating it on the wrong condition) leaves `entries` empty after the
//    first AddOrGet, failing the first ASSERT.
//  - Emitting on fewer than all shards, or more than once on any shard, fails the
//    entries.size() == num_shards assertion or the per-shard seen[] uniqueness check.
//  - Emitting with the wrong opcode/authorship/wire index/payload fails the EXPECTs in the loop.
//  - Re-emitting on an already-registered uuid (i.e. not actually gating on `inserted`) grows
//    entries.size() on the second, idempotent AddOrGet call.
TEST_F(MultiShardOriginJournalFamilyTest, AddOrGetEmitsOriginOnNewIndexOnly) {
  // JournalSlice::Init() caches extended_framing_ = IsActiveReplica() once, and Op::ORIGIN can
  // only be written with extended framing (serializer.cc DCHECKs this). Set before
  // journal::StartInThread() below; MultiMasterFamilyTest's inherited saver_ restores it.
  absl::SetFlag(&FLAGS_active_replica, true);

  const size_t num_shards = shard_set->size();
  ASSERT_GT(num_shards, 1u) << "test requires more than one shard to be meaningful";

  OriginOpcodeCapturingConsumer consumer;
  std::vector<uint32_t> consumer_ids(num_shards, 0);
  // RunBriefInParallel (not RunBlockingInParallel): StartInThread/RegisterConsumer don't preempt,
  // so the brief (DispatchBrief) contract is fine here -- this loop is just setup, not the
  // fan-out under test.
  shard_set->RunBriefInParallel([&](EngineShard* shard) {
    journal::StartInThread();
    consumer_ids[shard->shard_id()] = journal::RegisterConsumer(&consumer);
  });

  PeerRegistry reg;
  reg.Init(GenerateNodeUuid());
  const std::string peer_uuid = GenerateNodeUuid();

  // AddOrGet's shard fan-out dispatches onto -- and blocks this fiber on -- every shard's own
  // proactor thread, the same cross-thread dispatch+wait shape as JournalExecutor::Execute
  // elsewhere in this file; run it from an explicit fiber for the same reason.
  uint32_t idx = 0;
  pp_->at(0)->LaunchFiber([&] { idx = reg.AddOrGet(peer_uuid); }).Join();

  {
    util::fb2::LockGuard lk(consumer.mu_);
    // Exactly one entry per shard -- not just a non-empty vector -- so a duplicate emission on
    // this first call (as opposed to only on a later, idempotent re-add) cannot pass unnoticed.
    ASSERT_EQ(num_shards, consumer.entries.size());

    std::vector<bool> seen(num_shards, false);
    for (const auto& e : consumer.entries) {
      EXPECT_EQ(journal::Op::ORIGIN, e.opcode);
      EXPECT_EQ(PeerRegistry::kSelfIdx, e.origin_idx);
      EXPECT_EQ(idx, e.wire_origin_idx);
      EXPECT_EQ(peer_uuid, e.cmd);
      ASSERT_LT(e.shard_id, num_shards);
      EXPECT_FALSE(seen[e.shard_id]) << "shard " << e.shard_id << " got more than one entry";
      seen[e.shard_id] = true;
    }
    for (size_t sid = 0; sid < num_shards; ++sid)
      EXPECT_TRUE(seen[sid]) << "shard " << sid << " never got an ORIGIN entry";
  }

  // Idempotent re-add of the same uuid: no new index, and (load-bearing for this test) no new
  // ORIGIN entry on any shard -- AddOrGet's fan-out must be gated on actually having inserted a
  // new index.
  uint32_t idx2 = 0;
  pp_->at(0)->LaunchFiber([&] { idx2 = reg.AddOrGet(peer_uuid); }).Join();
  EXPECT_EQ(idx, idx2);
  {
    util::fb2::LockGuard lk(consumer.mu_);
    EXPECT_EQ(num_shards, consumer.entries.size());
  }

  shard_set->RunBriefInParallel(
      [&](EngineShard* shard) { journal::UnregisterConsumer(consumer_ids[shard->shard_id()]); });
}

// drakeydb: P4-0 Task 2b -- boots with active_replica=true (ActiveReplicaFamilyTest, above) so
// DbSlice::DeleteExpiredStep's member-expiry reaper -- gated on IsActiveReplica(), Step 4 of the
// task brief -- actually runs; OriginJournalFamilyTest does not set the flag. Also
// single-shard/single-thread (OriginJournalFamilyTest's own reason, mirrored here) so one
// consumer registered on shard 0 is guaranteed to observe the reaper's DEL regardless of key
// hashing.
class ReaperJournalFamilyTest : public ActiveReplicaFamilyTest {
 protected:
  ReaperJournalFamilyTest() {
    num_threads_ = 1;
    absl::SetFlag(&FLAGS_num_shards, 1);
  }

  void DeleteReapedContainerForTest(DbSlice& db_slice, const DbContext& cntx, string_view key) {
    PrimeTable* table = db_slice.GetTables(cntx.db_index);
    auto it = table->Find(key);
    ASSERT_NE(it, table->end());
    db_slice.DeleteReapedContainer(cntx, key, DbSlice::Iterator(it, StringOrView::FromView(key)),
                                   true);
  }
};

// drakeydb: P4-0 Task 2b acceptance case. DbSlice::DeleteExpiredStep's member-expiry reaper must
// derive its own DEL exactly like a read would -- through the three-arg Del()+RecordDerivedDelete
// path (fix round 1's redesign; DeleteExpiredStep no longer touches SetFamily::DeleteSetIfEmpty
// at all -- see task-2b-report.md section 14), which sets kEntryFlagDerived so
// PassesPeerEchoFilter (types.cc; exhaustively pinned on the pure function by
// PassesPeerEchoFilterTest in journal_test.cc) keeps it off mesh-peer links -- while
// a plain consumer, standing in for a plain replica, still sees it: the peer filter is applied
// only when streaming to a mesh peer specifically (JournalStreamer::ShouldWrite, streamer.cc),
// never at journal::RegisterConsumer's point of capture, so this test's consumer (like
// EmptiedCollectionDeleteCarriesDerivedFlag's above) sees the entry unconditionally. `rs` is
// never read by anything in this test -- no SMEMBERS/FIELDTTL/etc. -- only the manually-driven
// DeleteExpiredStep call below (the same call engine_shard.cc's heartbeat makes) can be
// responsible for the cleanup.
//
// Falsifying: reverting Step 2's callback extension in DbSlice::DeleteExpiredStep leaves `rs`
// alive as a set forever -- EXPECT_EQ(Run({"exists", "rs"}).GetInt(), 0) fails first, before the
// DEL is ever captured. Verbatim text recorded in the P4 task-2b report.
TEST_F(ReaperJournalFamilyTest, MemberExpiryReaperDeleteCarriesDerivedFlag) {
  OriginFlagCapturingConsumer consumer;
  uint32_t consumer_id = 0;
  pp_->at(0)
      ->LaunchFiber([&] {
        journal::StartInThread();
        consumer_id = journal::RegisterConsumer(&consumer);
      })
      .Join();

  EXPECT_EQ(Run({"sadd", "rs", "m"}).GetInt(), 1);
  Run({"fieldexpire", "rs", "1", "m"});
  AdvanceTime(1100);

  // Drive the reaper the same way engine_shard.cc's heartbeat does (see
  // generic_family_test.cc's KeyspaceNotificationNoAtomicSectionOnExpiry for the same pattern
  // applied to whole-key expiry).
  shard_set->RunBriefInParallel([](EngineShard* shard) {
    DbSlice& db_slice = namespaces->GetDefaultNamespace().GetDbSlice(shard->shard_id());
    DbContext db_cntx;
    db_cntx.db_index = 0;
    db_cntx.time_now_ms = TEST_current_time_ms;
    db_slice.DeleteExpiredStep(db_cntx, 100);
  });

  pp_->at(0)->LaunchFiber([&] { journal::UnregisterConsumer(consumer_id); }).Join();

  // Guard against a vacuous pass: the set must have actually been cleaned up by the reaper.
  EXPECT_EQ(Run({"exists", "rs"}).GetInt(), 0);

  const CapturedEntry* del = LastDel(consumer.entries);
  ASSERT_NE(nullptr, del);
  EXPECT_TRUE(del->entry_flags & journal::kEntryFlagDerived)
      << "reaper DEL must be flagged derived or the peer filter forwards it";

  // Directly exercises the real filter with the real captured flags, rather than only citing
  // PassesPeerEchoFilterTest's generic coverage: a mesh peer must drop this exact entry.
  journal::JournalItem peer_check{};
  peer_check.opcode = journal::Op::COMMAND;
  peer_check.origin_idx = del->origin_idx;
  peer_check.entry_flags = del->entry_flags;
  EXPECT_FALSE(journal::PassesPeerEchoFilter(peer_check))
      << "reaper DEL must never reach a mesh peer -- the peer derives its own";
}

TEST_F(ReaperJournalFamilyTest, LocalOnlyReaperDoesNotJournalNamespaceBlindDelete) {
  OriginFlagCapturingConsumer consumer;
  uint32_t consumer_id = 0;
  pp_->at(0)
      ->LaunchFiber([&] {
        journal::StartInThread();
        consumer_id = journal::RegisterConsumer(&consumer);
      })
      .Join();

  EXPECT_EQ(Run({"sadd", "local-only-reap", "member"}).GetInt(), 1);
  Run({"fieldexpire", "local-only-reap", "1", "member"});
  AdvanceTime(1100);
  consumer.entries.clear();

  shard_set->RunBriefInParallel([](EngineShard* shard) {
    Namespace& ns = namespaces->GetDefaultNamespace();
    DbSlice& db_slice = ns.GetDbSlice(shard->shard_id());
    DbContext cntx{&ns, 0, TEST_current_time_ms};
    journal::DisableFlushGuard guard(shard->journal());
    db_slice.DeleteExpiredStep(cntx, 100000,
                               {.ensure_member_reaping = true, .journal_deletions = false});
  });

  pp_->at(0)->LaunchFiber([&] { journal::UnregisterConsumer(consumer_id); }).Join();

  EXPECT_EQ(Run({"exists", "local-only-reap"}).GetInt(), 0);
  EXPECT_EQ(LastDel(consumer.entries), nullptr)
      << "a namespace-local reap cannot emit a wire DEL without namespace identity";
}

// Pins the direct delete mechanism independently from the snapshot gate. A registered inert
// consumer makes an accidental return to FindMutable/DeleteSetIfEmpty observable through
// OnChange, while remaining safe to invoke directly because it serializes no data.
TEST_F(ReaperJournalFamilyTest, ReaperDeleteBypassesChangeCallbacks) {
  class CountingChangeConsumer final : public DbSlice::ChangeConsumerInterface {
   public:
    void OnChange(DbIndex, const ChangeReq&) override {
      ++calls;
    }
    unsigned calls = 0;
  } change_consumer;

  OriginFlagCapturingConsumer journal_consumer;
  uint32_t journal_consumer_id = 0;
  EXPECT_EQ(Run({"sadd", "direct-reaper-delete", "member"}).GetInt(), 1);

  pp_->at(0)->Await([&] {
    EngineShard* shard = EngineShard::tlocal();
    DbSlice& db_slice = namespaces->GetDefaultNamespace().GetDbSlice(shard->shard_id());
    journal::StartInThread();
    journal_consumer_id = journal::RegisterConsumer(&journal_consumer);

    shard->shard_lock()->Acquire(IntentLock::EXCLUSIVE);
    db_slice.RegisterOnChange(&change_consumer);
    shard->shard_lock()->Release(IntentLock::EXCLUSIVE);

    DbContext cntx{&namespaces->GetDefaultNamespace(), 0, TEST_current_time_ms};
    {
      journal::DisableFlushGuard guard(shard->journal());
      DeleteReapedContainerForTest(db_slice, cntx, "direct-reaper-delete");
    }

    EXPECT_TRUE(db_slice.UnregisterOnChange(&change_consumer));
    journal::UnregisterConsumer(journal_consumer_id);
  });

  EXPECT_EQ(change_consumer.calls, 0u)
      << "the direct reaper delete must not enter OnChange inside the atomic section";
  EXPECT_EQ(Run({"exists", "direct-reaper-delete"}).GetInt(), 0);
  const CapturedEntry* del = LastDel(journal_consumer.entries);
  ASSERT_NE(del, nullptr);
  EXPECT_TRUE(del->entry_flags & journal::kEntryFlagDerived);
}

// drakeydb: P4-0 Task 2b, fix round 6 Critical 1 -- the whole member-reap block used to live
// entirely inside `if (!it->first.HasExpire())` (db_slice.cc's DeleteExpiredStep), so a key that
// ALSO carries a whole-key TTL fell into the other arm, which checks only the whole-key deadline
// and returns early when it is not yet due -- the member walk never ran, on any node, and Task
// 1's mesh-peer phantom-container divergence (this reaper's whole reason to exist) returned
// intact for exactly that shape. A container with per-member TTLs that also has a key-level TTL
// is the ordinary way to bound a session or cache hash (SADD+FIELDEXPIRE+EXPIRE), and EXPIRE can
// be added to an existing HEXPIRE'd/FIELDEXPIRE'd key at any time -- silently removing it from
// reaper coverage for the life of that TTL. See task-2b-report.md for the demonstrated escalation
// to PERMANENT divergence (delete, recreate without a whole-key TTL, then a stale whole-key TTL
// on the peer fires and gets suppressed right back).
//
// This exercises the "whole-key TTL present but not yet due" arm specifically -- the one the bug
// lived in. MemberExpiryReaperDeleteCarriesDerivedFlag above already covers the "no whole-key TTL
// at all" arm and is unaffected by this fix (whole_key_due is false either way there).
//
// Falsifying: reverting the fix (member walk gated back to `if (!it->first.HasExpire())`) makes
// EXPECT_EQ(Run({"exists", "s2"}), 0) fail -- "s2" is never walked, never reaped, and stays a set
// with one dead member forever. Verbatim output recorded in task-2b-report.md.
TEST_F(ReaperJournalFamilyTest, MemberExpiryReaperCoversSetWithNotYetDueWholeKeyTtl) {
  ASSERT_EQ(Run({"sadd", "s2", "m1"}).GetInt(), 1);
  Run({"fieldexpire", "s2", "1", "m1"});
  ASSERT_EQ(Run({"expire", "s2", "100"}).GetInt(), 1);  // whole-key TTL, far from due
  AdvanceTime(1100);                                    // only the member TTL elapses

  shard_set->RunBriefInParallel([](EngineShard* shard) {
    DbSlice& db_slice = namespaces->GetDefaultNamespace().GetDbSlice(shard->shard_id());
    DbContext db_cntx;
    db_cntx.db_index = 0;
    db_cntx.time_now_ms = TEST_current_time_ms;
    db_slice.DeleteExpiredStep(db_cntx, 100000);
  });

  EXPECT_EQ(Run({"exists", "s2"}).GetInt(), 0)
      << "a set with a not-yet-due whole-key TTL was never walked by the member reaper -- "
         "Critical 1's fix did not close the gap";
}

// drakeydb: P4-0 Task 2b, fix round 6 Critical 1 -- same shape on hashes (the coordinator's own
// note: "Same shape on hashes via HTTL"), and additionally covers a PARTIAL reap (one field due,
// one not) so the container survives with the not-yet-due whole-key TTL and the surviving field
// both intact -- not just the "whole container disappears" case the set test above covers.
TEST_F(ReaperJournalFamilyTest, MemberExpiryReaperCoversHashWithNotYetDueWholeKeyTtl) {
  ASSERT_EQ(Run({"hset", "h2", "gone", "v1", "keep", "v2"}).GetInt(), 2);
  Run({"fieldexpire", "h2", "1", "gone"});
  ASSERT_EQ(Run({"expire", "h2", "100"}).GetInt(), 1);  // whole-key TTL, far from due
  AdvanceTime(1100);

  shard_set->RunBriefInParallel([](EngineShard* shard) {
    DbSlice& db_slice = namespaces->GetDefaultNamespace().GetDbSlice(shard->shard_id());
    DbContext db_cntx;
    db_cntx.db_index = 0;
    db_cntx.time_now_ms = TEST_current_time_ms;
    db_slice.DeleteExpiredStep(db_cntx, 100000);
  });

  EXPECT_EQ(Run({"hlen", "h2"}).GetInt(), 1)
      << "a hash with a not-yet-due whole-key TTL was never walked by the member reaper -- "
         "Critical 1's fix did not close the gap";
  EXPECT_EQ(Run({"hget", "h2", "keep"}), "v2");
  EXPECT_GT(Run({"ttl", "h2"}).GetInt(), 0)
      << "the surviving container's whole-key TTL must be untouched by the member walk";
}

namespace {
// drakeydb: P4-0 Task 2b redesign -- a SliceSnapshot data consumer that deterministically proves
// the traversal fiber is stuck holding SerializerBase::stream_mu_ (mid big-value chunk push,
// RdbSerializer::PushToConsumerIfNeeded's "preempt point") before the test triggers the reaper
// concurrently. No sleep, no timing race: ConsumeData signals `entered_` the instant it's called
// (i.e. the instant stream_mu_ is held) and then blocks on `release_` until the test says so.
class BlockingSnapshotConsumer : public SliceSnapshot::SnapshotDataConsumerInterface {
 public:
  void ConsumeData(std::string /*data*/, ExecutionState* /*cntx*/) override {
    entered_.Notify();
    // drakeydb: P4-0 Task 2b, fix round 1 -- bounded, not release_.Wait() unconditionally. If
    // the hazard this test exists to catch regresses in a RELEASE build (where Preempt's
    // LOG(DFATAL) only logs and still yields, rather than aborting -- see the test's own comment
    // below), the reaper's DeleteExpiredStep call blocks on stream_mu_, which THIS fiber holds
    // while waiting right here for that same call to return and release() us -- a genuine,
    // unrecoverable circular wait between the two fibers, not merely a slow test. Left
    // unbounded, that hangs until ctest's global timeout with no indication of why. Bounding it
    // lets this fiber give up, return, and release stream_mu_, which unblocks the reaper's
    // fiber in turn -- converting a silent multi-minute hang into a readable failure in seconds.
    if (!release_.WaitFor(std::chrono::seconds(15))) {
      // drakeydb: P4-0 Task 2b, fix round 8 -- the specific mechanism this message used to name
      // (the reaper's DeleteExpiredStep call deadlocking on stream_mu_ via OnChange) is no
      // longer reachable: HasRegisteredCallbacks() (db_slice.cc) skips the reap entirely while
      // this consumer is registered, before the reap path ever reaches any delete mechanism, old
      // or new. A timeout here now most likely means Release() was never called (a bug in this
      // test itself) or a genuinely different, new hang -- not the original stream_mu_ hazard.
      ADD_FAILURE() << "BlockingSnapshotConsumer::ConsumeData timed out waiting to be released -- "
                    << "either this test never called Release(), or something new is stuck; the "
                    << "original stream_mu_/OnChange hazard this class was built to catch is no "
                    << "longer reachable (HasRegisteredCallbacks() skips the reap before it can "
                    << "touch stream_mu_ at all -- see the test below for why)";
    }
  }
  void Finalize() override {
  }

  void WaitEntered() {
    entered_.Wait();
  }
  void Release() {
    release_.Notify();
  }

 private:
  util::fb2::Done entered_;
  util::fb2::Done release_;
};
}  // namespace

// drakeydb: P4-0 Task 2b redesign -- ORIGINAL regression test for the fiber-atomic-section
// hazard the coordinator's review surfaced (task-2b-report.md): the reaper's delete used to
// route through SetFamily::DeleteSetIfEmpty/HSetFamily::DeleteIfEmpty -> DbSlice::FindMutable ->
// PreUpdateBlocking -> CallChangeCallbacks -> SerializerBase::OnChange, which can synchronously
// block on stream_mu_ whenever a concurrent BGSAVE/full-sync is mid-way through serializing a
// large value on this same shard. Forces that exact contention -- not a synthetic stand-in --
// deterministically: a real SliceSnapshot (the same class RdbSaver/full-sync drive in production)
// serializes a value larger than serialization_max_chunk_size, and BlockingSnapshotConsumer above
// lets the test know, without polling or sleeping, the instant that snapshot's traversal fiber is
// genuinely stuck holding stream_mu_. Only then does the test trigger the reaper -- wrapped in the
// same journal::DisableFlushGuard atomic section engine_shard.cc's real heartbeat uses -- so any
// blocking on stream_mu_ here would hit Scheduler::Preempt's IsFiberAtomicSection() check for
// real, not hypothetically.
//
// drakeydb: P4-0 Task 2b, fix round 6 Critical 2 (coordinator's own correction of fix round 1's
// redirect) -- OnChange's SECOND role (BucketDependencies::Wait, serializer_base.cc) is what made
// every other mutation path safe against a mid-entry snapshot, and bypassing it (to fix the
// preemption the paragraph above describes) reopened a data race the non-blocking property this
// test proves cannot see. Fixed by skipping the reap entirely while any snapshot/streamer
// consumer is registered (HasRegisteredCallbacks(), db_slice.cc) -- see
// MemberExpiryReaperSkipsContainerDuringConcurrentSnapshot below for that fix's own regression
// test and falsification.
//
// drakeydb: P4-0 Task 2b, fix round 8 -- both this test's falsification claim and
// BlockingSnapshotConsumer::ConsumeData's timeout message above used to say that reverting the
// delete mechanism to DeleteSetIfEmpty/DeleteIfEmpty makes this test abort the entire process via
// Scheduler::Preempt's atomic-section check. That claim is no longer reproducible and has been
// removed: with the HasRegisteredCallbacks() gate in place, the reap is skipped before it ever
// reaches ANY delete mechanism -- old or new -- while this consumer is registered, so neither
// path can be exercised by this test anymore, regardless of which one production code uses. This
// test's remaining, still-live purpose is exactly what its name says: prove the reaper does not
// block a concurrent snapshot -- true unconditionally now, since skipping trivially cannot block
// on anything.
//
// ReaperDeleteBypassesChangeCallbacks independently pins the round-1 direct-delete redirect with
// an inert registered consumer. This test remains responsible only for the real snapshot gate:
// a serializing consumer makes container mutation unsafe even when the delete itself cannot
// preempt.
TEST_F(ReaperJournalFamilyTest, MemberExpiryReaperDoesNotBlockOnConcurrentBgsave) {
  // Activates SerializerBase::stream_mu_ (server/serializer_base.cc: `stream_mu_(!absl::GetFlag(
  // FLAGS_serialization_tagged_chunks))`) -- inactive (a no-op OptionalMutex) under the tagged-
  // chunks default, which is exactly why this test must set it explicitly to exercise the hazard.
  BaseFamilyTest::SetTestFlag("serialization_tagged_chunks", "false");

  // drakeydb: journal::StartInThread() (needed for RecordDerivedDelete's ring-buffer DCHECK, per
  // the existing MemberExpiryReaperDeleteCarriesDerivedFlag test above) must run on the shard's
  // own thread, matching every other journal::StartInThread() call site in this file.
  pp_->at(0)->LaunchFiber([&] { journal::StartInThread(); }).Join();

  // Many large keys, not one: PrimeTable::Traverse's bucket order is a deterministic function of
  // key hashes (fixed-seed XXH64, LockTag::Fingerprint), not insertion order, so a single "big"
  // key might land in a bucket the traversal reaches only after "rs"'s -- in which case "rs"
  // would already be marked serialized (stale) by the time the reaper runs, and the reaper's
  // OnChange call would take the cheap BucketDependencies::Wait fast-path instead of contending
  // stream_mu_. Enough large keys spread across the table make it overwhelmingly likely (and, for
  // this fixed key set and fixed hash seed, deterministically repeatable either way) that the
  // traversal is already stuck before it ever reaches "rs"'s bucket.
  for (int i = 0; i < 32; ++i) {
    ASSERT_EQ(Run({"set", absl::StrCat("big", i), string(200000, 'x')}), "OK");
  }
  ASSERT_EQ(Run({"sadd", "rs", "m"}).GetInt(), 1);
  Run({"fieldexpire", "rs", "1", "m"});

  // Pause the production heartbeat reaper before advancing past the TTL. A zero walk budget
  // cannot do this because the heartbeat deliberately clamps it to one. Explicit synchronous
  // reap calls flip active mode on only while no fiber can yield.
  const bool saved_active_replica = absl::GetFlag(FLAGS_active_replica);
  absl::SetFlag(&FLAGS_active_replica, false);
  absl::Cleanup restore_active_replica = [saved_active_replica] {
    absl::SetFlag(&FLAGS_active_replica, saved_active_replica);
  };
  AdvanceTime(1100);

  BlockingSnapshotConsumer consumer;
  ExecutionState exec_state;

  pp_->at(0)->Await([&] {
    EngineShard* shard = EngineShard::tlocal();
    DbSlice& db_slice = namespaces->GetDefaultNamespace().GetDbSlice(shard->shard_id());

    // ServerState::tlocal()->serialization_max_chunk_size is cached at shard-thread init from
    // the flag of the same name; the pytest/production default (64KB, or 300000 in the pytest
    // harness) is already smaller than "big"'s 200000 bytes, so no override is needed here.
    // DbSlice::RegisterOnChange (SliceSnapshot::Start's own RegisterChangeListener call)
    // DCHECKs the shard's intent lock is held -- in production DFLY SYNC's GLOBAL_TRANS command
    // scheduling already holds it; this test drives SliceSnapshot directly, off the
    // command-dispatch path, so it must satisfy that same precondition explicitly (mirrors
    // RdbTest.PeerFullSyncFiltersConcurrentJournalPlainReplicaUnaffected, rdb_test.cc).
    shard->shard_lock()->Acquire(IntentLock::EXCLUSIVE);

    SliceSnapshot snapshot(CompressionMode::NONE, &db_slice, &consumer, &exec_state,
                           DflyVersion::CURRENT_VER);
    snapshot.Start(/*stream_journal=*/false, SliceSnapshot::SnapshotFlush::kAllow);

    // Blocks this fiber until the background traversal fiber is inside ConsumeData for "big"'s
    // chunk -- i.e. genuinely holding stream_mu_ -- guaranteed, not merely likely.
    consumer.WaitEntered();

    // Trigger the reaper concurrently, from this fiber, inside the real heartbeat's atomic
    // section -- enabling active mode only for this synchronous call. See
    // MemberExpiryReaperSkipsContainerDuringConcurrentSnapshot below for what this call is and
    // is not expected to do now.
    {
      journal::DisableFlushGuard guard(shard->journal());
      absl::SetFlag(&FLAGS_active_replica, true);
      DbContext db_cntx;
      db_cntx.db_index = 0;
      db_cntx.time_now_ms = TEST_current_time_ms;
      db_slice.DeleteExpiredStep(db_cntx, 100);
      absl::SetFlag(&FLAGS_active_replica, false);
    }

    consumer.Release();
    snapshot.WaitSnapshotting();
    shard->shard_lock()->Release(IntentLock::EXCLUSIVE);
  });

  // drakeydb: P4-0 Task 2b, fix round 6 Critical 2 -- "rs" must NOT have been reaped: a
  // registered snapshot consumer was present for the entire DeleteExpiredStep call above, so the
  // HasRegisteredCallbacks() gate must have skipped it (deferral, not vacuous inaction -- see the
  // follow-up reap below, which proves it resumes correctly once the consumer is gone). The
  // active-mode pause above rules out the real background heartbeat as an alternative
  // explanation for "rs" being gone here.
  EXPECT_EQ(Run({"exists", "rs"}).GetInt(), 1)
      << "the reaper reaped a container while a snapshot consumer was registered -- the "
         "HasRegisteredCallbacks() gate did not skip it";
  EXPECT_EQ(Run({"get", "big0"}), string(200000, 'x'))
      << "the concurrent snapshot itself must have completed undisturbed";

  // The skip must be a deferral, not a permanent miss: with the consumer now unregistered, the
  // very next reap call must clean "rs" up normally.
  absl::SetFlag(&FLAGS_active_replica, true);
  shard_set->RunBriefInParallel([](EngineShard* shard) {
    DbSlice& db_slice = namespaces->GetDefaultNamespace().GetDbSlice(shard->shard_id());
    DbContext db_cntx;
    db_cntx.db_index = 0;
    db_cntx.time_now_ms = TEST_current_time_ms;
    db_slice.DeleteExpiredStep(db_cntx, 100);
  });
  absl::SetFlag(&FLAGS_active_replica, false);
  EXPECT_EQ(Run({"exists", "rs"}).GetInt(), 0)
      << "the reaper did not resume once the snapshot consumer unregistered";
}

// drakeydb: P4-0 Task 2b, fix round 6 Critical 2 -- regression test for the race the coordinator
// found in their own fix round 1 redirect: OnChange (SerializerBase, called by the command-path
// helpers fix round 1 redirected DeleteExpiredStep away from, to avoid a DIFFERENT hazard -- see
// MemberExpiryReaperDoesNotBlockOnConcurrentBgsave above) has a second role besides the
// preemption that redirect was right to avoid -- CallChangeCallbacks -> ProcessBucket(on_update=
// true) -> BucketDependencies::Wait(bucket_address) (serializer_base.cc) blocks the MUTATOR until
// a concurrently-serializing SliceSnapshot finishes that bucket. That wait is what makes every
// other mutation path safe against a mid-entry snapshot; going around OnChange (as the reap path
// does, by necessity, to avoid the preemption) also goes around that wait. SaveHSetObject
// (rdb_save.cc) calls set_time(0) ("disables lazy expiry during serialization"), then
// SaveLen(UpperBoundSize()), then iterates with an explicit preempt point
// (PushToConsumerIfNeeded) -- in that window, without the wait, the reaper could on the same key:
// undo the serializer's set_time(0) via SetMemberTime (db_slice.cc) so its own ++it starts
// dropping members below the already-declared length; free sds objects and collapse chains under
// the serializer's live iterator; or Del() the container the serializer is mid-iteration over.
//
// MemberExpiryReaperDoesNotBlockOnConcurrentBgsave above cannot catch this: it proves the reaper
// does not BLOCK, a different question from whether skipping the wait is SAFE, and it uses a
// 200KB STRING value, which never exercises container mutation at all. This test mirrors its
// setup but seeds a member-TTL'd HASH large enough to span multiple serialization chunks, and
// asserts on data correctness (the round-tripped member count) rather than non-blocking.
//
// Fix: HasRegisteredCallbacks() (db_slice.h) added to the reap gate (db_slice.cc) -- skip the
// sweep entirely while any snapshot/streamer consumer is registered, rather than trying to make
// racing it safe by argument. Costs nothing (resumes next tick) and removes the race by
// construction.
//
// Falsifying: reverting the HasRegisteredCallbacks() gate reintroduces the exact race this test
// exercises -- verified by hand (temporarily removing the `!HasRegisteredCallbacks() &&` conjunct
// and rebuilding) -- verbatim output recorded in task-2b-report.md.
TEST_F(ReaperJournalFamilyTest, MemberExpiryReaperSkipsContainerDuringConcurrentSnapshot) {
  BaseFamilyTest::SetTestFlag("serialization_tagged_chunks", "false");
  pp_->at(0)->LaunchFiber([&] { journal::StartInThread(); }).Join();

  // A member-TTL'd hash large enough to span multiple serialization chunks (the default
  // serialization_max_chunk_size is well under this): half the fields carry a member TTL due by
  // the time the reaper runs, the other half never expire -- both must survive the race window
  // untouched by the reaper's own mutation, whichever kind of corruption would have hit them.
  constexpr int kFields = 500;
  vector<string> hset_args{"hset", "bighash"};
  for (int i = 0; i < kFields; ++i) {
    hset_args.push_back(absl::StrCat("f", i));
    hset_args.push_back(string(200, 'x'));
  }
  ASSERT_EQ(Run(hset_args).GetInt(), kFields);
  vector<string> fieldexpire_args{"fieldexpire", "bighash", "1"};
  for (int i = 0; i < kFields; i += 2)
    fieldexpire_args.push_back(absl::StrCat("f", i));
  Run(fieldexpire_args);

  // A zero walk budget is clamped to one by the production heartbeat. Disable active mode before
  // advancing past the TTL, then enable it only around the explicit synchronous calls.
  const bool saved_active_replica = absl::GetFlag(FLAGS_active_replica);
  absl::SetFlag(&FLAGS_active_replica, false);
  absl::Cleanup restore_active_replica = [saved_active_replica] {
    absl::SetFlag(&FLAGS_active_replica, saved_active_replica);
  };
  AdvanceTime(1100);

  BlockingSnapshotConsumer consumer;
  ExecutionState exec_state;

  pp_->at(0)->Await([&] {
    EngineShard* shard = EngineShard::tlocal();
    DbSlice& db_slice = namespaces->GetDefaultNamespace().GetDbSlice(shard->shard_id());
    shard->shard_lock()->Acquire(IntentLock::EXCLUSIVE);

    SliceSnapshot snapshot(CompressionMode::NONE, &db_slice, &consumer, &exec_state,
                           DflyVersion::CURRENT_VER);
    snapshot.Start(/*stream_journal=*/false, SliceSnapshot::SnapshotFlush::kAllow);

    consumer.WaitEntered();

    {
      journal::DisableFlushGuard guard(shard->journal());
      // This call must still skip "bighash" entirely, since a snapshot consumer is registered
      // for its whole duration.
      absl::SetFlag(&FLAGS_active_replica, true);
      DbContext db_cntx;
      db_cntx.db_index = 0;
      db_cntx.time_now_ms = TEST_current_time_ms;
      db_slice.DeleteExpiredStep(db_cntx, 100);
      absl::SetFlag(&FLAGS_active_replica, false);
    }

    consumer.Release();
    snapshot.WaitSnapshotting();
    shard->shard_lock()->Release(IntentLock::EXCLUSIVE);
  });

  // The reaper must not have touched "bighash" while the snapshot was registered: still all
  // kFields present immediately after the race window, nothing reaped by the manual
  // DeleteExpiredStep call made while the consumer was registered. The active-mode pause above
  // rules out the real background heartbeat as an alternative explanation.
  ASSERT_EQ(Run({"hlen", "bighash"}).GetInt(), kFields)
      << "the reaper reaped members from a container mid-snapshot -- HasRegisteredCallbacks() "
         "gate did not skip it";
  // Spot-check a surviving (never-expired, odd-indexed) field's actual VALUE, not just the
  // count: a use-after-free or collapsed-chain corruption could leave a plausible-looking count
  // with garbage content.
  EXPECT_EQ(Run({"hget", "bighash", "f1"}), string(200, 'x'))
      << "a surviving field's value was corrupted by the race this fix closes";

  // drakeydb: P4-0 Task 2b, fix round 7 -- moved BEFORE the debug-reload round trip below
  // (deliberately, was after it): the coordinator's scoped re-review found that, run after
  // reload, this block cannot fail regardless of whether the reaper itself works -- by the time
  // it runs, rdb_load.cc's own lazy-expiry check has already dropped the due fields on load
  // (see the reload comment below), so the reloaded hash no longer reports
  // HasMemberExpiration() and the reaper's gate (db_slice.cc) skips it outright; the assertion
  // held whether or not DeleteExpiredStep did anything. Here, before any reload has happened,
  // the only thing that can have reaped "bighash" down to kFields / 2 is this explicit
  // DeleteExpiredStep call itself -- the skip above was a deferral, not a permanent miss, and
  // this is what actually distinguishes that from a stuck/broken resume.
  absl::SetFlag(&FLAGS_active_replica, true);
  shard_set->RunBriefInParallel([](EngineShard* shard) {
    DbSlice& db_slice = namespaces->GetDefaultNamespace().GetDbSlice(shard->shard_id());
    DbContext db_cntx;
    db_cntx.db_index = 0;
    db_cntx.time_now_ms = TEST_current_time_ms;
    db_slice.DeleteExpiredStep(db_cntx, 100000);
  });
  absl::SetFlag(&FLAGS_active_replica, false);
  ASSERT_EQ(Run({"hlen", "bighash"}).GetInt(), kFields / 2)
      << "the reaper must resume and reap the due fields once the snapshot consumer unregisters";

  // The concurrently-serialized container must remain internally consistent: a subsequent,
  // ordinary save+reload exercises the real save path again and would surface any corruption
  // (a dangling/collapsed chain, a freed sds object, a declared-length mismatch) left behind by
  // the race this fix closes. By this point the reaper above has already reaped the container
  // down to kFields / 2 -- this checks the round trip preserves that state faithfully, not
  // whether the due fields get dropped (that was already checked, non-vacuously, above).
  BaseFamilyTest::SetTestFlag("dbfilename", absl::StrCat("reaper_snapshot_race_", getpid()));
  ASSERT_EQ(Run({"debug", "reload"}), "OK");
  EXPECT_EQ(Run({"hlen", "bighash"}).GetInt(), kFields / 2)
      << "round-tripped member count changed across the save -- data corruption from the race "
         "this fix closes, or an unrelated regression in the save/load path";
  EXPECT_EQ(Run({"hget", "bighash", "f1"}), string(200, 'x'))
      << "a surviving field's value was corrupted by the race this fix closes, or by the "
         "round trip";
  EXPECT_THAT(Run({"hget", "bighash", "f0"}), ArgType(RespExpr::NIL))
      << "an already-due field survived (or reappeared after) the round trip";
}

// drakeydb: P4-0 Task 2b, fix round 2, Important B/3 -- the reaper's memory-accounting
// reconciliation, exercised end to end through the real DeleteExpiredStep call (matching
// engine_shard.cc's heartbeat), not just at the DenseSet/OAHTable unit level (string_map_test.cc/
// oah_set_test.cc cover ReaperExpireStep's own correctness in isolation). A PARTIAL reap --
// container survives, only some fields drop -- must shrink obj_memory_usage; leaving it
// unchanged (fix round 1's AutoUpdater::Run() bug notwithstanding -- that one WAS reconciling,
// just with an unwanted side effect, see the WATCH test below) would mean this test's whole
// point: a background sweep that touched nothing from any client's perspective still correctly
// unwinds the DB-wide byte counters it perturbed.
//
// Falsifying: commenting out the AccountObjectMemory block in DeleteExpiredStep's reaper branch
// (verified by hand -- see task-2b-report.md) makes `after` come back equal to `before`, since
// nothing else in the reap path touches obj_memory_usage for a surviving container.
//
// drakeydb: P4-0 Task 2b, fix round 4 -- the `100000` passed to DeleteExpiredStep below is its
// own `count` parameter, bounding how many PRIME TABLE keys this call's outer traversal visits
// -- a fix round 2 version of this comment called it "budget large enough to finish in one call"
// as if it were the reaper's OWN per-container walk budget (FLAGS_reaper_member_walk_budget,
// db_slice.cc), a completely different quantity: how many slots ReaperExpireStep examines
// WITHIN one container. Conflating the two meant this test's "enough shrinkage happened"
// actually rode on whatever the ambient flag default (300) happened to be, well under kFields --
// no correctness bug (any nonzero shrinkage still satisfies EXPECT_LT below), but a machine- and
// default-speed-dependent test rather than a deliberately-provisioned one, and liable to break
// silently if the default is ever retuned again. Pin it explicitly here, comfortably above
// kFields, so this test asserts what it says it asserts regardless of the flag's current default.
TEST_F(ReaperJournalFamilyTest, MemberExpiryReaperReconcilesMemoryAccounting) {
  constexpr int kFields = 2000;
  const uint32_t saved_walk_budget = absl::GetFlag(FLAGS_reaper_member_walk_budget);
  absl::SetFlag(&FLAGS_reaper_member_walk_budget, 100000);
  absl::Cleanup restore_walk_budget = [saved_walk_budget] {
    absl::SetFlag(&FLAGS_reaper_member_walk_budget, saved_walk_budget);
  };

  vector<string> hset_args{"hset", "bighash"};
  for (int i = 0; i < kFields; ++i) {
    hset_args.push_back(absl::StrCat("f", i));
    hset_args.push_back(string(200, 'x'));  // padding so the shrink is measurable
  }
  Run(hset_args);

  // Field-expire half the fields; the other half keeps the container alive (partial reap).
  vector<string> fieldexpire_args{"fieldexpire", "bighash", "1"};
  for (int i = 0; i < kFields; i += 2)
    fieldexpire_args.push_back(absl::StrCat("f", i));
  Run(fieldexpire_args);

  AdvanceTime(1100);

  size_t before = GetMetrics().db_stats[0].obj_memory_usage;

  size_t reported_deleted_bytes = 0;
  shard_set->RunBriefInParallel([&](EngineShard* shard) {
    DbSlice& db_slice = namespaces->GetDefaultNamespace().GetDbSlice(shard->shard_id());
    DbContext db_cntx;
    db_cntx.db_index = 0;
    db_cntx.time_now_ms = TEST_current_time_ms;
    // count=100000 bounds the outer prime-table traversal, not the reaper's own per-container
    // walk -- see this test's comment above for why that distinction matters here.
    reported_deleted_bytes = db_slice.DeleteExpiredStep(db_cntx, 100000).deleted_bytes;
  });

  size_t after = GetMetrics().db_stats[0].obj_memory_usage;

  // Guard against a vacuous pass: half the fields must actually be gone (a real partial reap,
  // not a no-op), and the container must have survived (the other half remains).
  ASSERT_EQ(Run({"hlen", "bighash"}).GetInt(), kFields / 2);
  EXPECT_LT(after, before) << "reaping half the fields shrank the container's MallocUsed(), but "
                              "obj_memory_usage was not correspondingly reconciled";
  EXPECT_EQ(reported_deleted_bytes, before - after)
      << "partial member expiry must contribute reclaimed bytes to eviction accounting";
}

// A partial hash reap is a real document mutation even though it bypasses command callbacks.
// Search postings must be removed before expired fields are freed and rebuilt from survivors.
TEST_F(ReaperJournalFamilyTest, MemberExpiryReaperRefreshesHashSearchIndex) {
  EXPECT_EQ(Run({"FT.CREATE", "reaper-index", "ON", "HASH", "PREFIX", "1", "reaper-doc:", "SCHEMA",
                 "tag", "TAG"}),
            "OK");
  EXPECT_EQ(Run({"hset", "reaper-doc:1", "tag", "red", "keep", "alive"}).GetInt(), 2);

  auto before = Run({"FT.SEARCH", "reaper-index", "@tag:{red}", "NOCONTENT"});
  ASSERT_THAT(before, ArgType(RespExpr::ARRAY));
  ASSERT_FALSE(before.GetVec().empty());
  EXPECT_THAT(before.GetVec().front(), IntArg(1));

  Run({"hexpire", "reaper-doc:1", "1", "FIELDS", "1", "tag"});
  AdvanceTime(1100);
  shard_set->RunBriefInParallel([](EngineShard* shard) {
    Namespace& ns = namespaces->GetDefaultNamespace();
    DbSlice& db_slice = ns.GetDbSlice(shard->shard_id());
    DbContext db_cntx{&ns, 0, TEST_current_time_ms};
    db_slice.DeleteExpiredStep(db_cntx, 100000);
  });

  EXPECT_EQ(Run({"hget", "reaper-doc:1", "keep"}), "alive");
  EXPECT_THAT(Run({"hget", "reaper-doc:1", "tag"}), ArgType(RespExpr::NIL));
  auto after = Run({"FT.SEARCH", "reaper-index", "@tag:{red}", "NOCONTENT"});
  ASSERT_THAT(after, ArgType(RespExpr::ARRAY));
  ASSERT_FALSE(after.GetVec().empty());
  EXPECT_THAT(after.GetVec().front(), IntArg(0));
}

// drakeydb: P4-0 Task 2b, fix round 2, Important B -- fix round 1's AutoUpdater::Run() also
// fired PostUpdate (db_slice.cc), which marks every WATCH registration on the key dirty. A
// client WATCHing a set/hash that the reaper merely walks (a background sweep invisible to any
// client, not a write) must not see a spurious EXEC abort with no write having occurred.
//
// Falsifying: reverting the reaper's accounting call back to AutoUpdater::Run() (verified by
// hand -- see task-2b-report.md) makes the EXEC below come back kExecFail instead of
// kExecSuccess, purely from the reaper's own heartbeat-driven walk.
TEST_F(ReaperJournalFamilyTest, MemberExpiryReaperDoesNotSpuriouslyAbortWatch) {
  const auto kExecSuccess = ArgType(RespExpr::ARRAY);

  ASSERT_EQ(Run({"sadd", "ws", "keep", "gone"}).GetInt(), 2);
  Run({"fieldexpire", "ws", "1", "gone"});
  AdvanceTime(1100);

  EXPECT_EQ(Run({"watch", "ws"}), "OK");

  shard_set->RunBriefInParallel([](EngineShard* shard) {
    DbSlice& db_slice = namespaces->GetDefaultNamespace().GetDbSlice(shard->shard_id());
    DbContext db_cntx;
    db_cntx.db_index = 0;
    db_cntx.time_now_ms = TEST_current_time_ms;
    db_slice.DeleteExpiredStep(db_cntx, 100000);
  });

  // Guard against a vacuous pass: the reaper must have actually walked/shrunk "ws" (a real
  // partial reap -- "keep" survives, "gone" doesn't), not merely left it untouched.
  ASSERT_EQ(Run({"scard", "ws"}).GetInt(), 1);

  Run({"multi"});
  Run({"get", "unrelated-key"});
  EXPECT_THAT(Run({"exec"}), kExecSuccess)
      << "the reaper's own walk of a WATCHed key must not abort an unrelated EXEC";
}

// drakeydb: P4-0 Task 2b, fix round 3, Important C round-trip coverage -- the coordinator's
// review flagged that no rdb_test case exercised member-TTL-container -> reaper-cleared-flag ->
// save -> reload, and that the existing SaveLoadExpiredValuesHmap/SSet-style tests
// (rdb_test.cc) don't cover it: those never drive the reaper, so the sticky flag stays set
// throughout and RDB save's own per-member expiry filtering (not the flag) is what's under test
// there. This is the positive half: a container whose member TTLs ALL expired and whose flag
// the reaper genuinely cleared (a complete, clean ReaperExpireStep pass) must still round-trip
// correctly -- right surviving members, right plain type, nothing silently corrupted by the new
// clear.
//
// Falsifying: forcing ReaperClearMemberExpiration() to fire unconditionally instead of only on
// complete_clean_pass (verified by hand -- see task-2b-report.md) does not break this specific
// test (a genuinely clean pass looks the same either way) -- it's the companion test below,
// MemberExpiryReaperUnclearedFlagPreservesTtlAcrossRdbRoundTrip, that catches an incorrect
// clear; see that test's own falsification.
TEST_F(ReaperJournalFamilyTest, MemberExpiryReaperClearedFlagSurvivesRdbRoundTrip) {
  // debug reload needs a dbfilename (BaseFamilyTest's default fixtures leave it unset; only
  // RdbTest's own SetUp calls InitWithDbFilename() to arrange this) -- read live by DoSave, so
  // setting it here (no full service reset needed) is sufficient. Unique per test to avoid
  // collisions with any other test in this binary that also sets it.
  BaseFamilyTest::SetTestFlag("dbfilename", absl::StrCat("reaper_rdb_test_cleared_", getpid()));
  ASSERT_EQ(Run({"hset", "rdbhash", "keep", "v1", "gone", "v2"}).GetInt(), 2);
  Run({"fieldexpire", "rdbhash", "1", "gone"});
  AdvanceTime(1100);

  // Drive the reaper directly, matching the real heartbeat's call, with a budget large enough
  // to finish in one pass -- so this call reports (and acts on) a complete, clean pass.
  shard_set->RunBriefInParallel([](EngineShard* shard) {
    DbSlice& db_slice = namespaces->GetDefaultNamespace().GetDbSlice(shard->shard_id());
    DbContext db_cntx;
    db_cntx.db_index = 0;
    db_cntx.time_now_ms = TEST_current_time_ms;
    db_slice.DeleteExpiredStep(db_cntx, 100000);
  });

  // Guard against a vacuous pass: "gone" must actually be gone, "keep" must survive -- a real
  // partial reap, and (since there is no live member TTL left anywhere in the container) one
  // that should have cleared the sticky flag.
  ASSERT_EQ(Run({"hlen", "rdbhash"}).GetInt(), 1);
  ASSERT_EQ(Run({"hget", "rdbhash", "keep"}), "v1");

  ASSERT_EQ(Run({"debug", "reload"}), "OK");

  EXPECT_EQ(Run({"type", "rdbhash"}), "hash");
  EXPECT_EQ(Run({"hget", "rdbhash", "keep"}), "v1");
  EXPECT_EQ(Run({"hlen", "rdbhash"}).GetInt(), 1);
}

// drakeydb: P4-0 Task 2b, fix round 3, Important C round-trip coverage -- the negative half,
// which is the one that actually catches a wrong clear: a container whose flag the reaper does
// NOT clear (because a live, not-yet-due member TTL survives the pass) must still preserve that
// TTL across an RDB round trip. If ReaperClearMemberExpiration() fired here anyway, the
// container would serialize as plain (no TTL-aware RDB opcode -- rdb_save.cc:179/:192/:494), and
// "later"'s TTL would be silently lost on reload -- the exact silent-data-corruption failure
// mode the coordinator's review named, not a wrong metric.
TEST_F(ReaperJournalFamilyTest, MemberExpiryReaperUnclearedFlagPreservesTtlAcrossRdbRoundTrip) {
  // See the sibling test above for why this is needed.
  BaseFamilyTest::SetTestFlag("dbfilename", absl::StrCat("reaper_rdb_test_uncleared_", getpid()));
  ASSERT_EQ(Run({"hset", "rdbhash2", "soon", "v1", "later", "v2"}).GetInt(), 2);
  Run({"fieldexpire", "rdbhash2", "1", "soon"});      // due almost immediately
  Run({"fieldexpire", "rdbhash2", "1000", "later"});  // stays live for a long time
  AdvanceTime(1100);  // "soon" now due; "later" still has ~998s left

  shard_set->RunBriefInParallel([](EngineShard* shard) {
    DbSlice& db_slice = namespaces->GetDefaultNamespace().GetDbSlice(shard->shard_id());
    DbContext db_cntx;
    db_cntx.db_index = 0;
    db_cntx.time_now_ms = TEST_current_time_ms;
    db_slice.DeleteExpiredStep(db_cntx, 100000);
  });

  // Guard against a vacuous pass: "soon" must actually be gone (a real, complete pass ran), but
  // "later" must survive WITH its TTL still armed -- this is the case that must leave the sticky
  // flag set, since one live member TTL remains.
  ASSERT_EQ(Run({"hlen", "rdbhash2"}).GetInt(), 1);
  ASSERT_GT(Run({"fieldttl", "rdbhash2", "later"}).GetInt(), 0)
      << "guard against a vacuous pass: later's TTL must still be armed before the round "
         "trip";

  ASSERT_EQ(Run({"debug", "reload"}), "OK");

  EXPECT_EQ(Run({"type", "rdbhash2"}), "hash");
  EXPECT_EQ(Run({"hget", "rdbhash2", "later"}), "v2");
  EXPECT_GT(Run({"fieldttl", "rdbhash2", "later"}).GetInt(), 0)
      << "later's member TTL must survive the RDB round trip -- FIELDTTL returning -1 "
         "here means it was lost, i.e. the container was incorrectly saved as a plain "
         "(no-TTL) type";
}

}  // namespace dfly
