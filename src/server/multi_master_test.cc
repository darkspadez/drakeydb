// Copyright 2026, drakeydb authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "server/multi_master.h"

#include <absl/container/flat_hash_set.h>
#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>
#include <sys/stat.h>
#include <unistd.h>

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
#include "io/file_util.h"
#include "server/engine_shard_set.h"
#include "server/node_identity.h"
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
  ReplicaSummary down{};
  down.host = "10.0.0.9";
  down.port = 7002;
  down.master_link_established = false;
  down.full_sync_in_progress = true;
  down.master_last_io_sec = 0;
  std::string s = RenderPeerReplicationInfo({up, down}, true, true);
  EXPECT_EQ(
      "active_replica:1\r\nmulti_master:1\r\nconnected_masters:2\r\n"
      "master0:host=localhost,port=7001,link_status=up,last_io_seconds_ago=3,"
      "sync_in_progress=0,node_uuid=01234567-89ab-4cde-8f01-23456789abcd\r\n"
      "master1:host=10.0.0.9,port=7002,link_status=down,last_io_seconds_ago=0,"
      "sync_in_progress=1\r\n",
      s);
  EXPECT_EQ("active_replica:1\r\nmulti_master:0\r\nconnected_masters:2\r\n",
            RenderPeerReplicationInfo({up, down}, false, false));
  EXPECT_EQ("active_replica:1\r\nmulti_master:0\r\nconnected_masters:0\r\n",
            RenderPeerReplicationInfo({}, false, true));
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
  auto resp = Run({"replconf", "listening-port", "1"});
  EXPECT_THAT(resp, ErrArg("active-replica"));
  resp = Run({"replconf", "capa", "dragonfly"});
  EXPECT_THAT(resp, ErrArg("active-replica"));
}

TEST_F(ActiveReplicaFamilyTest, ReplTakeoverRefusedWhileActive) {
  EXPECT_THAT(Run({"repltakeover", "1"}), ErrArg("active-replica"));
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

}  // namespace dfly
