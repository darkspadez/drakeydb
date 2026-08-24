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
#include "facade/reply_builder.h"
#include "io/file_util.h"
#include "server/dflycmd.h"
#include "server/engine_shard_set.h"
#include "server/journal/executor.h"
#include "server/journal/journal.h"
#include "server/journal/types.h"
#include "server/node_identity.h"
#include "server/server_family.h"
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

TEST_F(ActiveReplicaFamilyTest, ReplconfRefusesOwnUuid) {
  // A version high enough to admit and PEER unset, so the own-uuid refusal below cannot be a
  // side effect of some other row's check -- if the own-uuid guard were removed, this exact
  // sequence would be admitted (an ARRAY reply), not refused.
  std::string info{ToSV(Run({"info", "replication"}).GetBuf())};
  size_t pos = info.find("\nnode_uuid:");
  ASSERT_NE(std::string::npos, pos);
  std::string own_uuid = info.substr(pos + 11, 36);

  Run({"replconf", "uuid", own_uuid});
  EXPECT_EQ("OK", Run({"replconf", "drakey-version", absl::StrCat(kDrakeydbReplVersion)}));
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
  EXPECT_EQ(0, del->entry_flags);  // A derived DEL, not an expiry DEL.
}

namespace {
// drakeydb: Phase 3 T5 -- captures opcode/origin_idx/cmd/shard_id for every journal entry seen.
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
  std::string cmd;
  ShardId shard_id;
};

class OriginOpcodeCapturingConsumer : public journal::JournalConsumerInterface {
 public:
  void ConsumeJournalChange(const journal::JournalChangeItem& item) override {
    util::fb2::LockGuard lk(mu_);
    entries.push_back({item.journal_item.opcode, item.journal_item.origin_idx,
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
//  - Emitting with the wrong opcode/origin_idx/payload fails the three EXPECTs in the loop.
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
      EXPECT_EQ(idx, e.origin_idx);
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

}  // namespace dfly
