// Copyright 2026, drakeydb authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "server/multi_master.h"

#include <absl/cleanup/cleanup.h>
#include <absl/container/flat_hash_map.h>
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
#include "server/journal/tx_executor.h"
#include "server/journal/types.h"
#include "server/multimaster_lww.h"
#include "server/node_identity.h"
#include "server/rdb_load.h"
#include "server/replica.h"
#include "server/server_family.h"
#include "server/snapshot.h"
#include "server/test_utils.h"
extern "C" {
#include "redis/crc64.h"
}

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
  // drakeydb: P4-4 -- bumped 67 -> 68 alongside kDrakeydbReplVersion itself (node_identity.h):
  // this phase adds the streaming LWW guard plus an applied-write stamp floor change to apply
  // semantics, so a mixed 67/68 mesh must be refused at handshake, not admitted and left to
  // silently diverge.
  EXPECT_EQ(68u, kDrakeydbReplVersion);
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

// drakeydb: P4-3 Task 3 -- Task 1 review Finding 6 (task-1-review.md): a negative value reaching
// --multi_master_tombstone_ttl (a uint64_t flag) sign-converts to a value near UINT64_MAX.
// TombstoneGcStep's ttl_s*1000 + MsPart() sum (db_slice.cc, mvcc.h) would overflow uint64 and
// wrap to a deadline in the past, reaping every live tombstone on the first GC tick. absl's own
// flag parser already rejects a literal "-1" on the command line for a uint64_t flag (it never
// reaches AbslParseFlag's negative branch), so this exercises the belt-and-suspenders guard
// directly via SetFlag, the same way a signed-source caller upstream of the CLI parser could.
TEST(MultiMasterFlags, TombstoneTtlRejectsSignConvertedValue) {
  absl::FlagSaver saver;
  absl::SetFlag(&FLAGS_active_replica, true);
  absl::SetFlag(&FLAGS_multi_master_tombstone_ttl, static_cast<uint64_t>(-1));
  EXPECT_FALSE(ValidateMultiMasterFlags());

  absl::SetFlag(&FLAGS_multi_master_tombstone_ttl, 600);
  EXPECT_TRUE(ValidateMultiMasterFlags());
}

// drakeydb: P4-3 Task 3, review fix (C1), Critical -- a budget of 0 buckets/tick can never make
// progress. Before this fix DbSlice::TombstoneGcStep (db_slice.cc) treated that identically to
// "ran out of budget mid-lap, more work queued" and reported pending work forever; the on-idle
// wrapper (the DbSlice constructor) maps a pending report into the proactor's highest
// re-scheduling frequency with no backoff, so this pegs a shard's core at 100% doing zero work.
// Reject at boot so a misconfigured value never reaches that path.
TEST(MultiMasterFlags, TombstoneGcBudgetRejectsZero) {
  absl::FlagSaver saver;
  absl::SetFlag(&FLAGS_active_replica, true);
  absl::SetFlag(&FLAGS_multi_master_tombstone_gc_budget, 0);
  EXPECT_FALSE(ValidateMultiMasterFlags());

  absl::SetFlag(&FLAGS_multi_master_tombstone_gc_budget, 64);
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

// P4-2 adds RDB opcode 221. A pre-P4-2 consumer advertises version 65 and cannot parse that
// opcode, so it must be rejected at admission rather than failing partway through full sync.
TEST_F(ActiveReplicaFamilyTest, ReplconfRefusesPreMvccSnapshotConsumer) {
  EXPECT_EQ("OK", Run({"replconf", "drakey-version", "65"}));
  auto resp = Run({"replconf", "capa", "dragonfly"});
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

  // drakeydb: review wave 2 (F5, MINOR) -- the positive half of NonActiveInfoHasNoActiveFields
  // below: these three must actually show up when active, or gating them behind IsActiveReplica()
  // could vacuously hide them always instead of only outside --active_replica.
  std::string mem_info{ToSV(Run({"info", "memory"}).GetBuf())};
  EXPECT_NE(std::string::npos, mem_info.find("mvcc_table_bytes:"));
  EXPECT_NE(std::string::npos, mem_info.find("mvcc_entries:"));
  EXPECT_NE(std::string::npos, mem_info.find("mvcc_tombstones:"));
  // drakeydb: P4-3 Task 8 -- mvcc_tombstones_dropped (Task 2) beside mvcc_tombstones, same gate.
  EXPECT_NE(std::string::npos, mem_info.find("mvcc_tombstones_dropped:"));
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

  // drakeydb: review wave 2 (F5, MINOR) -- server_family.cc used to append mvcc_table_bytes/
  // mvcc_entries/mvcc_tombstones to INFO memory unconditionally; a non-active node emitted all
  // three as 0, the only observable delta from upstream on this command (the journal wire itself
  // was already byte-identical). Gated on IsActiveReplica(), matching mvcc_unstamped_writes/
  // mvcc_clock_ahead_ms/mvcc_stale_epoch's existing gate in the "replication" section above.
  //
  // Falsifying: deleting the `if (IsActiveReplica())` guard around these three appends
  // (server_family.cc) makes all three EXPECT_EQ below fail (found instead of npos).
  std::string mem_info{ToSV(Run({"info", "memory"}).GetBuf())};
  EXPECT_EQ(std::string::npos, mem_info.find("mvcc_table_bytes:"));
  EXPECT_EQ(std::string::npos, mem_info.find("mvcc_entries:"));
  EXPECT_EQ(std::string::npos, mem_info.find("mvcc_tombstones:"));
  // drakeydb: P4-3 Task 8 -- mvcc_tombstones_dropped must be gated identically to its siblings.
  EXPECT_EQ(std::string::npos, mem_info.find("mvcc_tombstones_dropped:"));
}

// drakeydb: P4-4 Task A1 -- NoteLwwDrop touches ServerState::tlocal(), which only resolves on a
// pool proactor thread (see this file's own ApplyReplicatedCommand/
// ApplyOnePeerWriteToEveryShard comments above for the SIGSEGV this fixture already learned that
// lesson from); mvcc_test.cc's plain TESTs run on the gtest main thread instead, so this one test
// lives here rather than beside ClassifyJournaledCommand/LwwGuardActive/LwwShouldDropKey/
// IncomingStamp in mvcc_test.cc. Three calls, not one, so a bug that sets the counter to a fixed
// value (e.g. `= 1` instead of `+= 1`) would still be caught.
TEST_F(MultiMasterFamilyTest, NoteLwwDropIncrementsCounterByExactlyOnePerCall) {
  auto& shard_set_ref = *shard_set;
  uint64_t before = 0, after = 0;
  shard_set_ref.Await(0, [&] { before = ServerState::tlocal()->stats.multimaster_lww_dropped; });

  shard_set_ref.Await(0, [&] { NoteLwwDrop("SET", "k1"); });
  shard_set_ref.Await(0, [&] { NoteLwwDrop("MSET", "k2"); });
  shard_set_ref.Await(0, [&] { NoteLwwDrop("DEL", "k3"); });

  shard_set_ref.Await(0, [&] { after = ServerState::tlocal()->stats.multimaster_lww_dropped; });
  EXPECT_EQ(after - before, 3u);
}

// drakeydb: P4-1 Task 5 -- the side table on DbTable. Storage only; nothing writes to it outside
// these tests until Task 7. --active_replica is boot-only (DbTable reads it at construction), so
// the flag must be set before BaseFamilyTest::SetUp() runs, not in the constructor/TearDown body.
class MvccStoreTest : public BaseFamilyTest {
 protected:
  void SetUp() override {
    // drakeydb: P4-1 Task 7's original comment here explained why this fixture used to need its
    // own explicit journal::StartInThread() call (LogAutoJournalOnShard/RecordJournal gate on
    // shard->journal(), off by default in tests): FLAGS_active_replica is set to true on the line
    // below, BEFORE BaseFamilyTest::SetUp() (which calls ResetService() -> ServerFamily::Init()),
    // so fix round 1's boot-time journal start (server_family.cc, gated on IsActiveReplica())
    // already covers every write in this fixture -- removed in fix round 2 (R2), which also
    // removed the identical redundancy from seven ReaperJournalFamilyTest tests in fix round 1
    // (F4): a fixture-level explicit start left here would mask a regression of the production
    // fix from all of this fixture's tests, the same masking F4 closed for the reaper tests.
    absl::SetFlag(&FLAGS_active_replica, true);
    BaseFamilyTest::SetUp();
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

  // drakeydb: Phase 4 Task 9 -- drives a JournalExecutor exactly the way a peer link's applier
  // does (SetApplyOrigin + SetApplyMvcc before Execute), tagging the command as authored by
  // `origin_idx` with author stamp `mvcc`. There is no shared harness for this: the same
  // SetApplyOrigin+Execute shape is inlined per-test at OriginJournalFamilyTest's
  // ApplyOriginTagsJournalEntries (this file) -- copied here rather than reused, since that
  // fixture pins num_shards=1 and never sets FLAGS_active_replica, unlike this one. Constructed
  // and driven on shard 0's own proactor thread via LaunchFiber, not shard_set->Await: Execute()
  // calls into Service::DispatchCommand, which needs ServerState::tlocal() to resolve on the
  // calling thread, and shard_set->Await would run this directly on shard 0's own
  // TxQueue-processing fiber, self-deadlocking the moment a dispatched command needs that same
  // queue to schedule a hop.
  // drakeydb: P4-4 Task A3 -- extended with `lww_guard`, forwarded verbatim to
  // JournalExecutor::SetApplyLwwGuard (a per-link setting, so it is set once here alongside
  // SetApplyOrigin/SetApplyMvcc rather than per entry -- see that method's own comment,
  // executor.h). Every pre-existing caller now passes false, preserving its prior meaning
  // (unguarded apply); new A3 tests pass true to exercise the veto.
  facade::DispatchResult ApplyReplicatedCommand(std::vector<std::string> args, uint32_t origin_idx,
                                                uint64_t mvcc, bool lww_guard) {
    facade::DispatchResult dispatch_result = facade::DispatchResult::ERROR;
    pp_->at(0)
        ->LaunchFiber([&] {
          JournalExecutor executor(service_.get());
          executor.SetApplyOrigin(origin_idx);
          executor.SetApplyMvcc(mvcc);
          executor.SetApplyLwwGuard(lww_guard);

          journal::ParsedEntry::CmdData cmd_data;
          cmd_data.Assign(args.begin(), args.end(), args.size());
          dispatch_result = executor.Execute(0, cmd_data);
        })
        .Join();
    return dispatch_result;
  }

  // drakeydb: Phase 4 Task 9, fix round (F1v2) -- constructs a real DflyShardReplica directly, no
  // socket, exactly mirroring Replica::InitiateDflySync's production construction call
  // (replica.cc) -- then, for each shard, applies a SET to a key that hashes to that shard
  // through the flow's real ExecuteTx, and returns the keys for the caller to check the
  // resulting stamps (see below for why assertions live there, not here). Defined as a genuine
  // member of this exact friended class (`friend class MvccStoreTest;`, replica.h), not inlined
  // into a TEST_F body: gtest generates a TEST_F's body as a method of a *derived* class, and
  // friendship is not inherited (same reasoning as DflyShardReplicaOriginTest in
  // peer_replication_test.cc).
  //
  // The origin_idx -> origin_hash registration is done manually here via
  // shard_set->pool()->AwaitBrief, the same shape
  // AppliedWriteOnFreshKeyKeepsAuthorStampVerbatim above uses -- NOT via DflyShardReplica's
  // constructor. An earlier version of this helper relied on
  // the constructor to do that registration (mirroring what was, at the time, production
  // behavior); that production behavior was reverted after it crashed the server (SIGSEGV,
  // reproduced 5/10 runs of test_active_replica_single_peer_replaces) -- see
  // DflyShardReplica's constructor and Replica::InitiateDflySync's shard_cb (both replica.cc) for
  // the full account. The registration now happens in InitiateDflySync's shard_cb, which this
  // test does not call (it constructs DflyShardReplica directly, bypassing Replica entirely, the
  // same way DflyShardReplicaOriginTest does for origin_idx) -- so this helper reproduces the
  // broadcast manually instead. That means this test no longer pins "does flow construction
  // register the hash automatically" (nothing in this file's reach does, post-fix; that
  // property now lives in InitiateDflySync, a private Replica method with no no-socket
  // construction path); what it still genuinely proves is multi-shard correctness of the
  // CONSUMING side -- ExecuteTx's real, per-shard application of an author's mvcc/origin_hash --
  // which AppliedWriteOnFreshKeyKeepsAuthorStampVerbatim above does not cover, since that test
  // only ever touches one key/shard.
  //
  // Construction AND every ExecuteTx call run inside one pp_->at(0)->LaunchFiber(...).Join(),
  // matching ApplyReplicatedCommand above: Execute() calls into Service::DispatchCommand, which
  // needs ServerState::tlocal() to resolve on the calling thread, and that's only ever
  // initialized on the pool's own proactor threads -- confirmed the hard way, by first writing
  // this without the wrapper and hitting a SIGSEGV in dfly::ServerState::gstate() (see
  // task-9-report.md for the verbatim crash -- a different crash from the constructor one above,
  // hit and fixed earlier in the same investigation).
  //
  // Assertions on the applied stamps are deliberately NOT inside the LaunchFiber lambda below:
  // gtest's ASSERT_* macros expand to a bare `return`, which only unwinds the immediately
  // enclosing function -- here, the lambda -- not ApplyOnePeerWriteToEveryShard itself, so an
  // in-lambda ASSERT_* failure would silently skip the remaining shards in THIS loop while the
  // caller carries on regardless, rather than actually stopping the test. Collecting keys here
  // and asserting on the resulting stamps after .Join() (ordinary control flow, not a lambda)
  // avoids that trap.
  std::vector<std::string> ApplyOnePeerWriteToEveryShard(uint32_t origin_idx, uint64_t origin_hash,
                                                         uint64_t author_mvcc) {
    const unsigned num_shards = shard_set->size();
    std::vector<std::string> keys(num_shards);
    for (unsigned target_shard = 0; target_shard < num_shards; ++target_shard) {
      for (int i = 0;; ++i) {
        CHECK_LT(i, 10000) << "could not find a key hashing to shard " << target_shard;
        keys[target_shard] = absl::StrCat("k", i);
        if (Shard(keys[target_shard], num_shards) == target_shard)
          break;
      }
    }

    shard_set->pool()->AwaitBrief([origin_idx, origin_hash](unsigned, auto*) {
      MvccStamper::tlocal()->RegisterOriginHash(origin_idx, origin_hash);
    });

    DflyShardReplica::ServerContext ctx{"127.0.0.1", 1, {}};
    MasterContext master_context;
    master_context.num_flows = num_shards;
    auto multi_shard_exe = std::make_shared<MultiShardExecution>();
    RdbLoadContext load_context;
    std::vector<bool> dispatch_ok(num_shards, false);
    pp_->at(0)
        ->LaunchFiber([&] {
          DflyShardReplica flow(ctx, master_context, /*flow_id=*/0, service_.get(), multi_shard_exe,
                                &load_context, origin_idx, /*peer_mode=*/true);
          for (unsigned target_shard = 0; target_shard < num_shards; ++target_shard) {
            TransactionData tx_data;
            tx_data.dbid = 0;
            tx_data.mvcc = author_mvcc;
            std::vector<std::string> parts{"SET", keys[target_shard], "v"};
            tx_data.command.Assign(parts.begin(), parts.end(), parts.size());

            // A single-key SET is never IsGlobalCmd(), so this takes ExecuteTx's non-global
            // path -- executor_->SetApplyMvcc(tx_data.mvcc); return executor_->Execute(...) --
            // the same call this task wires in production (replica.cc, ExecuteTx's first
            // branch). Fresh ExecutionState per call: IsRunning() must read true at entry, and
            // nothing here ever cancels or errors it, so reusing one is equally valid; a fresh
            // one just keeps each iteration visibly independent.
            ExecutionState exec_st;
            dispatch_ok[target_shard] = flow.ExecuteTx(std::move(tx_data), &exec_st);
          }
        })
        .Join();

    for (unsigned target_shard = 0; target_shard < num_shards; ++target_shard) {
      EXPECT_TRUE(dispatch_ok[target_shard]) << "ExecuteTx failed for key '" << keys[target_shard]
                                             << "' (target shard " << target_shard << ")";
    }
    return keys;
  }

  // drakeydb: P4-4 Task A3 -- registers `origin_idx` -> `origin_hash` on EVERY shard thread, the
  // same shape AppliedWriteOnFreshKeyKeepsAuthorStampVerbatim (above) and
  // ApplyOnePeerWriteToEveryShard (above) both use directly: MvccStamper::tlocal() is per-thread,
  // and ShouldDropForLww's own IncomingStamp call (transaction.cc) runs on whichever shard thread
  // owns the guarded key, so registering on only one thread would DCHECK-fail (or silently fail
  // open in release) the moment a test's key lands on a different shard.
  void RegisterPeerOriginHash(uint32_t origin_idx, uint64_t origin_hash) {
    shard_set->pool()->AwaitBrief([origin_idx, origin_hash](unsigned, auto*) {
      MvccStamper::tlocal()->RegisterOriginHash(origin_idx, origin_hash);
    });
  }

  // drakeydb: P4-4 Task A3 -- ServerState::Stats::multimaster_lww_dropped is thread-local (one
  // ServerState per shard/proactor thread), same shape as unstamped_writes
  // (PureWriteWorkloadLeavesNoUnstampedWrites, above) -- summed across every shard rather than
  // read off shard 0, since a guarded test's key can land on any of them.
  uint64_t TotalLwwDropped() {
    std::atomic<uint64_t> total{0};
    shard_set->pool()->AwaitBrief(
        [&](unsigned, auto*) { total += ServerState::tlocal()->stats.multimaster_lww_dropped; });
    return total.load();
  }

  // drakeydb: P4-4 Task A3 -- see TotalLwwDropped above; every drop test also asserts this did NOT
  // advance, proving the veto took the "return OpStatus::OK without ever arming the key" path
  // rather than arming and then abandoning it.
  uint64_t TotalUnstampedWrites() {
    std::atomic<uint64_t> total{0};
    shard_set->pool()->AwaitBrief(
        [&](unsigned, auto*) { total += MvccStamper::tlocal()->stats().unstamped_writes; });
    return total.load();
  }
};

namespace {
// drakeydb: Phase 4, P4-1 Task 10 -- sums TEST_VerifyMvccTable(0) (db_slice.cc) across every
// shard. Each shard's callback writes to its own index of `per_shard`, never a shared accumulator
// -- shard_set->RunBriefInParallel dispatches onto each shard's own proactor thread, so a naive
// `mismatches += ...` shared across threads would be a data race (this fixture does not pin
// num_shards=1, unlike OriginJournalFamilyTest elsewhere in this file, so relying on it would be
// exactly the single-proactor-only trap: it would happen to pass here but be silently wrong).
// Routes through Namespace::GetDbSlice (the ReaperJournalFamilyTest precedent above in this
// file), not a nonexistent EngineShard::db_slice() -- and deliberately through
// GetDefaultNamespace() specifically, which is what makes TEST_VerifyMvccTable's own default-
// namespace gate (db_slice.cc) actually engage here instead of short-circuiting to 0.
//
// drakeydb: P4-3 Task 2 -- moved ahead of TEST_F(MvccStoreTest, SideTableIsAllocatedInActiveMode)
// (was originally defined much later in this file, after its first caller); a free function must
// be declared/defined before its first use in the same translation unit, and
// SameShardRenameToFreshDestKeepsTheInvariant below is now this helper's earliest caller.
size_t SumMvccMismatchesAcrossShards() {
  std::vector<size_t> per_shard(shard_set->size(), 0);
  shard_set->RunBriefInParallel([&](EngineShard* shard) {
    DbSlice& db_slice = namespaces->GetDefaultNamespace().GetDbSlice(shard->shard_id());
    per_shard[shard->shard_id()] = db_slice.TEST_VerifyMvccTable(0);
  });
  size_t total = 0;
  for (size_t m : per_shard)
    total += m;
  return total;
}
}  // namespace

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

// drakeydb: P4-1 Task 8 -- direct coverage for EraseMvcc's string_view overload. The test above
// only ever exercises the PrimeKey overload; PerformDeletionAtomic's delete path (this task) now
// calls the string_view one exclusively, to avoid a GetSlice() scratch-copy on every delete (see
// db_slice.cc). A regression confined to that overload -- e.g. it silently no-ops, or looks up
// the wrong bucket -- would still leave the PrimeKey-overload test above green.
TEST_F(MvccStoreTest, EraseMvccStringViewOverloadRemovesTheStampAndDecrementsCount) {
  Run({"set", "k", "v"});
  auto& shard_set_ref = *shard_set;
  const ShardId sid = Shard("k", shard_set_ref.size());
  const MvccStamp written{777, 111};

  shard_set_ref.Await(sid, [&] {
    namespaces->GetDefaultNamespace().GetCurrentDbSlice().SetMvcc(0, std::string_view{"k"},
                                                                  written);
  });
  ASSERT_EQ(GetMetrics().db_stats[0].mvcc_entries, 1u);

  shard_set_ref.Await(sid, [&] {
    namespaces->GetDefaultNamespace().GetCurrentDbSlice().EraseMvcc(0, std::string_view{"k"});
  });

  std::optional<MvccStamp> got;
  shard_set_ref.Await(
      sid, [&] { got = namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, "k"); });
  EXPECT_FALSE(got.has_value()) << "EraseMvcc(string_view) must remove the stamp";
  EXPECT_EQ(GetMetrics().db_stats[0].mvcc_entries, 0u)
      << "EraseMvcc(string_view) must decrement mvcc_entries";
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

// drakeydb: P4-2 Task 4 -- TDD RED for the mvcc_table_bytes key-duplication blind spot (P4-1
// Task 12's memory benchmark finding; see server_family.cc's mvcc_table_bytes comment). DbTable::
// mvcc heap-allocates its own copy of every key over CompactObj::kInlineLen (16 B, SetMvcc,
// db_slice.cc), but mvcc_table_bytes used to equal DashTable::mem_usage() alone, which never sees
// that second copy -- a table of 32-char keys and a table of 8-char keys reported identical
// mvcc_table_bytes. This proves that gap is closed. Falsification (task-4-report.md): reverting
// the mvcc_key_dup_bytes accounting in db_slice.cc's SetMvcc/EnsureMvcc/EraseMvcc collapses
// delta_long to 0, same as delta_short, and the EXPECT_GT below fails.
TEST_F(MvccStoreTest, TableBytesAccountsForDuplicatedKeyHeapBytes) {
  Run({"set", "k", "v"});
  auto& shard_set_ref = *shard_set;
  const ShardId sid = Shard("k", shard_set_ref.size());

  // Stays well under the initial segment's ~840-slot capacity (see
  // TableMemoryGrowsWithWritesAndDropsOnFlush above) for both phases combined, so
  // DashTable::mem_usage() itself never grows across either phase -- any mvcc_table_bytes delta
  // below is then attributable solely to duplicated key heap bytes, not table structure.
  constexpr int kN = 300;

  // Builds an exact-length, unique-per-i, non-numeric (leading letter, so CompactObj::SetString's
  // string2ll fast path never fires) key.
  auto make_key = [](char prefix, size_t len, int i) {
    std::string suffix = std::to_string(i);
    std::string key(len, '0');
    key[0] = prefix;
    key.replace(key.size() - suffix.size(), suffix.size(), suffix);
    return key;
  };

  auto bytes_now = [&] { return GetMetrics().db_stats[0].mvcc_table_bytes; };

  size_t before_short = bytes_now();
  shard_set_ref.Await(sid, [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    for (int i = 0; i < kN; ++i) {
      PrimeKey pk{make_key('s', 8, i)};
      db_slice.SetMvcc(0, pk, MvccStamp{uint64_t(i) + 1, 1});
    }
  });
  size_t delta_short = bytes_now() - before_short;

  size_t before_long = bytes_now();
  shard_set_ref.Await(sid, [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    for (int i = 0; i < kN; ++i) {
      PrimeKey pk{make_key('l', 32, i)};
      db_slice.SetMvcc(0, pk, MvccStamp{uint64_t(i) + 1, 1});
    }
  });
  size_t delta_long = bytes_now() - before_long;

  // 8-char keys never leave CompactObj's 16-byte inline union (kInlineLen == 16): MallocUsed()
  // == 0 for every one (CompactObj::HasAllocated() is false for any inline taglen_), so
  // delta_short must be exactly 0 regardless of the fix.
  EXPECT_EQ(delta_short, 0u) << "8-char keys are inline and must not contribute heap bytes";

  // Every 32-char key is forced through CompactObj::EncodeString (str.size() > 20 skips the
  // int/inline fast paths entirely in SetString, compact_object.cc) and lands on SMALL_TAG or
  // LARGE_STR_TAG, both heap-allocated. EncodeString's own DCHECK_GT(encoded.size(), kInlineLen)
  // guarantees encoded.size() >= 17, so SmallString's remainder (size - kPrefLen(10),
  // small_string.h) is >= 7 heap bytes per key even in the best case, before
  // mi_malloc_usable_size's allocator rounding (which can only round up). Floor at kN * 7
  // accordingly: mathematically safe, not just empirically likely.
  EXPECT_GT(delta_long, static_cast<size_t>(kN) * 7)
      << "delta_short=" << delta_short << " delta_long=" << delta_long
      << " -- mvcc_table_bytes must grow for the 32-char table's duplicated key heap bytes, "
         "which DashTable::mem_usage() alone (the pre-fix formula) cannot see";
}

// drakeydb: P4-2 Task 4, review fix round 1 -- closes a gap flagged in review: no existing test
// exercised EraseMvcc's `db.stats.mvcc_key_dup_bytes -= it->first.MallocUsed();` line (db_slice.cc)
// with a non-zero value. Every DEL in the pre-existing C++ suite deletes inline (<= 16 char) keys,
// whose MallocUsed() is always 0, so a regression that silently breaks the erase-side accounting --
// e.g. moving that `-=` to *after* db.mvcc->Erase(it), the exact hazard the comment right above it
// warns about, since Erase() destroys the key and MallocUsed() reads 0 afterwards -- would pass
// every existing test while permanently over-reporting mvcc_table_bytes on every delete of a
// duplicated-key entry. Drives real SET + DEL through the normal command path (not a direct
// EraseMvcc call) so this exercises the actual production route: PerformDeletionAtomic -> Disarm ->
// EraseMvcc(string_view) (db_slice.cc), the same site review flagged.
// Falsification (task-4-report.md, fix round 1): moving the `-=` after Erase() makes the final
// EXPECT_EQ below fail with mvcc_table_bytes stuck above baseline by the total un-subtracted
// duplicated-key bytes.
// drakeydb: P4-3 Task 2 -- since Task 2, a plain DEL tombstones (keeps the slot, and so its
// duplicated-key heap bytes) instead of erasing. This test is about EraseMvcc's own accounting
// specifically ("on a REAL delete", per its name), which tombstoning-by-default would defeat --
// --multi_master_tombstone_ttl=0 opts back into the pre-Task-2 always-erase behavior for exactly
// that purpose (see TombstonesEnabled(), multi_master.h).
TEST_F(MvccStoreTest, EraseAccountsForDuplicatedKeyHeapBytesOnRealDelete) {
  absl::SetFlag(&FLAGS_multi_master_tombstone_ttl, 0);
  absl::Cleanup restore_ttl = [] { absl::SetFlag(&FLAGS_multi_master_tombstone_ttl, 600); };

  Run({"set", "k", "v"});
  size_t baseline = GetMetrics().db_stats[0].mvcc_table_bytes;

  // 32-char keys: str.size() > 20 skips CompactObj::SetString's int/inline fast paths entirely
  // (compact_object.cc), so every one of these is unconditionally heap-allocated -- same
  // reasoning as TableBytesAccountsForDuplicatedKeyHeapBytes above. kN stays well under the
  // initial segment's ~840-slot capacity (TableMemoryGrowsWithWritesAndDropsOnFlush above) so
  // DashTable::mem_usage() itself never grows across insert or delete -- isolating both deltas
  // below to mvcc_key_dup_bytes alone.
  constexpr int kN = 100;
  std::vector<std::string> keys;
  keys.reserve(kN);
  for (int i = 0; i < kN; ++i) {
    std::string suffix = std::to_string(i);
    std::string key(32, '0');
    key[0] = 'e';
    key.replace(key.size() - suffix.size(), suffix.size(), suffix);
    keys.push_back(std::move(key));
  }

  for (const auto& key : keys) {
    Run({"set", key, "v"});
  }
  size_t after_insert = GetMetrics().db_stats[0].mvcc_table_bytes;
  ASSERT_GT(after_insert, baseline + static_cast<size_t>(kN) * 7)
      << "expected the 32-char keys to carry duplicated heap bytes before delete -- see "
         "TableBytesAccountsForDuplicatedKeyHeapBytes above for the >= 7 B/key derivation";

  for (const auto& key : keys) {
    Run({"del", key});
  }
  size_t after_delete = GetMetrics().db_stats[0].mvcc_table_bytes;

  EXPECT_EQ(after_delete, baseline)
      << "after_insert=" << after_insert << " after_delete=" << after_delete
      << " baseline=" << baseline
      << " -- mvcc_table_bytes must drop back to the pre-insert baseline once every "
         "duplicated-key entry is deleted through the real DEL path";
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

// RDB loading runs in a separate fiber on the shard thread. It must finalize the loaded value's
// normal bookkeeping without either arming that value or clearing a transaction arm that was
// already pending on the same thread. This pins the mechanism directly: the pending write keeps
// its arm and receives the later commit, while the loaded value retains its explicit persisted
// stamp.
//
// drakeydb: P4-2 Task 2 -- extended from P4-1 Task 10's original version, which only exercised
// the {0,0} snapshot fallback (the only outcome the loader could produce back then). The loaded
// key here now carries a non-zero stamp, simulating CreateObjectOnShard's new
// item->has_mvcc ? item->mvcc : MvccStamp{} apply (rdb_load.cc) for a key whose RDB stream
// carried a RDB_OPCODE_DF_MVCC record. The property under test is unchanged -- loader
// finalization must not touch another transaction's pending arm, and must not let that other
// transaction's Commit() touch the loaded key -- only the concrete value the loaded key ends up
// with changes, from {0,0} to the persisted stamp.
TEST_F(MvccStoreTest, LoaderFinalizationPreservesAnotherTransactionsPendingArm) {
  constexpr uint64_t kPendingMvcc = 0x1234'5678;
  // drakeydb: P4-3 Task 3, review fix (C2i) -- leading nibble changed from 0xA to 0x2 (otherwise
  // an arbitrary, purely-for-recognizability constant, unrelated to this test's actual subject):
  // 0xA's top bit set bit 63 of the packed stamp, i.e. the tombstone bit, incidentally. SetMvcc
  // (the live-key setter this test exercises below) now masks that bit on every write -- a live
  // key carrying it is invalid state -- so the unmasked original value could never round-trip
  // through SetMvcc verbatim again, which is exactly what this constant is asked to do below.
  // This test is about arm/commit isolation, not tombstones, so the fix is the constant, not the
  // new masking behavior.
  const MvccStamp kPersistedStamp{0x2AAABBBBCCCC1111ULL, 0xDEADBEEFULL};
  std::optional<MvccStamp> prepared_stamp;
  std::optional<MvccStamp> pending_stamp;
  std::optional<MvccStamp> loaded_stamp;

  shard_set->Await(0, [&] {
    Namespace& default_ns = namespaces->GetDefaultNamespace();
    DbSlice& db_slice = default_ns.GetCurrentDbSlice();
    DbContext db_cntx{&default_ns, 0, GetCurrentTimeMs()};

    PrimeValue pending_value;
    pending_value.SetString("pending-value");
    auto pending = db_slice.AddOrUpdate(db_cntx, "pending-key", std::move(pending_value), 0);
    ASSERT_TRUE(pending);
    pending->post_updater.Run();

    prepared_stamp = db_slice.GetMvcc(0, "pending-key");
    ASSERT_TRUE(prepared_stamp.has_value());
    EXPECT_TRUE(prepared_stamp->Empty())
        << "PostUpdate must prepare a zero-authority slot before journal commit";

    PrimeValue loaded_value;
    loaded_value.SetString("loaded-value");
    auto loaded = db_slice.AddOrUpdate(db_cntx, "loaded-key", std::move(loaded_value), 0);
    ASSERT_TRUE(loaded);
    db_slice.SetMvcc(0, std::string_view{"loaded-key"}, kPersistedStamp);
    loaded->post_updater.RunWithoutMvccArm();

    MvccStamper::tlocal()->Commit(
        kPendingMvcc, 0,
        // drakeydb: P4-4 Task A5 fix round 1 -- CommitFn's 4th (tombstone) and 5th (the arm's
        // captured prev_stamp) arguments are both unused here; this test is about arm/commit
        // isolation, not the floor.
        [&](DbIndex db, std::string_view key, const MvccStamp& stamp, bool, const MvccStamp&) {
          db_slice.SetExistingMvcc(db, key, stamp);
        });
    pending_stamp = db_slice.GetMvcc(0, "pending-key");
    loaded_stamp = db_slice.GetMvcc(0, "loaded-key");
  });

  ASSERT_TRUE(pending_stamp.has_value());
  EXPECT_EQ(pending_stamp->Mvcc(), kPendingMvcc)
      << "loader finalization must not clear another transaction's pending arm";
  ASSERT_TRUE(loaded_stamp.has_value());
  EXPECT_EQ(*loaded_stamp, kPersistedStamp)
      << "a loaded key must not be swept into an unrelated COMMAND commit, and must keep exactly "
         "its persisted stamp -- not {0,0}, and not the concurrent transaction's stamp";
}

// drakeydb: review fix round 1 (F1) -- a write through a non-default namespace must leave no
// stamp anywhere: not in its own namespace's side table (not propagated to any peer, so under
// "a key's stamp advances iff that same stamp is propagated" it must never advance), and not on
// a same-named key in the default namespace either. The journal assertions below are the primary
// safety boundary because every journal COMMAND is applied to the peer's default namespace.
//
// In production, ns1 is reached via ServerFamily::DoAuth (server_family.cc:2109-2120), which sets
// `cntx->ns = &namespaces->GetOrInsert(cred.ns)` for an ACL user carrying a NAMESPACE: directive
// (acl_family.cc's MaybeParseNamespace). This test exercises the behavior AFTER namespace
// selection via RunViaNamespace, not the ACL SETUSER/AUTH selection chain itself:
// BaseFamilyTest::Run(id, slice) (test_utils.cc) unconditionally
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
// correctness -- MaybeParseNamespace, DoAuth -- is pre-existing and unchanged by this task).
//
// The journal consumer is the non-vacuous boundary check: SET takes the explicit RecordJournal
// path while LPUSH takes the automatic path, and either entry would be applied in a peer's default
// namespace because the wire has no namespace identity. The side-table assertions independently
// prove that the local-only write neither acquires authority in ns1 nor perturbs the default
// namespace's same-named key.
TEST_F(MvccStoreTest, NonDefaultNamespaceWriteLeavesNoStampAnywhere) {
  // A key of the same name, already stamped in the default namespace, to prove the ns1 write
  // below does not perturb it.
  ASSERT_EQ(Run({"set", "shared", "v0"}), "OK");
  auto original = StampOf("shared");
  ASSERT_TRUE(original.has_value());

  Namespace& ns1 = namespaces->GetOrInsert("ns1");
  class CountingJournalConsumer final : public journal::JournalConsumerInterface {
   public:
    void ConsumeJournalChange(const journal::JournalChangeItem& item) override {
      if (item.journal_item.opcode == journal::Op::COMMAND)
        ++commands;
    }
    void ThrottleIfNeeded() override {
    }

    std::atomic<size_t> commands{0};
  } consumer;
  std::vector<uint32_t> consumer_ids(shard_set->size());
  shard_set->RunBriefInParallel([&](EngineShard* shard) {
    consumer_ids[shard->shard_id()] = journal::RegisterConsumer(&consumer);
  });
  absl::Cleanup unregister_consumer = [&] {
    shard_set->RunBriefInParallel(
        [&](EngineShard* shard) { journal::UnregisterConsumer(consumer_ids[shard->shard_id()]); });
  };

  ASSERT_EQ(RunViaNamespace(&ns1, {"set", "shared", "v1"}), "OK");
  ASSERT_EQ(RunViaNamespace(&ns1, {"lpush", "automatic", "v"}).GetInt(), 1);
  EXPECT_THAT(RunViaNamespace(&ns1, {"debug", "mvcc", "shared"}), ErrArg("default namespace"));
  const std::string_view helper_key = "namespace-helper-key";
  shard_set->Await(Shard(helper_key, shard_set->size()), [&] {
    DbContext ns_cntx{&ns1, 0, GetCurrentTimeMs()};
    RecordDelete(ns_cntx, helper_key);
    RecordDerivedDelete(ns_cntx, helper_key);
    RecordExpiryBlocking(ns_cntx, helper_key);
  });

  std::move(unregister_consumer).Invoke();
  EXPECT_EQ(consumer.commands.load(), 0u)
      << "neither transaction writes nor direct delete helpers from a non-default namespace may "
         "enter the namespace-blind journal";

  const ShardId sid = Shard("shared", shard_set->size());
  std::optional<MvccStamp> ns1_stamp;
  shard_set->Await(sid, [&] { ns1_stamp = ns1.GetDbSlice(sid).GetMvcc(0, "shared"); });
  EXPECT_FALSE(ns1_stamp.has_value())
      << "a non-default-namespace write must never be stamped -- it is not propagated to any "
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

// drakeydb: P4-3 Task 3, review fix round 5 (R1) -- a namespace created at RUNTIME must register
// no tombstone-GC on-idle task on any shard.
//
// helio's RemoveOnIdleTask (proactor_base.cc, not ours to edit) pops only TRAILING empty slots and
// never clamps ProactorBase::on_idle_next_, while RunOnIdleTasks short-circuits only on a fully
// EMPTY array. The invariant every registration site must keep is therefore: never remove a
// trailing on-idle task while an earlier-registered one is still live -- doing so shrinks the
// array below a cursor that may still point at the old top index, and a later idle tick indexes
// past the end (DCHECK_LT in a debug build, an out-of-bounds read in a release one).
//
// The DEFAULT namespace's task keeps that invariant by construction order: EngineShardSet::Init
// builds its DbSlices (engine_shard_set.cc) BEFORE StartPeriodicHeartbeatFiber registers
// "defrag", so it is id 0 and "defrag" is id 1, and Service::Shutdown removes id 0 first
// (main_service.cc, via Namespaces::StopTombstoneGc, ahead of PreShutdown) -- leaving a hole,
// popping nothing. A namespace created at runtime -- production reaches this through
// ServerFamily::DoAuth's `cntx->ns = &namespaces->GetOrInsert(cred.ns)` for an ACL user carrying
// a NAMESPACE: directive (server_family.cc), which is exactly the GetOrInsert call this test
// makes -- builds its DbSlices AFTER "defrag" is already registered, so its task would land at an
// id ABOVE "defrag"'s. The same StopTombstoneGc fan-out would then remove a TRAILING task with
// two live entries beneath it, and the array WOULD shrink.
//
// This test pins the gate rather than the abort deliberately: the abort additionally requires
// on_idle_next_ to be sitting at the top index at that exact moment (the budget-exhausted exit),
// which nothing in reach can force without editing helio. The gate is the property that makes the
// abort unreachable, and it is otherwise entirely invisible -- a non-default namespace's
// tombstone count is already always 0 (the F1 gate asserted below), so nothing else observes
// whether the task exists.
TEST_F(MvccStoreTest, NonDefaultNamespaceRegistersNoTombstoneGcTask) {
  Namespace& ns1 = namespaces->GetOrInsert("ns1-gc");

  for (ShardId sid = 0; sid < shard_set->size(); ++sid) {
    shard_set->Await(sid, [&] {
      EXPECT_TRUE(namespaces->GetDefaultNamespace().GetDbSlice(sid).TEST_HasTombstoneGcTask())
          << "sanity, shard " << sid
          << ": the default namespace on an --active_replica node must register the GC task, or "
             "the non-default assertion below passes vacuously";
      EXPECT_FALSE(ns1.GetDbSlice(sid).TEST_HasTombstoneGcTask())
          << "shard " << sid
          << ": a runtime-created namespace registers its GC task ABOVE \"defrag\", so removing "
             "it at shutdown pops a trailing slot while live tasks remain -- it must not register "
             "one at all";
    });
  }

  // The gate must not have changed what a non-default namespace's deletes do: still erase, never
  // tombstone (PerformDeletionAtomic's own F1 gate, db_slice.cc), exactly as before this fix.
  const std::string_view key = "ns-gc-key";
  ASSERT_EQ(RunViaNamespace(&ns1, {"set", key, "v"}), "OK");
  ASSERT_EQ(RunViaNamespace(&ns1, {"del", key}).GetInt(), 1);

  const ShardId sid = Shard(key, shard_set->size());
  std::optional<MvccStamp> ns1_stamp;
  size_t ns1_tombstones = 0;
  shard_set->Await(sid, [&] {
    ns1_stamp = ns1.GetDbSlice(sid).GetMvcc(0, key);
    ns1_tombstones = ns1.GetDbSlice(sid).GetStats().db_stats[0].mvcc_tombstones;
  });
  EXPECT_FALSE(ns1_stamp.has_value())
      << "a non-default-namespace DEL must leave no slot behind at all -- not a tombstone the "
         "(now absent) GC task would have had to reclaim";
  EXPECT_EQ(ns1_tombstones, 0u);
  EXPECT_THAT(RunViaNamespace(&ns1, {"get", key}), ArgType(RespExpr::NIL));
}

// drakeydb: P4-1 Task 8; updated P4-3 Task 2 -- PerformDeletionAtomic must disarm on every delete
// path, or a pending arm can re-stamp a key that no longer exists (see the DoesNotResurrectAStamp
// case below). Before Task 2 this meant an erase and an absent stamp; since Task 2 a plain DEL
// (kExplicit) tombstones instead -- see ExplicitAndExpiredDeletesLeaveTombstonesEvictionDoesNot
// below for the full delete-reason matrix this is one slice of.
TEST_F(MvccStoreTest, DeleteTombstonesTheStamp) {
  Run({"set", "k", "v"});
  ASSERT_TRUE(StampOf("k").has_value());
  Run({"del", "k"});
  auto tomb = StampOf("k");
  ASSERT_TRUE(tomb.has_value()) << "a deleted key must keep its slot, as a tombstone";
  EXPECT_TRUE(tomb->IsTombstone());
}

// OpDelV2 arms the key (post_updater.Run()) before deleting and journals after. Without the
// disarm, Commit would write the DEL's own stamp onto BOTH the live pre-delete arm and the
// tombstone arm -- harmless numerically (same mvcc either way, see MvccStamperTest.
// CommitMarksOnlyTombstoneArms in mvcc_test.cc), but Disarm existing first is still what keeps a
// key armed twice before deletion (e.g. HDEL emptying a hash, see HdelEmptyingHashDoesNotResurrect
// AStamp below) from leaving a stray, un-tombstoned arm for Commit to mis-stamp.
TEST_F(MvccStoreTest, DeleteInSameCallbackDoesNotResurrectAStamp) {
  Run({"set", "k", "v"});
  // drakeydb: review fix round 1 (minor) -- guard the precondition. Without this, a total
  // stamping failure (e.g. Arm/Commit wired wrong) would leave "k" unstamped from the SET
  // already, and the assertions below would pass for the wrong reason.
  ASSERT_TRUE(StampOf("k").has_value());
  Run({"del", "k"});
  auto tomb = StampOf("k");
  ASSERT_TRUE(tomb.has_value())
      << "the DEL's own journal entry must stamp the tombstone it armed, not leave the key absent";
  EXPECT_TRUE(tomb->IsTombstone());
  EXPECT_EQ(GetMetrics().db_stats[0].mvcc_entries, 1u)
      << "the tombstone still occupies one mvcc_entries slot -- it is not erased";
}

// drakeydb: P4-3 Task 2 -- the delete-reason matrix; this task's headline test. kExplicit earns a
// tombstone at least as new as the value it replaces. kEvicted/kSlotFlush are capacity/topology
// decisions, not deletions -- a peer's copy is authoritative there, so resurrection on that
// peer's next full sync is the intended outcome, and the slot is erased exactly as every reason
// was before this task (see EvictedDeleteLeavesNoSlotOrTombstoneCredit below).
//
// kExpired is DELIBERATELY not exercised here as a tombstoning reason -- it shares
// PerformDeletionAtomic's earns_tombstone branch with kExplicit (db_slice.cc), so
// ExplicitDeleteLeavesATombstoneAtLeastAsNewAsTheValue below only needs to pin kExplicit's own
// stamp-freshness property; kExpired's equivalent coverage is
// LazyExpiryEarnsATombstoneWithSelfOrigin (this file) and
// ReaperJournalFamilyTest.MemberExpiryReaperDeleteEarnsATombstone (further down this file).
//
// drakeydb: P4-3 Task 11 -- until this task, kExpired was excluded here on purpose: two real
// call sites (DbSlice::ExpireIfNeeded, DbSlice::DeleteReapedContainer) journaled the DEL before
// calling Del()/PerformDeletionAtomic, so the tombstone arm that reason would need was created
// too late for any Commit() to consume it (the STOP finding from Task 2's report). Task 11
// reordered both sites -- delete first, journal after, matching every kExplicit site -- closing
// that gap; see the reorder's own comments in db_slice.cc for the fix and DeleteReason's comment
// (db_slice.h) for the resolution.
TEST_F(MvccStoreTest, ExplicitDeleteLeavesATombstoneAtLeastAsNewAsTheValue) {
  Run({"set", "gone", "v"});
  auto before_stamp = StampOf("gone");
  ASSERT_TRUE(before_stamp.has_value())
      << "the SET above must leave a stamp -- without one this test would dereference nullopt "
         "(undefined behavior) instead of failing here";
  const MvccStamp before = *before_stamp;
  Run({"del", "gone"});
  auto tomb = StampOf("gone");
  ASSERT_TRUE(tomb.has_value()) << "an explicit DEL must leave a tombstone";
  EXPECT_TRUE(tomb->IsTombstone());
  EXPECT_GE(tomb->Mvcc(), before.Mvcc());  // the tombstone is at least as new as the value
  EXPECT_EQ(GetMetrics().db_stats[0].mvcc_tombstones, 1u);
}

// drakeydb: P4-3 Task 2 -- driven directly via DbSlice::DelMutable(..., kEvicted), the same entry
// point production eviction uses (FreeMemWithEvictionStepAtomic, db_slice.cc), rather than
// actually forcing the server into a low-memory eviction sweep: this test is about
// PerformDeletionAtomic's reason branch, not about reproducing eviction's own trigger conditions.
TEST_F(MvccStoreTest, EvictedDeleteLeavesNoSlotOrTombstoneCredit) {
  Run({"set", "evictme", "v"});
  ASSERT_TRUE(StampOf("evictme").has_value());

  shard_set->Await(Shard("evictme", shard_set->size()), [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    DbContext cntx{&namespaces->GetDefaultNamespace(), 0, GetCurrentTimeMs()};
    auto it_upd = db_slice.FindMutable(cntx, "evictme");
    ASSERT_TRUE(IsValid(it_upd.it));
    db_slice.DelMutable(cntx, std::move(it_upd), DbSlice::DeleteReason::kEvicted);
  });

  EXPECT_FALSE(StampOf("evictme").has_value())
      << "eviction is a capacity decision, not a deletion -- it must still erase, not tombstone";
  EXPECT_EQ(GetMetrics().db_stats[0].mvcc_tombstones, 0u);
}

// drakeydb: P4-3 Task 2 -- at --multi_master_max_tombstones, a delete that would have earned a
// tombstone degrades to an erase instead (today's pre-Task-2, resurrection-on-full-sync
// behavior), and the degradation is counted in mvcc_tombstones_dropped rather than silently
// dropped or left to grow the side table unbounded. "first"/"second" must land on the SAME shard:
// the cap is enforced per (DATABASE, SHARD) pair -- table->stats.mvcc_tombstones lives on one
// DbTable per SELECT-able db index on each shard (PerformDeletionAtomic, db_slice.cc), NOT a
// single per-shard budget -- so two keys on different shards (or in different databases) would
// each see their own pair's count start at 0 and neither would degrade.
TEST_F(MvccStoreTest, DeleteAtTombstoneCapDegradesToEraseAndCountsTheDrop) {
  absl::SetFlag(&FLAGS_multi_master_max_tombstones, 1);
  absl::Cleanup restore = [] { absl::SetFlag(&FLAGS_multi_master_max_tombstones, 1000000); };

  unsigned n = shard_set->size();
  std::string first, second;
  for (int i = 0;; ++i) {
    first = absl::StrCat("cap-first-", i);
    second = absl::StrCat("cap-second-", i);
    if (Shard(first, n) == Shard(second, n))
      break;
    CHECK_LT(i, 10000) << "could not find a same-shard key pair";
  }

  Run({"set", first, "v"});
  Run({"del", first});
  auto first_tomb = StampOf(first);
  ASSERT_TRUE(first_tomb.has_value());
  EXPECT_TRUE(first_tomb->IsTombstone());
  ASSERT_EQ(GetMetrics().db_stats[0].mvcc_tombstones_dropped, 0u);

  Run({"set", second, "v"});
  Run({"del", second});
  EXPECT_FALSE(StampOf(second).has_value())
      << "this shard is already at its tombstone cap, so this delete must degrade to an erase";
  EXPECT_EQ(GetMetrics().db_stats[0].mvcc_tombstones, 1u)
      << "the cap must not be exceeded -- the degraded delete must not add a second tombstone";
  EXPECT_EQ(GetMetrics().db_stats[0].mvcc_tombstones_dropped, 1u);
}

// drakeydb: P4-3 Task 3 -- the idle-task GC that reclaims a tombstone once
// MvccStamp::DeadlineMs(ttl_ms) has passed, bounded per call by
// --multi_master_tombstone_gc_budget (MvccTable::Traverse calls, each visiting at most one
// non-empty logical bucket). Drives DbSlice::TombstoneGcStep() directly, per shard, the same way
// SumMvccMismatchesAcrossShards above drives TEST_VerifyMvccTable directly, rather than waiting
// on the real on-idle scheduler (registered from the DbSlice constructor) -- this proves the step
// function itself is correct regardless of when/how often the scheduler happens to call it.
TEST_F(MvccStoreTest, TombstoneGcReapsExpiredTombstonesWithinBudget) {
  absl::SetFlag(&FLAGS_multi_master_tombstone_ttl, 1);        // seconds
  absl::SetFlag(&FLAGS_multi_master_tombstone_gc_budget, 1);  // buckets/step -- see below
  absl::Cleanup restore = [] {
    absl::SetFlag(&FLAGS_multi_master_tombstone_ttl, 600);
    absl::SetFlag(&FLAGS_multi_master_tombstone_gc_budget, 64);
  };

  constexpr int kExpiring = 200;
  for (int i = 0; i < kExpiring; ++i)
    Run({"set", absl::StrCat("gc-expiring-", i), "v"});
  for (int i = 0; i < kExpiring; ++i)
    Run({"del", absl::StrCat("gc-expiring-", i)});
  ASSERT_EQ(GetMetrics().db_stats[0].mvcc_tombstones, static_cast<size_t>(kExpiring));

  AdvanceTime(2000);  // past the 1s (1000ms) deadline the first batch's tombstones carry

  constexpr int kSurvivors = 10;
  for (int i = 0; i < kSurvivors; ++i)
    Run({"set", absl::StrCat("gc-survivor-", i), "v"});
  for (int i = 0; i < kSurvivors; ++i)
    Run({"del", absl::StrCat("gc-survivor-", i)});
  ASSERT_EQ(GetMetrics().db_stats[0].mvcc_tombstones, static_cast<size_t>(kExpiring + kSurvivors))
      << "both batches must be tombstoned (not yet reaped) before any GC step runs";

  // One step per shard, budget=1 bucket: ExpireTablePolicy's kBucketNum=56 buckets/segment
  // (dash.h) cannot possibly be drained of kExpiring=200 tombstones by visiting just one, so this
  // is what proves the budget actually bounds a single step's work instead of draining the whole
  // table in one call.
  shard_set->RunBriefInParallel([&](EngineShard* shard) {
    DbSlice& db_slice = namespaces->GetDefaultNamespace().GetDbSlice(shard->shard_id());
    db_slice.TombstoneGcStep();
  });
  EXPECT_GT(GetMetrics().db_stats[0].mvcc_tombstones, static_cast<size_t>(kSurvivors))
      << "a budget of 1 bucket must not reap all " << kExpiring
      << " expired tombstones in a single step";

  // Drive every shard's GC to completion. pending[shard] tracks whether THAT shard still has more
  // work, mirroring SumMvccMismatchesAcrossShards' per-shard-slot pattern above -- each shard's
  // own thread only ever writes its own slot, so nothing here needs a lock or an atomic.
  std::vector<uint8_t> pending(shard_set->size(), 1);
  auto any_pending = [&pending] {
    for (uint8_t v : pending) {
      if (v)
        return true;
    }
    return false;
  };
  int guard = 0;
  while (any_pending()) {
    shard_set->RunBriefInParallel([&](EngineShard* shard) {
      DbSlice& db_slice = namespaces->GetDefaultNamespace().GetDbSlice(shard->shard_id());
      pending[shard->shard_id()] = db_slice.TombstoneGcStep() ? 1 : 0;
    });
    ASSERT_LT(++guard, 100000) << "TombstoneGcStep never converged";
  }

  EXPECT_EQ(GetMetrics().db_stats[0].mvcc_tombstones, static_cast<size_t>(kSurvivors))
      << "expired tombstones must be fully reaped; unexpired survivors from the second batch "
         "must not be";
}

// drakeydb: P4-3 Task 3, self-review finding (Critical) -- PerformDeletionAtomic (db_slice.cc)
// writes a SYNCHRONOUS zero-authority placeholder (MvccStamp{kTombstoneBit, 0}, so Mvcc()==0)
// before the delete's own journal commit lands. That placeholder's DeadlineMs(ttl_ms) is just
// ttl_ms (MsPart()==0), which is always <= any real now_ms, so unless TombstoneGcStep excludes it
// explicitly, it looks permanently "expired" for as long as it exists -- and that window is real:
// journal::RecordEntry's AddLogRecord (journal.cc) can yield on JournalStreamer::ThrottleIfNeeded
// whenever a peer link is backpressured, before Commit() overwrites the placeholder with the
// delete's real, minted stamp. If TombstoneGcStep ran during that yield and erased the
// placeholder, the eventual Commit() -> SetExistingMvcc would CHECK-fail ("MVCC slot must be
// prepared before journal commit") the instant the delete's own commit resumed. Reproduces the
// placeholder's exact shape directly (SetTombstone), the way PerformDeletionAtomic creates it,
// rather than actually stalling a peer link to hit the yield window.
TEST_F(MvccStoreTest, TombstoneGcSkipsAnUncommittedPlaceholder) {
  absl::SetFlag(&FLAGS_multi_master_tombstone_ttl, 1);  // seconds; ttl_ms == 1000
  absl::Cleanup restore = [] { absl::SetFlag(&FLAGS_multi_master_tombstone_ttl, 600); };

  const std::string key = "gc-placeholder";
  shard_set->Await(Shard(key, shard_set->size()), [&] {
    DbSlice& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    db_slice.SetTombstone(0, key, MvccStamp{MvccClock::kTombstoneBit, 0});

    // Drive the GC to the end of its lap over this (tiny) table, so the placeholder gets at
    // least one real chance to be (wrongly) reaped if the Mvcc() != 0 guard is missing.
    int guard = 0;
    while (db_slice.TombstoneGcStep())
      ASSERT_LT(++guard, 100000) << "TombstoneGcStep never converged";

    auto stamp = db_slice.GetMvcc(0, key);
    ASSERT_TRUE(stamp.has_value())
        << "GC erased a placeholder tombstone that was still mid-epoch (uncommitted)";
    EXPECT_TRUE(stamp->IsTombstone());
    EXPECT_EQ(stamp->Mvcc(), 0u);
    EXPECT_EQ(db_slice.GetStats().db_stats[0].mvcc_tombstones, 1u);

    // The eventual journal commit (Commit()'s CommitFn calls this) must still find the slot --
    // this is exactly what would CHECK-fail ("MVCC slot must be prepared before journal commit")
    // had GC erased it out from under the pending arm.
    db_slice.SetExistingMvcc(0, key, MvccStamp{MvccClock::kTombstoneBit | (5000ULL << 20), 42});
  });
}

// drakeydb: P4-3 Task 3, review fix (C1), Critical -- the reviewer's own repro: with
// --multi_master_tombstone_gc_budget=0 (bypassing ValidateMultiMasterFlags via absl::SetFlag,
// the same way a future CONFIG SET could), TombstoneGcStep returned true on every one of 5
// consecutive calls despite doing zero work, because "budget==0 with a database left to try" was
// treated identically to "ran out of budget mid-lap, real progress was made". The on-idle
// wrapper (the DbSlice constructor, db_slice.cc) maps a true return into kOnIdleMaxLevel with no
// backoff, so this would peg a shard's proactor core at 100% forever. Real expired work is set
// up first so a false return below is provably the zero-budget guard, not just "nothing to do".
TEST_F(MvccStoreTest, TombstoneGcStepNeverReportsPendingWithZeroBudget) {
  absl::SetFlag(&FLAGS_multi_master_tombstone_ttl, 1);
  absl::SetFlag(&FLAGS_multi_master_tombstone_gc_budget, 0);
  absl::Cleanup restore = [] {
    absl::SetFlag(&FLAGS_multi_master_tombstone_ttl, 600);
    absl::SetFlag(&FLAGS_multi_master_tombstone_gc_budget, 64);
  };

  const std::string key = "gc-zero-budget";
  Run({"set", key, "v"});
  Run({"del", key});
  AdvanceTime(2000);
  ASSERT_EQ(GetMetrics().db_stats[0].mvcc_tombstones, 1u);

  shard_set->Await(Shard(key, shard_set->size()), [&] {
    DbSlice& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    for (int i = 0; i < 5; ++i)
      EXPECT_FALSE(db_slice.TombstoneGcStep()) << "iteration " << i;
  });
}

// drakeydb: P4-3 Task 3, review fix (C2i), Critical -- rdb_load.cc's opcode-221 path
// (CreateObjectOnShard, rdb_load.cc:3389) installs a wire-read stamp through SetMvcc verbatim,
// with no bit-63 check unlike the KeyDB aux path (rdb_load.cc:3170) -- so a corrupted or
// malicious stream (or a plain-replica full sync, or DEBUG LOAD) carrying a bit-63-set stamp for
// a LIVE key used to plant a slot TombstoneGcStep's IsTombstone() check would treat as reapable.
// Erasing it would underflow mvcc_tombstones (an unsigned counter) to roughly UINT64_MAX, after
// which PerformDeletionAtomic's `mvcc_tombstones < max_tombstones` cap check can never pass
// again, silently disabling tombstoning on that shard forever. Fixed by masking bit 63 in
// SetMvcc itself (the live-key setter) rather than trusting every caller to never pass one.
TEST_F(MvccStoreTest, LiveKeyStampMasksTombstoneBitAndSurvivesGc) {
  absl::SetFlag(&FLAGS_multi_master_tombstone_ttl, 1);
  absl::Cleanup restore = [] { absl::SetFlag(&FLAGS_multi_master_tombstone_ttl, 600); };

  const std::string key = "loader-live-key";
  Run({"set", key, "v"});  // a real, live prime key

  shard_set->Await(Shard(key, shard_set->size()), [&] {
    DbSlice& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    // Simulates the opcode-221 wire stamp install for this key, corrupted or malicious, with
    // bit 63 set -- exactly what SetMvcc receives verbatim from rdb_load.cc, unmasked.
    const MvccStamp wire_stamp{MvccClock::kTombstoneBit | (7000ULL << 20), 99};
    db_slice.SetMvcc(0, key, wire_stamp);

    auto stored = db_slice.GetMvcc(0, key);
    ASSERT_TRUE(stored.has_value());
    EXPECT_FALSE(stored->IsTombstone())
        << "SetMvcc must mask bit 63 -- it is the live-key setter, SetTombstone is the only "
           "legitimate bit-63 writer";
    EXPECT_EQ(stored->Mvcc(), wire_stamp.Mvcc());
    EXPECT_EQ(stored->origin_hash, wire_stamp.origin_hash);
  });

  AdvanceTime(2000);  // past the 1s deadline this stamp would carry if it were a real tombstone

  shard_set->RunBriefInParallel([&](EngineShard* shard) {
    DbSlice& db_slice = namespaces->GetDefaultNamespace().GetDbSlice(shard->shard_id());
    db_slice.TombstoneGcStep();
  });

  shard_set->Await(Shard(key, shard_set->size()), [&] {
    DbSlice& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    auto stored = db_slice.GetMvcc(0, key);
    ASSERT_TRUE(stored.has_value()) << "GC must not erase a live key's (masked) stamp";
    EXPECT_FALSE(stored->IsTombstone());
  });
  EXPECT_EQ(Run({"get", key}), "v");
  EXPECT_EQ(GetMetrics().db_stats[0].mvcc_tombstones, 0u);
}

// drakeydb: P4-3 Task 3, review fix (C2ii), Critical -- IsTombstone() alone is not a safe erase
// predicate on a dense table: it only reflects what a producer CLAIMED, not what prime
// independently holds. Constructs that shape directly via SetTombstone (the one legitimate
// bit-63 writer) on a key that still has a live prime entry -- the state a producer that bypasses
// SetMvcc's mask (C2i above, any future one, not just today's loader) could otherwise leave --
// and confirms GC leaves it alone by confirming, against prime itself, that a candidate has no
// live counterpart before erasing.
TEST_F(MvccStoreTest, GcDoesNotEraseATombstoneFlaggedSlotWithALivePrimeKey) {
  absl::SetFlag(&FLAGS_multi_master_tombstone_ttl, 1);
  absl::Cleanup restore = [] { absl::SetFlag(&FLAGS_multi_master_tombstone_ttl, 600); };

  const std::string key = "tombstone-shaped-but-live";
  Run({"set", key, "v"});

  shard_set->Await(Shard(key, shard_set->size()), [&] {
    DbSlice& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    db_slice.SetTombstone(0, key, MvccStamp{MvccClock::kTombstoneBit | (7000ULL << 20), 99});
  });

  AdvanceTime(2000);  // past the 1s deadline

  shard_set->RunBriefInParallel([&](EngineShard* shard) {
    DbSlice& db_slice = namespaces->GetDefaultNamespace().GetDbSlice(shard->shard_id());
    db_slice.TombstoneGcStep();
  });

  shard_set->Await(Shard(key, shard_set->size()), [&] {
    DbSlice& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    auto stored = db_slice.GetMvcc(0, key);
    ASSERT_TRUE(stored.has_value())
        << "GC erased a tombstone-flagged slot that still has a live prime key";
    EXPECT_TRUE(stored->IsTombstone());
    EXPECT_EQ(db_slice.GetStats().db_stats[0].key_count, 1u)
        << "the live prime key must still be there too";
  });

  // Clean up the invariant-violating state this test intentionally injected (a tombstone-bit
  // slot coexisting with a live prime key -- the exact shape under test): a real DEL's own
  // placeholder-write logic re-establishes the dense invariant before this test ends, unlike a
  // direct EraseMvcc/SetTombstone here, which would not go through OnCbFinishBlocking the way
  // this fixture's later commands (including TearDown) do.
  Run({"del", key});
}

// drakeydb: P4-3 Task 3, review fix (I3), Critical -- with a bucket budget smaller than one
// database's own lap length, always starting TombstoneGcStep's for-loop at database 0
// deterministically starved every later database forever (see the definition, db_slice.cc, for
// why: db0's own persisted cursor can land back on "start a new lap" in EXACTLY the tick the
// budget also runs out for db1's turn). Reproduced with two databases: a backlog on db0 far
// larger than one budget=1 step can visit, and a small one on db1 -- before this fix, db1's
// tombstones were never reaped no matter how many steps ran.
TEST_F(MvccStoreTest, TombstoneGcServicesEveryDatabaseUnderRoundRobin) {
  absl::SetFlag(&FLAGS_multi_master_tombstone_ttl, 1);        // seconds
  absl::SetFlag(&FLAGS_multi_master_tombstone_gc_budget, 1);  // buckets/step
  absl::Cleanup restore = [] {
    absl::SetFlag(&FLAGS_multi_master_tombstone_ttl, 600);
    absl::SetFlag(&FLAGS_multi_master_tombstone_gc_budget, 64);
  };

  constexpr int kDb0Keys = 200;  // large enough that no single budget=1 step ever drains it
  for (int i = 0; i < kDb0Keys; ++i)
    Run({"set", absl::StrCat("db0-key-", i), "v"});
  for (int i = 0; i < kDb0Keys; ++i)
    Run({"del", absl::StrCat("db0-key-", i)});

  Run({"select", "1"});
  constexpr int kDb1Keys = 3;  // small: fits comfortably inside a handful of budget=1 steps
  for (int i = 0; i < kDb1Keys; ++i)
    Run({"set", absl::StrCat("db1-key-", i), "v"});
  for (int i = 0; i < kDb1Keys; ++i)
    Run({"del", absl::StrCat("db1-key-", i)});
  Run({"select", "0"});

  AdvanceTime(2000);  // past the 1s deadline both batches' tombstones carry

  ASSERT_EQ(GetMetrics().db_stats[0].mvcc_tombstones, static_cast<size_t>(kDb0Keys));
  ASSERT_EQ(GetMetrics().db_stats[1].mvcc_tombstones, static_cast<size_t>(kDb1Keys));

  // Drive enough steps that db1's tiny backlog would have to be fully reaped by now if it ever
  // gets a turn at all -- but nowhere near enough to drain db0's much larger one, so this proves
  // db1 was serviced WITHOUT waiting for db0 to finish first.
  for (int step = 0; step < 20; ++step) {
    shard_set->RunBriefInParallel([&](EngineShard* shard) {
      DbSlice& db_slice = namespaces->GetDefaultNamespace().GetDbSlice(shard->shard_id());
      db_slice.TombstoneGcStep();
    });
  }

  EXPECT_EQ(GetMetrics().db_stats[1].mvcc_tombstones, 0u)
      << "database 1 must be serviced without waiting for database 0's much larger backlog to "
         "finish first";
  EXPECT_GT(GetMetrics().db_stats[0].mvcc_tombstones, 0u)
      << "sanity check: database 0's backlog must still be far from drained after only 20 "
         "budget=1 steps, otherwise this test isn't distinguishing round-robin from luck";
}

// drakeydb: review fix round 1 (F1) -- "a"/"b" restored (the brief's original pair). Verified
// empirically (temporary LOG(WARNING) of Shard(key, shard_set->size()); the reviewer separately
// confirmed via XXH64(...) % 2) that under this fixture's shard count (2), Shard("a")=1 and
// Shard("b")=0: different shards, so this exercises generic_family.cc's cross-shard Renamer
// path (RenameGeneric's GetUniqueShardCnt() != 1 branch), not the single-shard OpRen fast path.
// That path used to resurface a real bug: Renamer::DeserializeDest journaled the RESTORE before
// add_res's AutoUpdater ran, so the destination was armed too late for its own commit to see it
// and was left permanently unstamped -- fixed in this same round by an explicit
// add_res->post_updater.Run() before RecordJournal (see generic_family.cc). An earlier version
// of this test substituted a same-shard key ("a"/"c") specifically to avoid tripping over that
// bug, which made the suite green without the bug being fixed -- exactly the "test routes around
// a known failure" pattern this fork's review process exists to catch. Restored to "a"/"b" so
// this test proves the fix instead of avoiding what it was meant to cover.
// drakeydb: P4-3 Task 2 -- RENAME's source removal goes through DelSrc/OpRen's default
// DeleteReason::kExplicit, same as a plain DEL, so since Task 2 the source is left as a
// tombstone rather than erased.
TEST_F(MvccStoreTest, RenameMovesTheStampByRecreatingIt) {
  Run({"set", "a", "v"});
  ASSERT_TRUE(StampOf("a").has_value());
  Run({"rename", "a", "b"});
  auto src_tomb = StampOf("a");
  ASSERT_TRUE(src_tomb.has_value()) << "the source key keeps its slot, as a tombstone";
  EXPECT_TRUE(src_tomb->IsTombstone());
  auto dest_stamp = StampOf("b");
  ASSERT_TRUE(dest_stamp.has_value()) << "the destination is armed and committed by RENAME's "
                                         "own journal entry";
  EXPECT_FALSE(dest_stamp->IsTombstone());
}

// drakeydb: P4-3 Task 2 -- the same-shard fast path (OpRen, generic_family.cc) relies on the
// DEFERRED auto-journal mechanism (CO::NO_AUTOJOURNAL + ReviveAutoJournal()): RENAME's own
// journal entry is written by Transaction::LogAutoJournalOnShard AFTER OpRen's callback returns,
// which is AFTER DbSlice::OnCbFinishBlocking's O(1) dense-invariant DCHECK already ran
// (Transaction::RunCallback calls OnCbFinishBlocking before LogAutoJournalOnShard --
// transaction.cc). ArmTombstone alone only touches the thread-local arm list, not db.mvcc, so
// without PerformDeletionAtomic's synchronous placeholder write (db_slice.cc, mirroring
// EnsureMvcc's placeholder for a plain write) the source key's slot still looked like a live,
// non-tombstone stamp with no prime key at that checkpoint.
//
// Falsifying: commenting out the `SetTombstone(table->index, del_it.key(), MvccStamp{
// MvccClock::kTombstoneBit, 0})` call in PerformDeletionAtomic (db_slice.cc) reproduces
// `Check failed: dbp->mvcc->size() - dbp->stats.mvcc_tombstones == dbp->prime.size() (2 vs. 1)`
// here -- see task-2-report.md for the verbatim abort.
TEST_F(MvccStoreTest, SameShardRenameToFreshDestKeepsTheInvariant) {
  unsigned n = shard_set->size();
  std::string k, r;
  for (int i = 0;; ++i) {
    k = absl::StrCat("k", i);
    r = absl::StrCat("r", i);
    if (Shard(k, n) == Shard(r, n))
      break;
    CHECK_LT(i, 10000) << "could not find a same-shard k/r pair";
  }
  Run({"set", k, "v"});
  Run({"rename", k, r});

  auto src_tomb = StampOf(k);
  ASSERT_TRUE(src_tomb.has_value());
  EXPECT_TRUE(src_tomb->IsTombstone());
  auto dest = StampOf(r);
  ASSERT_TRUE(dest.has_value());
  EXPECT_FALSE(dest->IsTombstone());
  EXPECT_EQ(SumMvccMismatchesAcrossShards(), 0u);
}

// drakeydb: P4-3 Task 2 -- verification duty #1's own regression coverage (see task-2-report.md).
// Renamer::DeserializeDest (generic_family.cc), for a destination that already exists, calls
// DelMutable(dest_key_) -- a REAL kExplicit delete, so it ArmTombstone's dest_key_ -- and THEN
// loader.Add(dest_key_, ...) + add_res->post_updater.Run() re-arms the SAME key live, both still
// inside the one callback RENAME's own single journal entry commits from (one HopStamp, no
// EndOfWriteEpoch in between): armed_ ends up holding a [tombstone-arm, live-arm] pair for
// dest_key_, both consumed by ONE Commit() call. This is exactly the "one callback reusing a
// single hop stamp for both a write and a delete of the same key" shape verification duty #1
// asks about -- established here to exist, but confirmed (by this test) to self-correct: only
// ONE journal entry ("RESTORE ... REPLACE") ever reaches the wire, SetTombstone/SetExistingMvcc's
// was_tombstone bookkeeping nets the transient tombstone credit back to zero as the live arm's
// commit immediately follows it in the same Commit() call, and the destination ends up live, not
// tombstoned -- so no independently-propagated tombstone stamp ever ties against a value's stamp
// for a peer to reject. See the report for why this differs from the STOP-worthy case the brief
// describes.
TEST_F(MvccStoreTest, RenameOntoAnExistingDestSelfCorrectsToALiveStamp) {
  Run({"set", "a", "v1"});
  Run({"set", "b", "v0"});
  auto b_before = StampOf("b");
  ASSERT_TRUE(b_before.has_value());
  ASSERT_FALSE(b_before->IsTombstone());
  const size_t tombstones_before = GetMetrics().db_stats[0].mvcc_tombstones;

  Run({"rename", "a", "b"});

  auto src_tomb = StampOf("a");
  ASSERT_TRUE(src_tomb.has_value()) << "the source key keeps its slot, as a tombstone";
  EXPECT_TRUE(src_tomb->IsTombstone());

  auto dest = StampOf("b");
  ASSERT_TRUE(dest.has_value());
  EXPECT_FALSE(dest->IsTombstone())
      << "the destination's OLD value was tombstoned and re-armed live in the same Commit() call "
         "-- the live arm must win, not leave the destination looking deleted";
  EXPECT_EQ(GetMetrics().db_stats[0].mvcc_tombstones, tombstones_before + 1)
      << "exactly one net-new tombstone (the source) -- the destination's transient tombstone "
         "credit must not leak";
  EXPECT_EQ(SumMvccMismatchesAcrossShards(), 0u);
}

// drakeydb: second-round review fix (Important #1) -- builds a RESTORE payload that PASSES
// GetRdbVersion's version+crc64 footer check (generic_family.cc), which runs at the COMMAND
// level before the transaction is even scheduled, but fails RdbRestoreValue::Add's actual RDB
// body deserialization once OpRestore (generic_family.cc) is running -- letting
// RestoreReplaceFailureRollsBackTheOrphanedTombstone below reach OpRestore's own
// DelMutable-then-fail sequence for real, rather than being rejected before ever touching the
// existing key. \xFF is not a valid RDB object-type tag; the footer's version+crc64 are computed
// correctly for that one invalid byte, exactly as GetRdbVersion expects: version (2 bytes LE)
// then crc64 of (body + version) (8 bytes LE) -- see GetRdbVersion's own comment for the layout.
std::string InvalidRestorePayloadWithValidFooter() {
  std::string prefix(1, '\xFF');
  constexpr uint16_t kVersion = 2;  // comfortably under RDB_VERSION; any accepted value works
  prefix.push_back(static_cast<char>(kVersion & 0xFF));
  prefix.push_back(static_cast<char>((kVersion >> 8) & 0xFF));
  uint64_t crc = crc64(0, reinterpret_cast<const unsigned char*>(prefix.data()), prefix.size());
  std::string payload = prefix;
  for (int i = 0; i < 8; ++i)
    payload.push_back(static_cast<char>((crc >> (8 * i)) & 0xFF));
  return payload;
}

// drakeydb: second-round review fix (Important #1) -- I3's PRODUCTION half
// (DbSlice::RollbackUncommittedTombstone, db_slice.cc) had no coverage: the only committed I3
// test (mvcc_test.cc) drives MvccStamper::EndOfWriteEpoch directly with a mock EraseFn, so
// neither RollbackUncommittedTombstone itself nor any of the eight real call-site wirings
// (transaction.cc x2, db_slice.cc x6) was exercised -- gutting RollbackUncommittedTombstone to a
// no-op left mvcc_test 30/30 and multi_master_test green. This drives a real, unmocked
// never-commits path end to end: RESTORE key 0 <payload> REPLACE on an existing key runs
// OpRestore (generic_family.cc), which DelMutable's the existing value (default reason
// kExplicit, so it ArmTombstones under this task's own gate) BEFORE attempting
// RdbRestoreValue::Add on the caller-supplied payload -- when that Add fails, OpRestore returns
// the error status directly, RESTORE being CO::JOURNALED (auto-journal, not NO_AUTOJOURNAL) means
// Transaction::LogAutoJournalOnShard's `if (result.status != OpStatus::OK) return;`
// (transaction.cc) skips journaling entirely, so no Commit() ever consumes the tombstone arm --
// exactly the "auto-journaled command returning non-OK" class of orphaning path I3 exists for.
TEST_F(MvccStoreTest, RestoreReplaceFailureRollsBackTheOrphanedTombstone) {
  Run({"set", "k", "v1"});
  ASSERT_TRUE(StampOf("k").has_value());

  auto res = Run({"restore", "k", "0", InvalidRestorePayloadWithValidFooter(), "REPLACE"});
  ASSERT_THAT(res, ArgType(RespExpr::ERROR))
      << "sanity: an invalid RDB body must fail, or this test proves nothing about the "
         "never-commits path";

  auto st = StampOf("k");
  EXPECT_FALSE(st.has_value())
      << "RollbackUncommittedTombstone (db_slice.cc), wired for real through "
         "MvccStamper::EndOfWriteEpoch and Transaction::RunCallback's epoch_end cleanup, must "
         "erase the orphaned placeholder OpRestore's failed commit left behind";
  EXPECT_EQ(GetMetrics().db_stats[0].mvcc_tombstones, 0u);
  EXPECT_EQ(SumMvccMismatchesAcrossShards(), 0u);
}

// drakeydb: P4-3 Task 2 review fix (C1, Critical) -- EnsureMvcc (db_slice.cc) did not clear a
// slot's tombstone flag when a deleted key was re-created: PostUpdate calls EnsureMvcc
// synchronously, before Arm(), at the exact moment prime.size() grows back for the new value, but
// (before this fix) the stale kTombstoneBit stayed set and mvcc_tombstones was not decremented
// until the new write's own (possibly deferred) journal commit -- a window
// OnCbFinishBlocking's O(1) DCHECK can observe for any auto-journaled command. Three
// reproductions, matching the controller-supplied recipes exactly.
//
// Falsifying: reverting EnsureMvcc's tombstone-clearing branch (db_slice.cc) reproduces
// `Check failed: dbp->mvcc->size() - dbp->stats.mvcc_tombstones == dbp->prime.size() (0 vs. 1)`
// on this test; see the report's fix-report addendum for the verbatim abort.
TEST_F(MvccStoreTest, RecreateAfterExplicitDeleteClearsTheTombstone) {
  Run({"set", "k", "v"});
  Run({"del", "k"});
  auto k_tomb = StampOf("k");
  ASSERT_TRUE(k_tomb.has_value())
      << "the DEL's own journal entry must stamp the tombstone it armed";
  EXPECT_TRUE(k_tomb->IsTombstone());

  Run({"lpush", "k", "a"});

  auto st = StampOf("k");
  ASSERT_TRUE(st.has_value());
  EXPECT_FALSE(st->IsTombstone()) << "a live re-create must clear the tombstone flag";
  EXPECT_EQ(GetMetrics().db_stats[0].mvcc_tombstones, 0u);
  EXPECT_EQ(SumMvccMismatchesAcrossShards(), 0u);
}

// drakeydb: P4-3 Task 2 review fix (C1, Critical) -- same bug, via a container command emptying
// (and so kExplicit-deleting, DeleteHw-style) and then immediately re-creating the SAME key,
// rather than a standalone DEL.
TEST_F(MvccStoreTest, RecreateAfterContainerEmptiedByCommandClearsTheTombstone) {
  Run({"lpush", "q", "a"});
  Run({"lpop", "q"});
  auto q_tomb = StampOf("q");
  ASSERT_TRUE(q_tomb.has_value())
      << "LPOP must leave the emptied list's key as a tombstone, not erase its slot";
  EXPECT_TRUE(q_tomb->IsTombstone());

  Run({"lpush", "q", "b"});

  auto st = StampOf("q");
  ASSERT_TRUE(st.has_value());
  EXPECT_FALSE(st->IsTombstone());
  EXPECT_EQ(GetMetrics().db_stats[0].mvcc_tombstones, 0u);
  EXPECT_EQ(SumMvccMismatchesAcrossShards(), 0u);
}

// drakeydb: P4-3 Task 2 review fix (C1, Critical) -- the single-command variant: OpRestore
// (generic_family.cc) deletes an existing key itself (DelMutable, default reason kExplicit, so it
// tombstones) then re-adds the restored value, all inside ONE auto-journaled RESTORE command
// (CO::JOURNALED, not NO_AUTOJOURNAL) -- so the eventual commit is deferred past
// OnCbFinishBlocking's check exactly like a deferred/auto-journaled write, unlike DEL's own
// explicit, synchronous RecordJournal.
TEST_F(MvccStoreTest, RestoreReplaceRecreatesClearingTheTombstone) {
  Run({"set", "k", "v1"});
  std::string dump = Run({"dump", "k"}).GetString();
  Run({"set", "k", "v2"});  // still live with a different value, so RESTORE below truly replaces

  Run({"restore", "k", "0", dump, "REPLACE"});

  auto st = StampOf("k");
  ASSERT_TRUE(st.has_value());
  EXPECT_FALSE(st->IsTombstone());
  EXPECT_EQ(GetMetrics().db_stats[0].mvcc_tombstones, 0u);
  EXPECT_EQ(SumMvccMismatchesAcrossShards(), 0u);
}

// drakeydb: P4-3 Task 2 review fix (C2, Critical) -- PerformDeletionAtomic's mvcc branch was not
// namespace-gated, unlike PostUpdate's own F1 gate and the DCHECK it exists to keep true: a
// non-default-namespace kExplicit DEL still tombstoned, and unlike Disarm/EraseMvcc (a harmless,
// guaranteed no-op miss there, since nothing in that namespace is ever armed or EnsureMvcc'd),
// SetTombstone's insert-tolerant Insert() call actually created a permanent, orphaned slot no
// Commit() could ever reach (RecordEntry only ever targets the default namespace).
//
// drakeydb: second-round review fix -- the slot-state assertions below (ns1_stamp/mvcc_entries/
// mvcc_tombstones) do NOT isolate this fix on their own: I3's EndOfWriteEpoch rollback (below in
// this file) independently cleans up the very same orphaned placeholder at the end of THIS
// command's own RunCallback, since EndOfWriteEpoch always fires regardless of namespace
// (mvcc_enabled_ is DbSlice-wide). Reverting only the C2 gate still passes those assertions.
// mvcc_unstamped_writes is the assertion that actually isolates C2: I3's rollback erases the
// stale SLOT, but it cannot undo the fact that ArmTombstone was called at all -- without the
// gate, PostUpdate never arms a non-default-namespace key (its own F1 gate), but
// PerformDeletionAtomic's ArmTombstone call did, unconditionally, adding one entry to armed_ that
// EndOfWriteEpoch then finds still pending (nothing in this namespace ever commits it) and counts
// toward unstamped_writes -- the "a read-mutation path is over-arming" canary
// (server_family.cc:3143-3150) that healthy operation keeps at 0. With the gate in place, that
// ArmTombstone call never happens, so the counter never moves.
//
// Falsifying: reverting the ns_ == GetDefaultNamespace() gate on PerformDeletionAtomic's mvcc
// block (db_slice.cc) reproduces mvcc_entries/mvcc_tombstones going 0->1 AND
// mvcc_unstamped_writes going 0->1 (1 vs. 0) on this test; see the report's fix-report addendum
// for the verbatim measurements.
TEST_F(MvccStoreTest, NonDefaultNamespaceDeleteLeavesNoOrphanedTombstone) {
  Namespace& ns1 = namespaces->GetOrInsert("ns1-c2");
  ASSERT_EQ(RunViaNamespace(&ns1, {"set", "gone", "v"}), "OK");
  ASSERT_EQ(RunViaNamespace(&ns1, {"del", "gone"}).GetInt(), 1);

  const ShardId sid = Shard("gone", shard_set->size());
  std::optional<MvccStamp> ns1_stamp;
  DbSlice::Stats slice_stats;
  shard_set->Await(sid, [&] {
    DbSlice& db_slice = ns1.GetDbSlice(sid);
    ns1_stamp = db_slice.GetMvcc(0, "gone");
    slice_stats = db_slice.GetStats();
  });
  EXPECT_FALSE(ns1_stamp.has_value())
      << "a non-default-namespace delete must not create an orphaned tombstone slot -- no "
         "journal commit can ever reach it to release it";
  DbStats total;
  for (const auto& db_stats : slice_stats.db_stats)
    total += db_stats;
  EXPECT_EQ(total.mvcc_entries, 0u);
  EXPECT_EQ(total.mvcc_tombstones, 0u);

  // Summed across every shard, matching PureWriteWorkloadLeavesNoUnstampedWrites' established
  // convention (this file) -- the specific shard "gone" hashes to is enough to prove the point,
  // but summing costs nothing and rules out a wrong-shard assumption.
  std::atomic<uint64_t> unstamped{0};
  shard_set->pool()->AwaitBrief(
      [&](unsigned, auto*) { unstamped += MvccStamper::tlocal()->stats().unstamped_writes; });
  EXPECT_EQ(unstamped.load(), 0u)
      << "a non-default-namespace delete must never call ArmTombstone at all -- I3's rollback "
         "erasing the resulting slot afterward is not the same as this never having armed";
}

// drakeydb: review fix round 2 -- the reviewer's bonus finding: GenericFamily::Copy always
// constructs its Renamer with do_copy=true, and FinalizeRename routes to DeserializeDest for
// every COPY (the `!do_copy_ && shard_id == src_sid_` branch that sends RENAME's source through
// DelSrc instead is never taken when do_copy_ is true) -- so COPY was affected by the same F1 bug
// regardless of shard placement, not just cross-shard RENAME. "a"/"c" is deliberately a
// *same-shard* pair here (both hash to 1 under this fixture): unlike RENAME, COPY has no
// single-shard fast path to fall back to, so this demonstrates the bug (and the fix) even in the
// case that would have been safe for RENAME.
TEST_F(MvccStoreTest, CopyStampsTheDestinationRegardlessOfShardPlacement) {
  Run({"set", "a", "v"});
  ASSERT_TRUE(StampOf("a").has_value());
  Run({"copy", "a", "c"});
  ASSERT_TRUE(StampOf("a").has_value()) << "COPY must not disturb the source's stamp";
  ASSERT_TRUE(StampOf("c").has_value()) << "the destination is armed and committed by COPY's "
                                           "own journal entry (via the same DeserializeDest)";
}

// drakeydb: P4-3 Task 11 -- resolves the STOP finding Task 2 left behind (see
// ExplicitDeleteLeavesATombstoneAtLeastAsNewAsTheValue's comment above and DeleteReason's
// comment, db_slice.h). DbSlice::ExpireIfNeeded now calls Del()/PerformDeletionAtomic BEFORE
// RecordExpiryBlocking, so the tombstone arm PerformDeletionAtomic places for this kExpired
// delete is consumed by this DEL's own Commit(), exactly the way an explicit DEL's arm is.
// Supersedes ExpiryErasesTheStamp, which pinned the pre-Task-11 erase behavior for the same
// scenario -- this test is its replacement, not an addition.
//
// Falsifying: reverting the ExpireIfNeeded reorder (db_slice.cc) so RecordExpiryBlocking runs
// before Del() again reproduces the orphaned-arm symptom: EndOfWriteEpoch's rollback (Task 2's
// I3) erases the synchronous zero-authority placeholder before any Commit() lands a real stamp
// on it, so StampOf comes back nullopt instead of a tombstone. Verbatim run in task-11-report.md.
TEST_F(MvccStoreTest, LazyExpiryEarnsATombstoneWithSelfOrigin) {
  // An ordinary self-authored write, purely so this test can compare its origin_hash against the
  // expiry tombstone's below without hardcoding this node's uuid hash.
  Run({"set", "control", "v"});
  auto control_stamp = StampOf("control");
  ASSERT_TRUE(control_stamp.has_value());
  const uint64_t self_origin_hash = control_stamp->origin_hash;

  Run({"set", "k", "v", "px", "10"});
  ASSERT_TRUE(StampOf("k").has_value());
  AdvanceTime(50);
  Run({"get", "k"});  // triggers lazy expiry (DbSlice::ExpireIfNeeded, the read path)

  auto tomb = StampOf("k");
  ASSERT_TRUE(tomb.has_value())
      << "a lazily-expired key must keep its slot, as a tombstone -- not an absent slot";
  EXPECT_TRUE(tomb->IsTombstone());
  EXPECT_NE(tomb->Mvcc(), 0u)
      << "must carry the expiry DEL's own real, minted stamp -- not the zero-authority "
         "{kTombstoneBit, 0} placeholder PerformDeletionAtomic writes synchronously, which "
         "EndOfWriteEpoch rolls back if nothing ever commits over it";
  EXPECT_EQ(tomb->origin_hash, self_origin_hash)
      << "an expiry is always a local decision (RecordExpiryBlocking pins origin_idx to "
         "kSelfIdx) -- it must never be attributed to a peer";
}

// drakeydb: P4-1 Task 8; updated P4-3 Task 2 -- a multi-key DEL is still kExplicit for every key
// it touches, so since Task 2 each deleted key keeps its slot as a tombstone instead of being
// erased; the untouched key is unaffected either way.
TEST_F(MvccStoreTest, MultiKeyDeleteTombstonesEveryStamp) {
  Run({"mset", "k1", "v1", "k2", "v2", "k3", "v3"});
  Run({"del", "k1", "k2"});
  auto t1 = StampOf("k1");
  auto t2 = StampOf("k2");
  ASSERT_TRUE(t1.has_value());
  EXPECT_TRUE(t1->IsTombstone());
  ASSERT_TRUE(t2.has_value());
  EXPECT_TRUE(t2->IsTombstone());
  auto k3_stamp = StampOf("k3");
  ASSERT_TRUE(k3_stamp.has_value());
  EXPECT_FALSE(k3_stamp->IsTombstone());
}

// drakeydb: review fix round 2 (F3) -- five NO_AUTOJOURNAL commands that build their own explicit
// journal entry while a live AutoUpdater had not yet run, so their commit hit an empty arm list
// and a *propagated* destination key was left permanently unstamped. Each test below stamps the
// key its command writes; falsified per-site by reverting that one Run() and confirming failure
// (see task-8-report.md's "Fix round 2" section for the verbatim output of each).

TEST_F(MvccStoreTest, PfmergeStampsTheDestination) {
  Run({"pfadd", "src", "a", "b", "c"});
  Run({"pfmerge", "dest", "src"});
  ASSERT_TRUE(StampOf("dest").has_value()) << "PFMERGE's own journal entry must stamp dest";
}

// journal_as_minid (and so the buggy explicit RecordJournal path) is only taken for MAXLEN/approx
// trims -- see JournalAsMinId, stream_family.cc. A MINID trim without "~" auto-journals instead
// and would not exercise this.
//
// Compares before/after rather than just has_value(): "s" already carries a stamp from the two
// XADDs below before XTRIM ever runs, so a bare has_value() check after XTRIM would pass whether
// or not XTRIM's own commit did anything -- it would just be re-observing the stale XADD stamp.
// The real assertion is that XTRIM mints its own, strictly newer stamp (Task 7's monotonicity
// invariant, also covered generally by StampsAreStrictlyIncreasingAcrossWrites).
TEST_F(MvccStoreTest, XtrimStampsTheStream) {
  Run({"xadd", "s", "*", "f", "v"});
  Run({"xadd", "s", "*", "f", "v"});
  auto before = StampOf("s");
  ASSERT_TRUE(before.has_value());
  Run({"xtrim", "s", "maxlen", "1"});
  auto after = StampOf("s");
  ASSERT_TRUE(after.has_value());
  EXPECT_TRUE(*before < *after) << "XTRIM's own journal entry must mint a fresh, strictly newer "
                                   "stamp, not leave the stale one from the XADDs above";
}

TEST_F(MvccStoreTest, XaddStampsTheStream) {
  Run({"xadd", "s", "*", "f", "v"});
  ASSERT_TRUE(StampOf("s").has_value()) << "XADD's own journal entry must stamp the stream key";
}

// Cross-shard src/dest: OpMoveSingleShard (MoveGeneric's GetUniqueShardCnt() == 1 branch) already
// runs both post_updater.Run() calls before its own explicit RecordJournal and was never broken
// -- only the cross-shard MoveTwoShards -> OpPush(..., journal_rewrite=true) path was. "a"/"b"
// are on different shards under this fixture, same pair already established for RENAME.
TEST_F(MvccStoreTest, LmoveStampsTheDestinationCrossShard) {
  Run({"rpush", "a", "v"});
  Run({"lmove", "a", "b", "left", "right"});
  ASSERT_TRUE(StampOf("b").has_value()) << "LMOVE's own journal entry must stamp the destination";
}

TEST_F(MvccStoreTest, SunionstoreStampsTheDestination) {
  Run({"sadd", "s1", "x", "y"});
  Run({"sunionstore", "dest", "s1"});
  ASSERT_TRUE(StampOf("dest").has_value()) << "SUNIONSTORE's own journal entry must stamp dest";
}

// drakeydb: review fix round 3 (F5) -- ZSetFamily::OpAdd is the sixth instance of the same class:
// PrepareZEntry returns a live ItAndUpdater with no post_updater reference anywhere before the
// explicit RecordJournal calls (DEL, then ZADD) that NO_AUTOJOURNAL ZDIFFSTORE/ZINTERSTORE/
// ZUNIONSTORE/ZRANGESTORE and GEORADIUS...STORE rely on. "dest" is pre-set to a plain string (a
// different type entirely) specifically to force the two-entry DEL-then-ZADD path (zparams.override
// is unconditionally true for these *STORE commands, so the DEL fires regardless of whether dest
// previously existed -- but giving it a real prior value makes the test's own setup meaningful,
// not just incidental). Compares before/after like XtrimStampsTheStream above, for the same
// reason: a bare has_value() after the command would be satisfied by a stale stamp surviving
// untouched, not necessarily by ZUNIONSTORE's own commit succeeding.
TEST_F(MvccStoreTest, ZunionstoreStampsTheDestinationAcrossTwoJournalEntries) {
  Run({"zadd", "z1", "1", "a"});
  Run({"set", "dest", "stale"});
  auto before = StampOf("dest");
  ASSERT_TRUE(before.has_value());
  Run({"zunionstore", "dest", "1", "z1"});
  auto after = StampOf("dest");
  ASSERT_TRUE(after.has_value());
  EXPECT_TRUE(*before < *after) << "ZUNIONSTORE's own journal entry must mint a fresh, strictly "
                                   "newer stamp for dest via its two-entry DEL+ZADD path";
}

// drakeydb: Phase 4 Task 9 -- the phase's acceptance criterion: an applied write carries the
// author's stamp verbatim, not a freshly-minted local one, and records the AUTHOR's origin hash,
// not the applier's own. Falsifying (see task-9-report.md for the verbatim run): removing the
// executor_->SetApplyMvcc(tx_data.mvcc) call in DflyShardReplica::ExecuteTx (replica.cc) leaves
// JournalExecutor's ConnectionContext::repl_mvcc at its 0 default, so RecordEntry's
// `entry.mvcc == 0` test (journal.cc) is true and it mints a fresh HopStamp instead of storing
// kAuthorMvcc, failing the first EXPECT_EQ below.
//
// drakeydb: P4-4 Task A5 -- renamed from AppliedWriteKeepsAuthorStampVerbatim: "verbatim" is no
// longer the WHOLE story for an applied write's LOCAL stored stamp -- FloorAppliedStamp (mvcc.h)
// floors it instead whenever the author's stamp is older than the key's own current one. "k"
// here is a BRAND NEW key, so its stored stamp `S` is a fresh {0,0} slot -- exactly
// FloorAppliedStamp's own "nothing to floor against" case -- so this test's assertion is
// unchanged; see UnguardedAppliedRmwWithOlderAuthorMvccFloorsInsteadOfRewindingTheStamp (below)
// for the floored case this task adds. The journal ENTRY this applier's own downstream peers see
// still carries kAuthorMvcc verbatim regardless -- RecordEntry writes entry.mvcc onto the wire
// entry BEFORE Commit() (and any floor) ever runs (journal.cc) -- only the LOCAL stored stamp is
// ever floored, never what gets propagated.
TEST_F(MvccStoreTest, AppliedWriteOnFreshKeyKeepsAuthorStampVerbatim) {
  constexpr uint64_t kAuthorMvcc = 0x1234'5678'9ABCULL;
  constexpr uint32_t kPeerIdx = 3;
  const uint64_t peer_hash = NodeUuidHash("6f1c4c3e-0000-4000-8000-00000000000b");

  shard_set->pool()->AwaitBrief(
      [&](unsigned, auto*) { MvccStamper::tlocal()->RegisterOriginHash(kPeerIdx, peer_hash); });
  ApplyReplicatedCommand({"set", "k", "v"}, kPeerIdx, kAuthorMvcc, /*lww_guard=*/false);

  auto st = StampOf("k");
  ASSERT_TRUE(st.has_value());
  EXPECT_EQ(st->Mvcc(), kAuthorMvcc) << "the applier must not re-mint -- stamps would otherwise "
                                        "inflate on every replication hop";
  EXPECT_EQ(st->origin_hash, peer_hash) << "and must record the AUTHOR, not itself";
}

// drakeydb: review wave 2 (F4, IMPORTANT) -- MvccStamper::Commit stamps EVERY currently-armed key
// with the entry it is given, not just the key its own payload names (mvcc.h). MSET arms
// key-by-key (OpMSet, string_family.cc: one Set() call per pair, one post_updater.Run() each) and
// only journals once at the very end -- so if a LATER key in the same MSET already has an expired
// whole-key TTL, AddOrFind -> FindInternal -> ExpireIfNeeded (db_slice.cc) fires
// RecordExpiryBlocking (tx_base.cc) for it MID-CALLBACK, and that call's own Commit() sweeps up
// every key armed so far -- including k1 here, armed by its own Set() one iteration earlier --
// before MSET's own trailing RecordJournal ever gets a chance to stamp it. Before this fix, k1
// came out stamped with a freshly-minted LOCAL HopStamp (RecordExpiryBlocking's hardcoded mvcc=0)
// instead of the replicated MSET's real author stamp.
//
// Falsifying: reverting RecordExpiryBlocking's `db_cntx.repl_mvcc` argument (tx_base.cc) back to
// a hardcoded 0 makes k1's EXPECT_EQ below fail -- st->Mvcc() comes back larger than kAuthorMvcc
// (a real HopStamp minted from the live wall clock, not this literal).
//
// drakeydb: P4-4 Task A5 -- k2's LIVE recreate (OpMSet's own Set() call for it, right after its
// lazy expiry) floors against that expiry's OWN self-minted tombstone stamp E -- DbSlice::
// EnsureMvcc (db_slice.cc) captures whatever the slot held before this write, including a
// tombstone it itself just cleared, and PostUpdate carries that on k2's arm for exactly this
// purpose. E is minted by RecordExpiryBlocking's CommitOwnTombstone -> HopStamp(db_cntx.
// time_now_ms) (tx_base.cc, mvcc.cc), and MvccClock::Next(now_ms) = max(now_ms << kCounterBits,
// last_ + 1). This fixture's clock is frozen (GetCurrentTimeMs() returns TEST_current_time_ms,
// engine_shard_set.h) and AdvanceTime only ever moves it forward, so E.Mvcc() is predicted below,
// independently and exactly, BEFORE the apply ever runs: `now_ms` is read directly, and shard 0's
// own clock().last() reflects k2's own earlier local SET (the only prior mint on this shard --
// EndOfWriteEpoch resets hop_stamp_/hop_started_ms_ after every command, but never clock_ itself,
// mvcc.cc, so `last_` persists across commands). k1's own assertions are unrelated to any of
// this -- see the comment above for that half.
TEST_F(MvccStoreTest, ExpiryMidMultiKeyAppliedWriteKeepsSiblingAuthorMvcc) {
  constexpr uint32_t kPeerIdx = 6;
  constexpr uint64_t kAuthorMvcc = 0x7777'0000'2222ULL;
  const uint64_t peer_hash = NodeUuidHash("6f1c4c3e-0000-4000-8000-0000000000cc");
  shard_set->pool()->AwaitBrief(
      [&](unsigned, auto*) { MvccStamper::tlocal()->RegisterOriginHash(kPeerIdx, peer_hash); });

  // Two distinct keys that hash to the same shard, so OpMSet processes both within one shard
  // callback (one MvccStamper::armed_ list) -- the ordering this test depends on does not exist
  // across shards.
  const unsigned num_shards = shard_set->size();
  std::string k1, k2;
  for (int i = 0; k2.empty(); ++i) {
    CHECK_LT(i, 10000) << "could not find two keys hashing to the same shard";
    std::string cand = absl::StrCat("mk", i);
    if (Shard(cand, num_shards) == 0) {
      (k1.empty() ? k1 : k2) = cand;
    }
  }

  // k2 already exists with a whole-key TTL that has elapsed by the time MSET below re-probes it,
  // but nothing has read it since, so it is still physically present -- ExpireIfNeeded discovers
  // this lazily, mid-MSET, exactly like the recipe FieldExpireCausedDeleteIsNotFlaggedDerived
  // (above in this file) uses for the analogous member-TTL case. AdvanceTime (mocked clock, no
  // real sleep) keeps this deterministic.
  Run({"set", k2, "old", "px", "10"});
  auto p_before_expiry = StampOf(k2);  // P: k2's own live stamp, captured BEFORE it expires
  ASSERT_TRUE(p_before_expiry.has_value());
  AdvanceTime(50);

  // Predict E.Mvcc() independently, from MvccClock::Next's own formula, BEFORE the apply below
  // ever runs -- see the comment above the test for why this is exact, not a guess: nothing else
  // mints on shard 0 between this read and the expiry's own HopStamp call inside the apply.
  const uint64_t now_ms_before_apply = GetCurrentTimeMs();
  uint64_t last_before_apply = 0;
  shard_set->Await(0, [&] { last_before_apply = MvccStamper::tlocal()->clock().last(); });
  const uint64_t predicted_e_mvcc =
      std::max(now_ms_before_apply << MvccClock::kCounterBits, last_before_apply + 1);

  ApplyReplicatedCommand({"mset", k1, "v1", k2, "v2"}, kPeerIdx, kAuthorMvcc, /*lww_guard=*/false);

  auto st1 = StampOf(k1);
  ASSERT_TRUE(st1.has_value());
  EXPECT_EQ(st1->Mvcc(), kAuthorMvcc)
      << "k1 was armed by its own Set() before k2's lazy expiry swept it into that entry's "
         "Commit() call -- it must still end up stamped with the MSET's real author mvcc";
  EXPECT_EQ(st1->origin_hash, peer_hash)
      << "k1 was swept into the expiry commit and must retain the MSET author's origin hash";

  auto st2 = StampOf(k2);
  ASSERT_TRUE(st2.has_value());
  EXPECT_EQ(st2->Mvcc(), predicted_e_mvcc)
      << "verifies the prediction above: E.Mvcc() must be EXACTLY max(now_ms << kCounterBits, "
         "last_ + 1) as computed BEFORE the apply ran, not merely 'some value bigger than "
         "kAuthorMvcc' -- a wrong capture (e.g. flooring against P instead of E) would produce "
         "P->Mvcc(), which is a real HopStamp too and would otherwise pass a looser check";
  EXPECT_GT(st2->Mvcc(), p_before_expiry->Mvcc())
      << "E must be strictly newer than P: both are minted by the SAME shard's MvccClock, which "
         "is strictly monotone across every mint regardless of how many epochs (commands) pass "
         "in between (mvcc.h) -- this holds for any correct capture, so it is a meaningful extra "
         "guard rail alongside the exact prediction above, not a substitute for it";
  uint64_t self_origin_hash = 0;
  shard_set->pool()->AwaitBrief(
      [&](unsigned, auto*) { self_origin_hash = MvccStamper::tlocal()->OriginHash(0); });
  // Whole-stamp equality against the INDEPENDENTLY predicted E (never st2's own Mvcc(), never
  // P's): pins origin_hash at EXACTLY self_origin_hash - 1 -- one below E's own self-mint, never
  // the peer's -- and bit 63 at EXACTLY 0 -- a live write, never left looking like a tombstone.
  EXPECT_EQ(*st2, (MvccStamp{predicted_e_mvcc, self_origin_hash - 1}));
}

// drakeydb: P4-3 Task 11, review ruling I2 -- a lazy expiry firing while applying a peer's
// command must mint its OWN fresh self stamp for its tombstone, never inherit that peer
// command's mvcc/origin (db_cntx.repl_mvcc/repl_origin_idx) -- an expiry is always a local
// decision (D-10), regardless of whose command happened to trigger it. Before this fix,
// RecordExpiryBlocking (tx_base.cc) forwarded db_cntx.repl_mvcc/repl_origin_idx unconditionally
// (review wave 2, F4) so a sibling key swept into the SAME Commit() call mid a replicated
// multi-key command would correctly retain the author's stamp (see
// ExpiryMidMultiKeyAppliedWriteKeepsSiblingAuthorMvcc above) -- but that same forwarding also
// reached the EXPIRING key's own tombstone arm, with no way to tell the two apart. If the
// peer's forwarded stamp predates the expired key's own prior stamp (clock skew, or simply an
// old command applied late, as here), the resulting tombstone is OLDER than the value it
// replaces and a peer's still-live copy of the same key would resurrect it forever.
//
// The simplest repro needs no multi-key command at all: replaying a peer's plain DEL of a key
// that is ALREADY (lazily, not yet swept) expired routes through DbSlice::FindMutable ->
// FindInternal -> ExpireIfNeeded BEFORE GenericFamily::OpDelV2 ever gets a valid iterator for it
// (IsValid(it.it) is false, so OpDelV2 just skips it and journals nothing of its own, per its
// journal_args/deleted_cnt bookkeeping) -- the ONLY journal entry this command produces is the
// expiry's own RecordExpiryBlocking call, so whatever it stamps "j" with is exactly, and only,
// what this test observes; nothing else can overwrite it afterward the way k2's resurrection
// masks the same transient stamp in the sibling test above.
//
// kOldPeerMvcc is deliberately tiny (far smaller than any real HopStamp, which is derived from
// wall-clock ms) so a stamp that wrongly inherited it would be strictly OLDER than "j"'s own
// prior live stamp -- exactly the resurrection hazard the ruling describes.
//
// Falsifying: removing the MvccStamper::tlocal()->CommitOwnTombstone(...) call at the top of
// RecordExpiryBlocking (tx_base.cc) reproduces this -- see task-11-report.md's fix report for
// the verbatim failure (tomb->Mvcc() comes back equal to kOldPeerMvcc, tomb->origin_hash equal
// to peer_hash, instead of a fresh self stamp).
TEST_F(MvccStoreTest, LazyExpiryDuringAppliedPeerCommandMintsFreshSelfStamp) {
  constexpr uint32_t kPeerIdx = 11;
  constexpr uint64_t kOldPeerMvcc = 0x1000ULL;
  const uint64_t peer_hash = NodeUuidHash("6f1c4c3e-0000-4000-8000-0000000000ee");
  shard_set->pool()->AwaitBrief(
      [&](unsigned, auto*) { MvccStamper::tlocal()->RegisterOriginHash(kPeerIdx, peer_hash); });

  // An ordinary self-authored write, purely so this test can compare its origin_hash against the
  // expiry tombstone's below without hardcoding this node's uuid hash (same technique as
  // LazyExpiryEarnsATombstoneWithSelfOrigin above).
  Run({"set", "control", "v"});
  auto control_stamp = StampOf("control");
  ASSERT_TRUE(control_stamp.has_value());
  const uint64_t self_origin_hash = control_stamp->origin_hash;

  Run({"set", "j", "v", "px", "10"});
  auto before_stamp = StampOf("j");
  ASSERT_TRUE(before_stamp.has_value());
  const MvccStamp before = *before_stamp;
  AdvanceTime(50);

  // Apply a peer's DEL of "j" as if replicated. "j" is already lazily expired but not yet
  // physically removed, so this command's own FindMutable lookup discovers and expires it.
  ApplyReplicatedCommand({"del", "j"}, kPeerIdx, kOldPeerMvcc, /*lww_guard=*/false);

  auto tomb = StampOf("j");
  ASSERT_TRUE(tomb.has_value())
      << "the applied peer DEL's own lazy expiry of j must still leave a tombstone";
  EXPECT_TRUE(tomb->IsTombstone());
  EXPECT_GT(tomb->Mvcc(), before.Mvcc())
      << "must be a freshly minted self stamp, strictly newer than the value it replaces -- not "
         "the peer's old mvcc, or a peer's still-live copy of j would resurrect it";
  EXPECT_NE(tomb->Mvcc(), kOldPeerMvcc);
  EXPECT_EQ(tomb->origin_hash, self_origin_hash)
      << "an expiry is always a local decision -- it must be self-originated even while applying "
         "a peer's command, never attributed to that peer";
  EXPECT_NE(tomb->origin_hash, peer_hash);
}

// drakeydb: Phase 4 Task 9, fix round (F1v2) -- proves ExecuteTx applies an author's mvcc AND
// origin_hash correctly regardless of which shard a replicated write's key lands on, which
// AppliedWriteOnFreshKeyKeepsAuthorStampVerbatim above cannot: that test touches exactly one
// key/shard, so it cannot distinguish "correct on every shard" from "correct on whichever shard
// this key happened to hash to." This test constructs a real DflyShardReplica -- exactly as
// Replica::InitiateDflySync does -- and applies through one key per shard (not just one key
// total), asserting the resulting stamp on each.
//
// This does NOT pin the origin-hash registration broadcast itself (InitiateDflySync's shard_cb,
// replica.cc) the way an earlier version of this test did: that version relied on
// DflyShardReplica's own constructor performing the registration, matching what was, at the
// time, production behavior. That behavior was reverted -- see the constructor's own comment and
// task-9-report.md -- after it crashed the server (SIGSEGV, reproduced 5/10 runs of
// test_active_replica_single_peer_replaces in multimaster_test.py) by introducing the
// constructor's first-ever fiber yield point inside InitiateDflySync's tight per-flow
// construction loop. The registration now lives in InitiateDflySync's shard_cb instead, a
// private Replica method with no no-socket construction path this file can reach the way
// DflyShardReplicaOriginTest reaches DflyShardReplica's public constructor -- so
// ApplyOnePeerWriteToEveryShard registers the origin hash manually (shard_set->pool()->AwaitBrief,
// the same shape AppliedWriteOnFreshKeyKeepsAuthorStampVerbatim already uses) rather than
// relying on construction to do it. See ApplyOnePeerWriteToEveryShard's own comment for the full
// account, including why construction and every apply run on thread 0 specifically
// (ServerState::tlocal() must resolve on the calling thread).
//
// Falsifying (see task-9-report.md for the verbatim run): no-op'ing the body of
// JournalExecutor::SetApplyMvcc (executor.h) -- the same mutation that falsifies
// AppliedWriteOnFreshKeyKeepsAuthorStampVerbatim -- makes every shard's Mvcc() EXPECT below fail
// with a freshly-minted HopStamp instead of kAuthorMvcc, since ExecuteTx's non-global path calls
// that same method before every Execute().
TEST_F(MvccStoreTest, AppliedWriteAppliesCorrectlyOnEveryShard) {
  const unsigned num_shards = shard_set->size();
  ASSERT_GT(num_shards, 1u) << "this test's entire point is proving correctness on shards OTHER "
                               "than whichever one a single key would happen to hash to -- with "
                               "only one shard there is nothing to distinguish it from";

  constexpr uint32_t kPeerIdx = 5;
  constexpr uint64_t kAuthorMvcc = 0x9999'0000'1111ULL;
  const uint64_t peer_hash = NodeUuidHash("a1b2c3d4-0000-4000-8000-0000000000aa");

  std::vector<std::string> keys = ApplyOnePeerWriteToEveryShard(kPeerIdx, peer_hash, kAuthorMvcc);
  for (unsigned target_shard = 0; target_shard < num_shards; ++target_shard) {
    auto st = StampOf(keys[target_shard]);
    ASSERT_TRUE(st.has_value()) << "key '" << keys[target_shard] << "' on shard " << target_shard;
    EXPECT_EQ(st->Mvcc(), kAuthorMvcc) << "shard " << target_shard;
    EXPECT_EQ(st->origin_hash, peer_hash) << "shard " << target_shard;
  }
}

namespace {
// drakeydb: review wave 2 (F2, IMPORTANT) -- unlike OriginFlagCapturingConsumer above (which
// reads origin_idx/entry_flags straight off item.journal_item, mirrored there by
// JournalSlice::AddLogRecord), JournalItem carries no mvcc field at all -- only the wire bytes
// do. So this consumer, like OriginOpcodeCapturingConsumer further down this file, reparses
// item.journal_item.data with a real JournalReader to reach ParsedEntry::mvcc.
struct MvccCapturedEntry {
  std::string cmd;
  uint32_t origin_idx;
  uint64_t mvcc;
};

class MvccCapturingConsumer : public journal::JournalConsumerInterface {
 public:
  void ConsumeJournalChange(const journal::JournalChangeItem& item) override {
    io::BytesSource source{item.journal_item.data};
    JournalReader reader{&source, 0};
    journal::ParsedEntry parsed;
    CHECK(!reader.ReadEntry(&parsed));
    util::fb2::LockGuard lk(mu_);
    entries.push_back({std::string(item.cmd), parsed.origin_idx, parsed.mvcc});
  }
  void ThrottleIfNeeded() override {
  }

  util::fb2::Mutex mu_;
  std::vector<MvccCapturedEntry> entries;  // guarded by mu_
};
}  // namespace

// drakeydb: review wave 2 (F2, IMPORTANT) -- tx_base.cc's RecordDelete/RecordDerivedDelete used
// to pass a hardcoded mvcc=0 to journal::RecordEntry regardless of DbContext::repl_mvcc, so
// journal::RecordEntry's "caller supplied no stamp" test (entry.mvcc == 0, journal.cc) was always
// true for a derived DEL and it always minted a fresh LOCAL HopStamp -- even when the DEL was
// itself derived from applying a peer's replicated command. Two peers independently applying the
// same replicated HDEL (each emptying their own copy of the hash) would then diverge onto two
// different, locally-minted stamps for the same logical delete instead of converging on the
// author's one stamp, exactly like AppliedWriteOnFreshKeyKeepsAuthorStampVerbatim above proves
// for ordinary (non-derived) applied writes.
//
// The emptied key itself can't be used to observe this (StampOf would read nullopt either way --
// the key is gone), so this test instead reads the derived DEL's own journal entry (mvcc travels
// on the wire under extended framing -- see JournalItem's comment above) exactly the way a
// downstream plain replica or a peer's Commit()-driven side-table stamp would.
//
// Falsifying: reverting either `db_cntx.repl_mvcc` argument in tx_base.cc back to a hardcoded 0
// makes del->mvcc below come back as a freshly-minted local stamp instead of kAuthorMvcc -- always
// larger, since HopStamp mints from the real current wall clock and kAuthorMvcc here is not a
// live timestamp.
TEST_F(MvccStoreTest, DerivedDeleteFromAppliedWriteKeepsAuthorStamp) {
  constexpr uint32_t kPeerIdx = 4;
  constexpr uint64_t kAuthorMvcc = 0x4242'0000'1111ULL;

  const size_t num_shards = shard_set->size();
  MvccCapturingConsumer consumer;
  std::vector<uint32_t> consumer_ids(num_shards, 0);
  shard_set->RunBriefInParallel([&](EngineShard* shard) {
    journal::StartInThread();
    consumer_ids[shard->shard_id()] = journal::RegisterConsumer(&consumer);
  });

  Run({"hset", "h", "f", "v"});  // local write; its own entry is irrelevant to this test

  ApplyReplicatedCommand({"hdel", "h", "f"}, kPeerIdx, kAuthorMvcc, /*lww_guard=*/false);

  shard_set->RunBriefInParallel(
      [&](EngineShard* shard) { journal::UnregisterConsumer(consumer_ids[shard->shard_id()]); });

  util::fb2::LockGuard lk(consumer.mu_);
  const MvccCapturedEntry* del = nullptr;
  for (auto it = consumer.entries.rbegin(); it != consumer.entries.rend(); ++it) {
    if (it->cmd == "DEL") {
      del = &*it;
      break;
    }
  }
  ASSERT_NE(nullptr, del) << "HDEL emptying the hash must derive a DEL";
  EXPECT_EQ(del->mvcc, kAuthorMvcc) << "a derived DEL caused by an applied write must reproduce "
                                       "the author's stamp, not mint a fresh local one";
  EXPECT_EQ(del->origin_idx, kPeerIdx);
}

// drakeydb: P4-4 Task A3 -- the generic single-key LWW veto's core case: a guarded peer link must
// drop a replicated SET whose author stamp is not strictly newer than the key's own stored stamp,
// leaving the local value AND its stamp completely untouched while still reporting the hop as
// successfully applied (never forcing a resync), and recording exactly one drop with no
// unstamped write left behind.
//
// Falsifying: removing ShouldDropForLww's veto (transaction.cc), i.e. making it always
// `return false`, makes the first EXPECT_EQ below observe "peer" instead of "local".
TEST_F(MvccStoreTest, StalePeerSetDroppedUnderLwwGuard) {
  constexpr uint32_t kPeerIdx = 20;
  constexpr uint64_t kStaleMvcc = 0x1000ULL;  // far smaller than any real HopStamp
  const uint64_t peer_hash = NodeUuidHash("6f1c4c3e-a3a3-4000-8000-000000000020");
  RegisterPeerOriginHash(kPeerIdx, peer_hash);

  ASSERT_EQ(Run({"set", "k", "local"}), "OK");
  auto before_stamp = StampOf("k");
  ASSERT_TRUE(before_stamp.has_value());
  ASSERT_GT(before_stamp->Mvcc(), kStaleMvcc)
      << "sanity: the local stamp must actually be newer than the peer's for this to be a "
         "meaningful drop";

  const uint64_t before_dropped = TotalLwwDropped();
  facade::DispatchResult res =
      ApplyReplicatedCommand({"set", "k", "peer"}, kPeerIdx, kStaleMvcc, /*lww_guard=*/true);
  EXPECT_EQ(res, facade::DispatchResult::OK)
      << "a dropped write must still report success -- signaling it any other way would force a "
         "full resync on every LWW conflict";

  EXPECT_EQ(Run({"get", "k"}), "local")
      << "the stale peer SET must never overwrite the newer local value";
  auto after_stamp = StampOf("k");
  ASSERT_TRUE(after_stamp.has_value());
  EXPECT_EQ(*after_stamp, *before_stamp) << "a dropped write must not disturb the stamp either";
  EXPECT_EQ(TotalLwwDropped(), before_dropped + 1);
  EXPECT_EQ(TotalUnstampedWrites(), 0u) << "a drop must never arm-then-abandon the key";
}

// drakeydb: P4-4 Task A3 -- the guard's sibling/control for the test above: the IDENTICAL stale
// SET, with the link's guard bit off, must still apply its VALUE exactly as arrival-order
// replication always has -- proving the drop above is caused by the guard, not some unrelated
// side effect of ApplyReplicatedCommand or of a stale mvcc by itself.
//
// drakeydb: P4-4 Task A5 -- this test's STAMP assertions changed from its pre-A5 version: SET is
// one of the commands ClassifyJournaledCommand (multimaster_lww.h) guards by NAME, but with the
// link's guard bit off here A3's veto never engages -- exactly the "unguarded command" case
// FloorAppliedStamp (mvcc.h) exists for. "k" already carries a real local stamp `S` by the time
// this stale peer SET commits, so the applied write's stamp is floored to just-below `S`, never
// kStaleMvcc verbatim -- committing verbatim would rewind the key's stamp, letting a LATER write
// whose stamp sits strictly between kStaleMvcc and `S` (older than `S`, but newer than this
// write's own stale mvcc) wrongly win a future comparison against it.
TEST_F(MvccStoreTest, StalePeerSetAppliesWithoutLwwGuard) {
  constexpr uint32_t kPeerIdx = 21;
  constexpr uint64_t kStaleMvcc = 0x1000ULL;
  const uint64_t peer_hash = NodeUuidHash("6f1c4c3e-a3a3-4000-8000-000000000021");
  RegisterPeerOriginHash(kPeerIdx, peer_hash);

  ASSERT_EQ(Run({"set", "k", "local"}), "OK");
  auto before_stamp = StampOf("k");
  ASSERT_TRUE(before_stamp.has_value());
  ASSERT_GT(before_stamp->Mvcc(), kStaleMvcc)
      << "sanity: the local stamp must actually be newer than the peer's for the floor below to "
         "be a meaningful check";

  facade::DispatchResult res =
      ApplyReplicatedCommand({"set", "k", "peer"}, kPeerIdx, kStaleMvcc, /*lww_guard=*/false);
  EXPECT_EQ(res, facade::DispatchResult::OK);

  EXPECT_EQ(Run({"get", "k"}), "peer")
      << "without the guard, replication is plain arrival order -- the peer's write applies "
         "regardless of its (older) mvcc";
  auto st = StampOf("k");
  ASSERT_TRUE(st.has_value());
  EXPECT_EQ(st->Mvcc(), before_stamp->Mvcc())
      << "FloorAppliedStamp keeps the Mvcc() field at S's own -- only origin_hash regresses";
  EXPECT_EQ(st->origin_hash, before_stamp->origin_hash - 1)
      << "an applied write's author stamp is never committed verbatim once a real prior stamp "
         "exists and the author's is older -- floored one origin_hash below it instead (mvcc.h)";
}

// drakeydb: P4-4 Task A3 -- F1 (multimaster_lww.h): a zero incoming mvcc must NEVER be guarded,
// even on a real peer link whose guard bit is on -- the "DFLY link to a non-active node" shape
// (a classic Redis/KeyDB link, by contrast, never sets peer_mode_ at all, so its guard bit is
// never on in production; this test isolates the F1 rule itself, on a registered PEER origin,
// rather than reproducing the classic-link shape literally -- see F2/review-fix-round-2 for why
// kSelfIdx would not exercise the real case). Two independent layers enforce F1: IsLwwGuarded()
// itself requires repl_mvcc_ != 0 (LwwGuardActive), which short-circuits ShouldDropForLww before
// IncomingStamp is ever called; IncomingStamp(0, ...) also independently returns nullopt on its
// own (its own fail-open contract, documented in multimaster_lww.h). Either one alone already
// prevents this specific entry from being vetoed -- removing only one of the two does not
// reproduce a visible failure here. Disabling BOTH (dropping the mvcc half of IsLwwGuarded AND
// IncomingStamp's own `mvcc == 0` check) does: the GET below then observes "local" instead of
// "peer", and BOTH surviving-stamp checks below observe the pre-apply stamp unchanged instead of
// a freshly-minted stamp under the peer's origin.
TEST_F(MvccStoreTest, UnstampedIncomingNeverGuarded) {
  constexpr uint32_t kPeerIdx = 22;
  const uint64_t peer_hash = NodeUuidHash("6f1c4c3e-a3a3-4000-8000-000000000022");
  RegisterPeerOriginHash(kPeerIdx, peer_hash);

  ASSERT_EQ(Run({"set", "k", "local"}), "OK");
  auto before_stamp = StampOf("k");
  ASSERT_TRUE(before_stamp.has_value());

  const uint64_t before_dropped = TotalLwwDropped();
  facade::DispatchResult res =
      ApplyReplicatedCommand({"set", "k", "peer"}, kPeerIdx, /*mvcc=*/0, /*lww_guard=*/true);
  EXPECT_EQ(res, facade::DispatchResult::OK);

  EXPECT_EQ(Run({"get", "k"}), "peer") << "mvcc == 0 must fail open even with the guard bit on";
  auto after_stamp = StampOf("k");
  ASSERT_TRUE(after_stamp.has_value());
  // journal::RecordEntry (journal.cc:141) mints a fresh local HopStamp whenever entry.mvcc == 0,
  // and MvccStamper::Commit (mvcc.cc:129) attributes that mint to entry.origin_idx verbatim --
  // the LINK's peer index, not this node's own -- so the resulting stamp carries the peer's
  // origin_hash even though the entry itself carried no author mvcc.
  EXPECT_GT(after_stamp->Mvcc(), before_stamp->Mvcc())
      << "an mvcc==0 apply still mints a fresh HopStamp on commit -- strictly newer than whatever "
         "was there before, never a no-op and never the incoming (nonexistent) mvcc verbatim";
  EXPECT_EQ(after_stamp->origin_hash, peer_hash)
      << "the freshly-minted stamp is attributed to the LINK's peer origin, not this node's own";
  EXPECT_EQ(TotalLwwDropped(), before_dropped) << "an unstamped apply must never count as a drop";
}

// drakeydb: P4-4 Task A3 -- a genuinely newer peer write must apply with the incoming author's
// stamp EXACTLY -- not max(local, incoming), and not a freshly-minted local stamp -- proving the
// guard only vetoes strictly-older writes, never interferes with a legitimate one.
TEST_F(MvccStoreTest, NewerPeerSetAppliesWithExactIncomingStamp) {
  constexpr uint32_t kPeerIdx = 23;
  const uint64_t peer_hash = NodeUuidHash("6f1c4c3e-a3a3-4000-8000-000000000023");
  RegisterPeerOriginHash(kPeerIdx, peer_hash);

  ASSERT_EQ(Run({"set", "k", "local"}), "OK");
  auto before_stamp = StampOf("k");
  ASSERT_TRUE(before_stamp.has_value());
  const uint64_t newer_mvcc = before_stamp->Mvcc() + 1;

  facade::DispatchResult res =
      ApplyReplicatedCommand({"set", "k", "peer"}, kPeerIdx, newer_mvcc, /*lww_guard=*/true);
  EXPECT_EQ(res, facade::DispatchResult::OK);

  EXPECT_EQ(Run({"get", "k"}), "peer");
  auto after_stamp = StampOf("k");
  ASSERT_TRUE(after_stamp.has_value());
  EXPECT_EQ(after_stamp->Mvcc(), newer_mvcc)
      << "the applied stamp must be the incoming author's stamp EXACTLY -- not a max() with the "
         "local stamp, and not a freshly-minted local one";
  EXPECT_EQ(after_stamp->origin_hash, peer_hash)
      << "must record the AUTHOR's origin hash, never this node's own";
}

// drakeydb: P4-4 Task A3 -- exact-tie half (a): incoming mvcc == stored mvcc, with the peer's
// origin_hash GREATER than self's. MergeAccepts orders lexicographically on (Mvcc(), origin_hash)
// (mvcc.h), so a tie must be broken by origin_hash alone, and the greater one must win/apply.
// self_hash is read off a throwaway control write rather than hardcoded, since it depends on this
// process's randomly-generated boot uuid (same technique as
// LazyExpiryDuringAppliedPeerCommandMintsFreshSelfStamp, above).
TEST_F(MvccStoreTest, ExactTieAppliesWhenIncomingOriginHashIsGreater) {
  constexpr uint32_t kPeerIdx = 24;

  ASSERT_EQ(Run({"set", "control", "v"}), "OK");
  auto control_stamp = StampOf("control");
  ASSERT_TRUE(control_stamp.has_value());
  const uint64_t self_hash = control_stamp->origin_hash;
  const uint64_t peer_hash = self_hash + 1;  // deterministically greater than self's
  RegisterPeerOriginHash(kPeerIdx, peer_hash);

  ASSERT_EQ(Run({"set", "k", "local"}), "OK");
  auto before_stamp = StampOf("k");
  ASSERT_TRUE(before_stamp.has_value());
  ASSERT_EQ(before_stamp->origin_hash, self_hash);

  const uint64_t before_dropped = TotalLwwDropped();
  facade::DispatchResult res = ApplyReplicatedCommand({"set", "k", "peer"}, kPeerIdx,
                                                      before_stamp->Mvcc(), /*lww_guard=*/true);
  EXPECT_EQ(res, facade::DispatchResult::OK);

  EXPECT_EQ(Run({"get", "k"}), "peer")
      << "an exact mvcc tie with a GREATER incoming origin_hash must apply";
  auto after_stamp = StampOf("k");
  ASSERT_TRUE(after_stamp.has_value());
  EXPECT_EQ(after_stamp->Mvcc(), before_stamp->Mvcc());
  EXPECT_EQ(after_stamp->origin_hash, peer_hash);
  EXPECT_EQ(TotalLwwDropped(), before_dropped);
}

// drakeydb: P4-4 Task A3 -- exact-tie half (b): same setup as above, but the peer's origin_hash is
// SMALLER than self's -- owner decision 2026-08-30 (mvcc.h): ties favor the STORED side, so this
// must drop.
TEST_F(MvccStoreTest, ExactTieDroppedWhenIncomingOriginHashIsSmaller) {
  constexpr uint32_t kPeerIdx = 25;

  ASSERT_EQ(Run({"set", "control", "v"}), "OK");
  auto control_stamp = StampOf("control");
  ASSERT_TRUE(control_stamp.has_value());
  const uint64_t self_hash = control_stamp->origin_hash;
  ASSERT_GT(self_hash, 0u) << "sanity: need room below self_hash for a strictly smaller peer hash";
  const uint64_t peer_hash = self_hash - 1;  // deterministically smaller than self's
  RegisterPeerOriginHash(kPeerIdx, peer_hash);

  ASSERT_EQ(Run({"set", "k", "local"}), "OK");
  auto before_stamp = StampOf("k");
  ASSERT_TRUE(before_stamp.has_value());

  const uint64_t before_dropped = TotalLwwDropped();
  facade::DispatchResult res = ApplyReplicatedCommand({"set", "k", "peer"}, kPeerIdx,
                                                      before_stamp->Mvcc(), /*lww_guard=*/true);
  EXPECT_EQ(res, facade::DispatchResult::OK);

  EXPECT_EQ(Run({"get", "k"}), "local")
      << "an exact mvcc tie with a SMALLER incoming origin_hash must drop -- ties favor the "
         "stored side";
  auto after_stamp = StampOf("k");
  ASSERT_TRUE(after_stamp.has_value());
  EXPECT_EQ(*after_stamp, *before_stamp);
  EXPECT_EQ(TotalLwwDropped(), before_dropped + 1);
  EXPECT_EQ(TotalUnstampedWrites(), 0u);
}

// drakeydb: P4-4 Task A3 -- a dropped write must suppress its auto-journal too, not just its own
// callback: SETNX, GETDEL, PERSIST, RESTORE and GETSET journal verbatim via
// Transaction::LogAutoJournalOnShard (no explicit RecordJournal of their own), so without the
// suppression a drop would still forward the client's original command to sub-replicas even
// though this node's own copy was never touched. SET and PEXPIREAT are included too, even though
// they journal explicitly (RecordJournal, inside SetCmd::RecordJournal / OpExpire) rather than via
// auto-journal -- both simply never reach their own RecordJournal call at all, since the callback
// that contains it never runs on a drop.
//
// Falsifying: removing LogAutoJournalOnShard's `if (lww_dropped) return;` early return
// (transaction.cc) -- while leaving RunCallback's own veto (ShouldDropForLww) intact, so the
// callback still never runs -- reproduces the failure this test exists to catch for the five
// auto-journaled blocks (SETNX, GETSET, GETDEL, PERSIST, RESTORE): each
// `EXPECT_EQ(consumer.commands.load(), pre)` observes `pre + 1` instead, because the skipped
// callback's forced `OpStatus::OK` result now sails through the pre-existing
// `result.status != OpStatus::OK` gate. The SET and PEXPIREAT blocks do NOT fail under this
// falsification alone: their own journal call (SetCmd::RecordJournal / OpExpire's RecordJournal)
// lives inside the callback body, which the still-intact veto keeps from ever running, so this
// specific removal has nothing to expose for those two -- the callback-skip in RunCallback is
// what covers them, not this early return.
TEST_F(MvccStoreTest, DroppedApplySuppressesAutoJournalForEveryGuardedSingleKeyCommand) {
  constexpr uint32_t kPeerIdx = 26;
  constexpr uint64_t kStaleMvcc = 0x1000ULL;
  const uint64_t peer_hash = NodeUuidHash("6f1c4c3e-a3a3-4000-8000-000000000026");
  RegisterPeerOriginHash(kPeerIdx, peer_hash);

  class CountingJournalConsumer final : public journal::JournalConsumerInterface {
   public:
    void ConsumeJournalChange(const journal::JournalChangeItem& item) override {
      if (item.journal_item.opcode == journal::Op::COMMAND)
        ++commands;
    }
    void ThrottleIfNeeded() override {
    }
    std::atomic<size_t> commands{0};
  } consumer;
  std::vector<uint32_t> consumer_ids(shard_set->size());
  shard_set->RunBriefInParallel([&](EngineShard* shard) {
    consumer_ids[shard->shard_id()] = journal::RegisterConsumer(&consumer);
  });
  absl::Cleanup unregister_consumer = [&] {
    shard_set->RunBriefInParallel(
        [&](EngineShard* shard) { journal::UnregisterConsumer(consumer_ids[shard->shard_id()]); });
  };

  {  // SET -- manually journaled (SetCmd::RecordJournal).
    SCOPED_TRACE("SET");
    ASSERT_EQ(Run({"set", "sj_set", "local"}), "OK");
    auto before = StampOf("sj_set");
    ASSERT_TRUE(before.has_value());
    const size_t pre = consumer.commands.load();
    const uint64_t pre_dropped = TotalLwwDropped();
    auto res = ApplyReplicatedCommand({"set", "sj_set", "peer"}, kPeerIdx, kStaleMvcc, true);
    EXPECT_EQ(res, facade::DispatchResult::OK);
    EXPECT_EQ(consumer.commands.load(), pre);
    EXPECT_EQ(Run({"get", "sj_set"}), "local");
    EXPECT_EQ(StampOf("sj_set"), before);
    EXPECT_EQ(TotalLwwDropped(), pre_dropped + 1);
  }

  {  // SETNX -- auto-journaled verbatim. sj_setnx is deleted (tombstoned), not merely left
     // absent or live: on a LIVE pre-existing key, SETNX's own IF_NOTEXIST precondition makes an
     // actually-applied write indistinguishable from a dropped one -- SetCmd::Set returns
     // SKIPPED either way, leaving value/stamp/journal count identically untouched, which would
     // make the value/stamp/journal-count checks below pass regardless of whether the veto ever
     // fired (the drop counter would still distinguish them, but a single surviving assertion is
     // a thin foundation for this block's whole point). A tombstoned key still carries a real
     // MVCC stamp for ShouldDropForLww to compare against (DbSlice::GetMvcc returns it regardless
     // of liveness), but SETNX's own precondition IS satisfied on it (the key does not exist), so
     // an actually-applied SETNX would create it with "peer" -- giving this block a real,
     // observable difference between applied and dropped.
    SCOPED_TRACE("SETNX");
    ASSERT_EQ(Run({"set", "sj_setnx", "local"}), "OK");
    ASSERT_EQ(Run({"del", "sj_setnx"}).GetInt(), 1);
    auto before = StampOf("sj_setnx");
    ASSERT_TRUE(before.has_value());
    ASSERT_TRUE(before->IsTombstone());
    const size_t pre = consumer.commands.load();
    const uint64_t pre_dropped = TotalLwwDropped();
    auto res = ApplyReplicatedCommand({"setnx", "sj_setnx", "peer"}, kPeerIdx, kStaleMvcc, true);
    EXPECT_EQ(res, facade::DispatchResult::OK);
    EXPECT_EQ(consumer.commands.load(), pre);
    EXPECT_EQ(Run({"exists", "sj_setnx"}).GetInt(), 0)
        << "a dropped SETNX on a tombstoned key must NOT create it -- an applied one would";
    EXPECT_EQ(StampOf("sj_setnx"), before);
    EXPECT_EQ(TotalLwwDropped(), pre_dropped + 1);
  }

  {  // GETSET -- auto-journaled verbatim.
    SCOPED_TRACE("GETSET");
    ASSERT_EQ(Run({"set", "sj_getset", "local"}), "OK");
    auto before = StampOf("sj_getset");
    ASSERT_TRUE(before.has_value());
    const size_t pre = consumer.commands.load();
    const uint64_t pre_dropped = TotalLwwDropped();
    auto res = ApplyReplicatedCommand({"getset", "sj_getset", "peer"}, kPeerIdx, kStaleMvcc, true);
    EXPECT_EQ(res, facade::DispatchResult::OK);
    EXPECT_EQ(consumer.commands.load(), pre);
    EXPECT_EQ(Run({"get", "sj_getset"}), "local");
    EXPECT_EQ(StampOf("sj_getset"), before);
    EXPECT_EQ(TotalLwwDropped(), pre_dropped + 1);
  }

  {  // GETDEL -- auto-journaled verbatim; also proves the key is not actually deleted.
    SCOPED_TRACE("GETDEL");
    ASSERT_EQ(Run({"set", "sj_getdel", "local"}), "OK");
    auto before = StampOf("sj_getdel");
    ASSERT_TRUE(before.has_value());
    const size_t pre = consumer.commands.load();
    const uint64_t pre_dropped = TotalLwwDropped();
    auto res = ApplyReplicatedCommand({"getdel", "sj_getdel"}, kPeerIdx, kStaleMvcc, true);
    EXPECT_EQ(res, facade::DispatchResult::OK);
    EXPECT_EQ(consumer.commands.load(), pre);
    EXPECT_EQ(Run({"exists", "sj_getdel"}).GetInt(), 1) << "the key must NOT be deleted";
    EXPECT_EQ(Run({"get", "sj_getdel"}), "local");
    EXPECT_EQ(StampOf("sj_getdel"), before);
    EXPECT_EQ(TotalLwwDropped(), pre_dropped + 1);
  }

  {  // PERSIST -- auto-journaled verbatim; also proves the TTL survives.
    SCOPED_TRACE("PERSIST");
    ASSERT_EQ(Run({"set", "sj_persist", "local", "px", "100000"}), "OK");
    auto before = StampOf("sj_persist");
    ASSERT_TRUE(before.has_value());
    ASSERT_GT(Run({"pttl", "sj_persist"}).GetInt(), 0) << "sanity: must actually have a TTL";
    const size_t pre = consumer.commands.load();
    const uint64_t pre_dropped = TotalLwwDropped();
    auto res = ApplyReplicatedCommand({"persist", "sj_persist"}, kPeerIdx, kStaleMvcc, true);
    EXPECT_EQ(res, facade::DispatchResult::OK);
    EXPECT_EQ(consumer.commands.load(), pre);
    EXPECT_GT(Run({"pttl", "sj_persist"}).GetInt(), 0) << "the TTL must survive the drop";
    EXPECT_EQ(StampOf("sj_persist"), before);
    EXPECT_EQ(TotalLwwDropped(), pre_dropped + 1);
  }

  {  // PEXPIREAT -- manually journaled (OpExpire's own RecordJournal); also proves no TTL is set.
    SCOPED_TRACE("PEXPIREAT");
    ASSERT_EQ(Run({"set", "sj_pexpireat", "local"}), "OK");
    auto before = StampOf("sj_pexpireat");
    ASSERT_TRUE(before.has_value());
    ASSERT_EQ(Run({"ttl", "sj_pexpireat"}).GetInt(), -1) << "sanity: no TTL yet";
    const size_t pre = consumer.commands.load();
    const uint64_t pre_dropped = TotalLwwDropped();
    const std::string future_ms = absl::StrCat(GetCurrentTimeMs() + 1'000'000);
    auto res = ApplyReplicatedCommand({"pexpireat", "sj_pexpireat", future_ms}, kPeerIdx,
                                      kStaleMvcc, true);
    EXPECT_EQ(res, facade::DispatchResult::OK);
    EXPECT_EQ(consumer.commands.load(), pre);
    EXPECT_EQ(Run({"ttl", "sj_pexpireat"}).GetInt(), -1)
        << "the key must remain without a TTL -- the drop must never set one";
    EXPECT_EQ(StampOf("sj_pexpireat"), before);
    EXPECT_EQ(TotalLwwDropped(), pre_dropped + 1);
  }

  {  // RESTORE -- auto-journaled verbatim. The dump is of a DIFFERENT key's value than
     // sj_restore's own current one, so an incorrectly-applied RESTORE...REPLACE is actually
     // observable: restoring the key's own unchanged dump verbatim would leave the value
     // identical either way and prove nothing about whether the callback ran.
    SCOPED_TRACE("RESTORE");
    ASSERT_EQ(Run({"set", "sj_restore", "local"}), "OK");
    ASSERT_EQ(Run({"set", "sj_restore_donor", "peer_would_win"}), "OK");
    std::string dump = Run({"dump", "sj_restore_donor"}).GetString();
    auto before = StampOf("sj_restore");
    ASSERT_TRUE(before.has_value());
    const size_t pre = consumer.commands.load();
    const uint64_t pre_dropped = TotalLwwDropped();
    auto res = ApplyReplicatedCommand({"restore", "sj_restore", "0", dump, "REPLACE"}, kPeerIdx,
                                      kStaleMvcc, true);
    EXPECT_EQ(res, facade::DispatchResult::OK);
    EXPECT_EQ(consumer.commands.load(), pre);
    EXPECT_EQ(Run({"get", "sj_restore"}), "local")
        << "a dropped RESTORE must not replace the key with the donor's different value";
    EXPECT_EQ(StampOf("sj_restore"), before);
    EXPECT_EQ(TotalLwwDropped(), pre_dropped + 1);
  }

  std::move(unregister_consumer).Invoke();
  EXPECT_EQ(TotalUnstampedWrites(), 0u) << "none of the drops above may arm-then-abandon a key";
}

// drakeydb: P4-4 Task A3 -- out-of-scope classes must be left completely alone by this task's
// generic veto: INCR is kUnguarded (delta-journaled RMW -- dropping a delta would permanently
// lose it, not just reorder it), and DEL/MSET are kMultiKeySelfGuarded (their own per-key guard is
// tasks A7/A8, not this one) -- GetShardArgs on MSET yields keys AND values in one contiguous
// range, which is exactly why this generic single-key helper must never touch it. All three must
// still apply a stale peer write, and none may ever be counted as an LWW drop by this task.
//
// drakeydb: P4-4 Task A5 -- every stamp assertion below changed from this test's pre-A5 version:
// each of these three keys already carries a real local stamp `S` (from its own `Run({"set"/
// "incr", ...})` setup below) that is newer than kStaleMvcc, so this is precisely the "unguarded
// applied write with an author stamp older than S" case FloorAppliedStamp (mvcc.h) exists for --
// none of these three commands is vetoed (that is this test's whole point), but their COMMITTED
// stamp is still floored to just-below `S`, never kStaleMvcc verbatim -- verbatim would rewind
// the key's stamp, letting a LATER write whose stamp sits strictly between kStaleMvcc and `S`
// (older than `S`, but newer than this write's own stale mvcc) wrongly win a future comparison.
TEST_F(MvccStoreTest, UnguardedAndSelfGuardedClassesAreNotVetoedByThisTask) {
  constexpr uint32_t kPeerIdx = 27;
  constexpr uint64_t kStaleMvcc = 0x1000ULL;
  const uint64_t peer_hash = NodeUuidHash("6f1c4c3e-a3a3-4000-8000-000000000027");
  RegisterPeerOriginHash(kPeerIdx, peer_hash);

  {  // INCR -- kUnguarded.
    SCOPED_TRACE("INCR");
    ASSERT_EQ(Run({"incr", "sj_incr"}).GetInt(), 1);
    auto before = StampOf("sj_incr");
    ASSERT_TRUE(before.has_value());
    ASSERT_GT(before->Mvcc(), kStaleMvcc) << "sanity: S must be newer than the stale author mvcc";
    const uint64_t pre_dropped = TotalLwwDropped();
    auto res = ApplyReplicatedCommand({"incr", "sj_incr"}, kPeerIdx, kStaleMvcc, true);
    EXPECT_EQ(res, facade::DispatchResult::OK);
    EXPECT_EQ(Run({"get", "sj_incr"}), "2")
        << "INCR is unguarded -- a stale mvcc must never veto it";
    auto after = StampOf("sj_incr");
    ASSERT_TRUE(after.has_value());
    EXPECT_EQ(after->Mvcc(), before->Mvcc())
        << "the INCR itself actually ran (the value changed above), but FloorAppliedStamp keeps "
           "the Mvcc() field at S's own, never the stale kStaleMvcc verbatim";
    EXPECT_EQ(after->origin_hash, before->origin_hash - 1)
        << "floored one origin_hash below S -- never the peer's own origin_hash verbatim";
    EXPECT_NE(*after, *before) << "sanity: the stamp must have actually changed -- the floor "
                                  "lands BELOW before, not above it";
    EXPECT_EQ(TotalLwwDropped(), pre_dropped) << "INCR must never count as an LWW drop";
  }

  {  // DEL -- kMultiKeySelfGuarded; A8's own guard does not exist yet.
    SCOPED_TRACE("DEL");
    ASSERT_EQ(Run({"set", "sj_del", "local"}), "OK");
    auto before = StampOf("sj_del");
    ASSERT_TRUE(before.has_value());
    ASSERT_GT(before->Mvcc(), kStaleMvcc) << "sanity: S must be newer than the stale author mvcc";
    const uint64_t pre_dropped = TotalLwwDropped();
    auto res = ApplyReplicatedCommand({"del", "sj_del"}, kPeerIdx, kStaleMvcc, true);
    EXPECT_EQ(res, facade::DispatchResult::OK);
    EXPECT_EQ(Run({"exists", "sj_del"}).GetInt(), 0)
        << "before A8 adds DEL's own per-key guard, a stale peer DEL must still apply";
    auto tomb = StampOf("sj_del");
    ASSERT_TRUE(tomb.has_value()) << "an explicit DEL must leave a tombstone, not erase the slot";
    EXPECT_TRUE(tomb->IsTombstone());
    EXPECT_EQ(tomb->Mvcc(), before->Mvcc())
        << "the applied DEL actually ran (the key is gone above), but its tombstone's stamp is "
           "floored to just-below S -- never the incoming author's stale mvcc verbatim";
    EXPECT_EQ(tomb->origin_hash, before->origin_hash - 1);
    EXPECT_EQ(TotalLwwDropped(), pre_dropped)
        << "this generic single-key veto must never classify or count DEL";
  }

  {  // MSET -- kMultiKeySelfGuarded; A7's own guard does not exist yet.
    SCOPED_TRACE("MSET");
    ASSERT_EQ(Run({"set", "sj_msk1", "local1"}), "OK");
    ASSERT_EQ(Run({"set", "sj_msk2", "local2"}), "OK");
    auto before1 = StampOf("sj_msk1");
    auto before2 = StampOf("sj_msk2");
    ASSERT_TRUE(before1.has_value());
    ASSERT_TRUE(before2.has_value());
    ASSERT_GT(before1->Mvcc(), kStaleMvcc);
    ASSERT_GT(before2->Mvcc(), kStaleMvcc);
    const uint64_t pre_dropped = TotalLwwDropped();
    auto res = ApplyReplicatedCommand({"mset", "sj_msk1", "peer1", "sj_msk2", "peer2"}, kPeerIdx,
                                      kStaleMvcc, true);
    EXPECT_EQ(res, facade::DispatchResult::OK);
    EXPECT_EQ(Run({"get", "sj_msk1"}), "peer1")
        << "before A7 adds MSET's own per-key guard, a stale peer MSET must still apply";
    EXPECT_EQ(Run({"get", "sj_msk2"}), "peer2");
    auto st1 = StampOf("sj_msk1");
    auto st2 = StampOf("sj_msk2");
    ASSERT_TRUE(st1.has_value());
    ASSERT_TRUE(st2.has_value());
    EXPECT_EQ(st1->Mvcc(), before1->Mvcc())
        << "MSET's own commit floors each key's stamp to just-below ITS OWN prior stamp -- "
           "never the incoming author's stale mvcc verbatim";
    EXPECT_EQ(st1->origin_hash, before1->origin_hash - 1);
    EXPECT_EQ(st2->Mvcc(), before2->Mvcc());
    EXPECT_EQ(st2->origin_hash, before2->origin_hash - 1);
    EXPECT_EQ(TotalLwwDropped(), pre_dropped)
        << "this generic single-key veto must never classify or count MSET";
  }
}

// ---------------------------------------------------------------------------
// P4-4 Task A5: FloorAppliedStamp's end-to-end proof. See mvcc.h's declaration for the pure-
// function why; mvcc_test.cc's FloorAppliedStampTest suite covers the function in isolation.
// ---------------------------------------------------------------------------

// drakeydb: P4-4 Task A5 -- the headline case the floor exists for: an UNGUARDED applied RMW
// (APPEND is not one of the names ClassifyJournaledCommand, multimaster_lww.h, guards, so A3's
// veto never even classifies it) whose author authored it before it ever saw this key's local
// stamp `S`. Item (i): the write still applies (its value change is real, not silently dropped),
// but its COMMITTED stamp floors to just-below `S` instead of the author's own (older) mvcc
// verbatim.
// Item (ii): with the floor in place, a stale GUARDED SET whose mvcc sits strictly between the
// APPEND's stale author mvcc and the ORIGINAL `S` must still be dropped -- proving the floor kept
// the key's stamp high enough to reject it, exactly as it would have rejected anything below `S`
// itself. See UnguardedAppliedDeleteWithOlderAuthorMvccFloorsTheTombstone (below) for the
// tombstone-arm sibling of this test, and rdb_test.cc's
// MergeLwwOnFullSyncHealsRmwDivergenceLeftByAnAppliedFloor for item (iii), the heal.
TEST_F(MvccStoreTest, UnguardedAppliedRmwWithOlderAuthorMvccFloorsInsteadOfRewindingTheStamp) {
  constexpr uint32_t kPeerIdx = 30;
  const uint64_t peer_hash = NodeUuidHash("6f1c4c3e-a5a5-4000-8000-000000000030");
  RegisterPeerOriginHash(kPeerIdx, peer_hash);

  ASSERT_EQ(Run({"set", "k", "v1"}), "OK");
  auto stored = StampOf("k");
  ASSERT_TRUE(stored.has_value());
  constexpr uint64_t kAuthorMvcc = 0x1000ULL;  // strictly older than any real HopStamp
  ASSERT_GT(stored->Mvcc(), kAuthorMvcc) << "sanity: S must be newer than the author's mvcc";

  // (i) APPEND is unguarded and its author mvcc is older than S -- the write applies, but its
  // stamp floors to just-below S rather than the author's stale mvcc verbatim.
  facade::DispatchResult res =
      ApplyReplicatedCommand({"append", "k", "x"}, kPeerIdx, kAuthorMvcc, /*lww_guard=*/false);
  EXPECT_EQ(res, facade::DispatchResult::OK);
  EXPECT_EQ(Run({"get", "k"}), "v1x") << "APPEND's own delta must still apply to the value";

  auto floored = StampOf("k");
  ASSERT_TRUE(floored.has_value());
  EXPECT_EQ(floored->Mvcc(), stored->Mvcc())
      << "FloorAppliedStamp keeps the Mvcc() field at S's own";
  EXPECT_EQ(floored->origin_hash, stored->origin_hash - 1)
      << "and lands exactly one origin_hash below S -- never the author's peer origin_hash "
         "verbatim, or a rewound stamp would let a later stale guarded write wrongly win";

  // (ii) a stale GUARDED SET with mvcc strictly between the APPEND's kAuthorMvcc and the
  // ORIGINAL S must still be dropped: the floor kept the key's live stamp high enough to reject
  // it, so the dirty "v1x" value survives untouched.
  const uint64_t before_dropped = TotalLwwDropped();
  const uint64_t mid_mvcc = kAuthorMvcc + (stored->Mvcc() - kAuthorMvcc) / 2;
  ASSERT_GT(mid_mvcc, kAuthorMvcc);
  ASSERT_LT(mid_mvcc, stored->Mvcc());
  res = ApplyReplicatedCommand({"set", "k", "stale"}, kPeerIdx, mid_mvcc, /*lww_guard=*/true);
  EXPECT_EQ(res, facade::DispatchResult::OK)
      << "a dropped write must still report success -- see StalePeerSetDroppedUnderLwwGuard above";
  EXPECT_EQ(Run({"get", "k"}), "v1x")
      << "the stale guarded SET must still be dropped after the floor -- the dirty RMW value "
         "must survive";
  EXPECT_EQ(TotalLwwDropped(), before_dropped + 1);
  auto after_veto = StampOf("k");
  ASSERT_TRUE(after_veto.has_value());
  EXPECT_EQ(*after_veto, *floored) << "the veto must not disturb the floored stamp either";
  EXPECT_EQ(TotalUnstampedWrites(), 0u) << "a drop must never arm-then-abandon the key";
}

// drakeydb: P4-4 Task A5 -- the tombstone-arm sibling of the RMW test above. DEL is
// kMultiKeySelfGuarded (its own per-key guard is a later task, not this one), so with the link's
// guard bit off here it is simply an unguarded applied delete -- PerformDeletionAtomic's own
// captured prev_stamp (db_slice.cc) is what lets the floor apply here at all, since by the time
// journal::RecordEntry's Commit() runs, the slot no longer holds `S` itself (only the delete's own
// zero-authority placeholder does). This covers any unguarded applied delete whose author stamp
// is older than the key's own -- a derived DEL from an applied RMW that empties a container, a
// plain DEL applied with the guard OFF, or (as here) a directly-applied DEL on an unguarded link.
TEST_F(MvccStoreTest, UnguardedAppliedDeleteWithOlderAuthorMvccFloorsTheTombstone) {
  constexpr uint32_t kPeerIdx = 31;
  const uint64_t peer_hash = NodeUuidHash("6f1c4c3e-a5a5-4000-8000-000000000031");
  RegisterPeerOriginHash(kPeerIdx, peer_hash);

  ASSERT_EQ(Run({"set", "k", "v1"}), "OK");
  auto stored = StampOf("k");
  ASSERT_TRUE(stored.has_value());
  constexpr uint64_t kAuthorMvcc = 0x1000ULL;
  ASSERT_GT(stored->Mvcc(), kAuthorMvcc) << "sanity: S must be newer than the author's mvcc";

  const size_t tombstones_before = GetMetrics().db_stats[0].mvcc_tombstones;
  facade::DispatchResult res =
      ApplyReplicatedCommand({"del", "k"}, kPeerIdx, kAuthorMvcc, /*lww_guard=*/false);
  EXPECT_EQ(res, facade::DispatchResult::OK);
  EXPECT_EQ(Run({"exists", "k"}).GetInt(), 0) << "the applied DEL must actually remove the key";

  auto tomb = StampOf("k");
  ASSERT_TRUE(tomb.has_value()) << "an explicit DEL must leave a tombstone, not erase the slot";
  EXPECT_TRUE(tomb->IsTombstone());
  EXPECT_EQ(tomb->Mvcc(), stored->Mvcc())
      << "the tombstone's Mvcc() floors to S's own, not the author's stale mvcc verbatim";
  EXPECT_EQ(tomb->origin_hash, stored->origin_hash - 1)
      << "and lands one origin_hash below S -- PerformDeletionAtomic's captured prev_stamp is "
         "what makes S available here, since the placeholder has already overwritten the slot "
         "by the time this commits";
  EXPECT_EQ(GetMetrics().db_stats[0].mvcc_tombstones, tombstones_before + 1)
      << "mvcc_tombstones must advance by exactly one for the floored tombstone -- "
         "SetExistingMvcc keys its accounting off the NEW stamp's bit, which is still set";
}

// drakeydb: P4-4 Task A5 fix round 1 -- a LIVE applied write over a TOMBSTONED slot. Before this
// fix, DbSlice::PostUpdate called EnsureMvcc (which resets a tombstone slot to {0,0} and returns
// void) BEFORE Arm(), so by the time journal::RecordEntry's Commit() ran, a live lookup of this
// key's slot saw {0,0} (fresh), never the tombstone T -- the applied write committed its stale
// author mvcc verbatim instead of flooring against T. EnsureMvcc now returns the stamp it found
// (T, here) and PostUpdate carries it on the plain arm itself, so the floor reads THAT, never a
// live lookup.
TEST_F(MvccStoreTest, UnguardedAppliedRmwOverATombstonedSlotFloorsAgainstTheTombstone) {
  constexpr uint32_t kPeerIdx = 32;
  const uint64_t peer_hash = NodeUuidHash("6f1c4c3e-a5a5-4000-8000-000000000032");
  RegisterPeerOriginHash(kPeerIdx, peer_hash);

  ASSERT_EQ(Run({"set", "k", "v"}), "OK");
  ASSERT_EQ(Run({"del", "k"}).GetInt(), 1);
  auto tomb = StampOf("k");
  ASSERT_TRUE(tomb.has_value());
  ASSERT_TRUE(tomb->IsTombstone()) << "sanity: k must actually be a tombstone (T), not erased";
  constexpr uint64_t kAuthorMvcc = 0x1000ULL;  // strictly older than any real HopStamp
  ASSERT_GT(tomb->Mvcc(), kAuthorMvcc) << "sanity: T must be newer than the author's mvcc";

  const size_t tombstones_before = GetMetrics().db_stats[0].mvcc_tombstones;
  facade::DispatchResult res =
      ApplyReplicatedCommand({"append", "k", "x"}, kPeerIdx, kAuthorMvcc, /*lww_guard=*/false);
  EXPECT_EQ(res, facade::DispatchResult::OK);
  EXPECT_EQ(Run({"get", "k"}), "x") << "APPEND over an absent (tombstoned) key creates it fresh";

  auto floored = StampOf("k");
  ASSERT_TRUE(floored.has_value());
  EXPECT_FALSE(floored->IsTombstone()) << "the recreated key is LIVE -- the floor must never "
                                          "leave a live write looking like a tombstone";
  // Whole-stamp equality: pins bit 63 at exactly 0 too, not merely IsTombstone() being falsy.
  EXPECT_EQ(*floored, (MvccStamp{tomb->Mvcc(), tomb->origin_hash - 1}))
      << "the plain (live) arm's own captured prev_stamp -- DbSlice::EnsureMvcc's return, carried "
         "by PostUpdate -- is T itself, captured by EnsureMvcc's tombstone-clearing branch BEFORE "
         "it reset the slot; the write floors against T, never the {0,0} the table would show by "
         "the time journal::RecordEntry's Commit() actually runs, and never kAuthorMvcc verbatim";
  EXPECT_EQ(GetMetrics().db_stats[0].mvcc_tombstones, tombstones_before - 1)
      << "the tombstone credit is released exactly once, by EnsureMvcc's own clear -- this plain "
         "arm's commit must not re-tombstone it (a.tombstone is false for this arm)";

  // A stale guarded SET with mvcc strictly between the APPEND's kAuthorMvcc and the ORIGINAL T
  // must still be dropped: the floor kept the key's live stamp high enough to reject it.
  const uint64_t before_dropped = TotalLwwDropped();
  const uint64_t mid_mvcc = kAuthorMvcc + (tomb->Mvcc() - kAuthorMvcc) / 2;
  ASSERT_GT(mid_mvcc, kAuthorMvcc);
  ASSERT_LT(mid_mvcc, tomb->Mvcc());
  res = ApplyReplicatedCommand({"set", "k", "stale"}, kPeerIdx, mid_mvcc, /*lww_guard=*/true);
  EXPECT_EQ(res, facade::DispatchResult::OK);
  EXPECT_EQ(Run({"get", "k"}), "x") << "the stale guarded SET must still be dropped after the "
                                       "floor -- the recreated value must survive";
  EXPECT_EQ(TotalLwwDropped(), before_dropped + 1);
  EXPECT_EQ(TotalUnstampedWrites(), 0u) << "a drop must never arm-then-abandon the key";
}

// drakeydb: P4-4 Task A5 fix round 2 -- the "DEL + re-create in one entry" case fix round 1
// found but did not close: RENAME onto an EXISTING dest, applied unguarded, is Renamer::
// DeserializeDest's own delete-then-recreate dance (generic_family.cc) -- but ONLY when src and
// dest land on DIFFERENT shards. RenameGeneric (generic_family.cc:1229-1236) routes a
// single-shard rename (src and dest already on the SAME shard, tx->GetUniqueShardCnt() == 1)
// straight to OpRen instead, which overwrites dest IN PLACE (FindMutable + post_updater.Run) and
// never deletes it at all -- no placeholder, no tombstone arm, nothing to inherit. This test
// therefore picks src_key/dest_key explicitly on DIFFERENT shards (asserted below, not left to
// chance), forcing the CROSS-shard path: DelMutable(dest) -> ArmTombstone (captures dest's true
// prior stamp S) -> loader.Add(dest) + post_updater.Run() -> EnsureMvcc (sees
// PerformDeletionAtomic's own placeholder, NOT S) -> Arm(). Without the round-2 fix, dest's live
// (recreate) arm floors against that placeholder (Mvcc()==0, "nothing to floor against") and
// commits its author stamp VERBATIM. MvccStamper::Arm now recognizes the placeholder shape and
// inherits the still-pending tombstone arm's own prev_stamp (S) instead (mvcc.h/.cc), so dest's
// live arm floors against S directly: a SINGLE floor, never verbatim and never floored twice
// (once via the tombstone arm's own commit, again via the live arm's).
TEST_F(MvccStoreTest, RenameOntoExistingDestAppliedUnguardedFloorsOnceAgainstDestStamp) {
  constexpr uint32_t kPeerIdx = 41;
  const uint64_t peer_hash = NodeUuidHash("6f1c4c3e-a5a5-4000-8000-000000000041");
  RegisterPeerOriginHash(kPeerIdx, peer_hash);

  // Same shard-search idiom ExpiryMidMultiKeyAppliedWriteKeepsSiblingAuthorMvcc (above) uses for
  // the opposite requirement: here src_key and dest_key must land on DIFFERENT shards, or RENAME
  // takes the single-shard OpRen path instead of the cross-shard Renamer::DeserializeDest one
  // this test exists to exercise.
  const unsigned num_shards = shard_set->size();
  ASSERT_GT(num_shards, 1u) << "this test's entire point is exercising the CROSS-shard "
                               "Renamer::DeserializeDest path, which needs at least 2 shards";
  std::string src_key, dest_key;
  for (int i = 0; dest_key.empty(); ++i) {
    CHECK_LT(i, 10000) << "could not find a key on a different shard from src_key";
    std::string cand = absl::StrCat("rn", i);
    if (src_key.empty()) {
      src_key = cand;
    } else if (Shard(cand, num_shards) != Shard(src_key, num_shards)) {
      dest_key = cand;
    }
  }
  ASSERT_NE(Shard(src_key, num_shards), Shard(dest_key, num_shards))
      << "sanity: src_key and dest_key must land on different shards, or this test silently "
         "degrades to exercising OpRen instead of Renamer::DeserializeDest";

  ASSERT_EQ(Run({"set", src_key, "v1"}), "OK");
  ASSERT_EQ(Run({"set", dest_key, "v0"}), "OK");
  auto src_before = StampOf(src_key);
  auto dest_before = StampOf(dest_key);
  ASSERT_TRUE(src_before.has_value());
  ASSERT_TRUE(dest_before.has_value());
  constexpr uint64_t kAuthorMvcc = 0x1000ULL;  // strictly older than any real HopStamp
  ASSERT_GT(dest_before->Mvcc(), kAuthorMvcc) << "sanity: dest's S must be newer than the author's";

  facade::DispatchResult res = ApplyReplicatedCommand({"rename", src_key, dest_key}, kPeerIdx,
                                                      kAuthorMvcc, /*lww_guard=*/false);
  EXPECT_EQ(res, facade::DispatchResult::OK);
  EXPECT_EQ(Run({"get", dest_key}), "v1") << "the rename's value must still apply";
  EXPECT_EQ(Run({"exists", src_key}).GetInt(), 0);

  auto dest = StampOf(dest_key);
  ASSERT_TRUE(dest.has_value());
  EXPECT_FALSE(dest->IsTombstone())
      << "the destination still ends up live, self-correcting exactly as "
         "RenameOntoAnExistingDestSelfCorrectsToALiveStamp proves for a LOCAL rename";
  // Whole-stamp equality: pins bit 63 at exactly 0, and Mvcc()/origin_hash exactly -- a SINGLE
  // floor against dest's own true prior stamp, not verbatim (kAuthorMvcc) and not floored twice
  // (dest_before.origin_hash - 2, which double-flooring through the placeholder-then-real chain
  // would produce).
  EXPECT_EQ(*dest, (MvccStamp{dest_before->Mvcc(), dest_before->origin_hash - 1}))
      << "dest's live (recreate) arm must inherit the tombstone arm's captured S, floor against "
         "it directly -- never PerformDeletionAtomic's own placeholder, and never S already "
         "floored once by the tombstone arm's own commit";

  auto src_tomb = StampOf(src_key);
  ASSERT_TRUE(src_tomb.has_value());
  EXPECT_TRUE(src_tomb->IsTombstone());
  EXPECT_EQ(*src_tomb, (MvccStamp{src_before->Mvcc(), src_before->origin_hash - 1}.AsTombstone()))
      << "src's own tombstone floors against ITS OWN prior stamp, per the ordinary single-arm "
         "rule -- src was never re-armed live in this same entry, so it needs no inheritance";
}

TEST_F(MvccStoreTest, TableMatchesPrimeAfterMixedWorkload) {
  for (int i = 0; i < 200; ++i) {
    Run({"set", absl::StrCat("k", i), "v"});
    if (i % 7 == 0) {
      // Rename the key just written -- renaming k(i-1) would hit one the i%3 branch deleted,
      // and RENAME on a missing key errors.
      Run({"rename", absl::StrCat("k", i), absl::StrCat("r", i)});
    } else if (i % 3 == 0) {
      Run({"del", absl::StrCat("k", i)});
    }
  }

  EXPECT_EQ(SumMvccMismatchesAcrossShards(), 0u)
      << "every live key needs exactly one stamp, and vice versa";
}

// drakeydb: review wave 2 (F1, CRITICAL) -- HDEL emptying a hash arms the key TWICE: ExecuteW's
// own it_res->post_updater.Run() (hset_family.cc) arms it once, then -- because the hash is now
// empty -- DeleteHw takes a SECOND, independent FindMutable/AutoUpdater on the same still-present
// key and arms it again via its own post_updater.Run(), before finally deleting it.
// MvccStamper::Disarm (mvcc.cc) used to erase only the first matching arm and `return`, leaving
// one arm behind for a key PerformDeletionAtomic had just erased from `prime`; the derived DEL's
// own journal::RecordEntry->Commit then reinserted a side-table stamp for that now-nonexistent
// key. This is the literal repro from the phase's review brief: under --active_replica,
// `HSET h f v` then `HDEL h f` aborted the whole process with
// `Check failed: dbp->mvcc->size() - dbp->stats.mvcc_tombstones == dbp->prime.size() (1 vs. 0)`
// at db_slice.cc's OnCbFinishBlocking -- reproduced verbatim before this fix; see
// final-fix-report.md.
//
// Falsifying: restoring Disarm's early `return` after the first erase (mvcc.cc) reproduces that
// exact DCHECK abort inside this test's own HDEL call -- see final-fix-report.md for the verbatim
// output.
//
// drakeydb: P4-3 Task 2 -- HDEL emptying a hash reaches PerformDeletionAtomic via DeleteHw's
// default DeleteReason::kExplicit (hset_family.cc), same as a plain DEL, so since Task 2 "h"
// keeps its slot as a tombstone instead of being erased. What this test still pins is the shape
// the comment above documents: the stray leftover arm a single-match Disarm would have left
// behind must not desync mvcc_entries/mvcc_tombstones (SumMvccMismatchesAcrossShards) even though
// it no longer produces an absent stamp.
TEST_F(MvccStoreTest, HdelEmptyingHashDoesNotResurrectAStamp) {
  Run({"hset", "h", "f", "v"});
  ASSERT_TRUE(StampOf("h").has_value());
  Run({"hdel", "h", "f"});
  auto tomb = StampOf("h");
  ASSERT_TRUE(tomb.has_value())
      << "the HDEL's own journal entry must stamp the tombstone it armed, not leave the key absent";
  EXPECT_TRUE(tomb->IsTombstone());
  EXPECT_EQ(SumMvccMismatchesAcrossShards(), 0u);
}

// drakeydb: review wave 2 (F1, CRITICAL) -- the same double-arm shape as
// HdelEmptyingHashDoesNotResurrectAStamp above, via a different pair of call sites:
// OpFieldExpire's own auto_updater.Run() (generic_family.cc) arms the key once, then --
// discovering the named field already lazily expired while trying to re-arm it, and the hash now
// empty -- HSetFamily::DeleteIfEmpty takes a second, independent FindMutable/AutoUpdater and arms
// it again before deleting. Recipe (a hash with one member whose TTL has already elapsed,
// re-probed via FIELDEXPIRE) is the hash counterpart of
// OriginJournalFamilyTest.FieldExpireCausedDeleteIsNotFlaggedDerived (this file), which pins this
// same recipe's journal-flag behavior on a SET; AdvanceTime (a mocked clock, no real sleep) is
// what keeps this deterministic instead of racing the ~100ms member-expiry reaper heartbeat.
//
// Falsifying: restoring Disarm's early `return` (mvcc.cc) reproduces the same
// mvcc->size()/prime->size() DCHECK abort as the HDEL test above -- see final-fix-report.md.
//
// drakeydb: P4-3 Task 11 -- renamed from FieldExpireEmptyingHashDoesNotResurrectAStamp and
// flipped its final assertion. HSetFamily::DeleteIfEmpty's derived DEL is DeleteReason::kExpired
// and already deleted before journaling (one of the "three other" sites named in DeleteReason's
// comment, db_slice.h) -- it needed no reordering -- but PerformDeletionAtomic's earns_tombstone
// branch (db_slice.cc) cannot distinguish which kExpired call site it came from: now that kExpired
// joins kExplicit there, this delete earns a tombstone too, exactly like HdelEmptyingHashDoes
// NotResurrectAStamp above. What this test still pins is unchanged: the double-arm shape must not
// desync mvcc_entries/mvcc_tombstones (SumMvccMismatchesAcrossShards), whether the result is an
// erase (pre-Task-11) or a tombstone (post-Task-11).
TEST_F(MvccStoreTest, FieldExpireEmptyingHashEarnsATombstone) {
  ASSERT_EQ(Run({"hset", "feh", "f", "v"}).GetInt(), 1);
  Run({"fieldexpire", "feh", "1", "f"});
  AdvanceTime(1100);
  Run({"fieldexpire", "feh", "1", "f"});

  // Guard against a vacuous pass: the hash must have actually been cleaned up.
  ASSERT_EQ(Run({"exists", "feh"}).GetInt(), 0);
  auto tomb = StampOf("feh");
  ASSERT_TRUE(tomb.has_value())
      << "the derived DEL's own journal entry must stamp the tombstone it armed, not leave the "
         "key absent";
  EXPECT_TRUE(tomb->IsTombstone());
  EXPECT_NE(tomb->Mvcc(), 0u)
      << "must carry the derived DEL's own real, minted stamp -- not a stuck zero-authority "
         "{kTombstoneBit, 0} placeholder (review M3: this test's own has_value()+IsTombstone() "
         "checks above would both pass for a stuck placeholder too)";
  EXPECT_EQ(SumMvccMismatchesAcrossShards(), 0u);
}

// drakeydb: review wave 2 (F1, CRITICAL) -- the SET counterpart of
// FieldExpireEmptyingHashEarnsATombstone above: OpFieldExpire's is_set branch calls
// SetFamily::DeleteSetIfEmpty (set_family.cc) instead of HSetFamily::DeleteIfEmpty, but takes the
// identical second-FindMutable/second-arm shape. Same recipe as
// OriginJournalFamilyTest.FieldExpireCausedDeleteIsNotFlaggedDerived (this file), which pins this
// scenario's journal-flag behavior; this test pins the mvcc side-table invariant instead.
//
// Falsifying: restoring Disarm's early `return` (mvcc.cc) reproduces the same
// mvcc->size()/prime->size() DCHECK abort as the two tests above -- see final-fix-report.md.
//
// drakeydb: P4-3 Task 11 -- renamed from FieldExpireEmptyingSetDoesNotResurrectAStamp; see
// FieldExpireEmptyingHashEarnsATombstone's comment above for why this now tombstones instead of
// erasing (SetFamily::DeleteSetIfEmpty is the SET-side analog of the "three other" sites named in
// DeleteReason's comment, db_slice.h).
TEST_F(MvccStoreTest, FieldExpireEmptyingSetEarnsATombstone) {
  ASSERT_EQ(Run({"sadd", "fes", "m"}).GetInt(), 1);
  Run({"fieldexpire", "fes", "1", "m"});
  AdvanceTime(1100);
  Run({"fieldexpire", "fes", "1", "m"});

  // Guard against a vacuous pass: the set must have actually been cleaned up.
  ASSERT_EQ(Run({"exists", "fes"}).GetInt(), 0);
  auto tomb = StampOf("fes");
  ASSERT_TRUE(tomb.has_value())
      << "the derived DEL's own journal entry must stamp the tombstone it armed, not leave the "
         "key absent";
  EXPECT_TRUE(tomb->IsTombstone());
  EXPECT_NE(tomb->Mvcc(), 0u)
      << "must carry the derived DEL's own real, minted stamp -- not a stuck zero-authority "
         "{kTombstoneBit, 0} placeholder (review M3: this test's own has_value()+IsTombstone() "
         "checks above would both pass for a stuck placeholder too)";
  EXPECT_EQ(SumMvccMismatchesAcrossShards(), 0u);
}

// drakeydb: Phase 4, P4-1 Task 10 -- MEMORY DEFRAGSEGMENTS (memory_cmd.cc:346's
// MemoryCmd::DefragmentSegments) is the ONLY caller of DbSlice::DefragTableSegments. DEBUG
// COMPACT-TABLE is a different mechanism entirely (buddy-segment merging via
// EngineShard::CompactTable) and would exercise none of the mirror loop this test targets -- using
// it here would pass vacuously, never reaching DefragTableSegments at all.
//
// Falsifying: see task-10-report.md for the verbatim run. Commenting out the EraseMvcc call in
// PerformDeletionAtomic reproduces the same "stamp with no live key" failure here as in
// TableMatchesPrimeAfterMixedWorkload above, since this test's setup also deletes half its keys.
TEST_F(MvccStoreTest, DefragRelocationPreservesStamps) {
  for (int i = 0; i < 500; ++i)
    Run({"set", absl::StrCat("k", i), std::string(200, 'x')});
  for (int i = 0; i < 500; i += 2)
    Run({"del", absl::StrCat("k", i)});

  auto before_stamp = StampOf("k1");
  ASSERT_TRUE(before_stamp.has_value());
  const MvccStamp before = *before_stamp;
  Run({"memory", "defragsegments"});  // the ONLY caller of DefragTableSegments (memory_cmd.cc:346).
                                      // NOT "debug compact-table", a different mechanism entirely.
  auto after_stamp = StampOf("k1");
  ASSERT_TRUE(after_stamp.has_value());
  EXPECT_EQ(*after_stamp, before) << "defrag must not lose or corrupt stamps";

  EXPECT_EQ(SumMvccMismatchesAcrossShards(), 0u);
}

// drakeydb: fix round 1 (F1, CRITICAL) -- DefragRelocationPreservesStamps above cannot, by
// construction, distinguish a present mirror loop from a deleted one: TryRelocateSegment is a
// content-preserving move local to whichever DashTable instance it is called on, invisible to
// StampOf/TEST_VerifyMvccTable, both of which resolve by hash lookup, never by segment position
// (recorded, with that test's own falsification proving it, in task-10-report.md). This test
// follows the template DefragDflyEngineTest.SegmentsRelocated (dragonfly_test.cc) already uses to
// prove the identical property for `prime`: force every segment to be reported "under-utilized"
// (PageUsage::SetForceReallocate(true) -- an unconditional-true override of the virtual
// IsPageForObjectUnderUtilized that DefragTableSegments' real caller, memory_cmd.cc, consults),
// collect every segment's address before and after one DefragTableSegments call, and assert every
// address changed. Applied to db->mvcc here, not db->prime.
//
// Falsifying: see task-10-report.md fix round 1. Replacing the mvcc mirror loop in
// DefragTableSegments (db_slice.cc) with `return;` immediately after the prime loop -- the exact
// mutation DefragRelocationPreservesStamps's own comment already tried and documented as
// undetectable by that test -- makes this test fail: every mvcc segment address is unchanged
// before/after.
TEST_F(MvccStoreTest, DefragActuallyRelocatesMvccSegments) {
  constexpr size_t kKeys = 5000;
  Run({"DEBUG", "POPULATE", std::to_string(kKeys), "key", "32"});

  shard_set->RunBriefInParallel([&](EngineShard* shard) {
    DbSlice& slice = namespaces->GetDefaultNamespace().GetDbSlice(shard->shard_id());
    DbTable* db = slice.GetDBTable(0);
    ASSERT_TRUE(db->mvcc) << "active-replica mode must allocate the mvcc side table";
    auto& mvcc = *db->mvcc;

    auto collect_addresses = [&] {
      absl::flat_hash_map<size_t, uintptr_t> seg_ptrs;
      detail::DashCursor cursor;
      do {
        auto [next, segment] = mvcc.VisitSegment(cursor);
        cursor = next;
        if (!segment) {
          ADD_FAILURE() << "Valid cursor did not resolve to a segment";
          return seg_ptrs;
        }
        seg_ptrs.emplace(segment->first, reinterpret_cast<uintptr_t>(segment->second));
      } while (cursor);
      return seg_ptrs;
    };

    const auto before = collect_addresses();
    const size_t size_before = mvcc.size();
    ASSERT_GT(before.size(), 1u) << "need more than one segment for this test to mean anything";

    PageUsage page_usage{CollectPageStats::NO, 0, CycleQuota::Unlimited()};
    page_usage.SetForceReallocate(true);

    slice.DefragTableSegments(0, &page_usage);

    const auto after = collect_addresses();

    EXPECT_EQ(after.size(), before.size());
    EXPECT_EQ(mvcc.size(), size_before);

    for (const auto& [sid, old_address] : before) {
      ASSERT_TRUE(after.contains(sid));
      EXPECT_NE(after.at(sid), old_address) << "mvcc segment " << sid << " was not relocated";
    }
  });

  EXPECT_EQ(SumMvccMismatchesAcrossShards(), 0u) << "forced defrag must not corrupt the mvcc table";
}

// drakeydb: Phase 4, P4-1 Task 10 -- rdb_load.cc's CreateObjectOnShard inserts every loaded key
// via DbSlice::AddOrUpdate. Its ItAndUpdater's AutoUpdater DOES eventually call PostUpdate (on
// destruction, or explicitly beside a tiered-storage stash -- db_slice.cc), so a loaded key does
// get armed; fix round 1 (F2) covers the separate bug that arm exposed (nothing in the load path
// ever committed or discarded it, so a later, unrelated write's Commit() could clobber this
// test's stamp -- {0,0} at the time this was written, now (P4-2 Task 2) whatever was actually
// persisted -- with local authority no peer ever saw -- see
// ReloadDoesNotLeaveArmsForALaterWriteToClobber, below). This test only proves the simpler,
// first-order property Step 3b was for: the reload path leaves every key stamped at all, dense
// with prime, immediately after the reload -- without an explicit stamp (persisted, or {0,0} when
// no record was present) beside the SetMCFlag mirror, a loaded key would be dense in prime but
// absent from mvcc, tripping OnCbFinishBlocking's DCHECK (db_slice.cc) on the next command that
// reaches it. ReloadDoesNotLeaveArmsForALaterWriteToClobber (below) is where the exact stamp
// VALUE surviving the round trip is actually checked.
//
// Needs --dbfilename set, or DEBUG RELOAD silently no-ops (BaseFamilyTest::SetUpTestSuite sets it
// to "" globally) -- that exact vacuous-test trap was caught on P4-0. Unique per pid, matching
// ReaperJournalFamilyTest's two RDB round-trip tests above (this file), the working precedent this
// follows: SetTestFlag alone is sufficient here (no ResetService/InitWithDbFilename needed)
// because DoSave reads the flag live. absl::FlagSaver (fix round 1, Minor) restores it on scope
// exit, matching MultiMasterFamilyTest's saver_ -- without it, dbfilename leaked globally into
// every later test in this binary that also reads or sets it.
//
// drakeydb: Task 11 fix round 1 (F2) -- --dir also pointed at a private GetTestTempPath, matching
// MultiMasterFamilyTest's constructor (:509, this file): dbfilename alone has no directory
// component (ValidateFilename, save_stages_controller.cc, rejects one outright), so without this
// the save/reload below wrote its .dfs files into whatever the process's cwd happened to be --
// the repo root, when this binary is run directly rather than via ctest. Same flag_saver restores
// it on scope exit.
TEST_F(MvccStoreTest, ReloadedKeysAreStampedSoTheInvariantHolds) {
  absl::FlagSaver flag_saver;
  absl::SetFlag(&FLAGS_dir, base::GetTestTempPath("mvcc_reload_test"));
  BaseFamilyTest::SetTestFlag("dbfilename", absl::StrCat("mvcc_reload_test_", getpid()));

  for (int i = 0; i < 100; ++i)
    Run({"set", absl::StrCat("k", i), "v"});
  ASSERT_EQ(Run({"debug", "reload"}), "OK");
  EXPECT_EQ(GetMetrics().db_stats[0].mvcc_entries, 100u)
      << "a reload that leaves keys unstamped trips the dense invariant on the next write";
  // EXISTS, not DBSIZE: verified empirically, not assumed (see task-10-report.md) -- DBSIZE
  // (ServerFamily::DbSize, server_family.cc) dispatches via a bare shard_set->RunBriefInParallel
  // call and never goes through Transaction::RunCallback/OnCbFinishBlocking at all. A write would
  // also prepare its own zero-authority slot before reaching the invariant, adding unnecessary
  // mutation to this probe. GenericFamily::Exists
  // (generic_family.cc) drives a real Transaction, the same shape as the HLEN calls that already
  // caught this bug's Section 5 instances (task-10-report.md) -- confirmed here by disabling
  // Step 3b's stamp and observing EXISTS reach and trip the DCHECK where DBSIZE had not.
  Run({"exists", "k0"});  // must not DCHECK -- a debug build aborts the whole test binary, not
                          // just this one test, if the invariant is violated here.
}

// drakeydb: fix round 1 (F2, IMPORTANT) -- regression test for the coordinator's finding, verified
// before being fixed (see task-10-report.md): rdb_load.cc's CreateObjectOnShard arms every loaded
// key (AddOrUpdate's AutoUpdater, see the comment above) but this file never journals a load, so
// nothing ever calls MvccStamper::Commit() for those arms. Without an EndOfWriteEpoch() call
// somewhere in the load path, those arms sat in armed_ until the next unrelated journaled write on
// the same shard thread, whose own Commit() then stamped every still-armed key -- not just its
// own -- with that write's mvcc/origin, clobbering whatever the load path had installed with local
// authority no peer ever saw: exactly the "stamp without propagation" direction D-7 forbids, and it
// would do so silently (TEST_VerifyMvccTable/OnCbFinishBlocking only check density, never the
// stamp's value).
//
// drakeydb: P4-2 Task 2 -- reload_keys are written BEFORE the reload, under this fixture's real
// (active, journaled) write path, so each one already carries a genuine, non-zero stamp by the
// time SAVE captures it; Task 1's saver then emits RDB_OPCODE_DF_MVCC for it, and Task 2's loader
// installs it verbatim. That stamp -- not {0,0} -- is what must survive both the reload itself
// and the trigger writes below unclobbered; comparing against the captured pre_reload_stamps
// (rather than Empty()) is a strictly stronger check than the original {0,0}-only version: it
// catches both arm leakage (this test's original purpose) AND a loader that silently drops or
// re-mints the persisted authority instead of installing it verbatim.
//
// One trigger write per shard, not one write total: armed_ is per-shard-thread (MvccStamper is
// thread-local), so a bug here only clobbers reload keys sharing a shard with the trigger write --
// a single trigger key would leave every other shard's reloaded keys looking correct regardless of
// whether the fix is present, the same "passes only because the fixture happens to hash everything
// onto one shard" trap this phase has hit before.
TEST_F(MvccStoreTest, ReloadDoesNotLeaveArmsForALaterWriteToClobber) {
  absl::FlagSaver flag_saver;
  // drakeydb: Task 11 fix round 1 (F2) -- see ReloadedKeysAreStampedSoTheInvariantHolds's comment
  // above for why --dir is set here too, not just --dbfilename.
  absl::SetFlag(&FLAGS_dir, base::GetTestTempPath("mvcc_reload_arm_leak_test"));
  BaseFamilyTest::SetTestFlag("dbfilename", absl::StrCat("mvcc_reload_arm_leak_test_", getpid()));

  const unsigned num_shards = shard_set->size();
  std::vector<std::string> reload_keys(num_shards);
  std::vector<MvccStamp> pre_reload_stamps(num_shards);
  for (unsigned target_shard = 0; target_shard < num_shards; ++target_shard) {
    for (int i = 0;; ++i) {
      CHECK_LT(i, 10000) << "could not find a key hashing to shard " << target_shard;
      reload_keys[target_shard] = absl::StrCat("r", i);
      if (Shard(reload_keys[target_shard], num_shards) == target_shard)
        break;
    }
    Run({"set", reload_keys[target_shard], "v"});
    auto st = StampOf(reload_keys[target_shard]);
    ASSERT_TRUE(st.has_value()) << reload_keys[target_shard];
    ASSERT_FALSE(st->Empty())
        << reload_keys[target_shard]
        << ": guard against a vacuous pass -- the pre-reload write itself must get a real "
           "stamp before this test can check it survives the round trip";
    pre_reload_stamps[target_shard] = *st;
  }

  ASSERT_EQ(Run({"debug", "reload"}), "OK");

  // Guard against a vacuous pass: every reloaded key's persisted stamp must survive the reload
  // itself, exactly, before the trigger writes below run, or this test would "pass" against a
  // reload path that lost or altered the stamp entirely, not just one that leaks arms.
  for (unsigned target_shard = 0; target_shard < num_shards; ++target_shard) {
    auto st = StampOf(reload_keys[target_shard]);
    ASSERT_TRUE(st.has_value()) << reload_keys[target_shard];
    ASSERT_EQ(*st, pre_reload_stamps[target_shard])
        << reload_keys[target_shard] << ": the persisted stamp did not survive the reload itself";
  }

  // One real, journaled write per shard -- the only thing that ever calls MvccStamper::Commit().
  for (unsigned target_shard = 0; target_shard < num_shards; ++target_shard) {
    std::string trigger;
    for (int i = 0;; ++i) {
      CHECK_LT(i, 10000) << "could not find a trigger key hashing to shard " << target_shard;
      trigger = absl::StrCat("trigger", i);
      if (Shard(trigger, num_shards) == target_shard)
        break;
    }
    Run({"set", trigger, "v"});
    auto trigger_stamp = StampOf(trigger);
    ASSERT_TRUE(trigger_stamp.has_value()) << trigger;
    EXPECT_FALSE(trigger_stamp->Empty())
        << trigger << ": the triggering write itself must still get a real stamp";
  }

  for (unsigned target_shard = 0; target_shard < num_shards; ++target_shard) {
    auto st = StampOf(reload_keys[target_shard]);
    ASSERT_TRUE(st.has_value()) << reload_keys[target_shard];
    EXPECT_EQ(*st, pre_reload_stamps[target_shard])
        << reload_keys[target_shard]
        << ": a reloaded key's persisted stamp was clobbered by a later, unrelated write -- "
           "its arm leaked past the load and got picked up by that write's Commit()";
  }
}

// drakeydb: Phase 4, P4-1 Task 11 -- DEBUG MVCC, modelled on DebugCmd::Inspect's shard-hop.
TEST_F(MvccStoreTest, DebugMvccReportsValueState) {
  Run({"set", "k", "v"});
  auto resp = Run({"debug", "mvcc", "k"});
  EXPECT_THAT(resp.GetString(), testing::HasSubstr("state:value"));
  EXPECT_THAT(resp.GetString(), testing::HasSubstr("mvcc:"));
  EXPECT_THAT(resp.GetString(), testing::HasSubstr("origin:"));
}

// drakeydb: P4-3 Task 2 -- the "state:tombstone" branch (debugcmd.cc) was wired in ahead of
// anything setting kTombstoneBit; a kExplicit DEL is now the first live path that reaches it.
//
// drakeydb: P4-3 Task 8 -- strengthened beyond substring presence: the task-8 brief asks this
// test to "assert the tombstone state + stamp", so this parses the printed `mvcc:<N>` value back
// out and cross-checks it against DbSlice::GetMvcc's own stamp for the same key (StampOf), the
// same side-table read production code (and DEBUG MVCC itself) uses. Catches a regression where
// the reply says "state:tombstone" but prints a stale, zero, or otherwise wrong stamp.
TEST_F(MvccStoreTest, DebugMvccReportsTombstoneState) {
  Run({"set", "k", "v"});
  Run({"del", "k"});

  auto tomb = StampOf("k");
  ASSERT_TRUE(tomb.has_value());
  ASSERT_TRUE(tomb->IsTombstone());

  auto resp = Run({"debug", "mvcc", "k"});
  const std::string body = resp.GetString();
  EXPECT_THAT(body, testing::HasSubstr("state:tombstone"));
  EXPECT_THAT(body, testing::HasSubstr(absl::StrCat("mvcc:", tomb->Mvcc())))
      << "printed stamp must match the side table's own tombstone stamp: " << body;
  EXPECT_THAT(body, testing::HasSubstr("origin:"));
}

TEST_F(MvccStoreTest, DebugMvccReportsAbsent) {
  EXPECT_THAT(Run({"debug", "mvcc", "nope"}).GetString(), testing::HasSubstr("state:absent"));
}

TEST_F(MvccStoreTest, DebugMvccVerifyReportsZeroMismatches) {
  for (int i = 0; i < 50; ++i)
    Run({"set", absl::StrCat("k", i), "v"});
  EXPECT_THAT(Run({"debug", "mvcc", "verify"}).GetString(), testing::HasSubstr("mismatches:0"));
}

// Not part of D9's acceptance test, but the third produced interface (alongside <key> and
// VERIFY above) -- covered here so a crash or empty-reply regression in the aggregate path
// doesn't first surface in production INFO/ops usage.
TEST_F(MvccStoreTest, DebugMvccWithNoKeyReportsPerShardAggregates) {
  Run({"set", "k", "v"});
  auto resp = Run({"debug", "mvcc"});
  EXPECT_THAT(resp.GetString(), testing::HasSubstr("shard0_entries:"));
  EXPECT_THAT(resp.GetString(), testing::HasSubstr("shard0_clock_ahead_ms:"));
  EXPECT_THAT(resp.GetString(), testing::HasSubstr("shard0_unstamped_writes:"));
}

// drakeydb: P4-3 Task 8 -- task-2-report.md flagged this as a minor gap: INFO memory got
// mvcc_tombstones_dropped, but the DEBUG MVCC no-key aggregate (this command) did not, leaving an
// operator staring at DEBUG MVCC with no way to see a shard degrading to resurrection. Drives a
// real cap-triggered drop (same shape as MvccStoreTest.DeleteAtTombstoneCapDegradesToEraseAndCounts
// TheDrop above) rather than asserting a placeholder zero, so this fails if the aggregate's count
// is wired to the wrong field or never incremented, not just if the label is missing.
TEST_F(MvccStoreTest, DebugMvccAggregateReportsTombstonesDropped) {
  absl::SetFlag(&FLAGS_multi_master_max_tombstones, 1);
  absl::Cleanup restore = [] { absl::SetFlag(&FLAGS_multi_master_max_tombstones, 1000000); };

  unsigned n = shard_set->size();
  std::string first, second;
  for (int i = 0;; ++i) {
    first = absl::StrCat("dbg-cap-first-", i);
    second = absl::StrCat("dbg-cap-second-", i);
    if (Shard(first, n) == Shard(second, n))
      break;
    CHECK_LT(i, 10000) << "could not find a same-shard key pair";
  }
  ShardId sid = Shard(first, n);

  Run({"set", first, "v"});
  Run({"del", first});
  Run({"set", second, "v"});
  Run({"del", second});  // this shard is already at cap: degrades to erase, counts as a drop

  auto resp = Run({"debug", "mvcc"});
  EXPECT_THAT(resp.GetString(),
              testing::HasSubstr(absl::StrCat("shard", sid, "_tombstones_dropped:1")))
      << resp.GetString();
}

// The "off means byte-identical to upstream" guard.
TEST_F(BaseFamilyTest, NonActiveModeAllocatesNoMvccTable) {
  Run({"set", "k", "v"});
  EXPECT_EQ(GetMetrics().db_stats[0].mvcc_table_bytes, 0u)
      << "a non-active node must pay nothing for MVCC";
}

// drakeydb: Phase 4, P4-1 Task 11 -- DEBUG, not DFLY (see debugcmd.cc's DebugCmd::Mvcc comment):
// must refuse by naming the flag rather than reporting a bare state:absent for every key, which
// would be indistinguishable from a real absence.
TEST_F(BaseFamilyTest, DebugMvccIsRefusedWhenInactive) {
  auto resp = Run({"debug", "mvcc", "k"});
  EXPECT_THAT(resp.GetString(), testing::HasSubstr("active_replica"))
      << "must explain itself rather than reporting a bare absent";
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

  // drakeydb: P4-4 Task A4 -- drives ONE command through a REAL MULTI/EXEC squash on a single
  // JournalExecutor to reach a SQUASHED_STUB's RunSquashedMultiCb. MultiCommandSquasher::Execute
  // has other callers too (Service::DispatchSquashedBatch's pipeline-batch path, and the
  // EVAL-squash path in FlushEvalAsyncCmds, both main_service.cc); MULTI/EXEC is used here because
  // it's the shape this fixture's own SquashedStubInheritsParentOrigin test (above) already uses,
  // extended to also set repl_lww_guard_/repl_mvcc_ per link (SetApplyLwwGuard/SetApplyMvcc), the
  // way a real peer flow would (MultiCommandSquasher::PrepareShardInfo's SetReplOrigin copy, and
  // the squashed-stub ctor in transaction.cc, are what carry them onto the stub). Unlike
  // MvccStoreTest's ApplyReplicatedCommand (fresh JournalExecutor per call), MULTI's queued state
  // lives on the executor's own ConnectionContext, so MULTI and EXEC here must share one instance.
  facade::DispatchResult ExecSquashedGuardedCommand(std::vector<std::string> cmd_args,
                                                    uint32_t origin_idx, uint64_t mvcc,
                                                    bool lww_guard) {
    facade::DispatchResult multi_res = facade::DispatchResult::ERROR;
    facade::DispatchResult cmd_res = facade::DispatchResult::ERROR;
    facade::DispatchResult exec_res = facade::DispatchResult::ERROR;
    pp_->at(0)
        ->LaunchFiber([&] {
          JournalExecutor executor(service_.get());
          executor.SetApplyOrigin(origin_idx);
          executor.SetApplyMvcc(mvcc);
          executor.SetApplyLwwGuard(lww_guard);

          auto dispatch = [&](std::vector<std::string> parts) {
            journal::ParsedEntry::CmdData cmd_data;
            cmd_data.Assign(parts.begin(), parts.end(), parts.size());
            return executor.Execute(0, cmd_data);
          };
          multi_res = dispatch({"MULTI"});
          cmd_res = dispatch(cmd_args);
          exec_res = dispatch({"EXEC"});
        })
        .Join();
    EXPECT_EQ(facade::DispatchResult::OK, multi_res);
    EXPECT_EQ(facade::DispatchResult::OK, cmd_res);
    return exec_res;
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

// drakeydb: P4-4 Task A4 acceptance case for the fail-open tripwire (transaction.cc,
// RunSquashedMultiCb). RunSquashedMultiCb is a second run point that bypasses RunCallback (and
// its ShouldDropForLww veto) entirely; in production it is reachable with a replicated-apply
// context only from a classic Redis/KeyDB link, which never carries an mvcc, so IsLwwGuarded()
// can never be true there today. This test forces that case anyway: a JournalExecutor whose link
// is marked guarded (SetApplyLwwGuard(true)) with a non-zero author stamp
// (SetApplyMvcc(kGuardedMvcc)), driving a real MULTI/SET/EXEC so MultiCommandSquasher builds a
// SQUASHED_STUB that inherits both (its own SetReplOrigin copy, plus the squashed-stub
// constructor in transaction.cc) -- exactly the seam the tripwire exists to catch.
//
// "threadsafe" death-test style avoids forking mid-flight of this fixture's own proactor/shard
// threads -- the same workaround and rationale as
// RdbMvccTest.RollbackFreshInsertRefusesNonFreshEntry (rdb_test.cc).
//
// NOT wrapped in #ifndef NDEBUG (unlike a plain DCHECK death test): absl's DFATAL pseudo-severity
// is well-defined in both build types -- FATAL when NDEBUG is undefined, ERROR otherwise
// (absl/log/log.h, absl/base/log_severity.h) -- so under NDEBUG, EXPECT_DEBUG_DEATH runs the
// statement in this same process without expecting a crash, and the #ifdef NDEBUG block below
// asserts the fail-open release behavior instead: the guarded write still applies, never silently
// dropped.
//
// Falsification: with the `if (IsLwwGuarded())` block deleted from RunSquashedMultiCb, this test
// fails with:
//   Death test: ExecSquashedGuardedCommand({"SET", "guarded-squash-key", "v"}, kPeerOrigin,
//   kGuardedMvcc, true)
//       Result: failed to die.
//    Error msg:
//   [  DEATH   ]
TEST_F(OriginJournalFamilyTest, GuardedSquashedStubTripwireFiresInDebug) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  constexpr uint32_t kPeerOrigin = 11;
  constexpr uint64_t kGuardedMvcc = 42;

  EXPECT_DEBUG_DEATH(
      ExecSquashedGuardedCommand({"SET", "guarded-squash-key", "v"}, kPeerOrigin, kGuardedMvcc,
                                 /*lww_guard=*/true),
      "multi-master LWW guard reached RunSquashedMultiCb");

#ifdef NDEBUG
  // Under NDEBUG the statement above ran for real (no crash) -- confirm it actually failed open:
  // the guarded write must still be visible, not dropped.
  EXPECT_EQ("v", Run({"get", "guarded-squash-key"}));
#endif  // NDEBUG
}

// drakeydb: P4-4 Task A4 -- the tripwire's control that isolates repl_lww_guard_: guard=false
// with a non-zero mvcc must not trip IsLwwGuarded() (LwwGuardActive, multimaster_lww.h, requires
// the guard bit set), so the squashed stub applies normally -- like today's only production
// caller (a classic Redis/KeyDB link via DispatchSquashedBatch, which never sets
// repl_lww_guard). A misfiring tripwire on this case would abort the whole process in a debug
// build (LOG(DFATAL) is LOG(FATAL) there), not merely fail an assertion, so this test passing is
// itself real evidence, not a vacuous one.
TEST_F(OriginJournalFamilyTest, NonGuardedSquashedStubNeverTripsTripwire) {
  constexpr uint32_t kPeerOrigin = 12;
  constexpr uint64_t kAuthorMvcc = 42;

  facade::DispatchResult exec_res = ExecSquashedGuardedCommand(
      {"SET", "unguarded-squash-key", "v"}, kPeerOrigin, kAuthorMvcc, /*lww_guard=*/false);
  EXPECT_EQ(facade::DispatchResult::OK, exec_res);
  EXPECT_EQ("v", Run({"get", "unguarded-squash-key"}));
}

// drakeydb: P4-4 Task A4 -- the tripwire's control that isolates repl_mvcc_: F1's rule (mvcc == 0
// is never guarded, since a classic Redis/KeyDB link never carries one) holds even with
// repl_lww_guard_ set -- LwwGuardActive (multimaster_lww.h) requires BOTH the guard bit and a
// non-zero mvcc. As with the guard=false control above, a misfiring tripwire here would abort the
// process in a debug build rather than let this test merely fail an assertion.
TEST_F(OriginJournalFamilyTest, ZeroMvccSquashedStubNeverTripsTripwire) {
  constexpr uint32_t kPeerOrigin = 13;

  facade::DispatchResult exec_res = ExecSquashedGuardedCommand(
      {"SET", "zero-mvcc-squash-key", "v"}, kPeerOrigin, /*mvcc=*/0, /*lww_guard=*/true);
  EXPECT_EQ(facade::DispatchResult::OK, exec_res);
  EXPECT_EQ("v", Run({"get", "zero-mvcc-squash-key"}));
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

  // drakeydb: P4-3 Task 11, Step 4 -- the brief's explicit non-active-replica ordering check:
  // "expiring-key"'s own baseline write (its SET) must still be journaled before the expiry's
  // mutation (its DEL) after ExpireIfNeeded's reorder in db_slice.cc. That reorder only moved
  // RecordExpiryBlocking relative to Del() INSIDE ExpireIfNeeded; PerformDeletionAtomic's
  // FiberAtomicGuard does NOT span that whole region (it is scoped to that one function, and the
  // jumped-over region includes the events_recording block's channel_store->SendMessages, which
  // may suspend -- see ExpireIfNeeded's own comment, db_slice.cc, for the corrected invariant
  // after review). What actually prevents another command's entry from landing in between here:
  // this GET dispatches through Transaction::RunCallback, and running_tx_ (set for the callback's
  // whole duration) blocks EngineShard::PollExecution/::Heartbeat from dispatching any OTHER
  // transaction on this shard thread until the callback returns (engine_shard.cc:614,799) -- so no
  // other command's journal entry can be emitted in the window this reorder crosses, regardless of
  // whether anything inside it yields. This fixture never sets --active_replica, so this is
  // exactly a non-active replica's view of the wire.
  int last_set_idx = -1, expiry_del_idx = -1;
  for (size_t i = 0; i < consumer.entries.size(); ++i) {
    if (consumer.entries[i].cmd == "SET")
      last_set_idx = static_cast<int>(i);
    if (consumer.entries[i].cmd == "DEL" &&
        (consumer.entries[i].entry_flags & journal::kEntryFlagExpired))
      expiry_del_idx = static_cast<int>(i);
  }
  ASSERT_NE(-1, last_set_idx) << "expiring-key's own SET must have been captured";
  ASSERT_NE(-1, expiry_del_idx) << "the expiry DEL must have been captured";
  EXPECT_LT(last_set_idx, expiry_del_idx)
      << "the key's baseline (its SET) must be journaled before its mutation (the expiry DEL)";
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
// adversarial review pass: SORT's own source-side effects must reach peers just like FIELDEXPIRE,
// so OpFetchContainerElements/OpFetchSortEntries' derived DEL must also reach peers -- same
// hazard, same fix (SortSourceEffectsMustReplicate, generic_family.cc -- renamed from
// WillAutoJournalVerbatim in P4-3 Task 7's fix round, when SORT gained CO::NO_AUTOJOURNAL and
// this predicate had to stop depending on whether SORT's own STORE effect happens to replay
// verbatim or hand-journal, since it answers a different, shard-count-independent question:
// does src's mutation need to reach peers at all -- keyed off the transaction's own CommandId,
// not a hardcoded name). SORT_RO shares those exact call sites but is CO::READONLY and never
// journals at all, so it must keep the suppressed default -- this is the "cannot be a literal
// false" requirement the predicate exists for. One consumer registration spans both halves;
// LastDel isolates each half's own DEL because the two halves use disjoint keys run strictly in
// sequence. See CrossShardSourceEffectsReplicateLikeSameShard below for the same invariant
// pinned across shards, not just within one.
//
// Falsifying: hardcoding SortSourceEffectsMustReplicate to always return false (or reverting
// either SORT call site's `!SortSourceEffectsMustReplicate(...)` back to the derived=true
// default) makes SORT's DEL come back flagged kEntryFlagDerived -- verified by hand during
// development.
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

namespace {
// drakeydb: P4-3 Task 7 -- like OriginOpcodeCapturingConsumer above, but decodes and keeps the
// full argument list (ParsedEntry::cmd.view(), cmd name first) rather than just origin/opcode
// metadata. That is the only way to prove or disprove that a cross-shard auto-journaled
// command's per-shard payload still carries enough information for a replica to reconstruct the
// command's effect -- the question Task 7's diagnosis turns on for SORT ... STORE.
struct DecodedJournalEntry {
  ShardId shard_id;
  std::vector<std::string> args;
  uint8_t entry_flags;  // journal::kEntryFlagDerived / kEntryFlagExpired, see journal/types.h
};

class DecodingEntryCapturingConsumer : public journal::JournalConsumerInterface {
 public:
  void ConsumeJournalChange(const journal::JournalChangeItem& item) override {
    io::BytesSource source{item.journal_item.data};
    JournalReader reader{&source, 0};
    journal::ParsedEntry parsed;
    CHECK(!reader.ReadEntry(&parsed));

    std::vector<std::string> args;
    for (std::string_view sv : parsed.cmd.view())
      args.emplace_back(sv);

    util::fb2::LockGuard lk(mu_);
    entries.push_back(
        {EngineShard::tlocal()->shard_id(), std::move(args), item.journal_item.entry_flags});
  }
  void ThrottleIfNeeded() override {
  }

  util::fb2::Mutex mu_;
  std::vector<DecodedJournalEntry> entries;  // guarded by mu_
};

// Returns a key named `prefix<i>` (i starting at 0) that hashes to a different shard than
// `avoid_sid` under this process's actual shard count -- the same linear-probe shape
// MvccStoreTest::ApplyOnePeerWriteToEveryShard (this file) uses to target a specific shard.
std::string FindKeyOnDifferentShard(std::string_view prefix, ShardId avoid_sid, size_t num_shards) {
  for (int i = 0;; ++i) {
    std::string candidate = absl::StrCat(prefix, i);
    if (Shard(candidate, num_shards) != avoid_sid)
      return candidate;
    CHECK_LT(i, 10000) << "could not find a '" << prefix << "' key hashing to a different shard";
  }
}
}  // namespace

// drakeydb: P4-3 Task 7. The issue register (D-3) records that `SORT ... STORE` does not
// replicate, reproduced with --active_replica off, and the trap (see the task brief) is that
// SORT was registered CO::JOURNALED with no NO_AUTOJOURNAL (generic_family.cc) -- normally
// meaning the dispatcher auto-journals SORT's verbatim command, which should be enough for a
// replica to reproduce the STORE. Step 1's diagnosis proved that assumption wrong specifically
// when source and destination hash to DIFFERENT shards:
//
// Mechanism (pre-fix), with evidence -- see task-7-report.md for the full account:
//  - CO::STORE_LAST_KEY makes the destination key a genuine second locked/scheduled key
//    (Transaction's key-index parser sets `bonus` to the STORE keyword's argument,
//    transaction.cc:1940-1948), so a cross-shard SORT ... STORE takes the multi-key
//    BuildShardIndex/InitShardData path (transaction.cc:209-264) instead of the single-key fast
//    path (InitByKeys's `NumArgs() == 1` check, transaction.cc:310), leaving unique_shard_cnt_ ==
//    2 -- one shard for `src`, one for `dst`.
//  - DispatchHop (transaction.cc:1020-1058) runs RunCallback on every active/keyed shard for
//    every hop, including the concluding one, so Transaction::LogAutoJournalOnShard
//    (transaction.cc:1700-1736) fired on BOTH shards once COORD_CONCLUDING was set -- regardless
//    of whether that particular shard's callback actually performed the STORE write.
//  - Since unique_shard_cnt_ != 1, LogAutoJournalOnShard's payload was NOT the full verbatim
//    command (transaction.cc:1727-1732): it was built from GetShardArgs(shard_id)
//    (transaction.cc:1456-1468), which slices out only THAT shard's own key argument. The source
//    shard therefore journaled a bare "SORT <src>" and the destination shard journaled a bare
//    "SORT <dst>" -- both missing BY/GET/LIMIT/STORE and the other key entirely. Neither entry,
//    nor both together, could reconstruct the STORE effect: a replica applying them verbatim
//    never created `dst`. Ruled out explicitly at the time: auto-journal DID fire (on both
//    shards, not suppressed), IsOmittableWrite was never involved (SORT never sets
//    is_omittable_operation), and STORE_LAST_KEY's *routing* was already correct (the local
//    `lrange dst` sanity check below always passed) -- the bug was purely in what
//    LogAutoJournalOnShard chose to journal for a >1-shard transaction.
//
// The fix (Step 2): SORT is now CO::NO_AUTOJOURNAL, reviving verbatim auto-journal only when
// GetUniqueShardCnt() == 1 (SortGeneric, generic_family.cc). A cross-shard STORE stays
// NO_AUTOJOURNAL, so nothing else journals the destination shard's write; OpStore instead hand-
// journals a single "RESTORE dst 0 <dump> REPLACE" -- the destination's actual resulting EFFECT,
// not a re-sortable recipe -- using the same arm-before-journal pattern Renamer::DeserializeDest
// (RENAME/COPY, this file) already established for exactly this "cross-shard destination write
// needs its own hand journal entry" shape. This test now pins that behavior; it is the same test
// that pinned the bug during Step 1's diagnosis (see its earlier revision in git history / the
// report for the pre-fix assertions), inverted to pin the fix instead.
//
// Falsifying: reverting Step 2 (dropping SORT's CO::NO_AUTOJOURNAL, or SortGeneric's
// ReviveAutoJournal call, or OpStore's hand-journal block) reproduces the original bug -- this
// test then fails exactly as it did pre-fix: dst_entry becomes null (no destination entry at
// all) or reverts to a bare "SORT <dst>" with no RESTORE/REPLACE. Verified in task-7-report.md.
TEST_F(MultiShardOriginJournalFamilyTest, CrossShardStoreHandJournalsRestoreOfDestinationEffect) {
  const size_t num_shards = shard_set->size();
  ASSERT_GT(num_shards, 1u) << "test requires more than one shard to be meaningful";

  const std::string src = "sort-src";
  const ShardId src_sid = Shard(src, num_shards);
  const std::string dst = FindKeyOnDifferentShard("sort-dst", src_sid, num_shards);
  const ShardId dst_sid = Shard(dst, num_shards);
  ASSERT_NE(src_sid, dst_sid) << "test setup must exercise two distinct shards";

  Run({"rpush", src, "3", "1", "2"});

  DecodingEntryCapturingConsumer consumer;
  std::vector<uint32_t> consumer_ids(num_shards, 0);
  shard_set->RunBriefInParallel([&](EngineShard* shard) {
    journal::StartInThread();
    consumer_ids[shard->shard_id()] = journal::RegisterConsumer(&consumer);
  });

  auto resp = Run({"sort", src, "store", dst});
  EXPECT_EQ(3, resp.GetInt());
  EXPECT_THAT(Run({"lrange", dst, "0", "-1"}).GetVec(), testing::ElementsAre("1", "2", "3"));

  shard_set->RunBriefInParallel(
      [&](EngineShard* shard) { journal::UnregisterConsumer(consumer_ids[shard->shard_id()]); });

  const DecodedJournalEntry* src_entry = nullptr;
  const DecodedJournalEntry* dst_entry = nullptr;
  {
    util::fb2::LockGuard lk(consumer.mu_);
    for (const auto& e : consumer.entries) {
      if (e.shard_id == src_sid)
        src_entry = &e;
      if (e.shard_id == dst_sid)
        dst_entry = &e;
    }
  }
  // The source shard journals nothing: SORT is CO::NO_AUTOJOURNAL and this cross-shard STORE
  // does not revive it (GetUniqueShardCnt() != 1), and there is no lazy member-expiry SREM to
  // compensate for (`src` is a plain TTL-less list here). A regression back to the pre-fix
  // per-shard auto-journal split would show up here as a bare "SORT <src>" entry.
  EXPECT_EQ(nullptr, src_entry) << "the source shard must not journal anything for this STORE";

  // The destination shard hand-journals exactly one RESTORE ... REPLACE entry: the resulting
  // EFFECT (a serialized list blob), not a re-sortable "SORT <dst>" recipe -- so a replica
  // converges on `dst` regardless of its own copy of `src`.
  ASSERT_NE(nullptr, dst_entry) << "the destination shard must hand-journal the STORE effect";
  ASSERT_GE(dst_entry->args.size(), 4u) << "RESTORE key ttl dump REPLACE";
  EXPECT_EQ("RESTORE", dst_entry->args[0]);
  EXPECT_EQ(dst, dst_entry->args[1]);
  EXPECT_EQ("REPLACE", dst_entry->args.back());
}

// drakeydb: P4-3 review wave (CodeRabbit, Major) -- the sorted same-shard STORE path's
// destination effect must survive its own fetch having deleted the source.
//
// OpFetchSortEntries lazily walks a fully-expired set, finds it empty, deletes the key and
// journals the non-derived source DEL -- and still reports success with zero entries, so the
// author runs OpStore(empty) and clears `dst`. The same-shard peer applies that source DEL first
// and then hits SortGeneric's missing-source early return, which never reaches OpStore -- so
// without this fix `dst` keeps a stale value there forever while the author has none. (The
// cross-shard path was already correct: OpStore hand-journals its destination effect in that case
// unconditionally, see CrossShardStoreHandJournalsRestoreOfDestinationEffect above.) The fix
// extends hand-journaling to exactly this same-shard case -- the destination DEL the author
// actually executed -- leaving the ordinary same-shard recipe replay (D-13's accepted exposure)
// untouched.
//
// Pinned under OriginJournalFamilyTest's num_shards=1 fixture deliberately: src and dst must land
// on ONE shard, or the STORE takes the cross-shard path that was already correct.
//
// Falsifying: reverting the `source_deleted_by_fetch` hand-journal extension in OpStore drops the
// destination's DEL from the journal (dst_del comes back null) while the author's own `exists`
// still reports the destination deleted -- verified by hand during development.
TEST_F(OriginJournalFamilyTest, FullExpirySortStoreJournalsDestinationDelete) {
  ASSERT_EQ(1u, shard_set->size()) << "this test pins the same-shard STORE path";

  DecodingEntryCapturingConsumer consumer;
  std::vector<uint32_t> consumer_ids(shard_set->size(), 0);
  shard_set->RunBriefInParallel([&](EngineShard* shard) {
    journal::StartInThread();
    consumer_ids[shard->shard_id()] = journal::RegisterConsumer(&consumer);
  });

  const std::string src = "sort-full-src";
  const std::string dst = "sort-full-dst";
  ASSERT_EQ(Shard(src, shard_set->size()), Shard(dst, shard_set->size()));

  EXPECT_EQ(Run({"sadd", src, "m"}).GetInt(), 1);
  Run({"rpush", dst, "stale"});
  Run({"fieldexpire", src, "1", "m"});
  AdvanceTime(1100);

  // The author's observable behavior is unchanged: reply 0, source gone, destination cleared.
  EXPECT_EQ(Run({"sort", src, "store", dst}).GetInt(), 0);
  EXPECT_EQ(Run({"exists", src}).GetInt(), 0);
  EXPECT_EQ(Run({"exists", dst}).GetInt(), 0);

  shard_set->RunBriefInParallel(
      [&](EngineShard* shard) { journal::UnregisterConsumer(consumer_ids[shard->shard_id()]); });

  const DecodedJournalEntry* src_del = nullptr;
  const DecodedJournalEntry* dst_del = nullptr;
  size_t src_del_idx = 0;
  size_t dst_del_idx = 0;
  size_t sort_idx = 0;
  bool saw_sort = false;
  {
    util::fb2::LockGuard lk(consumer.mu_);
    for (size_t i = 0; i < consumer.entries.size(); ++i) {
      const DecodedJournalEntry& e = consumer.entries[i];
      if (e.args == std::vector<std::string>{"DEL", src}) {
        src_del = &e;
        src_del_idx = i;
      } else if (e.args == std::vector<std::string>{"DEL", dst}) {
        dst_del = &e;
        dst_del_idx = i;
      } else if (!e.args.empty() && e.args[0] == "SORT") {
        saw_sort = true;
        sort_idx = i;
      }
    }
  }

  ASSERT_NE(nullptr, src_del) << "the source's own lazy-expiry DEL must still be journaled";
  EXPECT_FALSE(src_del->entry_flags & journal::kEntryFlagDerived);
  ASSERT_NE(nullptr, dst_del)
      << "the destination's delete must be journaled, or a peer applying the source DEL and then "
         "skipping the SORT (source missing) keeps dst's stale value forever";
  EXPECT_LT(src_del_idx, dst_del_idx)
      << "the source effect must precede the destination effect, as in the partial-expiry case";
  ASSERT_TRUE(saw_sort) << "SORT itself must still auto-journal verbatim on this same-shard path";
  EXPECT_LT(dst_del_idx, sort_idx)
      << "the destination effect must be journaled before the concluding verbatim SORT entry";
}

// drakeydb: P4-3 Task 7 fix round (C1/C2 review).
// CrossShardStoreHandJournalsRestoreOfDestinationEffect above proves the DESTINATION side of a
// cross-shard SORT ... STORE; this test proves the SOURCE side, which the first pass of the fix
// broke silently (see SortSourceEffectsMustReplicate's comment, above, for the full mechanism this
// pins). Pinned in its own multi-shard fixture specifically because the pre-existing same-shard
// coverage for both invariants (SortPartialExpiryJournalsSourceEffectBeforeDestinationEffect,
// SortDerivedDeleteReachesPeersButSortRoStaysSuppressed, both above) runs under
// OriginJournalFamilyTest, which pins num_shards=1 -- so neither test could ever have caught a
// shard-count-dependent regression in these source-side effects. This one deliberately does not
// pin the shard count, the same way CrossShardStoreHandJournalsRestoreOfDestinationEffect above
// does not.
//
// Falsifying (C1): reverting SortSourceEffectsMustReplicate to gate on
// Transaction::IsAutoJournalSuppressed() (as the first Task 7 patch did) makes the partial-expiry
// half fail -- no SREM entry is captured on the source shard at all.
// Falsifying (C2): the same revert makes the full-expiry half fail -- the DEL entry on the
// source shard comes back with entry_flags & journal::kEntryFlagDerived set, which
// PassesPeerEchoFilter (journal/types.cc) would then drop from every peer link.
TEST_F(MultiShardOriginJournalFamilyTest, CrossShardSourceEffectsReplicateLikeSameShard) {
  const size_t num_shards = shard_set->size();
  ASSERT_GT(num_shards, 1u) << "test requires more than one shard to be meaningful";

  // -- Partial expiry: SREM must reach the source shard's journal, cross-shard STORE or not --
  // mirrors SortPartialExpiryJournalsSourceEffectBeforeDestinationEffect's sorted-path case
  // above, with `dst` forced onto a different shard than `src`.
  {
    const std::string src = "sort-partial-src";
    const ShardId src_sid = Shard(src, num_shards);
    const std::string dst = FindKeyOnDifferentShard("sort-partial-dst", src_sid, num_shards);
    ASSERT_NE(src_sid, Shard(dst, num_shards)) << "test setup must exercise two distinct shards";

    EXPECT_EQ(Run({"sadd", src, "1", "2"}).GetInt(), 2);
    Run({"fieldexpire", src, "1", "1"});
    AdvanceTime(1100);

    DecodingEntryCapturingConsumer consumer;
    std::vector<uint32_t> consumer_ids(num_shards, 0);
    shard_set->RunBriefInParallel([&](EngineShard* shard) {
      journal::StartInThread();
      consumer_ids[shard->shard_id()] = journal::RegisterConsumer(&consumer);
    });

    Run({"sort", src, "store", dst});

    EXPECT_EQ(Run({"scard", src}).GetInt(), 1);
    EXPECT_EQ(Run({"sismember", src, "2"}).GetInt(), 1);
    EXPECT_EQ(Run({"llen", dst}).GetInt(), 1);
    EXPECT_EQ(Run({"lindex", dst, "0"}), "2");

    shard_set->RunBriefInParallel(
        [&](EngineShard* shard) { journal::UnregisterConsumer(consumer_ids[shard->shard_id()]); });

    const DecodedJournalEntry* srem_entry = nullptr;
    {
      util::fb2::LockGuard lk(consumer.mu_);
      for (const auto& e : consumer.entries) {
        if (e.shard_id == src_sid && !e.args.empty() && e.args[0] == "SREM")
          srem_entry = &e;
      }
    }
    ASSERT_NE(nullptr, srem_entry)
        << "cross-shard SORT ... STORE must still journal the source's own partial-expiry SREM, "
           "exactly like a same-shard STORE does";
    EXPECT_THAT(srem_entry->args, testing::ElementsAre("SREM", src, "1"));
  }

  // -- Full expiry: the source's own DEL must NOT be flagged derived, cross-shard STORE or not --
  // mirrors SortDerivedDeleteReachesPeersButSortRoStaysSuppressed's SORT half above, with `dst`
  // forced onto a different shard than `src`.
  {
    const std::string src = "sort-full-src";
    const ShardId src_sid = Shard(src, num_shards);
    const std::string dst = FindKeyOnDifferentShard("sort-full-dst", src_sid, num_shards);
    ASSERT_NE(src_sid, Shard(dst, num_shards)) << "test setup must exercise two distinct shards";

    EXPECT_EQ(Run({"sadd", src, "m"}).GetInt(), 1);
    Run({"fieldexpire", src, "1", "m"});
    AdvanceTime(1100);

    DecodingEntryCapturingConsumer consumer;
    std::vector<uint32_t> consumer_ids(num_shards, 0);
    shard_set->RunBriefInParallel([&](EngineShard* shard) {
      journal::StartInThread();
      consumer_ids[shard->shard_id()] = journal::RegisterConsumer(&consumer);
    });

    Run({"sort", src, "by", "nosort", "store", dst});

    EXPECT_EQ(Run({"exists", src}).GetInt(), 0);

    shard_set->RunBriefInParallel(
        [&](EngineShard* shard) { journal::UnregisterConsumer(consumer_ids[shard->shard_id()]); });

    const DecodedJournalEntry* del_entry = nullptr;
    {
      util::fb2::LockGuard lk(consumer.mu_);
      for (const auto& e : consumer.entries) {
        if (e.shard_id == src_sid && !e.args.empty() && e.args[0] == "DEL")
          del_entry = &e;
      }
    }
    ASSERT_NE(nullptr, del_entry)
        << "cross-shard SORT ... STORE must still journal the source's own full-expiry DEL, "
           "exactly like a same-shard STORE does";
    EXPECT_FALSE(del_entry->entry_flags & journal::kEntryFlagDerived)
        << "SORT's own source-side DEL must reach peers regardless of STORE's shard count, or "
           "PassesPeerEchoFilter silently drops it from every peer link";
  }
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
    DbContext db_cntx{&namespaces->GetDefaultNamespace(), 0, TEST_current_time_ms};
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

// drakeydb: P4-3 Task 11 -- the DeleteReapedContainer equivalent of MvccStoreTest's
// LazyExpiryEarnsATombstoneWithSelfOrigin above (multi_master_test.cc): DeleteReapedContainer now
// calls Del()/PerformDeletionAtomic BEFORE RecordDerivedDelete too, so the derived DEL's own
// Commit() lands the tombstone's real stamp instead of leaving PerformDeletionAtomic's
// synchronous placeholder for EndOfWriteEpoch to roll back. Reads the stamp from inside the same
// shard_set->RunBriefInParallel callback that drives DeleteExpiredStep, on this fixture's single
// shard/thread, so there is no yield between the reap and the read -- mirroring StampOf's own
// same-shard-thread precondition in MvccStoreTest.
//
// Falsifying: reverting the DeleteReapedContainer reorder (db_slice.cc) reproduces the orphaned-
// arm symptom -- `tomb` comes back nullopt (I3's rollback erases the uncommitted placeholder)
// instead of a real tombstone. Verbatim run in task-11-report.md.
TEST_F(ReaperJournalFamilyTest, MemberExpiryReaperDeleteEarnsATombstone) {
  EXPECT_EQ(Run({"sadd", "rs-tomb", "m"}).GetInt(), 1);
  Run({"fieldexpire", "rs-tomb", "1", "m"});
  AdvanceTime(1100);

  std::optional<MvccStamp> tomb;
  uint64_t self_origin_hash = 0;
  shard_set->RunBriefInParallel([&](EngineShard* shard) {
    DbSlice& db_slice = namespaces->GetDefaultNamespace().GetDbSlice(shard->shard_id());
    DbContext db_cntx{&namespaces->GetDefaultNamespace(), 0, TEST_current_time_ms};
    db_slice.DeleteExpiredStep(db_cntx, 100);
    tomb = db_slice.GetMvcc(0, "rs-tomb");
    self_origin_hash = MvccStamper::tlocal()->OriginHash(0);
  });

  // Guard against a vacuous pass: the container must have actually been reaped.
  EXPECT_EQ(Run({"exists", "rs-tomb"}).GetInt(), 0);

  ASSERT_TRUE(tomb.has_value())
      << "the reaper's derived DEL must earn a tombstone, not leave the slot absent";
  EXPECT_TRUE(tomb->IsTombstone());
  EXPECT_NE(tomb->Mvcc(), 0u)
      << "must carry the derived DEL's own real, minted stamp -- not the zero-authority "
         "{kTombstoneBit, 0} placeholder PerformDeletionAtomic writes synchronously";
  EXPECT_EQ(tomb->origin_hash, self_origin_hash)
      << "a reaper-derived delete is a local decision -- must be self-originated, never "
         "attributed to a peer";
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
    DbContext db_cntx{&namespaces->GetDefaultNamespace(), 0, TEST_current_time_ms};
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
    DbContext db_cntx{&namespaces->GetDefaultNamespace(), 0, TEST_current_time_ms};
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
      DbContext db_cntx{&namespaces->GetDefaultNamespace(), 0, TEST_current_time_ms};
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
    DbContext db_cntx{&namespaces->GetDefaultNamespace(), 0, TEST_current_time_ms};
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
      DbContext db_cntx{&namespaces->GetDefaultNamespace(), 0, TEST_current_time_ms};
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
    DbContext db_cntx{&namespaces->GetDefaultNamespace(), 0, TEST_current_time_ms};
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

  // drakeydb: fix round 3 (R6) -- pauses the background heartbeat's own member-expiry reaper for
  // this whole test, restored via Cleanup, mirroring
  // MemberExpiryReaperDoesNotBlockOnConcurrentBgsave's established pattern above in this file.
  // EngineShard::Heartbeat computes
  // `member_reap_active = IsActiveReplica()` and folds it into ttl_key_count -- forced to 0 when
  // false -- so with no whole-key-TTL'd key anywhere in this (single-shard, per
  // ReaperJournalFamilyTest's ctor) test, `if (ttl_key_count > 0)` is false and the heartbeat
  // never calls DeleteExpiredStep at all while this is off. DeleteExpiredStep (db_slice.cc) reads
  // `reap_member_expiry = IsActiveReplica()` itself, live, at the top of every call and gates the
  // member-walk block on it directly (`if (reap_member_expiry && walk_budget != 0 && ...)`,
  // db_slice.cc) -- not cached the way mvcc_enabled_ is -- so re-enabling it only around the
  // manual call below (inside its own dispatched callback -- see that callback's own comment for
  // why it is not bracketed here instead) is what lets that call, and only that call, actually
  // reap.
  //
  // A first attempt at this fix only scoped the elevated FLAGS_reaper_member_walk_budget below to
  // this measurement window (still worth keeping, see its own comment) without touching
  // active_replica, on the theory that the heartbeat could only fully steal the measurement while
  // the budget was elevated. Verified insufficient by repeated runs, not assumed: 3 of 30 runs
  // still failed, now with a PARTIAL steal --
  // `reported_deleted_bytes: Which is: 218528` against `before - after: Which is: 260464` --
  // because RunBriefInParallel's own dispatch below is itself a yield point, and the heartbeat
  // could still interleave inside the narrowed window (task-10-report.md fix round 3). Suppressing
  // the heartbeat's walk at its source removes the race instead of narrowing it.
  const bool saved_active_replica = absl::GetFlag(FLAGS_active_replica);
  absl::SetFlag(&FLAGS_active_replica, false);
  absl::Cleanup restore_active_replica = [saved_active_replica] {
    absl::SetFlag(&FLAGS_active_replica, saved_active_replica);
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

  // Elevated so the manual call below completes the whole reap in its own single pass instead of
  // being bounded by the ambient default (300, db_slice.cc's ABSL_FLAG) -- unrelated to the
  // active_replica toggle above, which is what actually prevents the heartbeat from racing it.
  const uint32_t saved_walk_budget = absl::GetFlag(FLAGS_reaper_member_walk_budget);
  absl::SetFlag(&FLAGS_reaper_member_walk_budget, 100000);
  absl::Cleanup restore_walk_budget = [saved_walk_budget] {
    absl::SetFlag(&FLAGS_reaper_member_walk_budget, saved_walk_budget);
  };

  size_t reported_deleted_bytes = 0;
  shard_set->RunBriefInParallel([&](EngineShard* shard) {
    // drakeydb: fix round 3 (R6) -- the flag toggle lives INSIDE this dispatched callback, not
    // around the RunBriefInParallel call (a first attempt at that placement, verified
    // insufficient by repeated runs: still 1 of 30 failures, task-10-report.md fix round 3).
    // RunBriefInParallel's own dispatch to this shard's fiber queue is itself a yield/scheduling
    // point from the calling (main test) fiber's perspective -- toggling the flag before that
    // dispatch leaves a window, between the toggle and this callback actually starting to run on
    // the shard's own thread, where the background heartbeat (also on this thread) could already
    // be scheduled and win the race with active_replica now true. Toggling here, immediately
    // before and after the one call that needs it, with no yield point anywhere in between (this
    // callback's only statement that could yield is the DeleteExpiredStep call itself, and its
    // container walk was already established to be non-preempting -- see the P4-0 Task 2b
    // comments a few dozen lines up in db_slice.cc), removes that specific window.
    absl::SetFlag(&FLAGS_active_replica, true);
    DbSlice& db_slice = namespaces->GetDefaultNamespace().GetDbSlice(shard->shard_id());
    DbContext db_cntx{&namespaces->GetDefaultNamespace(), 0, TEST_current_time_ms};
    // count=100000 bounds the outer prime-table traversal, not the reaper's own per-container
    // walk -- see this test's comment above for why that distinction matters here.
    reported_deleted_bytes = db_slice.DeleteExpiredStep(db_cntx, 100000).deleted_bytes;
    absl::SetFlag(&FLAGS_active_replica, false);
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
    DbContext db_cntx{&namespaces->GetDefaultNamespace(), 0, TEST_current_time_ms};
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
    DbContext db_cntx{&namespaces->GetDefaultNamespace(), 0, TEST_current_time_ms};
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
    DbContext db_cntx{&namespaces->GetDefaultNamespace(), 0, TEST_current_time_ms};
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
