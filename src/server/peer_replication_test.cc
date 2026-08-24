// Copyright 2026, drakeydb authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "server/peer_replication.h"

#include <absl/flags/declare.h>
#include <absl/flags/flag.h>
#include <absl/flags/reflection.h>
#include <absl/functional/function_ref.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>

#include <array>
#include <atomic>
#include <chrono>
#include <memory>

#include "base/gtest.h"
#include "facade/facade_test.h"
#include "server/engine_shard_set.h"
#include "server/journal/executor.h"
#include "server/journal/serializer.h"
#include "server/journal/streamer.h"
#include "server/journal/test_capturing_socket.h"
#include "server/journal/tx_executor.h"
#include "server/journal/types.h"
#include "server/multi_master.h"
#include "server/node_identity.h"
#include "server/rdb_load.h"
#include "server/rdb_load_context.h"
#include "server/rdb_save.h"
#include "server/replica.h"
#include "server/test_utils.h"
#include "util/fibers/fibers.h"
#include "util/fibers/pool.h"

ABSL_DECLARE_FLAG(bool, force_epoll);
ABSL_DECLARE_FLAG(std::string, dir);
ABSL_DECLARE_FLAG(bool, active_replica);
ABSL_DECLARE_FLAG(bool, multi_master);

namespace dfly {

TEST(PeerIdentityClaimsTest, RejectsDuplicateAndReleasesOrMovesClaims) {
  PeerIdentityClaims claims;
  EXPECT_TRUE(claims.TryClaim(1, "peer-a"));
  EXPECT_TRUE(claims.TryClaim(1, "peer-a"));  // idempotent reconnect
  EXPECT_FALSE(claims.TryClaim(2, "peer-a"));

  claims.Release(1);
  EXPECT_TRUE(claims.TryClaim(2, "peer-a"));
  EXPECT_TRUE(claims.TryClaim(2, "peer-b"));
  EXPECT_TRUE(claims.TryClaim(1, "peer-a"));  // moving owner 2 released its old UUID

  // A failed identity switch releases the caller's stale claim.
  EXPECT_FALSE(claims.TryClaim(1, "peer-b"));
  EXPECT_TRUE(claims.TryClaim(3, "peer-a"));
  claims.Release(2);
  EXPECT_TRUE(claims.TryClaim(1, "peer-b"));
}

// Launch::post-constructed fibers only get queued (AddReady) on the constructing thread's
// scheduler; they don't start running until that thread yields (e.g. at Join()). So fibers built
// directly on the bare gtest thread never actually interleave -- a real test of SyncGate's FIFO
// ordering and cancellation needs fibers on distinct proactor threads, so a holder fiber on one
// thread can genuinely block waiter fibers running on another.
class SyncGateTest : public ::testing::Test {
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

namespace {

constexpr int kPollIters = 1000;
constexpr auto kPollInterval = std::chrono::milliseconds(5);  // kPollIters * kPollInterval ~= 5s.

// Polls `pred` until true (or 5s). Returns the final value. Must run inside a fiber so each wait
// yields to the proactor scheduler.
bool WaitFor(absl::FunctionRef<bool()> pred) {
  for (int n = 0; n < kPollIters; ++n) {
    if (pred())
      return true;
    util::ThisFiber::SleepFor(kPollInterval);
  }
  return false;
}

// Runs the bounded polling loop on proactor `i`. This lets the bare gtest thread safely observe
// gate state without taking a fiber mutex itself or blocking a proactor thread.
bool AwaitUntil(util::ProactorPool* pp, unsigned i, absl::FunctionRef<bool()> pred) {
  return pp->at(i)->Await([&] { return WaitFor(pred); });
}

}  // namespace

TEST_F(SyncGateTest, SerializesAndIsFifo) {
  SyncGate gate;
  std::atomic_int order_idx{0};
  std::array<int, 3> order{-1, -1, -1};
  auto never = [] { return false; };
  util::fb2::Fiber a = pp_->at(0)->LaunchFiber(util::fb2::Launch::post, [&] {
    auto lease = gate.Acquire(never);
    ASSERT_TRUE(lease);
    // Hold until two waiters are queued behind us; bounded so a mutual-exclusion regression
    // fails this assertion instead of spinning forever.
    ASSERT_TRUE(WaitFor([&] { return gate.NumWaiting() >= 2; }));
    EXPECT_TRUE(gate.IsHeld());
    order[order_idx++] = 0;
  });
  ASSERT_TRUE(AwaitUntil(pp_.get(), 1, [&] { return gate.IsHeld(); }));
  util::fb2::Fiber b = pp_->at(1)->LaunchFiber(util::fb2::Launch::post, [&] {
    auto lease = gate.Acquire(never);
    ASSERT_TRUE(lease);
    order[order_idx++] = 1;
  });
  ASSERT_TRUE(AwaitUntil(pp_.get(), 1, [&] { return gate.NumWaiting() >= 1; }));
  util::fb2::Fiber c = pp_->at(0)->LaunchFiber(util::fb2::Launch::post, [&] {
    auto lease = gate.Acquire(never);
    ASSERT_TRUE(lease);
    order[order_idx++] = 2;
  });
  a.Join();
  b.Join();
  c.Join();
  EXPECT_EQ((std::array<int, 3>{0, 1, 2}), order);
  EXPECT_FALSE(pp_->at(0)->Await([&] { return gate.IsHeld(); }));
  EXPECT_EQ(0u, pp_->at(0)->Await([&] { return gate.NumWaiting(); }));
}

TEST_F(SyncGateTest, CancelledWaiterGetsEmptyLease) {
  SyncGate gate;
  std::atomic_bool cancel{false}, release{false};
  util::fb2::Fiber holder = pp_->at(0)->LaunchFiber(util::fb2::Launch::post, [&] {
    auto lease = gate.Acquire([] { return false; });
    ASSERT_TRUE(WaitFor([&] { return release.load(); }));
  });
  ASSERT_TRUE(AwaitUntil(pp_.get(), 1, [&] { return gate.IsHeld(); }));
  std::atomic_bool got{true}, waiter_done{false};
  util::fb2::Fiber waiter = pp_->at(1)->LaunchFiber(util::fb2::Launch::post, [&] {
    auto lease = gate.Acquire([&] { return cancel.load(); });
    got = static_cast<bool>(lease);
    waiter_done = true;
  });
  ASSERT_TRUE(AwaitUntil(pp_.get(), 0, [&] { return gate.NumWaiting() >= 1; }));
  cancel = true;
  // waiter.Join() blocks unconditionally, and `holder` deliberately never releases until after
  // this check (that's what proves cancellation -- not the gate freeing up -- is what unblocks
  // the waiter). If SyncGate's cancellation check has regressed, Acquire() may never return, so
  // wait for `waiter_done` with a bound first: on timeout this ASSERT_TRUE fails (with a
  // diagnostic) and returns before the unconditional Join() below can hang. `waiter` is
  // deliberately left un-joined on that path rather than Detach()-ed: once `holder` (below) also
  // gives up on its own bound and releases the gate, a detached-but-still-blocked-in-Acquire()
  // `waiter` could eventually resume and touch this function's now-destroyed locals; leaving it
  // joinable instead means its destructor's own CHECK aborts the process deterministically,
  // before any other fiber gets a chance to run.
  ASSERT_TRUE(AwaitUntil(pp_.get(), 0, [&] { return waiter_done.load(); }))
      << "waiter fiber did not return within the time bound -- "
         "SyncGate::Acquire cancellation appears broken";
  waiter.Join();
  EXPECT_FALSE(got.load());
  EXPECT_EQ(0u, pp_->at(0)->Await([&] { return gate.NumWaiting(); }));
  release = true;
  holder.Join();  // bounded: holder's own wait above gives up after ~5s regardless of `release`.
  EXPECT_FALSE(pp_->at(0)->Await([&] { return gate.IsHeld(); }));
}

TEST_F(SyncGateTest, AlreadyCancelledWaiterDoesNotAcquireFreeGate) {
  SyncGate gate;
  pp_->at(0)->Await([&] {
    auto lease = gate.Acquire([] { return true; });
    EXPECT_FALSE(lease);
    EXPECT_FALSE(gate.IsHeld());
    EXPECT_EQ(0u, gate.NumWaiting());
  });
}

TEST_F(SyncGateTest, ExternalLoadingDefersGrant) {
  std::atomic_bool loading{true};
  SyncGate gate([&] { return loading.load(); });
  std::atomic_bool acquired{false}, done{false};
  util::fb2::Fiber f = pp_->at(0)->LaunchFiber(util::fb2::Launch::post, [&] {
    auto lease = gate.Acquire([] { return false; });
    acquired = static_cast<bool>(lease);
    done = true;
  });
  pp_->at(1)->Await([] { util::ThisFiber::SleepFor(std::chrono::milliseconds(250)); });
  EXPECT_FALSE(acquired.load());
  loading = false;
  // f.Join() blocks unconditionally; if SyncGate stopped re-checking external_loading_ after the
  // first poll, Acquire() may never return even though `loading` is now false. Wait for `done`
  // with a bound first: on timeout this ASSERT_TRUE fails (with a diagnostic) and returns before
  // the unconditional Join() below can hang. `f` is deliberately left un-joined on that path
  // (not Detach()-ed) so its destructor's own CHECK aborts the process deterministically instead
  // of risking a detached, still-blocked-in-Acquire() fiber later touching this function's
  // now-destroyed locals -- see the identical reasoning in CancelledWaiterGetsEmptyLease above.
  ASSERT_TRUE(AwaitUntil(pp_.get(), 1, [&] { return done.load(); }))
      << "fiber did not return within the time bound after external_loading cleared -- "
         "SyncGate may not be re-checking external_loading_";
  f.Join();
  EXPECT_TRUE(acquired.load());
}

// Fixture for PeerReplicationManager, modelled on MultiMasterFamilyTest (multi_master_test.cc):
// its own private --dir, active/multi-master mode on by default, and a FlagSaver to restore all
// three flags on teardown (saver_ is the fixture's only data member, so -- same reasoning as
// MultiMasterFamilyTest -- it is constructed, capturing the pre-test values, before the
// constructor body below runs, and destroyed, restoring them, after TearDown()).
//
// Every PeerReplicationManager call in the tests below -- including read-only ones like Size()
// and Summaries() -- runs inside pp_->at(0)->Await(...): the manager's mutex is a fiber mutex
// (util::fb2::Mutex) and the bare gtest thread is not a fiber, so it must never take it directly.
// Each test ends with an explicit mgr.Shutdown() inside that Await so that when `mgr` (a local
// variable) is destroyed on the bare gtest thread at the end of the test body, its destructor's
// own Shutdown() call finds closed_ already true and peers_ already empty: it still takes mu_,
// but uncontended and with zero Replica calls, which is safe even off a fiber.
class PeerManagerFamilyTest : public BaseFamilyTest {
 protected:
  PeerManagerFamilyTest() {
    absl::SetFlag(&FLAGS_dir, base::GetTestTempPath("peer_mgr"));
    absl::SetFlag(&FLAGS_active_replica, true);
    absl::SetFlag(&FLAGS_multi_master, true);
  }

  absl::FlagSaver saver_;
};

using StartMode = PeerReplicationManager::StartMode;
constexpr char kReplid[] = "0123456789abcdef0123456789abcdef01234567";
constexpr char kUnresolvableHost[] = "invalid host";

TEST_F(PeerManagerFamilyTest, BlockingAddToUnreachablePortFailsAndLeavesNoPeer) {
  PeerRegistry reg;
  reg.Init(GenerateNodeUuid());
  PeerReplicationManager mgr(service_.get(), &reg);
  pp_->at(0)->Await([&] {
    bool already = false;
    GenericError ec =
        mgr.Add({kUnresolvableHost, 1}, kReplid, StartMode::kBlockingHandshake, &already);
    EXPECT_TRUE(ec) << "the invalid host must fail DNS resolution";
    EXPECT_FALSE(already);
    EXPECT_EQ(0u, mgr.Size());
    EXPECT_TRUE(mgr.Summaries().empty());
    mgr.Shutdown();
  });
}

TEST_F(PeerManagerFamilyTest, BackgroundAddRemoveNoOneAndDuplicate) {
  PeerRegistry reg;
  reg.Init(GenerateNodeUuid());
  PeerReplicationManager mgr(service_.get(), &reg);
  pp_->at(0)->Await([&] {
    bool already = false;
    EXPECT_FALSE(mgr.Add({kUnresolvableHost, 1}, kReplid, StartMode::kBackground, &already));
    EXPECT_FALSE(already);
    EXPECT_FALSE(mgr.Add({kUnresolvableHost, 1}, kReplid, StartMode::kBackground, &already));
    EXPECT_TRUE(already);  // duplicate endpoint is a no-op
    EXPECT_FALSE(mgr.Add({kUnresolvableHost, 2}, kReplid, StartMode::kBackground, &already));
    EXPECT_FALSE(already);
    EXPECT_EQ(2u, mgr.Size());
    auto sums = mgr.Summaries();
    ASSERT_EQ(2u, sums.size());
    EXPECT_EQ(kUnresolvableHost, sums[0].host);
    EXPECT_EQ(1, sums[0].port);
    EXPECT_EQ(2, sums[1].port);
    EXPECT_FALSE(sums[0].master_link_established);
    EXPECT_TRUE(mgr.Remove({kUnresolvableHost, 1}));
    EXPECT_FALSE(mgr.Remove({kUnresolvableHost, 1}));
    EXPECT_EQ(1u, mgr.Size());
    mgr.RemoveAll();
    EXPECT_EQ(0u, mgr.Size());
    mgr.Shutdown();
  });
}

TEST_F(PeerManagerFamilyTest, SinglePeerReplaceWhenMultiMasterOff) {
  absl::SetFlag(&FLAGS_multi_master, false);
  PeerRegistry reg;
  reg.Init(GenerateNodeUuid());
  PeerReplicationManager mgr(service_.get(), &reg);
  pp_->at(0)->Await([&] {
    bool already = false;
    EXPECT_FALSE(mgr.Add({kUnresolvableHost, 1}, kReplid, StartMode::kBackground, &already));
    EXPECT_FALSE(mgr.Add({kUnresolvableHost, 2}, kReplid, StartMode::kBackground, &already));
    ASSERT_EQ(1u, mgr.Size());
    EXPECT_EQ(2, mgr.Endpoints()[0].port);
    mgr.Shutdown();
  });
}

TEST_F(PeerManagerFamilyTest, ShutdownRefusesFurtherAdds) {
  PeerRegistry reg;
  reg.Init(GenerateNodeUuid());
  PeerReplicationManager mgr(service_.get(), &reg);
  pp_->at(0)->Await([&] {
    bool already = false;
    EXPECT_FALSE(mgr.Add({kUnresolvableHost, 1}, kReplid, StartMode::kBackground, &already));
    mgr.Shutdown();
    EXPECT_TRUE(mgr.Add({kUnresolvableHost, 2}, kReplid, StartMode::kBackground, &already));
    EXPECT_EQ(0u, mgr.Size());
  });
}

TEST_F(PeerManagerFamilyTest, ExclusivePeerLoadingRejectsConcurrentLoader) {
  pp_->at(0)->Await([&] {
    ASSERT_TRUE(service_->RequestExclusiveLoadingState());
    EXPECT_FALSE(service_->RequestLoadingState());
    service_->RemoveLoadingState();

    ASSERT_TRUE(service_->RequestLoadingState());
    ASSERT_TRUE(service_->RequestLoadingState());
    service_->RemoveLoadingState();
    EXPECT_FALSE(service_->RequestExclusiveLoadingState());
    service_->RemoveLoadingState();
  });
}

TEST_F(PeerManagerFamilyTest, ExclusiveLoadingIsUnavailableOutsideActiveMode) {
  absl::SetFlag(&FLAGS_active_replica, false);
  pp_->at(0)->Await([&] {
    EXPECT_FALSE(service_->RequestExclusiveLoadingState());
    ASSERT_TRUE(service_->RequestLoadingState());
    ASSERT_TRUE(service_->RequestLoadingState());
    service_->RemoveLoadingState();
    service_->RemoveLoadingState();
  });
}

// drakeydb: Phase 3 T6 -- verifies DflyShardReplica threads the origin idx passed at
// construction into its JournalExecutor via SetApplyOrigin(). That single value is what both
// (a) PrepareTransaction (main_service.cc) reads, via the executor's ConnectionContext, to tag
// every command this flow applies, and (b) the peer's PING re-record
// (Replica::StableSyncDflyReadFb, replica.cc) reads via
// executor_->connection_context()->repl_origin_idx for its own origin stamp -- so this pins the
// one shared source of truth both consumers depend on.
//
// No socket is exercised: DflyShardReplica's constructor performs no I/O (ConnectAndAuth happens
// later, in StartSyncFlow/FullSyncDflyFb), so this is a pure, in-process construction test.
//
// Falsifying (verified by hand): removing the executor_->SetApplyOrigin(origin_idx) call from
// DflyShardReplica's constructor (or dropping/ignoring the origin_idx parameter) leaves
// repl_origin_idx at its kSelfIdx/0 default, and the kPeerIdx EXPECT_EQ below fails.
namespace {

// drakeydb: Phase 3 T7b -- builds a single-entry journal blob (mirroring rdb_test.cc's
// MakeJournalDel, which established this exact JournalWriter -> RdbSerializer::WriteJournalEntry
// technique) wrapping a SET command, for feeding into RdbLoaderBase::HandleJournalBlob via
// RdbLoader::Load() directly, no socket involved.
std::string MakeJournalSet(std::string_view key, std::string_view val) {
  io::StringSink sink;
  JournalWriter writer(&sink);
  std::array<std::string_view, 2> kv{key, val};
  writer.Write(journal::Entry{1, journal::Op::COMMAND, 0, std::nullopt,
                              journal::Entry::Payload("SET", ArgSlice{kv.data(), kv.size()})});

  RdbSerializer serializer(CompressionMode::NONE);
  CHECK(!serializer.WriteJournalEntry(std::move(sink).str()));
  return serializer.Flush(RdbSerializer::FlushState::kFlushEndEntry);
}

// drakeydb: Phase 3 T7b -- mirrors rdb_test.cc's WrapInRdb (file-local there, so duplicated here
// rather than shared across translation units for one 8-line helper): wraps a raw body in the
// magic/EOF/checksum framing RdbLoader::Load() requires. The all-zero checksum is the same
// convention RdbSerializer::SendEofAndChecksum itself always writes (rdb_save.cc hard-codes
// chksum = 0), which RdbLoader::VerifyChecksum() is documented to skip.
std::string WrapInRdbForTest(std::string_view body) {
  std::string out = absl::StrFormat("REDIS%04d", RDB_SER_VERSION);
  out.append(body);
  out.push_back(static_cast<char>(RDB_OPCODE_EOF));
  constexpr uint8_t checksum[8] = {};
  out.append(reinterpret_cast<const char*>(checksum), sizeof(checksum));
  return out;
}

// drakeydb: Phase 3 T7b -- captures the origin_idx of the last Op::COMMAND entry this node
// itself journals while registered. Does not need friend access (JournalConsumerInterface is a
// public interface), so it lives outside the friended fixture class below, unlike the methods
// that touch DflyShardReplica's private members directly.
struct CapturingConsumer : public journal::JournalConsumerInterface {
  std::optional<uint32_t> last_command_origin_idx;

  void ConsumeJournalChange(const journal::JournalChangeItem& item) override {
    if (item.journal_item.opcode == journal::Op::COMMAND)
      last_command_origin_idx = item.journal_item.origin_idx;
  }
  void ThrottleIfNeeded() override {
  }
};

}  // namespace

class DflyShardReplicaOriginTest : public BaseFamilyTest {
 protected:
  // Constructs a DflyShardReplica with `origin_idx` (no socket -- the constructor performs no
  // I/O) and returns the origin idx observed on its JournalExecutor's ConnectionContext. Defined
  // as a genuine member of this exact friended class, not inlined into a TEST_F body: gtest
  // generates TEST_F's body as a method of a *derived* class
  // (DflyShardReplicaOriginTest_Name_Test), and friendship is not inherited, so code accessing
  // DflyShardReplica's private/protected members must run as a member of DflyShardReplicaOriginTest
  // itself (same reasoning as MemBufControllerTest in rdb_test.cc).
  uint32_t ObservedOriginIdx(uint32_t origin_idx) {
    DflyShardReplica::ServerContext ctx{"127.0.0.1", 1, {}};
    MasterContext master_context;
    master_context.num_flows = 1;
    auto multi_shard_exe = std::make_shared<MultiShardExecution>();
    RdbLoadContext load_context;
    DflyShardReplica flow(ctx, master_context, /*flow_id=*/0, service_.get(), multi_shard_exe,
                          &load_context, origin_idx, /*peer_mode=*/false);
    return flow.executor_->connection_context()->repl_origin_idx;
  }

  // drakeydb: Phase 3 T7b -- constructs a DflyShardReplica with `origin_idx`, replays a
  // hand-crafted "SET key replayed-value" through its rdb_loader_ (the full-sync
  // concurrent-journal-blob apply path, RdbLoaderBase::HandleJournalBlob -- reaching rdb_loader_,
  // a private member, is why this runs as a member of this friended class, same reasoning as
  // ObservedOriginIdx above), then returns the origin_idx THIS node stamped on the re-journaled
  // write. `key` must hash to shard 0 (the caller is responsible for choosing one, e.g. via
  // Shard(key, shard_set->size()) -- see the TEST_F below): the capturing listener below is
  // registered on shard 0 only, matching the pp_->at(0) fiber this always runs on (same
  // convention as ObservedOriginIdx, which never leaves shard 0 either since it does no actual
  // dispatch). Runs entirely off-socket: Load() reads a std::string source, not the network.
  uint32_t ObservedReplayedOriginIdx(uint32_t origin_idx, std::string_view key) {
    // drakeydb: Phase 3 T7b -- the SET's normal auto-journal path (PrepareTransaction) is a
    // silent no-op on a shard where journaling was never enabled -- which a fresh BaseFamilyTest
    // boot never does on its own (production enables it the first time a replica connects).
    // Idempotent (an early return once already initialized), so safe on every invocation.
    journal::StartInThread();

    DflyShardReplica::ServerContext ctx{"127.0.0.1", 1, {}};
    MasterContext master_context;
    master_context.num_flows = 1;
    auto multi_shard_exe = std::make_shared<MultiShardExecution>();
    RdbLoadContext load_context;
    DflyShardReplica flow(ctx, master_context, /*flow_id=*/0, service_.get(), multi_shard_exe,
                          &load_context, origin_idx, /*peer_mode=*/true);

    CapturingConsumer capture;
    uint32_t cb_id = journal::RegisterConsumer(&capture);

    std::string rdb = WrapInRdbForTest(MakeJournalSet(key, "replayed-value"));
    io::BytesSource src{io::Buffer(rdb)};
    std::error_code ec = flow.rdb_loader_->Load(&src);
    CHECK(!ec) << ec.message();

    journal::UnregisterConsumer(cb_id);
    CHECK(capture.last_command_origin_idx.has_value())
        << "the replayed SET was never re-journaled on this node";
    return *capture.last_command_origin_idx;
  }
};

TEST_F(DflyShardReplicaOriginTest, ConstructorThreadsOriginIntoExecutor) {
  constexpr uint32_t kPeerIdx = 7;  // some peer's PeerRegistry index; != PeerRegistry::kSelfIdx.
  pp_->at(0)->Await([&] {
    EXPECT_EQ(kPeerIdx, ObservedOriginIdx(kPeerIdx));

    // A non-peer flow (PeerRegistry::kSelfIdx == 0) must stay byte-identical to upstream: 0 is
    // already ConnectionContext::repl_origin_idx's default, so SetApplyOrigin(0) is a true no-op.
    EXPECT_EQ(PeerRegistry::kSelfIdx, ObservedOriginIdx(PeerRegistry::kSelfIdx));
  });
}

// drakeydb: Phase 3 T7b -- a peer's FULL SYNC has a second apply path besides the stable-sync
// executor_ ConstructorThreadsOriginIntoExecutor above covers: the concurrent journal blob
// embedded in the full sync itself, replayed via rdb_loader_ (RdbLoaderBase::HandleJournalBlob).
// Proves that path also stamps -- and re-journals -- with THIS flow's origin, not kSelfIdx by
// default, and that the underlying write genuinely applies (not merely gets tagged).
//
// Falsifying (verified by hand -- see task-7b-report.md): removing the
// rdb_loader_->SetApplyOrigin(origin_idx) call added to DflyShardReplica's constructor
// (replica.cc) makes the kPeerIdx EXPECT_EQ below fail (observes PeerRegistry::kSelfIdx instead
// of kPeerIdx); the kSelfIdx EXPECT_EQ is unaffected, since SetApplyOrigin(0) is a no-op either
// way -- exactly mirroring ConstructorThreadsOriginIntoExecutor's own falsification shape for
// executor_ above.
TEST_F(DflyShardReplicaOriginTest, FullSyncJournalBlobAppliesAndReJournalsWithFlowsOrigin) {
  // Pick key names that hash to shard 0: ObservedReplayedOriginIdx registers its capturing
  // journal listener on shard 0 only (matching the pp_->at(0) fiber below), so a key hashing to
  // any other shard would silently miss the entry instead of failing loudly.
  auto shard0_key = [&](std::string_view prefix) {
    std::string key;
    for (unsigned i = 0; i < 1000; ++i) {
      key = absl::StrCat(prefix, i);
      if (Shard(key, shard_set->size()) == 0)
        return key;
    }
    ADD_FAILURE() << "could not find a shard-0 key for prefix " << prefix;
    return key;
  };
  const std::string peer_key = shard0_key("t7b-peer-key-");
  const std::string self_key = shard0_key("t7b-self-key-");

  constexpr uint32_t kPeerIdx = 11;  // some peer's PeerRegistry index; != PeerRegistry::kSelfIdx.
  pp_->at(0)->Await([&] {
    EXPECT_EQ(kPeerIdx, ObservedReplayedOriginIdx(kPeerIdx, peer_key));
    // A non-peer flow keeps re-journaling as self-origin -- byte-identical to upstream.
    EXPECT_EQ(PeerRegistry::kSelfIdx, ObservedReplayedOriginIdx(PeerRegistry::kSelfIdx, self_key));
  });

  // The writes genuinely applied -- not merely got tagged with the right origin.
  EXPECT_EQ(Run({"GET", peer_key}), "replayed-value");
  EXPECT_EQ(Run({"GET", self_key}), "replayed-value");
}

// drakeydb: Phase 3 T6b -- verifies DflyShardReplica threads `peer_mode` from construction into
// AdoptAuthoritativeLsn's gate, and that AdoptAuthoritativeLsn itself sets journal_rec_executed_
// (JournalExecutedCount(), what a reconnecting flow reports as its partial-sync resume LSN -- see
// Replica::InitiateDflySync's partial_sync_lsn) to master_lsn + 1 -- unconditionally overwriting
// whatever count-based value the seed held, proving this is a true adoption rather than a
// relative nudge that could compound drift.
//
// No socket is exercised, for the same reason as DflyShardReplicaOriginTest above: the
// constructor performs no I/O, and AdoptAuthoritativeLsn touches only journal_rec_executed_, so
// this is a pure, in-process test of the exact method StableSyncDflyReadFb's Op::LSN branch
// calls -- not a reimplementation of its logic.
//
// Falsifying (verified by hand -- see task-6b-report.md): gating AdoptAuthoritativeLsn's body
// out entirely (as if peer mode were never threaded through) makes the first EXPECT_EQ below
// fail (43u vs the untouched seed, 5u). Dropping the `if (!peer_mode_) return;` guard makes the
// second EXPECT_EQ fail (43u instead of the untouched seed, 5u) -- proving non-peer flows are
// unaffected is exactly what that guard is for.
class DflyShardReplicaPeerModeTest : public BaseFamilyTest {
 protected:
  uint64_t ObservedJournalExecutedAfterMarker(uint64_t seed, uint64_t master_lsn, bool peer_mode) {
    DflyShardReplica::ServerContext ctx{"127.0.0.1", 1, {}};
    MasterContext master_context;
    master_context.num_flows = 1;
    auto multi_shard_exe = std::make_shared<MultiShardExecution>();
    RdbLoadContext load_context;
    DflyShardReplica flow(ctx, master_context, /*flow_id=*/0, service_.get(), multi_shard_exe,
                          &load_context, /*origin_idx=*/0, peer_mode);
    flow.SetRecordsExecuted(seed);
    flow.AdoptAuthoritativeLsn(master_lsn);
    return flow.JournalExecutedCount();
  }

  // drakeydb: Phase 3 T6b fix-round-1 (C1) -- genuine member, not inlined into a TEST_F body (see
  // this class's own reasoning above, same as DflyShardReplicaOriginTest's): sets apply_failed_
  // the same way StableSyncDflyReadFb's ExecuteTx failure branch does (replica.cc), then calls
  // the real AdoptAuthoritativeLsn.
  uint64_t ObservedJournalExecutedAfterFailedApplyThenMarker(uint64_t seed, uint64_t master_lsn) {
    DflyShardReplica::ServerContext ctx{"127.0.0.1", 1, {}};
    MasterContext master_context;
    master_context.num_flows = 1;
    auto multi_shard_exe = std::make_shared<MultiShardExecution>();
    RdbLoadContext load_context;
    DflyShardReplica flow(ctx, master_context, /*flow_id=*/0, service_.get(), multi_shard_exe,
                          &load_context, /*origin_idx=*/0, /*peer_mode=*/true);
    flow.SetRecordsExecuted(seed);
    flow.apply_failed_ = true;
    flow.AdoptAuthoritativeLsn(master_lsn);
    return flow.JournalExecutedCount();
  }

  // drakeydb: Phase 3 T6b fix-round-1 (Q1) -- genuine member driving the real
  // AdoptAuthoritativeLsn from a caller-supplied LSN. The caller (see
  // AdoptAuthoritativeLsnComposesWithRealSenderMarker) decodes that value from a real,
  // sender-produced marker via a real TransactionReader entirely outside this friended class --
  // decoding needs no DflyShardReplica access at all -- so only the actual adoption call has to
  // run as a genuine member here.
  uint64_t ObservedJournalExecutedAfterAdopt(uint64_t master_lsn) {
    DflyShardReplica::ServerContext ctx{"127.0.0.1", 1, {}};
    MasterContext master_context;
    master_context.num_flows = 1;
    auto multi_shard_exe = std::make_shared<MultiShardExecution>();
    RdbLoadContext load_context;
    DflyShardReplica flow(ctx, master_context, /*flow_id=*/0, service_.get(), multi_shard_exe,
                          &load_context, /*origin_idx=*/0, /*peer_mode=*/true);
    flow.AdoptAuthoritativeLsn(master_lsn);
    return flow.JournalExecutedCount();
  }
};

TEST_F(DflyShardReplicaPeerModeTest, AdoptAuthoritativeLsnSetsExecutedCountInPeerModeOnly) {
  pp_->at(0)->Await([&] {
    // Peer mode: journal_rec_executed_ becomes master_lsn + 1, discarding the seed entirely.
    EXPECT_EQ(43u, ObservedJournalExecutedAfterMarker(/*seed=*/5, /*master_lsn=*/42,
                                                      /*peer_mode=*/true));

    // Non-peer mode: byte-identical to upstream's "Do nothing" Op::LSN branch -- the seed must
    // survive untouched.
    EXPECT_EQ(5u, ObservedJournalExecutedAfterMarker(/*seed=*/5, /*master_lsn=*/42,
                                                     /*peer_mode=*/false));
  });
}

// drakeydb: Phase 3 T6b fix-round-1 (C1) -- once an apply has failed on this flow (a local OOM
// applying an entry is a normal operational outcome -- facade::DispatchResult::OOM -- not a
// can't-happen), AdoptAuthoritativeLsn must refuse to adopt ANY later marker, no matter its
// value: without this, the very next authoritative Op::LSN marker (this task's own gap-correction
// marker, the fully-filtered-link resolution marker, or the pre-existing periodic heartbeat)
// would silently overwrite journal_rec_executed_ past the entry that was never actually applied,
// and a reconnect would never re-offer it -- permanent, silent divergence from the mesh. This
// pins that refusal directly (apply_failed_ set the same way StableSyncDflyReadFb's ExecuteTx
// failure branch sets it -- replica.cc -- via friend access, since driving that branch for real
// needs a live socket StableSyncDflyReadFb's own tests avoid for the same reason
// DflyShardReplicaOriginTest's comment gives).
//
// Falsifying (verified by hand -- see task-6b-report.md): removing the `if (apply_failed_)
// return;` guard from AdoptAuthoritativeLsn makes the EXPECT_EQ below fail (43 instead of the
// un-advanced seed, 5).
TEST_F(DflyShardReplicaPeerModeTest, AdoptAuthoritativeLsnSuppressedAfterApplyFailure) {
  pp_->at(0)->Await([&] {
    EXPECT_EQ(5u, ObservedJournalExecutedAfterFailedApplyThenMarker(/*seed=*/5,
                                                                    /*master_lsn=*/42));
  });
}

// drakeydb: Phase 3 T6b fix-round-1 (Q1) -- the original PeerModeGapMarkerCoalescesAndReceiver
// LandsOnTrueLsn (journal/journal_test.cc) hand-computed its expected journal_rec_executed_ as
// `tx_data.lsn + 1`, duplicating AdoptAuthoritativeLsn's own arithmetic rather than calling it --
// so it could not have failed even if the real method used `+ 2`. This test drives the REAL
// sender (a real peer-mode JournalStreamer processing real journal::RecordEntry calls, so the
// marker's value is computed by streamer.cc's actual code, not chosen by this test), decodes it
// with a real TransactionReader, and feeds that DECODED value into the REAL
// DflyShardReplica::AdoptAuthoritativeLsn (replica.cc's own expression,
// AdoptAuthoritativeLsn(tx_data.lsn), just called from here instead of from inside
// StableSyncDflyReadFb's loop, which needs a live socket -- see DflyShardReplicaOriginTest's own
// comment on why a bare construction test is this codebase's established alternative). The
// closing assertion compares against journal::GetLsn(), read directly after the recording is
// done -- not a formula this test shares with either the sender's or the receiver's arithmetic --
// so an inconsistency on EITHER side is what this test can catch that the two single-sided tests
// (this file's AdoptAuthoritativeLsnSetsExecutedCountInPeerModeOnly and journal_test.cc's own
// sender-side coalescing test) cannot.
//
// The one dropped entry below is deliberately the ONLY drop in this scenario, not a run of
// several: it is the very first entry this fresh streamer's drop path (fix-round-1, C2) ever
// evaluates, and that path's own last_lsn_time_ throttle -- like the pre-existing write-path
// periodic marker it is borrowed from -- always fires on its first-ever check (see streamer.cc's
// drop-path comment, and journal_test.cc's MixedOriginBacklogPeerVsFullStream for the same
// property spelled out in detail). A second, immediately-following drop would not reliably
// produce a second marker for the write-path gap-correction code to compose instead (its own
// coalescing behavior is already covered, against exact hand-verified values, by
// journal_test.cc's PeerModeGapMarkerCoalescesAndTransactionReaderAdoptsCleanly) -- so this test
// keeps to the one guaranteed-deterministic marker instead of also depending on throttle timing.
//
// journal::RecordEntry is called directly against this fixture's real (BaseFamilyTest) shard --
// unlike journal_test.cc's own tests, which use a bespoke, deliberately minimal single-shard
// fixture built for exactly this purpose (see JournalStreamerPeerFilterTest's own comment).
// DflyShardReplica needs a real, valid Service* from its very first constructor statement
// (service_(*service) -- an unconditional dereference), which only BaseFamilyTest provides in
// this codebase; building one from scratch to keep this test in journal_test.cc's fixture instead
// was judged more invasive than the alternative taken here.
TEST_F(DflyShardReplicaPeerModeTest, AdoptAuthoritativeLsnComposesWithRealSenderMarker) {
  constexpr uint32_t kPeerIdx = 9;  // some peer's PeerRegistry index; != PeerRegistry::kSelfIdx.
  pp_->at(0)->Await([&] {
    // Unlike journal_test.cc's own fixture (which calls this in its own SetUp), a plain
    // BaseFamilyTest never starts this shard's journal on its own -- production only does so
    // lazily, when a replica first connects (dflycmd.cc's JOURNAL START). Idempotent
    // (JournalSlice::Init() is a no-op if already initialized), so safe to call unconditionally.
    journal::StartInThread();
    LSN base = journal::GetLsn();  // not assumed to be a pristine 1: this fixture does not own
                                   // its shard exclusively. Used only to seed tx_reader below,
                                   // matching a real partial resume's own seeding.

    ExecutionState send_cntx;
    JournalStreamer::Config config;
    config.peer_mode = true;
    CapturingFiberSocket socket;
    JournalStreamer streamer(&send_cntx, config);
    streamer.Start(&socket);  // start_partial_sync_at == 0: registers as a live listener now.

    std::array<std::string_view, 2> set_a{"a", "1"};
    journal::RecordEntry(0, journal::Op::COMMAND, 0, std::nullopt,
                         journal::Entry::Payload{"SET", ArgSlice{set_a.data(), set_a.size()}},
                         PeerRegistry::kSelfIdx);  // self -- kept.
    std::array<std::string_view, 2> set_b{"b", "2"};
    journal::RecordEntry(0, journal::Op::COMMAND, 0, std::nullopt,
                         journal::Entry::Payload{"SET", ArgSlice{set_b.data(), set_b.size()}},
                         kPeerIdx);  // peer -- dropped; the very first entry this streamer's
                                     // drop path evaluates, so it gets its own real,
                                     // sender-computed marker immediately (see this test's own
                                     // header comment).

    // drakeydb: Phase 3 T6b fix-round-2 -- read immediately after the last RecordEntry, before
    // Cancel(). Cancel() can yield this fiber (WaitForInflightToComplete awaits the socket write's
    // completion), and this fixture does not own shard 0 exclusively -- it's a real BaseFamilyTest
    // shard (see this test's own header comment) -- so an unrelated journal record landing on
    // shard 0 during that yield would advance GetLsn() past what this scenario itself produced,
    // decoupling "ground truth" from the marker this test is actually checking. Closed in practice
    // today (no keys, no other clients touch this shard during this Await), but not by
    // construction -- reading here removes the window entirely instead of relying on that holding.
    LSN true_next_lsn = journal::GetLsn();  // ground truth: the ONE thing this test does not
                                            // derive from any formula shared with the code under
                                            // test, on either the sender or the receiver side.

    streamer.Cancel();

    base::IoBuf buf;
    io::BufSink sink{&buf};
    sink.Write(io::Buffer(socket.captured));
    io::BufSource source{&buf};
    JournalReader reader{&source, 0};
    TransactionReader tx_reader{base - 1, /*peer_mode=*/true};
    ExecutionState read_cntx;
    TransactionData tx_data;

    ASSERT_TRUE(tx_reader.NextTxData(&reader, &read_cntx, &tx_data));  // SET a
    ASSERT_EQ(journal::Op::COMMAND, tx_data.opcode);

    ASSERT_TRUE(tx_reader.NextTxData(&reader, &read_cntx, &tx_data));  // the real drop-path marker
    ASSERT_EQ(journal::Op::LSN, tx_data.opcode);

    // drakeydb: Phase 3 T6b fix-round-1 (Q1) -- ObservedJournalExecutedAfterAdopt is a genuine
    // member of DflyShardReplicaPeerModeTest (see that class's own comment): this lambda is a
    // separate, unrelated closure type, not a member of the friended class itself, so it cannot
    // touch DflyShardReplica's private members (or the protected ServerContext) directly --
    // constructing a DflyShardReplica right here, inline, does not compile.
    uint64_t observed = ObservedJournalExecutedAfterAdopt(tx_data.lsn);  // the decoded value --
                                                                         // not a literal.

    // The composition: the sender's drop-path marker carries the dropped entry's own true LSN
    // (streamer.cc); adopting it (+1) must land exactly on the next LSN the master has not
    // assigned yet.
    EXPECT_EQ(true_next_lsn, observed);
  });
}

}  // namespace dfly
