// Copyright 2026, drakeydb authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "server/peer_replication.h"

#include <absl/flags/declare.h>
#include <absl/flags/flag.h>
#include <absl/flags/reflection.h>
#include <absl/functional/function_ref.h>

#include <array>
#include <atomic>
#include <chrono>
#include <memory>

#include "base/gtest.h"
#include "facade/facade_test.h"
#include "server/multi_master.h"
#include "server/node_identity.h"
#include "server/test_utils.h"
#include "util/fibers/fibers.h"
#include "util/fibers/pool.h"

ABSL_DECLARE_FLAG(bool, force_epoll);
ABSL_DECLARE_FLAG(std::string, dir);
ABSL_DECLARE_FLAG(bool, active_replica);
ABSL_DECLARE_FLAG(bool, multi_master);

namespace dfly {

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

}  // namespace dfly
