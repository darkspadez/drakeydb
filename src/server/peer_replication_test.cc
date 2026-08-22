// Copyright 2026, drakeydb authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "server/peer_replication.h"

#include <absl/flags/declare.h>
#include <absl/flags/flag.h>
#include <absl/functional/function_ref.h>

#include <array>
#include <atomic>
#include <chrono>
#include <memory>
#include <thread>

#include "base/gtest.h"
#include "util/fibers/fibers.h"
#include "util/fibers/pool.h"

ABSL_DECLARE_FLAG(bool, force_epoll);

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
// Polls `pred` inside proactor `i` until true (or 5s). Returns the final value.
bool AwaitUntil(util::ProactorPool* pp, unsigned i, absl::FunctionRef<bool()> pred) {
  for (int n = 0; n < 1000; ++n) {
    if (pp->at(i)->Await([&] { return pred(); }))
      return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  return false;
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
    while (gate.NumWaiting() < 2)  // hold until two waiters are queued behind us
      util::ThisFiber::SleepFor(std::chrono::milliseconds(5));
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
    while (!release.load())
      util::ThisFiber::SleepFor(std::chrono::milliseconds(5));
  });
  ASSERT_TRUE(AwaitUntil(pp_.get(), 1, [&] { return gate.IsHeld(); }));
  std::atomic_bool got{true};
  util::fb2::Fiber waiter = pp_->at(1)->LaunchFiber(util::fb2::Launch::post, [&] {
    auto lease = gate.Acquire([&] { return cancel.load(); });
    got = static_cast<bool>(lease);
  });
  ASSERT_TRUE(AwaitUntil(pp_.get(), 0, [&] { return gate.NumWaiting() >= 1; }));
  cancel = true;
  waiter.Join();  // returns within ~100ms although the holder never released
  EXPECT_FALSE(got.load());
  EXPECT_EQ(0u, pp_->at(0)->Await([&] { return gate.NumWaiting(); }));
  release = true;
  holder.Join();
  EXPECT_FALSE(pp_->at(0)->Await([&] { return gate.IsHeld(); }));
}

TEST_F(SyncGateTest, ExternalLoadingDefersGrant) {
  std::atomic_bool loading{true};
  SyncGate gate([&] { return loading.load(); });
  std::atomic_bool acquired{false};
  util::fb2::Fiber f = pp_->at(0)->LaunchFiber(util::fb2::Launch::post, [&] {
    auto lease = gate.Acquire([] { return false; });
    acquired = static_cast<bool>(lease);
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(250));
  EXPECT_FALSE(acquired.load());
  loading = false;
  f.Join();
  EXPECT_TRUE(acquired.load());
}

}  // namespace dfly
