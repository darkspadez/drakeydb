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

constexpr int kPollIters = 1000;
constexpr auto kPollInterval = std::chrono::milliseconds(5);  // kPollIters * kPollInterval ~= 5s.

// Polls `pred` inside proactor `i` until true (or 5s). Returns the final value. For use from the
// bare gtest thread to check gate state, which must never be touched directly from that thread.
bool AwaitUntil(util::ProactorPool* pp, unsigned i, absl::FunctionRef<bool()> pred) {
  for (int n = 0; n < kPollIters; ++n) {
    if (pp->at(i)->Await([&] { return pred(); }))
      return true;
    std::this_thread::sleep_for(kPollInterval);
  }
  return false;
}

// Polls `pred` until true (or 5s). Returns the final value. For use *inside* a fiber only (calls
// `pred` directly and sleeps via ThisFiber): bounds what would otherwise be an unbounded spin, so
// a regression that keeps `pred` false forever fails the enclosing ASSERT/EXPECT instead of
// hanging the fiber -- and, transitively, whatever later joins it -- forever.
bool WaitFor(absl::FunctionRef<bool()> pred) {
  for (int n = 0; n < kPollIters; ++n) {
    if (pred())
      return true;
    util::ThisFiber::SleepFor(kPollInterval);
  }
  return false;
}

// Polls a plain atomic flag (not gate state, so safe to read from the bare gtest thread the same
// way this file already reads/writes `cancel`/`release`/`got` directly) until true, or 5s. Used
// before Fiber::Join() on a fiber whose body calls SyncGate::Acquire(): Join() blocks
// unconditionally, so if Acquire() itself never returns (the regression these tests exist to
// catch), a bare Join() would hang the whole test binary with no diagnostic.
bool WaitForFlag(const std::atomic_bool& flag) {
  for (int n = 0; n < kPollIters; ++n) {
    if (flag.load())
      return true;
    std::this_thread::sleep_for(kPollInterval);
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
  ASSERT_TRUE(WaitForFlag(waiter_done)) << "waiter fiber did not return within the time bound -- "
                                           "SyncGate::Acquire cancellation appears broken";
  waiter.Join();
  EXPECT_FALSE(got.load());
  EXPECT_EQ(0u, pp_->at(0)->Await([&] { return gate.NumWaiting(); }));
  release = true;
  holder.Join();  // bounded: holder's own wait above gives up after ~5s regardless of `release`.
  EXPECT_FALSE(pp_->at(0)->Await([&] { return gate.IsHeld(); }));
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
  std::this_thread::sleep_for(std::chrono::milliseconds(250));
  EXPECT_FALSE(acquired.load());
  loading = false;
  // f.Join() blocks unconditionally; if SyncGate stopped re-checking external_loading_ after the
  // first poll, Acquire() may never return even though `loading` is now false. Wait for `done`
  // with a bound first: on timeout this ASSERT_TRUE fails (with a diagnostic) and returns before
  // the unconditional Join() below can hang. `f` is deliberately left un-joined on that path
  // (not Detach()-ed) so its destructor's own CHECK aborts the process deterministically instead
  // of risking a detached, still-blocked-in-Acquire() fiber later touching this function's
  // now-destroyed locals -- see the identical reasoning in CancelledWaiterGetsEmptyLease above.
  ASSERT_TRUE(WaitForFlag(done)) << "fiber did not return within the time bound after "
                                    "external_loading cleared -- SyncGate may not be "
                                    "re-checking external_loading_";
  f.Join();
  EXPECT_TRUE(acquired.load());
}

}  // namespace dfly
