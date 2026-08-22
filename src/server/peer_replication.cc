// Copyright 2026, drakeydb authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "server/peer_replication.h"

#include <algorithm>
#include <chrono>
#include <mutex>
#include <utility>

namespace dfly {

SyncGate::SyncGate(ExternalLoadingFn external_loading)
    : external_loading_(std::move(external_loading)) {
}

SyncGate::Lease::Lease(Lease&& o) noexcept : gate_(std::exchange(o.gate_, nullptr)) {
}

SyncGate::Lease& SyncGate::Lease::operator=(Lease&& o) noexcept {
  if (this != &o) {
    if (gate_)
      gate_->Release();
    gate_ = std::exchange(o.gate_, nullptr);
  }
  return *this;
}

SyncGate::Lease::~Lease() {
  if (gate_)
    gate_->Release();
}

SyncGate::Lease SyncGate::Acquire(absl::FunctionRef<bool()> cancelled) {
  std::unique_lock lk(mu_);
  const uint64_t my = next_ticket_++;
  waiters_.push_back(my);
  auto ready = [&] {
    return !held_ && waiters_.front() == my && !(external_loading_ && external_loading_());
  };
  while (!ready()) {
    if (cancelled()) {
      waiters_.erase(std::find(waiters_.begin(), waiters_.end(), my));
      // fb2::CondVarAny has no internal mutex (unlike std::condition_variable_any): notify_all()
      // must be called while still holding mu_, or it races the wait queue it touches.
      cv_.notify_all();
      return Lease{};
    }
    cv_.wait_for(lk, std::chrono::milliseconds(100));
  }
  waiters_.pop_front();
  held_ = true;
  return Lease{this};
}

void SyncGate::Release() {
  std::lock_guard lk(mu_);
  held_ = false;
  // fb2::CondVarAny has no internal mutex (unlike std::condition_variable_any): notify_all() must
  // be called while still holding mu_, or it races the wait queue it touches.
  cv_.notify_all();
}

bool SyncGate::IsHeld() const {
  std::lock_guard lk(mu_);
  return held_;
}

size_t SyncGate::NumWaiting() const {
  std::lock_guard lk(mu_);
  return waiters_.size();
}

}  // namespace dfly
