// Copyright 2026, drakeydb authors.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <absl/functional/function_ref.h>

#include <cstdint>
#include <deque>
#include <functional>

#include "util/fibers/synchronization.h"

namespace dfly {

// SyncGate serializes peer full-sync handshakes so that at most one runs at a time: waiters are
// admitted strictly in FIFO (ticket) order, and admission can additionally be deferred while an
// external condition (e.g. this node's own initial RDB load) is in progress. A waiter may bail
// out of the queue early via a cancellation predicate.
//
// Thread-safe: every method takes an internal fiber mutex. Acquire() blocks the calling fiber
// (it must be called from a fiber, never from bare, non-fiber code) until it is granted the
// gate or cancelled.
class SyncGate {
 public:
  using ExternalLoadingFn = std::function<bool()>;

  // `external_loading`, if set, is polled (under the internal mutex, so it must be cheap and
  // non-blocking) to decide whether a caller at the front of the queue may be granted the gate.
  explicit SyncGate(ExternalLoadingFn external_loading = nullptr);

  // Move-only RAII handle for gate ownership. A held (non-empty) Lease releases the gate -- and
  // wakes the next waiter, if any -- when it is destroyed or overwritten via move-assignment.
  class Lease {
   public:
    Lease() = default;
    Lease(Lease&& o) noexcept;
    Lease& operator=(Lease&& o) noexcept;
    ~Lease();  // releases if held

    explicit operator bool() const {
      return gate_ != nullptr;
    }

   private:
    friend class SyncGate;
    explicit Lease(SyncGate* g) : gate_(g) {
    }

    SyncGate* gate_ = nullptr;
  };

  // Blocks until this caller holds the gate (FIFO among waiters) and external_loading() is false.
  // Returns an empty Lease if cancelled() becomes true while waiting (checked every 100ms and on
  // every release). Must be called from a fiber.
  Lease Acquire(absl::FunctionRef<bool()> cancelled);

  bool IsHeld() const;
  size_t NumWaiting() const;

 private:
  void Release();

  mutable util::fb2::Mutex mu_;
  util::fb2::CondVarAny cv_;
  bool held_ = false;
  uint64_t next_ticket_ = 0;
  std::deque<uint64_t> waiters_;  // FIFO of tickets still waiting
  ExternalLoadingFn external_loading_;
};

}  // namespace dfly
