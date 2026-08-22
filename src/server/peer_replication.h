// Copyright 2026, drakeydb authors.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <absl/base/thread_annotations.h>
#include <absl/functional/function_ref.h>

#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "server/execution_state.h"  // GenericError
#include "server/replica_types.h"    // ReplicaSummary
#include "util/fibers/synchronization.h"

namespace dfly {

class Replica;
class Service;
class PeerRegistry;

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

// PeerReplicationManager owns an active node's peer links: the set of peer-mode Replica objects
// (see ReplicaPeerMode in replica.h) this node is fanning in from. It implements the active-node
// half of REPLICAOF -- Add ("<host> <port>"), Remove ("REMOVE <host> <port>"), RemoveAll ("NO
// ONE") -- plus the lifecycle hooks ServerFamily needs (Shutdown, PauseAll) and the read paths
// used by INFO (Summaries, Endpoints, Size).
//
// Without --multi_master, attaching a new peer replaces whatever was previously attached (classic
// single-master REPLICAOF semantics); with --multi_master, peers accumulate (fan-in). Detaching a
// peer (Remove/RemoveAll) only stops the link -- data already merged from it is never rolled back.
//
// Thread-safe: every method may be called from any fiber. mu_ only ever guards the `peers_`
// vector and the `closed_` flag -- both cheap to touch -- so it is never held across a network
// operation or a call into a Replica: Replica's own methods (Stop(), Pause(), GetSummary()) hop
// to that replica's own proactor and so must never run while mu_ is held.
class PeerReplicationManager {
 public:
  struct Endpoint {
    std::string host;
    uint16_t port = 0;
  };

  // REPLICAOF <host> <port> in active mode. kBlockingHandshake: connect+greet synchronously like
  // upstream REPLICAOF -- on failure nothing changes and the error is returned. kBackground: boot
  // time (--replicaof); starts the reconnect loop and returns immediately.
  enum class StartMode { kBlockingHandshake, kBackground };  // REPLICAOF vs boot --replicaof

  PeerReplicationManager(Service* service, PeerRegistry* registry);
  ~PeerReplicationManager();  // calls Shutdown()

  // Attaches `ep` as a new peer. Without --multi_master, replaces whatever peer was previously
  // attached (the old one is stopped only after the new one is registered, so a failed handshake
  // never tears down a working link). With --multi_master, appends instead. Attaching an endpoint
  // that is already attached is a no-op: *already_attached is set and Add() returns success.
  // Fails (attaching nothing) once Shutdown() has been called.
  GenericError Add(const Endpoint& ep, std::string_view self_replid, StartMode mode,
                   bool* already_attached);

  // REPLICAOF REMOVE <host> <port>: stops the matching link, if any, and reports whether one was
  // found. Data already merged from that peer is left in place.
  bool Remove(const Endpoint& ep);

  // REPLICAOF NO ONE: stops every attached link. Data already merged from them is left in place.
  void RemoveAll();

  // RemoveAll(), then refuses every future Add(). Idempotent: called by ~PeerReplicationManager()
  // and by ServerFamily::Shutdown().
  void Shutdown();

  // Pauses or resumes every attached link (ServerFamily::PauseReplication).
  void PauseAll(bool pause);

  // One summary per attached peer, in attach order. Each hops to that peer's own proactor.
  std::vector<ReplicaSummary> Summaries() const;

  // The attached endpoints, in attach order.
  std::vector<Endpoint> Endpoints() const;

  size_t Size() const;

  SyncGate& sync_gate();

 private:
  mutable util::fb2::Mutex mu_;
  std::vector<std::shared_ptr<Replica>> peers_ ABSL_GUARDED_BY(mu_);  // attach order
  bool closed_ ABSL_GUARDED_BY(mu_) = false;
  SyncGate gate_;
  Service* service_;
  PeerRegistry* registry_;
};

}  // namespace dfly
