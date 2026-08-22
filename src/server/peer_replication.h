// Copyright 2026, drakeydb authors.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <absl/base/thread_annotations.h>
#include <absl/functional/function_ref.h>

#include <cstddef>
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
  // every release). cancelled(), like external_loading(), is evaluated under the internal mutex,
  // so it must be cheap and non-blocking. Must be called from a fiber.
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
// Each attached peer is identified by the exact endpoint (host string + port) it was given to
// Add() -- not by a resolved address or anything read back from the peer -- so Add()/Remove() can
// look a peer up as a pure, non-blocking in-memory comparison under mu_ (see PeerLink) instead of
// having to ask the peer's own Replica.
//
// Thread-safe: every method may be called from any fiber. mu_ guards the `peers_` vector and the
// `closed_` flag, and -- deliberately -- is also held across every call this class makes into a
// Replica (Stop(), Pause(), GetSummary()), so mu_ serializes all Replica-touching operations: at
// most one runs at a time, and none can race another on the same Replica (e.g. INFO replication's
// Summaries() can never overlap a concurrent Remove()/RemoveAll()/Shutdown()'s Stop() on the same
// Replica -- see Replica::Stop() vs Replica::GetSummary() on shard_flows_). The one exception is
// the network handshake in Add() (Start()/EnableReplication(), and the later
// StartMainReplicationFiber()): that step runs on a freshly constructed Replica no other method
// can reach yet (it is not in peers_), so nothing can race it, and it must not block mu_ since it
// waits on DNS/TCP. Holding mu_ across Stop()/Pause()/GetSummary() cannot deadlock: Replica never
// calls back into PeerReplicationManager.
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

  // The attached endpoints, in attach order. A pure read of what was stored at Add() time --
  // unlike Summaries(), this never hops to a peer's Replica.
  std::vector<Endpoint> Endpoints() const;

  size_t Size() const;

  SyncGate& sync_gate();

 private:
  mutable util::fb2::Mutex mu_;

  // A peer's identity is the endpoint it was given at Add() time (exact host string + port),
  // stored alongside its Replica so Add()/Remove() can look a peer up by endpoint as a pure,
  // non-blocking in-memory comparison under mu_. Replica itself exposes no accessor for this
  // cheaper than GetSummary() (ProtocolClient::GetHost()/GetPort() are hidden by Replica's
  // private inheritance from ProtocolClient) -- and GetSummary() is one of the calls this class
  // makes while holding mu_ (see the class comment above), so using it for the lookup here would
  // mean paying for a proactor hop, under the lock, on every Add()/Remove().
  struct PeerLink {
    Endpoint ep;  // as given to Add(); identifies this peer for Add()/Remove() lookups
    std::shared_ptr<Replica> replica;
  };
  std::vector<PeerLink> peers_ ABSL_GUARDED_BY(mu_);  // attach order
  bool closed_ ABSL_GUARDED_BY(mu_) = false;
  SyncGate gate_;
  Service* service_;
  PeerRegistry* registry_;
};

}  // namespace dfly
