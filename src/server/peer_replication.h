// Copyright 2026, drakeydb authors.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <absl/base/thread_annotations.h>
#include <absl/container/flat_hash_map.h>
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

// Tracks which live peer-mode Replica currently owns each remote node UUID. Unlike PeerRegistry's
// append-only origin-index mapping, these claims are ephemeral: Stop() releases a claim so another
// endpoint may attach that peer later. A reconnect may atomically move an owner's claim if the
// endpoint starts presenting a different UUID.
//
// This registry is deliberately independent of PeerReplicationManager::mu_: a background
// Replica discovers its UUID asynchronously in Greet(), and calling back into the manager while a
// concurrent Remove()/Shutdown() holds its mutex across Replica::Stop() would deadlock.
class PeerIdentityClaims {
 public:
  // Claims `uuid` for `owner_id`, replacing any different UUID previously held by that owner.
  // Returns false when another live owner already holds `uuid`; in that case any previous claim
  // held by `owner_id` is released because the endpoint has changed identity.
  //
  // Always (re)starts the claim as not-established -- see MarkEstablished/HasUnestablishedClaim
  // below. TryClaim is only ever called once per connection attempt (Replica::Greet(), right
  // after the peer's uuid becomes known), so a fresh call here always represents a handshake that
  // has not yet reached stable sync, even when it is re-confirming the same uuid an earlier
  // (now-dropped) connection to the same owner already held.
  bool TryClaim(uint64_t owner_id, std::string_view uuid);

  // Releases the claim held by `owner_id`, if any. Idempotent.
  void Release(uint64_t owner_id);

  // drakeydb D-7: marks `owner_id`'s current claim, if any, as established (its link has reached
  // stable sync -- Replica's R_SYNC_OK). A no-op if `owner_id` holds no claim, e.g. it raced a
  // Release(). Never resets to false on its own; only a fresh TryClaim (a new connection attempt)
  // does that -- see TryClaim's own comment.
  void MarkEstablished(uint64_t owner_id);

  // drakeydb D-7: true iff some live owner currently claims `uuid` and MarkEstablished has not
  // been called for that claim. Backs PeerReplicationManager::HasUnestablishedPeerWithUuid (the
  // reciprocal-connect tiebreak's "is P one of our own not-yet-established peer links" check) --
  // this registry has its own independent lock (see the class comment above), so this never hops
  // into a Replica the way PeerReplicationManager::Summaries() does.
  bool HasUnestablishedClaim(std::string_view uuid) const;

 private:
  void ReleaseLocked(uint64_t owner_id) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  struct Claim {
    uint64_t owner_id = 0;
    bool established = false;
  };

  mutable util::fb2::Mutex mu_;
  absl::flat_hash_map<std::string, Claim> owners_by_uuid_ ABSL_GUARDED_BY(mu_);
  absl::flat_hash_map<uint64_t, std::string> uuids_by_owner_ ABSL_GUARDED_BY(mu_);
};

// drakeydb D-7: the reciprocal-connect uuid tiebreak, as a pure function of the three facts the
// admission check (server_family.cc's ReplConf, CAPA dragonfly case) has on hand: whether this
// node already has its own not-yet-established peer link claiming the connecting consumer's uuid
// (PeerReplicationManager::HasUnestablishedPeerWithUuid), this node's own uuid, and the
// consumer's uuid P. True means "refuse this consumer with a retryable error".
//
// Ported from KeyDB's processReplconfUuid (replication.cpp:1557-1599), whose own tiebreak is dead
// code: it guards on FSameUuidNoNil(mi->master_uuid, c->uuid), true only when the two uuids are
// *equal* (replication.cpp:92-98), so the following `memcmp(mi->master_uuid, c->uuid, ...) < 0`
// compares a value with itself and is always 0 -- freeClientAsync never fires. This ports the
// *intent* (self uuid vs. peer uuid), not the code: `self_uuid < peer_uuid` refuses.
//
// Both nodes of a reciprocal pair call this with the two uuids in swapped roles (each one's
// self_uuid is the other's peer_uuid) and -- barring the timing skew documented on
// HasUnestablishedPeerWithUuid, where one side's own outbound claim has not been recorded yet --
// the same has_unestablished_own_link result, so it is impossible for both calls to return true
// (self_uuid < peer_uuid and peer_uuid < self_uuid cannot both hold) and, whenever
// has_unestablished_own_link is true on both sides, impossible for both to return false either:
// exactly one of < or > holds for two distinct strings.
bool ShouldRefuseReciprocalPeer(bool has_unestablished_own_link, std::string_view self_uuid,
                                std::string_view peer_uuid);

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
// Management lookups identify each attached peer by the exact endpoint (host string + port) given
// to Add(), so Add()/Remove() remain pure in-memory comparisons under mu_. Separately, each
// successful Greet() must claim the remote node UUID in identity_claims_ before sync begins; this
// prevents endpoint aliases from consuming the same journal stream twice.
//
// Thread-safe: every method may be called from any fiber. mu_ guards the `peers_` vector and the
// `closed_` flag, and -- deliberately -- is also held across every call this class makes into a
// Replica (StartMainReplicationFiber(), Stop(), Pause(), GetSummary()), so mu_ serializes all
// Replica-touching operations: at most one runs at a time, and none can race another on the same
// Replica (e.g. INFO replication's Summaries() can never overlap a concurrent
// Remove()/RemoveAll()/Shutdown()'s Stop() on the same Replica -- see Replica::Stop() vs
// Replica::GetSummary() on shard_flows_). The one exception is the network handshake in Add()
// (Start()/EnableReplication()): that step runs on a freshly constructed Replica no other method
// can reach yet (it is not in peers_), so nothing can race it, and it must not block mu_ since it
// waits on DNS/TCP. Holding mu_ across the serialized calls cannot deadlock: Replica never calls
// back into PeerReplicationManager.
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
  // that is already attached is a no-op: *already_attached is set and Add() returns success. A
  // different endpoint presenting an attached UUID fails a blocking handshake; a background link
  // stays down/retrying until that UUID is released. Fails once Shutdown() has been called.
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

  // drakeydb D-7: true iff one of our own peer links currently claims `uuid` and has not yet
  // reached stable sync. Backs the reciprocal-connect tiebreak (server_family.cc's ReplConf) --
  // called from a connection fiber mid another peer's own admission handshake, so, unlike
  // Summaries(), this deliberately never hops into a Replica's own proactor: it answers from
  // identity_claims_ alone, which (see PeerIdentityClaims's class comment) keeps its own lock
  // independent of mu_ for exactly this reason. Holds mu_ only for the closed_ check, so a
  // Shutdown() in progress is never reported as still holding a claim.
  bool HasUnestablishedPeerWithUuid(std::string_view uuid) const;

  size_t Size() const;

  SyncGate& sync_gate();

 private:
  mutable util::fb2::Mutex mu_;

  // A peer's management key is the endpoint given at Add() time (exact host string + port), stored
  // alongside its Replica so Add()/Remove() can look a peer up by endpoint as a pure,
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
  PeerIdentityClaims identity_claims_;
  Service* service_;
  PeerRegistry* registry_;
};

}  // namespace dfly
