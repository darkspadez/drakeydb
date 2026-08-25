// Copyright 2026, drakeydb authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "server/peer_replication.h"

#include <algorithm>
#include <chrono>
#include <mutex>
#include <optional>
#include <utility>

#include "base/logging.h"
#include "server/multi_master.h"
#include "server/replica.h"
#include "server/server_state.h"

namespace dfly {

bool PeerIdentityClaims::TryClaim(uint64_t owner_id, std::string_view uuid) {
  DCHECK(!uuid.empty());
  util::fb2::LockGuard lk(mu_);

  auto claim = owners_by_uuid_.find(uuid);
  if (claim != owners_by_uuid_.end() && claim->second.owner_id != owner_id) {
    ReleaseLocked(owner_id);
    return false;
  }

  auto previous = uuids_by_owner_.find(owner_id);
  if (previous != uuids_by_owner_.end() && previous->second != uuid) {
    owners_by_uuid_.erase(previous->second);
    previous->second = uuid;
  } else if (previous == uuids_by_owner_.end()) {
    uuids_by_owner_.try_emplace(owner_id, uuid);
  }
  // Always (re)starts as not-established -- see this method's own doc comment (peer_replication.h).
  owners_by_uuid_.insert_or_assign(std::string(uuid), Claim{owner_id, false});
  return true;
}

void PeerIdentityClaims::Release(uint64_t owner_id) {
  util::fb2::LockGuard lk(mu_);
  ReleaseLocked(owner_id);
}

void PeerIdentityClaims::ReleaseLocked(uint64_t owner_id) {
  auto owner = uuids_by_owner_.find(owner_id);
  if (owner == uuids_by_owner_.end())
    return;

  auto claim = owners_by_uuid_.find(owner->second);
  if (claim != owners_by_uuid_.end() && claim->second.owner_id == owner_id)
    owners_by_uuid_.erase(claim);
  uuids_by_owner_.erase(owner);
}

void PeerIdentityClaims::MarkEstablished(uint64_t owner_id) {
  util::fb2::LockGuard lk(mu_);
  auto owner = uuids_by_owner_.find(owner_id);
  if (owner == uuids_by_owner_.end())
    return;
  auto claim = owners_by_uuid_.find(owner->second);
  if (claim != owners_by_uuid_.end() && claim->second.owner_id == owner_id)
    claim->second.established = true;
}

bool PeerIdentityClaims::HasUnestablishedClaim(std::string_view uuid) const {
  util::fb2::LockGuard lk(mu_);
  auto claim = owners_by_uuid_.find(uuid);
  return claim != owners_by_uuid_.end() && !claim->second.established;
}

bool ShouldRefuseReciprocalPeer(bool has_unestablished_own_link, std::string_view self_uuid,
                                std::string_view peer_uuid) {
  return has_unestablished_own_link && self_uuid < peer_uuid;
}

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

// ABSL_NO_THREAD_SAFETY_ANALYSIS is on this method's declaration (peer_replication.h): see that
// declaration's own comment for why std::unique_lock is required here instead of LockGuard.
SyncGate::Lease SyncGate::Acquire(absl::FunctionRef<bool()> cancelled) {
  std::unique_lock lk(mu_);
  const uint64_t ticket = next_ticket_++;
  waiters_.push_back(ticket);
  // ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_): Clang's thread-safety analysis gives a lambda's
  // operator() its own empty lockset, so this function-level ABSL_NO_THREAD_SAFETY_ANALYSIS
  // (peer_replication.h) does not cover it -- same reasoning as PeerReplicationManager::Add()'s
  // has_endpoint lambda further below. Satisfied here: mu_ is held via std::unique_lock for this
  // whole function, and ready() is only ever called below, still within that same scope.
  auto ready = [&]() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    return !held_ && waiters_.front() == ticket && !(external_loading_ && external_loading_());
  };
  while (true) {
    if (cancelled()) {
      waiters_.erase(std::find(waiters_.begin(), waiters_.end(), ticket));
      // fb2::CondVarAny has no internal mutex (unlike std::condition_variable_any): notify_all()
      // must be called while still holding mu_, or it races the wait queue it touches.
      cv_.notify_all();
      return Lease{};
    }
    if (ready())
      break;
    cv_.wait_for(lk, std::chrono::milliseconds(100));
  }
  waiters_.pop_front();
  held_ = true;
  return Lease{this};
}

void SyncGate::Release() {
  util::fb2::LockGuard lk(mu_);
  held_ = false;
  // fb2::CondVarAny has no internal mutex (unlike std::condition_variable_any): notify_all() must
  // be called while still holding mu_, or it races the wait queue it touches.
  cv_.notify_all();
}

bool SyncGate::IsHeld() const {
  util::fb2::LockGuard lk(mu_);
  return held_;
}

size_t SyncGate::NumWaiting() const {
  util::fb2::LockGuard lk(mu_);
  return waiters_.size();
}

namespace {

constexpr char kClosedMsg[] = "peer replication manager is shut down";

bool SameEndpoint(const PeerReplicationManager::Endpoint& a,
                  const PeerReplicationManager::Endpoint& b) {
  return a.host == b.host && a.port == b.port;
}

}  // namespace

PeerReplicationManager::PeerReplicationManager(Service* service, PeerRegistry* registry)
    : gate_([] {
        auto* ss = ServerState::tlocal();
        return ss != nullptr && ss->gstate() == GlobalState::LOADING;
      }),
      service_(service),
      registry_(registry) {
}

PeerReplicationManager::~PeerReplicationManager() {
  Shutdown();
}

GenericError PeerReplicationManager::Add(const Endpoint& ep, std::string_view self_replid,
                                         StartMode mode, bool* already_attached) {
  *already_attached = false;

  // A peer's identity is the endpoint stored in its PeerLink (see the class/member comments), so
  // this is a pure in-memory scan -- never touches any peer's Replica, never hops. Annotated
  // ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_): Clang's thread-safety analysis gives a lambda's operator()
  // its own empty lockset, so reading peers_ here would otherwise fail -Werror=thread-safety on
  // the Clang CI legs even though every call site below holds mu_ via LockGuard.
  auto has_endpoint = [this](const Endpoint& target) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    for (const auto& pl : peers_) {
      if (SameEndpoint(pl.ep, target))
        return true;
    }
    return false;
  };

  // Step 1: refuse if closed; short-circuit if `ep` is already attached, so a plain duplicate
  // REPLICAOF never pays for a network handshake below.
  {
    util::fb2::LockGuard lk(mu_);
    if (closed_)
      return GenericError{kClosedMsg};
    if (has_endpoint(ep)) {
      *already_attached = true;
      return {};
    }
  }

  // Step 2: build and start the candidate replica with mu_ released -- Start() blocks on DNS/TCP.
  auto r = std::make_shared<Replica>(ep.host, ep.port, service_, self_replid, std::nullopt,
                                     ReplicaPeerMode{&gate_, registry_, &identity_claims_});
  if (mode == StartMode::kBlockingHandshake) {
    GenericError ec = r->Start();
    if (ec || r->IsContextCancelled()) {
      // drakeydb D-7 (review round 2): a blocking REPLICAOF's one and only Start() attempt can
      // fail for two known-transient reasons instead of a real one: it lost the
      // reciprocal-connect uuid tiebreak (ShouldRefuseReciprocalPeer,
      // HasUnestablishedPeerWithUuid -- device_or_resource_busy), or the target is itself
      // transiently LOADING from a third, unrelated peer, which rejects even our PING before
      // our own admission check is ever reached (Greet()'s own comment --
      // resource_unavailable_try_again). Neither is a real failure, and both are expected to
      // clear themselves within a retry or two (round-2 review: reproduced this exact second
      // case losing to the first fix alone -- see task-8-report.md). Falling straight through
      // to `return ec` below would surface "-ERR replication cancelled" indistinguishable from
      // a DNS failure or an own-uuid refusal (GenericError(std::string)'s std::error_code
      // member is always default-constructed -- see Replica::LastGreetEc()'s own doc comment,
      // replica.h -- so `ec` itself cannot carry the specific errc here; LastGreetEc() is the
      // one channel that does) -- strictly worse than pre-task behavior, where this would have
      // just succeeded. Recognized *only* by these two specific errcs: every other reason
      // Start() can fail (unreachable host, own uuid, a uuid already claimed by another live
      // peer, ...) still fails the command exactly as before -- silently backgrounding those
      // would hide a real misconfiguration.
      std::error_code greet_ec = r->LastGreetEc();
      if (greet_ec == std::errc::device_or_resource_busy ||
          greet_ec == std::errc::resource_unavailable_try_again) {
        r->EnableReplication();         // fresh state_mask_/fiber; Start() never touched sync_fb_.
        mode = StartMode::kBackground;  // Step 3 below must not re-start the fiber.
      } else {
        return ec ? ec : GenericError{"replication cancelled"};
      }
    }
  } else {
    r->EnableReplication();
  }

  // Step 3: single critical section -- re-check (closes the race where a concurrent Add()
  // attached the same endpoint while we were blocked above) and, if still clear, register:
  // replace the existing peer set unless --multi_master is on. Both the check and the mutation
  // happen under the same lock acquisition, so this is fully atomic; no residual race. Any
  // replaced peer(s) are stopped before mu_ is released rather than after (see the class
  // comment): mu_ serializes every Replica-touching call, so a replaced peer's Stop() can never
  // run concurrently with a GetSummary()/Pause() that some other manager method is mid-way
  // through on the same Replica. For a blocking handshake, starting the main fiber is part of
  // publication too: once `r` is visible in peers_, Remove()/Shutdown() may call Stop(), so the
  // sync_fb_ assignment must happen under the same lock that serializes those operations.
  bool closed = false;
  {
    util::fb2::LockGuard lk(mu_);
    closed = closed_;
    if (!closed) {
      *already_attached = has_endpoint(ep);
      if (!*already_attached) {
        std::vector<PeerLink> replaced;
        if (!IsMultiMaster()) {
          replaced = std::move(peers_);
          peers_.clear();
        }
        peers_.push_back(PeerLink{ep, r});
        for (auto& pl : replaced)
          pl.replica->Stop();
        if (mode == StartMode::kBlockingHandshake)
          r->StartMainReplicationFiber(std::nullopt);
      }
    }
  }
  if (closed) {
    r->Stop();
    return GenericError{kClosedMsg};
  }
  if (*already_attached) {
    r->Stop();
    return {};
  }

  return {};
}

bool PeerReplicationManager::Remove(const Endpoint& ep) {
  // mu_ held across Stop(): see the class comment -- this serializes Remove() against every other
  // Replica-touching call the manager makes (Summaries(), PauseAll(), RemoveAll(), Shutdown(), and
  // the replaced-peer Stop()s inside Add()), so this target's Stop() can never race a GetSummary()
  // or Pause() some other method is mid-way through on it.
  util::fb2::LockGuard lk(mu_);
  auto it = std::find_if(peers_.begin(), peers_.end(),
                         [&](const PeerLink& pl) { return SameEndpoint(pl.ep, ep); });
  if (it == peers_.end())
    return false;
  std::shared_ptr<Replica> target = std::move(it->replica);
  peers_.erase(it);
  target->Stop();
  return true;
}

void PeerReplicationManager::RemoveAll() {
  // mu_ held across every Stop(): see the class comment / Remove() above.
  util::fb2::LockGuard lk(mu_);
  std::vector<PeerLink> snapshot = std::move(peers_);
  peers_.clear();
  for (auto& pl : snapshot)
    pl.replica->Stop();
}

void PeerReplicationManager::Shutdown() {
  // mu_ held across every Stop(): see the class comment / Remove() above.
  util::fb2::LockGuard lk(mu_);
  closed_ = true;
  std::vector<PeerLink> snapshot = std::move(peers_);
  peers_.clear();
  for (auto& pl : snapshot)
    pl.replica->Stop();
}

void PeerReplicationManager::PauseAll(bool pause) {
  // mu_ held across every Pause(): see the class comment / Remove() above.
  util::fb2::LockGuard lk(mu_);
  for (auto& pl : peers_)
    pl.replica->Pause(pause);
}

std::vector<ReplicaSummary> PeerReplicationManager::Summaries() const {
  // mu_ held across every GetSummary(): see the class comment / Remove() above -- this is what
  // keeps INFO replication from ever observing a Replica mid-Stop() from a concurrent
  // Remove()/RemoveAll()/Shutdown()/Add() replace.
  util::fb2::LockGuard lk(mu_);
  std::vector<ReplicaSummary> out;
  out.reserve(peers_.size());
  for (auto& pl : peers_)
    out.push_back(pl.replica->GetSummary());
  return out;
}

std::vector<PeerReplicationManager::Endpoint> PeerReplicationManager::Endpoints() const {
  util::fb2::LockGuard lk(mu_);
  std::vector<Endpoint> out;
  out.reserve(peers_.size());
  for (auto& pl : peers_)
    out.push_back(pl.ep);
  return out;
}

bool PeerReplicationManager::HasUnestablishedPeerWithUuid(std::string_view uuid) const {
  {
    util::fb2::LockGuard lk(mu_);
    if (closed_)
      return false;
  }
  // Deliberately outside the lock above and answered by identity_claims_ alone: see this method's
  // own doc comment (peer_replication.h) and the PeerIdentityClaims class comment for why that
  // registry's lock is kept independent of mu_.
  return identity_claims_.HasUnestablishedClaim(uuid);
}

size_t PeerReplicationManager::Size() const {
  util::fb2::LockGuard lk(mu_);
  return peers_.size();
}

SyncGate& PeerReplicationManager::sync_gate() {
  return gate_;
}

}  // namespace dfly
