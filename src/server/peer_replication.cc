// Copyright 2026, drakeydb authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "server/peer_replication.h"

#include <algorithm>
#include <chrono>
#include <mutex>
#include <optional>
#include <utility>

#include "server/multi_master.h"
#include "server/replica.h"
#include "server/server_state.h"

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
                                     ReplicaPeerMode{&gate_, registry_});
  if (mode == StartMode::kBlockingHandshake) {
    GenericError ec = r->Start();
    if (ec || r->IsContextCancelled())
      return ec ? ec : GenericError{"replication cancelled"};
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
  // through on the same Replica.
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

  // Step 4: unlocked -- same ordering as upstream ReplicaOfInternal -- start the main replication
  // fiber only after registration.
  if (mode == StartMode::kBlockingHandshake)
    r->StartMainReplicationFiber(std::nullopt);
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

size_t PeerReplicationManager::Size() const {
  util::fb2::LockGuard lk(mu_);
  return peers_.size();
}

SyncGate& PeerReplicationManager::sync_gate() {
  return gate_;
}

}  // namespace dfly
