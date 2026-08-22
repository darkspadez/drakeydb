// Copyright 2026, drakeydb authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "server/peer_replication.h"

#include <algorithm>
#include <chrono>
#include <mutex>
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

bool SameEndpoint(const PeerReplicationManager::Endpoint& ep, const ReplicaSummary& s) {
  return ep.host == s.host && ep.port == s.port;
}

// Hops to each peer's own proactor (via GetSummary()) to compare it against `ep`. Replica has no
// cheaper accessor for its connection target: ProtocolClient::GetHost()/GetPort() are hidden by
// Replica's private inheritance from ProtocolClient, and GetSummary() is the only public window
// onto host/port. Must therefore be called with mu_ NOT held -- see the class-level comment.
bool AnyMatches(const std::vector<std::shared_ptr<Replica>>& snapshot,
                const PeerReplicationManager::Endpoint& ep) {
  for (const auto& p : snapshot) {
    if (SameEndpoint(ep, p->GetSummary()))
      return true;
  }
  return false;
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

  // Step 1: refuse if closed; short-circuit if `ep` is already attached, so a plain duplicate
  // REPLICAOF never pays for a network handshake below. peers_ is snapshotted under mu_, but the
  // actual endpoint comparison (which hops to each peer's own proactor) happens after releasing
  // the lock.
  bool closed = false;
  std::vector<std::shared_ptr<Replica>> snapshot;
  {
    util::fb2::LockGuard lk(mu_);
    closed = closed_;
    if (!closed)
      snapshot = peers_;
  }
  if (closed)
    return GenericError{kClosedMsg};
  if (AnyMatches(snapshot, ep)) {
    *already_attached = true;
    return {};
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

  // Step 3: re-check -- closes the race where a concurrent Add() attached the same endpoint while
  // we were blocked above -- and, if still clear, register. A vanishingly narrow window remains
  // between this check and the commit below (the check itself must run off mu_); accepted the
  // same way upstream ReplicaOfInternal accepts its own "weak check" races (see its comment).
  {
    util::fb2::LockGuard lk(mu_);
    closed = closed_;
    if (!closed)
      snapshot = peers_;
  }
  if (!closed && AnyMatches(snapshot, ep)) {
    r->Stop();
    *already_attached = true;
    return {};
  }

  std::vector<std::shared_ptr<Replica>> replaced;
  {
    util::fb2::LockGuard lk(mu_);
    closed = closed_;
    if (!closed) {
      if (!IsMultiMaster()) {
        replaced = std::move(peers_);
        peers_.clear();
      }
      peers_.push_back(r);
    }
  }
  if (closed) {
    r->Stop();
    return GenericError{kClosedMsg};
  }

  // Step 4: unlocked. Stop the peer(s) this one replaced, then -- same ordering as upstream
  // ReplicaOfInternal -- start the main replication fiber only after registration.
  for (auto& p : replaced)
    p->Stop();
  if (mode == StartMode::kBlockingHandshake)
    r->StartMainReplicationFiber(std::nullopt);
  return {};
}

bool PeerReplicationManager::Remove(const Endpoint& ep) {
  std::vector<std::shared_ptr<Replica>> snapshot;
  {
    util::fb2::LockGuard lk(mu_);
    snapshot = peers_;
  }
  std::shared_ptr<Replica> target;
  for (auto& p : snapshot) {
    if (SameEndpoint(ep, p->GetSummary())) {
      target = p;
      break;
    }
  }
  if (!target)
    return false;

  bool removed = false;
  {
    util::fb2::LockGuard lk(mu_);
    auto it = std::find(peers_.begin(), peers_.end(), target);
    if (it != peers_.end()) {
      peers_.erase(it);
      removed = true;
    }
  }
  if (removed)
    target->Stop();
  return removed;
}

void PeerReplicationManager::RemoveAll() {
  std::vector<std::shared_ptr<Replica>> snapshot;
  {
    util::fb2::LockGuard lk(mu_);
    snapshot = std::move(peers_);
    peers_.clear();
  }
  for (auto& p : snapshot)
    p->Stop();
}

void PeerReplicationManager::Shutdown() {
  std::vector<std::shared_ptr<Replica>> snapshot;
  {
    util::fb2::LockGuard lk(mu_);
    closed_ = true;
    snapshot = std::move(peers_);
    peers_.clear();
  }
  for (auto& p : snapshot)
    p->Stop();
}

void PeerReplicationManager::PauseAll(bool pause) {
  std::vector<std::shared_ptr<Replica>> snapshot;
  {
    util::fb2::LockGuard lk(mu_);
    snapshot = peers_;
  }
  for (auto& p : snapshot)
    p->Pause(pause);
}

std::vector<ReplicaSummary> PeerReplicationManager::Summaries() const {
  std::vector<std::shared_ptr<Replica>> snapshot;
  {
    util::fb2::LockGuard lk(mu_);
    snapshot = peers_;
  }
  std::vector<ReplicaSummary> out;
  out.reserve(snapshot.size());
  for (auto& p : snapshot)
    out.push_back(p->GetSummary());
  return out;
}

std::vector<PeerReplicationManager::Endpoint> PeerReplicationManager::Endpoints() const {
  std::vector<Endpoint> out;
  for (auto& s : Summaries())
    out.push_back({s.host, s.port});
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
