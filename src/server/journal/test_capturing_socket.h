// Copyright 2026, drakeydb authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <string>

#include "util/fiber_socket_base.h"

namespace dfly {

// drakeydb: Phase 3 T5 -- an in-memory util::FiberSocketBase double for JournalStreamer tests.
// JournalStreamer only ever calls AsyncWrite/AsyncWriteSome on its destination (never the sync
// Write/WriteSome path, and never reads), and io::AsyncSink::AsyncWrite (io.cc) is pure buffer
// bookkeeping with no ProactorBase dependency, so a synchronous, immediate, in-memory
// AsyncWriteSome needs no real proactor, fiber, or OS socket at all -- just a shard/journal
// thread-local, which the caller's fixture provides. Everything else is unreachable from
// JournalStreamer's write-only usage and is stubbed only to satisfy the abstract interface.
//
// drakeydb: Phase 3 T6b fix-round-1 (Q1) -- factored out of journal/journal_test.cc into this
// shared header so peer_replication_test.cc's AdoptAuthoritativeLsnComposesWithRealSenderMarker
// can drive a real, peer-mode JournalStreamer too (composing a real sender-emitted marker with
// the real DflyShardReplica::AdoptAuthoritativeLsn -- see that test's own comment for why a
// duplicate copy of this class was rejected in favor of one shared definition).
class CapturingFiberSocket : public util::FiberSocketBase {
 public:
  CapturingFiberSocket() : util::FiberSocketBase(nullptr) {
  }

  void AsyncWriteSome(const iovec* v, uint32_t len, io::AsyncProgressCb cb) override {
    size_t total = 0;
    for (uint32_t i = 0; i < len; ++i) {
      captured.append(reinterpret_cast<const char*>(v[i].iov_base), v[i].iov_len);
      total += v[i].iov_len;
    }
    cb(io::Result<size_t>(total));
  }

  io::Result<size_t> WriteSome(const iovec* v, uint32_t len) override {
    size_t total = 0;
    for (uint32_t i = 0; i < len; ++i) {
      captured.append(reinterpret_cast<const char*>(v[i].iov_base), v[i].iov_len);
      total += v[i].iov_len;
    }
    return total;
  }

  // Unused by these tests -- stubbed to satisfy FiberSocketBase's abstract interface.
  std::error_code Shutdown(int) override {
    return {};
  }
  AcceptResult Accept() override {
    return nonstd::make_unexpected(std::make_error_code(std::errc::not_supported));
  }
  std::error_code Connect(const endpoint_type&, std::function<void(int)>) override {
    return {};
  }
  std::error_code Close() override {
    return {};
  }
  bool IsOpen() const override {
    return true;
  }
  io::Result<size_t> RecvMsg(const msghdr&, int) override {
    return size_t{0};
  }
  io::Result<size_t> Recv(const io::MutableBytes&, int) override {
    return size_t{0};
  }
  void set_timeout(uint32_t) override {
  }
  uint32_t timeout() const override {
    return 0;
  }
  endpoint_type LocalEndpoint() const override {
    return {};
  }
  endpoint_type RemoteEndpoint() const override {
    return {};
  }
  void RegisterOnErrorCb(std::function<void(uint32_t)>) override {
  }
  void CancelOnErrorCb() override {
  }
  void AsyncReadSome(const iovec*, uint32_t, io::AsyncProgressCb) override {
  }
  void RegisterOnRecv(OnRecvCb) override {
  }
  void ResetOnRecvHook() override {
  }
  bool IsUDS() const override {
    return false;
  }
  native_handle_type native_handle() const override {
    return -1;
  }
  std::error_code Create(unsigned short) override {
    return {};
  }
  std::error_code Bind(const struct sockaddr*, unsigned) override {
    return {};
  }
  std::error_code Listen(unsigned) override {
    return {};
  }
  std::error_code Listen(uint16_t, unsigned) override {
    return {};
  }
  std::error_code ListenUDS(const char*, mode_t, unsigned) override {
    return {};
  }
  io::Result<size_t> TrySend(io::Bytes) override {
    return size_t{0};
  }
  io::Result<size_t> TrySend(const iovec*, uint32_t) override {
    return size_t{0};
  }
  io::Result<size_t> TryRecv(io::MutableBytes) override {
    return size_t{0};
  }

  std::string captured;
};

}  // namespace dfly
