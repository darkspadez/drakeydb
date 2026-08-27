// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "core/compact_object.h"
#include "core/dash.h"

namespace dfly {

namespace detail {

using PrimeKey = CompactKey;
using PrimeValue = CompactValue;

struct PrimeTablePolicy {
  enum : uint8_t { kSlotNum = 14, kBucketNum = 56 };

  static constexpr bool kUseVersion = true;

  static uint64_t HashFn(const PrimeKey& s) {
    return s.HashCode();
  }

  static uint64_t HashFn(std::string_view u) {
    return CompactObj::HashCode(u);
  }

  static void DestroyKey(PrimeKey& cs) {
    cs.Reset();
  }

  static void DestroyValue(PrimeValue& o) {
    o.Reset();
  }

  static bool Equal(const PrimeKey& s1, std::string_view s2) {
    return s1 == s2;
  }
};

struct ExpireTablePolicy {
  enum : uint8_t { kSlotNum = 14, kBucketNum = 56 };
  static constexpr bool kUseVersion = false;

  static uint64_t HashFn(const PrimeKey& s) {
    return s.HashCode();
  }

  static uint64_t HashFn(std::string_view u) {
    return CompactObj::HashCode(u);
  }

  static void DestroyKey(PrimeKey& cs) {
    cs.Reset();
  }

  // drakeydb: P4-1 Task 5 -- templated so this policy can back both mcflag (uint32_t) and
  // DbTable::mvcc (MvccStamp). Both are trivially-destructible PODs with no owned resources, so
  // this stays a no-op for either; a fix-minimal change over the pre-P4 uint32_t-only signature.
  template <typename T> static void DestroyValue(T&) {
  }

  static bool Equal(const PrimeKey& s1, std::string_view s2) {
    return s1 == s2;
  }
};

}  // namespace detail
}  // namespace dfly
