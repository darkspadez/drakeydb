// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <type_traits>

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

  // drakeydb: P4-1 Task 5, fix round 1 -- templated so this policy can back both mcflag
  // (uint32_t) and DbTable::mvcc (MvccStamp). const T&, not T&: matches this codebase's other
  // three templated DestroyValue precedents (dash_test.cc:84, dash_bench.cc:61,
  // tiering/small_bins.h:115) and restores the rvalue-binding the pre-P4 by-value uint32_t
  // signature had. The static_assert restores the type guard that fixed uint32_t signature
  // enforced by accident: a future owning value type under this policy (plausible for P4-5's
  // tombstones) must fail to compile here rather than silently leak via a no-op destroy.
  template <typename T> static void DestroyValue(const T&) {
    static_assert(std::is_trivially_destructible_v<std::remove_cvref_t<T>>,
                  "ExpireTablePolicy's no-op destroy is only valid for trivially destructible "
                  "values");
  }

  static bool Equal(const PrimeKey& s1, std::string_view s2) {
    return s1 == s2;
  }
};

}  // namespace detail
}  // namespace dfly
