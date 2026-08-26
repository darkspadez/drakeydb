// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <type_traits>
#include <vector>

#include "core/detail/stateless_allocator.h"

namespace dfly {

// DenseSet is a nice but over-optimized data-structure. Probably is not worth it in the first
// place but sometimes the OCD kicks in and one can not resist.
// The advantage of it over redis-dict is smaller meta-data waste.
// dictEntry is 24 bytes, i.e it uses at least 32N bytes where N is the expected length.
// dict requires to allocate dictEntry per each addition in addition to the supplied key.
// It also wastes space in case of a set because it stores a value pointer inside dictEntry.
// To summarize:
// 100% utilized dict uses N*24 + N*8 = 32N bytes not including the key space.
// for 75% utilization (1/0.75 buckets): N*1.33*8 + N*24 = 35N
//
// This class uses 8 bytes per bucket (similarly to dictEntry*) but it used it for both
// links and keys. For most cases, we remove the need for another redirection layer
// and just store the key, so no "dictEntry" allocations occur.
// For those cells that require chaining, the bucket is
// changed in run-time to represent a linked chain.
// Additional feature - in order to to reduce collisions, we insert items into
// neighbour cells but only if they are empty (not chains). This way we reduce the number of
// empty (unused) spaces at full utilization from 36% to ~21%.
// 100% utilized table requires: N*8 + 0.2N*16 = 11.2N bytes or ~20 bytes savings.
// 75% utilization: N*1.33*8 + 0.12N*16 = 13N or ~22 bytes savings per record.
// with potential replacements of hset/zset data structures.
// static_assert(sizeof(dictEntry) == 24);

class DenseSet {
  struct DenseLinkKey;
  // we can assume that high 12 bits of user address space
  // can be used for tagging. At most 52 bits of address are reserved for
  // some configurations, and usually it's 48 bits.
  // https://docs.kernel.org/arch/arm64/memory.html
  static constexpr size_t kLinkBit = 1ULL << 52;
  static constexpr size_t kDisplaceBit = 1ULL << 53;
  static constexpr size_t kDisplaceDirectionBit = 1ULL << 54;
  static constexpr size_t kTtlBit = 1ULL << 55;
  static constexpr size_t kTagMask = 4095ULL << 52;  // we reserve 12 high bits.

  class DensePtr {
   public:
    explicit DensePtr(void* p = nullptr) : ptr_(p) {
    }

    // Imports the object with its metadata except the link bit that is reset.
    static DensePtr From(DenseLinkKey* o) {
      DensePtr res;
      res.ptr_ = (void*)(o->uptr() & (~kLinkBit));
      return res;
    }

    uint64_t uptr() const {
      return uint64_t(ptr_);
    }

    bool IsObject() const {
      return (uptr() & kLinkBit) == 0;
    }

    bool IsLink() const {
      return (uptr() & kLinkBit) != 0;
    }

    bool HasTtl() const {
      return (uptr() & kTtlBit) != 0;
    }

    bool IsEmpty() const {
      return ptr_ == nullptr;
    }

    void* Raw() const {
      return (void*)(uptr() & ~kTagMask);
    }

    bool IsDisplaced() const {
      return (uptr() & kDisplaceBit) == kDisplaceBit;
    }

    void SetLink(DenseLinkKey* lk) {
      ptr_ = (void*)(uintptr_t(lk) | kLinkBit);
    }

    void SetDisplaced(int direction) {
      ptr_ = (void*)(uptr() | kDisplaceBit);
      if (direction == 1) {
        ptr_ = (void*)(uptr() | kDisplaceDirectionBit);
      }
    }

    void ClearDisplaced() {
      ptr_ = (void*)(uptr() & ~(kDisplaceBit | kDisplaceDirectionBit));
    }

    // returns 1 if the displaced node is right of the correct bucket and -1 if it is left
    int GetDisplacedDirection() const {
      return (uptr() & kDisplaceDirectionBit) == kDisplaceDirectionBit ? 1 : -1;
    }

    void SetTtl(bool b) {
      if (b)
        ptr_ = (void*)(uptr() | kTtlBit);
      else
        ptr_ = (void*)(uptr() & (~kTtlBit));
    }

    void Reset() {
      ptr_ = nullptr;
    }

    void* GetObject() const {
      if (IsObject()) {
        return Raw();
      }

      return AsLink()->Raw();
    }

    // Sets pointer but preserves tagging info
    void SetObject(void* obj) {
      assert(IsObject());
      ptr_ = (void*)((uptr() & kTagMask) | (uintptr_t(obj) & ~kTagMask));
    }

    DenseLinkKey* AsLink() {
      return (DenseLinkKey*)Raw();
    }

    const DenseLinkKey* AsLink() const {
      return (const DenseLinkKey*)Raw();
    }

    DensePtr* Next() {
      if (!IsLink()) {
        return nullptr;
      }

      return &AsLink()->next;
    }

    const DensePtr* Next() const {
      if (!IsLink()) {
        return nullptr;
      }

      return &AsLink()->next;
    }

   private:
    void* ptr_ = nullptr;
  };

  struct DenseLinkKey : public DensePtr {
    DensePtr next;  // could be LinkKey* or Object *.
  };

  static_assert(sizeof(DensePtr) == sizeof(uintptr_t));
  static_assert(sizeof(DenseLinkKey) == 2 * sizeof(uintptr_t));

 protected:
  using DensePtrAllocator = StatelessAllocator<DensePtr>;
  using ChainVectorIterator = std::vector<DensePtr, DensePtrAllocator>::iterator;
  using ChainVectorConstIterator = std::vector<DensePtr, DensePtrAllocator>::const_iterator;

  class IteratorBase {
    friend class DenseSet;

   public:
    IteratorBase(DenseSet* owner, ChainVectorIterator list_it, DensePtr* e)
        : owner_(owner), curr_list_(list_it), curr_entry_(e) {
    }

    // returns the expiry time of the current entry or UINT32_MAX if no ttl is set.
    uint32_t ExpiryTime() const {
      return curr_entry_->HasTtl() ? owner_->ObjExpireTime(curr_entry_->GetObject()) : UINT32_MAX;
    }

    void SetExpiryTime(uint32_t ttl_sec);

    bool HasExpiry() const {
      return curr_entry_->HasTtl();
    }

    // drakeydb: P4-0 Task 2b, fix round 4 -- test-only. Whether the position this iterator is
    // CURRENTLY at is itself tagged as a link (i.e. something else still follows via `.next`),
    // meaning this position is provably NOT the chain's tail. Used by
    // ReaperExpireStepChecksTtlOnEveryChainNode (string_map_test.cc) to deterministically select
    // a field to arm at a non-tail chain position, rather than relying on insertion-order
    // heuristics that turned out not to reliably land there in practice.
    bool DebugCurrIsLink() const {
      return curr_entry_->IsLink();
    }

   protected:
    IteratorBase() : owner_(nullptr), curr_entry_(nullptr) {
    }

    IteratorBase(const DenseSet* owner, bool is_end);

    void Advance();

    DenseSet* owner_;
    ChainVectorIterator curr_list_;
    DensePtr* curr_entry_;
  };

 public:
  static constexpr uint32_t kMaxBatchLen = 32;

  explicit DenseSet();
  virtual ~DenseSet();

  void Clear() {
    ClearStep(0, entries_.size());
  }

  // Returns the next bucket index that should be cleared.
  // Returns BucketCount when all objects are erased.
  uint32_t ClearStep(uint32_t start, uint32_t count);

  // Returns the number of elements in the map. Note that it might be that some of these elements
  // have expired and can't be accessed.
  size_t UpperBoundSize() const {
    return size_;
  }

  // Returns an accurate size, post-expiration. O(n).
  size_t SizeSlow();

  bool Empty() const {
    return size_ == 0;
  }

  size_t BucketCount() const {
    return entries_.size();
  }

  size_t ObjMallocUsed() const {
    return obj_malloc_used_;
  }

  size_t SetMallocUsed() const {
    return entries_.capacity() * sizeof(DensePtr) + num_links_ * sizeof(DenseLinkKey);
  }

  using ItemCb = std::function<void(const void*)>;

  uint32_t Scan(uint32_t cursor, const ItemCb& cb) const;
  void Reserve(size_t sz);

  // Shrinks the table to the specified size. The size must be a power of 2,
  // >= kMinSize, and >= current number of elements.
  // This method should be called explicitly when memory reclamation is needed.
  void Shrink(size_t new_size);

  void Fill(DenseSet* other) const;

  // set an abstract time that allows expiry.
  void set_time(uint32_t val) {
    time_now_ = val;
  }

  uint32_t time_now() const {
    return time_now_;
  }

  bool ExpirationUsed() const {
    return expiration_used_;
  }

  // drakeydb: P4-0 Task 2b Important A/C -- reaper-only. Bounds a member-expiry sweep to at most
  // max_slots RAW entries_ slots per call (entries_[reaper_cursor_..]), resuming from where a
  // previous call left off. Each occupied slot's own direct content is force-expired first via
  // ExpireIfNeeded(nullptr, &entries_[i]) -- the same call IteratorBase::Advance makes for a
  // top-level slot (dense_set.cc). Any attached chain is then walked explicitly, one link at a
  // time, mirroring Advance()'s own link-stepping branch (a tail deletion can collapse `node`
  // out of being a link in place, in which case it must be re-examined rather than stepped past
  // -- see the loop below).
  //
  // HasTtl() is checked at EVERY node this walk visits -- the head slot and every interior link
  // -- not just once at the end. The tag bit is per-node (DensePtr, above): IteratorBase::
  // SetExpiryTime stamps whichever DensePtr the iterator currently sits on, so a live TTL can
  // land on the head, on an interior link, or on the chain's tail, independently. An earlier
  // version of this function checked only the node the walk happened to end on (the terminal
  // element), which silently missed a live TTL sitting anywhere earlier in the chain and could
  // let ReaperClearMemberExpiration() below fire while real, still-armed TTLs remained -- those
  // entries would then be dropped from the next RDB save, since rdb_save.cc picks the plain
  // (non-TTL) encoding once the sticky flag this clears is false. That is why every node is
  // checked here, not just the one the walk finishes on.
  //
  // reaper_any_ttl_seen_ also accumulates writes made by ordinary mutation while a pass is in
  // flight: every call site that arms a TTL (PushFront, AddUnique, AddOrReplaceObj, the iterator
  // SetExpiryTime path -- dense_set.cc) also sets reaper_any_ttl_seen_ = true directly, so a TTL
  // armed on an already-examined slot mid-pass still poisons this pass's "clean" verdict even
  // though the walk itself will never revisit that slot. See those call sites for the mirrored
  // write.
  //
  // Unlike a callback-based walk (container_utils::IterateSet/IterateMap), which only ever sees
  // *surviving* members and therefore cannot bound a container whose members have ALL already
  // expired (nothing would ever call the callback), this counts every slot EXAMINED toward the
  // budget, survivors and skipped-because-already-expired alike.
  //
  // Returns true iff this call completed a full logical pass over the whole table (reaper_cursor_
  // wrapped back to 0) AND no node visited during that pass, nor any mutation that raced with it,
  // ever carried a live TTL -- together, the only condition under which
  // ReaperClearMemberExpiration() below is safe to call. A pass that is truncated (budget ran out
  // before reaching the end) or that found a live-but-not-yet-due TTL always returns false,
  // regardless of how much was expired.
  bool ReaperExpireStep(uint32_t max_slots) {
    if (entries_.empty()) {
      reaper_cursor_ = 0;
      reaper_any_ttl_seen_ = false;
      reaper_pass_capacity_log_ = 0;
      return true;
    }
    // A resumed pass (reaper_cursor_ != 0) treats entries_[reaper_cursor_..] as "not yet
    // examined by this pass". Grow()/Shrink() (table resize) and AddUnique()'s displacement
    // cascade (dense_set.cc) can both relocate an entry across that boundary -- resize always
    // changes capacity_log_ (Grow increments it, Shrink recomputes it -- both always in lockstep
    // with entries_.size()), which this catches directly; AddUnique's same-size displacement is
    // caught separately where it happens (see reaper_cursor_ = 0 there). Either way, bucket
    // indices from before the change no longer mean "already examined" / "not yet examined", so
    // resuming at the old cursor could silently skip an entry that moved below it. Restart the
    // pass from scratch rather than risk that -- correctness over avoiding a re-walk.
    //
    // Known narrow gap, not closed: comparing capacity_log_ (or, equivalently, entries_.size() --
    // they're in lockstep, so neither is more "airtight" than the other here) is a snapshot
    // comparison, not a monotonic one. A SHRINK immediately followed by regrowth back to exactly
    // the prior capacity, both within a single in-flight pass, leaves capacity_log_ reading the
    // same value on the next resumed call despite the table having been rebuilt in between --
    // this check would not fire, and a relocated entry could still be missed. Needs an operator
    // SHRINK plus regrowth inside one heartbeat interval to reach, so narrow. Closing it airtight
    // would need a monotonically-increasing generation counter bumped inside Grow()/Shrink()
    // themselves, which would cost back the sizeof(DenseSet) savings the byte-sized snapshot
    // below was chosen for (a uint32_t generation counter re-grows this class from 64 to 72
    // bytes, the same regression closed above) -- judged not worth it for a gap this narrow.
    if (reaper_cursor_ != 0 && capacity_log_ != reaper_pass_capacity_log_) {
      reaper_cursor_ = 0;
      reaper_any_ttl_seen_ = false;
    }
    reaper_pass_capacity_log_ = static_cast<uint8_t>(capacity_log_);
    size_t end = std::min<size_t>(entries_.size(), size_t{reaper_cursor_} + max_slots);
    for (size_t i = reaper_cursor_; i < end; ++i) {
      DensePtr* node = &entries_[i];
      if (node->IsEmpty())
        continue;
      // Check/expire this slot's own direct content first -- the exact call Advance() makes
      // when it first arrives at a top-level slot (dense_set.cc). kTtlBit/kLinkBit are
      // independent tag bits (DensePtr, above): entries_[i].HasTtl() is only meaningful for
      // entries_[i]'s OWN content, not (necessarily) for a chain it links to, which is why this
      // alone was NOT sufficient -- a slot whose own content survives (no TTL, or a not-yet-due
      // one) but that also links to further, ALREADY-EXPIRED chained entries needs the loop
      // below too, or those entries are silently never examined.
      ExpireIfNeeded(nullptr, node);
      // Walk any attached chain exactly as Advance()'s link-stepping branch does: while
      // positioned on a link, check/expire the NEXT entry in the chain (using the link itself
      // as `prev`, since deleting a tail object can free `prev`'s own LinkKey -- see
      // ExpireIfNeededInternal's "node_in_prev_link" comment, dense_set.cc), then step onto it
      // -- UNLESS that deletion collapsed `node` itself out of being a link (the tail-deletion
      // case just described), in which case `node` now directly holds the surviving value and
      // must be re-examined in place, not stepped past -- exactly what re-testing the while
      // condition on `node` (not on a separately-advanced pointer) does here.
      //
      // HasTtl() is a per-node tag bit (DensePtr, above), settable independently on the head
      // slot OR on any interior `.next` link along the chain (IteratorBase::SetExpiryTime just
      // stamps whatever DensePtr it currently sits on -- dense_set.cc). So the check must happen
      // at EVERY position this loop visits, not only once after the walk exits: checking only
      // the terminal node (as an earlier version of this function did) misses a live TTL on the
      // head or on any interior link, which then gets this pass wrongly counted "clean".
      while (true) {
        if (node->IsEmpty())
          break;
        if (node->HasTtl())
          reaper_any_ttl_seen_ = true;
        if (!node->IsLink())
          break;
        DenseLinkKey* plink = node->AsLink();
        ExpireIfNeeded(node, &plink->next);
        if (node->IsLink())
          node = &plink->next;
        // else: the tail deletion above collapsed `node` in place (it now directly holds the
        // survivor); loop back and re-check it without stepping past it.
      }
    }
    if (end >= entries_.size()) {
      bool clean_pass = !reaper_any_ttl_seen_;
      reaper_cursor_ = 0;
      reaper_any_ttl_seen_ = false;
      reaper_pass_capacity_log_ = 0;
      return clean_pass;
    }
    reaper_cursor_ = static_cast<uint32_t>(end);
    return false;
  }

  // Reaper-only. The caller must have just received `true` from ReaperExpireStep -- a complete,
  // clean pass -- immediately before calling this; see that method's own comment for why a
  // truncated or dirty pass must never reach here (it would strand any not-yet-examined or
  // not-yet-due member TTL permanently, since nothing would ever walk this container again).
  void ReaperClearMemberExpiration() {
    expiration_used_ = false;
  }

 protected:
  // Virtual functions to be implemented for generic data
  virtual uint64_t Hash(const void* obj, uint32_t cookie) const = 0;
  virtual bool ObjEqual(const void* left, const void* right, uint32_t right_cookie) const = 0;
  virtual size_t ObjectAllocSize(const void* obj) const = 0;
  virtual uint32_t ObjExpireTime(const void* obj) const = 0;
  virtual void ObjUpdateExpireTime(const void* obj, uint32_t ttl_sec) = 0;
  virtual void ObjDelete(void* obj) const = 0;
  virtual void* ObjectClone(const void* obj, bool has_ttl, bool add_ttl) const = 0;

  void CollectExpired();

  bool EraseInternal(void* obj, uint32_t cookie) {
    auto [prev, found] = Find(obj, BucketId(obj, cookie), cookie);
    if (found) {
      Delete(prev, found);
      return true;
    }
    return false;
  }

  // Like EraseInternal but returns the detached object instead of deleting it.
  // Returns nullptr if the object was not found.
  void* DetachInternal(void* obj, uint32_t cookie) {
    auto [prev, found] = Find(obj, BucketId(obj, cookie), cookie);
    if (found) {
      return Delete(prev, found, true);
    }
    return nullptr;
  }

  void* FindInternal(const void* obj, uint64_t hashcode, uint32_t cookie) const;

  IteratorBase FindIt(const void* ptr, uint32_t cookie) {
    if (Empty())
      return IteratorBase{};

    auto [bid, _, curr] = Find2(ptr, BucketId(ptr, cookie), cookie);
    if (curr) {
      return IteratorBase(this, entries_.begin() + bid, curr);
    }
    return IteratorBase{};
  }

  // Get iterator to start of random non-empty chain (bucket)
  ChainVectorIterator GetRandomChain();

  // Wrap RandomChain() into iterator and advance with reservoir sampling
  IteratorBase GetRandomIterator();

  void* PopInternal();

  void IncreaseMallocUsed(size_t delta) {
    obj_malloc_used_ += delta;
  }

  void DecreaseMallocUsed(size_t delta) {
    obj_malloc_used_ -= delta;
  }

  // Returns the previous object if it has been replaced.
  // nullptr, if obj was added.
  void* AddOrReplaceObj(void* obj, bool has_ttl);

  // Assumes that the object does not exist in the set.
  void AddUnique(void* obj, bool has_ttl, uint64_t hashcode);

  void Prefetch(uint64_t hash);

 private:
  DenseSet(const DenseSet&) = delete;
  DenseSet& operator=(DenseSet&) = delete;

  bool Equal(DensePtr dptr, const void* ptr, uint32_t cookie) const;

  struct CloneItem {
    DensePtr ptr;
    void* obj = nullptr;
    bool has_ttl = false;
  };

  void CloneBatch(unsigned len, CloneItem* items, DenseSet* other) const;

  using ClearItem = CloneItem;
  void ClearBatch(unsigned len, ClearItem* items);

  uint32_t BucketId(uint64_t hash) const {
    assert(capacity_log_ > 0);
    return hash >> (64 - capacity_log_);
  }

  uint32_t BucketId(const void* ptr, uint32_t cookie) const {
    return BucketId(Hash(ptr, cookie));
  }

  // return a ChainVectorIterator (a.k.a iterator) or end if there is an empty chain found
  ChainVectorIterator FindEmptyAround(uint32_t bid);

  // Return if bucket has no item which is not displaced and right/left bucket has no displaced item
  // belong to given bid
  bool NoItemBelongsBucket(uint32_t bid) const;
  void Grow(size_t prev_size);

  // ============ Pseudo Linked List Functions for interacting with Chains ==================
  size_t PushFront(ChainVectorIterator, void* obj, bool has_ttl);
  void PushFront(ChainVectorIterator, DensePtr);

  DensePtr PopPtrFront(ChainVectorIterator);

  // ============ Pseudo Linked List in DenseSet end ==================

  // returns (prev, item) pair. If item is root, then prev is null.
  std::pair<DensePtr*, DensePtr*> Find(const void* ptr, uint32_t bid, uint32_t cookie) {
    auto [_, p, c] = Find2(ptr, bid, cookie);
    return {p, c};
  }

  // returns bid and (prev, item) pair. If item is root, then prev is null.
  std::tuple<size_t, DensePtr*, DensePtr*> Find2(const void* ptr, uint32_t bid, uint32_t cookie);

  DenseLinkKey* NewLink(void* data, DensePtr next);

  inline void FreeLink(DenseLinkKey* plink) {
    // deallocate the link if it is no longer a link as it is now in an empty list
    DensePtrAllocator::resource()->deallocate(plink, sizeof(DenseLinkKey), alignof(DenseLinkKey));
    --num_links_;
  }

  // Returns true if *node was deleted.
  bool ExpireIfNeeded(DensePtr* prev, DensePtr* node) const {
    if (node->HasTtl()) {
      return ExpireIfNeededInternal(prev, node);
    }
    return false;
  }

  bool ExpireIfNeededInternal(DensePtr* prev, DensePtr* node) const;

  // Deletes the object pointed by ptr and removes it from the set.
  // If ptr is a link then it will be deleted internally.
  // If detach is true, returns the raw object instead of calling ObjDelete.
  void* Delete(DensePtr* prev, DensePtr* ptr, bool detach = false);

  // Processes a single bucket during Shrink, relocating elements as needed.
  void ShrinkBucket(size_t bucket_idx);

  std::vector<DensePtr, DensePtrAllocator> entries_;

  mutable size_t obj_malloc_used_ = 0;
  mutable uint32_t size_ = 0;       // number of elements in the set.
  mutable uint32_t num_links_ = 0;  // number of links in the set.
  unsigned capacity_log_ = 0;

  uint32_t time_now_ = 0;

  // drakeydb: P4-0 Task 2b Important A/C -- reaper-only resume state for ReaperExpireStep. Not
  // used by any other DenseSet caller; persists a bounded walk's progress across heartbeat
  // ticks so a container bigger than one call's budget makes genuine forward progress instead
  // of re-examining the same prefix forever. reaper_any_ttl_seen_ accumulates across the calls
  // that make up one logical pass (reset when a pass starts, i.e. when reaper_cursor_ is 0) and
  // is what ReaperExpireStep uses to decide whether a just-completed pass was clean (safe to
  // clear expiration_used_) or found a still-live member TTL (must not).
  mutable uint32_t reaper_cursor_ = 0;

  mutable bool expiration_used_ = false;
  mutable bool reaper_any_ttl_seen_ = false;
  // drakeydb: P4-0 Task 2b Important B -- reaper-only. capacity_log_ as of the last
  // ReaperExpireStep call that left a pass in flight (reaper_cursor_ != 0), detecting a resize
  // between resumed calls without needing a second full copy of entries_.size() (Grow()/Shrink()
  // always update capacity_log_ in lockstep with it). A uint8_t (not the mutable uint32_t
  // reaper_cursor_ is) so this and the two mutable bools above it fit into tail padding that
  // already existed before this task, rather than growing sizeof(DenseSet). This is a snapshot
  // comparison, not a monotonic one, and has a known narrow gap -- see ReaperExpireStep's own
  // comment for what it does and does not guard against.
  mutable uint8_t reaper_pass_capacity_log_ = 0;
};

inline void* DenseSet::FindInternal(const void* obj, uint64_t hashcode, uint32_t cookie) const {
  if (entries_.empty())
    return nullptr;

  uint32_t bid = BucketId(hashcode);
  DensePtr* ptr = const_cast<DenseSet*>(this)->Find(obj, bid, cookie).second;
  return ptr ? ptr->GetObject() : nullptr;
}

}  // namespace dfly
