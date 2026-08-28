# PR #6 Review Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close PR #6's namespace, loader-epoch, and post-journal allocation correctness gaps,
then address the remaining review comments and documentation inaccuracies.

**Architecture:** Non-default namespaces remain local-only because the journal wire has no
namespace identity. RDB loading finalizes normal database bookkeeping without entering the MVCC
arm/commit protocol. Default-namespace writes create a zero-authority MVCC slot before journaling,
allowing the journal commit to update an existing slot without allocation after propagation.

**Tech Stack:** C++20, Dragonfly shard/fiber runtime, DashTable, GoogleTest, CMake/Ninja,
pre-commit/clang-format.

**Spec:** `docs/superpowers/specs/2026-08-25-phase4-mvcc-lww-design.md`

## Global Constraints

- Preserve the invariant: a key's stamp advances if and only if the resulting value state is
  propagated carrying that stamp.
- Do not add namespace identity to the Phase 3/4 journal wire in this PR.
- Keep `mvcc.cc` independent of `GetCurrentTimeMs()`; callers pass time explicitly.
- Do not edit the `helio/` submodule.
- Do not commit or push without explicit user authorization.

---

### Task 1: Namespace-local journaling boundary

**Files:**
- Modify: `src/server/transaction.cc`
- Modify: `src/server/tx_base.cc`
- Modify: `src/server/debugcmd.cc`
- Modify: `src/server/multi_master_test.cc`

**Interfaces:**
- Consumes: `Transaction::namespace_`, `DbContext::ns`,
  `Namespaces::GetDefaultNamespace()`.
- Produces: one rule for every COMMAND journal path: only the default namespace is serializable;
  `DEBUG MVCC` explicitly refuses unsupported namespaces.

- [ ] **Step 1: Add failing namespace journal tests**

Register a capturing journal consumer on the target shard, execute both an explicitly journaled
`SET` and an auto-journaled `LPUSH` through `RunViaNamespace`, and require zero captured COMMAND
entries. Also require `DEBUG MVCC` through the same namespace to return an explicit unsupported
namespace error.

```cpp
ASSERT_EQ(RunViaNamespace(&ns1, {"set", "explicit", "v"}), "OK");
ASSERT_EQ(RunViaNamespace(&ns1, {"lpush", "automatic", "v"}).GetInt(), 1);
EXPECT_TRUE(consumer.entries.empty());
EXPECT_THAT(RunViaNamespace(&ns1, {"debug", "mvcc", "explicit"}),
            ErrArg("default namespace"));
```

- [ ] **Step 2: Run the focused tests and observe failure**

Run:

```bash
cd build-dbg
ninja multi_master_test
./multi_master_test --gtest_filter='MvccStoreTest.*Namespace*'
```

Expected before the fix: the consumer sees journal entries and `DEBUG MVCC` reports an absent
stamp instead of an error.

- [ ] **Step 3: Gate every namespace-aware journal entry point**

Put the transaction gate in `LogJournalOnShard`, which covers explicit `RecordJournal` calls and
the automatic path:

```cpp
if (namespace_ != &namespaces->GetDefaultNamespace())
  return;
```

Apply the equivalent `DbContext::ns` check before `RecordDelete`, `RecordDerivedDelete`, and
`RecordExpiryBlocking` call `journal::RecordEntry`:

```cpp
if (db_cntx.ns != &namespaces->GetDefaultNamespace())
  return;
```

Reject non-default namespace calls at the start of `DebugCmd::Mvcc`:

```cpp
if (cntx_->ns != &namespaces->GetDefaultNamespace())
  return cmd_cntx->SendError("DEBUG MVCC is supported only in the default namespace");
```

- [ ] **Step 4: Re-run the focused tests**

Run the command from Step 2. Expected: PASS.

### Task 2: Loader-safe updater finalization

**Files:**
- Modify: `src/server/db_slice.h`
- Modify: `src/server/db_slice.cc`
- Modify: `src/server/rdb_load.cc`
- Modify: `src/server/multi_master_test.cc`

**Interfaces:**
- Consumes: `DbSlice::AutoUpdater::Run()`, `DbSlice::PostUpdate`.
- Produces: `void AutoUpdater::RunWithoutMvccArm()` and
  `void PostUpdate(DbIndex, std::string_view, bool arm_mvcc)`.

- [ ] **Step 1: Add a focused pending-arm regression test**

On one shard, create and arm a normal pending key, finalize an RDB-shaped second update through
`RunWithoutMvccArm`, commit the pending arm, and assert that only the pending key receives the
commit stamp while the loaded key stays `{0,0}`.

```cpp
pending.post_updater.Run();
db_slice.SetMvcc(0, loaded_key, MvccStamp{});
loaded.post_updater.RunWithoutMvccArm();
MvccStamper::tlocal()->Commit(kMvcc, 0, [&](DbIndex db, std::string_view key,
                                            const MvccStamp& stamp) {
  db_slice.SetExistingMvcc(db, key, stamp);
});
```

- [ ] **Step 2: Add the named no-arm finalization seam**

Keep the existing `Run()` behavior and share its accounting/invalidation logic:

```cpp
void AutoUpdater::Run() {
  RunInternal(true);
}

void AutoUpdater::RunWithoutMvccArm() {
  RunInternal(false);
}
```

Pass `arm_mvcc` into `PostUpdate` and execute `MvccStamper::Arm` only when it is true.

- [ ] **Step 3: Use the seam for every loaded object**

After sticky/MC-flag and `{0,0}` installation in `CreateObjectOnShard`, finalize the updater once:

```cpp
updater.post_updater.RunWithoutMvccArm();
```

Remove all RDB-loader `EndOfWriteEpoch()` calls and their obsolete race-acceptance comments. The
tiered branch must not run the updater a second time.

- [ ] **Step 4: Run loader regressions**

Run:

```bash
cd build-dbg
ninja multi_master_test
./multi_master_test --gtest_filter='MvccStoreTest.*Reload*:MvccStoreTest.*Loader*'
```

Expected: PASS, including the existing reload-arm-leak coverage and the new pending-arm test.

### Task 3: Allocation-free journal commit

**Files:**
- Modify: `src/server/db_slice.h`
- Modify: `src/server/db_slice.cc`
- Modify: `src/server/journal/journal.cc`
- Modify: `src/server/multi_master_test.cc`

**Interfaces:**
- Produces: `void DbSlice::EnsureMvcc(DbIndex, std::string_view)` and
  `void DbSlice::SetExistingMvcc(DbIndex, std::string_view, const MvccStamp&)`.
- Consumes: Task 2's `PostUpdate(..., bool arm_mvcc)`.

- [ ] **Step 1: Test pre-journal slot preparation**

Finalize a direct default-namespace update before calling `Commit`, verify its side-table slot is
present with `{0,0}`, then commit and verify the same slot changes to the supplied stamp.

```cpp
updated.post_updater.Run();
ASSERT_EQ(db_slice.GetMvcc(0, key), MvccStamp{});
MvccStamper::tlocal()->Commit(kMvcc, 0, [&](DbIndex db, std::string_view armed,
                                            const MvccStamp& stamp) {
  db_slice.SetExistingMvcc(db, armed, stamp);
});
EXPECT_EQ(db_slice.GetMvcc(0, key), MvccStamp{kMvcc, self_hash});
```

- [ ] **Step 2: Prepare a zero-authority slot before arming**

In default-namespace `PostUpdate`, call `EnsureMvcc` before `Arm`. `EnsureMvcc` inserts `{0,0}`
only when absent, increments `mvcc_entries` only for a new entry, and never overwrites an existing
stamp.

```cpp
auto [it, inserted] = db.mvcc->Insert(key, MvccStamp{});
if (inserted)
  ++db.stats.mvcc_entries;
```

- [ ] **Step 3: Make the post-journal operation update-only**

`SetExistingMvcc` uses `Find` and a hard invariant check, then performs only assignment:

```cpp
auto it = db.mvcc->Find(key);
CHECK(!it.is_done()) << "MVCC slot must be prepared before journal commit";
it->second = stamp;
```

Change `journal::RecordEntry` to call `SetExistingMvcc`, remove the `std::bad_alloc` catch and the
`<new>` include, and update the comments to state that allocation happens before `AddLogRecord`.

- [ ] **Step 4: Run stamp and invariant regressions**

Run:

```bash
cd build-dbg
ninja multi_master_test mvcc_test
./multi_master_test --gtest_filter='MvccStoreTest.*Stamp*:MvccStoreTest.*Journal*:MvccStoreTest.*Invariant*'
./mvcc_test
```

Expected: PASS.

### Task 4: Wall-clock rollback backstop

**Files:**
- Modify: `src/server/mvcc.h`
- Modify: `src/server/mvcc.cc`
- Modify: `src/server/mvcc_test.cc`

**Interfaces:**
- Produces: `uint64_t hop_started_ms_`, reset with the epoch and used only to age the hop memo.

- [ ] **Step 1: Add a failing rollback test**

```cpp
const uint64_t first = s->HopStamp(10'000);
const uint64_t after_rollback = s->HopStamp(9'000);
EXPECT_GT(after_rollback, first);
EXPECT_EQ(s->stats().stale_epoch, 1u);
```

- [ ] **Step 2: Age the memo by observation time**

Re-mint if there is no memo, time moved backward, or more than `kMaxEpochMs` elapsed since the
memo was observed:

```cpp
const bool stale = hop_stamp_ != 0 &&
                   (now_ms < hop_started_ms_ || hop_started_ms_ + kMaxEpochMs < now_ms);
if (hop_stamp_ == 0 || stale) {
  if (stale)
    ++stats_.stale_epoch;
  hop_stamp_ = clock_.Next(now_ms);
  hop_started_ms_ = now_ms;
}
```

Reset both fields in `EndOfWriteEpoch` and `TEST_Reset`.

- [ ] **Step 3: Run the MVCC unit suite**

Run:

```bash
cd build-dbg
ninja mvcc_test
./mvcc_test
```

Expected: PASS.

### Task 5: Review nits and documentation accuracy

**Files:**
- Modify: `src/server/multi_master_test.cc`
- Modify: `src/server/server_family.cc`
- Modify: `src/server/debugcmd.cc`
- Modify: `docs/superpowers/specs/2026-08-25-phase4-mvcc-lww-design.md`

**Interfaces:** None.

- [ ] **Step 1: Guard the defrag test's optionals**

```cpp
auto before_stamp = StampOf("k1");
ASSERT_TRUE(before_stamp.has_value());
const MvccStamp before = *before_stamp;
Run({"memory", "defragsegments"});
auto after_stamp = StampOf("k1");
ASSERT_TRUE(after_stamp.has_value());
EXPECT_EQ(*after_stamp, before);
```

- [ ] **Step 2: Correct backlog and scope documentation**

State that the backlog is byte-bounded, while age eviction is incremental and may temporarily
retain entries older than the target after a burst. Amend the P4-1 scope table to acknowledge the
boot-time journal lifecycle/cost change required to stamp peerless active-node writes.

- [ ] **Step 3: Remove comments superseded by the fixes**

Replace claims that namespace writes reach the journal unstamped, that loader epoch clearing is an
accepted limitation, and that `DEBUG MVCC` intentionally returns misleading non-default results.

- [ ] **Step 4: Format and validate changed files**

Run:

```bash
pre-commit run --files \
  src/server/transaction.cc src/server/tx_base.cc src/server/debugcmd.cc \
  src/server/db_slice.h src/server/db_slice.cc src/server/rdb_load.cc \
  src/server/journal/journal.cc src/server/mvcc.h src/server/mvcc.cc \
  src/server/mvcc_test.cc src/server/multi_master_test.cc src/server/server_family.cc \
  docs/superpowers/specs/2026-08-25-phase4-mvcc-lww-design.md \
  docs/superpowers/plans/2026-08-28-pr6-review-fixes.md
git diff --check
```

Expected: all hooks pass and `git diff --check` prints nothing.

### Task 6: Integrated verification and final review

**Files:** None.

**Interfaces:** Consumes all prior tasks.

- [ ] **Step 1: Build the affected targets and main binary**

```bash
cd build-dbg
ninja mvcc_test multi_master_test dragonfly
```

- [ ] **Step 2: Run affected C++ tests**

```bash
cd build-dbg
./mvcc_test
./multi_master_test --gtest_filter='MvccStoreTest.*'
```

- [ ] **Step 3: Review the final diff against the fixed base**

```bash
git diff --check
git diff origin/main...HEAD
git diff
```

Confirm namespace writes cannot enter a namespace-blind journal, RDB loading never mutates a
foreign epoch, no allocation-capable operation follows `AddLogRecord` in MVCC commit, and every
new behavior has non-vacuous regression coverage.
