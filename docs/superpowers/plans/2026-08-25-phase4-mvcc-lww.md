# drakeydb Phase 4 — MVCC Store + Stamping (P4-0, P4-1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give every key in active mode a 16-byte `{mvcc, origin_hash}` stamp that is
bit-identical on the author and on every applier, and close the P3 derived-DEL hole
that currently blocks `--active_replica` from being safe by default.

**Architecture:** A thread-local `MvccStamper` mints one stamp per shard-callback.
`DbSlice::PostUpdate` *arms* the keys a callback touched; `journal::RecordEntry`
*commits* the stamp to every armed key at the moment the journal entry is emitted —
so a key is stamped if and only if that same stamp is propagated. Stamps live in a
side `DashTable<PrimeKey, MvccStamp>` on `DbTable`, allocated only in active mode,
mirroring the `mcflag` precedent. Nothing consumes the stamps yet: this plan builds
the store and proves the bit-identity property. The LWW decisions that read them are
P4-3 and P4-4.

**Tech Stack:** C++20, Dragonfly/helio fibers (`util::fb2`), gtest + `BaseFamilyTest`,
pytest + `df_factory`, CMake/Ninja.

**Spec:** `docs/superpowers/specs/2026-08-25-phase4-mvcc-lww-design.md`

**Scope:** This plan covers **P4-0** (derived-DEL fix + clock-skew metric) and
**P4-1** (the MVCC store, clock, stamping hook, wire, observability, benchmark).
P4-2 through P4-5 — RDB persistence, merge LWW, the streaming guard, tombstones —
get their own plans, written once P4-1's arm/commit contract exists in code.

## Global Constraints

Every task's requirements implicitly include this section.

- **`--active_replica` off must remain byte-identical to upstream** on the journal
  wire. Prove it by extending P3's existing golden-buffer test, never by inspection.
- **`CompactObj` must not grow.** `static_assert(sizeof(CompactObj) == 18)` at
  `src/core/compact_object.cc:613` is load-bearing. Do **not** take the
  `uint8_t unused : 2` bits at `src/core/compact_object.h:648` — the mvcc table is
  dense, so the bit would read 1 on essentially every delete and buy nothing.
- **`src/server/engine_shard.{h,cc}`, `src/core/dash.h`, and `src/core/compact_object.*`
  stay untouched.** Every hook in this plan lives in `db_slice.cc`, `transaction.cc`,
  `journal/journal.cc`, `table.{h,cc}`, or new files.
- **Fiber-safe primitives only** (`util::fb2::Mutex`, `util::fb2::Fiber`). Never
  `std::thread` or `std::mutex`.
- **Code style:** `.clang-format`, Google C++ 2020, 100-char lines. `snake_case`
  variables, `PascalCase` functions, `kPascalCase` constants. Run
  `pre-commit run --files <changed>` before every commit (host venv:
  `~/.venvs/precommit/bin/pre-commit`).
- **Commit message lines <= 100 characters**, subject in the imperative, suffixed
  `(P4)`.
- All new behaviour is **additive and flag-gated** so the upstream merge surface
  stays minimal.

## Build & Test Commands

All builds and tests run **inside the container**; git runs on the **host** (the
worktree's `.git` points outside the container mount).

```bash
# once per session, on the host
orbctl start && docker start drakeydb-p2

# build (container). -j4 is required: 12-way OOM-kills cc1plus in the 8 GB VM.
docker exec drakeydb-p2 sh -c 'cd /src/build-dbg && ninja -j4 dragonfly'
docker exec drakeydb-p2 sh -c 'cd /src/build-dbg && ninja -j4 multi_master_test'

# a single C++ test
docker exec drakeydb-p2 sh -c '/src/build-dbg/multi_master_test --gtest_filter="MvccClockTest.*"'

# full C++ gate -- a COMPLETE ninja must precede it or unbuilt binaries read as "Not Run"
docker exec drakeydb-p2 sh -c 'cd /src/build-dbg && ninja -j4 && ctest -V -L DFLY'

# pytest. DRAGONFLY_PATH must be ABSOLUTE.
docker exec drakeydb-p2 sh -c \
  'cd /src && DRAGONFLY_PATH=/src/build-dbg/dragonfly /tmp/tv/bin/python -m pytest \
   tests/dragonfly/multimaster_test.py -x -q'

# format, on the host
~/.venvs/precommit/bin/pre-commit run --files <changed files>
```

## File Structure

**New files:**

| Path | Responsibility |
|---|---|
| `src/server/mvcc.h` | `MvccStamp` (16-byte POD, ordering), `MvccClock` (per-thread, `ms << 20 \| counter`), `MvccStamper` (arm/commit, hop memo, stats). Header-only where it is hot. |
| `src/server/mvcc.cc` | `MvccStamper::tlocal()`, arm arena management, `NodeUuidHash`. |
| `src/server/mvcc_test.cc` | Pure units: clock monotonicity/overflow/step-back, stamp ordering, arm/commit. No fixture, no shard set. |

**Modified files:**

| Path | Change |
|---|---|
| `src/server/journal/types.h` | `kEntryFlagDerived` constant (P4-0). |
| `src/server/journal/types.cc` | `PassesPeerEchoFilter` suppresses the derived flag (P4-0). |
| `src/server/tx_base.{h,cc}` | `RecordDerivedDelete()` (P4-0); `DbContext::repl_mvcc` (P4-1). |
| `src/server/hset_family.cc`, `src/server/set_family.cc` | Two call sites switch to `RecordDerivedDelete` (P4-0). |
| `src/server/replica.cc`, `src/server/server_family.cc` | Clock-skew comparison, warning, metric (P4-0). |
| `src/server/table.{h,cc}` | `DbTable::mvcc` side table + `mvcc_table_memory()` (P4-1). |
| `src/server/db_slice.{h,cc}` | `mvcc_enabled_`, stamp accessors, `PostUpdate` arm, `PerformDeletionAtomic` disarm/erase, `DeleteReason`, defrag mirror, invariant, stats (P4-1). |
| `src/server/journal/journal.cc` | `RecordEntry` mints + commits (P4-1). |
| `src/server/transaction.{h,cc}` | `EndOfWriteEpoch()` placement, `repl_mvcc_` into `GetDbContext()` (P4-1). |
| `src/server/journal/tx_executor.{h,cc}`, `journal/executor.h`, `replica.{h,cc}` | Inbound mvcc + `peer_origin_hash_` (P4-1). |
| `src/server/debugcmd.cc` | `DEBUG MVCC` (P4-1). |
| `src/server/multi_master.{h,cc}` | `PeerRegistry::GetUuidHash` (P4-1). |
| `src/server/CMakeLists.txt` | `mvcc.cc` into `dragonfly_lib`; `mvcc_test` target. |
| `tests/dragonfly/multimaster_test.py` | P4-0 and P4-1 end-to-end coverage. |
| `tests/dragonfly/multimaster_memory_test.py` | New: the memory benchmark deliverable. |

---

# PR P4-0 — Derived-DEL fix + clock-skew metric

Independent of the MVCC work and separately mergeable to main. It ships first
because a wrongly-forwarded DEL would later create a *persistent* tombstone on a
peer that still holds live data.

---

### Task 1: Suppress derived DELs to peers

**Files:**
- Modify: `src/server/journal/types.h` (flag constant, next to `kEntryFlagExpired`)
- Modify: `src/server/journal/types.cc:51-66` (`PassesPeerEchoFilter`)
- Modify: `src/server/tx_base.h`, `src/server/tx_base.cc:68-75` (new helper)
- Modify: `src/server/hset_family.cc:1650` (call site)
- Modify: `src/server/set_family.cc:1657` (call site)
- Test: `src/server/journal/journal_test.cc`, `src/server/multi_master_test.cc`

**Interfaces:**
- Consumes: `journal::kEntryFlagExpired`, `journal::PassesPeerEchoFilter(const JournalItem&)`,
  `RecordDelete(const DbContext&, std::string_view)` — all from P3.
- Produces: `journal::kEntryFlagDerived` (`uint8_t`, `1 << 1`);
  `void RecordDerivedDelete(const DbContext& db_cntx, std::string_view key)` in
  `namespace dfly` (`tx_base.h`).

**Why this is the whole fix.** `docs/PLAN.md` describes threading a "why is this
empty" signal through ~a dozen call sites. There are 16, but all funnel through two
helpers — `HSetFamily::DeleteIfEmpty` (`hset_family.cc:1637`) and
`SetFamily::DeleteSetIfEmpty` (`set_family.cc:1644`) — and **every** derived DEL
from them should be suppressed to peers:

- *Command-caused* (`SREM s lastmember`, `HDEL`): the causing command is itself
  journaled and propagates. The peer applies it, its own collection empties, and its
  own helper derives the same DEL locally. Forwarding ours is redundant.
- *Expiry-caused* (`HTTL` on a lazily-expired hash — `hset_family.cc:996`, a **read**
  path; the search doc-accessor cleanup at `doc_accessors.cc:212-220`): the peer
  expires on its own clock, which is the semantic `kEntryFlagExpired` already encodes.
- *Peer-applied* commands already carry peer origin and are filtered today.

**Step 1 of this task is to falsify that argument before relying on it.** If a case
is found where the causing command does not propagate and the peer cannot derive the
DEL itself, stop and fall back to `docs/PLAN.md`'s per-call-site signal.

- [ ] **Step 1: Enumerate the call sites and check the invariant holds**

Run, and read every hit:

```bash
grep -rn "DeleteIfEmpty\|DeleteSetIfEmpty" src/server/ --include=*.cc --include=*.h
```

For each of the 16 call sites, write one line in the SDD ledger answering: *does the
command that reached this site itself get journaled and propagate?* Expected answer
for all of them: yes (a write command), or no-but-the-peer-derives-it-independently
(a read path triggering lazy expiry). `debugcmd.cc:1328,1330` are DEBUG-only and
local. **If any site answers "no, and the peer cannot derive it", stop and escalate.**

- [ ] **Step 2: Write the failing filter unit test**

In `src/server/journal/journal_test.cc`, beside the existing `PassesPeerEchoFilterTest`
cases:

```cpp
TEST(PassesPeerEchoFilterTest, DerivedDeleteIsSuppressedToPeers) {
  JournalItem item;
  item.opcode = journal::Op::COMMAND;
  item.origin_idx = PeerRegistry::kSelfIdx;  // self-originated, so origin alone passes it
  item.entry_flags = journal::kEntryFlagDerived;
  EXPECT_FALSE(journal::PassesPeerEchoFilter(item))
      << "a derived DEL must never reach a peer -- the peer derives its own";
}

TEST(PassesPeerEchoFilterTest, DerivedAndExpiredAreDistinctFlags) {
  EXPECT_NE(journal::kEntryFlagDerived, journal::kEntryFlagExpired);
  JournalItem item;
  item.opcode = journal::Op::COMMAND;
  item.origin_idx = PeerRegistry::kSelfIdx;
  item.entry_flags = journal::kEntryFlagExpired | journal::kEntryFlagDerived;
  EXPECT_FALSE(journal::PassesPeerEchoFilter(item));
}
```

- [ ] **Step 3: Run it to verify it fails**

```bash
docker exec drakeydb-p2 sh -c 'cd /src/build-dbg && ninja -j4 journal_test'
```

Expected: **compile error**, `kEntryFlagDerived` is not a member of `journal`.

- [ ] **Step 4: Add the flag and the filter clause**

In `src/server/journal/types.h`, beside `kEntryFlagExpired`:

```cpp
// drakeydb: P4-0 -- a DEL derived from a collection becoming empty, rather than issued
// directly. Never forwarded to a peer: a command-caused empty propagates via the causing
// command (the peer derives its own DEL), and an expiry-caused empty is the peer's own
// clock's business. Distinct from kEntryFlagExpired so diagnostics can tell them apart.
constexpr uint8_t kEntryFlagDerived = 1 << 1;
```

In `src/server/journal/types.cc`, inside `PassesPeerEchoFilter`, beside the existing
expiry clause:

```cpp
  if (item.entry_flags & kEntryFlagDerived)
    return false;
```

- [ ] **Step 5: Run the filter test to verify it passes**

```bash
docker exec drakeydb-p2 sh -c 'cd /src/build-dbg && ninja -j4 journal_test && \
  /src/build-dbg/journal_test --gtest_filter="PassesPeerEchoFilterTest.*"'
```

Expected: PASS, including the pre-existing cases.

- [ ] **Step 6: Write the failing family test that the helpers set the flag**

In `src/server/multi_master_test.cc`, in the `OriginJournalFamilyTest` fixture (it
already captures emitted journal entries — reuse its consumer verbatim):

```cpp
TEST_F(OriginJournalFamilyTest, EmptiedCollectionDeleteCarriesDerivedFlag) {
  Run({"sadd", "s", "a"});
  auto before = consumer_.entries().size();
  Run({"srem", "s", "a"});  // empties the set -> SetFamily::DeleteSetIfEmpty derives a DEL

  // The SREM itself, then the derived DEL.
  ASSERT_GT(consumer_.entries().size(), before + 1);
  const auto& del = consumer_.entries().back();
  EXPECT_EQ(del.opcode, journal::Op::COMMAND);
  EXPECT_TRUE(del.entry_flags & journal::kEntryFlagDerived)
      << "derived DEL must be flagged or the peer filter forwards it";
  EXPECT_FALSE(del.entry_flags & journal::kEntryFlagExpired)
      << "an SREM-caused empty is not an expiry";
}

TEST_F(OriginJournalFamilyTest, UserIssuedDeleteIsNotFlaggedDerived) {
  Run({"set", "k", "v"});
  Run({"del", "k"});
  const auto& del = consumer_.entries().back();
  EXPECT_FALSE(del.entry_flags & journal::kEntryFlagDerived)
      << "a client DEL must still reach peers";
}
```

- [ ] **Step 7: Run it to verify it fails**

```bash
docker exec drakeydb-p2 sh -c 'cd /src/build-dbg && ninja -j4 multi_master_test && \
  /src/build-dbg/multi_master_test --gtest_filter="OriginJournalFamilyTest.*Derived*"'
```

Expected: FAIL on `EmptiedCollectionDeleteCarriesDerivedFlag` —
`entry_flags & kEntryFlagDerived` is 0.

- [ ] **Step 8: Add `RecordDerivedDelete` and switch the two call sites**

In `src/server/tx_base.h`, beside the existing `RecordDelete` declarations:

```cpp
// drakeydb: P4-0 -- records a DEL derived from a collection becoming empty. Identical to
// RecordDelete(const DbContext&, ...) except it sets journal::kEntryFlagDerived, which
// journal::PassesPeerEchoFilter uses to keep the entry off peer links. Plain (full-stream)
// replicas still receive it, exactly as they do today.
void RecordDerivedDelete(const DbContext& db_cntx, std::string_view key);
```

In `src/server/tx_base.cc`, beside `RecordDelete(const DbContext&, ...)`:

```cpp
void RecordDerivedDelete(const DbContext& db_cntx, string_view key) {
  journal::RecordEntry(0, journal::Op::COMMAND, db_cntx.db_index, KeySlot(key),
                       Payload("DEL", ArgSlice{key}), db_cntx.repl_origin_idx,
                       /* mvcc= */ 0, journal::kEntryFlagDerived);
}
```

In `src/server/hset_family.cc:1650` and `src/server/set_family.cc:1657`, replace
`RecordDelete(db_cntx, key);` with `RecordDerivedDelete(db_cntx, key);` and update the
adjacent P3 comment to name the new flag.

- [ ] **Step 9: Run both tests to verify they pass**

```bash
docker exec drakeydb-p2 sh -c 'cd /src/build-dbg && ninja -j4 multi_master_test journal_test && \
  /src/build-dbg/multi_master_test --gtest_filter="OriginJournalFamilyTest.*" && \
  /src/build-dbg/journal_test --gtest_filter="PassesPeerEchoFilterTest.*"'
```

Expected: PASS.

- [ ] **Step 10: Falsify, per D9**

Revert only the `RecordDerivedDelete` switch at `set_family.cc:1657` (leave the flag
and the filter clause in place), rebuild, and re-run
`OriginJournalFamilyTest.EmptiedCollectionDeleteCarriesDerivedFlag`. **Record the
observed failure text in the SDD ledger**, then restore. Expected text:

```
Value of: del.entry_flags & journal::kEntryFlagDerived
  Actual: false
Expected: true
derived DEL must be flagged or the peer filter forwards it
```

- [ ] **Step 11: Add the pytest differential**

In `tests/dragonfly/multimaster_test.py`. A convergence assertion cannot see this
bug — both nodes converge either way — so assert the **differential** between a peer
link and a plain replica, which is what actually changed:

```python
async def test_derived_delete_reaches_plain_replica_but_not_peer(df_factory):
    """A collection emptied by SREM derives a DEL. A plain (full-stream) replica must
    still see it; a mesh peer must not, because the peer derives its own from the SREM.

    Falsified by reverting RecordDerivedDelete in set_family.cc: the peer's command
    counter then shows the extra forwarded DEL."""
    a = df_factory.create(**active_args())
    b = df_factory.create(**active_args())
    plain = df_factory.create()
    df_factory.start_all([a, b, plain])
    c_a, c_b, c_plain = a.client(), b.client(), plain.client()

    attach(c_b, a)                                  # b <- a, peer link
    await c_plain.execute_command("replicaof", "localhost", a.port)
    await wait_for_peers(c_b, 1)
    await wait_available_async(c_plain)

    await c_a.execute_command("sadd", "s", "m")
    await assert_eventually(lambda: _exists(c_b, "s") and _exists(c_plain, "s"))

    before_b = await _total_commands(c_b)
    await c_a.execute_command("srem", "s", "m")     # empties -> derived DEL on a

    # Both end up without the key: b derives its own DEL from the replicated SREM.
    await assert_eventually(lambda: not _exists(c_b, "s") and not _exists(c_plain, "s"))

    # The load-bearing assertion: b applied the SREM and nothing else. A forwarded
    # derived DEL would show up here as an extra command.
    await asyncio.sleep(1.0)
    delta = await _total_commands(c_b) - before_b
    assert delta <= STORM_BOUND, f"peer applied {delta} commands, expected <= {STORM_BOUND}"
```

Add the two module-level helpers beside the existing ones if they do not exist:

```python
async def _exists(client, key) -> bool:
    return bool(await client.execute_command("exists", key))

async def _total_commands(client) -> int:
    info = await client.info("stats")
    return int(info["total_commands_processed"])
```

- [ ] **Step 12: Run the pytest**

```bash
docker exec drakeydb-p2 sh -c \
  'cd /src && DRAGONFLY_PATH=/src/build-dbg/dragonfly /tmp/tv/bin/python -m pytest \
   tests/dragonfly/multimaster_test.py::test_derived_delete_reaches_plain_replica_but_not_peer -x -q'
```

Expected: PASS. Run it **10 times** — it has timing-sensitive waits — and record the
pass rate in the ledger.

- [ ] **Step 13: Format and commit**

```bash
~/.venvs/precommit/bin/pre-commit run --files \
  src/server/journal/types.h src/server/journal/types.cc src/server/journal/journal_test.cc \
  src/server/tx_base.h src/server/tx_base.cc src/server/hset_family.cc \
  src/server/set_family.cc src/server/multi_master_test.cc tests/dragonfly/multimaster_test.py
git add -A && git commit -m "fix: keep derived collection-empty DELs off peer links (P4)"
```

---

### Task 2: Clock-skew warning and metric

**Files:**
- Modify: `src/server/replica.cc` (in `Greet()`, after the UUID reply is parsed)
- Modify: `src/server/multi_master.cc` (`RenderPeerReplicationInfo`)
- Modify: `src/server/replica_types.h` (`ReplicaSummary`)
- Test: `src/server/multi_master_test.cc`, `tests/dragonfly/multimaster_test.py`

**Interfaces:**
- Consumes: `MasterContext::master_clock_ms` — populated by P1's
  `REPLCONF UUID` exchange (`+<uuid> <ms>`) and, per `docs/PLAN.md`, **read nowhere
  else today**. `ParseReplconfUuidReply` in `node_identity.h`.
- Produces: `int64_t ReplicaSummary::clock_skew_ms`; INFO field
  `clock_skew_ms=<n>` appended to each `masterN:` peer line of the active-mode
  replication block.

**Why now.** LWW quality rests entirely on NTP, and D3's `max(tick, stored + 1)`
repair deliberately fabricates future timestamps under skew (bounded by the skew, and
strictly better than the write vanishing). That trade is only acceptable if the skew
is *visible*. This task makes the phase's central assumption observable before
anything depends on it.

- [ ] **Step 1: Write the failing unit test for the skew computation**

The comparison is a pure function, so test it as one. In
`src/server/multi_master_test.cc`:

```cpp
TEST(ClockSkew, ComputesSignedSkewAndThreshold) {
  // Peer's clock ahead of ours -> positive skew.
  EXPECT_EQ(ComputeClockSkewMs(/* local_ms= */ 1'000, /* peer_ms= */ 1'500), 500);
  // Peer behind -> negative.
  EXPECT_EQ(ComputeClockSkewMs(/* local_ms= */ 1'500, /* peer_ms= */ 1'000), -500);
  // Absent peer clock (a pre-exchange master) -> no skew, never a warning.
  EXPECT_EQ(ComputeClockSkewMs(/* local_ms= */ 1'500, /* peer_ms= */ 0), 0);

  EXPECT_FALSE(IsClockSkewConcerning(0));
  EXPECT_FALSE(IsClockSkewConcerning(-kClockSkewWarnMs + 1));
  EXPECT_TRUE(IsClockSkewConcerning(kClockSkewWarnMs));
  EXPECT_TRUE(IsClockSkewConcerning(-kClockSkewWarnMs))
      << "skew is concerning in both directions";
}
```

- [ ] **Step 2: Run it to verify it fails**

```bash
docker exec drakeydb-p2 sh -c 'cd /src/build-dbg && ninja -j4 multi_master_test'
```

Expected: **compile error**, `ComputeClockSkewMs` not declared.

- [ ] **Step 3: Implement the pure helpers**

In `src/server/multi_master.h`:

```cpp
// drakeydb: P4-0 -- LWW quality depends entirely on synchronised clocks, and P4's local-stamp
// repair (max(tick, stored + 1)) deliberately fabricates timestamps up to the skew ahead of
// true time. Both are only acceptable if the skew is observable.
constexpr int64_t kClockSkewWarnMs = 250;

// Signed difference peer - local, in ms. Returns 0 when peer_clock_ms is 0, which is how a
// pre-exchange master (or a plain Redis) reports "no clock" -- never treat that as skew.
int64_t ComputeClockSkewMs(int64_t local_clock_ms, int64_t peer_clock_ms);

bool IsClockSkewConcerning(int64_t skew_ms);
```

In `src/server/multi_master.cc`:

```cpp
int64_t ComputeClockSkewMs(int64_t local_clock_ms, int64_t peer_clock_ms) {
  if (peer_clock_ms == 0)
    return 0;
  return peer_clock_ms - local_clock_ms;
}

bool IsClockSkewConcerning(int64_t skew_ms) {
  return skew_ms >= kClockSkewWarnMs || skew_ms <= -kClockSkewWarnMs;
}
```

- [ ] **Step 4: Run the unit test to verify it passes**

```bash
docker exec drakeydb-p2 sh -c 'cd /src/build-dbg && ninja -j4 multi_master_test && \
  /src/build-dbg/multi_master_test --gtest_filter="ClockSkew.*"'
```

Expected: PASS.

- [ ] **Step 5: Wire it into the handshake**

In `src/server/replica.cc`, in `Greet()` immediately after `master_context_.master_clock_ms`
is assigned from `ParseReplconfUuidReply` (search for `master_clock_ms` — P1 sets it and
nothing reads it):

```cpp
  // drakeydb: P4-0 -- the handshake clock echo is the only cross-node clock sample we get.
  const int64_t skew_ms =
      ComputeClockSkewMs(GetCurrentTimeMs(), master_context_.master_clock_ms);
  clock_skew_ms_.store(skew_ms, std::memory_order_relaxed);
  LOG_IF(WARNING, IsClockSkewConcerning(skew_ms))
      << "Peer " << master_context_.master_node_uuid << " clock differs by " << skew_ms
      << " ms (threshold " << kClockSkewWarnMs
      << " ms). Last-write-wins resolution degrades with clock skew; check NTP.";
```

Add `std::atomic<int64_t> clock_skew_ms_{0};` to `Replica`'s members in `replica.h`
(it is read from the INFO fiber, written from the replication fiber), surface it from
the existing summary accessor into `ReplicaSummary::clock_skew_ms`
(`replica_types.h`), and render it in `RenderPeerReplicationInfo`
(`multi_master.cc`) as a `,clock_skew_ms=<n>` pair appended to each `masterN:` line,
exactly as `node_uuid` already is at `multi_master.cc:96-97`. Those per-peer lines are a
single comma-separated k=v value, NOT one INFO field per attribute; redis-py parses the csv
into a nested dict, which is why the pytest below reads `info["master0"]["clock_skew_ms"]`
(see `tests/dragonfly/multimaster_test.py:106` for the same idiom on `node_uuid`).

- [ ] **Step 6: Build and run the whole multi_master suite**

```bash
docker exec drakeydb-p2 sh -c 'cd /src/build-dbg && ninja -j4 multi_master_test dragonfly && \
  /src/build-dbg/multi_master_test'
```

Expected: all pre-existing cases still PASS, plus the new `ClockSkew.*`.

- [ ] **Step 7: Add the pytest that the field appears per peer**

```python
async def test_peer_clock_skew_reported(df_factory):
    """Both nodes run on one host, so real skew is ~0; the assertion is that the field
    exists and is small. Falsified by removing the RenderPeerReplicationInfo line --
    the KeyError names the missing field."""
    a = df_factory.create(**active_args())
    b = df_factory.create(**active_args())
    df_factory.start_all([a, b])
    c_a, c_b = a.client(), b.client()
    attach(c_b, a)
    await wait_for_peers(c_b, 1)

    info = await c_b.info("replication")
    assert "clock_skew_ms" in info["master0"], sorted(info["master0"])
    assert abs(int(info["master0"]["clock_skew_ms"])) < 250
```

- [ ] **Step 8: Run it**

```bash
docker exec drakeydb-p2 sh -c \
  'cd /src && DRAGONFLY_PATH=/src/build-dbg/dragonfly /tmp/tv/bin/python -m pytest \
   tests/dragonfly/multimaster_test.py::test_peer_clock_skew_reported -x -q'
```

Expected: PASS.

- [ ] **Step 9: Falsify, per D9**

Delete the `clock_skew_ms=` append from `RenderPeerReplicationInfo`, rebuild, and
re-run the pytest. Record the observed failure (a `KeyError`/assertion naming the
missing field) in the ledger, then restore.

- [ ] **Step 10: Full P4-0 gate and commit**

```bash
docker exec drakeydb-p2 sh -c 'cd /src/build-dbg && ninja -j4 && ctest -V -L DFLY'
docker exec drakeydb-p2 sh -c \
  'cd /src && DRAGONFLY_PATH=/src/build-dbg/dragonfly /tmp/tv/bin/python -m pytest \
   tests/dragonfly/multimaster_test.py tests/dragonfly/replication_test.py -q'
~/.venvs/precommit/bin/pre-commit run --files <changed files>
git add -A && git commit -m "feat: warn and report on peer clock skew (P4)"
```

Expected: `ctest` 87/87 plus the new cases; `multimaster_test.py` at its P3 baseline
of 41 plus the two added here; `replication_test.py` 43 passed.

**P4-0 is now complete and independently mergeable.** Open it as its own PR before
starting Task 3.

---

# PR P4-1 — MVCC store, clock, and stamping

The foundation. When this lands, every key in active mode carries a stamp that is
bit-identical on the author and on every applier — but **nothing reads it yet**. The
LWW decisions are P4-3 and P4-4.

Build order is constrained: Task 5 (side table) before anything that stores; Task 6
(`repl_mvcc`) and Task 7 (stamping) before Task 8 (deletes).

---

### Task 3: `MvccStamp` and `MvccClock`

**Files:**
- Create: `src/server/mvcc.h`, `src/server/mvcc.cc`
- Create: `src/server/mvcc_test.cc`
- Modify: `src/server/CMakeLists.txt`

**Interfaces:**
- Consumes: `GetCurrentTimeMs()` from `src/server/engine_shard_set.h:155` —
  `absl::GetCurrentTimeNanos()/1e6`, and it honours `TEST_current_time_ms`, which is
  what makes every clock test here deterministic with no fake-clock injection.
- Produces: `struct MvccStamp {uint64_t packed; uint64_t origin_hash;}` with
  `Mvcc()`, `IsTombstone()`, `MsPart()`, `operator<`, `operator==`;
  `class MvccClock` with `uint64_t Next(uint64_t now_ms)`, `uint64_t last() const`,
  `uint64_t AheadMs(uint64_t now_ms) const`, `void TEST_Set(uint64_t)`;
  `uint64_t NodeUuidHash(std::string_view uuid)`;
  constants `MvccClock::kCounterBits = 20`, `kCounterMask`, `kTombstoneBit`, `kStampMask`.

Pure value types and arithmetic. No storage, no wiring, no `DbSlice`.

- [ ] **Step 1: Write the failing tests**

Create `src/server/mvcc_test.cc`:

```cpp
// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/mvcc.h"

#include <gmock/gmock.h>

#include "base/gtest.h"

namespace dfly {

using namespace std;

TEST(MvccClockTest, MsShiftMatchesKeyDbLayout) {
  MvccClock clock;
  const uint64_t s = clock.Next(1'000);
  EXPECT_EQ(s, uint64_t(1'000) << MvccClock::kCounterBits);
  EXPECT_EQ(s >> MvccClock::kCounterBits, 1'000u);
  EXPECT_EQ(s & MvccClock::kCounterMask, 0u);
}

TEST(MvccClockTest, MonotonicWithinSameMs) {
  MvccClock clock;
  uint64_t prev = clock.Next(5'000);
  for (int i = 0; i < 1'000; ++i) {
    const uint64_t cur = clock.Next(5'000);
    EXPECT_GT(cur, prev) << "stamps must be strictly increasing at iteration " << i;
    prev = cur;
  }
  EXPECT_EQ(prev >> MvccClock::kCounterBits, 5'000u) << "1000 ticks must not overflow the ms field";
}

// The NTP test: a backwards wall-clock step must never produce a stamp we already issued.
TEST(MvccClockTest, NeverGoesBackwardsOnClockStep) {
  MvccClock clock;
  const uint64_t base = clock.Next(1'000);
  uint64_t prev = base;
  for (int i = 0; i < 10; ++i) {
    const uint64_t cur = clock.Next(900);  // wall clock jumped back 100 ms
    EXPECT_GT(cur, prev) << "iteration " << i;
    EXPECT_GT(cur, base);
    prev = cur;
  }
}

TEST(MvccClockTest, CounterOverflowCarriesIntoMs) {
  MvccClock clock;
  const uint64_t ms = 7'000;
  clock.TEST_Set((ms << MvccClock::kCounterBits) | MvccClock::kCounterMask);
  const uint64_t next = clock.Next(ms);
  EXPECT_EQ(next, (ms + 1) << MvccClock::kCounterBits)
      << "counter exhaustion must carry into the ms field, not wrap";
}

TEST(MvccClockTest, AheadMsReportsSkew) {
  MvccClock clock;
  clock.Next(2'000);
  EXPECT_EQ(clock.AheadMs(2'000), 0u);
  EXPECT_EQ(clock.AheadMs(1'000), 1'000u) << "clock 1s ahead of a retarded wall clock";
  EXPECT_EQ(clock.AheadMs(3'000), 0u) << "never reports negative";
}

TEST(MvccStampTest, SizeAndAlignment) {
  static_assert(sizeof(MvccStamp) == 16, "the side table's per-slot cost depends on this");
  static_assert(alignof(MvccStamp) == 8);
  EXPECT_EQ(sizeof(MvccStamp), 16u);
}

TEST(MvccStampTest, LexicographicOrderOnMvccThenOrigin) {
  const MvccStamp a{100, 5};
  const MvccStamp b{100, 9};
  const MvccStamp c{101, 1};
  EXPECT_LT(a, b) << "equal mvcc must break the tie on origin_hash";
  EXPECT_LT(b, c) << "mvcc dominates origin_hash";
  EXPECT_FALSE(b < a);
  EXPECT_FALSE(a < a) << "irreflexive";
}

// The tombstone marker rides bit 63 and must not perturb the ordering decision 1 specifies.
TEST(MvccStampTest, TombstoneBitIsMaskedFromComparison) {
  const MvccStamp value{100, 5};
  const MvccStamp tombstone{100 | MvccClock::kTombstoneBit, 5};

  EXPECT_TRUE(tombstone.IsTombstone());
  EXPECT_FALSE(value.IsTombstone());
  EXPECT_EQ(tombstone.Mvcc(), value.Mvcc());
  EXPECT_FALSE(value < tombstone) << "the marker must not make a tombstone win";
  EXPECT_FALSE(tombstone < value) << "...nor lose";
}

TEST(MvccStampTest, MsPartIgnoresTombstoneBit) {
  const MvccStamp t{(uint64_t(9'999) << MvccClock::kCounterBits) | MvccClock::kTombstoneBit, 0};
  EXPECT_EQ(t.MsPart(), 9'999u);
}

TEST(NodeUuidHashTest, StableAndDistinct) {
  const string a = "6f1c4c3e-0000-4000-8000-000000000001";
  const string b = "6f1c4c3e-0000-4000-8000-000000000002";
  EXPECT_EQ(NodeUuidHash(a), NodeUuidHash(a)) << "must be stable -- it is persisted and compared "
                                                 "against values written by other nodes";
  EXPECT_NE(NodeUuidHash(a), NodeUuidHash(b));
  EXPECT_NE(NodeUuidHash(a), 0u) << "0 is reserved for 'no origin'";
}

}  // namespace dfly
```

- [ ] **Step 2: Run to verify it fails**

Register the target first — in `src/server/CMakeLists.txt`, add `mvcc.cc` to the
`dragonfly_lib` source list (alphabetically, beside `multi_master.cc`), and add
beside the other test registrations:

```cmake
helio_cxx_test(mvcc_test dfly_test_lib LABELS DFLY)
```

Also add `mvcc_test` to the `add_dependencies(check_dfly ...)` list. Then:

```bash
docker exec drakeydb-p2 sh -c 'cd /src/build-dbg && cmake . >/dev/null && ninja -j4 mvcc_test'
```

Expected: **fatal error**, `server/mvcc.h: No such file or directory`.

- [ ] **Step 3: Write `mvcc.h`**

```cpp
// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <cstdint>
#include <string_view>
#include <tuple>

namespace dfly {

// drakeydb: Phase 4. A per-shard logical clock in KeyDB's layout (ms << 20 | counter,
// KeyDB/src/server.cpp:7263-7287) so a KeyDB-sourced mvcc-tstamp compares meaningfully against a
// locally minted one during P7 onboarding.
//
// Unlike KeyDB's process-global atomic, this is a plain member of a thread-local stamper: KeyDB
// serialises every write in the process on one cache line, which a shared-nothing server must not.
class MvccClock {
 public:
  static constexpr unsigned kCounterBits = 20;
  static constexpr uint64_t kCounterMask = (1ULL << kCounterBits) - 1;

  // Bit 63 marks a tombstone. Safe indefinitely: ms << 20 with today's ms (~1.77e12, 2^40.7)
  // reaches bit 60, and bit 61 is not reached until ~2079.
  static constexpr uint64_t kTombstoneBit = 1ULL << 63;
  static constexpr uint64_t kStampMask = ~kTombstoneBit;

  // Strictly increasing, always -- including across a backwards NTP step, where it advances one
  // counter tick per call until the wall clock catches up. Counter exhaustion carries into the ms
  // field exactly as KeyDB's fetch_add(1) does.
  uint64_t Next(uint64_t now_ms) {
    const uint64_t cand = now_ms << kCounterBits;
    last_ = (cand > last_) ? cand : last_ + 1;
    return last_;
  }

  uint64_t last() const {
    return last_;
  }

  // Milliseconds this clock currently runs ahead of the wall clock; 0 when in step. Non-zero
  // means this node wins every LWW conflict for that long -- see INFO mvcc_clock_ahead_ms.
  uint64_t AheadMs(uint64_t now_ms) const {
    const uint64_t ms = last_ >> kCounterBits;
    return ms > now_ms ? ms - now_ms : 0;
  }

  void TEST_Set(uint64_t v) {
    last_ = v;
  }

 private:
  uint64_t last_ = 0;
};

// Per-key version. 16 bytes; ordered lexicographically on (mvcc, origin_hash).
//
// origin_hash is what makes the order *total*, and that is load-bearing: KeyDB's merge lets an
// incoming write win an exact tie (KeyDB/src/db.cpp:384), so in a mesh A adopts B's value while B
// adopts A's and they swap permanently. Ties are common rather than rare -- under light load the
// 20-bit counter sits at 0, so two nodes writing in the same millisecond collide.
struct MvccStamp {
  uint64_t packed = 0;       // (tombstone << 63) | (ms << 20) | counter
  uint64_t origin_hash = 0;  // NodeUuidHash of the AUTHOR node's uuid, never the sender's

  uint64_t Mvcc() const {
    return packed & MvccClock::kStampMask;
  }
  bool IsTombstone() const {
    return (packed & MvccClock::kTombstoneBit) != 0;
  }
  uint64_t MsPart() const {
    return Mvcc() >> MvccClock::kCounterBits;
  }
  bool Empty() const {
    return packed == 0 && origin_hash == 0;
  }

  friend bool operator<(const MvccStamp& a, const MvccStamp& b) {
    return std::tie(a.Mvcc(), a.origin_hash) < std::tie(b.Mvcc(), b.origin_hash);
  }
  friend bool operator==(const MvccStamp& a, const MvccStamp& b) {
    return a.packed == b.packed && a.origin_hash == b.origin_hash;
  }
};

static_assert(sizeof(MvccStamp) == 16, "side-table per-slot cost is computed from this");

// Stable across processes, builds and architectures -- the hash is persisted in the RDB and
// compared against values written by other nodes, so std::hash is unusable here.
uint64_t NodeUuidHash(std::string_view uuid);

}  // namespace dfly
```

- [ ] **Step 4: Write `mvcc.cc`**

```cpp
// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/mvcc.h"

#include <xxhash.h>

namespace dfly {

namespace {
// Distinct from LockTag::Fingerprint's seed (tx_base.cc:94) so the two hash spaces cannot be
// confused in a debugger or a log.
constexpr uint64_t kOriginHashSeed = 0x9E3779B97F4A7C15ULL;
}  // namespace

uint64_t NodeUuidHash(std::string_view uuid) {
  return XXH64(uuid.data(), uuid.size(), kOriginHashSeed);
}

}  // namespace dfly
```

- [ ] **Step 5: Run the tests to verify they pass**

```bash
docker exec drakeydb-p2 sh -c 'cd /src/build-dbg && ninja -j4 mvcc_test && /src/build-dbg/mvcc_test'
```

Expected: PASS, 10 cases.

- [ ] **Step 6: Falsify the two load-bearing claims, per D9**

1. In `Next`, change `(cand > last_) ? cand : last_ + 1` to plain `cand`. Rebuild, run
   `MvccClockTest.NeverGoesBackwardsOnClockStep`. Record the observed failure, restore.
2. In `operator<`, use `a.packed` instead of `a.Mvcc()`. Rebuild, run
   `MvccStampTest.TombstoneBitIsMaskedFromComparison`. Record the observed failure
   (`the marker must not make a tombstone win`), restore.

- [ ] **Step 7: Format and commit**

```bash
~/.venvs/precommit/bin/pre-commit run --files src/server/mvcc.h src/server/mvcc.cc \
  src/server/mvcc_test.cc src/server/CMakeLists.txt
git add -A && git commit -m "feat: add MvccClock and MvccStamp value types (P4)"
```

---

### Task 4: `MvccStamper` — arm, commit, and the hop memo

**Files:**
- Modify: `src/server/mvcc.h`, `src/server/mvcc.cc`
- Modify: `src/server/mvcc_test.cc`

**Interfaces:**
- Consumes: Task 3's `MvccClock`, `MvccStamp`, `NodeUuidHash`.
- Produces: `class MvccStamper` with `static MvccStamper* tlocal()`,
  `uint64_t HopStamp()`, `void Arm(DbIndex, std::string_view)`,
  `void Disarm(DbIndex, std::string_view)`,
  `void Commit(uint64_t mvcc, uint32_t origin_idx, const CommitFn&)`,
  `void EndOfWriteEpoch()`, `uint64_t OriginHash(uint32_t origin_idx)`,
  `void SetSelfUuid(std::string_view)`,
  `void RegisterOriginHash(uint32_t idx, uint64_t hash)`,
  `struct Stats {uint64_t unstamped_writes, stale_epoch;}` + `const Stats& stats()`,
  and `void TEST_Reset()`.

`Commit` takes a callback rather than touching `DbSlice`, so this whole task stays
testable with no shard set and no database. Task 7 supplies the real callback.

**Two design points that are load-bearing, not stylistic:**

1. **The hop memo.** `HopStamp()` returns the *same* value for every call within one
   shard-callback. This is what makes author and applier agree when the author lumps
   several journal entries into one callback: the applier receives those entries
   separately and stamps each key from its own entry, so the entries must carry an
   identical mvcc. It also means the 20-bit counter is consumed per *hop*, not per
   *key* — a 100-key `MSET` costs one counter value.
2. **The stale-epoch backstop.** If a code path forgets to call `EndOfWriteEpoch()`,
   reusing an ancient hop stamp would be silent corruption. Instead, re-mint once the
   memo is older than `kMaxEpochMs` and count it, so a missed epoch degrades visibly.

**Arming must not allocate.** libstdc++ SSO holds 15 chars, so a `std::string` per arm
would allocate on every write to a 16-byte key. Use a reused arena plus `(offset, len)`.

- [ ] **Step 1: Write the failing tests**

Append to `src/server/mvcc_test.cc`:

```cpp
namespace {
// Collects what Commit would have written, so this task needs no DbSlice.
struct Recorder {
  std::vector<std::pair<std::string, MvccStamp>> writes;

  MvccStamper::CommitFn Fn() {
    return [this](DbIndex db, std::string_view key, const MvccStamp& st) {
      writes.emplace_back(std::string(key), st);
    };
  }
};

MvccStamper* FreshStamper() {
  MvccStamper* s = MvccStamper::tlocal();
  s->TEST_Reset();
  s->SetSelfUuid("6f1c4c3e-0000-4000-8000-00000000000a");
  return s;
}
}  // namespace

TEST(MvccStamperTest, CommitStampsEveryArmedKey) {
  MvccStamper* s = FreshStamper();
  Recorder rec;
  s->Arm(0, "k1");
  s->Arm(0, "k2");
  s->Commit(4242, /* origin_idx= */ 0, rec.Fn());

  ASSERT_EQ(rec.writes.size(), 2u);
  EXPECT_EQ(rec.writes[0].first, "k1");
  EXPECT_EQ(rec.writes[1].first, "k2");
  EXPECT_EQ(rec.writes[0].second.Mvcc(), 4242u);
  EXPECT_EQ(rec.writes[0].second.origin_hash, rec.writes[1].second.origin_hash);
  EXPECT_EQ(s->stats().unstamped_writes, 0u);
}

TEST(MvccStamperTest, EndOfEpochDropsUncommittedArms) {
  MvccStamper* s = FreshStamper();
  Recorder rec;
  s->Arm(0, "orphan");
  s->EndOfWriteEpoch();
  s->Commit(1, 0, rec.Fn());

  EXPECT_TRUE(rec.writes.empty()) << "an arm with no journal entry must not be stamped";
  EXPECT_EQ(s->stats().unstamped_writes, 1u)
      << "and the drop must be counted -- this is the production canary for read paths "
         "that mutate without journaling";
}

TEST(MvccStamperTest, DisarmRemovesOnlyTheNamedKey) {
  MvccStamper* s = FreshStamper();
  Recorder rec;
  s->Arm(0, "keep");
  s->Arm(0, "drop");
  s->Disarm(0, "drop");
  s->Commit(7, 0, rec.Fn());

  ASSERT_EQ(rec.writes.size(), 1u);
  EXPECT_EQ(rec.writes[0].first, "keep");
}

TEST(MvccStamperTest, DisarmIsScopedToTheDbIndex) {
  MvccStamper* s = FreshStamper();
  Recorder rec;
  s->Arm(0, "k");
  s->Arm(1, "k");
  s->Disarm(1, "k");
  s->Commit(7, 0, rec.Fn());

  ASSERT_EQ(rec.writes.size(), 1u);
  EXPECT_EQ(rec.writes[0].first, "k");
}

// The bit-identity mechanism: several entries minted inside one callback share a stamp.
TEST(MvccStamperTest, HopStampIsStableWithinEpochAndAdvancesAfter) {
  MvccStamper* s = FreshStamper();
  const uint64_t first = s->HopStamp();
  for (int i = 0; i < 5; ++i)
    EXPECT_EQ(s->HopStamp(), first) << "iteration " << i;

  s->EndOfWriteEpoch();
  EXPECT_GT(s->HopStamp(), first);
}

TEST(MvccStamperTest, PeerMvccIsNeverReminted) {
  MvccStamper* s = FreshStamper();
  Recorder rec;
  s->RegisterOriginHash(3, 0xABCDEF);
  s->Arm(0, "k");
  s->Commit(/* mvcc= */ 999, /* origin_idx= */ 3, rec.Fn());

  ASSERT_EQ(rec.writes.size(), 1u);
  EXPECT_EQ(rec.writes[0].second.Mvcc(), 999u) << "an applied write keeps the author's stamp "
                                                  "verbatim, or stamps inflate on every hop";
  EXPECT_EQ(rec.writes[0].second.origin_hash, 0xABCDEFu) << "and the author's origin, not ours";
}

TEST(MvccStamperTest, SelfOriginIsIndexZero) {
  MvccStamper* s = FreshStamper();
  EXPECT_EQ(s->OriginHash(0), NodeUuidHash("6f1c4c3e-0000-4000-8000-00000000000a"));
}

TEST(MvccStamperTest, ManyArmsDoNotInvalidateEarlierOnes) {
  // Guards the arena implementation: a reallocating buffer must not corrupt earlier (off, len).
  MvccStamper* s = FreshStamper();
  Recorder rec;
  std::vector<std::string> keys;
  for (int i = 0; i < 256; ++i) {
    keys.push_back(absl::StrCat("key-with-a-long-enough-name-to-force-growth-", i));
    s->Arm(0, keys.back());
  }
  s->Commit(5, 0, rec.Fn());

  ASSERT_EQ(rec.writes.size(), 256u);
  for (int i = 0; i < 256; ++i)
    EXPECT_EQ(rec.writes[i].first, keys[i]) << "arm " << i << " was corrupted by later growth";
}
```

- [ ] **Step 2: Run to verify it fails**

```bash
docker exec drakeydb-p2 sh -c 'cd /src/build-dbg && ninja -j4 mvcc_test'
```

Expected: **compile error**, `MvccStamper` not declared.

- [ ] **Step 3: Implement `MvccStamper` in `mvcc.h`**

```cpp
// (add near the top of mvcc.h)
#include <absl/container/inlined_vector.h>

#include <functional>
#include <string>
#include <vector>

#include "server/common.h"  // DbIndex

// (append inside namespace dfly, after MvccStamp)

// Thread-local minting and arm/commit bookkeeping. One per proactor thread, which is the right
// granularity: several DbSlices may share a thread via namespaces, and the clock only has to be
// monotone, never unique.
//
// Contract: DbSlice::PostUpdate ARMS the keys a shard callback touched; journal::RecordEntry
// COMMITS the entry's stamp to every armed key at the moment the entry is emitted; the end of the
// callback DISCARDS whatever is still armed. That is what enforces the phase's central invariant:
//
//   a key's stamp advances if and only if that same stamp is propagated to peers.
//
// Violating it in the "stamp but do not propagate" direction causes permanent, silent divergence:
// the node then rejects peer writes that should have won, forever.
class MvccStamper {
 public:
  using CommitFn = std::function<void(DbIndex, std::string_view, const MvccStamp&)>;

  // Re-mint the hop stamp if the memo is older than this. A missed EndOfWriteEpoch then degrades
  // visibly (stats().stale_epoch) instead of silently reusing an ancient stamp.
  static constexpr uint64_t kMaxEpochMs = 50;

  struct Stats {
    uint64_t unstamped_writes = 0;  // arms discarded with no journal entry -- see INFO
    uint64_t stale_epoch = 0;       // hop memo re-minted by the backstop above
  };

  static MvccStamper* tlocal();

  void SetSelfUuid(std::string_view uuid);
  void RegisterOriginHash(uint32_t origin_idx, uint64_t hash);
  uint64_t OriginHash(uint32_t origin_idx) const;

  // Stable for the whole shard callback. See kMaxEpochMs.
  uint64_t HopStamp();

  void Arm(DbIndex db_index, std::string_view key);
  void Disarm(DbIndex db_index, std::string_view key);

  // mvcc == 0 means "mint locally"; a non-zero value is an applied write's author stamp and is
  // stored verbatim. Clears the arm list.
  void Commit(uint64_t mvcc, uint32_t origin_idx, const CommitFn& fn);

  void EndOfWriteEpoch();

  const Stats& stats() const {
    return stats_;
  }
  const MvccClock& clock() const {
    return clock_;
  }

  void TEST_Reset();

 private:
  struct Armed {
    DbIndex db_index;
    uint32_t off;
    uint32_t len;
  };

  std::string_view ArmedKey(const Armed& a) const {
    return std::string_view(arena_.data() + a.off, a.len);
  }

  MvccClock clock_;
  uint64_t hop_stamp_ = 0;
  std::string arena_;  // cleared, never shrunk, so steady-state arming does not allocate
  absl::InlinedVector<Armed, 4> armed_;
  std::vector<uint64_t> origin_hash_cache_;  // dense by origin_idx; index 0 == self
  Stats stats_;
};
```

- [ ] **Step 4: Implement it in `mvcc.cc`**

```cpp
// (add to mvcc.cc)
#include "server/engine_shard_set.h"  // GetCurrentTimeMs

namespace dfly {

MvccStamper* MvccStamper::tlocal() {
  static thread_local MvccStamper stamper;
  return &stamper;
}

void MvccStamper::SetSelfUuid(std::string_view uuid) {
  RegisterOriginHash(0, NodeUuidHash(uuid));  // PeerRegistry::kSelfIdx == 0
}

void MvccStamper::RegisterOriginHash(uint32_t origin_idx, uint64_t hash) {
  if (origin_hash_cache_.size() <= origin_idx)
    origin_hash_cache_.resize(origin_idx + 1, 0);
  origin_hash_cache_[origin_idx] = hash;
}

uint64_t MvccStamper::OriginHash(uint32_t origin_idx) const {
  return origin_idx < origin_hash_cache_.size() ? origin_hash_cache_[origin_idx] : 0;
}

uint64_t MvccStamper::HopStamp() {
  const uint64_t now = GetCurrentTimeMs();
  if (hop_stamp_ == 0 || (hop_stamp_ >> MvccClock::kCounterBits) + kMaxEpochMs < now) {
    if (hop_stamp_ != 0)
      ++stats_.stale_epoch;
    hop_stamp_ = clock_.Next(now);
  }
  return hop_stamp_;
}

void MvccStamper::Arm(DbIndex db_index, std::string_view key) {
  const uint32_t off = static_cast<uint32_t>(arena_.size());
  arena_.append(key);
  armed_.push_back(Armed{db_index, off, static_cast<uint32_t>(key.size())});
}

void MvccStamper::Disarm(DbIndex db_index, std::string_view key) {
  for (auto it = armed_.begin(); it != armed_.end(); ++it) {
    if (it->db_index == db_index && ArmedKey(*it) == key) {
      armed_.erase(it);
      return;
    }
  }
}

void MvccStamper::Commit(uint64_t mvcc, uint32_t origin_idx, const CommitFn& fn) {
  if (armed_.empty())
    return;

  const MvccStamp stamp{mvcc != 0 ? mvcc : HopStamp(), OriginHash(origin_idx)};
  for (const Armed& a : armed_)
    fn(a.db_index, ArmedKey(a), stamp);

  armed_.clear();
  arena_.clear();  // keeps capacity
}

void MvccStamper::EndOfWriteEpoch() {
  stats_.unstamped_writes += armed_.size();
  armed_.clear();
  arena_.clear();
  hop_stamp_ = 0;
}

void MvccStamper::TEST_Reset() {
  clock_ = MvccClock{};
  hop_stamp_ = 0;
  armed_.clear();
  arena_.clear();
  origin_hash_cache_.clear();
  stats_ = Stats{};
}

}  // namespace dfly
```

- [ ] **Step 5: Run the tests to verify they pass**

```bash
docker exec drakeydb-p2 sh -c 'cd /src/build-dbg && ninja -j4 mvcc_test && /src/build-dbg/mvcc_test'
```

Expected: PASS, all 19 cases.

- [ ] **Step 6: Falsify the hop memo, per D9**

In `HopStamp()`, drop the memo — return `clock_.Next(GetCurrentTimeMs())` every call.
Rebuild, run `MvccStamperTest.HopStampIsStableWithinEpochAndAdvancesAfter`. Record the
observed failure, restore. This is the test that protects author/applier bit-identity
for effect-rewriting commands.

- [ ] **Step 7: Commit**

```bash
~/.venvs/precommit/bin/pre-commit run --files src/server/mvcc.h src/server/mvcc.cc src/server/mvcc_test.cc
git add -A && git commit -m "feat: add MvccStamper arm/commit with per-callback hop memo (P4)"
```

---

### Task 5: The side table on `DbTable`

**Files:**
- Modify: `src/server/table.h:126-176`, `src/server/table.cc:110-132`
- Modify: `src/server/db_slice.h`, `src/server/db_slice.cc`
- Test: `src/server/multi_master_test.cc`

**Interfaces:**
- Consumes: `MvccStamp` (Task 3); `IsActiveReplica()` from `server/multi_master.h`;
  the `mcflag` precedent at `table.h:128` and `db_slice.cc:1171-1197`.
- Produces: `using MvccTable = DashTable<PrimeKey, MvccStamp, detail::ExpireTablePolicy>;`
  and `std::unique_ptr<MvccTable> DbTable::mvcc`;
  `size_t DbTable::mvcc_table_memory() const`;
  on `DbSlice`: `void SetMvcc(DbIndex, const PrimeKey&, const MvccStamp&)`,
  `std::optional<MvccStamp> GetMvcc(DbIndex, std::string_view) const`,
  `void EraseMvcc(DbIndex, const PrimeKey&)`, `bool mvcc_enabled() const`,
  and `size_t DbSlice::mvcc_table_memory() const`.

Storage only. Nothing writes to it yet.

**The memory metric trap — do not get this wrong.** `DbTable::table_memory()`
(`table.h:174-176`) returns `prime.mem_usage()` **only**; mcflag has never been counted.
So `table_used_memory` in INFO will **not** move when this table fills, and a benchmark
comparing it reports a ~0 delta. Track a separate `mvcc_table_memory_` accumulator on
`DbSlice`. **Do not fold `mvcc->mem_usage()` into `table_memory()`** — that breaks
`DCHECK_EQ(table->table_memory(), table_before)` at `db_slice.cc:2087`, which asserts
deletes do not shrink the prime table.

- [ ] **Step 1: Write the failing tests**

In `src/server/multi_master_test.cc`, add a fixture that boots with the flag on. Follow
the existing `ActiveReplicaFamilyTest` pattern in this file for how the flag is set.

```cpp
class MvccStoreTest : public BaseFamilyTest {
 protected:
  void SetUp() override {
    absl::SetFlag(&FLAGS_active_replica, true);
    BaseFamilyTest::SetUp();
  }
  void TearDown() override {
    BaseFamilyTest::TearDown();
    absl::SetFlag(&FLAGS_active_replica, false);
  }
};

TEST_F(MvccStoreTest, SideTableIsAllocatedInActiveMode) {
  Run({"set", "k", "v"});
  EXPECT_GT(GetMetrics().db_stats[0].mvcc_table_bytes, 0u)
      << "active mode must allocate the side table";
}

TEST_F(MvccStoreTest, RoundTripsAStamp) {
  Run({"set", "k", "v"});
  auto& shard_set_ref = *shard_set;
  const MvccStamp written{12345, 999};
  shard_set_ref.Await(Shard("k", shard_set_ref.size()), [&] {
    auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();
    PrimeKey pk{"k"};
    db_slice.SetMvcc(0, pk, written);
  });

  std::optional<MvccStamp> got;
  shard_set_ref.Await(Shard("k", shard_set_ref.size()), [&] {
    got = namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, "k");
  });
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(*got, written);
}

TEST_F(MvccStoreTest, AbsentKeyReturnsNullopt) {
  std::optional<MvccStamp> got;
  shard_set->Await(Shard("nope", shard_set->size()), [&] {
    got = namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, "nope");
  });
  EXPECT_FALSE(got.has_value());
}

TEST_F(MvccStoreTest, FlushAllDropsTheSideTable) {
  Run({"set", "k", "v"});
  shard_set->Await(Shard("k", shard_set->size()), [&] {
    PrimeKey pk{"k"};
    namespaces->GetDefaultNamespace().GetCurrentDbSlice().SetMvcc(0, pk, MvccStamp{1, 1});
  });
  Run({"flushall"});
  EXPECT_EQ(GetMetrics().db_stats[0].mvcc_entries, 0u);
}

// The "off means byte-identical to upstream" guard.
TEST_F(BaseFamilyTest, NonActiveModeAllocatesNoMvccTable) {
  Run({"set", "k", "v"});
  EXPECT_EQ(GetMetrics().db_stats[0].mvcc_table_bytes, 0u)
      << "a non-active node must pay nothing for MVCC";
}
```

- [ ] **Step 2: Run to verify it fails**

```bash
docker exec drakeydb-p2 sh -c 'cd /src/build-dbg && ninja -j4 multi_master_test'
```

Expected: **compile error**, no member `mvcc_table_bytes` in `DbStats`.

- [ ] **Step 3: Add the table to `DbTable`**

In `src/server/table.h`, beside the `mcflag` declaration at `:128`:

```cpp
// drakeydb: Phase 4. Per-key MVCC stamp, mirroring the mcflag side-table precedent above.
// unique_ptr because it is allocated only in active mode -- a non-active node pays 8 bytes per
// DbTable rather than an empty DashTable. Same policy as mcflag/expire: 14 slots, no versioning.
using MvccTable = DashTable<PrimeKey, MvccStamp, detail::ExpireTablePolicy>;
std::unique_ptr<MvccTable> mvcc;

size_t mvcc_table_memory() const {
  return mvcc ? mvcc->mem_usage() : 0;
}
```

Do **not** touch `table_memory()`. Add `#include "server/mvcc.h"` to `table.h`.

In `src/server/table.cc`, in the constructor after the `mcflag` initialiser:

```cpp
  if (IsActiveReplica())
    mvcc = std::make_unique<MvccTable>(0, detail::ExpireTablePolicy{}, mr);
```

and in `DbTable::Clear()`:

```cpp
  if (mvcc)
    mvcc->Clear();
```

> `DbTable::Clear()` has no caller today — mirror it for hygiene, but the real flush
> path is `DbSlice::FlushDbIndexes` (`db_slice.cc:1055-1106`), which moves the whole
> `DbTable` out and `CreateDb`s a fresh one. That drops the side table for free.

- [ ] **Step 4: Add the `DbSlice` accessors**

In `src/server/db_slice.h`, beside `SetMCFlag`/`GetMCFlag`:

```cpp
  bool mvcc_enabled() const {
    return mvcc_enabled_;
  }
  void SetMvcc(DbIndex db_ind, const PrimeKey& key, const MvccStamp& stamp);
  std::optional<MvccStamp> GetMvcc(DbIndex db_ind, std::string_view key) const;
  void EraseMvcc(DbIndex db_ind, const PrimeKey& key);
  size_t mvcc_table_memory() const {
    return mvcc_table_memory_;
  }
```

and as members, beside `journal_omit_redundant_writes_` (`db_slice.h:690` area):

```cpp
  bool mvcc_enabled_ = false;
  size_t mvcc_table_memory_ = 0;
```

In `src/server/db_slice.cc`, initialise `mvcc_enabled_(IsActiveReplica())` in the
constructor initialiser list (beside `journal_omit_redundant_writes_`, `db_slice.cc:477`),
and implement, modelled on `SetMCFlag`/`GetMCFlag` at `:1171-1197`:

```cpp
void DbSlice::SetMvcc(DbIndex db_ind, const PrimeKey& key, const MvccStamp& stamp) {
  auto& db = *db_arr_[db_ind];
  if (!db.mvcc)
    return;
  string scratch;
  auto [it, inserted] = db.mvcc->Insert(key.GetSlice(&scratch), stamp);
  it->second = stamp;
}

optional<MvccStamp> DbSlice::GetMvcc(DbIndex db_ind, string_view key) const {
  auto& db = *db_arr_[db_ind];
  if (!db.mvcc)
    return nullopt;
  auto it = db.mvcc->Find(key);
  if (it.is_done())
    return nullopt;
  return it->second;
}

void DbSlice::EraseMvcc(DbIndex db_ind, const PrimeKey& key) {
  auto& db = *db_arr_[db_ind];
  if (!db.mvcc)
    return;
  string scratch;
  if (auto it = db.mvcc->Find(key.GetSlice(&scratch)); it != db.mvcc->end())
    db.mvcc->Erase(it);
}
```

Maintain `mvcc_table_memory_` alongside the existing `table_memory_` updates in
`CreateDb` (`db_slice.cc:1753` area) and `FlushDbIndexes` (`:1070` area).

- [ ] **Step 5: Surface the stats**

Put the **counters** on `DbTableStats` (`table.h:54`), not on `DbStats`:

```cpp
  size_t mvcc_entries = 0;
  size_t mvcc_tombstones = 0;  // stays 0 until P4-5; declared now so Task 10's invariant compiles
```

`DbStats : public DbTableStats` (`db_slice.h:43`), so the aggregate inherits both, and
Task 10's `DCHECK` can read `db.stats.mvcc_tombstones` — `db.stats` is a `DbTableStats`
(`table.h:136`), so declaring these on `DbStats` instead would not compile. Maintain
`mvcc_entries` in `SetMvcc`/`EraseMvcc`.

Add `size_t mvcc_table_bytes = 0;` to `DbStats` **only** — it is computed in
`DbSlice::GetStats` (`db_slice.cc:493-514`) from `mvcc_table_memory()` rather than
maintained as a counter, mirroring how `table_memory` is already handled. Sum all three
in `DbStats::operator+=` and merge them in `Metrics::Merge` (`metrics.cc:773` area)
beside `lsn_buffer_bytes`.

- [ ] **Step 6: Run the tests to verify they pass**

```bash
docker exec drakeydb-p2 sh -c 'cd /src/build-dbg && ninja -j4 multi_master_test && \
  /src/build-dbg/multi_master_test --gtest_filter="MvccStoreTest.*:*NonActiveModeAllocates*"'
```

Expected: PASS, 5 cases.

- [ ] **Step 7: Confirm the geometry assumption the memory estimate rests on**

Add to `mvcc_test.cc`:

```cpp
TEST(MvccTableGeometry, BucketMatchesTheSizingArgument) {
  // 26 (BucketBase<14>) + 14*18 (PrimeKey) + 2 pad + 14*16 (MvccStamp) = 504.
  // If this changes, the ~41 B/key figure and the benchmark's [34,48] band are both stale.
  EXPECT_EQ(sizeof(DbTable::MvccTable::Segment_t::Bucket), 504u);
}
```

Run it. **If it fails, stop** — record the actual value and recompute the per-key cost
before continuing, because Task 12's assertion band derives from it.

- [ ] **Step 8: Full C++ gate and commit**

```bash
docker exec drakeydb-p2 sh -c 'cd /src/build-dbg && ninja -j4 && ctest -V -L DFLY'
~/.venvs/precommit/bin/pre-commit run --files src/server/table.h src/server/table.cc \
  src/server/db_slice.h src/server/db_slice.cc src/server/mvcc_test.cc \
  src/server/multi_master_test.cc src/server/metrics.h src/server/metrics.cc
git add -A && git commit -m "feat: add per-key MVCC side table on DbTable (P4)"
```

---

### Task 6: `DbContext::repl_mvcc`

**Files:**
- Modify: `src/server/tx_base.h:67-76`
- Modify: `src/server/transaction.h:344-352` (`GetDbContext`)

**Interfaces:**
- Consumes: `Transaction::repl_mvcc_` — already exists from P3
  (`transaction.h:668-675`, passed into `journal::RecordEntry` by
  `LogJournalOnShard` at `transaction.cc:1668`).
- Produces: `uint64_t DbContext::repl_mvcc`.

Two lines, no behaviour change, shipped as its own commit so a bisect can isolate it.

**This is a hard prerequisite the approved plan does not mention.** Without it, a
delete applied from a peer cannot reproduce the peer's stamp, so `RENAME`, `GETDEL`
and `RESTORE REPLACE` would diverge between author and applier once P4-5 adds
tombstones. It is an exact mirror of what P3 did for `repl_origin_idx`.

- [ ] **Step 1: Add the field**

In `src/server/tx_base.h`, immediately after `repl_origin_idx`:

```cpp
  // drakeydb: Phase 4 -- the applied entry's MVCC stamp, mirroring repl_origin_idx above. Lets
  // delete paths that stamp directly (rather than through arm/commit) reproduce the author's
  // stamp byte for byte. 0 means "mint locally".
  uint64_t repl_mvcc = 0;
```

In `src/server/transaction.h`, in `GetDbContext()` beside the existing
`ctx.repl_origin_idx = repl_origin_idx_;`:

```cpp
    ctx.repl_mvcc = repl_mvcc_;
```

- [ ] **Step 2: Build and confirm nothing changed**

```bash
docker exec drakeydb-p2 sh -c 'cd /src/build-dbg && ninja -j4 && ctest -V -L DFLY'
```

Expected: 87/87 plus the new tests, all green. This task adds no test of its own —
Task 8 is what exercises it.

- [ ] **Step 3: Commit**

```bash
~/.venvs/precommit/bin/pre-commit run --files src/server/tx_base.h src/server/transaction.h
git add -A && git commit -m "feat: carry the apply-context MVCC stamp on DbContext (P4)"
```

---

### Task 7: The stamping hook — arm in `PostUpdate`, commit in `RecordEntry`

**This is the risky task in the plan. Read the landmine section before writing code.**

**Files:**
- Modify: `src/server/db_slice.cc:1426` (`PostUpdate`)
- Modify: `src/server/journal/journal.cc:90` (`RecordEntry`)
- Modify: `src/server/transaction.cc:730`, `:1543` (epoch end)
- Modify: `src/server/db_slice.cc` — four sweep paths
- Modify: `src/server/server_family.cc` (seed the self uuid hash at boot)
- Test: `src/server/multi_master_test.cc`

**Interfaces:**
- Consumes: `MvccStamper::tlocal()` (Task 4); `DbSlice::SetMvcc` (Task 5);
  `journal::RecordEntry(TxId, Op, DbIndex, optional<SlotId>, Entry::Payload, uint32_t origin_idx = 0, uint64_t mvcc = 0, uint8_t entry_flags = 0)`
  (`journal/journal.h:50-52`).
- Produces: after this task, any key written by a journaled command carries a stamp
  equal to that journal entry's `mvcc`.

#### The landmine

```
transaction.cc:717   db_slice.OnCbFinishBlocking();          <-- the OBVIOUS epoch end
transaction.cc:730     LogAutoJournalOnShard(shard, result);   <-- the entry is emitted HERE
transaction.cc:1541  db_slice.OnCbFinishBlocking();          <-- same inversion
transaction.cc:1543    LogAutoJournalOnShard(shard, result);
```

`OnCbFinishBlocking` is the natural place to end a stamping epoch, and it runs
**before** the journal entry exists. Put `EndOfWriteEpoch()` there and every
auto-journaled command — `INCR`, `LPUSH`, `HSET`, `SETRANGE`, `APPEND`, `ZADD`,
`SADD`, i.e. the majority of writes — arms, discards, and only then journals, against
an empty arm list. **The stamp never lands. Peers still converge by arrival order,
every existing test still passes, and P4-4's LWW guard later does nothing at all
because every stamp is 0.** Step 1 exists to make that failure loud.

The epoch must end as the **last statement** of `RunCallback` and
`RunSquashedMultiCb`, after `LogAutoJournalOnShard`.

#### Why the hook cannot be simpler

`PostUpdate` fires on paths that mutate without journaling, so "stamp in `PostUpdate`"
alone over-stamps and "gate on `cid_->IsJournaled()`" does not fix it:

1. **`GETEX <key>` with no options** — a pure read that mutates. Registered
   `CO::JOURNALED | ... | CO::NO_AUTOJOURNAL` (`string_family.cc:1850`); `CmdGetEx`
   calls `FindMutable` unconditionally (`:1425`) but journals only inside
   `if (shard->journal() && exp_params.IsDefined())` (`:1436`). It *is* journaled, so
   the flag gate fails here.
2. **`EXPIRE key ttl XX` that is not satisfied** — `OpExpire` runs
   `post_updater.Run()` (`generic_family.cc:847`) before `UpdateExpire` returns
   `SKIPPED`.
3. **Lazy collection-field expiry on a read path** (`hset_family.cc:996`,
   `set_family.cc:1178`, `doc_accessors.cc:220`) — the mutation is real and the
   non-journaling is by design. **There is no call-site fix**, which is what rules out
   "stamp in `PostUpdate` and patch the offenders".

Arm/commit handles all three: they arm, no entry is emitted, and the epoch end
discards the arm and counts it in `mvcc_unstamped_writes`.

- [ ] **Step 1: Write the landmine test first**

In `src/server/multi_master_test.cc`, in the `MvccStoreTest` fixture. Add this helper
beside it:

```cpp
  std::optional<MvccStamp> StampOf(std::string_view key) {
    std::optional<MvccStamp> out;
    shard_set->Await(Shard(key, shard_set->size()), [&] {
      out = namespaces->GetDefaultNamespace().GetCurrentDbSlice().GetMvcc(0, key);
    });
    return out;
  }
```

```cpp
// LPUSH is auto-journaled AND mutates in place, so it never calls AddOrUpdate. It is the
// command that fails if EndOfWriteEpoch is placed at transaction.cc:717 instead of after :730.
TEST_F(MvccStoreTest, AutoJournaledCommandStampsKey) {
  Run({"lpush", "mylist", "a"});
  auto st = StampOf("mylist");
  ASSERT_TRUE(st.has_value()) << "auto-journaled write left no stamp -- check that "
                                 "EndOfWriteEpoch runs AFTER LogAutoJournalOnShard";
  EXPECT_GT(st->Mvcc(), 0u);
  EXPECT_NE(st->origin_hash, 0u) << "self origin hash must be seeded at boot";
}

TEST_F(MvccStoreTest, NoAutoJournalCommandStampsKey) {
  Run({"set", "k", "v"});  // SET is NO_AUTOJOURNAL and journals via SetCmd::RecordJournal
  ASSERT_TRUE(StampOf("k").has_value());
}

TEST_F(MvccStoreTest, StampsAreStrictlyIncreasingAcrossWrites) {
  Run({"set", "k", "v1"});
  const uint64_t first = StampOf("k")->Mvcc();
  for (int i = 0; i < 50; ++i) {
    Run({"set", "k", absl::StrCat("v", i)});
    const uint64_t cur = StampOf("k")->Mvcc();
    EXPECT_GT(cur, first) << "iteration " << i;
  }
}

TEST_F(MvccStoreTest, MultiKeySameShardCommandSharesOneStamp) {
  // Both keys hash to one shard in the single-shard test fixture.
  Run({"mset", "k1", "v1", "k2", "v2"});
  auto a = StampOf("k1");
  auto b = StampOf("k2");
  ASSERT_TRUE(a.has_value() && b.has_value());
  EXPECT_EQ(a->Mvcc(), b->Mvcc()) << "one shard callback mints one stamp";
}

// The over-stamp cases. Each must leave the stamp untouched and bump the counter.
TEST_F(MvccStoreTest, ReadOnlyGetExDoesNotStampKey) {
  Run({"set", "k", "v"});
  const MvccStamp before = *StampOf("k");
  Run({"getex", "k"});  // no expiry option -> mutates nothing, journals nothing
  EXPECT_EQ(*StampOf("k"), before)
      << "a read that runs an AutoUpdater must not advance the stamp -- if it does, this node "
         "silently rejects peer writes that should have won";
}

TEST_F(MvccStoreTest, SkippedExpireDoesNotStampKey) {
  Run({"set", "k", "v"});
  const MvccStamp before = *StampOf("k");
  EXPECT_EQ(0, CheckedInt({"expire", "k", "100", "XX"}));  // no TTL set -> predicate fails
  EXPECT_EQ(*StampOf("k"), before);
}
```

- [ ] **Step 2: Run to verify it fails**

```bash
docker exec drakeydb-p2 sh -c 'cd /src/build-dbg && ninja -j4 multi_master_test && \
  /src/build-dbg/multi_master_test --gtest_filter="MvccStoreTest.*"'
```

Expected: FAIL on `AutoJournaledCommandStampsKey` —
`st.has_value()` is false, no stamp is written by anything yet.

- [ ] **Step 3: Arm in `PostUpdate`**

At the end of `DbSlice::PostUpdate` (`db_slice.cc:1426`):

```cpp
  // drakeydb: Phase 4 -- arm this key. journal::RecordEntry commits the entry's stamp to every
  // armed key; the end of the shard callback discards whatever is still armed. That is what
  // enforces "a key's stamp advances iff that same stamp is propagated". See server/mvcc.h.
  if (mvcc_enabled_)
    MvccStamper::tlocal()->Arm(db_ind, key);
```

- [ ] **Step 4: Mint and commit in `RecordEntry`**

In `src/server/journal/journal.cc:90`, `RecordEntry`. **Order matters and the two halves
split around `AddLogRecord`:** mint **before** it, so the wire carries the same value the
side table will store; commit **after** it, so a key is only stamped once its entry is
durable. Getting this backwards puts mvcc 0 on the wire and makes every applier mint its
own stamp.

```cpp
  // drakeydb: Phase 4 -- mint BEFORE AddLogRecord so the wire carries this exact value.
  if (MvccEnabled() && opcode == Op::COMMAND && entry.mvcc == 0)
    entry.mvcc = MvccStamper::tlocal()->HopStamp();
```

then, immediately **after** `AddLogRecord(entry)`:

```cpp
  // drakeydb: Phase 4. Mint locally when the caller supplied no stamp; a non-zero mvcc is an
  // applied write's author stamp and is kept verbatim, or stamps would inflate on every hop.
  // 0 is unreachable for a real stamp (ms << 20, ms ~ 1.77e12).
  // drakeydb: Phase 4 -- commit AFTER the entry is durable. A non-zero mvcc arriving from a
  // peer is that author's stamp and is stored verbatim, or stamps inflate on every hop.
  if (MvccEnabled() && opcode == Op::COMMAND) {
    MvccStamper::tlocal()->Commit(entry.mvcc, entry.origin_idx,
                                  [](DbIndex db, std::string_view key, const MvccStamp& st) {
                                    EngineShard::tlocal()->db_slice().SetMvcc(db, PrimeKey{key},
                                                                             st);
                                  });
  }
```

Gating on `Op::COMMAND` excludes `SELECT`/`PING`/`LSN`/`ORIGIN`. `MvccEnabled()` is a
thin `IsActiveReplica()` wrapper local to this TU.

- [ ] **Step 5: End the epoch in the right place**

As the **last statement** of `Transaction::RunCallback`, after
`LogAutoJournalOnShard(shard, result)` at `transaction.cc:730` and after
`shard->set_running_tx(nullptr)`:

```cpp
  // drakeydb: Phase 4 -- MUST be after LogAutoJournalOnShard, not inside OnCbFinishBlocking
  // (transaction.cc:717), which runs before the journal entry exists. See server/mvcc.h.
  MvccStamper::tlocal()->EndOfWriteEpoch();
```

Same, after `LogAutoJournalOnShard` in `Transaction::RunSquashedMultiCb` (`:1543`).

Then the four non-transactional sweep paths in `db_slice.cc`, each at function exit:
`DeleteExpiredStep` (`:1597`), `ExpireAllIfNeeded` (`:1505`),
`FreeMemWithEvictionStepAtomic` (`:1658`), and per bucket-batch in `FlushSlotsFb`
(`:935`).

- [ ] **Step 6: Disable the journal-omission optimisation in active mode**

`DbSlice::IsOmittableWrite` (`db_slice.cc:2152-2166`) suppresses a journal entry when
there is exactly one eventually-consistent snapshot consumer that has not yet reached
the bucket — the snapshot will carry the value instead. Under arm/commit that write
**arms but never commits**, so the key ends up with *no stamp at all* and Task 10's
invariant fires.

The optimisation is already inert in a real mesh (it requires
`change_cb_.size() == 1 && journal::GetCallbackCount() == 1`, so it can only fire
during a single full sync), so turning it off in active mode costs a handful of
redundant journal entries during one sync — which P3's peer full-sync filter already
handles — and buys a dense, checkable invariant.

At the top of `IsOmittableWrite`, beside the existing flag check:

```cpp
  // drakeydb: Phase 4 -- an omitted write arms but never commits, leaving the key unstamped.
  // The optimisation only fires during a single full sync anyway, so active mode forgoes it.
  if (mvcc_enabled_)
    return false;
```

Add a regression test:

```cpp
TEST_F(MvccStoreTest, WritesDuringSnapshotAreStillStamped) {
  Run({"debug", "populate", "100"});
  // A write while a snapshot is in flight is the case IsOmittableWrite targets.
  Run({"set", "during", "v"});
  ASSERT_TRUE(StampOf("during").has_value())
      << "a journal-omitted write must still be stamped, or the key is permanently unstamped";
}
```

- [ ] **Step 7: Seed the self uuid hash at boot**

In `ServerFamily::Init()`, where the node identity is loaded (P1 code — search for
`node_identity` / the loaded uuid), broadcast it to every shard thread:

```cpp
  // drakeydb: Phase 4 -- every proactor thread needs the self origin hash before the first write.
  shard_set->pool()->AwaitBrief(
      [uuid = node_uuid_](unsigned, auto*) { MvccStamper::tlocal()->SetSelfUuid(uuid); });
```

- [ ] **Step 8: Run the tests to verify they pass**

```bash
docker exec drakeydb-p2 sh -c 'cd /src/build-dbg && ninja -j4 multi_master_test && \
  /src/build-dbg/multi_master_test --gtest_filter="MvccStoreTest.*"'
```

Expected: PASS, all 11 cases.

- [ ] **Step 9: Falsify the landmine explicitly, per D9 — do not skip this**

Move `EndOfWriteEpoch()` from after `LogAutoJournalOnShard` into
`DbSlice::OnCbFinishBlocking()`. Rebuild. Run:

```bash
docker exec drakeydb-p2 sh -c '/src/build-dbg/multi_master_test \
  --gtest_filter="MvccStoreTest.AutoJournaledCommandStampsKey"'
```

Expected failure text, to be recorded verbatim in the SDD ledger:

```
auto-journaled write left no stamp -- check that EndOfWriteEpoch runs AFTER
LogAutoJournalOnShard
```

Then also run the whole pytest multimaster suite **in that broken state** and record
that it still passes — that is the evidence for why this test has to exist. Restore.

- [ ] **Step 10: Check the over-stamp counter on a real workload**

```bash
docker exec drakeydb-p2 sh -c '/src/build-dbg/multi_master_test --gtest_filter="MvccStoreTest.*"'
```

Then add and run:

```cpp
TEST_F(MvccStoreTest, PureWriteWorkloadLeavesNoUnstampedWrites) {
  for (int i = 0; i < 200; ++i)
    Run({"set", absl::StrCat("k", i), "v"});
  EXPECT_EQ(GetMetrics().mvcc_unstamped_writes, 0u)
      << "a pure write workload must never discard an arm";
}
```

(The metric is wired in Task 11; until then read `MvccStamper::tlocal()->stats()`
through a shard hop.) Expected: 0.

- [ ] **Step 11: Full gate and commit**

```bash
docker exec drakeydb-p2 sh -c 'cd /src/build-dbg && ninja -j4 && ctest -V -L DFLY'
docker exec drakeydb-p2 sh -c \
  'cd /src && DRAGONFLY_PATH=/src/build-dbg/dragonfly /tmp/tv/bin/python -m pytest \
   tests/dragonfly/multimaster_test.py tests/dragonfly/replication_test.py -q'
~/.venvs/precommit/bin/pre-commit run --files src/server/db_slice.cc src/server/journal/journal.cc \
  src/server/transaction.cc src/server/server_family.cc src/server/multi_master_test.cc
git add -A && git commit -m "feat: stamp keys via arm/commit tied to journal emission (P4)"
```

---

### Task 8: Delete coverage

**Files:**
- Modify: `src/server/db_slice.h:351,594`, `src/server/db_slice.cc:906,927,2025`
- Test: `src/server/multi_master_test.cc`

**Interfaces:**
- Consumes: `DbSlice::EraseMvcc` (Task 5); `MvccStamper::Disarm` (Task 4).
  (`DbContext::repl_mvcc` from Task 6 is **not** used here — it is a P4-5 dependency,
  for applied deletes reproducing a peer's tombstone stamp.)
- Produces: `enum class DeleteReason : uint8_t { kExplicit, kExpired, kEvicted, kSlotFlush };`
  in `db_slice.h`; `DbSlice::Del` and `PerformDeletionAtomic` gain a trailing
  `DeleteReason reason = DeleteReason::kExplicit`.

In P4-1 **every reason erases** — tombstones are P4-5. The enum lands now because
Task 10's invariant and P4-5's tombstone table both key off it, and threading it later
would touch the same five call sites twice.

**Why `Disarm` is required.** `OpDelV2` (`generic_family.cc:1273`) runs
`post_updater.Run()` — which arms the key — *before* calling `Del`, and journals
afterwards. Without a disarm, the subsequent `Commit` would write a stamp for a key
that no longer exists, and Task 10's invariant would fire.

- [ ] **Step 1: Write the failing tests**

```cpp
TEST_F(MvccStoreTest, DeleteErasesTheStamp) {
  Run({"set", "k", "v"});
  ASSERT_TRUE(StampOf("k").has_value());
  Run({"del", "k"});
  EXPECT_FALSE(StampOf("k").has_value()) << "a deleted key must not leave a stamp behind";
}

// OpDelV2 arms the key (post_updater.Run()) before deleting and journals after. Without the
// disarm, Commit writes a stamp for a key that is already gone.
TEST_F(MvccStoreTest, DeleteInSameCallbackDoesNotResurrectAStamp) {
  Run({"set", "k", "v"});
  Run({"del", "k"});
  EXPECT_FALSE(StampOf("k").has_value())
      << "the DEL's own journal entry must not re-stamp the key it just removed";
  EXPECT_EQ(GetMetrics().db_stats[0].mvcc_entries, 0u);
}

TEST_F(MvccStoreTest, RenameMovesTheStampByRecreatingIt) {
  Run({"set", "a", "v"});
  Run({"rename", "a", "b"});
  EXPECT_FALSE(StampOf("a").has_value());
  ASSERT_TRUE(StampOf("b").has_value()) << "the destination is armed and committed by RENAME's "
                                           "own journal entry";
}

TEST_F(MvccStoreTest, ExpiryErasesTheStamp) {
  Run({"set", "k", "v", "px", "10"});
  ASSERT_TRUE(StampOf("k").has_value());
  AdvanceTime(50);
  Run({"get", "k"});  // triggers lazy expiry
  EXPECT_FALSE(StampOf("k").has_value());
}

TEST_F(MvccStoreTest, MultiKeyDeleteErasesEveryStamp) {
  Run({"mset", "k1", "v1", "k2", "v2", "k3", "v3"});
  Run({"del", "k1", "k2"});
  EXPECT_FALSE(StampOf("k1").has_value());
  EXPECT_FALSE(StampOf("k2").has_value());
  EXPECT_TRUE(StampOf("k3").has_value());
}
```

Use whatever time-advance helper `BaseFamilyTest` already provides (`TEST_current_time_ms`
via the existing `AdvanceTime`/`UpdateTime` helper — grep the fixture and match it).

- [ ] **Step 2: Run to verify it fails**

Expected: FAIL on `DeleteErasesTheStamp` — the stamp survives the delete.

- [ ] **Step 3: Add `DeleteReason` and thread it**

In `src/server/db_slice.h`, above the `Del` declaration:

```cpp
// drakeydb: Phase 4 -- why a key is being removed. In P4-1 every reason erases the stamp; P4-5
// gives kExplicit and kExpired a tombstone while kEvicted and kSlotFlush keep erasing.
// Eviction deliberately gets no tombstone: it is a local capacity decision, so resurrection from
// a peer is desirable -- the peer's copy is authoritative.
enum class DeleteReason : uint8_t { kExplicit, kExpired, kEvicted, kSlotFlush };
```

Add `DeleteReason reason = DeleteReason::kExplicit` as the trailing parameter of both
`Del` (`db_slice.h:351`) and `PerformDeletionAtomic` (`:594`), and forward it at the
single call site (`db_slice.cc:927`).

Set a non-default reason at exactly these five sites, all inside `db_slice.cc`:

| Line | Reason |
|---|---|
| `:249` `PrimeEvictionPolicy::Evict` | `kEvicted` |
| `:1725` `FreeMemWithEvictionStepAtomic` | `kEvicted` |
| `:1497` `ExpireIfNeeded` | `kExpired` |
| `:962` `FlushSlotsFb` | `kSlotFlush` |
| `:1035` `FlushSlots` on_change | `kSlotFlush` |

Everything else keeps the `kExplicit` default.

- [ ] **Step 4: Disarm and erase in `PerformDeletionAtomic`**

Immediately after the existing mcflag block (`db_slice.cc:2029-2035`):

```cpp
  // drakeydb: Phase 4. Disarm first: OpDelV2 arms the key via post_updater.Run() before calling
  // Del and journals afterwards, so without this the DEL's own Commit would re-stamp a key that
  // no longer exists. The mvcc table is dense, so unlike mcflag there is no HasFlag()-style bit
  // to skip the probe -- and there is always an entry to remove, so the probe is not wasted.
  if (mvcc_enabled_) {
    MvccStamper::tlocal()->Disarm(table->index, del_it.key());
    EraseMvcc(table->index, del_it->first);
  }
```

- [ ] **Step 5: Run the tests to verify they pass**

```bash
docker exec drakeydb-p2 sh -c 'cd /src/build-dbg && ninja -j4 multi_master_test && \
  /src/build-dbg/multi_master_test --gtest_filter="MvccStoreTest.*"'
```

Expected: PASS, all 16 cases.

- [ ] **Step 6: Falsify the disarm, per D9**

Remove the `Disarm` call, keep `EraseMvcc`. Rebuild, run
`MvccStoreTest.DeleteInSameCallbackDoesNotResurrectAStamp`. Record the observed
failure, restore.

- [ ] **Step 7: Full gate and commit**

```bash
docker exec drakeydb-p2 sh -c 'cd /src/build-dbg && ninja -j4 && ctest -V -L DFLY'
~/.venvs/precommit/bin/pre-commit run --files src/server/db_slice.h src/server/db_slice.cc \
  src/server/multi_master_test.cc
git add -A && git commit -m "feat: erase MVCC stamps on every delete path (P4)"
```

---

### Task 9: Inbound plumbing — an applied write keeps the author's stamp

**Files:**
- Modify: `src/server/journal/tx_executor.h:48-54`, `src/server/journal/tx_executor.cc:74-79`
- Modify: `src/server/journal/executor.h:55-57`
- Modify: `src/server/replica.h`, `src/server/replica.cc:473,1517,1547,1580`
- Modify: `src/server/multi_master.h`, `src/server/multi_master.cc`
- Test: `src/server/multi_master_test.cc`, `tests/dragonfly/multimaster_test.py`

**Interfaces:**
- Consumes: `journal::ParsedEntry::mvcc` (P3, on the wire since `serializer.cc:305`);
  `MvccStamper::RegisterOriginHash` (Task 4); `NodeUuidHash` (Task 3);
  `MasterContext::master_node_uuid` (P1).
- Produces: `uint64_t TransactionData::mvcc`;
  `void JournalExecutor::SetApplyMvcc(uint64_t)`;
  `Replica::peer_origin_hash_`; `uint64_t PeerRegistry::GetUuidHash(uint32_t idx) const`.

**The counterintuitive part: the origin hash cannot come from the wire.**
`journal::PassesPeerEchoFilter` (`journal/types.cc:51-66`) forwards only entries whose
`origin_idx == kSelfIdx`, so **every COMMAND arriving on a peer link carries
`origin_idx == 0`**, meaning "the sender". Resolving that through `PeerRegistry` would
label every foreign write as self. Under no-forward v1 the sender *is* the author on
the streaming path, so the correct source is the **link's** peer uuid, which `Replica`
already derives `peer_origin_idx_` from at `replica.cc:473`.

**And `mvcc` is dropped inbound today.** `TransactionData::AddEntry`
(`tx_executor.cc:74-79`) copies only `command`, `dbid` and `txid`;
`TransactionData` has no `mvcc` field. `ConnectionContext::repl_mvcc` exists and is
written by nobody. Without this task an applied write mints a *local* stamp and the
phase's acceptance criterion fails.

- [ ] **Step 1: Write the failing end-to-end test**

```cpp
// The phase's acceptance criterion: an applied write carries the author's stamp verbatim.
TEST_F(MvccStoreTest, AppliedWriteKeepsAuthorStampVerbatim) {
  // OriginJournalFamilyTest's harness drives a JournalExecutor with a chosen apply context;
  // reuse it. Apply a SET carrying a known author mvcc and origin, then read the stored stamp.
  constexpr uint64_t kAuthorMvcc = 0x1234'5678'9ABCULL;
  constexpr uint32_t kPeerIdx = 3;
  const uint64_t peer_hash = NodeUuidHash("6f1c4c3e-0000-4000-8000-00000000000b");

  shard_set->AwaitBrief([&](unsigned, auto*) {
    MvccStamper::tlocal()->RegisterOriginHash(kPeerIdx, peer_hash);
  });
  ApplyReplicatedCommand({"set", "k", "v"}, kPeerIdx, kAuthorMvcc);

  auto st = StampOf("k");
  ASSERT_TRUE(st.has_value());
  EXPECT_EQ(st->Mvcc(), kAuthorMvcc) << "the applier must not re-mint -- stamps would otherwise "
                                        "inflate on every replication hop";
  EXPECT_EQ(st->origin_hash, peer_hash) << "and must record the AUTHOR, not itself";
}
```

Add `ApplyReplicatedCommand(args, origin_idx, mvcc)` to the fixture, modelled on how
`OriginJournalFamilyTest` already drives `JournalExecutor` with `SetApplyOrigin` — read
that helper and extend it with `SetApplyMvcc`.

- [ ] **Step 2: Run to verify it fails**

Expected: **compile error**, `SetApplyMvcc` not declared. After stubbing it, expect a
value failure: `st->Mvcc()` is a freshly minted local stamp, not `kAuthorMvcc`.

- [ ] **Step 3: Carry `mvcc` inbound**

In `src/server/journal/tx_executor.h`, add to `TransactionData` beside `opcode`:

```cpp
  // drakeydb: Phase 4 -- the author's stamp, so the applier stores it verbatim rather than
  // minting a local one. Dropped before P4; see AddEntry.
  uint64_t mvcc = 0;
  uint8_t entry_flags = 0;
```

In `tx_executor.cc`, in `AddEntry`'s `Op::EXPIRED`/`Op::COMMAND` case:

```cpp
      mvcc = entry.mvcc;
      entry_flags = entry.entry_flags;
```

In `src/server/journal/executor.h`, beside `SetApplyOrigin`:

```cpp
  // Per-ENTRY, unlike SetApplyOrigin which is per-link.
  void SetApplyMvcc(uint64_t mvcc) {
    conn_context_.repl_mvcc = mvcc;
  }
```

In `src/server/replica.cc`, call `executor_->SetApplyMvcc(tx_data.mvcc)` immediately
before **both** `Execute` calls in `ExecuteTx` (`:1547` and the global-command path at
`:1580`), and in `RdbLoaderBase::HandleJournalBlob` (`rdb_load.cc:3006` area) before
its `Execute`.

- [ ] **Step 4: Resolve the origin hash from the link**

Add `uint64_t GetUuidHash(uint32_t idx) const` to `PeerRegistry` backed by a parallel
append-only `std::vector<uint64_t> idx_to_hash_`, filled in `Init` and `AddOrGet`
(`multi_master.cc:104,110`). Indices are dense, monotonic and never reclaimed, so no
extra synchronisation is needed beyond the existing registry mutex.

In `replica.h` add `uint64_t peer_origin_hash_ = 0;` beside `peer_origin_idx_`; set it
at `replica.cc:473` where `peer_origin_idx_` is derived:

```cpp
  // drakeydb: Phase 4 -- the AUTHOR's hash. It cannot come from the wire: PassesPeerEchoFilter
  // forwards only self-origin entries, so every COMMAND arriving here carries origin_idx == 0.
  // Under no-forward v1 the sender is the author, so the link's uuid is the correct source.
  peer_origin_hash_ = NodeUuidHash(master_context_.master_node_uuid);
```

Register it on the shard threads when the flow starts (`replica.cc:1517` area, beside
`SetApplyOrigin`):

```cpp
  MvccStamper::tlocal()->RegisterOriginHash(peer_origin_idx_, peer_origin_hash_);
```

- [ ] **Step 5: Run to verify it passes**

Expected: PASS.

- [ ] **Step 6: Add the pytest acceptance test**

```python
async def test_replicated_key_stamp_matches_origin(df_factory):
    """The phase's headline criterion. Falsified by removing SetApplyMvcc in replica.cc:
    B then mints its own stamp and the mvcc values differ."""
    a = df_factory.create(**active_args())
    b = df_factory.create(**active_args())
    df_factory.start_all([a, b])
    c_a, c_b = a.client(), b.client()
    attach(c_b, a)
    await wait_for_peers(c_b, 1)

    await c_a.execute_command("set", "k", "v")
    await assert_eventually(lambda: _exists(c_b, "k"))

    stamp_a = _parse_mvcc(await c_a.execute_command("debug", "mvcc", "k"))
    stamp_b = _parse_mvcc(await c_b.execute_command("debug", "mvcc", "k"))
    assert stamp_a["mvcc"] == stamp_b["mvcc"], f"{stamp_a} != {stamp_b}"
    assert stamp_a["origin"] == stamp_b["origin"], "both must name A as the author"


def _parse_mvcc(reply) -> dict:
    text = reply.decode() if isinstance(reply, bytes) else reply
    return dict(part.split(":", 1) for part in text.split())
```

> `DEBUG MVCC` lands in Task 11. Write this test now, mark it
> `@pytest.mark.skip(reason="needs DEBUG MVCC from task 11")`, and **remove the skip
> as the last step of Task 11**.

- [ ] **Step 7: Falsify, per D9**

Remove the `SetApplyMvcc` call in `ExecuteTx`. Rebuild, run
`AppliedWriteKeepsAuthorStampVerbatim`. Record the observed failure, restore.

- [ ] **Step 8: Full gate and commit**

```bash
docker exec drakeydb-p2 sh -c 'cd /src/build-dbg && ninja -j4 && ctest -V -L DFLY'
docker exec drakeydb-p2 sh -c \
  'cd /src && DRAGONFLY_PATH=/src/build-dbg/dragonfly /tmp/tv/bin/python -m pytest \
   tests/dragonfly/multimaster_test.py tests/dragonfly/replication_test.py -q'
git add -A && git commit -m "feat: apply peer MVCC stamps verbatim on the replica side (P4)"
```

---

### Task 10: Defrag mirror and the leak invariant

**Files:**
- Modify: `src/server/db_slice.cc:2005-2023` (`DefragTableSegments`)
- Modify: `src/server/table.h` (second cursor)
- Modify: `src/server/db_slice.h`, `src/server/db_slice.cc:2097` (`OnCbFinishBlocking`)
- Test: `src/server/multi_master_test.cc`

**Interfaces:**
- Consumes: Task 5's table, Task 8's delete path.
- Produces: `DbTable::mvcc_defrag_cursor`;
  `size_t DbSlice::TEST_VerifyMvccTable(DbIndex) const` returning a mismatch count.

**Why the defrag mirror matters.** `DefragTableSegments` relocates `prime` segments
only. Nothing else relocates the side table's, so without a mirror it accumulates
mimalloc fragmentation for the process's lifetime. The per-object defrag at
`engine_shard.cc:375` does **not** help — it relocates a `PrimeValue`'s heap, and the
side table holds its own key copy.

- [ ] **Step 1: Write the failing tests**

```cpp
TEST_F(MvccStoreTest, TableMatchesPrimeAfterMixedWorkload) {
  for (int i = 0; i < 200; ++i) {
    Run({"set", absl::StrCat("k", i), "v"});
    if (i % 7 == 0) {
      // Rename the key just written -- renaming k(i-1) would hit one the i%3 branch deleted,
      // and RENAME on a missing key errors.
      Run({"rename", absl::StrCat("k", i), absl::StrCat("r", i)});
    } else if (i % 3 == 0) {
      Run({"del", absl::StrCat("k", i)});
    }
  }
  size_t mismatches = 0;
  shard_set->AwaitFiberOnAll([&](auto*, EngineShard* shard) {
    mismatches += shard->db_slice().TEST_VerifyMvccTable(0);
  });
  EXPECT_EQ(mismatches, 0u) << "every live key needs exactly one stamp, and vice versa";
}

TEST_F(MvccStoreTest, DefragRelocationPreservesStamps) {
  for (int i = 0; i < 500; ++i)
    Run({"set", absl::StrCat("k", i), std::string(200, 'x')});
  for (int i = 0; i < 500; i += 2)
    Run({"del", absl::StrCat("k", i)});

  const auto before = *StampOf("k1");
  Run({"debug", "compact-table"});  // match the actual subcommand name in debugcmd.cc
  EXPECT_EQ(*StampOf("k1"), before) << "defrag must not lose or corrupt stamps";

  size_t mismatches = 0;
  shard_set->AwaitFiberOnAll([&](auto*, EngineShard* shard) {
    mismatches += shard->db_slice().TEST_VerifyMvccTable(0);
  });
  EXPECT_EQ(mismatches, 0u);
}
```

- [ ] **Step 2: Run to verify it fails**

Expected: **compile error**, `TEST_VerifyMvccTable` not declared.

- [ ] **Step 3: Implement the verifier**

In `src/server/db_slice.cc`:

```cpp
size_t DbSlice::TEST_VerifyMvccTable(DbIndex db_ind) const {
  auto& db = *db_arr_[db_ind];
  if (!db.mvcc)
    return 0;

  size_t mismatches = 0;
  string scratch;
  db.prime.CVisit([&](const auto& it) {  // match the actual traversal API in dash.h
    if (db.mvcc->Find(it->first.GetSlice(&scratch)).is_done()) {
      LOG(ERROR) << "mvcc: live key with no stamp: " << it->first.ToString();
      ++mismatches;
    }
  });
  db.mvcc->CVisit([&](const auto& it) {
    if (db.prime.Find(it->first.GetSlice(&scratch)).is_done()) {
      LOG(ERROR) << "mvcc: stamp with no live key: " << it->first.ToString();
      ++mismatches;
    }
  });
  return mismatches;
}
```

Match `dash.h`'s real traversal API — read it rather than assuming `CVisit`.

Add the O(1) form to `OnCbFinishBlocking` (`db_slice.cc:2097`) behind `#ifndef NDEBUG`:

```cpp
#ifndef NDEBUG
  // drakeydb: Phase 4 -- cheap dense invariant. mvcc_tombstones is 0 until P4-5.
  if (mvcc_enabled_) {
    auto& db = *db_arr_[cntx.db_index];
    if (db.mvcc)
      DCHECK_EQ(db.mvcc->size() - db.stats.mvcc_tombstones, db.prime.size());
  }
#endif
```

- [ ] **Step 4: Mirror the defrag**

Add `detail::DashCursor mvcc_defrag_cursor;` to `DbTable` and reset it in `Clear()`.
In `DefragTableSegments`, after the existing `prime` loop, add a structurally identical
loop over `db_table->mvcc` guarded by `if (!db_table->mvcc) return;` and using the new
cursor. Keep the same `FiberAtomicGuard` and quota checks — read the existing loop and
mirror it exactly.

- [ ] **Step 5: Run to verify it passes**

Expected: PASS.

- [ ] **Step 6: Falsify, per D9**

Comment out the `EraseMvcc` call in `PerformDeletionAtomic`. Rebuild, run
`TableMatchesPrimeAfterMixedWorkload`. Record the observed failure — it should report a
non-zero mismatch count with `mvcc: stamp with no live key` lines in the log. Restore.

- [ ] **Step 7: Full gate and commit**

```bash
docker exec drakeydb-p2 sh -c 'cd /src/build-dbg && ninja -j4 && ctest -V -L DFLY'
git add -A && git commit -m "feat: mirror defrag and add the MVCC table leak invariant (P4)"
```

---

### Task 11: `DEBUG MVCC` and INFO fields

**Files:**
- Modify: `src/server/debugcmd.cc` (`DebugCmd::Run` dispatch + a new handler)
- Modify: `src/server/server_family.cc:2821` (INFO memory), the active replication block
- Modify: `src/server/metrics.h`, `src/server/metrics.cc:773`
- Test: `src/server/multi_master_test.cc`, `tests/dragonfly/multimaster_test.py`

**Interfaces:**
- Consumes: `DbSlice::GetMvcc`, `TEST_VerifyMvccTable`, `MvccStamper::stats()`,
  `MvccClock::AheadMs`.
- Produces: `DEBUG MVCC <key>`, `DEBUG MVCC`, `DEBUG MVCC VERIFY`; INFO fields
  `mvcc_table_bytes`, `mvcc_entries`, `mvcc_tombstones`, `mvcc_clock_ahead_ms`,
  `mvcc_unstamped_writes`, `mvcc_stale_epoch`.

**`DEBUG`, not `DFLY` — this corrects `docs/PLAN.md`.** `DFLY` is registered
`CO::ADMIN | CO::GLOBAL_TRANS | CO::HIDDEN` (`server_family.cc:4308`), so a per-key
stamp read would take a **global transaction across every shard** — serialising against
all traffic and perturbing exactly what it measures. `DEBUG` is
`CO::ADMIN | CO::LOADING` (`:4282`): same admin/ACL posture, one shard hop, and it works
during LOADING, which is when you most want to inspect stamps during a merge sync. It
also has the per-key precedent (`DEBUG OBJECT`, `debugcmd.cc:1202-1261`) and keeps
replication-critical `dflycmd.cc` untouched.

- [ ] **Step 1: Write the failing tests**

```cpp
TEST_F(MvccStoreTest, DebugMvccReportsValueState) {
  Run({"set", "k", "v"});
  auto resp = Run({"debug", "mvcc", "k"});
  EXPECT_THAT(resp.GetString(), testing::HasSubstr("state:value"));
  EXPECT_THAT(resp.GetString(), testing::HasSubstr("mvcc:"));
  EXPECT_THAT(resp.GetString(), testing::HasSubstr("origin:"));
}

TEST_F(MvccStoreTest, DebugMvccReportsAbsent) {
  EXPECT_THAT(Run({"debug", "mvcc", "nope"}).GetString(), testing::HasSubstr("state:absent"));
}

TEST_F(MvccStoreTest, DebugMvccVerifyReportsZeroMismatches) {
  for (int i = 0; i < 50; ++i)
    Run({"set", absl::StrCat("k", i), "v"});
  EXPECT_THAT(Run({"debug", "mvcc", "verify"}).GetString(), testing::HasSubstr("mismatches:0"));
}

TEST_F(BaseFamilyTest, DebugMvccIsRefusedWhenInactive) {
  auto resp = Run({"debug", "mvcc", "k"});
  EXPECT_THAT(resp.GetString(), testing::HasSubstr("active_replica"))
      << "must explain itself rather than reporting a bare absent";
}
```

- [ ] **Step 2: Run to verify it fails**

Expected: an unknown-subcommand error from `DEBUG`.

- [ ] **Step 3: Implement `DEBUG MVCC`**

Add a `MVCC` case to `DebugCmd::Run`'s dispatch (`debugcmd.cc:760-843`) and a handler
modelled on `DebugCmd::Inspect` (`:1202-1261`) — resolve the shard with
`Shard(key, shard_set->size())` and `ess.Await(sid, cb)`. Return a flat RESP simple
string:

```
state:value mvcc:<u64> ms:<u64> counter:<u32> origin:<hex16> shard:<n>
state:tombstone mvcc:<u64> ms:<u64> origin:<hex16> shard:<n>     # P4-5
state:absent shard:<n>
```

`DEBUG MVCC` with no key returns per-shard aggregates
(`entries`, `tombstones`, `bytes`, `clock_last`, `clock_ahead_ms`, `unstamped_writes`).
`DEBUG MVCC VERIFY` runs `TEST_VerifyMvccTable` on every shard and returns
`mismatches:<n>`. When `--active_replica` is off, return an error naming the flag.

- [ ] **Step 4: Add the INFO fields**

`INFO memory` (`server_family.cc:2821`): `mvcc_table_bytes`, `mvcc_entries`,
`mvcc_tombstones`. Inside the active-mode replication block: `mvcc_clock_ahead_ms`,
`mvcc_unstamped_writes`, `mvcc_stale_epoch`. Aggregate the stamper stats across shard
threads in `Metrics::Merge`.

> `mvcc_unstamped_writes` and `mvcc_clock_ahead_ms` are the two that matter
> operationally: the first says a read-mutation path is over-arming, the second says
> NTP stepped backwards and this node is about to win every conflict.

- [ ] **Step 5: Run to verify it passes, then unskip Task 9's pytest**

Remove the `@pytest.mark.skip` from `test_replicated_key_stamp_matches_origin` and run:

```bash
docker exec drakeydb-p2 sh -c \
  'cd /src && DRAGONFLY_PATH=/src/build-dbg/dragonfly /tmp/tv/bin/python -m pytest \
   tests/dragonfly/multimaster_test.py -q'
```

Expected: PASS, including the previously skipped acceptance test.

- [ ] **Step 6: Falsify, per D9**

Remove the `SetApplyMvcc` call again and run
`test_replicated_key_stamp_matches_origin`. It must now fail on differing `mvcc`
values — this is the end-to-end proof that the whole chain works. Record the failure
text, restore.

- [ ] **Step 7: Full gate and commit**

```bash
docker exec drakeydb-p2 sh -c 'cd /src/build-dbg && ninja -j4 && ctest -V -L DFLY'
git add -A && git commit -m "feat: add DEBUG MVCC and MVCC INFO fields (P4)"
```

---

### Task 12: The memory benchmark

**Files:**
- Create: `tests/dragonfly/multimaster_memory_test.py`

**Interfaces:**
- Consumes: `df_factory`, `active_args()`, the INFO fields from Task 11.
- Produces: a measured RSS delta and a regression guard on the bucket geometry.

**The trap this test exists to avoid: do not compare `table_used_memory`.**
`DbTable::table_memory()` returns `prime.mem_usage()` only, so the side table is
invisible to it and a benchmark reading it reports a ~0 delta and concludes, wrongly,
that MVCC is free. Compare `used_memory` — the side table is allocated from the shard's
`MiMemoryResource`, so it *is* inside `used_memory` and *is* subject to `maxmemory`.

Expected result from the geometry (bucket 504 B / 14 slots = 36.03 B/slot, ~41 B/key at
a 87.5% load factor): **~41 MB per 1M short keys**, rising to ~73-79 MB for 24-32 byte
keys because `kInlineLen = 16` means longer keys are heap-allocated a second time in
the side table. That is +26% to +45% RSS against a ~155-165 MB baseline.

- [ ] **Step 1: Write the benchmark**

```python
"""drakeydb P4: the memory cost of the MVCC side table.

Deliverable for the P4-1 gate. Also a regression guard: assertion 1 fails loudly if
anyone changes MvccStamp's size or the dash bucket geometry the estimate rests on."""
import pytest

from . import dfly_args
from .instance import DflyInstanceFactory
from .multimaster_test import active_args

KEY_COUNT = 1_000_000
VALUE_SIZE = 100


async def _mem(client) -> dict:
    info = await client.info("memory")
    return {k: int(info[k]) for k in ("used_memory", "used_memory_rss", "table_used_memory")
            if k in info}


@pytest.mark.slow
@pytest.mark.parametrize("key_len", [8, 16, 24, 32])
async def test_mvcc_table_memory_cost(df_factory: DflyInstanceFactory, key_len: int):
    prefix = "k" * (key_len - 8)  # DEBUG POPULATE appends an 8-char numeric suffix

    off = df_factory.create(proactor_threads=4)
    on = df_factory.create(**active_args(multi=False, proactor_threads=4))
    df_factory.start_all([off, on])
    c_off, c_on = off.client(), on.client()

    for c in (c_off, c_on):
        await c.execute_command("debug", "populate", KEY_COUNT, prefix, VALUE_SIZE)

    m_off, m_on = await _mem(c_off), await _mem(c_on)
    info_on = await c_on.info("memory")
    mvcc_bytes = int(info_on["mvcc_table_bytes"])
    entries = int(info_on["mvcc_entries"])

    assert entries == KEY_COUNT, f"expected a stamp per key, got {entries}"

    # 1. Geometry guard. 36.03 B/slot at 100% load, ~45 B/key at 80%. Outside this band the
    #    sizing argument in the design spec is stale and must be recomputed before P4-2.
    per_key = mvcc_bytes / entries
    assert 34 <= per_key <= 48, f"{per_key:.1f} B/key is outside the geometric bound [34, 48]"

    # 2. The metric must actually account for the delta. NOT table_used_memory -- see the
    #    docstring; DbTable::table_memory() returns prime.mem_usage() only.
    delta = m_on["used_memory"] - m_off["used_memory"]
    assert abs(delta - mvcc_bytes) < 0.15 * mvcc_bytes, (
        f"used_memory moved {delta} but mvcc_table_bytes reports {mvcc_bytes}")

    print(f"\nkey_len={key_len}: mvcc={mvcc_bytes / 2**20:.1f} MiB "
          f"({per_key:.1f} B/key), used_memory delta={delta / 2**20:.1f} MiB "
          f"({100 * delta / m_off['used_memory']:.1f}% over baseline)")
```

- [ ] **Step 2: Run it and record the table**

```bash
docker exec drakeydb-p2 sh -c \
  'cd /src && DRAGONFLY_PATH=/src/build-dbg/dragonfly /tmp/tv/bin/python -m pytest \
   tests/dragonfly/multimaster_memory_test.py -q -s'
```

Record the printed table in the SDD ledger and in `docs/PLAN.md`'s P4 section.
**If `per_key` lands outside [34, 48], stop and escalate** — the design spec's memory
argument and risk register entry #3 both depend on that band, and P4-5's tombstone cap
is sized from it.

- [ ] **Step 3: Commit**

```bash
~/.venvs/precommit/bin/pre-commit run --files tests/dragonfly/multimaster_memory_test.py
git add -A && git commit -m "test: measure the MVCC side table's memory cost (P4)"
```

---

## P4-1 exit gate

Before opening the PR, all of the following, with output pasted into the SDD ledger:

- [ ] `ninja -j4` completes warning-free (CI uses `-Werror`).
- [ ] `ctest -V -L DFLY` — 87 pre-existing plus `mvcc_test` and the new `multi_master_test` cases.
- [ ] `multimaster_test.py` at its P3 baseline of 41, plus everything added in P4-0 and P4-1.
- [ ] `replication_test.py` 43 passed, `replication_specific_test.py` 61 passed,
      `replication_resilience_test.py` 41 passed + 1 pre-existing xfail.
- [ ] Timing-adjacent new tests run 10-15x, pass rates recorded.
- [ ] `clang++ -Wthread-safety` over the concurrency-relevant TUs: **0 diagnostics.**
      `CMakeLists.txt:77-79` gates this on Clang and the container builds with g++, so it
      never runs in the ordinary local gate — and this PR adds a new thread-local plus a
      new per-`DbTable` structure.
- [ ] The journal golden-buffer test still passes with `--active_replica` off, proving
      the wire is byte-identical to upstream.
- [ ] `mvcc_unstamped_writes == 0` after a seeder write workload.
- [ ] Every falsification step recorded with its observed failure text.
- [ ] `pre-commit` clean across all changed files.

## Follow-ups this plan deliberately defers

- **P4-2** RDB persistence: `RDB_OPCODE_DF_MVCC = 221` (a new opcode, **not** a
  `DF_MASK` flag bit — DF_MASK's reader skips unknown flags without consuming their
  payload, so an old loader would misparse silently), the `ctime`-based fallback for
  aux-less keys, and the KeyDB `mvcc-tstamp` read branch.
- **P4-3** merge-on-full-sync LWW at `rdb_load.cc:3258`, plus a separate
  `SetMergeLww` — do **not** repurpose `SetOverrideExistingKeys`, which has three live
  callers, two of which must not get merge LWW.
- **P4-4** the streaming guard and per-key split. The compare must run **inside**
  `Transaction::RunCallback` under the key locks; a pre-dispatch check in
  `JournalExecutor::Execute` causes permanent divergence.
- **P4-5** tombstones: bit 63 of `packed`, the `DeleteReason` table from Task 8 gaining
  tombstone semantics for `kExplicit`/`kExpired`, TTL + GC, and a separate RDB section.
- **Open for the owner:** tombstone GC placement (idle task + inline cap, as designed,
  versus three lines in `EngineShard::RetireExpiredAndEvict`, which is more robust under
  load but touches a reserved file). Defaulted to the idle-task design.
