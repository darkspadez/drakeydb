# drakeydb Phase 3 — Origin-tagged Journal + Active Pair/Mesh — Design Spec

> **Superseded in places.** This spec is the design as approved at the start of P3. Several of
> its claims were corrected during implementation — most importantly, D-3's assertion that the
> `PrepareTransaction` hook covers *every* journaling path (EXEC's `dist_trans` and the
> non-atomic squash stub bypass it), and D-7's assumption that a refused peer always retries on
> the 500 ms loop (interactive `REPLICAOF` greets synchronously and has no retry). The
> authoritative as-built record, including behaviour changes and known limitations, is the
> **Phase 3 section of `docs/PLAN.md`**.



## Context

drakeydb is Jon's public fork of `dragonflydb/dragonfly` that ports KeyDB-style
active-replica / multi-master replication. P0–P2 are merged (`origin/main` =
`cd8e0602`); the canonical living plan is `docs/PLAN.md`.

P2 gave an active node the ability to **consume** from many masters (fan-in, merge
instead of flush, `SyncGate`, `PeerReplicationManager`). To keep loops impossible
before origins existed, P2 took a blunt shortcut: an active node **refuses every
replication consumer** at one choke point in `ReplConf`
(`server_family.cc:3692-3695`). So today a mesh cannot exist — nobody can replicate
*from* an active node.

P3 removes that shortcut safely. It gives every journal entry an **origin identity**,
teaches the outbound streamer to send a peer **only locally-originated entries**, and
replaces the blanket refusal with real **peer admission**. That combination is what
makes A↔B (and an N-node mesh) converge without an echo storm: each of the N×(N−1)
one-way streams carries only its sender's own writes, so an applied write is never
reflected back.

Outcome: two or more active nodes can be peered in both directions, cross-writes
converge, and a plain read-only replica hung off an active node still sees the whole
stream (all origins + expiry DELs).

---

## Decisions locked while grilling

| # | Decision |
|---|---|
| D1 | **Journal framing v2 lands in P3.** The deprecated varint (`serializer.cc:78`, written as `1`, read+discarded at `:228-230`) becomes a header: non-active nodes still write `1` (byte-identical to upstream); active nodes write `2` followed by `{varint origin_idx, varint mvcc, varint flags}`. `mvcc` is written as `0` until P4 — packed-uint encoding is self-describing, so P4 needs no second framing change. |
| D2 | **Wire `origin_idx` is sender-relative**, `0` = the sending node itself. `Op::ORIGIN (16)` control entries publish the sender's idx↔uuid dictionary to full-stream consumers, and are **recorded into the local journal** when `PeerRegistry` allocates a new index (so they ride the normal broadcast and the ring-buffer replay). On a **peer** link a non-zero origin_idx is a no-forward protocol violation → drop + rate-limited ERROR (this doubles as a live echo-storm assertion). |
| D2b | **An active node refuses a source that does not identify itself.** P2 accepted fan-in from a stock Dragonfly master (uuid simply absent); P3 reverses that in peer mode — no uuid, no origin, so no admission. KeyDB masters are unaffected (they do reply with a uuid), so P7 onboarding is untouched; plain-Redis fan-in was never a stated goal. |
| D3 | **Admission via two new REPLCONF k/v pairs.** `REPLCONF CLIENT-VERSION` stays `CURRENT_VER` — sending `65` there is unsafe (`server_family.cc:3753-3758` raw-casts client input to `DflyVersion`; 65 is out-of-range UB and satisfies every `>= VER6` gate at `rdb_save.cc:1782`, `snapshot.cc:101`, exactly as `node_identity.h:14-18` warns). Instead: every drakeydb replica sends `REPLCONF DRAKEY-VERSION 65`; peer-mode additionally sends `REPLCONF PEER 1`. `ReplConf`'s parser is strictly k/v pairs (`args.size() % 2 == 1` → error), so both fit the existing loop unchanged. |
| D4 | Active-node admission table: `DRAKEY-VERSION ≥ 65` **+** `PEER 1` → **peer stream** (self-origin only, expiry-DELs suppressed). `DRAKEY-VERSION ≥ 65` alone → **plain replica**, full stream. Anything else (stock Dragonfly, older drakeydb, Redis) → refused with the P2 error text. |
| D5 | **Peer PINGs are re-recorded with the peer's origin**, not skipped (`replica.cc:1244`). `Op::PING` has no payload, so the origin rides the memory-side `JournalItem` field only; the peer filter drops it from outbound peer streams while a plain sub-replica still receives it. |
| D6 | **Reciprocal-connect UUID tiebreak is pulled forward from P8 into P3.** P3 is the first phase where an active *pair* exists, and `CI{"DFLY", …}` carries no `CO::LOADING`, so a peer's `DFLY THREAD`/`FLOW` is rejected while we merge-load (`main_service.cc:1392`) and the peer retries on the 500 ms loop (`replica.cc:273`). Deterministic ordering replaces that churn. |
| D7 | **All three P2 carryovers are fixed in P3**: (c) shared-`dir` test identities, (d) `SyncGate` `ABSL_GUARDED_BY`, (e) `node_identity` uuid TOCTOU. (e) is a correctness item now — from P3 on, origin filtering keys on uuid identity, so a node silently changing uuid across restart is a data-correctness bug. |
| D8 | **One branch, one PR** — `feat/phase3-origin-journal` off `origin/main`, same cadence as P2: Sonnet 5 implements each task, Opus 5 does a spec-compliance review **and** a code-quality review per task, Opus 5 does a final whole-branch review before the PR. |

**Accepted (already-agreed) caveat, unchanged:** a peer full sync still merges with
`SetOverrideExistingKeys(true)` — last-loaded-wins until P6. So a reconnect/restart
full sync can overwrite our newer local values with a peer's older copies. P3's mesh
tests therefore assert that the nodes converge to an **identical** state, never that a
specific (newest) value wins. P6 closes this.

---

## Design

### D-1. Verified facts this design rests on (read at `cd8e0602`)

- `Op` = `{SELECT=6, EXPIRED=9 /*sunset*/, COMMAND=10, PING=13, LSN=15}`
  (`journal/types.h:18`). `16` is free.
- The deprecated varint is genuinely free: upstream `0697efbb2` ("remove
  shard_cnt/EXPIRED arguments in journal", 2026-03-06) turned it into a literal
  `Write(1u)`; the reader consumes it into `[[maybe_unused]] uint32_t unused`
  (`serializer.cc:228-230`). The reader has **no `default:` case** — an unknown opcode
  is silently parsed as `COMMAND`.
- **Every COMMAND blob is self-contained.** `JournalSlice::AddLogRecord` constructs a
  fresh `JournalWriter` per entry (`journal_slice.cc:120-121`), so `cur_dbid_` is
  always empty and a `SELECT` prefix is emitted inside every COMMAND record. Dropping
  a record therefore cannot strand a later record in the wrong db.
- **`JournalItem{lsn, time_ms, data}` is what the ring buffer stores**
  (`journal_slice.h:83`); `cmd`/`slot` live only on the transient `JournalChangeItem`.
  The partial-replay path builds a bare `JournalChangeItem` from
  `journal::GetEntry(lsn)` (`streamer.cc:216`), which returns **only `data`** — so a
  filter keyed off `JournalChangeItem` alone would pass everything on reconnect. This
  is PLAN.md Risk #1, and it forces origin onto `JournalItem`.
- **`Transaction` cannot reach a `ConnectionContext`** (zero matches in
  `transaction.h`; `OpArgs{shard, tx, DbContext}` doesn't carry one). The single place
  a `Transaction` is bound to a `ConnectionContext` is `PrepareTransaction`
  (`main_service.cc:861-894`), specifically `cmd_ctx->SetupTx(cid,
  dfly_cntx->transaction)` at `:881` — which also covers MULTI reuse via
  `MultiSwitchCmd`.
- **`JournalExecutor` owns one persistent `ConnectionContext`** per shard flow
  (`executor.cc:33-41`), and it already mutates it per entry (`SelectDb`,
  `executor.cc:91`). A peer link's origin is **constant for the link's lifetime**, so
  it is set once at flow setup rather than per entry.
- **Expiry DELs are indistinguishable from user DELs today.** `Op::EXPIRED` is dead on
  the write side; all three producers (`db_slice.cc:1479` `ExpireIfNeeded`,
  `db_slice.cc:245` `PrimeEvictionPolicy::Evict`, `db_slice.cc:1734`
  `FreeMemWithEvictionStepAtomic`) go through `RecordExpiryBlocking` → `RecordDelete`
  → `journal::RecordEntry(0, Op::COMMAND, dbid, …, "DEL")` (`tx_base.cc:68-70`).
  `RecordDelete` has 3 **non-expiry** callers (`hset_family.cc:1648`,
  `set_family.cc:1655`, `generic_family.cc:791`), so the flag belongs on
  `RecordExpiryBlocking`, not `RecordDelete`.
- An active node keeps master semantics: `SetShardStates` is skipped in peer mode
  (`replica.cc:264, 369`), so `EngineShard::is_replica_` stays `false` and
  `Heartbeat()`'s `!IsReplica()` expiry gate (`engine_shard.cc:846`) keeps firing.
- The master already learns a consumer's uuid: `REPLCONF UUID` →
  `conn_state.replication_info.repl_node_uuid` (`server_family.cc:3768`) → copied into
  the immutable `ReplicaInfo::node_uuid_` by `CreateSyncSession`
  (`dflycmd.cc:790,794`). `Greet()` sends `UUID` strictly before `capa dragonfly`
  (`replica.cc:406` vs `:456`), so it is always populated in time.
- `JournalStreamer::Config{should_sent_lsn, init_from_stable_sync,
  start_partial_sync_at}` is constructed at exactly one site,
  `DflyCmd::StartStableSyncInThread` (`dflycmd.cc:760-763`).
- `PeerRegistry` (`multi_master.h:60-89`) is append-only, fiber-safe, `kSelfIdx = 0`,
  no reclamation — exactly what origin indices need.

### D-2. Wire format (`journal/serializer.{h,cc}`)

`JournalWriter` gains an explicit `bool extended_framing` ctor flag (set from
`IsActiveReplica()` at the `AddLogRecord` call site — no hidden global inside the
serializer, so the round-trip test can drive both modes directly).

```
COMMAND entry, legacy (extended_framing == false)  — byte-identical to upstream
  [SELECT prefix] opcode(10) txid  1  payload
COMMAND entry, v2 (extended_framing == true)
  [SELECT prefix] opcode(10) txid  2  origin_idx  mvcc  flags  payload
```

`JournalReader` is **version-agnostic and always accepts both** (a non-active drakeydb
replica of an active master must parse v2): branch on the header value — `1` → legacy,
`2` → read the three varints, anything else → return an error instead of today's
silent misparse.

`Op::ORIGIN = 16` is added to the enum, written as `opcode(16) idx uuid-string`, with
an explicit case in the reader **and** in `TransactionData::AddEntry`
(`tx_executor.cc:58-76`, whose `default:` is a `DCHECK`).

ORIGIN entries are **journal-recorded**: when `PeerRegistry` allocates a new index, the
entry is recorded on every shard's journal (the journal is per-shard thread-local, so
this is a fan-out from the connection fiber that called `AddOrGet` — allocation happens
in `Greet()`, before full sync, so it does not contend with the sync gate). Three
consequences to implement deliberately:

- ORIGIN is recorded with origin `kSelfIdx`, so the self-origin filter would *pass* it
  to peers. The peer filter therefore needs an **explicit opcode-based drop** for
  `Op::ORIGIN` — it is the one control opcode peers must not receive (`Op::LSN` /
  `Op::PING` still always pass).
- ORIGIN entries occupy ring-buffer space and `ItemBytes` accounting. Negligible in
  volume (one per peer, ever), but the accounting must be correct.
- A consumer that partial-resyncs from a point after its ORIGIN entry was evicted from
  the backlog will miss that mapping. Acceptable in P3 — the dictionary is
  observability-only; nothing translates indices, because a plain replica is not active
  and never forwards. Note it in `PLAN.md` as a P8 item if the dictionary ever gains a
  functional consumer.

### D-3. Origin plumbing

- `journal/types.h`: `EntryBase += uint32_t origin_idx{0}; uint64_t mvcc{0}; uint8_t
  entry_flags{0};` and — critically — `JournalItem += uint32_t origin_idx{0}; uint8_t
  entry_flags{0};` so the ring buffer keeps them. `JournalChangeItem` inherits them via
  its embedded `JournalItem`. Audit `ItemBytes()` accounting in `journal_slice.cc`.
- `journal_slice.cc:107-134`: copy origin/flags from the `Entry` onto the
  `JournalItem`, and add a `JournalSlice::GetEntryMeta(lsn)` (or widen `GetEntry`) so
  the partial-replay path can filter without reparsing `data`.
- `conn_context.h` (~352, beside `is_replicating`): `+= uint32_t repl_origin_idx =
  PeerRegistry::kSelfIdx; uint64_t repl_mvcc = 0;`. Nothing resets `ConnectionContext`
  per command (`CommandContext::ReuseInternal`, `conn_context.cc:316-320`, only clears
  `cid_/tx_/tail_args_/start_cycle`), so the field persists per connection as intended.
- `journal/executor.{h,cc}`: `JournalExecutor` gains a `SetApplyOrigin(uint32_t)`
  writing `conn_context_.repl_origin_idx`. `DflyShardReplica` calls it once at flow
  setup with the idx `Replica::Greet()` obtained from
  `PeerRegistry::AddOrGet(peer_uuid)`. Non-peer flows keep the `kSelfIdx` default.
- `main_service.cc:881`: right after `cmd_ctx->SetupTx(...)`, propagate
  `dfly_cntx->repl_origin_idx` / `repl_mvcc` onto the `Transaction`. This is the single
  hook that covers **both** auto-journaling (`LogAutoJournalOnShard`) and all 66 manual
  `RecordJournal` call sites, since both funnel through
  `Transaction::LogJournalOnShard` (`transaction.cc:1660-1663`). The squashed-multi
  stub ctor (`main_service.cc:2414-2416`) inherits from its parent.
- `tx_base.cc`: add a transaction-aware `RecordDelete` overload and route the 3
  non-expiry callers through it, so a peer-applied command's derived DELs inherit the
  peer origin instead of being re-attributed to us.

**Uuid-less sources are refused in peer mode (D2b).** P2 accepted a source that sent no
`REPLCONF UUID` (`replica.cc:448-451` currently treats the exchange as optional and just
releases any stale identity claim). Without a uuid there is no origin to stamp, and
stamping `kSelfIdx` would make an active node forward that data to its peers, breaking
the no-forward rule. P3 turns that branch into a refusal: peer mode requires a uuid,
returning the same retryable `operation_not_permitted` as the existing self-uuid and
duplicate-uuid refusals, with a rate-limited ERROR.

Scope of the change: KeyDB masters **do** reply with a uuid
(`KeyDB/src/replication.cpp:1590-1594`), so P7 onboarding is unaffected; drakeydb
masters have replied with one since P1. What stops working is fan-in from a **stock
Dragonfly or plain Redis** master while `--active_replica` is on — never a stated goal,
but a documented reversal of a P2 behaviour, so it needs a `PLAN.md` note, a P3 record
entry, and a pytest case asserting the refusal. Non-active `REPLICAOF` is untouched.

### D-4. Streamer peer filter (`journal/streamer.{h,cc}`)

`Config += {bool peer_mode; std::string peer_uuid;}`, populated in
`DflyCmd::StartStableSyncInThread` from the `ReplicaInfo` already in hand
(`GetNodeUuid()`, plus the new peer/fork-version fields). The filter goes in the base
`JournalStreamer::ShouldWrite` so **both** the live path
(`ConsumeJournalChange`, `streamer.cc:128`) and the partial-replay path
(`MaybePartialStreamLSNs`, `streamer.cc:206-246`) get it:

- peer consumer: drop entries with `origin_idx != kSelfIdx`, drop entries with the
  expiry flag set, and drop `Op::ORIGIN` by opcode (it is recorded self-origin, so the
  origin test alone would pass it through).
- plain (full-stream) consumer: no filtering — it receives all origins, expiry DELs and
  the journal-recorded `Op::ORIGIN` dictionary entries.
- `Op::LSN` / `Op::PING` are **never** filtered — LSN bookkeeping and ack/partial-resume
  accounting depend on them. (A peer's PING re-record per D5 is dropped by the *origin*
  test, not by opcode.)

`RestoreStreamer`'s override is untouched (active mode is incompatible with cluster
mode at boot, so the two never coexist).

### D-5. Peer admission (`server_family.cc`, `dflycmd.{h,cc}`)

Replace the blanket refusal at `server_family.cc:3692-3695` with:
- two new k/v cases in the `ReplConf` loop — `DRAKEY-VERSION <n>` and `PEER <0|1>` —
  recorded on `conn_state.replication_info` and carried into `ReplicaInfo` by
  `CreateSyncSession` alongside the existing `node_uuid`;
- an admission check at the `CAPA dragonfly` case (the point where the sync session is
  created) implementing D4's table, keeping P2's error text for the refusal branch;
- a peer consumer must also present a UUID, and must not present **our own** uuid
  (mirrors the consumer-side refusal in `Greet()`).

Replica side: `ConfigureDflyMaster` (`replica.cc:535-550`) sends `DRAKEY-VERSION 65`
always and `PEER 1` when `IsPeerMode()`; `CLIENT-VERSION` is left at `CURRENT_VER`. A
pre-fork master answers `-ERR` to the new pairs — tolerated exactly like the existing
`REPLCONF UUID` fallback (`replica.cc:408-410`).

### D-6. Expiry flag

`RecordExpiryBlocking` (`tx_base.h:231`) — the sole wrapper used by all three expiry /
eviction producers and by nothing else — sets `entry_flags` bit0 on the recorded entry.
Origin stays `kSelfIdx`: an expiry is always locally generated. The peer filter drops
flagged entries; plain replicas still receive them.

### D-7. Reciprocal-connect tiebreak

**KeyDB's version of this is dead code.** `processReplconfUuid`
(`KeyDB/src/replication.cpp:1557-1599`) walks its own not-yet-connected masters and
does `if (FSameUuidNoNil(mi->master_uuid, c->uuid)) { if (memcmp(mi->master_uuid,
c->uuid, UUID_BINARY_LEN) < 0) freeClientAsync(c); }`. `FSameUuidNoNil`
(`replication.cpp:92-98`) is true only when the two uuids are **equal**, so the
`memcmp` on the next line compares a value with itself and is always `0` —
`freeClientAsync` never fires. The comparison was meant to be against `cserver.uuid`.
P3 ports the **intent**, and records this alongside the other verified-dead KeyDB
mechanisms in `PLAN.md`.

Ported mechanism, on the **master side**, at the admission point (mirroring KeyDB's
placement in the uuid handler):

1. A consumer presents `DRAKEY-VERSION ≥ 65`, `PEER 1` and its uuid `P`.
2. If `P` matches one of **our own** peer links that is **not yet established**
   (`link_status != up` or `sync_in_progress`, from `PeerReplicationManager::Summaries()`,
   which already carries `master_node_uuid` — KeyDB's `repl_state != REPL_STATE_CONNECTED`
   skip), this is a reciprocal connect.
3. Tiebreak on **`self_uuid` vs `P`** (the fix): if `self_uuid < P`, refuse this consumer
   with a retryable error; otherwise admit it. Both nodes evaluate the same predicate with
   the operands swapped, so **exactly one** side refuses and the pair never both-defers or
   neither-defers.
4. The refused side's `Greet()` fails and retries on the existing 500 ms loop
   (`replica.cc:273`), by which point the winner has left LOADING. P2 already softened
   peer-mode greet errors to `LOG_EVERY_T` (`replica.cc:304`), so this stays quiet.

This is a determinism layer **on top of** the existing retry, not a replacement: if both
sides check before either has started serving, the retry path still resolves it. All
state is local, so no new handshake field beyond D3's is needed.

### D-8. Carryovers (D7 in the decisions table)

- **(e) uuid TOCTOU** — `node_identity.cc` `PersistUuid`: create with
  `O_CREAT|O_EXCL`; on `EEXIST`, re-read and **adopt** the winner's uuid instead of
  keeping the locally generated one. Covered by a C++ unit that races two loads against
  one dir.
- **(d)** annotate `SyncGate`'s members `ABSL_GUARDED_BY(mu_)` and switch its lock
  sites to `util::fb2::LockGuard` (matching `PeerReplicationManager`).
- **(c)** give instances that pass `dir="{DRAGONFLY_TMP}/"` a distinct identity too
  (inject `--node_uuid` regardless of `dir=`, or per-instance subdirs) —
  `tests/dragonfly/instance.py`, plus the affected case at
  `replication_specific_test.py` ~1613.

### D-9. Tests

- **C++** — `journal_test.cc`: v1/v2 round-trip both directions, v2-reader-reads-v1,
  unknown-header rejection, `Op::ORIGIN` round-trip, and a **mixed-origin ring-buffer
  backlog** case proving the partial-replay path filters (PLAN.md Risk #1).
  `multi_master_test.cc`/`peer_replication_test.cc`: admission table, expiry flag,
  uuid-TOCTOU race.
- **pytest** (`multimaster_test.py`, extending the existing `# ---- Phase 2` section) —
  A↔B bidirectional convergence under seeder load, asserted with
  `Seeder.capture()` equality (`tests/dragonfly/seeder/__init__.py:41-49`) under
  `@assert_eventually`, gated on the existing `wait_for_peers` helper (line 261);
  echo-storm absence (sample `INFO stats` command counters twice after quiescence and
  assert a bounded delta); FLUSHALL flood parity; a plain sub-replica of A observing
  B-origin writes **and** expiry DELs; a simultaneous-attach reciprocal test; 3-node
  mesh convergence; node kill/restart (asserting mutual convergence, **not** newest-wins
  — see the accepted caveat).

---

## File map

| File | Change |
|---|---|
| `src/server/journal/types.h` | `Op::ORIGIN=16`; `EntryBase += origin_idx/mvcc/entry_flags`; **`JournalItem += origin_idx/entry_flags`** |
| `src/server/journal/serializer.{h,cc}` | `extended_framing` ctor flag; v2 COMMAND header; `Op::ORIGIN`; strict header validation on read |
| `src/server/journal/journal_slice.{h,cc}` | thread origin/flags onto `JournalItem`; metadata accessor for ring-buffer replay; `ItemBytes` audit |
| `src/server/journal/streamer.{h,cc}` | `Config += {peer_mode, peer_uuid}`; base `ShouldWrite` peer filter (origin, expiry flag, **and an explicit `Op::ORIGIN` opcode drop**) |
| `src/server/multi_master.{h,cc}` | record an `Op::ORIGIN` journal entry on every shard when `PeerRegistry` allocates a new index |
| `src/server/journal/executor.{h,cc}` | `SetApplyOrigin`; write `repl_origin_idx` into the persistent `conn_context_` |
| `src/server/journal/tx_executor.cc` | `case Op::ORIGIN` in `AddEntry` |
| `src/server/conn_context.h` | `+= repl_origin_idx, repl_mvcc` (~352); `+= DRAKEY-VERSION/PEER` on `ReplicationInfo` |
| `src/server/main_service.cc` | propagate origin onto the `Transaction` at `PrepareTransaction` (`:881`); stub-tx inheritance (`:2414`) |
| `src/server/transaction.{h,cc}` | origin/mvcc member + use in `LogJournalOnShard` (`:1660-1663`) |
| `src/server/tx_base.{h,cc}` | expiry flag in `RecordExpiryBlocking`; tx-aware `RecordDelete` overload |
| `src/server/replica.{h,cc}` | send `DRAKEY-VERSION`/`PEER`; pass peer origin idx to shard flows; peer-origin PING re-record (`:1244`); reciprocal tiebreak |
| `src/server/server_family.{h,cc}` | replace the blanket refusal with the admission table; new `ReplConf` cases |
| `src/server/dflycmd.{h,cc}` | `ReplicaInfo` fork-version/peer fields; thread them into `JournalStreamer::Config` |
| `src/server/node_identity.cc` | uuid TOCTOU fix |
| `src/server/peer_replication.h` | `ABSL_GUARDED_BY` annotations |
| `src/server/journal/journal_test.cc`, `multi_master_test.cc`, `peer_replication_test.cc` | new C++ cases |
| `tests/dragonfly/instance.py`, `multimaster_test.py` | shared-dir identity fix; P3 suite |
| `docs/PLAN.md`, `docs/UPSTREAM-SYNC.md`, `docs/superpowers/{specs,plans}/2026-08-23-phase3-*` | status, decisions, watchlist |

**Deliberately untouched:** `helio/`, `engine_shard.cc`, `dash.h`, `compact_object.*`.

## Global constraints

- Non-active behaviour stays **byte-identical to upstream**, including journal bytes:
  every new path is gated on `IsActiveReplica()`/`IsPeerMode()`, and the legacy framing
  branch must be proven byte-identical by test, not by inspection.
- Upstream-file diffs stay additive and flag-gated; every hook carries a `// drakeydb:`
  comment. No `std::mutex`/`std::thread` (fibers only). New files get
  `// Copyright 2026, drakeydb authors.`
- Keep `lag=` the trailing field of `slaveN:` lines.
- C++ clang-format 100 cols; Python black 100 cols; commit subject/body ≤ 100 chars;
  trailer `Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>`; `(P3)` suffix.
- Build/test inside the container (worktree at `/src`, `--security-opt
  seccomp=unconfined`, `ninja -j4`, absolute `DRAGONFLY_PATH`); a full `ninja` must
  precede `ctest -L DFLY`. Format on the host via `~/.venvs/precommit/bin/pre-commit`.

## Verification

Final gate, matching P2's D11:

1. Full `ninja -j4` warning-free, then `ctest -L DFLY` — expect 87/87 plus the new
   cases, 0 failures.
2. `pytest` with an absolute `DRAGONFLY_PATH`: `multimaster_test.py` (P1+P2+P3 suites),
   full `replication_test.py`, `replication_config_test.py`, full
   `replication_resilience_test.py`, full `cluster_test.py`,
   `replication_specific_test.py` — 0 failures, no flakes; the replication/multimaster
   suites re-run green on the final HEAD.
3. **Byte-identical proof**: a non-active node's serialized journal output is unchanged
   vs. `origin/main` (unit-level assertion, plus stock-Dragonfly interop unaffected).
4. **Echo-storm proof**: after A↔B seeder load quiesces, both nodes' command counters
   plateau within a bounded delta and `Seeder.capture()` matches.
5. `pre-commit run --files` clean over every changed file.
6. Opus 5 whole-branch review clean.

## Risks / watch items

| # | Risk | Mitigation |
|---|---|---|
| 1 | Partial-replay path filters on stale/default metadata → post-reconnect echo storm | Origin lives on `JournalItem` (ring buffer), not `JournalChangeItem`; dedicated mixed-origin backlog C++ test |
| 2 | v2 framing reaches a reader that can't parse it | Admission refuses `DRAKEY-VERSION < 65`; reader errors on unknown header instead of misparsing; legacy branch proven byte-identical |
| 3 | Origin lost on derived writes (SPOP→SREM, collection-emptying DELs) | Single hook at `PrepareTransaction` covers all 66 `RecordJournal` sites; the 3 direct `RecordDelete` callers audited in T4 |
| 4 | Reciprocal tiebreak both-defer / neither-defer | Deterministic uuid comparison; layered on the existing retry so neither outcome stalls; simultaneous-attach pytest |
| 5 | Restart full sync regresses newer local values (no merge LWW until P6) | Accepted caveat; tests assert mutual convergence, not newest-wins; documented in `PLAN.md` |
| 6 | Upstream churn in `serializer.cc` / `streamer.cc` | Already on the `UPSTREAM-SYNC.md` watchlist; framing diff kept ~30 lines in one low-churn file |
| 7 | Journal-recorded `Op::ORIGIN` leaks to peer links (it is self-origin, so the origin filter passes it) | Explicit opcode-based drop in the peer filter, with a C++ case asserting a peer link never receives `Op::ORIGIN` |
| 8 | D2b reverses a P2 behaviour (stock-Dragonfly fan-in stops working under `--active_replica`) | Deliberate; documented in `PLAN.md` + the P3 record, covered by a pytest asserting the refusal. KeyDB and drakeydb sources both send a uuid, so P7 is unaffected |
