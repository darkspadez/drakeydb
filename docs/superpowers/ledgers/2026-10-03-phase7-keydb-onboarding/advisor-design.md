# Phase 7 — advisor design (verified against `origin/main` @ `c60dfdb` and KeyDB v6.3.4)

## Corrections after review (2026-10-03)

The body below is the advisor's design as delivered. Where it disagrees with this section,
`decisions.md` and the Phase 7 spec win.

- **`KEYDB.MVCCRESTORE` exists, is live, and is applied** (decision 22). B.7's drop list is right
  for the `KEYDB.CRON` / `EXPIREMEMBER` family but must not grow this command: it carries data (one
  per key an active KeyDB loads from an RDB, RREPLAY-wrapped, `replication.cpp:5573-5594`), so it
  is translated to `RESTORE key <abs-ms|0> <dump> REPLACE ABSTTL`, stamped with its own `<mvcc>`
  argument and LWW-guarded on peer links (spec D-7a, Task 2.6). Its `<expire>` is an absolute ms
  deadline, or `INVALID_EXPIRE` = `LLONG_MAX` for a key with no TTL (not `-1`).
- **The KeyDB directive is `multi-master-no-forward`** (`config.cpp:2976`). B.10's
  `multimaster-no-forward yes` makes KeyDB abort at startup (`Bad directive or wrong number of
  arguments`, confirmed against v6.3.4).
- **U-8 is withdrawn**; the explorer's reading was right and the advisor's was wrong (see
  "Verification notes" below). `store_cb` sets `zparams.journal_update = true`
  (`geo_family.cc:654`) and `ZSetFamily::OpAdd` hand-journals the destination (`zset_family.cc:1963`
  for an empty result; `:2053-2072` for `DEL` then `ZADD`). Live: 16/16 destinations replicated.
- **Counter names**: process-wide counters carry the `multimaster_` prefix in the spec (D-13), e.g.
  `multimaster_keydb_author_overflow`; B.10's `keydb_author_overflow` and `keydb_rdb_*` are the
  short forms. `keydb_mvccrestore_failed` is new (per-link, decision 22).
- **Line drift** (anchors re-read at `c60dfdb`; the spec and the plan's anchor table use the right
  column):

| Advisor anchor | At `c60dfdb` |
|---|---|
| `main_service.cc:1540-1549` (unknown command into the NONE builder) | `:1538-1548` |
| `transaction.cc:807-840` (`ShouldDropForLww`) | `:807-842` |
| `journal/executor.cc:76-79` (dispatch), `:52-53` (rewrite gate) | `:80-83`, `:57-58` |
| `journal/executor.cc:36` (null-owner context) | `:34-42` |
| `journal/journal.cc:109-116` (applied-write floor flag) | `:114-121` |
| `replica.cc:555-559` (`RegisterOriginHash` via `AwaitBrief`) | `:545-549` |
| `replica.cc:585-596` (`DRAKEY-VERSION` / `PEER` tolerance) | `:573-584` |
| `replica.cc:1218-1227` (deferred skipped bytes) | `:1217-1227` |
| `multimaster_lww.cc:122-126` (`IncomingStamp` `DCHECK`) | `:134-140` |
| `main_service.cc:~2448-2452` (`EvalInternal` migration) | `:2458-2463` |
| `main_service.cc:1431` (`TAKEN_OVER` `conn()` deref) | `:1430` |
| `multi_master.cc:130-141` (boot limitations warning) | `:144-151` |
| `docs/multi-master.md:316-381` | `:311-381` |

Produced by the design advisor on 2026-10-03; decisions 13 (replica active expiry) and the
`capa activeExpire` gating were amended afterwards by the owner (see `decisions.md`). Line numbers
are at `c60dfdb` unless noted; KeyDB paths are `KeyDB/src/*` at tag v6.3.4. Items marked
[inference] / [unverified] were not proven by reading code.

## B.1 RREPLAY envelope parsing + nested unwrap

**Wire shape** (`replication.cpp:562-694`): when the master is `fActiveReplica`, `fSendRaw=false`
(`:584`) and every replica-bound command — including cron `PING` (`:4846-4861`), `REPLCONF GETACK *`
(`server.cpp:2918-2926`) and `MULTI`/`EXEC` (`multi.cpp:133-141`, one envelope each) — is fed as

```
*5\r\n$7\r\nRREPLAY\r\n$36\r\n<author uuid>\r\n$<n>\r\n<RESP array from catCommandForAofAndActiveReplication>\r\n$<k>\r\n<db>\r\n$<m>\r\n<mvcc>\r\n
```

(`:663-686`; db via `writeProtoNum`, mvcc via `incrementMvccTstamp(); getMvccTstamp()` `:650-654`).
No raw `SELECT` is fed in active mode (`:606` only when `fSendRaw`); db travels in the envelope. The
master's `master_repl_offset` advances by the outer bytes — exactly what `ReadRespReply`'s
`total_read` sums (`protocol_client.cc:296-315`), so offset accounting needs no special case.

**Forwarding nests envelopes**: `replicaReplayCommand` ends with
`alsoPropagate(cserver.rreplayCommand, …, c->argv, c->argc)` unless `multimaster_no_forward`
(`config.cpp:2976`, default **off**); `rreplay` is `noprop` (`server.cpp:1145`); the forwarded argv
is wrapped again with the forwarder's uuid/mvcc. KeyDB's bound: `REPLAY_MAX_NESTING 64`
(`replication.cpp:5297`).

**Where:** `Replica::ConsumeRedisStream` (`replica.cc:1111-1280`). Today `RREPLAY` is batched like
any command and `DispatchCommand` reports it unknown into the `ReplyMode::NONE` builder
(`main_service.cc:1540-1549`) — silent total data loss on every active-KeyDB link.

- New fork-only files `src/server/classic_replay.{h,cc}`:
  - `struct RreplayEnvelope { std::string_view uuid; std::string_view inner; std::optional<DbIndex> db; uint64_t mvcc; }`,
    `enum class RreplayParse { kOk, kBadArity, kBadUuid, kBadDb, kBadMvcc }`,
    `ParseRreplayEnvelope(const RespVec&, RreplayEnvelope*)` mirroring KeyDB's validation
    (argc ≥ 3; uuid = `IsValidNodeUuid`; optional db ≥ 0 and < `FLAGS_dbnum`; optional u64 mvcc).
  - `ClassicApplyRewrites(cmn::BackedArguments*)` (B.9), `IsKeyDbOnlyCommand(name)` (B.7),
    `AuthorDedup` (B.3), `ClassicAuthorMap` (B.2).
- In `ConsumeRedisStream`, keep the raw-command batch path untouched for non-envelope commands;
  add one branch before queuing: `if EqualsIgnoreCase(cmd, "RREPLAY")` → **flush the pending raw
  batch first** (ordering), then `HandleRreplay(args, depth=0)`, then `repl_offs_ += total_read`.
- `HandleRreplay`: parse → failure: count + `LOG_EVERY_T(ERROR, 60)` + **skip**; self-uuid → count,
  skip; dedup (B.3) → count, skip; otherwise parse `inner` with a **fresh**
  `facade::RedisParser(Mode::SERVER)` (never `parser_`) until fully consumed. Per inner command:
  `MULTI`/`EXEC` skip (as today `:1196`); `RREPLAY` → recurse depth+1, refuse at 64 (malformed);
  `PING` skip; `REPLCONF` (GETACK) skip [optionally poke the acks fiber]; `SELECT n` → set db;
  KeyDB-only → drop + count (B.7); `service_.FindCmd(name) == nullptr` → count
  `classic_unknown_cmds_dropped` + rate-limited warning; else dispatch (B.2). Leftover/partial inner
  bytes → malformed.
- **Inner db**: envelope db applies to the inner command. Add a `SelectDb` helper mirroring
  `JournalExecutor::SelectDb` (`journal/executor.cc:85-100`). Synthetic SELECTs are not stream bytes
  and must not touch `repl_offs_`.
- **Error policy — skip, never disconnect** (owner decision 14). KeyDB replies an error and continues
  for every validation failure; a disconnect cannot recover the bytes (a partial resync replays the
  identical envelope → livelock). Document: nonzero `rreplay_malformed` ⇒ re-run `REPLICAOF` to
  force a full resync.
- Keep `RREPLAY` out of the command registry (no client should send it; registering it would change
  `COMMAND` output on non-active nodes).

## B.2 Apply context per command

Plumbing: `ConnectionContext::{repl_origin_idx, repl_mvcc, repl_lww_guard}` (`conn_context.h:369-378`)
→ `PrepareTransaction` copies them onto each Transaction (`main_service.cc:891-894`) → guard veto
`Transaction::ShouldDropForLww` (`transaction.cc:807-840`, `IncomingStamp(repl_mvcc_, repl_origin_idx_)`),
self-guards in `OpMSet` (`string_family.cc:419`) / `OpDelV2` (`generic_family.cc:1488`, DEL and
UNLINK `:1556-1565`) → commit stamp `{entry.mvcc, OriginHash(origin_idx)}` (`journal.cc:171-178`,
`mvcc.cc:203`), floored for applied writes (`journal.cc:109-116`).

The guard path works with `DispatchCommand`: `JournalExecutor::Execute` is
`service_->DispatchCommand(ParsedArgs{*cmd_cntx}, cmd_cntx, ONLY_SYNC)` (`journal/executor.cc:76-79`)
plus the pre-dispatch `ApplyLwwRewrites` gated on `LwwGuardActive` (`:52-53`). `CommandContext` is-a
`ParsedCommand` is-a `cmn::BackedArguments` (`conn_context.h:433`, `facade/parsed_command.h:46`),
so `ConsumeRedisStream` can call `ApplyLwwRewrites(ctx)` directly. No need to route classic links
through `JournalExecutor`.

Per envelope command, peer mode only (a plain replica keeps mvcc 0 / origin 0):
1. `repl_origin_idx = ClassicAuthorMap::IdxFor(uuid)`, `repl_mvcc = envelope.mvcc` (0 → local mint,
   never guarded).
2. `repl_lww_guard` set once at link setup: `IsPeerMode() && IsActiveReplica() &&
   FLAGS_multi_master_stream_lww` (same expression as `replica.cc:1755`). Raw commands have mvcc 0 so
   the guard is inert for them.
3. If `LwwGuardActive`: `ApplyLwwRewrites(ctx)` then `ClassicApplyRewrites(ctx)`; dispatch
   `ONLY_SYNC`; restore `repl_mvcc = 0`, `repl_origin_idx = peer_origin_idx_`.
4. **EVAL/EVALSHA exception**: `RunSquashedMultiCb` has a `LOG(DFATAL)` tripwire for `IsLwwGuarded()`
   (`transaction.cc:1644-1648`); dispatch scripts with mvcc 0 (unguarded, local mint), update that
   comment and `docs/multi-master.md:316-381` (whose "classic links never guarded" premise ends).

Origin hash for inner authors: `MvccStamper::Commit` derives the hash from `origin_idx`
(`mvcc.cc:203`, `OriginHash` `:72-74`). `ClassicAuthorMap::IdxFor(uuid)`: link uuid →
`peer_origin_idx_`; else `PeerRegistry::AddOrGet(uuid)` + `RegisterOriginHash(idx,
NodeUuidHash(uuid))` on all proactors (same primitive as `replica.cc:555-559`), memoized per link;
**process-wide cap 256** distinct classic authors, after which the write is stamped with the link's
origin, counted (`keydb_author_overflow`) and warned. Non-self origins are refused by
`PassesPeerEchoFilter` (`journal/types.cc:51-57`) so no-forward holds. `IncomingStamp`'s
`DCHECK(hash != 0)` (`multimaster_lww.cc:122-126`) is satisfied because registration precedes dispatch.

## B.3 Author dedup

- `AuthorDedup`: process-wide `absl::flat_hash_map<std::string, Entry{uint64_t mvcc; uint64_t last_seen_ms;}>`
  under a mutex whose critical sections never yield (links run on different proactors).
- Semantics = KeyDB: `ShouldDrop(uuid, mvcc)` ⇔ `mvcc != 0 && entry.mvcc >= mvcc`;
  `Advance(uuid, mvcc)` after a well-formed, non-dup envelope is processed — including skipped inner
  `PING`/`MULTI` (KeyDB advances on `fExec || CLIENT_MULTI`). `mvcc == 0` never deduped, never advances.
- Shared across all classic links, plain or peer.
- Bound 4096 entries; evict smallest `last_seen_ms`.
- Partial PSYNC replays only unprocessed bytes → no false drops. Full resync: never reset (KeyDB's
  clock is monotone across restarts, `server.cpp:7270-7287`). Edge: a KeyDB restored from an old image
  with its clock behind its past can be shadowed until it catches up (KeyDB has the same exposure).
- Nested: the outer author watermark advances even when the inner was deduped.

## B.4 Partial PSYNC for classic links (`--classic_partial_psync`, default true)

Upstream tracks `master_repl_id` + `repl_offs_` from `+FULLRESYNC <id> <off>` (`replica.cc:1880-1889`);
`repl_offs_ += total_read` per message, skipped MULTI/EXEC bytes deferred until the preceding batch
applies (`:1218-1227`, `:1262`); acks fiber sends `REPLCONF ACK <repl_offs_>` every
`--replication_acks_interval` ms or 1024 bytes (`:1671-1696`); `PSYNC` always sends `-1`
(`:719-727`); `+CONTINUE` → `not_supported` (`:1922-1928`).

Master side: `masterTryPartialResynchronization` accepts `PSYNC <replid|replid2> <offset>` when
`repl_backlog_off <= offset <= repl_backlog_off+histlen` (`replication.cpp:894-969`), replies
`+CONTINUE <replid>` under psync2 (`:966-967`). Backlog default 1 MB (`config.cpp:2949`). An active
KeyDB's replid does **not** change when it syncs from its own masters (`:3051-3062`); it changes at
first-slave backlog creation (`:1472-1477`) and backlog expiry (`:4963`) — both handled by FULLRESYNC
fallback.

Design:
- Request `offs = (flag && !master_repl_id.empty() && classic_stable_reached_) ? repl_offs_ + 1 : -1`.
  `classic_stable_reached_` is set only after the loader finishes (or after a CONTINUE); it is not
  `passed_full_sync_` (DFLY path only, `:1037`, `:1090`). A drop mid-load → full resync.
- `+CONTINUE [<newid>]` → adopt `<newid>`, keep `repl_offs_`, **no LOADING / loader / flush / merge**,
  `R_SYNC_OK`, counter `classic_psync_partial_ok`.
- **Leftover hand-off** — the only silent-loss hole: `ParseReplicationHeader` reads into
  `InitiatePSync`'s local `io_buf` (`:716`, `:732`) while `ConsumeRedisStream` allocates a fresh one
  (`:1112`); bytes after `+CONTINUE\r\n` in the same read must be handed over.
- Fallback: non-`+` / `-ERR` / `-NOMASTERLINK` / `-LOADING` → existing bad-header reconnect; count
  `classic_psync_partial_fallback` when a partial was requested and FULLRESYNC came back.
- In-memory only.
- Hazards + catching tests: (a) leftover after CONTINUE; (b) off-by-one PSYNC offset; (c) deferred
  MULTI/EXEC bytes; (d) envelope bytes; (e) parser INPUT_PENDING across reads (upstream path);
  (f) mid-load drop → must FULLRESYNC.

## B.5 Greeting

- Accept `+OK active-replica` (and any `+OK …` suffix, e.g. `keydb-fastsync-save`) at
  `replica.cc:431-432` and `:610-611`. Without this nothing else is reachable against a real active KeyDB.
- `capa activeExpire` — owner amendment: send `REPLCONF capa activeExpire` as a separate command
  only after the master's capa reply advertised `active-replica` (both plain and peer links), and
  enable replica active expiry on such links (decision 13). Non-KeyDB handshakes stay byte-identical.
- Already fine: `REPLCONF UUID` parses KeyDB's bare `+<uuid>`; `DRAKEY-VERSION`/`PEER` get
  `-ERR Unrecognized REPLCONF option` (KeyDB `:1735-1737`), already tolerated (`replica.cc:585-596`).
  KeyDB rejects `PSYNC` while loading in active mode (`replication.cpp:1293-1296`) → existing retry.

## B.6 RDB loader

KeyDB layout (`rdb.cpp:1133-1200`): `[EXPIRETIME_MS][IDLE][FREQ]` → `AUX "mvcc-tstamp"` (active only)
→ type, key, value → `AUX "keydb-subexpire-key"`, `AUX "keydb-subexpire-when"` per member TTL
**after** the key. `RDB_VERSION 9`. `RDB_TYPE_CRON 64` = string script, ms start, ms interval,
len + keys, len + args (`:1078-1089`, load `:2577-2588`). Header aux `repl-masters`, `lua`.

- **Type 64**: not in `rdbIsObjectTypeDF` (`rdb_extensions.h:21-26`), no DF collision. Today
  hard-fails at `rdb_load.cc:2703-2711`. Parse-skip it, count `keydb_rdb_cron_skipped`, and
  `settings.Reset()` so the cron key's `mvcc-tstamp` does not leak onto the next key.
- **Subexpire aux**: skip in `HandleAux` without touching `settings`; count on `-when`; one
  `LOG_EVERY_T(WARNING, 60)` rollup. Converting to native member TTLs is a registered follow-up.
- **Aux noise**: `repl-masters` → recognized (VLOG); unknown aux → warn once per distinct name per
  loader. The bit-63 `mvcc-tstamp` warning (`:3193`) is per key and unthrottled — a KeyDB that synced
  from plain Redis stamps every key `OBJ_MVCC_INVALID` → rate-limit + counter `keydb_rdb_mvcc_invalid`.
- **D-8 precedence**: aux precedes the key while opcode 221 immediately precedes the type byte
  (`:2508-2523` vs `:2620`) → last-in-stream wins → opcode 221 wins. Pin with a test.
- **Graceful CHECKs**: `ParseReplicationHeader` `CHECK_EQ(kRdbEofMarkSize, token.size())` (`:1910`)
  and `InitiatePSync`'s EOF-token `CHECK`s (`:823-835`) → `LOG(ERROR)` + error code.

## B.7 KeyDB-only command drop

Inside the unwrap and on the raw path (a non-active KeyDB sends `PEXPIREMEMBERAT` raw):
`PEXPIREMEMBERAT` (the only member-expiry form KeyDB propagates, `aof.cpp:716-718`), `EXPIREMEMBER`,
`EXPIREMEMBERAT`, `PERSIST key subkey` (3-arg form only), `KEYDB.CRON`, `KEYDB.HRENAME`,
`KEYDB.NHSET`, `KEYDB.NHGET`, `KEYDB.MEXISTS`, `RREPLAY` (never dispatched). Counter +
`LOG_EVERY_T(WARNING, 60)`. Separately count unknown commands (`FindCmd == nullptr`).

## B.8 U-9

`EvalInternal`'s single-shard branch calls `conn_cntx->conn()->RequestAsyncMigration(...)` when
`sid != ss->thread_index()` (`main_service.cc` ~`:2448-2452`); `conn()` is null for
`ConsumeRedisStream`'s `ConnectionContext{nullptr, {}}` (`replica.cc:1113`) and `JournalExecutor`'s
(`journal/executor.cc:36`). Fix: `&& conn_cntx->conn() != nullptr`. Test
`EvalReplicatedApplyNoConnNoCrash`. Adjacent, out of scope, register: the `TAKEN_OVER` branch also
dereferences `conn()` (`main_service.cc:1431`).

## B.9 Guard audit — what KeyDB v6.3.4 propagates

Propagation = post-`rewriteClientCommandVector` argv → `catCommandForAofAndActiveReplication`
(`aof.cpp:682-726`).

| Client command | On the wire | Classification today | Verdict |
|---|---|---|---|
| `SET k v` | `SET k v` | kSingleKey | OK |
| `SET k v EX/PX/EXAT/PXAT n` (+ NX/XX/GET/KEEPTTL) | `SET k v PXAT <abs>` (`t_string.cpp:131-133`, `aof.cpp:688-712`) | kSingleKey | OK |
| `SET k v NX` / `XX` (no expiry) | verbatim | kSingleKey | **MISCLASSIFIED**: `SetCmd::Set` evaluates NX/XX first (`string_family.cc:971-990`) → strip NX/XX/GET under `LwwGuardActive` |
| `SET k v KEEPTTL` | verbatim | kSingleKey | acceptable (document) |
| `SETEX`/`PSETEX` | `SET k v PXAT <abs>` | kSingleKey | OK |
| `SETNX` | verbatim | kSingleKey | OK (`ApplyLwwRewrites` → SET) |
| `GETSET` | `SET k v` (`t_string.cpp:429`) | kSingleKey | OK |
| `GETDEL` | `DEL k` / `UNLINK k` | self-guarded | OK |
| `GETEX …` / `PERSIST` | `PEXPIREAT` / `PERSIST` / `DEL` | unguarded / self-guarded | acceptable |
| `EXPIRE` family | `PEXPIREAT k abs`; elapsed → `DEL`/`UNLINK` | unguarded / self-guarded | acceptable |
| `MSET` | verbatim | self-guarded | OK |
| `MSETNX` | verbatim (only when it set all) | drakeydb MSETNX skips all if any key exists (`string_family.cc:1783-1790`) | **MISCLASSIFIED** → rewrite `MSETNX`→`MSET` under guard |
| `DEL`/`UNLINK` | verbatim | self-guarded | OK |
| `RESTORE` | verbatim | kSingleKey + REPLACE | OK (relative TTL is D-28-class) |
| deltas (`INCR*`, `APPEND`, `H*`, `L*`, …) | verbatim | unguarded | by design |
| `INCRBYFLOAT` | `SET k v KEEPTTL` (`t_string.cpp:671-673`) | kSingleKey | OK |
| `RENAME`, `FLUSHALL/DB`, `EVAL*` | verbatim | unguarded | EVAL dispatched with mvcc 0 |
| expiry deletes | not propagated (`db.cpp:1980`) | — | each node expires itself |

## B.10 Metrics / flags / docs

- Per-link counters on `Replica` (atomics, via `ReplicaSummary`, `replica_types.h:14`) rendered in
  the plain-replica block and `RenderPeerReplicationInfo`: `rreplay_unwrapped`, `rreplay_malformed`,
  `rreplay_self_dropped`, `keydb_cmds_dropped`, `classic_unknown_cmds_dropped`,
  `classic_psync_partial_ok`, `classic_psync_partial_fallback`. Process-wide:
  `multimaster_rreplay_deduped`, `keydb_author_overflow`. Loader: `keydb_rdb_cron_skipped`,
  `keydb_rdb_subexpire_dropped`, `keydb_rdb_mvcc_invalid`. Prometheus `_total` mirrors. Avoid
  `ServerState::Stats` (its `sizeof == 31*8` static_assert is a merge hazard).
- Flag `--classic_partial_psync` (bool, true) in `classic_replay.{h,cc}`. Boot limitations warning
  (`multi_master.cc:130-141`) gains "KeyDB member TTLs and cron jobs are dropped on onboarding".
- Byte-identity exceptions to record: partial PSYNC (flag-gated), U-9 fix (ungated), loader accepts
  KeyDB type 64 / subexpire (strictly more permissive). Unwrap and replica active expiry only apply
  after an `active-replica` capa reply, which upstream cannot even handshake past.
- Docs: `docs/multi-master.md` "Onboarding from KeyDB" (topology, `multimaster-no-forward yes`,
  backlog sizing, cutover, expiry semantics, member TTL / cron loss, counters); `docs/differences.md`;
  `docs/UPSTREAM-SYNC.md` watchlist rows; `docs/ISSUE-REGISTER.md` close D-1, D-8, U-9; add
  TAKEN_OVER null-deref and subexpire-conversion follow-ups.

## Verification notes from the advisor

- Keep `WITH_SEARCH` ON (default): search gates tests at `src/server/CMakeLists.txt:76-85, 209`.
- KeyDB build: `make -j4 BUILD_TLS=no USE_SYSTEMD=no MALLOC=libc`; gcc-13 breakage [unverified];
  ladder: `CXXFLAGS='-include cstdint'`, then gcc-12.
- U-8 (GEORADIUS STORE replication): advisor reads it as **correct** as written (both commands
  `CO::NO_AUTOJOURNAL`, `geo_family.cc:779,782`; STORE writes via `ZSetFamily::OpAdd` in `store_cb`
  `:652-662` with no `RecordJournal`); an explorer read it as wrong. Live-test and record.

## Risks (ranked)

1. Handshake blocker — nothing testable against real KeyDB until fixed (P7-0).
2. Per-command dispatch throughput vs owner's "must keep up" bar (P7-1 Task 1.5/1.6).
3. Guard on classic links ends `multi-master.md`'s structural claim; EVAL must be unguarded.
4. Partial-PSYNC leftover hand-off — the one silent-loss hole; repeat its test ×10.
5. `PeerRegistry` indices are append-only — a flapping forwarder with rotating uuids consumes the
   256 cap until restart (documented).
6. KeyDB build on gcc 13 [unverified].
7. Plain-replica expiry semantics (addressed by decision 13).
8. Dedup watermark never resets — KeyDB's own exposure; documented.
