# drakeydb Phase 7 — KeyDB One-Way Onboarding — Design Spec

> Status: design locked with the owner 2026-10-03, revised the same day after the spec/plan review
> and the advisor's resolution of its forks (per-author dedup reservation, offset/watermark
> coupling, the release-build perf bar, the full-sync tail). Branch
> `feat/phase7-0-closeout-and-harness` off `origin/main` (`c60dfdb`); `src/` is unchanged since
> except the P7-0 implementation (Tasks 0.4-0.6 and 0.9, which also fixed U-10), the review fix round
> that followed it (U-12, a log fix in `ParseReplicationHeader`), the whole-branch review's loud
> log lines for the RREPLAY envelopes a build without P7-1 drops (`replica.cc`) and the adversarial
> pass's two fixes: the refusal of active-KeyDB links until P7-1 (decision 23, D-2) and U-13 (an
> empty command name in a classic stream). The other commits since are ledger/doc commits and the
> test/CI harness (Tasks 0.7, 0.8).
> Ships as five stacked sub-PRs — see [PR stack](#pr-stack).
>
> This is the next numbered phase: Phase 5 was superseded by P4-4, Phase 6 was delivered by P4-3,
> and the design spec's planned P4-5 (tombstones) shipped inside P4-3. `docs/PLAN.md` remains
> canonical for the fork as a whole; where this spec and PLAN.md's Phase 7 stub disagree on
> mechanism, **this spec is newer** (PLAN.md carries a "Corrections (2026-10-03)" block).
>
> Sources: `docs/superpowers/ledgers/2026-10-03-phase7-keydb-onboarding/decisions.md` (owner
> decisions, binding) and `advisor-design.md` (verified design, B.1-B.10). Owner decisions override
> the advisor where they differ. KeyDB references are `KeyDB/src/*` at tag v6.3.4.

## Context

P1-P4 made drakeydb an active-replica peer: identity, origin-tagged journal, MVCC stamps, merge and
streaming LWW, bounded tombstones. All of that is exercised between **drakeydb nodes** (DFLY
protocol) or against **plain Redis**. Phase 7 delivers the roadmap's last interop promise —
`docs/PLAN.md`: "One-way onboarding only: drakeydb consumes from live KeyDB masters" — and it turns
out the promise has never worked against a real active KeyDB:

- **A handshake Critical.** KeyDB answers every `REPLCONF` that carries a `capa` with
  `+OK active-replica` (`replication.cpp:1740-1753`). `Replica::Greet()` requires exactly `OK`
  through the strict-equality `ProtocolClient::CheckRespIsSimpleReply`
  (`protocol_client.cc:433-436`) at `replica.cc:432` and again at `:611`. Reproduced live against
  KeyDB v6.3.4 (ledger `progress.md`): a plain and an `--active_replica` drakeydb both fail
  `REPLICAOF` with `Bad response to "REPLCONF capa eof capa psync2": "+OK active-replica\r\n"` and
  receive no keys. P1's "inbound KeyDB works" was validated against plain Redis only.
- **A silent total-loss bug behind it.** Once the handshake passes, an active KeyDB wraps *every*
  replica-bound command (including cron `PING`, `REPLCONF GETACK *`, `MULTI`, `EXEC`) in
  `*5 RREPLAY <uuid> <cmd> <db> <mvcc>`. `Replica::ConsumeRedisStream` (`replica.cc:1111-1280`)
  batches it like any command; `Service::DispatchCommand` reports it unknown into a
  `ReplyMode::NONE` builder (`main_service.cc:1538-1548`) and the offset advances. The only trace is
  an `unknown_RREPLAY` row in INFO commandstats (`main_service.cc:1918-1927`).
- **No conflict resolution on classic links.** `ConsumeRedisStream`'s `ConnectionContext` sets only
  `repl_origin_idx` (`replica.cc:1124`): no `repl_mvcc`, no `repl_lww_guard`. Writes from a KeyDB
  peer are locally minted (they always win) and never LWW-guarded (`docs/multi-master.md:311-381`
  documents that as structural). ISSUE-REGISTER D-1.
- **Full syncs hard-fail on KeyDB data.** RDB object type 64 (`KEYDB.CRON` jobs) is rejected at
  `rdb_load.cc:2703-2711`; every member-TTL aux (`keydb-subexpire-*`) and `repl-masters` logs an
  `Unrecognized RDB AUX field` warning (`:3215`); a KeyDB that synced from plain Redis stamps keys
  `OBJ_MVCC_INVALID`, logged once per key (`:3193`).
- **An expiry gap.** An active KeyDB never propagates expiry deletes (`db.cpp:1966-1985`), so a
  plain drakeydb replica — whose heartbeat never expires (`engine_shard.cc:847`) — keeps expired
  keys that are never read, forever.
- **No partial resync.** `InitiatePSync` always sends `PSYNC <id> -1` (`replica.cc:719-727`) and
  `+CONTINUE` is `not_supported` (`:1922-1928`). Every link drop is a full resync.
- **Latent crashes on the same path.** Six `CHECK`s abort the process on a malformed full-sync tail
  (`replica.cc:823-835`) plus one on a malformed EOF token (`:1910`); a replicated single-shard
  `EVAL` dereferences a null `conn()` (`main_service.cc:2458-2463`, ISSUE-REGISTER U-9).
- **A correct full sync is treated as corrupt when the stream follows the RDB at once.** The bytes
  after the RDB are the start of the replication stream, but `RdbLoader::Load`'s first read is not
  bounded by `source_limit_` (`rdb_load.cc:2421-2427`; later reads are clamped, `:1203`), so it can
  swallow them and the tail `CHECK`s fire. A disk-based master flushes its buffered commands right
  behind the file, and KeyDB defaults to `repl-diskless-sync no` (`config.cpp:2826`). In a live
  probe against KeyDB v6.3.4, 2 of 6 trials logged `Bad full sync tail ... bytes left over` and
  looped in resync (the graceful form of the check); on unmodified `c60dfdb` the same condition
  aborts the process, an upstream latent crash.

Phase 7 closes all of these, and the outcome is the fork's interop claim: **a drakeydb node — plain
or `--active_replica` — attaches to one or more active KeyDB masters, completes the handshake,
unwraps and applies their stream without loss or duplication, resolves conflicts by LWW with
KeyDB-authored stamps, survives link drops with a partial resync, keeps up with KeyDB under
sustained load, and fails loudly (never silently) on what it cannot represent.**

Out of scope: serving replication *to* KeyDB; KeyDB as a replica of drakeydb; converting member TTLs
to native ones (documented as lost, with a registered follow-up); the overdue upstream sync (its own
PR after P7); the tombstone-lifecycle cluster D-14/16/20/27/29 (its own phase after P7).

## Decisions locked with the owner

Decided 2026-10-03. Numbers match `decisions.md`.

| # | Topic | Decision |
|---|---|---|
| 1 | Order | P4 close-out (docs + full P4 exit-gate baseline) as P7-0's first tasks, then P7 |
| 2 | Upstream sync (45 days overdue, upstream at v2.0.0) | Separate PR **after** P7 |
| 3 | Tombstone-lifecycle cluster (D-14/16/20/27/29) | Own phase after P7; stale "P4-5"/"PR-B" owners re-owned now |
| 4 | Branches/PRs | Stacked sub-PRs `feat/phase7-<n>-<slug>`; each opened against `main` when green, watched and driven to mergeable; the owner merges |
| 5 | Topology | Every drakeydb node attaches to KeyDB directly; v1 no-forward invariant kept; cutover via `REPLICAOF REMOVE` |
| 6 | RREPLAY unwrap | On **every** classic link, plain replicas included |
| 7 | Streaming LWW | Stamp KeyDB writes `{envelope mvcc, author-uuid hash}`; audit KeyDB's propagated forms; then guard ON under `--multi_master_stream_lww` |
| 8 | KeyDB extras | Skip + rate-limited warn + counters (RDB type 64 CRON, `keydb-subexpire-*` aux, EXPIREMEMBER family, `KEYDB.*`); member TTLs documented as lost. `KEYDB.MVCCRESTORE` is **not** one of the drops: it is applied (decision 22, D-7) |
| 9 | KeyDB mesh | N KeyDB masters; per-author-uuid monotonic mvcc dedup shared by all classic links, bounded; docs recommend KeyDB `multi-master-no-forward yes`; nested unwrap <= 64; self-uuid drop |
| 10 | Partial PSYNC | In scope for **all** classic links behind `--classic_partial_psync` (default true); in-memory only (restart means full resync) |
| 11 | Apply path | Per-command dispatch for RREPLAY (own mvcc/origin each); raw classic streams keep squashing |
| 12 | Perf bar | **Must keep up with KeyDB**: bounded lag under sustained load, else optimize within P7 |
| 13 | Expiry gap | A plain replica whose master advertises `active-replica` **runs active expiry itself** (KeyDB's model) |
| 14 | Malformed envelope | Skip + count + rate-limited warn; never disconnect; offsets stay exact |
| 15 | Metrics | Per-link `rreplay_*` / `keydb_*` / `classic_*` fields in the link's INFO block + Prometheus `_total` mirrors; process-wide counters under `multimaster_` |
| 16 | KeyDB binary | v6.3.4 built from source; `KEYDB_SERVER_PATH`; tests skip locally without it; **gate runs set `KEYDB_REQUIRED=1`**, which turns a missing binary into a failure |
| 17 | CI | Minimal fork-owned `.github/workflows/drakeydb-ci.yml` (KeyDB build + suite) pulled forward into P7-0 |
| 18 | Roles | Implementer Sonnet, reviewer Opus, advisor Fable; effort xhigh |
| 19 | Rigor | Per task: brief, implement + falsify, spec+quality review, fix loop. Per sub-PR: whole-branch review + adversarial pass |
| 20 | Ledger | `docs/superpowers/ledgers/2026-10-03-phase7-keydb-onboarding/`, committed |
| 21 | Verification | Build in the session container; per-task targeted tests; full gate per sub-PR |
| 22 | `KEYDB.MVCCRESTORE` (owner, 2026-10-03; closes [O-1](#open-design-items)) | **Applied, LWW-guarded**: translated to `RESTORE key <ttl> <payload> REPLACE ABSTTL`, stamped with the command's own `<mvcc>` plus the envelope author's uuid hash, guarded on peer links, verbatim and unstamped on plain replicas; an unloadable payload is skipped, counted (`keydb_mvccrestore_failed`) and warned with a rate limit. Task 2.6 (P7-2) |

**Clarifications (2026-10-03, from the spec review and the advisor; not new decisions, recorded in
`decisions.md`).** *9:* `multi-master-no-forward yes` is recommended only when every drakeydb node
is `--active_replica --multi_master` and attached to every KeyDB master; otherwise KeyDB's
forwarding stays on and the dedup absorbs the duplicates (D-13). *12:* "keep up" is measured on
release builds as an apply/produce rate ratio plus a lag and drain bound, against a KeyDB-replica
comparator (D-12). *14:* "malformed" is per nesting level: a malformed inner envelope does not
advance its own watermark, the enclosing one still does (D-3, D-5). *22:* when a
`KEYDB.MVCCRESTORE`'s own `<mvcc>` is unusable the envelope's is used, and a type-64 (cron) payload
is a counted drop rather than a failure (D-7a).

**Owner amendments to the advisor design** (the advisor's B.5 / B.10 text predates them):
`capa activeExpire` is sent as a *separate* `REPLCONF`, only after an `active-replica` capa reply,
on plain **and** peer links (D-2); plain replicas of an active KeyDB run active expiry, so the
heartbeat's expiry is split out of `RetireExpiredAndEvict` behind a per-shard flag (D-9); partial
PSYNC is in scope (D-8); the perf bar is bounded lag with a squasher micro-batch fallback (D-12);
`KEYDB_REQUIRED` and `drakeydb-ci.yml` (D-14).

### Open design items

None. The one that was open is decided:

**O-1. `KEYDB.MVCCRESTORE` handling — decided 2026-10-03: Option A (decision 22).** The command is
in v6.3.4's command table (`server.cpp:1168`; handler `mvccrestoreCommand`, `cluster.cpp:5206`):
`KEYDB.MVCCRESTORE key <mvcc> <expire> <DUMP payload>` sets the key's mvcc to `<mvcc>` and merges
with `dbMerge` (`db.cpp:376-390`: overwrite when `old_mvcc <= incoming`). `<expire>` is an absolute
ms deadline, and a key with no TTL is sent as `INVALID_EXPIRE` (`LLONG_MAX`), not `-1` (D-1.15). It
is **live**: an active KeyDB that has replicas emits one per key it inserts while loading an RDB —
including a merge full sync from one of its own masters — via `replicationNotifyLoadedKey`
(`replication.cpp:5573-5594`, called from `rdb.cpp:2942`), RREPLAY-wrapped. It is data-carrying, so
unlike the `KEYDB.CRON` / `EXPIREMEMBER` family a drop is silent key loss on a KeyDB mesh resync.
The options were:

- **(A) Apply — chosen.** Translate to `RESTORE key <ttl> <payload> REPLACE ABSTTL`, stamp it with
  its own `<mvcc>`, LWW-guard it on peer links; a plain replica applies it unguarded.
- **(B) Drop** + count + rate-limited warn, like the other `KEYDB.*` extras, and document the loss.
  Rejected: the loss is silent data loss, not a missing feature.

How (A) is built: D-7 (translation, failure accounting), D-4 and D-6 (stamp source, guard path),
D-13 (counter). It lands as Task 2.6 in P7-2. Until then — P7-1 and earlier — the command is not in
the D-7 drop list and reaches the unknown-command path (counted, warned, not applied).

## Corrections to PLAN.md's Phase 7 stub

| The stub / ledger said | Corrected to | Why |
|---|---|---|
| Greeting adds `capa activeExpire` — "KeyDB refuses active full sync without it" (`replication.cpp:1798`) | KeyDB does not refuse; `putSlaveOnline` only logs a deprecation warning (`replication.cpp:1798-1803`). drakeydb sends it as a separate `REPLCONF`, only after an `active-replica` reply, because it implements replica active expiry (D-2, D-9) | Reading `putSlaveOnline`; KeyDB's own replicas send it in the same `REPLCONF` as `eof`/`psync2` (`:3557-3566`) |
| "per-link monotonic mvcc dedup" | Per **author** uuid, process-wide, shared by every classic link (D-5) | KeyDB's `g_mapremote` (`replication.cpp:5369`) is keyed by author uuid, not by link |
| "apply-context from envelope" | Cannot ride the squasher: `repl_mvcc` is copied once per batch (`main_service.cc:1815-1816`, `multi_command_squasher.cc:114-115`). RREPLAY commands dispatch one by one (D-3, D-4) | Per-batch stamp cannot carry per-command mvcc |
| "KeyDB RDB `mvcc-tstamp` parse (shared with P6)" | Already delivered by P4-2 (`HandleAux`, `rdb_load.cc:3160-3211`). P7 adds only type 64, subexpire aux, aux noise (D-10) | Stub predates P4-2 |
| "Verify: docker `eqalpha/keydb`; suite gated on image availability" | KeyDB v6.3.4 built from source, `KEYDB_SERVER_PATH`, `KEYDB_REQUIRED=1` in gates (D-14) | Owner decision 16 |
| `drakeydb-ci.yml` belongs to Phase 9 | A minimal version is pulled forward into P7-0 (D-14) | Owner decision 17 |
| (not in the stub) expiry | An active KeyDB never propagates expiry deletes (`db.cpp:1966-1985`): a plain replica must expire itself (D-9) | Owner decision 13 |
| (not in the stub) handshake | **New Critical:** drakeydb cannot complete a handshake with an active KeyDB at all (Context, D-2) | Live reproduction |

**Corrections found while re-verifying the ledger against the tree:**

- The recommended KeyDB directive is **`multi-master-no-forward yes`** (`config.cpp:2976`,
  `keydb.conf:2040`), not `multimaster-no-forward` — which makes KeyDB abort at startup (`Bad
  directive or wrong number of arguments`, confirmed against v6.3.4). KeyDB logs that it *requires
  a mesh topology* when set (`config.cpp:2705-2710`): every KeyDB master must replicate from every
  other.
- ISSUE-REGISTER U-8 (GEORADIUS `STORE` "never replicates") is wrong: `geo_family.cc:652-657` passes
  `journal_update = true` and `ZSetFamily::OpAdd` hand-journals (`zset_family.cc:1963` for an empty
  result; `:2053` opens the branch that records `DEL`, `:2055`, then `ZADD`, `:2072`). Task 0.1's
  live re-test (16/16 destinations replicated) withdrew it.
- KeyDB refusing PSYNC while loading in active mode is `startBgsaveForReplication` failing
  (`replication.cpp:1293-1296`), surfacing as `-ERR BGSAVE failed, replication can't continue`
  (`:1341`) — handled by the existing bad-header reconnect, not a distinct reply.
- `KEYDB.MVCCRESTORE` is in v6.3.4's command table and is live on the wire — the earlier ledger
  note to the contrary was wrong and must not be carried forward. Decided: it is applied
  (decision 22, D-7). Its `<expire>` field is an absolute ms deadline with `INVALID_EXPIRE`
  (`LLONG_MAX`) for "no TTL", not `-1` as an earlier draft of O-1 assumed (D-1.15).
- The advisor's `[unverified]` gcc-13 KeyDB build risk is closed: v6.3.4 built cleanly on gcc 13.3.
- Line anchors in `advisor-design.md` drifted in places (thirteen are listed, with the correct
  values, in that file's "Corrections after review" table); every anchor in this spec and in the
  plan's "Verified anchors" table was re-read at `c60dfdb`. The second review found three more: the
  eviction call is `engine_shard.cc:964` (not `:962`), `RedisServer` is `instance.py:563`, and the
  flush calls in `InitiatePSync` are `replica.cc:769` (`FlushSlots`) and `:771` (`FlushAll`).
- KeyDB mints a **new node uuid at every start** (`uuid_generate`, `server.cpp:4082`; it is not
  persisted), so each KeyDB restart is a new author and, for a link, a new `PeerRegistry` index
  (D-4, Risk 7).
- Bytes behind a full sync's RDB are replication-stream bytes, not corruption: the existing tail
  `CHECK`s encode the opposite assumption (Context, D-10).

## Design

### D-1. Verified facts this design rests on (read at `c60dfdb`)

1. **Handshake.** KeyDB sets `fCapaCommand` for any `REPLCONF` containing `capa` and replies
   `+OK` plus ` active-replica` when `fActiveReplica`, plus ` keydb-fastsync-save` only if the
   replica sent `capa keydb-fastsync` and fast sync is enabled (`replication.cpp:1740-1753`).
   `REPLCONF capa activeExpire` therefore also answers `+OK active-replica`. `REPLCONF UUID` answers
   a bare `+<uuid>` (already parsed, `replica.cc:439-482`); unknown options (`DRAKEY-VERSION`,
   `PEER`) answer `-ERR Unrecognized REPLCONF option` (already tolerated, `replica.cc:573-584`).
2. **Wire shape.** With `fActiveReplica`, `fSendRaw = false` (`replication.cpp:584`) and every
   replica-bound command goes out as `*5\r\n$7\r\nRREPLAY\r\n$36\r\n<uuid>\r\n$<n>\r\n<RESP
   array>\r\n$<k>\r\n<db>\r\n$<m>\r\n<mvcc>\r\n` (`:562-694`; db and mvcc via `writeProtoNum`; mvcc
   from `incrementMvccTstamp`). No raw `SELECT` is fed in this mode (`:606` only when `fSendRaw`).
   Cron `PING` (`:4846-4861`), `REPLCONF GETACK *` (`server.cpp:2918-2926`) and `MULTI`/`EXEC`
   (`multi.cpp:133-141`, one envelope each) are wrapped like data. The master's offset advances by
   the outer bytes, which is exactly what `ReadRespReply` sums as `total_read`
   (`protocol_client.cc:293,308,314`), so offsets need no special case. Inner payload comes from
   `catCommandForAofAndActiveReplication` (`aof.cpp:682-726`).
3. **Nesting.** `replicaReplayCommand` (`replication.cpp:5371`) ends with
   `alsoPropagate(rreplayCommand, ...)` unless `multi-master-no-forward` (default off,
   `config.cpp:2976`; `:5507-5508`), so the forwarded envelope is wrapped again with the
   forwarder's uuid/mvcc. `REPLAY_MAX_NESTING 64` (`:5297`): the 65th push fails.
4. **Dedup and own-uuid.** KeyDB drops an envelope when it carries its own uuid (`:5435`) or when
   `mvcc != 0 && remote.mvcc >= mvcc` (`:5453`), and advances the author's watermark only when the
   inner command executed or opened a MULTI (`:5485`) — with the *outer* author advancing even when
   the inner envelope was deduped (the inner `rreplay` is itself an executed command).
   `db` is validated `< dbnum`, uuid via `uuid_parse` (36 chars); `argc >= 3` (`:5389-5433`).
5. **Expiry.** `propagateExpire` skips the propagate when `fActiveReplica` (`db.cpp:1980`).
   drakeydb's heartbeat never expires on a replica shard (`engine_shard.cc:847`); only the read
   path lazily does, gated by `--replica_delete_expired` (default true, `db_slice.cc:60`, gate at
   `:2097-2098`).
6. **drakeydb stream apply.** One parsed message at a time; MULTI/EXEC skipped, everything else
   queued into a batch of up to `max_squash_cmd_num` and dispatched through
   `DispatchSquashedBatch` (`replica.cc:1169-1270`). `repl_offs_` advances per applied command;
   skipped bytes are deferred behind a non-empty batch (`:1217-1227`, `:1262`). The `io_buf` is
   local (`:1112`); `InitiatePSync` has its own `io_buf{128}` (`:716`).
7. **Apply-context plumbing.** `ConnectionContext::{repl_origin_idx, repl_mvcc, repl_lww_guard}`
   (`conn_context.h:369-378`) are copied onto each `Transaction` in `PrepareTransaction`
   (`main_service.cc:891-894`); the veto is `Transaction::ShouldDropForLww`
   (`transaction.cc:807-842`) plus self-guards in `OpMSet` (`string_family.cc:419`) and `OpDelV2`
   (`generic_family.cc:1488`). `CommandContext` is a `facade::ParsedCommand` is a
   `cmn::BackedArguments` (`conn_context.h:433`, `facade/parsed_command.h:46`), so
   `ApplyLwwRewrites(cmn::BackedArguments*)` (`multimaster_lww.cc:65`) can be called on it
   directly — `JournalExecutor` is not needed (it does the same, `journal/executor.cc:47-62`).
8. **The guard vocabulary** is seven names: `DEL`, `GETDEL`, `GETSET`, `MSET`, `RESTORE`, `SET`,
   `SETNX` (`multimaster_lww.cc:37-42`; `PEXPIREAT`/`PERSIST` are deliberately absent).
   `SET ... NX|XX` evaluates the condition *before* anything else (`string_family.cc:971-992`), and
   drakeydb's `MSETNX` skips every key if one exists (`string_family.cc:1763-1798`) — both
   reproduce the author's command, not its result, and `MSETNX` is unclassified.
9. **EVAL under the guard.** `RunSquashedMultiCb` carries a `LOG(DFATAL)` tripwire for
   `IsLwwGuarded()` because `repl_mvcc_` is batch-level there (`transaction.cc:1624-1648`).
   `IsLwwGuarded()` is `guard && mvcc != 0` (`transaction.h:400-401`, `multimaster_lww.h:88`), so a
   dispatch with the guard bit off never trips it, whatever its mvcc (D-4.4).
10. **Loader.** Opcode 221 is consumed immediately before the type byte (`rdb_load.cc:2508-2523`);
    `mvcc-tstamp` / `keydb-subexpire-*` aux come through `HandleAux` (`:3054-3219`); KeyDB's aux
    precedes the key, its subexpire aux follows it (`rdb.cpp:1167`, `:1194-1195`). Type 64 is not in
    `rdbIsObjectTypeDF` (`rdb_extensions.h:21-26`), so there is no DF collision (DF types are
    30-37).
11. **PSYNC.** `ParseReplicationHeader` overwrites `repl_offs_` and `master_repl_id` the moment a
    `+FULLRESYNC` line parses (`replica.cc:1880-1889`), *before* the `$` line validates.
    Master side: `masterTryPartialResynchronization` accepts when
    `repl_backlog_off <= offset <= repl_backlog_off + histlen` (`replication.cpp:894-969`) and
    replies `+CONTINUE <replid>` under psync2 (`:966-967`); backlog defaults to 1 MB
    (`config.cpp:2949`).
12. **`PeerRegistry::AddOrGet`** assigns an append-only index and fans an `Op::ORIGIN` journal
    entry to every shard (`multi_master.cc:226-278`), so every distinct author costs a journal LSN
    per shard for the process lifetime.
13. **`ServerState::Stats`** has `static_assert(sizeof(Stats) == 31 * 8)` (`server_state.cc:67`);
    new counters stay out of it.
14. **KeyDB's own propagated forms** (B.9 audit, `aof.cpp:682-726`, `t_string.cpp:117-160`): `SET`
    with `EX/PX/EXAT/PXAT` propagates as exactly `SET k v PXAT <abs>` (NX/XX/GET/KEEPTTL dropped);
    without an expiry, `SET k v NX|XX|KEEPTTL` propagates verbatim and `GET` is stripped;
    `SETEX`/`PSETEX`/`GETSET`/`INCRBYFLOAT` propagate as `SET`; `MSETNX` propagates verbatim;
    `EXPIRE*` as `PEXPIREAT`; member TTLs as `PEXPIREMEMBERAT`. KeyDB propagates only commands
    that dirtied the dataset (`server.cpp:4618-4648`): a failed `SET ... NX|XX` returns before
    `dirty++` (`t_string.cpp:104-108`) and a failed `MSETNX` returns `0` before any `setKey`
    (`:556-563`), so every conditional that reaches the wire already *succeeded on its author*.
15. **`KEYDB.MVCCRESTORE`** (`server.cpp:1168`, arity 5; `mvccrestoreCommand`, `cluster.cpp:5206`;
    emitter `replicationNotifyLoadedKey`, `replication.cpp:5573-5594`, called from `rdb.cpp:2942`,
    which returns unless `fActiveReplica` and a replica is attached). On the wire it is
    `KEYDB.MVCCRESTORE <key> <mvcc> <expire> <DUMP payload>` inside an RREPLAY envelope (the db
    travels in the envelope). `<mvcc>` is the key's own mvcc (`mvccFromObj`), not the envelope's.
    `<expire>` is the key's absolute ms deadline, or `INVALID_EXPIRE` = `LLONG_MAX` (`expire.h:8`)
    when it has none; the handler applies a TTL only for `expire >= 0`, and live-checked on v6.3.4
    `-1` and `INVALID_EXPIRE` both leave no TTL while `0` expires the key at once. The payload is
    `createDumpPayload`: type byte, value, an AUX `mvcc-tstamp` field, the 2-byte RDB version (9)
    and an 8-byte CRC64. drakeydb's `RESTORE` loads such payloads (live-checked on an unmodified
    `c60dfdb` build: string raw and int, list, hash, set intset and hashtable, zset and stream,
    small and large; the trailing aux is ignored), with these edges: on a replicated-apply context
    it skips the CRC and checks only length and `version <= RDB_VERSION` (`GetRdbVersion` rejects
    `size <= 10`, the footer's size; `journal_emulated` becomes `ignore_crc`, `generic_family.cc:69`,
    `:2892`); `ABSTTL 0` means no TTL, `-1` is an error, and
    `LLONG_MAX` is accepted but clamped to a TTL about 8.5 years out; with `REPLACE` it deletes the
    resident key *before* it parses the payload (`OpRestore`, ISSUE-REGISTER D-31); an elapsed
    absolute TTL deletes the resident key and creates nothing. `rdbIsObjectTypeDF` rejects
    `RDB_TYPE_CRON` (64, KeyDB `rdb.h:96`), the one KeyDB-only object type.
16. **KeyDB's node uuid is minted anew at every start** (`uuid_generate`, `server.cpp:4082`; it is
    not persisted), so a restarted KeyDB is a different author and a different link uuid.
17. **KeyDB's mvcc is `(ms << 20) | counter`** (`MVCC_MS_SHIFT 20`, `server.h:960`;
    `incrementMvccTstamp`, `server.cpp:7270-7287`): the high bits are the author's wall clock when
    it minted the stamp, which is what the classic-link skew estimate reads (D-13).

### D-2. Handshake and capability negotiation (B.5, owner amendment)

New fork-only `src/server/classic_replay.{h,cc}`:

```cpp
struct CapaReply {
  bool ok = false;
  bool active_replica = false;
  bool keydb_fastsync_save = false;
};
CapaReply ParseCapaReply(std::string_view simple_string);   // "OK" or "OK <word> ..."
```

`ok` iff the first token is exactly `OK` (case-sensitive, as today); tokens after it are
space-separated, unknown words ignored. `Greet()` replaces its two strict checks (`replica.cc:432`,
`:611`) with `ParseCapaReply` over the single-string response; a non-`OK` first token is still a
bad response. Every other `CheckRespIsSimpleReply("OK")` in `Greet()` is unchanged.

- `active_replica` is recorded per `Greet()` in a `Replica` member (`master_active_replica_`),
  cleared at the top of each `Greet()`. It is the *only* trigger for D-9 and for activeExpire. It is
  set by the capa reply, which is not the last step of `Greet()`, so after a **failed** `Greet()` it
  may be stale: anything that reads it (INFO, D-9, activeExpire) does so only once `R_GREETED` is set
  in `state_mask_`.
- **`capa activeExpire`.** Immediately after the first capa reply that reveals `active-replica`,
  at either of the two capa sites, send `REPLCONF capa activeExpire` as its **own** command, **once
  per `Greet()`** (a lambda in `Greet()` with its own sent bit). KeyDB answers `+OK active-replica`
  again; this reply is parsed leniently (`ParseCapaReply(...).ok`, a warning once otherwise,
  continue) — the master already showed it is active, so expiry (D-9) does not depend on the
  acknowledgement; a transport error still fails the `Greet()` like any other command's. Sent on
  plain **and** peer links: a peer link's node already runs active expiry (`!IsReplica()`,
  heartbeat), and a plain link's node implements it via D-9, so the capability is advertised only
  where true. A peer link sends it but never touches the shard flag (D-9).
- A master that does not answer `active-replica` — plain Redis, Valkey, stock Dragonfly, drakeydb,
  a non-active KeyDB — sees a handshake **byte-identical to today**; no extra command is sent.
  `keydb-fastsync-save` is parsed and ignored: drakeydb never sends `capa keydb-fastsync`.
- **Interim refusal (P7-0 only; decision 23, the adversarial pass's C1).** With the suffix accepted
  but RREPLAY not yet unwrapped, a link to an active KeyDB would connect, full-sync, report itself
  up (KeyDB even reports the replica caught up) and drop every streamed write. So until P7-1,
  `Greet()` **refuses** a link whose capa reply advertises `active-replica`, **after** the reply
  parsed (`ParseCapaReply` still accepts the suffix; the refusal is a logged policy, not a parse
  failure), at both capa sites: `LOG_EVERY_T(ERROR, 60)` ("advertises active-replica: this build
  cannot apply its RREPLAY stream yet (Phase 7, P7-1); refusing the link") and
  `std::errc::protocol_not_supported`, which takes the existing failed-handshake paths: `REPLICAOF`
  fails **with the reason in the reply** (`-ERR Protocol not supported: master advertises
  active-replica; unsupported until P7-1`, built by `Replica::Start()`, which every `REPLICAOF`
  path sends as it is; the ERROR log is rate-limited, so the reply is where each attempt's reason
  shows) and leaves no link, and a `--replicaof` link retries on the usual 500 ms reconnect (the
  retry's per-attempt WARNING is rate-limited like the peer refusals). It happens before `PSYNC`
  (and before `REPLCONF UUID` at the first capa site, which is the only one a real KeyDB reaches),
  so the master never forks an RDB for the replica or lists it. The refused link has no master: the
  refusal clears the uuid and clock an earlier greeting of the same `Replica` recorded (the first
  capa site comes before the identity exchange that clears them) and, in peer mode, gives the UUID
  admission back. In peer mode a `REPLICAOF` to an endpoint that is already attached (one attached
  by `--replicaof` and being refused in the background) answers `OK`, the existing "already
  attached" short-circuit that makes no handshake, while that link keeps being refused. The refusal
  is marked `// drakeydb: P7-0 interim` in `replica.cc`; **P7-1 Task 1.2 removes it** together with
  the three strict `xfail`s that pin it. `ConsumeRedisStream` keeps a defensive ERROR for an RREPLAY
  from a master that did not advertise `active-replica`, until Task 1.2's unwrap replaces it.

### D-3. Unwrapping RREPLAY (B.1)

`ConsumeRedisStream` keeps its raw-command path untouched. One branch is added ahead of queuing:
if `args[0]` equals `RREPLAY` (case-insensitive), **flush the pending raw batch first** (ordering:
an envelope must not overtake an earlier raw command); if the link is still running, apply the
envelope, then `repl_offs_ += total_read` and `replica_waker_.notify()`. The existing
batch-dispatch block is refactored into a lambda used by both sites. The envelope is processed
*before* `io_buf.ConsumeInput(...)` because its `RespVec` views point into `io_buf`; no envelope
`string_view` outlives that loop iteration. When `exec_st_.IsRunning()` is false after the
pre-envelope flush, the loop breaks without touching the envelope: `repl_offs_` is then the first
undispatched raw command, which is exactly the PSYNC resume point (D-5, D-8).

`RREPLAY` stays **out of the command registry**: no client should send it, and registering it
would change `COMMAND` output on non-active nodes.

```cpp
struct RreplayEnvelope { std::string uuid;               // normalized lowercase; owned
                         std::string_view inner;         // views the stream buffer: one iteration
                         std::optional<DbIndex> db; uint64_t mvcc = 0; };
enum class RreplayParse { kOk, kBadArity, kBadUuid, kBadDb, kBadMvcc };
RreplayParse ParseRreplayEnvelope(const facade::RespVec& args, RreplayEnvelope* out);
```

`uuid` is an owned string because the normalized form is not a view of the wire bytes, and it keys
the dedup and author maps; `inner` is a view into the stream buffer. `ParseRreplayEnvelope`
mirrors KeyDB's validation (D-1.4): `argc >= 3`; uuid is `IsValidNodeUuid` (`node_identity.h:47`);
inner is a string; optional db is an integer with `0 <= db < FLAGS_dbnum` (`generic_family.cc:57`);
optional mvcc is a `u64`. uuid is compared and keyed in **normalized lowercase** form
(`NormalizeNodeUuid`), including the self-uuid check.

Two deliberate deviations from KeyDB's number parsing, neither reachable from a real KeyDB, whose
values are canonical decimals: **mvcc.** KeyDB reads it with a bare `strtoull`
(`getUnsignedLongLongFromObject`, `object.cpp:743-769`), which takes `-5` (wrapped to 2^64-5),
`12abc` and an overflowing number (`ULLONG_MAX`); `-5` then poisons KeyDB's own per-author dedup
watermark, which every later envelope of that author falls below. drakeydb accepts digits only,
within 64 bits (`kBadMvcc` otherwise), so that P7-2's watermark stays sane. **db.** KeyDB reads it
with `string2ll`, which refuses a leading zero (`03`); drakeydb accepts `03`, as it does for any
db it parses (Task 1.1). The code keeps both.

`ClassicApplier::HandleRreplay(args, depth = 1)` (testable without a socket) returns
`EnvelopeResult::{kConsumed, kNotConsumed}`; `IsRreplay(const RespExpr&)` is the stream loop's
predicate. At each level:

1. **Parse.** Any failure: `rreplay_malformed++`, `LOG_EVERY_T(WARNING, 60)`, **skip** (`kConsumed`,
   no watermark touched). Never disconnect (owner decision 14): KeyDB itself replies an error and
   continues, and a disconnect cannot recover the bytes — a partial resync replays the identical
   envelope, a livelock. Operators with a nonzero `rreplay_malformed` re-run `REPLICAOF` to force a
   full resync. Malformed is per level (clarification of decision 14): a malformed inner of a nested
   envelope advances nothing itself, and the enclosing level still advances (step 6).
2. **Cancellation, depth 1 only.** For a layer that parsed `kOk`, or failed only on its mvcc
   (step 3 selects its db), `running()` is checked **once**, before the outermost envelope touches
   the context at all: its db is selected (a first selection dispatches a synthetic `SELECT`) and
   its commands are dispatched. False returns `kNotConsumed` with nothing touched. After this check
   the whole envelope tree runs to completion (D-5), save one exit: the leaf's `Reserve` may still
   return `kCancelled`, before it dispatches anything. `DispatchCommand(ONLY_SYNC)` returns only
   after a command ran or was rejected before running, so no "cancelled mid-dispatch, may not have
   run" state exists. A layer that failed an earlier check selects nothing and is consumed without
   the check.
3. **Db, then self, then nesting** — KeyDB's order in `replicaReplayCommand`
   (`replication.cpp:5416`, `:5435`, `:5442`). The layer's db, if it has one, is **selected as the
   layer is validated** (`SelectDb`, below), before the author is looked at and whatever the inner
   command turns out to be: a self-authored envelope, a control command, an inner that is not one
   command, a nesting overflow and an mvcc that fails to parse (KeyDB reads it after the db) have
   all moved the stream's selected db. A layer that fails the arity, uuid or db check has not.
   Then uuid equals self: `rreplay_self_dropped++`, skip (`kConsumed`), no advance. Then a layer
   deeper than 64 is malformed (step 1's counter and warning).
4. **Dedup early-out** (D-5). At every level `IsApplied(uuid, mvcc)` drops the envelope
   before its inner is parsed (`rreplay_deduped++`, `kConsumed`): a cheap read. It does not close
   the race between two links, which can pass it together; the *leaf's* reservation does (step 5).
5. **Inner command.** A valid inner holds exactly **one** command (KeyDB's
   `catCommandForAofAndActiveReplication` emits one, `aof.cpp:682-726`). Parse it with a
   `RedisParser(Mode::SERVER)` local to the call — one per nesting depth by construction, never the
   link's `parser_` — so no parser state is shared between levels or envelopes and there is nothing
   to recreate after an error. An empty inner, a partial command, bytes after the command, or a
   command whose name is not a string (`*0\r\n`, `*-1\r\n`, `*1\r\n$-1\r\n`: valid RESP that parses
   to an array, a nil array, a nil; U-14) are malformed (step 1) and nothing is applied. Then, by
   class: `MULTI`/`EXEC`/`PING`/`REPLCONF`/`SELECT` skip (the db travels in the envelope);
   `RREPLAY` recurses with `depth + 1`, malformed at 65; KeyDB-only commands drop and count (D-7);
   `KEYDB.MVCCRESTORE` is translated to a `RESTORE` first (D-7a); `FindCmd == nullptr` counts
   `classic_unknown_cmds_dropped`; anything else is a leaf and dispatches with the apply context
   (D-4) under a reservation (D-5).
6. **Advance.** An envelope whose command was handled advances its own author's watermark (D-5):
   after a skipped command, after a dispatched one that was not rejected (`Commit`), and after a
   nested inner that returned `kConsumed` — deduped or not. At depth 64 a malformed 65th level
   (step 1) returns `kConsumed`, so the 64th level still advances. A `kNotConsumed` inner makes this
   level `kNotConsumed` and advances nothing.

**Selected db.** The stream's apply context has one selected db (`conn_state.db_index`), shared by
the raw commands and the envelopes, as KeyDB's master client has one: `replicaReplayCommand`
selects each layer's db on its client while it validates the layer (`:5416`), carries it down to
the client that runs the inner command (`:5468`) and back up afterwards (`:5478`, `:5488`), so
the db a stream ends a layer in is the one the next command without a db runs in — a raw command
and the 3-argument `RREPLAY` (no db) alike. drakeydb selects in step 3 through a `SelectDb` helper
mirroring `JournalExecutor::SelectDb` (`journal/executor.cc:85-100`: first use dispatches a real
`SELECT` so the `DbTable` exists, later uses set `conn_state.db_index`); a nested layer selects as
well, so the innermost layer that has a db decides the leaf's, and an inner without one inherits.
One deviation, harmless against real KeyDB: KeyDB would run a wrapped `SELECT n` and move the db to
`n`, while step 5 skips it; KeyDB never wraps one, as the db travels in the envelope. If a
`SelectDb` fails (counted in `classic_apply_errors`; a `SELECT` to db > 0 does in a real cluster,
`--cluster_mode=yes`, but not in `emulated`: `generic_family.cc:3117` tests `IsClusterEnabled()`),
the leaf of the layer that asked is skipped unless a deeper layer selects another db. Synthetic
`SELECT`s are not stream bytes and never touch `repl_offs_`.

The selected db is state of the *stream*, not of one connection. A Redis replica keeps its cached
master client, and with it the db, across a partial resync, and the master's backlog after the
resume point carries no `SELECT` unless its db changed (KeyDB, from the same code: the cached master
survives `replicationCacheMaster`/`replicationResurrectCachedMaster`, `replication.cpp:4322`,
`:4450`, and `resetClient` leaves its db alone; `replicaseldb`, Redis's `slaveseldb`, makes the
master feed a `SELECT` when it differs from the command's db, `:476`, and is reset to -1 only when a
replica attaches for a full sync, `:874`, or the node turns master, `:4054`). **P7-3 must persist
the selected db across a `+CONTINUE`** (D-8, hazard (i)) and re-select it on the resumed link: a
context that restarts in db 0 applies the resumed writes in the wrong db. Observed by hand, once,
on `redis-server` 7.0.15 (no Redis source tree is at hand): after a write in db 3 and a killed
replica link, a further write in db 3 reaches `PSYNC <replid> <offset+1>` as `+CONTINUE` and a bare
`set`, with no `SELECT`, and a `redis-server` replica ends with the key in db 3 and
`sync_partial_ok:1`. P7-3's test is what pins it in the tree.

**Dispatch** is per command, `DispatchCommand(ParsedArgs{*cmd}, cmd, ONLY_SYNC)` on a
`CommandContext` built with `FillBackedArgs`. Its reply builder is a link-owned
`CapturingReplyBuilder{ReplyMode::ONLY_ERR}`, not the `NONE` one: `NONE` records no payload, and
`InvokeCmd` consumes `last_error_` itself (`main_service.cc:1740`, `:1750`), so an error reply
would be invisible after the dispatch. After each dispatch `Take()` plus `TryExtractError` tell
whether the command replied an error; that counts `classic_apply_errors`, is warned with
`LOG_EVERY_T`, and the stream carries on (a known command that fails — `WRONGTYPE` after a
divergence, OOM — is skipped, never a disconnect). A mixed stream stays correct because only
envelopes take this path; a raw `SELECT` or `DEL` (KeyDB's dead stale-key path emits one,
`replication.cpp:5561`) still batches as before.

### D-4. Apply context, authors, and stamps (B.2)

Per dispatched inner command, **peer mode only** (a plain replica keeps `repl_mvcc = 0` and its
link origin — it has no mvcc side table and its sub-replicas speak framing v1):

1. `repl_origin_idx = ClassicAuthorMap::IdxFor(uuid)`; `repl_mvcc = envelope.mvcc` (0 means local
   mint and never guarded). The author is the **innermost** envelope's uuid, not the forwarder's.
   The one exception to the mvcc source is `KEYDB.MVCCRESTORE`, which stamps with its **own**
   `<mvcc>` argument (D-7).
2. `repl_lww_guard` is set once at link setup to `IsPeerMode() && IsActiveReplica() &&
   FLAGS_multi_master_stream_lww` — the same expression as `replica.cc:1755`. Raw commands have
   mvcc 0, so the guard is inert for them.
3. **Rewrites run on every envelope dispatch**, plain and peer, guard or no guard:
   `ApplyLwwRewrites` then `ClassicApplyRewrites` (D-6); then the dispatch; afterwards, in peer
   mode, restore `repl_mvcc = 0` and `repl_origin_idx = peer_origin_idx_`. KeyDB propagates only
   commands that dirtied the dataset (D-1.14), so every conditional on the wire already
   *succeeded on its author*; replaying the **condition** against a replica whose local state
   differs (a stale key, expiry skew) can skip a write the author made, and the rewrites turn the
   wire form into the author's effect. They are not gated on `LwwGuardActive`: they run on envelope
   links only (raw streams are untouched) and an envelope exists only after the master answered
   `active-replica`, so a non-KeyDB master sees no difference. `JournalExecutor`'s gate on
   `LwwGuardActive` for DFLY links is unchanged; `ApplyLwwRewrites`'s contract comment
   (`multimaster_lww.cc`) gains the envelope carve-out.
4. **EVAL/EVALSHA** on an envelope link dispatch **stamped but unguarded**: `repl_mvcc =
   envelope.mvcc` and the author's `repl_origin_idx` (peer mode), with `repl_lww_guard = false` for
   that one dispatch, restored afterwards. The `RunSquashedMultiCb` tripwire tests
   `IsLwwGuarded()` = `guard && mvcc != 0` (`transaction.h:400-401`, `multimaster_lww.h:88`), so it
   never fires, and a script's writes carry the author's real stamp instead of a local mint. A
   verbatim `EVAL` applies even when the local state is newer (unguarded, like every delta). The
   tripwire's comment and `docs/multi-master.md:311-381` (whose "classic links are never guarded"
   premise ends here) are rewritten. KeyDB 6.2 defaults to effects replication, so verbatim `EVAL`
   is the rare case.

**`ClassicAuthorMap::IdxFor(uuid)`** resolves the author's `PeerRegistry` index: the link's own uuid
maps to `peer_origin_idx_`; any other (forwarded) uuid goes through `PeerRegistry::AddOrGet` plus
`MvccStamper::RegisterOriginHash(idx, NodeUuidHash(uuid))` on every proactor via
`shard_set->pool()->AwaitBrief` (the primitive at `replica.cc:545-549`), memoized per link.
Registration precedes dispatch, so `IncomingStamp`'s `DCHECK(hash != 0)`
(`multimaster_lww.cc:134-140`) holds. A foreign origin is refused by `PassesPeerEchoFilter`
(`journal/types.cc:51-67`), so **no-forward holds by construction** for classic-applied writes.
With `ReplicaPeerMode::registry == nullptr` (tests), stamping is off.

**Author cap.** `PeerRegistry` indices are append-only and live on in journaled entries (D-1.12:
each distinct author costs one `Op::ORIGIN` LSN per shard for the process lifetime), and KeyDB
mints a new uuid at every start (D-1.16), so authors accumulate across KeyDB restarts and a
flapping forwarder. `--classic_author_cap` (default 4096, declared in `classic_replay.cc`) bounds
the distinct **forwarded** authors, process-wide; a link's *own* uuid never counts against it (it
is registered by `Greet()`'s peer admission and maps to `peer_origin_idx_`). Beyond the cap a
forwarded author is stamped with the **link's** idx — never `kSelfIdx`, which would defeat
no-forward — and keeps the envelope's mvcc; `multimaster_keydb_author_overflow` is bumped and a
`LOG_EVERY_T` warns. There is no reclamation (the registry is append-only and the indices are in
journaled entries); the dedup map is bounded separately (D-5) and an over-cap author still
dedups. `classic_link_uuid_changes` (per link) counts a master that presented a different uuid than
on the link's previous `Greet()`, i.e. a KeyDB restart, and `multimaster_peer_registry_size` makes
the growth visible (D-13).

RDB-aux stamps (`mvcc-tstamp`, P4-2) carry the *sender's* origin hash because the file has no
author; a stream write carries the true author's. The two can differ for one forwarded key on an
exact mvcc tie only — documented as a residual. A `KEYDB.MVCCRESTORE` stamp has the same property
as an RDB-aux one: the key's own mvcc with the sending author's hash (D-7a).

### D-5. Author dedup (B.3; review C1, I1, I2)

`AuthorDedup` is process-wide state shared by every classic link, plain or peer — KeyDB's watermark
is process-wide too (`g_mapremote`, `replication.cc:5369`):

```cpp
struct Entry { uint64_t applied = 0; uint64_t inflight = 0; uint64_t last_seen_ms = 0; };
// absl::flat_hash_map<std::string, Entry> under util::fb2::Mutex mu_ + util::fb2::CondVarAny cv_
enum class Verdict { kApply, kDrop, kCancelled };
```

`mu_` is a **leaf lock**: never held across a dispatch or a `PeerRegistry::AddOrGet`, and never
re-entered (`fb2::Mutex` is cross-thread safe but not reentrant, `synchronization.cc:70,94`); `cv_`
is notified only while holding `mu_`. The map is not pointer-stable, so every wait looks the entry
up again.

**Why a reservation (C1).** `ShouldDrop` before the dispatch and `Advance` after it leaves a
window that spans the dispatch's yields: two links carrying the same envelope (a forwarding KeyDB
mesh, a peer attached to both masters) both pass the check, both dispatch, and a delta (`INCR`,
`APPEND`, ...) lands twice. KeyDB cannot race — its replay is single-threaded. A global lock around
the dispatch would serialize every link, which contradicts decision 12 and the no-forward
topology, so the reservation is **per author and on the innermost dispatched level only**:

- `Reserve(uuid, mvcc, running)`: `mvcc == 0` returns `kApply` (never deduped, never reserved).
  Otherwise, under `mu_`: insert the entry (past 4096 entries evict the smallest `last_seen_ms`
  among those with `inflight == 0`), then loop: `applied >= mvcc` returns `kDrop`; `inflight != 0`
  waits on `cv_` for up to 100 ms and returns `kCancelled` if `running()` is false afterwards;
  otherwise set `inflight = mvcc`, stamp `last_seen_ms` and return `kApply`.
- `Commit(uuid, mvcc)`: `inflight = 0`, `applied = max(applied, mvcc)`, `notify_all`.
  `Release(uuid)`: `inflight = 0`, `notify_all`. `Advance(uuid, mvcc)`: `applied = max(applied,
  mvcc)`. `IsApplied(uuid, mvcc)`: `applied >= mvcc`. Keys are the normalized uuid (D-3).

**Leaf** (an inner command that is not itself an `RREPLAY`, reserved under the *innermost*
envelope's author and mvcc; one reservation per envelope tree, never held across the recursion):
the `running()` check of D-3.2 has passed, then `Reserve`:

- `kDrop`: consumed, `rreplay_deduped++`.
- `kCancelled`: **not** consumed — no command was dispatched and nothing was counted; the selected
  db may have moved (below).
- `kApply`: rewrites (D-4.3), `DispatchCommand`. Every layer's db was already selected, while the
  layer was validated (D-3.3), before this leaf's `Reserve`. `DispatchResult::OK` is `Commit`; OK
  includes "ran and replied an error such as `WRONGTYPE`" (KeyDB parity: its `commandsExecuted++`
  either way). `ERROR` / `OOM` — rejected before the command ran (`VerifyCommandState`,
  `main_service.cc:1589-1610`; `PrepareTransaction`, `:1628-1632`; the `DENYOOM` gate,
  `:1721-1724`) or reported OOM by the command itself — is `Release` plus `classic_apply_errors++`
  and a `LOG_EVERY_T`: the envelope is still **consumed** (the offset advances, the link does not
  stall) but the watermark does not move.

**A `kCancelled` after a layer's db was selected is allowed.** KeyDB v6.3.4 always sends the db, so
by the time the leaf reserves, its layers have selected theirs (D-3.3), and the cancelled envelope
leaves the context's selected db moved. That is harmless because selecting is idempotent and
replayable: on resume the same envelope is re-read, and its layers select the same dbs in the same
order before anything is dispatched, so the moved db is exactly the state the resumed stream
recreates. It holds even if P7-3 keeps the context's selected db across a partial resync (D-3
"Selected db"): a layer that selects overwrites it, and an envelope with no db in any layer selected
nothing. The synthetic `SELECT` a first selection dispatches is no stream command (it only makes the
db's tables exist, as `JournalExecutor::SelectDb` does), so "no command was dispatched" holds.
`kNotConsumed` means: no command was dispatched, nothing was counted, the offset stays.

**Outer levels** hold no reservation: they take the `IsApplied` early-out (the one KeyDB has) and
otherwise recurse. An inner result of `kConsumed` — *including a deduped inner*: KeyDB parity, the
inner `rreplay` is itself an executed command — `Advance`s the outer author; `kNotConsumed`
propagates. **Skipped inner commands** (`PING`/`MULTI`/`EXEC`/`REPLCONF`/`SELECT`, a KeyDB-only
drop, an unknown-command drop, a rejected or type-64 `KEYDB.MVCCRESTORE`) are consumed and
`Advance` only; KeyDB advances on `fExec || CLIENT_MULTI` (D-1.4). **Never advanced:** a malformed
or self-authored envelope; a malformed inner of a nested envelope does not advance itself but its
outer still does (D-3.6).

**Cancellation and offsets (I1).** There are two not-consumed outcomes, both before the first
command of the envelope is dispatched: `running()` being false before the outermost envelope's
first touch of the context (D-3.2), checked **once** per outermost envelope; and a leaf `Reserve`
that returns `kCancelled`, which may follow the layers' db selections (the rule above). The point of
no return for *dispatching and counting* is the leaf's first dispatch, after which the whole tree
finishes unconditionally (`Reserve`'s `running` callback only matters until it).
`Commit`/`Advance` run *before* `repl_offs_ += total_read`, so a resume offset never covers an
envelope whose watermark was not recorded; a link cancelled between the two resumes at the old
offset and the replayed envelope is deduped. A
rejected-before-run envelope counts its bytes but leaves the watermark alone, so the same envelope
replayed by another link, or by a partial resync, still applies once.

**Replay.** A partial resync replays only unprocessed bytes: an envelope that committed is dropped,
one that never dispatched applies exactly once. mvcc-0 envelopes (an `RREPLAY` whose mvcc argument
is absent or 0) are never deduped and can double-apply on replay or across links; v6.3.4 always
sends five arguments with a nonzero mvcc, so this is unreachable from it and is documented.

**Reset on a flushing full sync (I2).** A classic full sync that **flushes** replaces the dataset,
so the watermarks that recorded what the *old* dataset held must go: after a master switch the
new master's stream re-delivers envelopes the old link had already applied (to data the flush has
just discarded), and a stale watermark would drop them — silent loss. `AuthorDedup::Clear()` runs
at the flush point of `InitiatePSync`, right after `FlushAll` (`replica.cc:771`) and after
`FlushSlots` (`:769`); it resets every `applied` to 0 and **keeps entries with `inflight != 0`** so
a reservation held by another link survives. It does not run on a peer merge sync (no flush, the
dataset is kept) nor on `+CONTINUE` (D-8). A plain non-cluster node has exactly one classic link
(`ReplicaOfInternal` joins the old one first, `server_family.cc:3667-3677`), so `Clear()` there only
drops the old link's history. Cluster `ADDREPLICAOF` links flush slot ranges; clearing there while
attached to a forwarding KeyDB mesh is a nonsensical topology, documented as unsupported. An
operator `FLUSHALL` on an attached node does not clear the dedup (KeyDB parity), also documented.

**Bound.** 4096 entries; at the bound the smallest `last_seen_ms` among entries with `inflight ==
0` is evicted (an O(n) scan on a rare insert). An evicted watermark can let a very late duplicate
through — documented. The bound is separate from `--classic_author_cap` (D-4). Otherwise the
watermark is not reset: KeyDB's mvcc is monotone across restarts (`server.cpp:7263-7287`) and a
restart mints a new uuid anyway (D-1.16), so a restarted KeyDB starts a fresh entry and the old one
ages out.

### D-6. Streaming LWW on classic links (B.9)

Guard ON for peer-mode classic links under `--multi_master_stream_lww` (decision 7). KeyDB's
propagated forms (D-1.14) classify as follows; two need `ClassicApplyRewrites`, in
`classic_replay.cc`, which runs on **every** envelope dispatch, plain and peer, not only when the
guard is active (D-4.3):

| KeyDB wire form | Today | Verdict |
|---|---|---|
| `SET k v [PXAT abs]`, `SETEX`/`PSETEX`/`GETSET`/`INCRBYFLOAT` (all arrive as `SET`) | `kSingleKey` | OK |
| `SET k v NX\|XX [KEEPTTL]` (no expiry) | `kSingleKey` | **Misclassified**: NX/XX evaluate against local state before the guard — strip NX/XX (and GET, defensively). `NX KEEPTTL` also loses `KEEPTTL` (`SET k v NX KEEPTTL` becomes `SET k v`: the NX succeeded, so the author's key had no TTL to keep, while a stale local key would keep its own); `XX KEEPTTL` keeps it (`SET k v XX KEEPTTL` becomes `SET k v KEEPTTL`) |
| `SET k v KEEPTTL` | `kSingleKey` | Acceptable; document |
| `SETNX`, `GETSET`, `GETDEL`, `RESTORE` | rewritten by `ApplyLwwRewrites` | OK |
| `MSET`, `DEL`, `UNLINK` | self-guarded | OK |
| `MSETNX` | unclassified; skips all if any key exists | **Misclassified**: it reached the wire only because it succeeded; rewrite to `MSET` (becomes self-guarded) |
| `PEXPIREAT`, `PERSIST`, `GETEX` effects | unguarded | Acceptable (arrival order; `FloorAppliedStamp` governs the local stamp) |
| `INCR*`, `APPEND`, `H*`, `L*`, ... deltas | unguarded | By design |
| `RENAME`, `FLUSHALL/DB` | unguarded | By design |
| `EVAL*` | unguarded | Dispatched stamped with the envelope mvcc and the guard off (D-4.4) |
| expiry deletes | not propagated | Each node expires itself (D-9) |
| `KEYDB.MVCCRESTORE` (inside an envelope only) | not a command here | Translated to `RESTORE ... REPLACE ABSTTL` (`kSingleKey`), stamped with its own `<mvcc>`, guarded like any other `RESTORE` (D-7) |

A `SET` with `EX|PX|EXAT|PXAT` — with or without NX/XX/GET — already propagates as exactly
`SET k v PXAT abs`, so only the no-expiry NX/XX forms need the strip (a failed `SET ... NX|XX`
returns before `dirty++` and is never propagated, `t_string.cpp:104-108`; `MSETNX` returns `0`
before any `setKey`, `:556-563`). The classifier table in `multimaster_lww.cc` itself is
unchanged.

### D-7. KeyDB-only and unknown commands (B.7)

`IsKeyDbOnlyCommand(args)` drops (counted in `keydb_cmds_dropped`, `LOG_EVERY_T(WARNING, 60)`):
`PEXPIREMEMBERAT` (the only member-expiry form KeyDB propagates, `aof.cpp:716-718`),
`EXPIREMEMBER`, `EXPIREMEMBERAT`, `PERSIST key subkey` (the 3-arg form only; `PERSIST key` is
standard), `KEYDB.CRON`, `KEYDB.HRENAME`, `KEYDB.NHSET`, `KEYDB.NHGET`, `KEYDB.MEXISTS`, and
`RREPLAY` (never dispatched as a command). **`KEYDB.MVCCRESTORE` is not on this list: it is
applied (D-7a).**

Inside envelopes, a command the dispatcher has no entry for (`CommandRegistry::FindExtended`, the
dispatcher's own lookup, which reads `ACL <sub>` as one name; not `FindCmd`) is counted
`classic_unknown_cmds_dropped` with its own rate-limited warning instead of being dispatched into
the error builder. The check runs after the layers' dbs were selected (D-3.3), so a KeyDB-only or
unknown leaf still leaves the db its layers selected, as KeyDB selects before it knows the
command. On the raw
path (a non-active KeyDB sends `PEXPIREMEMBERAT` raw) only the KeyDB-only check runs; raw-path
unknowns stay on upstream's `unknown_*` accounting so the squashed hot path gains no second registry
lookup. Member TTLs are documented as lost; native conversion is a registered follow-up.

#### D-7a. `KEYDB.MVCCRESTORE` is applied (decision 22)

It carries data: an active KeyDB with replicas emits one per key it inserts while loading an RDB,
including a merge full sync from one of its own masters (D-1.15), so dropping it would lose keys
silently on a KeyDB mesh resync. **It only ever arrives inside an RREPLAY envelope** — the emitter
returns unless the sender is an active replica, and an active master wraps every replica-bound
command (D-1.2), a forwarded one included (nested) — so it is handled in the envelope loop only. A
raw-path occurrence is not translated (a raw stream carries no mvcc authority) and stays on
upstream's `unknown_*` accounting.

`TranslateMvccRestore` (`classic_replay.cc`) rewrites `KEYDB.MVCCRESTORE key <mvcc> <expire>
<payload>` (exactly five arguments) in place to `RESTORE key <ttl> <payload> REPLACE ABSTTL` and
returns the command's own mvcc:

| `<expire>` | `<ttl>` | Why |
|---|---|---|
| negative, or `LLONG_MAX` (`INVALID_EXPIRE`) | `0` (no TTL) | KeyDB sends `INVALID_EXPIRE` for a TTL-less key, not `-1`; passed through, `ABSTTL` would clamp it to a TTL about 8.5 years out |
| `0` | `1` | KeyDB's `0` is a deadline at the epoch, so the key is already expired; `RESTORE`'s `0` means no TTL |
| anything else | unchanged | An absolute ms deadline, which `ABSTTL` takes as is |

- **mvcc source.** The stamp's mvcc is the command's own `<mvcc>`, the key's mvcc as the sender
  holds it, *not* the envelope's, which is a fresh tick minted when the command was fed. Stamping
  with the envelope's would make a restored key look as new as the resync itself, so it would beat
  every newer local write. When `<mvcc>` is 0 or has bit 63 set (`OBJ_MVCC_INVALID`, a key KeyDB
  synced from plain Redis; the loader treats the `mvcc-tstamp` aux the same way, `rdb_load.cc:3193`)
  the envelope's mvcc is used instead. The origin hash is the envelope author's (innermost), so the
  D-4 residual applies: it names the sender, not the key's original writer, which matters on an
  exact mvcc tie only. The dedup watermark (D-5) is unaffected; it advances with the envelope's
  mvcc.
- **Guard path.** Peer mode: `repl_mvcc` is the value above and `repl_origin_idx` the author's, then
  the ordinary D-4.3 sequence (`ApplyLwwRewrites` finds `REPLACE` already present;
  `ClassicApplyRewrites` leaves it alone), and `Transaction::ShouldDropForLww` vetoes the restore
  when the stored stamp wins (`RESTORE` is `kSingleKey`). Ties favor the stored side where KeyDB's
  `dbMerge` lets the incoming side win, and the compare is on `{mvcc, origin_hash}` where KeyDB's is
  on mvcc alone; both differ on an exact mvcc tie only. `--multi_master_stream_lww=false` applies it
  in arrival order, still stamped. A plain replica keeps `repl_mvcc = 0`: the translated `RESTORE`
  applies verbatim and unstamped, like every plain classic write.
- **Failure accounting.** Before dispatch the translator rejects: not five arguments; `<mvcc>` not
  a u64 or `<expire>` not an i64 (KeyDB itself replies an error, `cluster.cpp:5212-5216`); and a
  payload `RESTORE` would not load: no longer than the 10-byte footer (`GetRdbVersion` rejects
  `size <= 10`), a footer version above `RDB_VERSION`, or a first byte outside `rdbIsObjectTypeDF`.
  The length and version are *also* rejected by `RESTORE` itself before `OpRestore` runs
  (`generic_family.cc:2891-2895`), so on their own they cannot reach the delete; the **type-byte
  check is what protects a resident key** from D-31, because `OpRestore` with `REPLACE` deletes the
  resident key *before* it parses the payload (ISSUE-REGISTER D-31) and an unloadable payload must
  never erase a good key, as it does not in KeyDB (`cluster.cpp:5232`). A rejected command is
  skipped, `keydb_mvccrestore_failed` is bumped, a `LOG_EVERY_T(WARNING, 60)` names the key and the
  reason, and the envelope still counts as consumed (`Advance` only, D-5). **One class is a drop,
  not a failure:** type byte 64 (`RDB_TYPE_CRON`, a KeyDB cron job in a `DUMP`) is a KeyDB-only value
  drakeydb cannot represent, exactly like `KEYDB.CRON` itself, so it is counted in
  `keydb_cmds_dropped`, warned and skipped, and `keydb_mvccrestore_failed` is not bumped. The CRC is
  not verified, matching a replicated-apply `RESTORE` (D-1.15) and KeyDB's own handler for
  replication links (`cluster.cpp:5219`). The one residual is a payload that passes these checks
  and still fails inside the loader (a module-typed value, a deep-integrity failure): the translated
  command is dispatched through the link's `ONLY_ERR` builder (D-3), an error reply also counts as
  `keydb_mvccrestore_failed` (not `classic_apply_errors`), and the resident key is lost locally
  exactly as D-31 describes.

### D-8. Partial PSYNC on classic links (B.4, decision 10)

`--classic_partial_psync` (bool, default true, declared in `classic_replay.cc`). In-memory only: a
restart, or a new `Replica` object, means a full resync.

- Request `PSYNC <master_repl_id> <repl_offs_ + 1>` only when the flag is on, `master_repl_id` is
  non-empty and `classic_stable_reached_`; otherwise today's `PSYNC ? -1` / `<id> -1`.
  `classic_stable_reached_` is set when `InitiatePSync` **returns success** — the loader finished,
  the EOF token or `$<len>` tail validated, and the bytes behind the RDB were handed to the stream
  consumer (D-10) — or after a `+CONTINUE`. It is not set "after the loader finishes" (a tail that
  fails validation leaves a loaded but unvalidated dataset and must request a full resync) and it
  is **not** `passed_full_sync_` (DFLY path only, `replica.cc:1037,1090`). A drop mid-load requests
  a full resync.
- **`classic_stable_reached_` is cleared at the exact point a `+FULLRESYNC` line parses**
  (`replica.cc:1880-1889`), because that code overwrites `repl_offs_`/`master_repl_id` before the
  `$` line validates. Without this, a malformed second line leaves a clobbered offset paired with a
  stale flag and the next attempt requests a wrong offset.
- `+CONTINUE [<newid>]`: adopt `<newid>`, keep `repl_offs_`, **no LOADING, loader, flush or merge**,
  `R_SYNC_OK`, `classic_psync_partial_ok++`. `ParseReplicationHeader` must also consume the line
  (today the `CONTINUE` branch returns without consuming, `:1922-1928`).
- **Leftover hand-off — the one silent-loss hole.** `ParseReplicationHeader` reads into
  `InitiatePSync`'s local `io_buf`; `ConsumeRedisStream` allocates its own. Bytes after
  `+CONTINUE\r\n` in the same read must reach the stream consumer. The mechanism is the `Replica`
  stream-prefix buffer that Task 0.6 introduces for the bytes behind a full sync's RDB (D-10):
  `InitiatePSync` fills it and `ConsumeRedisStream` drains it into its `io_buf` before its first
  read, counting those bytes into `repl_offs_` like any other. Task 3.1 adds only the `+CONTINUE`
  producer.
- `ConsumeRedisStream` sends `REPLCONF ACK 0` first (`:1131`); on a partial resync the master
  ignores a non-greater ack (KeyDB/Redis `if (offset > c->repl_ack_off)`), and the acks fiber
  immediately sends the real offset. The task verifies this against both masters.
- Fallback: a non-`+` reply, `-ERR`, `-NOMASTERLINK`, `-LOADING`, or the active-KeyDB
  `-ERR BGSAVE failed` keep the existing bad-header reconnect. When a partial was requested and
  `+FULLRESYNC` came back, `classic_psync_partial_fallback++`. An active KeyDB's replid does not
  change when it syncs from its own masters (`replication.cpp:3051-3062`); it changes at first-slave
  backlog creation and backlog expiry — both fall back to FULLRESYNC.
- Peer-mode partial resync skips the merge load entirely (no tombstone churn).
- Hazards, each with a catching test (D-15): (a) leftover after CONTINUE; (b) off-by-one PSYNC
  offset; (c) deferred MULTI/EXEC bytes; (d) envelope bytes; (e) parser `INPUT_PENDING` across
  reads; (f) mid-load drop must FULLRESYNC; (g) header overwrite on a malformed FULLRESYNC; (h)
  stream bytes behind the RDB of a full sync (Task 0.6, the same buffer); (i) the stream's selected
  db must survive the resume (D-3 "Selected db"): the backlog after the resume point carries no
  `SELECT`, so a context that restarts in db 0 applies the resumed writes there.

### D-9. Active expiry on plain replicas of an active KeyDB (decision 13)

A plain replica whose master answered `active-replica` runs active expiry itself. `EngineShard::
Heartbeat` runs `RetireExpiredAndEvict` only when `!IsReplica()` (upstream's `engine_shard.cc`),
and that function interleaves expiry (`DeleteExpiredStep`) with eviction
(`FreeMemWithEvictionStepAtomic`). As built (Task 1.4, `ReplicaActiveExpiryTest`):

- **The seam.** New per-shard flag `replica_active_expiry_` on `EngineShard` (`engine_shard.h`,
  beside `is_replica_`), `SetReplicaActiveExpiry(bool)` / `ReplicaActiveExpiry()`. `Heartbeat` gates
  on `!IsReplica() || replica_active_expiry_`. There is no `expire_only` parameter:
  `RetireExpiredAndEvict` derives `eviction_goal = (!IsReplica() && FLAGS_enable_heartbeat_eviction)
  ? CalculateEvictionBytes() : 0`, so on a replica neither `FreeMemWithEvictionStepAtomic` (whose
  `DCHECK(!owner_->IsReplica())` stays as it is and is unreachable) nor `CalculateEvictionBytes`
  (which advances `eviction_state_`) runs. The extra-namespace and rotating-db sweeps, and the
  member-TTL reaper, are gated on `IsActiveReplica()`, a boot flag, so a plain replica skips them.
  Every other replica gate stays: the insert path's eviction and `apply_memory_limit`
  (`db_slice.cc`), and `rdb_load.cc`'s.
- **A second gate opens too.** `DbSlice::ExpireIfNeeded` returns without deleting when `owner_->
  IsReplica() && !FLAGS_replica_delete_expired`; the sweep calls it. The gate becomes `IsReplica() &&
  !owner_->ReplicaActiveExpiry() && !FLAGS_replica_delete_expired`. Defaults are unchanged. With the
  flag on, the read path deletes on such a replica too, whatever `--replica_delete_expired` says
  (KeyDB's read path on a replica only hides an expired key, `expireIfNeeded` returns 1; it is
  KeyDB's sweep, `expireOwnKeys`, that deletes there).
  Without the flag, `--replica_delete_expired=false` leaves an expired key **served as live**; with
  it true (the production default) the read path deletes it and journals the delete.
- **Who sets it.** The flag follows `master_active_replica_` (D-2), through one helper,
  `Replica::ApplyReplicaActiveExpiry(bool)`, which mirrors `SetShardStates` and does nothing for a
  peer-mode link (those nodes never leave master mode and already expire) or for a link that is not
  the node's **main** link. The main link is marked by `Replica::SetMainLink()`, called by
  `ServerFamily::ReplicaOfInternal` on `replica_` and never by `AddReplicaOf`. It is not `!slot_
  range_`: `REPLICAOF <host> <port> <start> <end>` gives the main link a slot range too (pinned by
  the `with_slot_range` pytest case). An `ADDREPLICAOF` link therefore never drives the flag
  (it shares `SetShardStates`, last writer wins), and replica active expiry through one is
  unsupported.
- **Where it is applied.** `Start()`'s `Greet()` runs before `MainReplicationFb` flips the shards to
  replica mode, so the fiber applies the recorded value right after `SetShardStates(true)` (only if
  `R_GREETED`: a `--replicaof` link is greeted by the fiber instead) and after every later
  successful `Greet()` (a master that stops advertising `active-replica` clears it), and clears it
  with `SetShardStates(false)`. A **failed** `Greet()` neither sets nor clears it. The window that
  leaves is documented: a KeyDB replaced by a plain Redis at the same host:port keeps expiring on its
  own until the first successful `Greet()` clears it, and a full resync follows, so nothing lasts
  (KeyDB's own replica behaves alike: `mi->isActive` is only rewritten by `parseMasterCapa`).
- **Journaling.** Deletions journal only when the shard has a journal: on a plain replica that is
  under `--experimental_cascaded_partial_sync` with a sub-replica, where the sub-replica gets the
  expiry `DEL` (flagged `kEntryFlagExpired`); the peer echo filter applies only in peer mode. No code
  beyond the existing path. With a **plain Redis** master the flag is never set: the replica still
  never expires on its own.
- A replica with `--replica_delete_expired=false` that onboards from an active KeyDB expires anyway;
  the master will never send the DEL. Documented.
- **The lag window (owner decision 24).** A replica that expires on its own clock can expire a key
  before a TTL extension for it arrives (`EXPIRE`, `PEXPIREAT`, `PERSIST`, stream lag and clock skew
  both widen the window). The extension is then a no-op on the replica, while the key lives on in
  KeyDB, forever after a `PERSIST`. KeyDB's own plain replicas have the same window. Accepted and
  documented, with no grace flag. The operator page, `docs/multi-master.md`, has no KeyDB section
  yet (P7-4 adds "Onboarding from KeyDB"); until it does, this paragraph and the `PLAN.md` note are
  where it is written down.

### D-10. RDB loader tolerance (B.6)

KeyDB layout (`rdb.cpp:1133-1200`): `[EXPIRETIME_MS][IDLE][FREQ]`, then `AUX "mvcc-tstamp"` (active
only), then type, key, value, then `AUX "keydb-subexpire-key"`/`"-when"` per member TTL **after**
the key. `RDB_VERSION 9`. Type 64 body (`rdb.cpp:1078-1089`, load `:2577-2588`): script string,
8-byte LE ms start, 8-byte LE ms interval, len + key strings, len + arg strings. All changes are
strictly more permissive; the write side is untouched.

- **Type 64**: before the `rdbIsObjectTypeDF` check (`rdb_load.cc:2703`), parse and skip (key,
  script, two `FetchInt<int64_t>`, two counted string lists), count `keydb_rdb_cron_skipped`, and
  `settings.Reset()` (`:2718`) so the cron key's preceding `mvcc-tstamp` does not leak onto the next
  key.
- **Subexpire aux**: skipped in `HandleAux` without touching `settings`; counted on `-when`
  (`keydb_rdb_subexpire_dropped`); one `LOG_EVERY_T(WARNING, 60)` rollup. Native conversion is a
  registered follow-up.
- **Aux noise**: `repl-masters` recognized (VLOG); unknown aux warns once per distinct name per
  loader; the bit-63 `mvcc-tstamp` warning (`:3193`, per key, unthrottled) becomes rate-limited with
  counter `keydb_rdb_mvcc_invalid`.
- **D-8 precedence (ISSUE-REGISTER D-8).** Aux precedes the key while opcode 221 immediately
  precedes the type byte, so last-in-stream wins and opcode 221 wins. Pinned by a test; closes D-8.
- **The full-sync tail (review C2).** Bytes after the RDB belong to the replication stream, not to
  the snapshot. `RdbLoader::Load`'s first read (`ReadAtLeast(bytes, 9)`, `rdb_load.cc:2421-2427`)
  is the one read `source_limit_` does not clamp (later reads are, `:1203`), so it can pull stream
  bytes into the loader's buffer. Fix, **ungated**: clamp the first read to the limit, and treat an
  RDB that runs past its declared size as `rdb_file_corrupted`, never a `DCHECK`. After a *correct*
  load `InitiatePSync` hands everything behind the RDB — the loader's `Leftover()` and the
  `UnusedPrefix()` of `ps` (or of the EOF path's chained source) — to `ConsumeRedisStream` through a
  `Replica` stream-prefix buffer (D-8 reuses it for `+CONTINUE`); the stream consumer parses it
  before its first socket read and counts it into `repl_offs_`. Only a short read, an overrun or an
  EOF-token mismatch is malformed.
- **Graceful errors**: `ParseReplicationHeader`'s EOF-token `CHECK_EQ` (`:1910`) and
  `InitiatePSync`'s six `CHECK`s (`:823-835`, which also encoded the "nothing may follow the RDB"
  assumption fixed above) become `LOG(ERROR)` plus an error code (reconnect, never abort).

### D-11. Hardening: U-9 (B.8)

`EvalInternal`'s single-shard branch calls `conn_cntx->conn()->RequestAsyncMigration(...)` when
`sid != thread_index` (`main_service.cc:2458-2463`); `conn()` is null for `ConsumeRedisStream`'s
`ConnectionContext{nullptr, {}}` (`replica.cc:1113`) and `JournalExecutor`'s
(`journal/executor.cc:34-42`). Fix: `&& conn_cntx->conn() != nullptr`, **ungated**. Test
`EvalReplicatedApplyNoConnNoCrash`. The `TAKEN_OVER` branch of `VerifyCommandState` dereferenced
`conn()` the same way (`main_service.cc:1430`); it was fixed alongside as ISSUE-REGISTER U-10
(P7-0 Task 0.5). So did `DispatchCommand`'s `MarkForClose()` after a handler threw, fixed in the
review round that followed as U-12 (`ReplicatedApplyHandlerThrowNoConnNoCrash`).

### D-12. Throughput bar (decision 12)

Per-command dispatch loses the squasher's batching for envelopes, and the owner's bar is that
drakeydb must **keep up with KeyDB**. Operationalised (clarification of decision 12) as a
measurement on **release builds** of both servers — a debug build says nothing about a production
bar:

- **Setup.** `./helio/blaze.sh -release -DWITH_AWS=OFF -DWITH_GCP=OFF`, building only
  `ninja -C build-opt -j4 dragonfly` with `CCACHE_DISABLE=1` (free disk first by deleting the
  non-gate debug test binaries; they relink from objects later). KeyDB `--server-threads 1` and
  drakeydb `--proactor_threads 2`, each pinned to its own cpuset with `taskset`, the load
  generator on a third: `redis-benchmark -P 100 -c 50 -t set,incr -r 100000`. Sample KeyDB's
  `master_repl_offset` and drakeydb's `slave_repl_offset` at 1 Hz.
- **Bar**, over the steady window: `apply_rate / produce_rate >= 0.95` (rates are the offset
  deltas per second); the maximum byte lag stays `<= max(2 s x produce_rate, 8 MB)`; the lag drains
  to zero within 2 s of the load stopping; the link never reconnects and KeyDB logs one full sync.
- **Comparator.** The same load with a second KeyDB attached as an active replica of the same
  master: drakeydb's lag must be within 1.5x of that replica's. Three runs, median. Also record
  the raw (squashed) path — a non-active KeyDB or Redis master — as the reference the envelope path
  pays against.
- **Gating.** The release bar runs when `DRAKEYDB_PERF=1` (a pinned, quiet box). The default (CI,
  debug) run is a **functional smoke**: rate-capped to about 5k ops/s with loose bounds (ratio
  `>= 0.5`, lag `< 32 MB`, drain `< 10 s`) — it proves the test and the plumbing, not the bar.

The test is `test_keydb_onboarding_keeps_up_under_load` (`slow`); the achieved ops/s and the lag
series are recorded in the ledger. The cost P7-2 adds (stamping, dedup reservation, guard) is
re-measured in peer mode (Task 2.4). If the **release** bar fails — and only then — **Task 1.6**
(conditional) optimizes inside P7: same-author/same-shard micro-batching through the squasher with
**per-command** mvcc, which needs per-command stamp plumbing because `repl_mvcc` is batch-level at
`main_service.cc:1815` and `multi_command_squasher.cc:114`; it starts with a measurement and a
design note, and stops for the advisor before touching squasher files. Peer lines gain a
`repl_offset=` field so the peer-mode lag is observable.

### D-13. Observability, flags, docs (B.10, decision 15)

Per-link counters on `Replica` (relaxed atomics, copied into `ReplicaSummary`,
`replica_types.h:14`), rendered only for **classic** links in the plain-replica block
(`server_family.cc:3164-3192`) as `key:value` lines and in `RenderPeerReplicationInfo`
(`multi_master.cc:195-216`) as `,key=value`: `rreplay_unwrapped`, `rreplay_malformed`,
`rreplay_self_dropped`, `keydb_cmds_dropped`, `keydb_mvccrestore_failed` (rejected
`KEYDB.MVCCRESTORE`s, D-7a), `classic_unknown_cmds_dropped`, `classic_apply_errors` (a known
command that replied an error or was rejected before it ran inside an envelope, D-3/D-5),
`classic_link_uuid_changes` (the master presented a new uuid: a KeyDB restart, D-4),
`classic_psync_partial_ok`, `classic_psync_partial_fallback`, plus `repl_offset` on peer lines.
Process-wide (relaxed atomics in `classic_replay.cc`, **not** `ServerState::Stats`, D-1.13):
`multimaster_rreplay_deduped`, `multimaster_keydb_author_overflow`,
`multimaster_keydb_rdb_cron_skipped`, `multimaster_keydb_rdb_subexpire_dropped`,
`multimaster_keydb_rdb_mvcc_invalid`, and the gauge `multimaster_peer_registry_size`
(`PeerRegistry::Size()`, which makes the append-only growth of Risk 7 visible). Prometheus mirrors
(`metrics.cc`) are `<name>_total` (the gauge keeps its name), process-wide sums for the per-link
ones.

**Where they render — a stock master keeps upstream's INFO.** A classic field renders only for a
classic link whose master answered `active-replica` **or** whose own counter is nonzero
(`ReplicaSummary` gains `classic_link` and `master_active_replica` for that). Process-wide counters
use a node-wide predicate (an active KeyDB master has completed a handshake since boot, or the
counter is nonzero; the loader counters can be nonzero when onboarding from a **non-active**
KeyDB, which also holds cron jobs and member TTLs; the series' predicate is decision 25, below):
beside `multimaster_lww_dropped` in the active-node block
(`server_family.cc:3138-3162`) and in the plain-replica block after the per-link fields. In
Prometheus a plain replica never reaches the master-side branch where
`multimaster_lww_dropped_total` lives (`metrics.cc:511-518`), so the mirrors are also emitted in the
replica-side branch (`:486-489`); otherwise the series would be missing exactly on the nodes that
onboard from KeyDB. With a stock Redis master and every counter zero, INFO and `/metrics` are
byte-for-byte upstream's.

As built (P7-1 Task 1.3, with its review round): INFO's predicate is per field. A link to an active
KeyDB shows every counter, zeros included, whatever its state; any other classic link shows only the
counters that moved (a non-active KeyDB sending `PEXPIREMEMBERAT` raw shows `keydb_cmds_dropped`
alone), and on a peer line `repl_offset` follows whenever any field is shown. "Active" sticks per
link: `ReplicaSummary::master_active_replica` is set once a completed `Greet()` of that `Replica`
found `active-replica` in a capa reply (`Replica::classic_master_was_active_`) and is never cleared
for that `Replica`, so a link that is down, or reconnecting with a handshake that fails, keeps its
zero fields. `classic_link` is the protocol of the last completed handshake and survives a
disconnect likewise. The per-Greet `Replica::master_active_replica_` (cleared at every `Greet()`,
read only once `R_GREETED` is set) is what Task 1.4's expiry decision uses; INFO does not read it.

**The `<name>_total` series deliberately deviate from the per-link wording above (owner decision
25).** They are the process-wide sums (`ClassicTotals()`, bumped with every per-link counter, so a
link that went away leaves its counts), emitted at one place in `Metrics::Print` after the
replica/master branches, which covers both and a node that was promoted with counters still
nonzero. A series renders when a process-wide sticky flag is set (`NoteActiveKeyDbMaster()`, called
by `Replica::Greet()` when a classic master that answered `active-replica` completes a handshake,
never cleared; `ActiveKeyDbMasterSeen()`), or when its own total is nonzero. So the zero series do
not come and go with a link, and `/metrics` does not look at the links: the first version computed
"some link has an active KeyDB master" per scrape, a hop to every peer thread under
`PeerReplicationManager::mu_`, and only to decide this. The `multimaster_*` process-wide INFO
counters named above arrive with the tasks that count them (P7-2, P7-4); Task 1.3 has none to
render there.

**Clock skew.** KeyDB answers `REPLCONF UUID` with a bare `+<uuid>` (D-1.1), so the handshake clock
echo that feeds `clock_skew_ms` for DFLY peers is absent and the field stays 0 ("no sample"). A
classic link derives an estimate from the stream instead (Task 2.7): for a depth-1 envelope with
`mvcc != 0` the sample is `(mvcc >> 20) - now_ms` (D-1.17), the master's clock minus ours minus the
stamp's age, so each sample is a lower bound on the true skew. Samples are taken at most once a
second and the link reports the maximum of the last 60, through the same `clock_skew_ms_` atomic
and `IsClockSkewConcerning` warning as a DFLY peer.

Flags: `--classic_partial_psync` (P7-3), `--classic_author_cap` (P7-2). The boot limitations
warning (`multi_master.cc:144-151`, one `LOG(WARNING)` literal) gains "KeyDB member TTLs and cron
jobs are dropped on onboarding" (Task 1.3). Docs: `docs/multi-master.md` "Onboarding from KeyDB":

- topology, and **`multi-master-no-forward yes` only when every drakeydb node is `--active_replica
  --multi_master` and attached to every KeyDB master**. A plain replica attaches to one master only,
  and with no-forward on a write reaches a node only from the KeyDB it was written on; KeyDB itself
  warns that the directive needs a mesh or data is lost (`config.cpp:2705-2710`, the forward is
  skipped at `replication.cpp:5507`). With a single KeyDB master there is nothing to forward. In
  every other topology forwarding stays on and the dedup (D-5) absorbs the duplicates;
- backlog sizing, cutover, expiry semantics, member-TTL/cron loss, `KEYDB.MVCCRESTORE` applied and
  its failure counters, the counters;
- unsupported or by-design combinations: the dedup is reset by a flushing full sync (so a cluster
  `ADDREPLICAOF` of a forwarding KeyDB mesh is unsupported) and not by an operator `FLUSHALL`
  (KeyDB parity); mvcc-0 envelopes are not deduped; a KeyDB restart is a new author (Risk 7);
- rewrite `:87-122` and `:311-381`.

Also `docs/differences.md`, `docs/UPSTREAM-SYNC.md` watchlist rows, ISSUE-REGISTER (close D-1, D-8,
U-9, U-10; add the `MarkForClose` and subexpire-conversion follow-ups; the full-sync-tail U-item,
D-10), `docs/build-from-source.md` (KeyDB build recipe, delivered by Task 0.7).

### D-14. Test harness and CI (decisions 16-17)

Delivered by Tasks 0.7 and 0.8, and hardened in their review round:

- `RedisServer` (`tests/dragonfly/instance.py:563`) falls back to `$REDIS_SERVER_PATH`, then
  `redis-server` on `PATH`, when its pinned binaries are absent (the fixture used to skip,
  `conftest.py:613-623`); `redis_replication_test.py` therefore runs locally.
- `KeyDBServer` class and `keydb_server` / `keydb_server_factory` fixtures honour
  `KEYDB_SERVER_PATH`; a missing binary skips locally and **fails** under `KEYDB_REQUIRED=1` (also
  `true`/`yes`), and a set-but-bad path always fails. Readiness requires the answering process to
  be the one we started. A `keydb` marker (`tests/pytest.ini`). KeyDB flags: `--active-replica yes`
  (must precede any `replicaof`, `config.cpp:742-753`), `--multi-master yes` for meshes.
  `tests/dragonfly/keydb_harness_test.py` pins the harness rules without a KeyDB.
- Golden RREPLAY captures from a real KeyDB v6.3.4,
  `tests/dragonfly/data/keydb_v6.3.4_rreplay_{stream,nested_stream}.bin` with a README (segment
  table, envelope layout), regenerated by `tests/dragonfly/tools/capture_keydb_rreplay.py`: the test
  vectors for D-3.
- `.github/workflows/drakeydb-ci.yml`: build drakeydb with the existing builder action, build and
  cache KeyDB v6.3.4 (restored and saved explicitly, so red runs still save), run
  `keydb_onboarding_test.py` and `multimaster_test.py` with `KEYDB_REQUIRED=1`. Upstream `ci.yml` is
  untouched. Not yet observed on GitHub.

Delivered by Task 0.6: `tests/dragonfly/fake_classic_master.py` — a minimal asyncio classic master
(handshake replies, a scripted `PSYNC` reply and raw bytes, an optionally delayed second write, and a
record of every request, connection and `REPLCONF ACK` offset), needed because the existing `Proxy`
replaces only the first line of one response (`proxy.py:21-37`): it makes the post-load paths,
malformed envelopes, mixed raw/envelope ordering and coalesced `+CONTINUE` + stream bytes
deterministic. Tasks 1.2 and 3.1 reuse it. Still to come: the `Proxy`'s additive request-capture
list (Task 1.4).

### D-15. Tests

Every test is **falsified** (revert, observe the named failure, restore, record verbatim in
`task-N-report.md`). The P4 lesson stands: assert **which value survived** and what a test would
still pass under if the feature were removed.

| Test | Falsify by |
|---|---|
| `ClassicReplayTest.ParseCapaReply*`; `test_greet_accepts_capa_reply_with_capability_words` (both Greet sites, proxy; `+OK keydb-fastsync-save` and an unknown word) | Reverting `Greet()` to strict `OK`: `REPLICAOF` fails with `Bad response ... "+OK keydb-fastsync-save\r\n"` |
| `test_greet_refuses_active_replica_capa_reply` (both Greet sites, proxy; `+OK active-replica` with and without `keydb-fastsync-save`; P7-0 interim, removed by Task 1.2) | Removing the refusal: `REPLICAOF` is accepted, the full sync lands, no ERROR is logged |
| `test_active_keydb_link_refused_until_p7_1` (real KeyDB; plain and peer node, `REPLICAOF` and `--replicaof`: the reason in the reply and no link left behind / retries bounded by the 500 ms cadence, the ERROR logged once, KeyDB never lists the replica and counts no sync, no key arrives; P7-0 interim, removed by Task 1.2) | Removing the refusal; `Start()` giving the reason (bare `replication cancelled`); `LOG_EVERY_T` back to `LOG` (one ERROR per attempt); un-quieting the retry WARNING; the reconnect sleep cut to 50 ms (attempts bound) |
| `test_refused_active_master_leaves_no_stale_identity` (fake master that is first not active, with a uuid, then answers every capa `+OK active-replica` and drops the link; plain and peer node: INFO shows no master uuid while the refusals go on, no PSYNC; the reason in the reply of a repeated `REPLICAOF` (plain) / `OK` for the attached endpoint (peer); a second master with the same uuid is admitted; P7-0 interim, removed by Task 1.2) | Dropping the clears in the refusal: the old uuid stays in INFO; dropping its `ReleasePeerIdentityClaim()`: the peer node's `REPLICAOF` of the second master fails as a duplicate |
| `test_keydb_active_handshake_and_full_sync`, `test_keydb_active_handshake_peer_mode` (real KeyDB; **strict `xfail`** until Task 1.2: the `REPLICAOF` is refused, the failure is `ActiveKeyDBRefused` (the refusal's reason in the reply) and nothing else) | Removing the refusal: the strict `xfail` XPASSes and fails the run |
| `test_classic_stream_empty_command_name_does_not_abort[diskless_later_write\|disk_later_write\|disk_behind_rdb]` (fake master: a valid sync, then `*1\r\n$0\r\n\r\n`, then `SET a 1`: replica alive, `a == 1`, settled ACK exact; U-13) | Dropping the `!cmd.empty()` guard in `ConsumeRedisStream`: SIGABRT (`i < size()`, `absl/types/span.h`) |
| `test_classic_stream_rreplay_is_dropped_and_logged_until_p7_1` (fake master that did not advertise `active-replica`; Task 1.2 replaces it) | Removing the defensive drop ERROR: nothing is logged |
| `test_psync_stream_bytes_behind_full_sync_are_applied` (fake master: `$<len>` + RDB + raw `SET a 1` in one `write()`; the `$EOF:` framing too): `a == 1`, offsets exact; `test_psync_*_token_mismatch*` and `*_length_*` reconnect | Unclamped first read (`CHECK`/reconnect loop, `a` never set); dropping the hand-off (`a == 0`); counting the hand-off twice (offsets apart) |
| `ClassicReplayTest.ParseRreplayEnvelope*` (incl. the golden captures); `ClassicApplyFamilyTest.*` (unwrap, db, skips, self, malformed, nested to 64, depth-65 leaves the 64th consumed, an inner with zero or two commands, or with an array, nil array or nil for a name, malformed, known-command error counted) | Dropping the 65th-nesting refusal; applying inner `PING`; accepting a second inner command; the `NONE` builder (no `classic_apply_errors`) |
| `ClassicApplyFamilyTest.RunningFalseBeforeDispatchReturnsNotConsumed*`, `.RunningFalseDuringFirstDispatchStillConsumesWholeEnvelope`, `.RejectedDispatchCountsBytesDoesNotAdvance`, `.ReplayAfterCommitBeforeCountIsDeduped` | Checking `running()` after the first dispatch; checking it before every inner dispatch; `Commit` on a rejected dispatch; deferring `Commit` past the return |
| `test_classic_stream_command_with_an_array_for_a_name_does_not_abort[empty_array\|nil_array\|behind_a_queued_command]` (fake master: a valid sync, then `*0\r\n` or `*-1\r\n`, with a raw command queued ahead of it in the last case, then `SET a 1`: replica alive, `a == 1`, settled ACK exact, the warning logged; U-14) | Dropping the U-14 guard in `ConsumeRedisStream`: SIGABRT (`std::bad_variant_access` out of `RespExpr::GetView`) |
| `test_classic_stream_info_command_does_not_abort[raw\|in_envelope]` (fake master: a valid sync, then `SET a 1`, `INFO` raw or inside an envelope, `SET b 2`: replica alive, `a == 1`, `b == 2`, settled ACK exact; in an envelope also `classic_apply_errors == 1` and the logged reason; U-15); `ClassicNoConnectionTest.*` (one case per guarded handler, dispatched raw on the stream's context: the reply is `No connection`, `OK` for `QUIT`; also `WATCH`, `REPLCONF GETACK`, `CLIENT LIST|PAUSE|HELP` and an `EVAL` of `INFO`, `HELLO` and `QUIT`); `ClassicApplyFamilyTest.ReplicatedMonitorAndSubscribeLeaveNothingForClientsToTripOver`, `.InfoInAnEnvelopeIsAnApplyErrorNotACrash`, `.EvalOfAConnectionCommandInAnEnvelopeIsAnApplyErrorNotACrash`, `.ReplicatedWatchLeavesNoRegistrationBehind` (counts `DbTable::watched_keys` across every shard and db, with a control that a real client's `WATCH` is counted); `test_classic_stream_eval_of_a_connection_command_does_not_abort[raw\|in_envelope]` (fake master: `SET a 1`, `EVAL` of `INFO`, `HELLO 3` and `QUIT`, `SET b 2`: replica alive, settled ACK exact, in an envelope `classic_apply_errors == 2`) | Removing the `ServerFamily::Info` guard: the replica process dies (SIGSEGV or SIGABRT) and `b` never arrives; removing the guard of a handler: that case dies, one at a time under `--gtest_filter`; removing the `MONITOR` or `SUBSCRIBE` guard: the next client command or `PUBLISH` dies; removing the `ServerFamily::Client` guard: the five `CLIENT` cases die and `LIST`, `PAUSE`, `HELP` get a reply instead of `No connection`; removing the `Watch` guard: the stream's `WATCH` is accepted and the registration outlives the link (no crash in a debug build, so the test reads the count); removing the `Info` or `Hello` guard: the `EVAL` cases and the pytest die; removing the `ReplConf` guard: `GETACK` replies `syntax error` |
| `test_plain_replica_unwraps_keydb_rreplay`, `..._nested_...`, `test_unwrap_offsets_exact`; `test_keydb_active_live_write_during_full_sync[plain_replica\|peer_mode]` (no longer `xfail`) | Skipping `repl_offs_ +=` (offset lags); removing unwrap (no keys; the live-write test goes back to strict-xfail) |
| `test_unwrap_flushes_raw_batch_before_envelope` (fake master: raw `SET a 1`, envelope `SET a 2`) | Removing the pre-envelope flush (final `a == 1`) |
| `test_unwrap_selected_db_is_the_one_the_raw_commands_after_it_run_in` (fake master: an envelope in db 2, then a raw `SET c` and a 3-argument envelope, then an envelope in db 0 and a raw `SET g`) | Giving the applier a connection context of its own: `c` lands in db 0 |
| `ClassicApplyFamilyTest.DbIsSelectedBeforeTheAuthorAndInnerChecks`, `.SkipsInnerControlCommands` (a control, self-authored, bad-mvcc, malformed-inner and over-nested layer each leave their db selected; arity, uuid and db failures do not) | Selecting the db after the self check, or only for a well-formed layer, or after the nesting check; `ParseRreplayEnvelope` not handing back the db of a bad mvcc |
| `ClassicApplyFamilyTest.FailedSelectSkipsTheCommandUnlessADeeperLayerSelects` (**real** cluster mode, `cluster_mode=yes` with a one-node config: `SELECT 3` fails there, not in `emulated`; own db, outer 3 / inner 0, outer 0 / inner 3, inner without a db, a later envelope without a db: one `classic_apply_errors` per failed select, `db_index` unchanged, only the commands that had a db applied) | Running the leaf without the `db_selected` guard (the skipped keys land in db 0); a failure that stays sticky for a deeper layer (`inner_db0` missing); a layer without a db resetting the failure (`no_inner_db` applied); remembering a failed `SELECT` as ensured (the retry runs in an unactivated db: SIGSEGV) |
| `test_keydb_only_commands_dropped_with_counters` (real KeyDB: `EXPIREMEMBER` and `KEYDB.CRON` dropped, `keydb_cmds_dropped == 2`, the set whole, no unknown or apply error); `ClassicReplayTest.IsKeyDbOnlyCommandTable`; `ClassicApplyFamilyTest.KeyDbOnlyDroppedAndCounted`, `.UnknownInnerCommandCountedNotDispatched`, `.KeyDbMvccRestoreCountedUnknownUntilItIsApplied` (decision 22: `classic_unknown_cmds_dropped`, never `keydb_cmds_dropped`), `.KeyDbOnlyAndUnknownLeavesKeepTheSelectedDb`, `.CountsAlsoFeedTheProcessWideTotals` | `IsKeyDbOnlyCommand` returning false (counter lands in `classic_unknown_cmds_dropped`); also matching `KEYDB.MVCCRESTORE`; `PERSIST` without the arity check; no pre-dispatch unknown check (the command reaches the dispatcher, `unknown_` is filled); a dropped command resetting the db; `Count` not feeding the totals |
| `test_info_and_metrics_show_classic_counters[plain_replica\|peer_mode]` (real KeyDB), `test_scripted_active_master_exact_classic_counters`, `test_scripted_quiet_active_master_shows_zero_counters`, `test_scripted_stock_master_raw_keydb_only_command_shows_only_that_counter` (exact counts, INFO and `/metrics`; the raw path); `PeerReplicationInfo.ShowsClassicFieldsOnlyForClassicLinks`, `.OmitsClassicFieldsWhenMasterNotActiveAndCountersZero`; `ClassicReplayTest.ClassicLinkFieldsFollowTheRenderPredicate`, `.ClassicTotalSeriesFollowTheRenderPredicate` | Omitting the INFO branch (plain) or the peer-line branch; omitting the replica-side Prometheus branch (the plain replica has no series); ignoring `master_active_replica` (a quiet active KeyDB shows nothing); removing the raw-path check |
| `test_scripted_active_master_down_link_keeps_zero_counters[plain_replica\|peer_mode]x[master_gone\|greet_fails]` (fake master that says `active-replica`: after a full sync the master goes away, or answers every capa with an error; INFO still shows the six zeros, and in peer mode the offset; D-13 "shown whatever its state"); `test_scripted_active_master_series_outlive_the_link[plain_replica\|peer_mode]` (`REPLICAOF NO ONE` / `REMOVE`: the six zero series stay; decision 25); `ClassicReplayTest.ActiveKeyDbMasterSeenSticksOnceNoted` | Reading `active-replica` only while the link is greeted (the down link shows nothing); clearing the per-link flag at the top of `Greet()` (`greet_fails` shows nothing); not calling `NoteActiveKeyDbMaster()` in `Greet()`, or computing the series' gate per scrape from the node's links (no series once the link is gone) |
| `test_info_and_metrics_absent_for_stock_master`, `..._absent_for_keydb_that_is_not_active[plain_replica\|peer_mode]`, `..._have_no_classic_fields_between_dfly_nodes[plain_replica\|peer_mode]` (no classic field or series, and the link's block ends where it did: `psync_successes`, `clock_skew_ms`); `test_active_replica_boot_warning_names_keydb_drops` | Rendering unconditionally (the stock-master and DFLY tests fail); removing the sentence from the boot warning |
| `test_greet_sends_capa_active_expire_only_after_active_replica_reply[never\|both_sites\|first_site_only\|second_site_only]x[plain_replica\|peer_mode]` (fake master, `FakeClassicMaster.requests`: with no `active-replica` reply none, else exactly one `["REPLCONF","capa","activeExpire"]` immediately after the request whose reply revealed it, either site, peer links too); `test_keydb_is_told_the_replica_expires_keys_itself` (real KeyDB: the `Synchronization with replica ... succeeded` line present, `does not support active expiration` absent) | Never sending it (the six active cases fail, and the KeyDB warning appears); sending it to every master (`never` and `second_site_only` fail); sending it from only one site (the other site's cases fail); once per site instead of once per `Greet()` (`both_sites` and `first_site_only` count two) |
| `test_plain_replica_of_active_keydb_expires_keys[plain\|with_slot_range]` (real KeyDB: N keys `PX 3000`, the replica's DBSIZE polled without reading a key reaches N+2 and then the two keys that did not expire, after KeyDB's own has; `with_slot_range` is `REPLICAOF <host> <port> 0 16383`); `test_plain_replica_of_plain_redis_never_expires_on_its_own` (a Redis 7 of its own with `enable-debug-command yes` and `DEBUG SET-ACTIVE-EXPIRE 0`: the replica's DBSIZE stays N+2 past the TTL, then drops by exactly the one key read on the master) | Not setting the shard flag (the replica's DBSIZE stays 52 while KeyDB's is 2); the flag set for any master (the control's replica falls to 2 while Redis holds 52); predicating on `!slot_range_` instead of the main-link marker (`with_slot_range` fails alike) |
| `ReplicaActiveExpiryTest.ReapsExpiredKeysButNeverEvicts` (shards `SetReplica(true)` with the flag on, memory limit far under usage: DBSIZE, not the `deleted` stat, shows the expired keys gone and the rest all there, `evicted_keys == 0`, over many more heartbeats), `.ControlAMasterHeartbeatUnderTheSamePressureEvicts`, `.ReapsNothingWithoutTheFlag`, `.BypassesReplicaDeleteExpiredFlag` (`--replica_delete_expired=false`: the sweep and the read path both delete), `.ControlWithoutTheFlagAReplicaThatDeletesNothingServesExpiredKeys` (served as live) | The heartbeat gate back to `!IsReplica()` (the sweep tests fail); the `ExpireIfNeeded` gate without the flag (`BypassesReplicaDeleteExpiredFlag`); eviction allowed on a replica (`DCHECK(!owner_->IsReplica())` in `FreeMemWithEvictionStepAtomic` aborts); the sweep on every replica (`ReapsNothingWithoutTheFlag`); no eviction at all (the control fails) |
| `test_keydb_onboarding_keeps_up_under_load` (`slow`; release bar under `DRAKEYDB_PERF=1`, smoke otherwise) | n/a measurement; control = per-command path made artificially slow must fail the smoke's ratio |
| `ClassicAuthorMapTest.*` (distinct idx and hash on every proactor, link uuid, memoization, `CapFallsBackToLinkIdxAndCounts`, `CapIgnoresLinkUuid`, `DedupStillWorksForOverCapAuthor`); `ClassicApplyFamilyTest.PeerEnvelopeStampsKey...`; `test_keydb_restart_consumes_one_author_slot` | Not registering the origin hash (`IncomingStamp` DCHECK); not setting `repl_mvcc` (stamp is a local mint); counting link uuids against the cap; overflow stamped `kSelfIdx` |
| `AuthorDedupTest.*` (`ConcurrentSameEnvelopeAppliesOnce`: two fibers on `pp_->at(0)`/`at(1)`, the dispatch stub sleeps 10 ms before `Commit`, exactly one `kApply`; `ReleaseLetsSecondLinkApply`; `CancelledWaiterReturnsNotConsumed`; `NestedOuterAdvancesWhenInnerDeduped`; `EvictionSkipsInflight`; `ClearResetsAppliedKeepsInflight`); `test_keydb_mesh_forwarded_duplicates_deduped` (KeyDB A and B forwarding, drakeydb on both, pipelined `INCR` x N at full rate must equal N) | Setting `inflight` without ever waiting (two `kApply`; the pytest ends in (N, 2N], statistical, the gtest is the deterministic proof); disabling dedup (2N) |
| `test_master_switch_full_sync_resets_author_dedup` (C1 and C2 a forwarding KeyDB mesh; pause the C2-to-C1 link at a proxy, `INCR` x N on C1 while the drakeydb replica holds it, `REPLICAOF C2`, resume: `c == N`) | Skipping `Clear()` at the flush point (`c == N - k`, the increments the old link had applied are dropped as stale) |
| `test_keydb_lww_concurrent_set_converges` (pause KeyDB link, newer local write on drakeydb, resume: stale KeyDB write must lose; guard off: it wins) | Guard bit off |
| `ClassicReplayTest.ClassicApplyRewrites*` (table-driven, incl. `NX KEEPTTL` and `XX KEEPTTL`); `test_keydb_set_nx_wins_over_stale_local`; `test_plain_replica_conditional_set_applies_despite_expiry_skew` (fake master, plain link) | Removing `ClassicApplyRewrites` (NX evaluates against the stale local key); gating the rewrites on `LwwGuardActive` (the plain-link test fails); not dropping `KEEPTTL` under `NX` |
| `ClassicApplyFamilyTest.EvalStampedWithEnvelopeMvccUnguardedNoDfatal`, `.EvalAppliesEvenWhenLocalIsNewer` | Dispatching `EVAL` with the guard on (the tripwire fires); with `repl_mvcc = 0` (the stamp assertion fails) |
| `test_keydb_writes_not_forwarded_to_peers` (KeyDB master, two `--active_replica --multi_master` drakeydb nodes that are each other's peers **and** both attached to it: each ends at exactly N after N `INCR`s; `assert_no_command_storm`) | Stamping classic authors with `kSelfIdx` (2N; a plain sub-replica instead of the second peer would make this vacuous) |
| `test_keydb_clock_skew_estimate_from_envelopes` (fake master: an envelope minted 60 s ahead gives `clock_skew_ms` near +60000 and the skew warning; real KeyDB on the same host stays within -2000 and +100); `ClassicApplyFamilyTest.SkewSample*` | Reading `mvcc` instead of `mvcc >> 20` (a ~10^6x skew) |
| `ClassicReplayTest.TranslateMvccRestore*` (table-driven: `<expire>` mapping, own-mvcc extraction, arity, payload pre-checks incl. `size <= 10` and type 64 as a drop class, byte-exact output) | Passing `INVALID_EXPIRE` through; taking the envelope's mvcc; dropping the type-byte check |
| `ClassicApplyFamilyTest.MvccRestore*` (stamp is own mvcc + author hash; a stale restore loses to a newer local write; plain link applies verbatim and unstamped; a non-DF, non-64 type leaves the resident key and counts `keydb_mvccrestore_failed`; a type-64 payload counts `keydb_cmds_dropped`) | Envelope mvcc; guard bit off; pre-checks removed (D-31 deletes the key) |
| `test_keydb_mvccrestore_from_keydb_mesh_merge_applies` (KeyDB B with a peer and a plain drakeydb attached merge-syncs from KeyDB A: keys and TTLs arrive; the peer keeps a newer local write, the plain replica takes A's) | Envelope mvcc or guard off (the peer takes A's stale value); `INVALID_EXPIRE` passthrough (TTL-less keys gain a TTL); no translation (keys never arrive) |
| `test_keydb_mvccrestore_unloadable_payload_skipped_and_counted` (fake master) | Removing the pre-checks (resident key deleted); not counting; counting type 64 as a failure |
| `test_classic_partial_psync_*` (exact INCR count, master `sync_partial_ok == 1`, `sync_full == 1`, `sync_partial_err == 0`), flag-off, mid-load drop, new-replid, KeyDB envelope variant, coalesced leftover, selected db across the resume (writes in db 3, drop, `+CONTINUE`, more writes with no fresh `SELECT`: all in db 3) | Fresh `io_buf` (lost bytes); `+0` offset; flag ignored; the apply context restarted in db 0 on `+CONTINUE` (the resumed writes land in db 0) |
| `RdbKeyDbTest.*`, **all under `--active_replica`** (type 64 skip + next key unstamped, subexpire, aux once, bit-63 throttle, opcode 221 beats aux, each with a control key proving stamps are observable) | Not calling `settings.Reset()` (cron stamp leaks); making the aux branch first-wins or opcode 221 non-overwriting (precedence flips) |
| `EvalReplicatedApplyNoConnNoCrash` | Removing the `conn() != nullptr` guard (null deref) |
| `ReplicatedApplyDuringTakeoverNoCrash` | Removing the null-`conn()` allowance in the `TAKEN_OVER` branch (null deref) |
| `ReplicatedApplyHandlerThrowNoConnNoCrash` (a stubbed `ECHO` handler that throws, applied through a `JournalExecutor`) | Removing the `conn() != nullptr` guard before `MarkForClose()` (SIGSEGV) |

## Byte-identity exceptions

**`--active_replica` off stays byte-identical to upstream on the journal wire and RDB output.** P7
changes neither (`kDrakeydbReplVersion` stays 68). What a *non-KeyDB* master or sub-replica can
observe changes only here:

1. **Partial PSYNC** (flag-gated by `--classic_partial_psync`, default true): a reconnecting classic
   replica sends `PSYNC <id> <offset+1>` instead of `<id> -1`. The one real exception to the slogan;
   `=false` restores today's bytes.
2. **Ungated crash, abort and refusal fixes** (P7-0). None changes a byte on the journal wire or in
   RDB output; in each, upstream crashed, aborted or refused the link:
   - **`+OK <suffix>` acceptance** (Task 0.4): a master that answers `REPLCONF capa` with `OK`, a
     space and words (`+OK keydb-fastsync-save`) is accepted. Upstream refused the link (`Bad
     response`, `REPLICAOF` failed). Any other reply is still refused. An active KeyDB's `+OK
     active-replica` parses the same way but is refused by policy until P7-1 (D-2, decision 23), so
     P7-0 alone changes nothing for it.
   - **U-13** (this round): an empty command name (`*1\r\n$0\r\n\r\n`) in a classic master's
     stream. Upstream read the first byte of the empty name (`replica.cc`, `ConsumeRedisStream`):
     SIGABRT in a debug build, a 1-byte out-of-bounds read in a release one. It is now dispatched
     as the unknown command it is and its bytes are counted exactly.
   - **U-14** (P7-1 review round): a command whose name is an array, `*0\r\n` or `*-1\r\n`, in a
     classic master's stream. Upstream read the name as a string, which throws
     `std::bad_variant_access` out of `RespExpr::GetView` and terminates the process. It is now
     skipped like `MULTI`/`EXEC`, with a rate-limited warning, its bytes counted exactly. Ungated:
     it is reachable from any classic master, not only a KeyDB one.
   - **U-15** (P7-1 Task 1.3 review round and its re-review): a connection-oriented command (`INFO`,
     `CLIENT SETNAME|GETNAME|INFO|ID|KILL`, `AUTH`, `HELLO`, `REPLCONF`, `QUIT`, `DFLY THREAD`,
     `MONITOR`, `SUBSCRIBE`, `SSUBSCRIBE`, `PSUBSCRIBE`, `WATCH`) in a classic master's stream, raw or
     inside an envelope. The apply context has no connection, and upstream dereferenced the null
     `conn()`: SIGSEGV at once, or (`MONITOR`, `SUBSCRIBE`) at the next client command, or (`WATCH`)
     a store through a dangling pointer into the dead stream context when the key is next written or
     its db flushed, which a debug build does not notice. Each now replies `No connection` and the
     stream carries on (`QUIT` replies `OK`). The same guards cover an `EVAL` body, because
     `redis.call` runs its command on the context of the `EVAL` (`CallFromScript`) and `INFO`,
     `HELLO` and `QUIT` are not `NOSCRIPT`, and the other two places that apply on a context without
     a connection, `JournalExecutor` and the RDB-load search-aux path (`LoadSearchCommandFromAux`,
     whose command name is fixed, so nothing there reaches a guarded handler today). Ungated; the
     guards live in `server_family.cc`, `main_service.cc` and `dflycmd.cc`. Two guards reach beyond the
     commands that crashed; what a stock master can observe of it is log-only:
     - `REPLCONF` is guarded at the top of its handler, so every variant now replies `No
       connection`, including `REPLCONF GETACK *`, which is real stock traffic. The text of the
       discarded reply changes (`Replicating a replica is unsupported` on a plain replica, `syntax
       error` past that refusal). Where `IsMaster()` is true (a peer-mode node), or
       `--experimental_cascaded_partial_sync` is on for a non-active node, the `LOG(ERROR) "Error in
       receiving command"` that every `GETACK` used to write also disappears.
     - `CLIENT` has one guard for every subcommand, so `LIST`, `PAUSE`, `UNPAUSE`, `TRACKING`,
       `CACHING`, `MIGRATE`, `HELP` and an unknown one are refused too. No stock master propagates
       `CLIENT`, and it is `NOSCRIPT`.

     ISSUE-REGISTER U-15 lists what was left (cluster mode) and why a filter at the stream boundary
     was rejected.
   - **U-9, U-10** (Task 0.5) and **U-12** (review round): null-`conn()` guards in `EvalInternal`'s
     migration, in `VerifyCommandState`'s `TAKEN_OVER` branch and in `DispatchCommand`'s close after
     a throwing handler. Upstream died with SIGSEGV on a replicated apply. U-10 also decides a
     behavior: a node being taken over keeps applying its own master's stream, which can make a
     takeover time out under sustained upstream writes (ISSUE-REGISTER U-10 has the reasoning).
   - **First-read clamp** (Task 0.6, `RdbLoader::Load`): applied only when the declared `$<len>` is
     smaller than the loader's first-read buffer (16 KB), so the first read would otherwise overshoot
     it. Upstream over-read the stream bytes behind the RDB and aborted. A source limit under 9
     bytes, which cannot hold the RDB signature, is a wrong-signature error.
   - **`EnsureReadInternal` guard**: an RDB that runs past its `$<len>` is an `rdb_file_corrupted`
     error with a log line. Upstream tripped a helio `DCHECK` in a debug build and returned the same
     error, without the line, in a release build.
   - **Stream hand-off** (Task 0.6): the bytes behind a correct RDB, in either framing, are applied
     as the start of the stream, only where upstream aborted on them (ISSUE-REGISTER U-11). A
     non-KeyDB master observes it: a disk-based master, Redis included, flushes the commands it
     buffered right behind the file, and such a master now syncs where it used to kill or loop the
     replica.
   - **EOF-token size and tail errors** (Task 0.6): a `$EOF:` token that is not 40 bytes, and a tail
     that disagrees with its header, are a logged error that reconnects. Upstream aborted on a
     `CHECK`.
3. **Loader** accepts KeyDB type 64 and subexpire aux, quiets noisy aux: strictly more permissive.
4. **Raw-path KeyDB-only drop** (ungated, D-7): `PEXPIREMEMBERAT` and the rest of
   `IsKeyDbOnlyCommand` are dropped and counted on the raw path instead of being dispatched as
   unknown commands. Reachable from a **non-active** KeyDB master (it sends `PEXPIREMEMBERAT` raw).
5. **Reachable only from an active KeyDB** (the one master that answers `active-replica` and sends
   RREPLAY; upstream cannot even complete that handshake): `REPLCONF capa activeExpire`, RREPLAY
   unwrap with its rewrites, EVAL stamping and dedup,
   `KEYDB.MVCCRESTORE` translation, replica active expiry (and its journaled expiry `DEL`s).
6. **Observability** (additive; INFO is already outside byte identity, ISSUE-REGISTER D-5.1):
   classic INFO fields and Prometheus `_total` series render only when the master answered
   `active-replica` or a counter is nonzero (D-13), so a stock Redis master's INFO and `/metrics`
   are upstream's.

`docs/UPSTREAM-SYNC.md`, `docs/PLAN.md`, `docs/differences.md` and ISSUE-REGISTER D-5 are updated to
this list in Task 4.4.

## PR stack

Five stacked sub-PRs. Each branch is cut from its predecessor's tip while that PR is in review and
rebased onto `origin/main` after the predecessor squash-merges; each PR is opened against `main`.

| PR | Branch | Scope |
|---|---|---|
| **P7-0** | `feat/phase7-0-closeout-and-harness` | P4 close-out docs (U-8 withdrawn), baseline gate on unmodified main, this spec and plan, `Greet()` accepts `+OK <suffix>` but refuses active-KeyDB links until P7-1 (decision 23), U-9, U-13, graceful PSYNC `CHECK`s and the full-sync tail hand-off (Task 0.6), KeyDB harness + fake classic master + smoke tests (done), `drakeydb-ci.yml` (done), a load-robust reaper-resume test (Task 0.9) |
| **P7-1** | `feat/phase7-1-rreplay-unwrap` | Envelope parse, unwrap in `ConsumeRedisStream`, KeyDB-only/unknown drop and counters, `capa activeExpire` + replica active expiry, throughput bar (conditional micro-batch task) |
| **P7-2** | `feat/phase7-2-author-stamps-dedup-guard` | Author map and cap, per-command stamps, author dedup (reservation), guard ON + classic rewrites, EVAL stamping, skew estimate, D-1 closed, `KEYDB.MVCCRESTORE` applied (decision 22) |
| **P7-3** | `feat/phase7-3-classic-partial-psync` | `--classic_partial_psync`, leftover hand-off, peer partial skips merge |
| **P7-4** | `feat/phase7-4-keydb-rdb-extras-docs-exit` | Type 64 skip, subexpire/aux noise, D-8 precedence test, operator docs, phase exit gate |

Order: P7-0 makes anything testable against real KeyDB; P7-1 makes data flow without loss; P7-2
makes it convergent; P7-3 and P7-4 are independent of each other and land last because both touch
`replica.cc` / `rdb_load.cc` regions P7-1/P7-2 reshape.

**The P7-1 PR description must say** that until P7-2's dedup lands a peer attached to two or more
forwarding KeyDB masters double-applies deltas (`INCR`, `APPEND`, ...), and that
`KEYDB.MVCCRESTORE` is unhandled (counted as an unknown command) until P7-2.

## File map

| Path | Change | PR |
|---|---|---|
| `src/server/classic_replay.{h,cc}` **(new)** | `CapaReply`/`ParseCapaReply`; `RreplayEnvelope`/`ParseRreplayEnvelope`; `ClassicApplier`; `ClassicApplyRewrites`; `TranslateMvccRestore`; `IsKeyDbOnlyCommand`; `ClassicAuthorMap` (+ `--classic_author_cap`); `AuthorDedup`; `ClassicLinkStats`/process counters; `--classic_partial_psync` (split into a second pair if it passes ~800 lines) | 0, 1, 2, 3 |
| `src/server/classic_replay_test.cc` **(new)** | Pure units and `ClassicApplyFamilyTest` | 0-3 |
| `src/server/replica.{h,cc}` | Capa parse + activeExpire send (once per `Greet()`); stream-prefix buffer and graceful `CHECK`s (0); unwrap hook and batch-flush lambda; `ApplyReplicaActiveExpiry` (three call sites) and the `SetMainLink()` marker; dedup `Clear()` at the flush point; PSYNC offset/CONTINUE; counters | 0-3 |
| `src/server/replica_types.h` | `ReplicaSummary` classic fields | 1, 2, 3 |
| `src/server/engine_shard.{h,cc}` | `replica_active_expiry_` and the heartbeat gate, `eviction_goal` kept 0 on a replica (authorized exception to the "untouched" list; ~20 lines, no `expire_only` parameter); `friend class ReplicaActiveExpiryTest` | 1 |
| `src/server/db_slice.cc` | One-expression `ExpireIfNeeded` gate (`!owner_->ReplicaActiveExpiry()`) | 1 |
| `src/server/main_service.cc` | U-9, U-10 and U-12 null-`conn()` guards (0); U-15 guards in `Quit`, `Monitor`, `Subscribe`, `PSubscribe`, `Watch` (1, review round and re-review) | 0, 1 |
| `src/server/dflycmd.cc` | U-15 null-`conn()` guard in `DFLY THREAD` (review round) | 1 |
| `src/server/transaction.cc`, `src/server/multimaster_lww.cc` | Comments only (`:1628-1648` tripwire; `ApplyLwwRewrites` contract) | 2 |
| `src/server/rdb_load.{h,cc}` | First-read clamp (0); type 64 skip, subexpire/aux handling, counters (4) | 0, 4 |
| `src/server/server_family.cc`, `multi_master.{h,cc}`, `metrics.cc` | INFO fields and gating, peer line, boot warning, Prometheus (incl. the replica-side branch); U-15 guards in `ServerFamily::Client`, `Auth`, `Info`, `Hello`, `ReplConf` (1) | 1, 2, 3, 4 |
| `src/server/CMakeLists.txt` | `classic_replay.cc` in `dragonfly_lib` (`:109-125`); `classic_replay_test` (`:200`, `:202-207`) | 0 |
| `src/server/dragonfly_test.cc` | `EvalReplicatedApplyNoConnNoCrash`, `ReplicatedApplyDuringTakeoverNoCrash`, `ReplicatedApplyHandlerThrowNoConnNoCrash` | 0 |
| `tests/dragonfly/keydb_onboarding_test.py` **(new)**, `fake_classic_master.py` **(new)** | KeyDB and fake-master suites | 0-4 |
| `tests/dragonfly/{instance,conftest,proxy}.py`, `tests/pytest.ini`, `keydb_harness_test.py`, `data/`, `tools/` | Harness, fixtures, marker, captures (delivered by Tasks 0.7, 0.8); request capture (1) | 0, 1 |
| `.github/workflows/drakeydb-ci.yml` **(new)** | KeyDB build + suite | 0 |
| `docs/{PLAN,README,UPSTREAM-SYNC,ISSUE-REGISTER,multi-master,differences,build-from-source}.md` | Close-out, KeyDB recipe, operator docs | 0, 4 |

**Newly exposed to upstream churn:** `replica.{h,cc}` (heaviest), `engine_shard.{h,cc}`,
`db_slice.cc` (one line), `rdb_load.cc`. All additions are small hooks into the new files.

## Global constraints

1. **No-forward invariant.** Nothing authored by a foreign origin is forwarded to peers. Classic
   authors are stamped with non-self `origin_idx`, which `PassesPeerEchoFilter` refuses.
2. **`--active_replica` off stays byte-identical** to upstream on the journal wire and RDB output
   except the exceptions above; the P3 golden-buffer journal test must stay green.
3. **New fork-only logic goes in new files** (`classic_replay.{h,cc}`); hooks into upstream files
   are minimal and listed in the File map.
4. CLAUDE.md: read before edit; fiber-aware `util::fb2::*` (never `std::mutex` in fiber paths); no
   `std::regex`; Google style, `snake_case` vars, `PascalCase` functions, `kPascalCase` constants;
   no commented-out code; comment density matches the surrounding file.
5. Do not edit `helio/`. Do not touch `src/core/dash.h`, `src/core/compact_object.*`.
6. Malformed input from a master is **skipped, counted and warned (rate-limited), never a
   disconnect and never a `CHECK`**; offsets stay exact. The carve-out: input that breaks the
   framing cannot be skipped past, so it is a logged error and a reconnect, still never a `CHECK`.
   That covers a malformed full-sync header or tail (Task 0.6, ISSUE-REGISTER U-11) and a RESP
   framing error in the stream; envelope-level malformations (a bad envelope, an unknown inner
   command, a failing apply) are the ones that skip.
7. `-Werror` clean; `ninja -C /home/user/drakeydb/build-dbg -j4` (never more than `-j4`); keep
   `WITH_SEARCH` ON.
8. **Falsify every test** and record the verbatim failing and passing output in
   `task-N-report.md` in the ledger directory.
9. Commit subjects <= 100 characters, conventional-commit prefix, suffix `(P7)`; do not commit or
   push without the orchestrator's go-ahead; never push to `main`.

## Verification

**Per task:** the targeted gtest binary plus touched pytest files; `pre-commit run --files
<changed>`; the falsification recorded.

**Per sub-PR gate** (`KEYDB_REQUIRED=1`, absolute paths; the plan's Global Constraints carry the
exact commands): a full `ninja -C /home/user/drakeydb/build-dbg -j4`, then the **full**
`ctest -L DFLY` (about 220 s here; a subset hides regressions in suites a PR did not expect to
touch), then pytest `multimaster_test.py`, `keydb_onboarding_test.py`, `keydb_harness_test.py` and
**`redis_replication_test.py`** — the upstream suite that drives `Greet`, `InitiatePSync` and
`ConsumeRedisStream` against a plain Redis, and runs locally through the `redis-server` fallback —
plus `replication_test.py` for P7-3, with `DRAGONFLY_PATH=/home/user/drakeydb/build-dbg/dragonfly`
and `KEYDB_SERVER_PATH=<abs keydb-server>`. Timing-sensitive tests run x10 to establish a pass
rate. Each PR also needs upstream `ci.yml` and `drakeydb-ci.yml` green before it is called done.

**Baseline (Task 0.2) and phase exit (Task 4.5):** full `ctest -L DFLY`; pytest
`multimaster_test.py`, `multimaster_merge_test.py`, `keydb_onboarding_test.py`,
`redis_replication_test.py`, `replication_test.py`, `replication_specific_test.py`,
`replication_resilience_test.py`, `replication_config_test.py`,
`cluster_test.py::test_cluster_migrations_sequence`; 0 failures, flake triage recorded, counts into
PLAN.md. The phase exit also records the D-12 release-build measurement (`DRAKEYDB_PERF=1`).

## Risks / watch items

| # | Risk | Mitigation |
|---|---|---|
| 1 | **Handshake blocker** — nothing is testable against real KeyDB until fixed | P7-0 Task 0.4 first, reproduced live and falsified |
| 2 | **Per-command dispatch throughput** vs the owner's "must keep up" bar | D-12 release-build bar in P7-1 (CI runs a smoke), re-run in peer mode in P7-2; Task 1.6 only on a release-bar failure |
| 3 | **`KEYDB.MVCCRESTORE`** (decision 22) is data on the wire: until Task 2.6 (P7-2) it is unapplied, which is key loss on a KeyDB mesh resync; a bad payload sent on to `RESTORE ... REPLACE` deletes the resident key (ISSUE-REGISTER D-31); a resync sends one envelope per key through the per-command path | P7-1 counts it as an unknown command and the PR says so; Task 2.6 pre-checks the payload, counts and warns, and pytest resyncs a few thousand keys; the D-12 load test covers the burst |
| 4 | **Guard on classic links** ends `multi-master.md`'s structural claim; EVAL must stay unguarded | Rewrite `:311-381`; tripwire comment; EVAL dispatched stamped but with the guard off (D-4.4) |
| 5 | **Partial-PSYNC leftover hand-off** (silent loss), the full-sync tail hand-off (Task 0.6) and the FULLRESYNC header overwrite | D-8, D-10; fake master makes the coalesced cases deterministic; repeat x10 |
| 6 | **Dedup watermark** can evict at 4096 (never an in-flight entry) and is cleared by a flushing full sync | KeyDB shares the exposure; D-5; documented |
| 7 | **`PeerRegistry` indices are append-only and KeyDB mints a new uuid at every start** (`server.cpp:4082`, not persisted): every restart of a KeyDB master is a new link uuid (one new index, outside the cap), every restart of a forwarding KeyDB a new author; a flapping forwarder eats the cap until drakeydb restarts | `--classic_author_cap` (default 4096, forwarded authors only) with link-idx fallback, counter and warning; `classic_link_uuid_changes` and `multimaster_peer_registry_size` make the growth visible; no reclamation (indices live in journaled entries); documented |
| 8 | **Expiry split touches `engine_shard.cc`/`db_slice.cc`**, upstream-hot files | ~20 lines + one gate expression; watchlist rows; eviction unreachable on replicas (`ReapsExpiredKeysButNeverEvicts`, with its master control) |
| 9 | **Author-hash divergence** between RDB-aux stamps (sender) and stream stamps (author) on exact mvcc ties | Documented residual |
| 10 | **Fake master / KeyDB test cost and flakiness** on a 4-core runner | `slow` marker; the release perf bar has fixed bounds and the CI smoke loose ones (D-12); x10 pass rates |
| 11 | **CI build time** for KeyDB | Cache keyed on the v6.3.4 tag |
| 12 | **Author-dedup reservation** (C1): a lost `Commit`/`Release` would stall every link that carries that author | `Reserve` waits in 100 ms slices and re-checks `running()`; an RAII reservation in the applier covers every exit of the leaf; `AuthorDedupTest.*` incl. the cancelled waiter; falsified by dropping the wait |
| 13 | **The release-build perf bar needs a quiet, pinned box**; a shared 4-core container is noisy | cpusets, x3 median, KeyDB-replica comparator; CI and debug run only the smoke; the bar is gated on `DRAKEYDB_PERF=1` |
