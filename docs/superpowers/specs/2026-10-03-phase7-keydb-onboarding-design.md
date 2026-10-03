# drakeydb Phase 7 — KeyDB One-Way Onboarding — Design Spec

> Status: design locked with the owner 2026-10-03. Branch `feat/phase7-0-closeout-and-harness`
> off `origin/main` (`c60dfdb`); the code under `src/` is unchanged since (only ledger and doc
> commits). Ships as five stacked sub-PRs — see [PR stack](#pr-stack).
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
  plan's "Verified anchors" table was re-read at `c60dfdb`.

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
    `EXPIRE*` as `PEXPIREAT`; member TTLs as `PEXPIREMEMBERAT`.
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
    it skips the CRC and checks only length and `version <= RDB_VERSION` (`journal_emulated` becomes
    `ignore_crc`, `generic_family.cc:69`, `:2892`); `ABSTTL 0` means no TTL, `-1` is an error, and
    `LLONG_MAX` is accepted but clamped to a TTL about 8.5 years out; with `REPLACE` it deletes the
    resident key *before* it parses the payload (`OpRestore`, ISSUE-REGISTER D-31); an elapsed
    absolute TTL deletes the resident key and creates nothing. `rdbIsObjectTypeDF` rejects
    `RDB_TYPE_CRON` (64, KeyDB `rdb.h:96`), the one KeyDB-only object type.

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
  cleared at the top of each `Greet()`. It is the *only* trigger for D-9 and for activeExpire.
- **`capa activeExpire`.** Immediately after the first capa reply (`replica.cc:431-432`) reveals
  `active-replica`, send `REPLCONF capa activeExpire` as its **own** command. KeyDB answers
  `+OK active-replica` again; this reply is parsed leniently (warn once, continue) — the master
  already showed it is active, so expiry (D-9) does not depend on the acknowledgement. Sent on plain
  **and** peer links: a peer link's node already runs active expiry (`!IsReplica()`, heartbeat),
  and a plain link's node implements it via D-9, so the capability is advertised only where true.
- A master that does not answer `active-replica` — plain Redis, Valkey, stock Dragonfly, drakeydb,
  a non-active KeyDB — sees a handshake **byte-identical to today**; no extra command is sent.
  `keydb-fastsync-save` is parsed and ignored: drakeydb never sends `capa keydb-fastsync`.

### D-3. Unwrapping RREPLAY (B.1)

`ConsumeRedisStream` keeps its raw-command path untouched. One branch is added ahead of queuing:
if `args[0]` equals `RREPLAY` (case-insensitive), **flush the pending raw batch first** (ordering:
an envelope must not overtake an earlier raw command), apply the envelope, then
`repl_offs_ += total_read` and `replica_waker_.notify()`. The existing batch-dispatch block is
refactored into a lambda used by both sites. The envelope is processed *before*
`io_buf.ConsumeInput(...)` because its `RespVec` views point into `io_buf`.

`RREPLAY` stays **out of the command registry**: no client should send it, and registering it
would change `COMMAND` output on non-active nodes.

```cpp
struct RreplayEnvelope { std::string_view uuid; std::string_view inner;
                         std::optional<DbIndex> db; uint64_t mvcc = 0; };
enum class RreplayParse { kOk, kBadArity, kBadUuid, kBadDb, kBadMvcc };
RreplayParse ParseRreplayEnvelope(const facade::RespVec& args, RreplayEnvelope* out);
```

`ParseRreplayEnvelope` mirrors KeyDB's validation (D-1.4): `argc >= 3`; uuid is
`IsValidNodeUuid` (`node_identity.h:47`); inner is a string; optional db is an integer with
`0 <= db < FLAGS_dbnum` (`generic_family.cc:57`); optional mvcc is a `u64`. uuid is compared and
keyed in **normalized lowercase** form (`NormalizeNodeUuid`), including the self-uuid check.

`ClassicApplier::HandleRreplay(args, depth = 1)` (testable without a socket):

1. Parse; any failure: `rreplay_malformed++`, `LOG_EVERY_T(WARNING, 60)`, **skip**. Never
   disconnect (owner decision 14): KeyDB itself replies an error and continues, and a disconnect
   cannot recover the bytes — a partial resync replays the identical envelope, a livelock.
   Operators with a nonzero `rreplay_malformed` re-run `REPLICAOF` to force a full resync.
2. uuid equals self: `rreplay_self_dropped++`, skip. Before dedup; does not advance.
3. Dedup (D-5): dropped envelopes count and skip.
4. Parse `inner` with a link-owned `RedisParser(Mode::SERVER)` (never `parser_`; created once,
   recreated after any non-`OK` result) until fully consumed. For each inner command:
   `MULTI`/`EXEC`/`PING`/`REPLCONF` skip; `SELECT n` sets the db; `RREPLAY` recurses with
   `depth + 1`, malformed at 65; KeyDB-only commands drop and count (D-7); `KEYDB.MVCCRESTORE` is
   translated to a `RESTORE` first (D-7); `FindCmd == nullptr` counts
   `classic_unknown_cmds_dropped`; anything else dispatches with the apply context (D-4).
   Partial or trailing inner bytes are malformed.
5. Advance the dedup watermark (D-5) once the envelope was *consumed*.

**Inner db.** The envelope db applies to the inner command through a `SelectDb` helper mirroring
`JournalExecutor::SelectDb` (`journal/executor.cc:85-100`: first use dispatches a real `SELECT` so
the `DbTable` exists, later uses set `conn_state.db_index`). Synthetic `SELECT`s are not stream
bytes and never touch `repl_offs_`.

**Dispatch** is per command, `DispatchCommand(ParsedArgs{*cmd}, cmd, ONLY_SYNC)` on a
`CommandContext` built with `FillBackedArgs`, replies into the link's `ReplyMode::NONE` builder.
A mixed stream stays correct because only envelopes take this path; a raw `SELECT` or `DEL` (KeyDB's
dead stale-key path emits one, `replication.cpp:5561`) still batches as before.

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
3. If `LwwGuardActive(guard, mvcc)`: `ApplyLwwRewrites` then `ClassicApplyRewrites` (D-6), then
   dispatch; afterwards restore `repl_mvcc = 0` and `repl_origin_idx = peer_origin_idx_`.
4. **EVAL/EVALSHA** dispatch with `repl_mvcc = 0` (unguarded, local mint stamped with the author's
   origin hash) because of the `transaction.cc:1644-1648` tripwire. The tripwire's comment and
   `docs/multi-master.md:311-381` (whose "classic links are never guarded" premise ends here) are
   rewritten. KeyDB 6.2 defaults to effects replication, so verbatim `EVAL` is the rare case.

**`ClassicAuthorMap::IdxFor(uuid)`** resolves the author's `PeerRegistry` index: the link's own uuid
maps to `peer_origin_idx_`; any other uuid goes through `PeerRegistry::AddOrGet` plus
`MvccStamper::RegisterOriginHash(idx, NodeUuidHash(uuid))` on every proactor via
`shard_set->pool()->AwaitBrief` (the primitive at `replica.cc:545-549`), memoized per link. A
**process-wide cap of 256** distinct classic authors (D-1.12: indices are append-only and each costs
an `Op::ORIGIN` LSN per shard); beyond it the write is stamped with the link's origin, counted
(`multimaster_keydb_author_overflow`) and warned. Registration precedes dispatch, so
`IncomingStamp`'s `DCHECK(hash != 0)` (`multimaster_lww.cc:134-140`) holds. A foreign origin is
refused by `PassesPeerEchoFilter` (`journal/types.cc:51-67`), so **no-forward holds by
construction** for classic-applied writes. With `ReplicaPeerMode::registry == nullptr` (tests),
stamping is off.

RDB-aux stamps (`mvcc-tstamp`, P4-2) carry the *sender's* origin hash because the file has no
author; a stream write carries the true author's. The two can differ for one forwarded key on an
exact mvcc tie only — documented as a residual. A `KEYDB.MVCCRESTORE` stamp has the same property
as an RDB-aux one: the key's own mvcc with the sending author's hash (D-7a).

### D-5. Author dedup (B.3)

`AuthorDedup`: process-wide `absl::flat_hash_map<std::string, {uint64_t mvcc; uint64_t
last_seen_ms}>` under a `util::fb2::Mutex` (links live on different proactors; critical sections
never yield; `fb2::Mutex` matches `PeerRegistry` and CLAUDE.md). Shared by all classic links, plain
or peer. It is process-wide state by definition — KeyDB's watermark is (`g_mapremote`).

- `ShouldDrop(uuid, mvcc)` iff `mvcc != 0 && entry.mvcc >= mvcc` (KeyDB parity). `mvcc == 0` is
  never deduped and never advances.
- `Advance(uuid, mvcc)` after an envelope was *consumed*: applied, or intentionally skipped (inner
  `PING`/`MULTI`/`EXEC`, KeyDB-only drop, unknown drop, a rejected `KEYDB.MVCCRESTORE`) — KeyDB
  advances on `fExec || CLIENT_MULTI`
  (D-1.4). **Never** for a malformed, self-authored or deduped envelope, and never when the link
  was cancelled mid-dispatch (the command may not have run; advancing would turn the replay into a
  silent drop). The advance happens before `repl_offs_` moves, with no yield between.
- Nested: the outer author's watermark advances even when the inner envelope was deduped.
- Bound **4096** entries; at the bound evict the smallest `last_seen_ms` (an O(n) scan on a rare
  insert). An evicted watermark can let a very late duplicate through — documented.
- Full resync never resets it: KeyDB's mvcc is monotone across restarts (`server.cpp:7263-7287`),
  and the post-snapshot stream has larger mvcc. A KeyDB restored from an old image whose clock is
  behind its past can be shadowed until it catches up (KeyDB has the same exposure).
- A partial resync replays only unprocessed bytes, so it produces no false drops.

### D-6. Streaming LWW on classic links (B.9)

Guard ON for peer-mode classic links under `--multi_master_stream_lww` (decision 7). KeyDB's
propagated forms (D-1.14) classify as follows; two need `ClassicApplyRewrites`, in
`classic_replay.cc`, gated on exactly `LwwGuardActive`:

| KeyDB wire form | Today | Verdict |
|---|---|---|
| `SET k v [PXAT abs]`, `SETEX`/`PSETEX`/`GETSET`/`INCRBYFLOAT` (all arrive as `SET`) | `kSingleKey` | OK |
| `SET k v NX\|XX` (no expiry) | `kSingleKey` | **Misclassified**: NX/XX evaluate against local state before the guard — strip NX/XX (and GET, defensively) |
| `SET k v KEEPTTL` | `kSingleKey` | Acceptable; document |
| `SETNX`, `GETSET`, `GETDEL`, `RESTORE` | rewritten by `ApplyLwwRewrites` | OK |
| `MSET`, `DEL`, `UNLINK` | self-guarded | OK |
| `MSETNX` | unclassified; skips all if any key exists | **Misclassified**: rewrite to `MSET` (becomes self-guarded) |
| `PEXPIREAT`, `PERSIST`, `GETEX` effects | unguarded | Acceptable (arrival order; `FloorAppliedStamp` governs the local stamp) |
| `INCR*`, `APPEND`, `H*`, `L*`, ... deltas | unguarded | By design |
| `RENAME`, `FLUSHALL/DB`, `EVAL*` | unguarded | `EVAL` dispatched with mvcc 0 (D-4.4) |
| expiry deletes | not propagated | Each node expires itself (D-9) |
| `KEYDB.MVCCRESTORE` (inside an envelope only) | not a command here | Translated to `RESTORE ... REPLACE ABSTTL` (`kSingleKey`), stamped with its own `<mvcc>`, guarded like any other `RESTORE` (D-7) |

A `SET` with `EX|PX|EXAT|PXAT` — with or without NX/XX/GET — already propagates as exactly
`SET k v PXAT abs`, so only the no-expiry NX/XX forms need the strip. The classifier table in
`multimaster_lww.cc` itself is unchanged.

### D-7. KeyDB-only and unknown commands (B.7)

`IsKeyDbOnlyCommand(args)` drops (counted in `keydb_cmds_dropped`, `LOG_EVERY_T(WARNING, 60)`):
`PEXPIREMEMBERAT` (the only member-expiry form KeyDB propagates, `aof.cpp:716-718`),
`EXPIREMEMBER`, `EXPIREMEMBERAT`, `PERSIST key subkey` (the 3-arg form only; `PERSIST key` is
standard), `KEYDB.CRON`, `KEYDB.HRENAME`, `KEYDB.NHSET`, `KEYDB.NHGET`, `KEYDB.MEXISTS`, and
`RREPLAY` (never dispatched as a command). **`KEYDB.MVCCRESTORE` is not on this list: it is
applied (D-7a).**

Inside envelopes, a command with `FindCmd == nullptr` is counted `classic_unknown_cmds_dropped`
with its own rate-limited warning instead of being dispatched into a `NONE` builder. On the raw
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
- **Failure accounting.** Before dispatch the translator rejects: not five arguments; `<mvcc>` not a
  u64 or `<expire>` not an i64 (KeyDB itself replies an error, `cluster.cpp:5212-5216`); and a
  payload `RESTORE` would not load: shorter than the 10-byte footer, a footer version above
  `RDB_VERSION`, or a first byte outside `rdbIsObjectTypeDF` (a KeyDB-only type). A rejected command
  is skipped, `keydb_mvccrestore_failed` is bumped, a `LOG_EVERY_T(WARNING, 60)` names the key and
  the reason, and the envelope still counts as consumed (the dedup watermark advances, D-5). The
  checks run *before* dispatch because `OpRestore` with `REPLACE` deletes the resident key before it
  parses the payload (ISSUE-REGISTER D-31); an unloadable payload must never erase a good key, as it
  does not in KeyDB (`cluster.cpp:5232`). The CRC is not verified, matching a replicated-apply
  `RESTORE` (D-1.15) and KeyDB's own handler for replication links (`cluster.cpp:5219`). The one
  residual is a payload that passes these checks and still fails inside the loader (a module-typed
  value, a deep-integrity failure): the translated command is dispatched into a capturing reply
  builder instead of the `NONE` one, an error reply also counts as `keydb_mvccrestore_failed`, and
  the resident key is lost locally exactly as D-31 describes.

### D-8. Partial PSYNC on classic links (B.4, decision 10)

`--classic_partial_psync` (bool, default true, declared in `classic_replay.cc`). In-memory only: a
restart, or a new `Replica` object, means a full resync.

- Request `PSYNC <master_repl_id> <repl_offs_ + 1>` only when the flag is on, `master_repl_id` is
  non-empty and `classic_stable_reached_`; otherwise today's `PSYNC ? -1` / `<id> -1`.
  `classic_stable_reached_` is set only after the loader finishes (or after a `+CONTINUE`); it is
  **not** `passed_full_sync_` (DFLY path only, `replica.cc:1037,1090`). A drop mid-load requests a
  full resync.
- **`classic_stable_reached_` is cleared at the exact point a `+FULLRESYNC` line parses**
  (`replica.cc:1880-1889`), because that code overwrites `repl_offs_`/`master_repl_id` before the
  `$` line validates. Without this, a malformed second line leaves a clobbered offset paired with a
  stale flag and the next attempt requests a wrong offset.
- `+CONTINUE [<newid>]`: adopt `<newid>`, keep `repl_offs_`, **no LOADING, loader, flush or merge**,
  `R_SYNC_OK`, `classic_psync_partial_ok++`. `ParseReplicationHeader` must also consume the line
  (today the `CONTINUE` branch returns without consuming, `:1922-1928`).
- **Leftover hand-off — the one silent-loss hole.** `ParseReplicationHeader` reads into
  `InitiatePSync`'s local `io_buf`; `ConsumeRedisStream` allocates its own. Bytes after
  `+CONTINUE\r\n` in the same read must reach the stream consumer: a `Replica` member buffer filled
  by `InitiatePSync` and drained into `ConsumeRedisStream`'s `io_buf` before its first read.
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
  reads; (f) mid-load drop must FULLRESYNC; (g) header overwrite on a malformed FULLRESYNC.

### D-9. Active expiry on plain replicas of an active KeyDB (decision 13)

A plain replica whose master answered `active-replica` runs active expiry itself. Today
`EngineShard::Heartbeat` runs `RetireExpiredAndEvict` only when `!IsReplica()`
(`engine_shard.cc:847`), and that function interleaves expiry (`DeleteExpiredStep`, `:942`) with
eviction (`FreeMemWithEvictionStepAtomic`, `:962`). The split:

- New per-shard flag `replica_active_expiry_` on `EngineShard` (`engine_shard.h`, beside
  `is_replica_` at `:327`) with `SetReplicaActiveExpiry(bool)`. `Heartbeat` becomes
  `if (!IsReplica()) RetireExpiredAndEvict(); else if (replica_active_expiry_)
  RetireExpiredAndEvict(/*expire_only=*/true);`. With `expire_only`, `eviction_goal` is forced to 0:
  eviction never runs on a replica, so `FreeMemWithEvictionStepAtomic`'s
  `DCHECK(!owner_->IsReplica())` (`db_slice.cc:2681`) is never reached and stays as is. The
  extra-namespace and rotating-db sweeps are already `IsActiveReplica()`-gated, so a non-active
  replica skips them.
- **A second gate must open too.** `DbSlice::ExpireIfNeeded` returns without deleting when
  `owner_->IsReplica() && !FLAGS_replica_delete_expired` (`db_slice.cc:2097-2098`); the sweep calls
  it (`:2574`). The gate becomes `IsReplica() && !owner_->ReplicaActiveExpiry() &&
  !FLAGS_replica_delete_expired`. Defaults are unchanged.
- The flag follows `master_active_replica_` (D-2): `Start()`'s `Greet()` runs before
  `MainReplicationFb` flips shards to replica mode (`replica.cc:272-273`), so the fiber applies the
  recorded value right after `SetShardStates(true)` and again after every later `Greet()` (a master
  that stops advertising `active-replica` clears it). It is cleared where shards leave replica mode
  (`SetShardStates(false)`, `:383-385`). Peer-mode links never flip shards to replica (`:272`), so
  they are unaffected: those nodes already expire.
- Deletions journal when a journal exists (`journal_deletions = true`, flagged
  `kEntryFlagExpired`), so a DFLY sub-replica of this replica converges. With a **plain Redis**
  master the flag is never set: the replica still never expires on its own.
- A replica with `--replica_delete_expired=false` that onboards from an active KeyDB expires anyway;
  the master will never send the DEL. Documented.

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
- **Graceful errors**: `ParseReplicationHeader`'s EOF-token `CHECK_EQ` (`:1910`) and
  `InitiatePSync`'s six `CHECK`s (`:823-835`) become `LOG(ERROR)` plus an error code (reconnect,
  never abort).

### D-11. Hardening: U-9 (B.8)

`EvalInternal`'s single-shard branch calls `conn_cntx->conn()->RequestAsyncMigration(...)` when
`sid != thread_index` (`main_service.cc:2458-2463`); `conn()` is null for `ConsumeRedisStream`'s
`ConnectionContext{nullptr, {}}` (`replica.cc:1113`) and `JournalExecutor`'s
(`journal/executor.cc:34-42`). Fix: `&& conn_cntx->conn() != nullptr`, **ungated**. Test
`EvalReplicatedApplyNoConnNoCrash`. Registered, not fixed: the `TAKEN_OVER` branch of
`VerifyCommandState` also dereferences `conn()` (`main_service.cc:1430`).

### D-12. Throughput bar (decision 12)

Per-command dispatch loses the squasher's batching for envelopes, and the owner's bar is that
drakeydb must **keep up with KeyDB**. Operationalised as: under sustained pipelined writes on
KeyDB, the drakeydb link never reconnects, the byte lag (`master_repl_offset` minus drakeydb's
`slave_repl_offset`) stays under a bound, and it drains to zero within a bound after the load stops
(`test_keydb_onboarding_keeps_up_under_load`, `slow`; ops/s recorded in the ledger; bounds are
calibrated on the first run and recorded — provisional until then). Cost in P7-2 (stamping, dedup
mutex, guard) is re-measured in peer mode. If the bar fails, **Task 1.6** (conditional) optimizes
inside P7: same-author/same-shard micro-batching through the squasher with **per-command** mvcc —
which needs per-command stamp plumbing because `repl_mvcc` is batch-level at
`main_service.cc:1815` and `multi_command_squasher.cc:114`; it starts with a measurement and a
design note, and stops for the advisor before touching squasher files. Peer lines gain a
`repl_offset=` field so the peer-mode lag is observable.

### D-13. Observability, flags, docs (B.10, decision 15)

Per-link counters on `Replica` (relaxed atomics, copied into `ReplicaSummary`,
`replica_types.h:14`), rendered only for **classic** links in the plain-replica block
(`server_family.cc:3164-3192`) as `key:value` lines and in `RenderPeerReplicationInfo`
(`multi_master.cc:195-216`) as `,key=value`: `rreplay_unwrapped`, `rreplay_malformed`,
`rreplay_self_dropped`, `keydb_cmds_dropped`, `keydb_mvccrestore_failed` (rejected
`KEYDB.MVCCRESTORE`s, D-7a), `classic_unknown_cmds_dropped`, `classic_psync_partial_ok`,
`classic_psync_partial_fallback`, plus `repl_offset` on peer lines.
Process-wide (relaxed atomics in `classic_replay.cc`, **not** `ServerState::Stats`, D-1.13):
`multimaster_rreplay_deduped`, `multimaster_keydb_author_overflow`,
`multimaster_keydb_rdb_cron_skipped`, `multimaster_keydb_rdb_subexpire_dropped`,
`multimaster_keydb_rdb_mvcc_invalid`. Prometheus mirrors (`metrics.cc`) are `<name>_total`,
process-wide sums for the per-link ones.

Flag: `--classic_partial_psync`. The boot limitations warning (`multi_master.cc:144-151`) gains
"KeyDB member TTLs and cron jobs are dropped on onboarding". Docs: `docs/multi-master.md`
"Onboarding from KeyDB" (topology, `multi-master-no-forward yes` and its full-mesh requirement,
backlog sizing, cutover, expiry semantics, member-TTL/cron loss, `KEYDB.MVCCRESTORE` applied and
its failure counter, counters; rewrite `:87-122` and
`:311-381`), `docs/differences.md`, `docs/UPSTREAM-SYNC.md` watchlist rows, ISSUE-REGISTER (close
D-1, D-8, U-9; add the TAKEN_OVER and subexpire-conversion follow-ups), `docs/build-from-source.md`
(KeyDB build recipe).

### D-14. Test harness and CI (decisions 16-17)

- `RedisServer` (`tests/dragonfly/instance.py:560`) falls back to `$REDIS_SERVER_PATH`, then
  `redis-server` on `PATH`, when its pinned binaries are absent (the fixture currently skips,
  `conftest.py:613-623`).
- `KeyDBServer` class and `keydb_server` / `keydb_server_factory` fixtures honour
  `KEYDB_SERVER_PATH`; a missing binary skips locally and **fails** under `KEYDB_REQUIRED=1`. A new
  `keydb` marker (`tests/pytest.ini`). KeyDB flags: `--active-replica yes` (must precede any
  `replicaof`, `config.cpp:742-753`), `--multi-master yes` for meshes.
- `tests/dragonfly/fake_classic_master.py` — a minimal asyncio classic master (handshake replies,
  scripted `PSYNC` reply and raw bytes, request capture), needed because the existing `Proxy`
  replaces only the first line of one response (`proxy.py:21-37`): it makes post-load CHECK paths,
  malformed envelopes, mixed raw/envelope ordering and coalesced `+CONTINUE` + stream bytes
  deterministic. `Proxy` gains an additive request-capture list.
- `.github/workflows/drakeydb-ci.yml`: build drakeydb with the existing builder action, build and
  cache KeyDB v6.3.4, run `keydb_onboarding_test.py` and `multimaster_test.py` with
  `KEYDB_REQUIRED=1`. Upstream `ci.yml` is untouched.

### D-15. Tests

Every test is **falsified** (revert, observe the named failure, restore, record verbatim in
`task-N-report.md`). The P4 lesson stands: assert **which value survived** and what a test would
still pass under if the feature were removed.

| Test | Falsify by |
|---|---|
| `ClassicReplayTest.ParseCapaReply*`; `test_greet_accepts_keydb_active_replica_capa_reply` (both Greet sites, proxy) | Reverting `Greet()` to strict `OK`: `REPLICAOF` fails with `Bad response ... "+OK active-replica\r\n"` |
| `test_keydb_active_handshake_and_full_sync` (real KeyDB) | Same revert |
| `ClassicReplayTest.ParseRreplayEnvelope*`; `ClassicApplyFamilyTest.*` (unwrap, db, skips, self, malformed, nested to 64) | Dropping the 65th-nesting refusal; applying inner `PING` |
| `test_plain_replica_unwraps_keydb_rreplay`, `..._nested_...`, `test_unwrap_offsets_exact` | Skipping `repl_offs_ +=` (offset lags); removing unwrap (no keys) |
| `test_unwrap_flushes_raw_batch_before_envelope` (fake master: raw `SET a 1`, envelope `SET a 2`) | Removing the pre-envelope flush (final `a == 1`) |
| `test_keydb_only_commands_dropped_with_counters` | `IsKeyDbOnlyCommand` returning false (counter lands in `classic_unknown_cmds_dropped`) |
| `test_greet_sends_capa_active_expire_only_after_active_replica_reply` (KeyDB log lacks "does not support active expiration"; Redis capture shows no send) | Never sending it (the KeyDB warning appears) |
| `test_plain_replica_of_active_keydb_expires_keys` (+ Redis control with `DEBUG SET-ACTIVE-EXPIRE 0`: replica must not expire) | Not setting the shard flag (DBSIZE stays N) |
| `test_keydb_onboarding_keeps_up_under_load` (`slow`) | n/a measurement; control = per-command path made artificially slow |
| `ClassicAuthorMapTest.*`, `ClassicApplyFamilyTest.PeerEnvelopeStampsKey...` | Not registering the origin hash (`IncomingStamp` DCHECK); not setting `repl_mvcc` (stamp is a local mint) |
| `AuthorDedupTest.*`; `test_keydb_mesh_forwarded_duplicates_deduped` (KeyDB A and B forwarding, drakeydb on both, INCR x N must equal N) | Disabling dedup (value 2N) |
| `test_keydb_lww_concurrent_set_converges` (pause KeyDB link, newer local write on drakeydb, resume: stale KeyDB write must lose; guard off: it wins) | Guard bit off |
| `test_keydb_set_nx_wins_over_stale_local` | Removing `ClassicApplyRewrites` (NX evaluates against the stale local key) |
| `test_keydb_writes_not_forwarded_to_peers` (no-forward; `assert_no_command_storm`) | Stamping classic authors with `kSelfIdx` |
| `ClassicReplayTest.TranslateMvccRestore*` (table-driven: `<expire>` mapping, own-mvcc extraction, arity, payload pre-checks, byte-exact output) | Passing `INVALID_EXPIRE` through; taking the envelope's mvcc; dropping the type-byte check |
| `ClassicApplyFamilyTest.MvccRestore*` (stamp is own mvcc + author hash; a stale restore loses to a newer local write; plain link applies verbatim and unstamped; a bad payload leaves the resident key and counts) | Envelope mvcc; guard bit off; pre-checks removed (D-31 deletes the key) |
| `test_keydb_mvccrestore_from_keydb_mesh_merge_applies` (KeyDB B with a peer and a plain drakeydb attached merge-syncs from KeyDB A: keys and TTLs arrive; the peer keeps a newer local write, the plain replica takes A's) | Envelope mvcc or guard off (the peer takes A's stale value); `INVALID_EXPIRE` passthrough (TTL-less keys gain a TTL); no translation (keys never arrive) |
| `test_keydb_mvccrestore_unloadable_payload_skipped_and_counted` (fake master) | Removing the pre-checks (resident key deleted); not counting |
| `test_classic_partial_psync_*` (exact INCR count, master `sync_partial_ok == 1`, `sync_full == 1`, `sync_partial_err == 0`), flag-off, mid-load drop, new-replid, KeyDB envelope variant, coalesced leftover | Fresh `io_buf` (lost bytes); `+0` offset; flag ignored |
| `RdbKeyDbTest.*` (type 64 skip + next key unstamped, subexpire, aux once, bit-63 throttle, opcode 221 beats aux) | Not calling `settings.Reset()` (cron stamp leaks) |
| `EvalReplicatedApplyNoConnNoCrash` | Removing the `conn() != nullptr` guard (null deref) |

## Byte-identity exceptions

**`--active_replica` off stays byte-identical to upstream on the journal wire and RDB output.** P7
changes neither (`kDrakeydbReplVersion` stays 68). What a *non-KeyDB* master or sub-replica can
observe changes only here:

1. **Partial PSYNC** (flag-gated by `--classic_partial_psync`, default true): a reconnecting classic
   replica sends `PSYNC <id> <offset+1>` instead of `<id> -1`. The one real exception to the slogan;
   `=false` restores today's bytes.
2. **U-9** (ungated): a null-`conn()` guard; crash fix, no wire effect.
3. **Loader** accepts KeyDB type 64 and subexpire aux, quiets noisy aux: strictly more permissive.
4. **Graceful errors** replace `CHECK` aborts on a malformed master handshake/full-sync tail.
5. **Reachable only from an active KeyDB** (the one master that answers `active-replica` and sends
   RREPLAY; upstream cannot even complete that handshake): `+OK <suffix>` acceptance,
   `REPLCONF capa activeExpire`, RREPLAY unwrap, `KEYDB.MVCCRESTORE` translation, replica active
   expiry (and its journaled expiry `DEL`s).
6. **Observability** (additive; INFO is already outside byte identity, ISSUE-REGISTER D-5.1):
   per-link INFO fields on classic links only, Prometheus `_total` series.

`docs/UPSTREAM-SYNC.md`, `docs/PLAN.md`, `docs/differences.md` and ISSUE-REGISTER D-5 are updated to
this list in Task 4.4.

## PR stack

Five stacked sub-PRs. Each branch is cut from its predecessor's tip while that PR is in review and
rebased onto `origin/main` after the predecessor squash-merges; each PR is opened against `main`.

| PR | Branch | Scope |
|---|---|---|
| **P7-0** | `feat/phase7-0-closeout-and-harness` | P4 close-out docs (U-8 withdrawn), baseline gate on unmodified main, this spec and plan, `Greet()` accepts `+OK <suffix>`, U-9, graceful PSYNC `CHECK`s, KeyDB harness + fake classic master + smoke test, `drakeydb-ci.yml`, a load-robust reaper-resume test (Task 0.9) |
| **P7-1** | `feat/phase7-1-rreplay-unwrap` | Envelope parse, unwrap in `ConsumeRedisStream`, KeyDB-only/unknown drop and counters, `capa activeExpire` + replica active expiry, throughput bar (conditional micro-batch task) |
| **P7-2** | `feat/phase7-2-author-stamps-dedup-guard` | Author map, per-command stamps, author dedup, guard ON + classic rewrites, D-1 closed, `KEYDB.MVCCRESTORE` applied (decision 22) |
| **P7-3** | `feat/phase7-3-classic-partial-psync` | `--classic_partial_psync`, leftover hand-off, peer partial skips merge |
| **P7-4** | `feat/phase7-4-keydb-rdb-extras-docs-exit` | Type 64 skip, subexpire/aux noise, D-8 precedence test, operator docs, phase exit gate |

Order: P7-0 makes anything testable against real KeyDB; P7-1 makes data flow without loss; P7-2
makes it convergent; P7-3 and P7-4 are independent of each other and land last because both touch
`replica.cc` / `rdb_load.cc` regions P7-1/P7-2 reshape.

## File map

| Path | Change | PR |
|---|---|---|
| `src/server/classic_replay.{h,cc}` **(new)** | `CapaReply`/`ParseCapaReply`; `RreplayEnvelope`/`ParseRreplayEnvelope`; `ClassicApplier`; `ClassicApplyRewrites`; `TranslateMvccRestore`; `IsKeyDbOnlyCommand`; `ClassicAuthorMap`; `AuthorDedup`; `ClassicLinkStats`/process counters; `--classic_partial_psync` (split into a second pair if it passes ~800 lines) | 0, 1, 2, 3 |
| `src/server/classic_replay_test.cc` **(new)** | Pure units and `ClassicApplyFamilyTest` | 0-3 |
| `src/server/replica.{h,cc}` | Capa parse + activeExpire send; unwrap hook and batch-flush lambda; expiry flag set/clear; PSYNC offset/CONTINUE/leftover; graceful `CHECK`s; counters | 0-3 |
| `src/server/replica_types.h` | `ReplicaSummary` classic fields | 1 |
| `src/server/engine_shard.{h,cc}` | `replica_active_expiry_`, `expire_only` split (authorized exception to the "untouched" list; ~15 lines) | 1 |
| `src/server/db_slice.cc` | One-line `ExpireIfNeeded` gate (`:2097-2098`) | 1 |
| `src/server/main_service.cc` | U-9 one-liner (`:2459`) | 0 |
| `src/server/transaction.cc` | Comment at `:1628-1648` only | 2 |
| `src/server/rdb_load.{h,cc}` | Type 64 skip, subexpire/aux handling, counters | 4 |
| `src/server/server_family.cc`, `multi_master.{h,cc}`, `metrics.cc` | INFO fields, peer line, boot warning, Prometheus | 1, 3, 4 |
| `src/server/CMakeLists.txt` | `classic_replay.cc` in `dragonfly_lib` (`:109-125`); `classic_replay_test` (`:200`, `:202-207`) | 0 |
| `src/server/dragonfly_test.cc` | `EvalReplicatedApplyNoConnNoCrash` | 0 |
| `tests/dragonfly/keydb_onboarding_test.py` **(new)**, `fake_classic_master.py` **(new)** | KeyDB and fake-master suites | 0-4 |
| `tests/dragonfly/{instance,conftest,proxy}.py`, `tests/pytest.ini` | Harness, fixtures, marker, request capture | 0, 1 |
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
   disconnect and never a `CHECK`**; offsets stay exact.
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
exact commands): a full `ninja -C /home/user/drakeydb/build-dbg -j4`, then
`ctest -L DFLY -R` over `multi_master_test`, `rdb_test`, `journal_test`, `peer_replication_test`,
`dragonfly_test`, `server_family_test` and `classic_replay_test`, then pytest
`multimaster_test.py` and `keydb_onboarding_test.py` (plus `replication_test.py` for P7-3) with
`DRAGONFLY_PATH=/home/user/drakeydb/build-dbg/dragonfly` and `KEYDB_SERVER_PATH=<abs keydb-server>`.
Timing-sensitive tests run x10 to establish a pass rate. Each PR also needs upstream `ci.yml` and
`drakeydb-ci.yml` green before it is called done.

**Baseline (Task 0.2) and phase exit (Task 4.5):** full `ctest -L DFLY`; pytest
`multimaster_test.py`, `multimaster_merge_test.py`, `keydb_onboarding_test.py`,
`replication_test.py`, `replication_specific_test.py`, `replication_resilience_test.py`,
`replication_config_test.py`, `cluster_test.py::test_cluster_migrations_sequence`; 0 failures, flake
triage recorded, counts into PLAN.md.

## Risks / watch items

| # | Risk | Mitigation |
|---|---|---|
| 1 | **Handshake blocker** — nothing is testable against real KeyDB until fixed | P7-0 Task 0.4 first, reproduced live and falsified |
| 2 | **Per-command dispatch throughput** vs the owner's "must keep up" bar | D-12 test in P7-1, re-run in peer mode in P7-2; conditional Task 1.6 |
| 3 | **`KEYDB.MVCCRESTORE`** (decision 22) is data on the wire: until Task 2.6 (P7-2) it is unapplied, which is key loss on a KeyDB mesh resync; a bad payload sent on to `RESTORE ... REPLACE` deletes the resident key (ISSUE-REGISTER D-31); a resync sends one envelope per key through the per-command path | P7-1 counts it as an unknown command and the PR says so; Task 2.6 pre-checks the payload, counts and warns, and pytest resyncs a few thousand keys; the D-12 load test covers the burst |
| 4 | **Guard on classic links** ends `multi-master.md`'s structural claim; EVAL must stay unguarded | Rewrite `:311-381`; tripwire comment; EVAL dispatched with mvcc 0 |
| 5 | **Partial-PSYNC leftover hand-off** (silent loss) and the FULLRESYNC header overwrite | D-8; fake master makes the coalesced case deterministic; repeat x10 |
| 6 | **Dedup watermark** never resets and can evict at 4096 | KeyDB shares the exposure; documented |
| 7 | **`PeerRegistry` indices are append-only**; a flapping forwarder with rotating uuids eats the 256 cap until restart | Cap + counter + warn; documented |
| 8 | **Expiry split touches `engine_shard.cc`/`db_slice.cc`**, upstream-hot files | ~15 lines + one gate; watchlist rows; eviction provably unreachable on replicas |
| 9 | **Author-hash divergence** between RDB-aux stamps (sender) and stream stamps (author) on exact mvcc ties | Documented residual |
| 10 | **Fake master / KeyDB test cost and flakiness** on a 4-core runner | `slow` marker, bounds calibrated and recorded, x10 pass rates |
| 11 | **CI build time** for KeyDB | Cache keyed on the v6.3.4 tag |
