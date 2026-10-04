# Phase 7 — decisions locked with the owner (2026-10-03)

State at start: `origin/main` @ `c60dfdb` (P0–P3, P4-0 … P4-4 squash-merged, no open PRs).
Next numbered phase = **Phase 7, KeyDB one-way onboarding** (Phase 5 superseded by P4-4;
Phase 6 delivered by P4-3; the spec's P4-5 tombstones shipped inside P4-3).

| # | Topic | Decision |
|---|---|---|
| 1 | Order | P4 close-out (docs + full P4 exit-gate baseline) as P7-0's first tasks, then P7 |
| 2 | Upstream sync (45 d overdue, upstream at v2.0.0) | Separate PR **after** P7 |
| 3 | Tombstone-lifecycle cluster (D-14/16/20/27/29) | Own phase after P7; re-own stale "P4-5"/"PR-B" owners now |
| 4 | Branches/PRs | Stacked sub-PRs `feat/phase7-<n>-<slug>` cut from `origin/main`; each opened vs main when green, watched and driven to mergeable; owner merges |
| 5 | Topology | Every drakeydb node attaches to KeyDB directly; v1 no-forward invariant kept; cutover via `REPLICAOF REMOVE` |
| 6 | RREPLAY unwrap | On **every** classic link (plain replicas too) |
| 7 | Streaming LWW | Stamp KeyDB writes `{envelope mvcc, author-uuid hash}`; audit KeyDB's propagated forms; then guard ON under `--multi_master_stream_lww` |
| 8 | KeyDB extras | Skip + rate-limited warn + counters (RDB type 64 CRON, `keydb-subexpire-*` aux, EXPIREMEMBER family, `KEYDB.*`); member TTLs documented as lost |
| 9 | KeyDB mesh | N KeyDB masters; per-author-uuid monotonic mvcc dedup shared across all classic links, bounded; docs recommend KeyDB `multi-master-no-forward yes` (`config.cpp:2976`; the spelling `multimaster-no-forward` makes KeyDB fail to start); nested unwrap ≤ 64; self-uuid drop |
| 10 | Partial PSYNC | In scope for all classic links behind `--classic_partial_psync` (default true); in-memory only (restart → full resync) |
| 11 | Apply path | Per-command dispatch for RREPLAY (own mvcc/origin each); raw classic streams keep squashing |
| 12 | Perf bar | **Must keep up with KeyDB**: bounded lag under sustained load, else optimize within P7 |
| 13 | Expiry gap | A plain replica whose master advertises `active-replica` **runs active expiry itself** (KeyDB's model) |
| 14 | Malformed envelope | Skip + count + rate-limited warn; never disconnect; offsets stay exact |
| 15 | Metrics | Per-link `rreplay_*` / `keydb_*` / `classic_*` fields in the link's INFO block + Prometheus `_total` mirrors; process-wide counters under `multimaster_` |
| 16 | KeyDB binary | v6.3.4 built from source; `KEYDB_SERVER_PATH`; tests skip locally without it; **gate runs set `KEYDB_REQUIRED=1`**, which turns a missing binary into a failure |
| 17 | CI | Minimal fork-owned `.github/workflows/drakeydb-ci.yml` (KeyDB build + suite) pulled forward into P7-0 |
| 18 | Roles | implementer = Sonnet, reviewer = Opus, advisor = Fable; effort xhigh |
| 19 | Rigor | Per task: brief → implement + falsify → spec+quality review → fix loop. Per sub-PR: whole-branch review + adversarial pass |
| 20 | Ledger | This directory, committed |
| 21 | Verification | Build in the session container; per-task targeted tests; full gate per sub-PR |
| 22 | `KEYDB.MVCCRESTORE` (owner, 2026-10-03; closes spec open item O-1) | **Applied, LWW-guarded.** Translated to `RESTORE key <ttl> <DUMP payload> REPLACE ABSTTL` (ttl = absolute ms, 0 = none), stamped with the command's **own** `<mvcc>` argument plus the envelope author's uuid hash, and run through the LWW guard on peer links; plain replicas apply it verbatim and unstamped. A payload drakeydb cannot load (KeyDB-only type, unsupported RDB version) is skipped, counted (`keydb_mvccrestore_failed`) and warned with a rate limit. Lands in P7-2 (Task 2.6) |
| 23 | Interim P7-0 behaviour (owner, 2026-10-04; adversarial pass C1) | **Refuse active-KeyDB links until P7-1.** With P7-0 alone a plain or peer `REPLICAOF <active KeyDB>` would connect, full-sync and report the link up (KeyDB even reports the replica caught up) while silently dropping every RREPLAY-wrapped streamed write. `Replica::Greet()` therefore fails the link once the capa reply has parsed as advertising `active-replica` (`ParseCapaReply` still accepts the suffix: the refusal is a logged policy, not a parse failure): an ERROR, `std::errc::protocol_not_supported`, the existing reconnect cadence (REPLICAOF fails with `replication cancelled` like any failed handshake; a `--replicaof` node retries every 500 ms). P7-1 Task 1.2 removes the refusal together with the strict xfails that pin it |

## Clarifications (2026-10-03, from spec review + advisor)

Clarifications of decisions 9, 12, 14 and 22. None of them changes a decision; the spec carries the
mechanism (D-n).

- **Decision 9: `multi-master-no-forward yes` has a precondition.** The docs recommend it only when
  every drakeydb node is `--active_replica --multi_master` and attached to every KeyDB master. A
  plain replica attaches to one master only, and with the directive on a write reaches a node only
  from the KeyDB it was written on; KeyDB itself warns that the directive needs a mesh or "dataloss
  will occur" (`config.cpp:2705-2710`; the forward is skipped at `replication.cpp:5507`). With one
  KeyDB master there is nothing to forward. In any other topology forwarding stays on and the
  per-author dedup absorbs the duplicates. (Spec D-5, D-13.)
- **Decision 12: "keep up" is a measurement.** On release builds of both servers (`taskset`-pinned,
  KeyDB `--server-threads 1`, drakeydb `--proactor_threads 2`, `redis-benchmark -P 100 -c 50 -t
  set,incr -r 100000`, `master_repl_offset` / `slave_repl_offset` sampled at 1 Hz):
  `apply_rate / produce_rate >= 0.95` over the steady window; maximum lag `<= max(2 s x
  produce_rate, 8 MB)`; the lag drains within 2 s of the load stopping; a second KeyDB attached as
  an active replica is the comparator (drakeydb within 1.5x of its lag); three runs, median; the
  raw squashed path is recorded as a reference. The release bar runs under `DRAKEYDB_PERF=1`; CI
  and debug builds run a rate-capped (about 5k ops/s) functional smoke with loose bounds (ratio
  `>= 0.5`, lag `< 32 MB`, drain `< 10 s`). The conditional squasher micro-batching task is
  triggered only by a failure of the release bar. (Spec D-12.)
- **Decision 14: "malformed" is per nesting level.** A malformed inner envelope is skipped, counted
  and warned, and does not advance its own watermark, but the enclosing envelope's author still
  advances (KeyDB parity: the inner `rreplay` is itself an executed command); at the 65th level the
  65th is the malformed one and the 64th still advances. A malformed or self-authored envelope never
  moves a watermark, and none of it disconnects. (Spec D-3, D-5.)
- **Decision 22: `<mvcc>` fallback and cron payloads.** When a `KEYDB.MVCCRESTORE`'s own `<mvcc>`
  is unusable (0, or bit 63 set: KeyDB's `OBJ_MVCC_INVALID` for a key it synced from plain Redis)
  the stamp falls back to the envelope's mvcc, as the RDB loader already treats the same sentinel
  (`rdb_load.cc:3193`). A payload whose type byte is 64 (a KeyDB cron job) is a counted drop in
  `keydb_cmds_dropped`, like `KEYDB.CRON`, and not a `keydb_mvccrestore_failed` failure. (Spec
  D-7a.)

## Corrections to PLAN.md's Phase 7 stub

- KeyDB does **not** refuse a replica without `capa activeExpire`; it only logs a deprecation warning
  (`KeyDB/src/replication.cpp:1798-1803`, v6.3.4).
- KeyDB's RREPLAY dedup is per **author uuid**, process-wide (`g_mapremote`), not per link.
- **`KEYDB.MVCCRESTORE` exists in v6.3.4 and is live** (an earlier version of this list said the
  opposite; that was wrong). It is in the command table (`server.cpp:1168`, arity 5, flags
  `write use-memory @keyspace @dangerous`); the handler is `mvccrestoreCommand`
  (`cluster.cpp:5206`): `KEYDB.MVCCRESTORE key <mvcc> <expire> <DUMP payload>`. It loads the
  payload, sets the object's mvcc to `<mvcc>`, and merges with `dbMerge` (`db.cpp:376-390`: add if
  absent, overwrite when `old_mvcc <= incoming`, otherwise keep the stored value); the reply is
  `+OK` either way. An active KeyDB that has replicas emits one per key it inserts while loading an
  RDB — including a merge full sync from one of its own masters — via `replicationNotifyLoadedKey`
  (`replication.cpp:5573-5594`, called from `rdb.cpp:2942`), RREPLAY-wrapped. `<expire>` is an
  **absolute ms deadline**, and a key with no TTL is sent as `INVALID_EXPIRE` = `LLONG_MAX`
  (`expire.h:8`, `rdb.cpp:3101`), **not** `-1`. The handler applies `setExpire` only for
  `expire >= 0`; live-checked on v6.3.4, `-1` and `INVALID_EXPIRE` both leave the key with no TTL
  and `0` makes it expire at once. Live capture (an active KeyDB with an attached raw replica,
  then `REPLICAOF` an active KeyDB holding 3 keys): 3 envelopes, db `0`, `<mvcc>` = the key's own
  mvcc, `<expire>` = `9223372036854775807` for the TTL-less keys and an absolute ms for the TTL'd
  one. Decision 22 applies it.
- An active KeyDB never propagates expiry deletes (`db.cpp:1966-1985`) — see decision 13.
- "Apply-context from envelope" cannot ride the squasher: `repl_mvcc` is copied once per batch
  (`main_service.cc:1815-1816`, `multi_command_squasher.cc:114-115`).
- **New Critical found during design:** drakeydb cannot complete a handshake with an *active* KeyDB at
  all — KeyDB answers every `REPLCONF capa …` with `+OK active-replica`
  (`replication.cpp:1740-1753`) and `Greet()` requires exactly `OK` via strict-equality
  `CheckRespIsSimpleReply` (`protocol_client.cc:433`, `replica.cc:432`, `:611`). P1's "inbound KeyDB
  works" claim was validated against plain Redis only.

## Local sandbox note (not part of the product)

The session sandbox blocks GitHub `/archive/` tarball downloads, so the local build uses a
never-committed shim in `helio/cmake/download_retry.cmake` that serves those tarballs from a mirror
built with `git clone` + `git archive | gzip -n` (byte-identical: the mirrored mimalloc tarball
matches the pinned SHA256). PR CI downloads normally.
