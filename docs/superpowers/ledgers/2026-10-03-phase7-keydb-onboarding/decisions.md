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
| 23 | Interim P7-0 behaviour (owner, 2026-10-04; adversarial pass C1) | **Refuse active-KeyDB links until P7-1.** With P7-0 alone a plain or peer `REPLICAOF <active KeyDB>` would connect, full-sync and report the link up (KeyDB even reports the replica caught up) while silently dropping every RREPLAY-wrapped streamed write. `Replica::Greet()` therefore fails the link once the capa reply has parsed as advertising `active-replica` (`ParseCapaReply` still accepts the suffix: the refusal is a logged policy, not a parse failure): an ERROR, `std::errc::protocol_not_supported`, the existing reconnect cadence (REPLICAOF fails with the reason in the reply, `Protocol not supported: master advertises active-replica; unsupported until P7-1`, while the ERROR log is rate-limited; a `--replicaof` node retries every 500 ms). The refusal also forgets the uuid and clock an earlier greeting recorded and, in peer mode, releases the UUID admission. P7-1 Task 1.2 removes the refusal together with the strict xfails that pin it |
| 24 | Replica-expiry window (owner, 2026-10-04; revised the same day after the Task 1.4 review showed the first premise wrong) | **A: keep as built and document precisely.** A flagged plain replica deletes an expired key on its first access after its own clock passes the deadline (any access, replicated or client) and otherwise in the heartbeat sweep. It diverges from the master when the master ran a command on the key before the deadline and the replica applies it after (or the mirror case below): a band of the stream lag plus clock skew around each expiry (an active KeyDB sends every TTL as an absolute `PXAT`/`PEXPIREAT`, so the band is the full lag for every key; the mirror case — a replica clock behind the master's by more than the lag — applies a command the master ran after the deadline to a key still live there; corrected from the Task 1.4 decision-24 revision, which checked the real captures). Outcomes per class (corrected after the Opus re-review of `425eeb9`): TTL refreshes (`EXPIRE`/`PEXPIRE`/`EXPIREAT`/`PEXPIREAT`/`PERSIST`/`GETEX`) and `SET XX` without an expiry find no key — **loss**; TTL-keeping writes (`INCR`/`APPEND`/`SETRANGE`/`HSET`/`HSETNX`/`SADD`/`LPUSH`/`SET .. KEEPTTL`, ...) recreate the key **without a TTL** while the master's key expires at its deadline and an active KeyDB never streams that DEL — a **permanent orphan** on the replica (decision 31); STORE-family and mover commands reading it as a source see it as missing. `SET NX` is not affected (a failed `SET NX` is not propagated). The same window exists on upstream Dragonfly's default replica of any classic master; there a stock master's expiry DEL later cleans up the orphan and a stale `RENAME`/`COPY` destination, while the losses (`PERSIST`, `SET XX`, moved elements, STORE results) are permanent under any master. KeyDB's own *active* replicas and drakeydb peers share the orphan (wording corrected after the review of `2bdf3d7`). The first version of this row claimed KeyDB's plain replicas share the window; they do not: they never delete on access and apply replicated writes to logically expired keys until their slow sweep reaps them (seconds to tens of minutes), a different and wider failure. Options B (copy KeyDB's plain replica) and C (sweep only) were rejected as wider. For the TTL-keeping class neither reliably avoids the orphan: under C the replica's own sweep usually reaps the due key first and the late write recreates it without a TTL, the same orphan; B converges only when the write lands before KeyDB's slow reap (corrected after the Opus review of `2bdf3d7`). Decisions 27 and 29 close the band, the orphan included, while the link is healthy |
| 25 | `/metrics` classic-series gate (owner, 2026-10-04; Task 1.3 review I-1) | **Sticky process-wide flag.** The classic `_total` series render once any active-KeyDB master has completed a Greet since boot (never cleared), or when the counter is nonzero. Replaces a per-scrape hop to every peer thread under the peer-manager lock, and stops zero series flapping on reconnects. A deliberate deviation from spec D-13's per-link wording for the series |
| 26 | `INFO` on the replication path (owner, 2026-10-04; Task 1.3 review O-1) | **Fix in P7-1 as ISSUE-REGISTER U-15.** A raw `INFO` from any classic master, or one inside an envelope, makes `ServerFamily::Info` dereference a null `conn()` (pre-existing upstream crash, same family as U-9/U-10/U-12). Ungated crash fix with an audit of the sibling `conn()` dereferences and a regression test; listed among the byte-identity exceptions |
| 27 | Envelope-time apply (owner, 2026-10-04; option D from the Task 1.4 design analysis) | **In P7-2.** Each enveloped command's transaction runs at the envelope author's time (the mvcc's millisecond part, `mvcc >> 20`) instead of the replica's clock, falling back to the local clock when the mvcc is 0 or implausibly far from it, so the replica makes the master's expiry decision on every access whatever the lag. On its own this leaves the heartbeat sweep and client reads on the replica's clock (a due key can be deleted before an earlier, still in-flight command lands), so it closes the band only for keys neither has reached; decision 29 closes those paths. The time is the **outermost** envelope layer's (decision 30). Due is decided as KeyDB does, strictly after the deadline, by comparing with the stamp minus 1 ms (a review design call: KeyDB's feed mints the envelope's mvcc after the command ran, and its own expiry test is `now > when`). Plan Task 2.8 |
| 28 | `--replica_delete_expired=false` on a flagged replica (owner, 2026-10-04) | **Stays ignored.** A plain replica of an active KeyDB always deletes expired keys on access (decision 24, option A); the flag is not an escape hatch to sweep-only behaviour. Documented in spec D-9 |
| 29 | Sweep clock and read-path hide (owner, 2026-10-04; Fable's analysis of decision 27's residual) | **In P7-2, Task 2.9.** On a flagged plain replica the heartbeat sweep runs on a stream clock: the largest plausible outermost envelope time applied by the main link, including the envelope KeyDB wraps around its periodic `PING`, floored at the local clock minus 60 s (`S = stream_ms ? max(stream_ms, now - 60 s) : now`), reset only when the master's uuid changes (a new KeyDB process; KeyDB mints a fresh uuid at every start) or the expiry flag is cleared, never on a reconnect or a `+CONTINUE` resume (refined in review of `f281564`: the replid is not yet known when the check runs). A client read of a key due on the local clock but not yet on `S` returns nil without deleting it; the mutable path is unchanged. A due key can linger hidden for up to one ping period (10 s default), 60 s with the link down. INFO `replica_stream_clock_lag_ms` |
| 30 | Which envelope layer's time (owner, 2026-10-04) | **The outermost layer's**, for the apply time (decision 27) and the stream clock (decision 29): the master drakeydb replicates decided expiry on its own clock when it re-ran a forwarded command, so its replica should hold what it holds. The innermost (original author's) mvcc stays the LWW stamp (Task 2.2). Moot under `multi-master-no-forward yes` |
| 31 | The option-A orphan in P7-1 (owner, 2026-10-04; Opus re-review of `425eeb9`) | **Document in P7-1, close in P7-2.** P7-1 ships option A with the permanent-orphan outcome (row 24) as a known interim limitation: spec D-9, an ISSUE-REGISTER entry and the P7-1 PR description, beside the double-apply-until-dedup note. Tasks 2.8 and 2.9 close it while the link is healthy |
| 32 | `SORT .. STORE` abort (owner, 2026-10-04; Opus review of `2bdf3d7`, C1) | **Fix in P7-1.** A fork regression from P4-0 (`1b6a2e82`), reproduced on `main`'s binary: `SORT <missing or wrong-type key> STORE <dst on another shard>` makes a shard callback return a non-OK status into `CHECK_EQ(OpStatus::OK, result)` (`transaction.cc:771`) and aborts the server, release builds included; on one shard it replies an empty array and leaves `dst` stale where Redis deletes it and replies 0. Reachable by any client, any classic master's stream and the D-9 window. Not a separate hotfix: `main` keeps it until P7-1 merges |
| 33 | `SCAN`/`KEYS`/`RANDOMKEY` and the read-path hide (owner, 2026-10-04) | **Task 2.9 also hides in `ScanCb`** (`generic_family.cc:814`, reached by `OpScan` for `RANDOMKEY`), so a monitoring scan cannot delete a key that is due on the replica's clock but not yet on the stream clock and bring the orphan back |
| 34 | SORT ordering (owner, 2026-10-05; P7-1 adversarial pass C1) | **Match Redis/KeyDB everywhere.** KeyDB replicates `SORT .. STORE` verbatim, so a drakeydb replica re-runs it and must order exactly as the master did. drakeydb breaks ties under `BY` on the weight and keeps a SET's iteration order under `BY nosort`; Redis/KeyDB break ties on the element (`sort.cpp:153-156`) and force a sorted order for a SET with `nosort` when the result is stored or produced inside a script, for replication determinism (`sort.cpp:298-308`). drakeydb's SORT adopts both rules for every caller (client-visible reply order changes for tied and nosort-SET cases versus upstream Dragonfly; an upstreamable Redis-compatibility fix). `ALPHA BY` ties depend on the master's input order and stay a documented limitation |

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
- **Decision 12: "keep up" is a measurement.** On release builds of both servers (pinned,
  KeyDB `--server-threads 1`, drakeydb `--proactor_threads 2`, `redis-benchmark -P 100 -c 50 -t
  set,incr -r 100000` run as two 25-connection processes, one `-t set` and one `-t incr`,
  `master_repl_offset` / `slave_repl_offset` sampled at 1 Hz):
  `apply_rate / produce_rate >= 0.95` over the steady window; maximum lag `<= max(2 s x
  produce_rate, 8 MB)`; the lag drains within 2 s of the load stopping; a second KeyDB attached as
  an active replica is the comparator (drakeydb's maximum lag within `max(1.5 c, c + max(1 MB, 40
  ms x produce rate))` of that replica's `c`: the floor keeps the INFO sampling skew from deciding
  the comparison, added in the Task 1.5 review rounds, spec D-12); three runs, median; the
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
