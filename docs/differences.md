# Differences with Redis

## String lengths, indices.

String sizes are limited to 256MB.
Indices (say in GETRANGE and SETRANGE commands) should be signed 32 bit integers in range
[-2147483647, 2147483648].

### String handling.

SORT does not take any locale into account.

## Expiry ranges.
Expirations are limited to 8 years. For commands with millisecond precision like PEXPIRE or PSETEX,
expirations greater than 2^28ms are quietly rounded to the nearest second losing precision of less than 0.001%.

## Lua
We use lua 5.4.4 that has been released in 2022.
That means we also support [lua integers](https://github.com/redis/redis/issues/5261).

## Active-replica RDB snapshots (drakeydb fork)

With `--active_replica` on, every locally written or peer-merged key carries an MVCC stamp
(`{clock, origin_hash}`), and `SAVE`/`BGSAVE`/`DEBUG RELOAD` persist it per key as RDB opcode 221
(`RDB_OPCODE_DF_MVCC`), 16 raw bytes written immediately before the key's type byte. A stock
(upstream) Dragonfly binary does not recognize opcode 221: its loader falls through every known
opcode check, `rdbIsObjectTypeDF()` rejects 221 as not a valid object type, and the load
hard-fails with "Unrecognized rdb object type: 221". A `drakeydb-mvcc` AUX field written earlier
in the same file at least gives a stock loader's log a clue ("Unrecognized RDB AUX field:
'drakeydb-mvcc'") before it reaches the fatal byte.

This is a **one-way door for incompatible consumers**: a snapshot written by an active node can
only be loaded by a drakeydb binary (the read side understands opcode 221 unconditionally, active
or not) — never by a stock Dragonfly. Negotiated drakeydb full sync deliberately carries the same
snapshot stream, including opcode 221. Peer admission requires the current fork protocol version
(`kDrakeydbReplVersion`, `node_identity.h` — **`68`** as of P4-4; opcode 221 alone first required
`66`, bumped to `67` when P4-3 added opcode 225, bumped again to `68` for P4-4's streaming LWW
guard and applied-write stamp floor, see `docs/multi-master.md`) before a single byte is
sent, so an older drakeydb, stock Dragonfly, or plain Redis consumer can never receive it from an
active node. The compatibility cliff therefore
appears when an active snapshot **file** is copied by hand onto a stock Dragonfly's `--dir` (or
loaded there via `DEBUG RELOAD`), while compatible drakeydb replication preserves the stamps. To
cross back deliberately, load the file with a current drakeydb under `--active_replica=false` and
save it again; the non-active write side omits opcode 221, producing a stock-compatible snapshot
at the cost of discarding all stamps. A non-active drakeydb node's own snapshots are
stock-compatible for the same reason.

## Merge LWW, tombstones, and mesh operator guidance (drakeydb fork)

P4-3 adds a second, related one-way door: RDB opcode 225 (`RDB_OPCODE_DF_TOMBSTONES`), a per-shard
delete-tombstone section with the same "write side active-only, read side unconditional" shape as
opcode 221 above, plus a merge-on-full-sync last-write-wins rule for what happens when two active
nodes (or an active node and a classic Redis/KeyDB master) full-sync from each other. See
[`docs/multi-master.md`](multi-master.md) for the full operator page: what the merge rule does and
does not guarantee, the three tombstone flags and how to size them, the `FLUSHALL`/`FLUSHDB`
tombstone-wipe hazard, the `--cache_mode` interaction, and the current fork protocol version.

## Cross-shard `SORT ... STORE` journals its result (drakeydb fork)

Since P4-3, a `SORT ... STORE dst` whose source key and `dst` land on **different shards**
journals its *effect* — a `RESTORE dst <serialized list>` — instead of the `SORT` command itself.
This applies on **every** node, including one running with `--active_replica=false`; it is the one
main journal-wire difference from upstream that is not flag-gated (the same-shard entries at the end
of this section are the others).

It is a bug fix, not a fork feature. Upstream registers `SORT` as `CO::JOURNALED` and lets the
dispatcher auto-journal it, but for a multi-shard transaction that auto-journal fires once per
participating shard on the concluding hop and builds its payload from `GetShardArgs(shard_id)` —
this shard's own key slice only, dropping `BY`/`GET`/`LIMIT`/`STORE` and the other key entirely.
The destination write was therefore **never replicated at all**, so a plain Dragonfly replica
silently did not converge on a cross-shard `SORT ... STORE`. drakeydb marks `SORT`
`CO::NO_AUTOJOURNAL` and has `SortGeneric` revive the auto-journal only when
`GetUniqueShardCnt() == 1` (no `STORE`, or a `STORE` landing on the source's shard — those still
replay the `SORT` command verbatim, apart from the same-shard entries at the end of this section).

Consequences:

- A replica no longer re-sorts anything for the cross-shard case; it applies the already-computed
  result, so it converges regardless of its own copy of the source key.
- The journal entry for that case is larger (the serialized destination list rather than the
  command), and reads as `RESTORE` rather than `SORT` in any journal-level tooling.
- The same-shard case still journals the sort *recipe*, which leaves a narrow residual where a
  `BY`/`GET` pattern key (not a transaction key) differs between nodes — tracked as D-13 in
  [`docs/ISSUE-REGISTER.md`](ISSUE-REGISTER.md).
- A same-shard `SORT <missing source> STORE <dst>` (also with `BY nosort`, and for a source that
  lazy member expiry emptied during the fetch) deletes `dst`, replies 0 as Redis does, and journals
  `DEL <dst>` alone, or nothing when there was no `dst`; upstream main journals the same `DEL`. A
  same-shard sorted `STORE` whose own fetch emptied the source journals `DEL <dst>` ahead of the
  verbatim `SORT`, and a partial lazy member expiry journals `SREM <key> <members>` ahead of it.

## `SORT` order follows Redis and KeyDB (drakeydb fork)

Redis and KeyDB give `SORT` a deterministic order, because a master replicates `SORT ... STORE` as
the command and its replicas must reproduce the result. Upstream Dragonfly differs in four places,
and so did this fork until P7-1 (owner decision 34, `docs/ISSUE-REGISTER.md` D-34, which tracks the
fix):

- **Ties under `BY`.** Redis breaks a tie between two elements with the same weight (or two
  elements whose `BY` keys do not exist) on the element itself. Dragonfly left them in the order it
  fetched them, which for a set is its iteration order.
- **`BY nosort` on a set that is stored or scripted.** Redis sorts the set alphabetically when the
  result is stored (`STORE`) or produced inside a script, so that replication and scripting are
  consistent; a list and a sorted set keep their native order. Dragonfly kept the set's iteration
  order.
- **`BY nosort` on a list or a sorted set under `DESC`.** Redis walks a list from its tail and a
  sorted set by descending rank, and counts `LIMIT` along that walk. Dragonfly ignored `DESC` here:
  the reply and the stored list came out in ascending order.
- **A missing weight under `ALPHA BY`.** Redis sorts a weight key that does not exist (or is not a
  string) before every weight that does, the empty string included. Dragonfly treated a missing
  weight as the empty string.

drakeydb's `SORT` follows Redis and KeyDB in all four for every caller, not only for a replicated
apply, so a client sees a different order than on upstream Dragonfly for tied weights, for a set
under `nosort` with `STORE` or in a script, for a list or sorted set under `nosort` and `DESC`, and
for a missing `ALPHA BY` weight. A set under plain `BY nosort`, neither stored nor scripted, still
comes out in its own iteration order, which Redis leaves open too.

**Limitation:** with `ALPHA` and `BY`, the order of two elements that have the same `BY` value is
the order Redis's own sort received them in (its sort is not stable), so a replica of a Redis or
KeyDB master can still order such ties differently from its master. drakeydb breaks them on the
element.

**Not supported:** hash-field patterns in `BY` and `GET` (`BY w_*->field`, `GET h_*->field`, which
Redis and KeyDB read as a field of the hash at `w_<element>`) are taken as part of the key name, so
they find nothing (`docs/ISSUE-REGISTER.md` D-35, owner decision pending).
