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

Round 2a (P7-1, owner decisions 36, 37 and 38) brought `SORT` in line with Redis and KeyDB in six
more places, all of them visible to a client and none of them a journal change:

- **`LIMIT`.** The arguments parse as Redis parses integers (no `+`, no leading zero, no space, none
  beyond `long long`; the same error text) and are clamped as Redis clamps them: a negative offset is
  0, a negative count means everything from the offset on, so `LIMIT 0 -1` is all of it, and an offset
  or count past the end is cut there. Upstream Dragonfly rejected a negative argument and one beyond
  32 bits.
- **`GET`.** A key that is missing or not a string, and a pattern without `*`, are nil in the reply
  (`$-1` in RESP2, `_` in RESP3) and an empty string in the list `STORE` leaves. Upstream replied
  an empty string for a missing key, and read the key named by a pattern without `*`.
- **Several `*` in a `BY` or `GET` pattern.** Only the first is replaced by the element and the rest
  are literal. Upstream answered `syntax error`.
- **Numbers.** A numeric element or `BY` weight is loaded as Redis loads it: C `strtod` over the bytes
  up to the first NUL (`"5\0x"` is 5), refused when anything follows the number (`"5 "`), on overflow
  and underflow (`1e400`, `1e-400`, and the denormals) and on NaN. Leading whitespace, hex (`0x10`)
  and `inf` are accepted. Upstream accepted trailing whitespace and out-of-range numbers and refused a
  NUL.
- **`ALPHA BY` ties.** Elements with the same weight keep the order they were read in for a list and
  for a set that Redis would hold as an intset (all members integers, at most `set-max-intset-entries`
  of them, 512 by default: then in ascending numeric order), `DESC` included, as Redis's stable `qsort`
  does; for any other set and for a sorted set they are ordered by the element. Upstream left them in
  the order the source iterated. The limit is the flag `--sort_set_max_intset_entries` (default 512, the
  Redis and KeyDB default; `0` turns the emulation off, so that every set is ordered by the element):
  set it to the `set-max-intset-entries` of the Redis or KeyDB master being replicated from.
- **RESP3.** A `SORT` or `SORT_RO` of a set or sorted set replies an array, not the set type.

**Limitations of `ALPHA BY` ties.** What a replica of a Redis or KeyDB master cannot follow is only
what the master's own order depends on: a hash-encoded set or a sorted set is read in a per-process
order, so there drakeydb orders ties by the element; a set the master holds as a hash set because of
its history (a non-integer member once, more members than its `set-max-intset-entries` once; drakeydb's
limit is `--sort_set_max_intset_entries`, default 512) is ordered by the intset rule
here; a `LIMIT` that cuts a `BY` sort uses a different, deterministic but unstable algorithm in Redis
(`pqsort`) which is not ported, so ties under such a `LIMIT` can differ; a Redis built on a libc whose
`qsort` is not a stable mergesort (the one checked, glibc 2.39, is) or a Redis 7.2 or newer, which
holds small string sets as listpacks, orders ties differently too. An `ALPHA` reply that contains a NUL
byte is compared in full here and up to the NUL by Redis (its replies use `strcoll`; its `STORE`
compares bytes, as drakeydb does). `docs/ISSUE-REGISTER.md` D-34 has the details.

Round 2b (P7-1, owner decisions 35 and 41) adds hash-field patterns, which a KeyDB client uses and
which a replica of a KeyDB master must follow, and the flag above:

- **Hash fields in `BY` and `GET`.** `BY w_*->field` and `GET h_*->field` read field `field` of the
  hash at the key the first `*` names (`w_<element>`), as Redis and KeyDB do; upstream Dragonfly took
  the `->` as part of the key name and found nothing. The first `->` after the `*` starts the field,
  and only if at least one character follows it (`GET w_*->` is the string key `w_<element>->`, and a
  `->` before the `*` is part of the key name). The key must be a hash and hold the field: a missing
  key, a key of another type and a missing field are all "no value", which is a nil in a reply (and
  `""` in the list `STORE` leaves), a weight of 0 under a numeric `BY` and a missing weight under
  `ALPHA BY`. Without a field the key must be a string, as before. A field whose TTL has passed
  (`HSETEX`, `HEXPIRE`) is missing, as for `HGET`. The hash is read on the shard that owns the key
  part (`w_<element>`), whatever shard the source or `STORE` destination is on. A pattern is scanned
  as a C string, as Redis does: a NUL byte hides a `*` or `->` that follows it, so `GET "#\0x"` is
  `GET #`. A `BY` pattern needs its `*` to sort; with only `->` it is `nosort`.
- **`--sort_set_max_intset_entries`.** The integer-set limit of the `ALPHA BY` tie rule above (default
  512). Set it to the master's `set-max-intset-entries` when replicating from Redis or KeyDB with
  another value; a set of more members than the limit is a hash set there, which no replica can
  follow, and is ordered by the element here.

SORT's pattern lookups do not support offloaded values (the string lookup says so in a `TODO`): a
hash that experimental hash offloading (`--tiered_experimental_hash_support`) has moved to disk is read
as having no field (`docs/ISSUE-REGISTER.md` D-35).
