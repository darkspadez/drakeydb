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

This is a **one-way door, and file-only**: a snapshot written by an active node can only be
loaded by a drakeydb binary (the read side understands opcode 221 unconditionally, active or
not) — never by a stock Dragonfly. It never surfaces over replication: an active node's peer
admission (fork protocol version + UUID negotiation) refuses any consumer that hasn't negotiated
the fork protocol before a single byte of the stream is sent, so a stock Dragonfly (or plain
Redis) can never `REPLICAOF` an active drakeydb node and receive opcode 221 in the first place.
The cliff only bites when a snapshot **file** produced by an active node is copied by hand onto a
stock Dragonfly's `--dir` (or loaded there via `DEBUG RELOAD`). A non-active drakeydb node never
emits opcode 221 — the write side is active-only — so its own snapshots stay stock-compatible.
