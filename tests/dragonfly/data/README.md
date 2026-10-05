# Golden KeyDB captures

Raw bytes that a real KeyDB v6.3.4 sent to a classic replica, kept as test vectors for the RREPLAY
parser and the classic-link applier (plan tasks 1.1 and 1.2). They were recorded by
`tests/dragonfly/tools/capture_keydb_rreplay.py`, a hand-rolled replica that speaks drakeydb's
classic handshake as it was before P7-1 Task 1.4 (without `REPLCONF capa activeExpire`) and keeps every
byte the master sends after the full sync's RDB. Run the
script from the repository root to record them again; KeyDB picks its node uuid and MVCC clock anew
on every run, so a new capture has different uuids and numbers (the structure stays) and its segment
table, printed by the script, replaces the ones below.

| File | Bytes | What |
| --- | --- | --- |
| `keydb_v6.3.4_rreplay_stream.bin` | 1953 | One active KeyDB (`active-replica yes`, `multi-master yes`) and its own writes |
| `keydb_v6.3.4_rreplay_nested_stream.bin` | 1944 | KeyDB A replicating from KeyDB B (A forwards: `multi-master-no-forward no`), seen by A's replica |

Both files are the stream only: the handshake and the RDB payload before it are not part of them. The
files hold plain ASCII, but are binary to git (`.gitattributes` here): no line ending conversion.

## What the replica saw before the stream

```
PING                                -> +PONG
REPLCONF listening-port 6399        -> +OK
REPLCONF capa eof capa psync2       -> +OK active-replica          (an active KeyDB's reply, not "+OK")
REPLCONF UUID <random uuid>         -> +<the master's node uuid>   (the uuid its envelopes carry)
PSYNC ? -1                          -> +FULLRESYNC <replid> 0
                                       $EOF:<40 hex chars>\r\n <RDB, 18991 bytes> <the same 40 chars>
```

The EOF-framed RDB is followed at once by the stream. The master starts streaming only after a
`REPLCONF ACK` that arrives once it has reaped its RDB child process, so an ACK sent straight after
the last RDB byte can be too early and the replica has to keep ACKing (the script does at every poll
of the socket, a real replica once a second).

## The envelope

Every command a KeyDB with `active-replica yes` streams is wrapped, the cron PING and the writes it
queued during a full sync included. Nothing is ever sent bare, and there is no `SELECT`:

```
*5\r\n $7\r\nRREPLAY\r\n  $36\r\n<uuid of the writing node>\r\n  $<n>\r\n<inner command, RESP>\r\n  $<d>\r\n<db>\r\n  $<m>\r\n<mvcc>\r\n
```

`inner` is a complete RESP command in one bulk string (`*3\r\n$3\r\nSET\r\n...`), so the bytes after
its last argument are that argument's CRLF and then the bulk string's own. `db` is the database the
command ran against and `mvcc` the writer's MVCC clock, both decimal in bulk strings. For example
segment 1 below, `SET k v`, is these 127 bytes (the uuid is this capture's):

```
"*5\r\n$7\r\nRREPLAY\r\n$36\r\nb1198d29-cb88-4110-922a-a6c99bd08471\r\n$27\r\n*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n\r\n$1\r\n0\r\n$19\r\n1878060925646274561\r\n"
```

## `keydb_v6.3.4_rreplay_stream.bin`

One RREPLAY envelope per segment; every uuid is `b1198d29-cb88-4110-922a-a6c99bd08471`.

| # | Offset | Length | Issued on KeyDB | Inner command (what KeyDB sent) | db |
| --- | --- | --- | --- | --- | --- |
| 0 | 0 | 150 | `SET live:during_sync streamed`, while the replica's full sync ran | `SET live:during_sync streamed` | 0 |
| 1 | 150 | 127 | `SET k v` | `SET k v` | 0 |
| 2 | 277 | 159 | `SET k2 v2 EX 100` | `SET k2 v2 PXAT 1791058569837` | 0 |
| 3 | 436 | 129 | `SELECT 3`, `SET k3 v3` | `SET k3 v3` | 3 |
| 4 | 565 | 115 | `MULTI` | `MULTI` | 0 |
| 5 | 680 | 127 | `SET a 1` | `SET a 1` | 0 |
| 6 | 807 | 130 | `INCR c` | `INCRBY c 1` | 0 |
| 7 | 937 | 114 | `EXEC` | `EXEC` | 0 |
| 8 | 1051 | 120 | `DEL k` | `DEL k` | 0 |
| 9 | 1171 | 168 | `EXPIREMEMBER seed:set m1 100` | `PEXPIREMEMBERAT seed:set m1 1791058571443` | 0 |
| 10 | 1339 | 168 | `PEXPIREMEMBERAT seed:set m2 <ms>` | `PEXPIREMEMBERAT seed:set m2 1791062069435` | 0 |
| 11 | 1507 | 168 | `EXPIREMEMBER seed:set m3 1`, then 2.5s wait | `PEXPIREMEMBERAT seed:set m3 1791058473246` | 0 |
| 12 | 1675 | 164 | `SET expiring v PX 200`, then 1.5s wait | `SET expiring v PXAT 1791058475348` | 0 |
| 13 | 1839 | 114 | cron PING (`repl-ping-replica-period 1`) | `ping` (lower case) | 0 |

What the table shows:

- KeyDB rewrites relative forms to absolute ones before it streams them: `EX` becomes `PXAT`,
  `INCR` becomes `INCRBY c 1`, `EXPIREMEMBER` becomes `PEXPIREMEMBERAT`.
- A transaction is four envelopes (`MULTI`, the writes, `EXEC`), not one holding the transaction. Their
  mvcc values rise but not by exactly one each (segments 4 to 7: `...219`, `...220`, `...222`, `...223`).
- Nothing follows segments 11 and 12 when the member and the key expire: an active replica does not
  stream its expiries, no `SREM` and no `DEL` (KeyDB `db.cpp` `propagateExpire`, `propagateSubkeyExpire`:
  "Active replicas do their own expiries, do not propogate"). A replica has to expire from the absolute
  times it was given.
- The writes of segment 0 were made after KeyDB forked its RDB child and before the transfer ended, so
  they are not in the RDB: they arrive first in the stream, wrapped like all the others.
- KeyDB logs, for a replica that does not send `REPLCONF capa activeExpire` (as the capture script
  does not; drakeydb sends it once the master's capa reply says `active-replica`):
  "replica ... does not support active expiration. This client may not correctly process key
  expirations" and "Connections between active replicas and traditional replicas is deprecated. This will
  be refused in future versions."

## `keydb_v6.3.4_rreplay_nested_stream.bin`

A is the node the replica attached to (uuid `3e4efc2d-a9f9-4a1e-90b7-ebd4171606de`) and B the one A
replicates from (uuid `307ea48c-fc10-47a1-91bb-263276269153`). A forwards what it replays, so each
write made on B reaches A's replica as an envelope of A's that holds B's complete envelope as its inner
command: depth 2. Outer and inner carry different mvcc values (A's and B's clocks), and the same db.

| # | Offset | Length | Written | Outer | Inner (B's envelope) | db |
| --- | --- | --- | --- | --- | --- | --- |
| 0 | 0 | 244 | on B during A's full sync to the replica: `SET live:during_sync x` | A | `SET live:during_sync x` | 0 |
| 1 | 244 | 228 | on B: `SET k v` | A | `SET k v` | 0 |
| 2 | 472 | 230 | on B: `SELECT 3`, `SET k3 v3` | A | `SET k3 v3` | 3 |
| 3 | 702 | 216 | on B: `MULTI` | A | `MULTI` | 0 |
| 4 | 918 | 228 | on B: `SET a 1` | A | `SET a 1` | 0 |
| 5 | 1146 | 231 | on B: `INCR c` | A | `INCRBY c 1` | 0 |
| 6 | 1377 | 215 | on B: `EXEC` | A | `EXEC` | 0 |
| 7 | 1592 | 221 | on B: `DEL k` | A | `DEL k` | 0 |
| 8 | 1813 | 131 | on A itself: `SET local v` | A | none: a plain envelope, depth 1 | 0 |

Segment 1 (228 bytes) shows the nesting; the inner envelope is 127 bytes, the bytes of a depth 1
`SET k v` envelope of B's:

```
"*5\r\n$7\r\nRREPLAY\r\n$36\r\n3e4efc2d-a9f9-4a1e-90b7-ebd4171606de\r\n$127\r\n*5\r\n$7\r\nRREPLAY\r\n$36\r\n307ea48c-fc10-47a1-91bb-263276269153\r\n$27\r\n*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n\r\n$1\r\n0\r\n$19\r\n1878060940078874625\r\n\r\n$1\r\n0\r\n$19\r\n1878060940079923202\r\n"
```
