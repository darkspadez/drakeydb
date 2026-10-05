# Task 1.8 report — the `SADD` reply count (P7-1 round 2c, owner decision 40, U-22)

Code `439d67f`, docs `189afbd`; brief `scratchpad/brief-2c.md`; evidence `scratchpad/2c/ev/`. Written by
the lead from the coder's final summary (the coder's session refused to create report files).

## What changed

- `src/server/set_family.cc` `OpAdd`: one hunk, `res +=` instead of `res =` on the string-set `Add` that
  follows an intset converted mid-call (it outgrew 256 entries or met a non-integer). The intset loop's
  count of added integers was overwritten by the string set's count over all members, which holds only
  the rest of the call: 300 integers to a new key replied 43. The set itself was always right.
- The journaled and replicated form never depended on the count: the auto-journal gates on the hop's
  status, and `OpAdd`'s own `SADD` entry is built from the members. Confirmed live with a pre-fix master
  and replica (`repl-live.txt`): `SADD` of 300 integers replied 43, both held identical members.
- Guard round-2 review nits: `ServerFamily::Role`'s comment (the other handlers that take
  `replicaof_mu_` and had no guard yet), `ErrArg("syntax error")` for the bare `DFLYMIGRATE` gtest, a
  read-only `FakeClassicMaster.stream_writer` property used by `keydb_onboarding_test.py`.
- Docs: ISSUE-REGISTER U-22, the spec's byte-identity bullet and file-map row, UPSTREAM-SYNC
  `set_family.cc` (1 hunk vs `dbc7e6c`).

## Live comparison

`sadd_matrix.py`, 69 cases, against Redis 7.0.15 (default and `set-max-intset-entries 256`), the
pre-fix build and the fixed build, with an independent set-arithmetic oracle. Redis equals the oracle
in all 63 cases it can run, at both limits. The pre-fix build deviates in 24 of 69, every one a
`SADD` reply (`SCARD` and the member digest right in all 69); the fixed build deviates in none, and
equals Redis in all 63 shared rows.

| case (`SADD` reply) | Redis 7 | Redis 7/256 | pre-fix | fixed |
|---|---|---|---|---|
| new key, 257 distinct integers | 257 | 257 | 0 | 257 |
| new key, 300 | 300 | 300 | 43 | 300 |
| new key, 600 | 600 | 600 | 343 | 600 |
| new key, 5000 | 5000 | 5000 | 4743 | 5000 |
| new key, 257 integers ×3 | 257 | 257 | 0 | 257 |
| existing 200 + 100 new | 100 | 100 | 43 | 100 |
| existing 256 + 1 new | 1 | 1 | 0 | 1 |
| `{1,2,3}` + `4 5 a 6` | 4 | 4 | 2 | 4 |
| existing 200 + integers, `a`, integers | 41 | 41 | 11 | 41 |
| two calls, 300 then 400 | [300,100] | [300,100] | [43,100] | [300,100] |

`SADDEX` (oracle only; Redis 7.0.15 has none), `SMOVE` and `SUNIONSTORE`/`SINTERSTORE`/`SDIFFSTORE`
over five destination kinds were equal on every build (their replies never come from `OpAdd`'s
count). `EVAL`, `MULTI`/`EXEC` and pipelined `SADD` follow the fix.

## Tests

`set_family_test` 39/39 (4 new: `SAddCountsAcrossIntsetOverflow`, `SAddCountsAfterConversion`,
`SAddExCountsAcrossIntsetConversion`, `SMoveAndStoreAcrossIntsetOverflow`); `classic_replay_test`
99/99; `multi_master_test` 224 + 1 skip; the strand pytest ×3 (6/6); whole `keydb_onboarding_test.py`
171 passed, 3 skipped (`DRAKEYDB_PERF`); pre-commit clean.

## Falsification

`res +=` back to `res =`: `set_family_test` 37 passed, 2 failed, 22 wrong replies, the same counts as
the pre-fix matrix (257 → 0, 300 → 43, 600 → 343, `{1,2,3}` + `4 5 a 6` → 2). Restored (md5 verified),
rebuilt, 39/39. The control rows and the `SADDEX`/`SMOVE`/store tests pass without the fix, as
designed. The test-only nits have no behaviour to mutate.

## Not run

Release build, ASAN, the tiered (`co.IsExternal()`) path of `OpAdd` (unchanged), a committed
replication test (the replica evidence is a manual live run).
