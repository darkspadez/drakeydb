# Task 1.7c report — SORT hash-field patterns and the intset-order flag (P7-1 SORT round 2b)

Owner decisions 35 (hash-field patterns) and 41 (a flag for the intset-order limit). Code
`7a538e4`; brief `scratchpad/brief-sort2b.md`; evidence `scratchpad/sort2b/`. Written by the lead
from the coder's final summary (the coder's tool refused to create this file).

## What changed

- `src/server/generic_family.cc`: `SortPattern` gains a `field`, parsed by `ParseSortPattern` and
  `IsSortElementPattern`, exactly as KeyDB's `lookupKeyByPattern` reads a pattern: the first `*` is
  the element; the first `->` after that `*` starts the field, only if a character follows it; a
  trailing `->` stays in the key name; a `->` before the `*` is part of the prefix; a second `*` is
  literal. The pattern is scanned as a C string, as KeyDB and Redis do: a NUL hides a `*` or `->`
  behind it, and `"#\0x"` means `#`; a field keeps its NULs. New `OpFetchHashFieldValue` and
  `OpFetchPatternValue`, used by both fetchers. The key part (`prefix + element + suffix`) decides
  the shard. Numeric `BY` and `ALPHA BY` use the field value with round 2a's rules; a missing field is
  a missing weight (first under `ALPHA`).
- Field reads go through the new `HSetFamily::GetFieldValue` (`hset_family.{h,cc}`, the HGET read
  through `HMapWrap`), so field TTLs are honoured; like HGET, SORT then deletes a hash that lazy field
  expiry emptied (`DeleteIfEmpty`).
- New flag `--sort_set_max_intset_entries` (default 512, boot-only), read once per call in
  `SortTiesInFetchOrder`, replaces round 2a's constant; `0` turns the integer-only-set ordering
  rule off (every set breaks ties on the element).
- `tests/fakeredis/test/test_mixins/test_generic_commands.py`: the `unsupported_server_types
  ("dragonfly")` marks are removed from `test_sort_with_store_option`,
  `test_sort_with_by_and_get_option` (both already passed on the earlier build: stale marks) and
  `test_sort_with_hash` (failed before this change).
- Docs: ISSUE-REGISTER D-35 (fixed) and a D-34 paragraph on the flag; `differences.md`;
  `multi-master.md`; UPSTREAM-SYNC (`generic_family.cc` now 37 hunks vs `dbc7e6c`, the
  `hset_family.cc` row, a fakeredis row); the spec's SORT bullet and file map.

## Live comparison (KeyDB 6.3.4, Redis 7.0.15, `LC_ALL=C`)

- Hash-field probe, 156 forms (28 `BY` and 22 `GET` shapes, reply and `STORE`): KeyDB and Redis
  agree on every form with a defined order; the build before this change differed on 44; this build
  on 1 and 2 shards differs on one, `BY w_*->f LIMIT 2 5 ALPHA` (the documented `pqsort` residual).
- NUL probe, 16 forms: 6 differed before, none now.
- Flag, with KeyDB and Redis at `set-max-intset-entries` 100 and 600: at 100, sets of 50 and 100
  members are intsets in KeyDB and match; 101, 150 and 300 members are hash sets there, and two KeyDB
  processes answer differently, so no replica can follow them. At 600, sets of 513, 550 and 600
  members match drakeydb with the flag at 600 (at the default 512 drakeydb breaks their ties on the
  element). With the flag at `0` every set is bytewise.

## Tests

| Suite | Result |
|---|---|
| `generic_family_test` | 111 pass (103 before) |
| `hset_family_test` | 45 pass |
| `multi_master_test` | 224 pass, 1 skip |
| `classic_replay_test` | 99 pass |
| `keydb_onboarding_test.py -k sort`, ×3 | 16 pass each |
| `multimaster_test.py -k sort` | 2 pass |
| `keydb_harness_test.py` | 21 pass |
| whole `keydb_onboarding_test.py` | 171 pass, 3 skip (`DRAKEYDB_PERF`) |
| fakeredis `-k sort -m real` (scratch copy) | 14 pass, incl. the three unmarked tests |

The replication and reply pytests fail on the build before this change (4 failed). Adversarial
`sort_keydb.py`: BAD 3, the known hash-set tie (`dst16`); its `->` extension (11 stored forms, incl.
`MULTI` and `EVAL`): BAD 3, all `dst16` (BAD 33 on the earlier build). `tieprobe.py`: unchanged.

## Falsifications (each applied, rebuilt, tested, restored; logs `scratchpad/sort2b/fals/`)

| Mutation | gtests failing | pytests failing |
|---|---|---|
| field ignored | 7 | 4 |
| shard from the full pattern | 6 | 2 (2-shard runs) |
| wrong-type string key read as a value | 1 | 4 |
| missing field made present and empty | 4 | 4 |
| flag ignored (512 back) | 1 | 2 (`limit600`) |
| `->` honoured before the `*` | 1 | 4 |
| last `->` used instead of the first | 1 | 4 |
| trailing `->` taken as a field marker | 2 | 4 |
| pattern scanned past a NUL | 1 | 4 |
| `#` matched exactly | 1 | 4 |
| no delete of the emptied hash | 1 | 0 |
| field TTL ignored | 2 | 0 |
| flag `0` treated as unlimited | 1 | 0 |

The TTL test was weak at first (an `HGET` before the SORT had already dropped the field); reordered,
both TTL tests now fail under that mutation.

## Residuals and risks

- A set above the master's intset limit is a hash set in KeyDB, whose order is per process: not
  reproducible (the pytest compares such sets by distinct weights only; a gtest pins the flag).
- A hash offloaded by experimental tiering (`--tiered_experimental_hash_support`) reads as having no
  field (documented, `TODO` beside the string equivalent).
- The delete of a hash that lazy field expiry emptied runs outside SORT's transaction and journals a
  derived `DEL`, as the existing string-weight lazy expiry does; not tested against a journal consumer.
- Field TTLs between drakeydb nodes: a same-shard `SORT .. STORE` replayed at another clock can read a
  different field (the existing D-13 exposure; documented).
- Ties under a `LIMIT` that cuts an `ALPHA BY` sort still differ from KeyDB's `pqsort` (decision 37).
