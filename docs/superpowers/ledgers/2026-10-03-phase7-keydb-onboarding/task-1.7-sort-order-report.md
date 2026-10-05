# Task 1.7 report: `SORT` orders as Redis and KeyDB do (decision 34, D-34)

Branch `feat/phase7-1-rreplay-unwrap`, started at `8b860ea` (with the orchestrator's `376c1b2` docs
commit on top). Not committed by the implementer. Brief: `brief-sort34.md`. Scratch material (the
probes, the generated expectations, every build, test and falsification log, the before-binary) is
in the orchestrator's scratchpad under `sort34/`; file names below are relative to it.

## What changed

Files: `src/server/generic_family.cc` (+65 -6 in 9 hunks), `src/server/generic_family_test.cc`
(+319), `src/server/classic_replay_test.cc` (+24), `tests/dragonfly/keydb_onboarding_test.py`
(+324), and the docs `docs/ISSUE-REGISTER.md` (D-34 rewritten, D-35 added),
`docs/differences.md`, `docs/UPSTREAM-SYNC.md` (the `generic_family.cc` row). `p71-pr-notes.md`
(scratchpad) has its decision 34 bullet replaced. `decisions.md` untouched.

### The four rules (all in `generic_family.cc`, marked `// drakeydb: P7-1 (decision 34) --`)

| Rule | KeyDB `sort.cpp` | Before | Now |
|---|---|---|---|
| 1. A numeric tie under `BY` breaks on the element | `:153-156` (`compareStringObjects(so1->obj, so2->obj)`) | `SortEntry::less` fell back to `key`, which under `BY` is the weight | falls back to `ResultKey()` (the bound element; `key` itself without `BY`), bytewise; `DESC` reverses the whole comparison, tie-break included (`greater = less(r, l)`, as KeyDB negates `cmp`) |
| 2. `ALPHA BY`: a missing weight sorts first | `:160-168` (NULL `cmpobj` before any present one; two missing tie) | missing and present-but-empty both `""` | `SortEntryAlpha::weight_missing`, set from a new `found` out-parameter of `OpFetchStringValue` (absent, expired or not a string); `less` compares the bit first, then the weight, then the element (ties on the element too, the documented residual) |
| 3. `BY nosort` on a SET that is stored or in a script is sorted `ALPHA` by the element, `BY` dropped | `:298-308` | iteration order | `nosort_set_sorted` in `SortGeneric`: `!to_sort && type == OBJ_SET && (store_key \|\| cntx->conn_state.script_info)` turns `to_sort` and `alpha` on; the already fetched `raw_elements` become `SortEntry<true>` (no second fetch, so no second lazy-expiry journaling); `GET`, `DESC`, `LIMIT` apply after the sort |
| 4. `BY nosort` on a LIST or ZSET honours `DESC` | `:356-382`, `:401-430` | `params.reversed` ignored | `std::reverse(raw_elements)` before `LIMIT`/`GET`/`STORE`; `LIMIT offset count` then counts along the reversed walk, which is KeyDB's `len - offset - 1` start going backwards |

Design notes:

- The script predicate is `cmd_cntx->server_conn_cntx()->conn_state.script_info`, as
  `SetReplies` (`set_family.cc`) already uses ("output is sorted under scripts"). `SortGeneric`
  already took that context, which a replicated apply has although `conn()` is null; the new
  `ClassicApplyFamilyTest` test below pins it on a stream's context.
- Journaling is untouched: `SortStoreNothing`, `OpStore`'s hand-journal, `source_deleted_by_fetch`,
  the single-shard revival (D-33) are as they were. A forced-sorted set STORE ends in
  `SortVisitor`'s concluding `Execute(store_callback, true)` with `source_deleted_by_fetch=false`,
  exactly the values the old unsorted STORE used; a set the unsorted fetch's expiry emptied still
  goes to `SortStoreNothing` before this point. `multi_master_test` (all SORT journal tests) is
  unchanged and passes.
- Cost: an `ALPHA` entry is 8 bytes bigger (a `bool` after a 64-byte base). Numeric entries are
  unchanged (the bit lives in `SortEntryAlpha`, beside `SortEntryScore`'s `double`).
- Ordering is shard-count independent: the same replies from a 1, 2 and 4 shard server
  (`probe-final-{1,2,4}.json`, identical).

### Beyond the brief: a crash found while checking `LIMIT` (item 5), fixed with two hunks

`LIMIT 1 4294967295` and `LIMIT 4294967295 1` are valid in Redis and KeyDB. Here `offset + count`
wrapped around a `uint32_t` in `GetSortRange` and in `SortVisitor`'s `partial_sort` end, so the range
ended before it began: the plain `SORT ln LIMIT 1 4294967295` replied a garbage array length
(`UnicodeDecodeError` in redis-py) and the `BY` and `BY nosort` forms killed the server (`SIGSEGV`,
rc -11; `probe-before.txt`). Reachable by a client and by a KeyDB master's stream. Both sums are
`uint64_t` now. The hunks are marked `// drakeydb: P7-1 --` (not "decision 34"), counted apart in
`UPSTREAM-SYNC.md`, and registered in D-34 ("found on the way"). I made this change although the
brief said "check and say": a crash on a command the brief had me rewrite, in the function the
brief names, with a two-line fix, seemed worse to leave and to report than to fix; revert the two
hunks (and the `LimitCountBeyondUint32DoesNotOverflow` test) if you want it separate.

## Tests

Expected lists come from real servers, not by hand. `probe.py` runs 123 forms against a standalone
KeyDB v6.3.4, Redis 7.0.15 and a drakeydb binary, same data; `gen_expected.py` ran every table row
on KeyDB and Redis and refused any row where the reply, the list a STORE leaves, or (nosort tables)
both inside EVAL differed between the two or between themselves (no row was refused after the
nosort-set tables were told that a plain SET reply is unordered); it printed the C++ rows
(`expected-cpp.txt`, `expected.json`). `verify_sort_ro.py` checked the 67 `SORT_RO` replies the
gtests assert on Redis (KeyDB 6.3.4 has no `SORT_RO`): 0 differ.

**gtest, `generic_family_test.cc`** (new fixture `GenericSortOrderTest`, 2 shards, `dst` on and off
the source's shard for every STORE; reuses `SortStoreDstKey`/`SortStoreCommand`):

| Test | Rows | What it pins |
|---|---|---|
| `TiedByWeightsBreakOnTheElement` | 15 | shared weights, all-missing weights, a list with mostly missing weights, 0 vs missing vs -1, list duplicates, `DESC`, `LIMIT`, `GET # GET h_*`; reply (`SORT`, `SORT_RO`) and STORE |
| `AlphaByPutsAMissingWeightFirst` | 7 | missing vs empty, a list-typed weight key as missing, `DESC`, `LIMIT`, `GET #` |
| `AlphaByTiesBreakOnTheElement` | 5 | drakeydb's own rule for what KeyDB leaves open (says so in a comment; KeyDB answers the list case `b a b a`) |
| `NosortSetThatIsStoredOrScriptedIsSortedAlpha` | 7 | STORE (both shards), EVAL reply of `SORT` and `SORT_RO`, EVAL with STORE; `DESC`, `LIMIT 0 3`, `DESC LIMIT 1 2`, `GET # GET h_*`, an intset (`1 10 2 ...`); and `MULTI` + STORE |
| `NosortSetReplyKeepsItsMembers` | - | a plain `BY nosort` set reply (no order asserted): each member once, `LIMIT` takes 3 distinct members, inside `MULTI` too |
| `NosortListAndZsetHonourDesc` | 16 | list and zset: ASC, `DESC`, `LIMIT 1 2`, `DESC LIMIT 1 2`, `DESC LIMIT 3 10`, `DESC LIMIT 9 2` (empty), `GET #`; reply, STORE (both shards), EVAL |
| `LimitCountBeyondUint32DoesNotOverflow` | 6 | the crash above, sorted, `BY`, `BY nosort`, `DESC`, a huge offset |

**gtest, `classic_replay_test.cc`:** `ClassicApplyFamilyTest.SortOfASetUnderNosortInAScriptIsSortedWhenAppliedFromAStream`
(raw and in an RREPLAY envelope; a script that `SORT`s a set under `BY nosort` and `RPUSH`es the
result: the list is `a .. j`, not the set's `f i h g ...`). This is the brief's "must also hold on
the replicated-apply path" made a test; it is outside the file the brief names.

**pytest, `keydb_onboarding_test.py`** (mark `keydb`; after the D-33 SORT test):

- `test_plain_replica_of_active_keydb_orders_sort_store_as_keydb_does[1, 2 shards]`: an active KeyDB
  master runs 60 stored forms (the `sort_keydb.py` forms and the new ones: nosort `DESC` list and
  zset, `LIMIT` variants, `ALPHA BY` missing vs empty, a count beyond `uint32`), seven sources that
  cannot be sorted or sort oddly (missing, wrong-type, non-numeric, hex) or are their own
  destination, a `MULTI` of three and three `EVAL`s; the replica catches up (a marker written
  last); every key is compared, type and order, and the replica must not have died.
- `test_sort_replies_in_the_order_keydb_does[1, 2 shards]`: 50 forms plus 8 inside `EVAL`, replies
  of a standalone KeyDB and a standalone drakeydb equal (a plain `BY nosort` set as a set). Ten more
  forms run as stored sorts only: a plain set under `nosort` has no order, and `GET` of a missing key
  replies nil there and `""` here.
- The data holds no `ALPHA BY` tie (a comment explains the exclusion, as the brief asked, and one
  more: a `1e400` list is left out because Redis and KeyDB keep `errno` between commands, see below).

Results on the final build (`build-dbg`, `ninja -j3 dragonfly generic_family_test multi_master_test
classic_replay_test`, no warnings):

| Run | Result |
|---|---|
| `generic_family_test` (all) | 95 passed (88 existing + 7 new) -- `final-gtest-generic.txt` |
| `multi_master_test` (all) | 224 passed, 1 skipped (`NodeIdentityFile.UnwritableDirIsEphemeral`, runs as root; not this change) -- `final-gtest-multimaster.txt`; the SORT journal tests (`SortDerivedDeleteReachesPeers...`, `SortPartialExpiry...`, `FullExpirySortStore...`, `SameShardStoreOfMissingSource...`, `CrossShardStoreHandJournals...`, `CrossShardStoreOfMissingSource...`) pass unchanged |
| `classic_replay_test` (all) | 90 passed (89 + 1 new) -- `final-gtest-classic-replay.txt` |
| the 4 new pytests, `--count=3` | 12 passed -- `final-pytest-new-x3.txt` |
| the D-33 SORT pytest (`..._sort_store_of_an_unsortable_source_does_not_abort`) | 6 passed -- `final-pytest-d33.txt` |
| `multimaster_test.py -k sort` (the two existing SORT pytests) | 2 passed -- `final-pytest-multimaster-sort.txt` |
| the 4 new pytests on the before-binary (`drakeydb-before`, the `8b860ea` build) | 4 failed: ordering diffs, and the `LIMIT` forms kill the server (`-11`) -- `pytest-new-before-binary.txt` |

Pytest was run as `KEYDB_SERVER_PATH=<scratchpad>/KeyDB/src/keydb-server KEYDB_REQUIRED=1
DRAGONFLY_PATH=/home/user/drakeydb/build-dbg/dragonfly flock /tmp/drakey-pytest.lock
/root/drakey-venv-pinned/bin/python -m pytest tests/dragonfly/keydb_onboarding_test.py -k "..."`.

### The adversarial probes, rerun on the final binary

- `adv-p71/sort_keydb.py` (active KeyDB master, plain drakeydb replicas of 1, 2, 4 shards): **`BAD 3`,
  not `BAD 0`**: the one remaining difference is `dst16`, `SORT s BY w_* ALPHA STORE`, once per
  replica. That is the documented `ALPHA BY` tie (b..j all weigh `"1"`; KeyDB `e h g f j i d c b a`
  against drakeydb `b c d .. j a`). It was `BAD 30` (ten keys per replica) before. The same script
  without that form (`adv-p71/sort_keydb_noties.py`, the line removed, nothing else) is **`BAD 0`**.
  Logs: `sort_keydb-after.log`, `sort_keydb-after-noties.log`.
- `sort_dfly.py`: `"diffs": []` for all three shard mixes. `sort_peers.py`: `BAD 0`.
  (`sort_dfly-after.log`, `sort_peers-after.log`.)
- `probe.py` against standalone KeyDB, 1, 2 and 4 shards: 80 of 115 forms differed before; 30 of 123
  differ after (123 = 115 + 8 odd-weight forms), the same 30 at every shard count. All 30 are: the
  two `ALPHA BY` ties, a plain `BY nosort` set under `LIMIT` (unordered in both), two `SORT_RO`
  forms (KeyDB has none; drakeydb equals Redis 7), `GET` of a missing key (nil vs `""`, 2), `GET` of
  a fixed key (2), negative `LIMIT` (9), `LIMIT` beyond `uint32` (6, a parse error here), two
  numeric spellings (trailing whitespace, overflow), and the four hash-field forms (D-35).

## Falsifications

Each: the good copy of `generic_family.cc` saved (`generic_family.cc.fixed`, md5 `9982bb52...`), the
mutation applied by `fals.sh` (it logs the exact replacement to `progress-sort34.md` before it
builds), `ninja -j3 dragonfly generic_family_test`, then
`generic_family_test --gtest_filter='GenericSortOrderTest.*'` and the 4 new pytests, then the good
copy back (`cmp` identical, logged). Raw output: `fals-<name>-{build,gtest,pytest}.txt`.

| | Mutation (exact) | gtest | pytest |
|---|---|---|---|
| (a) tie-break back to the weight | `return l.ResultKey() < r.ResultKey();` -> `return l.key < r.key;` | 3 fail: `TiedByWeightsBreakOnTheElement` (`SORT s BY w_*`: actual `f i h g j c e d b a`, element #0 `f` is not `b`), `AlphaByTiesBreakOnTheElement`, `LimitCountBeyondUint32DoesNotOverflow` (a `BY` form with ties) | 4 of 4 fail: `dst:0 (SORT s BY w_* STORE): KeyDB [b c d e f g h i j a], replica [f i h g j c e d b a]` |
| (b) no forced sort for a stored set | `(params.store_key \|\| cntx->conn_state.script_info);` -> `(cntx->conn_state.script_info);` | 1 fails: `NosortSetThatIsStoredOrScriptedIsSortedAlpha` (STORE on both shards: actual `f i h g j c e d b a`, element #0 `f` is not `a`); the other 6 pass | the 2 replication pytests fail (`dst:50 (SORT s BY nosort STORE): KeyDB [a .. j], replica [f i h g j c e d b a]`, 7 keys, the `EVAL` store too); the 2 reply pytests pass (EVAL still sorts) |
| (c) script predicate always false | same line -> `(params.store_key \|\| false);` | the same 1 fails, on its `EVAL` assertions (`Run(eval)`: actual `f i h g j c e d b a`) | the 2 reply pytests fail (`EVAL SORT s BY nosort: KeyDB [a .. j], drakeydb [f i h g j c e d b a]`); the replication pytests pass |
| (d) `DESC` ignored on the list/zset walk | `... && params.reversed && (source_type == OBJ_LIST ...` -> `... && false && ...` | 2 fail: `NosortListAndZsetHonourDesc`, `LimitCountBeyondUint32DoesNotOverflow` (its `zl BY nosort DESC` row) | 4 of 4 fail: `dst:24 (SORT zl BY nosort DESC STORE): KeyDB [v w z y x], replica [x y z w v]` |
| (e) missing-weight bit ignored | `entry.SetWeightMissing(!found);  // ...` -> `entry.SetWeightMissing(false);` | 1 fails: `AlphaByPutsAMissingWeightFirst` (`SORT sa BY aw_* ALPHA`: actual `b c f d a e`, element #0 `b` is not `c`) | 4 of 4 fail: `dst:15 (SORT sa BY aw_* ALPHA STORE): KeyDB [c b f d a e], replica [b c f d a e]` |
| (f) `LIMIT` overflow in `GetSortRange` | `std::min<uint64_t>(uint64_t{bounds->offset} + bounds->count, ...)` -> `std::min<uint32_t>(bounds->offset + bounds->count, ...)` | the test binary dies with SIGSEGV (rc 139) in `LimitCountBeyondUint32DoesNotOverflow` | 4 of 4 fail, the server dies (`exit code -11`) |
| (g) `LIMIT` overflow in the partial sort only | the `SortVisitor` `sort_it` sum back to `uint32_t` | 1 fails: `LimitCountBeyondUint32DoesNotOverflow` (not sorted) | 4 of 4 fail: `dst:39 (SORT ln LIMIT 1 4294967295 STORE): KeyDB [3 3 5 5 5 7 9], replica [5 9 3 7 5 5 3]` |
| (h) script predicate false, on a stream | as (c); `classic_replay_test` only | `ClassicApplyFamilyTest.SortOfASetUnderNosortInAScriptIsSortedWhenAppliedFromAStream` fails (`lrange` of both `raw` and `in-envelope` destinations: actual `f i h g j c e d b a`, element #0 `f` is not `a`) | - |

After the last one the good copy was restored (`cmp` identical to `generic_family.cc.fixed`), the
tree rebuilt (`build-final2.txt`) and the suites rerun (`generic_family_test` 95, `classic_replay_test`
90 passed).

## Item 5: nothing else in the ordering differs on these forms

Measured by `probe.py` on standalone KeyDB, and for numbers by `numparse.py` (46 spellings of a `BY`
weight against KeyDB, restarted per spelling):

- **Odd numerics** (`0x10`, `" 5"`, `1e3`, `+-inf`, `""`) converge, as D-34 says, for elements
  (`odd_numerics`) and for weights (`odd_weights*`). Also equal: `0x1A`, `0x1p3`, `.5`, `5.`, `+5`,
  `-0`, a leading tab or newline, `inf`, `infinity`, `-Infinity`, `nan` (an error in both).
  **Different**, and not ordering: KeyDB rejects a trailing space/tab/newline (`"5 "`) and an
  out-of-range number (`1e400`, `-1e400`, `1e-400`, a 400-digit number) with "can't be converted",
  and drakeydb accepts them (`absl::SimpleAtod`). A failing SORT is not replicated, so no replica
  diverges from it. The other direction is the one that would: KeyDB accepts a number followed by a
  NUL byte (`"5\0"`, `strtod` stops there) and drakeydb rejects it, so that `SORT .. STORE` leaves a
  drakeydb replica's `dst` stale (binary weights only; recorded in D-34, not fixed).
- **`ALPHA` without `BY`**: elements, bytewise; identical strings only for list duplicates
  (`alpha_list*`, `alpha_list_dups_store`): equal. `ALPHA` replies in KeyDB use `strcoll`, so under a
  non-C locale they could differ from drakeydb's bytewise order; STORE is bytewise there too. That
  is the existing "SORT does not take any locale into account" in `differences.md`.
- **`LIMIT`**: in range (`0 0`, `2 0`, `5 100`, `100 2`, `nosort` list/zset past the end): equal.
  Found: the `uint32` overflow above (fixed). Left: a negative argument and one beyond `uint32` are
  errors here (`value is not an integer or out of range`, pinned by upstream's `SortNegativeLimit`)
  where Redis and KeyDB accept them (negative offset 0, negative count all, a larger count clamped),
  so `SORT l LIMIT 0 -1 STORE d` succeeds on a KeyDB master and fails on its drakeydb replica, leaving
  `d` stale. Recorded in D-34; fixing it changes an upstream test, so it is the owner's call.
- **`GET #`**: equal everywhere it was used (`num_by_get`, `nosort_*_desc_get`, ...). Not equal and
  not ordering: `GET` of a key that does not exist replies `""` here and nil in Redis and KeyDB
  (STORE keeps `""` in both; upstream's `SortGet` test pins the `""`), and a `GET` pattern without
  `*` reads the literal key here (`SORT s GET str STORE d` stores `str`'s value) where Redis and
  KeyDB give nil. Recorded in D-34; the second can leave a KeyDB replica's `dst` different.
- **Hash-field patterns** (`->`): D-35, below.

### One real-KeyDB comparison for D-35 (probed, not implemented)

A set `s` of `a .. j`, and a hash `hw_<c>` per element, `f` = the code of the letter mod 3, `g` =
`G<c>` (standalone KeyDB 6.3.4 against the final build, 2 shards; `probe-final-2.txt`):

```
SORT s BY hw_*->f             KeyDB c f i a d g j b e h      drakeydb a b c d e f g h i j
SORT s BY hw_*->f STORE d     the same two lists in d
SORT s ALPHA GET hw_*->g      KeyDB Ga Gb .. Gj              drakeydb "" "" ... "" (ten)
SORT s ALPHA GET hw_*->g STORE d   the same two lists in d
```

drakeydb takes `hw_a->f` as the key name, which does not exist: every weight is missing (0, a tie,
element order) and every `GET` value is `""`. (Before the fix the first line was the set's
iteration order.) `ISSUE-REGISTER.md` D-35 records it, owner decision pending; the fakeredis test
`test_sort_with_hash` stays marked `unsupported_server_types("dragonfly")` and still fails here.

### Upstream tests that pin the old order

`generic_family_test` passes in full, so none does. `tests/dragonfly/*.py` has no SORT assertion
outside `multimaster_test.py` (two plain `SORT .. STORE` of a list, passing) and
`keydb_onboarding_test.py`. The fakeredis suite (`tests/fakeredis/test/test_mixins/test_generic_commands.py`)
is **not runnable as it stands** in the pinned venv (`fakeredis` 2.39 has no `fakeredis._server`,
which its `conftest.py` imports; `hypothesis` is missing too). I ran it from a scratch copy with a
small shim for that import, the `--self-contained-html` option dropped, and the
`unsupported_server_types("dragonfly")` marker removed from its three SORT tests, `-m real` against
a drakeydb on port 6380, once with the before-binary and once with the final one
(`fakeredis-before.txt`, `fakeredis-after.txt`): identical, 12 passed, 2 failed. The two failures are
`test_sort_with_by_and_get_option` (line 172: `get="data_1"`, the fixed-key `GET`) and
`test_sort_with_hash` (line 199: `->`), so those marks stay. **`test_sort_with_store_option`,
also marked unsupported for Dragonfly, passes on both the before and the after binary**: the mark is
stale and unrelated to this change. I did not unmark it: the harness only ran from a patched copy,
and the pass does not come from this work.

## Deviations from the brief

1. **`sort_keydb.py` is `BAD 3`, not `BAD 0`**, for the one form that is the documented residual;
   `BAD 0` is with that form removed (above). The brief expected `BAD 0` of the unmodified probe,
   which contains that form.
2. **A `LIMIT` overflow crash was fixed** (two extra hunks, one test), see above.
3. **A gtest in `classic_replay_test.cc`**, a file the brief does not name, for the stream context.
4. **D-34's text grew** beyond the status and the two list items the brief asked for: the other
   differences found while checking item 5 (negative `LIMIT`, `GET`, numeric spellings) are recorded
   there as "left open", because they were found here and the register is where they would be
   missed; promote them to their own entries if you prefer.
5. **The ALPHA BY tie gtest table is not from KeyDB** (KeyDB's answer there depends on its input
   order); the test says so. Every other table row is.
6. `p71-pr-notes.md`: the existing decision 34 bullet was replaced (it ended "fill in the commit"),
   not a second one added. It still says to fill in the commit.
7. `lodd2` (`1e400`) from the adversarial probe is not in the pytest data, see the next section.

## Findings that are not this change

- **Redis 7.0.15 and KeyDB 6.3.4 keep `errno` between commands.** `sortCommand` tests
  `errno == ERANGE` after `strtod` without clearing it, so after a numeric SORT of a value that
  overflows (`1e400`), every later numeric SORT of non-integer-encoded elements fails with "One or
  more scores can't be converted into double" until some syscall changes `errno` (checked: both
  servers, `SORT sn` fails before and after nothing else). It makes a master's replies depend on its
  history; since a failing SORT is not replicated, a replica never sees it. It also made the
  adversarial probe's `lsp`/`lhex`/`words` errors vacuous (the `1e400` list ran just before them).
  That a failing SORT is not replicated shows in the same probe: five master-side errors
  (wrong-type, non-numeric) and `classic_apply_errors` stayed 0 on every replica.
- `ALPHA BY` ties are not reproducible even between two KeyDB processes (`alpha_by_tie_set`: KeyDB
  and Redis, and KeyDB twice, gave different orders), which is what makes the residual a limitation
  and not a defect.

## Open risks

- **Mixed-version replication.** A same-shard `SORT .. STORE` is journaled as the command, and a
  DFLY replica or peer of an older build re-runs it with the old order: a tied `BY`, `nosort` set
  or missing-weight form then leaves `dst` in a different order on it than on a new master (D-13's
  same-shard recipe, not new, but this change is what makes the two builds disagree). Cross-shard
  `STORE` journals the computed `RESTORE` and is safe. `kDrakeydbReplVersion` is not bumped.
- The numeric-parse and `GET` differences above stay: the negative-`LIMIT` and `GET` ones, and the
  NUL one, can leave a drakeydb replica of a KeyDB master with a different or stale `dst`.
- D-35 is open until the owner decides.
- Not run: a release (`build-opt`) build, ASAN/UBSAN, the full pytest suite (only the new,
  SORT-related and `fakeredis` subsets above), or the other `multi_master`/`replication` pytests.
- `ALPHA` sort entries are 8 bytes larger each.
- The tests need a `keydb-server` for the two pytests; without one they skip, or fail with
  `KEYDB_REQUIRED=1`, as the other `keydb` tests do.
