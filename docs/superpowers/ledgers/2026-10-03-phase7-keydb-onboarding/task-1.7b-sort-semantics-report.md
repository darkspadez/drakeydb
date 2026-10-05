# Task 1.7b report: `SORT` follows Redis and KeyDB for `LIMIT`, `GET`, numbers, `ALPHA BY` ties and RESP3 (decisions 36, 37, 38; D-34)

Branch `feat/phase7-1-rreplay-unwrap`, started at `a74fdb9`. Not committed by the implementer. Brief:
`brief-sort2a.md`; review: `review-sort34.md` (I1, M1-M5, N1-N3). Scratch material (the probes, the
generators of the expected values, every build, test and falsification log, the before-binary) is
in the orchestrator's scratchpad under `sort2a/`; file names below are relative to it. Hash-field
patterns (`->`, decision 35, D-35) are round 2b and untouched; the pattern code has one helper where
a field would go.

## What changed

Files: `src/server/generic_family.cc` (all hunks `// drakeydb: P7-1 (decision N) --`),
`src/server/generic_family_test.cc`, `tests/dragonfly/keydb_onboarding_test.py`,
`tests/dragonfly/instance.py` (`LC_ALL=C` for the KeyDB fixture), and the docs
`docs/ISSUE-REGISTER.md` (D-34, D-13, U-20, D-35 pointer), `docs/differences.md`,
`docs/multi-master.md` ("Upgrade a mesh in lockstep"), `docs/UPSTREAM-SYNC.md` (the
`generic_family.cc` row and the `instance.py` row) and the spec's byte-identity SORT bullet;
`p71-pr-notes.md` (scratchpad). `decisions.md` untouched. `classic_replay_test.cc` untouched.

### The rules, in `generic_family.cc`

| # | Rule (KeyDB `sort.cpp`) | Before | Now |
|---|---|---|---|
| 1 | `LIMIT` offset and count parse with `string2ll` and clamp (`:220-229`, `:324-333`) | `SimpleAtoi` into `uint32` (negative and > 32 bit refused, `+1` and `01` accepted) | `ParseSortLimit` (`string2ll`, `CmdArgParser::Report(INVALID_INT)` for the Redis error text); `GetSortRange` clamps: negative offset 0, negative count the rest, past the end cut; `SortBounds` is `int64_t`; the visitor's partial sort ends at the same range |
| 2 | `GET` of a missing or non-string key, or of a pattern without `*`, is NULL: nil in a reply, `""` stored (`:82-87`, `:530-536`, `:561`) | `""` in the reply; a `*`-less pattern read the literal key | `SortEntryBase::get_values` is `vector<optional<string>>`; `FetchGetPatternValues` sets nullopt for a missing/non-string key (the `found` out-parameter) and for a `*`-less pattern (no fetch); repliers `SendNull()`, `OpStore` stores `""` |
| 3 | only the first `*` of a `BY`/`GET` pattern is replaced (`:233-239`, `:84-102`) | `syntax error` for more than one | `SortPattern`/`ParseSortPattern` (first `*`, prefix and suffix), used by `FetchGetPatternValues` and `PopulateSortEntriesFromByPattern`; the `star_count` checks in `SortGeneric` are gone; `BY` without `*` is still "nosort" |
| 4 | numeric weights and elements load with `strtod` over the bytes up to the first NUL, refused on a remainder, `ERANGE`, NaN (`:475-483`) | `absl::SimpleAtod` | `ParseSortScore`: `errno = 0`, `strtod(c_str())`, `*end == '\0' && errno != ERANGE && !isnan`; `c_str()` already ends at a NUL for `strtod`, so `"5\0x"` is 5 and `"\0"` is 0 with no copy; KeyDB's stale-`errno` quirk is not reproduced |
| 5 | `ALPHA BY` ties keep the fetch order for a list and an intset (stable libc `qsort`; `:505-508`) | broke on the element | `SortEntry::Precedes`/`CompareWeights` replace `less`/`greater`; `SortEntryAlpha::seq` (fetch position, shares the bool's padding) and `SortParams::ties_in_fetch_order` make the order total, DESC included; `SortTiesInFetchOrder` decides the source: a list, or a set whose members are all strict `int64` and at most 512 (then the elements are put in ascending numeric order first); any other set and a zset break on the element as before |
| 6 | `SORT` of a set or zset replies an array in RESP3 | `StartCollection(SET)` (`~`) | `rb->StartArray()` in both repliers; `SortVisitor::result_type` is gone |

Nits: N1 marker on `SetWeightMissing` (and `SetSeq`); N2 `sort.cpp` ranges in the comments are
`:296-310`, `:356-380`, `:401-439` (checked against the file); N3 the `std::min<uint32_t>` narrowing
went with the rewritten `GetSortRange`.

### Design notes

- **The intset predicate is decided on the members.** KeyDB holds a set as an intset when every
  member passes `string2ll` and there are at most `set-max-intset-entries` (512 by default; on both
  live servers `OBJECT ENCODING` is `intset` at 512 members and `hashtable` at 513) of them, and an intset iterates in ascending numeric order. drakeydb's own
  intset ends at **256** (`kMaxIntSetEntries`, `set_family.cc:64`), above which a set is a dense set
  here with an arbitrary iteration order, and a set with member TTLs is a dense set at any size. Going
  by the encoding would therefore break the ties of a 257 to 512 member integer set on the element
  where KeyDB keeps ascending order; going by content (and sorting the fetched elements numerically
  first, at most 512 of them, so the cost is bounded) gives KeyDB's order on every build. For a
  **600-member** integer set the two choices agree: by encoding it is a dense set, and by content it is
  over 512, so either way its ties break on the element, which is also all a KeyDB hash set deserves
  (its order differs between a KeyDB and a Redis process, measured). They differ for 257 to 512
  members, where only the content rule matches KeyDB; the tests cover 100, 256, 257, 300, 512, 513
  and 600, and falsify the encoding rule (m7b below).
- **Total order, not `stable_sort`.** `partial_sort` under `LIMIT` is not stable, so `ties_in_fetch_order`
  compares the fetch position on a tie. That keeps the O(n log k) selection, agrees with a full
  stable sort, and keeps `DESC` from reversing ties (a `DESC` implemented as `less(r, l)` with an index
  tie-break reverses them; falsified below). The numeric `BY` keeps its element tie-break, KeyDB's own
  rule, which is already total.
- **`pqsort` is not ported.** KeyDB uses it only for a `BY` sort whose `LIMIT` cuts the result
  (`:505-506`); it insertion-sorts fewer than 7 elements (`pqsort.c:108`), which is why small results
  agree, and is deterministic (KeyDB and Redis agree with each other on the 2000-element list under
  `LIMIT 0 50`; drakeydb does not). A `LIMIT` that does not cut (`LIMIT 0 -1`) is `qsort` and agrees.
- **`errno` and fibers.** `ParseSortScore` sets and reads `errno` with no yield in between, so the
  thread-local value is not shared across a fiber switch.
- **No journal change.** `SortStoreNothing`, `OpStore`'s hand-journal and the single-shard revive are
  as they were; only what is replied or stored changes.

## The live-verified edge tables

Every table was produced by running the same data and commands on a standalone KeyDB 6.3.4, Redis
7.0.15 (both started with `LC_ALL=C`), the build before this change (`drakeydb-before`, the `a74fdb9`
build) and this change, on 1 and 2 shards; `probes/*.py`, raw output in `logs/probe-*.txt`. KeyDB and
Redis agreed in every row below unless a row says otherwise (KeyDB 6.3.4 has no `SORT_RO`; there
drakeydb is compared with Redis). "equal" is: KeyDB, Redis, and drakeydb on 1 and 2 shards.

### `LIMIT` (`probe-limit-{before,after}.txt`; `SORT l LIMIT o c` with `l = 3 1 2 5 4`, and the same with `STORE`)

| `LIMIT` | KeyDB and Redis | before | after |
|---|---|---|---|
| `0 -1`, `-1 -1`, `-3 -7`, `-9223372036854775808 -9223372036854775808` | 1 2 3 4 5 | error | equal |
| `-5 3`, `-1 2` | 1 2 3 / 1 2 | error | equal |
| `2 -1`, `2 -5`, `3 9223372036854775807` | 3 4 5 / 3 4 5 / 4 5 | error | equal |
| `-9223372036854775808 1` | 1 | error | equal |
| `1 4294967296` | 2 3 4 5 | error | equal |
| `4294967296 1`, `9223372036854775807 1`, `9223372036854775807 9223372036854775807` | empty | error | equal |
| `99 1`, `5 1`, `0 0`, `2 0` | empty | empty | equal |
| `4 1`, `0 2`, `1 3`, `1 100` | 5 / 1 2 / 2 3 4 / 2 3 4 5 | same | equal |
| `+1 2`, `01 2`, `1 +2`, `1 02`, `" 1" 2`, `"1 " 2` | **error** | accepted (2 3) | equal |
| `-0 2`, `0 -0`, `1.5 2`, `a 1`, `"" 1`, `1 ""`, `0x10 1`, `1e2 1`, `0 99999999999999999999`, `9223372036854775808 1`, `-9223372036854775809 1` | error | error | equal |
| `LIMIT 1`, `LIMIT` | `syntax error` | same | equal |

The error is `ERR value is not an integer or out of range` everywhere. A stored sort over an error
leaves `dst` as it was; one over a clamped range leaves the clamped list. Also clamped, as Redis:
`BY nosort DESC` on a list and a zset (`LIMIT -2 3`, `1 -1`, `2 -1`), `ALPHA`, `DESC`, `BY w_*`.
The one non-equal form is `SORT <set> BY nosort DESC LIMIT 1 -1`, a plain set under `nosort`, which
has no order in Redis and KeyDB either (they differ from each other).

### `GET` and patterns (`probe-get-{before,after}.txt`; `l = 1 2 3`, `o_1 = first`, `o_2` a hash, `o_3 = third`, `fixed = FIXED`)

| Form | KeyDB and Redis, reply | `STORE` list | before | after |
|---|---|---|---|---|
| `GET o_*` | first nil third | first "" third | first "" third | equal |
| `ALPHA GET o_*` on `a b c` (`o_a = A`, `o_b = ""`, `o_c` missing) | A "" nil | A "" "" | A "" "" | equal |
| `GET # GET o_*` | 1 first 2 nil 3 third | 1 first 2 "" 3 third | `""` for the nil | equal |
| `GET fixed` | nil nil nil | "" "" "" | FIXED x3 | equal |
| `GET fixed GET #`, `BY nosort GET fixed`, `BY w_* GET fixed` | nil (and `#` values) | "" | FIXED | equal |
| `BY nostar` (no `*`), `BY nostar DESC` | 1 2 3 / 3 2 1 (nosort) | same | same | equal |
| `BY nostar GET o_*` | first nil third | first "" third | `""` | equal |
| `BY w_*_*`, with keys `w_<e>_*` | ordered by them | same | `syntax error` | equal |
| `BY w_**`, `BY *_*` | first `*` only | same | `syntax error` | equal |
| `BY **` | `One or more scores can't be converted` | untouched | `syntax error` | equal |
| `GET h_**` (keys `h_<e>*`) | S1 S2 S3 | S1 S2 S3 | `syntax error` | equal |
| `GET h_* GET h_**` | nil S1 nil S2 nil S3 | "" S1 ... | `syntax error` | equal |
| `GET **`, `GET *x*`, `GET pre_*_*`, `GET o_*_*` | per the first `*` | same | `syntax error` | equal |
| `GET x*x` (key `x1x`) | xx nil nil | xx "" "" | xx "" "" | equal |
| EVAL of `SORT l GET o_*` and `GET fixed` | nil as `false` in Lua | | `""` / FIXED | equal |
| `SORT_RO` of the above | Redis only (KeyDB: unknown command) | | | equal to Redis |

### Numbers (`probe-num-{before,after}.txt`, `num_table.py`; 77 spellings x 6 forms)

Forms per spelling `V`: `SORT [V, "3"]` and `SORT [V, "3"] STORE`, the same on a set `{V, "3"}`,
`SORT [a, b] BY w_*` with `w_a = V`, `w_b = 3`, with `STORE`, and with `DESC` (462 rows). KeyDB and Redis
agree on every row. KeyDB and Redis keep `errno` between commands, so after one `ERANGE` every later
non-integer numeric SORT fails; the probe clears it between cases (KeyDB: a failing `CONFIG SET dir`;
Redis: `REPLICAOF 127.0.0.1 1` then `NO ONE`, which does a failing `connect`) and the table does not
depend on it. Before this change 19 spellings (114 rows) differed; after it none.

| Spellings | KeyDB and Redis | before | after |
|---|---|---|---|
| `5 `, `5\t`, `5\n` (trailing whitespace) | refused | accepted | equal |
| `1e400`, `-1e400`, `1e-400`, `1e-310`, `4.9e-324`, `1.7976931348623159e308`, a 400-digit number, `-` and 400 digits, `0.` and 400 zeros and `1` (overflow, underflow, denormals) | refused | accepted | equal |
| `5\0`, `5\0x`, `\05`, `\0`, `\0abc`, `5\01e400`, `5\0\0` (a NUL ends the number; `\0` alone and `\0abc` are 0) | accepted | refused | equal |
| `0x10`, `0x1A`, `0X1a`, `0x1p3`, `0x1.8p1`, `1e5`, `1E5`, `1e+5`, `.5`, `5.`, `-.5e-2`, `+5`, `-0`, `0e0`, `00012`, `1.5e-3`, ` 5`, `  5`, `\t5`, `\n5`, `inf`, `+inf`, `-inf`, `INF`, `iNf`, `infinity`, `-Infinity`, `2.2250738585072014e-308`, `1.7976931348623157e308`, `9223372036854775807`, `9223372036854775808`, `-9223372036854775808`, `-9223372036854775809`, `9007199254740993`, `""`, `0` | accepted | accepted | equal |
| `0x`, `1e`, `e5`, `--5`, `5e+`, `1,5`, ` ` (space), `+ 5`, `5 5`, `0b1`, `1d5`, `1f`, `1_0`, `infx`, `in`, `nan`, `NaN`, `-nan`, `nan(1)`, `abc\05`, `1e400\0`, a non-ASCII digit | refused | refused | equal |

Refused is `ERR One or more scores can't be converted into double`; a refused `STORE` leaves `dst`
as it was. A missing weight is 0, and a weight key of another type is missing (rows in the
`GenericSortOrderTest.TiedByWeights...` table, round 1).

### `ALPHA BY` ties (`probe-tie-both.txt`, `probe-intsets.txt`, `tieprobe-after.txt`)

| Source and form | KeyDB vs Redis | before | after |
|---|---|---|---|
| list `b a b a`, `BY nokey_* ALPHA` (ASC, DESC), STORE | equal: `b a b a` | `a a b b` | equal |
| list `q p r s t`, weights `m m (missing) "" m`, `ASC` / `DESC` / `LIMIT 1 3` / `DESC LIMIT 1 3` / `GET #` | `r s q p t` / `q p t s r` / `s q p` / `p t s` / `r s q p t` | `r s p q t` / `t q p s r` / `s p q` (element tie-break) | equal |
| the review's 2000-element list (weights `DE FR US ""`, 100 missing), `ASC`, `DESC`, `LIMIT 0 -1`, `DESC LIMIT -3 -1`, STORE | equal, stable list order | element order | equal |
| the same list, `LIMIT 0 50` (cuts: `pqsort`) | equal to each other, not stable | element order | **differs** (residual 1) |
| intset `30 2 10 1 200 9`, `name_9 = aaa`, the rest `same`: `BY name_* ALPHA` / `DESC` | `9 1 2 10 30 200` / `1 2 10 30 200 9` | `9 1 10 2 200 30` / `30 200 2 10 1 9` | equal |
| the same, `BY nokey_* ALPHA` / `DESC` | `1 2 9 10 30 200` (both) | `1 10 2 200 30 9` / `9 30 200 2 10 1` | equal |
| integer sets of 100, 256 (an intset in drakeydb too), 257, 300, 512 (a dense set in drakeydb), `BY nokey_* ALPHA` ASC/DESC/STORE | equal, ascending numeric (both directions) | bytewise | equal |
| integer sets of 513 and 600 | **KeyDB and Redis differ from each other** (hash order) | bytewise / reversed | the element rule (bytewise; reversed for DESC); residual 2 |
| string sets, mixed int/string sets, a zset with equal weights | **differ from each other** | element | the element rule |
| `SADD hist 30 2 10 1 200 9`, `SADD hist x`, `SREM hist x` (a KeyDB hash set by history) | differ from each other | element | ascending: residual 3 |
| numeric `BY` ties over a zset and an intset (`BY zqw_*`, `BY inw_*`), `BY nosort` of a zset with equal scores (ASC, DESC, LIMIT) | equal, element order | equal (round 1) | equal |

### RESP3 (`probe-resp3-raw.txt`; the first bytes of the reply on a connection that sent `HELLO 3`)

| Command | KeyDB / Redis | before | after |
|---|---|---|---|
| `SORT s ALPHA`, `s BY nosort LIMIT 0 2`, `s BY w_*`, `z ALPHA`, `z BY nosort` (set and zset sources), `SORT_RO` of them | `*` | `~` | `*` |
| `SORT l` (a list) | `*` | `*` | `*` |
| `SORT s ALPHA GET # GET nokey_*` | `*6` with `_` (RESP3 null) for each nil | `*`/`~` with `$0` | `*6` with `_` |

redis-py hands a RESP3 set back as a list and the gtest parser reads `~` as an array, so neither can
tell the two apart: `test_sort_replies_an_array_in_resp3` reads the bytes off a raw socket.

## Tests

Expected values are taken from the servers, not derived by hand: `gen/gen_expected2.py` (the tables,
and the C++ for the data they need, from one fixture) and `gen/gen_numbers.py` (the number table)
ran every row on KeyDB and Redis and dropped any row where the reply, the STORE count, the list
STORE leaves, or `SORT_RO` on Redis differed (none was dropped). The two exceptions are said so in the
test: the ALPHA BY ties of a hash set, a zset and a mixed set (`kAlphaByElementTieCases`, drakeydb's
own rule, KeyDB's answer is per process) and the 513 and 600 member integer sets.

**gtest, `generic_family_test.cc`** (`GenericSortOrderTest`, 2 shards, every STORE on and off the
source's shard):

| Test | Rows | What it pins |
|---|---|---|
| `LimitIsClampedLikeRedis` | 27 | `LIMIT 0 -1`, `-5 3`, `2 -1`, `99 1`, offset == size, `1 0`, `-1 -1`, `-3 -7`, `2 99999999999`, the `int64` extremes, `DESC`, `ALPHA`, `BY nokey_*`, `BY nosort` on a list, a zset and a list under `DESC`, `BY w_*`; reply and STORE |
| `LimitRejectsWhatStringToLongLongRejects` | 17 + 3 | `+1 2`, `01 2`, `1 +2`, `1 02`, `" 1" 2`, `"1 " 2`, `-0 2`, `0 -0`, `0 99999999999999999999`, `9223372036854775808 1`, `1.5 2`, `0x10 1`, `1e2 1`, `a 1`, `"" 1`, `1 ""`, ...: the Redis error from `SORT`, `SORT_RO` and `STORE` (dst untouched, on and off the shard), also for a missing source; a later refused `LIMIT` after a valid one; `LIMIT 1` and `LIMIT` are `syntax error` |
| `OnlyTheFirstAsteriskOfAPatternIsSubstituted` | 9 | `BY ws_*_*`, `BY wd_**` (ASC, DESC, `GET #`), `GET gh_**`, `BY nostar` (nosort, ASC and DESC) |
| `GetOfNothingIsNilInTheReplyAndEmptyInStore` | 18 | a missing key, the empty string (not nil), a hash (nil), `GET fixed`, `GET # GET fixed`, `BY nosort`/`BY gw_*`/`DESC`/`ALPHA`/`LIMIT` with it, a zset, several `*`, a missing `ALPHA BY` weight with `GET`; reply (`SORT`, `SORT_RO`; nil is `RespExpr::NIL`) and STORE (`""`) |
| `NumbersLoadAsRedisLoadsScores` | 79 | each spelling as an element and as a weight: 46 accepted (orders from KeyDB), 33 refused (the Redis error from `SORT`, `SORT_RO`, `STORE`; dst untouched) |
| `AlphaByTiesKeepTheFetchOrderOfAListOrAnIntegerSet` | 15 | `b a b a`, a list with missing and empty weights (ASC, DESC, `LIMIT`, `GET #`), the review's intset (`name_9`: `9 1 2 10 30 200`, DESC `1 2 10 30 200 9`, `BY nokey_*`), `LIMIT 0 -1` |
| `AlphaByTiesOverIntegerSetsOfEverySize` | 7 sizes x 2 | 100, 256, 257, 300, 512 (ascending numeric, both directions; the 257+ ones are dense sets here, asserted through `DEBUG OBJECT`), 513 and 600 (the element rule) |
| `AlphaByTiesOfOtherSourcesBreakOnTheElement` | 8 | a string set, a mixed set, a zset: ASC and DESC (was `AlphaByTiesBreakOnTheElement`; the list row moved to the table above) |
| `ZsetAndIntsetTiesUnderByAndNosort` | 12 | review M4: equal zset scores under `BY nosort` (ASC, DESC, `LIMIT`, `GET #`), numeric `BY` ties over a zset and over an intset (`1 10 2 200 30 9`) |

Changed upstream tests (each hunk says what upstream asserted, `// drakeydb: P7-1 (decision 36)`):
`GenericFamilyTest.SortNegativeLimit` (clamped, not errors), `SortBy` (several `*`: first only),
`SortGet` tests 6, 12 and 14 (nil for a missing key, for several `*`, for a pattern without `*`).

**pytest, `keydb_onboarding_test.py`** (`keydb` mark, 1 and 2 shards; `SORT_ORDER_FORMS` grew from
50 forms (10 more stored-only) to 124 (7 stored-only)):

- `test_plain_replica_of_active_keydb_orders_sort_store_as_keydb_does[1, 2]` and
  `test_sort_replies_in_the_order_keydb_does[1, 2]`: the clamped and refused `LIMIT` forms, several
  `*`, `GET` of nothing (nil replies now compared, they were stored-only), `ALPHA BY` ties over a list,
  the intset, 300 and 512 member sets and the review's 2000-element list (`LIMIT 0 -1` and `DESC
  LIMIT -3 -1` do not cut; a cutting `LIMIT` on it is left out, residual 1), equal zset scores, a NUL
  in a number (`lnul`, `lw2`), a trailing space (`lsp`: both refuse), and last, `lerange` (`1e400`: both
  refuse; last because KeyDB then keeps `errno`). The replication test now also asserts the replica
  counted no `classic_apply_errors`, and (review M4), on 2 shards, that at least 5 stored sorts have
  their destination on their source's shard and at least 5 on another (65 and 71 of 136: `DEBUG
  OBJECT` names the shard), so the two ways of applying a SORT are both exercised.
- `test_sort_replies_an_array_in_resp3[1, 2]` (new, no KeyDB needed): `HELLO 3` on a raw socket;
  every set, zset and list form, `SORT` and `SORT_RO`, `GET`, `LIMIT`, `BY nosort`, and EVAL, starts
  with `*`, and a nil `GET` value is `_`.
- `instance.py`: `KeyDBServer.start` runs keydb-server with `LC_ALL=C` (review M5; `ALPHA` replies of
  KeyDB use `strcoll`, which follows the inherited locale). **`RedisServer` needs the same only if a
  test compares `ALPHA` replies against it, and none does** (`grep`: no `SORT` in a test that uses
  `RedisServer`); left alone, and the line is one to copy if one is written.

Results on the final build (`build-dbg`, `nice ninja -j3 dragonfly generic_family_test
multi_master_test classic_replay_test`, no warnings). After the last falsification the tree was restored
(`generic_family.cc` md5 `92c932d0...`, `cmp` identical to `fixed/`), rebuilt, and every line below was run
on it; then comment-only `// drakeydb:` markers were added to hunks that lacked one (md5 now
`dce3626f998e26b7a6e38ff01b7cf63a`, pre-commit clean), the four targets rebuilt, and the three gtest suites
(`logs/gtest-*-final2.txt`: 103, 224 + 1 skipped, 99 passed) and the sort pytests (36 passed x3,
`logs/pytest-sort-onboarding-x3-final2.txt`) run again. The full-file pytest run and the falsifications
were on the `92c932d0...` code, which the marker edit does not change:

| Run | Result |
|---|---|
| `generic_family_test` (all) | 103 passed (95 + 8 new `GenericSortOrderTest` tests; `AlphaByTiesBreakOnTheElement` renamed) -- `logs/gtest-generic-final.txt`; 4 of the 95 failed on this change before their rewrite: the 3 upstream tests above and the old tie test |
| `multi_master_test` (all) | 224 passed, 1 skipped (`NodeIdentityFile.UnwritableDirIsEphemeral`, runs as root; not this change); the SORT journal tests unchanged -- `logs/gtest-multimaster-final.txt` |
| `classic_replay_test` (all) | 99 passed -- `logs/gtest-classic-replay-final.txt` |
| the 6 new/extended pytests | 6 passed; x3 with the other SORT pytests (`-k sort --count=3`): 36 passed -- `logs/pytest-sort-onboarding-x3-final.txt` |
| `multimaster_test.py -k sort` (two plain `SORT .. STORE` pytests) | 2 passed -- `logs/pytest-multimaster-sort-final.txt` |
| `keydb_harness_test.py` (the `KeyDBServer` fixture, `LC_ALL=C` change) | 21 passed -- `logs/pytest-keydb-harness-final.txt` |
| the 6 new/extended pytests on the before-binary (`a74fdb9`) | 6 failed: `~` for a set, `ERR value is not an integer` for every clamped `LIMIT`, tie and intset order diffs -- `logs/pytest-new-on-before-binary.txt` |
| the whole of `keydb_onboarding_test.py` | 167 passed, 3 skipped (the three release-bar perf tests, which need `DRAKEYDB_PERF=1`), 347 s -- `logs/pytest-onboarding-full.txt` |
| `pre-commit run --files` on every changed file | clean (clang-format reflowed comments once; black clean) |

### Probes rerun (brief: "expect `BAD 0`")

- `adv-p71/sort_keydb.py` (active KeyDB master, plain drakeydb replicas of 1, 2, 4 shards, `LC_ALL=C`):
  **`BAD 3`, not `BAD 0`**, and only for `dst16`, `SORT s BY w_* ALPHA STORE`, once per replica
  (`logs/sort_keydb-after.log`): `s` is a set of ten **letters**, hash-encoded in KeyDB, and nine of them
  weigh `"1"`, so the tie is over a hash set, which is residual 2 (KeyDB `c e i j d f g h b a`, drakeydb
  `b c d e f g h i j a`; two KeyDB processes would not agree either, `probe-tie-both.txt`). Every other
  form converges and `classic_apply_errors` is 0. The same script without that one line
  (`sort_keydb_noties.py`) is **`BAD 0`**.
- `sort_peers.py`: `BAD 0`. `sort_dfly.py`: `"diffs": []` for 1/4, 4/1 and 3/2 shards.
- The review's `tieprobe.py` (my ports; `logs/tieprobe-after.txt`): ASC, DESC and STORE of the 2000-element
  list are the stable list order on KeyDB, Redis and drakeydb (1 and 2 shards); `LIMIT 0 50` is `neither` on
  KeyDB and Redis (pqsort) and the stable order on drakeydb (residual 1).
- Upstream SORT tests: `generic_family_test` (above); `tests/dragonfly/*.py` has no SORT assertion
  outside `multimaster_test.py` (two plain `SORT .. STORE`, passing) and `keydb_onboarding_test.py`; no
  upstream or fakeredis test pins RESP3 `~` (both parsers read it as an array).
- fakeredis (`tests/fakeredis/test/test_mixins/test_generic_commands.py`): still not runnable as it
  stands in the pinned venv (round 1); run from `sort2a/fakeredis-copy`, the round 1 patched copy with
  the three `unsupported_server_types("dragonfly")` marks on SORT tests removed, `-m real` against a
  drakeydb on 6380 (`logs/fakeredis-after.txt`): **13 passed, 1 failed**. The failure is
  `test_sort_with_hash` (`->`, D-35, round 2b). **Two tests whose mark says Dragonfly does not
  support them now pass: `test_sort_with_by_and_get_option`** (it asserts `sort(..., get="data_1") ==
  [None, None, None, None]`, the nil of a `*`-less `GET`; it failed in round 1) **and
  `test_sort_with_store_option`** (stale since round 1). I did not unmark them: the harness only ran
  from a patched copy.

## Falsification

Each mutation was applied by `fals.py` to a copy of the verified `generic_family.cc`
(`fixed/generic_family.cc`, md5 `92c932d0e792f51f75177e9ff596a9b2`), logged in `progress-sort2a.md`
with the restore command before it was built, then `ninja -j3 dragonfly generic_family_test`, the whole
`generic_family_test`, and the 6 new/extended pytests (`-k 'sort_replies_an_array_in_resp3 or
orders_sort_store_as_keydb_does or sort_replies_in_the_order_keydb_does'`), then restored (`cmp`
identical, logged). Raw output: `fals/<name>-{build,gtest,pytest}.txt`, `fals/summary*.json`,
`fals/driver*.log`. R is the replication pytest, Y the reply pytest, 3 the RESP3 pytest (x2 shards each).

| Mutation (exact) | gtests that fail | pytests that fail |
|---|---|---|
| m1a. `LIMIT` negative refused again: `if (parsed_offset < 0 \|\| parsed_count < 0) return parser->Report(CmdArgParser::INVALID_INT);` before `params->bounds = ...` | `GenericFamilyTest.SortNegativeLimit`, `LimitIsClampedLikeRedis`, `AlphaByTiesKeepTheFetchOrder...` (its `LIMIT 0 -1` row), `ZsetAndIntsetTies...` | R, Y (4 of 6) |
| m1b. negative count is empty: `bounds->count < 0 ? size :` -> `bounds->count < 0 ? 0 :` | the same four | R, Y |
| m1c. negative offset is empty: `start = bounds->offset < 0 ? size : std::clamp(...)` | `SortNegativeLimit`, `LimitIsClampedLikeRedis` | R, Y |
| m2. `string2ll` -> `absl::SimpleAtoi` for both arguments (`+1`, `01` accepted) | `LimitRejectsWhatStringToLongLongRejects` | Y (a refused SORT is not replicated, so R cannot see it) |
| m3. `GET` of a missing key back to `""`: `found ? optional<string>{value} : nullopt` -> `optional<string>{value}` | `SortGet`, `GetOfNothingIsNilInTheReplyAndEmptyInStore` | Y, 3 (STORE keeps `""` either way, so R passes) |
| m4. a `*`-less `GET` reads the literal key (`sort_pattern ? KeyFor(...) : string(pattern)`) | `SortGet`, `GetOfNothing...` | R, Y |
| m5. the old checks back: more than one `*` in `BY` or `GET` is `syntax error` | `SortBy`, `SortGet`, `GetOfNothing...`, `OnlyTheFirstAsterisk...` | R, Y |
| m6a. trailing space accepted: `*end == '\0' \|\| absl::ascii_isspace(*end)` | `NumbersLoadAsRedisLoadsScores` | Y (second pass, after the `lsp` form was added: the first pass passed all 6) |
| m6b. NUL rule off: `if (item.find('\0') != string::npos) return false;` first | `NumbersLoad...` | R, Y |
| m6c. `ERANGE` accepted: `errno != ERANGE` dropped | `NumbersLoad...` | Y (second pass, after the last form `lerange` was added: the first pass passed all 6) |
| m7a. `ALPHA BY` ties on the element again: `ties_in_fetch_order = false` | `AlphaByTiesKeepTheFetchOrder...`, `AlphaByTiesOverIntegerSetsOfEverySize`, `GetOfNothing...` (its `ml ... GET` row) | R, Y (list, intset, 300/512 and the 2000 elements) |
| m7b. the predicate by drakeydb's own intset threshold: `kKeydbMaxIntsetEntries = 256` | `AlphaByTiesOverIntegerSetsOfEverySize` (257, 300, 512) | R, Y (the 300 and 512 sets) |
| m7c. no ascending numeric order for the intset (the `std::sort` of the members removed) | `AlphaByTiesOverIntegerSetsOfEverySize` | R, Y (300, 512) |
| m8. `DESC` as `less(r, l)`: `if (reversed) return Precedes(r, l, false, ties_in_fetch_order);` first | `AlphaByTiesKeepTheFetchOrder...` (DESC rows), `AlphaByTiesOverIntegerSets...` | R, Y |
| m9. RESP3 back to the set type: `rb->StartCollection(n, CollectionType::SET)` in both repliers | none (the gtest parser reads `~` as an array) | 3 only |

m6a and m6c passed every pytest in the first pass, because a refused SORT is not replicated and the
data held no refused number the reply test sorted; the forms `lsp` and `lerange` were added to the reply
test and both mutations re-run (`fals/driver2.log`): they fail it now. After the last mutation the
good copy was restored (`cmp` identical to `fixed/generic_family.cc`, md5 above), the four targets
rebuilt (`logs/build-final-after-fals.txt`) and every run in the table above done on that build.

## Residuals (documented in D-34, `docs/differences.md`; none fixed)

1. **Ties under a `LIMIT` that cuts a `BY` sort** (KeyDB `pqsort`, `sort.cpp:505-506`): deterministic,
   so KeyDB and Redis agree with each other, not stable, so not with drakeydb on the 2000-element
   list under `LIMIT 0 50` (and equal below 7 elements, where `pqsort` insertion-sorts). Not ported.
2. **Hash-encoded sets and zsets**: per-process order in KeyDB; ties break on the element here.
3. **A KeyDB set that is a hash set for its history** (a non-integer member once, over 512 once, a
   different `set-max-intset-entries`) holding only integers: its dict order against our ascending order.
4. **Another libc**: this box has glibc 2.39 whose `qsort` is a stable mergesort (probe
   `probes/qsort_stable.c`: no unstable pair among 2 to 3,000,000 16-byte records); a KeyDB on musl or on
   another glibc is not checked.
5. **A Redis 7.2 or newer master** holds small string sets as listpacks (insertion order): not checked,
   no such binary here.
6. **`ALPHA` replies with a NUL byte** (`strcoll` stops at it in a reply; `STORE`, the replicated form, is
   bytewise in KeyDB as here): client-visible only, measured (`logs/probe-alpha-nul.txt`).
7. **Locale**: `ALPHA` replies of a KeyDB follow its locale; drakeydb is bytewise.

Mixed versions (D-13, `docs/multi-master.md`): a node of an older build re-running a same-shard
`SORT .. STORE` orders a tied, `nosort`-set, missing-weight or `ALPHA BY` tie form the old way, and
fails (dst stale) on a negative `LIMIT`, several `*`, or a number the new build accepts, while a
new-build master computed the new result; `kDrakeydbReplVersion` is not bumped (the gate refuses
admission on the active master's side only, so it would not cover an older node behind a newer
master, nor a non-active DFLY link, and would force a lockstep upgrade for an ordering nuance).

## Found, not fixed

- **`SADD` of more than 256 integers to a new key in one call replies a wrong count** (`SADD k <257
  distinct integers>` replies 0, 300 replies 43, 512 replies 255, 600 replies 343; `SCARD` is right).
  Cause in `OpAdd` (`set_family.cc`): after the intset outgrows `kMaxIntSetEntries` the code converts
  it and then assigns `res = StringSetWrapper{...}.Add(vals, ...)`, which counts only the members the
  dense set did not hold yet. Not SORT's; the line is in the fork's base commit (`05abfdd`), so it is
  upstream's. Found building the 257+ member test sets; the SORT tests do not check that reply.
  Noted at the end of D-34; it deserves its own U- entry if you want it tracked.

## Deviations from the brief

1. `sort_keydb.py` is `BAD 3`, not `BAD 0` (the hash-set tie, residual 2; the form without it is `BAD 0`).
2. The RESP3 type is tested by a pytest on a raw socket, not by a gtest: the gtest parser and redis-py
   both read `~` as an array. The brief listed it under the gtests.
3. The brief's `GET h_**` row is `GET gh_**` (a key family named `gh_`); `BY w_*_*` is `BY ws_*_*`.
4. The brief says the ties table row for a list "with missing weights" and "the 600 member set (whatever
   your predicate says, matching KeyDB)": no KeyDB answer exists for 513 and above (KeyDB and Redis
   disagree with each other), so those rows assert drakeydb's documented rule and say so; the sizes up
   to 512, which KeyDB does answer, are compared.
5. After the falsification run showed that m6a and m6c (trailing space, `ERANGE`) failed only the gtest,
   two forms were added to the reply pytest (`lsp`, and `lerange` last); both mutations then fail it.
6. `RedisServer` was not given `LC_ALL=C` (nothing compares an `ALPHA` reply against it); see above.

## Not run, not verified

- A release (`build-opt`) build, ASAN or UBSAN, the full pytest suite beyond `keydb_onboarding_test.py`,
  `multimaster_test.py -k sort` and `keydb_harness_test.py`, CI.
- KeyDB on another libc, Redis 7.2 or newer (residuals 4 and 5).
- The fakeredis suite in the repo as it stands (it is not runnable in the pinned venv); a patched copy only.

## Open risks

- **Mixed-version replication** (above).
- The intset predicate assumes KeyDB's default `set-max-intset-entries` (512); a master configured
  otherwise orders a set between the two limits as a hash set (residual 3). A flag would make it
  configurable; not added.
- `ALPHA` sort entries carry a `uint32` fetch position in the padding of the `bool` they already had: no
  size change over round 1.
- A numeric sort of 1e6 elements now calls `strtod` per element where it called `absl::SimpleAtod`;
  not measured.

## Open questions

1. Port `pqsort` (a deterministic quicksort, `pqsort.c`) so that the ties under a cutting `LIMIT` match
   too (residual 1)? Decision 37 says not ported; the probes show KeyDB and Redis agree with each other
   there, so it would be reproducible.
2. Unmark `test_sort_with_by_and_get_option` and `test_sort_with_store_option` in the fakeredis suite
   (`unsupported_server_types("dragonfly")`), now that both pass on the patched copy?
3. Give the `SADD` count bug its own U- entry (and fix it, a one-line change)?
4. Should the intset limit follow a flag, for a KeyDB master with another `set-max-intset-entries`?
5. D-35 (`->`) is round 2b; the pattern helper (`SortPattern`) is where its field goes.
