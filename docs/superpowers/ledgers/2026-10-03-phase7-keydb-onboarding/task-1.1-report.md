# Task 1.1 report: `ParseRreplayEnvelope`

Branch `feat/phase7-1-rreplay-unwrap` (stacked on P7-0, HEAD `dbc7e6c`). Not committed (the
orchestrator commits). Implementer: Sonnet. Plan: Task 1.1; spec D-3 (envelope shape and
validation), D-1.4.

## What changed

| File | Change |
|---|---|
| `src/server/classic_replay.h:46-68` | `struct RreplayEnvelope` (owned normalized `uuid`, `inner` view, `std::optional<DbIndex> db`, `mvcc`), `enum class RreplayParse`, `ParseRreplayEnvelope(const facade::RespVec&, RreplayEnvelope*)`, with the contract written out. |
| `src/server/classic_replay.cc:52-120` | `ParseRreplayEnvelope` and the file-local `ParseUnsignedDecimal`. |
| `src/server/classic_replay_test.cc:189-397` | Ten `ClassicReplayTest.ParseRreplayEnvelope*` cases plus a `Wire` helper that owns the bytes and runs a server-mode `RedisParser` over them, and the embedded golden vectors. |

Validation order and rules (mirrors `replication.cpp:5389-5433`): `args.size() >= 3` (the name counts,
as KeyDB's `argc`); `args[1]` is a string and `IsValidNodeUuid` (case-insensitive) else `kBadUuid`;
`args[2]` is a string; the optional `args[3]` is a decimal with `0 <= db < FLAGS_dbnum` else
`kBadDb`; the optional `args[4]` is a decimal that fits `uint64_t` else `kBadMvcc`; arguments past
the fifth are ignored (KeyDB ignores them). On `kOk` all four fields are reset, so a reused
`RreplayEnvelope` keeps nothing from the previous parse (`ThreeArguments...` pins it).

## Tests (`ninja -C /home/user/drakeydb/build-dbg -j4 classic_replay_test && ./build-dbg/classic_replay_test`)

Vectors: the gtest does not read `tests/`, so it embeds eight real KeyDB v6.3.4 segments as literals
(solo stream segments 1, 3, 4, 6, 7, 9, 13 and nested segment 1; the README lists them by offset and
length). They are parsed by `RedisParser` exactly as the stream loop does. Segment 13 is the
lower-case cron `ping`; segment 3 carries db 3; segments 4 and 7 are the `MULTI` and `EXEC`; the
nested segment is parsed twice, the outer envelope's `inner` being the inner envelope.

| Test | Pins |
|---|---|
| `ParseRreplayEnvelopeGoldenSet` | real `SET k v`: uuid, inner bytes, db 0, mvcc `1878060925646274561` (above 2^60, so a 53-bit or signed parse would fail) |
| `ParseRreplayEnvelopeGoldenDbAndControlCommands` | db 3, `MULTI`, `EXEC`, `INCRBY`, lower-case `ping`, `PEXPIREMEMBERAT` |
| `ParseRreplayEnvelopeGoldenNestedDepthTwo` | depth 2: the outer's `inner` is itself an envelope that parses `kOk` and holds the `SET` |
| `ParseRreplayEnvelopeThreeArgumentsHasNoDbAndNoMvcc` | 3-arg form: db nullopt, mvcc 0, and a reused struct is reset; 4-arg form: db and no mvcc |
| `ParseRreplayEnvelopeIgnoresArgumentsPastTheFifth` | KeyDB reads `argv[1..4]` only |
| `ParseRreplayEnvelopeBadArity` | 2 args, 1 arg, 0 args, a non-string inner |
| `ParseRreplayEnvelopeUuid` | 35 and 37 chars, dash at 8 and at 23, non-hex, trailing space, empty, non-string; upper and mixed case accepted and returned lowercase |
| `ParseRreplayEnvelopeDb` | `0`, `15` ok; `16` (dbnum 16), `-1`, `abc`, empty, `1x`, ` 1`, `+1`, 20 digits, 2^64 bad; the limit follows `--dbnum` (set to 4: `3` ok, `4` bad) |
| `ParseRreplayEnvelopeMvcc` | `0`, 2^63, 2^64-1 ok; `-5`, `abc`, empty, `12 `, `1.5`, 2^64, 23 digits bad |
| `ParseRreplayEnvelopeChecksInKeyDbOrder` | bad uuid beats bad db; bad db beats bad mvcc |

`RreplayParse` has a `PrintTo`, so a failure names `kBadDb`, not `4-byte object <03-00 00-00>`.

### Step 2: observed failure (before the implementation)

The header declared the function, so the test compiled; it failed at link:

```
$ ninja -C /home/user/drakeydb/build-dbg -j4 classic_replay_test
/usr/bin/ld: src/server/CMakeFiles/classic_replay_test.dir/classic_replay_test.cc.o: in function `dfly::ClassicReplayTest_ParseRreplayEnvelopeGoldenSet_Test::TestBody()':
classic_replay_test.cc:(.text+0x2a3a): undefined reference to `dfly::ParseRreplayEnvelope(std::vector<facade::RespExpr, std::allocator<facade::RespExpr> > const&, dfly::RreplayEnvelope*)'
... (the same for each of the ten tests)
collect2: error: ld returned 1 exit status
ninja: build stopped: subcommand failed.
```

### Passing

```
$ ./build-dbg/classic_replay_test --gtest_filter='ClassicReplayTest.*'      # at the end of Task 1.1: 12 tests
[==========] 12 tests from 1 test suite ran. (0 ms total)
[  PASSED  ] 12 tests.
```
(`ParseCapaReply*` included. The suite is 13 tests now: Task 1.2 added `IsRreplayIsCaseInsensitive...`.)

## Falsification

Each was a one-line edit to `classic_replay.cc`, a rebuild with the command above and a run with
`./build-dbg/classic_replay_test --gtest_filter='ClassicReplayTest.ParseRreplayEnvelope*'`, then the
file restored. Line numbers are those of the test file at the time of the run.

**(a) `db < dbnum` changed to `db <= dbnum`** (`value > absl::GetFlag(FLAGS_dbnum)`), the plan's first:

```
/home/user/drakeydb/src/server/classic_replay_test.cc:338: Failure
Expected equality of these values:
  parse("16")
    Which is: kOk
  RreplayParse::kBadDb
    Which is: kBadDb

/home/user/drakeydb/src/server/classic_replay_test.cc:351: Failure
Expected equality of these values:
  parse("4")
    Which is: kOk
  RreplayParse::kBadDb
    Which is: kBadDb

[  FAILED  ] ClassicReplayTest.ParseRreplayEnvelopeDb (0 ms)
[  PASSED  ] 9 tests.
[  FAILED  ] 1 test, listed below:
[  FAILED  ] ClassicReplayTest.ParseRreplayEnvelopeDb
```

**(b) uuid accepted only in lowercase** (`|| args[1].GetView() != absl::AsciiStrToLower(args[1].GetView())`
added to the uuid check), the plan's second:

```
/home/user/drakeydb/src/server/classic_replay_test.cc:307: Failure
Expected equality of these values:
  parse("B1198D29-CB88-4110-922A-A6C99BD08471")
    Which is: kBadUuid
  RreplayParse::kOk
    Which is: kOk

/home/user/drakeydb/src/server/classic_replay_test.cc:309: Failure
Expected equality of these values:
  parse("B1198d29-cb88-4110-922a-A6c99bd08471")
    Which is: kBadUuid
  RreplayParse::kOk
    Which is: kOk

[  FAILED  ] ClassicReplayTest.ParseRreplayEnvelopeUuid (0 ms)
[  PASSED  ] 9 tests.
```

Two more, mine (run before `PrintTo` existed, so the enum prints as bytes in the first):

**(c) uuid not normalized** (`out->uuid = string(args[1].GetView())`):

```
classic_replay_test.cc:291: Failure
  env.uuid
    Which is: "B1198D29-CB88-4110-922A-A6C99BD08471"
  kUuid
    Which is: "b1198d29-cb88-4110-922a-a6c99bd08471"
classic_replay_test.cc:293: Failure
  env.uuid
    Which is: "B1198d29-cb88-4110-922a-A6c99bd08471"
[  FAILED  ] ClassicReplayTest.ParseRreplayEnvelopeUuid
```

**(d) sloppy decimals** (the digits-only check in `ParseUnsignedDecimal` removed, so `absl::SimpleAtoi`
decides):

```
classic_replay_test.cc:326: Failure
  parse(" 1")
classic_replay_test.cc:327: Failure
  parse("+1")
[  FAILED  ] ClassicReplayTest.ParseRreplayEnvelopeDb
classic_replay_test.cc:357: Failure
  parse("12 ")
[  FAILED  ] ClassicReplayTest.ParseRreplayEnvelopeMvcc
 2 FAILED TESTS
```

The source was restored after each run (`diff` against the saved copy was empty) and the suite
rebuilt and rerun: 12 of 12 passed.

### What each test would still pass under with the feature removed

- A parser that returns `kOk` for any vector of three or more strings fails every `Bad*` test and
  `Uuid`; the golden tests alone would still pass it. That is why the hand-built negatives are there.
- A parser that checks only the shape (arity, uuid, db, mvcc) but takes `inner` from the wrong
  argument fails the golden tests (the inner bytes are compared exactly).
- The nested test says nothing about unwrapping beyond one level: it proves only that the inner
  envelope is a valid envelope. Depth is Task 1.2's `NestedUnwrapAllowedTo64AndRefuses65th`.

## Deviations from the plan and spec

1. **A non-string inner command is `kBadArity`.** The enum has no value for KeyDB's "Expected command
   buffer arg2", and the spec fixes the enum. A server-mode parser yields only strings, so only a
   hand-built vector reaches it (one test). Documented in the header and in the source.
2. **Numbers are strict decimals** (ASCII digits only), not KeyDB's `string2ll` / `strtoull`:
   `+1`, ` 1` and the like are `kBadDb` / `kBadMvcc`. KeyDB never emits them (`writeProtoNum`); the
   plan's own negatives (`-5`, `abc`, overflow) hold either way. Leading zeros are accepted.
3. The plan's "its README prints segment 1 and nested segment 1 as literals": I embedded eight
   segments, transcribed from a script's dump of the `.bin` files, and then checked all eight byte for
   byte against the `.bin` slices at the README's offsets and lengths with a second script (127, 129,
   115, 114, 130, 114, 168 and 228 bytes, all equal).

## Not done / risks

- The cap on `db` is `--dbnum` as read at the call; the flag is runtime-immutable in production.
  `DbIndex` is 16 bits; `--dbnum` is bounded by `kMaxDbId` (1024) at startup, so the cast is safe.
- Clang was not part of the local build (gcc 13.3). A `clang++ -fsyntax-only` of the touched
  translation units reports nothing located in them (its only complaint is the upstream
  `compact_object.h` unused private field `reserved_`, which this tree already has).

## Checks

`pre-commit run --files src/server/classic_replay.h src/server/classic_replay.cc
src/server/classic_replay_test.cc` at the end of this task:

```
trim trailing whitespace.................................................Passed
fix end of files.........................................................Passed
Clang formatting.........................................................Passed
(pyflakes, check python ast, black: no files to check)
```
