// Copyright 2026, drakeydb authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "server/classic_replay.h"

#include <absl/cleanup/cleanup.h>
#include <absl/flags/flag.h>
#include <absl/flags/reflection.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/substitute.h>

#include <initializer_list>
#include <limits>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "base/gtest.h"
#include "facade/facade_test.h"
#include "facade/redis_parser.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/generic_family.h"
#include "server/namespaces.h"
#include "server/test_utils.h"

namespace dfly {

using namespace std;
using facade::RedisParser;
using facade::RespExpr;
using facade::RespVec;

TEST(ClassicReplayTest, ParseCapaReplyAcceptsOkAndSuffixes) {
  CapaReply plain = ParseCapaReply("OK");
  EXPECT_TRUE(plain.ok);
  EXPECT_FALSE(plain.active_replica);
  EXPECT_FALSE(plain.keydb_fastsync_save);

  CapaReply active = ParseCapaReply("OK active-replica");
  EXPECT_TRUE(active.ok);
  EXPECT_TRUE(active.active_replica);
  EXPECT_FALSE(active.keydb_fastsync_save);

  CapaReply fastsync = ParseCapaReply("OK keydb-fastsync-save");
  EXPECT_TRUE(fastsync.ok);
  EXPECT_FALSE(fastsync.active_replica);
  EXPECT_TRUE(fastsync.keydb_fastsync_save);

  CapaReply both = ParseCapaReply("OK active-replica keydb-fastsync-save");
  EXPECT_TRUE(both.ok);
  EXPECT_TRUE(both.active_replica);
  EXPECT_TRUE(both.keydb_fastsync_save);

  CapaReply spaced = ParseCapaReply("OK   active-replica  keydb-fastsync-save ");
  EXPECT_TRUE(spaced.ok);
  EXPECT_TRUE(spaced.active_replica);
  EXPECT_TRUE(spaced.keydb_fastsync_save);

  // Words this build does not know are ignored, not refused.
  CapaReply unknown = ParseCapaReply("OK some-future-capa active-replica");
  EXPECT_TRUE(unknown.ok);
  EXPECT_TRUE(unknown.active_replica);
  EXPECT_FALSE(unknown.keydb_fastsync_save);
}

TEST(ClassicReplayTest, ParseCapaReplyRejectsNonOk) {
  for (const char* bad : {"OKAY", "ok", "Ok", "ERR", "", " ", " OK", "OKactive-replica",
                          "OK-active-replica", "active-replica OK", "ERR active-replica"}) {
    CapaReply reply = ParseCapaReply(bad);
    EXPECT_FALSE(reply.ok) << bad;
    EXPECT_FALSE(reply.active_replica) << bad;
    EXPECT_FALSE(reply.keydb_fastsync_save) << bad;
  }
}

// So that a failed comparison names the result instead of printing its bytes (found by ADL, hence
// not in the unnamed namespace).
static void PrintTo(RreplayParse parse, ostream* os) {
  static constexpr const char* kNames[] = {"kOk", "kBadArity", "kBadUuid", "kBadDb", "kBadMvcc"};
  *os << kNames[static_cast<size_t>(parse)];
}

static void PrintTo(EnvelopeResult result, ostream* os) {
  *os << (result == EnvelopeResult::kConsumed ? "kConsumed" : "kNotConsumed");
}

namespace {

// The RESP bytes of one command, as a master streams it.
string Resp(initializer_list<string_view> words) {
  string out = absl::StrCat("*", words.size(), "\r\n");
  for (string_view word : words)
    absl::StrAppend(&out, "$", word.size(), "\r\n", word, "\r\n");
  return out;
}

// Parses the one command in `bytes` the way the stream loop does; `args` view `bytes`.
void ParseResp(string_view bytes, RespVec* args) {
  RedisParser parser(RedisParser::Mode::SERVER);
  uint32_t consumed = 0;
  EXPECT_EQ(RedisParser::OK,
            parser.Parse(
                RedisParser::Buffer{reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size()},
                &consumed, args));
  EXPECT_EQ(bytes.size(), consumed);
}

// A parsed command that owns the bytes its arguments view, so it must not move.
struct Wire {
  // The command `words`.
  Wire(initializer_list<string_view> words) : Wire(Tag{}, Resp(words)) {
  }

  // The command in `wire_bytes`, which are streamed bytes as they are.
  static Wire FromBytes(string_view wire_bytes) {
    return Wire(Tag{}, string(wire_bytes));
  }

  Wire(Wire&&) = delete;

  string bytes;
  RespVec args;

 private:
  struct Tag {};

  Wire(Tag, string wire_bytes) : bytes(std::move(wire_bytes)) {
    ParseResp(bytes, &args);
  }
};

constexpr string_view kUuid = "b1198d29-cb88-4110-922a-a6c99bd08471";

// Segments of tests/dragonfly/data/keydb_v6.3.4_rreplay_stream.bin, captured from a real KeyDB
// v6.3.4 (the README there lists them by offset and length). Embedded because the test does not
// read tests/. Segment 1: `SET k v` in db 0.
constexpr string_view kGoldenSet =
    "*5\r\n$7\r\nRREPLAY\r\n$36\r\nb1198d29-cb88-4110-922a-a6c99bd08471\r\n$27\r\n*3\r\n$"
    "3\r\nSET\r\n"
    "$1\r\nk\r\n$1\r\nv\r\n\r\n$1\r\n0\r\n$19\r\n1878060925646274561\r\n";
// Segment 3: `SELECT 3`, `SET k3 v3` (db 3 travels in the envelope).
constexpr string_view kGoldenSetDb3 =
    "*5\r\n$7\r\nRREPLAY\r\n$36\r\nb1198d29-cb88-4110-922a-a6c99bd08471\r\n$29\r\n*3\r\n$"
    "3\r\nSET\r\n"
    "$2\r\nk3\r\n$2\r\nv3\r\n\r\n$1\r\n3\r\n$19\r\n1878060926489329669\r\n";
// Segments 4 and 7: the MULTI and the EXEC of a transaction, one envelope each.
constexpr string_view kGoldenMulti =
    "*5\r\n$7\r\nRREPLAY\r\n$36\r\nb1198d29-cb88-4110-922a-a6c99bd08471\r\n$15\r\n*1\r\n$"
    "5\r\nMULTI\r\n"
    "\r\n$1\r\n0\r\n$19\r\n1878060926910857219\r\n";
constexpr string_view kGoldenExec =
    "*5\r\n$7\r\nRREPLAY\r\n$36\r\nb1198d29-cb88-4110-922a-a6c99bd08471\r\n$14\r\n*1\r\n$"
    "4\r\nEXEC\r\n"
    "\r\n$1\r\n0\r\n$19\r\n1878060926910857223\r\n";
// Segment 6: `INCR c` goes out as `INCRBY c 1`.
constexpr string_view kGoldenIncrBy =
    "*5\r\n$7\r\nRREPLAY\r\n$36\r\nb1198d29-cb88-4110-922a-a6c99bd08471\r\n$30\r\n*3\r\n$"
    "6\r\nINCRBY\r\n"
    "$1\r\nc\r\n$1\r\n1\r\n\r\n$1\r\n0\r\n$19\r\n1878060926910857222\r\n";
// Segment 13: the cron PING, in lower case.
constexpr string_view kGoldenPing =
    "*5\r\n$7\r\nRREPLAY\r\n$36\r\nb1198d29-cb88-4110-922a-a6c99bd08471\r\n$14\r\n*1\r\n$"
    "4\r\nping\r\n"
    "\r\n$1\r\n0\r\n$19\r\n1878060934338969600\r\n";
// Segment 9: `EXPIREMEMBER seed:set m1 100` as KeyDB propagates it.
constexpr string_view kGoldenExpireMember =
    "*5\r\n$7\r\nRREPLAY\r\n$36\r\nb1198d29-cb88-4110-922a-a6c99bd08471\r\n$68\r\n*4\r\n"
    "$15\r\nPEXPIREMEMBERAT\r\n$8\r\nseed:set\r\n$2\r\nm1\r\n$13\r\n1791058571443\r\n\r\n$"
    "1\r\n0\r\n"
    "$19\r\n1878060927751815169\r\n";

// Segment 1 of keydb_v6.3.4_rreplay_nested_stream.bin: KeyDB A (3e4efc2d...) forwarded `SET k v`
// that KeyDB B (307ea48c...) wrote, so B's whole envelope is the inner command of A's.
constexpr string_view kGoldenNested =
    "*5\r\n$7\r\nRREPLAY\r\n$36\r\n3e4efc2d-a9f9-4a1e-90b7-ebd4171606de\r\n$127\r\n*5\r\n$"
    "7\r\nRREPLAY\r\n"
    "$36\r\n307ea48c-fc10-47a1-91bb-263276269153\r\n$27\r\n*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$"
    "1\r\nv\r\n"
    "\r\n$1\r\n0\r\n$19\r\n1878060940078874625\r\n\r\n$1\r\n0\r\n$19\r\n1878060940079923202\r\n";

// Restores --dbnum when a test changes it.
auto ScopedDbnum(uint32_t value) {
  uint32_t previous = absl::GetFlag(FLAGS_dbnum);
  absl::SetFlag(&FLAGS_dbnum, value);
  return absl::MakeCleanup([previous] { absl::SetFlag(&FLAGS_dbnum, previous); });
}

}  // namespace

TEST(ClassicReplayTest, ParseRreplayEnvelopeGoldenSet) {
  auto restore = ScopedDbnum(16);
  Wire wire = Wire::FromBytes(kGoldenSet);
  ASSERT_EQ(wire.args.size(), 5u);

  RreplayEnvelope env;
  ASSERT_EQ(ParseRreplayEnvelope(wire.args, &env), RreplayParse::kOk);
  EXPECT_EQ(env.uuid, kUuid);
  EXPECT_EQ(env.inner, "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n");
  ASSERT_TRUE(env.db.has_value());
  EXPECT_EQ(*env.db, 0u);
  EXPECT_EQ(env.mvcc, 1878060925646274561u);
}

TEST(ClassicReplayTest, ParseRreplayEnvelopeGoldenDbAndControlCommands) {
  auto restore = ScopedDbnum(16);
  struct Case {
    string_view wire;
    string_view inner;
    DbIndex db;
    uint64_t mvcc;
  };
  // What an active KeyDB wraps besides writes: the db travels in the envelope (there is no
  // SELECT), a transaction is one envelope per command, and the cron PING is in lower case.
  const Case cases[] = {
      {kGoldenSetDb3, "*3\r\n$3\r\nSET\r\n$2\r\nk3\r\n$2\r\nv3\r\n", 3, 1878060926489329669u},
      {kGoldenMulti, "*1\r\n$5\r\nMULTI\r\n", 0, 1878060926910857219u},
      {kGoldenExec, "*1\r\n$4\r\nEXEC\r\n", 0, 1878060926910857223u},
      {kGoldenIncrBy, "*3\r\n$6\r\nINCRBY\r\n$1\r\nc\r\n$1\r\n1\r\n", 0, 1878060926910857222u},
      {kGoldenPing, "*1\r\n$4\r\nping\r\n", 0, 1878060934338969600u},
      {kGoldenExpireMember,
       "*4\r\n$15\r\nPEXPIREMEMBERAT\r\n$8\r\nseed:set\r\n$2\r\nm1\r\n$13\r\n1791058571443\r\n", 0,
       1878060927751815169u},
  };
  for (const Case& c : cases) {
    Wire wire = Wire::FromBytes(c.wire);
    RreplayEnvelope env;
    ASSERT_EQ(ParseRreplayEnvelope(wire.args, &env), RreplayParse::kOk) << c.inner;
    EXPECT_EQ(env.uuid, kUuid);
    EXPECT_EQ(env.inner, c.inner);
    ASSERT_TRUE(env.db.has_value()) << c.inner;
    EXPECT_EQ(*env.db, c.db) << c.inner;
    EXPECT_EQ(env.mvcc, c.mvcc) << c.inner;
  }
}

// A forwarding KeyDB wraps the envelope it received: the inner command of the outer envelope is
// itself an envelope, and unwrapping it again reaches the command.
TEST(ClassicReplayTest, ParseRreplayEnvelopeGoldenNestedDepthTwo) {
  auto restore = ScopedDbnum(16);
  Wire outer_wire = Wire::FromBytes(kGoldenNested);
  RreplayEnvelope outer;
  ASSERT_EQ(ParseRreplayEnvelope(outer_wire.args, &outer), RreplayParse::kOk);
  EXPECT_EQ(outer.uuid, "3e4efc2d-a9f9-4a1e-90b7-ebd4171606de");
  EXPECT_EQ(outer.mvcc, 1878060940079923202u);
  ASSERT_TRUE(outer.db.has_value());
  EXPECT_EQ(*outer.db, 0u);

  Wire inner_wire = Wire::FromBytes(outer.inner);
  ASSERT_EQ(inner_wire.args.size(), 5u);
  EXPECT_EQ(inner_wire.args[0].GetView(), "RREPLAY");
  RreplayEnvelope inner;
  ASSERT_EQ(ParseRreplayEnvelope(inner_wire.args, &inner), RreplayParse::kOk);
  EXPECT_EQ(inner.uuid, "307ea48c-fc10-47a1-91bb-263276269153");
  EXPECT_EQ(inner.inner, "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n");
  EXPECT_EQ(inner.mvcc, 1878060940078874625u);
  ASSERT_TRUE(inner.db.has_value());
  EXPECT_EQ(*inner.db, 0u);
}

TEST(ClassicReplayTest, ParseRreplayEnvelopeThreeArgumentsHasNoDbAndNoMvcc) {
  auto restore = ScopedDbnum(16);
  Wire wire{"RREPLAY", kUuid, "*1\r\n$4\r\nPING\r\n"};
  RreplayEnvelope env;
  // Left over from an earlier parse: kOk must reset what the short form leaves out.
  env.db = 7;
  env.mvcc = 99;
  ASSERT_EQ(ParseRreplayEnvelope(wire.args, &env), RreplayParse::kOk);
  EXPECT_EQ(env.uuid, kUuid);
  EXPECT_EQ(env.inner, "*1\r\n$4\r\nPING\r\n");
  EXPECT_FALSE(env.db.has_value());
  EXPECT_EQ(env.mvcc, 0u);

  // The 4-argument form: a db and no mvcc.
  Wire with_db{"RREPLAY", kUuid, "x", "5"};
  ASSERT_EQ(ParseRreplayEnvelope(with_db.args, &env), RreplayParse::kOk);
  ASSERT_TRUE(env.db.has_value());
  EXPECT_EQ(*env.db, 5u);
  EXPECT_EQ(env.mvcc, 0u);
}

// KeyDB reads argv[1..4] and ignores whatever follows.
TEST(ClassicReplayTest, ParseRreplayEnvelopeIgnoresArgumentsPastTheFifth) {
  auto restore = ScopedDbnum(16);
  Wire wire{"RREPLAY", kUuid, "x", "2", "42", "garbage", "more"};
  RreplayEnvelope env;
  ASSERT_EQ(ParseRreplayEnvelope(wire.args, &env), RreplayParse::kOk);
  ASSERT_TRUE(env.db.has_value());
  EXPECT_EQ(*env.db, 2u);
  EXPECT_EQ(env.mvcc, 42u);
}

TEST(ClassicReplayTest, ParseRreplayEnvelopeBadArity) {
  RreplayEnvelope env;
  Wire two{"RREPLAY", kUuid};
  EXPECT_EQ(ParseRreplayEnvelope(two.args, &env), RreplayParse::kBadArity);
  Wire one{"RREPLAY"};
  EXPECT_EQ(ParseRreplayEnvelope(one.args, &env), RreplayParse::kBadArity);
  EXPECT_EQ(ParseRreplayEnvelope(RespVec{}, &env), RreplayParse::kBadArity);

  // The parser of a server only yields strings, but the check is KeyDB's own ("Expected command
  // buffer arg2"): a vector that carries a number as the inner command is refused as well.
  Wire numeric{"RREPLAY", kUuid, "x"};
  numeric.args[2].type = RespExpr::INT64;
  numeric.args[2].u = int64_t{5};
  EXPECT_EQ(ParseRreplayEnvelope(numeric.args, &env), RreplayParse::kBadArity);
}

TEST(ClassicReplayTest, ParseRreplayEnvelopeUuid) {
  RreplayEnvelope env;
  auto parse = [&env](string_view uuid) {
    Wire wire{"RREPLAY", uuid, "x", "0", "1"};
    return ParseRreplayEnvelope(wire.args, &env);
  };

  EXPECT_EQ(parse(kUuid), RreplayParse::kOk);
  // KeyDB's uuid_parse is case-insensitive; the envelope carries it normalized to lowercase.
  EXPECT_EQ(parse("B1198D29-CB88-4110-922A-A6C99BD08471"), RreplayParse::kOk);
  EXPECT_EQ(env.uuid, kUuid);
  EXPECT_EQ(parse("B1198d29-cb88-4110-922a-A6c99bd08471"), RreplayParse::kOk);
  EXPECT_EQ(env.uuid, kUuid);

  EXPECT_EQ(parse("b1198d29-cb88-4110-922a-a6c99bd0847"), RreplayParse::kBadUuid);    // 35 chars
  EXPECT_EQ(parse("b1198d29-cb88-4110-922a-a6c99bd084711"), RreplayParse::kBadUuid);  // 37 chars
  EXPECT_EQ(parse("b1198d29cb88-4110-922a-a6c99bd08471-"), RreplayParse::kBadUuid);   // dash at 8
  EXPECT_EQ(parse("b1198d29-cb88-4110-922aa-6c99bd08471"), RreplayParse::kBadUuid);   // dash at 23
  EXPECT_EQ(parse("b1198d29-cb88-4110-922a-a6c99bd0847g"), RreplayParse::kBadUuid);   // not hex
  EXPECT_EQ(parse("b1198d29-cb88-4110-922a-a6c99bd0847 "), RreplayParse::kBadUuid);
  EXPECT_EQ(parse(""), RreplayParse::kBadUuid);

  Wire not_a_string{"RREPLAY", kUuid, "x"};
  not_a_string.args[1].type = RespExpr::INT64;
  not_a_string.args[1].u = int64_t{5};
  EXPECT_EQ(ParseRreplayEnvelope(not_a_string.args, &env), RreplayParse::kBadUuid);
}

TEST(ClassicReplayTest, ParseRreplayEnvelopeDb) {
  auto restore = ScopedDbnum(16);
  RreplayEnvelope env;
  auto parse = [&env](string_view db) {
    Wire wire{"RREPLAY", kUuid, "x", db, "1"};
    return ParseRreplayEnvelope(wire.args, &env);
  };

  EXPECT_EQ(parse("0"), RreplayParse::kOk);
  EXPECT_EQ(parse("15"), RreplayParse::kOk);
  EXPECT_EQ(env.db, DbIndex{15});

  EXPECT_EQ(parse("16"), RreplayParse::kBadDb);  // db < dbnum, not db <= dbnum
  EXPECT_EQ(parse("-1"), RreplayParse::kBadDb);
  EXPECT_EQ(parse("abc"), RreplayParse::kBadDb);
  EXPECT_EQ(parse(""), RreplayParse::kBadDb);
  EXPECT_EQ(parse("1x"), RreplayParse::kBadDb);
  EXPECT_EQ(parse(" 1"), RreplayParse::kBadDb);
  EXPECT_EQ(parse("+1"), RreplayParse::kBadDb);
  EXPECT_EQ(parse("99999999999999999999"), RreplayParse::kBadDb);  // does not fit 64 bits
  EXPECT_EQ(parse("18446744073709551616"), RreplayParse::kBadDb);  // 2^64

  // The limit is --dbnum's current value.
  auto small = ScopedDbnum(4);
  EXPECT_EQ(parse("3"), RreplayParse::kOk);
  EXPECT_EQ(parse("4"), RreplayParse::kBadDb);
  EXPECT_EQ(parse("15"), RreplayParse::kBadDb);
}

TEST(ClassicReplayTest, ParseRreplayEnvelopeMvcc) {
  auto restore = ScopedDbnum(16);
  RreplayEnvelope env;
  auto parse = [&env](string_view mvcc) {
    Wire wire{"RREPLAY", kUuid, "x", "0", mvcc};
    return ParseRreplayEnvelope(wire.args, &env);
  };

  EXPECT_EQ(parse("0"), RreplayParse::kOk);
  EXPECT_EQ(env.mvcc, 0u);
  // KeyDB's stamps are (ms << 20) | counter: far above 2^53, and bit 63 can be set.
  EXPECT_EQ(parse("9223372036854775808"), RreplayParse::kOk);
  EXPECT_EQ(env.mvcc, uint64_t{1} << 63);
  EXPECT_EQ(parse("18446744073709551615"), RreplayParse::kOk);
  EXPECT_EQ(env.mvcc, numeric_limits<uint64_t>::max());

  EXPECT_EQ(parse("-5"), RreplayParse::kBadMvcc);
  EXPECT_EQ(parse("abc"), RreplayParse::kBadMvcc);
  EXPECT_EQ(parse(""), RreplayParse::kBadMvcc);
  EXPECT_EQ(parse("12 "), RreplayParse::kBadMvcc);
  EXPECT_EQ(parse("1.5"), RreplayParse::kBadMvcc);
  EXPECT_EQ(parse("18446744073709551616"), RreplayParse::kBadMvcc);  // 2^64
  EXPECT_EQ(parse("99999999999999999999999"), RreplayParse::kBadMvcc);
}

// KeyDB validates in argument order: with a bad db and a bad mvcc the db is what it reports.
TEST(ClassicReplayTest, ParseRreplayEnvelopeChecksInKeyDbOrder) {
  auto restore = ScopedDbnum(16);
  RreplayEnvelope env;
  Wire bad_uuid_and_db{"RREPLAY", "nope", "x", "99", "-1"};
  EXPECT_EQ(ParseRreplayEnvelope(bad_uuid_and_db.args, &env), RreplayParse::kBadUuid);
  Wire bad_db_and_mvcc{"RREPLAY", kUuid, "x", "99", "-1"};
  EXPECT_EQ(ParseRreplayEnvelope(bad_db_and_mvcc.args, &env), RreplayParse::kBadDb);

  // KeyDB selects the db (replication.cpp:5416) before it reads the mvcc (:5427), so a bad mvcc
  // is the one failure that hands the db back.
  Wire good_db_bad_mvcc{"RREPLAY", kUuid, "x", "7", "-1"};
  env.db.reset();
  EXPECT_EQ(ParseRreplayEnvelope(good_db_bad_mvcc.args, &env), RreplayParse::kBadMvcc);
  ASSERT_TRUE(env.db.has_value());
  EXPECT_EQ(*env.db, 7u);
}

TEST(ClassicReplayTest, IsRreplayIsCaseInsensitiveAndNeedsAString) {
  RespExpr name{RespExpr::STRING};
  for (string_view spelling : {"RREPLAY", "rreplay", "RReplay"}) {
    name.u = RespExpr::Buffer{reinterpret_cast<const uint8_t*>(spelling.data()), spelling.size()};
    EXPECT_TRUE(ClassicApplier::IsRreplay(name)) << spelling;
  }
  for (string_view other : {"RREPLAYX", "REPLAY", "SET", ""}) {
    name.u = RespExpr::Buffer{reinterpret_cast<const uint8_t*>(other.data()), other.size()};
    EXPECT_FALSE(ClassicApplier::IsRreplay(name)) << other;
  }
  RespExpr number{RespExpr::INT64};
  number.u = int64_t{5};
  EXPECT_FALSE(ClassicApplier::IsRreplay(number));
}

// Every command spelling a KeyDB-only check must decide on, with or without a case, as it stands in
// a stream. `KEYDB.MVCCRESTORE` is the one KEYDB.* command that is not dropped: it carries data
// (spec D-7a, decision 22).
TEST(ClassicReplayTest, IsKeyDbOnlyCommandTable) {
  struct Case {
    vector<string_view> words;
    bool only;
  };
  const vector<Case> cases = {
      {{"PEXPIREMEMBERAT", "s", "m", "1791058571443"}, true},
      {{"EXPIREMEMBER", "s", "m", "100"}, true},
      {{"EXPIREMEMBERAT", "s", "m", "1791058571"}, true},
      {{"KEYDB.CRON", "job", "single", "1000", "return 1"}, true},
      {{"KEYDB.HRENAME", "h", "a", "b"}, true},
      {{"KEYDB.NHSET", "k", "a", "1"}, true},
      {{"KEYDB.NHGET", "k", "a"}, true},
      {{"KEYDB.MEXISTS", "k", "a", "b"}, true},
      // An envelope is unwrapped, never dispatched.
      {{"RREPLAY", kUuid, "x", "0", "1"}, true},
      // Without case: KeyDB's own cron PING is in lower case, and nothing says these are not.
      {{"pexpirememberat", "s", "m", "1"}, true},
      {{"ExpireMember", "s", "m", "100"}, true},
      {{"keydb.cron", "job", "single", "1000", "return 1"}, true},
      {{"KeyDb.NhGet", "k", "a"}, true},
      {{"rreplay", kUuid, "x"}, true},
      // PERSIST is standard with a key, and KeyDB-only with a key and a subkey.
      {{"PERSIST", "k", "m"}, true},
      {{"persist", "k", "m"}, true},
      {{"PERSIST", "k"}, false},
      {{"persist", "k"}, false},
      {{"PERSIST"}, false},
      {{"PERSIST", "k", "m", "x"}, false},
      // Applied, not dropped.
      {{"KEYDB.MVCCRESTORE", "k", "1", "0", "payload"}, false},
      {{"keydb.mvccrestore", "k", "1", "0", "payload"}, false},
      // Near misses: the names are matched whole.
      {{"KEYDB.CRONX", "job"}, false},
      {{"KEYDB.CRO", "job"}, false},
      {{"KEYDB.", "job"}, false},
      {{"KEYDB", "job"}, false},
      {{"KEYDB.HGET", "h", "a"}, false},
      {{"EXPIREMEMBERS", "s", "m", "100"}, false},
      {{"EXPIREMEMBE", "s", "m", "100"}, false},
      {{"PEXPIREMEMBER", "s", "m", "100"}, false},
      {{"RREPLAYX", kUuid, "x"}, false},
      {{"EXPIRE", "k", "100"}, false},
      {{"PEXPIREAT", "k", "1791058571443"}, false},
      {{"HEXPIRE", "h", "100", "FIELDS", "1", "f"}, false},
      {{"SET", "k", "v"}, false},
      {{"PING"}, false},
      {{""}, false},
  };
  for (const Case& c : cases) {
    string bytes = absl::StrCat("*", c.words.size(), "\r\n");
    for (string_view word : c.words)
      absl::StrAppend(&bytes, "$", word.size(), "\r\n", word, "\r\n");
    Wire wire = Wire::FromBytes(bytes);
    EXPECT_EQ(IsKeyDbOnlyCommand(wire.args), c.only) << c.words[0] << " / " << c.words.size();
  }

  // No name to read: not KeyDB-only, and not a crash.
  EXPECT_FALSE(IsKeyDbOnlyCommand(RespVec{}));
  Wire numeric{"PEXPIREMEMBERAT", "s", "m", "1"};
  numeric.args[0].type = RespExpr::INT64;
  numeric.args[0].u = int64_t{5};
  EXPECT_FALSE(IsKeyDbOnlyCommand(numeric.args));
}

namespace {

ClassicLinkCounts Counts(uint64_t unwrapped, uint64_t malformed, uint64_t self, uint64_t keydb,
                         uint64_t unknown, uint64_t errors) {
  ClassicLinkCounts counts;
  counts.rreplay_unwrapped = unwrapped;
  counts.rreplay_malformed = malformed;
  counts.rreplay_self_dropped = self;
  counts.keydb_cmds_dropped = keydb;
  counts.classic_unknown_cmds_dropped = unknown;
  counts.classic_apply_errors = errors;
  return counts;
}

// The names and values of `fields`, in order.
vector<pair<string, uint64_t>> NamesAndValues(const vector<ClassicCounterValue>& fields) {
  vector<pair<string, uint64_t>> out;
  for (const ClassicCounterValue& field : fields)
    out.emplace_back(string(field.name), field.value);
  return out;
}

ReplicaSummary ClassicLink(bool active, const ClassicLinkCounts& counts) {
  ReplicaSummary link{};
  link.classic_link = true;
  link.master_active_replica = active;
  link.classic = counts;
  return link;
}

}  // namespace

// A classic field is shown for a classic link whose master answered active-replica, or whose own
// counter moved: with a stock master and every counter zero INFO stays what upstream prints.
TEST(ClassicReplayTest, ClassicLinkFieldsFollowTheRenderPredicate) {
  using Fields = vector<pair<string, uint64_t>>;

  // An active KeyDB master: every counter, in the order of spec D-13, zeros included.
  ReplicaSummary active = ClassicLink(true, Counts(7, 0, 1, 2, 3, 4));
  EXPECT_TRUE(ClassicLinkShown(active));
  EXPECT_EQ(NamesAndValues(ClassicLinkFields(active)), (Fields{{"rreplay_unwrapped", 7},
                                                               {"rreplay_malformed", 0},
                                                               {"rreplay_self_dropped", 1},
                                                               {"keydb_cmds_dropped", 2},
                                                               {"classic_unknown_cmds_dropped", 3},
                                                               {"classic_apply_errors", 4}}));
  EXPECT_EQ(NamesAndValues(ClassicLinkFields(ClassicLink(true, Counts(0, 0, 0, 0, 0, 0)))).size(),
            6u);

  // Any other master with every counter zero: nothing.
  ReplicaSummary stock = ClassicLink(false, Counts(0, 0, 0, 0, 0, 0));
  EXPECT_FALSE(ClassicLinkShown(stock));
  EXPECT_TRUE(ClassicLinkFields(stock).empty());

  // ... with a counter that moved (a non-active KeyDB sending a raw PEXPIREMEMBERAT): that counter
  // only, and the link is shown.
  ReplicaSummary moved = ClassicLink(false, Counts(0, 0, 0, 3, 0, 0));
  EXPECT_TRUE(ClassicLinkShown(moved));
  EXPECT_EQ(NamesAndValues(ClassicLinkFields(moved)), (Fields{{"keydb_cmds_dropped", 3}}));
  moved = ClassicLink(false, Counts(0, 2, 0, 0, 0, 9));
  EXPECT_EQ(NamesAndValues(ClassicLinkFields(moved)),
            (Fields{{"rreplay_malformed", 2}, {"classic_apply_errors", 9}}));

  // A link that is not classic (a DFLY master) shows nothing, whatever its summary holds.
  ReplicaSummary dfly = active;
  dfly.classic_link = false;
  EXPECT_FALSE(ClassicLinkShown(dfly));
  EXPECT_TRUE(ClassicLinkFields(dfly).empty());
  EXPECT_FALSE(ClassicMasterActive(dfly));
  EXPECT_TRUE(ClassicMasterActive(active));
  EXPECT_FALSE(ClassicMasterActive(stock));

  // Every field carries a help text.
  for (const ClassicCounterValue& field : ClassicLinkFields(active))
    EXPECT_FALSE(field.help.empty()) << field.name;
}

TEST(ClassicReplayTest, ClassicTotalSeriesFollowTheRenderPredicate) {
  using Fields = vector<pair<string, uint64_t>>;

  EXPECT_TRUE(ClassicTotalSeries(Counts(0, 0, 0, 0, 0, 0), false).empty());
  EXPECT_EQ(NamesAndValues(ClassicTotalSeries(Counts(0, 0, 0, 0, 0, 0), true)).size(), 6u);
  EXPECT_EQ(NamesAndValues(ClassicTotalSeries(Counts(5, 0, 0, 0, 2, 0), false)),
            (Fields{{"rreplay_unwrapped", 5}, {"classic_unknown_cmds_dropped", 2}}));
  EXPECT_EQ(NamesAndValues(ClassicTotalSeries(Counts(5, 0, 0, 0, 2, 0), true)),
            (Fields{{"rreplay_unwrapped", 5},
                    {"rreplay_malformed", 0},
                    {"rreplay_self_dropped", 0},
                    {"keydb_cmds_dropped", 0},
                    {"classic_unknown_cmds_dropped", 2},
                    {"classic_apply_errors", 0}}));
}

namespace {

constexpr string_view kSelfUuid = "00000000-0000-4000-8000-000000000001";
constexpr string_view kAuthorA = "11111111-1111-4111-8111-111111111111";
constexpr string_view kAuthorB = "22222222-2222-4222-8222-222222222222";

// `RREPLAY <uuid> <inner> <db> <mvcc>` as a master streams it.
string Envelope(string_view uuid, string_view inner, string_view db = "0", string_view mvcc = "7") {
  return Resp({"RREPLAY", uuid, inner, db, mvcc});
}

// `levels` envelopes around `command`, innermost first authored by kAuthorA, the others by
// kAuthorB.
string Nest(unsigned levels, string_view command, string_view db = "0") {
  string wire = Envelope(kAuthorA, command, db);
  for (unsigned i = 1; i < levels; ++i)
    wire = Envelope(kAuthorB, wire, db);
  return wire;
}

}  // namespace

// The applier on the apply context of ConsumeRedisStream (replica.cc): no connection, a replicated
// apply. It dispatches on a proactor thread's fiber, as the replication fiber does.
class ClassicApplyFamilyTest : public BaseFamilyTest {
 protected:
  struct Link {
    Link(Service* service, function<bool()> running)
        : cntx(nullptr, acl::UserCredentials{}),
          applier(service, &cntx, string(kSelfUuid), "test-master:1", &stats, std::move(running)) {
      cntx.is_replicating = true;
      cntx.journal_emulated = true;
      cntx.skip_acl_validation = true;
      cntx.ns = &namespaces->GetDefaultNamespace();
    }

    // Feeds the envelope in `wire` to the applier the way the stream loop does.
    EnvelopeResult Apply(string_view wire, unsigned depth = 1) {
      Wire parsed = Wire::FromBytes(wire);
      return applier.HandleRreplay(parsed.args, depth);
    }

    uint64_t unwrapped() const {
      return stats.rreplay_unwrapped.load();
    }
    uint64_t malformed() const {
      return stats.rreplay_malformed.load();
    }
    uint64_t self_dropped() const {
      return stats.rreplay_self_dropped.load();
    }
    uint64_t apply_errors() const {
      return stats.classic_apply_errors.load();
    }
    uint64_t keydb_dropped() const {
      return stats.keydb_cmds_dropped.load();
    }
    uint64_t unknown_dropped() const {
      return stats.classic_unknown_cmds_dropped.load();
    }

    ConnectionContext cntx;
    ClassicLinkStats stats;
    ClassicApplier applier;
  };

  // Runs `body` with a fresh link on proactor 0's fiber. `running` is the link's running().
  template <typename F>
  void OnLink(
      F body, function<bool()> running = [] { return true; }) {
    pp_->at(0)
        ->LaunchFiber([&] {
          Link link(service_.get(), std::move(running));
          body(link);
        })
        .Join();
  }

  // Replaces the handler of `name` by one that counts its calls.
  void CountCalls(string_view name, atomic_int* calls) {
    auto handler = [calls](facade::CmdArgParser, CommandContext* cmd_cntx) {
      calls->fetch_add(1);
      cmd_cntx->SendOk();
    };
    std::move(*service_->mutable_registry()->Find(name)).SetHandler(handler);
  }

  // The value of `key` in `db`, or nullopt.
  optional<string> Get(string_view key, unsigned db = 0) {
    Run({"select", to_string(db)});
    RespExpr value = Run({"get", key});
    Run({"select", "0"});
    if (value.type == RespExpr::NIL)
      return nullopt;
    return string(value.GetView());
  }
};

TEST_F(ClassicApplyFamilyTest, AppliesInnerCommandInEnvelopeDb) {
  OnLink([&](Link& link) {
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"SET", "k", "v"}), "0", "10")),
              EnvelopeResult::kConsumed);
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"SET", "k3", "v3"}), "3", "11")),
              EnvelopeResult::kConsumed);
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"INCRBY", "c", "5"}), "0", "12")),
              EnvelopeResult::kConsumed);
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"HSET", "h", "f", "v"}), "0", "13")),
              EnvelopeResult::kConsumed);
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"SET", "gone", "x"}), "0", "14")),
              EnvelopeResult::kConsumed);
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"DEL", "gone"}), "0", "15")),
              EnvelopeResult::kConsumed);
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"SET", "k", "v2"}), "0", "16")),
              EnvelopeResult::kConsumed);

    EXPECT_EQ(link.unwrapped(), 7u);
    EXPECT_EQ(link.malformed(), 0u);
    EXPECT_EQ(link.self_dropped(), 0u);
    EXPECT_EQ(link.apply_errors(), 0u);
  });

  EXPECT_EQ(Get("k"), "v2");
  EXPECT_EQ(Get("k3", 3), "v3");
  EXPECT_EQ(Get("k3", 0), nullopt);  // the db of the envelope, not db 0
  EXPECT_EQ(Get("c"), "5");
  EXPECT_EQ(Get("gone"), nullopt);
  EXPECT_EQ(Run({"hget", "h", "f"}), "v");
}

// The db travels in the envelope: the first use of one selects it for real, later ones switch to
// it, the 3-argument form has none and runs in the db already selected, and an inner envelope's db
// is the one that counts (an inner without one inherits the outer's).
TEST_F(ClassicApplyFamilyTest, SelectSetsEnvelopeDb) {
  OnLink([&](Link& link) {
    EXPECT_EQ(link.cntx.conn_state.db_index, 0u);
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"SET", "a", "1"}), "3")),
              EnvelopeResult::kConsumed);
    EXPECT_EQ(link.cntx.conn_state.db_index, 3u);
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"SET", "b", "2"}), "5")),
              EnvelopeResult::kConsumed);
    EXPECT_EQ(link.cntx.conn_state.db_index, 5u);
    // Back to a db the link already used.
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"SET", "c", "3"}), "3")),
              EnvelopeResult::kConsumed);
    EXPECT_EQ(link.cntx.conn_state.db_index, 3u);

    // No db in the envelope: the selected one, as for KeyDB's master client.
    EXPECT_EQ(link.Apply(Resp({"RREPLAY", kAuthorA, Resp({"SET", "d", "4"})})),
              EnvelopeResult::kConsumed);
    EXPECT_EQ(link.cntx.conn_state.db_index, 3u);

    // Nested: the innermost envelope's db, else the enclosing one's.
    EXPECT_EQ(link.Apply(Envelope(kAuthorB, Envelope(kAuthorA, Resp({"SET", "e", "5"}), "7"), "2")),
              EnvelopeResult::kConsumed);
    EXPECT_EQ(
        link.Apply(Envelope(kAuthorB, Resp({"RREPLAY", kAuthorA, Resp({"SET", "f", "6"})}), "4")),
        EnvelopeResult::kConsumed);
    EXPECT_EQ(link.apply_errors(), 0u);
    EXPECT_EQ(link.unwrapped(), 8u);
  });

  EXPECT_EQ(Get("a", 3), "1");
  EXPECT_EQ(Get("b", 5), "2");
  EXPECT_EQ(Get("c", 3), "3");
  EXPECT_EQ(Get("d", 3), "4");
  EXPECT_EQ(Get("e", 7), "5");
  EXPECT_EQ(Get("f", 4), "6");
  for (string_view key : {"a", "b", "c", "d", "e", "f"})
    EXPECT_EQ(Get(key, 0), nullopt) << key;
}

TEST_F(ClassicApplyFamilyTest, SkipsInnerControlCommands) {
  atomic_int pings = 0, replconfs = 0;
  CountCalls("PING", &pings);
  CountCalls("REPLCONF", &replconfs);

  OnLink([&](Link& link) {
    // Each carries db 3. KeyDB selects the db of an envelope whatever it wraps, so the context ends
    // in db 3, but the commands are not dispatched. The names are matched without case: KeyDB's
    // cron PING is in lower case.
    for (const string& command : {Resp({"ping"}), Resp({"PING"}), Resp({"REPLCONF", "GETACK", "*"}),
                                  Resp({"MULTI"}), Resp({"Exec"}), Resp({"SELECT", "5"})}) {
      EXPECT_EQ(link.Apply(Envelope(kAuthorA, command, "3")), EnvelopeResult::kConsumed) << command;
    }
    EXPECT_EQ(link.cntx.conn_state.db_index, 3u);  // not 5: the SELECT inside was not dispatched
    EXPECT_FALSE(link.cntx.conn_state.exec_info.IsCollecting());
    EXPECT_EQ(link.apply_errors(), 0u);  // an EXEC dispatched with no MULTI would be one
    EXPECT_EQ(link.unwrapped(), 6u);

    // Had the MULTI run, this would only be queued. It has no db, and runs in the one the control
    // commands selected.
    EXPECT_EQ(link.Apply(Resp({"RREPLAY", kAuthorA, Resp({"SET", "after", "v"})})),
              EnvelopeResult::kConsumed);
  });

  EXPECT_EQ(pings.load(), 0);
  EXPECT_EQ(replconfs.load(), 0);
  EXPECT_EQ(Get("after", 3), "v");
  EXPECT_EQ(Get("after", 0), nullopt);
  EXPECT_EQ(Get("after", 5), nullopt);
}

// KeyDB's replicaReplayCommand selects the db of a layer as it validates it (replication.cpp:5416),
// before the author (:5435) and the inner command are looked at, and the master client stays there:
// whatever the layer then turns out to be, the next command without a db of its own runs in it.
TEST_F(ClassicApplyFamilyTest, DbIsSelectedBeforeTheAuthorAndInnerChecks) {
  const string kSet = Resp({"SET", "x", "1"});
  // The next command with no db of its own, in whatever db the context is in.
  auto next = [&](Link& link, string_view key) {
    EXPECT_EQ(link.Apply(Resp({"RREPLAY", kAuthorA, Resp({"SET", key, "1"})})),
              EnvelopeResult::kConsumed);
  };

  OnLink([&](Link& link) {
    // Authored by this node: dropped, after its db was selected.
    EXPECT_EQ(link.Apply(Envelope(kSelfUuid, kSet, "3")), EnvelopeResult::kConsumed);
    EXPECT_EQ(link.self_dropped(), 1u);
    EXPECT_EQ(link.cntx.conn_state.db_index, 3u);
    next(link, "after_self");

    // A bad mvcc is read after the db.
    EXPECT_EQ(link.Apply(Resp({"RREPLAY", kAuthorA, kSet, "4", "-5"})), EnvelopeResult::kConsumed);
    EXPECT_EQ(link.malformed(), 1u);
    EXPECT_EQ(link.cntx.conn_state.db_index, 4u);
    next(link, "after_bad_mvcc");

    // An inner that is not a command, then one that is not even a string-named command.
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, "not a command", "5")), EnvelopeResult::kConsumed);
    EXPECT_EQ(link.malformed(), 2u);
    EXPECT_EQ(link.cntx.conn_state.db_index, 5u);
    next(link, "after_not_a_command");
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, "*0\r\n", "6")), EnvelopeResult::kConsumed);
    EXPECT_EQ(link.malformed(), 3u);
    EXPECT_EQ(link.cntx.conn_state.db_index, 6u);
    next(link, "after_empty_array");

    // A layer that fails before its db is taken has not selected it: the uuid, the arity and the
    // db itself are checked first.
    EXPECT_EQ(link.Apply(Resp({"RREPLAY", "not-a-uuid", kSet, "7", "7"})),
              EnvelopeResult::kConsumed);
    EXPECT_EQ(link.Apply(Resp({"RREPLAY", kAuthorA})), EnvelopeResult::kConsumed);
    EXPECT_EQ(link.Apply(Resp({"RREPLAY", kAuthorA, kSet, "16", "7"})), EnvelopeResult::kConsumed);
    EXPECT_EQ(link.malformed(), 6u);
    EXPECT_EQ(link.cntx.conn_state.db_index, 6u);

    // Nested, the layers select one after the other and the context is left in the last one's db: a
    // self-authored inner layer, an inner with a bad mvcc.
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Envelope(kSelfUuid, kSet, "8"), "2")),
              EnvelopeResult::kConsumed);
    EXPECT_EQ(link.self_dropped(), 2u);
    EXPECT_EQ(link.cntx.conn_state.db_index, 8u);
    next(link, "after_nested_self");
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"RREPLAY", kAuthorB, kSet, "9", "-5"}), "2")),
              EnvelopeResult::kConsumed);
    EXPECT_EQ(link.malformed(), 7u);
    EXPECT_EQ(link.cntx.conn_state.db_index, 9u);
    next(link, "after_nested_bad_mvcc");

    // The layer past the nesting limit (the 65th) is validated and selected before its depth is
    // counted: a malformed layer all the same.
    string wire = Envelope(kAuthorA, kSet, "10");
    for (unsigned i = 1; i <= ClassicApplier::kMaxNesting; ++i)
      wire = Envelope(kAuthorB, wire, "1");
    EXPECT_EQ(link.Apply(wire), EnvelopeResult::kConsumed);
    EXPECT_EQ(link.malformed(), 8u);
    EXPECT_EQ(link.cntx.conn_state.db_index, 10u);
    next(link, "after_too_deep");

    EXPECT_EQ(link.apply_errors(), 0u);
  });

  EXPECT_EQ(Get("x", 0), nullopt);  // no inner command ran
  for (const auto& [key, db] : vector<pair<string_view, unsigned>>{{"after_self", 3},
                                                                   {"after_bad_mvcc", 4},
                                                                   {"after_not_a_command", 5},
                                                                   {"after_empty_array", 6},
                                                                   {"after_nested_self", 8},
                                                                   {"after_nested_bad_mvcc", 9},
                                                                   {"after_too_deep", 10}}) {
    EXPECT_EQ(Get(key, db), "1") << key;
    EXPECT_EQ(Get(key, 0), nullopt) << key;
  }
}

// A db that cannot be selected (a cluster node has db 0 only: `SELECT 3` replies an error) leaves
// the layer that asked with no db to run in: its command is skipped and the failed SELECT counted,
// once per attempt. The selected db stays where it was, and a deeper layer that selects a db of its
// own takes over, the last select winning as it does for KeyDB's master client.
TEST_F(ClassicApplyFamilyTest, FailedSelectSkipsTheCommandUnlessADeeperLayerSelects) {
  // SELECT refuses db > 0 in a real cluster only (`IsClusterEnabled()`), not in the emulated one.
  absl::FlagSaver flag_saver;
  SetTestFlag("cluster_mode", "yes");
  ResetService();
  // The node owns every slot, so that the test's own reads are not redirected. The applier's
  // context is a replicating one and is not checked for ownership.
  const string config = absl::Substitute(
      R"json([{"slot_ranges": [{"start": 0, "end": 16383}],
               "master": {"id": "$0", "ip": "10.0.0.1", "port": 7000, "health": "online"},
               "replicas": []}])json",
      Run({"cluster", "myid"}).GetString());
  ASSERT_EQ(RunPrivileged({"dflycluster", "config", config}), "OK");

  OnLink([&](Link& link) {
    uint64_t errors = 0, unwrapped = 0;
    // Applies `wire`, which has `layers` envelope layers and asks for `failed_selects` dbs that
    // cannot be selected.
    auto apply = [&](string_view what, const string& wire, unsigned layers,
                     unsigned failed_selects) {
      EXPECT_EQ(link.Apply(wire), EnvelopeResult::kConsumed) << what;
      errors += failed_selects;
      unwrapped += layers;
      EXPECT_EQ(link.apply_errors(), errors) << what;
      EXPECT_EQ(link.unwrapped(), unwrapped) << what;
      EXPECT_EQ(link.malformed(), 0u) << what;
      EXPECT_EQ(link.cntx.conn_state.db_index, 0u) << what;  // a failed select moves nothing
    };

    // The envelope's own db cannot be selected: no command, one counted error.
    apply("own db", Envelope(kAuthorA, Resp({"SET", "own_db", "1"}), "3"), 1, 1);
    // A failed SELECT is not remembered as one that worked: the next try fails and counts again.
    apply("own db again", Envelope(kAuthorA, Resp({"SET", "own_db_again", "1"}), "3"), 1, 1);

    // Outer db 3 fails, the inner layer selects db 0 and applies there.
    apply("outer db 3, inner db 0",
          Envelope(kAuthorB, Envelope(kAuthorA, Resp({"SET", "inner_db0", "1"}), "0"), "3"), 2, 1);

    // Outer db 0 selects, the inner layer's db 3 fails: the command has no db and is skipped.
    apply("outer db 0, inner db 3",
          Envelope(kAuthorB, Envelope(kAuthorA, Resp({"SET", "inner_db3", "1"}), "3"), "0"), 2, 1);

    // An inner layer without a db inherits the failure of the outer one.
    apply("outer db 3, inner without a db",
          Envelope(kAuthorB, Resp({"RREPLAY", kAuthorA, Resp({"SET", "no_inner_db", "1"})}), "3"),
          2, 1);

    // The skip lasts for the envelope that asked: a later one without a db runs where the context
    // is.
    apply("no db", Resp({"RREPLAY", kAuthorA, Resp({"SET", "no_db", "1"})}), 1, 0);
  });

  EXPECT_EQ(CheckedInt({"dbsize"}), 2);
  EXPECT_EQ(Get("inner_db0"), "1");
  EXPECT_EQ(Get("no_db"), "1");
  for (string_view key : {"own_db", "own_db_again", "inner_db3", "no_inner_db"})
    EXPECT_EQ(Get(key), nullopt) << key;
}

TEST_F(ClassicApplyFamilyTest, DropsSelfAuthoredEnvelope) {
  OnLink([&](Link& link) {
    EXPECT_EQ(link.Apply(Envelope(kSelfUuid, Resp({"SET", "mine", "v"}))),
              EnvelopeResult::kConsumed);
    // The uuid is compared normalized.
    string upper = absl::AsciiStrToUpper(kSelfUuid);
    EXPECT_EQ(link.Apply(Envelope(upper, Resp({"SET", "mine", "v"}))), EnvelopeResult::kConsumed);
    EXPECT_EQ(link.self_dropped(), 2u);
    EXPECT_EQ(link.unwrapped(), 0u);

    // Our envelope forwarded back inside another author's: the outer layer unwraps, the inner is
    // dropped.
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Envelope(kSelfUuid, Resp({"SET", "mine", "v"})))),
              EnvelopeResult::kConsumed);
    EXPECT_EQ(link.self_dropped(), 3u);
    EXPECT_EQ(link.unwrapped(), 1u);
    EXPECT_EQ(link.malformed(), 0u);
    EXPECT_EQ(link.apply_errors(), 0u);

    // An envelope of anyone else still applies.
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"SET", "theirs", "v"}))),
              EnvelopeResult::kConsumed);
  });

  EXPECT_EQ(Get("mine"), nullopt);
  EXPECT_EQ(Get("theirs"), "v");
}

TEST_F(ClassicApplyFamilyTest, MalformedEnvelopeSkippedAndCounted) {
  const string kSet = Resp({"SET", "k", "v"});
  vector<pair<string, string>> cases = {
      {"bad uuid", Resp({"RREPLAY", "not-a-uuid", kSet, "0", "7"})},
      {"bad db", Resp({"RREPLAY", kAuthorA, kSet, "16", "7"})},
      {"negative db", Resp({"RREPLAY", kAuthorA, kSet, "-1", "7"})},
      {"bad mvcc", Resp({"RREPLAY", kAuthorA, kSet, "0", "-5"})},
      {"arity", Resp({"RREPLAY", kAuthorA})},
      {"empty inner", Envelope(kAuthorA, "")},
      {"two commands", Envelope(kAuthorA, kSet + Resp({"SET", "k2", "v2"}))},
      {"trailing bytes", Envelope(kAuthorA, kSet + "xx")},
      {"trailing newline", Envelope(kAuthorA, kSet + "\r\n")},
      {"not RESP", Envelope(kAuthorA, "*x\r\n")},
      {"huge array", Envelope(kAuthorA, "*99999999\r\n$3\r\nSET\r\n")},
      {"huge string", Envelope(kAuthorA, "*1\r\n$99999999\r\nSET\r\n")},
      // Valid RESP, but an array, a nil array and a nil where a command's name goes (U-14): there
      // is no name to read, and the parser holds no string there.
      {"inner empty array", Envelope(kAuthorA, "*0\r\n")},
      {"inner nil array", Envelope(kAuthorA, "*-1\r\n")},
      {"inner nil name", Envelope(kAuthorA, "*1\r\n$-1\r\n")},
  };
  // Every way the inner command can be cut short.
  for (size_t len = 0; len < kSet.size(); ++len)
    cases.emplace_back(absl::StrCat("inner cut at ", len), Envelope(kAuthorA, kSet.substr(0, len)));

  OnLink([&](Link& link) {
    uint64_t expected = 0;
    for (const auto& [name, wire] : cases) {
      EXPECT_EQ(link.Apply(wire), EnvelopeResult::kConsumed) << name;
      EXPECT_EQ(link.malformed(), ++expected) << name;
    }
    EXPECT_EQ(link.unwrapped(), 0u);
    EXPECT_EQ(link.self_dropped(), 0u);
    EXPECT_EQ(link.apply_errors(), 0u);
    EXPECT_EQ(link.cntx.conn_state.db_index, 0u);

    // The next envelope is not affected.
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, kSet)), EnvelopeResult::kConsumed);
    EXPECT_EQ(link.unwrapped(), 1u);
  });

  EXPECT_EQ(CheckedInt({"dbsize"}), 1);
  EXPECT_EQ(Get("k"), "v");
  EXPECT_EQ(Get("k2"), nullopt);
}

// Malformed is per level: a bad inner envelope is skipped and counted, the envelope around it is
// well formed and counted as unwrapped.
TEST_F(ClassicApplyFamilyTest, MalformedInnerEnvelopeLeavesOuterUnwrapped) {
  OnLink([&](Link& link) {
    string bad_inner = Resp({"RREPLAY", "not-a-uuid", Resp({"SET", "k", "v"}), "0", "7"});
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, bad_inner)), EnvelopeResult::kConsumed);
    EXPECT_EQ(link.malformed(), 1u);
    EXPECT_EQ(link.unwrapped(), 1u);
  });
  EXPECT_EQ(Get("k"), nullopt);
}

TEST_F(ClassicApplyFamilyTest, NestedUnwrapAllowedTo64AndRefuses65th) {
  atomic_int running_calls = 0;
  OnLink(
      [&](Link& link) {
        // 64 levels apply: KeyDB's own limit (REPLAY_MAX_NESTING).
        EXPECT_EQ(link.Apply(Nest(64, Resp({"SET", "deep64", "v"}))), EnvelopeResult::kConsumed);
        EXPECT_EQ(link.malformed(), 0u);
        EXPECT_EQ(link.unwrapped(), 64u);
        EXPECT_EQ(running_calls.load(), 1);  // once for the outermost envelope, not per level

        // The 65th is malformed and applies nothing, and the 64 around it are consumed.
        EXPECT_EQ(link.Apply(Nest(65, Resp({"SET", "deep65", "v"}))), EnvelopeResult::kConsumed);
        EXPECT_EQ(link.malformed(), 1u);
        EXPECT_EQ(link.unwrapped(), 64u + 64u);
        EXPECT_EQ(link.apply_errors(), 0u);

        // The depth is where the call starts: a 64th level may be applied directly, a 65th may not.
        EXPECT_EQ(link.Apply(Nest(1, Resp({"SET", "start64", "v"})), 64),
                  EnvelopeResult::kConsumed);
        EXPECT_EQ(link.malformed(), 1u);
        EXPECT_EQ(link.Apply(Nest(1, Resp({"SET", "start65", "v"})), 65),
                  EnvelopeResult::kConsumed);
        EXPECT_EQ(link.malformed(), 2u);

        // A lower case inner name is an envelope too.
        string lower = Resp({"rreplay", kAuthorA, Resp({"SET", "lower", "v"}), "0", "7"});
        EXPECT_EQ(link.Apply(Envelope(kAuthorB, lower)), EnvelopeResult::kConsumed);
        EXPECT_EQ(link.malformed(), 2u);
      },
      [&running_calls] {
        running_calls.fetch_add(1);
        return true;
      });

  EXPECT_EQ(Get("deep64"), "v");
  EXPECT_EQ(Get("deep65"), nullopt);
  EXPECT_EQ(Get("start64"), "v");
  EXPECT_EQ(Get("start65"), nullopt);
  EXPECT_EQ(Get("lower"), "v");
}

// A known command that replies an error (a divergence: WRONGTYPE) is counted, not fatal.
TEST_F(ClassicApplyFamilyTest, KnownCommandErrorReplyCounted) {
  Run({"hset", "h", "f", "v"});

  OnLink([&](Link& link) {
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"INCR", "h"}))), EnvelopeResult::kConsumed);
    EXPECT_EQ(link.apply_errors(), 1u);
    EXPECT_EQ(link.unwrapped(), 1u);
    EXPECT_EQ(link.malformed(), 0u);

    // The stream carries on.
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"SET", "k", "v"}))), EnvelopeResult::kConsumed);
    EXPECT_EQ(link.apply_errors(), 1u);
    EXPECT_EQ(link.unwrapped(), 2u);
  });

  EXPECT_EQ(Get("k"), "v");
  EXPECT_EQ(Run({"hget", "h", "f"}), "v");
}

// The dispatcher rejects this before the command runs; it is consumed all the same.
TEST_F(ClassicApplyFamilyTest, RejectedDispatchCountsAndConsumes) {
  OnLink([&](Link& link) {
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"SET", "k"}))), EnvelopeResult::kConsumed);
    EXPECT_EQ(link.apply_errors(), 1u);
    EXPECT_EQ(link.unwrapped(), 1u);
    EXPECT_EQ(link.malformed(), 0u);
    EXPECT_EQ(link.unknown_dropped(), 0u);  // a known command with the wrong arity is no unknown

    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"SET", "k", "v"}))), EnvelopeResult::kConsumed);
    EXPECT_EQ(link.apply_errors(), 1u);
  });
  EXPECT_EQ(Get("k"), "v");
}

// What an active KeyDB wraps that drakeydb has no equivalent of is skipped, counted and warned
// about, and never reaches the dispatcher: `PERSIST k m` would be an arity error, the others
// unknown commands. A layer that is dropped was still taken apart, so it is unwrapped.
TEST_F(ClassicApplyFamilyTest, KeyDbOnlyDroppedAndCounted) {
  Run({"set", "k", "v"});
  Run({"expire", "k", "100"});

  OnLink([&](Link& link) {
    const vector<string> commands = {
        Resp({"PEXPIREMEMBERAT", "s", "m", "1791058571443"}),
        Resp({"EXPIREMEMBER", "s", "m", "100"}),
        Resp({"EXPIREMEMBERAT", "s", "m", "1791058571"}),
        Resp({"PERSIST", "k", "m"}),
        Resp({"KEYDB.CRON", "job", "single", "100000", "return 1"}),
        Resp({"KEYDB.HRENAME", "h", "a", "b"}),
        Resp({"KEYDB.NHSET", "k", "a", "1"}),
        Resp({"KEYDB.NHGET", "k", "a"}),
        Resp({"KEYDB.MEXISTS", "k", "a", "b"}),
        Resp({"pexpirememberat", "s", "m", "1791058571443"}),
    };
    uint64_t dropped = 0;
    for (const string& command : commands) {
      EXPECT_EQ(link.Apply(Envelope(kAuthorA, command, "3")), EnvelopeResult::kConsumed) << command;
      EXPECT_EQ(link.keydb_dropped(), ++dropped) << command;
    }
    EXPECT_EQ(link.unwrapped(), dropped);
    EXPECT_EQ(link.unknown_dropped(), 0u);  // not unknown: they are KeyDB's
    EXPECT_EQ(link.apply_errors(), 0u);     // and never dispatched
    EXPECT_EQ(link.malformed(), 0u);

    // `PERSIST k m` did not clear the TTL of `k`, which `PERSIST k` (a standard command) does.
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"PERSIST", "k"}))), EnvelopeResult::kConsumed);
    EXPECT_EQ(link.keydb_dropped(), dropped);
    EXPECT_EQ(link.apply_errors(), 0u);

    // The stream carries on.
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"SET", "after", "v"}))),
              EnvelopeResult::kConsumed);
    EXPECT_EQ(link.unwrapped(), dropped + 2);
  });

  EXPECT_THAT(service_->UknownCmdMap(), testing::IsEmpty());  // none went to the dispatcher
  EXPECT_EQ(CheckedInt({"ttl", "k"}), -1);                    // from PERSIST k, not PERSIST k m
  EXPECT_EQ(Get("after"), "v");
  EXPECT_EQ(CheckedInt({"dbsize"}), 2);
}

// A command this server has no command for is counted and not dispatched: dispatched, it would
// only fill the error builder and upstream's `unknown_` accounting with a command that is not the
// operator's to fix.
TEST_F(ClassicApplyFamilyTest, UnknownInnerCommandCountedNotDispatched) {
  OnLink([&](Link& link) {
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"NOSUCHCMD", "x"}))), EnvelopeResult::kConsumed);
    EXPECT_EQ(link.unknown_dropped(), 1u);
    EXPECT_EQ(link.apply_errors(), 0u);
    EXPECT_EQ(link.keydb_dropped(), 0u);
    EXPECT_EQ(link.unwrapped(), 1u);
    EXPECT_EQ(link.malformed(), 0u);

    // Without case, as the dispatcher reads a name, and a subcommand: `ACL <nothing>` is a command,
    // `ACL NOSUCHSUB` is not.
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"nosuchcmd2"}))), EnvelopeResult::kConsumed);
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"ACL", "NOSUCHSUB"}))),
              EnvelopeResult::kConsumed);
    EXPECT_EQ(link.unknown_dropped(), 3u);
    EXPECT_EQ(link.apply_errors(), 0u);

    // A known command is no unknown one, whether it applies or the dispatcher rejects it.
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"SET", "k", "v"}))), EnvelopeResult::kConsumed);
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"SET", "k"}))), EnvelopeResult::kConsumed);
    EXPECT_EQ(link.unknown_dropped(), 3u);
    EXPECT_EQ(link.apply_errors(), 1u);
    EXPECT_EQ(link.unwrapped(), 5u);
  });

  EXPECT_THAT(service_->UknownCmdMap(), testing::IsEmpty());
  EXPECT_EQ(Get("k"), "v");
}

// KEYDB.MVCCRESTORE is data, not a KeyDB-only command to drop (decision 22): Task 2.6 of P7-2
// translates and applies it. Until then drakeydb does not know it, and it is counted as an unknown
// command, never in keydb_cmds_dropped, whose operators would read it as a loss they accepted.
TEST_F(ClassicApplyFamilyTest, KeyDbMvccRestoreCountedUnknownUntilItIsApplied) {
  OnLink([&](Link& link) {
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"KEYDB.MVCCRESTORE", "k", "1878060925646274561",
                                                  "-1", "not a dump payload"}))),
              EnvelopeResult::kConsumed);
    EXPECT_EQ(link.unknown_dropped(), 1u);
    EXPECT_EQ(link.keydb_dropped(), 0u);
    EXPECT_EQ(link.apply_errors(), 0u);
    EXPECT_EQ(link.unwrapped(), 1u);
  });

  EXPECT_EQ(CheckedInt({"dbsize"}), 0);
}

// KeyDB selects the db of a layer before it knows what the layer holds, and so does the applier:
// a KeyDB-only or an unknown command leaves the db its layers selected, and the next command with
// no db of its own runs there.
TEST_F(ClassicApplyFamilyTest, KeyDbOnlyAndUnknownLeavesKeepTheSelectedDb) {
  OnLink([&](Link& link) {
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"EXPIREMEMBER", "s", "m", "100"}), "3")),
              EnvelopeResult::kConsumed);
    EXPECT_EQ(link.keydb_dropped(), 1u);
    EXPECT_EQ(link.cntx.conn_state.db_index, 3u);
    EXPECT_EQ(link.Apply(Resp({"RREPLAY", kAuthorA, Resp({"SET", "after_keydb_only", "v"})})),
              EnvelopeResult::kConsumed);

    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"NOSUCHCMD", "x"}), "5")),
              EnvelopeResult::kConsumed);
    EXPECT_EQ(link.unknown_dropped(), 1u);
    EXPECT_EQ(link.cntx.conn_state.db_index, 5u);
    EXPECT_EQ(link.Apply(Resp({"RREPLAY", kAuthorA, Resp({"SET", "after_unknown", "v"})})),
              EnvelopeResult::kConsumed);

    // Nested: the innermost layer that has a db decides, whatever its command turns out to be.
    EXPECT_EQ(
        link.Apply(Envelope(
            kAuthorB, Envelope(kAuthorA, Resp({"KEYDB.CRON", "j", "single", "9"}), "7"), "2")),
        EnvelopeResult::kConsumed);
    EXPECT_EQ(link.keydb_dropped(), 2u);
    EXPECT_EQ(link.cntx.conn_state.db_index, 7u);
    EXPECT_EQ(link.Apply(Resp({"RREPLAY", kAuthorA, Resp({"SET", "after_nested", "v"})})),
              EnvelopeResult::kConsumed);

    EXPECT_EQ(link.apply_errors(), 0u);
  });

  EXPECT_EQ(Get("after_keydb_only", 3), "v");
  EXPECT_EQ(Get("after_unknown", 5), "v");
  EXPECT_EQ(Get("after_nested", 7), "v");
  for (string_view key : {"after_keydb_only", "after_unknown", "after_nested"})
    EXPECT_EQ(Get(key, 0), nullopt) << key;
}

// Every bump of a link's counter is also one of the process-wide totals that /metrics exports, so
// a link that is gone leaves its counts behind. The totals are the whole process's, hence deltas.
TEST_F(ClassicApplyFamilyTest, CountsAlsoFeedTheProcessWideTotals) {
  const ClassicLinkCounts before = ClassicTotals().Snapshot();

  ClassicLinkCounts link_counts;
  OnLink([&](Link& link) {
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"SET", "k", "v"}))), EnvelopeResult::kConsumed);
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"EXPIREMEMBER", "s", "m", "100"}))),
              EnvelopeResult::kConsumed);
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"NOSUCHCMD"}))), EnvelopeResult::kConsumed);
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"SET", "k"}))), EnvelopeResult::kConsumed);
    EXPECT_EQ(link.Apply(Envelope(kSelfUuid, Resp({"SET", "k", "v"}))), EnvelopeResult::kConsumed);
    EXPECT_EQ(link.Apply(Resp({"RREPLAY", "not-a-uuid", Resp({"SET", "k", "v"})})),
              EnvelopeResult::kConsumed);
    link_counts = link.stats.Snapshot();
  });

  EXPECT_EQ(link_counts.rreplay_unwrapped, 4u);
  EXPECT_EQ(link_counts.rreplay_malformed, 1u);
  EXPECT_EQ(link_counts.rreplay_self_dropped, 1u);
  EXPECT_EQ(link_counts.keydb_cmds_dropped, 1u);
  EXPECT_EQ(link_counts.classic_unknown_cmds_dropped, 1u);
  EXPECT_EQ(link_counts.classic_apply_errors, 1u);

  const ClassicLinkCounts after = ClassicTotals().Snapshot();
  EXPECT_EQ(after.rreplay_unwrapped - before.rreplay_unwrapped, link_counts.rreplay_unwrapped);
  EXPECT_EQ(after.rreplay_malformed - before.rreplay_malformed, link_counts.rreplay_malformed);
  EXPECT_EQ(after.rreplay_self_dropped - before.rreplay_self_dropped,
            link_counts.rreplay_self_dropped);
  EXPECT_EQ(after.keydb_cmds_dropped - before.keydb_cmds_dropped, link_counts.keydb_cmds_dropped);
  EXPECT_EQ(after.classic_unknown_cmds_dropped - before.classic_unknown_cmds_dropped,
            link_counts.classic_unknown_cmds_dropped);
  EXPECT_EQ(after.classic_apply_errors - before.classic_apply_errors,
            link_counts.classic_apply_errors);
}

// A link that is not running at the outermost envelope has nothing touched: not a dispatch, not the
// SELECT of the envelope's db, not a counter. The envelope is not consumed, so the stream's offset
// stays at it and a partial resync starts from it.
TEST_F(ClassicApplyFamilyTest, RunningFalseBeforeDispatchReturnsNotConsumed) {
  OnLink(
      [&](Link& link) {
        EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"SET", "k", "v"}), "3")),
                  EnvelopeResult::kNotConsumed);
        EXPECT_EQ(link.cntx.conn_state.db_index, 0u);  // no synthetic SELECT either
        EXPECT_EQ(link.unwrapped(), 0u);
        EXPECT_EQ(link.malformed(), 0u);
        EXPECT_EQ(link.self_dropped(), 0u);
        EXPECT_EQ(link.apply_errors(), 0u);

        EXPECT_EQ(link.Apply(Nest(3, Resp({"SET", "k", "v"}))), EnvelopeResult::kNotConsumed);
        EXPECT_EQ(link.unwrapped(), 0u);

        // The db of an envelope is a touch too, even when the envelope is then dropped or rejected:
        // the self-authored one and the one with a bad mvcc wait for the link, and the context has
        // not moved. One that fails before its db is taken touches nothing and is consumed.
        EXPECT_EQ(link.Apply(Envelope(kSelfUuid, Resp({"SET", "k", "v"}), "3")),
                  EnvelopeResult::kNotConsumed);
        EXPECT_EQ(link.Apply(Resp({"RREPLAY", kAuthorA, Resp({"SET", "k", "v"}), "3", "-5"})),
                  EnvelopeResult::kNotConsumed);
        EXPECT_EQ(link.cntx.conn_state.db_index, 0u);
        EXPECT_EQ(link.self_dropped(), 0u);
        EXPECT_EQ(link.malformed(), 0u);
        EXPECT_EQ(link.Apply(Resp({"RREPLAY", "not-a-uuid", Resp({"SET", "k", "v"}), "3", "7"})),
                  EnvelopeResult::kConsumed);
        EXPECT_EQ(link.malformed(), 1u);
        EXPECT_EQ(link.cntx.conn_state.db_index, 0u);

        // Only the outermost envelope asks: a layer applied from a depth below 1 does not.
        EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"SET", "k2", "v2"})), 2),
                  EnvelopeResult::kConsumed);
        EXPECT_EQ(link.unwrapped(), 1u);
      },
      [] { return false; });

  EXPECT_EQ(Get("k"), nullopt);
  EXPECT_EQ(Get("k", 3), nullopt);
  EXPECT_EQ(Get("k2"), "v2");
}

// The link stops while the first dispatch is in flight: the envelope's tree still completes, and is
// consumed. running() answers true once and false ever after, which is what a link cancelled during
// the first dispatch looks like to the applier; it is asked exactly once.
TEST_F(ClassicApplyFamilyTest, RunningFalseDuringFirstDispatchStillConsumesWholeEnvelope) {
  atomic_int running_calls = 0;
  OnLink(
      [&](Link& link) {
        // The first dispatch is the SELECT of db 3 and the command is the second.
        EXPECT_EQ(link.Apply(Nest(3, Resp({"SET", "k", "v"}), "3")), EnvelopeResult::kConsumed);
        EXPECT_EQ(link.unwrapped(), 3u);
        EXPECT_EQ(link.apply_errors(), 0u);
        EXPECT_EQ(running_calls.load(), 1);
      },
      [&running_calls] { return running_calls.fetch_add(1) == 0; });

  EXPECT_EQ(Get("k", 3), "v");
}

}  // namespace dfly
