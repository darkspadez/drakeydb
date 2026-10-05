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
#include "facade/reply_capture.h"
#include "server/command_registry.h"
#include "server/common.h"
#include "server/conn_context.h"
#include "server/engine_shard.h"
#include "server/engine_shard_set.h"
#include "server/generic_family.h"
#include "server/namespaces.h"
#include "server/test_utils.h"

ABSL_DECLARE_FLAG(int32_t, hz);
ABSL_DECLARE_FLAG(bool, replica_delete_expired);

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

// The process-wide flag Replica::Greet sets once an active KeyDB master has completed a handshake.
// Its state before this call is not asserted: it lives as long as the process, and another test, or
// a repeat, may have set it. There is no way to clear it, which is the point.
TEST(ClassicReplayTest, ActiveKeyDbMasterSeenSticksOnceNoted) {
  NoteActiveKeyDbMaster();
  EXPECT_TRUE(ActiveKeyDbMasterSeen());
  NoteActiveKeyDbMaster();
  EXPECT_TRUE(ActiveKeyDbMasterSeen());
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

  // Dispatches the command in `wire` on the context of `link` the way the raw path of
  // Replica::ConsumeRedisStream does. Returns the text of the error it replied, or nullopt if it
  // did not reply one.
  optional<string> DispatchRaw(Link& link, string_view wire) {
    facade::CapturingReplyBuilder rb{facade::ReplyMode::ONLY_ERR};
    Wire parsed = Wire::FromBytes(wire);
    CommandContext cmd;
    cmd.Init(&rb, &link.cntx);
    facade::FillBackedArgs(parsed.args, &cmd);
    service_->DispatchCommand(facade::ParsedArgs{cmd}, &cmd, facade::AsyncPreference::ONLY_SYNC);

    facade::CapturingReplyBuilder::Payload reply = rb.Take();
    auto error = facade::CapturingReplyBuilder::TryExtractError(reply);
    if (!error.has_value())
      return nullopt;
    return string(error->first);
  }

  // The number of keys with a WATCH registered, in any db of any shard.
  size_t WatchedKeyCount() {
    atomic_size_t count = 0;
    shard_set->RunBriefInParallel([&](EngineShard* shard) {
      for (const auto& db :
           namespaces->GetDefaultNamespace().GetDbSlice(shard->shard_id()).databases()) {
        if (db != nullptr)
          count += db->watched_keys.size();
      }
    });
    return count;
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

    // `PERSIST k m`, in db 0 where `k` is. What pins that it is dropped is the counters: it was
    // counted as dropped, and not as an apply error, which is what it would be if it were
    // dispatched (Dragonfly's PERSIST takes one argument). The TTL cannot tell the two apart:
    // dispatched, the arity error leaves it too, so it is not asserted for this command.
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"PERSIST", "k", "m"}))),
              EnvelopeResult::kConsumed);
    EXPECT_EQ(link.keydb_dropped(), ++dropped);

    EXPECT_EQ(link.unwrapped(), dropped);
    EXPECT_EQ(link.unknown_dropped(), 0u);  // not unknown: they are KeyDB's
    EXPECT_EQ(link.apply_errors(), 0u);     // and never dispatched
    EXPECT_EQ(link.malformed(), 0u);

    // `PERSIST k`, a standard command, is applied and clears the TTL of `k`.
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"PERSIST", "k"}))), EnvelopeResult::kConsumed);
    EXPECT_EQ(link.keydb_dropped(), dropped);
    EXPECT_EQ(link.apply_errors(), 0u);

    // The stream carries on.
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"SET", "after", "v"}))),
              EnvelopeResult::kConsumed);
    EXPECT_EQ(link.unwrapped(), dropped + 2);
  });

  EXPECT_THAT(service_->UknownCmdMap(), testing::IsEmpty());  // none went to the dispatcher
  EXPECT_EQ(CheckedInt({"ttl", "k"}), -1);                    // `PERSIST k` was not dropped
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

// A command that works on its own connection, dispatched the way the raw path of
// Replica::ConsumeRedisStream does, on a context with no connection. Each of them used to
// dereference a null Connection* (ISSUE-REGISTER U-15) and kill the process, so a case is a crash
// when the guard of its handler is gone: run one at a time (--gtest_filter) to tell which.
struct NoConnectionCase {
  const char* name;
  string wire;
  // QUIT has nothing to close and says OK; every other command replies "No connection".
  bool replies_error = true;
};

class ClassicNoConnectionTest : public ClassicApplyFamilyTest,
                                public testing::WithParamInterface<NoConnectionCase> {};

TEST_P(ClassicNoConnectionTest, ReplicatedApplyOfAConnectionCommandIsAnErrorNotACrash) {
  OnLink([&](Link& link) {
    optional<string> error = DispatchRaw(link, GetParam().wire);
    if (GetParam().replies_error) {
      ASSERT_TRUE(error.has_value());
      EXPECT_THAT(*error, testing::HasSubstr("No connection"));
    } else {
      EXPECT_FALSE(error.has_value());
    }
  });
}

INSTANTIATE_TEST_SUITE_P(
    U15, ClassicNoConnectionTest,
    testing::Values(
        NoConnectionCase{"Info", Resp({"INFO"})},
        NoConnectionCase{"InfoSection", Resp({"INFO", "commandstats"})},
        NoConnectionCase{"ClientSetName", Resp({"CLIENT", "SETNAME", "x"})},
        NoConnectionCase{"ClientGetName", Resp({"CLIENT", "GETNAME"})},
        NoConnectionCase{"ClientInfo", Resp({"CLIENT", "INFO"})},
        NoConnectionCase{"ClientId", Resp({"CLIENT", "ID"})},
        NoConnectionCase{"ClientKill", Resp({"CLIENT", "KILL", "ID", "99999"})},
        // The subcommands that never dereferenced the connection are refused by the one guard of
        // CLIENT too.
        NoConnectionCase{"ClientList", Resp({"CLIENT", "LIST"})},
        NoConnectionCase{"ClientPause", Resp({"CLIENT", "PAUSE", "1"})},
        NoConnectionCase{"ClientHelp", Resp({"CLIENT", "HELP"})},
        NoConnectionCase{"Auth", Resp({"AUTH", "secret"})},
        NoConnectionCase{"AuthUser", Resp({"AUTH", "user", "secret"})},
        NoConnectionCase{"Hello", Resp({"HELLO", "2"})},
        NoConnectionCase{"HelloSetName", Resp({"HELLO", "3", "SETNAME", "x"})},
        NoConnectionCase{"ReplconfListeningPort", Resp({"REPLCONF", "LISTENING-PORT", "6380"})},
        NoConnectionCase{"ReplconfCapaDragonfly", Resp({"REPLCONF", "CAPA", "dragonfly"})},
        // Real stock traffic: the guard sits at the top of the handler, so this one, which never
        // crashed, is refused too.
        NoConnectionCase{"ReplconfGetack", Resp({"REPLCONF", "GETACK", "*"})},
        NoConnectionCase{"Quit", Resp({"QUIT"}), false},
        NoConnectionCase{"Monitor", Resp({"MONITOR"})},
        NoConnectionCase{"Subscribe", Resp({"SUBSCRIBE", "c"})},
        NoConnectionCase{"Ssubscribe", Resp({"SSUBSCRIBE", "c"})},
        NoConnectionCase{"Psubscribe", Resp({"PSUBSCRIBE", "c*"})},
        NoConnectionCase{"Watch", Resp({"WATCH", "k"})},
        NoConnectionCase{"DflyThread", Resp({"DFLY", "THREAD", "1"})},
        // A hidden command (COMMAND does not list it). It names its connection before it looks up
        // the migration, and is registered whatever the cluster mode.
        NoConnectionCase{"DflymigrateFlow", Resp({"DFLYMIGRATE", "FLOW", "x", "0"})},
        // A script's redis.call runs the command on the context of the EVAL, a null one here
        // (CallFromScript). Its error is the script's error, which carries the handler's text.
        NoConnectionCase{"EvalInfo", Resp({"EVAL", "return redis.call('INFO')", "0"})},
        NoConnectionCase{"EvalHello", Resp({"EVAL", "return redis.call('HELLO', '3')", "0"})},
        NoConnectionCase{"EvalQuit", Resp({"EVAL", "return redis.call('QUIT')", "0"}), false}),
    [](const testing::TestParamInfo<NoConnectionCase>& info) { return string(info.param.name); });

// The commands that rewire this node's own replication link (ISSUE-REGISTER U-17). Streamed by a
// classic master they run on the replication fiber, and Replica::Stop of the link they replace
// joins that fiber (SIGABRT, `Check failed: active != this`); a raw REPLTAKEOVER parks the fiber
// on its own socket instead. The fixture's node is a master with no link, so what these cases pin
// is the reply (without the guard: OK, or the refusal of the command's own check, or a failed
// connect to port 1), and the abort itself is pinned by the scripted-master pytests.
INSTANTIATE_TEST_SUITE_P(
    U17, ClassicNoConnectionTest,
    testing::Values(
        NoConnectionCase{"ReplicaofNoOne", Resp({"REPLICAOF", "NO", "ONE"})},
        NoConnectionCase{"SlaveofNoOne", Resp({"SLAVEOF", "NO", "ONE"})},
        NoConnectionCase{"ReplicaofHost", Resp({"REPLICAOF", "127.0.0.1", "1"})},
        NoConnectionCase{"SlaveofHost", Resp({"SLAVEOF", "127.0.0.1", "1"})},
        // Only an active-replica node takes REMOVE; on this one it is a bad port: guarded first.
        NoConnectionCase{"ReplicaofRemove", Resp({"REPLICAOF", "REMOVE", "127.0.0.1", "1"})},
        NoConnectionCase{"Addreplicaof", Resp({"ADDREPLICAOF", "127.0.0.1", "1", "0", "16383"})},
        NoConnectionCase{"ReplTakeover", Resp({"REPLTAKEOVER", "0"})},
        NoConnectionCase{"ReplTakeoverSave", Resp({"REPLTAKEOVER", "1", "SAVE"})}),
    [](const testing::TestParamInfo<NoConnectionCase>& info) { return string(info.param.name); });

// The commands that take replicaof_mu_ (ISSUE-REGISTER U-19). A client's REPLICAOF NO ONE holds it
// across Replica::Stop, which joins the replication fiber, and streamed by a classic master these
// run on that fiber: parked on the mutex, the fiber never ends and neither does the command. The
// fixture's node is a master, so what these cases pin is the reply (without the guard: ROLE's
// array, OK, "I am master", OK), and the deadlock is pinned by the scripted-master pytests.
INSTANTIATE_TEST_SUITE_P(
    U19, ClassicNoConnectionTest,
    testing::Values(NoConnectionCase{"Role", Resp({"ROLE"})},
                    NoConnectionCase{"DebugReplicaPause", Resp({"DEBUG", "REPLICA", "PAUSE"})},
                    NoConnectionCase{"DebugReplicaResume", Resp({"DEBUG", "REPLICA", "RESUME"})},
                    NoConnectionCase{"DebugReplicaOffset", Resp({"DEBUG", "REPLICA", "OFFSET"})},
                    NoConnectionCase{"DebugReplDiag", Resp({"DEBUG", "REPLDIAG"})}),
    [](const testing::TestParamInfo<NoConnectionCase>& info) { return string(info.param.name); });

// DFLYMIGRATE is hidden and registered in every cluster mode, so a node without a cluster config
// answers it too (ISSUE-REGISTER U-21). `ACK` read the config through a null pointer, for a client
// and for a classic master's stream alike: run each of these alone to tell which one crashed.
TEST_F(ClassicApplyFamilyTest, DflymigrateAckWithoutAClusterConfigIsUnknownMigration) {
  EXPECT_EQ(Run({"dflymigrate", "ack", "x", "1"}), "UNKNOWN_MIGRATION");

  OnLink([&](Link& link) {
    // Not an error: the migration source tells UNKNOWN_MIGRATION apart from the errors.
    EXPECT_FALSE(DispatchRaw(link, Resp({"DFLYMIGRATE", "ACK", "x", "1"})).has_value());
  });
}

// `DFLYMIGRATE` alone is allowed by its arity. The handler read a subcommand that is not there and
// left the parser's error unchecked, which a debug build's parser destructor asserts on.
TEST_F(ClassicApplyFamilyTest, BareDflymigrateIsAnErrorNotAnAbort) {
  EXPECT_THAT(Run({"dflymigrate"}), ErrArg("syntax error"));

  OnLink([&](Link& link) { EXPECT_TRUE(DispatchRaw(link, Resp({"DFLYMIGRATE"})).has_value()); });
}

// DFLY FLOW names, migrates and keeps the connection it arrives on, once the replid and the id of a
// replica session in the preparation state match. `REPLCONF capa dragonfly` creates such a session,
// and the fixture's node is a master, which takes it. A replica refuses that, so a classic stream
// reaches this only on a peer-mode node or with --experimental_cascaded_partial_sync, and it has to
// know the replid (ISSUE-REGISTER U-15): run alone, the case is a crash without the guard.
TEST_F(ClassicApplyFamilyTest, DflyFlowOfALiveSessionIsAnErrorNotACrash) {
  RespExpr capa = Run({"REPLCONF", "capa", "dragonfly"});
  ASSERT_EQ(capa.type, RespExpr::ARRAY);
  const string replid{capa.GetVec()[0].GetView()};
  const string sync_id{capa.GetVec()[1].GetView()};

  OnLink([&](Link& link) {
    optional<string> error = DispatchRaw(link, Resp({"DFLY", "FLOW", replid, sync_id, "0"}));
    ASSERT_TRUE(error.has_value());
    EXPECT_THAT(*error, testing::HasSubstr("No connection"));
  });
}

// The emulated cluster node answers CLUSTER INFO|SLOTS|NODES|SHARDS with the address its client
// connected to (ClusterFamily::GetEmulatedShardInfo), so they read the connection of the context
// they run on. The other cluster modes answer from the config.
class ClassicEmulatedClusterTest : public ClassicApplyFamilyTest {
 protected:
  void SetUp() override {
    SetTestFlag("cluster_mode", "emulated");
    ClassicApplyFamilyTest::SetUp();
  }

 private:
  absl::FlagSaver saver_;  // puts cluster_mode back for the tests that follow
};

class ClassicNoConnectionEmulatedClusterTest
    : public ClassicEmulatedClusterTest,
      public testing::WithParamInterface<NoConnectionCase> {};

// Each case used to dereference a null Connection* (ISSUE-REGISTER U-15, found by the P7-1
// adversarial pass): run one at a time (--gtest_filter) to tell which, as for the cases above.
TEST_P(ClassicNoConnectionEmulatedClusterTest, ReplicatedClusterQueryIsAnErrorNotACrash) {
  OnLink([&](Link& link) {
    optional<string> error = DispatchRaw(link, GetParam().wire);
    ASSERT_TRUE(error.has_value());
    EXPECT_THAT(*error, testing::HasSubstr("No connection"));
  });
}

INSTANTIATE_TEST_SUITE_P(
    I3, ClassicNoConnectionEmulatedClusterTest,
    testing::Values(NoConnectionCase{"ClusterInfo", Resp({"CLUSTER", "INFO"})},
                    NoConnectionCase{"ClusterSlots", Resp({"CLUSTER", "SLOTS"})},
                    NoConnectionCase{"ClusterNodes", Resp({"CLUSTER", "NODES"})},
                    NoConnectionCase{"ClusterShards", Resp({"CLUSTER", "SHARDS"})},
                    NoConnectionCase{"EvalClusterInfo",
                                     Resp({"EVAL", "return redis.call('CLUSTER', 'INFO')", "0"})}),
    [](const testing::TestParamInfo<NoConnectionCase>& info) { return string(info.param.name); });

// The guard is for a context without a connection only: a client of the emulated node gets its
// answers, and the cluster commands that never read the connection answer on the stream's context.
TEST_F(ClassicEmulatedClusterTest, ClientsAndConnectionlessSubcommandsStillAnswer) {
  EXPECT_THAT(Run({"cluster", "info"}).GetString(), testing::HasSubstr("cluster_state:ok"));
  EXPECT_THAT(Run({"cluster", "nodes"}).GetString(), testing::HasSubstr("myself,master"));
  EXPECT_EQ(Run({"cluster", "slots"}).GetVec().size(), 1u);
  EXPECT_EQ(Run({"cluster", "shards"}).GetVec().size(), 1u);

  OnLink([&](Link& link) {
    EXPECT_FALSE(DispatchRaw(link, Resp({"CLUSTER", "MYID"})).has_value());
    EXPECT_FALSE(DispatchRaw(link, Resp({"CLUSTER", "KEYSLOT", "k"})).has_value());
    EXPECT_FALSE(DispatchRaw(link, Resp({"CLUSTER", "HELP"})).has_value());
    // An unknown subcommand is still the syntax error it was, not the guard's.
    optional<string> error = DispatchRaw(link, Resp({"CLUSTER", "NOSUCHSUB"}));
    ASSERT_TRUE(error.has_value());
    EXPECT_THAT(*error, testing::Not(testing::HasSubstr("No connection")));
  });
}

// What a replicated MONITOR or SUBSCRIBE would leave behind is worse than the failed command: a
// null connection in the monitor list, and a context that is gone in the channel store, which the
// next client's command or PUBLISH dereferences. Neither is registered now, so the clients go on.
TEST_F(ClassicApplyFamilyTest, ReplicatedMonitorAndSubscribeLeaveNothingForClientsToTripOver) {
  OnLink([&](Link& link) {
    const vector<string> commands = {Resp({"MONITOR"}), Resp({"SUBSCRIBE", "c"}),
                                     Resp({"PSUBSCRIBE", "c*"})};
    for (const string& command : commands)
      EXPECT_EQ(link.Apply(Envelope(kAuthorA, command)), EnvelopeResult::kConsumed) << command;
    EXPECT_EQ(link.unwrapped(), commands.size());
    EXPECT_EQ(link.apply_errors(), commands.size());  // each one failed, none applied

    // The stream's context is not a monitor, so what follows applies.
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"SET", "after", "v"}))),
              EnvelopeResult::kConsumed);
    EXPECT_EQ(link.apply_errors(), commands.size());
  });

  EXPECT_EQ(Get("after"), "v");
  EXPECT_EQ(Run({"set", "k", "v"}), "OK");  // would dereference a null monitor connection
  EXPECT_EQ(CheckedInt({"publish", "c", "m"}), 0);
  EXPECT_EQ(CheckedInt({"publish", "c1", "m"}), 0);  // nobody holds a subscription
}

// INFO inside an envelope, as the stream carries it: a command that did not apply, counted, and
// the commands around it applied.
TEST_F(ClassicApplyFamilyTest, InfoInAnEnvelopeIsAnApplyErrorNotACrash) {
  OnLink([&](Link& link) {
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"SET", "before", "v"}))),
              EnvelopeResult::kConsumed);
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"INFO"}))), EnvelopeResult::kConsumed);
    EXPECT_EQ(link.apply_errors(), 1u);
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"SET", "after", "v"}))),
              EnvelopeResult::kConsumed);
    EXPECT_EQ(link.apply_errors(), 1u);
    EXPECT_EQ(link.unwrapped(), 3u);
  });

  EXPECT_EQ(Get("before"), "v");
  EXPECT_EQ(Get("after"), "v");
}

// A script's redis.call runs on the context of its EVAL (CallFromScript), so the stream's EVAL of
// INFO or HELLO reaches the same handlers as the raw command. The EVAL is a command that did not
// apply, counted; a QUIT in a script has nothing to close and applies. The commands around apply.
TEST_F(ClassicApplyFamilyTest, EvalOfAConnectionCommandInAnEnvelopeIsAnApplyErrorNotACrash) {
  OnLink([&](Link& link) {
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"SET", "before", "v"}))),
              EnvelopeResult::kConsumed);
    for (const char* body : {"return redis.call('INFO')", "return redis.call('HELLO', '3')"})
      EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"EVAL", body, "0"}))),
                EnvelopeResult::kConsumed)
          << body;
    EXPECT_EQ(link.apply_errors(), 2u);
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"EVAL", "return redis.call('QUIT')", "0"}))),
              EnvelopeResult::kConsumed);
    EXPECT_EQ(link.apply_errors(), 2u);
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"SET", "after", "v"}))),
              EnvelopeResult::kConsumed);
    EXPECT_EQ(link.apply_errors(), 2u);
    EXPECT_EQ(link.unwrapped(), 5u);
  });

  EXPECT_EQ(Get("before"), "v");
  EXPECT_EQ(Get("after"), "v");
}

// drakeydb: P7-1 (decision 34) -- BY nosort on a set sorts it ALPHA inside a script (ISSUE-REGISTER
// D-34). A script a classic stream applies runs on a context that has no connection (conn() is
// null), so the test is whether SORT still sees it is under a script: its result, written with
// RPUSH, must be in the elements' order, not the set's own, raw and in an envelope alike.
TEST_F(ClassicApplyFamilyTest, SortOfASetUnderNosortInAScriptIsSortedWhenAppliedFromAStream) {
  Run({"sadd", "s", "j", "i", "h", "g", "f", "e", "d", "c", "b", "a"});
  const char* script =
      "local r = redis.call('SORT', KEYS[1], 'BY', 'nosort'); "
      "redis.call('RPUSH', KEYS[2], unpack(r)); return #r";

  OnLink([&](Link& link) {
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"EVAL", script, "2", "s", "in-envelope"}))),
              EnvelopeResult::kConsumed);
    EXPECT_EQ(DispatchRaw(link, Resp({"EVAL", script, "2", "s", "raw"})), nullopt);
    EXPECT_EQ(link.apply_errors(), 0u);
  });

  for (string_view dst : {"in-envelope", "raw"}) {
    SCOPED_TRACE(dst);
    EXPECT_THAT(Run({"lrange", dst, "0", "-1"}),
                RespElementsAre("a", "b", "c", "d", "e", "f", "g", "h", "i", "j"));
  }
}

// WATCH puts a pointer to the dirty flag of its connection in the shards and drops it only when the
// connection closes, or on UNWATCH, EXEC and RESET. The context of a replicated apply is a local of
// the stream loop and is gone when the link ends, so a WATCH it kept would have the next write to
// the key, or a FLUSHDB, store through a dangling pointer (ISSUE-REGISTER U-15). A debug build
// stores into the dead stack without a sound, so the test looks at the registration itself.
TEST_F(ClassicApplyFamilyTest, ReplicatedWatchLeavesNoRegistrationBehind) {
  // The count does see a registration.
  EXPECT_EQ(Run({"watch", "control"}), "OK");
  EXPECT_EQ(WatchedKeyCount(), 1u);
  EXPECT_EQ(Run({"unwatch"}), "OK");
  EXPECT_EQ(WatchedKeyCount(), 0u);

  OnLink([&](Link& link) {
    EXPECT_THAT(DispatchRaw(link, Resp({"WATCH", "k"})),
                testing::Optional(testing::HasSubstr("No connection")));
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"WATCH", "k", "k2"}))),
              EnvelopeResult::kConsumed);
    EXPECT_EQ(link.apply_errors(), 1u);
    EXPECT_EQ(WatchedKeyCount(), 0u);
  });

  // The link is gone: nothing is left for a write or a flush to store through.
  EXPECT_EQ(WatchedKeyCount(), 0u);
  EXPECT_EQ(Run({"set", "k", "v"}), "OK");
  EXPECT_EQ(Run({"flushdb"}), "OK");
}

// A plain replica of an active KeyDB expires keys itself, because that master propagates no DEL for
// them (spec D-9, decision 13). The shards' replica active expiry flag is what lets the heartbeat
// sweep run on a replica, and it opens the read-path gate of DbSlice::ExpireIfNeeded too; it never
// opens eviction. The fixture runs the heartbeats itself (EngineShard::Heartbeat is private to its
// friend), with the periodic one off, so that only they act, and sets the shards' state directly:
// SetReplica alone leaves ServerState::is_master true, so commands still run.
class ReplicaActiveExpiryTest : public BaseFamilyTest {
 protected:
  static constexpr unsigned kPlainKeys = 100;
  static constexpr unsigned kTtlKeys = 100;
  static constexpr int kHeartbeatRounds = 200;

  void SetUp() override {
    // saver_ puts back the flags these tests change; one found off its default here means an
    // earlier test leaked it.
    for (const char* name : {"hz", "replica_delete_expired"}) {
      absl::CommandLineFlag* flag = absl::FindCommandLineFlag(name);
      ASSERT_NE(flag, nullptr) << name;
      EXPECT_EQ(flag->CurrentValue(), flag->DefaultValue()) << name;
    }
    absl::SetFlag(&FLAGS_hz, 0);
    BaseFamilyTest::SetUp();
    shard_set->TEST_EnableCacheMode();
  }

  // kPlainKeys keys without a TTL and kTtlKeys that are due once time moves on, all with values
  // that are not inlined, so that heartbeat eviction has something to take.
  void Populate() {
    const string value(1000, '.');
    for (unsigned i = 0; i < kPlainKeys; ++i)
      ASSERT_EQ(Run({"set", absl::StrCat("plain:", i), value}), "OK");
    for (unsigned i = 0; i < kTtlKeys; ++i)
      ASSERT_EQ(Run({"set", absl::StrCat("ttl:", i), value, "PX", "100"}), "OK");
    ASSERT_EQ(DbSize(), kPlainKeys + kTtlKeys);
    AdvanceTime(1000);
  }

  void SetReplicaMode(bool replica, bool active_expiry) {
    shard_set->RunBriefInParallel([=](EngineShard* shard) {
      shard->SetReplica(replica);
      shard->SetReplicaActiveExpiry(active_expiry);
    });
  }

  // A usage far over the limit: a master's heartbeat evicts to get under it.
  void ApplyMemoryPressure() {
    max_memory_limit = 1000;
  }

  void RunHeartbeats() {
    shard_set->pool()->AwaitFiberOnAll([](unsigned, util::ProactorBase*) {
      if (EngineShard* shard = EngineShard::tlocal(); shard != nullptr)
        shard->Heartbeat();
    });
  }

  // DBSIZE counts entries and reads no key, so an expired key is not deleted by asking.
  int64_t DbSize() {
    return CheckedInt({"dbsize"});
  }

  // Runs heartbeats until the keys with a TTL are gone from the table, or gives up.
  void RunHeartbeatsUntilReaped() {
    for (int i = 0; i < kHeartbeatRounds && DbSize() != kPlainKeys; ++i)
      RunHeartbeats();
  }

  absl::FlagSaver saver_;
};

TEST_F(ReplicaActiveExpiryTest, ReapsExpiredKeysButNeverEvicts) {
  Populate();
  SetReplicaMode(/*replica=*/true, /*active_expiry=*/true);
  ApplyMemoryPressure();

  RunHeartbeatsUntilReaped();
  EXPECT_EQ(DbSize(), kPlainKeys);

  // The pressure stays on: keep running heartbeats, and nothing else leaves the table.
  for (int i = 0; i < 20; ++i)
    RunHeartbeats();
  EXPECT_EQ(DbSize(), kPlainKeys);
  EXPECT_EQ(GetMetrics().events.evicted_keys, 0u);
  EXPECT_EQ(CheckedInt({"exists", "plain:0", "plain:50", "plain:99"}), 3);
}

// The control of the one above: the same data and pressure on a shard that is not a replica does
// evict, so "never evicts" above is the replica's doing and not an inert setup.
TEST_F(ReplicaActiveExpiryTest, ControlAMasterHeartbeatUnderTheSamePressureEvicts) {
  Populate();
  ApplyMemoryPressure();

  RunHeartbeats();
  EXPECT_GT(GetMetrics().events.evicted_keys, 0u);
}

TEST_F(ReplicaActiveExpiryTest, ReapsNothingWithoutTheFlag) {
  Populate();
  SetReplicaMode(/*replica=*/true, /*active_expiry=*/false);

  for (int i = 0; i < 50; ++i)
    RunHeartbeats();
  EXPECT_EQ(DbSize(), kPlainKeys + kTtlKeys);
}

// --replica_delete_expired=false is how a replica is told never to delete an expired key; a replica
// that expires keys itself does, in the sweep and on the read path.
TEST_F(ReplicaActiveExpiryTest, BypassesReplicaDeleteExpiredFlag) {
  absl::SetFlag(&FLAGS_replica_delete_expired, false);
  Populate();
  SetReplicaMode(/*replica=*/true, /*active_expiry=*/true);

  // The read path: an expired key is deleted by the read that finds it.
  EXPECT_THAT(Run({"get", "ttl:0"}), ArgType(RespExpr::NIL));
  EXPECT_EQ(DbSize(), kPlainKeys + kTtlKeys - 1);

  RunHeartbeatsUntilReaped();
  EXPECT_EQ(DbSize(), kPlainKeys);
}

TEST_F(ReplicaActiveExpiryTest, ControlWithoutTheFlagAReplicaThatDeletesNothingServesExpiredKeys) {
  absl::SetFlag(&FLAGS_replica_delete_expired, false);
  Populate();
  SetReplicaMode(/*replica=*/true, /*active_expiry=*/false);

  EXPECT_EQ(Run({"get", "ttl:0"}), string(1000, '.'));  // served as live
  for (int i = 0; i < 50; ++i)
    RunHeartbeats();
  EXPECT_EQ(DbSize(), kPlainKeys + kTtlKeys);
}

// Owner decision 24, option A, pinned: the documented window. A replica that expires keys itself
// decides on the clock of the command it is running (Transaction::InitTxTime), so a TTL refresh the
// master ran before the deadline and the replica applies after it finds no key, and is a no-op:
// the key is lost on the replica while the master keeps it. Nothing runs a heartbeat here, so it
// is the access that deletes the key, as DBSIZE shows. Decision 27 (plan Task 2.8) gives
// enveloped commands the master's clock, which closes the window for them (its tests are the
// envelope ones); a command run as here, with no envelope, keeps the local clock and this test.
TEST_F(ReplicaActiveExpiryTest, ARefreshThatArrivesAfterTheDeadlineFindsNoKey) {
  SetReplicaMode(/*replica=*/true, /*active_expiry=*/true);
  ASSERT_EQ(Run({"set", "k", "v", "PX", "100"}), "OK");
  AdvanceTime(1000);
  ASSERT_EQ(DbSize(), 1);  // due, and still in the table: only an access deletes it

  EXPECT_EQ(CheckedInt({"persist", "k"}), 0);
  EXPECT_EQ(DbSize(), 0);
}

// The control of the one above: the same refresh before the deadline works on such a replica, so
// the 0 above is the deletion's doing and not a PERSIST that cannot work on a replica's shard.
TEST_F(ReplicaActiveExpiryTest, ControlARefreshBeforeTheDeadlineKeepsTheKey) {
  SetReplicaMode(/*replica=*/true, /*active_expiry=*/true);
  ASSERT_EQ(Run({"set", "k", "v", "PX", "100"}), "OK");
  EXPECT_EQ(CheckedInt({"persist", "k"}), 1);
  AdvanceTime(1000);
  EXPECT_EQ(Run({"get", "k"}), "v");
}

// What each class of command does on a replica that has deleted a due key, run after the deadline
// against a key that expired meanwhile (spec D-9, owner decisions 24 and 31). The master ran each
// of them before the deadline, saw the key, and replied (and holds) other than what is asserted
// here; an active KeyDB never streams the expiry's DEL, so nothing corrects the replica afterwards.
// The groups are the outcomes of the spec's table: a loss (the command finds no key; the master's
// key outlives the deadline), an orphan (the command creates a key with no TTL; the master's key
// keeps its deadline and is gone at it), a missing source (the movers and STORE commands, in the
// next test), and the commands that converge. A conditional SET .. NX is not a row: a failed one is
// not propagated by KeyDB (server.cpp:4624 gates propagation on the dataset having changed,
// t_string.cpp:104-109 returns before it does), so it never reaches a replica as it failed.
TEST_F(ReplicaActiveExpiryTest, EveryCommandClassOfTheWindowHasItsDocumentedOutcome) {
  SetReplicaMode(/*replica=*/true, /*active_expiry=*/true);

  const uint64_t far_ms = GetCurrentTimeMs() + 100000000;
  const string far_ms_str = absl::StrCat(far_ms);
  const string far_sec_str = absl::StrCat(far_ms / 1000);

  // One key per command, all with a 100 ms TTL: strings holding "5" ("5555" for SETRANGE, whose
  // reply is the value's length), and one of every container.
  auto expiring_string = [&](string_view key, string_view value = "5") {
    ASSERT_EQ(Run({"set", key, value, "PX", "100"}), "OK") << key;
  };
  auto expiring = [&](initializer_list<string_view> create, string_view key) {
    ASSERT_GT(CheckedInt(create), 0) << key;
    ASSERT_EQ(CheckedInt({"pexpire", key, "100"}), 1) << key;
  };
  for (string_view key :
       {"s:expire", "s:pexpire", "s:expireat", "s:pexpireat", "s:persist", "s:getex", "s:incr",
        "s:append", "s:xx", "s:xx_keepttl", "s:keepttl", "s:set", "s:del"}) {
    expiring_string(key);
  }
  expiring_string("s:setrange", "5555");
  expiring({"hset", "h", "f", "1"}, "h");
  expiring({"rpush", "l", "a"}, "l");
  expiring({"sadd", "st", "a"}, "st");
  AdvanceTime(1000);
  ASSERT_EQ(DbSize(), 17);  // all due, none deleted yet

  // Loss: TTL refreshes are no-ops, and the key is gone (for good after a PERSIST), while the
  // master's lives on with the TTL it was given, or none. So is a SET .. XX, which the master
  // applied to its live key: it replaced the value and dropped the TTL.
  EXPECT_EQ(CheckedInt({"expire", "s:expire", "60"}), 0);
  EXPECT_EQ(CheckedInt({"pexpire", "s:pexpire", "60000"}), 0);
  EXPECT_EQ(CheckedInt({"expireat", "s:expireat", far_sec_str}), 0);
  EXPECT_EQ(CheckedInt({"pexpireat", "s:pexpireat", far_ms_str}), 0);
  EXPECT_EQ(CheckedInt({"persist", "s:persist"}), 0);
  EXPECT_THAT(Run({"getex", "s:getex", "persist"}), ArgType(RespExpr::NIL));
  EXPECT_THAT(Run({"set", "s:xx", "new", "xx"}), ArgType(RespExpr::NIL));
  for (string_view key :
       {"s:expire", "s:pexpire", "s:expireat", "s:pexpireat", "s:persist", "s:getex", "s:xx"}) {
    EXPECT_EQ(CheckedInt({"exists", key}), 0) << key;
  }

  // Converges: SET .. XX KEEPTTL finds no key too, but the master's key keeps its deadline and is
  // gone at it, so the two end alike.
  EXPECT_THAT(Run({"set", "s:xx_keepttl", "new", "xx", "keepttl"}), ArgType(RespExpr::NIL));
  EXPECT_EQ(CheckedInt({"exists", "s:xx_keepttl"}), 0);

  // Orphan: a write that keeps the TTL computes from nothing and creates the key with none. The
  // master replied, and holds, 6 after INCR, "5x" after APPEND, "x555" (length 4) after SETRANGE,
  // a hash, a list and a set of two, and "new" after SET .. KEEPTTL; all of them with the deadline
  // the replica no longer has, so they are gone from the master at it and stay on the replica.
  EXPECT_EQ(CheckedInt({"incr", "s:incr"}), 1);
  EXPECT_EQ(CheckedInt({"append", "s:append", "x"}), 1);
  EXPECT_EQ(Run({"get", "s:append"}), "x");
  EXPECT_EQ(CheckedInt({"setrange", "s:setrange", "0", "x"}), 1);
  EXPECT_EQ(Run({"get", "s:setrange"}), "x");
  EXPECT_EQ(CheckedInt({"hset", "h", "g", "2"}), 1);
  EXPECT_EQ(CheckedInt({"hlen", "h"}), 1);
  EXPECT_EQ(CheckedInt({"lpush", "l", "b"}), 1);
  EXPECT_EQ(CheckedInt({"llen", "l"}), 1);
  EXPECT_EQ(CheckedInt({"sadd", "st", "b"}), 1);
  EXPECT_EQ(CheckedInt({"scard", "st"}), 1);
  EXPECT_EQ(Run({"set", "s:keepttl", "new", "keepttl"}), "OK");
  EXPECT_EQ(Run({"get", "s:keepttl"}), "new");
  for (string_view key : {"s:incr", "s:append", "s:setrange", "h", "l", "st", "s:keepttl"}) {
    EXPECT_EQ(CheckedInt({"pttl", key}), -1) << key << " was recreated with a TTL";
  }

  // Converges: a plain SET replaces the key, with no TTL on the master either; DEL removes it.
  EXPECT_EQ(Run({"set", "s:set", "new"}), "OK");
  EXPECT_EQ(Run({"get", "s:set"}), "new");
  EXPECT_EQ(CheckedInt({"pttl", "s:set"}), -1);
  EXPECT_EQ(CheckedInt({"del", "s:del"}), 0);
  EXPECT_EQ(CheckedInt({"exists", "s:del"}), 0);
}

// The movers and the STORE family, with the due key as their source (spec D-9, "source missing").
// The master moved or combined a live source, so the destination it holds is not what the replica
// computes from nothing: a mover or a STORE reads the source as missing, and what lands in the
// destination is lost. Where the destination already existed the replica leaves it as it was
// (RENAME) or empties it (SUNIONSTORE, whose result is empty), and the master's holds the source's
// data; with no destination, the replica has none. RENAME and COPY carry the source's deadline to
// the destination, so on the master it is gone at it all the same: a replica with no destination
// converges, and one that held a live destination keeps it for good (a stale key, not a lag). A
// moved element (LMOVE, SMOVE) or a computed result (STORE) has no deadline there, and the
// difference stays.
TEST_F(ReplicaActiveExpiryTest, MoversAndStoresSeeTheDueSourceAsMissing) {
  SetReplicaMode(/*replica=*/true, /*active_expiry=*/true);

  auto expiring = [&](initializer_list<string_view> create, string_view key) {
    ASSERT_GT(CheckedInt(create), 0) << key;
    ASSERT_EQ(CheckedInt({"pexpire", key, "100"}), 1) << key;
  };
  ASSERT_EQ(Run({"set", "s:rename", "5", "PX", "100"}), "OK");
  ASSERT_EQ(Run({"set", "s:copy", "5", "PX", "100"}), "OK");
  ASSERT_EQ(Run({"set", "s:rename_over", "5", "PX", "100"}), "OK");
  ASSERT_EQ(Run({"set", "s:old", "old"}), "OK");  // the live destination of RENAME
  expiring({"rpush", "lmove:src", "a"}, "lmove:src");
  expiring({"rpush", "lmove:over_src", "a"}, "lmove:over_src");
  ASSERT_EQ(CheckedInt({"rpush", "lmove:over_dst", "z"}), 1);
  expiring({"sadd", "smove:src", "a"}, "smove:src");
  expiring({"sadd", "union:src", "a"}, "union:src");
  expiring({"sadd", "union:over_src", "a"}, "union:over_src");
  ASSERT_EQ(CheckedInt({"sadd", "union:over_dst", "z"}), 1);
  AdvanceTime(1000);
  ASSERT_EQ(DbSize(), 11);  // eight due, three live, none deleted yet

  // With no destination: the source is missing, and none is created.
  EXPECT_THAT(Run({"rename", "s:rename", "s:renamed"}), ErrArg("no such key"));
  EXPECT_EQ(CheckedInt({"copy", "s:copy", "s:copied"}), 0);
  EXPECT_THAT(Run({"lmove", "lmove:src", "lmove:dst", "left", "left"}), ArgType(RespExpr::NIL));
  EXPECT_EQ(CheckedInt({"smove", "smove:src", "smove:dst", "a"}), 0);
  EXPECT_EQ(CheckedInt({"sunionstore", "union:dst", "union:src"}), 0);
  EXPECT_EQ(CheckedInt({"exists", "s:renamed", "s:copied", "lmove:dst", "smove:dst", "union:dst"}),
            0);

  // With a live destination: RENAME leaves it as it was, LMOVE does not push to it and
  // SUNIONSTORE of a missing source empties it. The master holds the source's data in all three.
  // Only RENAME's destination takes the source's deadline there (KeyDB `db.cpp:1507-1511`), so the
  // master's is gone at it while this one, with no TTL of its own, stays for good (COPY .. REPLACE
  // is the same, argued and not run).
  EXPECT_THAT(Run({"rename", "s:rename_over", "s:old"}), ErrArg("no such key"));
  EXPECT_EQ(Run({"get", "s:old"}), "old");
  EXPECT_THAT(Run({"lmove", "lmove:over_src", "lmove:over_dst", "left", "left"}),
              ArgType(RespExpr::NIL));
  EXPECT_EQ(CheckedInt({"llen", "lmove:over_dst"}), 1);
  EXPECT_EQ(CheckedInt({"sunionstore", "union:over_dst", "union:over_src"}), 0);
  EXPECT_EQ(CheckedInt({"exists", "union:over_dst"}), 0);
}

}  // namespace dfly
