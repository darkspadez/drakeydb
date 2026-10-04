// Copyright 2026, drakeydb authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "server/classic_replay.h"

#include <absl/cleanup/cleanup.h>
#include <absl/flags/flag.h>
#include <absl/strings/str_cat.h>

#include <initializer_list>
#include <limits>
#include <ostream>
#include <string>
#include <string_view>

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
    // Each carries db 3, which a dispatch would select. The names are matched without case: KeyDB's
    // cron PING is in lower case.
    for (const string& command : {Resp({"ping"}), Resp({"PING"}), Resp({"REPLCONF", "GETACK", "*"}),
                                  Resp({"MULTI"}), Resp({"Exec"}), Resp({"SELECT", "5"})}) {
      EXPECT_EQ(link.Apply(Envelope(kAuthorA, command, "3")), EnvelopeResult::kConsumed) << command;
    }
    EXPECT_EQ(link.cntx.conn_state.db_index, 0u);
    EXPECT_FALSE(link.cntx.conn_state.exec_info.IsCollecting());
    EXPECT_EQ(link.apply_errors(), 0u);  // an EXEC dispatched with no MULTI would be one
    EXPECT_EQ(link.unwrapped(), 6u);

    // Had the MULTI run, this would only be queued.
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"SET", "after", "v"}))),
              EnvelopeResult::kConsumed);
  });

  EXPECT_EQ(pings.load(), 0);
  EXPECT_EQ(replconfs.load(), 0);
  EXPECT_EQ(Get("after"), "v");
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

// The dispatcher rejects these before the command runs; they are consumed all the same.
TEST_F(ClassicApplyFamilyTest, RejectedDispatchCountsAndConsumes) {
  OnLink([&](Link& link) {
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"SET", "k"}))), EnvelopeResult::kConsumed);
    EXPECT_EQ(link.apply_errors(), 1u);
    // An unknown command is rejected the same way (a later task gives it a counter of its own).
    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"NOSUCHCMD", "x"}))), EnvelopeResult::kConsumed);
    EXPECT_EQ(link.apply_errors(), 2u);
    EXPECT_EQ(link.unwrapped(), 2u);
    EXPECT_EQ(link.malformed(), 0u);

    EXPECT_EQ(link.Apply(Envelope(kAuthorA, Resp({"SET", "k", "v"}))), EnvelopeResult::kConsumed);
    EXPECT_EQ(link.apply_errors(), 2u);
  });
  EXPECT_EQ(Get("k"), "v");
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
