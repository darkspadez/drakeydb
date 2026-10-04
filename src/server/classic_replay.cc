// Copyright 2026, drakeydb authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "server/classic_replay.h"

#include <absl/flags/flag.h>
#include <absl/strings/ascii.h>
#include <absl/strings/escaping.h>
#include <absl/strings/match.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>

#include <algorithm>
#include <array>
#include <iterator>
#include <limits>

#include "base/logging.h"
#include "facade/redis_parser.h"
#include "facade/reply_capture.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/generic_family.h"
#include "server/main_service.h"
#include "server/node_identity.h"

namespace dfly {

using namespace std;

CapaReply ParseCapaReply(string_view simple_string) {
  CapaReply reply;
  if (!absl::ConsumePrefix(&simple_string, "OK"))
    return reply;
  // "OKAY" or "OKactive-replica" is not an OK followed by words.
  if (!simple_string.empty() && simple_string.front() != ' ')
    return reply;

  reply.ok = true;
  for (string_view word : absl::StrSplit(simple_string, ' ', absl::SkipEmpty())) {
    if (word == "active-replica")
      reply.active_replica = true;
    else if (word == "keydb-fastsync-save")
      reply.keydb_fastsync_save = true;
  }
  return reply;
}

namespace {

// A non-negative decimal that fits 64 bits: digits only, so no sign, space or suffix (absl's
// SimpleAtoi would take all of them).
bool ParseUnsignedDecimal(string_view text, uint64_t* value) {
  if (text.empty() || !all_of(text.begin(), text.end(), absl::ascii_isdigit))
    return false;
  return absl::SimpleAtoi(text, value);
}

string_view RreplayParseName(RreplayParse parse) {
  switch (parse) {
    case RreplayParse::kOk:
      return "well formed";
    case RreplayParse::kBadArity:
      return "too few arguments, or an inner command that is not a string";
    case RreplayParse::kBadUuid:
      return "the author uuid is not a uuid";
    case RreplayParse::kBadDb:
      return "the db is not a number below --dbnum";
    case RreplayParse::kBadMvcc:
      return "the mvcc is not an unsigned number";
  }
  return "unknown";
}

// The counters of a classic link in the order INFO shows them (spec D-13), with what the two
// halves of their rendering need: where each lives in the atomics of a link and in a summary, and
// the help text of the /metrics series.
struct ClassicCounterDef {
  string_view name;
  string_view help;
  atomic<uint64_t> ClassicLinkStats::*stat;
  uint64_t ClassicLinkCounts::*count;
};

constexpr ClassicCounterDef kClassicCounters[] = {
    {"rreplay_unwrapped", "RREPLAY envelope layers taken apart on classic replication links.",
     &ClassicLinkStats::rreplay_unwrapped, &ClassicLinkCounts::rreplay_unwrapped},
    {"rreplay_malformed", "RREPLAY envelope layers skipped as malformed on classic links.",
     &ClassicLinkStats::rreplay_malformed, &ClassicLinkCounts::rreplay_malformed},
    {"rreplay_self_dropped",
     "RREPLAY envelopes authored by this node that came back around a mesh, dropped.",
     &ClassicLinkStats::rreplay_self_dropped, &ClassicLinkCounts::rreplay_self_dropped},
    {"keydb_cmds_dropped",
     "KeyDB-only commands (member expiry, KEYDB.CRON, ...) dropped on classic links.",
     &ClassicLinkStats::keydb_cmds_dropped, &ClassicLinkCounts::keydb_cmds_dropped},
    {"classic_unknown_cmds_dropped",
     "Commands inside RREPLAY envelopes this server has no command for, dropped.",
     &ClassicLinkStats::classic_unknown_cmds_dropped,
     &ClassicLinkCounts::classic_unknown_cmds_dropped},
    {"classic_apply_errors",
     "Commands inside RREPLAY envelopes that did not apply: rejected, or replied an error.",
     &ClassicLinkStats::classic_apply_errors, &ClassicLinkCounts::classic_apply_errors},
};

// The commands only a KeyDB has (spec D-7), besides `PERSIST key subkey`. KEYDB.MVCCRESTORE is not
// among them: it is applied (D-7a).
constexpr string_view kKeyDbOnlyCommands[] = {"PEXPIREMEMBERAT", "EXPIREMEMBER",  "EXPIREMEMBERAT",
                                              "KEYDB.CRON",      "KEYDB.HRENAME", "KEYDB.NHSET",
                                              "KEYDB.NHGET",     "KEYDB.MEXISTS", "RREPLAY"};

bool AnyCounterMoved(const ClassicLinkCounts& counts) {
  return any_of(begin(kClassicCounters), end(kClassicCounters),
                [&counts](const ClassicCounterDef& def) { return counts.*def.count != 0; });
}

// The counters of `counts` worth showing: all of them, or only the nonzero ones.
vector<ClassicCounterValue> CounterValues(const ClassicLinkCounts& counts, bool show_zeros) {
  vector<ClassicCounterValue> out;
  for (const ClassicCounterDef& def : kClassicCounters) {
    if (show_zeros || counts.*def.count != 0)
      out.push_back({def.name, def.help, counts.*def.count});
  }
  return out;
}

// What KeyDB wraps in an envelope like a write, but that is not data: its liveness PING, the
// GETACK its master asks for, and the MULTI/EXEC of a transaction. The db travels in the envelope,
// and the commands of a transaction are applied one by one, as for a raw MULTI/EXEC.
bool IsControlCommand(string_view name) {
  for (string_view control : {"MULTI", "EXEC", "PING", "REPLCONF", "SELECT"}) {
    if (absl::EqualsIgnoreCase(name, control))
      return true;
  }
  return false;
}

}  // namespace

RreplayParse ParseRreplayEnvelope(const facade::RespVec& args, RreplayEnvelope* out) {
  using facade::RespExpr;

  // args[0] is the command name: KeyDB's argc >= 3 is the name, the uuid and the inner command.
  if (args.size() < 3)
    return RreplayParse::kBadArity;
  if (args[1].type != RespExpr::STRING || !IsValidNodeUuid(args[1].GetView()))
    return RreplayParse::kBadUuid;
  // KeyDB's "Expected command buffer arg2". The enum has no value of its own for it, and a server
  // parser only yields strings, so only a hand-built vector gets here.
  if (args[2].type != RespExpr::STRING)
    return RreplayParse::kBadArity;

  std::optional<DbIndex> db;
  if (args.size() >= 4) {
    uint64_t value = 0;
    if (args[3].type != RespExpr::STRING || !ParseUnsignedDecimal(args[3].GetView(), &value) ||
        value >= absl::GetFlag(FLAGS_dbnum)) {
      return RreplayParse::kBadDb;
    }
    db = static_cast<DbIndex>(value);
  }

  uint64_t mvcc = 0;
  if (args.size() >= 5 &&
      (args[4].type != RespExpr::STRING || !ParseUnsignedDecimal(args[4].GetView(), &mvcc))) {
    // KeyDB has selected the db by now (replication.cpp:5416), so the caller needs it.
    out->db = db;
    return RreplayParse::kBadMvcc;
  }

  out->uuid = NormalizeNodeUuid(args[1].GetView());
  out->inner = args[2].GetView();
  out->db = db;
  out->mvcc = mvcc;
  return RreplayParse::kOk;
}

bool IsKeyDbOnlyCommand(const facade::RespVec& args) {
  if (args.empty() || args[0].type != facade::RespExpr::STRING)
    return false;

  // The raw path of the stream asks this of every command it queues: EqualsIgnoreCase compares
  // the lengths first, so the names of the stream's ordinary commands cost a few comparisons.
  const string_view name = args[0].GetView();
  for (string_view only : kKeyDbOnlyCommands) {
    if (absl::EqualsIgnoreCase(name, only))
      return true;
  }
  // `PERSIST key` is a standard command; KeyDB's `PERSIST key subkey` removes a member's TTL.
  return args.size() == 3 && absl::EqualsIgnoreCase(name, "PERSIST");
}

ClassicLinkCounts ClassicLinkStats::Snapshot() const {
  ClassicLinkCounts counts;
  for (const ClassicCounterDef& def : kClassicCounters)
    counts.*def.count = (this->*def.stat).load(memory_order_relaxed);
  return counts;
}

ClassicLinkStats& ClassicTotals() {
  static ClassicLinkStats totals;
  return totals;
}

bool ClassicLinkShown(const ReplicaSummary& link) {
  return link.classic_link && (link.master_active_replica || AnyCounterMoved(link.classic));
}

vector<ClassicCounterValue> ClassicLinkFields(const ReplicaSummary& link) {
  if (!ClassicLinkShown(link))
    return {};
  return CounterValues(link.classic, ClassicMasterActive(link));
}

vector<ClassicCounterValue> ClassicTotalSeries(const ClassicLinkCounts& totals,
                                               bool any_master_active) {
  return CounterValues(totals, any_master_active);
}

bool ClassicApplier::SkipKeyDbOnly(const facade::RespVec& args) {
  if (!IsKeyDbOnlyCommand(args))
    return false;

  Count(&ClassicLinkStats::keydb_cmds_dropped);
  LOG_EVERY_T(WARNING, 60) << "Dropping a KeyDB-only command from " << link_ << ": "
                           << absl::CHexEscape(args[0].GetView().substr(0, 32))
                           << " (KeyDB member TTLs and cron jobs have no equivalent here); the "
                              "drops are counted in keydb_cmds_dropped";
  return true;
}

void ClassicApplier::Count(atomic<uint64_t> ClassicLinkStats::*counter, uint64_t n) {
  (stats_->*counter).fetch_add(n, memory_order_relaxed);
  (ClassicTotals().*counter).fetch_add(n, memory_order_relaxed);
}

ClassicApplier::ClassicApplier(Service* service, ConnectionContext* cntx, string self_uuid,
                               string link, ClassicLinkStats* stats, function<bool()> running)
    : service_(service),
      cntx_(cntx),
      self_uuid_(std::move(self_uuid)),
      link_(std::move(link)),
      stats_(stats),
      running_(std::move(running)),
      reply_(make_unique<facade::CapturingReplyBuilder>(facade::ReplyMode::ONLY_ERR)) {
}

ClassicApplier::~ClassicApplier() = default;

bool ClassicApplier::IsRreplay(const facade::RespExpr& name) {
  return name.type == facade::RespExpr::STRING && absl::EqualsIgnoreCase(name.GetView(), "RREPLAY");
}

EnvelopeResult ClassicApplier::HandleRreplay(const facade::RespVec& args, unsigned depth) {
  DCHECK_GE(depth, 1u);

  // Nested envelopes are unwrapped in a loop, not by recursion: a fiber's stack is 40 KB in a
  // release build, and 64 levels of a frame holding a parser and an envelope could exceed it.
  // Each layer's views point at the bytes of the stream buffer, not into the vector they were
  // parsed into, so `inner_args` can be reused for the next layer.
  facade::RespVec inner_args;
  const facade::RespVec* current = &args;
  // False once the db of a layer could not be selected, until a deeper layer selects one: the
  // command then has no db to run in and is skipped.
  bool db_selected = true;
  uint64_t layers = 0;

  for (;; ++depth) {
    // KeyDB's order (replicaReplayCommand): validate the layer, select its db, look at its author,
    // count its nesting. Its mvcc is validated after the db is selected, so a bad one leaves the db
    // selected.
    RreplayEnvelope env;
    const RreplayParse parse = ParseRreplayEnvelope(*current, &env);

    if (parse == RreplayParse::kOk || parse == RreplayParse::kBadMvcc) {
      // The point of no return: from the first dispatch or select of the outermost envelope on, the
      // whole tree is applied, whatever happens to the link. Nothing above has touched the context.
      if (depth == 1 && !running_())
        return EnvelopeResult::kNotConsumed;

      if (env.db)
        db_selected = SelectDb(*env.db);
    }

    if (parse != RreplayParse::kOk) {
      NoteMalformed(RreplayParseName(parse), depth);
      break;
    }

    if (env.uuid == self_uuid_) {
      Count(&ClassicLinkStats::rreplay_self_dropped);
      break;
    }

    if (depth > kMaxNesting) {
      NoteMalformed(absl::StrCat("nested deeper than ", kMaxNesting, " envelopes"), depth);
      break;
    }

    if (!ParseSingleCommand(env.inner, &inner_args)) {
      NoteMalformed("the inner command is not exactly one command", depth);
      break;
    }
    ++layers;

    if (!IsRreplay(inner_args[0])) {
      if (db_selected)
        ApplyCommand(inner_args);
      break;
    }
    current = &inner_args;
  }

  Count(&ClassicLinkStats::rreplay_unwrapped, layers);
  return EnvelopeResult::kConsumed;
}

bool ClassicApplier::ParseSingleCommand(string_view bytes, facade::RespVec* args) {
  if (bytes.empty())
    return false;

  // No array or string in the bytes can be longer than the bytes.
  const uint32_t limit =
      static_cast<uint32_t>(min<size_t>(bytes.size(), numeric_limits<uint32_t>::max()));
  facade::RedisParser parser(facade::RedisParser::Mode::SERVER, limit, limit);
  uint32_t consumed = 0;
  facade::RedisParser::Result result = parser.Parse(
      facade::RedisParser::Buffer{reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size()},
      &consumed, args);
  if (result != facade::RedisParser::OK || consumed != bytes.size() || args->empty())
    return false;
  // `*0`, `*-1` and `*1 $-1` are one RESP value each, and their first element is an array or a nil,
  // which has no name to read.
  return args->front().type == facade::RespExpr::STRING;
}

void ClassicApplier::ApplyCommand(const facade::RespVec& args) {
  const string_view name = args[0].GetView();
  if (IsControlCommand(name) || SkipKeyDbOnly(args))
    return;

  CommandContext cmd;
  cmd.Init(reply_.get(), cntx_);
  facade::FillBackedArgs(args, &cmd);
  // The dispatcher would reject a command it has no entry for as an unknown one, into the error
  // builder and upstream's `unknown_` accounting. Inside an envelope it is a command of a KeyDB
  // (KEYDB.MVCCRESTORE until P7-2 applies it, or one a newer KeyDB added), so it is counted on its
  // own. The lookup is the dispatcher's, which reads `ACL <sub>` as one name.
  if (service_->mutable_registry()->FindExtended(facade::ParsedArgs{cmd}).first == nullptr) {
    Count(&ClassicLinkStats::classic_unknown_cmds_dropped);
    LOG_EVERY_T(WARNING, 60) << "Dropping a command of an RREPLAY envelope from " << link_
                             << " that this server has no command for: "
                             << absl::CHexEscape(name.substr(0, 32))
                             << "; the drops are counted in classic_unknown_cmds_dropped";
    return;
  }
  if (optional<string> error = Dispatch(&cmd); error)
    NoteApplyError(name, *error);
}

optional<string> ClassicApplier::Dispatch(CommandContext* cmd) {
  facade::DispatchResult result =
      service_->DispatchCommand(facade::ParsedArgs{*cmd}, cmd, facade::AsyncPreference::ONLY_SYNC);
  facade::CapturingReplyBuilder::Payload reply = reply_->Take();
  if (auto error = facade::CapturingReplyBuilder::TryExtractError(reply); error)
    return string(error->first);
  if (result != facade::DispatchResult::OK)
    return "the command was not dispatched";
  return nullopt;
}

bool ClassicApplier::SelectDb(DbIndex db) {
  if (ensured_dbs_.size() <= db)
    ensured_dbs_.resize(db + 1);

  if (ensured_dbs_[db]) {
    cntx_->conn_state.db_index = db;
    return true;
  }

  CommandContext select;
  select.Init(reply_.get(), cntx_);
  const string index = absl::StrCat(db);
  const array<string_view, 2> parts = {"SELECT", index};
  select.Assign(parts.begin(), parts.end(), parts.size());
  if (optional<string> error = Dispatch(&select); error) {
    NoteApplyError("SELECT", *error);
    return false;
  }
  ensured_dbs_[db] = true;
  return true;
}

void ClassicApplier::NoteMalformed(string_view why, unsigned depth) {
  Count(&ClassicLinkStats::rreplay_malformed);
  LOG_EVERY_T(WARNING, 60) << "Skipping a malformed RREPLAY envelope from " << link_
                           << " (nesting level " << depth << "): " << why;
}

void ClassicApplier::NoteApplyError(string_view command, string_view error) {
  Count(&ClassicLinkStats::classic_apply_errors);
  LOG_EVERY_T(WARNING, 10) << "A command of an RREPLAY envelope from " << link_
                           << " did not apply and is skipped: "
                           << absl::CHexEscape(command.substr(0, 32)) << ": " << error;
}

}  // namespace dfly
