// Copyright 2026, drakeydb authors.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "facade/resp_expr.h"
#include "server/common_types.h"

namespace facade {
class CapturingReplyBuilder;
}  // namespace facade

namespace dfly {

class CommandContext;
class ConnectionContext;
class Service;

// What a classic (Redis-protocol) master said in reply to a `REPLCONF capa ...` command. A stock
// master answers `+OK`; an active KeyDB appends capability words to every capa reply
// (`+OK active-replica`, possibly followed by `keydb-fastsync-save`).
struct CapaReply {
  bool ok = false;
  bool active_replica = false;
  bool keydb_fastsync_save = false;
};

// Parses the simple string of a capa reply, i.e. "OK" or "OK <word> ...". `ok` is set iff the first
// token is exactly "OK" (case-sensitive, as before the suffix was tolerated); the words after it
// are separated by spaces and unknown ones are ignored.
CapaReply ParseCapaReply(std::string_view simple_string);

// An active KeyDB wraps every command it streams to a replica in
//   *5 RREPLAY <uuid of the writing node> <the command, RESP encoded> <db> <mvcc>
// (tests/dragonfly/data/README.md has real captures). A node that forwards what it replays wraps
// the envelope it received once more, so the inner command may itself be an envelope.
struct RreplayEnvelope {
  // The author, normalized to lowercase: it keys the author maps, and the normalized form is not a
  // view of the wire bytes.
  std::string uuid;
  // The wrapped command's RESP bytes. Views the buffer the envelope was parsed from, so it must
  // not outlive the stream loop iteration that read it.
  std::string_view inner;
  // Absent in KeyDB's 3-argument form: the inner command then runs in the db already selected.
  std::optional<DbIndex> db;
  // The author's MVCC clock when it minted the envelope; 0 when absent, which KeyDB never
  // deduplicates.
  uint64_t mvcc = 0;
};

enum class RreplayParse { kOk, kBadArity, kBadUuid, kBadDb, kBadMvcc };

// Validates `args` (the parsed `RREPLAY ...` command, name included) the way KeyDB's
// replicaReplayCommand does (replication.cpp:5389-5433) and fills `out` on kOk: at least three
// arguments (the command name, the uuid, the inner command, a string), a uuid KeyDB's uuid_parse
// takes (case-insensitive), an optional decimal db with 0 <= db < --dbnum, an optional decimal
// unsigned 64-bit mvcc. Arguments past the fifth are ignored, as KeyDB ignores them. The checks run
// in that order and the first failure names the result.
//
// `out` is unspecified after a failure, except `out->db` after kBadMvcc: KeyDB selects the db as it
// validates it (:5416), before it reads the mvcc (:5427), so that is the one failure of a layer
// that has moved its client's db.
RreplayParse ParseRreplayEnvelope(const facade::RespVec& args, RreplayEnvelope* out);

// Counters of one classic link. Relaxed atomics: the replication fiber bumps them, an INFO fiber
// on another thread reads them.
struct ClassicLinkStats {
  // Envelope layers (a forwarded envelope is two) that parsed, were not the node's own, and whose
  // inner command was taken apart: the layer's command was applied, skipped as a control command,
  // or rejected by the dispatcher.
  std::atomic<uint64_t> rreplay_unwrapped{0};
  // Envelope layers that were not well formed (RreplayParse), whose inner bytes were not exactly
  // one command, or that nested deeper than ClassicApplier::kMaxNesting. Skipped, never a
  // disconnect: the bytes cannot be recovered by reconnecting.
  std::atomic<uint64_t> rreplay_malformed{0};
  // Envelopes authored by this node, which came back around a mesh; dropped.
  std::atomic<uint64_t> rreplay_self_dropped{0};
  // Inner commands that did not apply: the dispatcher rejected them before they ran (unknown
  // command, wrong arity, out of memory) or they ran and replied an error (WRONGTYPE after a
  // divergence, ...).
  std::atomic<uint64_t> classic_apply_errors{0};
};

enum class EnvelopeResult {
  // The envelope is dealt with, whatever came of it: the stream offset advances past its bytes.
  kConsumed,
  // No command was dispatched and nothing was counted, the link is stopping: the offset must not
  // advance, so that a partial resync resumes at this envelope. The context's selected db may have
  // moved, and that is harmless: selecting is idempotent, and the resumed stream reads this
  // envelope again and its layers select the same dbs in the same order before anything is
  // dispatched. Today nothing has been selected either, as running() is asked first; the dedup
  // reservation of the leaf (P7-2) may cancel after a layer selected its db.
  kNotConsumed,
};

// Applies the RREPLAY envelopes of a classic link's stream, one command at a time. Socket-free:
// Replica::ConsumeRedisStream feeds it the parsed `RREPLAY ...` command and owns the offsets.
//
// Each envelope holds one command, which is dispatched on its own with its own db, instead of
// riding the raw stream's squashed batches (a batch has one apply context, and the envelope's db,
// and later its mvcc and author, belong to one command). Control commands KeyDB wraps like data
// (PING, REPLCONF GETACK, MULTI, EXEC) are skipped: the db travels in the envelope and a
// transaction is one envelope per command, which the server applies one by one as it does for a
// raw MULTI/EXEC. Nothing here is a disconnect or a CHECK: an envelope that cannot be applied is
// counted, warned about at a limited rate and skipped (owner decision 14).
//
// The context's selected db moves as KeyDB's master client's does. replicaReplayCommand selects the
// db of every layer as it validates it (replication.cpp:5416), before it looks at the author
// (:5435) or the inner command, and the commands of the stream run in the db that was last
// selected. So a layer that is well formed up to its db selects it, whatever follows: a
// self-authored layer, a control command, an inner that is not a command, and a bad mvcc included.
// A layer without a db (the 3-argument form) selects nothing, and a layer that fails earlier has
// not moved it. The one difference is a wrapped SELECT, which is skipped here and would move
// KeyDB's client; KeyDB never wraps one.
class ClassicApplier {
 public:
  // KeyDB's REPLAY_MAX_NESTING: the 64th wrapping applies, the 65th is malformed.
  static constexpr unsigned kMaxNesting = 64;

  // `cntx` is the stream's apply context: no connection, is_replicating, journal_emulated. Its
  // selected db is the one the envelopes move (see the class comment) and the raw commands of the
  // stream run in. `self_uuid` is this node's normalized uuid, whose envelopes are dropped.
  // `link` names the master in logs. `running` tells whether the link is still running.
  ClassicApplier(Service* service, ConnectionContext* cntx, std::string self_uuid, std::string link,
                 ClassicLinkStats* stats, std::function<bool()> running);
  ~ClassicApplier();

  ClassicApplier(const ClassicApplier&) = delete;
  ClassicApplier& operator=(const ClassicApplier&) = delete;

  // Whether a command of the stream is an envelope (a case-insensitive RREPLAY).
  static bool IsRreplay(const facade::RespExpr& name);

  // Applies the envelope `args` (the whole `RREPLAY ...` command) found at nesting level `depth`
  // (1: straight off the stream), and every envelope nested in it. `args`, and the buffer its
  // views point into, must stay valid for the call.
  //
  // `running()` is asked at most once, and only for the outermost envelope (depth 1): when its
  // layer is well formed up to its db (RreplayParse kOk or kBadMvcc), before anything is dispatched
  // or selected. A layer that fails before its db (arity, uuid, db) touches nothing and is
  // consumed without asking, and a call that starts deeper than 1 never asks. Once it answered
  // true the whole tree runs to completion: a cancelled link neither applies half an envelope nor
  // loses the end of one, and the offset it resumes from names an envelope that either applied
  // whole or did not start. Only a false answer yields kNotConsumed; everything else is consumed.
  EnvelopeResult HandleRreplay(const facade::RespVec& args, unsigned depth = 1);

 private:
  // Parses `bytes` as exactly one command. False for none, a partial one, bytes behind it, or a
  // name that is not a string: `*0\r\n`, `*-1\r\n` and `*1\r\n$-1\r\n` are valid RESP and parse
  // to an array, a nil array and a nil, none of which is a command to name or dispatch.
  static bool ParseSingleCommand(std::string_view bytes, facade::RespVec* args);

  // Runs the unwrapped command, `args[0]` of which is a string, in the db the context has
  // selected. Control commands are skipped.
  void ApplyCommand(const facade::RespVec& args);

  // Dispatches the already filled `cmd` and returns the text of its failure, if it failed: the
  // dispatcher rejected it, or it replied an error.
  std::optional<std::string> Dispatch(CommandContext* cmd);

  // Selects `db` for the context. The first use of a db dispatches a real SELECT (so its tables
  // exist on every shard, as JournalExecutor::SelectDb does); later uses only set the index. False
  // if that SELECT failed (counted): the selected db is then unchanged.
  bool SelectDb(DbIndex db);

  void NoteMalformed(std::string_view why, unsigned depth);
  void NoteApplyError(std::string_view command, std::string_view error);

  Service* service_;
  ConnectionContext* cntx_;
  std::string self_uuid_;
  std::string link_;
  ClassicLinkStats* stats_;
  std::function<bool()> running_;

  // The replies to the envelopes' commands: errors only. `ReplyMode::NONE` records nothing, and
  // the dispatcher consumes the builder's last error itself (InvokeCmd), so a command that failed
  // would be invisible after it returned.
  std::unique_ptr<facade::CapturingReplyBuilder> reply_;

  std::vector<bool> ensured_dbs_;
};

}  // namespace dfly
