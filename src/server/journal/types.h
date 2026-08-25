// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <variant>

#include "common/backed_args.h"
#include "server/common_types.h"
#include "server/table.h"

namespace dfly {
namespace journal {

enum class Op : uint8_t {
  SELECT = 6,
  EXPIRED = 9 /* sunset*/,
  COMMAND = 10,
  PING = 13,
  LSN = 15,
  ORIGIN = 16
};

// drakeydb: entry_flags bit for an expiry-triggered deletion. Op::EXPIRED (9, above) is sunset
// on the write side; this bit is its Phase 3 replacement carrier.
inline constexpr uint8_t kEntryFlagExpired = 1 << 0;

// drakeydb: P4-0 -- a DEL derived from a collection becoming empty, rather than issued
// directly. Never forwarded to a peer: a command-caused empty propagates via the causing
// command (the peer derives its own DEL), and an expiry-caused empty is the peer's own
// clock's business. Distinct from kEntryFlagExpired so diagnostics can tell them apart.
constexpr uint8_t kEntryFlagDerived = 1 << 1;

struct EntryBase {
  TxId txid;
  Op opcode;
  DbIndex dbid;
  std::optional<SlotId> slot;
  LSN lsn{0};

  // drakeydb: Phase 3 origin metadata. All defaulted so non-active nodes stay byte-identical to
  // upstream, and placed after `lsn` so the aggregate-init constructors below keep compiling.
  uint32_t origin_idx{0};  // PeerRegistry index of this entry's author; kSelfIdx (0) == self.
  uint64_t mvcc{0};        // Reserved for future conflict resolution; not yet threaded further.
  uint8_t entry_flags{0};  // Bitmask; bit0 == kEntryFlagExpired.
};

// This struct represents a single journal entry.
// Those are either control instructions or commands.
struct Entry : public EntryBase {
  // Payload represents a non-owning view into a command executed on the shard.
  struct Payload {
    std::string_view cmd;
    std::variant<ShardArgs,           // Shard parts.
                 ArgSlice,            // Parts of a full command.
                 facade::ParsedArgs>  // Full command backed by a span or BackedArguments.
        args;

    Payload() = default;

    Payload(std::string_view c, const ShardArgs& a) : cmd(c), args(a) {
    }
    Payload(std::string_view c, ArgSlice a) : cmd(c), args(a) {
    }
    Payload(std::string_view c, const facade::ParsedArgs& a) : cmd(c), args(a) {
    }
  };

  Entry(TxId txid, Op opcode, DbIndex dbid, std::optional<SlotId> slot_id, Payload pl)
      : EntryBase{txid, opcode, dbid, slot_id}, payload{std::move(pl)} {
  }

  Entry(journal::Op opcode, DbIndex dbid, std::optional<SlotId> slot_id)
      : EntryBase{0, opcode, dbid, slot_id, 0} {
  }

  Entry(journal::Op opcode, LSN lsn) : EntryBase{0, opcode, 0, std::nullopt, lsn} {
  }

  Entry(TxId txid, journal::Op opcode, DbIndex dbid, std::optional<SlotId> slot_id)
      : EntryBase{txid, opcode, dbid, slot_id, 0} {
  }

  bool HasPayload() const {
    return !payload.cmd.empty();
  }

  std::string ToString() const;

  Payload payload;
};

struct ParsedEntry : public EntryBase {
  using CmdData = cmn::BackedArguments;
  CmdData cmd;

  // drakeydb: Phase 3 uuid carrier for Op::ORIGIN entries only (populated by
  // JournalReader::ReadEntry, see serializer.cc). Deliberately NOT folded into `cmd`: a
  // dispatcher that keys off whether `cmd` is populated (rdb_load.cc's HandleJournalBlob,
  // replica.cc's StableSyncDflyReadFb) must not be able to mistake an origin uuid for a command
  // name. Callers must still switch on `opcode` regardless -- this field is only meaningful when
  // opcode == Op::ORIGIN.
  std::string origin_uuid;

  ParsedEntry(const ParsedEntry&) = delete;
  ParsedEntry() = default;

  std::string ToString() const;
};

struct JournalItem {
  LSN lsn;
  uint64_t time_ms = 0;
  std::string data;

  // drakeydb: Phase 3 origin metadata, mirrored from EntryBase by JournalSlice::AddLogRecord.
  // Lives here (not only on JournalChangeItem) because the ring buffer stores JournalItem; this
  // is what lets a later peer-echo filter read origin/flags without reparsing `data` -- see
  // JournalSlice::GetEntryMeta().
  uint32_t origin_idx{0};
  uint8_t entry_flags{0};

  // drakeydb: Phase 3 T5 -- mirrors EntryBase::opcode, exactly as origin_idx/entry_flags above,
  // so JournalStreamer::ShouldWrite (the peer-echo filter) can drop Op::ORIGIN entries by opcode
  // without reparsing `data`. Reparsing isn't viable: JournalWriter::Write emits a nested SELECT
  // entry *inside* a COMMAND blob's bytes (see serializer.cc), so data[0] is frequently
  // Op::SELECT rather than the entry's own opcode. Defaults to Op::COMMAND (the common case);
  // JournalSlice::AddLogRecord always overwrites this from the real Entry being recorded, so the
  // default is only ever observed for a JournalItem that was never populated via AddLogRecord.
  Op opcode{Op::COMMAND};
};

struct JournalChangeItem {
  JournalItem journal_item;

  std::string_view cmd;
  std::optional<SlotId> slot;
};

struct JournalConsumerInterface {
  virtual ~JournalConsumerInterface() = default;

  // Receives a journal change for serializing
  virtual void ConsumeJournalChange(const JournalChangeItem& item) = 0;
  // Waits for writing the serialized data
  virtual void ThrottleIfNeeded() = 0;
};

// drakeydb: Phase 3 T7b -- the ONE definition of the peer-echo-prevention rule: drop an entry
// not authored by this node itself (origin_idx != PeerRegistry::kSelfIdx), drop an
// expiry-triggered deletion (every node expires independently on its own clock, so peers must
// not receive and re-apply each other's expiry deletions -- see kEntryFlagExpired above), and
// drop an Op::ORIGIN dictionary entry (a mesh peer discovers every other node directly and does
// not need these relayed; also, ORIGIN entries are always recorded self-origin by
// PeerRegistry::AddOrGet, so the origin_idx check above cannot be relied on to catch them -- see
// multi_master.cc's AddOrGet). Op::LSN and Op::PING are deliberately NOT dropped by opcode: LSN
// bookkeeping and ack/partial-resume accounting on the receiving side depend on both reaching the
// consumer (see TransactionReader::NextTxData's DCHECK_EQ/adoption logic and
// DflyShardReplica::journal_rec_executed_); a peer's re-recorded PING carries that peer's origin
// (T6), so the origin_idx check above -- not an opcode check -- is what suppresses it.
//
// Two consumers apply this identical rule to two different windows of the same replication
// stream, and must never diverge: JournalStreamer::ShouldWrite (streamer.cc) for the STABLE-SYNC
// stream, and SliceSnapshot::ConsumeJournalChange (snapshot.cc) for the FULL-SYNC window's
// concurrent journal blob. Defined out-of-line in types.cc (not here) because it needs
// PeerRegistry::kSelfIdx from multi_master.h, which must NOT be pulled into this header --
// journal/types.h is one of the most widely-included headers in the tree, and multi_master.h
// drags in flat_hash_map/nonstd::expected/replica_types.h/fiber sync for one constant (see the
// same discipline already applied to ConnectionContext::repl_origin_idx, conn_context.h).
bool PassesPeerEchoFilter(const JournalItem& item);

}  // namespace journal
}  // namespace dfly
