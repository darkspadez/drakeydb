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

}  // namespace journal
}  // namespace dfly
