// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/serializer.h"

#include <system_error>

#include "base/logging.h"
#include "io/io.h"
#include "io/io_buf.h"
#include "server/error.h"
#include "server/journal/types.h"
#include "server/main_service.h"
#include "server/serializer_commons.h"
#include "server/transaction.h"

using namespace std;

namespace dfly {

namespace {
// drakeydb: generous bound on Op::ORIGIN's uuid length. A real node uuid is 36 chars
// (node_identity.h's RFC-4122 v4 form); this cap exists only to stop a corrupt or hostile frame
// from forcing a huge allocation (or an uncaught std::length_error) in the reader fiber before
// any content validation happens.
constexpr uint64_t kMaxOriginUuidLen = 128;
}  // namespace

JournalWriter::JournalWriter(io::Sink* sink, bool extended_framing)
    : sink_{sink}, extended_framing_{extended_framing} {
}

void JournalWriter::Write(uint64_t v) {
  uint8_t buf[10];
  unsigned len = WritePackedUInt(v, buf);
  sink_->Write(io::Bytes{buf}.first(len));
}

void JournalWriter::Write(std::string_view sv) {
  Write(sv.size());
  if (!sv.empty())  // arguments can be empty strings
    sink_->Write(io::Buffer(sv));
}

void JournalWriter::Write(const journal::Entry::Payload& payload) {
  if (payload.cmd.empty())
    return;

  size_t num_elems = 0, size = 0;
  for (string_view str : base::it::Wrap(cmn::kToSV, payload.args)) {
    num_elems++;
    size += str.size();
  };

  Write(1 + num_elems);

  size_t cmd_size = payload.cmd.size() + size;
  Write(cmd_size);
  Write(payload.cmd);

  for (string_view str : base::it::Wrap(cmn::kToSV, payload.args))
    this->Write(str);
}

void JournalWriter::Write(const journal::Entry& entry) {
  // Check if entry has a new db index and we need to emit a SELECT entry.
  // drakeydb: Op::ORIGIN carries no dbid meaning -- excluded here like SELECT/LSN/PING so it
  // neither triggers a spurious nested SELECT nor mutates cur_dbid_.
  if (entry.opcode != journal::Op::SELECT && entry.opcode != journal::Op::LSN &&
      entry.opcode != journal::Op::PING && entry.opcode != journal::Op::ORIGIN &&
      (!cur_dbid_ || entry.dbid != *cur_dbid_)) {
    Write(journal::Entry{journal::Op::SELECT, entry.dbid, entry.slot});
    cur_dbid_ = entry.dbid;
  }

  VLOG(1) << "Writing entry " << entry.ToString();

  Write(uint8_t(entry.opcode));

  switch (entry.opcode) {
    case journal::Op::SELECT:
      return Write(entry.dbid);
    case journal::Op::LSN:
      return Write(entry.lsn);
    case journal::Op::PING:
      return;
    case journal::Op::COMMAND:
      Write(entry.txid);
      if (!extended_framing_) {
        Write(1u);  // deprecated field, kept for backward compatibility.
      } else {
        // drakeydb: Phase 3 journal framing v2. `mvcc` is written as 0 until P4 fills it in --
        // packed-uint encoding is self-describing, so widening it later needs no further framing
        // change (see JournalWriter's ctor comment).
        Write(2u);
        Write(entry.origin_idx);
        Write(entry.mvcc);
        Write(entry.entry_flags);
      }
      Write(entry.payload);
      break;
    case journal::Op::ORIGIN:
      // drakeydb: Op::ORIGIN is v2-only. Nothing constructs one with extended_framing_ == false
      // today, but a future non-active emitter putting v2-only bytes into an otherwise
      // upstream-compatible stream would silently break byte-identity, so guard it explicitly.
      DCHECK(extended_framing_) << "Op::ORIGIN written without extended_framing";
      // idx + uuid only, no txid and no Entry::Payload framing (the uuid rides in
      // entry.payload.cmd as a plain string, but is written directly via the string primitive
      // below, not through the args-array Write(Payload&) path).
      Write(entry.origin_idx);
      Write(entry.payload.cmd);
      break;
    default:
      LOG(FATAL) << "Unknown journal opcode: " << static_cast<int>(entry.opcode);
      break;
  };
}

JournalReader::JournalReader(io::Source* source, DbIndex dbid)
    : source_{source}, buf_{4096}, dbid_{dbid} {
}

void JournalReader::SetSource(io::Source* source) {
  CHECK_EQ(buf_.InputLen(), 0ULL);
  source_ = source;
}

std::error_code JournalReader::EnsureRead(size_t num) {
  // Check if we already have enough.
  if (buf_.InputLen() >= num)
    return {};

  uint64_t remainder = num - buf_.InputLen();
  buf_.EnsureCapacity(remainder);

  // Try reading at least how much we need, but possibly more
  uint64_t read;
  SET_OR_RETURN(source_->ReadAtLeast(buf_.AppendBuffer(), remainder), read);

  // Happens on end of stream (for example, a too-small string buffer or a closed socket)
  if (read < remainder) {
    return make_error_code(errc::io_error);
  }

  buf_.CommitWrite(read);
  return {};
}

template <typename UT> io::Result<UT> JournalReader::ReadUInt() {
  // Determine type and number of following bytes.
  if (auto ec = EnsureRead(1); ec)
    return make_unexpected(ec);
  PackedUIntMeta meta{buf_.InputBuffer()[0]};
  buf_.ConsumeInput(1);

  if (auto ec = EnsureRead(meta.ByteSize()); ec)
    return make_unexpected(ec);

  // Read and check intenger.
  uint64_t res;
  SET_OR_UNEXPECT(ReadPackedUInt(meta, buf_.InputBuffer()), res);
  buf_.ConsumeInput(meta.ByteSize());

  if (res > std::numeric_limits<UT>::max())
    return make_unexpected(make_error_code(errc::result_out_of_range));
  return static_cast<UT>(res);
}

template io::Result<uint8_t> JournalReader::ReadUInt<uint8_t>();
template io::Result<uint16_t> JournalReader::ReadUInt<uint16_t>();
template io::Result<uint32_t> JournalReader::ReadUInt<uint32_t>();
template io::Result<uint64_t> JournalReader::ReadUInt<uint64_t>();

std::error_code JournalReader::ReadString(io::MutableBytes buffer) {
  size_t size = buffer.size();
  uint64_t available = std::min(size, buf_.InputLen());
  uint64_t remainder = 0;

  if (available < size) {
    remainder = size - available;
  }

  buf_.ReadAndConsume(available, buffer.data());

  // If remainder of string is bigger than threshold - read and populate directly
  // output buffer otherwise use intermediate io_buf.
  bool is_short_remainder = remainder < (buf_.Capacity() / 2);

  auto remainder_buf_pos = buffer.data() + available;

  if (remainder) {
    if (is_short_remainder) {
      if (auto ec = EnsureRead(remainder); ec)
        return ec;
      buf_.ReadAndConsume(remainder, remainder_buf_pos);
    } else {
      uint64_t read;
      SET_OR_RETURN(source_->Read({remainder_buf_pos, remainder}), read);
      if (read < remainder) {
        return make_error_code(errc::io_error);
      }
    }
  }

  return {};
}

std::error_code JournalReader::ReadCommand(journal::ParsedEntry::CmdData* data) {
  size_t num_strings = 0;
  SET_OR_RETURN(ReadUInt<uint64_t>(), num_strings);

  size_t cmd_size = 0;
  SET_OR_RETURN(ReadUInt<uint64_t>(), cmd_size);

  data->Reserve(num_strings, cmd_size + num_strings /* +\0 char*/);

  // Read all strings consecutively.
  for (size_t i = 0; i < num_strings; ++i) {
    size_t size = 0;
    SET_OR_RETURN(ReadUInt<uint64_t>(), size);
    if (size > cmd_size) {  // corrupted entry
      return make_error_code(errc::io_error);
    }
    data->PushArg(size);
    uint8_t* ptr = reinterpret_cast<uint8_t*>(data->data(i));
    if (auto ec = ReadString({ptr, size}); ec)
      return ec;

    ptr[size] = '\0';  // null terminate

    cmd_size -= size;
  }

  return {};
}

std::error_code JournalReader::ReadEntry(journal::ParsedEntry* dest) {
  uint8_t int_op;
  SET_OR_RETURN(ReadUInt<uint8_t>(), int_op);
  journal::Op opcode = static_cast<journal::Op>(int_op);

  if (opcode == journal::Op::SELECT) {
    SET_OR_RETURN(ReadUInt<uint16_t>(), dbid_);
    return ReadEntry(dest);
  }

  dest->dbid = dbid_;
  dest->opcode = opcode;
  dest->cmd.clear();
  // drakeydb: reset Phase 3 fields on every entry so a reused ParsedEntry never leaks a
  // previous v2 entry's origin metadata onto one that doesn't carry any (legacy-framed COMMAND,
  // PING, LSN, or ORIGIN itself, which only ever sets origin_idx/origin_uuid below).
  dest->origin_idx = 0;
  dest->mvcc = 0;
  dest->entry_flags = 0;
  dest->origin_uuid.clear();

  if (opcode == journal::Op::PING) {
    return {};
  }

  if (opcode == journal::Op::LSN) {
    SET_OR_RETURN(ReadUInt<uint64_t>(), dest->lsn);
    return {};
  }

  if (opcode == journal::Op::ORIGIN) {
    // drakeydb: origin announcement -- idx + uuid, symmetric with the writer's payload-free
    // branch. The uuid lands in its own field (ParsedEntry::origin_uuid, see types.h for why),
    // leaving `cmd` empty -- but callers must still dispatch on `opcode`, not on whether
    // `cmd`/`origin_uuid` is populated. See TransactionData::AddEntry's dedicated case, and the
    // explicit ORIGIN guards in rdb_load.cc/replica.cc, none of which treat this as a command.
    SET_OR_RETURN(ReadUInt<uint32_t>(), dest->origin_idx);
    uint64_t uuid_len = 0;
    SET_OR_RETURN(ReadUInt<uint64_t>(), uuid_len);
    // Bound-check before allocating -- see kMaxOriginUuidLen's comment above.
    if (uuid_len > kMaxOriginUuidLen)
      return make_error_code(errc::illegal_byte_sequence);
    dest->origin_uuid.resize(uuid_len);
    uint8_t* ptr = reinterpret_cast<uint8_t*>(dest->origin_uuid.data());
    if (auto ec = ReadString({ptr, uuid_len}); ec)
      return ec;
    return {};
  }

  SET_OR_RETURN(ReadUInt<uint64_t>(), dest->txid);

  // drakeydb: Phase 3 framing-version header, read for every opcode that reaches this generic
  // tail (COMMAND today; EXPIRED is sunset on the write side but old streams may still carry
  // it in this same shape). Deliberately version-agnostic -- it never consults
  // IsActiveReplica() -- so a non-active drakeydb replica can parse an active peer's v2 stream,
  // and an active node can parse a plain v1 stream from an upstream master. 1 == legacy
  // (nothing further to read here). 2 == extended: origin_idx/mvcc/entry_flags follow, before
  // the command payload. Anything else is a stream we cannot safely interpret.
  uint32_t framing_version;
  SET_OR_RETURN(ReadUInt<uint32_t>(), framing_version);
  if (framing_version == 2) {
    SET_OR_RETURN(ReadUInt<uint32_t>(), dest->origin_idx);
    SET_OR_RETURN(ReadUInt<uint64_t>(), dest->mvcc);
    SET_OR_RETURN(ReadUInt<uint8_t>(), dest->entry_flags);
  } else if (framing_version != 1) {
    return make_error_code(errc::illegal_byte_sequence);
  }

  VLOG(1) << "Read entry " << dest->ToString();

  return ReadCommand(&dest->cmd);
}

}  // namespace dfly
