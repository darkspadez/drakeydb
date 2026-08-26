#include <absl/cleanup/cleanup.h>
#include <absl/flags/reflection.h>
#include <absl/strings/str_join.h>
#include <sys/socket.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <random>
#include <string>
#include <system_error>

#include "base/flags.h"
#include "base/gtest.h"
#include "base/logging.h"
#include "core/detail/gen_utils.h"
#include "facade/facade_stats.h"
#include "server/common.h"
#include "server/engine_shard_set.h"
#include "server/journal/journal_slice.h"
#include "server/journal/pending_buf.h"
#include "server/journal/serializer.h"
#include "server/journal/streamer.h"
#include "server/journal/test_capturing_socket.h"
#include "server/journal/tx_executor.h"
#include "server/journal/types.h"
#include "server/multi_master.h"
#include "server/serializer_commons.h"
#include "server/server_state.h"
#include "strings/human_readable.h"
#include "util/fiber_socket_base.h"
#include "util/fibers/fibers.h"
#include "util/fibers/pool.h"

ABSL_DECLARE_FLAG(uint32_t, shard_repl_backlog_time_ms);
ABSL_DECLARE_FLAG(strings::MemoryBytesFlag, shard_repl_backlog_max_bytes);
ABSL_DECLARE_FLAG(bool, active_replica);

using namespace testing;
using namespace std;
using namespace util;

namespace dfly {
namespace journal {

namespace {

#ifdef USE_ABSL_LOG
class ScopedLogCapture : public absl::LogSink {
 public:
  ScopedLogCapture() {
    absl::AddLogSink(this);
  }
  ~ScopedLogCapture() override {
    absl::RemoveLogSink(this);
  }
  void Send(const absl::LogEntry& entry) override {
    logs.emplace_back(entry.text_message());
  }

  vector<string> logs;
};
#else
class ScopedLogCapture : public google::LogSink {
 public:
  ScopedLogCapture() {
    google::AddLogSink(this);
  }
  ~ScopedLogCapture() override {
    google::RemoveLogSink(this);
  }
  void send(google::LogSeverity severity, const char* full_filename, const char* base_filename,
            int line, const struct tm* tm_time, const char* message, size_t message_len) override {
    logs.emplace_back(message, message_len);
  }

  vector<string> logs;
};
#endif

}  // namespace

struct EntryPayloadVisitor {
  void operator()(const Entry::Payload& p) {
    out->append(p.cmd).append(" ");
    *out += visit([](const auto& args) { return absl::StrJoin(args, " "); }, p.args);
  }

  string* out;
};

// Extract payload from entry in string form.
std::string ExtractPayload(ParsedEntry& entry) {
  return absl::StrJoin(entry.cmd.view(), " ");
}

std::string ExtractPayload(Entry& entry) {
  std::string out;
  EntryPayloadVisitor visitor{&out};
  visitor(entry.payload);
  return out;
}

// Mock non-owned types with underlying storage.
using StoredSlices = vector<vector<string_view>>;
using StoredLists = vector<pair<vector<string>, CmdArgVec>>;

template <typename... Ss> ArgSlice StoreSlice(StoredSlices* vec, Ss... strings) {
  vec->emplace_back(initializer_list<string_view>{strings...});
  return ArgSlice{vec->back().data(), vec->back().size()};
}

template <typename... Ss> CmdArgList StoreList(StoredLists* vec, Ss... strings) {
  vector<string> stored_strings{strings...};
  CmdArgVec out;
  for (auto& s : stored_strings) {
    out.emplace_back(s.data(), s.size());
  }

  vec->emplace_back(std::move(stored_strings), std::move(out));
  auto& arg_vec = vec->back().second;
  return CmdArgList{arg_vec.data(), arg_vec.size()};
}

// Test serializing and de-serializing entries.
TEST(Journal, WriteRead) {
  StoredSlices slices{};
  StoredLists lists{};

  auto slice = [v = &slices](auto... ss) { return StoreSlice(v, ss...); };
  auto list = [v = &lists](auto... ss) { return StoreList(v, ss...); };
  using Payload = Entry::Payload;

  std::vector<Entry> test_entries = {
      {0, Op::COMMAND, 0, nullopt, Payload("MSET", slice("A", "1", "B", "2"))},
      {0, Op::COMMAND, 0, nullopt, Payload("MSET", slice("C", "3"))},
      {1, Op::COMMAND, 0, nullopt, Payload("DEL", list("A", "B"))},
      {2, Op::COMMAND, 1, nullopt, Payload("LPUSH", list("l", "v1", "v2"))},
      {3, Op::COMMAND, 0, nullopt, Payload("MSET", slice("D", "4"))},
      {4, Op::COMMAND, 1, nullopt, Payload("DEL", list("l1"))},
      {5, Op::COMMAND, 2, nullopt, Payload("DEL", list("E", "2"))}};

  // Write all entries to a buffer.
  base::IoBuf buf;
  io::BufSink sink{&buf};

  JournalWriter writer{&sink};
  for (const auto& entry : test_entries) {
    writer.Write(entry);
  }

  // Read them back.
  io::BufSource source{&buf};
  JournalReader reader{&source, 0};

  ParsedEntry res;
  for (unsigned i = 0; i < test_entries.size(); i++) {
    auto& expected = test_entries[i];

    auto ec = reader.ReadEntry(&res);
    ASSERT_FALSE(ec);

    ASSERT_EQ(expected.opcode, res.opcode);
    ASSERT_EQ(expected.txid, res.txid);
    ASSERT_EQ(expected.dbid, res.dbid);
    ASSERT_EQ(ExtractPayload(expected), ExtractPayload(res));
  }
}

// drakeydb: Phase 3 T2 -- proves the legacy (extended_framing == false) wire format is
// byte-identical to pre-T2 upstream framing. `kGoldenLegacyBytes` was captured by dumping
// JournalWriter's raw output for this exact 7-entry fixture *before* the T2 change landed (see
// task-2-report.md for the capture procedure); it must only ever be refreshed by re-capturing
// against a known-good pre-Phase-3 build, never by re-deriving it from the new code.
TEST(Journal, WriteLegacyFramingIsByteIdentical) {
  StoredSlices slices{};
  StoredLists lists{};

  auto slice = [v = &slices](auto... ss) { return StoreSlice(v, ss...); };
  auto list = [v = &lists](auto... ss) { return StoreList(v, ss...); };
  using Payload = Entry::Payload;

  std::vector<Entry> test_entries = {
      {0, Op::COMMAND, 0, nullopt, Payload("MSET", slice("A", "1", "B", "2"))},
      {0, Op::COMMAND, 0, nullopt, Payload("MSET", slice("C", "3"))},
      {1, Op::COMMAND, 0, nullopt, Payload("DEL", list("A", "B"))},
      {2, Op::COMMAND, 1, nullopt, Payload("LPUSH", list("l", "v1", "v2"))},
      {3, Op::COMMAND, 0, nullopt, Payload("MSET", slice("D", "4"))},
      {4, Op::COMMAND, 1, nullopt, Payload("DEL", list("l1"))},
      {5, Op::COMMAND, 2, nullopt, Payload("DEL", list("E", "2"))}};

  base::IoBuf buf;
  io::BufSink sink{&buf};

  JournalWriter writer{&sink, /*extended_framing=*/false};
  for (const auto& entry : test_entries) {
    writer.Write(entry);
  }

  // Golden buffer: bytes for the 7 entries above, one literal chunk per entry (chunk sizes are
  // 20/14/13/21/16/14/15, summing to 113). Captured pre-change; see comment above.
  constexpr char kGoldenLegacyBytes[] =
      "\x06\x00\x0a\x00\x01\x05\x08\x04\x4d\x53\x45\x54\x01\x41\x01\x31\x01\x42\x01\x32"
      "\x0a\x00\x01\x03\x06\x04\x4d\x53\x45\x54\x01\x43\x01\x33"
      "\x0a\x01\x01\x03\x05\x03\x44\x45\x4c\x01\x41\x01\x42"
      "\x06\x01\x0a\x02\x01\x04\x0a\x05\x4c\x50\x55\x53\x48\x01\x6c\x02\x76\x31\x02\x76\x32"
      "\x06\x00\x0a\x03\x01\x03\x06\x04\x4d\x53\x45\x54\x01\x44\x01\x34"
      "\x06\x01\x0a\x04\x01\x02\x05\x03\x44\x45\x4c\x02\x6c\x31"
      "\x06\x02\x0a\x05\x01\x03\x05\x03\x44\x45\x4c\x01\x45\x01\x32";
  constexpr size_t kGoldenLegacyLen = sizeof(kGoldenLegacyBytes) - 1;
  ASSERT_EQ(kGoldenLegacyLen, 113u);
  std::string_view golden(kGoldenLegacyBytes, kGoldenLegacyLen);

  io::Bytes written_bytes = buf.InputBuffer();
  std::string_view written(reinterpret_cast<const char*>(written_bytes.data()),
                           written_bytes.size());
  EXPECT_EQ(written, golden);

  // Belt-and-suspenders: the framing-version/deprecated-field byte of the first COMMAND entry
  // (index 4 -- SELECT opcode, SELECT dbid, COMMAND opcode, txid, *header*) must be the literal
  // `1`, so a mis-captured golden literal that happened to already match the *new* v2 output
  // could not silently hide a framing drift right at the header.
  ASSERT_GE(written.size(), 5u);
  EXPECT_EQ(static_cast<uint8_t>(written[4]), 1u);

  // drakeydb: fix-round-1 finding 3 -- pin the *default* too. The assertion above only proves
  // `extended_framing=false` is byte-identical; nothing else in this suite would notice the
  // ctor's default argument flipping from false to true, which would silently break every
  // upstream-facing call site that constructs JournalWriter with just a sink (cmd_serializer.cc,
  // streamer.cc). Build a second writer with no explicit argument and check it against the same
  // golden buffer.
  base::IoBuf default_buf;
  io::BufSink default_sink{&default_buf};
  JournalWriter default_writer{&default_sink};  // no extended_framing argument -- pins the default
  for (const auto& entry : test_entries) {
    default_writer.Write(entry);
  }
  io::Bytes default_written_bytes = default_buf.InputBuffer();
  std::string_view default_written(reinterpret_cast<const char*>(default_written_bytes.data()),
                                   default_written_bytes.size());
  EXPECT_EQ(default_written, golden);
}

// drakeydb: Phase 3 T2 -- extended_framing == true round trips origin_idx/mvcc/entry_flags.
TEST(Journal, WriteReadExtendedFraming) {
  StoredSlices slices{};
  auto slice = [v = &slices](auto... ss) { return StoreSlice(v, ss...); };
  using Payload = Entry::Payload;

  Entry entry{7, Op::COMMAND, 2, nullopt, Payload("SET", slice("key", "value"))};
  entry.origin_idx = 3;
  entry.mvcc = 0;  // drakeydb: P4 fills mvcc; P3 always writes 0 (see task brief).
  entry.entry_flags = kEntryFlagExpired;

  base::IoBuf buf;
  io::BufSink sink{&buf};
  JournalWriter writer{&sink, /*extended_framing=*/true};
  writer.Write(entry);

  io::BufSource source{&buf};
  JournalReader reader{&source, 0};
  ParsedEntry res;
  auto ec = reader.ReadEntry(&res);
  ASSERT_FALSE(ec);

  EXPECT_EQ(res.opcode, Op::COMMAND);
  EXPECT_EQ(res.txid, 7u);
  EXPECT_EQ(res.dbid, 2u);
  EXPECT_EQ(res.origin_idx, 3u);
  EXPECT_EQ(res.mvcc, 0u);
  EXPECT_EQ(res.entry_flags, kEntryFlagExpired);
  EXPECT_EQ(ExtractPayload(entry), ExtractPayload(res));
}

// drakeydb: Phase 3 T2 -- the reader is version-agnostic: one JournalReader instance, with no
// knowledge of either writer's framing choice, parses a v2 entry followed by a v1 entry, and the
// legacy-framed entry does not inherit the previous v2 entry's origin metadata.
TEST(Journal, ReadEntryIsVersionAgnostic) {
  StoredSlices slices{};
  auto slice = [v = &slices](auto... ss) { return StoreSlice(v, ss...); };
  using Payload = Entry::Payload;

  base::IoBuf buf;
  io::BufSink sink{&buf};

  Entry extended{1, Op::COMMAND, 0, nullopt, Payload("SET", slice("b", "2"))};
  extended.origin_idx = 5;
  extended.entry_flags = kEntryFlagExpired;
  JournalWriter extended_writer{&sink, /*extended_framing=*/true};
  extended_writer.Write(extended);

  Entry legacy{2, Op::COMMAND, 0, nullopt, Payload("SET", slice("a", "1"))};
  JournalWriter legacy_writer{&sink, /*extended_framing=*/false};
  legacy_writer.Write(legacy);

  io::BufSource source{&buf};
  JournalReader reader{&source, 0};

  ParsedEntry res;
  ASSERT_FALSE(reader.ReadEntry(&res));
  EXPECT_EQ(res.txid, 1u);
  EXPECT_EQ(res.origin_idx, 5u);
  EXPECT_EQ(res.entry_flags, kEntryFlagExpired);
  EXPECT_EQ(ExtractPayload(extended), ExtractPayload(res));

  ASSERT_FALSE(reader.ReadEntry(&res));
  EXPECT_EQ(res.txid, 2u);
  // Must NOT leak the previous (extended-framed) entry's origin_idx/entry_flags onto this
  // legacy-framed one -- ReadEntry resets Phase 3 fields on every call.
  EXPECT_EQ(res.origin_idx, 0u);
  EXPECT_EQ(res.entry_flags, 0u);
  EXPECT_EQ(ExtractPayload(legacy), ExtractPayload(res));
}

// drakeydb: Phase 3 T2 -- an unrecognized framing-version header must error out rather than
// silently misparsing the rest of the stream (today's upstream behavior, which this branch
// replaces).
//
// drakeydb: fix-round-1 finding 1 -- a well-formed (empty) legacy payload follows the bogus
// header, and the assertion checks the *specific* error code. Without the payload, the buffer
// would end right after the header, so even a version of ReadEntry with the rejection branch
// deleted would fall through to ReadCommand, hit EOF, and return errc::io_error -- still
// truthy, so a bare EXPECT_TRUE(ec) could not tell "rejected the header" apart from "ran out of
// bytes". With the payload present, that same buggy code path would instead succeed (num_strings
// == 0 needs no further bytes), so the specific-error-code assertion below is what actually
// pins the rejection branch's presence and behavior.
TEST(Journal, ReadEntryRejectsUnknownFramingVersion) {
  base::IoBuf buf;
  io::BufSink sink{&buf};

  // Hand-craft a COMMAND entry with an out-of-range framing-version header (3); no JournalWriter
  // can legitimately produce this, so the raw primitives are used directly.
  JournalWriter writer{&sink, /*extended_framing=*/false};
  writer.Write(static_cast<uint64_t>(Op::COMMAND));  // opcode
  writer.Write(uint64_t{0});                         // txid
  writer.Write(uint64_t{3});                         // bogus framing-version header
  writer.Write(uint64_t{0});                         // well-formed payload: num_strings = 0
  writer.Write(uint64_t{0});                         // well-formed payload: cmd_size = 0

  io::BufSource source{&buf};
  JournalReader reader{&source, 0};
  ParsedEntry res;
  std::error_code ec = reader.ReadEntry(&res);
  EXPECT_EQ(ec, make_error_code(errc::illegal_byte_sequence));
}

// drakeydb: Phase 3 T2 -- Op::ORIGIN round trips idx + uuid, and TransactionData::AddEntry (see
// tx_executor.cc) has its own case for it so it is never mistaken for a command.
//
// drakeydb: fix-round-1 finding 2 -- the uuid now lands in its own field (origin_uuid), and
// `cmd` stays empty for an ORIGIN entry (see types.h), so a dispatcher keying off cmd.empty()
// (rdb_load.cc, replica.cc) can no longer mistake the uuid for a command name.
TEST(Journal, OriginEntryRoundTrips) {
  Entry entry{Op::ORIGIN, /*dbid=*/0, nullopt};
  entry.origin_idx = 4;
  const string_view kUuid = "11111111-2222-3333-4444-555555555555";
  entry.payload.cmd = kUuid;

  base::IoBuf buf;
  io::BufSink sink{&buf};
  JournalWriter writer{&sink, /*extended_framing=*/true};
  writer.Write(entry);

  io::BufSource source{&buf};
  JournalReader reader{&source, 0};
  ParsedEntry res;
  auto ec = reader.ReadEntry(&res);
  ASSERT_FALSE(ec);

  EXPECT_EQ(res.opcode, Op::ORIGIN);
  EXPECT_EQ(res.origin_idx, 4u);
  EXPECT_EQ(res.mvcc, 0u);
  EXPECT_EQ(res.entry_flags, 0u);
  EXPECT_TRUE(res.cmd.empty());
  EXPECT_EQ(res.origin_uuid, kUuid);
}

// Op::ORIGIN belongs exclusively to extended framing. In debug builds DFATAL proves misuse is
// diagnosed; in release builds, where DFATAL is non-fatal, pin the stronger wire invariant: a
// rejected ORIGIN must not emit even its opcode byte into the legacy stream.
TEST(JournalDeathTest, LegacyWriterRejectsOriginBeforeWriting) {
  Entry entry{Op::ORIGIN, /*dbid=*/0, nullopt};
  entry.origin_idx = 4;
  entry.payload.cmd = "11111111-2222-3333-4444-555555555555";

#ifndef NDEBUG
  EXPECT_DEATH(
      {
        base::IoBuf buf;
        io::BufSink sink{&buf};
        JournalWriter writer{&sink};
        writer.Write(entry);
      },
      "Op::ORIGIN written without extended_framing");
#else
  base::IoBuf buf;
  io::BufSink sink{&buf};
  JournalWriter writer{&sink, /*extended_framing=*/false};
  writer.Write(entry);
  EXPECT_TRUE(buf.InputBuffer().empty());
#endif
}

// drakeydb: fix-round-1 finding 4 -- Op::ORIGIN's uuid length is bounds-checked before any
// allocation, so a corrupt or hostile frame cannot force a huge allocation (or an uncaught
// std::length_error) in the reader fiber. No bytes follow the absurd length -- if the bound
// check were missing or came after the allocation/read attempt, this would instead hit EOF and
// return errc::io_error, so the specific-error-code assertion is what distinguishes "rejected
// up front" from "ran out of bytes trying to honor it".
TEST(Journal, OriginEntryRejectsOversizedUuidLength) {
  base::IoBuf buf;
  io::BufSink sink{&buf};

  JournalWriter writer{&sink, /*extended_framing=*/true};
  writer.Write(static_cast<uint64_t>(Op::ORIGIN));  // opcode
  writer.Write(uint64_t{0});                        // origin_idx
  writer.Write(uint64_t{1'000'000});                // absurd uuid length; no bytes follow

  io::BufSource source{&buf};
  JournalReader reader{&source, 0};
  ParsedEntry res;
  std::error_code ec = reader.ReadEntry(&res);
  EXPECT_EQ(ec, make_error_code(errc::illegal_byte_sequence));
}

TEST(Journal, PendingBuf) {
  PendingBuf pbuf;

  ASSERT_TRUE(pbuf.Empty());
  ASSERT_EQ(pbuf.Size(), 0);

  pbuf.Push("one");
  pbuf.Push(" smallllllllllllllllllllllllllllllll");
  pbuf.Push(" test");

  ASSERT_FALSE(pbuf.Empty());
  ASSERT_EQ(pbuf.Size(), 44);

  {
    auto& sending_buf = pbuf.PrepareSendingBuf();
    ASSERT_EQ(sending_buf.buf.size(), 3);
    ASSERT_EQ(sending_buf.mem_size, 44);

    ASSERT_EQ(sending_buf.buf[0], "one");
    ASSERT_EQ(sending_buf.buf[1], " smallllllllllllllllllllllllllllllll");
    ASSERT_EQ(sending_buf.buf[2], " test");
  }

  const size_t string_num = PendingBuf::Buf::kMaxBufSize + 1000;
  std::vector<std::string> test_data;
  test_data.reserve(string_num);

  absl::InsecureBitGen gen;

  for (size_t i = 0; i < string_num; ++i) {
    auto str = GetRandomHex(gen, 10, 90);
    test_data.push_back(str);
    pbuf.Push(std::move(str));
  }

  const size_t test_data_size =
      std::accumulate(test_data.begin(), test_data.end(), 0,
                      [](size_t size, const auto& s) { return s.size() + size; });

  ASSERT_FALSE(pbuf.Empty());
  ASSERT_EQ(pbuf.Size(), 44 + test_data_size);

  pbuf.Pop();

  ASSERT_FALSE(pbuf.Empty());
  ASSERT_EQ(pbuf.Size(), test_data_size);

  {
    auto& sending_buf = pbuf.PrepareSendingBuf();

    const size_t send_buf_size =
        std::accumulate(test_data.begin(), test_data.begin() + PendingBuf::Buf::kMaxBufSize, 0,
                        [](size_t size, const auto& s) { return s.size() + size; });

    ASSERT_EQ(sending_buf.buf.size(), PendingBuf::Buf::kMaxBufSize);
    ASSERT_EQ(sending_buf.mem_size, send_buf_size);

    for (size_t i = 0; i < sending_buf.buf.size(); ++i) {
      ASSERT_EQ(sending_buf.buf[i], test_data[i]);
    }
  }

  pbuf.Pop();

  test_data.erase(test_data.begin(), test_data.begin() + PendingBuf::Buf::kMaxBufSize);

  const size_t last_buf_size =
      std::accumulate(test_data.begin(), test_data.end(), 0,
                      [](size_t size, const auto& s) { return s.size() + size; });

  ASSERT_FALSE(pbuf.Empty());
  ASSERT_EQ(pbuf.Size(), last_buf_size);

  {
    auto& sending_buf = pbuf.PrepareSendingBuf();

    ASSERT_EQ(sending_buf.buf.size(), 1000);
    ASSERT_EQ(sending_buf.mem_size, last_buf_size);

    for (size_t i = 0; i < sending_buf.buf.size(); ++i) {
      ASSERT_EQ(sending_buf.buf[i], test_data[i]);
    }
  }

  pbuf.Pop();

  ASSERT_TRUE(pbuf.Empty());
  ASSERT_EQ(pbuf.Size(), 0);
}

void AddSetRecord(JournalSlice* slice, string_view value) {
  array<string_view, 2> args{"key", value};
  slice->AddLogRecord(
      Entry{0, Op::COMMAND, 0, nullopt, Entry::Payload{"SET", ArgSlice{args.data(), args.size()}}});
}

// drakeydb: Phase 3 T1 -- origin_idx/entry_flags must survive AddLogRecord -> ring buffer ->
// GetEntryMeta without being reparsed from `data`.
TEST(Journal, OriginMetadataSurvivesRoundTrip) {
  JournalSlice slice;
  slice.Init();

  array<string_view, 2> args{"key", "value"};
  Entry entry{0, Op::COMMAND, 0, nullopt,
              Entry::Payload{"SET", ArgSlice{args.data(), args.size()}}};
  entry.origin_idx = 7;
  entry.entry_flags = kEntryFlagExpired;
  slice.AddLogRecord(entry);

  const JournalItem& meta = slice.GetEntryMeta(1);
  EXPECT_EQ(meta.lsn, 1u);
  EXPECT_EQ(meta.origin_idx, 7u);
  EXPECT_EQ(meta.entry_flags, kEntryFlagExpired);

  // Default (self-originated) entries stay at the zero-value defaults -- non-active behavior is
  // unaffected.
  AddSetRecord(&slice, "x");
  const JournalItem& default_meta = slice.GetEntryMeta(2);
  EXPECT_EQ(default_meta.origin_idx, 0u);
  EXPECT_EQ(default_meta.entry_flags, 0u);
}

TEST(Journal, BacklogHonorsByteLimitAndReplacesOversizedRecord) {
  absl::FlagSaver flag_saver;
  absl::SetFlag(&FLAGS_shard_repl_backlog_time_ms, 0u);

  JournalSlice probe;
  probe.Init();
  AddSetRecord(&probe, "x");
  const size_t item_bytes = probe.GetRingBufferBytes();
  ASSERT_GT(item_bytes, 1u);

  const size_t max_bytes = 2 * item_bytes - 1;
  absl::SetFlag(&FLAGS_shard_repl_backlog_max_bytes, strings::MemoryBytesFlag{max_bytes});
  JournalSlice slice;
  slice.Init();

  AddSetRecord(&slice, "x");
  AddSetRecord(&slice, "x");
  EXPECT_EQ(slice.GetRingBufferSize(), 1u);
  EXPECT_FALSE(slice.IsLSNInBuffer(1));
  EXPECT_TRUE(slice.IsLSNInBuffer(2));
  EXPECT_LE(slice.GetRingBufferBytes(), max_bytes);

  string large_value(2048, 'x');
  AddSetRecord(&slice, large_value);
  EXPECT_EQ(slice.GetRingBufferSize(), 1u);
  EXPECT_FALSE(slice.IsLSNInBuffer(2));
  EXPECT_TRUE(slice.IsLSNInBuffer(3));

  AddSetRecord(&slice, "x");
  EXPECT_EQ(slice.GetRingBufferSize(), 1u);
  EXPECT_FALSE(slice.IsLSNInBuffer(3));
  EXPECT_TRUE(slice.IsLSNInBuffer(4));
  EXPECT_LE(slice.GetRingBufferBytes(), max_bytes);
}

TEST(Journal, BacklogDefaultByteLimitUsesHalfPercentOfMaxmemory) {
  absl::FlagSaver flag_saver;
  absl::SetFlag(&FLAGS_shard_repl_backlog_time_ms, 1000u);
  absl::SetFlag(&FLAGS_shard_repl_backlog_max_bytes, strings::MemoryBytesFlag{});

  const size_t original_max_memory = max_memory_limit.exchange(200 * 1024, memory_order_relaxed);
  auto restore_max_memory = absl::MakeCleanup(
      [original_max_memory] { max_memory_limit.store(original_max_memory, memory_order_relaxed); });

  JournalSlice slice;
  slice.Init();

  string large_value(2048, 'x');
  AddSetRecord(&slice, large_value);
  AddSetRecord(&slice, "x");

  // 0.5% of 200 KiB is 1 KiB, so the oversized record is evicted.
  EXPECT_FALSE(slice.IsLSNInBuffer(1));
  EXPECT_TRUE(slice.IsLSNInBuffer(2));
}

TEST(Journal, BacklogGrowsBeyondInitialCapacity) {
  absl::FlagSaver flag_saver;
  absl::SetFlag(&FLAGS_shard_repl_backlog_time_ms, 0u);
  absl::SetFlag(&FLAGS_shard_repl_backlog_max_bytes, strings::MemoryBytesFlag{4 * 1024 * 1024});

  JournalSlice slice;
  slice.Init();

  constexpr size_t kRecordCount = 10'000;
  for (size_t i = 0; i < kRecordCount; ++i) {
    AddSetRecord(&slice, "value");
  }

  EXPECT_EQ(slice.GetRingBufferSize(), kRecordCount);
  EXPECT_TRUE(slice.IsLSNInBuffer(1));
  EXPECT_TRUE(slice.IsLSNInBuffer(kRecordCount));
}

TEST(Journal, BacklogDropsOldestWhenMetadataCannotGrow) {
  absl::FlagSaver flag_saver;
  absl::SetFlag(&FLAGS_shard_repl_backlog_time_ms, 0u);

  const string large_value(128, 'x');
  JournalSlice large_probe;
  large_probe.Init();
  AddSetRecord(&large_probe, large_value);
  const size_t large_item_bytes = large_probe.GetRingBufferBytes();

  JournalSlice small_probe;
  small_probe.Init();
  AddSetRecord(&small_probe, "x");
  const size_t small_item_bytes = small_probe.GetRingBufferBytes();
  ASSERT_LT(small_item_bytes, large_item_bytes);

  constexpr size_t kInitialCapacity = 8192;
  absl::SetFlag(&FLAGS_shard_repl_backlog_max_bytes,
                strings::MemoryBytesFlag{kInitialCapacity * large_item_bytes + small_item_bytes});

  JournalSlice slice;
  slice.Init();
  for (size_t i = 0; i < kInitialCapacity; ++i) {
    AddSetRecord(&slice, large_value);
  }

  AddSetRecord(&slice, "x");

  EXPECT_EQ(slice.GetRingBufferSize(), kInitialCapacity);
  EXPECT_FALSE(slice.IsLSNInBuffer(1));
  EXPECT_TRUE(slice.IsLSNInBuffer(kInitialCapacity + 1));
}

TEST(Journal, BacklogCleansExpiredEntriesOnAppend) {
  absl::FlagSaver flag_saver;
  absl::SetFlag(&FLAGS_shard_repl_backlog_time_ms, 1000u);
  absl::SetFlag(&FLAGS_shard_repl_backlog_max_bytes, strings::MemoryBytesFlag{1024 * 1024});

  const uint64_t original_time = TEST_current_time_ms;
  auto restore_time = absl::MakeCleanup([original_time] { TEST_current_time_ms = original_time; });
  TEST_current_time_ms = 1000;

  JournalSlice slice;
  slice.Init();
  AddSetRecord(&slice, "first");
  EXPECT_TRUE(slice.IsLSNInBuffer(1));

  TEST_current_time_ms = 1999;
  EXPECT_TRUE(slice.IsLSNInBuffer(1));

  TEST_current_time_ms = 2000;
  EXPECT_TRUE(slice.IsLSNInBuffer(1));

  AddSetRecord(&slice, "second");
  EXPECT_EQ(slice.GetRingBufferSize(), 1u);
  EXPECT_FALSE(slice.IsLSNInBuffer(1));
  EXPECT_TRUE(slice.IsLSNInBuffer(2));
}

TEST(Journal, BacklogBoundsTimeBasedCleanup) {
  absl::FlagSaver flag_saver;
  absl::SetFlag(&FLAGS_shard_repl_backlog_time_ms, 1000u);
  absl::SetFlag(&FLAGS_shard_repl_backlog_max_bytes, strings::MemoryBytesFlag{1024 * 1024});

  const uint64_t original_time = TEST_current_time_ms;
  auto restore_time = absl::MakeCleanup([original_time] { TEST_current_time_ms = original_time; });

  JournalSlice slice;
  slice.Init();

  TEST_current_time_ms = 1000;
  constexpr size_t kExpiredEntries = 101;
  for (size_t i = 0; i < kExpiredEntries; ++i) {
    AddSetRecord(&slice, "expired");
  }

  TEST_current_time_ms = 2000;
  AddSetRecord(&slice, "current");

  EXPECT_EQ(slice.GetRingBufferSize(), 2u);
  EXPECT_FALSE(slice.IsLSNInBuffer(1));
  EXPECT_FALSE(slice.IsLSNInBuffer(kExpiredEntries - 1));
  EXPECT_TRUE(slice.IsLSNInBuffer(kExpiredEntries));
  EXPECT_TRUE(slice.IsLSNInBuffer(kExpiredEntries + 1));
}

// drakeydb: Phase 3 T7b -- journal::PassesPeerEchoFilter (types.h/.cc) is the ONE shared
// definition of the peer-echo-prevention rule: JournalStreamer::ShouldWrite (streamer.cc, the
// stable-sync stream) and SliceSnapshot::ConsumeJournalChange (snapshot.cc, the full-sync
// window's concurrent journal blob) both call it rather than each re-deriving the rule. This
// exhaustively pins each of its three drop conditions -- plus the two things it must NOT drop by
// opcode -- on the pure function itself, independent of either caller's own plumbing (which have
// their own coverage: JournalStreamerPeerFilterTest below for the streamer, and
// multi_master_test.cc / peer_replication_test.cc for the full-sync send/receive sides).
//
// Falsifying: removing any one of the three `return false` branches in PassesPeerEchoFilter
// (types.cc) makes exactly the EXPECT_FALSE case for that condition fail (the matching
// EXPECT_TRUE cases are unaffected, since they exercise the OTHER branches); removing the whole
// function body down to `return true;` fails every EXPECT_FALSE case at once.
TEST(PassesPeerEchoFilterTest, DropsForeignOriginExpiryDelAndOriginOpcodeOnly) {
  constexpr uint32_t kPeerIdx = 3;  // some peer's PeerRegistry index; != PeerRegistry::kSelfIdx.
  ScopedLogCapture log_capture;

  JournalItem self_write{};
  self_write.origin_idx = PeerRegistry::kSelfIdx;
  self_write.opcode = Op::COMMAND;
  EXPECT_TRUE(PassesPeerEchoFilter(self_write)) << "a plain self-origin write must pass";

  JournalItem foreign_write{};
  foreign_write.origin_idx = kPeerIdx;
  foreign_write.opcode = Op::COMMAND;
  EXPECT_FALSE(PassesPeerEchoFilter(foreign_write)) << "a foreign-origin write must be dropped";

  JournalItem expiry_del{};
  expiry_del.origin_idx = PeerRegistry::kSelfIdx;
  expiry_del.opcode = Op::COMMAND;
  expiry_del.entry_flags = kEntryFlagExpired;
  EXPECT_FALSE(PassesPeerEchoFilter(expiry_del)) << "an expiry-flagged DEL must be dropped";

  JournalItem origin_entry{};
  // Op::ORIGIN entries are always recorded self-origin (PeerRegistry::AddOrGet emits them
  // locally) -- give this one origin_idx == kSelfIdx too, so only the opcode check can catch it,
  // exactly matching what production actually emits (see multi_master.cc's AddOrGet).
  origin_entry.origin_idx = PeerRegistry::kSelfIdx;
  origin_entry.opcode = Op::ORIGIN;
  EXPECT_FALSE(PassesPeerEchoFilter(origin_entry)) << "an Op::ORIGIN entry must be dropped";

  // Op::LSN and Op::PING are never dropped by opcode -- LSN bookkeeping and ack/partial-resume
  // accounting on the receiving side depend on both reaching the consumer.
  JournalItem self_lsn{};
  self_lsn.origin_idx = PeerRegistry::kSelfIdx;
  self_lsn.opcode = Op::LSN;
  EXPECT_TRUE(PassesPeerEchoFilter(self_lsn)) << "Op::LSN must not be dropped by opcode";

  JournalItem self_ping{};
  self_ping.origin_idx = PeerRegistry::kSelfIdx;
  self_ping.opcode = Op::PING;
  EXPECT_TRUE(PassesPeerEchoFilter(self_ping)) << "Op::PING must not be dropped by opcode";

  // A foreign-origin PING IS dropped -- by the origin_idx check (every entry's general rule),
  // not by an opcode-specific carve-out. Distinguishes "never dropped by opcode" from "never
  // dropped at all".
  JournalItem foreign_ping{};
  foreign_ping.origin_idx = kPeerIdx;
  foreign_ping.opcode = Op::PING;
  EXPECT_FALSE(PassesPeerEchoFilter(foreign_ping));

  size_t violation_logs = count_if(log_capture.logs.begin(), log_capture.logs.end(), [](auto& log) {
    return log.find("Refusing to forward foreign-origin journal entry") != string::npos;
  });
  EXPECT_EQ(1u, violation_logs) << "the peer-protocol diagnostic must be rate-limited";
}

// drakeydb: P4-0 -- a DEL derived from a collection command emptying its key (e.g. HTTL
// discovering a hash field's TTL has lazily expired) must never reach a mesh peer, except where
// the causing command's own replay there cannot be relied on to reproduce it (FIELDEXPIRE, SORT
// -- see the RecordDerivedDelete `derived` parameter, tx_base.h): ordinarily, the peer either
// replays the causing command and derives the same DEL itself, or expires the same data on its
// own clock. See kEntryFlagDerived's own comment (types.h) for the full argument, and
// docs/PLAN.md's Phase 4 section for the per-call-site enumeration that backs it. Distinct from
// kEntryFlagExpired (a *whole-key* TTL expiry) so diagnostics can tell the two apart.
TEST(PassesPeerEchoFilterTest, DerivedDeleteIsSuppressedToPeers) {
  JournalItem item;
  item.opcode = journal::Op::COMMAND;
  item.origin_idx = PeerRegistry::kSelfIdx;  // self-originated, so origin alone passes it
  item.entry_flags = journal::kEntryFlagDerived;
  EXPECT_FALSE(journal::PassesPeerEchoFilter(item))
      << "a derived DEL must never reach a peer -- the peer derives its own";
}

TEST(PassesPeerEchoFilterTest, DerivedAndExpiredAreDistinctFlags) {
  EXPECT_NE(journal::kEntryFlagDerived, journal::kEntryFlagExpired);
  JournalItem item;
  item.opcode = journal::Op::COMMAND;
  item.origin_idx = PeerRegistry::kSelfIdx;
  item.entry_flags = journal::kEntryFlagExpired | journal::kEntryFlagDerived;
  EXPECT_FALSE(journal::PassesPeerEchoFilter(item));
}

// drakeydb: Phase 3 T6b fix-round-1 (Q1) -- CapturingFiberSocket now lives in
// test_capturing_socket.h, shared with peer_replication_test.cc's
// AdoptAuthoritativeLsnComposesWithRealSenderMarker (see that test's own comment).

// drakeydb: Phase 3 T5 -- exposes JournalStreamer's protected partial-replay driver for direct
// testing. streamer.h documents why MaybePartialStreamLSNs is protected (not private) rather than
// friended via gtest_prod.h's FRIEND_TEST: streamer.h is a production header compiled into the
// `dragonfly` binary itself, and this project's CMake (helio_cxx_test, helio/cmake/internal.cmake)
// only adds gtest's include directory to *_test targets, so a header a production target depends
// on cannot include <gtest/gtest_prod.h>.
class TestPartialSyncStreamer : public JournalStreamer {
 public:
  TestPartialSyncStreamer(ExecutionState* cntx, Config config) : JournalStreamer(cntx, config) {
  }
  using JournalStreamer::MaybePartialStreamLSNs;
};

// drakeydb: Phase 3 T6b fix-round-2 -- JournalStreamer::Config::lsn_marker_throttle_sec
// (streamer.h) set to this value makes the write-path periodic marker / drop-path resolution
// marker throttle (ConsumeJournalChange, streamer.cc) provably never re-fire during a test's real
// execution window, no matter how slow or loaded the machine running it is -- replacing what used
// to be an implicit, unstated assumption that the whole test body would finish inside the
// hard-coded 3-second window (it does not always: a Sanitizers-matrix runner under load is exactly
// where that assumption broke -- see task-6b-report.md's round-2 section). 1,000,000 seconds
// (~11.5 days) is comfortably larger than any test could plausibly run, yet comfortably smaller
// than the current UNIX epoch (~1.7 billion seconds as of 2026) -- last_lsn_time_ starts at its
// zero default, so the very first throttle check ("now - 0 > threshold") must still evaluate true
// for a fresh streamer's first marker to fire immediately, exactly as every comment in this file
// documents.
constexpr int kNeverThrottleSec = 1'000'000;

// Needs a real per-shard journal (EngineShard::tlocal()) to drive the actual
// journal::RecordEntry/journal::GetEntryMeta/JournalStreamer::MaybePartialStreamLSNs code path --
// not a reimplementation of it -- so a minimal single-shard ProactorPool + EngineShardSet is
// stood up, mirroring TransactionTest's fixture in transaction_test.cc.
class JournalStreamerPeerFilterTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // drakeydb: Phase 3 T5 -- JournalSlice::Init() caches extended_framing_ = IsActiveReplica()
    // once, and Op::ORIGIN can only be written with extended framing (serializer.cc DCHECKs
    // this). saver_ restores the flag in TearDown regardless of test outcome.
    absl::SetFlag(&FLAGS_active_replica, true);
    // The default backlog byte budget is 0.5% of max_memory_limit, which is unset (0) in this
    // fixture -- CleanEntries would then evict every entry almost as soon as it's added (see
    // BacklogHonorsByteLimitAndReplacesOversizedRecord above for the same trap). Give the backlog
    // a generous, fixed budget instead, comfortably larger than this test's handful of entries.
    absl::SetFlag(&FLAGS_shard_repl_backlog_max_bytes, strings::MemoryBytesFlag{1024 * 1024});

    pp_.reset(fb2::Pool::Epoll(1));
    pp_->Run();
    pp_->AwaitBrief([](unsigned index, ProactorBase*) {
      ServerState::Init(index, 1, nullptr, nullptr);
      if (facade::tl_facade_stats == nullptr) {
        facade::tl_facade_stats = new facade::FacadeStats;
      }
    });

    shard_set = new EngineShardSet(pp_.get());
    // Pass a no-op shard handler (not nullptr): the periodic shard-handler fiber would otherwise
    // invoke an empty std::function and crash if it fires during the test (see TransactionTest).
    shard_set->Init(1, [] {});

    pp_->at(0)->Await([] { StartInThread(); });
  }

  void TearDown() override {
    shard_set->PreShutdown();
    shard_set->Shutdown();
    delete shard_set;
    shard_set = nullptr;

    pp_->Stop();
    pp_.reset();
  }

  absl::FlagSaver saver_;

  // Decodes a captured byte stream (as accumulated by CapturingFiberSocket::captured) back into
  // a per-entry tag list, so the test can assert on entry identity/order without needing
  // ParsedEntry itself to be copyable (it isn't -- its copy ctor is explicitly deleted, which
  // also suppresses the implicit move ctor, so it cannot live in a std::vector).
  static std::vector<std::string> Decode(const std::string& bytes) {
    base::IoBuf buf;
    io::BufSink sink{&buf};
    sink.Write(io::Buffer(bytes));
    io::BufSource source{&buf};
    JournalReader reader{&source, 0};

    std::vector<std::string> tags;
    ParsedEntry entry;
    while (true) {
      auto ec = reader.ReadEntry(&entry);
      if (ec)
        break;
      switch (entry.opcode) {
        case Op::COMMAND:
          tags.push_back(absl::StrCat("CMD:", ExtractPayload(entry)));
          break;
        case Op::PING:
          tags.push_back("PING");
          break;
        case Op::ORIGIN:
          tags.push_back(absl::StrCat("ORIGIN:", entry.origin_uuid));
          break;
        // drakeydb: Phase 3 T6b -- surface the marker's carried LSN explicitly (rather than
        // folding it into "OTHER" below), since PeerModeGapMarkerCoalescesAndReceiverLandsOnTrueLsn
        // and the MixedOriginBacklogPeerVsFullStream update below both assert on its exact numeric
        // value, not just its presence.
        case Op::LSN:
          tags.push_back(absl::StrCat("LSN:", entry.lsn));
          break;
        default:
          tags.push_back("OTHER");
          break;
      }
    }
    return tags;
  }

  std::unique_ptr<util::ProactorPool> pp_;
};

// drakeydb: Phase 3 T5 -- the brief's headline acceptance test (PLAN.md Risk #1 and #7 in one
// test). Writes a ring-buffer backlog alternating self/peer origin, with one expiry-flagged DEL
// and one Op::ORIGIN entry mixed in, then drives the REAL partial-replay path
// (MaybePartialStreamLSNs, not a reimplementation of it) for a peer-mode consumer and a
// full-stream consumer, and asserts the peer consumer receives only its own non-expiry,
// non-ORIGIN writes while the full-stream consumer receives everything. Also exercises the live
// path (ConsumeJournalChange), by writing two more entries after both streamers have caught up
// and re-registered as live listeners (which MaybePartialStreamLSNs does at its own end).
//
// Falsifying (verified by hand -- see task-5-report.md for the exact reverts and results):
//  - Reverting MaybePartialStreamLSNs to populate only `data`+`lsn` (dropping origin_idx/
//    entry_flags/opcode) makes the peer capture equal the full capture for the backlog portion:
//    every backlog entry defaults to origin_idx=kSelfIdx/entry_flags=0/opcode=Op::COMMAND from
//    ShouldWrite's point of view, even for the real PING/DEL/ORIGIN entries (whose *data* bytes
//    are unaffected, since only the filtering metadata is wrong) -- so all 7 pass the peer
//    filter, not just 3.
//  - Removing the origin_idx check lets LSN2/LSN5/LSN9 (peer-origin SET/PING/SET) through to the
//    peer capture.
//  - Removing the entry_flags check lets LSN3 (expiry-flagged DEL) through.
//  - Removing the Op::ORIGIN opcode check lets LSN6 through -- this is the one drop rule the
//    origin_idx check cannot backstop, because this test gives it origin_idx == kSelfIdx (as
//    PeerRegistry::AddOrGet actually records it -- see multi_master.cc), so only the explicit
//    opcode check catches it.
//
// drakeydb: fix-round-1 -- LSN2/LSN9 (peer-origin SET) are representative of shipped behavior:
// Transaction::LogJournalOnShard already threads a real peer origin onto COMMAND entries applied
// from a peer (T3, tested in multi_master_test.cc's OriginJournalFamilyTest suite). LSN5
// (peer-origin PING) was NOT representative of what production emitted at the time this test was
// written; T6 closed that gap -- replica.cc's StableSyncDflyReadFb now re-records a received PING
// with the sending peer's origin (via executor_->connection_context()->repl_origin_idx), not the
// default self origin -- so LSN5 now also pins real, shipped behavior, like LSN2/LSN9.
TEST_F(JournalStreamerPeerFilterTest, MixedOriginBacklogPeerVsFullStream) {
  constexpr uint32_t kPeerIdx = 5;  // some peer's PeerRegistry index; != PeerRegistry::kSelfIdx.
  const std::string kUuid = "11111111-2222-3333-4444-555555555555";

  pp_->at(0)->Await([&] {
    // --- Backlog: LSN1..LSN7, alternating self/peer origin plus one expiry DEL and one ORIGIN.
    array<string_view, 2> set_a{"a", "1"};
    RecordEntry(0, Op::COMMAND, 0, nullopt,
                Entry::Payload{"SET", ArgSlice{set_a.data(), set_a.size()}},
                PeerRegistry::kSelfIdx);  // LSN1: self -- kept by both.

    array<string_view, 2> set_b{"b", "2"};
    RecordEntry(0, Op::COMMAND, 0, nullopt,
                Entry::Payload{"SET", ArgSlice{set_b.data(), set_b.size()}},
                kPeerIdx);  // LSN2: peer -- dropped for peer, kept full.

    array<string_view, 1> del_c{"c"};
    RecordEntry(0, Op::COMMAND, 0, nullopt,
                Entry::Payload{"DEL", ArgSlice{del_c.data(), del_c.size()}}, PeerRegistry::kSelfIdx,
                /*mvcc=*/0,
                kEntryFlagExpired);  // LSN3: self, expiry DEL -- dropped for peer, kept full.

    RecordEntry(0, Op::PING, 0, nullopt, {},
                PeerRegistry::kSelfIdx);  // LSN4: self PING -- never dropped by opcode; kept by
                                          // both (origin check also passes: self).

    RecordEntry(0, Op::PING, 0, nullopt, {},
                kPeerIdx);  // LSN5: a PING carrying a peer origin -- dropped for peer via the
                            // *origin* check (not opcode), kept full. Representative of shipped
                            // behavior since T6 (see the fix-round-1 note below and ShouldWrite's
                            // own comment in streamer.cc).

    {
      // LSN6: Op::ORIGIN, given origin_idx == kSelfIdx (matching how PeerRegistry::AddOrGet
      // actually records it -- see multi_master.cc) so the origin_idx check alone would NOT
      // catch it; only the opcode check does. Dropped for peer, kept full.
      Entry::Payload payload;
      payload.cmd = kUuid;
      RecordEntry(0, Op::ORIGIN, 0, nullopt, payload, PeerRegistry::kSelfIdx);
    }

    array<string_view, 2> set_d{"d", "4"};
    RecordEntry(0, Op::COMMAND, 0, nullopt,
                Entry::Payload{"SET", ArgSlice{set_d.data(), set_d.size()}},
                PeerRegistry::kSelfIdx);  // LSN7: trailing self write -- kept by both; proves
                                          // processing continues correctly past the ORIGIN entry.

    ASSERT_EQ(8u, GetLsn());  // next LSN to assign; confirms exactly 7 entries were recorded.

    // --- Drive the real partial-replay path for a peer-mode and a full-stream consumer.
    ExecutionState peer_cntx;
    JournalStreamer::Config peer_config;
    peer_config.start_partial_sync_at = 1;
    peer_config.peer_mode = true;
    // drakeydb: Phase 3 T6b fix-round-2 -- see kNeverThrottleSec's own comment. Without this, the
    // exact marker vector asserted below (expected_peer) would depend on this whole scenario --
    // every RecordEntry call after the first, plus both streamers' partial-replay and live-path
    // machinery -- finishing inside a real 3-second wall-clock window, which is not guaranteed on
    // a slow or loaded runner (this project's Sanitizers CI matrix, for one).
    peer_config.lsn_marker_throttle_sec = kNeverThrottleSec;
    CapturingFiberSocket peer_socket;
    TestPartialSyncStreamer peer_streamer(&peer_cntx, peer_config);
    peer_streamer.Start(&peer_socket);
    ASSERT_TRUE(peer_streamer.MaybePartialStreamLSNs());

    ExecutionState full_cntx;
    JournalStreamer::Config full_config;
    full_config.start_partial_sync_at = 1;  // peer_mode left false: full-stream consumer.
    CapturingFiberSocket full_socket;
    TestPartialSyncStreamer full_streamer(&full_cntx, full_config);
    full_streamer.Start(&full_socket);
    ASSERT_TRUE(full_streamer.MaybePartialStreamLSNs());

    // --- Live path: both streamers re-registered as listeners at the end of
    // MaybePartialStreamLSNs; two more entries should reach them via ConsumeJournalChange.
    array<string_view, 2> set_e{"e", "5"};
    RecordEntry(0, Op::COMMAND, 0, nullopt,
                Entry::Payload{"SET", ArgSlice{set_e.data(), set_e.size()}},
                PeerRegistry::kSelfIdx);  // LSN8: self -- kept by both.

    array<string_view, 2> set_f{"f", "6"};
    RecordEntry(0, Op::COMMAND, 0, nullopt,
                Entry::Payload{"SET", ArgSlice{set_f.data(), set_f.size()}},
                kPeerIdx);  // LSN9: peer -- dropped for peer, kept full.

    // drakeydb: Phase 3 T6b -- the peer capture now also carries gap-correction and drop-path
    // resolution Op::LSN markers (see ConsumeJournalChange in streamer.cc).
    //
    // "LSN:2" (fix-round-1, C2) fires on LSN2, the very FIRST entry this streamer's drop path
    // ever evaluates: last_lsn_time_ (reused by the drop-path block, like the pre-existing
    // write-path periodic marker it was borrowed from) also starts at its zero default, so the
    // very first throttle check -- on either path -- is always true; this fires immediately
    // rather than waiting out a throttle window that hasn't started yet. Not a new quirk: the
    // pre-existing write-path periodic marker (config_.should_sent_lsn, left false here) already
    // had the identical "fires immediately on the very first check" property before this task.
    // "LSN:3" (dropped LSN3, no marker of its own: fix-round-2's kNeverThrottleSec guarantees --
    // by construction, not by how fast this scenario happens to run -- that the drop-path
    // throttle cannot re-fire before LSN3 is evaluated, since it just fired on LSN2 above) does
    // NOT appear; instead "LSN:3" precedes PING (LSN4) as fix-round-1's original write-path
    // gap-correction marker (this task, not the C2 fix-round-1 addition -- and never throttled at
    // all, see ConsumeJournalChange), correcting for LSN2/LSN3 having been dropped since
    // last_lsn_writen_ was last advanced (by the LSN2 drop-path marker, to 2) -- "LSN:6" precedes
    // CMD:SET d 4 (LSN7) the same way, correcting for LSN5/LSN6. No marker precedes CMD:SET a 1:
    // peer_config.start_partial_sync_at is 1 below, so JournalStreamer's constructor
    // (fix-round-1, streamer.cc) seeds last_lsn_writen_ to 1 - 1 = 0, and 0 + 1 == 1 (LSN1's own
    // LSN) is a non-gap -- this is specific to resuming at LSN 1, not a general "first entry
    // never gets a marker" property (a resume above LSN 1 -- the common case: a reconnecting peer
    // almost never resumes from LSN 1 -- would seed last_lsn_writen_ to match instead; see
    // PeerModePartialResumeAboveLsn1EmitsNoSpuriousFirstMarker below for that case). None
    // precedes CMD:SET e 5 (LSN8 immediately follows LSN7 with nothing dropped in between --
    // proving a contiguous run of self-writes, including one spanning the partial-replay/live-path
    // seam, gets no spurious marker). expected_full is intentionally NOT updated: peer_mode is
    // false for full_config below, so ConsumeJournalChange's gap-correction and drop-path blocks
    // never trigger for it -- this is this test's own proof that the T6b change leaves the
    // full-stream path byte-identical.
    std::vector<std::string> expected_peer = {"CMD:SET a 1", "LSN:2",       "LSN:3",      "PING",
                                              "LSN:6",       "CMD:SET d 4", "CMD:SET e 5"};
    std::vector<std::string> expected_full = {
        "CMD:SET a 1", "CMD:SET b 2", "CMD:DEL c",  "PING", "PING", absl::StrCat("ORIGIN:", kUuid),
        "CMD:SET d 4", "CMD:SET e 5", "CMD:SET f 6"};

    EXPECT_EQ(expected_peer, JournalStreamerPeerFilterTest::Decode(peer_socket.captured));
    EXPECT_EQ(expected_full, JournalStreamerPeerFilterTest::Decode(full_socket.captured));

    peer_streamer.Cancel();
    full_streamer.Cancel();
  });
}

// drakeydb: Phase 3 T6b -- proves ConsumeJournalChange's write-path gap-correction marker (a)
// coalesces any number of consecutive drops into exactly one marker, (b) never fires for a
// contiguous run of kept writes (including one spanning the partial-replay/live-path seam -- see
// MixedOriginBacklogPeerVsFullStream above for that case), and (c) that a peer-mode
// TransactionReader (tx_executor.cc) parses the resulting stream -- both marker kinds included,
// see below -- without tripping the DCHECK_EQ a non-adopting parse would (see
// NonPeerModeStillDchecksMismatchedLsnMarker below), i.e. that adoption, not comparison, is
// genuinely what runs here. What this test does NOT cover: that adopting a decoded marker value
// correctly lands DflyShardReplica::journal_rec_executed_ on the master's true LSN --
// AdoptAuthoritativeLsn is unreachable from this file's minimal, Service-less fixture (see
// JournalStreamerPeerFilterTest's own comment); that composition (real sender marker -> real
// TransactionReader decode -> real AdoptAuthoritativeLsn, checked against ground truth
// independent of both the sender's `- 1` and the receiver's `+ 1`) is proven by
// peer_replication_test.cc's AdoptAuthoritativeLsnComposesWithRealSenderMarker instead.
//
// drakeydb: fix-round-1 (C2) -- this scenario's 3 consecutive drops (LSN2-4) also exercise the
// drop-path resolution marker (streamer.cc): LSN2 is the very first entry that path ever
// evaluates for this streamer, so it fires immediately (see its own RecordEntry comment above),
// leaving only LSN3/LSN4 for the write-path marker before LSN5 to coalesce -- still exactly one
// marker on THAT path, which remains this test's main point; "LSN:2" in `expected` below is the
// real-world interaction of the two mechanisms, not a separate scenario.
//
// drakeydb: fix-round-1 (Q1) -- this test used to also hand-track a local `journal_rec_executed`
// shadow variable, applying `tx_data.lsn + 1` itself on the marker and `++` on every other entry,
// then compared it to GetLsn(). That reimplemented AdoptAuthoritativeLsn's own arithmetic instead
// of calling it, so it could not have failed even if the real method used `+ 2`: two hand-derived
// numbers agreeing proves nothing about the code under test. Removed; see
// peer_replication_test.cc's test above for where that property is now actually proven, against
// the real method.
//
// Falsifying (verified by hand -- see task-6b-report.md):
//  - Reverting ConsumeJournalChange's write-path gap-correction block (streamer.cc) entirely: the
//    peer capture drops the "LSN:4" entry, and the `ASSERT_EQ(expected, Decode(...))` below fails
//    immediately (4 entries captured, not 5).
//  - Gating that block on drop count instead of an LSN-arithmetic gap (i.e. re-checking on every
//    write instead of only after a real gap): the marker would also precede CMD:SET f 6, which
//    the `ASSERT_EQ(expected, ...)` below also catches (6 entries, extra "LSN:6").
//  - Reverting NextTxData's peer-mode branch (tx_executor.cc) so Op::LSN is always compared, not
//    adopted: the second NextTxData call below (LSN2's drop-path marker) still returns true and
//    reports tx_data.lsn correctly (that part of the wire format is unaffected), but
//    DCHECK_EQ(2, 1) fires immediately -- this specific revert aborts the whole test binary
//    rather than failing the assertion cleanly, which is exactly the crash this task exists to
//    prevent once peer_mode
//    is live (task brief's "Consequences once peer_mode is enabled").
TEST_F(JournalStreamerPeerFilterTest, PeerModeGapMarkerCoalescesAndTransactionReaderAdoptsCleanly) {
  constexpr uint32_t kPeerIdx = 5;  // some peer's PeerRegistry index; != PeerRegistry::kSelfIdx.
  pp_->at(0)->Await([&] {
    array<string_view, 2> set_a{"a", "1"};
    RecordEntry(0, Op::COMMAND, 0, nullopt,
                Entry::Payload{"SET", ArgSlice{set_a.data(), set_a.size()}},
                PeerRegistry::kSelfIdx);  // LSN1: self -- kept, no marker (start_partial_sync_at
                                          // below is 1, so last_lsn_writen_ seeds to 0; see this
                                          // test's own header comment).

    array<string_view, 2> set_b{"b", "2"};
    RecordEntry(0, Op::COMMAND, 0, nullopt,
                Entry::Payload{"SET", ArgSlice{set_b.data(), set_b.size()}},
                kPeerIdx);  // LSN2: peer -- dropped (1st of 3 consecutive drops); ALSO the very
                            // first entry this streamer's drop path (fix-round-1, C2) ever
                            // evaluates, so it gets its own immediate marker (last_lsn_time_
                            // starts at its zero default too -- see this test's header comment)
                            // rather than waiting out a throttle window that hasn't started yet.

    array<string_view, 2> set_c{"c", "3"};
    RecordEntry(0, Op::COMMAND, 0, nullopt,
                Entry::Payload{"SET", ArgSlice{set_c.data(), set_c.size()}},
                kPeerIdx);  // LSN3: peer -- dropped (2nd); no marker of its own -- peer_config's
                            // lsn_marker_throttle_sec is set to kNeverThrottleSec below, which
                            // guarantees (by construction, not by how fast this test happens to
                            // run) that the shared last_lsn_time_ throttle cannot re-fire so soon
                            // after LSN2's marker.

    array<string_view, 2> set_d{"d", "4"};
    RecordEntry(0, Op::COMMAND, 0, nullopt,
                Entry::Payload{"SET", ArgSlice{set_d.data(), set_d.size()}},
                kPeerIdx);  // LSN4: peer -- dropped (3rd); same reason, still no marker.

    array<string_view, 2> set_e{"e", "5"};
    RecordEntry(0, Op::COMMAND, 0, nullopt,
                Entry::Payload{"SET", ArgSlice{set_e.data(), set_e.size()}},
                PeerRegistry::kSelfIdx);  // LSN5: self -- kept; must be preceded by exactly ONE
                                          // write-path marker, carrying 4 (LSN5 - 1) --
                                          // last_lsn_writen_ was last advanced to 2 by LSN2's
                                          // drop-path marker, so LSN3/LSN4 (not all three of
                                          // LSN2-4) are what the write-path gap check still has
                                          // left to coalesce here.

    array<string_view, 2> set_f{"f", "6"};
    RecordEntry(0, Op::COMMAND, 0, nullopt,
                Entry::Payload{"SET", ArgSlice{set_f.data(), set_f.size()}},
                PeerRegistry::kSelfIdx);  // LSN6: self, immediately after LSN5 -- kept, no marker
                                          // (contiguous: proves the marker isn't emitted on every
                                          // write, only after an actual gap).

    ASSERT_EQ(7u, GetLsn());  // next LSN to assign; confirms exactly 6 entries were recorded.

    ExecutionState peer_cntx;
    JournalStreamer::Config peer_config;
    peer_config.start_partial_sync_at = 1;
    peer_config.peer_mode = true;
    // drakeydb: Phase 3 T6b fix-round-2 -- see kNeverThrottleSec's own comment: without this, the
    // exact marker vector asserted below depends on this whole scenario finishing inside a real
    // 3-second wall-clock window, which is not guaranteed under load.
    peer_config.lsn_marker_throttle_sec = kNeverThrottleSec;
    CapturingFiberSocket peer_socket;
    TestPartialSyncStreamer peer_streamer(&peer_cntx, peer_config);
    peer_streamer.Start(&peer_socket);
    ASSERT_TRUE(peer_streamer.MaybePartialStreamLSNs());
    peer_streamer.Cancel();

    std::vector<std::string> expected = {"CMD:SET a 1", "LSN:2", "LSN:4", "CMD:SET e 5",
                                         "CMD:SET f 6"};
    ASSERT_EQ(expected, JournalStreamerPeerFilterTest::Decode(peer_socket.captured));

    // --- Receiver side: replay the exact same bytes through a peer-mode TransactionReader,
    // proving it parses the coalesced marker (and the entries around it) cleanly -- opcodes and
    // the marker's own value match what Decode() already pinned above, and no NextTxData call
    // trips a DCHECK. What this does NOT claim to prove -- that adopting tx_data.lsn correctly
    // updates DflyShardReplica::journal_rec_executed_ -- is proven separately, against the real
    // AdoptAuthoritativeLsn, by peer_replication_test.cc (see this test's own header comment).
    // Calls are counted explicitly, not driven to EOF, so a wrong entry count can't hide behind
    // ReadEntry's own EOF-shaped error path (this phase has already caught a test that passed
    // vacuously only via an EOF error -- see task-6b-report.md).
    base::IoBuf buf;
    io::BufSink sink{&buf};
    sink.Write(io::Buffer(peer_socket.captured));
    io::BufSource source{&buf};
    JournalReader reader{&source, 0};

    constexpr uint64_t kSeed = 1;  // matches peer_config.start_partial_sync_at
    TransactionReader tx_reader{kSeed - 1, /*peer_mode=*/true};
    ExecutionState read_cntx;
    TransactionData tx_data;

    ASSERT_TRUE(tx_reader.NextTxData(&reader, &read_cntx, &tx_data));  // CMD:SET a 1 (LSN1)
    EXPECT_EQ(Op::COMMAND, tx_data.opcode);

    ASSERT_TRUE(tx_reader.NextTxData(&reader, &read_cntx, &tx_data));  // LSN2's drop-path marker
    ASSERT_EQ(Op::LSN, tx_data.opcode);
    EXPECT_EQ(2u, tx_data.lsn);

    ASSERT_TRUE(tx_reader.NextTxData(&reader, &read_cntx, &tx_data));  // the write-path marker
    ASSERT_EQ(Op::LSN, tx_data.opcode);
    EXPECT_EQ(4u, tx_data.lsn);

    ASSERT_TRUE(tx_reader.NextTxData(&reader, &read_cntx, &tx_data));  // CMD:SET e 5 (LSN5)
    EXPECT_EQ(Op::COMMAND, tx_data.opcode);

    ASSERT_TRUE(tx_reader.NextTxData(&reader, &read_cntx, &tx_data));  // CMD:SET f 6 (LSN6)
    EXPECT_EQ(Op::COMMAND, tx_data.opcode);

    EXPECT_TRUE(read_cntx.IsRunning());  // exactly 5 entries consumed cleanly; no DCHECK abort
                                         // and no parse error along the way.
  });
}

// drakeydb: Phase 3 T6b -- proves NextTxData's non-peer path is untouched: an Op::LSN marker that
// does NOT match the running count still trips DCHECK_EQ (tx_executor.cc), exactly as upstream.
// This is the byte-identical-outside-peer-mode guarantee's falsifying counterpart to
// PeerModeGapMarkerCoalescesAndTransactionReaderAdoptsCleanly above (which proves peer mode
// *adopts* a mismatched marker instead of flagging it): together they pin that the new branch
// really is conditioned on peer_mode, not a blanket replacement of the old check.
//
// A death test, not a plain assertion: DCHECK_EQ aborts the process in a DCHECK-enabled build
// (this is one -- see CLAUDE.md's build instructions), so "does the check still fire" can only be
// observed by expecting that abort; a build with DCHECKs compiled out could not tell a working
// check apart from a silently-removed one. No fixture/proactor pool is used here (unlike the
// tests above): gtest's death tests fork() the process, and this statement needs none of that
// machinery, so avoiding it sidesteps any question about forking a multi-fibered process.
//
// drakeydb: fix-round-1 (Q2) -- the death-test pattern used to be "" (matches any non-zero exit),
// which would also have matched the DCHECK_NE(dest->lsn, 0u) two lines above the intended
// DCHECK_EQ, or any other unrelated abort in this statement -- proving only that *something* died,
// not that the specific check under test fired. Matched now on the exact "Check failed:
// dest->lsn == " prefix NextTxData's DCHECK_EQ(dest->lsn, *lsn_) renders (observed verbatim
// during this fix's own falsification pass -- see task-6b-report.md).
//
// Falsifying (verified by hand -- see task-6b-report.md): constructing tx_reader with
// peer_mode=true turns this from a crash into a silent, successful adoption -- EXPECT_DEATH then
// fails because the statement did not die.
TEST(JournalDeathTest, NonPeerModeStillDchecksMismatchedLsnMarker) {
  StoredSlices slices{};
  auto slice = [v = &slices](auto... ss) { return StoreSlice(v, ss...); };
  using Payload = Entry::Payload;

  base::IoBuf buf;
  io::BufSink sink{&buf};
  JournalWriter writer{&sink};
  writer.Write(Entry{0, Op::COMMAND, 0, nullopt, Payload("SET", slice("k", "v"))});
  // Deliberately wrong: the COMMAND above advances the reader's internal count to 1, not 99.
  writer.Write(Entry{Op::LSN, /*lsn=*/99});

  io::BufSource source{&buf};
  JournalReader reader{&source, 0};
  TransactionReader tx_reader{0};  // peer_mode defaults to false
  ExecutionState cntx;
  TransactionData tx_data;

  ASSERT_TRUE(tx_reader.NextTxData(&reader, &cntx, &tx_data));  // consumes the COMMAND; lsn_ -> 1
  ASSERT_EQ(Op::COMMAND, tx_data.opcode);

  EXPECT_DEATH(tx_reader.NextTxData(&reader, &cntx, &tx_data), "Check failed: dest->lsn == ");
}

// drakeydb: Phase 3 T6b fix-round-1 (C2) -- proves a peer link whose entries are ALL dropped
// still eventually informs the receiver of the master's true LSN, and keeps doing so over time
// rather than firing once and going silent forever. Without ConsumeJournalChange's drop-path
// block (streamer.cc), ShouldWrite's early return precedes BOTH marker blocks -- the
// gap-correction one added earlier in this task and the pre-existing write-path periodic one --
// so a 100%-filtered link (the steady state for a read-mostly node whose peer writes heavily, in
// a mesh -- not a corner case) would emit nothing at all, ever: journal_rec_executed_ would never
// advance while the master's true LSN raced ahead, and the eventual reconnect (or ring-buffer
// eviction before it) would force a full resync whose last-loaded-wins merge (rdb_load.cc) can
// resurrect stale values.
//
// drakeydb: Phase 3 T6b fix-round-2 -- the original round-1 version of this test proved "keeps
// refreshing" with a real ~4.5s sleep between the two RecordEntry calls, needed to clear
// last_lsn_time_'s then-hard-coded 3-second throttle window despite time_t's 1-second truncation
// (3.1s alone was observed to flake on exactly that boundary -- see task-6b-report.md's original
// round). peer_config.lsn_marker_throttle_sec = 0 below disables the throttle outright (see its
// own doc comment in streamer.h and ConsumeJournalChange's special case in streamer.cc), so both
// drops get their own marker unconditionally -- no sleep, no wall-clock dependency of any kind,
// and no throttle-window race for a slow or loaded runner (this project's Sanitizers CI matrix,
// for one) to lose.
//
// Falsifying (verified by hand -- see task-6b-report.md): removing the drop-path block entirely
// makes the EXPECT_EQ below fail (empty capture vs. the two expected markers).
TEST_F(JournalStreamerPeerFilterTest, PeerModeFullyFilteredLinkStillEmitsResolutionMarker) {
  constexpr uint32_t kPeerIdx = 5;  // some peer's PeerRegistry index; != PeerRegistry::kSelfIdx.
  pp_->at(0)->Await([&] {
    ExecutionState peer_cntx;
    JournalStreamer::Config peer_config;
    peer_config.peer_mode = true;
    peer_config.lsn_marker_throttle_sec = 0;  // fix-round-2: always fire -- see header comment.
    CapturingFiberSocket peer_socket;
    JournalStreamer peer_streamer(&peer_cntx, peer_config);
    peer_streamer.Start(&peer_socket);  // live listener; start_partial_sync_at defaults to 0.

    array<string_view, 2> set_a{"a", "1"};
    RecordEntry(0, Op::COMMAND, 0, nullopt,
                Entry::Payload{"SET", ArgSlice{set_a.data(), set_a.size()}},
                kPeerIdx);  // LSN1: peer -- dropped; the throttle is disabled, so this gets its
                            // own marker unconditionally, not merely because it's the first check.

    array<string_view, 2> set_b{"b", "2"};
    RecordEntry(0, Op::COMMAND, 0, nullopt,
                Entry::Payload{"SET", ArgSlice{set_b.data(), set_b.size()}},
                kPeerIdx);  // LSN2: peer -- also dropped, immediately after LSN1 with no sleep in
                            // between; proves the mechanism keeps firing on every eligible entry
                            // instead of firing once and going silent, without depending on real
                            // elapsed wall-clock time to prove it.

    peer_streamer.Cancel();

    // One marker per drop here (the throttle is disabled), each carrying that drop's own true LSN
    // directly -- not LSN - 1: unlike the write-path marker, neither drop has a following write of
    // its own to set up for (see ConsumeJournalChange).
    std::vector<std::string> expected = {"LSN:1", "LSN:2"};
    EXPECT_EQ(expected, JournalStreamerPeerFilterTest::Decode(peer_socket.captured));
  });
}

// drakeydb: Phase 3 T6b fix-round-1 (minor) -- pins the seeding fix in JournalStreamer's
// constructor (streamer.cc): a partial resume starting above LSN 1 -- the common case, since a
// reconnecting peer almost never resumes from LSN 1 -- must not spuriously mark the very first
// entry it sends as following a gap merely because that entry's LSN isn't 1.
//
// Falsifying (verified by hand -- see task-6b-report.md): reverting the constructor to always
// seed last_lsn_writen_ to 0 makes the EXPECT_EQ below fail (an extra "LSN:3" precedes
// "CMD:SET d 4").
TEST_F(JournalStreamerPeerFilterTest, PeerModePartialResumeAboveLsn1EmitsNoSpuriousFirstMarker) {
  pp_->at(0)->Await([&] {
    // Three unrelated entries to advance the journal past LSN 1 -- content doesn't matter, only
    // that they occupy LSN1-3 -- so the resume below can legitimately start at LSN 4. None of the
    // three are ever sent.
    for (int i = 0; i < 3; ++i) {
      array<string_view, 2> set_x{"x", "1"};
      RecordEntry(0, Op::COMMAND, 0, nullopt,
                  Entry::Payload{"SET", ArgSlice{set_x.data(), set_x.size()}},
                  PeerRegistry::kSelfIdx);
    }

    array<string_view, 2> set_d{"d", "4"};
    RecordEntry(0, Op::COMMAND, 0, nullopt,
                Entry::Payload{"SET", ArgSlice{set_d.data(), set_d.size()}},
                PeerRegistry::kSelfIdx);  // LSN4: self -- the first entry this resume actually
                                          // sends; must NOT be preceded by a marker.

    ASSERT_EQ(5u, GetLsn());

    ExecutionState peer_cntx;
    JournalStreamer::Config peer_config;
    peer_config.start_partial_sync_at = 4;  // resumes exactly at the one entry we want sent.
    peer_config.peer_mode = true;
    CapturingFiberSocket peer_socket;
    TestPartialSyncStreamer peer_streamer(&peer_cntx, peer_config);
    peer_streamer.Start(&peer_socket);
    ASSERT_TRUE(peer_streamer.MaybePartialStreamLSNs());
    peer_streamer.Cancel();

    std::vector<std::string> expected = {"CMD:SET d 4"};
    EXPECT_EQ(expected, JournalStreamerPeerFilterTest::Decode(peer_socket.captured));
  });
}

}  // namespace journal
}  // namespace dfly
