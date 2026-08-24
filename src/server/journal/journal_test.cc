#include <absl/cleanup/cleanup.h>
#include <absl/flags/reflection.h>
#include <absl/strings/str_join.h>
#include <sys/socket.h>

#include <array>
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

// drakeydb: Phase 3 T5 -- an in-memory util::FiberSocketBase double for the JournalStreamer tests
// below. JournalStreamer only ever calls AsyncWrite/AsyncWriteSome on its destination (never the
// sync Write/WriteSome path, and never reads), and io::AsyncSink::AsyncWrite (io.cc) is pure
// buffer bookkeeping with no ProactorBase dependency, so a synchronous, immediate, in-memory
// AsyncWriteSome needs no real proactor, fiber, or OS socket at all -- just a shard/journal
// thread-local, which the fixture below provides. Everything else is unreachable from
// JournalStreamer's write-only usage and is stubbed only to satisfy the abstract interface.
class CapturingFiberSocket : public util::FiberSocketBase {
 public:
  CapturingFiberSocket() : util::FiberSocketBase(nullptr) {
  }

  void AsyncWriteSome(const iovec* v, uint32_t len, io::AsyncProgressCb cb) override {
    size_t total = 0;
    for (uint32_t i = 0; i < len; ++i) {
      captured.append(reinterpret_cast<const char*>(v[i].iov_base), v[i].iov_len);
      total += v[i].iov_len;
    }
    cb(io::Result<size_t>(total));
  }

  io::Result<size_t> WriteSome(const iovec* v, uint32_t len) override {
    size_t total = 0;
    for (uint32_t i = 0; i < len; ++i) {
      captured.append(reinterpret_cast<const char*>(v[i].iov_base), v[i].iov_len);
      total += v[i].iov_len;
    }
    return total;
  }

  // Unused by these tests -- stubbed to satisfy FiberSocketBase's abstract interface.
  error_code Shutdown(int) override {
    return {};
  }
  AcceptResult Accept() override {
    return nonstd::make_unexpected(std::make_error_code(std::errc::not_supported));
  }
  error_code Connect(const endpoint_type&, std::function<void(int)>) override {
    return {};
  }
  error_code Close() override {
    return {};
  }
  bool IsOpen() const override {
    return true;
  }
  io::Result<size_t> RecvMsg(const msghdr&, int) override {
    return size_t{0};
  }
  io::Result<size_t> Recv(const io::MutableBytes&, int) override {
    return size_t{0};
  }
  void set_timeout(uint32_t) override {
  }
  uint32_t timeout() const override {
    return 0;
  }
  endpoint_type LocalEndpoint() const override {
    return {};
  }
  endpoint_type RemoteEndpoint() const override {
    return {};
  }
  void RegisterOnErrorCb(std::function<void(uint32_t)>) override {
  }
  void CancelOnErrorCb() override {
  }
  void AsyncReadSome(const iovec*, uint32_t, io::AsyncProgressCb) override {
  }
  void RegisterOnRecv(OnRecvCb) override {
  }
  void ResetOnRecvHook() override {
  }
  bool IsUDS() const override {
    return false;
  }
  native_handle_type native_handle() const override {
    return -1;
  }
  error_code Create(unsigned short) override {
    return {};
  }
  error_code Bind(const struct sockaddr*, unsigned) override {
    return {};
  }
  error_code Listen(unsigned) override {
    return {};
  }
  error_code Listen(uint16_t, unsigned) override {
    return {};
  }
  error_code ListenUDS(const char*, mode_t, unsigned) override {
    return {};
  }
  io::Result<size_t> TrySend(io::Bytes) override {
    return size_t{0};
  }
  io::Result<size_t> TrySend(const iovec*, uint32_t) override {
    return size_t{0};
  }
  io::Result<size_t> TryRecv(io::MutableBytes) override {
    return size_t{0};
  }

  std::string captured;
};

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
                kPeerIdx);  // LSN5: peer's re-recorded PING -- dropped for peer via the *origin*
                            // check (not opcode), kept full.

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

    std::vector<std::string> expected_peer = {"CMD:SET a 1", "PING", "CMD:SET d 4", "CMD:SET e 5"};
    std::vector<std::string> expected_full = {
        "CMD:SET a 1", "CMD:SET b 2", "CMD:DEL c",  "PING", "PING", absl::StrCat("ORIGIN:", kUuid),
        "CMD:SET d 4", "CMD:SET e 5", "CMD:SET f 6"};

    EXPECT_EQ(expected_peer, JournalStreamerPeerFilterTest::Decode(peer_socket.captured));
    EXPECT_EQ(expected_full, JournalStreamerPeerFilterTest::Decode(full_socket.captured));

    peer_streamer.Cancel();
    full_streamer.Cancel();
  });
}

}  // namespace journal
}  // namespace dfly
