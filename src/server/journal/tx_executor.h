// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <unordered_map>

#include "server/execution_state.h"
#include "server/journal/types.h"
#include "util/fibers/synchronization.h"

namespace dfly {

struct JournalReader;

// Coordinator for multi shard execution.
class MultiShardExecution {
 public:
  struct TxExecutionSync {
    util::fb2::Barrier barrier;
    std::atomic_uint32_t counter;
    util::fb2::BlockingCounter block;

    explicit TxExecutionSync(uint32_t counter)
        : barrier(counter), counter(counter), block(counter) {
    }
  };

  bool InsertTxToSharedMap(TxId txid, uint32_t shard_cnt);
  TxExecutionSync& Find(TxId txid);
  void Erase(TxId txid);
  void CancelAllBlockingEntities();

 private:
  util::fb2::Mutex map_mu;
  std::unordered_map<TxId, TxExecutionSync> tx_sync_execution;
  bool cancelled_{false};  // Protected by map_mu
};

// This class holds the commands of transaction in single shard.
// Once all commands were received, the transaction can be executed.
struct TransactionData {
  // Update the data from ParsedEntry
  void AddEntry(journal::ParsedEntry&& entry);

  bool IsGlobalCmd() const;

  TxId txid{0};
  DbIndex dbid{0};
  journal::ParsedEntry::CmdData command;

  journal::Op opcode;
  uint64_t lsn = 0;
};

// Utility for reading TransactionData from a journal reader.
// The journal stream can contain interleaved data for multiple multi transactions,
// expiries and out of order executed transactions that need to be grouped on the replica side.
struct TransactionReader {
  // drakeydb: Phase 3 T6b -- `peer_mode` selects how an arriving Op::LSN marker is handled (see
  // NextTxData): outside peer mode (the default -- every pre-existing caller, e.g.
  // incoming_slot_migration.cc, is unaffected), it is compared against the running count and
  // DCHECK_EQ'd, exactly as upstream. In peer mode it is instead adopted as authoritative -- see
  // streamer.cc's ConsumeJournalChange for why a peer link's stream can legitimately have gaps a
  // plain count can't self-correct.
  TransactionReader(std::optional<uint64_t> lsn = std::nullopt, bool peer_mode = false)
      : lsn_(lsn), peer_mode_(peer_mode) {
  }

  bool NextTxData(JournalReader* reader, ExecutionState* cntx, TransactionData* dest);

 private:
  std::optional<uint64_t> lsn_ = 0;
  bool peer_mode_ = false;
};

}  // namespace dfly
