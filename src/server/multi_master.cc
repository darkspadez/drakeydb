// Copyright 2026, drakeydb authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "server/multi_master.h"

#include "absl/flags/declare.h"
#include "absl/flags/flag.h"
#include "absl/strings/str_cat.h"
#include "base/logging.h"
#include "facade/cmd_arg_parser.h"

ABSL_FLAG(bool, active_replica, false,
          "drakeydb: stay a writable master while replicating from the masters given to "
          "REPLICAOF / --replicaof (KeyDB active-replica). Boot-only.");
ABSL_FLAG(bool, multi_master, false,
          "drakeydb: let REPLICAOF attach several masters at once (fan-in). Requires "
          "--active_replica (KeyDB multi-master). Boot-only.");
ABSL_DECLARE_FLAG(std::string, cluster_mode);
ABSL_DECLARE_FLAG(std::string, tiered_prefix);
ABSL_DECLARE_FLAG(bool, experimental_cascaded_partial_sync);

namespace dfly {

bool IsActiveReplica() {
  return absl::GetFlag(FLAGS_active_replica);
}

bool IsMultiMaster() {
  return absl::GetFlag(FLAGS_multi_master);
}

bool ValidateMultiMasterFlags() {
  const bool active = absl::GetFlag(FLAGS_active_replica);
  if (absl::GetFlag(FLAGS_multi_master) && !active) {
    LOG(ERROR) << "--multi_master requires --active_replica";
    return false;
  }
  if (!active)
    return true;
  if (!absl::GetFlag(FLAGS_cluster_mode).empty()) {
    LOG(ERROR) << "--active_replica is incompatible with --cluster_mode";
    return false;
  }
  if (!absl::GetFlag(FLAGS_tiered_prefix).empty()) {
    LOG(ERROR) << "--active_replica is incompatible with tiering (--tiered_prefix)";
    return false;
  }
  if (absl::GetFlag(FLAGS_experimental_cascaded_partial_sync)) {
    LOG(ERROR) << "--active_replica is incompatible with --experimental_cascaded_partial_sync";
    return false;
  }
  return true;
}

nonstd::expected<PeerReplicaOfCmd, facade::ErrorReply> ParsePeerReplicaOfArgs(
    facade::ParsedArgs args) {
  PeerReplicaOfCmd cmd;
  facade::CmdArgParser parser(args);
  if (parser.Check("NO")) {
    parser.ExpectTag("ONE");
    cmd.kind = PeerReplicaOfCmd::Kind::kNoOne;
  } else {
    if (parser.Check("REMOVE"))
      cmd.kind = PeerReplicaOfCmd::Kind::kRemove;
    cmd.host = parser.Next<std::string>();
    cmd.port = parser.Next<facade::Positive<uint16_t>>("port is out of range");
    if (auto err = parser.TakeError(); err)
      return nonstd::make_unexpected(facade::ErrorReply("port is out of range"));
  }
  if (parser.HasNext())
    return nonstd::make_unexpected(
        facade::ErrorReply("slot ranges are not supported in active-replica mode"));
  if (auto err = parser.TakeError(); err)
    return nonstd::make_unexpected(err.MakeReply());
  return cmd;
}

std::string RenderPeerReplicationInfo(const std::vector<ReplicaSummary>& peers, bool multi_master,
                                      bool show_peer_lines) {
  std::string out = absl::StrCat("active_replica:1\r\nmulti_master:", multi_master ? 1 : 0,
                                 "\r\nconnected_masters:", peers.size(), "\r\n");
  if (!show_peer_lines)
    return out;
  for (size_t i = 0; i < peers.size(); ++i) {
    const ReplicaSummary& p = peers[i];
    absl::StrAppend(&out, "master", i, ":host=", p.host, ",port=", p.port,
                    ",link_status=", p.master_link_established ? "up" : "down",
                    ",last_io_seconds_ago=", p.master_last_io_sec,
                    ",sync_in_progress=", p.full_sync_in_progress ? 1 : 0);
    if (!p.master_node_uuid.empty())
      absl::StrAppend(&out, ",node_uuid=", p.master_node_uuid);
    absl::StrAppend(&out, "\r\n");
  }
  return out;
}

void PeerRegistry::Init(std::string_view self_uuid) {
  util::fb2::LockGuard lk(mu_);
  CHECK(idx_to_uuid_.empty()) << "PeerRegistry::Init() must be called exactly once";
  CHECK(uuid_to_idx_.empty()) << "PeerRegistry::Init() must be called exactly once";
  uuid_to_idx_.try_emplace(std::string(self_uuid), kSelfIdx);
  idx_to_uuid_.emplace_back(self_uuid);
}

uint32_t PeerRegistry::AddOrGet(std::string_view uuid) {
  util::fb2::LockGuard lk(mu_);
  auto [it, inserted] = uuid_to_idx_.try_emplace(std::string(uuid), idx_to_uuid_.size());
  if (inserted)
    idx_to_uuid_.emplace_back(uuid);
  return it->second;
}

std::optional<uint32_t> PeerRegistry::FindIdx(std::string_view uuid) const {
  util::fb2::LockGuard lk(mu_);
  auto it = uuid_to_idx_.find(uuid);
  if (it == uuid_to_idx_.end())
    return std::nullopt;
  return it->second;
}

std::string PeerRegistry::GetUuid(uint32_t idx) const {
  util::fb2::LockGuard lk(mu_);
  if (idx >= idx_to_uuid_.size())
    return "";
  return idx_to_uuid_[idx];
}

size_t PeerRegistry::Size() const {
  util::fb2::LockGuard lk(mu_);
  return idx_to_uuid_.size();
}

}  // namespace dfly
