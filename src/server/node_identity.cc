// Copyright 2026, drakeydb authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "server/node_identity.h"

#include <absl/random/random.h>
#include <absl/strings/ascii.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>
#include <unistd.h>

#include <cstdlib>
#include <filesystem>
#include <memory>
#include <system_error>
#include <vector>

#include "base/flags.h"
#include "base/logging.h"
#include "core/detail/gen_utils.h"
#include "io/file.h"
#include "io/file_util.h"
#include "server/detail/snapshot_storage.h"

ABSL_FLAG(std::string, node_uuid, "",
          "Override the persistent node UUID (testing). Must be a valid 36-character uuid; "
          "never persisted - the node runs with an ephemeral identity.");

namespace dfly {

namespace fs = std::filesystem;

std::string GenerateNodeUuid() {
  absl::InsecureBitGen gen;
  std::string hex = GetRandomHex(gen, 32);
  hex[12] = '4';                                 // version 4
  hex[16] = "89ab"[absl::Uniform(gen, 0u, 4u)];  // RFC-4122 variant
  return absl::StrCat(hex.substr(0, 8), "-", hex.substr(8, 4), "-", hex.substr(12, 4), "-",
                      hex.substr(16, 4), "-", hex.substr(20, 12));
}

bool IsValidNodeUuid(std::string_view uuid) {
  if (uuid.size() != 36)
    return false;
  for (size_t i = 0; i < uuid.size(); ++i) {
    if (i == 8 || i == 13 || i == 18 || i == 23) {
      if (uuid[i] != '-')
        return false;
    } else if (!absl::ascii_isxdigit(static_cast<unsigned char>(uuid[i]))) {
      return false;
    }
  }
  return true;
}

std::string NormalizeNodeUuid(std::string_view uuid) {
  DCHECK(IsValidNodeUuid(uuid));
  return absl::AsciiStrToLower(uuid);
}

bool ParseReplconfUuidReply(std::string_view reply, std::string* master_uuid, uint64_t* master_ms) {
  std::vector<std::string_view> tokens = absl::StrSplit(reply, ' ', absl::SkipEmpty());
  if (tokens.empty() || !IsValidNodeUuid(tokens[0]))
    return false;
  uint64_t ms = 0;
  if (tokens.size() >= 2 && !absl::SimpleAtoi(tokens[1], &ms))
    return false;
  *master_uuid = NormalizeNodeUuid(tokens[0]);
  *master_ms = ms;
  return true;
}

namespace {

// Create a directory and all its parents if they don't exist. Independent copy of the
// file-local helper at server/detail/save_stages_controller.cc:40 (that file is not modified by
// this fork).
std::error_code CreateDirs(fs::path dir_path) {
  std::error_code ec;
  fs::file_status dir_status = fs::status(dir_path, ec);
  if (ec == std::errc::no_such_file_or_directory) {
    fs::create_directories(dir_path, ec);
    if (!ec)
      dir_status = fs::status(dir_path, ec);
  }
  return ec;
}

// Writes `uuid` to `file` via a temp file plus atomic rename, creating `dir` first if needed.
// The temp file name carries a pid suffix because parallel ctest/pytest processes can share a
// cwd. Returns false on any failure (directory creation, open, write, or rename), in which case
// it best-effort removes the temp file.
//
// Deliberately no fsync: the worst case after a crash or power loss is a *missing* file, never a
// torn one, since rename(2) is atomic and the old file (if any) is never touched in place. A
// missing file just falls back to the "generate a new identity" path on the next boot.
bool PersistUuid(const fs::path& dir, const fs::path& file, std::string_view uuid) {
  std::error_code ec = CreateDirs(dir);
  if (ec) {
    LOG(ERROR) << "Could not create directory " << dir << ": " << ec.message();
    return false;
  }

  fs::path tmp_file{file};
  tmp_file += ".tmp.";
  tmp_file += std::to_string(getpid());

  io::Result<io::WriteFile*> res = io::OpenWrite(tmp_file.string());
  if (!res) {
    LOG(ERROR) << "Failed to open " << tmp_file << " with error: " << res.error().message();
    return false;
  }
  std::unique_ptr<io::WriteFile> wf(res.value());

  std::error_code write_ec = wf->Write(absl::StrCat(uuid, "\n"));
  if (write_ec) {
    LOG(ERROR) << "Failed to write " << tmp_file << " with error: " << write_ec.message();
    wf->Close();
    fs::remove(tmp_file, ec);
    return false;
  }

  write_ec = wf->Close();
  if (write_ec) {
    LOG(WARNING) << "Failed to close " << tmp_file << " with error: " << write_ec.message();
  }

  fs::rename(tmp_file, file, ec);
  if (ec) {
    LOG(ERROR) << "Failed to rename " << tmp_file << " to " << file << ": " << ec.message();
    fs::remove(tmp_file, ec);
    return false;
  }
  return true;
}

}  // namespace

io::Result<NodeIdentity> LoadOrCreateNodeIdentity(std::string_view dir,
                                                  std::string_view override_uuid) {
  if (!override_uuid.empty()) {
    if (!IsValidNodeUuid(override_uuid)) {
      LOG(ERROR) << "--node_uuid is not a valid uuid: " << override_uuid;
      return nonstd::make_unexpected(std::make_error_code(std::errc::invalid_argument));
    }
    return NodeIdentity{NormalizeNodeUuid(override_uuid), true};
  }
  if (detail::IsCloudPath(dir)) {
    LOG(WARNING) << "--dir is a cloud path; node identity is ephemeral and will not survive "
                    "a restart";
    return NodeIdentity{GenerateNodeUuid(), true};
  }

  fs::path dir_path{std::string{dir}};  // "" => cwd-relative, same as snapshot paths
  fs::path file_path = dir_path / kNodeUuidFileName;

  io::Result<std::string> contents = io::ReadFileToString(file_path.string());
  if (contents) {
    std::string trimmed{absl::StripAsciiWhitespace(*contents)};
    if (!IsValidNodeUuid(trimmed)) {
      LOG(ERROR) << "Corrupt node uuid file " << file_path
                 << "; delete the file to generate a new identity";
      return nonstd::make_unexpected(std::make_error_code(std::errc::illegal_byte_sequence));
    }
    return NodeIdentity{NormalizeNodeUuid(trimmed), false};
  }
  if (contents.error() != std::errc::no_such_file_or_directory) {
    LOG(ERROR) << "Could not read node uuid file " << file_path << ": "
               << contents.error().message();
    return nonstd::make_unexpected(contents.error());
  }

  NodeIdentity id{GenerateNodeUuid(), false};
  if (!PersistUuid(dir_path, file_path, id.uuid)) {
    LOG(WARNING) << "Could not persist node uuid to " << file_path
                 << "; node identity is ephemeral and will not survive a restart";
    id.ephemeral = true;
  }
  return id;
}

NodeIdentity InitNodeIdentityOrExit(std::string_view dir) {
  auto res = LoadOrCreateNodeIdentity(dir, absl::GetFlag(FLAGS_node_uuid));
  if (!res) {
    LOG(ERROR) << "Failed to initialize node identity, exiting";
    exit(1);
  }
  return *res;
}

}  // namespace dfly
