// Copyright 2026, drakeydb authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "server/node_identity.h"

#include <absl/cleanup/cleanup.h>
#include <absl/random/random.h>
#include <absl/strings/ascii.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstdlib>
#include <filesystem>
#include <system_error>
#include <vector>

#include "base/flags.h"
#include "base/logging.h"
#include "core/detail/gen_utils.h"
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

ReplconfUuidReplyStatus ParseReplconfUuidReply(std::string_view reply, std::string* master_uuid,
                                               uint64_t* master_ms) {
  if (reply == "OK")
    return ReplconfUuidReplyStatus::kUnsupported;

  std::vector<std::string_view> tokens = absl::StrSplit(reply, ' ', absl::SkipEmpty());
  if (tokens.empty() || !IsValidNodeUuid(tokens[0]))
    return ReplconfUuidReplyStatus::kMalformed;
  uint64_t ms = 0;
  if (tokens.size() >= 2 && !absl::SimpleAtoi(tokens[1], &ms))
    return ReplconfUuidReplyStatus::kMalformed;
  *master_uuid = NormalizeNodeUuid(tokens[0]);
  *master_ms = ms;
  return ReplconfUuidReplyStatus::kSuccess;
}

namespace {

std::error_code ErrnoError() {
  return std::error_code(errno, std::generic_category());
}

bool SyncFd(int fd, const fs::path& path) {
  while (fsync(fd) == -1) {
    if (errno == EINTR)
      continue;
    LOG(ERROR) << "Failed to sync " << path << ": " << ErrnoError().message();
    return false;
  }
  return true;
}

// Writes `uuid` through a unique temp file, syncs its data, atomically renames it, then syncs the
// parent directory. A true return means the identity and its directory entry are durable. Failures
// before rename remove the temp file; failures after rename may leave a valid final file but still
// return false because its durability could not be confirmed.
bool PersistUuid(const fs::path& dir, const fs::path& file, std::string_view uuid) {
  std::error_code ec;
  if (!dir.empty()) {
    fs::create_directories(dir, ec);
    if (ec) {
      LOG(ERROR) << "Could not create directory " << dir << ": " << ec.message();
      return false;
    }
  }

  std::string tmp_template = absl::StrCat(file.string(), ".tmp.XXXXXX");
  int fd = mkstemp(tmp_template.data());
  if (fd == -1) {
    LOG(ERROR) << "Failed to create temporary node uuid file next to " << file << ": "
               << ErrnoError().message();
    return false;
  }
  fs::path tmp_file{tmp_template};
  bool renamed = false;
  absl::Cleanup cleanup = [&] {
    if (fd != -1)
      close(fd);
    if (!renamed)
      fs::remove(tmp_file, ec);
  };

  std::string payload = absl::StrCat(uuid, "\n");
  size_t offset = 0;
  while (offset < payload.size()) {
    ssize_t written = write(fd, payload.data() + offset, payload.size() - offset);
    if (written == -1 && errno == EINTR)
      continue;
    if (written <= 0) {
      std::error_code write_ec =
          written == -1 ? ErrnoError() : std::make_error_code(std::errc::io_error);
      LOG(ERROR) << "Failed to write " << tmp_file << ": " << write_ec.message();
      return false;
    }
    offset += written;
  }

  if (fchmod(fd, 0644) == -1) {
    LOG(ERROR) << "Failed to set permissions on " << tmp_file << ": " << ErrnoError().message();
    return false;
  }
  if (!SyncFd(fd, tmp_file))
    return false;
  if (close(fd) == -1) {
    fd = -1;
    LOG(ERROR) << "Failed to close " << tmp_file << ": " << ErrnoError().message();
    return false;
  }
  fd = -1;

  if (rename(tmp_file.c_str(), file.c_str()) == -1) {
    LOG(ERROR) << "Failed to rename " << tmp_file << " to " << file << ": "
               << ErrnoError().message();
    return false;
  }
  renamed = true;

  fs::path parent_dir = dir.empty() ? fs::path{"."} : dir;
  int dir_fd = open(parent_dir.c_str(), O_RDONLY | O_DIRECTORY);
  if (dir_fd == -1) {
    LOG(ERROR) << "Failed to open node uuid directory " << parent_dir << ": "
               << ErrnoError().message();
    return false;
  }
  absl::Cleanup close_dir = [&] {
    if (dir_fd != -1)
      close(dir_fd);
  };
  if (!SyncFd(dir_fd, parent_dir))
    return false;
  if (close(dir_fd) == -1) {
    dir_fd = -1;
    LOG(ERROR) << "Failed to close node uuid directory " << parent_dir << ": "
               << ErrnoError().message();
    return false;
  }
  dir_fd = -1;
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
