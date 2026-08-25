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

// Writes `uuid` through a unique temp file, syncs its data, then publishes it at `file` via
// link() -- never rename() -- and syncs the parent directory. Unlike rename(), which always wins
// and silently overwrites, link() fails atomically with EEXIST if `file` already exists. That
// closes a TOCTOU where two processes booting concurrently against the same empty --dir would
// each see "no such file" on LoadOrCreateNodeIdentity's initial read, each generate a different
// uuid, and each have its own rename() unconditionally overwrite the other's -- leaving the loser
// running with an in-memory uuid that a restart, which only ever reads the file, would never
// reproduce.
//
// On success, returns the uuid now durably on disk at `file`: ordinarily `uuid` itself, but the
// winner's uuid instead when this call lost the link() race -- LoadOrCreateNodeIdentity adopts
// that value rather than keep the one it generated locally. Returns an error only for a genuine
// I/O failure (including a lost race whose winner's file cannot be read back); the caller
// downgrades that to an ephemeral identity, exactly as it did when this function still returned
// bool.
io::Result<std::string> PersistUuid(const fs::path& dir, const fs::path& file,
                                    std::string_view uuid) {
  std::error_code ec;
  if (!dir.empty()) {
    fs::create_directories(dir, ec);
    if (ec) {
      LOG(ERROR) << "Could not create directory " << dir << ": " << ec.message();
      return nonstd::make_unexpected(ec);
    }
  }

  std::string tmp_template = absl::StrCat(file.string(), ".tmp.XXXXXX");
  int fd = mkstemp(tmp_template.data());
  if (fd == -1) {
    LOG(ERROR) << "Failed to create temporary node uuid file next to " << file << ": "
               << ErrnoError().message();
    return nonstd::make_unexpected(ErrnoError());
  }
  fs::path tmp_file{tmp_template};
  // Unlike the old rename()-based publish, link() below never consumes the temp file's own
  // directory entry -- it only adds `file` as a second name for the same inode -- so tmp_file
  // must always be removed here, on every exit path: success, a lost race, or any error below.
  absl::Cleanup cleanup = [&] {
    if (fd != -1)
      close(fd);
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
      return nonstd::make_unexpected(write_ec);
    }
    offset += written;
  }

  if (fchmod(fd, 0644) == -1) {
    LOG(ERROR) << "Failed to set permissions on " << tmp_file << ": " << ErrnoError().message();
    return nonstd::make_unexpected(ErrnoError());
  }
  if (!SyncFd(fd, tmp_file))
    return nonstd::make_unexpected(std::make_error_code(std::errc::io_error));
  if (close(fd) == -1) {
    fd = -1;
    LOG(ERROR) << "Failed to close " << tmp_file << ": " << ErrnoError().message();
    return nonstd::make_unexpected(ErrnoError());
  }
  fd = -1;

  if (link(tmp_file.c_str(), file.c_str()) == -1) {
    if (errno != EEXIST) {
      LOG(ERROR) << "Failed to link " << tmp_file << " to " << file << ": "
                 << ErrnoError().message();
      return nonstd::make_unexpected(ErrnoError());
    }
    // Lost the race: some other process already published its own uuid at `file` first. The
    // temp file above is already fully written and fsynced, so -- exactly like the winner's own
    // temp file -- it could only ever have published a complete file; there is no torn-write
    // window to worry about on either side of this race. Adopt whatever is actually at `file`
    // instead of keeping the uuid generated locally: a restart would read the winner's file
    // anyway, so silently keeping ours would make this boot's identity unreproducible across a
    // restart -- exactly the bug this function exists to close.
    io::Result<std::string> winner = io::ReadFileToString(file.string());
    if (!winner) {
      LOG(ERROR) << "Lost the race to create " << file
                 << " but could not read the winner's uuid: " << winner.error().message();
      return nonstd::make_unexpected(winner.error());
    }
    std::string trimmed{absl::StripAsciiWhitespace(*winner)};
    if (!IsValidNodeUuid(trimmed)) {
      LOG(ERROR) << "Lost the race to create " << file
                 << " but its contents are not a valid uuid: " << *winner;
      return nonstd::make_unexpected(std::make_error_code(std::errc::illegal_byte_sequence));
    }
    // The winner's own PersistUuid call owns syncing the parent directory (it created the new
    // directory entry, not us), so this call returns here without repeating that sync.
    return NormalizeNodeUuid(trimmed);
  }

  fs::path parent_dir = dir.empty() ? fs::path{"."} : dir;
  int dir_fd = open(parent_dir.c_str(), O_RDONLY | O_DIRECTORY);
  if (dir_fd == -1) {
    LOG(ERROR) << "Failed to open node uuid directory " << parent_dir << ": "
               << ErrnoError().message();
    return nonstd::make_unexpected(ErrnoError());
  }
  absl::Cleanup close_dir = [&] {
    if (dir_fd != -1)
      close(dir_fd);
  };
  if (!SyncFd(dir_fd, parent_dir))
    return nonstd::make_unexpected(std::make_error_code(std::errc::io_error));
  if (close(dir_fd) == -1) {
    dir_fd = -1;
    LOG(ERROR) << "Failed to close node uuid directory " << parent_dir << ": "
               << ErrnoError().message();
    return nonstd::make_unexpected(ErrnoError());
  }
  dir_fd = -1;
  return std::string(uuid);
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
  io::Result<std::string> persisted = PersistUuid(dir_path, file_path, id.uuid);
  if (persisted) {
    // Ordinarily *persisted == id.uuid; if this call lost the create race inside PersistUuid, it
    // is instead the winner's uuid, and id must adopt it -- see PersistUuid's own comment.
    id.uuid = *persisted;
  } else {
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
