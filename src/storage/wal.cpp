#include "flexql/wal.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <vector>

namespace {

bool WriteAll(int fd, const void *data, size_t size) {
  const char *ptr = static_cast<const char *>(data);
  size_t written = 0;
  while (written < size) {
    const ssize_t n = write(fd, ptr + written, size - written);
    if (n <= 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    written += static_cast<size_t>(n);
  }
  return true;
}

ssize_t ReadWithRetry(int fd, void *buf, size_t size) {
  while (true) {
    const ssize_t n = read(fd, buf, size);
    if (n < 0 && errno == EINTR) {
      continue;
    }
    return n;
  }
}

enum class ReadStatus {
  Ok,
  Eof,
  Truncated,
  Error,
};

ReadStatus ReadExactWithTailSupport(int fd, void *data, size_t size) {
  char *ptr = static_cast<char *>(data);
  size_t read_total = 0;

  while (read_total < size) {
    const ssize_t n = ReadWithRetry(fd, ptr + read_total, size - read_total);
    if (n == 0) {
      return read_total == 0 ? ReadStatus::Eof : ReadStatus::Truncated;
    }
    if (n < 0) {
      return ReadStatus::Error;
    }
    read_total += static_cast<size_t>(n);
  }
  return ReadStatus::Ok;
}

}  // namespace

namespace flexql {

WriteAheadLog::WriteAheadLog(std::string path) : path_(std::move(path)), fd_(-1) {}

WriteAheadLog::~WriteAheadLog() {
  if (fd_ >= 0) {
    close(fd_);
    fd_ = -1;
  }
}

bool WriteAheadLog::Open(std::string *error) {
  std::lock_guard lock(mutex_);
  if (fd_ >= 0) {
    return true;
  }

  fd_ = open(path_.c_str(), O_CREAT | O_WRONLY | O_APPEND, 0644);
  if (fd_ < 0) {
    if (error != nullptr) {
      *error = "Failed to open WAL file: " + path_;
    }
    return false;
  }
  return true;
}

bool WriteAheadLog::AppendRecord(const std::string &sql, std::string *error) {
  if (sql.size() > UINT32_MAX) {
    if (error != nullptr) {
      *error = "WAL record too large";
    }
    return false;
  }

  std::lock_guard lock(mutex_);
  if (fd_ < 0) {
    if (error != nullptr) {
      *error = "WAL is not open";
    }
    return false;
  }

  const uint32_t len = static_cast<uint32_t>(sql.size());
  if (!WriteAll(fd_, &len, sizeof(len)) || !WriteAll(fd_, sql.data(), sql.size())) {
    if (error != nullptr) {
      *error = "Failed to write WAL record";
    }
    return false;
  }

  if (fsync(fd_) != 0) {
    if (error != nullptr) {
      *error = "Failed to fsync WAL";
    }
    return false;
  }

  return true;
}

bool WriteAheadLog::Replay(const std::function<bool(const std::string &, std::string *)> &apply, std::string *error) {
  const int replay_fd = open(path_.c_str(), O_RDONLY);
  if (replay_fd < 0) {
    if (errno == ENOENT) {
      return true;
    }
    if (error != nullptr) {
      *error = "Failed to open WAL for replay";
    }
    return false;
  }

  while (true) {
    uint32_t len = 0;
    const ReadStatus len_status = ReadExactWithTailSupport(replay_fd, &len, sizeof(len));
    if (len_status == ReadStatus::Eof) {
      break;
    }
    if (len_status == ReadStatus::Truncated) {
      // Ignore truncated tail record for crash tolerance.
      break;
    }
    if (len_status == ReadStatus::Error) {
      close(replay_fd);
      if (error != nullptr) {
        *error = "Failed while reading WAL length";
      }
      return false;
    }

    if (len > 64U * 1024U * 1024U) {
      // Treat unreasonable length as a corrupted tail and stop replay.
      break;
    }

    std::string sql;
    sql.resize(len);
    if (len > 0) {
      const ReadStatus payload_status = ReadExactWithTailSupport(replay_fd, sql.data(), len);
      if (payload_status == ReadStatus::Truncated || payload_status == ReadStatus::Eof) {
        // Ignore truncated tail record for crash tolerance.
        break;
      }
      if (payload_status == ReadStatus::Error) {
        close(replay_fd);
        if (error != nullptr) {
          *error = "Failed while reading WAL payload";
        }
        return false;
      }
    }

    std::string apply_error;
    if (!apply(sql, &apply_error)) {
      close(replay_fd);
      if (error != nullptr) {
        *error = "WAL replay failed: " + apply_error;
      }
      return false;
    }
  }

  close(replay_fd);
  return true;
}

}  // namespace flexql
