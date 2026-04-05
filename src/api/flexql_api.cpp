#include "flexql/flexql.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "flexql/network_protocol.hpp"

struct FlexQL {
  int socket_fd = -1;
  std::mutex mutex;
};

namespace {

void SetErrorMessage(char **errmsg, const std::string &msg) {
  if (errmsg == nullptr) {
    return;
  }
  char *buffer = static_cast<char *>(std::malloc(msg.size() + 1));
  if (buffer == nullptr) {
    *errmsg = nullptr;
    return;
  }
  std::memcpy(buffer, msg.c_str(), msg.size());
  buffer[msg.size()] = '\0';
  *errmsg = buffer;
}

int ConnectToServer(const char *host, int port) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  addrinfo *result = nullptr;
  const std::string port_str = std::to_string(port);
  if (getaddrinfo(host, port_str.c_str(), &hints, &result) != 0) {
    return -1;
  }

  int fd = -1;
  for (addrinfo *rp = result; rp != nullptr; rp = rp->ai_next) {
    fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
    if (fd < 0) {
      continue;
    }
    if (connect(fd, rp->ai_addr, rp->ai_addrlen) == 0) {
      break;
    }
    close(fd);
    fd = -1;
  }

  freeaddrinfo(result);
  return fd;
}

}  // namespace

extern "C" {

int flexql_open(const char *host, int port, FlexQL **db) {
  if (host == nullptr || db == nullptr || port <= 0 || port > 65535) {
    return FLEXQL_ERROR;
  }

  const int fd = ConnectToServer(host, port);
  if (fd < 0) {
    return FLEXQL_ERROR;
  }

  auto handle = std::make_unique<FlexQL>();
  handle->socket_fd = fd;
  *db = handle.release();
  return FLEXQL_OK;
}

int flexql_close(FlexQL *db) {
  if (db == nullptr) {
    return FLEXQL_ERROR;
  }

  int fd = -1;
  {
    std::lock_guard lock(db->mutex);
    fd = db->socket_fd;
    db->socket_fd = -1;
  }

  if (fd >= 0) {
    shutdown(fd, SHUT_RDWR);
    close(fd);
  }

  delete db;
  return FLEXQL_OK;
}

int flexql_exec(
    FlexQL *db,
    const char *sql,
    int (*callback)(void *, int, char **, char **),
    void *arg,
    char **errmsg) {
  if (errmsg != nullptr) {
    *errmsg = nullptr;
  }

  if (db == nullptr || sql == nullptr || db->socket_fd < 0) {
    SetErrorMessage(errmsg, "Invalid database handle");
    return FLEXQL_ERROR;
  }

  std::lock_guard lock(db->mutex);

  if (!flexql::SendString(db->socket_fd, sql)) {
    SetErrorMessage(errmsg, "Failed to send SQL to server");
    return FLEXQL_ERROR;
  }

  int32_t status = flexql::kStatusError;
  std::string server_err;
  if (!flexql::RecvInt32(db->socket_fd, &status) || !flexql::RecvString(db->socket_fd, &server_err)) {
    SetErrorMessage(errmsg, "Failed to read response from server");
    return FLEXQL_ERROR;
  }

  if (status != flexql::kStatusOk) {
    SetErrorMessage(errmsg, server_err.empty() ? "Execution failed" : server_err);
    return FLEXQL_ERROR;
  }

  int32_t column_count = 0;
  if (!flexql::RecvInt32(db->socket_fd, &column_count) || column_count < 0) {
    SetErrorMessage(errmsg, "Invalid response header");
    return FLEXQL_ERROR;
  }

  std::vector<std::string> column_names(static_cast<size_t>(column_count));
  for (int32_t i = 0; i < column_count; ++i) {
    if (!flexql::RecvString(db->socket_fd, &column_names[static_cast<size_t>(i)])) {
      SetErrorMessage(errmsg, "Failed to read column names");
      return FLEXQL_ERROR;
    }
  }

  bool keep_callback = true;

  while (true) {
    int32_t has_row = 0;
    if (!flexql::RecvInt32(db->socket_fd, &has_row)) {
      SetErrorMessage(errmsg, "Failed to read row marker");
      return FLEXQL_ERROR;
    }

    if (has_row == 0) {
      break;
    }

    std::vector<std::string> row_values(static_cast<size_t>(column_count));
    for (int32_t i = 0; i < column_count; ++i) {
      if (!flexql::RecvString(db->socket_fd, &row_values[static_cast<size_t>(i)])) {
        SetErrorMessage(errmsg, "Failed to read row value");
        return FLEXQL_ERROR;
      }
    }

    if (callback != nullptr && keep_callback) {
      std::vector<char *> value_ptrs(static_cast<size_t>(column_count));
      std::vector<char *> name_ptrs(static_cast<size_t>(column_count));

      for (int32_t i = 0; i < column_count; ++i) {
        value_ptrs[static_cast<size_t>(i)] = row_values[static_cast<size_t>(i)].empty()
                                                 ? const_cast<char *>("")
                                                 : row_values[static_cast<size_t>(i)].data();
        name_ptrs[static_cast<size_t>(i)] = column_names[static_cast<size_t>(i)].empty()
                                                ? const_cast<char *>("")
                                                : column_names[static_cast<size_t>(i)].data();
      }

      const int cb_rc = callback(arg, column_count, value_ptrs.data(), name_ptrs.data());
      if (cb_rc == 1) {
        keep_callback = false;
      }
    }
  }

  return FLEXQL_OK;
}

void flexql_free(void *ptr) {
  std::free(ptr);
}

}  // extern "C"
