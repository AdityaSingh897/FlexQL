#include "flexql/network_protocol.hpp"

#include <arpa/inet.h>
#include <sys/socket.h>

#include <vector>

namespace {

bool SendAll(int fd, const void *data, size_t size) {
  const char *ptr = static_cast<const char *>(data);
  size_t sent = 0;
  while (sent < size) {
    const ssize_t n = send(fd, ptr + sent, size - sent, 0);
    if (n <= 0) {
      return false;
    }
    sent += static_cast<size_t>(n);
  }
  return true;
}

bool RecvAll(int fd, void *data, size_t size) {
  char *ptr = static_cast<char *>(data);
  size_t received = 0;
  while (received < size) {
    const ssize_t n = recv(fd, ptr + received, size - received, 0);
    if (n <= 0) {
      return false;
    }
    received += static_cast<size_t>(n);
  }
  return true;
}

}  // namespace

namespace flexql {

bool SendInt32(int fd, int32_t value) {
  const int32_t net = htonl(value);
  return SendAll(fd, &net, sizeof(net));
}

bool RecvInt32(int fd, int32_t *value) {
  int32_t net = 0;
  if (!RecvAll(fd, &net, sizeof(net))) {
    return false;
  }
  *value = ntohl(net);
  return true;
}

bool SendString(int fd, const std::string &value) {
  if (!SendInt32(fd, static_cast<int32_t>(value.size()))) {
    return false;
  }
  if (value.empty()) {
    return true;
  }
  return SendAll(fd, value.data(), value.size());
}

bool RecvString(int fd, std::string *value) {
  int32_t len = 0;
  if (!RecvInt32(fd, &len)) {
    return false;
  }
  if (len < 0) {
    return false;
  }
  value->resize(static_cast<size_t>(len));
  if (len == 0) {
    return true;
  }
  return RecvAll(fd, value->data(), static_cast<size_t>(len));
}

}  // namespace flexql
