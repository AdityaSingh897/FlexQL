#ifndef FLEXQL_NETWORK_PROTOCOL_HPP
#define FLEXQL_NETWORK_PROTOCOL_HPP

#include <cstdint>
#include <string>

namespace flexql {

constexpr int32_t kStatusOk = 0;
constexpr int32_t kStatusError = 1;

bool SendInt32(int fd, int32_t value);
bool RecvInt32(int fd, int32_t *value);

bool SendString(int fd, const std::string &value);
bool RecvString(int fd, std::string *value);

}  // namespace flexql

#endif
