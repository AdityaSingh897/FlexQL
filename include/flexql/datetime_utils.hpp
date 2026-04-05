#ifndef FLEXQL_DATETIME_UTILS_HPP
#define FLEXQL_DATETIME_UTILS_HPP

#include <cstdint>
#include <optional>
#include <string>

namespace flexql {

std::optional<int64_t> ParseDateTimeToEpochSeconds(const std::string &value);
std::string EpochSecondsToDateTimeString(int64_t epoch_seconds);
int64_t CurrentEpochSeconds();

}  // namespace flexql

#endif
