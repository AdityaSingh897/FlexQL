#include "flexql/datetime_utils.hpp"

#include <chrono>
#include <ctime>
#include <iomanip>
#include <optional>
#include <sstream>

namespace {

bool ParseWithFormat(const std::string &value, const char *fmt, std::tm *out) {
  std::istringstream ss(value);
  ss >> std::get_time(out, fmt);
  return !ss.fail();
}

int64_t TmToEpochUtc(std::tm tm_value) {
#if defined(_WIN32)
  return static_cast<int64_t>(_mkgmtime(&tm_value));
#else
  return static_cast<int64_t>(timegm(&tm_value));
#endif
}

}  // namespace

namespace flexql {

std::optional<int64_t> ParseDateTimeToEpochSeconds(const std::string &value) {
  std::tm tm_value{};
  if (ParseWithFormat(value, "%Y-%m-%d %H:%M:%S", &tm_value) ||
      ParseWithFormat(value, "%Y-%m-%dT%H:%M:%S", &tm_value) ||
      ParseWithFormat(value, "%Y-%m-%d", &tm_value)) {
    return TmToEpochUtc(tm_value);
  }

  // Allow raw epoch integer values for convenience.
  try {
    size_t idx = 0;
    const auto parsed = std::stoll(value, &idx, 10);
    if (idx == value.size()) {
      return parsed;
    }
  } catch (...) {
  }

  return std::nullopt;
}

std::string EpochSecondsToDateTimeString(int64_t epoch_seconds) {
  std::time_t t = static_cast<std::time_t>(epoch_seconds);
  std::tm tm_value{};
#if defined(_WIN32)
  gmtime_s(&tm_value, &t);
#else
  gmtime_r(&t, &tm_value);
#endif
  char buffer[20] = {0};
  std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", &tm_value);
  return std::string(buffer);
}

int64_t CurrentEpochSeconds() {
  return std::chrono::duration_cast<std::chrono::seconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

}  // namespace flexql
