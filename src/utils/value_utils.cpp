#include "flexql/value_utils.hpp"

#include <cmath>
#include <cstdint>
#include <iomanip>
#include <sstream>

#include "flexql/datetime_utils.hpp"
#include "flexql/string_utils.hpp"

namespace {

std::string Unquote(const std::string &input) {
  if (input.size() >= 2 &&
      ((input.front() == '\'' && input.back() == '\'') || (input.front() == '"' && input.back() == '"'))) {
    return input.substr(1, input.size() - 2);
  }
  return input;
}

std::string DecimalToString(double value) {
  std::ostringstream oss;
  oss << std::setprecision(15) << value;
  std::string out = oss.str();
  if (out.find('.') != std::string::npos) {
    while (!out.empty() && out.back() == '0') {
      out.pop_back();
    }
    if (!out.empty() && out.back() == '.') {
      out.push_back('0');
    }
  }
  return out;
}

}  // namespace

namespace flexql {

std::optional<ColumnType> ParseColumnType(const std::string &token) {
  const auto upper = ToUpper(token);
  if (upper == "INT") {
    return ColumnType::Int;
  }
  if (upper == "DECIMAL") {
    return ColumnType::Decimal;
  }
  if (upper == "VARCHAR" || upper == "TEXT") {
    return ColumnType::Varchar;
  }
  if (upper == "DATETIME") {
    return ColumnType::DateTime;
  }
  return std::nullopt;
}

bool ParseLiteralAsType(const std::string &literal, ColumnType type, ScalarValue *out, std::string *error) {
  const std::string normalized = Trim(literal);
  out->type = type;

  try {
    switch (type) {
      case ColumnType::Int: {
        size_t idx = 0;
        const auto parsed = std::stoll(normalized, &idx, 10);
        if (idx != normalized.size()) {
          if (error != nullptr) {
            *error = "Invalid INT literal: " + literal;
          }
          return false;
        }
        out->int_value = parsed;
        return true;
      }
      case ColumnType::Decimal: {
        size_t idx = 0;
        const auto parsed = std::stod(normalized, &idx);
        if (idx != normalized.size()) {
          if (error != nullptr) {
            *error = "Invalid DECIMAL literal: " + literal;
          }
          return false;
        }
        out->decimal_value = parsed;
        return true;
      }
      case ColumnType::Varchar: {
        out->string_value = Unquote(normalized);
        return true;
      }
      case ColumnType::DateTime: {
        const auto maybe_epoch = ParseDateTimeToEpochSeconds(Unquote(normalized));
        if (!maybe_epoch.has_value()) {
          if (error != nullptr) {
            *error = "Invalid DATETIME literal: " + literal;
          }
          return false;
        }
        out->int_value = *maybe_epoch;
        return true;
      }
    }
  } catch (...) {
    if (error != nullptr) {
      *error = "Failed to parse literal: " + literal;
    }
    return false;
  }

  if (error != nullptr) {
    *error = "Unsupported literal type";
  }
  return false;
}

std::string ScalarToString(const ScalarValue &value) {
  switch (value.type) {
    case ColumnType::Int:
      return std::to_string(value.int_value);
    case ColumnType::Decimal:
      return DecimalToString(value.decimal_value);
    case ColumnType::Varchar:
      return value.string_value;
    case ColumnType::DateTime:
      return EpochSecondsToDateTimeString(value.int_value);
  }
  return "";
}

bool CompareScalars(const ScalarValue &left, CompareOp op, const ScalarValue &right) {
  if (left.type != right.type) {
    return false;
  }

  switch (left.type) {
    case ColumnType::Int:
    case ColumnType::DateTime: {
      const int64_t l = left.int_value;
      const int64_t r = right.int_value;
      switch (op) {
        case CompareOp::Eq:
          return l == r;
        case CompareOp::Ne:
          return l != r;
        case CompareOp::Lt:
          return l < r;
        case CompareOp::Le:
          return l <= r;
        case CompareOp::Gt:
          return l > r;
        case CompareOp::Ge:
          return l >= r;
      }
      break;
    }
    case ColumnType::Decimal: {
      const double l = left.decimal_value;
      const double r = right.decimal_value;
      switch (op) {
        case CompareOp::Eq:
          return l == r;
        case CompareOp::Ne:
          return l != r;
        case CompareOp::Lt:
          return l < r;
        case CompareOp::Le:
          return l <= r;
        case CompareOp::Gt:
          return l > r;
        case CompareOp::Ge:
          return l >= r;
      }
      break;
    }
    case ColumnType::Varchar: {
      const auto &l = left.string_value;
      const auto &r = right.string_value;
      switch (op) {
        case CompareOp::Eq:
          return l == r;
        case CompareOp::Ne:
          return l != r;
        case CompareOp::Lt:
          return l < r;
        case CompareOp::Le:
          return l <= r;
        case CompareOp::Gt:
          return l > r;
        case CompareOp::Ge:
          return l >= r;
      }
      break;
    }
  }
  return false;
}

std::string SerializeScalarForIndex(const ScalarValue &value) {
  switch (value.type) {
    case ColumnType::Int:
      return "I:" + std::to_string(value.int_value);
    case ColumnType::Decimal:
      return "D:" + DecimalToString(value.decimal_value);
    case ColumnType::Varchar:
      return "S:" + value.string_value;
    case ColumnType::DateTime:
      return "T:" + std::to_string(value.int_value);
  }
  return "";
}

}  // namespace flexql
