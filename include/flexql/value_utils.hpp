#ifndef FLEXQL_VALUE_UTILS_HPP
#define FLEXQL_VALUE_UTILS_HPP

#include <optional>
#include <string>

#include "flexql/types.hpp"

namespace flexql {

std::optional<ColumnType> ParseColumnType(const std::string &token);

bool ParseLiteralAsType(
    const std::string &literal,
    ColumnType type,
    ScalarValue *out,
    std::string *error);

std::string ScalarToString(const ScalarValue &value);
bool CompareScalars(const ScalarValue &left, CompareOp op, const ScalarValue &right);
std::string SerializeScalarForIndex(const ScalarValue &value);

}  // namespace flexql

#endif
