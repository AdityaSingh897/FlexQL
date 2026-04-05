#ifndef FLEXQL_PARSER_HPP
#define FLEXQL_PARSER_HPP

#include <optional>
#include <string>

#include "flexql/types.hpp"

namespace flexql {

class SqlParser {
 public:
  static std::optional<Statement> Parse(const std::string &sql, QueryError *error);
};

}  // namespace flexql

#endif
