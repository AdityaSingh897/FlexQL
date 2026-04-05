#ifndef FLEXQL_TYPES_HPP
#define FLEXQL_TYPES_HPP

#include <cstdint>
#include <variant>
#include <optional>
#include <string>
#include <vector>

namespace flexql {

enum class ColumnType {
  Int,
  Decimal,
  Varchar,
  DateTime,
};

struct ScalarValue {
  ColumnType type = ColumnType::Varchar;
  int64_t int_value = 0;
  double decimal_value = 0.0;
  std::string string_value;
};

struct ColumnDef {
  std::string name;
  ColumnType type;
  bool primary_key = false;
};

struct QualifiedColumn {
  std::optional<std::string> table;
  std::string column;
};

enum class CompareOp {
  Eq,
  Ne,
  Lt,
  Le,
  Gt,
  Ge,
};

struct WhereClause {
  QualifiedColumn lhs;
  CompareOp op;
  std::string rhs_literal;
};

struct JoinClause {
  std::string right_table;
  QualifiedColumn left_col;
  QualifiedColumn right_col;
};

struct SelectItem {
  bool is_star = false;
  QualifiedColumn column;
};

struct CreateTableStmt {
  std::string table_name;
  std::vector<ColumnDef> columns;
};

struct InsertStmt {
  std::string table_name;
  std::vector<std::vector<std::string>> rows;
  std::optional<std::string> expires_literal;
};

struct SelectStmt {
  std::vector<SelectItem> items;
  std::string from_table;
  std::optional<JoinClause> join;
  std::optional<WhereClause> where;
};

using Statement = std::variant<CreateTableStmt, InsertStmt, SelectStmt>;

struct QueryError {
  std::string message;
};

}  // namespace flexql

#endif
