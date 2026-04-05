#ifndef FLEXQL_EXECUTOR_HPP
#define FLEXQL_EXECUTOR_HPP

#include <functional>
#include <string>
#include <vector>

#include "flexql/database.hpp"
#include "flexql/query_cache.hpp"
#include "flexql/types.hpp"

namespace flexql {

class QueryExecutor {
 public:
  using ColumnsCallback = std::function<void(const std::vector<std::string> &)>;
  using RowCallback = std::function<bool(const std::vector<std::string> &)>;

  QueryExecutor(Database *db, QueryCache *cache);

  bool Execute(
      const std::string &sql,
      const ColumnsCallback &on_columns,
      const RowCallback &on_row,
      std::string *error);

 private:
  bool ExecuteCreate(
      const CreateTableStmt &stmt,
      const ColumnsCallback &on_columns,
      const RowCallback &on_row,
      std::string *error);

  bool ExecuteInsert(
      const InsertStmt &stmt,
      const ColumnsCallback &on_columns,
      const RowCallback &on_row,
      std::string *error);

  bool ExecuteSelect(
      const std::string &sql,
      const SelectStmt &stmt,
      const ColumnsCallback &on_columns,
      const RowCallback &on_row,
      std::string *error);

  Database *db_;
  QueryCache *cache_;
};

}  // namespace flexql

#endif
