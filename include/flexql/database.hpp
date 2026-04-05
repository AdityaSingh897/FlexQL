#ifndef FLEXQL_DATABASE_HPP
#define FLEXQL_DATABASE_HPP

#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include "flexql/table.hpp"

namespace flexql {

class Database {
 public:
  bool CreateTable(const std::string &name, std::vector<ColumnDef> columns, std::string *error);
  std::shared_ptr<Table> GetTable(const std::string &name) const;

 private:
  mutable std::shared_mutex mutex_;
  std::unordered_map<std::string, std::shared_ptr<Table>> tables_;
};

}  // namespace flexql

#endif
