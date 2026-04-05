#include "flexql/database.hpp"

#include <mutex>
#include <shared_mutex>

#include "flexql/string_utils.hpp"

namespace flexql {

bool Database::CreateTable(const std::string &name, std::vector<ColumnDef> columns, std::string *error) {
  if (columns.empty()) {
    if (error != nullptr) {
      *error = "CREATE TABLE requires at least one column";
    }
    return false;
  }

  size_t primary_count = 0;
  size_t primary_col_idx = 0;
  for (size_t i = 0; i < columns.size(); ++i) {
    if (columns[i].primary_key) {
      ++primary_count;
      primary_col_idx = i;
    }
  }

  if (primary_count == 0) {
    columns[0].primary_key = true;
    primary_col_idx = 0;
    primary_count = 1;
  }

  if (primary_count > 1) {
    if (error != nullptr) {
      *error = "Only one PRIMARY KEY column is supported";
    }
    return false;
  }

  const std::string key = ToLower(name);

  std::unique_lock lock(mutex_);
  if (tables_.find(key) != tables_.end()) {
    if (error != nullptr) {
      *error = "Table already exists: " + name;
    }
    return false;
  }

  tables_.emplace(key, std::make_shared<Table>(name, std::move(columns), primary_col_idx));
  return true;
}

std::shared_ptr<Table> Database::GetTable(const std::string &name) const {
  const std::string key = ToLower(name);
  std::shared_lock lock(mutex_);
  const auto it = tables_.find(key);
  if (it == tables_.end()) {
    return nullptr;
  }
  return it->second;
}

}  // namespace flexql
