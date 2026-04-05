#ifndef FLEXQL_TABLE_HPP
#define FLEXQL_TABLE_HPP

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "flexql/primary_index.hpp"
#include "flexql/types.hpp"

namespace flexql {

struct ColumnStorage {
  ColumnType type;
  std::vector<int64_t> int_values;
  std::vector<double> decimal_values;
  std::vector<std::string> string_values;
};

class Table {
 public:
  Table(std::string name, std::vector<ColumnDef> columns, size_t primary_col_idx);

  const std::string &name() const;
  const std::vector<ColumnDef> &columns() const;
  std::optional<size_t> GetColumnIndex(const std::string &column_name) const;
  bool InsertRows(const std::vector<std::vector<std::string>> &raw_rows, int64_t expires_epoch, std::string *error);
  size_t row_count() const;
  bool IsExpired(size_t row_id, int64_t now_epoch) const;
  int64_t ExpiryAt(size_t row_id) const;

  bool ReadValue(size_t column_idx, size_t row_id, ScalarValue *out) const;
  std::string ReadValueAsString(size_t column_idx, size_t row_id) const;

  bool IsPrimaryColumn(size_t column_idx) const;
  std::optional<size_t> FindRowByPrimaryLiteral(const std::string &literal, std::string *error) const;
  std::optional<size_t> FindRowByPrimaryValue(const ScalarValue &value) const;

  std::shared_mutex &mutex();

 private:
  std::string name_;
  std::vector<ColumnDef> columns_;
  std::vector<ColumnStorage> data_;
  std::unordered_map<std::string, size_t> column_positions_;
  std::vector<int64_t> expires_at_;

  size_t primary_col_idx_;
  PrimaryIndex primary_index_;

  mutable std::shared_mutex mutex_;
};

}  // namespace flexql

#endif
