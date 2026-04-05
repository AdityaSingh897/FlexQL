#include "flexql/table.hpp"

#include <unordered_set>
#include <utility>

#include "flexql/string_utils.hpp"
#include "flexql/value_utils.hpp"

namespace flexql {

Table::Table(std::string name, std::vector<ColumnDef> columns, size_t primary_col_idx)
    : name_(std::move(name)), columns_(std::move(columns)), primary_col_idx_(primary_col_idx) {
  data_.reserve(columns_.size());
  for (size_t i = 0; i < columns_.size(); ++i) {
    const auto &col = columns_[i];
    data_.push_back(ColumnStorage{col.type, {}, {}, {}});
    column_positions_.emplace(ToLower(col.name), i);
  }
}

const std::string &Table::name() const {
  return name_;
}

const std::vector<ColumnDef> &Table::columns() const {
  return columns_;
}

std::optional<size_t> Table::GetColumnIndex(const std::string &column_name) const {
  const auto it = column_positions_.find(ToLower(column_name));
  if (it == column_positions_.end()) {
    return std::nullopt;
  }
  return it->second;
}

bool Table::InsertRows(const std::vector<std::vector<std::string>> &raw_rows, int64_t expires_epoch, std::string *error) {
  if (raw_rows.empty()) {
    if (error != nullptr) {
      *error = "INSERT requires at least one row";
    }
    return false;
  }

  std::vector<std::vector<ScalarValue>> parsed_rows;
  parsed_rows.reserve(raw_rows.size());

  std::unordered_set<std::string> batch_primary_keys;
  batch_primary_keys.reserve(raw_rows.size());

  for (const auto &raw_values : raw_rows) {
    if (raw_values.size() != columns_.size()) {
      if (error != nullptr) {
        *error = "Column count mismatch in INSERT";
      }
      return false;
    }

    std::vector<ScalarValue> parsed_values(columns_.size());
    for (size_t i = 0; i < columns_.size(); ++i) {
      if (!ParseLiteralAsType(raw_values[i], columns_[i].type, &parsed_values[i], error)) {
        return false;
      }
    }

    const auto pk_key = SerializeScalarForIndex(parsed_values[primary_col_idx_]);
    if (primary_index_.Find(pk_key).has_value() || batch_primary_keys.find(pk_key) != batch_primary_keys.end()) {
      if (error != nullptr) {
        *error = "Duplicate primary key value";
      }
      return false;
    }

    batch_primary_keys.insert(pk_key);
    parsed_rows.push_back(std::move(parsed_values));
  }

  for (const auto &parsed_values : parsed_rows) {
    const size_t new_row_id = expires_at_.size();
    for (size_t i = 0; i < columns_.size(); ++i) {
      auto &storage = data_[i];
      const auto &value = parsed_values[i];
      switch (storage.type) {
        case ColumnType::Int:
        case ColumnType::DateTime:
          storage.int_values.push_back(value.int_value);
          break;
        case ColumnType::Decimal:
          storage.decimal_values.push_back(value.decimal_value);
          break;
        case ColumnType::Varchar:
          storage.string_values.push_back(value.string_value);
          break;
      }
    }

    expires_at_.push_back(expires_epoch);
    primary_index_.Insert(SerializeScalarForIndex(parsed_values[primary_col_idx_]), new_row_id);
  }
  return true;
}

size_t Table::row_count() const {
  return expires_at_.size();
}

bool Table::IsExpired(size_t row_id, int64_t now_epoch) const {
  if (row_id >= expires_at_.size()) {
    return true;
  }
  return expires_at_[row_id] <= now_epoch;
}

int64_t Table::ExpiryAt(size_t row_id) const {
  if (row_id >= expires_at_.size()) {
    return 0;
  }
  return expires_at_[row_id];
}

bool Table::ReadValue(size_t column_idx, size_t row_id, ScalarValue *out) const {
  if (column_idx >= columns_.size() || row_id >= row_count()) {
    return false;
  }

  const auto &col = columns_[column_idx];
  const auto &storage = data_[column_idx];
  out->type = col.type;

  switch (col.type) {
    case ColumnType::Int:
    case ColumnType::DateTime:
      out->int_value = storage.int_values[row_id];
      return true;
    case ColumnType::Decimal:
      out->decimal_value = storage.decimal_values[row_id];
      return true;
    case ColumnType::Varchar:
      out->string_value = storage.string_values[row_id];
      return true;
  }

  return false;
}

std::string Table::ReadValueAsString(size_t column_idx, size_t row_id) const {
  ScalarValue value;
  if (!ReadValue(column_idx, row_id, &value)) {
    return "";
  }
  return ScalarToString(value);
}

bool Table::IsPrimaryColumn(size_t column_idx) const {
  return column_idx == primary_col_idx_;
}

std::optional<size_t> Table::FindRowByPrimaryLiteral(const std::string &literal, std::string *error) const {
  ScalarValue value;
  if (!ParseLiteralAsType(literal, columns_[primary_col_idx_].type, &value, error)) {
    return std::nullopt;
  }
  return FindRowByPrimaryValue(value);
}

std::optional<size_t> Table::FindRowByPrimaryValue(const ScalarValue &value) const {
  return primary_index_.Find(SerializeScalarForIndex(value));
}

std::shared_mutex &Table::mutex() {
  return mutex_;
}

}  // namespace flexql
