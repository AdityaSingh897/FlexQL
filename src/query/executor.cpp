#include "flexql/executor.hpp"

#include <algorithm>
#include <limits>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>

#include "flexql/datetime_utils.hpp"
#include "flexql/parser.hpp"
#include "flexql/string_utils.hpp"
#include "flexql/value_utils.hpp"

namespace {

struct ResolvedColumn {
  int table_side = 0;  // 0 = left, 1 = right
  size_t col_idx = 0;
  std::string output_name;
};

struct ResolvedWhere {
  int table_side = 0;  // 0 = left, 1 = right
  size_t col_idx = 0;
  flexql::CompareOp op = flexql::CompareOp::Eq;
  flexql::ScalarValue rhs;
};

}  // namespace

namespace flexql {

QueryExecutor::QueryExecutor(Database *db, QueryCache *cache) : db_(db), cache_(cache) {}

bool QueryExecutor::Execute(
    const std::string &sql,
    const ColumnsCallback &on_columns,
    const RowCallback &on_row,
    std::string *error) {
  QueryError parse_error;
  auto stmt = SqlParser::Parse(sql, &parse_error);
  if (!stmt.has_value()) {
    if (error != nullptr) {
      *error = parse_error.message;
    }
    return false;
  }

  if (std::holds_alternative<CreateTableStmt>(*stmt)) {
    return ExecuteCreate(std::get<CreateTableStmt>(*stmt), on_columns, on_row, error);
  }
  if (std::holds_alternative<InsertStmt>(*stmt)) {
    return ExecuteInsert(std::get<InsertStmt>(*stmt), on_columns, on_row, error);
  }
  return ExecuteSelect(sql, std::get<SelectStmt>(*stmt), on_columns, on_row, error);
}

bool QueryExecutor::ExecuteCreate(
    const CreateTableStmt &stmt,
    const ColumnsCallback &,
    const RowCallback &,
    std::string *error) {
  if (!db_->CreateTable(stmt.table_name, stmt.columns, error)) {
    return false;
  }

  cache_->Clear();
  return true;
}

bool QueryExecutor::ExecuteInsert(
    const InsertStmt &stmt,
    const ColumnsCallback &,
    const RowCallback &,
    std::string *error) {
  const auto table = db_->GetTable(stmt.table_name);
  if (table == nullptr) {
    if (error != nullptr) {
      *error = "Table not found: " + stmt.table_name;
    }
    return false;
  }

  int64_t expires_epoch = std::numeric_limits<int64_t>::max();
  if (stmt.expires_literal.has_value()) {
    ScalarValue expires_value;
    if (!ParseLiteralAsType(*stmt.expires_literal, ColumnType::DateTime, &expires_value, error)) {
      return false;
    }
    expires_epoch = expires_value.int_value;
  }

  std::unique_lock lock(table->mutex());
  if (!table->InsertRows(stmt.rows, expires_epoch, error)) {
    return false;
  }

  cache_->Clear();
  return true;
}

bool QueryExecutor::ExecuteSelect(
    const std::string &sql,
    const SelectStmt &stmt,
    const ColumnsCallback &on_columns,
    const RowCallback &on_row,
    std::string *error) {
  const std::string cache_key = Trim(sql);
  if (auto cached = cache_->Get(cache_key); cached.has_value()) {
    if (on_columns) {
      on_columns(cached->column_names);
    }
    if (on_row) {
      for (const auto &row : cached->rows) {
        if (!on_row(row)) {
          break;
        }
      }
    }
    return true;
  }

  CachedQueryResult cache_result;
  bool collect_cache = true;
  auto update_cache_valid_until = [&](int64_t expires_epoch) {
    if (expires_epoch < cache_result.valid_until_epoch) {
      cache_result.valid_until_epoch = expires_epoch;
    }
  };

  auto emit_columns = [&](const std::vector<std::string> &cols) {
    cache_result.column_names = cols;
    if (on_columns) {
      on_columns(cols);
    }
  };

  auto emit_row = [&](const std::vector<std::string> &row) {
    if (collect_cache) {
      if (cache_result.rows.size() < 10000) {
        cache_result.rows.push_back(row);
      } else {
        collect_cache = false;
      }
    }
    if (on_row) {
      return on_row(row);
    }
    return true;
  };

  const auto left_table = db_->GetTable(stmt.from_table);
  if (left_table == nullptr) {
    if (error != nullptr) {
      *error = "Table not found: " + stmt.from_table;
    }
    return false;
  }

  if (!stmt.join.has_value()) {
    std::shared_lock lock(left_table->mutex());

    std::vector<size_t> projection_cols;
    std::vector<std::string> projection_names;

    if (stmt.items.size() == 1 && stmt.items[0].is_star) {
      for (size_t i = 0; i < left_table->columns().size(); ++i) {
        projection_cols.push_back(i);
        projection_names.push_back(left_table->columns()[i].name);
      }
    } else {
      for (const auto &item : stmt.items) {
        if (item.is_star) {
          if (error != nullptr) {
            *error = "SELECT list cannot mix '*' with explicit columns";
          }
          return false;
        }
        if (item.column.table.has_value() && !IEquals(*item.column.table, stmt.from_table)) {
          if (error != nullptr) {
            *error = "Unknown table qualifier in SELECT: " + *item.column.table;
          }
          return false;
        }
        const auto maybe_idx = left_table->GetColumnIndex(item.column.column);
        if (!maybe_idx.has_value()) {
          if (error != nullptr) {
            *error = "Unknown column: " + item.column.column;
          }
          return false;
        }
        projection_cols.push_back(*maybe_idx);
        projection_names.push_back(item.column.column);
      }
    }

    std::optional<size_t> where_col_idx;
    std::optional<ScalarValue> where_rhs;
    CompareOp where_op = CompareOp::Eq;

    if (stmt.where.has_value()) {
      if (stmt.where->lhs.table.has_value() && !IEquals(*stmt.where->lhs.table, stmt.from_table)) {
        if (error != nullptr) {
          *error = "Unknown table qualifier in WHERE clause";
        }
        return false;
      }

      const auto maybe_idx = left_table->GetColumnIndex(stmt.where->lhs.column);
      if (!maybe_idx.has_value()) {
        if (error != nullptr) {
          *error = "Unknown WHERE column: " + stmt.where->lhs.column;
        }
        return false;
      }

      ScalarValue rhs;
      if (!ParseLiteralAsType(stmt.where->rhs_literal, left_table->columns()[*maybe_idx].type, &rhs, error)) {
        return false;
      }

      where_col_idx = *maybe_idx;
      where_rhs = rhs;
      where_op = stmt.where->op;
    }

    emit_columns(projection_names);

    const int64_t now = CurrentEpochSeconds();

    if (where_col_idx.has_value() && where_op == CompareOp::Eq && left_table->IsPrimaryColumn(*where_col_idx)) {
      const auto row_id = left_table->FindRowByPrimaryValue(*where_rhs);
      if (row_id.has_value() && !left_table->IsExpired(*row_id, now)) {
        ScalarValue left;
        left_table->ReadValue(*where_col_idx, *row_id, &left);
        if (CompareScalars(left, where_op, *where_rhs)) {
          std::vector<std::string> out;
          out.reserve(projection_cols.size());
          for (size_t col_idx : projection_cols) {
            out.push_back(left_table->ReadValueAsString(col_idx, *row_id));
          }
          update_cache_valid_until(left_table->ExpiryAt(*row_id));
          emit_row(out);
        }
      }
    } else {
      for (size_t row_id = 0; row_id < left_table->row_count(); ++row_id) {
        if (left_table->IsExpired(row_id, now)) {
          continue;
        }

        if (where_col_idx.has_value()) {
          ScalarValue left;
          left_table->ReadValue(*where_col_idx, row_id, &left);
          if (!CompareScalars(left, where_op, *where_rhs)) {
            continue;
          }
        }

        std::vector<std::string> out;
        out.reserve(projection_cols.size());
        for (size_t col_idx : projection_cols) {
          out.push_back(left_table->ReadValueAsString(col_idx, row_id));
        }
        update_cache_valid_until(left_table->ExpiryAt(row_id));

        if (!emit_row(out)) {
          break;
        }
      }
    }
  } else {
    const auto right_table = db_->GetTable(stmt.join->right_table);
    if (right_table == nullptr) {
      if (error != nullptr) {
        *error = "Table not found: " + stmt.join->right_table;
      }
      return false;
    }

    std::shared_lock<std::shared_mutex> left_lock(left_table->mutex(), std::defer_lock);
    std::shared_lock<std::shared_mutex> right_lock(right_table->mutex(), std::defer_lock);
    if (ToLower(left_table->name()) <= ToLower(right_table->name())) {
      left_lock.lock();
      right_lock.lock();
    } else {
      right_lock.lock();
      left_lock.lock();
    }

    auto resolve_column = [&](const QualifiedColumn &col, bool for_output, std::string *resolve_error) -> std::optional<ResolvedColumn> {
      auto try_resolve_on = [&](int side, const std::shared_ptr<Table> &tbl, const std::string &table_name) -> std::optional<ResolvedColumn> {
        const auto idx = tbl->GetColumnIndex(col.column);
        if (!idx.has_value()) {
          return std::nullopt;
        }
        std::string out_name = col.column;
        if (for_output && col.table.has_value()) {
          out_name = *col.table + "." + col.column;
        }
        if (for_output && !col.table.has_value()) {
          out_name = col.column;
        }
        return ResolvedColumn{side, *idx, out_name};
      };

      if (col.table.has_value()) {
        if (IEquals(*col.table, left_table->name()) || IEquals(*col.table, stmt.from_table)) {
          return try_resolve_on(0, left_table, left_table->name());
        }
        if (IEquals(*col.table, right_table->name()) || IEquals(*col.table, stmt.join->right_table)) {
          return try_resolve_on(1, right_table, right_table->name());
        }
        if (resolve_error != nullptr) {
          *resolve_error = "Unknown table qualifier: " + *col.table;
        }
        return std::nullopt;
      }

      const auto left_idx = left_table->GetColumnIndex(col.column);
      const auto right_idx = right_table->GetColumnIndex(col.column);

      if (left_idx.has_value() && right_idx.has_value()) {
        if (resolve_error != nullptr) {
          *resolve_error = "Ambiguous column: " + col.column;
        }
        return std::nullopt;
      }
      if (left_idx.has_value()) {
        return ResolvedColumn{0, *left_idx, col.column};
      }
      if (right_idx.has_value()) {
        return ResolvedColumn{1, *right_idx, col.column};
      }

      if (resolve_error != nullptr) {
        *resolve_error = "Unknown column: " + col.column;
      }
      return std::nullopt;
    };

    std::string resolve_error;
    auto left_join = resolve_column(stmt.join->left_col, false, &resolve_error);
    if (!left_join.has_value()) {
      if (error != nullptr) {
        *error = resolve_error;
      }
      return false;
    }
    auto right_join = resolve_column(stmt.join->right_col, false, &resolve_error);
    if (!right_join.has_value()) {
      if (error != nullptr) {
        *error = resolve_error;
      }
      return false;
    }

    if (left_join->table_side == right_join->table_side) {
      if (error != nullptr) {
        *error = "JOIN condition must compare columns from different tables";
      }
      return false;
    }

    size_t left_join_col = left_join->table_side == 0 ? left_join->col_idx : right_join->col_idx;
    size_t right_join_col = left_join->table_side == 1 ? left_join->col_idx : right_join->col_idx;

    if (left_table->columns()[left_join_col].type != right_table->columns()[right_join_col].type) {
      if (error != nullptr) {
        *error = "JOIN columns must have the same type";
      }
      return false;
    }

    std::vector<ResolvedColumn> output_cols;
    if (stmt.items.size() == 1 && stmt.items[0].is_star) {
      for (size_t i = 0; i < left_table->columns().size(); ++i) {
        output_cols.push_back(ResolvedColumn{0, i, left_table->name() + "." + left_table->columns()[i].name});
      }
      for (size_t i = 0; i < right_table->columns().size(); ++i) {
        output_cols.push_back(ResolvedColumn{1, i, right_table->name() + "." + right_table->columns()[i].name});
      }
    } else {
      for (const auto &item : stmt.items) {
        if (item.is_star) {
          if (error != nullptr) {
            *error = "SELECT list cannot mix '*' with explicit columns";
          }
          return false;
        }
        auto resolved = resolve_column(item.column, true, &resolve_error);
        if (!resolved.has_value()) {
          if (error != nullptr) {
            *error = resolve_error;
          }
          return false;
        }
        output_cols.push_back(*resolved);
      }
    }

    std::optional<ResolvedWhere> where;
    if (stmt.where.has_value()) {
      auto where_col = resolve_column(stmt.where->lhs, false, &resolve_error);
      if (!where_col.has_value()) {
        if (error != nullptr) {
          *error = resolve_error;
        }
        return false;
      }

      const ColumnType where_type =
          where_col->table_side == 0 ? left_table->columns()[where_col->col_idx].type : right_table->columns()[where_col->col_idx].type;

      ScalarValue rhs;
      if (!ParseLiteralAsType(stmt.where->rhs_literal, where_type, &rhs, error)) {
        return false;
      }

      where = ResolvedWhere{where_col->table_side, where_col->col_idx, stmt.where->op, rhs};
    }

    std::vector<std::string> column_names;
    column_names.reserve(output_cols.size());
    for (const auto &col : output_cols) {
      column_names.push_back(col.output_name);
    }
    emit_columns(column_names);

    auto emit_join_row = [&](size_t left_row, size_t right_row) {
      if (where.has_value()) {
        ScalarValue lhs;
        if (where->table_side == 0) {
          left_table->ReadValue(where->col_idx, left_row, &lhs);
        } else {
          right_table->ReadValue(where->col_idx, right_row, &lhs);
        }
        if (!CompareScalars(lhs, where->op, where->rhs)) {
          return true;
        }
      }

      std::vector<std::string> out;
      out.reserve(output_cols.size());
      for (const auto &col : output_cols) {
        if (col.table_side == 0) {
          out.push_back(left_table->ReadValueAsString(col.col_idx, left_row));
        } else {
          out.push_back(right_table->ReadValueAsString(col.col_idx, right_row));
        }
      }
      update_cache_valid_until(std::min(left_table->ExpiryAt(left_row), right_table->ExpiryAt(right_row)));
      return emit_row(out);
    };

    const int64_t now = CurrentEpochSeconds();

    if (right_table->IsPrimaryColumn(right_join_col)) {
      for (size_t left_row = 0; left_row < left_table->row_count(); ++left_row) {
        if (left_table->IsExpired(left_row, now)) {
          continue;
        }

        ScalarValue join_val;
        left_table->ReadValue(left_join_col, left_row, &join_val);
        const auto maybe_right_row = right_table->FindRowByPrimaryValue(join_val);
        if (!maybe_right_row.has_value()) {
          continue;
        }
        if (right_table->IsExpired(*maybe_right_row, now)) {
          continue;
        }

        if (!emit_join_row(left_row, *maybe_right_row)) {
          break;
        }
      }
    } else if (left_table->IsPrimaryColumn(left_join_col)) {
      for (size_t right_row = 0; right_row < right_table->row_count(); ++right_row) {
        if (right_table->IsExpired(right_row, now)) {
          continue;
        }

        ScalarValue join_val;
        right_table->ReadValue(right_join_col, right_row, &join_val);
        const auto maybe_left_row = left_table->FindRowByPrimaryValue(join_val);
        if (!maybe_left_row.has_value()) {
          continue;
        }
        if (left_table->IsExpired(*maybe_left_row, now)) {
          continue;
        }

        if (!emit_join_row(*maybe_left_row, right_row)) {
          break;
        }
      }
    } else {
      const bool build_left = left_table->row_count() <= right_table->row_count();

      std::unordered_multimap<std::string, size_t> hash;
      if (build_left) {
        hash.reserve(left_table->row_count());
        for (size_t row = 0; row < left_table->row_count(); ++row) {
          if (left_table->IsExpired(row, now)) {
            continue;
          }
          ScalarValue key_val;
          left_table->ReadValue(left_join_col, row, &key_val);
          hash.emplace(SerializeScalarForIndex(key_val), row);
        }

        for (size_t right_row = 0; right_row < right_table->row_count(); ++right_row) {
          if (right_table->IsExpired(right_row, now)) {
            continue;
          }
          ScalarValue key_val;
          right_table->ReadValue(right_join_col, right_row, &key_val);
          const auto key = SerializeScalarForIndex(key_val);
          const auto range = hash.equal_range(key);
          for (auto it = range.first; it != range.second; ++it) {
            if (!emit_join_row(it->second, right_row)) {
              right_row = right_table->row_count();
              break;
            }
          }
        }
      } else {
        hash.reserve(right_table->row_count());
        for (size_t row = 0; row < right_table->row_count(); ++row) {
          if (right_table->IsExpired(row, now)) {
            continue;
          }
          ScalarValue key_val;
          right_table->ReadValue(right_join_col, row, &key_val);
          hash.emplace(SerializeScalarForIndex(key_val), row);
        }

        for (size_t left_row = 0; left_row < left_table->row_count(); ++left_row) {
          if (left_table->IsExpired(left_row, now)) {
            continue;
          }
          ScalarValue key_val;
          left_table->ReadValue(left_join_col, left_row, &key_val);
          const auto key = SerializeScalarForIndex(key_val);
          const auto range = hash.equal_range(key);
          for (auto it = range.first; it != range.second; ++it) {
            if (!emit_join_row(left_row, it->second)) {
              left_row = left_table->row_count();
              break;
            }
          }
        }
      }
    }
  }

  if (collect_cache) {
    cache_->Put(cache_key, std::move(cache_result));
  }
  return true;
}

}  // namespace flexql
