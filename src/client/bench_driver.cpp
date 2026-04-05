#include <algorithm>
#include <chrono>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>

#include "flexql/flexql.h"

namespace {

struct RowCounter {
  uint64_t rows = 0;
};

int CountRowsCallback(void *data, int, char **, char **) {
  auto *counter = static_cast<RowCounter *>(data);
  ++counter->rows;
  return 0;
}

bool ExecSql(
    FlexQL *db,
    const std::string &sql,
    int (*callback)(void *, int, char **, char **),
    void *arg,
    const std::string &label) {
  char *errmsg = nullptr;
  const int rc = flexql_exec(db, sql.c_str(), callback, arg, &errmsg);
  if (rc != FLEXQL_OK) {
    std::cerr << "SQL error during " << label << ": " << (errmsg != nullptr ? errmsg : "unknown") << '\n';
    if (errmsg != nullptr) {
      flexql_free(errmsg);
    }
    return false;
  }
  if (errmsg != nullptr) {
    flexql_free(errmsg);
  }
  return true;
}

double ElapsedSeconds(std::chrono::steady_clock::time_point start, std::chrono::steady_clock::time_point end) {
  return std::chrono::duration_cast<std::chrono::duration<double>>(end - start).count();
}

void PrintUsage(const char *program) {
  std::cerr << "Usage: " << program << " <host> <port> <rows> [repeat_queries] [insert_batch_size]\n";
}

}  // namespace

int main(int argc, char **argv) {
  if (argc < 4) {
    PrintUsage(argv[0]);
    return 1;
  }

  const char *host = argv[1];
  const int port = std::atoi(argv[2]);
  const uint64_t rows = static_cast<uint64_t>(std::strtoull(argv[3], nullptr, 10));
  const int repeats = argc >= 5 ? std::atoi(argv[4]) : 5;
  const uint64_t insert_batch_size = argc >= 6 ? static_cast<uint64_t>(std::strtoull(argv[5], nullptr, 10)) : 1000;

  if (port <= 0 || port > 65535 || rows == 0 || repeats <= 0 || insert_batch_size == 0) {
    PrintUsage(argv[0]);
    return 1;
  }

  FlexQL *db = nullptr;
  if (flexql_open(host, port, &db) != FLEXQL_OK) {
    std::cerr << "Cannot connect to FlexQL server\n";
    return 1;
  }

  const uint64_t run_id = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count());
  const std::string table_name = "bench_" + std::to_string(run_id);

  std::string create_sql = "CREATE TABLE " + table_name +
                           " (id INT PRIMARY KEY, score DECIMAL, name VARCHAR, created DATETIME);";
  if (!ExecSql(db, create_sql, nullptr, nullptr, "CREATE TABLE")) {
    flexql_close(db);
    return 1;
  }

  const auto insert_start = std::chrono::steady_clock::now();
  uint64_t inserted = 0;
  while (inserted < rows) {
    const uint64_t batch_start = inserted + 1;
    const uint64_t batch_end = std::min(rows, inserted + insert_batch_size);
    std::string insert_sql = "INSERT INTO " + table_name + " VALUES ";
    bool first = true;
    for (uint64_t i = batch_start; i <= batch_end; ++i) {
      if (!first) {
        insert_sql.append(", ");
      }
      first = false;
      insert_sql.append("(")
          .append(std::to_string(i))
          .append(", ")
          .append(std::to_string(i))
          .append(".0, 'u")
          .append(std::to_string(i))
          .append("', '2026-01-01 00:00:00')");
    }
    insert_sql.append(" EXPIRES '2099-01-01 00:00:00';");

    if (!ExecSql(db, insert_sql, nullptr, nullptr, "INSERT_BATCH")) {
      flexql_close(db);
      return 1;
    }

    inserted = batch_end;
    if (inserted % 1000000 == 0 || inserted == rows) {
      std::fprintf(stderr, "[progress] inserted=%" PRIu64 "\n", inserted);
    }
  }
  const auto insert_end = std::chrono::steady_clock::now();

  const uint64_t mid = rows / 2 == 0 ? 1 : (rows / 2);
  std::string point_query = "SELECT * FROM " + table_name + " WHERE id = " + std::to_string(mid) + ";";

  RowCounter point_first;
  const auto point_first_start = std::chrono::steady_clock::now();
  if (!ExecSql(db, point_query, CountRowsCallback, &point_first, "POINT SELECT first")) {
    flexql_close(db);
    return 1;
  }
  const auto point_first_end = std::chrono::steady_clock::now();

  uint64_t point_repeat_rows = 0;
  const auto point_repeat_start = std::chrono::steady_clock::now();
  for (int r = 0; r < repeats; ++r) {
    RowCounter c;
    if (!ExecSql(db, point_query, CountRowsCallback, &c, "POINT SELECT repeat")) {
      flexql_close(db);
      return 1;
    }
    point_repeat_rows += c.rows;
  }
  const auto point_repeat_end = std::chrono::steady_clock::now();

  std::string scan_query = "SELECT id, score FROM " + table_name + " WHERE score >= 0;";
  RowCounter scan_counter;
  const auto scan_start = std::chrono::steady_clock::now();
  if (!ExecSql(db, scan_query, CountRowsCallback, &scan_counter, "FULL SCAN SELECT")) {
    flexql_close(db);
    return 1;
  }
  const auto scan_end = std::chrono::steady_clock::now();

  const double insert_s = ElapsedSeconds(insert_start, insert_end);
  const double point_first_s = ElapsedSeconds(point_first_start, point_first_end);
  const double point_repeat_total_s = ElapsedSeconds(point_repeat_start, point_repeat_end);
  const double point_repeat_avg_s = point_repeat_total_s / static_cast<double>(repeats);
  const double scan_s = ElapsedSeconds(scan_start, scan_end);

  std::cout << "BENCHMARK_CONFIG rows=" << rows << " repeats=" << repeats
            << " insert_batch_size=" << insert_batch_size << " table=" << table_name << '\n';
  std::cout << "INSERT_SECONDS=" << insert_s << '\n';
  std::cout << "POINT_SELECT_ROWS_FIRST=" << point_first.rows << '\n';
  std::cout << "POINT_SELECT_SECONDS_FIRST=" << point_first_s << '\n';
  std::cout << "POINT_SELECT_ROWS_REPEATED_TOTAL=" << point_repeat_rows << '\n';
  std::cout << "POINT_SELECT_SECONDS_REPEATED_TOTAL=" << point_repeat_total_s << '\n';
  std::cout << "POINT_SELECT_SECONDS_REPEATED_AVG=" << point_repeat_avg_s << '\n';
  std::cout << "FULL_SCAN_ROWS=" << scan_counter.rows << '\n';
  std::cout << "FULL_SCAN_SECONDS=" << scan_s << '\n';

  flexql_close(db);
  return 0;
}
