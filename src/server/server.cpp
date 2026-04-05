#include "flexql/server.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <mutex>
#include <sstream>
#include <thread>
#include <vector>

#include "flexql/datetime_utils.hpp"
#include "flexql/executor.hpp"
#include "flexql/network_protocol.hpp"
#include "flexql/parser.hpp"
#include "flexql/string_utils.hpp"

namespace {

constexpr int64_t kFallbackDefaultTtlSeconds = 60;

int64_t LoadDefaultTtlSeconds() {
  const char *env = std::getenv("FLEXQL_DEFAULT_TTL_SECONDS");
  if (env == nullptr) {
    return kFallbackDefaultTtlSeconds;
  }

  try {
    const int64_t value = std::stoll(env);
    if (value > 0) {
      return value;
    }
  } catch (...) {
  }
  return kFallbackDefaultTtlSeconds;
}

bool LoadRequireExpiresFlag() {
  const char *env = std::getenv("FLEXQL_REQUIRE_EXPIRES");
  if (env == nullptr) {
    return false;
  }
  const std::string normalized = flexql::ToLower(flexql::Trim(env));
  return normalized == "1" || normalized == "true" || normalized == "yes" || normalized == "on";
}

std::string BuildInsertSqlWithResolvedExpires(const flexql::InsertStmt &stmt, int64_t expires_epoch) {
  std::ostringstream sql;
  sql << "INSERT INTO " << stmt.table_name << " VALUES ";
  for (size_t row_idx = 0; row_idx < stmt.rows.size(); ++row_idx) {
    if (row_idx > 0) {
      sql << ", ";
    }
    sql << "(";
    for (size_t col_idx = 0; col_idx < stmt.rows[row_idx].size(); ++col_idx) {
      if (col_idx > 0) {
        sql << ", ";
      }
      sql << stmt.rows[row_idx][col_idx];
    }
    sql << ")";
  }
  sql << " EXPIRES '" << flexql::EpochSecondsToDateTimeString(expires_epoch) << "';";
  return sql.str();
}

}  // namespace

namespace flexql {

FlexQLServer::FlexQLServer(int port)
    : port_(port),
      listen_fd_(-1),
      running_(false),
      default_ttl_seconds_(LoadDefaultTtlSeconds()),
      require_expires_(LoadRequireExpiresFlag()),
      cache_(128, 10000),
      wal_("data/flexql.wal") {}

bool FlexQLServer::Run(std::string *error) {
  std::error_code ec;
  std::filesystem::create_directories("data", ec);
  if (ec) {
    if (error != nullptr) {
      *error = "Failed to create data directory";
    }
    return false;
  }

  if (!wal_.Open(error)) {
    return false;
  }

  QueryExecutor recovery_executor(&db_, &cache_);
  if (!wal_.Replay(
          [&](const std::string &sql, std::string *replay_error) {
            return recovery_executor.Execute(
                sql,
                nullptr,
                [](const std::vector<std::string> &) { return true; },
                replay_error);
          },
          error)) {
    return false;
  }

  listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd_ < 0) {
    if (error != nullptr) {
      *error = "Failed to create listening socket";
    }
    return false;
  }

  int reuse = 1;
  setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = INADDR_ANY;
  addr.sin_port = htons(static_cast<uint16_t>(port_));

  if (bind(listen_fd_, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
    if (error != nullptr) {
      *error = "Failed to bind to port " + std::to_string(port_);
    }
    close(listen_fd_);
    listen_fd_ = -1;
    return false;
  }

  if (listen(listen_fd_, 128) < 0) {
    if (error != nullptr) {
      *error = "Failed to listen on socket";
    }
    close(listen_fd_);
    listen_fd_ = -1;
    return false;
  }

  running_.store(true);

  while (running_.load()) {
    sockaddr_in client_addr{};
    socklen_t client_len = sizeof(client_addr);
    const int client_fd = accept(listen_fd_, reinterpret_cast<sockaddr *>(&client_addr), &client_len);
    if (client_fd < 0) {
      if (!running_.load()) {
        break;
      }
      continue;
    }

    std::thread(&FlexQLServer::HandleClient, this, client_fd).detach();
  }

  if (listen_fd_ >= 0) {
    close(listen_fd_);
    listen_fd_ = -1;
  }

  return true;
}

void FlexQLServer::Stop() {
  running_.store(false);
  if (listen_fd_ >= 0) {
    shutdown(listen_fd_, SHUT_RDWR);
    close(listen_fd_);
    listen_fd_ = -1;
  }
}

void FlexQLServer::HandleClient(int client_fd) {
  QueryExecutor executor(&db_, &cache_);

  while (true) {
    std::string sql;
    if (!RecvString(client_fd, &sql)) {
      break;
    }

    std::string error;
    bool io_ok = true;
    bool header_sent = false;
    bool is_mutation = false;
    std::string sql_to_execute = sql;

    QueryError parse_error;
    auto parsed = SqlParser::Parse(sql, &parse_error);
    if (!parsed.has_value()) {
      if (!SendInt32(client_fd, kStatusError) || !SendString(client_fd, parse_error.message)) {
        break;
      }
      continue;
    }
    is_mutation = std::holds_alternative<CreateTableStmt>(*parsed) || std::holds_alternative<InsertStmt>(*parsed);

    if (auto *insert = std::get_if<InsertStmt>(&*parsed); insert != nullptr) {
      if (!insert->expires_literal.has_value()) {
        if (require_expires_) {
          if (!SendInt32(client_fd, kStatusError) ||
              !SendString(client_fd, "INSERT requires EXPIRES when FLEXQL_REQUIRE_EXPIRES is enabled")) {
            break;
          }
          continue;
        }
        const int64_t expires_epoch = CurrentEpochSeconds() + default_ttl_seconds_;
        sql_to_execute = BuildInsertSqlWithResolvedExpires(*insert, expires_epoch);
      }
    }

    auto send_success_header = [&](const std::vector<std::string> &columns) {
      if (header_sent) {
        return true;
      }
      if (!SendInt32(client_fd, kStatusOk) || !SendString(client_fd, "")) {
        return false;
      }
      if (!SendInt32(client_fd, static_cast<int32_t>(columns.size()))) {
        return false;
      }
      for (const auto &col : columns) {
        if (!SendString(client_fd, col)) {
          return false;
        }
      }
      header_sent = true;
      return true;
    };

    auto execute_and_stream = [&]() {
      return executor.Execute(
          sql_to_execute,
          [&](const std::vector<std::string> &col_names) {
            if (!send_success_header(col_names)) {
              io_ok = false;
            }
          },
          [&](const std::vector<std::string> &row) {
            if (!io_ok) {
              return false;
            }
            if (!header_sent) {
              if (!send_success_header({})) {
                io_ok = false;
                return false;
              }
            }
            if (!SendInt32(client_fd, 1)) {
              io_ok = false;
              return false;
            }
            for (const auto &value : row) {
              if (!SendString(client_fd, value)) {
                io_ok = false;
                return false;
              }
            }
            return true;
          },
          &error);
    };

    bool ok = false;
    if (is_mutation) {
      std::lock_guard lock(mutation_mutex_);
      ok = execute_and_stream();
      if (ok) {
        if (!wal_.AppendRecord(sql_to_execute, &error)) {
          if (!SendInt32(client_fd, kStatusError) || !SendString(client_fd, "Durability failure: " + error)) {
            break;
          }
          Stop();
          break;
        }
      }
    } else {
      ok = execute_and_stream();
    }

    if (!io_ok) {
      break;
    }

    if (!ok) {
      if (!SendInt32(client_fd, kStatusError) || !SendString(client_fd, error)) {
        break;
      }
      continue;
    }

    if (!header_sent && !send_success_header({})) {
      break;
    }

    if (!SendInt32(client_fd, 0)) {
      break;
    }
  }

  close(client_fd);
}

}  // namespace flexql
