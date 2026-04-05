#ifndef FLEXQL_SERVER_HPP
#define FLEXQL_SERVER_HPP

#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>

#include "flexql/database.hpp"
#include "flexql/query_cache.hpp"
#include "flexql/wal.hpp"

namespace flexql {

class FlexQLServer {
 public:
  explicit FlexQLServer(int port);

  bool Run(std::string *error);
  void Stop();

 private:
  void HandleClient(int client_fd);

  int port_;
  int listen_fd_;
  std::atomic<bool> running_;
  std::mutex mutation_mutex_;
  int64_t default_ttl_seconds_;
  bool require_expires_;

  Database db_;
  QueryCache cache_;
  WriteAheadLog wal_;
};

}  // namespace flexql

#endif
