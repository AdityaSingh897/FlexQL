#ifndef FLEXQL_QUERY_CACHE_HPP
#define FLEXQL_QUERY_CACHE_HPP

#include <cstdint>
#include <cstddef>
#include <list>
#include <limits>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace flexql {

struct CachedQueryResult {
  int64_t valid_until_epoch = std::numeric_limits<int64_t>::max();
  std::vector<std::string> column_names;
  std::vector<std::vector<std::string>> rows;
};

class QueryCache {
 public:
  explicit QueryCache(size_t max_entries = 128, size_t max_rows_per_entry = 10000);

  std::optional<CachedQueryResult> Get(const std::string &key);
  void Put(const std::string &key, CachedQueryResult value);
  void Clear();

 private:
  struct Entry {
    std::string key;
    CachedQueryResult value;
  };

  using ListIt = std::list<Entry>::iterator;

  size_t max_entries_;
  size_t max_rows_per_entry_;

  std::list<Entry> lru_;
  std::unordered_map<std::string, ListIt> map_;
  std::mutex mutex_;
};

}  // namespace flexql

#endif
