#include "flexql/query_cache.hpp"

#include "flexql/datetime_utils.hpp"

namespace flexql {

QueryCache::QueryCache(size_t max_entries, size_t max_rows_per_entry)
    : max_entries_(max_entries), max_rows_per_entry_(max_rows_per_entry) {}

std::optional<CachedQueryResult> QueryCache::Get(const std::string &key) {
  std::lock_guard lock(mutex_);
  const auto it = map_.find(key);
  if (it == map_.end()) {
    return std::nullopt;
  }

  if (it->second->value.valid_until_epoch <= CurrentEpochSeconds()) {
    lru_.erase(it->second);
    map_.erase(it);
    return std::nullopt;
  }

  lru_.splice(lru_.begin(), lru_, it->second);
  return it->second->value;
}

void QueryCache::Put(const std::string &key, CachedQueryResult value) {
  if (value.rows.size() > max_rows_per_entry_) {
    return;
  }

  std::lock_guard lock(mutex_);

  const auto it = map_.find(key);
  if (it != map_.end()) {
    it->second->value = std::move(value);
    lru_.splice(lru_.begin(), lru_, it->second);
    return;
  }

  lru_.push_front(Entry{key, std::move(value)});
  map_[key] = lru_.begin();

  while (map_.size() > max_entries_) {
    auto last = lru_.end();
    --last;
    map_.erase(last->key);
    lru_.pop_back();
  }
}

void QueryCache::Clear() {
  std::lock_guard lock(mutex_);
  map_.clear();
  lru_.clear();
}

}  // namespace flexql
