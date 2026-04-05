#include "flexql/primary_index.hpp"

namespace flexql {

bool PrimaryIndex::Insert(const std::string &key, size_t row_id) {
  const auto [it, inserted] = rows_.emplace(key, row_id);
  return inserted;
}

std::optional<size_t> PrimaryIndex::Find(const std::string &key) const {
  const auto it = rows_.find(key);
  if (it == rows_.end()) {
    return std::nullopt;
  }
  return it->second;
}

}  // namespace flexql
