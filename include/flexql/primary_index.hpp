#ifndef FLEXQL_PRIMARY_INDEX_HPP
#define FLEXQL_PRIMARY_INDEX_HPP

#include <cstddef>
#include <optional>
#include <string>
#include <unordered_map>

namespace flexql {

class PrimaryIndex {
 public:
  bool Insert(const std::string &key, size_t row_id);
  std::optional<size_t> Find(const std::string &key) const;

 private:
  std::unordered_map<std::string, size_t> rows_;
};

}  // namespace flexql

#endif
