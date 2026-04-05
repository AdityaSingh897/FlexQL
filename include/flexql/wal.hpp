#ifndef FLEXQL_WAL_HPP
#define FLEXQL_WAL_HPP

#include <functional>
#include <mutex>
#include <string>

namespace flexql {

class WriteAheadLog {
 public:
  explicit WriteAheadLog(std::string path);
  ~WriteAheadLog();

  WriteAheadLog(const WriteAheadLog &) = delete;
  WriteAheadLog &operator=(const WriteAheadLog &) = delete;

  bool Open(std::string *error);
  bool AppendRecord(const std::string &sql, std::string *error);
  bool Replay(const std::function<bool(const std::string &, std::string *)> &apply, std::string *error);

 private:
  std::string path_;
  int fd_;
  std::mutex mutex_;
};

}  // namespace flexql

#endif
