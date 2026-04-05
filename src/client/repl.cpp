#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <string>

#include "flexql/flexql.h"
#include "flexql/string_utils.hpp"

namespace {

struct PrintState {
  bool printed_any = false;
};

int PrintCallback(void *data, int column_count, char **values, char **column_names) {
  auto *state = static_cast<PrintState *>(data);
  for (int i = 0; i < column_count; ++i) {
    const char *col = column_names[i] != nullptr ? column_names[i] : "";
    const char *val = values[i] != nullptr ? values[i] : "NULL";
    std::printf("%s = %s\n", col, val);
  }
  std::printf("\n");
  state->printed_any = true;
  return 0;
}

}  // namespace

int main(int argc, char **argv) {
  if (argc < 3) {
    std::cerr << "Usage: " << argv[0] << " <host> <port>\n";
    return 1;
  }

  const char *host = argv[1];
  const int port = std::atoi(argv[2]);

  FlexQL *db = nullptr;
  if (flexql_open(host, port, &db) != FLEXQL_OK) {
    std::cerr << "Cannot connect to FlexQL server\n";
    return 1;
  }

  std::cout << "Connected to FlexQL server\n";

  while (true) {
    std::cout << "flexql> " << std::flush;

    std::string query;
    if (!std::getline(std::cin, query)) {
      break;
    }

    std::string trimmed = flexql::Trim(query);
    if (trimmed.empty()) {
      continue;
    }

    if (trimmed == ".exit" || trimmed == "exit") {
      break;
    }

    while (trimmed.find(';') == std::string::npos) {
      std::string continuation;
      if (!std::getline(std::cin, continuation)) {
        break;
      }
      query.append(" ").append(continuation);
      trimmed = flexql::Trim(query);
    }

    PrintState state;
    char *errmsg = nullptr;
    const int rc = flexql_exec(db, query.c_str(), PrintCallback, &state, &errmsg);
    if (rc != FLEXQL_OK) {
      std::cerr << "SQL error: " << (errmsg != nullptr ? errmsg : "unknown") << '\n';
      if (errmsg != nullptr) {
        flexql_free(errmsg);
      }
      continue;
    }

    if (errmsg != nullptr) {
      flexql_free(errmsg);
    }
  }

  flexql_close(db);
  std::cout << "Connection closed\n";
  return 0;
}
