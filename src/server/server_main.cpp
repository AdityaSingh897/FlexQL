#include <csignal>
#include <iostream>
#include <string>

#include "flexql/server.hpp"

namespace {

flexql::FlexQLServer *g_server = nullptr;

void SignalHandler(int) {
  if (g_server != nullptr) {
    g_server->Stop();
  }
}

}  // namespace

int main(int argc, char **argv) {
  int port = 9000;
  if (argc >= 2) {
    port = std::stoi(argv[1]);
  }

  flexql::FlexQLServer server(port);
  g_server = &server;

  std::signal(SIGINT, SignalHandler);
  std::signal(SIGTERM, SignalHandler);

  std::string error;
  if (!server.Run(&error)) {
    std::cerr << "Server error: " << error << '\n';
    return 1;
  }

  return 0;
}
