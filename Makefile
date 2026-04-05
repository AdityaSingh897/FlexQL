CXX ?= g++
CXXFLAGS ?= -std=c++17 -O3 -pthread -Iinclude

BUILD_DIR := build

SERVER_BIN := $(BUILD_DIR)/flexql-server
CLIENT_BIN := $(BUILD_DIR)/flexql-client
BENCH_BIN := $(BUILD_DIR)/flexql-bench

UTIL_SRCS := \
	src/utils/string_utils.cpp \
	src/utils/datetime_utils.cpp \
	src/utils/value_utils.cpp

SERVER_SRCS := \
	$(UTIL_SRCS) \
	src/parser/parser.cpp \
	src/index/primary_index.cpp \
	src/storage/table.cpp \
	src/storage/database.cpp \
	src/storage/wal.cpp \
	src/cache/query_cache.cpp \
	src/network/network_protocol.cpp \
	src/query/executor.cpp \
	src/server/server.cpp \
	src/server/server_main.cpp

CLIENT_SRCS := \
	$(UTIL_SRCS) \
	src/network/network_protocol.cpp \
	src/api/flexql_api.cpp \
	src/client/repl.cpp

BENCH_SRCS := \
	$(UTIL_SRCS) \
	src/network/network_protocol.cpp \
	src/api/flexql_api.cpp \
	src/client/bench_driver.cpp

HOST ?= 127.0.0.1
PORT ?= 9000
ROWS ?= 1000000
REPEATS ?= 5
INSERT_BATCH_SIZE ?= 1000

.PHONY: all server client bench clean run-server run-client benchmark clean-data

all: server client bench

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

server: $(SERVER_BIN)

client: $(CLIENT_BIN)

bench: $(BENCH_BIN)

$(SERVER_BIN): $(SERVER_SRCS) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) $(SERVER_SRCS) -o $(SERVER_BIN)

$(CLIENT_BIN): $(CLIENT_SRCS) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) $(CLIENT_SRCS) -o $(CLIENT_BIN)

$(BENCH_BIN): $(BENCH_SRCS) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) $(BENCH_SRCS) -o $(BENCH_BIN)

run-server: server
	$(SERVER_BIN) $(PORT)

run-client: client
	$(CLIENT_BIN) $(HOST) $(PORT)

benchmark: bench
	./scripts/run_benchmark.sh $(HOST) $(PORT) $(ROWS) $(REPEATS) $(INSERT_BATCH_SIZE)

clean:
	rm -f $(SERVER_BIN) $(CLIENT_BIN) $(BENCH_BIN)

clean-data:
	rm -rf data
