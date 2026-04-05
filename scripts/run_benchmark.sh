#!/usr/bin/env bash
set -euo pipefail

HOST="${1:-127.0.0.1}"
PORT="${2:-9000}"
ROWS="${3:-1000000}"
REPEATS="${4:-5}"
INSERT_BATCH_SIZE="${5:-1000}"

if [[ ! -x ./build/flexql-bench ]]; then
  echo "build/flexql-bench not found. Build binaries first." >&2
  echo "Example build command:" >&2
  echo "g++ -std=c++17 -O3 -pthread -Iinclude src/utils/string_utils.cpp src/utils/datetime_utils.cpp src/utils/value_utils.cpp src/network/network_protocol.cpp src/api/flexql_api.cpp src/client/bench_driver.cpp -o build/flexql-bench" >&2
  exit 1
fi

./build/flexql-bench "$HOST" "$PORT" "$ROWS" "$REPEATS" "$INSERT_BATCH_SIZE"
