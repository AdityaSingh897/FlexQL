# FlexQL

FlexQL is a modular C++ client-server SQL-like database driver.

## Implemented Features

- Multithreaded server handling multiple clients concurrently
- Client REPL using required C API
- Required C API:
  - `typedef struct FlexQL FlexQL;`
  - `int flexql_open(const char *host, int port, FlexQL **db);`
  - `int flexql_close(FlexQL *db);`
  - `int flexql_exec(FlexQL *db, const char *sql, int (*callback)(void *, int, char **, char **), void *arg, char **errmsg);`
  - `void flexql_free(void *ptr);`
- Error codes:
  - `FLEXQL_OK` = `0`
  - `FLEXQL_ERROR` = `1`
- SQL subset:
  - `CREATE TABLE`
  - `INSERT ... VALUES (...) EXPIRES 'YYYY-MM-DD HH:MM:SS'`
  - `SELECT *` and `SELECT col1, col2`
  - Single-condition `WHERE` (no `AND`/`OR`)
  - `INNER JOIN ... ON ...` with optional single `WHERE`
- Expiration filtering on every read path
- Primary-key index for fast lookup
- LRU query cache for repeated `SELECT`
- Persistent write-ahead log (WAL) for durability and crash recovery
- Batch insert support:
  - `INSERT INTO table VALUES (...), (...), ... EXPIRES 'YYYY-MM-DD HH:MM:SS';`
- Default expiration support:
  - If `EXPIRES` is omitted, the server uses a default TTL of 60 seconds.
  - Can be configured with `FLEXQL_DEFAULT_TTL_SECONDS`.
  - Strict mode can be re-enabled via `FLEXQL_REQUIRE_EXPIRES=1`.

## Project Structure

```text
include/flexql/
  flexql.h
  parser.hpp
  database.hpp
  table.hpp
  wal.hpp
  primary_index.hpp
  query_cache.hpp
  executor.hpp
  network_protocol.hpp
  datetime_utils.hpp
  string_utils.hpp
  value_utils.hpp
  types.hpp

src/
  api/
  cache/
  client/
  index/
  network/
  parser/
  query/
  server/
  storage/
  utils/
```

## Build


### Makefile

```bash
make all
```

Individual targets:

```bash
make server
make client
make bench
```

### Direct g++ build

```bash
mkdir -p build

# Server
g++ -std=c++17 -O3 -pthread -Iinclude \
  src/utils/string_utils.cpp \
  src/utils/datetime_utils.cpp \
  src/utils/value_utils.cpp \
  src/parser/parser.cpp \
  src/index/primary_index.cpp \
  src/storage/table.cpp \
  src/storage/database.cpp \
  src/storage/wal.cpp \
  src/cache/query_cache.cpp \
  src/network/network_protocol.cpp \
  src/query/executor.cpp \
  src/server/server.cpp \
  src/server/server_main.cpp \
  -o build/flexql-server

# Client
g++ -std=c++17 -O3 -pthread -Iinclude \
  src/utils/string_utils.cpp \
  src/utils/datetime_utils.cpp \
  src/utils/value_utils.cpp \
  src/network/network_protocol.cpp \
  src/api/flexql_api.cpp \
  src/client/repl.cpp \
  -o build/flexql-client

# Benchmark driver
g++ -std=c++17 -O3 -pthread -Iinclude \
  src/utils/string_utils.cpp \
  src/utils/datetime_utils.cpp \
  src/utils/value_utils.cpp \
  src/network/network_protocol.cpp \
  src/api/flexql_api.cpp \
  src/client/bench_driver.cpp \
  -o build/flexql-bench
```

## Run

Terminal 1:

```bash
make run-server PORT=9000
```

Optional runtime expiration configuration:

```bash
FLEXQL_DEFAULT_TTL_SECONDS=60 FLEXQL_REQUIRE_EXPIRES=0 make run-server PORT=9000
```

Terminal 2:

```bash
make run-client HOST=127.0.0.1 PORT=9000
```

## Dedicated Benchmark Command Sequence

1. Start server:

```bash
make run-server PORT=9000
```

2. Build benchmark driver (if not already built):

```bash
make bench
```

3. Run benchmark sequence:

```bash
make benchmark HOST=127.0.0.1 PORT=9000 ROWS=1000000 REPEATS=5 INSERT_BATCH_SIZE=1000
```

For your final stress test, replace `1000000` with `10000000`.

Optional fresh run (clears persisted data):

```bash
rm -rf data
```

## REPL Example

```sql
CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR, score DECIMAL, created DATETIME);
INSERT INTO users VALUES (1, 'Alice', 9.5, '2026-01-01 00:00:00') EXPIRES '2099-01-01 00:00:00';
INSERT INTO users VALUES (2, 'Bob', 8.1, '2026-01-01 10:05:00'); -- uses default TTL if strict mode disabled
SELECT * FROM users;
SELECT name, score FROM users WHERE id = 1;
```
# FlexQL
