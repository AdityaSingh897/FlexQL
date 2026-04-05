# FlexQL Design Document

## 1. Data Storage Design

### Storage Model
- Chosen model: **disk-backed WAL (source of truth) + column-major in-memory working set**.
- Reason:
  - Faster projection (`SELECT col1, col2`) because only required columns are accessed.
  - Better cache locality for scan-heavy `WHERE` filtering.
  - Persistent durability comes from WAL on disk; memory accelerates execution.

### Internal Representation
- Database:
  - `Database` owns table map (`unordered_map<string, shared_ptr<Table>>`) with case-insensitive table lookup.
- Table:
  - Schema: vector of `ColumnDef`.
  - Column data:
    - `INT` and `DATETIME`: `vector<int64_t>`
    - `DECIMAL`: `vector<double>`
    - `VARCHAR`: `vector<string>`
  - Row expiration: `vector<int64_t> expires_at_` (epoch seconds).

### Schema Access
- Column names are mapped once (`unordered_map<lowercase_name, column_index>`) for O(1) column resolution.

## 2. Indexing Method

### Primary Index
- Exactly one primary key per table (first column is default if no explicit `PRIMARY KEY` is declared).
- Data structure: `unordered_map<string, row_id>` (`PrimaryIndex`).
- Key format is type-tagged serialized scalar (`I:...`, `D:...`, `S:...`, `T:...`) to avoid collisions across types.

### Where It Is Used
- Fast path for single-table queries:
  - `SELECT ... FROM table WHERE primary_key = value`
- Fast path for joins:
  - If one join-side column is a primary key, lookup uses index instead of full hash join.

## 3. Caching Strategy

### Cache Type
- LRU query cache (`QueryCache`) for repeated `SELECT` statements.
- Key: trimmed SQL string.
- Value: column names + result rows.

### Policy
- LRU replacement with max entry count.
- Entry row-cap prevents large-result memory blowups.
- Cache invalidation on write operations:
  - `CREATE TABLE`
  - `INSERT`

## 4. Persistence and Fault Tolerance

### Persistent Storage
- Durability is implemented using a **write-ahead log (WAL)** file on disk:
  - Path: `data/flexql.wal`
  - Each record is stored as:
    - 4-byte length prefix
    - SQL payload bytes
- Every mutating statement (`CREATE TABLE`, `INSERT`) is fsynced before success is returned to the client.

### Recovery
- On server startup:
  - WAL is opened
  - All records are replayed in order through the same execution engine
  - In-memory indexes/cache are rebuilt from replayed data
- Crash tolerance:
  - Truncated WAL tail records are ignored during replay
  - This allows recovery from process/system crashes during write.

## 5. Expiration Timestamp Handling

### Insert
- Supported syntax:
  - `INSERT INTO table VALUES (...) EXPIRES 'YYYY-MM-DD HH:MM:SS';`
- Also supported:
  - `INSERT INTO table VALUES (...);` (default expiration mode)
- Stored as epoch seconds.
- Default expiration behavior:
  - If `EXPIRES` is not provided, server uses `now + default_ttl_seconds`.
  - Default `default_ttl_seconds = 60`.
  - Runtime configurable via `FLEXQL_DEFAULT_TTL_SECONDS`.
  - Strict requirement can be enabled with `FLEXQL_REQUIRE_EXPIRES=1`.
- Durability note:
  - For missing `EXPIRES`, server rewrites SQL with a resolved absolute timestamp before WAL append.
  - This keeps replay deterministic after restart/crash.

### Query Behavior
- Expired rows are filtered out in:
  - `SELECT`
  - `WHERE`
  - `INNER JOIN`
- Expiration check is applied before row output.

## 6. Multithreading and Concurrency Control

### Server Concurrency
- TCP server accepts multiple clients.
- Each client connection is handled by a dedicated thread.

### Data Safety
- Database table map guarded by `shared_mutex`:
  - shared lock for reads
  - unique lock for creates
- Each table guarded by its own `shared_mutex`:
  - shared lock for select/join reads
  - unique lock for insert writes
- Join acquires two table locks in deterministic order (lexicographic table name) to avoid deadlocks.
- Query cache guarded by mutex.
- Mutating operations are serialized with a commit mutex so WAL order and applied order stay consistent.

## 7. SQL Subset Implemented

- `CREATE TABLE table_name (column TYPE, ...);`
- `INSERT INTO table_name VALUES (... ) EXPIRES '...';`
- `INSERT INTO table_name VALUES (...), (...), ... EXPIRES '...';` (batch insert)
- `INSERT INTO table_name VALUES (...);` (uses default TTL when strict mode is disabled)
- `SELECT * FROM table_name;`
- `SELECT col1, col2 FROM table_name;`
- `SELECT ... FROM table_name WHERE column op value;`
  - One condition only
  - No `AND`/`OR`
- `SELECT ... FROM tableA INNER JOIN tableB ON tableA.col = tableB.col [WHERE ...];`

## 8. Client API and REPL

### Required API
- `flexql_open`
- `flexql_close`
- `flexql_exec`
- `flexql_free`

### Opaque Handle
- `FlexQL` is opaque in public header.
- Internal socket/mutex state is hidden in API implementation.

### Callback Semantics
- For `SELECT`, callback is called once per returned row with:
  - `columnCount`
  - `values[]`
  - `columnNames[]`
- Callback return `1` aborts further callback delivery (remaining rows are drained to keep protocol consistent).

## 9. Network Protocol

- Client sends SQL as length-prefixed string.
- Server responds with:
  - status
  - error string
  - on success: column metadata + streamed row values.

## 10. Performance-Oriented Decisions for 10M Rows

- Column-major layout reduces unnecessary column reads.
- Primary-key hash index gives O(1)-average key lookup.
- Join path uses:
  - primary-index lookup when possible
  - otherwise hash join with smaller-side build.
- LRU cache accelerates repeated read queries.
- Batch insert support reduces network round trips and per-commit fsync overhead.
- Build flags use `-O3`.

## 11. Benchmark Driver Sequence

- Added a dedicated benchmark driver executable: `flexql-bench`.
- It runs a fixed workflow:
  - Create a benchmark table
  - Insert `N` rows with expiration timestamps using configurable batch size
  - Measure first point lookup
  - Measure repeated point lookup (cache impact)
  - Measure full scan select
- Wrapper script: `scripts/run_benchmark.sh`.
- Output is machine-readable key/value timing lines for easy reporting.

## 12. Performance Results

The following measurements were collected on April 3, 2026 using:

- Command: `make benchmark HOST=127.0.0.1 PORT=9000 ROWS=<...> REPEATS=<...> INSERT_BATCH_SIZE=1000`
- Workload: local client/server benchmark driver with persistent WAL enabled.

### Run A: 100,000 rows

- Config:
  - `rows=100000`
  - `repeats=3`
  - `insert_batch_size=1000`
- Results:
  - `INSERT_SECONDS=8.84584`
  - `POINT_SELECT_ROWS_FIRST=1`
  - `POINT_SELECT_SECONDS_FIRST=0.0820063`
  - `POINT_SELECT_ROWS_REPEATED_TOTAL=3`
  - `POINT_SELECT_SECONDS_REPEATED_TOTAL=0.247002`
  - `POINT_SELECT_SECONDS_REPEATED_AVG=0.0823339`
  - `FULL_SCAN_ROWS=100000`
  - `FULL_SCAN_SECONDS=0.363071`

### Run B: 10,000,000 rows

- Config:
  - `rows=10000000`
  - `repeats=5`
  - `insert_batch_size=1000`
- Results:
  - `INSERT_SECONDS=920.998`
  - `POINT_SELECT_ROWS_FIRST=1`
  - `POINT_SELECT_SECONDS_FIRST=0.0824704`
  - `POINT_SELECT_ROWS_REPEATED_TOTAL=5`
  - `POINT_SELECT_SECONDS_REPEATED_TOTAL=0.40964`
  - `POINT_SELECT_SECONDS_REPEATED_AVG=0.0819279`
  - `FULL_SCAN_ROWS=10000000`
  - `FULL_SCAN_SECONDS=21.3331`
