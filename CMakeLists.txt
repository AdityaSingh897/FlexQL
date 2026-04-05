cmake_minimum_required(VERSION 3.16)
project(FlexQL LANGUAGES C CXX)

set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

add_library(flexql_common
  src/utils/string_utils.cpp
  src/utils/datetime_utils.cpp
  src/utils/value_utils.cpp
  src/parser/parser.cpp
  src/index/primary_index.cpp
  src/storage/table.cpp
  src/storage/database.cpp
  src/storage/wal.cpp
  src/cache/query_cache.cpp
  src/network/network_protocol.cpp
  src/query/executor.cpp
)

target_include_directories(flexql_common PUBLIC include)
target_link_libraries(flexql_common PUBLIC pthread)
target_compile_options(flexql_common PRIVATE -O3)

add_library(flexql_api
  src/api/flexql_api.cpp
)

target_include_directories(flexql_api PUBLIC include)
target_link_libraries(flexql_api PUBLIC flexql_common pthread)
target_compile_options(flexql_api PRIVATE -O3)

add_executable(flexql-server
  src/server/server.cpp
  src/server/server_main.cpp
)

target_include_directories(flexql-server PRIVATE include)
target_link_libraries(flexql-server PRIVATE flexql_common pthread)
target_compile_options(flexql-server PRIVATE -O3)

add_executable(flexql-client
  src/client/repl.cpp
)

target_include_directories(flexql-client PRIVATE include)
target_link_libraries(flexql-client PRIVATE flexql_api pthread)
target_compile_options(flexql-client PRIVATE -O3)

add_executable(flexql-bench
  src/client/bench_driver.cpp
)

target_include_directories(flexql-bench PRIVATE include)
target_link_libraries(flexql-bench PRIVATE flexql_api pthread)
target_compile_options(flexql-bench PRIVATE -O3)
