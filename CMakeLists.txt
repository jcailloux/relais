cmake_minimum_required(VERSION 3.20)
project(jcailloux-relais LANGUAGES CXX)

add_library(jcailloux-relais INTERFACE)
add_library(jcailloux::relais ALIAS jcailloux-relais)

target_include_directories(jcailloux-relais INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

target_compile_features(jcailloux-relais INTERFACE cxx_std_23)

# Workaround for GCC 15 bug with AVX2 intrinsics in avx2intrin.h
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "15")
    target_compile_options(jcailloux-relais INTERFACE -mno-avx2)
endif()

# =============================================================================
# Dependencies
# =============================================================================
include(FetchContent)

FetchContent_Declare(
    shardmap
    GIT_REPOSITORY https://github.com/jcailloux/shardmap
    GIT_TAG        ad69bd2909c9425260a8b36cb16053d72d837cb8
)
FetchContent_Declare(
    glaze
    GIT_REPOSITORY https://github.com/stephenberry/glaze.git
    GIT_TAG        v7.0.2
    FIND_PACKAGE_ARGS
)
FetchContent_MakeAvailable(shardmap glaze)

# Direct I/O dependencies
find_package(PostgreSQL REQUIRED)

# Link all dependencies
target_link_libraries(jcailloux-relais INTERFACE
    shardmap
    glaze::glaze
    PostgreSQL::PostgreSQL
)

# =============================================================================
# Tests
# =============================================================================

option(RELAIS_BUILD_TESTS "Build relais tests" ON)

if(RELAIS_BUILD_TESTS)
    enable_testing()

    # Fetch Catch2 for testing
    FetchContent_Declare(Catch2
        GIT_REPOSITORY https://github.com/catchorg/Catch2.git
        GIT_TAG        v3.12.0
    )

    FetchContent_GetProperties(Catch2)
    if(NOT catch2_POPULATED)
        FetchContent_MakeAvailable(Catch2)
    endif()

    list(APPEND CMAKE_MODULE_PATH ${catch2_SOURCE_DIR}/extras)
    include(CTest)
    include(Catch)

    # -------------------------------------------------------------------------
    # Integration Tests (require PostgreSQL and Redis)
    # -------------------------------------------------------------------------
    # These tests require:
    #   - PostgreSQL database with relais_test_items table
    #   - Redis server
    #
    # Environment variables:
    #   DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS
    #   REDIS_HOST (optional, default: 127.0.0.1)
    #   REDIS_PORT (optional, default: 6379)
    #
    # To set up the test database:
    #   psql -U postgres -c "CREATE DATABASE relais_test"
    #   psql -U postgres -d relais_test -f tests/migrations/000001_create_test_items.sql
    # -------------------------------------------------------------------------

    # Test fixtures include directory
    set(RELAIS_TEST_FIXTURES_DIR ${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures)

    add_executable(relais_tests
        tests/relais/test_generated_wrapper.cpp
    )

    target_compile_definitions(relais_tests
            PRIVATE
                RELAIS_BUILDING_TESTS
    )

    target_include_directories(relais_tests PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/tests
    )

    target_link_libraries(relais_tests
            PRIVATE
            jcailloux::relais
            Catch2::Catch2WithMain
    )

    # Discover tests for CTest
    list(APPEND CMAKE_MODULE_PATH ${catch2_SOURCE_DIR}/extras)
    include(CTest)
    include(Catch)
    catch_discover_tests(relais_tests)

    # Test: BaseRepository (no caching, direct DB access)
    add_executable(test_relais_base
        tests/relais/test_base_repository.cpp
    )
    target_include_directories(test_relais_base PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
    )
    target_compile_definitions(test_relais_base PRIVATE RELAIS_BUILDING_TESTS)
    target_link_libraries(test_relais_base PRIVATE
        jcailloux::relais
        Catch2::Catch2WithMain
    )
    catch_discover_tests(test_relais_base
        PROPERTIES LABELS "relais;integration;db"
                   RESOURCE_LOCK "db_redis"
    )

    # Test: RedisRepository (L2 Redis caching)
    add_executable(test_relais_redis
        tests/relais/test_redis_repository.cpp
    )
    target_include_directories(test_relais_redis PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
    )
    target_compile_definitions(test_relais_redis PRIVATE RELAIS_BUILDING_TESTS)
    target_link_libraries(test_relais_redis PRIVATE
        jcailloux::relais
        Catch2::Catch2WithMain
    )
    catch_discover_tests(test_relais_redis
        PROPERTIES LABELS "relais;integration;db;redis"
                   RESOURCE_LOCK "db_redis"
    )

    # Test: ListMixin (L1 list cache invalidation)
    add_executable(test_relais_decl_list_cache
        tests/relais/test_decl_list_cache.cpp
    )
    target_include_directories(test_relais_decl_list_cache PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
    )
    target_compile_definitions(test_relais_decl_list_cache PRIVATE RELAIS_BUILDING_TESTS)
    target_link_libraries(test_relais_decl_list_cache PRIVATE
        jcailloux::relais
        Catch2::Catch2WithMain
    )
    catch_discover_tests(test_relais_decl_list_cache
        PROPERTIES LABELS "relais;integration;db;list"
                   RESOURCE_LOCK "db_redis"
    )

    # Test: CachedRepository (L1 caching)
    add_executable(test_relais_cached
        tests/relais/test_cached_repository.cpp
    )
    target_include_directories(test_relais_cached PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
    )
    target_compile_definitions(test_relais_cached PRIVATE RELAIS_BUILDING_TESTS)
    target_link_libraries(test_relais_cached PRIVATE
        jcailloux::relais
        Catch2::Catch2WithMain
    )
    catch_discover_tests(test_relais_cached
        PROPERTIES LABELS "relais;integration;db;cache"
                   RESOURCE_LOCK "db_redis"
    )

    # Test: PartialKey (composite PK with partial key queries)
    add_executable(test_relais_partial_key
        tests/relais/test_partial_key.cpp
    )
    target_include_directories(test_relais_partial_key PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
    )
    target_compile_definitions(test_relais_partial_key PRIVATE RELAIS_BUILDING_TESTS)
    target_link_libraries(test_relais_partial_key PRIVATE
        jcailloux::relais
        Catch2::Catch2WithMain
    )
    catch_discover_tests(test_relais_partial_key
        PROPERTIES LABELS "relais;integration;db;partial-key"
                   RESOURCE_LOCK "db_redis"
    )

    # Test: FullCache (L1+L2 cache hierarchy)
    add_executable(test_relais_full_cache
        tests/relais/test_full_cache.cpp
    )
    target_include_directories(test_relais_full_cache PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
    )
    target_compile_definitions(test_relais_full_cache PRIVATE RELAIS_BUILDING_TESTS)
    target_link_libraries(test_relais_full_cache PRIVATE
        jcailloux::relais
        Catch2::Catch2WithMain
    )
    catch_discover_tests(test_relais_full_cache
        PROPERTIES LABELS "relais;integration;db;full-cache"
                   RESOURCE_LOCK "db_redis"
    )

    # Test: DeclList Redis (declarative list caching at L2)
    add_executable(test_relais_decl_list_redis
        tests/relais/test_decl_list_redis.cpp
    )
    target_include_directories(test_relais_decl_list_redis PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
    )
    target_compile_definitions(test_relais_decl_list_redis PRIVATE RELAIS_BUILDING_TESTS)
    target_link_libraries(test_relais_decl_list_redis PRIVATE
        jcailloux::relais
        Catch2::Catch2WithMain
    )
    catch_discover_tests(test_relais_decl_list_redis
        PROPERTIES LABELS "relais;integration;db;redis;list"
                   RESOURCE_LOCK "db_redis"
    )

    # Test: DeclList Full (declarative list caching at L1+L2)
    add_executable(test_relais_decl_list_full
        tests/relais/test_decl_list_full.cpp
    )
    target_include_directories(test_relais_decl_list_full PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
    )
    target_compile_definitions(test_relais_decl_list_full PRIVATE RELAIS_BUILDING_TESTS)
    target_link_libraries(test_relais_decl_list_full PRIVATE
        jcailloux::relais
        Catch2::Catch2WithMain
    )
    catch_discover_tests(test_relais_decl_list_full
        PROPERTIES LABELS "relais;integration;db;full-cache;list"
                   RESOURCE_LOCK "db_redis"
    )

    # Test: Warmup (cache infrastructure priming)
    add_executable(test_relais_warmup
        tests/relais/test_warmup.cpp
    )
    target_include_directories(test_relais_warmup PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
    )
    target_compile_definitions(test_relais_warmup PRIVATE RELAIS_BUILDING_TESTS)
    target_link_libraries(test_relais_warmup PRIVATE
        jcailloux::relais
        Catch2::Catch2WithMain
    )
    catch_discover_tests(test_relais_warmup
        PROPERTIES LABELS "relais;integration;db;warmup"
                   RESOURCE_LOCK "db_redis"
    )

    # Test: L1 Config (exhaustive L1 parameter testing)
    add_executable(test_relais_l1_config
        tests/relais/config/test_l1_config.cpp
    )
    target_include_directories(test_relais_l1_config PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
    )
    target_compile_definitions(test_relais_l1_config PRIVATE RELAIS_BUILDING_TESTS)
    target_link_libraries(test_relais_l1_config PRIVATE
        jcailloux::relais
        Catch2::Catch2WithMain
    )
    catch_discover_tests(test_relais_l1_config
        PROPERTIES LABELS "relais;integration;db;config"
                   RESOURCE_LOCK "db_redis"
    )

    # Test: L2 Config (exhaustive L2 parameter testing)
    add_executable(test_relais_l2_config
        tests/relais/config/test_l2_config.cpp
    )
    target_include_directories(test_relais_l2_config PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
    )
    target_compile_definitions(test_relais_l2_config PRIVATE RELAIS_BUILDING_TESTS)
    target_link_libraries(test_relais_l2_config PRIVATE
        jcailloux::relais
        Catch2::Catch2WithMain
    )
    catch_discover_tests(test_relais_l2_config
        PROPERTIES LABELS "relais;integration;db;redis;config"
                   RESOURCE_LOCK "db_redis"
    )

    # Test: Concurrency (stress testing with parallel operations)
    add_executable(test_relais_concurrency
        tests/relais/test_concurrency.cpp
    )
    target_include_directories(test_relais_concurrency PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
    )
    target_compile_definitions(test_relais_concurrency PRIVATE RELAIS_BUILDING_TESTS)
    target_link_libraries(test_relais_concurrency PRIVATE
        jcailloux::relais
        Catch2::Catch2WithMain
    )
    catch_discover_tests(test_relais_concurrency
        PROPERTIES LABELS "relais;integration;db;concurrency"
i                   RESOURCE_LOCK "db_redis"
    )

    # Thread Sanitizer support for concurrency tests
    option(RELAIS_ENABLE_TSAN "Enable Thread Sanitizer for concurrency tests" OFF)
    if(RELAIS_ENABLE_TSAN)
        target_compile_options(test_relais_concurrency PRIVATE
            -fsanitize=thread -fno-omit-frame-pointer)
        target_link_options(test_relais_concurrency PRIVATE -fsanitize=thread)
        message(STATUS "Thread Sanitizer enabled for test_relais_concurrency")
    endif()

    # -------------------------------------------------------------------------
    # Combined test target for running all integration tests
    # -------------------------------------------------------------------------
    add_custom_target(test_relais_integration
        DEPENDS
            relais_tests
            test_relais_base
            test_relais_redis
            test_relais_cached
            test_relais_decl_list_cache
            test_relais_partial_key
            test_relais_full_cache
            test_relais_decl_list_redis
            test_relais_decl_list_full
            test_relais_warmup
            test_relais_l1_config
            test_relais_l2_config
            test_relais_concurrency
        COMMENT "Build all relais integration tests"
    )

    # Combined test executable: all tests in a single Catch2 binary
    add_executable(test_relais_all
        tests/relais/test_all_runner.cpp
        tests/relais/test_generated_wrapper.cpp
        tests/relais/test_base_repository.cpp
        tests/relais/test_redis_repository.cpp
        tests/relais/test_cached_repository.cpp
        tests/relais/test_decl_list_cache.cpp
        tests/relais/test_partial_key.cpp
        tests/relais/test_full_cache.cpp
        tests/relais/test_decl_list_redis.cpp
        tests/relais/test_decl_list_full.cpp
        tests/relais/test_warmup.cpp
        tests/relais/config/test_l1_config.cpp
        tests/relais/config/test_l2_config.cpp
        tests/relais/test_concurrency.cpp
    )
    target_include_directories(test_relais_all PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
    )
    target_compile_definitions(test_relais_all PRIVATE
        RELAIS_BUILDING_TESTS
        RELAIS_COMBINED_TESTS
    )
    target_link_libraries(test_relais_all PRIVATE
        jcailloux::relais
        Catch2::Catch2WithMain
    )

    # -------------------------------------------------------------------------
    # DbProvider + Log tests
    # -------------------------------------------------------------------------
    add_executable(test_relais_dbprovider_log
        tests/relais/test_dbprovider_log.cpp
    )
    target_include_directories(test_relais_dbprovider_log PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
    )
    target_compile_definitions(test_relais_dbprovider_log PRIVATE RELAIS_BUILDING_TESTS)
    target_link_libraries(test_relais_dbprovider_log PRIVATE
        jcailloux::relais
        Catch2::Catch2WithMain
    )
    catch_discover_tests(test_relais_dbprovider_log
        PROPERTIES LABELS "integration;relais"
                   RESOURCE_LOCK "db_redis"
    )

    # -------------------------------------------------------------------------
    # I/O Tests (io/ layer — unit + integration)
    # -------------------------------------------------------------------------

    # Unit: Task<T> coroutine tests
    add_executable(test_io_task tests/io/test_io_task.cpp)
    target_include_directories(test_io_task PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
    target_link_libraries(test_io_task PRIVATE jcailloux::relais Catch2::Catch2WithMain)
    catch_discover_tests(test_io_task PROPERTIES LABELS "io")

    # Unit: IoContext concept tests
    add_executable(test_io_context tests/io/test_io_context.cpp)
    target_include_directories(test_io_context PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
    target_link_libraries(test_io_context PRIVATE jcailloux::relais Catch2::Catch2WithMain)
    catch_discover_tests(test_io_context PROPERTIES LABELS "io")

    # Unit: PgParams/PgResult type tests
    add_executable(test_io_pg_types tests/io/test_io_pg_types.cpp)
    target_include_directories(test_io_pg_types PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
    target_link_libraries(test_io_pg_types PRIVATE jcailloux::relais Catch2::Catch2WithMain)
    catch_discover_tests(test_io_pg_types PROPERTIES LABELS "io")

    # Unit: Redis types tests
    add_executable(test_io_redis_types tests/io/test_io_redis_types.cpp)
    target_include_directories(test_io_redis_types PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
    target_link_libraries(test_io_redis_types PRIVATE jcailloux::relais Catch2::Catch2WithMain)
    catch_discover_tests(test_io_redis_types PROPERTIES LABELS "io")

    # Compile: PgPool template instantiation
    add_executable(test_io_pg_pool_compile tests/io/test_io_pg_pool_compile.cpp)
    target_include_directories(test_io_pg_pool_compile PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
    target_link_libraries(test_io_pg_pool_compile PRIVATE jcailloux::relais Catch2::Catch2WithMain)
    catch_discover_tests(test_io_pg_pool_compile PROPERTIES LABELS "io")

    # Integration: PostgreSQL (requires running PG)
    add_executable(test_io_pg_integration tests/io/test_io_pg_integration.cpp)
    target_include_directories(test_io_pg_integration PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
    target_link_libraries(test_io_pg_integration PRIVATE jcailloux::relais Catch2::Catch2WithMain)
    catch_discover_tests(test_io_pg_integration
        PROPERTIES LABELS "io;integration" RESOURCE_LOCK "db_redis")

    # Unit: RESP2 writer/parser tests
    add_executable(test_io_resp2 tests/io/test_io_resp2.cpp)
    target_include_directories(test_io_resp2 PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
    target_link_libraries(test_io_resp2 PRIVATE jcailloux::relais Catch2::Catch2WithMain)
    catch_discover_tests(test_io_resp2 PROPERTIES LABELS "io")

    # Integration: Redis (requires running Redis)
    add_executable(test_io_redis_integration tests/io/test_io_redis_integration.cpp)
    target_include_directories(test_io_redis_integration PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
    target_link_libraries(test_io_redis_integration PRIVATE jcailloux::relais Catch2::Catch2WithMain)
    catch_discover_tests(test_io_redis_integration
        PROPERTIES LABELS "io;integration" RESOURCE_LOCK "db_redis")

endif()

# =============================================================================
# Benchmarks
# =============================================================================

option(RELAIS_BUILD_BENCHMARKS "Build relais benchmarks" OFF)

if(RELAIS_BUILD_BENCHMARKS)
    # Fetch Catch2 if not already available from tests
    if(NOT TARGET Catch2::Catch2WithMain)
        FetchContent_Declare(Catch2
            GIT_REPOSITORY https://github.com/catchorg/Catch2.git
            GIT_TAG        v3.12.0
        )
        FetchContent_MakeAvailable(Catch2)
    endif()

    # Benchmark: relais cache hierarchy
    add_executable(bench_relais_cache
        benchmarks/bench_relais_cache.cpp
    )
    target_include_directories(bench_relais_cache PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
    )
    target_compile_definitions(bench_relais_cache PRIVATE RELAIS_BUILDING_TESTS)
    target_link_libraries(bench_relais_cache PRIVATE
        jcailloux::relais
        Catch2::Catch2WithMain
    )

    # Benchmark: Redis I/O layer
    add_executable(bench_io_redis
        benchmarks/bench_io_redis.cpp
    )
    target_include_directories(bench_io_redis PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
    )
    target_link_libraries(bench_io_redis PRIVATE
        jcailloux::relais
        Catch2::Catch2WithMain
    )

    # Benchmark: PostgreSQL I/O layer
    add_executable(bench_io_pg
        benchmarks/bench_io_pg.cpp
    )
    target_include_directories(bench_io_pg PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
    )
    target_link_libraries(bench_io_pg PRIVATE
        jcailloux::relais
        Catch2::Catch2WithMain
    )
endif()
