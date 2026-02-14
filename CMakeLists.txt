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
    pqcoro
    GIT_REPOSITORY https://github.com/jcailloux/pqcoro.git
    GIT_TAG        75d5a0b573bb77b7851fa0440d23d5927bf80eca
)
FetchContent_Declare(
    glaze
    GIT_REPOSITORY https://github.com/stephenberry/glaze.git
    GIT_TAG        v7.0.2
    FIND_PACKAGE_ARGS
)
FetchContent_MakeAvailable(shardmap pqcoro glaze)

# Link all dependencies
target_link_libraries(jcailloux-relais INTERFACE
    shardmap
    pqcoro
    glaze::glaze
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
        tests/test_generated_wrapper.cpp
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

    # pqcoro test include path (EpollIoContext.h, TestRunner.h)
    set(PQCORO_TEST_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../pqcoro/tests)

    # Test: BaseRepository (no caching, direct DB access)
    add_executable(test_relais_base
        tests/test_base_repository.cpp
    )
    target_include_directories(test_relais_base PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
        ${PQCORO_TEST_INCLUDE_DIR}
    )
    target_compile_definitions(test_relais_base PRIVATE RELAIS_BUILDING_TESTS)
    target_link_libraries(test_relais_base PRIVATE
        jcailloux::relais
        Catch2::Catch2WithMain
    )
    catch_discover_tests(test_relais_base
        PROPERTIES LABELS "integration;db"
    )

    # Test: RedisRepository (L2 Redis caching)
    add_executable(test_relais_redis
        tests/test_redis_repository.cpp
    )
    target_include_directories(test_relais_redis PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
        ${PQCORO_TEST_INCLUDE_DIR}
    )
    target_compile_definitions(test_relais_redis PRIVATE RELAIS_BUILDING_TESTS)
    target_link_libraries(test_relais_redis PRIVATE
        jcailloux::relais
        Catch2::Catch2WithMain
    )
    catch_discover_tests(test_relais_redis
        PROPERTIES LABELS "integration;db;redis"
    )

    # Test: ListMixin (L1 list cache invalidation)
    add_executable(test_relais_decl_list_cache
        tests/test_decl_list_cache.cpp
    )
    target_include_directories(test_relais_decl_list_cache PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
        ${PQCORO_TEST_INCLUDE_DIR}
    )
    target_compile_definitions(test_relais_decl_list_cache PRIVATE RELAIS_BUILDING_TESTS)
    target_link_libraries(test_relais_decl_list_cache PRIVATE
        jcailloux::relais
        Catch2::Catch2WithMain
    )
    catch_discover_tests(test_relais_decl_list_cache
        PROPERTIES LABELS "integration;db;list"
    )

    # Test: CachedRepository (L1 caching)
    add_executable(test_relais_cached
        tests/test_cached_repository.cpp
    )
    target_include_directories(test_relais_cached PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
        ${PQCORO_TEST_INCLUDE_DIR}
    )
    target_compile_definitions(test_relais_cached PRIVATE RELAIS_BUILDING_TESTS)
    target_link_libraries(test_relais_cached PRIVATE
        jcailloux::relais
        Catch2::Catch2WithMain
    )
    catch_discover_tests(test_relais_cached
        PROPERTIES LABELS "integration;db;cache"
    )

    # Test: PartialKey (composite PK with partial key queries)
    add_executable(test_relais_partial_key
        tests/test_partial_key.cpp
    )
    target_include_directories(test_relais_partial_key PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
        ${PQCORO_TEST_INCLUDE_DIR}
    )
    target_compile_definitions(test_relais_partial_key PRIVATE RELAIS_BUILDING_TESTS)
    target_link_libraries(test_relais_partial_key PRIVATE
        jcailloux::relais
        Catch2::Catch2WithMain
    )
    catch_discover_tests(test_relais_partial_key
        PROPERTIES LABELS "integration;db;partial-key"
    )

    # Test: FullCache (L1+L2 cache hierarchy)
    add_executable(test_relais_full_cache
        tests/test_full_cache.cpp
    )
    target_include_directories(test_relais_full_cache PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
        ${PQCORO_TEST_INCLUDE_DIR}
    )
    target_compile_definitions(test_relais_full_cache PRIVATE RELAIS_BUILDING_TESTS)
    target_link_libraries(test_relais_full_cache PRIVATE
        jcailloux::relais
        Catch2::Catch2WithMain
    )
    catch_discover_tests(test_relais_full_cache
        PROPERTIES LABELS "integration;db;full-cache"
    )

    # Test: DeclList Redis (declarative list caching at L2)
    add_executable(test_relais_decl_list_redis
        tests/test_decl_list_redis.cpp
    )
    target_include_directories(test_relais_decl_list_redis PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
        ${PQCORO_TEST_INCLUDE_DIR}
    )
    target_compile_definitions(test_relais_decl_list_redis PRIVATE RELAIS_BUILDING_TESTS)
    target_link_libraries(test_relais_decl_list_redis PRIVATE
        jcailloux::relais
        Catch2::Catch2WithMain
    )
    catch_discover_tests(test_relais_decl_list_redis
        PROPERTIES LABELS "integration;db;redis;list"
    )

    # Test: DeclList Full (declarative list caching at L1+L2)
    add_executable(test_relais_decl_list_full
        tests/test_decl_list_full.cpp
    )
    target_include_directories(test_relais_decl_list_full PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
        ${PQCORO_TEST_INCLUDE_DIR}
    )
    target_compile_definitions(test_relais_decl_list_full PRIVATE RELAIS_BUILDING_TESTS)
    target_link_libraries(test_relais_decl_list_full PRIVATE
        jcailloux::relais
        Catch2::Catch2WithMain
    )
    catch_discover_tests(test_relais_decl_list_full
        PROPERTIES LABELS "integration;db;full-cache;list"
    )

    # Test: Warmup (cache infrastructure priming)
    add_executable(test_relais_warmup
        tests/test_warmup.cpp
    )
    target_include_directories(test_relais_warmup PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
        ${PQCORO_TEST_INCLUDE_DIR}
    )
    target_compile_definitions(test_relais_warmup PRIVATE RELAIS_BUILDING_TESTS)
    target_link_libraries(test_relais_warmup PRIVATE
        jcailloux::relais
        Catch2::Catch2WithMain
    )
    catch_discover_tests(test_relais_warmup
        PROPERTIES LABELS "integration;db;warmup"
    )

    # Test: L1 Config (exhaustive L1 parameter testing)
    add_executable(test_relais_l1_config
        tests/config/test_l1_config.cpp
    )
    target_include_directories(test_relais_l1_config PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
        ${PQCORO_TEST_INCLUDE_DIR}
    )
    target_compile_definitions(test_relais_l1_config PRIVATE RELAIS_BUILDING_TESTS)
    target_link_libraries(test_relais_l1_config PRIVATE
        jcailloux::relais
        Catch2::Catch2WithMain
    )
    catch_discover_tests(test_relais_l1_config
        PROPERTIES LABELS "integration;db;config"
    )

    # Test: L2 Config (exhaustive L2 parameter testing)
    add_executable(test_relais_l2_config
        tests/config/test_l2_config.cpp
    )
    target_include_directories(test_relais_l2_config PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
        ${PQCORO_TEST_INCLUDE_DIR}
    )
    target_compile_definitions(test_relais_l2_config PRIVATE RELAIS_BUILDING_TESTS)
    target_link_libraries(test_relais_l2_config PRIVATE
        jcailloux::relais
        Catch2::Catch2WithMain
    )
    catch_discover_tests(test_relais_l2_config
        PROPERTIES LABELS "integration;db;redis;config"
    )

    # Test: Concurrency (stress testing with parallel operations)
    add_executable(test_relais_concurrency
        tests/test_concurrency.cpp
    )
    target_include_directories(test_relais_concurrency PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
        ${PQCORO_TEST_INCLUDE_DIR}
    )
    target_compile_definitions(test_relais_concurrency PRIVATE RELAIS_BUILDING_TESTS)
    target_link_libraries(test_relais_concurrency PRIVATE
        jcailloux::relais
        Catch2::Catch2WithMain
    )
    catch_discover_tests(test_relais_concurrency
        PROPERTIES LABELS "integration;db;concurrency"
    )

    # Thread Sanitizer support for concurrency tests
    option(RELAIS_ENABLE_TSAN "Enable Thread Sanitizer for concurrency tests" OFF)
    if(RELAIS_ENABLE_TSAN)
        target_compile_options(test_relais_concurrency PRIVATE
            -fsanitize=thread -fno-omit-frame-pointer)
        target_link_options(test_relais_concurrency PRIVATE -fsanitize=thread)
        message(STATUS "Thread Sanitizer enabled for test_relais_concurrency")
    endif()

    # Test: Benchmark (performance measurements, hidden by default)
    add_executable(test_relais_benchmark
        tests/test_benchmark.cpp
    )
    target_include_directories(test_relais_benchmark PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
        ${PQCORO_TEST_INCLUDE_DIR}
    )
    target_compile_definitions(test_relais_benchmark PRIVATE RELAIS_BUILDING_TESTS)
    target_link_libraries(test_relais_benchmark PRIVATE
        jcailloux::relais
        Catch2::Catch2WithMain
    )

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
        tests/test_all_runner.cpp
        tests/test_generated_wrapper.cpp
        tests/test_base_repository.cpp
        tests/test_redis_repository.cpp
        tests/test_cached_repository.cpp
        tests/test_decl_list_cache.cpp
        tests/test_partial_key.cpp
        tests/test_full_cache.cpp
        tests/test_decl_list_redis.cpp
        tests/test_decl_list_full.cpp
        tests/test_warmup.cpp
        tests/config/test_l1_config.cpp
        tests/config/test_l2_config.cpp
        tests/test_concurrency.cpp
    )
    target_include_directories(test_relais_all PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
        ${PQCORO_TEST_INCLUDE_DIR}
    )
    target_compile_definitions(test_relais_all PRIVATE
        RELAIS_BUILDING_TESTS
        RELAIS_COMBINED_TESTS
    )
    target_link_libraries(test_relais_all PRIVATE
        jcailloux::relais
        Catch2::Catch2WithMain
    )
    catch_discover_tests(test_relais_all)

    # -------------------------------------------------------------------------
    # DbProvider + Log tests (depends on pqcoro, NOT on Drogon)
    # -------------------------------------------------------------------------
    # Only built when pqcoro target is available (e.g., from parent project)

    if(TARGET pqcoro::pqcoro OR TARGET pqcoro)
        add_executable(test_relais_dbprovider_log
            tests/test_dbprovider_log.cpp
        )
        target_include_directories(test_relais_dbprovider_log PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/include
            ${CMAKE_CURRENT_SOURCE_DIR}/../pqcoro/tests
        )
        target_link_libraries(test_relais_dbprovider_log PRIVATE
            $<IF:$<TARGET_EXISTS:pqcoro::pqcoro>,pqcoro::pqcoro,pqcoro>
            Catch2::Catch2WithMain
        )
        catch_discover_tests(test_relais_dbprovider_log
            PROPERTIES LABELS "integration;pqcoro"
        )
    endif()
endif()
