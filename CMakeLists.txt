cmake_minimum_required(VERSION 3.20)
project(jcailloux-drogon-smartrepo LANGUAGES CXX)

add_library(jcailloux-drogon-smartrepo INTERFACE)
add_library(jcailloux::drogon-smartrepo ALIAS jcailloux-drogon-smartrepo)

target_include_directories(jcailloux-drogon-smartrepo INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

target_compile_features(jcailloux-drogon-smartrepo INTERFACE cxx_std_23)

# Workaround for GCC 15 bug with AVX2 intrinsics in avx2intrin.h
# Remove this when GCC fixes the issue (likely GCC 15.1 or 15.2)
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "15")
    target_compile_options(jcailloux-drogon-smartrepo INTERFACE -mno-avx2)
endif()

# =============================================================================
# Dependencies
# =============================================================================

include(FetchContent)

# shardmap (required)
# Try to find it, or fetch it if standalone
if(NOT TARGET jcailloux::shardmap AND NOT TARGET shardmap)
    # Check if shardmap is available locally (sibling directory)
    if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/../shardmap/CMakeLists.txt")
        add_subdirectory(
            ${CMAKE_CURRENT_SOURCE_DIR}/../shardmap
            ${CMAKE_CURRENT_BINARY_DIR}/shardmap
        )
    else()
        message(FATAL_ERROR "jcailloux-drogon-smartrepo: shardmap is required. Add it before this library or place it in ../shardmap/")
    endif()
endif()

# Drogon - web framework (system dependency, not fetched)
find_package(Drogon REQUIRED)

# glaze - JSON serialization (header-only)
# FIND_PACKAGE_ARGS: prefer system/parent-provided version if available
# Disable AVX2 to avoid GCC 15 bug with avx2intrin.h
#set(glaze_ENABLE_AVX2 OFF CACHE BOOL "" FORCE)
FetchContent_Declare(glaze
    GIT_REPOSITORY https://github.com/stephenberry/glaze.git
    GIT_TAG        v4.2.3
    FIND_PACKAGE_ARGS
)
FetchContent_MakeAvailable(glaze)

# Link all dependencies
target_link_libraries(jcailloux-drogon-smartrepo INTERFACE
    $<IF:$<TARGET_EXISTS:jcailloux::shardmap>,jcailloux::shardmap,shardmap>
    Drogon::Drogon
    glaze::glaze
)

# =============================================================================
# Tests
# =============================================================================

option(SMARTREPO_BUILD_TESTS "Build smartrepo tests" ON)

if(SMARTREPO_BUILD_TESTS)
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
    #   - PostgreSQL database with smartrepo_test_items table
    #   - Redis server
    #
    # Environment variables:
    #   DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS
    #   REDIS_HOST (optional, default: 127.0.0.1)
    #   REDIS_PORT (optional, default: 6379)
    #
    # To set up the test database:
    #   psql -U postgres -c "CREATE DATABASE smartrepo_test"
    #   psql -U postgres -d smartrepo_test -f tests/migrations/000001_create_test_items.sql
    # -------------------------------------------------------------------------

    # Test fixtures include directory
    set(SMARTREPO_TEST_FIXTURES_DIR ${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures)

    add_executable(smartrepo_tests
        tests/test_generated_wrapper.cpp
        tests/fixtures/mocks/Mock_SmartrepoTestOrders.cc
    )

    target_compile_definitions(smartrepo_tests
            PRIVATE
                SMARTREPO_BUILDING_TESTS
    )

    target_include_directories(smartrepo_tests PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/tests
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures/mocks
    )

    target_link_libraries(smartrepo_tests
            PRIVATE
            jcailloux::drogon-smartrepo
            Catch2::Catch2WithMain
    )

    # Discover tests for CTest
    list(APPEND CMAKE_MODULE_PATH ${catch2_SOURCE_DIR}/extras)
    include(CTest)
    include(Catch)
    catch_discover_tests(smartrepo_tests)

    # Test: BaseRepository (no caching, direct DB access)
    add_executable(test_smartrepo_base
        tests/test_base_repository.cpp
    )
    target_include_directories(test_smartrepo_base PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
    )
    target_compile_definitions(test_smartrepo_base PRIVATE SMARTREPO_BUILDING_TESTS)
    target_link_libraries(test_smartrepo_base PRIVATE
        jcailloux::drogon-smartrepo
        Catch2::Catch2WithMain
    )
    catch_discover_tests(test_smartrepo_base
        PROPERTIES LABELS "integration;db"
    )

    # Test: RedisRepository (L2 Redis caching)
    add_executable(test_smartrepo_redis
        tests/test_redis_repository.cpp
    )
    target_include_directories(test_smartrepo_redis PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
    )
    target_compile_definitions(test_smartrepo_redis PRIVATE SMARTREPO_BUILDING_TESTS)
    target_link_libraries(test_smartrepo_redis PRIVATE
        jcailloux::drogon-smartrepo
        Catch2::Catch2WithMain
    )
    catch_discover_tests(test_smartrepo_redis
        PROPERTIES LABELS "integration;db;redis"
    )

    # Test: ListMixin (L1 list cache invalidation)
    add_executable(test_smartrepo_decl_list_cache
        tests/test_decl_list_cache.cpp
    )
    target_include_directories(test_smartrepo_decl_list_cache PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
    )
    target_compile_definitions(test_smartrepo_decl_list_cache PRIVATE SMARTREPO_BUILDING_TESTS)
    target_link_libraries(test_smartrepo_decl_list_cache PRIVATE
        jcailloux::drogon-smartrepo
        Catch2::Catch2WithMain
    )
    catch_discover_tests(test_smartrepo_decl_list_cache
        PROPERTIES LABELS "integration;db;list"
    )

    # Test: CachedRepository (L1 caching)
    add_executable(test_smartrepo_cached
        tests/test_cached_repository.cpp
    )
    target_include_directories(test_smartrepo_cached PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
    )
    target_compile_definitions(test_smartrepo_cached PRIVATE SMARTREPO_BUILDING_TESTS)
    target_link_libraries(test_smartrepo_cached PRIVATE
        jcailloux::drogon-smartrepo
        Catch2::Catch2WithMain
    )
    catch_discover_tests(test_smartrepo_cached
        PROPERTIES LABELS "integration;db;cache"
    )

    # Test: PartialKey (composite PK with partial key queries)
    add_executable(test_smartrepo_partial_key
        tests/test_partial_key.cpp
    )
    target_include_directories(test_smartrepo_partial_key PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
    )
    target_compile_definitions(test_smartrepo_partial_key PRIVATE SMARTREPO_BUILDING_TESTS)
    target_link_libraries(test_smartrepo_partial_key PRIVATE
        jcailloux::drogon-smartrepo
        Catch2::Catch2WithMain
    )
    catch_discover_tests(test_smartrepo_partial_key
        PROPERTIES LABELS "integration;db;partial-key"
    )

    # Test: FullCache (L1+L2 cache hierarchy)
    add_executable(test_smartrepo_full_cache
        tests/test_full_cache.cpp
    )
    target_include_directories(test_smartrepo_full_cache PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
    )
    target_compile_definitions(test_smartrepo_full_cache PRIVATE SMARTREPO_BUILDING_TESTS)
    target_link_libraries(test_smartrepo_full_cache PRIVATE
        jcailloux::drogon-smartrepo
        Catch2::Catch2WithMain
    )
    catch_discover_tests(test_smartrepo_full_cache
        PROPERTIES LABELS "integration;db;full-cache"
    )

    # Test: DeclList Redis (declarative list caching at L2)
    add_executable(test_smartrepo_decl_list_redis
        tests/test_decl_list_redis.cpp
    )
    target_include_directories(test_smartrepo_decl_list_redis PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
    )
    target_compile_definitions(test_smartrepo_decl_list_redis PRIVATE SMARTREPO_BUILDING_TESTS)
    target_link_libraries(test_smartrepo_decl_list_redis PRIVATE
        jcailloux::drogon-smartrepo
        Catch2::Catch2WithMain
    )
    catch_discover_tests(test_smartrepo_decl_list_redis
        PROPERTIES LABELS "integration;db;redis;list"
    )

    # Test: DeclList Full (declarative list caching at L1+L2)
    add_executable(test_smartrepo_decl_list_full
        tests/test_decl_list_full.cpp
    )
    target_include_directories(test_smartrepo_decl_list_full PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
    )
    target_compile_definitions(test_smartrepo_decl_list_full PRIVATE SMARTREPO_BUILDING_TESTS)
    target_link_libraries(test_smartrepo_decl_list_full PRIVATE
        jcailloux::drogon-smartrepo
        Catch2::Catch2WithMain
    )
    catch_discover_tests(test_smartrepo_decl_list_full
        PROPERTIES LABELS "integration;db;full-cache;list"
    )

    # Test: Warmup (cache infrastructure priming)
    add_executable(test_smartrepo_warmup
        tests/test_warmup.cpp
    )
    target_include_directories(test_smartrepo_warmup PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
    )
    target_compile_definitions(test_smartrepo_warmup PRIVATE SMARTREPO_BUILDING_TESTS)
    target_link_libraries(test_smartrepo_warmup PRIVATE
        jcailloux::drogon-smartrepo
        Catch2::Catch2WithMain
    )
    catch_discover_tests(test_smartrepo_warmup
        PROPERTIES LABELS "integration;db;warmup"
    )

    # Test: L1 Config (exhaustive L1 parameter testing)
    add_executable(test_smartrepo_l1_config
        tests/config/test_l1_config.cpp
    )
    target_include_directories(test_smartrepo_l1_config PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
    )
    target_compile_definitions(test_smartrepo_l1_config PRIVATE SMARTREPO_BUILDING_TESTS)
    target_link_libraries(test_smartrepo_l1_config PRIVATE
        jcailloux::drogon-smartrepo
        Catch2::Catch2WithMain
    )
    catch_discover_tests(test_smartrepo_l1_config
        PROPERTIES LABELS "integration;db;config"
    )

    # Test: L2 Config (exhaustive L2 parameter testing)
    add_executable(test_smartrepo_l2_config
        tests/config/test_l2_config.cpp
    )
    target_include_directories(test_smartrepo_l2_config PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
    )
    target_compile_definitions(test_smartrepo_l2_config PRIVATE SMARTREPO_BUILDING_TESTS)
    target_link_libraries(test_smartrepo_l2_config PRIVATE
        jcailloux::drogon-smartrepo
        Catch2::Catch2WithMain
    )
    catch_discover_tests(test_smartrepo_l2_config
        PROPERTIES LABELS "integration;db;redis;config"
    )

    # Test: Concurrency (stress testing with parallel operations)
    add_executable(test_smartrepo_concurrency
        tests/test_concurrency.cpp
    )
    target_include_directories(test_smartrepo_concurrency PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
    )
    target_compile_definitions(test_smartrepo_concurrency PRIVATE SMARTREPO_BUILDING_TESTS)
    target_link_libraries(test_smartrepo_concurrency PRIVATE
        jcailloux::drogon-smartrepo
        Catch2::Catch2WithMain
    )
    catch_discover_tests(test_smartrepo_concurrency
        PROPERTIES LABELS "integration;db;concurrency"
    )

    # Thread Sanitizer support for concurrency tests
    option(SMARTREPO_ENABLE_TSAN "Enable Thread Sanitizer for concurrency tests" OFF)
    if(SMARTREPO_ENABLE_TSAN)
        target_compile_options(test_smartrepo_concurrency PRIVATE
            -fsanitize=thread -fno-omit-frame-pointer)
        target_link_options(test_smartrepo_concurrency PRIVATE -fsanitize=thread)
        message(STATUS "Thread Sanitizer enabled for test_smartrepo_concurrency")
    endif()

    # Test: Benchmark (performance measurements, hidden by default)
    add_executable(test_smartrepo_benchmark
        tests/test_benchmark.cpp
    )
    target_include_directories(test_smartrepo_benchmark PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
    )
    target_compile_definitions(test_smartrepo_benchmark PRIVATE SMARTREPO_BUILDING_TESTS)
    target_link_libraries(test_smartrepo_benchmark PRIVATE
        jcailloux::drogon-smartrepo
        Catch2::Catch2WithMain
    )

    # -------------------------------------------------------------------------
    # Combined test target for running all integration tests
    # -------------------------------------------------------------------------
    add_custom_target(test_smartrepo_integration
        DEPENDS
            smartrepo_tests
            test_smartrepo_base
            test_smartrepo_redis
            test_smartrepo_cached
            test_smartrepo_decl_list_cache
            test_smartrepo_partial_key
            test_smartrepo_full_cache
            test_smartrepo_decl_list_redis
            test_smartrepo_decl_list_full
            test_smartrepo_warmup
            test_smartrepo_l1_config
            test_smartrepo_l2_config
            test_smartrepo_concurrency
        COMMENT "Build all smartrepo integration tests"
    )

    # Combined test executable: all tests in a single Catch2 binary
    add_executable(test_smartrepo_all
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
        tests/fixtures/mocks/Mock_SmartrepoTestOrders.cc
    )
    target_include_directories(test_smartrepo_all PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests
        ${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures/mocks
    )
    target_compile_definitions(test_smartrepo_all PRIVATE
        SMARTREPO_BUILDING_TESTS
        SMARTREPO_COMBINED_TESTS
    )
    target_link_libraries(test_smartrepo_all PRIVATE
        jcailloux::drogon-smartrepo
        Catch2::Catch2WithMain
    )
    catch_discover_tests(test_smartrepo_all)
endif()
