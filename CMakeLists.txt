cmake_minimum_required(VERSION 3.20)
project(jcailloux-relais VERSION 0.4.0 LANGUAGES CXX)

add_library(jcailloux-relais INTERFACE)
add_library(jcailloux::relais ALIAS jcailloux-relais)

target_include_directories(jcailloux-relais INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

target_compile_features(jcailloux-relais INTERFACE cxx_std_23)

# GDSF eviction: compile-time toggle (memory budget is runtime via RELAIS_L1_MAX_MEMORY env var)
option(RELAIS_GDSF_ENABLED "Enable GDSF size-aware L1 eviction" OFF)
if(RELAIS_GDSF_ENABLED)
    target_compile_definitions(jcailloux-relais INTERFACE RELAIS_GDSF_ENABLED=1)
endif()

# L1 cleanup frequency: sweep every 2^N insertions (0 = disabled, default 9 = every 512)
set(RELAIS_CLEANUP_FREQUENCY_LOG2 9 CACHE STRING "L1 cleanup frequency as log2 (0=disabled, 9=every 512 insertions)")
if(NOT RELAIS_CLEANUP_FREQUENCY_LOG2 EQUAL 9)
    target_compile_definitions(jcailloux-relais INTERFACE RELAIS_CLEANUP_FREQUENCY_LOG2=${RELAIS_CLEANUP_FREQUENCY_LOG2})
endif()

# CMake modules (available to consumers via FetchContent)
list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

# =============================================================================
# Dependencies
# =============================================================================
include(FetchContent)

FetchContent_Declare(
    glaze
    GIT_REPOSITORY https://github.com/stephenberry/glaze.git
    GIT_TAG        v7.0.2
    FIND_PACKAGE_ARGS 7.0
)
FetchContent_MakeAvailable(glaze)

# ParlayHash (vendored) — lock-free hash map + epoch-based reclamation
add_library(parlayhash INTERFACE)
target_include_directories(parlayhash INTERFACE
    "${CMAKE_CURRENT_SOURCE_DIR}/third_party/parlayhash/include")
target_compile_features(parlayhash INTERFACE cxx_std_17)

find_package(Threads REQUIRED)
find_package(PostgreSQL REQUIRED)

target_link_libraries(jcailloux-relais INTERFACE
    $<BUILD_INTERFACE:glaze::glaze>
    $<BUILD_INTERFACE:parlayhash>
    Threads::Threads
    PostgreSQL::PostgreSQL
)

# =============================================================================
# Install
# =============================================================================
include(CMakePackageConfigHelpers)
include(GNUInstallDirs)

install(TARGETS jcailloux-relais
    EXPORT jcailloux-relaisTargets
)

install(DIRECTORY include/
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

install(EXPORT jcailloux-relaisTargets
    NAMESPACE jcailloux::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/jcailloux-relais
)

configure_package_config_file(
    cmake/jcailloux-relaisConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/jcailloux-relaisConfig.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/jcailloux-relais
)

write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/jcailloux-relaisConfigVersion.cmake
    COMPATIBILITY SameMajorVersion
)

install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/jcailloux-relaisConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/jcailloux-relaisConfigVersion.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/jcailloux-relais
)

install(FILES
    cmake/RelaisGenerateWrappers.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/jcailloux-relais
)

install(FILES
    scripts/generate_entities.py
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/jcailloux-relais/scripts
)

# =============================================================================
# Tests
# =============================================================================

option(RELAIS_BUILD_TESTS "Build relais tests" ${PROJECT_IS_TOP_LEVEL})

if(RELAIS_BUILD_TESTS)
    enable_testing()

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
    # Entity wrapper generation
    # -------------------------------------------------------------------------
    include(RelaisGenerateWrappers)
    relais_generate_wrappers(
        SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures/
        OUTPUT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures/generated/
    )

    # -------------------------------------------------------------------------
    # Helper function: one-liner per test instead of 10 lines
    # -------------------------------------------------------------------------
    #   relais_add_test(<name> <source>
    #       [DEFINITIONS def1 def2 ...]
    #       [LABELS "label1;label2;..."]
    #       [RESOURCE_LOCK])
    # -------------------------------------------------------------------------
    function(relais_add_test name source)
        cmake_parse_arguments(ARG "RESOURCE_LOCK" "" "LABELS;DEFINITIONS" ${ARGN})

        add_executable(${name} ${source})
        target_include_directories(${name} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)

        if(ARG_DEFINITIONS)
            target_compile_definitions(${name} PRIVATE ${ARG_DEFINITIONS})
        endif()

        target_link_libraries(${name} PRIVATE jcailloux::relais Catch2::Catch2WithMain)

        if(ARG_LABELS AND ARG_RESOURCE_LOCK)
            catch_discover_tests(${name}
                PROPERTIES LABELS "${ARG_LABELS}"
                           RESOURCE_LOCK "db_redis")
        elseif(ARG_LABELS)
            catch_discover_tests(${name}
                PROPERTIES LABELS "${ARG_LABELS}")
        else()
            catch_discover_tests(${name})
        endif()
    endfunction()

    # -------------------------------------------------------------------------
    # Unit tests
    # -------------------------------------------------------------------------
    relais_add_test(relais_tests                tests/relais/test_generated_wrapper.cpp          DEFINITIONS RELAIS_BUILDING_TESTS)
    relais_add_test(test_relais_base_compile    tests/relais/test_base_repository_compile.cpp    DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;compile")
    relais_add_test(test_relais_repo_compile    tests/relais/test_repository_compile.cpp         DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;compile")
    relais_add_test(test_io_task          tests/io/test_io_task.cpp               LABELS "io")
    relais_add_test(test_io_context       tests/io/test_io_context.cpp            LABELS "io")
    relais_add_test(test_io_pg_types      tests/io/test_io_pg_types.cpp           LABELS "io")
    relais_add_test(test_io_redis_types   tests/io/test_io_redis_types.cpp        LABELS "io")
    relais_add_test(test_io_pg_pool_compile tests/io/test_io_pg_pool_compile.cpp  LABELS "io")
    relais_add_test(test_io_resp2         tests/io/test_io_resp2.cpp              LABELS "io")
    relais_add_test(test_epoll_io_context tests/io/test_epoll_io_context.cpp    LABELS "io;epoll")
    relais_add_test(test_batch_scheduler  tests/io/test_batch_scheduler.cpp     LABELS "io;batch")

    # -------------------------------------------------------------------------
    # Integration tests (require PostgreSQL + Redis)
    # -------------------------------------------------------------------------
    relais_add_test(test_io_pg_integration    tests/io/test_io_pg_integration.cpp    LABELS "io;integration" RESOURCE_LOCK)
    relais_add_test(test_io_redis_integration tests/io/test_io_redis_integration.cpp LABELS "io;integration" RESOURCE_LOCK)
    relais_add_test(test_io_pg_pipeline       tests/io/test_pg_pipeline.cpp          LABELS "io;integration;pipeline" RESOURCE_LOCK)
    relais_add_test(test_io_redis_pool        tests/io/test_redis_pool.cpp           LABELS "io;integration;redis-pool" RESOURCE_LOCK)
    relais_add_test(test_batch_scheduler_integration tests/io/test_batch_scheduler_integration.cpp LABELS "io;integration;batch" RESOURCE_LOCK)

    relais_add_test(test_relais_base            tests/relais/test_base_repository.cpp      DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db"                 RESOURCE_LOCK)
    relais_add_test(test_relais_redis           tests/relais/test_redis_repository.cpp     DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;redis"            RESOURCE_LOCK)
    relais_add_test(test_relais_decl_list_cache tests/relais/test_decl_list_cache.cpp      DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;list"             RESOURCE_LOCK)
    relais_add_test(test_relais_cached          tests/relais/test_cached_repository.cpp    DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;cache"            RESOURCE_LOCK)
    relais_add_test(test_relais_partition_key     tests/relais/test_partition_key.cpp          DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;partition-key"      RESOURCE_LOCK)
    relais_add_test(test_relais_full_cache      tests/relais/test_full_cache.cpp           DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;full-cache"       RESOURCE_LOCK)
    relais_add_test(test_relais_decl_list_redis tests/relais/test_decl_list_redis.cpp      DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;redis;list"       RESOURCE_LOCK)
    relais_add_test(test_relais_decl_list_full  tests/relais/test_decl_list_full.cpp       DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;full-cache;list"  RESOURCE_LOCK)
    relais_add_test(test_relais_warmup          tests/relais/test_warmup.cpp               DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;warmup"           RESOURCE_LOCK)
    relais_add_test(test_relais_l1_config       tests/relais/config/test_l1_config.cpp     DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;config"           RESOURCE_LOCK)
    relais_add_test(test_relais_l2_config       tests/relais/config/test_l2_config.cpp     DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;redis;config"     RESOURCE_LOCK)
    relais_add_test(test_relais_concurrency     tests/relais/test_concurrency.cpp          DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;concurrency"      RESOURCE_LOCK)
    relais_add_test(test_relais_dbprovider_log  tests/relais/test_dbprovider_log.cpp       DEFINITIONS RELAIS_BUILDING_TESTS LABELS "integration;relais"                     RESOURCE_LOCK)
    relais_add_test(test_relais_composite_key  tests/relais/test_composite_key.cpp        DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;composite-key"    RESOURCE_LOCK)
    relais_add_test(test_relais_gdsf          tests/relais/test_gdsf.cpp                DEFINITIONS RELAIS_BUILDING_TESTS "RELAIS_GDSF_ENABLED=1" LABELS "relais;integration;db;gdsf"             RESOURCE_LOCK)
    if(NOT RELAIS_GDSF_ENABLED)
        relais_add_test(test_relais_gdsf_disabled tests/relais/test_gdsf_disabled.cpp       DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;gdsf")
    endif()

    # Thread Sanitizer support for concurrency tests
    option(RELAIS_ENABLE_TSAN "Enable Thread Sanitizer for concurrency tests" OFF)
    if(RELAIS_ENABLE_TSAN)
        target_compile_options(test_relais_concurrency PRIVATE
            -fsanitize=thread -fno-omit-frame-pointer)
        target_link_options(test_relais_concurrency PRIVATE -fsanitize=thread)
    endif()

    # -------------------------------------------------------------------------
    # Combined targets
    # -------------------------------------------------------------------------
    add_custom_target(test_relais_integration
        DEPENDS
            relais_tests
            test_relais_base
            test_relais_redis
            test_relais_cached
            test_relais_decl_list_cache
            test_relais_partition_key
            test_relais_full_cache
            test_relais_decl_list_redis
            test_relais_decl_list_full
            test_relais_warmup
            test_relais_l1_config
            test_relais_l2_config
            test_relais_concurrency
            test_relais_composite_key
            test_relais_gdsf
            test_relais_gdsf_disabled
        COMMENT "Build all relais integration tests"
    )

    # Single binary combining all relais tests (excluding GDSF tests which
    # require RELAIS_GDSF_ENABLED and would change behavior of all repos).
    # GDSF tests are run via standalone binaries: test_relais_gdsf, test_relais_gdsf_disabled.
    add_executable(test_relais_all
        tests/relais/test_all_runner.cpp
        tests/relais/test_generated_wrapper.cpp
        tests/relais/test_base_repository.cpp
        tests/relais/test_redis_repository.cpp
        tests/relais/test_cached_repository.cpp
        tests/relais/test_decl_list_cache.cpp
        tests/relais/test_partition_key.cpp
        tests/relais/test_full_cache.cpp
        tests/relais/test_decl_list_redis.cpp
        tests/relais/test_decl_list_full.cpp
        tests/relais/test_warmup.cpp
        tests/relais/config/test_l1_config.cpp
        tests/relais/config/test_l2_config.cpp
        tests/relais/test_concurrency.cpp
        tests/relais/test_composite_key.cpp
    )
    target_include_directories(test_relais_all PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
    target_compile_definitions(test_relais_all PRIVATE RELAIS_BUILDING_TESTS RELAIS_COMBINED_TESTS)
    target_link_libraries(test_relais_all PRIVATE jcailloux::relais Catch2::Catch2WithMain)

endif()

# =============================================================================
# Benchmarks
# =============================================================================

option(RELAIS_BUILD_BENCHMARKS "Build relais benchmarks" OFF)

if(RELAIS_BUILD_BENCHMARKS)
    if(NOT TARGET Catch2::Catch2WithMain)
        FetchContent_Declare(Catch2
            GIT_REPOSITORY https://github.com/catchorg/Catch2.git
            GIT_TAG        v3.12.0
        )
        FetchContent_MakeAvailable(Catch2)
    endif()

    add_executable(bench_relais_cache benchmarks/bench_relais_cache.cpp)
    target_include_directories(bench_relais_cache PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks
        ${CMAKE_CURRENT_SOURCE_DIR}/tests)
    target_compile_definitions(bench_relais_cache PRIVATE RELAIS_BUILDING_TESTS)
    target_link_libraries(bench_relais_cache PRIVATE jcailloux::relais Catch2::Catch2WithMain)

    add_executable(bench_io_redis benchmarks/bench_io_redis.cpp)
    target_include_directories(bench_io_redis PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks
        ${CMAKE_CURRENT_SOURCE_DIR}/tests)
    target_link_libraries(bench_io_redis PRIVATE jcailloux::relais Catch2::Catch2WithMain)

    add_executable(bench_io_pg benchmarks/bench_io_pg.cpp)
    target_include_directories(bench_io_pg PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks
        ${CMAKE_CURRENT_SOURCE_DIR}/tests)
    target_link_libraries(bench_io_pg PRIVATE jcailloux::relais Catch2::Catch2WithMain)

    add_executable(bench_io_batch benchmarks/bench_io_batch.cpp)
    target_include_directories(bench_io_batch PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks
        ${CMAKE_CURRENT_SOURCE_DIR}/tests)
    target_compile_definitions(bench_io_batch PRIVATE RELAIS_BUILDING_TESTS)
    target_link_libraries(bench_io_batch PRIVATE jcailloux::relais Catch2::Catch2WithMain)

    add_executable(bench_gdsf benchmarks/bench_gdsf.cpp)
    target_include_directories(bench_gdsf PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks
        ${CMAKE_CURRENT_SOURCE_DIR}/tests)
    target_compile_definitions(bench_gdsf PRIVATE RELAIS_BUILDING_TESTS RELAIS_GDSF_ENABLED=1)
    target_link_libraries(bench_gdsf PRIVATE jcailloux::relais Catch2::Catch2WithMain)
endif()
