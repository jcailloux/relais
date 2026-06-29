cmake_minimum_required(VERSION 3.20)
project(jcailloux-relais VERSION 2.0.0 LANGUAGES CXX)

add_library(jcailloux-relais INTERFACE)
add_library(jcailloux::relais ALIAS jcailloux-relais)
# Export under the alias name so find_package() consumers and add_subdirectory()
# consumers both see jcailloux::relais (not jcailloux::jcailloux-relais).
set_target_properties(jcailloux-relais PROPERTIES EXPORT_NAME relais)

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

# Cache hit/miss metrics: zero-cost when disabled (OFF by default, always ON for benchmarks)
option(RELAIS_ENABLE_METRICS "Enable per-repo cache hit/miss metrics" OFF)
if(RELAIS_ENABLE_METRICS)
    target_compile_definitions(jcailloux-relais INTERFACE RELAIS_ENABLE_METRICS=1)
endif()

# L1 cleanup frequency: sweep every 2^N insertions (0 = disabled, default 9 = every 512)
set(RELAIS_CLEANUP_FREQUENCY_LOG2 9 CACHE STRING "L1 cleanup frequency as log2 (0=disabled, 9=every 512 insertions)")
if(NOT RELAIS_CLEANUP_FREQUENCY_LOG2 EQUAL 9)
    target_compile_definitions(jcailloux-relais INTERFACE RELAIS_CLEANUP_FREQUENCY_LOG2=${RELAIS_CLEANUP_FREQUENCY_LOG2})
endif()

# Profile-Guided Optimization: propagated via INTERFACE to all consumers
# Usage (same build directory for both phases):
#   1. cmake -B build -DRELAIS_ENABLE_PGO=generate -DCMAKE_BUILD_TYPE=Release
#      cmake --build build -j$(nproc) && ./build/my_app   # training workload
#   2. cmake -B build -DRELAIS_ENABLE_PGO=use
#      cmake --build build -j$(nproc)                     # optimized binary
set(RELAIS_ENABLE_PGO "OFF" CACHE STRING "Profile-Guided Optimization (OFF, generate, use)")
set_property(CACHE RELAIS_ENABLE_PGO PROPERTY STRINGS OFF generate use)

if(NOT RELAIS_ENABLE_PGO STREQUAL "OFF")
    set(RELAIS_PGO_DIR "${CMAKE_BINARY_DIR}/pgo-data")
    if(RELAIS_ENABLE_PGO STREQUAL "generate")
        target_compile_options(jcailloux-relais INTERFACE
            -fprofile-generate=${RELAIS_PGO_DIR} -fprofile-update=atomic)
        target_link_options(jcailloux-relais INTERFACE
            -fprofile-generate=${RELAIS_PGO_DIR})
    elseif(RELAIS_ENABLE_PGO STREQUAL "use")
        target_compile_options(jcailloux-relais INTERFACE
            -fprofile-use=${RELAIS_PGO_DIR}
            -fprofile-correction
            -Wno-missing-profile)
        target_link_options(jcailloux-relais INTERFACE
            -fprofile-use=${RELAIS_PGO_DIR})
    endif()
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
    GIT_TAG        v7.8.1
    FIND_PACKAGE_ARGS 7.8
)
FetchContent_MakeAvailable(glaze)

include(GNUInstallDirs)   # defines CMAKE_INSTALL_INCLUDEDIR, used just below

# ParlayHash (vendored) — lock-free hash map + epoch-based reclamation.
# Exported as part of our install set: the headers ship into the package's
# include dir and the INTERFACE target carries an INSTALL_INTERFACE include path,
# so a find_package() consumer resolves <parlay_hash/...> with nothing to re-add.
add_library(parlayhash INTERFACE)
target_include_directories(parlayhash INTERFACE
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/third_party/parlayhash/include>"
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>")
target_compile_features(parlayhash INTERFACE cxx_std_17)

find_package(Threads REQUIRED)
find_package(PostgreSQL REQUIRED)

target_link_libraries(jcailloux-relais INTERFACE
    $<BUILD_INTERFACE:glaze::glaze>
    parlayhash
    Threads::Threads
    PostgreSQL::PostgreSQL
)

# =============================================================================
# Install
# =============================================================================
include(CMakePackageConfigHelpers)

# Export both the public target and the vendored parlayhash INTERFACE target so
# the installed jcailloux-relaisTargets.cmake is self-contained.
install(TARGETS jcailloux-relais parlayhash
    EXPORT jcailloux-relaisTargets
)

install(DIRECTORY include/
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

# Vendored parlay_hash headers — ship alongside relais headers so the exported
# parlayhash INSTALL_INTERFACE include path resolves in a find_package() consumer.
install(DIRECTORY third_party/parlayhash/include/
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
    relais_add_test(relais_tests                tests/relais/test_generated_entity.cpp          DEFINITIONS RELAIS_BUILDING_TESTS)
    relais_add_test(test_relais_base_compile    tests/relais/test_pg_repo_compile.cpp    DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;compile")
    relais_add_test(test_relais_repo_compile    tests/relais/test_repo_compile.cpp         DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;compile")
    relais_add_test(test_io_task          tests/io/test_io_task.cpp               LABELS "io")
    relais_add_test(test_io_context       tests/io/test_io_context.cpp            LABELS "io")
    relais_add_test(test_io_pg_types      tests/io/test_io_pg_types.cpp           LABELS "io")
    relais_add_test(test_io_redis_types   tests/io/test_io_redis_types.cpp        LABELS "io")
    relais_add_test(test_io_pg_pool_compile tests/io/test_io_pg_pool_compile.cpp  LABELS "io")
    relais_add_test(test_io_resp2         tests/io/test_io_resp2.cpp              LABELS "io")
    relais_add_test(test_epoll_io_context tests/io/test_epoll_io_context.cpp    LABELS "io;epoll")
    relais_add_test(test_batch_scheduler  tests/io/test_batch_scheduler.cpp     LABELS "io;batch")
    relais_add_test(test_io_spawn         tests/io/test_spawn.cpp               LABELS "io;spawn")
    relais_add_test(test_io_when_all      tests/io/test_when_all.cpp            LABELS "io;whenall")
    relais_add_test(test_io_context_conformance tests/io/test_io_context_conformance.cpp LABELS "io;conformance")
    relais_add_test(test_controllable_io_context tests/io/test_controllable_io_context.cpp LABELS "io;conformance;fixture")

    # -------------------------------------------------------------------------
    # Integration tests (require PostgreSQL + Redis)
    # -------------------------------------------------------------------------
    relais_add_test(test_io_pg_integration    tests/io/test_io_pg_integration.cpp    LABELS "io;integration" RESOURCE_LOCK)
    relais_add_test(test_pg_probe             tests/io/test_pg_probe.cpp             LABELS "io;integration;fixture" RESOURCE_LOCK)
    relais_add_test(test_io_redis_integration tests/io/test_io_redis_integration.cpp LABELS "io;integration" RESOURCE_LOCK)
    relais_add_test(test_io_pg_pipeline       tests/io/test_pg_pipeline.cpp          LABELS "io;integration;pipeline" RESOURCE_LOCK)
    relais_add_test(test_io_redis_pool        tests/io/test_redis_pool.cpp           LABELS "io;integration;redis-pool" RESOURCE_LOCK)
    relais_add_test(test_batch_scheduler_integration tests/io/test_batch_scheduler_integration.cpp LABELS "io;integration;batch" RESOURCE_LOCK)
    relais_add_test(test_io_pool_integration  tests/io/test_io_pool_integration.cpp   LABELS "io;integration;iopool" RESOURCE_LOCK)
    relais_add_test(test_io_liveness          tests/io/test_io_liveness.cpp           LABELS "io;integration;liveness" RESOURCE_LOCK)

    relais_add_test(test_relais_base            tests/relais/test_pg_repo.cpp      DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db"                 RESOURCE_LOCK)
    relais_add_test(test_relais_redis           tests/relais/test_redis_repo.cpp     DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;redis"            RESOURCE_LOCK)
    relais_add_test(test_relais_decl_list_cache tests/relais/test_decl_list_cache.cpp      DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;list"             RESOURCE_LOCK)
    relais_add_test(test_relais_cached          tests/relais/test_local_repo.cpp    DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;cache"            RESOURCE_LOCK)
    relais_add_test(test_relais_partition_key     tests/relais/test_partition_key.cpp          DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;partition-key"      RESOURCE_LOCK)
    relais_add_test(test_relais_full_cache      tests/relais/test_full_cache.cpp           DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;full-cache"       RESOURCE_LOCK)
    relais_add_test(test_relais_decl_list_redis tests/relais/test_decl_list_redis.cpp      DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;redis;list"       RESOURCE_LOCK)
    relais_add_test(test_relais_decl_list_full  tests/relais/test_decl_list_full.cpp       DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;full-cache;list"  RESOURCE_LOCK)
    relais_add_test(test_relais_decl_list_composite tests/relais/test_decl_list_composite.cpp DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;list;composite"   RESOURCE_LOCK)
    relais_add_test(test_relais_decl_list_in    tests/relais/test_decl_list_in.cpp         DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;redis;list;in"    RESOURCE_LOCK)
    relais_add_test(test_relais_decl_list_not_in tests/relais/test_decl_list_not_in.cpp    DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;redis;list;nin" RESOURCE_LOCK)
    relais_add_test(test_relais_gen_sql         tests/relais/test_generated_sql.cpp        DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;sql;generator")
    relais_add_test(test_relais_gen_list_in     tests/relais/test_gen_list_in.cpp          DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;list;in;generator")
    relais_add_test(test_relais_gen_list_nin    tests/relais/test_gen_list_nin.cpp         DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;list;nin;generator")
    relais_add_test(test_relais_gen_list_limits tests/relais/test_gen_list_limits.cpp      DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;list;limits;generator")
    relais_add_test(test_relais_list_limits     tests/relais/test_list_limits.cpp          DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;list;limits")
    relais_add_test(test_relais_list_parse_bool tests/relais/test_list_parse_bool.cpp      DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;list;parse;bool")
    relais_add_test(test_relais_array_mapping   tests/relais/test_array_mapping.cpp        DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;array"           RESOURCE_LOCK)
    relais_add_test(test_relais_warmup          tests/relais/test_warmup.cpp               DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;warmup"           RESOURCE_LOCK)
    relais_add_test(test_relais_l1_config       tests/relais/config/test_l1_config.cpp     DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;config"           RESOURCE_LOCK)
    relais_add_test(test_relais_l2_config       tests/relais/config/test_l2_config.cpp     DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;redis;config"     RESOURCE_LOCK)
    relais_add_test(test_relais_concurrency     tests/relais/test_concurrency.cpp          DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;concurrency"      RESOURCE_LOCK)
    relais_add_test(test_relais_dbprovider_log  tests/relais/test_pg_provider_log.cpp       DEFINITIONS RELAIS_BUILDING_TESTS LABELS "integration;relais"                     RESOURCE_LOCK)
    relais_add_test(test_relais_provider_preinit tests/relais/test_pg_provider_preinit.cpp  DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;preinit")
    relais_add_test(test_relais_composite_key  tests/relais/test_composite_key.cpp        DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;composite-key"    RESOURCE_LOCK)
    relais_add_test(test_relais_multiview      tests/relais/test_multiview.cpp            LABELS "relais;multiview")
    relais_add_test(test_relais_recheck_guard  tests/relais/test_recheck_guard.cpp        LABELS "relais;recheck")
    relais_add_test(test_relais_l2_gen         tests/relais/test_l2_gen.cpp               DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;redis;l2gen"       RESOURCE_LOCK)
    relais_add_test(test_relais_find_many      tests/relais/test_find_many.cpp            DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;redis;findmany"    RESOURCE_LOCK)
    relais_add_test(test_relais_erase_batch    tests/relais/test_erase_batch.cpp          DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;erasemany"        RESOURCE_LOCK)
    relais_add_test(test_relais_invalidate_many tests/relais/test_invalidate_many.cpp     DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;redis;invalidatemany"  RESOURCE_LOCK)
    relais_add_test(test_relais_erase_invalidate_batch tests/relais/test_erase_invalidate_batch.cpp DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;redis;list;batch" RESOURCE_LOCK)
    relais_add_test(test_relais_where_predicate tests/relais/test_where_predicate.cpp     DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;redis;list;where"   RESOURCE_LOCK)
    relais_add_test(test_relais_cache_coherence tests/relais/test_cache_coherence.cpp     DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;redis;coherence"    RESOURCE_LOCK)
    relais_add_test(test_relais_assigned_key   tests/relais/test_simple_assigned_key.cpp  DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;assigned-key"     RESOURCE_LOCK)
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
        # whenAll latch + symmetric transfer: mono-thread, but TSan validates the
        # resume chain and frame lifetimes carry no bogus happens-before / UAF.
        target_compile_options(test_io_when_all PRIVATE
            -fsanitize=thread -fno-omit-frame-pointer)
        target_link_options(test_io_when_all PRIVATE -fsanitize=thread)
        # Liveness suite: timeouts + teardown across the loop thread and the
        # detached fires — TSan validates no data race / UAF in the resume chains.
        target_compile_options(test_io_liveness PRIVATE
            -fsanitize=thread -fno-omit-frame-pointer)
        target_link_options(test_io_liveness PRIVATE -fsanitize=thread)
        # NB: the cache-coherence suite is single-loop (one event-loop thread), so
        # its concurrency-relevant surface — the shared static L1 under concurrent
        # find/update/erase, and the coroutine resume chains — is TSan-validated by
        # test_relais_concurrency and test_io_liveness, not here. It is gated under
        # ASan/LSan instead (the relevant sanitizer for its uncertain-unwind
        # eviction, detached-frame teardown, and self-heal revive).
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
            test_relais_decl_list_composite
            test_relais_warmup
            test_relais_l1_config
            test_relais_l2_config
            test_relais_concurrency
            test_relais_composite_key
            test_relais_assigned_key
            test_relais_gdsf
            test_relais_gdsf_disabled
        COMMENT "Build all relais integration tests"
    )

    # Single binary combining all relais tests (excluding GDSF tests which
    # require RELAIS_GDSF_ENABLED and would change behavior of all repos).
    # GDSF tests are run via standalone binaries: test_relais_gdsf, test_relais_gdsf_disabled.
    add_executable(test_relais_all
        tests/relais/test_all_runner.cpp
        tests/relais/test_generated_entity.cpp
        tests/relais/test_pg_repo.cpp
        tests/relais/test_redis_repo.cpp
        tests/relais/test_local_repo.cpp
        tests/relais/test_decl_list_cache.cpp
        tests/relais/test_partition_key.cpp
        tests/relais/test_full_cache.cpp
        tests/relais/test_decl_list_redis.cpp
        tests/relais/test_decl_list_full.cpp
        tests/relais/test_decl_list_composite.cpp
        tests/relais/test_array_mapping.cpp
        tests/relais/test_warmup.cpp
        tests/relais/config/test_l1_config.cpp
        tests/relais/config/test_l2_config.cpp
        tests/relais/test_concurrency.cpp
        tests/relais/test_composite_key.cpp
        tests/relais/test_simple_assigned_key.cpp
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
    target_compile_definitions(bench_relais_cache PRIVATE RELAIS_BUILDING_TESTS RELAIS_ENABLE_METRICS=1)
    target_link_libraries(bench_relais_cache PRIVATE jcailloux::relais Catch2::Catch2WithMain)

    add_executable(bench_io_redis benchmarks/bench_io_redis.cpp)
    target_include_directories(bench_io_redis PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks
        ${CMAKE_CURRENT_SOURCE_DIR}/tests)
    target_compile_definitions(bench_io_redis PRIVATE RELAIS_ENABLE_METRICS=1)
    target_link_libraries(bench_io_redis PRIVATE jcailloux::relais Catch2::Catch2WithMain)

    add_executable(bench_io_pg benchmarks/bench_io_pg.cpp)
    target_include_directories(bench_io_pg PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks
        ${CMAKE_CURRENT_SOURCE_DIR}/tests)
    target_compile_definitions(bench_io_pg PRIVATE RELAIS_ENABLE_METRICS=1)
    target_link_libraries(bench_io_pg PRIVATE jcailloux::relais Catch2::Catch2WithMain)

    add_executable(bench_io_batch benchmarks/bench_io_batch.cpp)
    target_include_directories(bench_io_batch PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks
        ${CMAKE_CURRENT_SOURCE_DIR}/tests)
    target_compile_definitions(bench_io_batch PRIVATE RELAIS_BUILDING_TESTS RELAIS_ENABLE_METRICS=1)
    target_link_libraries(bench_io_batch PRIVATE jcailloux::relais Catch2::Catch2WithMain)

    add_executable(bench_gdsf benchmarks/bench_gdsf.cpp)
    target_include_directories(bench_gdsf PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks
        ${CMAKE_CURRENT_SOURCE_DIR}/tests)
    target_compile_definitions(bench_gdsf PRIVATE RELAIS_BUILDING_TESTS RELAIS_GDSF_ENABLED=1 RELAIS_ENABLE_METRICS=1)
    target_link_libraries(bench_gdsf PRIVATE jcailloux::relais Catch2::Catch2WithMain)
endif()

# =============================================================================
# Examples
# =============================================================================

option(RELAIS_BUILD_EXAMPLES "Build relais examples" OFF)

if(RELAIS_BUILD_EXAMPLES)
    # Self-contained — no database required.
    add_executable(example_event_loop_basics examples/event_loop_basics.cpp)
    target_link_libraries(example_event_loop_basics PRIVATE jcailloux::relais)

    # Requires PostgreSQL + Redis reachable at runtime.
    add_executable(example_iopool_nloop examples/iopool_nloop.cpp)
    target_link_libraries(example_iopool_nloop PRIVATE jcailloux::relais)
endif()
