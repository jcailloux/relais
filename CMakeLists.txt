cmake_minimum_required(VERSION 3.20)
project(jcailloux-relais VERSION 4.0.0 LANGUAGES CXX)

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

        add_executable(${name} ${source}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures/LsanSuppressions.cpp)
        target_include_directories(${name} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)

        if(ARG_DEFINITIONS)
            target_compile_definitions(${name} PRIVATE ${ARG_DEFINITIONS})
        endif()

        target_link_libraries(${name} PRIVATE jcailloux::relais Catch2::Catch2WithMain)

        if(ARG_LABELS AND ARG_RESOURCE_LOCK)
            catch_discover_tests(${name}
                PROPERTIES LABELS "${ARG_LABELS}"
                           RESOURCE_LOCK "db")
        elseif(ARG_LABELS)
            catch_discover_tests(${name}
                PROPERTIES LABELS "${ARG_LABELS}")
        else()
            catch_discover_tests(${name})
        endif()
    endfunction()

    # -------------------------------------------------------------------------
    # Rejection tests: a declaration the library must refuse
    # -------------------------------------------------------------------------
    #   relais_add_compile_fail_test(<name> <source> <regex>)
    #     passes when compiling <source> prints <regex> (the static_assert text);
    #     the target is built only by the test, never by the default build.
    #   relais_add_generator_reject_test(<name> <sources_dir> <regex>)
    #     passes when the entity generator prints <regex> for <sources_dir>.
    # -------------------------------------------------------------------------
    function(relais_add_compile_fail_test name source regex)
        add_library(${name} OBJECT EXCLUDE_FROM_ALL ${source})
        target_link_libraries(${name} PRIVATE jcailloux::relais)
        add_test(NAME ${name}
                 COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target ${name})
        set_tests_properties(${name} PROPERTIES
            PASS_REGULAR_EXPRESSION "${regex}"
            LABELS "relais;compile-fail"
            RESOURCE_LOCK "build_tree")
    endfunction()

    function(relais_add_generator_reject_test name sources_dir regex)
        add_test(NAME ${name}
                 COMMAND ${Python3_EXECUTABLE} ${PROJECT_SOURCE_DIR}/scripts/generate_entities.py
                         --sources ${sources_dir}
                         --output-dir ${CMAKE_CURRENT_BINARY_DIR}/generator_reject/${name})
        set_tests_properties(${name} PROPERTIES
            PASS_REGULAR_EXPRESSION "${regex}"
            LABELS "relais;generator")
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
    relais_add_test(test_number_text      tests/io/test_number_text.cpp           LABELS "io")
    add_test(NAME precaution_sites
             COMMAND ${CMAKE_COMMAND} -DROOT=${PROJECT_SOURCE_DIR}
                     -P ${PROJECT_SOURCE_DIR}/cmake/CheckPrecautionSites.cmake)
    set_tests_properties(precaution_sites PROPERTIES LABELS "lint")
    relais_add_test(test_io_pg_pool_compile tests/io/test_io_pg_pool_compile.cpp  LABELS "io")
    relais_add_test(test_epoll_io_context tests/io/test_epoll_io_context.cpp    LABELS "io;epoll")
    relais_add_test(test_deadline_queue   tests/io/test_deadline_queue.cpp      LABELS "io;deadline")
    relais_add_test(test_batch_scheduler  tests/io/test_batch_scheduler.cpp     LABELS "io;batch")
    relais_add_test(test_write_affinity   tests/io/test_write_affinity.cpp      DEFINITIONS RELAIS_BUILDING_TESTS LABELS "io;batch;unit")
    relais_add_test(test_io_spawn         tests/io/test_spawn.cpp               LABELS "io;spawn")
    relais_add_test(test_io_when_all      tests/io/test_when_all.cpp            LABELS "io;whenall")
    relais_add_test(test_io_context_conformance tests/io/test_io_context_conformance.cpp LABELS "io;conformance")
    relais_add_test(test_controllable_io_context tests/io/test_controllable_io_context.cpp LABELS "io;conformance;fixture")

    # -------------------------------------------------------------------------
    # Integration tests (require PostgreSQL)
    # -------------------------------------------------------------------------
    relais_add_test(test_io_pg_integration    tests/io/test_io_pg_integration.cpp    LABELS "io;integration" RESOURCE_LOCK)
    relais_add_test(test_pg_probe             tests/io/test_pg_probe.cpp             LABELS "io;integration;fixture" RESOURCE_LOCK)
    relais_add_test(test_io_pg_pipeline       tests/io/test_pg_pipeline.cpp          LABELS "io;integration;pipeline" RESOURCE_LOCK)
    relais_add_test(test_batch_scheduler_integration tests/io/test_batch_scheduler_integration.cpp DEFINITIONS RELAIS_BUILDING_TESTS LABELS "io;integration;batch" RESOURCE_LOCK)
    relais_add_test(test_io_pool_integration  tests/io/test_io_pool_integration.cpp   LABELS "io;integration;iopool" RESOURCE_LOCK)
    relais_add_test(test_io_liveness          tests/io/test_io_liveness.cpp           DEFINITIONS RELAIS_BUILDING_TESTS LABELS "io;integration;liveness" RESOURCE_LOCK)
    relais_add_test(test_io_adversarial_loops tests/io/test_io_adversarial_loops.cpp  LABELS "io;integration;adversarial" RESOURCE_LOCK)
    relais_add_test(test_io_attach            tests/io/test_io_attach.cpp             LABELS "io;integration;attach" RESOURCE_LOCK)

    relais_add_test(test_relais_base            tests/relais/test_pg_repo.cpp      DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db"                 RESOURCE_LOCK)
    relais_add_test(test_relais_decl_list_cache tests/relais/test_decl_list_cache.cpp      DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;list"             RESOURCE_LOCK)
    relais_add_test(test_relais_cached          tests/relais/test_local_repo.cpp    DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;cache"            RESOURCE_LOCK)
    relais_add_test(test_relais_partition_key     tests/relais/test_partition_key.cpp          DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;partition-key"      RESOURCE_LOCK)
    relais_add_test(test_relais_decl_list_full  tests/relais/test_decl_list_full.cpp       DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;full-cache;list"  RESOURCE_LOCK)
    relais_add_test(test_relais_decl_list_composite tests/relais/test_decl_list_composite.cpp DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;list;composite"   RESOURCE_LOCK)
    relais_add_test(test_relais_decl_list_in    tests/relais/test_decl_list_in.cpp         DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;list;in"    RESOURCE_LOCK)
    relais_add_test(test_relais_decl_list_not_in tests/relais/test_decl_list_not_in.cpp    DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;list;nin" RESOURCE_LOCK)
    relais_add_test(test_relais_enum_list      tests/relais/test_enum_list.cpp            DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;list;enum" RESOURCE_LOCK)
    relais_add_test(test_relais_list_filter_widths tests/relais/test_list_filter_widths.cpp DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;list;widths" RESOURCE_LOCK)
    relais_add_test(test_relais_gen_sql         tests/relais/test_generated_sql.cpp        DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;sql;generator")
    relais_add_test(test_relais_gen_list_in     tests/relais/test_gen_list_in.cpp          DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;list;in;generator")
    relais_add_test(test_relais_gen_list_nin    tests/relais/test_gen_list_nin.cpp         DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;list;nin;generator")
    relais_add_test(test_relais_gen_list_is_null tests/relais/test_gen_list_is_null.cpp    DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;list;is_null;generator")
    relais_add_test(test_relais_decl_list_is_null tests/relais/test_decl_list_is_null.cpp  DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;list;is_null" RESOURCE_LOCK)
    relais_add_test(test_relais_list_null_match tests/relais/test_list_null_match.cpp DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;list;null" RESOURCE_LOCK)
    relais_add_compile_fail_test(compile_fail_is_null_non_optional
        tests/compile_fail/is_null_non_optional.cpp
        "IS_NULL filter requires a std::optional member")
    relais_add_compile_fail_test(compile_fail_heap_bytes_unsupported
        tests/compile_fail/heap_bytes_unsupported.cpp
        "heapBytes: no rule counts the heap this type owns")
    relais_add_compile_fail_test(compile_fail_heap_bytes_counted_twice
        tests/compile_fail/heap_bytes_counted_twice.cpp
        "heapBytes: this container's allocator counts its blocks already")
    relais_add_compile_fail_test(compile_fail_weighed_function_lambda
        tests/compile_fail/weighed_function_lambda.cpp
        "heapBytes: no rule counts the heap this type owns")
    relais_add_generator_reject_test(generator_rejects_is_null_non_optional
        ${PROJECT_SOURCE_DIR}/tests/generator/is_null_non_optional
        "is_null requires a std::optional member")
    relais_add_generator_reject_test(generator_rejects_duplicate_filter_param
        ${PROJECT_SOURCE_DIR}/tests/generator/is_null_duplicate_param
        "filter parameter 'published_at' is declared twice")
    # Unit tests of the entity generator itself (stdlib unittest, no dependency)
    add_test(NAME generator_unit_tests
             COMMAND ${Python3_EXECUTABLE} -m unittest discover -s ${PROJECT_SOURCE_DIR}/tests/scripts)
    set_tests_properties(generator_unit_tests PROPERTIES LABELS "relais;generator")
    relais_add_test(test_relais_gen_list_limits tests/relais/test_gen_list_limits.cpp      DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;list;limits;generator")
    relais_add_test(test_relais_list_limits     tests/relais/test_list_limits.cpp          DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;list;limits")
    relais_add_test(test_relais_list_parse_bool tests/relais/test_list_parse_bool.cpp      DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;list;parse;bool")
    relais_add_test(test_relais_list_cursor_width tests/relais/test_list_cursor_width.cpp DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;list;cursor")
    relais_add_test(test_relais_list_key_golden tests/relais/test_list_key_golden.cpp    DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;list;golden")
    relais_add_test(test_relais_array_mapping   tests/relais/test_array_mapping.cpp        DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;array"           RESOURCE_LOCK)
    relais_add_test(test_relais_warmup          tests/relais/test_warmup.cpp               DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;warmup"           RESOURCE_LOCK)
    relais_add_test(test_relais_l1_config       tests/relais/config/test_l1_config.cpp     DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;config"           RESOURCE_LOCK)
    relais_add_test(test_relais_concurrency     tests/relais/test_concurrency.cpp          DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;concurrency"      RESOURCE_LOCK)
    relais_add_test(test_relais_dbprovider_log  tests/relais/test_pg_provider_log.cpp       DEFINITIONS RELAIS_BUILDING_TESTS LABELS "integration;relais"                     RESOURCE_LOCK)
    relais_add_test(test_relais_provider_preinit tests/relais/test_pg_provider_preinit.cpp  DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;preinit")
    relais_add_test(test_relais_composite_key  tests/relais/test_composite_key.cpp        DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;composite-key"    RESOURCE_LOCK)
    relais_add_test(test_relais_multiview      tests/relais/test_multiview.cpp            LABELS "relais;multiview")
    relais_add_test(test_relais_recheck_guard  tests/relais/test_recheck_guard.cpp        LABELS "relais;recheck")
    relais_add_test(test_relais_chunk_map      tests/relais/test_chunk_map.cpp            DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;chunkmap")
    relais_add_test(test_relais_find_many      tests/relais/test_find_many.cpp            DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;findmany"    RESOURCE_LOCK)
    relais_add_test(test_relais_erase_batch    tests/relais/test_erase_batch.cpp          DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;erasemany"        RESOURCE_LOCK)
    relais_add_test(test_relais_erase_invalidate_batch tests/relais/test_erase_invalidate_batch.cpp DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;list;batch" RESOURCE_LOCK)
    relais_add_test(test_relais_where_predicate tests/relais/test_where_predicate.cpp     DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;list;where"   RESOURCE_LOCK)
    relais_add_test(test_relais_count_where   tests/relais/test_count_where.cpp        DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;where;count"   RESOURCE_LOCK)
    relais_add_test(test_relais_cache_coherence tests/relais/test_cache_coherence.cpp     DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;coherence"    RESOURCE_LOCK)
    relais_add_test(test_relais_assigned_key   tests/relais/test_simple_assigned_key.cpp  DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;assigned-key"     RESOURCE_LOCK)
    relais_add_test(test_relais_upsert         tests/relais/test_upsert.cpp               DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;upsert"       RESOURCE_LOCK)
    relais_add_test(test_relais_conditional_writes tests/relais/test_conditional_writes.cpp DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;conditional" RESOURCE_LOCK)
    relais_add_test(test_relais_conditional_contention tests/relais/test_conditional_writes_contention.cpp DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;integration;db;conditional;concurrency" RESOURCE_LOCK)
    relais_add_test(test_relais_gdsf          tests/relais/test_gdsf.cpp                DEFINITIONS RELAIS_BUILDING_TESTS "RELAIS_GDSF_ENABLED=1" LABELS "relais;integration;db;gdsf"             RESOURCE_LOCK)
    # Sees the entry pool's malloc calls: linked with --wrap=malloc.
    relais_add_test(test_relais_entry_layout  tests/relais/test_entry_layout.cpp        LABELS "relais;gdsf")
    target_link_options(test_relais_entry_layout PRIVATE -Wl,--wrap=malloc)
    # Follows every block of the process against the L1 ledger: linked with
    # --wrap=malloc, --wrap=calloc and --wrap=free. No database.
    relais_add_test(test_relais_ledger        tests/relais/test_ledger.cpp              DEFINITIONS RELAIS_BUILDING_TESTS "RELAIS_GDSF_ENABLED=1" LABELS "relais;gdsf;ledger")
    target_link_options(test_relais_ledger PRIVATE -Wl,--wrap=malloc -Wl,--wrap=calloc -Wl,--wrap=free)
    # The maintenance thread, against a registry entry of the test. No database.
    relais_add_test(test_relais_maintenance   tests/relais/test_maintenance.cpp         DEFINITIONS RELAIS_BUILDING_TESTS LABELS "relais;maintenance")
    # Returns from main while the maintenance thread sweeps: its own main, no
    # database, exit code 0 on success.
    add_executable(test_relais_exit_during_sweep tests/relais/test_exit_during_sweep.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures/LsanSuppressions.cpp)
    target_include_directories(test_relais_exit_during_sweep PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
    target_link_libraries(test_relais_exit_during_sweep PRIVATE jcailloux::relais)
    add_test(NAME test_relais_exit_during_sweep COMMAND test_relais_exit_during_sweep)
    set_tests_properties(test_relais_exit_during_sweep PROPERTIES LABELS "relais;maintenance")
    # An entry's weight is read from the allocator in use: the tests that check
    # it run again under jemalloc and mimalloc, preloaded, when installed. Not
    # in a sanitizer build, whose runtime must be the first library loaded.
    if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND NOT CMAKE_CXX_FLAGS MATCHES "-fsanitize")
        foreach(allocator jemalloc mimalloc)
            find_library(RELAIS_${allocator}_LIBRARY NAMES ${allocator})
            if(RELAIS_${allocator}_LIBRARY)
                set(preload "LD_PRELOAD=${RELAIS_${allocator}_LIBRARY};RELAIS_TEST_ALLOCATOR=${allocator}")
                # test_relais_gdsf runs under TSan in a TSan build (below).
                if(NOT RELAIS_ENABLE_TSAN)
                    add_test(NAME test_relais_gdsf_${allocator}
                             COMMAND test_relais_gdsf "[block-bytes],[entry-bytes]")
                    set_tests_properties(test_relais_gdsf_${allocator} PROPERTIES
                        ENVIRONMENT "${preload}"
                        LABELS "relais;integration;db;gdsf;allocator"
                        RESOURCE_LOCK "db")
                endif()
                add_test(NAME test_relais_entry_layout_${allocator}
                         COMMAND test_relais_entry_layout)
                set_tests_properties(test_relais_entry_layout_${allocator} PROPERTIES
                    ENVIRONMENT "${preload}"
                    LABELS "relais;gdsf;allocator")
                add_test(NAME test_relais_ledger_${allocator}
                         COMMAND test_relais_ledger)
                set_tests_properties(test_relais_ledger_${allocator} PROPERTIES
                    ENVIRONMENT "${preload}"
                    LABELS "relais;gdsf;ledger;allocator")
            endif()
        endforeach()
    endif()
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
        # Conditional writes raced from several IoPool loops sharing the repos'
        # L1: TSan validates the cache cascade of claim / patchIf across threads.
        target_compile_options(test_relais_conditional_contention PRIVATE
            -fsanitize=thread -fno-omit-frame-pointer)
        target_link_options(test_relais_conditional_contention PRIVATE -fsanitize=thread)
        # ChunkMap mutations raced from plain threads: TSan validates the
        # conditional removal and the ghost placeholder cleanup.
        target_compile_options(test_relais_chunk_map PRIVATE
            -fsanitize=thread -fno-omit-frame-pointer)
        target_link_options(test_relais_chunk_map PRIVATE -fsanitize=thread)
        # Log callback swapped while plain threads log: TSan validates the
        # callback slot.
        target_compile_options(test_relais_dbprovider_log PRIVATE
            -fsanitize=thread -fno-omit-frame-pointer)
        target_link_options(test_relais_dbprovider_log PRIVATE -fsanitize=thread)
        # purge() raced against the GDSF sweep: TSan validates the sweep
        # exclusion over the histogram both write.
        target_compile_options(test_relais_gdsf PRIVATE
            -fsanitize=thread -fno-omit-frame-pointer)
        target_link_options(test_relais_gdsf PRIVATE -fsanitize=thread)
        # The maintenance thread against the ticking and the passing threads:
        # TSan validates what a served sweep publishes to a thread that waits.
        target_compile_options(test_relais_maintenance PRIVATE
            -fsanitize=thread -fno-omit-frame-pointer)
        target_link_options(test_relais_maintenance PRIVATE -fsanitize=thread)
        # NB: the cache-coherence suite is single-loop (one event-loop thread), so
        # its concurrency-relevant surface — the shared static L1 under concurrent
        # find/update/erase, and the coroutine resume chains — is TSan-validated by
        # test_relais_concurrency and test_io_liveness, not here. It is gated under
        # ASan/LSan instead (the relevant sanitizer for its uncertain-unwind
        # eviction, detached-frame teardown, and self-heal revive).
    endif()

    # -------------------------------------------------------------------------
    # Integrations: each library is fetched for its tests only, at a pinned
    # version. Off by default, since each needs its own system dependencies.
    # -------------------------------------------------------------------------
    option(RELAIS_TEST_DROGON
        "Test the trantor and Drogon integrations (fetches Drogon; needs jsoncpp, uuid, zlib)" OFF)
    if(RELAIS_TEST_DROGON)
        # Drogon's library alone: no tools, examples, ORM or tests of its own.
        function(relais_fetch_drogon)
            set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)  # trantor's options honour these
            set(CMAKE_CXX_STANDARD 23)  # Drogon builds with the standard of the tests
            set(BUILD_CTL OFF)
            set(BUILD_EXAMPLES OFF)
            set(BUILD_ORM OFF)
            set(BUILD_BROTLI OFF)
            set(BUILD_YAML_CONFIG OFF)
            set(BUILD_C-ARES OFF)
            set(BUILD_TESTING OFF)
            set(BUILD_SHARED_LIBS OFF)
            FetchContent_Declare(drogon
                GIT_REPOSITORY https://github.com/drogonframework/drogon.git
                GIT_TAG        v1.9.13
                GIT_SHALLOW    TRUE)
            FetchContent_MakeAvailable(drogon)
        endfunction()
        relais_fetch_drogon()

        relais_add_test(test_trantor        tests/integration/test_trantor.cpp      LABELS "integration;trantor" RESOURCE_LOCK)
        relais_add_test(test_trantor_loops  tests/io/test_io_adversarial_loops.cpp  DEFINITIONS RELAIS_TEST_TRANTOR_LOOP LABELS "io;integration;adversarial;trantor" RESOURCE_LOCK)
        relais_add_test(test_drogon         tests/integration/test_drogon.cpp       LABELS "integration;drogon" RESOURCE_LOCK)
        target_link_libraries(test_trantor PRIVATE trantor)
        target_link_libraries(test_trantor_loops PRIVATE trantor)
        target_link_libraries(test_drogon PRIVATE drogon)

        if(RELAIS_ENABLE_TSAN)
            # trantor's own queues and loops instrumented too, or TSan cannot
            # see their synchronization.
            foreach(target trantor drogon test_trantor test_trantor_loops test_drogon)
                target_compile_options(${target} PRIVATE -fsanitize=thread -fno-omit-frame-pointer)
                target_link_options(${target} PRIVATE -fsanitize=thread)
            endforeach()
        endif()
    endif()

    option(RELAIS_TEST_ASIO "Test the asio integration (fetches standalone asio)" OFF)
    option(RELAIS_TEST_BOOST_ASIO "Test the Boost.Asio integration (needs Boost)" OFF)
    if(RELAIS_TEST_ASIO)
        FetchContent_Declare(asio
            GIT_REPOSITORY https://github.com/chriskohlhoff/asio.git
            GIT_TAG        asio-1-38-2
            GIT_SHALLOW    TRUE)
        FetchContent_MakeAvailable(asio)  # headers only, no CMake project
        add_library(relais_test_asio INTERFACE)
        target_include_directories(relais_test_asio SYSTEM INTERFACE ${asio_SOURCE_DIR}/include)
        target_compile_definitions(relais_test_asio INTERFACE ASIO_STANDALONE ASIO_NO_DEPRECATED)

        relais_add_test(test_asio        tests/integration/test_asio.cpp        LABELS "integration;asio" RESOURCE_LOCK)
        relais_add_test(test_asio_async  tests/integration/test_asio_async.cpp  LABELS "integration;asio" RESOURCE_LOCK)
        relais_add_test(test_asio_loops  tests/io/test_io_adversarial_loops.cpp DEFINITIONS RELAIS_TEST_ASIO_LOOP LABELS "io;integration;adversarial;asio" RESOURCE_LOCK)
        foreach(target test_asio test_asio_async test_asio_loops)
            target_link_libraries(${target} PRIVATE relais_test_asio)
        endforeach()
        list(APPEND RELAIS_ASIO_TESTS test_asio test_asio_async test_asio_loops)
    endif()
    if(RELAIS_TEST_BOOST_ASIO)
        find_package(Boost 1.74 CONFIG REQUIRED)
        relais_add_test(test_boost_asio        tests/integration/test_asio.cpp        DEFINITIONS RELAIS_TEST_BOOST_ASIO LABELS "integration;asio" RESOURCE_LOCK)
        relais_add_test(test_boost_asio_loops  tests/io/test_io_adversarial_loops.cpp DEFINITIONS RELAIS_TEST_BOOST_ASIO RELAIS_TEST_ASIO_LOOP LABELS "io;integration;adversarial;asio" RESOURCE_LOCK)
        set(boost_asio_tests test_boost_asio test_boost_asio_loops)
        # relais::async needs Boost 1.82 (Asio 1.27).
        if(Boost_VERSION VERSION_GREATER_EQUAL 1.82)
            relais_add_test(test_boost_asio_async tests/integration/test_asio_async.cpp DEFINITIONS RELAIS_TEST_BOOST_ASIO LABELS "integration;asio" RESOURCE_LOCK)
            list(APPEND boost_asio_tests test_boost_asio_async)
        endif()
        foreach(target ${boost_asio_tests})
            target_link_libraries(${target} PRIVATE Boost::headers)
            target_compile_definitions(${target} PRIVATE BOOST_ASIO_NO_DEPRECATED)
        endforeach()
        list(APPEND RELAIS_ASIO_TESTS ${boost_asio_tests})
    endif()
    if(RELAIS_ENABLE_TSAN)
        # asio is all headers: its queues are instrumented with the tests.
        foreach(target ${RELAIS_ASIO_TESTS})
            target_compile_options(${target} PRIVATE -fsanitize=thread -fno-omit-frame-pointer)
            target_link_options(${target} PRIVATE -fsanitize=thread)
        endforeach()
    endif()

    option(RELAIS_TEST_LIBUV "Test the libuv integration (fetches libuv)" OFF)
    if(RELAIS_TEST_LIBUV)
        # libuv's static library alone: no shared library, tests or tidy targets.
        function(relais_fetch_libuv)
            set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)  # libuv's options honour these
            set(LIBUV_BUILD_SHARED OFF)
            set(LIBUV_BUILD_TESTS OFF)
            set(LIBUV_BUILD_BENCH OFF)
            set(ENABLE_CLANG_TIDY OFF)
            set(BUILD_TESTING OFF)
            FetchContent_Declare(libuv
                GIT_REPOSITORY https://github.com/libuv/libuv.git
                GIT_TAG        v1.53.0
                GIT_SHALLOW    TRUE)
            FetchContent_MakeAvailable(libuv)
        endfunction()
        relais_fetch_libuv()

        relais_add_test(test_libuv        tests/integration/test_libuv.cpp       LABELS "integration;libuv" RESOURCE_LOCK)
        relais_add_test(test_libuv_loops  tests/io/test_io_adversarial_loops.cpp DEFINITIONS RELAIS_TEST_LIBUV_LOOP LABELS "io;integration;adversarial;libuv" RESOURCE_LOCK)
        target_link_libraries(test_libuv PRIVATE uv_a)
        target_link_libraries(test_libuv_loops PRIVATE uv_a)

        if(RELAIS_ENABLE_TSAN)
            # libuv's queues and async handles instrumented too, or TSan cannot
            # see their synchronization.
            target_compile_options(uv_a PRIVATE -fsanitize=thread -fno-omit-frame-pointer)
            foreach(target test_libuv test_libuv_loops)
                target_compile_options(${target} PRIVATE -fsanitize=thread -fno-omit-frame-pointer)
                target_link_options(${target} PRIVATE -fsanitize=thread)
            endforeach()
        endif()
    endif()

    # A loop relais ships no adapter for, run through the descriptor of
    # integration/fd.h: the example of the documentation.
    option(RELAIS_TEST_GLIB "Test relais on a GLib main loop, through its descriptor (needs GLib)" OFF)
    if(RELAIS_TEST_GLIB)
        find_package(PkgConfig REQUIRED)
        pkg_check_modules(RELAIS_GLIB REQUIRED IMPORTED_TARGET glib-2.0)
        relais_add_test(test_glib tests/integration/test_glib.cpp LABELS "integration;glib;fd" RESOURCE_LOCK)
        target_link_libraries(test_glib PRIVATE PkgConfig::RELAIS_GLIB)
    endif()

    # -------------------------------------------------------------------------
    # Combined targets
    # -------------------------------------------------------------------------
    add_custom_target(test_relais_integration
        DEPENDS
            relais_tests
            test_relais_base
            test_relais_cached
            test_relais_decl_list_cache
            test_relais_partition_key
            test_relais_decl_list_full
            test_relais_decl_list_composite
            test_relais_warmup
            test_relais_l1_config
            test_relais_concurrency
            test_relais_composite_key
            test_relais_assigned_key
            test_relais_upsert
            test_relais_gdsf
            test_relais_gdsf_disabled
            test_relais_entry_layout
            test_relais_ledger
            test_relais_maintenance
            test_relais_exit_during_sweep
        COMMENT "Build all relais integration tests"
    )

    # Single binary combining all relais tests (excluding GDSF tests which
    # require RELAIS_GDSF_ENABLED and would change behavior of all repos).
    # GDSF tests are run via standalone binaries: test_relais_gdsf, test_relais_gdsf_disabled.
    add_executable(test_relais_all
        tests/relais/test_all_runner.cpp
        tests/relais/test_generated_entity.cpp
        tests/relais/test_pg_repo.cpp
        tests/relais/test_local_repo.cpp
        tests/relais/test_decl_list_cache.cpp
        tests/relais/test_partition_key.cpp
        tests/relais/test_decl_list_full.cpp
        tests/relais/test_decl_list_composite.cpp
        tests/relais/test_array_mapping.cpp
        tests/relais/test_warmup.cpp
        tests/relais/config/test_l1_config.cpp
        tests/relais/test_concurrency.cpp
        tests/relais/test_composite_key.cpp
        tests/relais/test_simple_assigned_key.cpp
        tests/relais/test_upsert.cpp
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
    target_compile_definitions(bench_relais_cache PRIVATE RELAIS_BENCHMARK RELAIS_BUILDING_TESTS RELAIS_ENABLE_METRICS=1)
    target_link_libraries(bench_relais_cache PRIVATE jcailloux::relais Catch2::Catch2WithMain)

    add_executable(bench_io_pg benchmarks/bench_io_pg.cpp)
    target_include_directories(bench_io_pg PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks
        ${CMAKE_CURRENT_SOURCE_DIR}/tests)
    target_compile_definitions(bench_io_pg PRIVATE RELAIS_BENCHMARK RELAIS_ENABLE_METRICS=1)
    target_link_libraries(bench_io_pg PRIVATE jcailloux::relais Catch2::Catch2WithMain)

    add_executable(bench_io_batch benchmarks/bench_io_batch.cpp)
    target_include_directories(bench_io_batch PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks
        ${CMAKE_CURRENT_SOURCE_DIR}/tests)
    target_compile_definitions(bench_io_batch PRIVATE RELAIS_BENCHMARK RELAIS_BUILDING_TESTS RELAIS_ENABLE_METRICS=1)
    target_link_libraries(bench_io_batch PRIVATE jcailloux::relais Catch2::Catch2WithMain)

    add_executable(bench_gdsf benchmarks/bench_gdsf.cpp)
    target_include_directories(bench_gdsf PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks
        ${CMAKE_CURRENT_SOURCE_DIR}/tests)
    target_compile_definitions(bench_gdsf PRIVATE RELAIS_BENCHMARK RELAIS_BUILDING_TESTS RELAIS_GDSF_ENABLED=1 RELAIS_ENABLE_METRICS=1)
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

    # Requires PostgreSQL reachable at runtime.
    add_executable(example_iopool_nloop examples/iopool_nloop.cpp)
    target_link_libraries(example_iopool_nloop PRIVATE jcailloux::relais)
endif()
