#[=======================================================================[.rst:
RelaisGenerateWrappers
----------------------

Provides the ``relais_generate_wrappers()`` function for generating
EntityWrapper headers from ``@relais``-annotated C++ structs.

Usage::

    include(RelaisGenerateWrappers)
    relais_generate_wrappers(
        SOURCES src/entities/
        OUTPUT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src/generated/
    )

    add_dependencies(my_app relais_generate_wrappers)

#]=======================================================================]

find_package(Python3 REQUIRED COMPONENTS Interpreter)

# Resolve script path: works both in-tree/FetchContent and after install
if(EXISTS "${CMAKE_CURRENT_LIST_DIR}/../scripts/generate_entities.py")
    set(_RELAIS_GENERATE_SCRIPT "${CMAKE_CURRENT_LIST_DIR}/../scripts/generate_entities.py")
else()
    set(_RELAIS_GENERATE_SCRIPT "${CMAKE_CURRENT_LIST_DIR}/scripts/generate_entities.py")
endif()

function(relais_generate_wrappers)
    cmake_parse_arguments(ARG "" "OUTPUT_DIR" "SOURCES" ${ARGN})

    if(NOT ARG_SOURCES)
        message(FATAL_ERROR "relais_generate_wrappers: SOURCES is required")
    endif()
    if(NOT ARG_OUTPUT_DIR)
        message(FATAL_ERROR "relais_generate_wrappers: OUTPUT_DIR is required")
    endif()

    # Resolve all source paths to absolute
    set(_abs_sources "")
    foreach(_src IN LISTS ARG_SOURCES)
        cmake_path(ABSOLUTE_PATH _src BASE_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} OUTPUT_VARIABLE _abs)
        list(APPEND _abs_sources "${_abs}")
    endforeach()

    # Resolve output dir to absolute
    cmake_path(ABSOLUTE_PATH ARG_OUTPUT_DIR BASE_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} OUTPUT_VARIABLE _abs_output)

    add_custom_target(relais_generate_wrappers
        COMMAND ${Python3_EXECUTABLE}
            ${_RELAIS_GENERATE_SCRIPT}
            --sources ${_abs_sources}
            --output-dir ${_abs_output}
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Generating entity wrappers from @relais annotations"
        VERBATIM
    )
endfunction()
