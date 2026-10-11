# Fails when a header sends a Lua script outside RedisCache::evalScript.
#
# Every script goes through RedisCache::evalScript / evalScriptArgs, which send
# EVALSHA and fall back to EVAL on NOSCRIPT. A literal "EVAL" or "EVALSHA" in
# code elsewhere bypasses that fallback (or resends the full body every call).
# Comment lines (first non-blank characters `//` or `*`) are ignored; a code line
# may carry the marker `relais:allow-raw-eval` to be allowed.
#
# Usage: cmake -DROOT=<source dir> -P cmake/CheckNoRawEval.cmake

if(NOT DEFINED ROOT)
    message(FATAL_ERROR "CheckNoRawEval: pass -DROOT=<source dir>")
endif()

file(GLOB_RECURSE headers "${ROOT}/include/*.h")
set(violations "")
foreach(header IN LISTS headers)
    file(STRINGS "${header}" lines REGEX "\"EVAL(SHA)?\"")
    foreach(line IN LISTS lines)
        if(line MATCHES "^[ \t]*(//|\\*)")
            continue()
        endif()
        if(line MATCHES "relais:allow-raw-eval")
            continue()
        endif()
        file(RELATIVE_PATH rel "${ROOT}" "${header}")
        string(STRIP "${line}" stripped)
        string(APPEND violations "  ${rel}: ${stripped}\n")
    endforeach()
endforeach()

if(violations)
    message(FATAL_ERROR
        "Raw EVAL/EVALSHA outside RedisCache::evalScript:\n${violations}"
        "Run scripts through RedisCache::evalScript or evalScriptArgs.")
endif()
message(STATUS "CheckNoRawEval: no raw EVAL/EVALSHA in ${ROOT}/include")
