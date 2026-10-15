# Fails when a cache tier handles an uncertain write without its settle ticket.
#
# A write abandoned on a timeout may still commit after the precautionary
# eviction. Each cache tier hands that eviction to detail::precaution or
# detail::precautionSync, with the ticket of the exception, so that it runs
# again once the write is settled. A handler of io::PgUncertainError in these
# headers must therefore read `settleTicket()` before it rethrows or keeps the
# exception, or carry the marker `precaution-exempt` (with the reason) when it
# has no eviction to replay.
#
# Usage: cmake -DROOT=<source dir> -P cmake/CheckPrecautionSites.cmake

if(NOT DEFINED ROOT)
    message(FATAL_ERROR "CheckPrecautionSites: pass -DROOT=<source dir>")
endif()

set(tiers LocalRepo ListMixin InvalidationMixin Repo)
set(needle "catch (const io::PgUncertainError")
string(LENGTH "${needle}" needle_len)
set(violations "")
set(sites 0)

foreach(tier IN LISTS tiers)
    set(header "${ROOT}/include/jcailloux/relais/repository/${tier}.h")
    file(READ "${header}" text)
    set(line 1)
    while(TRUE)
        string(FIND "${text}" "${needle}" at)
        if(at EQUAL -1)
            break()
        endif()
        string(SUBSTRING "${text}" 0 ${at} before)
        string(REGEX MATCHALL "\n" newlines "${before}")
        list(LENGTH newlines n)
        math(EXPR line "${line} + ${n}")
        math(EXPR next "${at} + ${needle_len}")
        string(SUBSTRING "${text}" ${next} -1 text)
        math(EXPR sites "${sites} + 1")

        # The handler, up to where it rethrows or keeps the exception.
        set(handler "${text}")
        foreach(stop "throw;" "std::current_exception()")
            string(FIND "${handler}" "${stop}" end)
            if(NOT end EQUAL -1)
                string(SUBSTRING "${handler}" 0 ${end} handler)
            endif()
        endforeach()
        if(NOT handler MATCHES "settleTicket\\(\\)" AND NOT handler MATCHES "precaution-exempt")
            string(APPEND violations "  include/jcailloux/relais/repository/${tier}.h:${line}\n")
        endif()
    endwhile()
endforeach()

if(violations)
    message(FATAL_ERROR
        "Uncertain-write handlers that drop their settle ticket:\n${violations}"
        "Pass e.settleTicket() to detail::precaution / precautionSync, or mark "
        "the handler `precaution-exempt: <reason>` when it evicts nothing.")
endif()
message(STATUS "CheckPrecautionSites: ${sites} uncertain-write handlers checked")
