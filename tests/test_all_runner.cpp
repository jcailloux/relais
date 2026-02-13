/**
 * test_all_runner.cpp
 * Single translation unit that registers the DrogonTestListener exactly once
 * for the combined test executable (test_relais_all).
 *
 * Individual test executables register the listener via test_helper.h directly
 * (the guard RELAIS_COMBINED_TESTS is not defined for them).
 */

#include <catch2/reporters/catch_reporter_event_listener.hpp>
#include <catch2/reporters/catch_reporter_registrars.hpp>

// Include test_helper.h without auto-registration (RELAIS_COMBINED_TESTS is defined)
#include "fixtures/test_helper.h"

namespace relais_test {
CATCH_REGISTER_LISTENER(DrogonTestListener)
}