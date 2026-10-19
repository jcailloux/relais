#pragma once

#include <string>
#include <string_view>

namespace relais_test {

// The test database. Its commits do not wait for the WAL flush
// (synchronous_commit=off): a case sees what it commits as it otherwise
// would, but never waits on the disk, which the short query timeouts of the
// settle cases would take for a lost round trip. Durability is not under test.

#define RELAIS_TEST_SERVER \
    "host=localhost port=5432 dbname=relais_test user=relais_test password=relais_test"
#define RELAIS_TEST_SETTINGS "-c synchronous_commit=off"

inline constexpr const char* kConnInfo =
    RELAIS_TEST_SERVER " options='" RELAIS_TEST_SETTINGS "'";

/// kConnInfo with more session settings (`-c name=value ...`).
inline std::string connInfoWith(std::string_view settings) {
    return std::string(RELAIS_TEST_SERVER " options='" RELAIS_TEST_SETTINGS " ")
        + std::string(settings) + "'";
}

#undef RELAIS_TEST_SETTINGS
#undef RELAIS_TEST_SERVER

}  // namespace relais_test
