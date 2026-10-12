#pragma once

#include <cstdint>
#include <string>

namespace relais_test {

// Caller-assigned key declared after the other columns. The generator still
// binds it first in SQL::insert, so every write names its row from the leading
// parameters. Generator-only fixture: no table, never sent to a database.
//
// @relais table=relais_test_late_keys
struct TestLateKey {
    std::string note;
    int64_t group_id = 0;   // @relais primary_key
    int64_t seq = 0;
    int64_t key_id = 0;     // @relais primary_key
};

}  // namespace relais_test
