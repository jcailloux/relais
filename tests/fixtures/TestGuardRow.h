#pragma once

#include <cstdint>
#include <optional>
#include <string>

namespace relais_test {

enum class GuardTone {
    Low = 0,
    High = 1
};

// One column per kind the prudent guard evaluator tells apart: integers,
// boolean, floating point, text under the default collation, varchar, char(n),
// text under a nondeterministic collation, an enum stored as text, and a
// timestamp. The table only serves the differential tests of that evaluator.
//
// @relais table=relais_test_guard_rows
struct TestGuardRow {
    int64_t id = 0;                     // @relais primary_key db_managed
    int32_t num = 0;
    std::optional<int64_t> big;
    bool flag = false;
    double ratio = 0.0;
    std::string name;                   // text
    std::string label;                  // varchar(20)
    std::string code;                   // char(4)
    std::string tag;                    // text, case-insensitive collation
    GuardTone tone = GuardTone::Low;    // @relais enum=low:Low,high:High
    std::optional<std::string> at;      // @relais timestamp
};

}  // namespace relais_test
