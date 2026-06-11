#pragma once

#include <cstdint>
#include <string>

namespace relais_test {

// Read-only entity (@relais read_only) — models a DB view. The generator emits
// no toInsertParams/update and an empty TraitsType::Field enum. Regression
// fixture proving Entity<> and the read-only Repo chain still instantiate.
// @relais table=relais_test_readonly_view read_only
struct TestReadOnlyView {
    int64_t id = 0;            // @relais primary_key
    std::string label;
    int32_t computed_score = 0;
};

}  // namespace relais_test