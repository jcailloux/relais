#pragma once

#include <cstdint>

namespace relais_test {

// Pure all-primary-key junction — every column is part of the composite key,
// so there is NO updatable column. Regression fixture for two coupled
// generator behaviors:
//   1. TraitsType::Field is still emitted (empty enum) so Entity<> instantiates.
//   2. SQL::update / toUpdateParams are suppressed (an empty SET clause would be
//      malformed), making update() unavailable rather than silently broken.
// @relais table=relais_test_all_pk_junction
struct TestAllPkJunction {
    int64_t user_id = 0;   // @relais primary_key
    int64_t role_id = 0;   // @relais primary_key
};

}  // namespace relais_test