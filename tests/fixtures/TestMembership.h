#pragma once

#include <cstdint>
#include <string>

namespace relais_test {

// @relais table=relais_test_memberships
struct TestMembership {
    int64_t user_id = 0;     // @relais primary_key
    int64_t group_id = 0;    // @relais primary_key
    std::string role;
    int64_t joined_at = 0;   // @relais db_managed
};

}  // namespace relais_test
