#pragma once

#include <cstdint>
#include <string>

namespace relais_test {

// Five-value arbitrary grid — proves allowedLimits is emitted for N != 4.
// @relais table=relais_test_limits
// @relais_list limits=5,10,20,50,100
struct TestLimits {
    int64_t id = 0; // @relais primary_key db_managed sortable:desc
    std::string category; // @relais filterable
    std::string created_at; // @relais timestamp db_managed
};

// Unsorted + duplicate input — proves the generator sorts and dedups:
// limits=50,10,10,25 -> allowedLimits = {10, 25, 50}.
// @relais table=relais_test_limits_messy
// @relais_list limits=50,10,10,25
struct TestLimitsMessy {
    int64_t id = 0; // @relais primary_key db_managed sortable:desc
    std::string category; // @relais filterable
    std::string created_at; // @relais timestamp db_managed
};

}  // namespace relais_test
