#pragma once

#include <cstdint>
#include <string>

namespace relais_test {

// @relais table=relais_test_users
struct TestUser {
    int64_t id = 0; // @relais primary_key db_managed
    std::string username;
    std::string email;
    int32_t balance = 0;
    std::string created_at; // @relais timestamp db_managed
};

}  // namespace relais_test
