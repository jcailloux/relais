#pragma once

#include <cstdint>
#include <string>

namespace relais_test {

// @relais table=relais_test_items
// @relais output=fixtures/generated/TestItemWrapper.h
struct TestItem {
    int64_t id = 0; // @relais primary_key db_managed
    std::string name;
    int32_t value = 0;
    std::string description;
    bool is_active = true;
    std::string created_at; // @relais timestamp db_managed
};

}  // namespace relais_test
