#pragma once

#include <cstdint>
#include <string>

namespace relais_test {

// @relais table=relais_test_purchases
// @relais output=fixtures/generated/TestPurchaseWrapper.h
// @relais_list limits=10,25,50
struct TestPurchase {
    int64_t id = 0; // @relais primary_key db_managed sortable:desc
    int64_t user_id = 0; // @relais filterable
    std::string product_name;
    int32_t amount = 0;
    std::string status; // @relais filterable
    std::string created_at; // @relais timestamp db_managed
};

}  // namespace relais_test
