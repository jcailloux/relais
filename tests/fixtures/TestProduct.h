#pragma once

#include <cstdint>
#include <optional>
#include <string>

namespace relais_test {

// @relais table=relais_test_products
struct TestProduct {
    int64_t id = 0;                       // @relais primary_key db_managed
    std::string productName;              // @relais column=product_name
    int32_t stockLevel = 0;               // @relais column=stock_level
    std::optional<int32_t> discountPct;   // @relais column=discount_pct
    bool available = true;                // @relais column=is_available
    std::string description;
    std::string createdAt;                // @relais timestamp db_managed column=created_at
};

}  // namespace relais_test
