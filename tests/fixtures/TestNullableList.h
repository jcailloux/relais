#pragma once

#include <cstdint>
#include <optional>
#include <string>

namespace relais_test {

// A list filtered on nullable columns, one filter per value operator on `rank`.
// Filters sort by parameter name, so `owner_id` follows `label_nin`: a group
// filtering both reads `owner_id` after the NOT IN slot of a NULL label.
// @relais table=relais_test_nullable_list
// @relais_list limits=10,25,50
struct TestNullableList {
    int64_t id = 0;                    // @relais primary_key db_managed sortable:desc
    int64_t owner_id = 0;              // @relais filterable
    std::optional<int32_t> rank;       // @relais filterable:rank_eq filterable:rank_ne:ne filterable:rank_gt:gt filterable:rank_ge:ge filterable:rank_lt:lt filterable:rank_le:le filterable:rank_in:in filterable:rank_nin:nin
    std::optional<std::string> label;  // @relais filterable:label_ne:ne filterable:label_nin:nin
};

}  // namespace relais_test
