#pragma once

#include <cstdint>
#include <optional>
#include <string>

namespace relais_test {

// Filters declared WITHOUT any sort/list — proves the generator emits a
// standalone FilterSet decoupled from ListDescriptor: HasFilterSet is true,
// HasListDescriptor is false, so the where-variants are available while
// ListMixin stays out of the chain. One filter per operator; `label` is
// nullable, for the three-valued logic of NE / NIN and for IS_NULL.
// @relais table=relais_test_filter_only
struct TestFilterOnly {
    int64_t id = 0;        // @relais primary_key db_managed
    std::string category;  // @relais filterable filterable:category_in:in
    int64_t owner_id = 0;  // @relais filterable
    int32_t score = 0;     // @relais filterable:score_gt:gt filterable:score_ge:ge filterable:score_lt:lt filterable:score_le:le
    std::optional<std::string> label;  // @relais filterable:label_ne:ne filterable:label_nin:nin filterable:label_null:is_null
};

}  // namespace relais_test
