#pragma once

#include <cstdint>
#include <string>

namespace relais_test {

// Filters declared WITHOUT any sort/list — proves the generator emits a
// standalone FilterSet decoupled from ListDescriptor: HasFilterSet is true,
// HasListDescriptor is false, so the where-variants are available while
// ListMixin stays out of the chain (étape 0b decorrelation).
// @relais table=relais_test_filter_only
struct TestFilterOnly {
    int64_t id = 0;        // @relais primary_key
    std::string category;  // @relais filterable
    int64_t owner_id = 0;  // @relais filterable
};

}  // namespace relais_test
