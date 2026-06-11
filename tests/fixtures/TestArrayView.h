#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace relais_test {

// Read-only aggregated view: one row per owner carrying the set of related rows
// collapsed into arrays (array_agg). Mirrors the codiga member_role_set pattern
// (a member's role_ids as int8[]). Exercises the array-column mapping:
//   int8[] -> std::vector<int64_t>   (numeric, no quoting)
//   text[] -> std::vector<std::string>  (quoting/escaping, commas inside elements)
// Point-lookup find(owner_id) returns one entity; the array fields are parsed by
// PgResult::Row::get<std::vector<T>>.
//
// @relais table=relais_test_array_view read_only
struct TestArrayView {
    int64_t owner_id = 0;              // @relais primary_key
    std::vector<int64_t> tag_ids;      // int8[] aggregate
    std::vector<std::string> labels;   // text[] aggregate
};

}  // namespace relais_test
