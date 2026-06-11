#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace relais_test {

// Writable entity with native array columns (int8[] / text[]). Unlike TestArrayView
// (a read-only aggregate), this is a real table: it exercises the WRITE path too —
// toInsertParams/toUpdateParams serialize std::vector<T> into PG array literals via
// PgParams::toParam, the inverse of the read parser. Round-trips through insert →
// find → update.
//
// @relais table=relais_test_array_rw
struct TestArrayRw {
    int64_t owner_id = 0;              // @relais primary_key
    std::vector<int64_t> tag_ids;      // int8[]
    std::vector<std::string> labels;   // text[]
};

}  // namespace relais_test
