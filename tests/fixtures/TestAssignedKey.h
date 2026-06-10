#pragma once

#include <cstdint>
#include <string>

namespace relais_test {

// Simple (non-composite) primary key that is *assigned by the caller*, i.e.
// NOT db_managed. This is the layout that exposed the silent UPDATE param
// off-by-one: toInsertParams begins with the PK column, so an UPDATE built
// from insert params shifted every value by one slot. db_managed simple keys
// (TestItem, TestUser, ...) hide the bug because the PK is excluded from
// insert params too — making toInsertParams and toUpdateParams coincide.
//
// `payload` sits right after the PK on purpose: under the old bug it received
// the PK value, so a round-trip update is enough to catch a regression.
//
// @relais table=relais_test_assigned_keys
struct TestAssignedKey {
    int64_t key_id = 0;    // @relais primary_key   (assigned, NOT db_managed)
    int64_t payload = 0;
    std::string note;
};

}  // namespace relais_test