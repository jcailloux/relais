#pragma once

#include <cstdint>

namespace relais_test {

// Composite-key counterpart of TestSlot: relative updates addressed by
// (group_id, bucket).
//
// @relais table=relais_test_slot_tallies
struct TestSlotTally {
    int64_t group_id = 0;  // @relais primary_key
    int64_t bucket = 0;    // @relais primary_key
    int64_t hits = 0;
};

}  // namespace relais_test
