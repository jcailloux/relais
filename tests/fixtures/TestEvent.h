/**
 * TestEvent.h
 * Pure data struct for PartialKey integration testing.
 * Represents an event in a partitioned table with composite PK (id, region).
 */

#pragma once

#include <cstdint>
#include <string>

namespace relais_test {

    // @relais table=relais_test_events
    struct TestEvent {
        int64_t id = 0;          // @relais primary_key db_managed
        std::string region;      // @relais partition_key
        int64_t user_id = 0;
        std::string title;
        int32_t priority = 0;
        std::string created_at;  // @relais db_managed
    };

}  // namespace relais_test
