/**
 * TestEvent.h
 * Pure data struct for PartialKey integration testing.
 * Represents an event in a partitioned table with composite PK (id, region).
 */

#pragma once

#include <cstdint>
#include <string>

namespace relais_test {

struct TestEvent {
    int64_t id = 0;
    std::string region;
    int64_t user_id = 0;
    std::string title;
    int32_t priority = 0;
    std::string created_at;
};

}  // namespace relais_test
