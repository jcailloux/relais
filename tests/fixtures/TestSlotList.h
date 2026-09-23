#pragma once

#include <cstdint>
#include <optional>
#include <string>

namespace relais_test {

// List view of relais_test_slots: the slot rows paged by group, for the list-
// and cross-invalidation paths of conditional writes. `state` is read as its
// database string (the enum mapping lives on TestSlot). A guarded write that
// moves `group_id` must drop the old group's page and the new one's.
// List queries select every column, so the fields follow the table's order.
//
// @relais table=relais_test_slots
// @relais_list limits=50
struct TestSlotList {
    int64_t id = 0;                         // @relais primary_key db_managed sortable:asc
    int64_t group_id = 0;                   // @relais filterable
    std::string state;
    std::optional<int64_t> holder;
    std::optional<std::string> expires_at;  // @relais timestamp
    int32_t counter = 0;
    int64_t version = 0;
    int32_t priority = 0;
};

}  // namespace relais_test
