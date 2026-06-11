#pragma once

#include <cstdint>

namespace relais_test {

// Composite-key list entity: an all-PK junction with @relais_list, mirroring a
// real member_roles table. Exercises keyset-cursor pagination over a composite
// primary key (tenant_id, item_id) — the tiebreaker must span both columns.
//
// @relais table=relais_test_composite_list
// @relais_list limits=50
struct TestCompositeKeyList {
    int64_t tenant_id = 0;  // @relais primary_key filterable sortable:asc
    int64_t item_id = 0;    // @relais primary_key filterable
};

}  // namespace relais_test