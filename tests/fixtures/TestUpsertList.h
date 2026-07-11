#pragma once

#include <cstdint>
#include <string>

namespace relais_test {

// Assigned-PK entity that ALSO carries a @relais_list — the one shape upsert's
// list-invalidation path needs, and the one no other fixture provides
// (TestAssignedKey has no list; TestCompositeKeyList is an all-PK junction with
// no updatable column, hence not upsertable). `owner_id` is the filter/sort
// dimension: an upsert that migrates it must drop the old owner's page AND the
// new owner's page (pre-image discriminant), whereas an insert-via-upsert only
// drops the target owner's page. `label` is a plain, non-key, non-filter column
// so the SET list of DO UPDATE is non-empty (the upsert-applicability gate).
//
// @relais table=relais_test_upsert_list
// @relais_list limits=50
struct TestUpsertList {
    int64_t id = 0;          // @relais primary_key   (assigned, NOT db_managed)
    int64_t owner_id = 0;    // @relais filterable sortable:asc
    std::string label;
};

}  // namespace relais_test
