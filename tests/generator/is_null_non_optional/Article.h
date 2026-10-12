#pragma once

#include <cstdint>
#include <string>

// Must be rejected: is_null on a member that cannot be null.

// @relais table=articles
struct Article {
    int64_t id = 0; // @relais primary_key db_managed
    int64_t author_id = 0; // @relais filterable:author_null:is_null
};
