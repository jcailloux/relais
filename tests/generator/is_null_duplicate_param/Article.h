#pragma once

#include <cstdint>
#include <optional>
#include <string>

// Must be rejected: the short form names both filters after the field.

// @relais table=articles
struct Article {
    int64_t id = 0; // @relais primary_key db_managed
    std::optional<std::string> published_at; // @relais filterable filterable:is_null
};
