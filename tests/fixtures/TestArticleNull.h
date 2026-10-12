#pragma once

#include <cstdint>
#include <optional>
#include <string>

namespace relais_test {

// Fixture for the IS_NULL filter operator, on the existing relais_test_articles
// table (published_at is its nullable column). Filters sort by parameter name:
// category (EQ) -> published_null (IS_NULL) -> views_in (IN), so the IS_NULL slot
// sits between two value-carrying slots in every encoded blob.

// @relais table=relais_test_articles
// @relais_list limits=10,25,50
struct TestArticleNull {
    int64_t id = 0; // @relais primary_key db_managed sortable:desc
    std::string category; // @relais filterable
    int64_t author_id = 0;
    std::string title;
    std::optional<int32_t> view_count; // @relais filterable:views_in:in
    bool is_published = false;
    std::optional<std::string> published_at; // @relais timestamp filterable:published_null:is_null
    std::string created_at; // @relais timestamp db_managed
};

}  // namespace relais_test
