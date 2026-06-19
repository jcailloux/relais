#pragma once

#include <cstdint>
#include <optional>
#include <string>

namespace relais_test {

// Dedicated fixture exercising the generator's NIN filter operator. Maps onto the
// existing relais_test_articles table (same columns as TestArticle), so no new
// migration is required and TransactionGuard already cleans it. Covers both
// annotation forms: short `filterable:nin` (category) and long
// `filterable:p:not_in` (author_id -> HTTP param "authors", proving the `not_in`
// alias normalizes to nin), side by side with an EQ (is_published) and a range
// (view_count GE) for multi-filter alignment.

// @relais table=relais_test_articles
// @relais_list limits=10,25,50
struct TestArticleNin {
    int64_t id = 0; // @relais primary_key db_managed sortable:desc
    std::string category; // @relais filterable:nin
    int64_t author_id = 0; // @relais filterable:authors:not_in
    std::string title;
    std::optional<int32_t> view_count; // @relais filterable:views_min:ge sortable
    bool is_published = false; // @relais filterable
    std::optional<std::string> published_at; // @relais timestamp
    std::string created_at; // @relais timestamp db_managed
};

}  // namespace relais_test
