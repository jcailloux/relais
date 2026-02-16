#pragma once

#include <cstdint>
#include <optional>
#include <string>

namespace relais_test {

// @relais table=relais_test_articles
// @relais_list limits=10,25,50
struct TestArticle {
    int64_t id = 0; // @relais primary_key db_managed sortable:desc
    std::string category; // @relais filterable
    int64_t author_id = 0; // @relais filterable
    std::string title;
    std::optional<int32_t> view_count; // @relais sortable
    bool is_published = false;
    std::optional<std::string> published_at; // @relais timestamp
    std::string created_at; // @relais timestamp db_managed
};

}  // namespace relais_test
