/**
 * TestQueryHelpers.h
 * Shared query builder helpers for declarative list tests.
 * Used by test_cached_repository.cpp, test_decl_list_cache.cpp,
 * test_decl_list_redis.cpp, test_decl_list_full.cpp.
 */

#pragma once

#include "TestRepositories.h"

namespace relais_test {

using ArticleListQuery = TestArticleListRepo::ListQuery;
using PurchaseListQuery = TestPurchaseListRepo::ListQuery;

inline ArticleListQuery makeArticleQuery(
    std::optional<std::string> category = std::nullopt,
    std::optional<int64_t> author_id = std::nullopt,
    uint16_t limit = 10
) {
    ArticleListQuery q;
    q.limit = limit;
    if (category) q.filters.template get<0>() = std::move(*category);
    if (author_id) q.filters.template get<1>() = *author_id;

    size_t h = std::hash<uint16_t>{}(limit) ^ 0xBEEF;  // salt to avoid collision with purchase
    if (q.filters.template get<0>()) h ^= std::hash<std::string_view>{}(*q.filters.template get<0>()) << 1;
    if (q.filters.template get<1>()) h ^= std::hash<int64_t>{}(*q.filters.template get<1>()) << 2;
    q.query_hash = h;
    return q;
}

inline PurchaseListQuery makePurchaseQuery(
    std::optional<int64_t> user_id = std::nullopt,
    std::optional<std::string> status = std::nullopt,
    uint16_t limit = 10
) {
    PurchaseListQuery q;
    q.limit = limit;
    if (user_id) q.filters.template get<0>() = *user_id;
    if (status) q.filters.template get<1>() = std::move(*status);

    size_t h = std::hash<uint16_t>{}(limit);
    if (q.filters.template get<0>()) h ^= std::hash<int64_t>{}(*q.filters.template get<0>()) << 1;
    if (q.filters.template get<1>()) h ^= std::hash<std::string_view>{}(*q.filters.template get<1>()) << 2;
    q.query_hash = h;
    return q;
}

} // namespace relais_test
