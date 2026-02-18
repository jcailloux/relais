/**
 * TestQueryHelpers.h
 * Shared query builder helpers for declarative list tests.
 * Used by test_cached_repository.cpp, test_decl_list_cache.cpp,
 * test_decl_list_redis.cpp, test_decl_list_full.cpp.
 */

#pragma once

#include "TestRepositories.h"
#include "jcailloux/relais/list/decl/HttpQueryParser.h"

namespace relais_test {

namespace ld = jcailloux::relais::cache::list::decl;

using ArticleListQuery = TestArticleListRepo::ListQuery;
using PurchaseListQuery = TestPurchaseListRepo::ListQuery;

inline ArticleListQuery makeArticleQuery(
    std::optional<std::string> category = std::nullopt,
    std::optional<int64_t> author_id = std::nullopt,
    uint16_t limit = 10
) {
    ArticleListQuery q;
    q.limit = limit;
    if (author_id) q.filters.template get<0>() = *author_id;
    if (category) q.filters.template get<1>() = std::move(*category);

    using Desc = TestArticleListRepo::ListDescriptorType;
    q.group_key = ld::groupCacheKey<Desc>(q);
    q.cache_key = ld::cacheKey<Desc>(q);
    return q;
}

inline PurchaseListQuery makePurchaseQuery(
    std::optional<int64_t> user_id = std::nullopt,
    std::optional<std::string> status = std::nullopt,
    uint16_t limit = 10
) {
    PurchaseListQuery q;
    q.limit = limit;
    if (status) q.filters.template get<0>() = std::move(*status);
    if (user_id) q.filters.template get<1>() = *user_id;

    using Desc = TestPurchaseListRepo::ListDescriptorType;
    q.group_key = ld::groupCacheKey<Desc>(q);
    q.cache_key = ld::cacheKey<Desc>(q);
    return q;
}

} // namespace relais_test
