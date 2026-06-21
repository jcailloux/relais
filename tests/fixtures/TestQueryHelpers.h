/**
 * TestQueryHelpers.h
 * Shared query builder helpers for declarative list tests.
 * Used by test_cached_repository.cpp, test_decl_list_cache.cpp,
 * test_decl_list_redis.cpp, test_decl_list_full.cpp.
 */

#pragma once

#include "TestRepositories.h"
#include "jcailloux/relais/list/spec/HttpQueryParser.h"

namespace relais_test {

namespace ld = jcailloux::relais::list::spec;

using ArticleListQuery = TestArticleListRepo::ListQuery;
using PurchaseListQuery = TestPurchaseListRepo::ListQuery;

inline ArticleListQuery makeArticleQuery(
    std::optional<std::string> category = std::nullopt,
    std::optional<int64_t> author_id = std::nullopt,
    uint16_t limit = 10
) {
    using Desc = TestArticleListRepo::ListDescriptorType;
    ld::ListQueryParams<Desc> p;
    p.limit = limit;
    if (author_id) p.filters.template get<"author_id">() = *author_id;
    if (category) p.filters.template get<"category">() = std::move(*category);
    return ld::seal<Desc>(std::move(p));
}

inline PurchaseListQuery makePurchaseQuery(
    std::optional<int64_t> user_id = std::nullopt,
    std::optional<std::string> status = std::nullopt,
    uint16_t limit = 10
) {
    using Desc = TestPurchaseListRepo::ListDescriptorType;
    ld::ListQueryParams<Desc> p;
    p.limit = limit;
    if (status) p.filters.template get<"status">() = std::move(*status);
    if (user_id) p.filters.template get<"user_id">() = *user_id;
    return ld::seal<Desc>(std::move(p));
}

} // namespace relais_test
