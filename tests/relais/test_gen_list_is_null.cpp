/**
 * test_gen_list_is_null.cpp
 *
 * The IS_NULL filter operator, from the generated descriptor down to every
 * consumer that runs without a server: SQL, both L1 matchers, predicate
 * compatibility and sort range, Lua schema, HTTP parsers and query builder.
 *
 * Fixture TestArticleNull: category (EQ) -> published_null (IS_NULL) ->
 * views_in (IN), so the IS_NULL slot sits between two filters that bind a
 * parameter. The byte layout of keys and blobs is pinned in
 * test_list_key_golden; the Lua matchers and the repos in test_decl_list_is_null.
 */

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include "fixtures/generated/TestArticleNullEntity.h"
#include <jcailloux/relais/list/spec/CanonicalEncoding.h>
#include <jcailloux/relais/list/spec/GeneratedCriteria.h>
#include <jcailloux/relais/list/spec/GeneratedTraits.h>
#include <jcailloux/relais/list/spec/HttpQueryParser.h>
#include <jcailloux/relais/list/spec/ListQueryBuilder.h>
#include <jcailloux/relais/list/spec/SortDescriptor.h>

namespace decl = jcailloux::relais::list::spec;
using relais_test::TestArticleNull;
using GenEntity = entity::generated::TestArticleNullEntity;
using Mapping = entity::generated::TestArticleNullMapping;

namespace {

// ListMixin adds the Entity alias to the embedded descriptor.
struct GenDesc : Mapping::ListDescriptor {
    using Entity = GenEntity;
};

using FilterTuple = std::remove_const_t<decltype(GenDesc::filters)>;
using F0 = std::tuple_element_t<0, FilterTuple>;
using F1 = std::tuple_element_t<1, FilterTuple>;
using F2 = std::tuple_element_t<2, FilterTuple>;

// IS_NULL on the sort column: the predicate must not narrow the sort range.
struct SortedOnNullable {
    using Entity = GenEntity;
    static constexpr auto filters = std::tuple{
        decl::Filter<"views_null", &TestArticleNull::view_count, "view_count", decl::Op::IS_NULL>{}
    };
    static constexpr auto sorts = std::tuple{
        decl::Sort<"view_count", &TestArticleNull::view_count, "view_count",
                   decl::SortDirection::Desc>{}
    };
};

GenEntity makeArticle(std::string category, std::optional<std::string> published_at,
                      std::optional<int32_t> view_count = 1) {
    GenEntity e;
    e.id = 1;
    e.category = std::move(category);
    e.published_at = std::move(published_at);
    e.view_count = view_count;
    return e;
}

const std::optional<std::string> kNull = std::nullopt;
const std::optional<std::string> kSet = std::string("2026-01-01 00:00:00+00");

// Both L1 matchers must agree: the free matchesFilters (write invalidation)
// and Filters::matchesFilters over extracted tags.
template<typename D>
bool l1Match(const GenEntity& e, const decl::Filters<D>& f) {
    const bool viaFree = decl::matchesFilters<D>(e, f);
    const bool viaTags = f.matchesFilters(decl::extractTags<D>(e));
    CHECK(viaFree == viaTags);
    return viaFree;
}

using Params = std::unordered_map<std::string, std::string>;

}  // namespace

// =============================================================================
// Generator output
// =============================================================================

static_assert(F0::op == decl::Op::EQ);
static_assert(F1::op == decl::Op::IS_NULL, "`filterable:published_null:is_null` -> Op::IS_NULL");
static_assert(F2::op == decl::Op::IN);
static_assert(F1::name.view() == std::string_view{"published_null"});
static_assert(F1::column() == std::string_view{"published_at"});
static_assert(std::is_same_v<F1::value_type, bool>);
static_assert(std::is_same_v<F1::filter_type, std::optional<bool>>);
static_assert(std::is_same_v<decltype(Mapping::FilterSet::Values::published_null),
                             std::optional<bool>>,
              "the eraseWhere predicate member is optional<bool>");

// =============================================================================
// SQL
// =============================================================================

TEST_CASE("IS_NULL SQL: true, false, inactive", "[list][is_null][unit][sql]") {
    decl::Filters<GenDesc> f;

    f.get<"published_null">() = true;
    auto w = decl::buildWhereClause<GenDesc>(f);
    CHECK(w.sql == "\"published_at\" IS NULL");
    CHECK(w.params.params.empty());
    CHECK(w.next_param == 1);

    f.get<"published_null">() = false;
    w = decl::buildWhereClause<GenDesc>(f);
    CHECK(w.sql == "\"published_at\" IS NOT NULL");
    CHECK(w.params.params.empty());
    CHECK(w.next_param == 1);

    f.get<"published_null">().reset();
    w = decl::buildWhereClause<GenDesc>(f);
    CHECK(w.sql.empty());
    CHECK(w.next_param == 1);
}

TEST_CASE("IS_NULL SQL binds nothing: the $n numbering stays continuous",
          "[list][is_null][unit][sql]") {
    decl::Filters<GenDesc> f;
    f.get<"category">() = std::string("tech");
    f.get<"published_null">() = true;
    f.get<"views_in">() = std::vector<int32_t>{3, 1};

    auto w = decl::buildWhereClause<GenDesc>(f);
    CHECK(w.sql == "\"category\"=$1 AND \"published_at\" IS NULL AND \"view_count\" = ANY($2)");
    CHECK(w.params.params.size() == 2);
    CHECK(w.next_param == 3);  // the keyset cursor binds from $3

    f.get<"published_null">() = false;
    w = decl::buildWhereClause<GenDesc>(f);
    CHECK(w.sql == "\"category\"=$1 AND \"published_at\" IS NOT NULL AND \"view_count\" = ANY($2)");
    CHECK(w.params.params.size() == 2);
    CHECK(w.next_param == 3);
}

// =============================================================================
// L1 matching
// =============================================================================

TEST_CASE("IS_NULL L1 match: null and non-null entity against true, false, inactive",
          "[list][is_null][unit][match]") {
    const auto nul = makeArticle("tech", kNull);
    const auto set = makeArticle("tech", kSet);
    decl::Filters<GenDesc> f;

    SECTION("true selects the null entity") {
        f.get<"published_null">() = true;
        CHECK(l1Match(nul, f));
        CHECK_FALSE(l1Match(set, f));
    }
    SECTION("false selects the non-null entity") {
        f.get<"published_null">() = false;
        CHECK_FALSE(l1Match(nul, f));
        CHECK(l1Match(set, f));
    }
    SECTION("inactive selects both") {
        CHECK(l1Match(nul, f));
        CHECK(l1Match(set, f));
    }
}

TEST_CASE("IS_NULL L1 match combines with the filters around it",
          "[list][is_null][unit][match]") {
    decl::Filters<GenDesc> f;
    f.get<"category">() = std::string("tech");
    f.get<"published_null">() = true;
    f.get<"views_in">() = std::vector<int32_t>{1, 2};

    CHECK(l1Match(makeArticle("tech", kNull, 2), f));
    CHECK_FALSE(l1Match(makeArticle("news", kNull, 2), f));  // EQ before fails
    CHECK_FALSE(l1Match(makeArticle("tech", kSet, 2), f));   // IS_NULL fails
    CHECK_FALSE(l1Match(makeArticle("tech", kNull, 9), f));  // IN after fails
}

// =============================================================================
// Predicate compatibility and sort range (eraseWhere)
// =============================================================================

TEST_CASE("IS_NULL predicate compatibility", "[list][is_null][unit][compat]") {
    decl::Filters<GenDesc> isNull, notNull, open;
    isNull.get<"published_null">() = true;
    notNull.get<"published_null">() = false;

    CHECK_FALSE(decl::predicateGroupCompatible<GenDesc>(isNull, notNull));
    CHECK_FALSE(decl::predicateGroupCompatible<GenDesc>(notNull, isNull));
    CHECK(decl::predicateGroupCompatible<GenDesc>(isNull, isNull));
    CHECK(decl::predicateGroupCompatible<GenDesc>(notNull, notNull));
    CHECK(decl::predicateGroupCompatible<GenDesc>(isNull, open));  // group wildcard
    CHECK(decl::predicateGroupCompatible<GenDesc>(open, notNull)); // predicate wildcard
}

TEST_CASE("IS_NULL on the sort column leaves the sort range full",
          "[list][is_null][unit][range]") {
    constexpr auto kMin = std::numeric_limits<int64_t>::min();
    constexpr auto kMax = std::numeric_limits<int64_t>::max();
    for (bool v : {true, false}) {
        decl::Filters<SortedOnNullable> pred;
        pred.get<"views_null">() = v;
        auto r = decl::predicateSortRange<SortedOnNullable>(pred, 0);
        CHECK(r.lo == kMin);
        CHECK(r.hi == kMax);
    }
}

// =============================================================================
// Lua schema
// =============================================================================

TEST_CASE("IS_NULL schema: bool type, operator 'N'", "[list][is_null][unit][schema]") {
    CHECK(decl::filterSchema<GenDesc>() == "s=1N4@");
}

// =============================================================================
// HTTP and query builder
// =============================================================================

TEST_CASE("IS_NULL HTTP value parses as a boolean", "[list][is_null][unit][parse]") {
    auto parsed = [](std::string v) {
        return decl::parseListQuery<GenDesc>(Params{{"published_null", std::move(v)}})
            .filters().get<"published_null">();
    };
    CHECK(parsed("true") == std::optional<bool>(true));
    CHECK(parsed("1") == std::optional<bool>(true));
    CHECK(parsed("false") == std::optional<bool>(false));
    CHECK(parsed("0") == std::optional<bool>(false));
    CHECK_FALSE(parsed("maybe").has_value());  // permissive: invalid -> inactive

    CHECK_FALSE(decl::parseListQuery<GenDesc>(Params{}).filters().get<"published_null">());

    auto strict = decl::parseListQueryStrict<GenDesc>(Params{{"published_null", "false"}});
    REQUIRE(strict.has_value());
    CHECK(strict->filters().get<"published_null">() == std::optional<bool>(false));

    auto bad = decl::parseListQueryStrict<GenDesc>(Params{{"published_null", "maybe"}});
    REQUIRE_FALSE(bad.has_value());
    CHECK(bad.error().type == decl::QueryValidationError::Type::InvalidValue);
    CHECK(bad.error().field == "published_null");
}

TEST_CASE("IS_NULL through the query builder", "[list][is_null][unit][builder]") {
    auto isNull = decl::ListQueryBuilder<GenDesc>{}.filter<"published_null">(true).build();
    auto notNull = decl::ListQueryBuilder<GenDesc>{}.filter<"published_null">(false).build();
    auto open = decl::ListQueryBuilder<GenDesc>{}.build();

    CHECK(isNull.filters().get<"published_null">() == std::optional<bool>(true));
    CHECK(notNull.filters().get<"published_null">() == std::optional<bool>(false));
    CHECK_FALSE(open.filters().get<"published_null">().has_value());

    // Three distinct groups, and HTTP lands on the same one as the builder.
    CHECK(isNull.groupKey() != notNull.groupKey());
    CHECK(isNull.groupKey() != open.groupKey());
    CHECK(notNull.groupKey() != open.groupKey());
    CHECK(decl::parseListQuery<GenDesc>(Params{{"published_null", "yes"}}).groupKey()
          == isNull.groupKey());
}
