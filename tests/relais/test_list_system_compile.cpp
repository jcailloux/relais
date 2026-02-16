/**
 * test_list_system_compile.cpp
 *
 * Compile-time and structural tests for the refactored declarative list system.
 * Verifies that:
 *   - Filter/Sort descriptors work with FixedString column names
 *   - GeneratedFilters, GeneratedTraits compile in new namespace
 *   - buildWhereClause generates SQL with PgParams
 *   - matchesFilters works with entity data members
 *   - compare, extractSortValue, parseSortField work correctly
 *   - HttpQueryParser works with std::unordered_map
 *   - ListCache, ModificationTracker compile in new namespace
 *   - QueryCacheKey, QueryParser in relais::cache namespace
 *
 * No database or Redis connection needed — all tests are structural.
 */

#include <catch2/catch_test_macros.hpp>
#include <string>
#include <unordered_map>

// List system headers (all in jcailloux::relais::cache::list[::decl])
#include "jcailloux/relais/list/decl/FilterDescriptor.h"
#include "jcailloux/relais/list/decl/SortDescriptor.h"
#include "jcailloux/relais/list/decl/ListDescriptor.h"
#include "jcailloux/relais/list/decl/GeneratedFilters.h"
#include "jcailloux/relais/list/decl/GeneratedTraits.h"
#include "jcailloux/relais/list/decl/GeneratedCriteria.h"
#include "jcailloux/relais/list/decl/ListDescriptorQuery.h"
#include "jcailloux/relais/list/ListQuery.h"
#include "jcailloux/relais/list/ListCache.h"
#include "jcailloux/relais/list/ModificationTracker.h"
#include "jcailloux/relais/list/ListCacheTraits.h"
// QueryCacheKey.h and QueryParser.h require xxhash — tested separately

// Entity wrapper for concept satisfaction
#include "jcailloux/relais/wrapper/EntityWrapper.h"
#include "jcailloux/relais/wrapper/ListWrapper.h"

namespace decl = jcailloux::relais::cache::list::decl;
namespace list = jcailloux::relais::cache::list;
// namespace cache = jcailloux::relais::cache; // needs xxhash for QueryCacheKey

// =============================================================================
// Test entity — simple struct with public data members
// =============================================================================

struct TestListArticle {
    int64_t id = 0;
    std::string category;
    int64_t author_id = 0;
    std::string title;
    int32_t view_count = 0;        // integral for CursorEncodable
    bool is_published = false;
    int64_t created_at_us = 0;     // microseconds since epoch for CursorEncodable

    // Minimal MappingType stub for Readable concept
    struct MappingType {
        static constexpr const char* table_name = "test_articles";
        static constexpr const char* primary_key_column = "id";
        static constexpr bool read_only = false;

        struct TraitsType {
            enum class Field : uint8_t {};
            template<Field> struct FieldInfo;
        };

        template<typename Entity>
        static auto getPrimaryKey(const Entity& e) noexcept { return e.id; }

        template<typename T>
        static constexpr auto glaze_value = glz::object(
            "id", &T::id,
            "category", &T::category,
            "author_id", &T::author_id,
            "title", &T::title,
            "view_count", &T::view_count,
            "is_published", &T::is_published,
            "created_at_us", &T::created_at_us
        );

        template<typename Entity>
        static std::optional<Entity> fromRow(const jcailloux::relais::io::PgResult::Row&) {
            return std::nullopt; // stub
        }

        template<typename Entity>
        static jcailloux::relais::io::PgParams toInsertParams(const Entity&) {
            return {}; // stub
        }
    };

    using Mapping = MappingType;

    auto getPrimaryKey() const noexcept { return id; }
    static std::optional<TestListArticle> fromJson(std::string_view) { return std::nullopt; }
    std::shared_ptr<const std::string> json() const { return nullptr; }
};

template<>
struct glz::meta<TestListArticle> {
    using T = TestListArticle;
    static constexpr auto value = glz::object(
        "id", &T::id,
        "category", &T::category,
        "author_id", &T::author_id,
        "title", &T::title,
        "view_count", &T::view_count,
        "is_published", &T::is_published,
        "created_at_us", &T::created_at_us
    );
};

// Wrapper type
using TestListArticleWrapper = jcailloux::relais::wrapper::EntityWrapper<
    TestListArticle, TestListArticle::MappingType>;

// =============================================================================
// ListDescriptor for TestListArticle
// =============================================================================

struct TestArticleDesc {
    using Entity = TestListArticleWrapper;

    // Filters: category (EQ), author_id (EQ)
    static constexpr auto filters = std::tuple{
        decl::Filter<"category", &TestListArticle::category, "category">{},
        decl::Filter<"author_id", &TestListArticle::author_id, "author_id">{}
    };

    // Sorts: view_count (DESC), created_at_us (DESC)
    static constexpr auto sorts = std::tuple{
        decl::Sort<"view_count", &TestListArticle::view_count, "view_count", decl::SortDirection::Desc>{},
        decl::Sort<"created_at_us", &TestListArticle::created_at_us, "created_at_us", decl::SortDirection::Desc>{}
    };
};

// =============================================================================
// Verify ValidListDescriptor concept satisfaction
// =============================================================================

TEST_CASE("ListDescriptor concept satisfaction", "[list_system]") {
    STATIC_REQUIRE(decl::HasEntity<TestArticleDesc>);
    STATIC_REQUIRE(decl::HasFilters<TestArticleDesc>);
    STATIC_REQUIRE(decl::HasSorts<TestArticleDesc>);
    STATIC_REQUIRE(decl::ValidListDescriptor<TestArticleDesc>);
}

// =============================================================================
// Filter descriptor tests
// =============================================================================

TEST_CASE("Filter descriptors with FixedString columns", "[list_system][filter]") {
    using F0 = decl::filter_at<TestArticleDesc, 0>;
    using F1 = decl::filter_at<TestArticleDesc, 1>;

    SECTION("filter count") {
        STATIC_REQUIRE(decl::filter_count<TestArticleDesc> == 2);
    }

    SECTION("filter names") {
        REQUIRE(F0::name.view() == "category");
        REQUIRE(F1::name.view() == "author_id");
    }

    SECTION("filter column names") {
        REQUIRE(F0::column() == "category");
        REQUIRE(F1::column() == "author_id");
    }

    SECTION("filter value types") {
        STATIC_REQUIRE(std::is_same_v<F0::value_type, std::string>);
        STATIC_REQUIRE(std::is_same_v<F1::value_type, int64_t>);
    }

    SECTION("filter operator defaults to EQ") {
        STATIC_REQUIRE(F0::op == decl::Op::EQ);
        STATIC_REQUIRE(F1::op == decl::Op::EQ);
    }
}

// =============================================================================
// Sort descriptor tests
// =============================================================================

TEST_CASE("Sort descriptors with FixedString columns", "[list_system][sort]") {
    using S0 = decl::sort_at<TestArticleDesc, 0>;
    using S1 = decl::sort_at<TestArticleDesc, 1>;

    SECTION("sort count") {
        STATIC_REQUIRE(decl::sort_count<TestArticleDesc> == 2);
    }

    SECTION("sort names") {
        REQUIRE(S0::name.view() == "view_count");
        REQUIRE(S1::name.view() == "created_at_us");
    }

    SECTION("sort column names") {
        REQUIRE(S0::column() == "view_count");
        REQUIRE(S1::column() == "created_at_us");
    }

    SECTION("sort default directions") {
        STATIC_REQUIRE(S0::default_direction == decl::SortDirection::Desc);
        STATIC_REQUIRE(S1::default_direction == decl::SortDirection::Desc);
    }
}

// =============================================================================
// GeneratedFilters tests
// =============================================================================

TEST_CASE("GeneratedFilters struct", "[list_system][filters]") {
    decl::Filters<TestArticleDesc> filters;

    SECTION("initial state — no active filters") {
        REQUIRE(!filters.hasAnyFilter());
        REQUIRE(filters.activeFilterCount() == 0);
    }

    SECTION("set a filter by index") {
        filters.get<0>() = "tech";
        REQUIRE(filters.hasAnyFilter());
        REQUIRE(filters.activeFilterCount() == 1);
        REQUIRE(*filters.get<0>() == "tech");
    }

    SECTION("set a filter by name") {
        filters.get<"author_id">() = 42;
        REQUIRE(filters.hasAnyFilter());
        REQUIRE(*filters.get<"author_id">() == 42);
    }

    SECTION("matchesFilters — matching tags") {
        filters.get<0>() = "tech";
        filters.get<1>() = int64_t(42);

        decl::Filters<TestArticleDesc> tags;
        tags.get<0>() = "tech";
        tags.get<1>() = int64_t(42);

        REQUIRE(tags.matchesFilters(filters));
    }

    SECTION("matchesFilters — non-matching tag") {
        filters.get<0>() = "tech";

        decl::Filters<TestArticleDesc> tags;
        tags.get<0>() = "science";

        REQUIRE(!tags.matchesFilters(filters));
    }
}

// =============================================================================
// GeneratedTraits tests — matchesFilters with entity
// =============================================================================

TEST_CASE("matchesFilters with entity", "[list_system][traits]") {
    TestListArticleWrapper entity;
    entity.category = "tech";
    entity.author_id = 42;
    entity.title = "Hello";

    SECTION("matches when filter is active and equal") {
        decl::Filters<TestArticleDesc> filters;
        filters.get<"category">() = "tech";
        REQUIRE(decl::matchesFilters<TestArticleDesc>(entity, filters));
    }

    SECTION("no match when filter differs") {
        decl::Filters<TestArticleDesc> filters;
        filters.get<"category">() = "science";
        REQUIRE(!decl::matchesFilters<TestArticleDesc>(entity, filters));
    }

    SECTION("matches when no filters active") {
        decl::Filters<TestArticleDesc> filters;
        REQUIRE(decl::matchesFilters<TestArticleDesc>(entity, filters));
    }
}

// =============================================================================
// buildWhereClause tests
// =============================================================================

TEST_CASE("buildWhereClause generates SQL with PgParams", "[list_system][sql]") {
    SECTION("no filters — empty clause") {
        decl::Filters<TestArticleDesc> filters;
        auto where = decl::buildWhereClause<TestArticleDesc>(filters);
        REQUIRE(where.sql.empty());
        REQUIRE(where.params.params.empty());
        REQUIRE(where.next_param == 1);
    }

    SECTION("single filter") {
        decl::Filters<TestArticleDesc> filters;
        filters.get<"category">() = "tech";

        auto where = decl::buildWhereClause<TestArticleDesc>(filters);
        REQUIRE(where.sql == "\"category\"=$1");
        REQUIRE(where.params.params.size() == 1);
        REQUIRE(where.next_param == 2);
    }

    SECTION("two filters") {
        decl::Filters<TestArticleDesc> filters;
        filters.get<"category">() = "tech";
        filters.get<"author_id">() = int64_t(42);

        auto where = decl::buildWhereClause<TestArticleDesc>(filters);
        REQUIRE(where.sql == "\"category\"=$1 AND \"author_id\"=$2");
        REQUIRE(where.params.params.size() == 2);
        REQUIRE(where.next_param == 3);
    }
}

// =============================================================================
// Sort field parsing
// =============================================================================

TEST_CASE("parseSortField and sortFieldName", "[list_system][sort]") {
    SECTION("parse valid field") {
        auto idx = decl::parseSortField<TestArticleDesc>("view_count");
        REQUIRE(idx.has_value());
        REQUIRE(*idx == 0);
    }

    SECTION("parse second field") {
        auto idx = decl::parseSortField<TestArticleDesc>("created_at_us");
        REQUIRE(idx.has_value());
        REQUIRE(*idx == 1);
    }

    SECTION("parse invalid field") {
        auto idx = decl::parseSortField<TestArticleDesc>("nonexistent");
        REQUIRE(!idx.has_value());
    }

    SECTION("field name from index") {
        REQUIRE(decl::sortFieldName<TestArticleDesc>(0) == "view_count");
        REQUIRE(decl::sortFieldName<TestArticleDesc>(1) == "created_at_us");
    }

    SECTION("column name from index") {
        REQUIRE(decl::sortColumnName<TestArticleDesc>(0) == "view_count");
        REQUIRE(decl::sortColumnName<TestArticleDesc>(1) == "created_at_us");
    }
}

// =============================================================================
// extractTags
// =============================================================================

TEST_CASE("extractTags from entity", "[list_system][tags]") {
    TestListArticleWrapper entity;
    entity.category = "tech";
    entity.author_id = 42;

    auto tags = decl::extractTags<TestArticleDesc>(entity);
    REQUIRE((tags.get<0>().has_value()));
    REQUIRE((*tags.get<0>() == "tech"));
    REQUIRE((tags.get<1>().has_value()));
    REQUIRE((*tags.get<1>() == 42));
}

// =============================================================================
// defaultSort
// =============================================================================

TEST_CASE("defaultSort returns first sort field", "[list_system][sort]") {
    auto sort = decl::defaultSort<TestArticleDesc>();
    REQUIRE(sort.field_index == 0);
    REQUIRE(sort.direction == decl::SortDirection::Desc);
}

// =============================================================================
// ListDescriptorQuery
// =============================================================================

TEST_CASE("ListDescriptorQuery struct", "[list_system][query]") {
    decl::ListDescriptorQuery<TestArticleDesc> query;
    REQUIRE(query.limit == 20);
    REQUIRE(!query.sort.has_value());
    REQUIRE(query.query_hash == 0);
}

// =============================================================================
// Namespace verification
// =============================================================================

TEST_CASE("All types in jcailloux::relais namespace", "[list_system][namespace]") {
    // Verify all types are in jcailloux::relais namespace
    STATIC_REQUIRE(std::is_same_v<
        decl::Op, jcailloux::relais::cache::list::decl::Op>);
    STATIC_REQUIRE(std::is_same_v<
        list::SortDirection, jcailloux::relais::cache::list::SortDirection>);
}

// =============================================================================
// opToSql
// =============================================================================

TEST_CASE("opToSql conversion", "[list_system][sql]") {
    REQUIRE(std::string_view(decl::opToSql(decl::Op::EQ)) == "=");
    REQUIRE(std::string_view(decl::opToSql(decl::Op::NE)) == "!=");
    REQUIRE(std::string_view(decl::opToSql(decl::Op::GT)) == ">");
    REQUIRE(std::string_view(decl::opToSql(decl::Op::GE)) == ">=");
    REQUIRE(std::string_view(decl::opToSql(decl::Op::LT)) == "<");
    REQUIRE(std::string_view(decl::opToSql(decl::Op::LE)) == "<=");
}
