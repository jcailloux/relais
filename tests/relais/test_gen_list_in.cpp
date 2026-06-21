/**
 * test_gen_list_in.cpp
 * Generator support for the IN filter operator (commit 8, plan §6).
 *
 * Proves the Python entity generator emits `Op::IN` from `@relais` annotations,
 * in BOTH forms, and that the generated embedded ListDescriptor is functionally
 * identical to a hand-written one:
 *   - short form  `// @relais filterable:in`        -> Op::IN, param = field name
 *   - long  form  `// @relais filterable:authors:in` -> Op::IN, custom param
 * The fixture TestArticleIn places those two IN filters next to a bare EQ
 * (is_published) and a range GE (views_min), so the static_asserts also guard
 * against the KNOWN_OPS change perturbing the other operators.
 *
 * No DB / Redis: the generated descriptor is driven straight through the free
 * helpers (parseListQuery -> buildWhereClause -> matchesFilters), the same path
 * the mixin chain uses.
 */

#include <catch2/catch_test_macros.hpp>

#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include "fixtures/generated/TestArticleInEntity.h"
#include <jcailloux/relais/list/spec/GeneratedCriteria.h>
#include <jcailloux/relais/list/spec/GeneratedTraits.h>
#include <jcailloux/relais/list/spec/HttpQueryParser.h>
#include <jcailloux/relais/list/spec/SortDescriptor.h>

namespace decl = jcailloux::relais::list::spec;
using GenEntity = entity::generated::TestArticleInEntity;

namespace {

// Reproduce ListMixin's descriptor augmentation (ListMixin.h:57): the embedded
// ListDescriptor carries filters/sorts; the mixin adds the Entity alias the
// free helpers resolve via `typename Descriptor::Entity`.
struct GenDesc : GenEntity::MappingType::ListDescriptor {
    using Entity = GenEntity;
};

// Filters are emitted sorted by HTTP param name (generate_entities.py sorts for
// deterministic cache keys): authors, category, is_published, views_min.
using FilterTuple = std::remove_const_t<decltype(GenDesc::filters)>;
using F0 = std::tuple_element_t<0, FilterTuple>;  // authors      (long  form :authors:in)
using F1 = std::tuple_element_t<1, FilterTuple>;  // category     (short form :in)
using F2 = std::tuple_element_t<2, FilterTuple>;  // is_published (bare filterable -> EQ)
using F3 = std::tuple_element_t<3, FilterTuple>;  // views_min    (:views_min:ge -> GE)

}  // namespace

// --- Compile-time: the generator emits the right Op for each annotation form --
static_assert(F0::op == decl::Op::IN, "long form `filterable:authors:in` -> Op::IN");
static_assert(F1::op == decl::Op::IN, "short form `filterable:in` -> Op::IN");
static_assert(F2::op == decl::Op::EQ, "bare `filterable` stays EQ");
static_assert(F3::op == decl::Op::GE, "`filterable:views_min:ge` stays GE");

static_assert(F0::name.view() == std::string_view{"authors"},
              "long form binds the custom HTTP param name");
static_assert(F1::name.view() == std::string_view{"category"},
              "short form binds the field name as param");
static_assert(F0::column() == std::string_view{"author_id"},
              "long form keeps the field's DB column");
static_assert(F1::column() == std::string_view{"category"});

namespace {

GenEntity makeEntity(int64_t author_id, std::string category,
                     std::optional<int32_t> view_count = std::nullopt,
                     bool is_published = false) {
    GenEntity e;
    e.id = 1;
    e.author_id = author_id;
    e.category = std::move(category);
    e.view_count = view_count;
    e.is_published = is_published;
    return e;
}

size_t countAny(std::string_view sql) {
    size_t n = 0;
    for (size_t p = sql.find("= ANY"); p != std::string_view::npos;
         p = sql.find("= ANY", p + 1))
        ++n;
    return n;
}

}  // namespace

TEST_CASE("generator emits a functional IN descriptor (both forms)", "[list][in][gen]") {
    // HTTP parse: long-form param "authors" (int64 set) + short-form "category"
    // (string set). parseInList sorts+dedups, so order/duplicates canonicalize.
    std::unordered_map<std::string, std::string> params{
        {"authors", "2,1,2"},
        {"category", "tech,science,tech"},
    };
    auto q = decl::parseListQuery<GenDesc>(params);

    REQUIRE(q.filters().template get<"authors">().has_value());
    REQUIRE(q.filters().template get<"category">().has_value());
    CHECK(*q.filters().template get<"authors">() == std::vector<int64_t>{1, 2});
    CHECK(*q.filters().template get<"category">()
          == std::vector<std::string>{"science", "tech"});

    SECTION("L3: both IN filters compile to `= ANY`") {
        auto wc = decl::buildWhereClause<GenDesc>(q.filters());
        CHECK(countAny(wc.sql) == 2);
        // Two IN sets => exactly two array params, no $n drift.
        CHECK(wc.params.params.size() == 2);
    }

    SECTION("L1: matchesFilters honors the generated Op::IN on both fields") {
        CHECK(decl::matchesFilters<GenDesc>(makeEntity(1, "tech"), q.filters()));
        CHECK(decl::matchesFilters<GenDesc>(makeEntity(2, "science"), q.filters()));
        // author_id ∉ {1,2}
        CHECK_FALSE(decl::matchesFilters<GenDesc>(makeEntity(9, "tech"), q.filters()));
        // category ∉ {science,tech}
        CHECK_FALSE(decl::matchesFilters<GenDesc>(makeEntity(1, "news"), q.filters()));
    }
}

TEST_CASE("generator IN coexists with EQ and range neighbors", "[list][in][gen]") {
    // authors IN {1} AND is_published = true AND views_min >= 10.
    // bool has no HTTP parser (parseValue<bool> -> nullopt), so the EQ filter is
    // set programmatically; this still drives the generated descriptor's mixed
    // IN/EQ/GE tuple through matchesFilters + buildWhereClause, proving the IN
    // filter (index 0) stays aligned with its scalar neighbors.
    decl::Filters<GenDesc> f;
    f.template get<"authors">() = std::vector<int64_t>{1};  // authors  IN {1}
    f.template get<"is_published">() = true;                     // is_published EQ true
    f.template get<"views_min">() = 10;                        // views_min GE 10

    CHECK(decl::matchesFilters<GenDesc>(makeEntity(1, "x", 20, true), f));
    // view_count 5 < 10 (range)
    CHECK_FALSE(decl::matchesFilters<GenDesc>(makeEntity(1, "x", 5, true), f));
    // is_published false (EQ)
    CHECK_FALSE(decl::matchesFilters<GenDesc>(makeEntity(1, "x", 20, false), f));
    // author ∉ {1} (IN, first position — proves alignment past it holds)
    CHECK_FALSE(decl::matchesFilters<GenDesc>(makeEntity(7, "x", 20, true), f));

    auto wc = decl::buildWhereClause<GenDesc>(f);
    CHECK(countAny(wc.sql) == 1);          // only the IN filter uses = ANY
    CHECK(wc.params.params.size() == 3);    // IN array + EQ bool + GE int
}
