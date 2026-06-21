/**
 * test_gen_list_nin.cpp
 * Generator support for the NIN filter operator (plan §8.7, §6).
 *
 * Proves the Python entity generator emits `Op::NIN` from `@relais` annotations,
 * in BOTH forms, and that the generated embedded ListDescriptor is functionally
 * identical to a hand-written one:
 *   - short form  `// @relais filterable:nin`            -> Op::NIN, param = field name
 *   - long  form  `// @relais filterable:authors:not_in`  -> Op::NIN, custom param
 * The `not_in` alias normalizes to nin in _parse_filterable_tag. The fixture
 * TestArticleNin places those two NIN filters next to a bare EQ (is_published)
 * and a range GE (views_min), so the static_asserts also guard against the
 * KNOWN_OPS change perturbing the other operators.
 *
 * No DB / Redis: the generated descriptor is driven straight through the free
 * helpers (parseListQuery -> buildWhereClause -> matchesFilters), the same path
 * the mixin chain uses. The NIN-specific verdict (anti-membership + empty-set
 * universe) is asserted; cross-tier agreement lives in test_decl_list_not_in.
 */

#include <catch2/catch_test_macros.hpp>

#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include "fixtures/generated/TestArticleNinEntity.h"
#include <jcailloux/relais/list/spec/GeneratedCriteria.h>
#include <jcailloux/relais/list/spec/GeneratedTraits.h>
#include <jcailloux/relais/list/spec/HttpQueryParser.h>
#include <jcailloux/relais/list/spec/SortDescriptor.h>

namespace decl = jcailloux::relais::list::spec;
using GenEntity = entity::generated::TestArticleNinEntity;

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
using F0 = std::tuple_element_t<0, FilterTuple>;  // authors      (long  form :authors:not_in)
using F1 = std::tuple_element_t<1, FilterTuple>;  // category     (short form :nin)
using F2 = std::tuple_element_t<2, FilterTuple>;  // is_published (bare filterable -> EQ)
using F3 = std::tuple_element_t<3, FilterTuple>;  // views_min    (:views_min:ge -> GE)

}  // namespace

// --- Compile-time: the generator emits the right Op for each annotation form --
static_assert(F0::op == decl::Op::NIN, "long form `filterable:authors:not_in` -> Op::NIN");
static_assert(F1::op == decl::Op::NIN, "short form `filterable:nin` -> Op::NIN");
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

size_t countNotAll(std::string_view sql) {
    size_t n = 0;
    for (size_t p = sql.find("!= ALL"); p != std::string_view::npos;
         p = sql.find("!= ALL", p + 1))
        ++n;
    return n;
}

}  // namespace

TEST_CASE("generator emits a functional NIN descriptor (both forms)", "[list][nin][gen]") {
    // HTTP parse: long-form param "authors" (int64 set) + short-form "category"
    // (string set). parseInList sorts+dedups, so order/duplicates canonicalize.
    std::unordered_map<std::string, std::string> params{
        {"authors", "2,1,2"},
        {"category", "tech,science,tech"},
    };
    auto q = decl::parseListQuery<GenDesc>(params);

    REQUIRE(q.filters().template get<0>().has_value());
    REQUIRE(q.filters().template get<1>().has_value());
    CHECK(*q.filters().template get<0>() == std::vector<int64_t>{1, 2});
    CHECK(*q.filters().template get<1>()
          == std::vector<std::string>{"science", "tech"});

    SECTION("L3: both NIN filters compile to `!= ALL`") {
        auto wc = decl::buildWhereClause<GenDesc>(q.filters());
        CHECK(countNotAll(wc.sql) == 2);
        // Two NIN sets => exactly two array params, no $n drift.
        CHECK(wc.params.params.size() == 2);
    }

    SECTION("L1: matchesFilters honors the generated Op::NIN (anti-membership)") {
        // author ∉ {1,2} AND category ∉ {science,tech} -> matches.
        CHECK(decl::matchesFilters<GenDesc>(makeEntity(9, "news"), q.filters()));
        // author 1 ∈ {1,2} -> excluded.
        CHECK_FALSE(decl::matchesFilters<GenDesc>(makeEntity(1, "news"), q.filters()));
        // category "tech" ∈ {science,tech} -> excluded.
        CHECK_FALSE(decl::matchesFilters<GenDesc>(makeEntity(9, "tech"), q.filters()));
    }
}

TEST_CASE("generator NIN empty-set is the universe", "[list][nin][gen]") {
    // NOT IN {} = everything matches (§1.2). An empty set is the identity for
    // anti-membership: SQL `!= ALL('{}')` is TRUE, L1 find on empty -> end().
    decl::Filters<GenDesc> f;
    f.template get<0>() = std::vector<int64_t>{};  // authors NOT IN {}

    CHECK(decl::matchesFilters<GenDesc>(makeEntity(1, "x"), f));
    CHECK(decl::matchesFilters<GenDesc>(makeEntity(999, "y"), f));

    auto wc = decl::buildWhereClause<GenDesc>(f);
    CHECK(countNotAll(wc.sql) == 1);
    CHECK(wc.params.params.size() == 1);
}

TEST_CASE("generator NIN coexists with EQ and range neighbors", "[list][nin][gen]") {
    // authors NOT IN {1} AND is_published = true AND views_min >= 10.
    // bool has no HTTP parser (parseValue<bool> -> nullopt), so the EQ filter is
    // set programmatically; this still drives the generated descriptor's mixed
    // NIN/EQ/GE tuple through matchesFilters + buildWhereClause, proving the NIN
    // filter (index 0) stays aligned with its scalar neighbors.
    decl::Filters<GenDesc> f;
    f.template get<0>() = std::vector<int64_t>{1};  // authors  NOT IN {1}
    f.template get<2>() = true;                     // is_published EQ true
    f.template get<3>() = 10;                        // views_min GE 10

    CHECK(decl::matchesFilters<GenDesc>(makeEntity(7, "x", 20, true), f));
    // author 1 ∈ {1} (NIN, first position — excluded; proves alignment past it holds)
    CHECK_FALSE(decl::matchesFilters<GenDesc>(makeEntity(1, "x", 20, true), f));
    // view_count 5 < 10 (range)
    CHECK_FALSE(decl::matchesFilters<GenDesc>(makeEntity(7, "x", 5, true), f));
    // is_published false (EQ)
    CHECK_FALSE(decl::matchesFilters<GenDesc>(makeEntity(7, "x", 20, false), f));

    auto wc = decl::buildWhereClause<GenDesc>(f);
    CHECK(countNotAll(wc.sql) == 1);       // only the NIN filter uses != ALL
    CHECK(wc.params.params.size() == 3);    // NIN array + EQ bool + GE int
}
