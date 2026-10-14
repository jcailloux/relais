/**
 * test_count_where.cpp
 *
 * Repo::countWhere against PostgreSQL: every filter operator compared to a
 * hand-written count(*), SQL three-valued logic on a nullable column, the
 * empty IN set against query(), and read-your-writes on every preset.
 *
 * Fixture TestFilterOnly declares filters without a list (a table that is
 * counted, never paged), one filter per operator; `label` is nullable.
 * Timeout, dead backend and coalescing live in test_cache_coherence.
 */

#include <catch2/catch_template_test_macros.hpp>
#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "fixtures/test_helper.h"
#include "fixtures/TestRepositories.h"
#include "fixtures/generated/TestArticleInEntity.h"
#include "fixtures/generated/TestFilterOnlyEntity.h"

using namespace relais_test;
using jcailloux::relais::Repo;
using FilterOnly = entity::generated::TestFilterOnlyEntity;
using Pred = FilterOnly::MappingType::FilterSet::Values;

namespace {

using CountUncached = Repo<FilterOnly, "test:count:uncached">;
using CountL1 = Repo<FilterOnly, "test:count:l1", cfg::Local>;
using ArticleInRepo = Repo<entity::generated::TestArticleInEntity, "test:count:article_in">;

void seed(const std::string& category, int64_t owner, int32_t score,
          std::optional<std::string> label) {
    if (label)
        execQueryArgs("INSERT INTO relais_test_filter_only (category, owner_id, score, label) "
                      "VALUES ($1, $2, $3, $4)", category, owner, score, *label);
    else
        execQueryArgs("INSERT INTO relais_test_filter_only (category, owner_id, score) "
                      "VALUES ($1, $2, $3)", category, owner, score);
}

size_t truth(const std::string& where) {
    auto sql = "SELECT count(*) FROM relais_test_filter_only WHERE " + where;
    return static_cast<size_t>(execQuery(sql.c_str())[0].get<int64_t>(0));
}

FilterOnly row(std::string category, int64_t owner, int32_t score,
               std::optional<std::string> label) {
    FilterOnly e;
    e.category = std::move(category);
    e.owner_id = owner;
    e.score = score;
    e.label = std::move(label);
    return e;
}

}  // namespace

TEST_CASE("[CountWhere] empty table, then matching and non-matching rows",
          "[integration][db][where][count]") {
    TransactionGuard tx;
    CHECK(sync(CountUncached::countWhere({})) == 0);
    CHECK(sync(CountUncached::countWhere({.category = "a"})) == 0);

    for (int i = 0; i < 3; ++i) seed("a", 1, i, std::nullopt);
    for (int i = 0; i < 2; ++i) seed("b", 1, i, std::nullopt);
    CHECK(sync(CountUncached::countWhere({.category = "a"})) == 3);
    CHECK(sync(CountUncached::countWhere({})) == 5);
}

TEST_CASE("[CountWhere] every operator matches a hand-written count(*)",
          "[integration][db][where][count]") {
    TransactionGuard tx;
    seed("a", 1, 1, "x");
    seed("a", 1, 2, std::nullopt);
    seed("b", 1, 3, "y");
    seed("b", 2, 4, std::nullopt);
    seed("c", 2, 5, "x");

    auto count = [](Pred p) { return sync(CountUncached::countWhere(std::move(p))); };

    CHECK(count({.category = "a"}) == truth("category = 'a'"));
    CHECK(count({.category_in = std::vector<std::string>{"a", "c"}})
          == truth("category IN ('a', 'c')"));
    CHECK(count({.owner_id = 2}) == truth("owner_id = 2"));
    CHECK(count({.score_ge = 2}) == truth("score >= 2"));
    CHECK(count({.score_gt = 2}) == truth("score > 2"));
    CHECK(count({.score_le = 2}) == truth("score <= 2"));
    CHECK(count({.score_lt = 2}) == truth("score < 2"));
    CHECK(count({.label_null = true}) == truth("label IS NULL"));
    CHECK(count({.label_null = false}) == truth("label IS NOT NULL"));
    CHECK(count({.category = "b", .owner_id = 1, .score_ge = 3})
          == truth("category = 'b' AND owner_id = 1 AND score >= 3"));
    CHECK(count({.category_in = std::vector<std::string>{"a", "b"},
                 .label_null = true, .score_lt = 4})
          == truth("category IN ('a', 'b') AND label IS NULL AND score < 4"));

    // The hand-written counts are not all equal: each operator is discriminated.
    CHECK(count({.score_ge = 2}) == 4);
    CHECK(count({.score_gt = 2}) == 3);
    CHECK(count({.label_null = true}) == 2);
}

TEST_CASE("[CountWhere] NE and NIN follow SQL three-valued logic on NULL rows",
          "[integration][db][where][count]") {
    TransactionGuard tx;
    seed("a", 1, 1, "x");
    seed("a", 1, 2, "y");
    seed("a", 1, 3, std::nullopt);

    CHECK(sync(CountUncached::countWhere({.label_ne = "x"})) == 1);
    CHECK(sync(CountUncached::countWhere({.label_ne = "x"})) == truth("label <> 'x'"));
    CHECK(sync(CountUncached::countWhere({.label_nin = std::vector<std::string>{"x"}})) == 1);
    CHECK(sync(CountUncached::countWhere({.label_nin = std::vector<std::string>{"x"}}))
          == truth("label NOT IN ('x')"));
    // An empty NIN set is vacuously true, NULL row included.
    CHECK(sync(CountUncached::countWhere({.label_nin = std::vector<std::string>{}})) == 3);
    CHECK(sync(CountUncached::countWhere({.label_nin = std::vector<std::string>{}}))
          == truth("label <> ALL('{}'::text[])"));
}

TEST_CASE("[CountWhere] an empty IN set counts what query() lists",
          "[integration][db][where][count]") {
    TransactionGuard tx;
    auto author = insertTestUser("count_in", "count_in@x", 0);
    insertTestArticle("tech", author, "a");
    insertTestArticle("news", author, "b");

    auto page = sync(ArticleInRepo::query(ArticleInRepo::queryBuilder()
        .filter<"category">(std::vector<std::string>{}).limit(10).build()));
    const auto counted = sync(ArticleInRepo::countWhere({.category = std::vector<std::string>{}}));
    CHECK(counted == page->size());
    CHECK(counted == 0);
}

TEMPLATE_TEST_CASE("[CountWhere] reads its own writes on every preset",
                   "[integration][db][where][count]",
                   CountUncached, CountL1) {
    using R = TestType;
    TransactionGuard tx;
    const Pred unlabelled{.label_null = true, .owner_id = 9};

    CHECK(sync(R::countWhere(unlabelled)) == 0);

    auto a = sync(R::insert(row("a", 9, 1, std::nullopt)));
    REQUIRE(a);
    CHECK(sync(R::countWhere(unlabelled)) == 1);

    auto b = sync(R::insert(row("b", 9, 2, std::nullopt)));
    REQUIRE(b);
    CHECK(sync(R::countWhere(unlabelled)) == 2);

    REQUIRE(sync(R::update(b->id, row("b", 9, 2, "z"))));
    CHECK(sync(R::countWhere(unlabelled)) == 1);
    CHECK(sync(R::countWhere({.label_null = false, .owner_id = 9})) == 1);

    REQUIRE(sync(R::erase(a->id)));
    CHECK(sync(R::countWhere(unlabelled)) == 0);
    CHECK(sync(R::countWhere({.owner_id = 9})) == 1);
}
