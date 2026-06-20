/**
 * test_erase_batch.cpp
 * Batched / predicate erase at L3 (PgRepo::eraseManyRaw / eraseWhereRaw).
 *
 * Étape 2 du plan erase/invalidate batch :
 *   - eraseManyRaw(span<Key>)  → delete_by_pk_batch (WHERE pk=ANY RETURNING *),
 *     one statement = deletion + the deleted entities (for downstream inval).
 *   - eraseWhereRaw<Descriptor>(filters) → ctid-bounded DELETE looped at K_pg,
 *     returns every deleted entity across chunks.
 *
 * Raw methods are protected (like findManyRaw) — reached via TestInternals on
 * Uncached* repos so we exercise the pure L3 path (no L1/L2 interference).
 *
 * SECTION tags:
 *   [erasemany]      — eraseManyRaw by enumerated keys
 *   [erasewhere]     — eraseWhereRaw by predicate
 *   [pg]             — pure L3
 *   [partition-key]  — cross-partition pk=ANY
 *   [chunking]       — real > K_pg loop convergence
 */

#include <optional>
#include <span>
#include <string>
#include <vector>

#include <catch2/catch_test_macros.hpp>

#include "fixtures/test_helper.h"
#include "fixtures/TestRepositories.h"
#include "fixtures/RelaisTestAccessors.h"

#include <jcailloux/relais/list/spec/GeneratedCriteria.h>

using namespace relais_test;
namespace decl = jcailloux::relais::list::spec;

namespace {

template<typename Repo, typename Key>
std::vector<typename Repo::EntityType> eraseManyRawSync(std::span<const Key> ids) {
    return sync(TestInternals::eraseManyRaw<Repo>(ids));
}

// COUNT(*) over a table with an optional predicate appended (literal — test-only).
int64_t countRows(const std::string& sql) {
    auto r = execQuery(sql.c_str());
    return r[0].get<int64_t>(0);
}

bool userExists(int64_t id) {
    return countRows("SELECT COUNT(*) FROM relais_test_users WHERE id = "
                     + std::to_string(id)) == 1;
}

}  // namespace

// ---------------------------------------------------------------------------
// eraseManyRaw — scalar key
// ---------------------------------------------------------------------------

TEST_CASE("eraseManyRaw scalar: deletes the enumerated set, returns the rows",
          "[erasemany][pg]") {
    TransactionGuard guard;

    auto u1 = insertTestUser("em_u1", "em_u1@x", 10);
    auto u2 = insertTestUser("em_u2", "em_u2@x", 20);
    auto u3 = insertTestUser("em_u3", "em_u3@x", 30);

    SECTION("subset deleted, returned entities carry the deleted rows") {
        std::vector<int64_t> ids = {u3, u1};
        auto out = eraseManyRawSync<UncachedTestUserRepo, int64_t>(ids);

        // RETURNING brings back exactly the deleted rows (order is PG's, so match
        // by content). u2 is untouched.
        REQUIRE(out.size() == 2);
        bool saw_u1 = false, saw_u3 = false;
        for (const auto& e : out) {
            if (e.username == "em_u1") { saw_u1 = true; REQUIRE(e.balance == 10); }
            if (e.username == "em_u3") { saw_u3 = true; REQUIRE(e.balance == 30); }
        }
        REQUIRE(saw_u1);
        REQUIRE(saw_u3);

        // Gone from the table; the unlisted row survives.
        REQUIRE(userExists(u2));
        REQUIRE_FALSE(userExists(u1));
        REQUIRE_FALSE(userExists(u3));
    }

    SECTION("absent ids contribute nothing (size <= ids.size())") {
        std::vector<int64_t> ids = {u1, -999, u2};  // -999 never present
        auto out = eraseManyRawSync<UncachedTestUserRepo, int64_t>(ids);
        REQUIRE(out.size() == 2);  // only u1, u2 existed
        REQUIRE_FALSE(userExists(u1));
        REQUIRE_FALSE(userExists(u2));
        REQUIRE(userExists(u3));
    }

    SECTION("empty ids → empty result, no query, no deletion") {
        std::vector<int64_t> ids;
        auto out = eraseManyRawSync<UncachedTestUserRepo, int64_t>(ids);
        REQUIRE(out.empty());
        REQUIRE(userExists(u1));  // untouched
    }
}

// ---------------------------------------------------------------------------
// eraseManyRaw — partition key (pk=ANY scans every partition, no pruning)
// ---------------------------------------------------------------------------

TEST_CASE("eraseManyRaw partition key: one ANY deletes across partitions",
          "[erasemany][partition-key]") {
    TransactionGuard guard;

    auto uid = insertTestUser("em_evt", "em_evt@x", 0);
    auto e_eu = insertTestEvent("eu", uid, "euro", 1);
    auto e_us = insertTestEvent("us", uid, "yankee", 2);

    std::vector<int64_t> ids = {e_us, e_eu};  // live in different partitions
    auto out = eraseManyRawSync<UncachedTestEventRepo, int64_t>(ids);

    REQUIRE(out.size() == 2);
    // RETURNING carries the partition column → the L1 single-partition evict hint
    // is derivable by upper layers.
    bool saw_eu = false, saw_us = false;
    for (const auto& e : out) {
        if (e.region == "eu") { saw_eu = true; REQUIRE(e.title == "euro"); }
        if (e.region == "us") { saw_us = true; REQUIRE(e.title == "yankee"); }
    }
    REQUIRE(saw_eu);
    REQUIRE(saw_us);
}

// ---------------------------------------------------------------------------
// eraseWhereRaw — predicate erase (single chunk)
// ---------------------------------------------------------------------------

TEST_CASE("eraseWhereRaw: selective predicate deletes only the matched rows",
          "[erasewhere][pg]") {
    TransactionGuard guard;
    using Desc = UncachedTestArticleRepo::ListDescriptorType;

    auto author = insertTestUser("ew_author", "ew@x", 0);
    auto other = insertTestUser("ew_other", "ew2@x", 0);
    insertTestArticle("tech", author, "A1", 10);
    insertTestArticle("news", author, "A2", 20);
    insertTestArticle("tech", author, "A3", 30);
    insertTestArticle("tech", other, "B1", 40);
    insertTestArticle("tech", other, "B2", 50);

    SECTION("author_id EQ deletes exactly that author's rows, returns them") {
        decl::Filters<Desc> f;
        f.template get<0>() = author;  // index 0 = author_id (filterable EQ)

        auto out = sync(TestInternals::eraseWhereRaw<UncachedTestArticleRepo, Desc>(f));
        REQUIRE(out.size() == 3);
        for (const auto& a : out) REQUIRE(a.author_id == author);

        // Only the other author's two rows remain.
        REQUIRE(countRows(
            "SELECT COUNT(*) FROM relais_test_articles WHERE author_id = "
            + std::to_string(author)) == 0);
        REQUIRE(countRows(
            "SELECT COUNT(*) FROM relais_test_articles WHERE author_id = "
            + std::to_string(other)) == 2);
    }

    SECTION("no match → empty result, nothing deleted") {
        decl::Filters<Desc> f;
        f.template get<0>() = int64_t{-12345};  // no such author

        auto out = sync(TestInternals::eraseWhereRaw<UncachedTestArticleRepo, Desc>(f));
        REQUIRE(out.empty());
        REQUIRE(countRows("SELECT COUNT(*) FROM relais_test_articles WHERE author_id IN ("
            + std::to_string(author) + "," + std::to_string(other) + ")") == 5);
    }

    SECTION("empty filters → unconditional purge of the table") {
        decl::Filters<Desc> f;  // no active filter → no inner WHERE
        auto out = sync(TestInternals::eraseWhereRaw<UncachedTestArticleRepo, Desc>(f));
        REQUIRE(out.size() == 5);
        REQUIRE(countRows("SELECT COUNT(*) FROM relais_test_articles") == 0);
    }
}

// ---------------------------------------------------------------------------
// eraseWhereRaw — real loop convergence over > K_pg rows (§5 chunking)
// ---------------------------------------------------------------------------

TEST_CASE("eraseWhereRaw converges past K_pg in bounded chunks",
          "[erasewhere][chunking]") {
    TransactionGuard guard;
    using Desc = UncachedTestArticleRepo::ListDescriptorType;

    // K_pg = 10000. Seed 10001 matching rows in one statement so the loop must
    // run twice (10000 then 1) before a short chunk ends it. author_id = 777 is
    // the selective predicate; one decoy row under another author must survive.
    auto decoy = insertTestUser("ew_decoy", "decoy@x", 0);
    execQuery(
        "INSERT INTO relais_test_articles (category, author_id, title, view_count, is_published) "
        "SELECT 'bulk', 777, 'b' || g, 0, false FROM generate_series(1, 10001) g");
    insertTestArticle("keep", decoy, "survivor", 1);

    decl::Filters<Desc> f;
    f.template get<0>() = int64_t{777};

    auto out = sync(TestInternals::eraseWhereRaw<UncachedTestArticleRepo, Desc>(f));

    // Every matching row comes back across the two RETURNING chunks.
    REQUIRE(out.size() == 10001);
    for (const auto& a : out) REQUIRE(a.author_id == 777);

    // Predicate fully drained; the non-matching decoy survives.
    REQUIRE(countRows(
        "SELECT COUNT(*) FROM relais_test_articles WHERE author_id = 777") == 0);
    REQUIRE(countRows(
        "SELECT COUNT(*) FROM relais_test_articles WHERE author_id = "
        + std::to_string(decoy)) == 1);
}
