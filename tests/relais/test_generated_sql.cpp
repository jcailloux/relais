/**
 * test_generated_sql.cpp
 *
 * Verrou sur le SQL statique émis par le générateur (scripts/generate_entities.py).
 * Ici on n'exécute aucune requête : on assert les strings constantes de
 * Mapping::SQL — leur forme, leur numérotation $n, leurs colonnes RETURNING.
 *
 * Cible étape 1 (plan erase/invalidate batch, §1) : delete_by_pk_batch.
 *   - clé scalaire   → WHERE pk = ANY($1) RETURNING <cols>
 *   - clé composite  → WHERE (k1,k2) IN (SELECT unnest($1::t[]), unnest($2::t[])) RETURNING <cols>
 *   - entité partitionnée → pas de partition pruning : pk = ANY($1) scanne toutes
 *     les partitions (choix de design acté — purge batch).
 *
 * SECTION naming :
 *   [SQL] — assertion directe sur une constante Mapping::SQL::*
 */

#include <string>
#include <string_view>

#include <catch2/catch_test_macros.hpp>

#include "fixtures/generated/TestUserEntity.h"
#include "fixtures/generated/TestEventEntity.h"
#include "fixtures/generated/TestCompositeKeyListEntity.h"
#include "fixtures/generated/TestAllPkJunctionEntity.h"
#include "fixtures/generated/TestArticleInEntity.h"

#include <jcailloux/relais/list/spec/GeneratedCriteria.h>
#include <jcailloux/relais/repository/PgRepo.h>

namespace {

using UserSQL = entity::generated::TestUserMapping::SQL;
using EventSQL = entity::generated::TestEventMapping::SQL;
using CompositeSQL = entity::generated::TestCompositeKeyListMapping::SQL;
using JunctionSQL = entity::generated::TestAllPkJunctionMapping::SQL;

namespace decl = jcailloux::relais::list::spec;
namespace detail = jcailloux::relais::detail;

// Mirror ListMixin's descriptor augmentation (cf. test_gen_list_in.cpp): the
// embedded ListDescriptor gains the Entity alias the free helpers resolve.
struct ArticleDesc : entity::generated::TestArticleInEntity::MappingType::ListDescriptor {
    using Entity = entity::generated::TestArticleInEntity;
};

}  // namespace

// #############################################################################
//  delete_by_pk_batch — clé scalaire
// #############################################################################

TEST_CASE("delete_by_pk_batch - scalar key", "[generator][sql][batch]") {
    SECTION("[SQL] WHERE pk = ANY($1) RETURNING all columns") {
        CHECK(std::string(UserSQL::delete_by_pk_batch) ==
              "DELETE FROM relais_test_users WHERE id = ANY($1) "
              "RETURNING id, username, email, balance, created_at");
    }

    SECTION("[SQL] RETURNING projects exactly returning_columns") {
        // Le set affecté est réhydraté en vector<E> ; RETURNING doit ramener
        // toutes les colonnes (= returning_columns), pas un sous-ensemble.
        const std::string sql = UserSQL::delete_by_pk_batch;
        const std::string ret = std::string("RETURNING ") + UserSQL::returning_columns;
        CHECK(sql.ends_with(ret));
    }

    SECTION("[SQL] single bind param — $1 present, $2 absent") {
        const std::string_view sql = UserSQL::delete_by_pk_batch;
        CHECK(sql.find("$1") != std::string_view::npos);
        CHECK(sql.find("$2") == std::string_view::npos);
    }
}

// #############################################################################
//  delete_by_pk_batch — entité partitionnée (pas de pruning)
// #############################################################################

TEST_CASE("delete_by_pk_batch - partitioned entity scans all partitions",
          "[generator][sql][batch][partition]") {
    SECTION("[SQL] pk = ANY($1), no partition key in the WHERE") {
        // Choix de design : la suppression batch par clés énumérées n'émet pas
        // de hint partition — pk=ANY($1) scanne toutes les partitions. La colonne
        // de partition (region) ne doit PAS apparaître dans le WHERE.
        const std::string sql = EventSQL::delete_by_pk_batch;
        CHECK(sql ==
              "DELETE FROM relais_test_events WHERE id = ANY($1) "
              "RETURNING id, region, user_id, title, priority, created_at");
        CHECK(sql.find("region =") == std::string::npos);
    }

    SECTION("[SQL] RETURNING includes the partition column for L1 evict hint") {
        // L'invalidation L1 single-partition a besoin de la colonne partition :
        // RETURNING doit la ramener même si le WHERE ne la filtre pas.
        const std::string sql = EventSQL::delete_by_pk_batch;
        CHECK(sql.find("RETURNING") != std::string::npos);
        CHECK(sql.substr(sql.find("RETURNING")).find("region") != std::string::npos);
    }
}

// #############################################################################
//  delete_by_pk_batch — clé composite (unnest-tuple)
// #############################################################################

TEST_CASE("delete_by_pk_batch - composite key", "[generator][sql][batch][composite]") {
    SECTION("[SQL] (k1,k2) IN (SELECT unnest($1::t[]), unnest($2::t[]))") {
        CHECK(std::string(CompositeSQL::delete_by_pk_batch) ==
              "DELETE FROM relais_test_composite_list "
              "WHERE (tenant_id, item_id) IN "
              "(SELECT unnest($1::bigint[]), unnest($2::bigint[])) "
              "RETURNING tenant_id, item_id");
    }

    SECTION("[SQL] one bind param per key component — $1 and $2, no $3") {
        const std::string_view sql = CompositeSQL::delete_by_pk_batch;
        CHECK(sql.find("$1") != std::string_view::npos);
        CHECK(sql.find("$2") != std::string_view::npos);
        CHECK(sql.find("$3") == std::string_view::npos);
    }

    SECTION("[SQL] all-PK junction mirrors the same tuple form") {
        CHECK(std::string(JunctionSQL::delete_by_pk_batch) ==
              "DELETE FROM relais_test_all_pk_junction "
              "WHERE (user_id, role_id) IN "
              "(SELECT unnest($1::bigint[]), unnest($2::bigint[])) "
              "RETURNING user_id, role_id");
    }
}

// #############################################################################
//  Cohérence delete_by_pk_batch ↔ select_by_pk_batch
// #############################################################################

TEST_CASE("delete_by_pk_batch shares the WHERE shape of select_by_pk_batch",
          "[generator][sql][batch]") {
    // Les deux dérivent du même `batch_where` côté générateur : le DELETE doit
    // reprendre exactement la clause WHERE du SELECT (au mot DELETE/SELECT et au
    // RETURNING près). Vérifie qu'on ne diverge pas en cas d'évolution du shape.
    auto where_of = [](std::string_view sql) -> std::string {
        const auto from = sql.find("WHERE ");
        const auto ret = sql.find(" RETURNING");
        REQUIRE(from != std::string_view::npos);
        const auto end = (ret == std::string_view::npos) ? sql.size() : ret;
        return std::string(sql.substr(from, end - from));
    };

    SECTION("[SQL] scalar key — identical WHERE") {
        CHECK(where_of(UserSQL::delete_by_pk_batch) ==
              where_of(UserSQL::select_by_pk_batch));
    }

    SECTION("[SQL] composite key — identical WHERE") {
        CHECK(where_of(CompositeSQL::delete_by_pk_batch) ==
              where_of(CompositeSQL::select_by_pk_batch));
    }
}

// #############################################################################
//  delete_where — assemblage runtime (étape 2, §1)
// #############################################################################
//
// delete_where N'EST PAS un template du générateur : le prédicat est injecté en
// milieu de string au runtime (sortie buildWhereClause, numérotée $n). On verrouille
// ici la forme produite par detail::buildDeleteWhereSql + la survie du $n quand le
// prédicat vient réellement de buildWhereClause<Descriptor>.

TEST_CASE("delete_where - ctid-bounded form", "[generator][sql][batch][where]") {
    SECTION("[SQL] unconditional purge — no inner WHERE") {
        // where_sql vide → purge totale : pas de clause WHERE dans le sous-select,
        // mais le LIMIT K_pg borne toujours la suppression par statement.
        const std::string sql = detail::buildDeleteWhereSql(
            "relais_test_users",
            "id, username, email, balance, created_at",
            /*where_sql=*/"", detail::kPgEraseChunk);
        CHECK(sql ==
              "DELETE FROM relais_test_users WHERE ctid IN "
              "(SELECT ctid FROM relais_test_users LIMIT 10000) "
              "RETURNING id, username, email, balance, created_at");
    }

    SECTION("[SQL] predicate purge — inner WHERE carries the $n predicate") {
        const std::string sql = detail::buildDeleteWhereSql(
            "relais_test_users",
            "id, username, email, balance, created_at",
            /*where_sql=*/"\"balance\" < $1", detail::kPgEraseChunk);
        CHECK(sql ==
              "DELETE FROM relais_test_users WHERE ctid IN "
              "(SELECT ctid FROM relais_test_users WHERE \"balance\" < $1 "
              "LIMIT 10000) "
              "RETURNING id, username, email, balance, created_at");
        // Le prédicat $n vit dans le sous-select (le ctid IN), pas après le LIMIT.
        CHECK(sql.find("$1") < sql.find("LIMIT"));
    }

    SECTION("[SQL] K_pg is the library chunk bound, not a per-entity policy") {
        // Constante librairie : 10k, identique quelle que soit l'entité.
        CHECK(detail::kPgEraseChunk == 10000);
        const std::string sql = detail::buildDeleteWhereSql(
            "t", "id", "", detail::kPgEraseChunk);
        CHECK(sql.find("LIMIT 10000") != std::string::npos);
    }
}

TEST_CASE("delete_where - $n survives buildWhereClause -> assembly",
          "[generator][sql][batch][where]") {
    // Le prédicat réel sort de buildWhereClause<Descriptor> ($n numéroté à partir
    // de 1) ; l'assembleur doit le replacer tel quel dans le sous-select ctid.
    decl::Filters<ArticleDesc> f;
    f.template get<"is_published">() = true;  // is_published EQ -> "is_published"=$1

    auto wc = decl::buildWhereClause<ArticleDesc>(f);
    REQUIRE(wc.sql.find("$1") != std::string::npos);
    REQUIRE(wc.params.params.size() == 1);

    const std::string sql = detail::buildDeleteWhereSql(
        entity::generated::TestArticleInMapping::table_name,
        entity::generated::TestArticleInMapping::SQL::returning_columns,
        wc.sql, detail::kPgEraseChunk);

    // Forme attendue : le WHERE de buildWhereClause est encapsulé dans le ctid IN,
    // suivi de LIMIT puis RETURNING <returning_columns>.
    const std::string inner = std::string(" WHERE ") + wc.sql + " LIMIT 10000)";
    CHECK(sql.find(inner) != std::string::npos);
    CHECK(sql.find("WHERE ctid IN (SELECT ctid FROM ") != std::string::npos);
    CHECK(sql.ends_with(std::string("RETURNING ")
                        + entity::generated::TestArticleInMapping::SQL::returning_columns));
    CHECK(sql.find("$1") < sql.find("LIMIT"));
}
