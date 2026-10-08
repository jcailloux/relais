/**
 * test_repository_compile.cpp
 *
 * Compile-time tests for the full Repo mixin chain.
 * Verifies that Repo.h, LocalRepo.h, InvalidationMixin.h,
 * and ListMixin.h compile correctly.
 *
 * Exercises all mixin combinations:
 *   - Uncached (PgRepo only)
 *   - L1 (LocalRepo)
 *   - L2 (RedisRepo)
 *   - L1+L2 (LocalRepo + RedisRepo)
 *   - With ListDescriptor (ListMixin auto-detected)
 *   - With cross-invalidation (InvalidationMixin)
 *   - Read-only variants
 *
 * No database or Redis connection needed — all tests are structural.
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>
#include <type_traits>
#include <tuple>

#include "fixtures/TestRepositories.h"

using namespace relais_test;

// Detects whether a full UPDATE ... SET is generated for an entity. The
// generator suppresses toUpdateParams (and SQL::update) when there is no
// updatable column, so this is false for read-only views and all-PK junctions.
template<typename E>
concept HasToUpdateParams = requires(const E& e) {
    { E::toUpdateParams(e) }
        -> std::convertible_to<jcailloux::relais::io::PgParams>;
};

// Detects whether the full Repo chain exposes update(). Gated on HasFullUpdate
// across every mixin layer, so it disappears for entities with no updatable
// column rather than failing to instantiate at the call site.
template<typename Repo>
concept RepoHasUpdate = requires(const typename Repo::KeyType& k,
                                 const typename Repo::EntityType& e) {
    Repo::update(k, e);
};

// =============================================================================
// Verify Repo instantiation compiles for all cache levels
// =============================================================================

TEST_CASE("Repo instantiation - all cache levels", "[repository][compile]") {
    SECTION("Uncached - PgRepo only") {
        STATIC_REQUIRE(std::is_same_v<UncachedTestItemRepo::EntityType,
                                       entity::generated::TestItemEntity>);
        STATIC_REQUIRE(std::is_same_v<UncachedTestItemRepo::KeyType, int64_t>);
    }

    SECTION("L1 - LocalRepo") {
        STATIC_REQUIRE(std::is_same_v<L1TestItemRepo::EntityType,
                                       entity::generated::TestItemEntity>);
        STATIC_REQUIRE(std::is_same_v<L1TestItemRepo::KeyType, int64_t>);
    }

    SECTION("L2 - RedisRepo") {
        STATIC_REQUIRE(std::is_same_v<L2TestItemRepo::EntityType,
                                       entity::generated::TestItemEntity>);
    }

    SECTION("L1+L2 - full hierarchy") {
        STATIC_REQUIRE(std::is_same_v<FullCacheTestItemRepo::EntityType,
                                       entity::generated::TestItemEntity>);
    }
}

// =============================================================================
// Verify Repo name() works
// =============================================================================

TEST_CASE("Repo name()", "[repository][compile]") {
    REQUIRE(std::string(UncachedTestItemRepo::name()) == "test:uncached");
    REQUIRE(std::string(L1TestItemRepo::name()) == "test:l1");
    REQUIRE(std::string(L2TestItemRepo::name()) == "test:l2");
    REQUIRE(std::string(FullCacheTestItemRepo::name()) == "test:both");
}

// =============================================================================
// Verify config() accessor
// =============================================================================

TEST_CASE("Repo config", "[repository][compile]") {
    SECTION("Uncached") {
        constexpr auto cfg = UncachedTestItemRepo::config;
        STATIC_REQUIRE(cfg.cache_level == jcailloux::relais::config::CacheLevel::None);
        STATIC_REQUIRE(!cfg.read_only);
    }

    SECTION("L1 local") {
        constexpr auto cfg = L1TestItemRepo::config;
        STATIC_REQUIRE(cfg.cache_level == jcailloux::relais::config::CacheLevel::L1);
    }

    SECTION("L2 Redis") {
        constexpr auto cfg = L2TestItemRepo::config;
        STATIC_REQUIRE(cfg.cache_level == jcailloux::relais::config::CacheLevel::L2);
    }

    SECTION("L1+L2 Both") {
        constexpr auto cfg = FullCacheTestItemRepo::config;
        STATIC_REQUIRE(cfg.cache_level == jcailloux::relais::config::CacheLevel::L1_L2);
    }
}

// =============================================================================
// Verify LocalRepo-specific features compile
// =============================================================================

TEST_CASE("LocalRepo features", "[repository][compile][cached]") {
    SECTION("l1Ttl") {
        auto ttl = L1TestItemRepo::l1Ttl();
        REQUIRE(ttl.count() > 0);
    }

    SECTION("size") {
        auto size = L1TestItemRepo::size();
        REQUIRE(size == 0);  // empty at start
    }

    SECTION("purge") {
        auto erased = L1TestItemRepo::purge();
        REQUIRE(erased == 0);  // empty cache
    }

    SECTION("warmup") {
        L1TestItemRepo::warmup();
    }
}

// =============================================================================
// L1 maintenance without L1: same API, neutral results
// =============================================================================

TEMPLATE_TEST_CASE("L1 maintenance without L1 is neutral", "[repository][compile][maintenance]",
                   UncachedTestItemRepo, L2TestItemRepo,
                   UncachedTestArticleRepo, L2TestArticleRepo) {
    STATIC_REQUIRE(std::is_same_v<decltype(TestType::size()), size_t>);
    STATIC_REQUIRE(std::is_same_v<decltype(TestType::purge()), size_t>);
    STATIC_REQUIRE(std::is_same_v<decltype(TestType::warmup()), void>);

    REQUIRE(TestType::size() == 0);
    REQUIRE(TestType::purge() == 0);
    TestType::warmup();
    REQUIRE(TestType::size() == 0);
}

// =============================================================================
// Verify config presets compile
// =============================================================================

TEST_CASE("Config presets", "[repository][compile]") {
    SECTION("ShortTTL") {
        constexpr auto cfg = ShortTTLTestItemRepo::config;
        STATIC_REQUIRE(cfg.cache_level == jcailloux::relais::config::CacheLevel::L1);
    }

    SECTION("WriteThrough") {
        constexpr auto cfg = WriteThroughTestItemRepo::config;
        STATIC_REQUIRE(cfg.update_strategy ==
            jcailloux::relais::config::UpdateStrategy::PopulateImmediately);
    }

    SECTION("FewChunks") {
        constexpr auto cfg = FewChunksTestItemRepo::config;
        STATIC_REQUIRE(cfg.l1_chunk_count_log2 == 1);
    }
}

// =============================================================================
// Verify ListMixin auto-detection (Article has ListDescriptor)
// =============================================================================

TEST_CASE("ListMixin auto-detected from ListDescriptor", "[repository][compile][list]") {
    SECTION("Article repo with list") {
        STATIC_REQUIRE(std::is_same_v<TestArticleListRepo::EntityType,
                                       entity::generated::TestArticleEntity>);
        // ListDescriptorType should exist if ListMixin is active
        using Desc = TestArticleListRepo::ListDescriptorType;
        (void)sizeof(Desc);  // verify type exists
    }

    SECTION("purge — unified (entity + list)") {
        auto erased = TestArticleListRepo::purge();
        REQUIRE(erased == 0);
    }

    SECTION("listSize") {
        REQUIRE(TestArticleListRepo::listSize() == 0);
    }
}

// =============================================================================
// Verify InvalidationMixin (cross-invalidation) compiles
// =============================================================================

TEST_CASE("InvalidationMixin with cross-invalidation", "[repository][compile][invalidation]") {
    SECTION("Purchase repo with User invalidation") {
        STATIC_REQUIRE(std::is_same_v<L1TestPurchaseRepo::EntityType,
                                       entity::generated::TestPurchaseEntity>);
    }
}

// =============================================================================
// Verify read-only repositories compile (write methods should be absent)
// =============================================================================

TEST_CASE("Read-only repositories", "[repository][compile][readonly]") {
    SECTION("ReadOnly uncached") {
        constexpr auto cfg = ReadOnlyTestItemRepo::config;
        STATIC_REQUIRE(cfg.read_only);
        STATIC_REQUIRE(cfg.cache_level == jcailloux::relais::config::CacheLevel::None);
    }

    SECTION("ReadOnly L2") {
        constexpr auto cfg = ReadOnlyL2TestItemRepo::config;
        STATIC_REQUIRE(cfg.read_only);
        STATIC_REQUIRE(cfg.cache_level == jcailloux::relais::config::CacheLevel::L2);
    }
}

// =============================================================================
// All-PK junction: every column is part of the composite key, so there is no
// updatable column. Regression for the empty TraitsType::Field path (Entity<>
// must still instantiate) and suppressed SQL::update/toUpdateParams.
// =============================================================================

TEST_CASE("All-PK junction entity", "[repository][compile][junction]") {
    using Entity = entity::generated::TestAllPkJunctionEntity;

    SECTION("Entity<> instantiates with an empty Field enum") {
        using Field = Entity::TraitsType::Field;   // would fail to compile if absent
        STATIC_REQUIRE(std::is_enum_v<Field>);
    }

    SECTION("composite key type") {
        STATIC_REQUIRE(std::is_same_v<UncachedTestAllPkJunctionRepo::KeyType,
                                       std::tuple<int64_t, int64_t>>);
    }

    SECTION("no toUpdateParams generated (no updatable column)") {
        STATIC_REQUIRE(!HasToUpdateParams<Entity>);
        // sanity: a normal entity DOES expose it
        STATIC_REQUIRE(HasToUpdateParams<entity::generated::TestItemEntity>);
    }

    SECTION("update() is cleanly absent from the whole chain") {
        STATIC_REQUIRE(!RepoHasUpdate<FullCacheTestAllPkJunctionRepo>);
        STATIC_REQUIRE(!RepoHasUpdate<L1TestAllPkJunctionRepo>);
        STATIC_REQUIRE(!RepoHasUpdate<UncachedTestAllPkJunctionRepo>);
        // sanity: a normal entity's repo keeps update()
        STATIC_REQUIRE(RepoHasUpdate<FullCacheTestItemRepo>);
    }

    SECTION("full mixin chain instantiates (L1+L2)") {
        STATIC_REQUIRE(std::is_same_v<FullCacheTestAllPkJunctionRepo::EntityType, Entity>);
        REQUIRE(FullCacheTestAllPkJunctionRepo::size() == 0);
        REQUIRE(std::string(UncachedTestAllPkJunctionRepo::name()) == "test:junction:uncached");
    }
}

// =============================================================================
// Read-only view (@relais read_only): no writes, empty Field enum.
// =============================================================================

TEST_CASE("Read-only view entity", "[repository][compile][readonly]") {
    using Entity = entity::generated::TestReadOnlyViewEntity;

    SECTION("mapping is read-only and Entity<> instantiates") {
        STATIC_REQUIRE(Entity::read_only);
        using Field = Entity::TraitsType::Field;
        STATIC_REQUIRE(std::is_enum_v<Field>);
    }

    SECTION("no toUpdateParams generated") {
        STATIC_REQUIRE(!HasToUpdateParams<Entity>);
    }

    SECTION("read-only repo chain instantiates") {
        STATIC_REQUIRE(UncachedTestReadOnlyViewRepo::config.read_only);
        STATIC_REQUIRE(L2TestReadOnlyViewRepo::config.read_only);
        STATIC_REQUIRE(std::is_same_v<UncachedTestReadOnlyViewRepo::KeyType, int64_t>);
        REQUIRE(std::string(UncachedTestReadOnlyViewRepo::name()) == "test:roview:uncached");
    }
}

// =============================================================================
// Verify User repository variants compile
// =============================================================================

TEST_CASE("User repository variants", "[repository][compile]") {
    REQUIRE(std::string(UncachedTestUserRepo::name()) == "test:user:uncached");
    REQUIRE(std::string(L1TestUserRepo::name()) == "test:user:l1");
    REQUIRE(std::string(L2TestUserRepo::name()) == "test:user:l2");
    REQUIRE(std::string(FullCacheTestUserRepo::name()) == "test:user:both");
}

// =============================================================================
// Verify Event (PartitionKey) repositories compile
// =============================================================================

TEST_CASE("PartitionKey event repositories", "[repository][compile][partition_key]") {
    SECTION("Uncached event") {
        STATIC_REQUIRE(std::is_same_v<UncachedTestEventRepo::KeyType, int64_t>);
    }

    SECTION("L1 event") {
        STATIC_REQUIRE(std::is_same_v<L1TestEventRepo::KeyType, int64_t>);
    }

    SECTION("L2 event") {
        STATIC_REQUIRE(std::is_same_v<L2TestEventRepo::KeyType, int64_t>);
    }

    SECTION("L1+L2 event") {
        STATIC_REQUIRE(std::is_same_v<L1L2TestEventRepo::KeyType, int64_t>);
    }
}

// =============================================================================
// API parity across cache presets
// =============================================================================
//
// The public Repo API is the same whatever the CacheConfig: switching preset
// never breaks a caller. exerciseApi calls every documented method, so each
// body is instantiated (a requires-expression only checks the declaration);
// apiSignatures records each method's return type, compared preset to preset.
// Entity-dependent methods are gated on the entity's concepts, never on the
// preset.

namespace parity {

namespace jr = jcailloux::relais;
namespace jre = jcailloux::relais::entity;

using BothTestArticleRepo = Repo<TestArticleEntity, "test:article:parity:both", cfg::Both>;
using L2TestAssignedKeyRepo = Repo<TestAssignedKeyEntity, "test:akey:parity:l2", cfg::Redis>;

/// Arguments for the field-update methods: one field written, guarded and
/// ordered on.
template<auto F, typename V>
struct FieldArgs {
    static auto update() { return jre::set<F>(V{1}); }
    static auto guard() { return jre::when(jre::eq<F>(V{1})); }
    static auto order() { return jre::orderBy(jre::asc<F>()); }
};

template<typename E> struct ApiArgs;
template<> struct ApiArgs<TestItemEntity>
    : FieldArgs<TestItemEntity::Field::value, int32_t> {};
template<> struct ApiArgs<TestArticleEntity>
    : FieldArgs<TestArticleEntity::Field::author_id, int64_t> {};
template<> struct ApiArgs<TestAssignedKeyEntity>
    : FieldArgs<TestAssignedKeyEntity::Field::payload, int64_t> {};

/// One method's return type, tagged with the method name for diagnostics.
template<cfg::FixedString Name, typename T>
struct Sig {};

template<typename R, typename E = typename R::EntityType, typename K = typename R::KeyType>
auto apiSignatures(const K& key, const E& ent, std::span<const K> ids) {
    using A = ApiArgs<E>;

    auto core = std::tuple<
        Sig<"name", decltype(R::name())>,
        Sig<"config", decltype(R::config)>,
        Sig<"find", decltype(R::find(key))>,
        Sig<"findJson", decltype(R::findJson(key))>,
        Sig<"findMany", decltype(R::findMany(ids))>,
        Sig<"insert", decltype(R::insert(ent))>,
        Sig<"erase", decltype(R::erase(key))>,
        Sig<"eraseMany", decltype(R::eraseMany(ids))>,
        Sig<"invalidate", decltype(R::invalidate(key))>,
        Sig<"invalidateMany", decltype(R::invalidateMany(ids))>,
        Sig<"size", decltype(R::size())>,
        Sig<"purge", decltype(R::purge())>,
        Sig<"warmup", decltype(R::warmup())>>{};

    auto binary = [] {
        if constexpr (jr::HasBinarySerialization<E>)
            return std::tuple<Sig<"findBinary", decltype(R::findBinary(key))>>{};
        else return std::tuple<>{};
    }();

    auto fullUpdate = [] {
        if constexpr (jr::HasFullUpdate<E> && jr::HasBinarySerialization<E>)
            return std::tuple<
                Sig<"update", decltype(R::update(key, ent))>,
                Sig<"updateJson", decltype(R::updateJson(key, std::string_view{}))>,
                Sig<"updateBinary", decltype(R::updateBinary(key, std::span<const uint8_t>{}))>>{};
        else if constexpr (jr::HasFullUpdate<E>)
            return std::tuple<
                Sig<"update", decltype(R::update(key, ent))>,
                Sig<"updateJson", decltype(R::updateJson(key, std::string_view{}))>>{};
        else return std::tuple<>{};
    }();

    auto upsert = [] {
        if constexpr (jr::HasUpsertSql<E>)
            return std::tuple<Sig<"upsert", decltype(R::upsert(ent))>>{};
        else return std::tuple<>{};
    }();

    auto fieldUpdate = [] {
        if constexpr (jr::HasFieldUpdate<E>)
            return std::tuple<
                Sig<"patch", decltype(R::patch(key, A::update()))>,
                Sig<"patchIf", decltype(R::patchIf(key, A::guard(), A::update()))>,
                Sig<"patchWhere", decltype(R::patchWhere(A::guard(), A::update()))>,
                Sig<"claim", decltype(R::claim(A::guard(), size_t{1}, A::update()))>,
                Sig<"claimOrdered", decltype(R::claim(A::guard(), A::order(), size_t{1}, A::update()))>>{};
        else return std::tuple<>{};
    }();

    auto where = [] {
        if constexpr (jr::HasFilterSet<E>) {
            using P = typename E::MappingType::FilterSet::Values;
            return std::tuple<
                Sig<"eraseWhere", decltype(R::eraseWhere(P{}))>,
                Sig<"invalidateWhere", decltype(R::invalidateWhere(P{}))>>{};
        }
        else return std::tuple<>{};
    }();

    auto list = [] {
        if constexpr (jr::HasListDescriptor<E>) {
            // The list query types are tagged per repo (a cursor from another
            // repo is rejected), so they differ between presets by design;
            // callers name them through the Repo aliases, which every preset
            // provides.
            using Q = typename R::ListQuery;
            auto common = std::tuple<
                Sig<"queryBuilder", std::bool_constant<
                    std::is_same_v<decltype(R::queryBuilder()), typename R::QueryBuilder>>>,
                Sig<"query", decltype(R::query(std::declval<const Q&>()))>,
                Sig<"queryJson", decltype(R::queryJson(std::declval<const Q&>()))>,
                Sig<"listSize", decltype(R::listSize())>>{};
            if constexpr (jr::HasBinarySerialization<E>)
                return std::tuple_cat(common,
                    std::tuple<Sig<"queryBinary", decltype(R::queryBinary(std::declval<const Q&>()))>>{});
            else return common;
        }
        else return std::tuple<>{};
    }();

#if RELAIS_ENABLE_METRICS
    auto metrics = std::tuple<
        Sig<"metrics", decltype(R::metrics())>,
        Sig<"resetMetrics", decltype(R::resetMetrics())>>{};
#else
    auto metrics = std::tuple<>{};
#endif

    return std::tuple_cat(core, binary, fullUpdate, upsert, fieldUpdate, where, list, metrics);
}

template<typename R>
using ApiSignatures = decltype(apiSignatures<R>(
    std::declval<const typename R::KeyType&>(),
    std::declval<const typename R::EntityType&>(),
    std::declval<std::span<const typename R::KeyType>>()));

/// Never run: taking its address instantiates every called method's body.
template<typename R>
jr::io::Task<void> exerciseApi() {
    using E = typename R::EntityType;
    using K = typename R::KeyType;
    using A = ApiArgs<E>;
    const K key{};
    const E ent{};
    const std::vector<K> keys;
    const std::span<const K> ids{keys};

    (void)R::name();
    (void)co_await R::find(key);
    (void)co_await R::findJson(key);
    (void)co_await R::findMany(ids);
    (void)co_await R::insert(ent);
    (void)co_await R::erase(key);
    (void)co_await R::eraseMany(ids);
    co_await R::invalidate(key);
    co_await R::invalidateMany(ids);
    (void)R::size();
    (void)R::purge();
    R::warmup();

    if constexpr (jr::HasBinarySerialization<E>) {
        (void)co_await R::findBinary(key);
    }
    if constexpr (jr::HasFullUpdate<E>) {
        (void)co_await R::update(key, ent);
        (void)co_await R::updateJson(key, std::string_view{});
        if constexpr (jr::HasBinarySerialization<E>) {
            (void)co_await R::updateBinary(key, std::span<const uint8_t>{});
        }
    }
    if constexpr (jr::HasUpsertSql<E>) {
        (void)co_await R::upsert(ent);
    }
    if constexpr (jr::HasFieldUpdate<E>) {
        (void)co_await R::patch(key, A::update());
        (void)co_await R::patchIf(key, A::guard(), A::update());
        (void)co_await R::patchWhere(A::guard(), A::update());
        (void)co_await R::claim(A::guard(), 1, A::update());
        (void)co_await R::claim(A::guard(), A::order(), 1, A::update());
    }
    if constexpr (jr::HasFilterSet<E>) {
        using P = typename E::MappingType::FilterSet::Values;
        (void)co_await R::eraseWhere(P{});
        co_await R::invalidateWhere(P{});
    }
    if constexpr (jr::HasListDescriptor<E>) {
        const auto q = R::queryBuilder().limit(10).build();
        (void)co_await R::query(q);
        (void)co_await R::queryJson(q);
        if constexpr (jr::HasBinarySerialization<E>) {
            (void)co_await R::queryBinary(q);
        }
        (void)R::listSize();
    }
#if RELAIS_ENABLE_METRICS
    (void)R::metrics();
    R::resetMetrics();
#endif
}

/// Same signatures on every preset, return types included.
template<typename Uncached, typename... Cached>
constexpr bool kSameApi = (std::is_same_v<ApiSignatures<Uncached>, ApiSignatures<Cached>> && ...);

}  // namespace parity

TEMPLATE_TEST_CASE("API parity - every documented method instantiates",
                   "[repository][compile][parity]",
                   UncachedTestItemRepo, L1TestItemRepo, L2TestItemRepo, FullCacheTestItemRepo,
                   UncachedTestArticleRepo, L1TestArticleRepo, L2TestArticleRepo,
                   parity::BothTestArticleRepo,
                   UncachedTestAssignedKeyRepo, L1TestAssignedKeyRepo,
                   parity::L2TestAssignedKeyRepo, FullCacheTestAssignedKeyRepo) {
    auto exercise = &parity::exerciseApi<TestType>;
    STATIC_REQUIRE(std::is_same_v<decltype(exercise), jcailloux::relais::io::Task<void> (*)()>);
    (void)exercise;
}

TEST_CASE("API parity - identical signatures across presets", "[repository][compile][parity]") {
    SECTION("entity without list") {
        STATIC_REQUIRE(parity::kSameApi<UncachedTestItemRepo,
            L1TestItemRepo, L2TestItemRepo, FullCacheTestItemRepo>);
    }
    SECTION("entity with list") {
        STATIC_REQUIRE(parity::kSameApi<UncachedTestArticleRepo,
            L1TestArticleRepo, L2TestArticleRepo, parity::BothTestArticleRepo>);
    }
    SECTION("entity with upsert") {
        STATIC_REQUIRE(parity::kSameApi<UncachedTestAssignedKeyRepo,
            L1TestAssignedKeyRepo, parity::L2TestAssignedKeyRepo, FullCacheTestAssignedKeyRepo>);
    }
}
