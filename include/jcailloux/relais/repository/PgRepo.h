#ifndef JCX_RELAIS_PGREPO_H
#define JCX_RELAIS_PGREPO_H

#include <algorithm>
#include <atomic>
#include <concepts>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "jcailloux/relais/io/Task.h"
#include "jcailloux/relais/io/pg/PgError.h"
#include "jcailloux/relais/io/pg/PgParams.h"
#include "jcailloux/relais/io/pg/PgResult.h"
#include "jcailloux/relais/PgProvider.h"
#include "jcailloux/relais/Log.h"
#include "jcailloux/relais/config/CacheConfig.h"
#include "jcailloux/relais/config/FixedString.h"
#include "jcailloux/relais/TypeTraits.h"
#include "jcailloux/relais/entity/EntityConcepts.h"
#include "jcailloux/relais/cache/CacheView.h"
#include "jcailloux/relais/entity/FieldUpdate.h"
#include "jcailloux/relais/repository/ConditionalWrite.h"
#include "jcailloux/relais/repository/GuardSql.h"
#include "jcailloux/relais/list/spec/GeneratedCriteria.h"

namespace relais_test { struct TestInternals; }

namespace jcailloux::relais {

// =========================================================================
// Concepts
// =========================================================================

/// E supports partial field updates (has TraitsType with Field enum)
template<typename E>
concept HasFieldUpdate = requires {
    typename E::TraitsType;
    typename E::TraitsType::Field;
};

/// E supports a full-row UPDATE: it has at least one updatable column, so the
/// generator emitted toUpdateParams (+ SQL::update). False for pure all-PK
/// junctions, where update() would reference a suppressed SQL::update. Gates
/// every update path so the method is cleanly absent rather than failing to
/// instantiate at the call site.
template<typename E>
concept HasFullUpdate = requires(const E& e) {
    E::toUpdateParams(e);
};

/// E can back a native INSERT ... ON CONFLICT DO UPDATE: creatable (read + write
/// + keyed) with a full-row update (at least one non-PK column to SET). Pairs
/// with HasUpsertSql (the Mapping actually emitted SQL::upsert) to gate every
/// upsert method — the entity shape here, the emitted SQL there.
template<typename E, typename Key>
concept UpsertableEntity = CreatableEntity<E, Key> && HasFullUpdate<E>;

// =========================================================================
// SQL helper for UPDATE ... RETURNING
// =========================================================================

namespace detail {

/// One SET assignment: the quoted column and how it takes its bound value.
/// Implicit from a column name for the plain `col=$n` form.
struct SetColumn {
    std::string_view column;
    entity::SetOp op = entity::SetOp::Assign;

    template<typename S>
        requires std::convertible_to<const S&, std::string_view>
    SetColumn(const S& col, entity::SetOp o = entity::SetOp::Assign)
        : column(col), op(o) {}
};

/// Append the SET list, numbering values $1..$N in order. Returns the next
/// free parameter index. `qual` prefixes the column read on the right-hand side
/// ("t." when the target is aliased); the assigned column is never qualified.
inline size_t appendSetClause(std::string& sql, std::initializer_list<SetColumn> sets,
                              std::string_view qual = {}) {
    size_t param = 1;
    bool first = true;
    for (const auto& s : sets) {
        if (!first) sql += ',';
        first = false;
        sql += s.column;
        sql += '=';
        switch (s.op) {
            case entity::SetOp::Assign:
                break;
            case entity::SetOp::Add:
                sql += qual;
                sql += s.column;
                sql += '+';
                break;
            case entity::SetOp::Subtract:
                sql += qual;
                sql += s.column;
                sql += '-';
                break;
            case entity::SetOp::NowPlus:
                sql += "now()+interval '1 microsecond'*";
                break;
        }
        sql += '$';
        sql += std::to_string(param++);
    }
    return param;
}

/// Append `"pk"=$n` for each key column (joined by AND), numbering from `param`.
inline void appendPkMatch(std::string& sql, std::string_view pk_column, size_t& param) {
    sql += '"';
    sql += pk_column;
    sql += "\"=$";
    sql += std::to_string(param++);
}

template<size_t N>
void appendPkMatch(std::string& sql, const std::array<const char*, N>& pk_columns, size_t& param) {
    for (size_t i = 0; i < N; ++i) {
        if (i > 0) sql += " AND ";
        appendPkMatch(sql, pk_columns[i], param);
    }
}

/// Append a comma-separated column list ("a, b, c"), each column prefixed by `qual`.
inline void appendQualifiedColumns(std::string& sql, std::string_view columns,
                                   std::string_view qual) {
    bool first = true;
    while (!columns.empty()) {
        auto comma = columns.find(',');
        auto col = columns.substr(0, comma);
        columns = comma == std::string_view::npos ? std::string_view{} : columns.substr(comma + 1);
        while (!col.empty() && col.front() == ' ') col.remove_prefix(1);
        while (!col.empty() && col.back() == ' ') col.remove_suffix(1);
        if (!first) sql += ',';
        first = false;
        sql += qual;
        sql += col;
    }
}

/// Number of columns in a comma-separated column list.
[[nodiscard]] constexpr int countColumns(std::string_view columns) {
    int n = columns.empty() ? 0 : 1;
    for (char c : columns) n += c == ',';
    return n;
}

/// Append `t."pk"=<cte>."pk"` for each key column (joined by AND).
inline void appendPkJoin(std::string& sql, std::string_view pk_column,
                         std::string_view cte = "o") {
    sql += "t.\"";
    sql += pk_column;
    sql += "\"=";
    sql += cte;
    sql += ".\"";
    sql += pk_column;
    sql += '"';
}

template<size_t N>
void appendPkJoin(std::string& sql, const std::array<const char*, N>& pk_columns,
                  std::string_view cte = "o") {
    for (size_t i = 0; i < N; ++i) {
        if (i > 0) sql += " AND ";
        appendPkJoin(sql, pk_columns[i], cte);
    }
}

/// Build the UPDATE of the rows `emit_where` matches, returning per `rows`:
///   Count:   UPDATE tbl SET <sets> WHERE <where>
///   After:   UPDATE tbl SET <sets> WHERE <where> RETURNING <cols>
///   Changes: WITH o AS (SELECT <cols> FROM tbl WHERE <where> FOR UPDATE)
///            UPDATE tbl AS t SET <sets> FROM o WHERE t.pk=o.pk
///            RETURNING o.<cols>, t.<cols>
/// Either way a row locked by a concurrent writer is waited on, then re-checked
/// against <where> on its committed version. Changes pays a second read of each
/// row (the CTE, which projects the version the write replaces), so it is only
/// asked for when that version is used. Its SET right-hand sides are qualified
/// with `t.`: unqualified, a column would be ambiguous between the target and
/// the CTE. Parameters: SET values, then those of <where>.
template<typename Pk, typename EmitWhere>
std::string buildUpdateSql(
    std::string_view table_name,
    const Pk& pk,
    std::initializer_list<SetColumn> sets,
    std::string_view columns,
    Returns rows,
    EmitWhere&& emit_where)
{
    const bool changes = rows == Returns::Changes;
    std::string set_clause;
    size_t param = appendSetClause(set_clause, sets, changes ? "t." : "");

    std::string sql;
    sql.reserve(256 + 3 * columns.size());
    if (!changes) {
        sql += "UPDATE ";
        sql += table_name;
        sql += " SET ";
        sql += set_clause;
        sql += " WHERE ";
        emit_where(sql, param);
        if (rows == Returns::After) {
            sql += " RETURNING ";
            sql += columns;
        }
        return sql;
    }
    sql += "WITH o AS (SELECT ";
    sql += columns;
    sql += " FROM ";
    sql += table_name;
    sql += " WHERE ";
    emit_where(sql, param);
    sql += " FOR UPDATE) UPDATE ";
    sql += table_name;
    sql += " AS t SET ";
    sql += set_clause;
    sql += " FROM o WHERE ";
    appendPkJoin(sql, pk);
    sql += " RETURNING ";
    appendQualifiedColumns(sql, columns, "o.");
    sql += ',';
    appendQualifiedColumns(sql, columns, "t.");
    return sql;
}

/// Build the predicate UPDATE (see buildUpdateSql).
template<typename Traits, typename Pred, typename Pk>
std::string buildPatchWhereSql(
    std::string_view table_name,
    const Pk& pk,
    std::initializer_list<SetColumn> sets,
    std::string_view columns,
    Returns rows)
{
    return buildUpdateSql(table_name, pk, sets, columns, rows,
        [](std::string& sql, size_t& param) { GuardSql<Traits, Pred>::emit(sql, param, {}); });
}

/// Build the keyed partial UPDATE (see buildUpdateSql), the key standing for
/// the predicate: `<pk>=$k [AND <guard>]`. Parameters: SET values, then the key
/// column(s), then the guard values. `pk` is a single column name or a
/// std::array of them (composite key); `Guard` is void for an unguarded patch.
template<typename Traits, typename Guard, typename Pk>
std::string buildKeyedPatchSql(
    std::string_view table_name,
    const Pk& pk,
    std::initializer_list<SetColumn> sets,
    std::string_view columns,
    Returns rows)
{
    return buildUpdateSql(table_name, pk, sets, columns, rows,
        [&pk](std::string& sql, size_t& param) {
            appendPkMatch(sql, pk, param);
            if constexpr (!std::is_void_v<Guard>) {
                sql += " AND ";
                GuardSql<Traits, Guard>::emit(sql, param, {});
            }
        });
}

/// Name of the rank column a claim returns after both row versions; unusual
/// enough not to collide with an entity column.
inline constexpr std::string_view kClaimRankColumn = "relais_claim_rank";

/// Build the claim UPDATE that takes the first n candidates of an ordering:
///   WITH l AS (SELECT <cols> FROM tbl WHERE <pred> ORDER BY <order> LIMIT $n
///              FOR UPDATE [SKIP LOCKED]),
///        c AS (SELECT l.*, row_number() OVER (ORDER BY <order>) AS rank FROM l)
///   UPDATE tbl AS t SET <sets> FROM c WHERE t.pk=c.pk [AND (SELECT count(*) FROM c)=$n]
///   [RETURNING [c.<cols>,] t.<cols>, c.rank]
/// PostgreSQL rejects a window function next to FOR UPDATE, hence the second
/// CTE ranking the locked rows; RETURNING order is unspecified, the rank
/// restores the ordering. Per `rows`, nothing is returned (Count), the
/// committed rows (After), or each row before then after (Changes). The count
/// check (Exact) writes nothing unless all n rows were obtained. Parameters:
/// SET values, then n, then the predicate values, then the ordering values;
/// the ordering is emitted once and pasted twice, so both copies share their
/// placeholders.
template<typename Traits, typename Pred, typename Order, typename Pk>
std::string buildClaimSql(
    std::string_view table_name,
    const Pk& pk,
    std::initializer_list<SetColumn> sets,
    std::string_view columns,
    ClaimMode mode,
    Lock lock,
    Returns rows)
{
    std::string set_clause;
    size_t param = appendSetClause(set_clause, sets, "t.");
    const std::string n_param = "$" + std::to_string(param++);

    std::string where;
    GuardSql<Traits, Pred>::emit(where, param, {});
    std::string order;
    OrderBySql<Traits, Order>::emit(order, param, {}, pk);

    std::string sql;
    sql.reserve(320 + 3 * columns.size() + 2 * order.size());
    sql += "WITH l AS (SELECT ";
    sql += columns;
    sql += " FROM ";
    sql += table_name;
    sql += " WHERE ";
    sql += where;
    sql += ' ';
    sql += order;
    sql += " LIMIT ";
    sql += n_param;
    sql += lock == Lock::SkipLocked ? " FOR UPDATE SKIP LOCKED)" : " FOR UPDATE)";
    sql += ", c AS (SELECT l.*,row_number() OVER (";
    sql += order;
    sql += ") AS ";
    sql += kClaimRankColumn;
    sql += " FROM l) UPDATE ";
    sql += table_name;
    sql += " AS t SET ";
    sql += set_clause;
    sql += " FROM c WHERE ";
    appendPkJoin(sql, pk, "c");
    if (mode == ClaimMode::Exact) {
        sql += " AND (SELECT count(*) FROM c)=";
        sql += n_param;
    }
    if (rows == Returns::Count) return sql;
    sql += " RETURNING ";
    if (rows == Returns::Changes) {
        appendQualifiedColumns(sql, columns, "c.");
        sql += ',';
    }
    appendQualifiedColumns(sql, columns, "t.");
    sql += ",c.";
    sql += kClaimRankColumn;
    return sql;
}

/// K_pg — per-statement LIMIT bound for the iterative predicate DELETE. Library
/// tuning constant (NOT a per-entity generation policy): 10k rows ≈ sub-second
/// of row locks / WAL per statement, so a large purge stays interruptible and
/// never holds a table-wide lock window. eraseWhereRaw loops at this granularity.
inline constexpr size_t kPgEraseChunk = 10000;

/// Build the iterative ctid-bounded predicate DELETE:
///   DELETE FROM <t> WHERE ctid IN (SELECT ctid FROM <t> [WHERE <pred>] LIMIT K)
///   RETURNING <cols>
/// Pure string assembly (no I/O) so the generated form is unit-testable. The
/// runtime-injected `where_sql` (buildWhereClause output, `$n`-numbered) can't be
/// a static C-string template — hence assembled here rather than by the Python
/// generator (cf. plan step 1 deferral). Empty `where_sql` → unconditional purge.
/// The ctid sub-select + LIMIT is what makes one statement delete at most K rows;
/// the caller re-runs the same statement until it returns 0 (the predicate keeps
/// matching the not-yet-deleted remainder).
inline std::string buildDeleteWhereSql(
    std::string_view table_name,
    std::string_view returning_columns,
    std::string_view where_sql,
    size_t chunk)
{
    std::string sql;
    sql.reserve(128);
    sql += "DELETE FROM ";
    sql += table_name;
    sql += " WHERE ctid IN (SELECT ctid FROM ";
    sql += table_name;
    if (!where_sql.empty()) {
        sql += " WHERE ";
        sql += where_sql;
    }
    sql += " LIMIT ";
    sql += std::to_string(chunk);
    sql += ") RETURNING ";
    sql += returning_columns;
    return sql;
}

}  // namespace detail

// =========================================================================
// PgRepo - CRUD operations with L3 (database) access only
// =========================================================================
//
// find() returns epoch-guarded CacheView. findJson()/findBinary() return
// serialized data by value (std::string / std::vector<uint8_t>).

template<typename E, config::FixedString Name, config::CacheConfig Cfg, typename Key>
requires ReadableEntity<E>
class PgRepo {
    using Mapping = typename E::MappingType;

public:
    using EntityType = E;
    using KeyType = Key;
    using WrapperType = E;
    using FindResultType = cache::CacheView<E>;

    static constexpr auto config = Cfg;
    static constexpr const char* name() { return Name; }

    // =====================================================================
    // Find by ID (L3: database only)
    // =====================================================================

    /// Find by ID. Returns epoch-guarded CacheView (empty if not found).
    static io::Immediate<cache::CacheView<E>> find(const Key& id) {
        return findTask(id);
    }

    // =====================================================================
    // Find by ID — JSON serialization (L3: database only)
    // =====================================================================

    /// Find by ID and return JSON string (empty if not found). A successful but
    /// empty result is the only "absent" signal; every L3 error propagates (a
    /// read timeout or DB-down must not masquerade as a missing row).
    static io::Immediate<std::string> findJson(const Key& id) {
        return findJsonTask(id);
    }

    // =====================================================================
    // Find by ID — binary serialization (L3: database only)
    // =====================================================================

    /// Find by ID and return binary (BEVE) vector (empty if not found).
    static io::Immediate<std::vector<uint8_t>> findBinary(const Key& id)
        requires HasBinarySerialization<E>
    {
        return findBinaryTask(id);
    }

    // =====================================================================
    // insert
    // =====================================================================

    /// Insert entity in database. Returns epoch-guarded CacheView (empty on error).
    static io::Task<cache::CacheView<E>> insert(const E& entity)
        requires MutableEntity<E> && (!Cfg.read_only)
    {
        auto result = co_await insertRaw(entity);
        if (!result) co_return {};
        co_return makeView(std::move(*result));
    }

    // =====================================================================
    // Upsert (INSERT ... ON CONFLICT DO UPDATE)
    // =====================================================================

    /// Upsert entity in database. Returns epoch-guarded CacheView of the row as
    /// it stands after conflict resolution (empty on error). Emitted only for
    /// caller-assigned-PK entities (HasUpsertSql).
    static io::Task<cache::CacheView<E>> upsert(const E& entity)
        requires UpsertableEntity<E, Key> && HasUpsertSql<E> && (!Cfg.read_only)
    {
        auto result = co_await upsertRaw(entity);
        if (!result) co_return {};
        co_return makeView(std::move(*result));
    }

    // =====================================================================
    // Update
    // =====================================================================

    /// Full update of entity in database.
    /// Returns: rows affected (0 if not found), or nullopt on DB error.
    static io::Task<std::optional<size_t>> update(const Key& id, const E& entity)
        requires MutableEntity<E> && HasFullUpdate<E> && (!Cfg.read_only)
    {
        auto outcome = co_await updateOutcome(id, entity);
        co_return outcome.affected;
    }

    // =====================================================================
    // Erase
    // =====================================================================

    /// Erase entity by ID.
    /// Returns: rows deleted (0 if not found), or nullopt on DB error.
    static io::Task<std::optional<size_t>> erase(const Key& id)
        requires (!Cfg.read_only)
    {
        co_return co_await eraseImpl(id, nullptr);
    }

protected:
    /// Internal erase with optional entity hint (for partition pruning).
    static io::Task<std::optional<size_t>> eraseImpl(
        const Key& id, const E* hint = nullptr)
        requires (!Cfg.read_only)
    {
        auto outcome = co_await eraseOutcome(id, hint);
        co_return outcome.affected;
    }

public:

    // =====================================================================
    // Partial update (patch)
    // =====================================================================

    /// Partial update. Returns epoch-guarded CacheView (empty on error).
    template<typename... Updates>
    static io::Task<cache::CacheView<E>> patch(const Key& id, Updates&&... updates)
        requires HasFieldUpdate<E> && (!Cfg.read_only)
    {
        auto row = co_await patchRow<false>(id, std::forward<Updates>(updates)...);
        co_return std::move(row.after);
    }

    /// Guarded partial update: applies `updates` only if the row satisfies
    /// `guard` at write time, evaluated by the database in the same statement.
    /// nullopt on DB error; an empty view when the guard is false or the row
    /// is absent; otherwise the committed row.
    template<typename... Gs, typename... Updates>
    static io::Task<std::optional<cache::CacheView<E>>> patchIf(
        const Key& id, const entity::Guards<Gs...>& guard, Updates&&... updates)
        requires HasFieldUpdate<E> && (!Cfg.read_only)
    {
        auto row = co_await patchIfRow<false>(id, guard, std::forward<Updates>(updates)...);
        if (!row) co_return std::nullopt;
        co_return std::move(row->after);
    }

    // =====================================================================
    // Invalidation pass-through (public interface)
    // =====================================================================

    static io::Task<void> invalidate([[maybe_unused]] const Key& id) {
        co_return;
    }

    template<typename... GroupArgs>
    static std::string makeGroupKey(GroupArgs&&... groupParts) {
        return makeListGroupKey(std::forward<GroupArgs>(groupParts)...);
    }

    static io::Task<size_t> invalidateListGroupByKey(
        [[maybe_unused]] const std::string& groupKey,
        [[maybe_unused]] int64_t entity_sort_val)
    {
        co_return 0;
    }

    static io::Task<size_t> invalidateAllListGroups()
    {
        co_return 0;
    }

protected:

    // =====================================================================
    // Batch invalidation common path (chemin commun)
    // =====================================================================
    //
    // The batch invalidation cascade (invalidateMany/eraseMany/...) is split into
    // two passes that each flow through the mixin chain (fire-and-forget cleanup):
    //   - invalidateManyCritical — the latency-critical, awaited work: L1 evict +
    //     L2 entity UNLINK + gen bump + L1 list-tracker bump. Kept synchronous so
    //     erase*/invalidate* return only once the entity tier is coherent (the
    //     read-fill recheck cannot reject a strictly-after stale entry, so the
    //     entity UNLINK must precede the caller's return).
    //   - invalidateManyDeferred — the detachable, order-free work: deduplicated
    //     cross-target invalidation + selective L2 list EVALs. Fired fire-and-forget
    //     by the facade (invalidate-stale tolerated; L1 list reads stay guarded by
    //     the synchronous tracker bump, L2 list/cross-target staleness is l2_ttl-
    //     bounded). Repo::invalidateManyImpl awaits both — the reference path the
    //     cascade unit tests reach via TestInternals.
    // L3 (this layer) owns no entity cache, so both passes are no-op terminators.
    //
    // WithLists gates the OWN-list tier (ListMixin): eraseMany passes true (the
    // entity left the table → its list pages change, like mono erase), while
    // invalidateMany passes false (the entity still exists → its lists are
    // untouched, like mono invalidate). Cross-inval + entity evict run in both.

    template<bool WithLists = true>
    static io::Task<void> invalidateManyCritical(
        [[maybe_unused]] std::span<const E> entities) {
        co_return;
    }

    template<bool WithLists = true>
    static io::Task<void> invalidateManyDeferred(
        [[maybe_unused]] std::span<const E> entities) {
        co_return;
    }

    /// Batch cascade for rows changed in place (patchWhere): same critical /
    /// deferred split as above, fed with each row before and after the write.
    /// The row still exists, so its own lists move from the old version's
    /// pages to the new one's. L3 has no cache: terminal no-ops.
    static io::Task<void> invalidateManyUpdatedCritical(
        [[maybe_unused]] std::span<const Change<E>> changes) {
        co_return;
    }

    static io::Task<void> invalidateManyUpdatedDeferred(
        [[maybe_unused]] std::span<const Change<E>> changes) {
        co_return;
    }

    /// Terminal no-ops for the predicate list fast-path (eraseWhere). L3 has no
    /// list cache; the real work lives in ListMixin when present. Always-present
    /// bottom of the chain so the call resolves even without a ListDescriptor.
    template<typename Desc>
    static io::Task<void> invalidateWhereListsCritical(
        [[maybe_unused]] const list::spec::Filters<Desc>& predicate) {
        co_return;
    }

    template<typename Desc>
    static io::Task<void> invalidateWhereListsDeferred(
        [[maybe_unused]] const list::spec::Filters<Desc>& predicate) {
        co_return;
    }

    // =====================================================================
    // Epoch memory pool for temporary entity allocations
    // =====================================================================

    static epoch::memory_pool<E>& pool() {
        // Force epoch_s + ThreadIdPool construction before the pool, so they
        // outlive ~memory_pool()→clear()→num_workers() at static teardown
        // (statics are destroyed in reverse construction order). Without this,
        // ThreadIdPool is created lazily on the first Retire — after the pool —
        // and freed before it, yielding a use-after-free at exit. Mirrors
        // parlay's own guard in get_default_pool().
        static const int deps [[maybe_unused]] =
            (epoch::internal::get_epoch(), parlay::num_thread_ids(), 0);
        static epoch::memory_pool<E> p;
        return p;
    }

    /// Allocate entity in pool, retire immediately, return epoch-guarded view.
    static cache::CacheView<E> makeView(E entity) {
        auto guard = epoch::EpochGuard::acquire();
        auto* ptr = pool().New(std::move(entity));
        pool().Retire(ptr);
        return cache::CacheView<E>(ptr, std::move(guard));
    }

    // =====================================================================
    // Read coroutines behind the Immediate-returning find* wrappers
    // =====================================================================

    static io::Task<cache::CacheView<E>> findTask(const Key& id) {
        auto entity = co_await findRaw(id);
        if (!entity) co_return {};
        co_return makeView(std::move(*entity));
    }

    static io::Task<std::string> findJsonTask(const Key& id) {
        auto params = io::PgParams::fromKey(id);
        auto result = co_await PgProvider::queryParams(
            Mapping::SQL::select_by_pk, params);
        if (result.empty()) co_return {};
        auto entity = E::fromRow(result[0]);
        if (!entity) co_return {};
        co_return entity->json();
    }

    static io::Task<std::vector<uint8_t>> findBinaryTask(const Key& id)
        requires HasBinarySerialization<E>
    {
        auto params = io::PgParams::fromKey(id);
        auto result = co_await PgProvider::queryParams(
            Mapping::SQL::select_by_pk, params);
        if (result.empty()) co_return {};
        auto entity = E::fromRow(result[0]);
        if (!entity) co_return {};
        co_return entity->binary();
    }

    // =====================================================================
    // Raw methods returning entity by value (for LocalRepo move path)
    // =====================================================================

    /// Find by ID, returning entity by value (no pool/view allocation).
    /// Routes through submitEntityRead for ANY-array batching.
    static io::Task<std::optional<E>> findRaw(const Key& id) {
        auto params = io::PgParams::fromKey(id);
        auto result = co_await PgProvider::entityQueryParams(
            Mapping::SQL::select_by_pk_batch,
            Mapping::SQL::select_by_pk, params);
        // A successful empty result means "absent"; any L3 error propagates so a
        // read timeout / DB-down is never confused with a missing row.
        if (result.empty()) co_return std::nullopt;
        co_return E::fromRow(result[0]);
    }

    /// Batched find by IDs via WHERE pk = ANY($1). Returns one optional<E> per
    /// id, aligned on request order (nullopt for absent ids). Submits ONE
    /// multi-key entity entry: when it lands in a batch its keys fuse with
    /// concurrent single finds into a single deduplicated ANY (K segments → 1),
    /// while remaining one alloc / one frame for the isolated findMany.
    /// Precondition: ids already deduplicated (dedup lives at the public entry,
    /// LocalRepo::findMany); no re-dedup here.
    static io::Task<std::vector<std::optional<E>>> findManyRaw(std::span<const Key> ids) {
        std::vector<std::optional<E>> out(ids.size());
        if (ids.empty()) co_return out;

        // One PgParams per requested key; the scheduler folds them (with any
        // concurrent finds) into the shared ANY array at fire time.
        std::vector<io::PgParams> keyParams;
        keyParams.reserve(ids.size());
        for (const auto& id : ids)
            keyParams.push_back(io::PgParams::fromKey(id));

        // A successful query yields the matched rows (absent ids stay nullopt);
        // any L3 error propagates rather than collapsing into an all-absent
        // vector that a caller could not tell apart from a genuine no-match.
        auto result = co_await PgProvider::entityQueryParamsMany(
            Mapping::SQL::select_by_pk_batch,
            Mapping::SQL::select_by_pk, std::move(keyParams));

        // ANY returns rows unordered and omits absent ids — match each row
        // back to its requested position by primary key (mirrors
        // distributeAnyResults). Linear match: N is small.
        const int n = result.rows();
        for (int r = 0; r < n; ++r) {
            auto entity = E::fromRow(result[r]);
            if (!entity) continue;
            auto k = entity->key();
            for (size_t i = 0; i < ids.size(); ++i) {
                if (!out[i] && ids[i] == k) {
                    out[i] = std::move(entity);
                    break;
                }
            }
        }
        co_return out;
    }

    /// Batched erase by enumerated keys via `delete_by_pk_batch`
    /// (WHERE pk = ANY($1) RETURNING *). One statement = deletion + the deleted
    /// rows, mirroring the mono-key `old` capture: the returned entities feed
    /// downstream cross-tier invalidation. Returns the entities actually deleted
    /// (absent ids contribute nothing) — size ≤ ids.size(), order is PG's.
    /// Precondition: ids deduplicated upstream. No partition pruning: pk=ANY
    /// scans all partitions (cf. plan §3); RETURNING still carries the partition
    /// column so callers can derive the L1 single-partition evict hint.
    /// nullopt signals a DB error (parité mono erase, whose EraseOutcome.affected
    /// is nullopt on PgError); an empty vector means "no matching row" — the
    /// public eraseMany maps the two to nullopt vs a 0 count.
    static io::Task<std::optional<std::vector<E>>> eraseManyRaw(std::span<const Key> ids)
        requires (!Cfg.read_only)
    {
        std::vector<E> out;
        if (ids.empty()) co_return out;
        try {
            std::vector<io::PgParams> keyParams;
            keyParams.reserve(ids.size());
            for (const auto& id : ids)
                keyParams.push_back(io::PgParams::fromKey(id));
            auto arrayParams = io::PgParams::buildArrayLiteral(keyParams);

            // Write path (seq-ordered): an eraseMany sequenced after an insert/
            // update of the same PK in the same flow lands in seq order. The
            // returned PgWriteResult carries the RETURNING rows; coalesced is
            // irrelevant — upper layers invalidate from the returned entities.
            auto w = co_await PgProvider::queryWrite(
                Mapping::SQL::delete_by_pk_batch, arrayParams);

            out.reserve(static_cast<size_t>(w.result.rows()));
            for (int r = 0; r < w.result.rows(); ++r) {
                if (auto e = E::fromRow(w.result[r]))
                    out.push_back(std::move(*e));
            }
            co_return out;
        } catch (const io::PgUncertainError&) {
            // Uncertain: the batch DELETE may have committed (lost ACK). Propagate
            // so the facade evicts by precaution; nullopt stays the deterministic
            // failure (DB unchanged).
            throw;
        } catch (const io::PgError& e) {
            RELAIS_LOG_ERROR << name() << ": eraseManyRaw DB error - " << e.what();
            co_return std::nullopt;
        }
    }

    /// Predicate erase: deletes every row matching `filters`, in K_pg-bounded
    /// chunks (detail::buildDeleteWhereSql), looping until a chunk deletes 0 rows.
    /// Returns ALL deleted entities (RETURNING per chunk, accumulated) so the
    /// caller drives chunk-by-chunk invalidation. The same statement re-runs each
    /// round: the ctid sub-select keeps matching the not-yet-deleted remainder, so
    /// the bound caps lock/WAL pressure per statement, never the total set.
    ///
    /// nullopt signals a DB error (parité mono erase / eraseManyRaw): a failure
    /// mid-loop is indistinguishable from "nothing matched" otherwise. An empty
    /// vector means the predicate matched no row. RETURNING projects all columns
    /// (the `*`-equivalent, always valid); compile-time column projection is a
    /// bandwidth optimisation deferred (never affects correctness).
    template<typename Descriptor>
        requires list::spec::ValidFilterSet<Descriptor> && (!Cfg.read_only)
    static io::Task<std::optional<std::vector<E>>> eraseWhereRaw(
        const list::spec::Filters<Descriptor>& filters)
    {
        std::vector<E> out;
        try {
            auto where = list::spec::buildWhereClause<Descriptor>(filters);
            const std::string sql = detail::buildDeleteWhereSql(
                Mapping::table_name, Mapping::SQL::returning_columns,
                where.sql, detail::kPgEraseChunk);

            for (;;) {
                auto result = co_await PgProvider::queryParams(
                    sql.c_str(), where.params);
                const int n = result.rows();
                if (n == 0) break;
                out.reserve(out.size() + static_cast<size_t>(n));
                for (int r = 0; r < n; ++r) {
                    if (auto e = E::fromRow(result[r]))
                        out.push_back(std::move(*e));
                }
                // A short chunk (< K_pg) means the predicate is exhausted; skip
                // the extra round-trip that would only confirm 0 rows.
                if (static_cast<size_t>(n) < detail::kPgEraseChunk) break;
            }
            co_return out;
        } catch (const io::PgUncertainError&) {
            // Uncertain: a chunk DELETE may have committed. Propagate so the
            // facade invalidates by precaution; nullopt stays the deterministic
            // failure.
            throw;
        } catch (const io::PgError& e) {
            RELAIS_LOG_ERROR << name() << ": eraseWhereRaw DB error - " << e.what();
            co_return std::nullopt;
        }
    }

    /// Predicate select: the read-side twin of eraseWhereRaw. Resolves the rows
    /// matching `filters` (SELECT <cols> FROM t WHERE pred) so invalidateWhere can
    /// materialise the affected entities WITHOUT deleting — they still exist, so
    /// their lists stay valid (cf. invalidateMany WithLists=false). No chunking:
    /// invalidation evicts cache, it does not hold a write lock. nullopt = DB
    /// error (parité). Projects all columns (the `*`-equivalent).
    template<typename Descriptor>
        requires list::spec::ValidFilterSet<Descriptor>
    static io::Task<std::optional<std::vector<E>>> selectWhereRaw(
        const list::spec::Filters<Descriptor>& filters)
    {
        // Read path: a successful query (even empty) returns the matched set;
        // any L3 error propagates (no swallow-to-nullopt), so invalidateWhere
        // surfaces a resolve failure instead of silently evicting nothing.
        auto where = list::spec::buildWhereClause<Descriptor>(filters);
        std::string sql;
        sql.reserve(128);
        sql += "SELECT ";
        sql += Mapping::SQL::returning_columns;
        sql += " FROM ";
        sql += Mapping::table_name;
        if (!where.sql.empty()) {
            sql += " WHERE ";
            sql += where.sql;
        }

        auto result = co_await PgProvider::queryParams(sql.c_str(), where.params);
        std::vector<E> out;
        out.reserve(static_cast<size_t>(result.rows()));
        for (int r = 0; r < result.rows(); ++r) {
            if (auto e = E::fromRow(result[r]))
                out.push_back(std::move(*e));
        }
        co_return out;
    }

    /// Insert entity in database, returning entity by value.
    static io::Task<std::optional<E>> insertRaw(const E& entity)
        requires MutableEntity<E> && (!Cfg.read_only)
    {
        try {
            auto params = E::toInsertParams(entity);
            // Write path (seq-ordered): keeps insert in seq with update/erase of
            // the same PK. coalesced ignored — two inserts with identical params
            // yield distinct PKs, so coalescing is effectively unreachable.
            auto w = co_await PgProvider::queryWrite(
                Mapping::SQL::insert, params);
            if (w.result.empty()) co_return std::nullopt;
            co_return E::fromRow(w.result[0]);
        } catch (const io::PgUncertainError&) {
            // Uncertain: the INSERT may have committed (RETURNING id lost).
            // Propagate so list/cross tiers invalidate from the input entity;
            // nullopt stays the deterministic failure.
            throw;
        } catch (const io::PgError& e) {
            RELAIS_LOG_ERROR << name() << ": insert error - " << e.what();
            co_return std::nullopt;
        }
    }

    /// Upsert entity in database, returning entity by value. Same params as
    /// insert ($1..$n); the ON CONFLICT SET pulls its values from the proposed
    /// row (EXCLUDED), so no extra parameters. RETURNING projects all columns —
    /// the returned entity is the row after conflict resolution (create OR
    /// update), reconstructed by fromRow exactly like insert.
    static io::Task<std::optional<E>> upsertRaw(const E& entity)
        requires UpsertableEntity<E, Key> && HasUpsertSql<E> && (!Cfg.read_only)
    {
        try {
            auto params = E::toInsertParams(entity);
            auto w = co_await PgProvider::queryWrite(
                Mapping::SQL::upsert, params);
            if (w.result.empty()) co_return std::nullopt;
            co_return E::fromRow(w.result[0]);
        } catch (const io::PgUncertainError&) {
            // Uncertain: the upsert may have committed (RETURNING lost). Propagate
            // so upper tiers evict by precaution — resilience follows the UPDATE
            // model (the key may pre-exist with a now-stale value), not insert.
            throw;
        } catch (const io::PgError& e) {
            RELAIS_LOG_ERROR << name() << ": upsert error - " << e.what();
            co_return std::nullopt;
        }
    }

    /// Build a conditional-write statement: `build(pk)` gets the key column
    /// (or the std::array of them for a composite key).
    template<typename Build>
    static std::string withPk(Build&& build) {
        if constexpr (is_tuple_v<Key>) return build(Mapping::primary_key_columns);
        else return build(Mapping::primary_key_column);
    }

    /// Width of one row version in a conditional write's RETURNING list.
    static constexpr int kRowWidth = detail::countColumns(Mapping::SQL::returning_columns);

    /// Parameters of a conditional write: the SET values of `updates`, with
    /// room reserved for the `extra` that follow (key, guard, ordering).
    template<typename... Updates>
    static io::PgParams setParams(size_t extra, Updates&&... updates) {
        io::PgParams params;
        params.params.reserve(sizeof...(Updates) + extra);
        (params.push(entity::fieldValue<typename E::TraitsType>(
            std::forward<Updates>(updates))), ...);
        return params;
    }

    static void pushKey(io::PgParams& params, const Key& id) {
        if constexpr (is_tuple_v<Key>) {
            std::apply([&](const auto&... col) { (params.push(col), ...); }, id);
        } else {
            params.push(id);
        }
    }

    /// One row of a conditional write returning `Rows`, if it decodes.
    template<Returns Rows>
    static std::optional<RowFor<E, Rows>> decodeRow(const io::PgResult::Row& row) {
        if constexpr (Rows == Returns::Changes) {
            auto before = E::fromRow(row);
            auto after = E::fromRow(row.shifted(kRowWidth));
            if (!before || !after) return std::nullopt;
            return Change<E>{std::move(*before), std::move(*after)};
        } else {
            return E::fromRow(row);
        }
    }

    /// The rows of a conditional write returning `Rows`: the affected count, or
    /// each row decoded, in `order` (row indices) when given.
    template<Returns Rows>
    static RowsFor<E, Rows> decodeRows(const io::PgResult& result,
                                       std::span<const int> order = {}) {
        if constexpr (Rows == Returns::Count) {
            return static_cast<size_t>(result.affectedRows());
        } else {
            const int n = result.rows();
            RowsFor<E, Rows> out;
            out.reserve(static_cast<size_t>(n));
            for (int i = 0; i < n; ++i) {
                if (auto row = decodeRow<Rows>(result[order.empty() ? i : order[i]]))
                    out.push_back(std::move(*row));
            }
            return out;
        }
    }

    /// The keyed patch statement returning `Rows` for these updates (and
    /// guard, unless void).
    template<Returns Rows, typename Guard, typename... Updates>
    static std::string keyedPatchSql() {
        using Traits = typename E::TraitsType;
        return withPk([](const auto& pk) {
            return detail::buildKeyedPatchSql<Traits, Guard>(
                Mapping::table_name, pk,
                {detail::SetColumn(
                    entity::fieldColumnName<Traits>(std::remove_cvref_t<Updates>{}),
                    entity::set_op_v<std::remove_cvref_t<Updates>>)...},
                Mapping::SQL::returning_columns, Rows);
        });
    }

    /// The committed row of a keyed patch, with the row it replaced when the
    /// tiers above asked for it (WithBefore).
    struct Patched {
        std::optional<E> before;
        E after;
    };

    template<bool WithBefore>
    static std::optional<Patched> decodePatched(const io::PgResult& result) {
        if (result.empty()) return std::nullopt;
        if constexpr (WithBefore) {
            auto change = decodeRow<Returns::Changes>(result[0]);
            if (!change) return std::nullopt;
            return Patched{std::move(change->before), std::move(change->after)};
        } else {
            auto after = decodeRow<Returns::After>(result[0]);
            if (!after) return std::nullopt;
            return Patched{std::nullopt, std::move(*after)};
        }
    }

    /// A keyed patch as the tiers above see it: a view of the committed row,
    /// and the row the write replaced (WithBefore). Both empty when nothing
    /// was written (row absent, guard false, or DB error).
    struct PatchedRow {
        std::optional<E> before;
        cache::CacheView<E> after;
    };

    /// Partial update, returning the committed row, and the one it replaced
    /// when WithBefore (only the list and cross-invalidation cascades read it:
    /// without it the statement is a plain UPDATE ... RETURNING). nullopt when
    /// the row is absent or on a DB error.
    template<bool WithBefore, typename... Updates>
    static io::Task<std::optional<Patched>> patchRaw(const Key& id, Updates&&... updates)
        requires HasFieldUpdate<E> && (!Cfg.read_only)
    {
        static_assert(sizeof...(Updates) > 0, "patch requires at least one field update");
        constexpr auto kRows = WithBefore ? Returns::Changes : Returns::After;
        // A relative update re-applies on every execution: two identical ones
        // must both run, so the write opts out of coalescing.
        constexpr auto mode =
            (entity::is_relative_update_v<Updates> || ...)
                ? io::batch::WriteMode::Exclusive
                : io::batch::WriteMode::Idempotent;
        try {
            static const auto sql = keyedPatchSql<kRows, void, Updates...>();

            auto params = setParams(io::PgParams::keyParamCount<Key>(),
                                    std::forward<Updates>(updates)...);
            pushKey(params, id);

            // Write path (seq-ordered): keeps patch in seq with update/erase of
            // the same PK. An absolute patch is idempotent, so a coalesced
            // follower observes the same change; a relative one is Exclusive and
            // always runs on its own.
            auto w = co_await PgProvider::queryWrite(sql.c_str(), params, mode);
            co_return decodePatched<WithBefore>(w.result);
        } catch (const io::PgUncertainError&) {
            // Uncertain: the UPDATE may have committed. Propagate; the cache
            // tiers above evict by precaution as it unwinds.
            throw;
        } catch (const io::PgError& e) {
            RELAIS_LOG_ERROR << name() << ": patch error - " << e.what();
            co_return std::nullopt;
        }
    }

    /// Outcome of a guarded patch. Refusal and DB error must stay apart: a
    /// false guard is a business answer, an error is a failure.
    struct GuardedPatchOutcome {
        std::optional<Patched> patched;  ///< empty if refused or absent
        bool error = false;              ///< deterministic DB error
    };

    /// Guarded partial update, returning the outcome by value (WithBefore: see
    /// patchRaw).
    template<bool WithBefore, typename... Gs, typename... Updates>
    static io::Task<GuardedPatchOutcome> patchIfRaw(
        const Key& id, const entity::Guards<Gs...>& guard, Updates&&... updates)
        requires HasFieldUpdate<E> && (!Cfg.read_only)
    {
        static_assert(sizeof...(Gs) > 0, "patchIf requires at least one guard");
        static_assert(sizeof...(Updates) > 0, "patchIf requires at least one field update");
        using Traits = typename E::TraitsType;
        using Guard = entity::Guards<Gs...>;
        constexpr auto kRows = WithBefore ? Returns::Changes : Returns::After;
        try {
            static const auto sql = keyedPatchSql<kRows, Guard, Updates...>();

            auto params = setParams(
                io::PgParams::keyParamCount<Key>() + detail::GuardSql<Traits, Guard>::params,
                std::forward<Updates>(updates)...);
            pushKey(params, id);
            detail::GuardSql<Traits, Guard>::bind(params, guard);

            // The guard is a decision the database takes on each execution: two
            // identical guarded writes must both be evaluated, never coalesced.
            auto w = co_await PgProvider::queryWrite(
                sql.c_str(), params, io::batch::WriteMode::Exclusive);
            co_return GuardedPatchOutcome{decodePatched<WithBefore>(w.result)};
        } catch (const io::PgUncertainError&) {
            // Uncertain: the UPDATE may have committed. Propagate; the cache
            // tiers above evict by precaution as it unwinds, as for patch.
            throw;
        } catch (const io::PgError& e) {
            RELAIS_LOG_ERROR << name() << ": patchIf error - " << e.what();
            co_return GuardedPatchOutcome{.error = true};
        }
    }

    /// Partial update as the tiers above see it (see PatchedRow).
    template<bool WithBefore, typename... Updates>
    static io::Task<PatchedRow> patchRow(const Key& id, Updates&&... updates)
        requires HasFieldUpdate<E> && (!Cfg.read_only)
    {
        auto patched = co_await patchRaw<WithBefore>(id, std::forward<Updates>(updates)...);
        if (!patched) co_return PatchedRow{};
        co_return PatchedRow{std::move(patched->before), makeView(std::move(patched->after))};
    }

    /// Guarded partial update as the tiers above see it. nullopt on DB error.
    template<bool WithBefore, typename... Gs, typename... Updates>
    static io::Task<std::optional<PatchedRow>> patchIfRow(
        const Key& id, const entity::Guards<Gs...>& guard, Updates&&... updates)
        requires HasFieldUpdate<E> && (!Cfg.read_only)
    {
        auto outcome = co_await patchIfRaw<WithBefore>(id, guard, std::forward<Updates>(updates)...);
        if (outcome.error) co_return std::nullopt;
        if (!outcome.patched) co_return PatchedRow{};
        co_return PatchedRow{std::move(outcome.patched->before),
                             makeView(std::move(outcome.patched->after))};
    }

    /// Predicate partial update, returning per `Rows` the number of rows
    /// changed, each committed row, or each row before and after. nullopt on a
    /// deterministic DB error; zero or an empty vector when no row matched.
    /// One statement, never chunked: a changed row may still match the
    /// predicate, so a chunked loop would not terminate.
    template<Returns Rows, typename... Gs, typename... Updates>
    static io::Task<ResultFor<E, Rows>> patchWhereRaw(
        const entity::Guards<Gs...>& pred, Updates&&... updates)
        requires HasFieldUpdate<E> && (!Cfg.read_only)
    {
        static_assert(sizeof...(Gs) > 0,
            "patchWhere requires a predicate: an empty one would change the whole table");
        static_assert(sizeof...(Updates) > 0, "patchWhere requires at least one field update");
        using Traits = typename E::TraitsType;
        using Pred = entity::Guards<Gs...>;
        try {
            static const auto sql = withPk([](const auto& pk) {
                return detail::buildPatchWhereSql<Traits, Pred>(
                    Mapping::table_name, pk,
                    {detail::SetColumn(
                        entity::fieldColumnName<Traits>(std::remove_cvref_t<Updates>{}),
                        entity::set_op_v<std::remove_cvref_t<Updates>>)...},
                    Mapping::SQL::returning_columns, Rows);
            });

            auto params = setParams(detail::GuardSql<Traits, Pred>::params,
                                    std::forward<Updates>(updates)...);
            detail::GuardSql<Traits, Pred>::bind(params, pred);

            // The predicate is re-evaluated by the database on each execution:
            // two identical writes must both run, never coalesced.
            auto w = co_await PgProvider::queryWrite(
                sql.c_str(), params, io::batch::WriteMode::Exclusive);
            co_return decodeRows<Rows>(w.result);
        } catch (const io::PgUncertainError&) {
            // Uncertain: the UPDATE may have committed, and the changed rows are
            // unknowable. Propagate; the facade logs the residual staleness.
            throw;
        } catch (const io::PgError& e) {
            RELAIS_LOG_ERROR << name() << ": patchWhere error - " << e.what();
            co_return std::nullopt;
        }
    }

    /// Claim: change the first `n` rows of `order` (void = primary key) among
    /// those matching `pred`, returning per `Rows` their count, each committed
    /// row, or each row before and after, in that order. nullopt on a
    /// deterministic DB error; zero or an empty vector when no row was taken
    /// (Exact: fewer than n candidates, nothing written).
    template<ClaimOptions O, Returns Rows, typename Order, typename... Gs, typename... Updates>
    static io::Task<ResultFor<E, Rows>> claimRaw(
        const entity::Guards<Gs...>& pred, const Order* order, size_t n,
        Updates&&... updates)
        requires HasFieldUpdate<E> && (!Cfg.read_only)
    {
        static_assert(sizeof...(Gs) > 0,
            "claim requires a predicate: an empty one would take any row of the table");
        static_assert(sizeof...(Updates) > 0, "claim requires at least one field update");
        using Traits = typename E::TraitsType;
        using Pred = entity::Guards<Gs...>;
        if (n == 0) co_return RowsFor<E, Rows>{};
        try {
            static const auto sql = withPk([](const auto& pk) {
                return detail::buildClaimSql<Traits, Pred, Order>(
                    Mapping::table_name, pk,
                    {detail::SetColumn(
                        entity::fieldColumnName<Traits>(std::remove_cvref_t<Updates>{}),
                        entity::set_op_v<std::remove_cvref_t<Updates>>)...},
                    Mapping::SQL::returning_columns, O.mode, O.lock, Rows);
            });

            auto params = setParams(
                1 + detail::GuardSql<Traits, Pred>::params
                  + detail::OrderBySql<Traits, Order>::params,
                std::forward<Updates>(updates)...);
            params.push(static_cast<int64_t>(n));
            detail::GuardSql<Traits, Pred>::bind(params, pred);
            if constexpr (!std::is_void_v<Order>)
                detail::OrderBySql<Traits, Order>::bind(params, *order);

            // Two identical claims must take distinct rows: never coalesced.
            auto w = co_await PgProvider::queryWrite(
                sql.c_str(), params, io::batch::WriteMode::Exclusive);

            if constexpr (Rows == Returns::Count) {
                co_return decodeRows<Rows>(w.result);
            } else {
                // The rank follows the row version(s); decode in rank order.
                constexpr int kRankColumn =
                    (Rows == Returns::Changes ? 2 : 1) * kRowWidth;
                const int rows = w.result.rows();
                std::vector<std::pair<int64_t, int>> ranked;
                ranked.reserve(static_cast<size_t>(rows));
                for (int r = 0; r < rows; ++r)
                    ranked.emplace_back(
                        w.result[r].shifted(kRankColumn).template get<int64_t>(0), r);
                std::sort(ranked.begin(), ranked.end());
                std::vector<int> order_idx;
                order_idx.reserve(ranked.size());
                for (const auto& [rank, r] : ranked) order_idx.push_back(r);
                co_return decodeRows<Rows>(w.result, order_idx);
            }
        } catch (const io::PgUncertainError&) {
            // Uncertain: the claim may have committed, and the rows taken are
            // unknowable. Propagate; the facade logs the residual staleness.
            throw;
        } catch (const io::PgError& e) {
            RELAIS_LOG_ERROR << name() << ": claim error - " << e.what();
            co_return std::nullopt;
        }
    }

    // =====================================================================
    // Write outcome types (for write coalescing propagation)
    // =====================================================================
    //
    // When the BatchScheduler coalesces identical writes (same SQL + same
    // params), followers receive the leader's result with coalesced=true.
    // Upper layers (RedisRepo, LocalRepo) use this to skip redundant
    // cache operations (L1 evict, L2 Redis SET/DEL).

    struct WriteOutcome {
        std::optional<size_t> affected;
        bool coalesced = false;
    };

    struct EraseOutcome {
        std::optional<size_t> affected;
        bool coalesced = false;
    };

    /// Update returning full outcome (success + coalesced flag).
    static io::Task<WriteOutcome> updateOutcome(const Key& id, const E& entity)
        requires MutableEntity<E> && HasFullUpdate<E> && (!Cfg.read_only)
    {
        try {
            auto keyParams = io::PgParams::fromKey(id);
            io::PgParams fieldParams = E::toUpdateParams(entity);
            io::PgParams params;
            params.params.reserve(keyParams.params.size() + fieldParams.params.size());
            for (auto& p : keyParams.params)
                params.params.push_back(std::move(p));
            for (auto& p : fieldParams.params)
                params.params.push_back(std::move(p));

            auto [result, coalesced] = co_await PgProvider::queryWrite(
                Mapping::SQL::update, params);
            co_return WriteOutcome{
                static_cast<size_t>(result.affectedRows()), coalesced};

        } catch (const io::PgUncertainError&) {
            // Uncertain: the UPDATE may have committed (lost ACK). Propagate so
            // each layer evicts its tier by precaution on the way out; the empty
            // WriteOutcome (nullopt) stays the deterministic failure (DB
            // unchanged) that keeps invalidations skipped.
            throw;
        } catch (const io::PgError& e) {
            RELAIS_LOG_ERROR << name() << ": update error - " << e.what();
            co_return WriteOutcome{};
        }
    }

    /// Erase returning full outcome (affected + coalesced flag).
    static io::Task<EraseOutcome> eraseOutcome(
        const Key& id, const E* hint = nullptr)
        requires (!Cfg.read_only)
    {
        try {
            io::batch::PgWriteResult w;
            if constexpr (HasPartitionHint<E>) {
                if (hint) {
                    auto params = Mapping::makePartitionHintParams(*hint);
                    w = co_await PgProvider::queryWrite(
                        Mapping::SQL::delete_with_partition, params);
                } else {
                    auto params = io::PgParams::fromKey(id);
                    w = co_await PgProvider::queryWrite(
                        Mapping::SQL::delete_by_pk, params);
                }
            } else {
                auto params = io::PgParams::fromKey(id);
                w = co_await PgProvider::queryWrite(
                    Mapping::SQL::delete_by_pk, params);
            }
            co_return EraseOutcome{
                static_cast<size_t>(w.result.affectedRows()), w.coalesced};
        } catch (const io::PgUncertainError&) {
            // Uncertain: the DELETE may have committed. Propagate so each layer
            // evicts its tier by precaution; the empty EraseOutcome (nullopt)
            // stays the deterministic failure (DB unchanged).
            throw;
        } catch (const io::PgError& e) {
            RELAIS_LOG_ERROR << name() << ": erase error - " << e.what();
            co_return EraseOutcome{};
        }
    }

    // =====================================================================
    // List query pass-through methods (no caching at L3 level)
    // =====================================================================

    template<typename... Args>
    static std::string makeListCacheKey(Args&&... args) {
        std::string key = std::string(name()) + ":list";
        ((key += ":" + toString(std::forward<Args>(args))), ...);
        return key;
    }

    template<typename... GroupArgs>
    static std::string makeListGroupKey(GroupArgs&&... groupParts) {
        std::string key = std::string(name()) + ":list";
        ((key += ":" + toString(std::forward<GroupArgs>(groupParts))), ...);
        return key;
    }

    template<typename QueryFn, typename... KeyArgs>
    static io::Task<std::vector<E>> cachedList(
        QueryFn&& query,
        [[maybe_unused]] KeyArgs&&... keyParts)
    {
        co_return co_await query();
    }

    template<typename QueryFn, typename... GroupArgs>
    static io::Task<std::vector<E>> cachedListTracked(
        QueryFn&& query,
        [[maybe_unused]] int limit,
        [[maybe_unused]] int offset,
        [[maybe_unused]] GroupArgs&&... groupParts)
    {
        co_return co_await query();
    }

    template<typename QueryFn, typename HeaderBuilder, typename... GroupArgs>
    static io::Task<std::vector<E>> cachedListTrackedWithHeader(
        QueryFn&& query,
        [[maybe_unused]] int limit,
        [[maybe_unused]] int offset,
        [[maybe_unused]] HeaderBuilder&& headerBuilder,
        [[maybe_unused]] GroupArgs&&... groupParts)
    {
        co_return co_await query();
    }

    template<typename... GroupArgs>
    static io::Task<size_t> invalidateListGroup(
        [[maybe_unused]] GroupArgs&&... groupParts)
    {
        co_return 0;
    }

    template<typename... GroupArgs>
    static io::Task<size_t> invalidateListGroupSelective(
        [[maybe_unused]] int64_t entity_sort_val,
        [[maybe_unused]] GroupArgs&&... groupParts)
    {
        co_return 0;
    }

    template<typename... GroupArgs>
    static io::Task<size_t> invalidateListGroupSelectiveUpdate(
        [[maybe_unused]] int64_t old_sort_val,
        [[maybe_unused]] int64_t new_sort_val,
        [[maybe_unused]] GroupArgs&&... groupParts)
    {
        co_return 0;
    }

    template<typename ListEntity, typename QueryFn, typename... KeyArgs>
    static io::Task<ListEntity> cachedListAs(
        QueryFn&& query,
        [[maybe_unused]] KeyArgs&&... keyParts)
    {
        co_return co_await query();
    }

    template<typename ListEntity, typename QueryFn, typename... GroupArgs>
    static io::Task<ListEntity> cachedListAsTracked(
        QueryFn&& query,
        [[maybe_unused]] int limit,
        [[maybe_unused]] int offset,
        [[maybe_unused]] GroupArgs&&... groupParts)
    {
        co_return co_await query();
    }

    template<typename ListEntity, typename QueryFn, typename HeaderBuilder, typename... GroupArgs>
    static io::Task<ListEntity> cachedListAsTrackedWithHeader(
        QueryFn&& query,
        [[maybe_unused]] int limit,
        [[maybe_unused]] int offset,
        [[maybe_unused]] HeaderBuilder&& headerBuilder,
        [[maybe_unused]] GroupArgs&&... groupParts)
    {
        co_return co_await query();
    }

    template<typename T>
    static std::string toString(const T& value) {
        if constexpr (is_tuple_v<T>) {
            std::string result;
            std::apply([&](const auto&... parts) {
                bool first = true;
                ((result += (first ? "" : ":"),
                  result += toString(parts),
                  first = false), ...);
            }, value);
            return result;
        } else if constexpr (std::is_integral_v<T>) {
            return std::to_string(value);
        } else {
            return std::string(value);
        }
    }

    friend struct ::relais_test::TestInternals;
};

}  // namespace jcailloux::relais

#endif //JCX_RELAIS_PGREPO_H
