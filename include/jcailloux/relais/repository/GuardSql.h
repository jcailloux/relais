#ifndef JCX_RELAIS_REPOSITORY_GUARD_SQL_H
#define JCX_RELAIS_REPOSITORY_GUARD_SQL_H

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "jcailloux/relais/entity/FieldGuard.h"
#include "jcailloux/relais/entity/FieldUpdate.h"
#include "jcailloux/relais/io/pg/PgParams.h"

namespace jcailloux::relais::detail {

// =============================================================================
// SQL emission and binding for typed guards and orderings
//
// The SQL of a guard depends on its type only, so callers build it once per
// instantiation (static). Values bind at runtime, in the same traversal order
// as emission: `emit` numbers placeholders from `param`, and `bind` pushes
// exactly `params` values. `qual` prefixes every column ("t." when the
// statement aliases the target table, empty otherwise).
// =============================================================================

[[nodiscard]] constexpr const char* cmpToSql(entity::Cmp c) noexcept {
    switch (c) {
        case entity::Cmp::Eq: return "=";
        case entity::Cmp::Ne: return "!=";
        case entity::Cmp::Gt: return ">";
        case entity::Cmp::Ge: return ">=";
        case entity::Cmp::Lt: return "<";
        case entity::Cmp::Le: return "<=";
    }
    return "=";
}

template<typename Traits, auto F>
void appendColumn(std::string& sql, std::string_view qual) {
    sql += qual;
    sql += Traits::template FieldInfo<F>::column_name;
}

/// A column whose enum is stored as text orders by its codec's rank.
template<typename Info>
inline constexpr bool has_codec_v = requires { typename Info::codec; };

/// An ordering guard on such a column compares ranks: its value must be an
/// enumerator.
template<typename Info, typename V>
inline constexpr bool ranks_enumerator_v = false;
template<typename Info, typename V>
    requires has_codec_v<Info>
inline constexpr bool ranks_enumerator_v<Info, V> = std::is_same_v<V, typename Info::enum_type>;

template<typename Traits, auto F>
void appendOrderedColumn(std::string& sql, std::string_view qual) {
    using Info = typename Traits::template FieldInfo<F>;
    if constexpr (has_codec_v<Info>) {
        Info::codec::appendRank(sql, qual, Info::column_name);
    } else {
        appendColumn<Traits, F>(sql, qual);
    }
}

[[nodiscard]] constexpr bool isOrdering(entity::Cmp c) noexcept {
    return c != entity::Cmp::Eq && c != entity::Cmp::Ne;
}

inline void appendPlaceholder(std::string& sql, size_t& param) {
    sql += '$';
    sql += std::to_string(param++);
}

template<typename Traits, typename G>
struct GuardSql;

template<typename Traits, auto F, entity::Cmp C, typename V>
struct GuardSql<Traits, entity::FieldGuard<F, C, V>> {
    using Info = typename Traits::template FieldInfo<F>;
    static constexpr bool ranked = has_codec_v<Info> && isOrdering(C);
    static constexpr size_t params = 1;

    static_assert(!ranked || ranks_enumerator_v<Info, V>,
        "an ordering guard on a mapped enum field takes an enumerator");

    static void emit(std::string& sql, size_t& param, std::string_view qual) {
        if constexpr (ranked) {
            appendOrderedColumn<Traits, F>(sql, qual);
        } else {
            appendColumn<Traits, F>(sql, qual);
        }
        sql += cmpToSql(C);
        appendPlaceholder(sql, param);
    }

    static void bind(io::PgParams& p, const entity::FieldGuard<F, C, V>& g) {
        if constexpr (ranked) {
            p.params.push_back(io::PgParam::bigint(
                static_cast<int64_t>(std::to_underlying(g.value))));
        } else {
            p.push(entity::detail::columnValue<Traits, F>(g.value));
        }
    }
};

template<typename Traits, auto F, entity::Cmp C>
struct GuardSql<Traits, entity::FieldGuardNow<F, C>> {
    static_assert(Traits::template FieldInfo<F>::is_timestamp,
        "comparing with dbNow requires a timestamp field");
    static constexpr size_t params = 0;

    static void emit(std::string& sql, size_t&, std::string_view qual) {
        appendColumn<Traits, F>(sql, qual);
        sql += cmpToSql(C);
        sql += "now()";
    }

    static void bind(io::PgParams&, const entity::FieldGuardNow<F, C>&) {}
};

template<typename Traits, auto F, bool Negated, typename V>
struct GuardSql<Traits, entity::FieldIn<F, Negated, V>> {
    static constexpr size_t params = 1;

    static void emit(std::string& sql, size_t& param, std::string_view qual) {
        appendColumn<Traits, F>(sql, qual);
        sql += Negated ? "!=ALL(" : "=ANY(";
        appendPlaceholder(sql, param);
        sql += ')';
    }

    static void bind(io::PgParams& p, const entity::FieldIn<F, Negated, V>& g) {
        using T = decltype(entity::detail::columnValue<Traits, F>(std::declval<const V&>()));
        std::vector<T> values;
        values.reserve(g.values.size());
        for (const auto& v : g.values)
            values.push_back(entity::detail::columnValue<Traits, F>(v));
        p.params.push_back(io::PgParams::arrayLiteral(values));
    }
};

template<typename Traits, auto F, bool Negated>
struct GuardSql<Traits, entity::NullGuard<F, Negated>> {
    static_assert(Traits::template FieldInfo<F>::is_nullable,
        "isNull<F>() / isNotNull<F>() require a nullable field");
    static constexpr size_t params = 0;

    static void emit(std::string& sql, size_t&, std::string_view qual) {
        appendColumn<Traits, F>(sql, qual);
        sql += Negated ? " IS NOT NULL" : " IS NULL";
    }

    static void bind(io::PgParams&, const entity::NullGuard<F, Negated>&) {}
};

/// Children joined by `sep`; the tuple is walked in declaration order for both
/// emission and binding.
template<typename Traits, typename... Gs>
struct GuardListSql {
    static constexpr size_t params = (GuardSql<Traits, Gs>::params + ... + 0);

    static void emit(std::string& sql, size_t& param, std::string_view qual, std::string_view sep) {
        bool first = true;
        ((sql += first ? std::string_view{} : sep, first = false,
          GuardSql<Traits, Gs>::emit(sql, param, qual)), ...);
    }

    static void bind(io::PgParams& p, const std::tuple<Gs...>& gs) {
        std::apply([&](const Gs&... g) { (GuardSql<Traits, Gs>::bind(p, g), ...); }, gs);
    }
};

template<typename Traits, typename... Gs>
struct GuardSql<Traits, entity::AllOf<Gs...>> {
    static constexpr size_t params = GuardListSql<Traits, Gs...>::params;

    static void emit(std::string& sql, size_t& param, std::string_view qual) {
        sql += '(';
        GuardListSql<Traits, Gs...>::emit(sql, param, qual, " AND ");
        sql += ')';
    }

    static void bind(io::PgParams& p, const entity::AllOf<Gs...>& g) {
        GuardListSql<Traits, Gs...>::bind(p, g.guards);
    }
};

template<typename Traits, typename... Gs>
struct GuardSql<Traits, entity::AnyOf<Gs...>> {
    static constexpr size_t params = GuardListSql<Traits, Gs...>::params;

    static void emit(std::string& sql, size_t& param, std::string_view qual) {
        sql += '(';
        GuardListSql<Traits, Gs...>::emit(sql, param, qual, " OR ");
        sql += ')';
    }

    static void bind(io::PgParams& p, const entity::AnyOf<Gs...>& g) {
        GuardListSql<Traits, Gs...>::bind(p, g.guards);
    }
};

/// Root predicate: children joined by AND, unparenthesized (each anyOf child
/// parenthesizes itself), so it can follow another condition with " AND ".
template<typename Traits, typename... Gs>
struct GuardSql<Traits, entity::Guards<Gs...>> {
    static constexpr size_t params = GuardListSql<Traits, Gs...>::params;

    static void emit(std::string& sql, size_t& param, std::string_view qual) {
        GuardListSql<Traits, Gs...>::emit(sql, param, qual, " AND ");
    }

    static void bind(io::PgParams& p, const entity::Guards<Gs...>& g) {
        GuardListSql<Traits, Gs...>::bind(p, g.guards);
    }
};

// -----------------------------------------------------------------------------
// Ordering
// -----------------------------------------------------------------------------

template<typename Traits, typename K>
struct OrderKeySql;

template<typename Traits, auto F, bool Desc, entity::Nulls N>
struct OrderKeySql<Traits, entity::OrderKey<F, Desc, N>> {
    static constexpr size_t params = 0;

    static void emit(std::string& sql, size_t&, std::string_view qual) {
        appendOrderedColumn<Traits, F>(sql, qual);
        sql += Desc ? " DESC" : " ASC";
        if constexpr (N == entity::Nulls::First) sql += " NULLS FIRST";
        if constexpr (N == entity::Nulls::Last) sql += " NULLS LAST";
    }

    static void bind(io::PgParams&, const entity::OrderKey<F, Desc, N>&) {}
};

/// CASE rather than `(guard) DESC`: a NULL guard would sort first in DESC,
/// while CASE sends it to the ELSE branch with the unsatisfied rows.
template<typename Traits, typename G>
struct OrderKeySql<Traits, entity::FirstKey<G>> {
    static constexpr size_t params = GuardSql<Traits, G>::params;

    static void emit(std::string& sql, size_t& param, std::string_view qual) {
        sql += "CASE WHEN ";
        GuardSql<Traits, G>::emit(sql, param, qual);
        sql += " THEN 0 ELSE 1 END";
    }

    static void bind(io::PgParams& p, const entity::FirstKey<G>& k) {
        GuardSql<Traits, G>::bind(p, k.guard);
    }
};

inline void appendPkOrder(std::string& sql, std::string_view qual, const char* pk_column) {
    sql += qual;
    sql += '"';
    sql += pk_column;
    sql += '"';
}

template<size_t N>
void appendPkOrder(std::string& sql, std::string_view qual,
                   const std::array<const char*, N>& pk_columns) {
    for (size_t i = 0; i < N; ++i) {
        if (i > 0) sql += ',';
        appendPkOrder(sql, qual, pk_columns[i]);
    }
}

/// ORDER BY for an optional typed ordering (void = none); the primary key,
/// ascending, always closes it.
template<typename Traits, typename Order>
struct OrderBySql;

template<typename Traits>
struct OrderBySql<Traits, void> {
    static constexpr size_t params = 0;

    template<typename Pk>
    static void emit(std::string& sql, size_t&, std::string_view qual, const Pk& pk) {
        sql += "ORDER BY ";
        appendPkOrder(sql, qual, pk);
    }
};

template<typename Traits, typename... Ks>
struct OrderBySql<Traits, entity::OrderBy<Ks...>> {
    static constexpr size_t params = (OrderKeySql<Traits, Ks>::params + ... + 0);

    template<typename Pk>
    static void emit(std::string& sql, size_t& param, std::string_view qual, const Pk& pk) {
        sql += "ORDER BY ";
        ((OrderKeySql<Traits, Ks>::emit(sql, param, qual), sql += ','), ...);
        appendPkOrder(sql, qual, pk);
    }

    static void bind(io::PgParams& p, const entity::OrderBy<Ks...>& o) {
        std::apply([&](const Ks&... k) { (OrderKeySql<Traits, Ks>::bind(p, k), ...); }, o.keys);
    }
};

}  // namespace jcailloux::relais::detail

#endif  // JCX_RELAIS_REPOSITORY_GUARD_SQL_H
