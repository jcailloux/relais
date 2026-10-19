#ifndef JCX_RELAIS_REPOSITORY_ROWMATCH_H
#define JCX_RELAIS_REPOSITORY_ROWMATCH_H

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "jcailloux/relais/entity/FieldGuard.h"
#include "jcailloux/relais/entity/FieldUpdate.h"
#include "jcailloux/relais/list/spec/GeneratedFilters.h"

namespace jcailloux::relais::detail {

/// What a predicate says of a cached copy: No only when it surely does not
/// hold, Maybe otherwise.
enum class RowMatch : uint8_t { No, Maybe };

/// A predicate evaluated on cached copies; empty: every row.
template<typename E>
using RowPredicate = std::function<RowMatch(const E&)>;

// =============================================================================
// ColumnFacts - what the database says of a table's columns
// =============================================================================
//
// Equality in C++ is equality in PostgreSQL for integer and boolean columns,
// enum types, and text or varchar under a deterministic collation. Elsewhere
// they may differ ('Pending' = 'pending' under a case-insensitive collation,
// trailing spaces ignored by char(n)), and nothing on the C++ side tells. The
// catalog does: it is read once, off the hot path, and never again (the
// schema is fixed for the life of the process). Until it is, no column is
// exact.
//

class ColumnFacts {
public:
    /// True once read, if the database compares `column` as C++ does.
    [[nodiscard]] bool exactEquality(std::string_view column) const noexcept {
        if (state_.load(std::memory_order_acquire) != kRead) return false;
        return std::ranges::find(exact_, column) != exact_.end();
    }

    [[nodiscard]] bool known() const noexcept {
        return state_.load(std::memory_order_acquire) == kRead;
    }

    /// True if the caller must read the catalog (none read it, none is reading).
    [[nodiscard]] bool claimRead() noexcept {
        uint8_t expected = kUnread;
        return state_.compare_exchange_strong(expected, kReading, std::memory_order_acq_rel);
    }

    /// The columns compared exactly; written once, by the claimant.
    void publish(std::vector<std::string> exact) noexcept {
        exact_ = std::move(exact);
        state_.store(kRead, std::memory_order_release);
    }

    /// The read failed: the next claimRead() reads again.
    void readFailed() noexcept { state_.store(kUnread, std::memory_order_release); }

    /// Lists, per column of the table named by $1, whether its equality is
    /// exact. The table resolves as the repo's statements resolve it.
    static constexpr const char* kSql =
        "SELECT a.attname::text,"
        " t.typtype = 'e'"
        " OR a.atttypid IN ('int2'::regtype, 'int4'::regtype, 'int8'::regtype, 'bool'::regtype)"
        " OR (a.atttypid IN ('text'::regtype, 'varchar'::regtype)"
        "     AND COALESCE(c.collisdeterministic, true))"
        " FROM pg_attribute a"
        " JOIN pg_type t ON t.oid = a.atttypid"
        " LEFT JOIN pg_collation c ON c.oid = a.attcollation"
        " WHERE a.attrelid = to_regclass($1) AND a.attnum > 0 AND NOT a.attisdropped";

#ifdef RELAIS_BUILDING_TESTS
    void reset() noexcept {
        exact_.clear();
        state_.store(kUnread, std::memory_order_release);
    }
#endif

private:
    static constexpr uint8_t kUnread = 0;
    static constexpr uint8_t kReading = 1;
    static constexpr uint8_t kRead = 2;

    std::atomic<uint8_t> state_{kUnread};
    std::vector<std::string> exact_;
};

// =============================================================================
// Prudent evaluation
//
// Says No only when the database surely evaluates the predicate to false (or
// NULL) on the row the copy holds; Maybe otherwise. Certain: comparisons of
// integers, equality of booleans, equality and membership of enums and text
// on an exact column (see ColumnFacts), nullity, and any comparison with a
// NULL column. Maybe: the database clock, floating point (NaN equals NaN in
// PostgreSQL), the ordering of text or enums, timestamps, and any node or
// type this evaluator does not know.
// =============================================================================

namespace match {

[[nodiscard]] constexpr RowMatch of(bool holds) noexcept {
    return holds ? RowMatch::Maybe : RowMatch::No;
}

/// A quoted column name ("\"col\"") as the catalog spells it.
[[nodiscard]] constexpr std::string_view bareColumn(std::string_view column) noexcept {
    if (column.size() >= 2 && column.front() == '"' && column.back() == '"')
        return column.substr(1, column.size() - 2);
    return column;
}

template<typename T> inline constexpr bool is_optional_v = false;
template<typename T> inline constexpr bool is_optional_v<std::optional<T>> = true;

template<typename T> struct unwrap { using type = T; };
template<typename T> struct unwrap<std::optional<T>> { using type = T; };

/// How far C++ comparison agrees with the database for a column type.
enum class Kind : uint8_t {
    Ordered,     // every comparison (integers)
    Equality,    // equality only (booleans)
    Column,      // equality only, if the column is exact (text, enums)
    Unknown,     // never certain
};

template<typename U>
consteval Kind kindOf() {
    if constexpr (std::is_same_v<U, bool>) return Kind::Equality;
    else if constexpr (std::is_integral_v<U>) return Kind::Ordered;
    else if constexpr (std::is_enum_v<U> || std::is_same_v<U, std::string>) return Kind::Column;
    else return Kind::Unknown;
}

[[nodiscard]] constexpr bool certain(Kind kind, entity::Cmp c, bool exact_column) noexcept {
    const bool equality = c == entity::Cmp::Eq || c == entity::Cmp::Ne;
    switch (kind) {
        case Kind::Ordered: return true;
        case Kind::Equality: return equality;
        case Kind::Column: return equality && exact_column;
        case Kind::Unknown: return false;
    }
    return false;
}

template<typename T>
[[nodiscard]] constexpr bool compare(entity::Cmp c, const T& a, const T& b) noexcept {
    switch (c) {
        case entity::Cmp::Eq: return a == b;
        case entity::Cmp::Ne: return a != b;
        case entity::Cmp::Gt: return a > b;
        case entity::Cmp::Ge: return a >= b;
        case entity::Cmp::Lt: return a < b;
        case entity::Cmp::Le: return a <= b;
    }
    return true;
}

/// The value a guard compares with, as the column receives it: the bound
/// parameter is the guard's value cast to the column's type, and a mapped
/// enum compares as its enumerator (the codec is a bijection).
template<typename Info, typename U, typename V>
[[nodiscard]] std::optional<U> boundAs(const V& value) {
    if constexpr (std::is_same_v<U, V>) return value;
    else if constexpr (std::is_arithmetic_v<U> && std::is_arithmetic_v<V>)
        return static_cast<typename Info::value_type>(value);
    else if constexpr (std::is_same_v<U, std::string> && std::is_constructible_v<std::string, const V&>)
        return std::string(value);
    else return std::nullopt;
}

// -----------------------------------------------------------------------------
// Typed guards
// -----------------------------------------------------------------------------

/// Unknown node: Maybe.
template<typename Traits, typename G>
struct Guard {
    template<typename E>
    static RowMatch eval(const E&, const G&, const ColumnFacts&) { return RowMatch::Maybe; }
};

/// The member a field reads into, for a copy of entity type E.
template<typename Info, typename E>
[[nodiscard]] const auto& member(const E& e) noexcept {
    return e.*(Info::member);
}

template<typename Info>
[[nodiscard]] bool exactColumn(const ColumnFacts& facts) noexcept {
    return facts.exactEquality(bareColumn(Info::column_name));
}

template<typename Traits, auto F, entity::Cmp C, typename V>
struct Guard<Traits, entity::FieldGuard<F, C, V>> {
    using Info = typename Traits::template FieldInfo<F>;

    template<typename E>
    static RowMatch eval(const E& e, const entity::FieldGuard<F, C, V>& g, const ColumnFacts& facts) {
        const auto& m = member<Info>(e);
        using M = std::remove_cvref_t<decltype(m)>;
        using U = typename unwrap<M>::type;
        if constexpr (is_optional_v<M>) {
            if (!m) return RowMatch::No;  // NULL compared to anything is not true
        }
        if constexpr (Info::is_timestamp) {
            return RowMatch::Maybe;
        } else {
            constexpr Kind kind = kindOf<U>();
            if (!certain(kind, C, kind == Kind::Column && exactColumn<Info>(facts)))
                return RowMatch::Maybe;
            const auto bound = boundAs<Info, U>(g.value);
            if (!bound) return RowMatch::Maybe;
            if constexpr (is_optional_v<M>) return of(compare(C, *m, *bound));
            else return of(compare(C, m, *bound));
        }
    }
};

template<typename Traits, auto F, bool Negated, typename V>
struct Guard<Traits, entity::FieldIn<F, Negated, V>> {
    using Info = typename Traits::template FieldInfo<F>;

    template<typename E>
    static RowMatch eval(const E& e, const entity::FieldIn<F, Negated, V>& g, const ColumnFacts& facts) {
        const auto& m = member<Info>(e);
        using M = std::remove_cvref_t<decltype(m)>;
        using U = typename unwrap<M>::type;
        if constexpr (is_optional_v<M>) {
            // NULL = ANY(set) is not true; NULL != ALL(set) is, on an empty set.
            if (!m) return of(Negated && g.values.empty());
        }
        if constexpr (Info::is_timestamp) {
            return RowMatch::Maybe;
        } else {
            constexpr Kind kind = kindOf<U>();
            if (!certain(kind, entity::Cmp::Eq, kind == Kind::Column && exactColumn<Info>(facts)))
                return RowMatch::Maybe;
            const U& value = [&]() -> const U& {
                if constexpr (is_optional_v<M>) return *m;
                else return m;
            }();
            bool found = false;
            for (const auto& v : g.values) {
                const auto bound = boundAs<Info, U>(v);
                if (!bound) return RowMatch::Maybe;
                found = found || *bound == value;
            }
            return of(found != Negated);
        }
    }
};

template<typename Traits, auto F, entity::Cmp C>
struct Guard<Traits, entity::FieldGuardNow<F, C>> {
    using Info = typename Traits::template FieldInfo<F>;

    template<typename E>
    static RowMatch eval(const E& e, const entity::FieldGuardNow<F, C>&, const ColumnFacts&) {
        const auto& m = member<Info>(e);
        if constexpr (is_optional_v<std::remove_cvref_t<decltype(m)>>) {
            if (!m) return RowMatch::No;
        }
        return RowMatch::Maybe;
    }
};

template<typename Traits, auto F, bool Negated>
struct Guard<Traits, entity::NullGuard<F, Negated>> {
    using Info = typename Traits::template FieldInfo<F>;

    template<typename E>
    static RowMatch eval(const E& e, const entity::NullGuard<F, Negated>&, const ColumnFacts&) {
        const auto& m = member<Info>(e);
        if constexpr (is_optional_v<std::remove_cvref_t<decltype(m)>>) {
            return of(m.has_value() == Negated);
        } else {
            return RowMatch::Maybe;
        }
    }
};

/// No if one child is No.
template<typename Traits, typename E, typename... Gs>
RowMatch allOf(const E& e, const std::tuple<Gs...>& gs, const ColumnFacts& facts) {
    return std::apply([&](const Gs&... g) {
        return ((Guard<Traits, Gs>::eval(e, g, facts) == RowMatch::No) || ...)
            ? RowMatch::No : RowMatch::Maybe;
    }, gs);
}

template<typename Traits, typename... Gs>
struct Guard<Traits, entity::AllOf<Gs...>> {
    template<typename E>
    static RowMatch eval(const E& e, const entity::AllOf<Gs...>& g, const ColumnFacts& facts) {
        return allOf<Traits>(e, g.guards, facts);
    }
};

/// No if every child is No.
template<typename Traits, typename... Gs>
struct Guard<Traits, entity::AnyOf<Gs...>> {
    template<typename E>
    static RowMatch eval(const E& e, const entity::AnyOf<Gs...>& g, const ColumnFacts& facts) {
        return std::apply([&](const Gs&... c) {
            return ((Guard<Traits, Gs>::eval(e, c, facts) == RowMatch::No) && ...)
                ? RowMatch::No : RowMatch::Maybe;
        }, g.guards);
    }
};

template<typename Traits, typename... Gs>
struct Guard<Traits, entity::Guards<Gs...>> {
    template<typename E>
    static RowMatch eval(const E& e, const entity::Guards<Gs...>& g, const ColumnFacts& facts) {
        return allOf<Traits>(e, g.guards, facts);
    }
};

// -----------------------------------------------------------------------------
// Declared filters (FilterSet predicates)
// -----------------------------------------------------------------------------

[[nodiscard]] constexpr entity::Cmp cmpOf(list::spec::Op op) noexcept {
    using list::spec::Op;
    switch (op) {
        case Op::NE: return entity::Cmp::Ne;
        case Op::GT: return entity::Cmp::Gt;
        case Op::GE: return entity::Cmp::Ge;
        case Op::LT: return entity::Cmp::Lt;
        case Op::LE: return entity::Cmp::Le;
        default: return entity::Cmp::Eq;  // EQ, IN, NIN: equality
    }
}

template<typename Descriptor, size_t I, typename E>
RowMatch filterAt(const E& e, const list::spec::Filters<Descriptor>& filters,
                  const ColumnFacts& facts) {
    using FT = list::spec::filter_at<Descriptor, I>;
    using list::spec::Op;
    const auto& fv = std::get<I>(filters.values);
    if (!fv.has_value()) return RowMatch::Maybe;  // inactive: holds
    const auto& m = list::spec::detail::extractMemberValue<FT::entity_ptr>(e);
    using M = std::remove_cvref_t<decltype(m)>;
    using U = typename unwrap<M>::type;

    if constexpr (FT::is_null_op) {
        return of(m.has_value() == *fv);
    } else {
        if constexpr (is_optional_v<M>) {
            if (!m) return of(FT::op == Op::NIN && fv->empty());
        }
        constexpr Kind kind = kindOf<U>();
        constexpr entity::Cmp c = cmpOf(FT::op);
        if (!certain(kind, c, kind == Kind::Column && facts.exactEquality(FT::column())))
            return RowMatch::Maybe;
        const U& value = [&]() -> const U& {
            if constexpr (is_optional_v<M>) return *m;
            else return m;
        }();
        if constexpr (FT::op == Op::IN || FT::op == Op::NIN) {
            const bool found = std::ranges::find(*fv, value) != fv->end();
            return of(found == (FT::op == Op::IN));
        } else {
            return of(compare(c, value, static_cast<U>(*fv)));
        }
    }
}

}  // namespace match

/// Prudent evaluation of a typed guard predicate on a cached copy.
template<typename E, typename G>
[[nodiscard]] RowMatch guardMatch(const E& e, const G& guard, const ColumnFacts& facts) {
    return match::Guard<typename E::TraitsType, G>::eval(e, guard, facts);
}

/// Prudent evaluation of a declared-filter predicate on a cached copy.
template<typename Descriptor, typename E>
[[nodiscard]] RowMatch filterMatch(const E& e, const list::spec::Filters<Descriptor>& filters,
                                   const ColumnFacts& facts) {
    return [&]<size_t... Is>(std::index_sequence<Is...>) {
        return ((match::filterAt<Descriptor, Is>(e, filters, facts) == RowMatch::No) || ...)
            ? RowMatch::No : RowMatch::Maybe;
    }(std::make_index_sequence<list::spec::filter_count<Descriptor>>{});
}

/// The predicate of an uncertain write, as the cache tiers test their copies.
/// It owns what it uses; `facts` outlives it (a repo's static state).
template<typename E, typename G>
[[nodiscard]] RowPredicate<E> guardPredicate(G guard, const ColumnFacts& facts) {
    return [guard = std::move(guard), &facts](const E& e) { return guardMatch(e, guard, facts); };
}

template<typename E, typename Descriptor>
[[nodiscard]] RowPredicate<E> filterPredicate(list::spec::Filters<Descriptor> filters,
                                              const ColumnFacts& facts) {
    return [filters = std::move(filters), &facts](const E& e) {
        return filterMatch<Descriptor>(e, filters, facts);
    };
}

}  // namespace jcailloux::relais::detail

#endif  // JCX_RELAIS_REPOSITORY_ROWMATCH_H
