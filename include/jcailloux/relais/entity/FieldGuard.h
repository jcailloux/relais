#ifndef JCX_RELAIS_ENTITY_FIELD_GUARD_H
#define JCX_RELAIS_ENTITY_FIELD_GUARD_H

#include <cstdint>
#include <initializer_list>
#include <optional>
#include <ranges>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace jcailloux::relais::entity {

// =============================================================================
// Typed guards: row conditions evaluated by the database
//
// A predicate is when(...), a conjunction whose children are leaves or
// anyOf(...); an anyOf has leaves or allOf(...) children; an allOf has leaves
// only. This bounded disjunctive normal form expresses any condition without
// arithmetic, and each distinct shape emits its SQL once per instantiation.
// Comparisons follow SQL semantics: on a nullable column, NULL compared to
// anything is not true.
// =============================================================================

enum class Cmp : uint8_t { Eq, Ne, Gt, Ge, Lt, Le };

/// Stands for the database clock on the right of a comparison: le<F>(dbNow).
struct DbNow {};
inline constexpr DbNow dbNow{};

/// col <op> $n
template<auto F, Cmp C, typename V>
struct FieldGuard {
    V value;
};

/// col <op> now()
template<auto F, Cmp C>
struct FieldGuardNow {};

/// col = ANY($n), or col != ALL($n) when Negated. The set binds as one array.
template<auto F, bool Negated, typename V>
struct FieldIn {
    std::vector<V> values;
};

/// col IS NULL, or col IS NOT NULL when Negated.
template<auto F, bool Negated>
struct NullGuard {};

template<typename... Gs>
struct AllOf {
    std::tuple<Gs...> guards;
};

template<typename... Gs>
struct AnyOf {
    std::tuple<Gs...> guards;
};

/// Root of a predicate: the conjunction of its children.
template<typename... Gs>
struct Guards {
    std::tuple<Gs...> guards;
};

// -----------------------------------------------------------------------------
// Structure traits
// -----------------------------------------------------------------------------

template<typename G> inline constexpr bool is_guard_leaf_v = false;
template<auto F, Cmp C, typename V> inline constexpr bool is_guard_leaf_v<FieldGuard<F, C, V>> = true;
template<auto F, Cmp C> inline constexpr bool is_guard_leaf_v<FieldGuardNow<F, C>> = true;
template<auto F, bool N, typename V> inline constexpr bool is_guard_leaf_v<FieldIn<F, N, V>> = true;
template<auto F, bool N> inline constexpr bool is_guard_leaf_v<NullGuard<F, N>> = true;

template<typename G> inline constexpr bool is_all_of_v = false;
template<typename... Gs> inline constexpr bool is_all_of_v<AllOf<Gs...>> = true;

template<typename G> inline constexpr bool is_any_of_v = false;
template<typename... Gs> inline constexpr bool is_any_of_v<AnyOf<Gs...>> = true;

/// Any guard that may stand on its own: a leaf, an anyOf or an allOf.
template<typename G>
inline constexpr bool is_guard_v = is_guard_leaf_v<G> || is_all_of_v<G> || is_any_of_v<G>;

template<typename G> inline constexpr bool is_guards_v = false;
template<typename... Gs> inline constexpr bool is_guards_v<Guards<Gs...>> = true;

/// The bounded shape: what each node accepts as a child.
template<typename G> inline constexpr bool valid_all_of_child_v = is_guard_leaf_v<G>;
template<typename G> inline constexpr bool valid_any_of_child_v = is_guard_leaf_v<G> || is_all_of_v<G>;
template<typename G> inline constexpr bool valid_when_child_v = is_guard_leaf_v<G> || is_any_of_v<G>;

// -----------------------------------------------------------------------------
// Factories
// -----------------------------------------------------------------------------

namespace detail {

template<typename V>
inline constexpr bool is_optional_like_v = false;
template<typename T>
inline constexpr bool is_optional_like_v<std::optional<T>> = true;
template<>
inline constexpr bool is_optional_like_v<std::nullopt_t> = true;

template<auto F, Cmp C, typename V>
auto compare(V&& value) {
    using D = std::decay_t<V>;
    static_assert(!is_optional_like_v<D> && !std::is_null_pointer_v<D>,
        "compare against NULL with isNull<F>() / isNotNull<F>()");
    if constexpr (std::is_same_v<D, DbNow>) {
        return FieldGuardNow<F, C>{};
    } else {
        return FieldGuard<F, C, D>{std::forward<V>(value)};
    }
}

}  // namespace detail

template<auto F> auto eq(auto&& v) { return detail::compare<F, Cmp::Eq>(std::forward<decltype(v)>(v)); }
template<auto F> auto ne(auto&& v) { return detail::compare<F, Cmp::Ne>(std::forward<decltype(v)>(v)); }
template<auto F> auto gt(auto&& v) { return detail::compare<F, Cmp::Gt>(std::forward<decltype(v)>(v)); }
template<auto F> auto ge(auto&& v) { return detail::compare<F, Cmp::Ge>(std::forward<decltype(v)>(v)); }
template<auto F> auto lt(auto&& v) { return detail::compare<F, Cmp::Lt>(std::forward<decltype(v)>(v)); }
template<auto F> auto le(auto&& v) { return detail::compare<F, Cmp::Le>(std::forward<decltype(v)>(v)); }

/// col is one of `values`. An empty set is allowed and matches no row.
template<auto F, std::ranges::input_range R>
auto in(R&& values) {
    using V = std::ranges::range_value_t<R>;
    return FieldIn<F, false, V>{std::vector<V>(std::ranges::begin(values), std::ranges::end(values))};
}
template<auto F, typename V>
auto in(std::initializer_list<V> values) {
    return FieldIn<F, false, V>{std::vector<V>(values)};
}

/// col is none of `values`. An empty set is allowed and matches every row.
template<auto F, std::ranges::input_range R>
auto notIn(R&& values) {
    using V = std::ranges::range_value_t<R>;
    return FieldIn<F, true, V>{std::vector<V>(std::ranges::begin(values), std::ranges::end(values))};
}
template<auto F, typename V>
auto notIn(std::initializer_list<V> values) {
    return FieldIn<F, true, V>{std::vector<V>(values)};
}

template<auto F> auto isNull() { return NullGuard<F, false>{}; }
template<auto F> auto isNotNull() { return NullGuard<F, true>{}; }

/// Conjunction, allowed inside anyOf only; its children are leaves.
template<typename... Gs>
auto allOf(Gs&&... gs) {
    static_assert(sizeof...(Gs) > 0, "allOf requires at least one guard");
    static_assert((valid_all_of_child_v<std::decay_t<Gs>> && ...),
        "allOf children must be leaf guards (eq, in, isNull, ...)");
    return AllOf<std::decay_t<Gs>...>{{std::forward<Gs>(gs)...}};
}

/// Disjunction; its children are leaves or allOf.
template<typename... Gs>
auto anyOf(Gs&&... gs) {
    static_assert(sizeof...(Gs) > 0, "anyOf requires at least one guard");
    static_assert((valid_any_of_child_v<std::decay_t<Gs>> && ...),
        "anyOf children must be leaf guards or allOf(...)");
    return AnyOf<std::decay_t<Gs>...>{{std::forward<Gs>(gs)...}};
}

/// Root predicate: all children hold. Its children are leaves or anyOf.
template<typename... Gs>
auto when(Gs&&... gs) {
    static_assert(sizeof...(Gs) > 0,
        "when requires at least one guard (a write without a predicate targets the whole table)");
    static_assert((valid_when_child_v<std::decay_t<Gs>> && ...),
        "when children must be leaf guards or anyOf(...); list conjunctions directly");
    return Guards<std::decay_t<Gs>...>{{std::forward<Gs>(gs)...}};
}

// =============================================================================
// Typed ordering
//
// The primary key, ascending, always closes the ordering, so it is total and
// deterministic. PostgreSQL defaults apply unless Nulls says otherwise
// (ASC NULLS LAST, DESC NULLS FIRST).
// =============================================================================

enum class Nulls : uint8_t { Default, First, Last };

template<auto F, bool Desc, Nulls N>
struct OrderKey {};

/// Rows satisfying the guard come first.
template<typename G>
struct FirstKey {
    G guard;
};

template<typename... Ks>
struct OrderBy {
    std::tuple<Ks...> keys;
};

template<typename K> inline constexpr bool is_order_key_v = false;
template<auto F, bool D, Nulls N> inline constexpr bool is_order_key_v<OrderKey<F, D, N>> = true;
template<typename G> inline constexpr bool is_order_key_v<FirstKey<G>> = true;

template<typename K> inline constexpr bool is_order_by_v = false;
template<typename... Ks> inline constexpr bool is_order_by_v<OrderBy<Ks...>> = true;

template<auto F, Nulls N = Nulls::Default> auto asc() { return OrderKey<F, false, N>{}; }
template<auto F, Nulls N = Nulls::Default> auto desc() { return OrderKey<F, true, N>{}; }

/// Put rows satisfying `guard` (a leaf, anyOf or allOf) first. A guard that
/// evaluates to NULL counts as not satisfied.
template<typename G>
auto first(G&& guard) {
    static_assert(is_guard_v<std::decay_t<G>>,
        "first() takes a guard (eq, in, isNull, anyOf, allOf, ...)");
    return FirstKey<std::decay_t<G>>{std::forward<G>(guard)};
}

template<typename... Ks>
auto orderBy(Ks&&... keys) {
    static_assert(sizeof...(Ks) > 0, "orderBy requires at least one key");
    static_assert((is_order_key_v<std::decay_t<Ks>> && ...),
        "orderBy takes asc<F>(), desc<F>() or first(guard)");
    return OrderBy<std::decay_t<Ks>...>{{std::forward<Ks>(keys)...}};
}

}  // namespace jcailloux::relais::entity

#endif  // JCX_RELAIS_ENTITY_FIELD_GUARD_H
