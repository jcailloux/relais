#ifndef JCX_RELAIS_ENTITY_FIELD_UPDATE_H
#define JCX_RELAIS_ENTITY_FIELD_UPDATE_H

#include <chrono>
#include <cstdint>
#include <string>
#include <type_traits>

namespace jcailloux::relais::entity {

// =============================================================================
// Typed field update descriptors for patch: absolute (set, setNull) and
// relative (increment, decrement, nowPlus)
// =============================================================================

/// Carries a value to set on a specific field (F is a Traits::Field enum value).
template<auto F, typename V>
struct FieldUpdate {
    V value;
};

/// Marker to set a nullable field to NULL.
template<auto F>
struct FieldSetNull {};

/// Adds a value to a numeric field, in the database: col = col + $n.
template<auto F, typename V>
struct FieldIncrement {
    V value;
};

/// Subtracts a value from a numeric field, in the database: col = col - $n.
template<auto F, typename V>
struct FieldDecrement {
    V value;
};

/// Sets a timestamp field to the database clock plus an offset:
/// col = now() + $n microseconds.
template<auto F>
struct FieldNowPlus {
    int64_t microseconds;
};

/// Create a FieldUpdate for the given field with the given value.
template<auto F>
auto set(auto&& val) {
    return FieldUpdate<F, std::decay_t<decltype(val)>>{
        std::forward<decltype(val)>(val)};
}

/// Create a FieldSetNull marker for the given nullable field.
template<auto F>
auto setNull() {
    return FieldSetNull<F>{};
}

/// Add `val` to a numeric field. The addition runs in the database, so
/// concurrent increments never lose each other.
template<auto F>
auto increment(auto&& val) {
    return FieldIncrement<F, std::decay_t<decltype(val)>>{
        std::forward<decltype(val)>(val)};
}

/// Subtract `val` from a numeric field. The subtraction runs in the database.
template<auto F>
auto decrement(auto&& val) {
    return FieldDecrement<F, std::decay_t<decltype(val)>>{
        std::forward<decltype(val)>(val)};
}

/// Set a timestamp field to the database's now() plus `offset` (microsecond
/// resolution, truncated toward zero). nowPlus<F>(0s) stamps the database time.
/// The database clock is the single time reference, so instances whose clocks
/// drift apart never disagree on a deadline.
template<auto F, typename Rep, typename Period>
auto nowPlus(std::chrono::duration<Rep, Period> offset) {
    return FieldNowPlus<F>{
        std::chrono::duration_cast<std::chrono::microseconds>(offset).count()};
}

// =============================================================================
// SetOp — how an update assigns its column
// =============================================================================

/// SQL shape of one SET assignment, given the update's bound value $n.
enum class SetOp : uint8_t {
    Assign,     // col = $n
    Add,        // col = col + $n
    Subtract,   // col = col - $n
    NowPlus,    // col = now() + $n microseconds
};

template<typename U> inline constexpr SetOp set_op_v = SetOp::Assign;
template<auto F, typename V> inline constexpr SetOp set_op_v<FieldIncrement<F, V>> = SetOp::Add;
template<auto F, typename V> inline constexpr SetOp set_op_v<FieldDecrement<F, V>> = SetOp::Subtract;
template<auto F> inline constexpr SetOp set_op_v<FieldNowPlus<F>> = SetOp::NowPlus;

/// True when re-running the update changes the row again (relative arithmetic,
/// database clock). Such writes must never be coalesced with an identical one.
template<typename U>
inline constexpr bool is_relative_update_v = set_op_v<std::remove_cvref_t<U>> != SetOp::Assign;

// =============================================================================
// fieldColumnName / fieldValue — extractors for SQL binding in patch
// =============================================================================

/// Extract quoted column name from a FieldUpdate.
/// Requires FieldInfo<F>::column_name to be defined.
template<typename Traits, auto F, typename V>
std::string fieldColumnName(const FieldUpdate<F, V>&) {
    return std::string(Traits::template FieldInfo<F>::column_name);
}

template<typename Traits, auto F>
std::string fieldColumnName(const FieldSetNull<F>&) {
    return std::string(Traits::template FieldInfo<F>::column_name);
}

template<typename Traits, auto F, typename V>
std::string fieldColumnName(const FieldIncrement<F, V>&) {
    return std::string(Traits::template FieldInfo<F>::column_name);
}

template<typename Traits, auto F, typename V>
std::string fieldColumnName(const FieldDecrement<F, V>&) {
    return std::string(Traits::template FieldInfo<F>::column_name);
}

template<typename Traits, auto F>
std::string fieldColumnName(const FieldNowPlus<F>&) {
    return std::string(Traits::template FieldInfo<F>::column_name);
}

/// Extract properly-typed value for SQL binding from a FieldUpdate.
/// Timestamps are stored as strings — no conversion needed.
template<typename Traits, auto F, typename V>
auto fieldValue(const FieldUpdate<F, V>& update) {
    using Info = typename Traits::template FieldInfo<F>;
    if constexpr (Info::is_timestamp) {
        return std::string(update.value);
    } else {
        return static_cast<typename Info::value_type>(update.value);
    }
}

/// Extract NULL value for SQL binding from a FieldSetNull.
template<typename Traits, auto F>
std::nullptr_t fieldValue(const FieldSetNull<F>&) {
    static_assert(Traits::template FieldInfo<F>::is_nullable,
        "setNull<F>() can only be used on nullable fields");
    return nullptr;
}

namespace detail {

/// Relative arithmetic needs a plain numeric column: NULL + n stays NULL, and a
/// timestamp or a bool has no meaningful + n.
template<typename Traits, auto F, typename V>
auto relativeValue(const V& value) {
    using Info = typename Traits::template FieldInfo<F>;
    using T = typename Info::value_type;
    static_assert(std::is_arithmetic_v<T> && !std::is_same_v<T, bool> && !Info::is_timestamp,
        "increment/decrement require a numeric (non-bool, non-timestamp) field");
    static_assert(!Info::is_nullable,
        "increment/decrement require a NOT NULL field (NULL + n is NULL)");
    static_assert(std::is_arithmetic_v<V> && !std::is_same_v<V, bool>,
        "increment/decrement take a numeric value");
    return static_cast<T>(value);
}

}  // namespace detail

template<typename Traits, auto F, typename V>
auto fieldValue(const FieldIncrement<F, V>& update) {
    return detail::relativeValue<Traits, F>(update.value);
}

template<typename Traits, auto F, typename V>
auto fieldValue(const FieldDecrement<F, V>& update) {
    return detail::relativeValue<Traits, F>(update.value);
}

template<typename Traits, auto F>
int64_t fieldValue(const FieldNowPlus<F>& update) {
    static_assert(Traits::template FieldInfo<F>::is_timestamp,
        "nowPlus<F>() can only be used on timestamp fields");
    return update.microseconds;
}

}  // namespace jcailloux::relais::entity

#endif  // JCX_RELAIS_ENTITY_FIELD_UPDATE_H
