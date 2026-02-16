#ifndef JCX_RELAIS_WRAPPER_FIELD_UPDATE_H
#define JCX_RELAIS_WRAPPER_FIELD_UPDATE_H

#include <string>
#include <type_traits>

namespace jcailloux::relais::wrapper {

// =============================================================================
// FieldUpdate / FieldSetNull — typed field update descriptors for patch
// =============================================================================

/// Carries a value to set on a specific field (F is a Traits::Field enum value).
template<auto F, typename V>
struct FieldUpdate {
    V value;
};

/// Marker to set a nullable field to NULL.
template<auto F>
struct FieldSetNull {};

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

}  // namespace jcailloux::relais::wrapper

#endif  // JCX_RELAIS_WRAPPER_FIELD_UPDATE_H
