#ifndef JCX_DROGON_WRAPPER_FIELD_UPDATE_H
#define JCX_DROGON_WRAPPER_FIELD_UPDATE_H

#include <string>
#include <type_traits>

#include <trantor/utils/Date.h>

namespace jcailloux::drogon::wrapper {

// =============================================================================
// FieldUpdate / FieldSetNull — typed field update descriptors for updateBy
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
// applyFieldUpdate — applies a single FieldUpdate to a Drogon model
// =============================================================================

/// Apply a value update: dispatches via Traits::FieldInfo<F> to the model setter.
template<typename Traits, auto F, typename V>
void applyFieldUpdate(typename Traits::Model& model, const FieldUpdate<F, V>& update) {
    using Info = typename Traits::template FieldInfo<F>;

    if constexpr (Info::is_timestamp) {
        if constexpr (std::is_convertible_v<V, std::string>) {
            (model.*Info::setter)(
                ::trantor::Date::fromDbStringLocal(update.value));
        } else if constexpr (std::is_same_v<std::decay_t<V>, ::trantor::Date>) {
            (model.*Info::setter)(update.value);
        } else {
            static_assert(std::is_convertible_v<V, std::string>,
                "Timestamp fields require a string or trantor::Date value");
        }
    } else {
        (model.*Info::setter)(
            static_cast<typename Info::value_type>(update.value));
    }
}

/// Apply a setNull: calls the setToNull method on the model.
template<typename Traits, auto F>
void applyFieldUpdate(typename Traits::Model& model, const FieldSetNull<F>&) {
    using Info = typename Traits::template FieldInfo<F>;
    static_assert(Info::is_nullable, "setNull<F>() can only be used on nullable fields");
    (model.*Info::setToNull)();
}

// =============================================================================
// fieldColumnName / fieldValue — extractors for criteria-based updateBy
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
template<typename Traits, auto F, typename V>
auto fieldValue(const FieldUpdate<F, V>& update) {
    using Info = typename Traits::template FieldInfo<F>;
    if constexpr (Info::is_timestamp) {
        if constexpr (std::is_same_v<std::decay_t<V>, ::trantor::Date>) {
            return update.value;
        } else {
            return ::trantor::Date::fromDbStringLocal(std::string(update.value));
        }
    } else {
        return static_cast<typename Info::value_type>(update.value);
    }
}

template<typename Traits, auto F>
std::nullptr_t fieldValue(const FieldSetNull<F>&) {
    return nullptr;
}

}  // namespace jcailloux::drogon::wrapper

#endif  // JCX_DROGON_WRAPPER_FIELD_UPDATE_H
