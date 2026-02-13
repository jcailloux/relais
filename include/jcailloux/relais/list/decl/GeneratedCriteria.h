#ifndef CODIBOT_CACHE_LIST_DECL_GENERATEDCRITERIA_H
#define CODIBOT_CACHE_LIST_DECL_GENERATEDCRITERIA_H

#include <string>
#include <type_traits>
#include <utility>

#include <drogon/orm/Criteria.h>

#include "FilterDescriptor.h"
#include "ListDescriptor.h"
#include "GeneratedFilters.h"

namespace jcailloux::drogon::cache::list::decl {

// =============================================================================
// Convert Op enum to Drogon CompareOperator
// =============================================================================

[[nodiscard]] constexpr ::drogon::orm::CompareOperator toDrogonOp(Op op) noexcept {
    using enum ::drogon::orm::CompareOperator;
    switch (op) {
        case Op::EQ: return EQ;
        case Op::NE: return NE;
        case Op::GT: return GT;
        case Op::GE: return GE;
        case Op::LT: return LT;
        case Op::LE: return LE;
    }
    return EQ;
}

// =============================================================================
// Value conversion for DB queries
// =============================================================================

namespace detail {

/// Convert filter value for DB query (NoConvert - pass through)
template<typename FilterType, typename T>
[[nodiscard]] auto convertForDb(const T& value) noexcept {
    if constexpr (std::is_same_v<typename FilterType::converter, AsString>) {
        // Use ADL to find toString
        using std::to_string;
        return std::string(toString(value));
    } else {
        // NoConvert - return as-is
        return value;
    }
}

}  // namespace detail

// =============================================================================
// Build Drogon Criteria from Filters
// =============================================================================

/// Build Drogon Criteria from filter values
/// @return pair of (Criteria, hasCriteria)
template<typename Descriptor>
    requires ValidListDescriptor<Descriptor>
[[nodiscard]] std::pair<::drogon::orm::Criteria, bool> buildCriteria(
    const Filters<Descriptor>& filters
) noexcept {
    using namespace ::drogon::orm;

    Criteria criteria;
    bool hasCriteria = false;

    auto addCriteria = [&](auto&& crit) {
        criteria = hasCriteria ? (criteria && crit) : crit;
        hasCriteria = true;
    };

    [&]<size_t... Is>(std::index_sequence<Is...>) {
        ([&] {
            using FilterType = filter_at<Descriptor, Is>;
            const auto& filter_value = filters.template get<Is>();

            if (filter_value.has_value()) {
                auto db_value = detail::convertForDb<FilterType>(*filter_value);
                addCriteria(Criteria(
                    FilterType::column(),
                    toDrogonOp(FilterType::op),
                    db_value
                ));
            }
        }(), ...);
    }(std::make_index_sequence<filter_count<Descriptor>>{});

    return {criteria, hasCriteria};
}

}  // namespace jcailloux::drogon::cache::list::decl

#endif  // CODIBOT_CACHE_LIST_DECL_GENERATEDCRITERIA_H
