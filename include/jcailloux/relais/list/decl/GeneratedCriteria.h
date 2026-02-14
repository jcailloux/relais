#ifndef CODIBOT_CACHE_LIST_DECL_GENERATEDCRITERIA_H
#define CODIBOT_CACHE_LIST_DECL_GENERATEDCRITERIA_H

#include <string>
#include <type_traits>
#include <utility>

#include "pqcoro/pg/PgParams.h"

#include "FilterDescriptor.h"
#include "ListDescriptor.h"
#include "GeneratedFilters.h"

namespace jcailloux::relais::cache::list::decl {

// =============================================================================
// Convert Op enum to SQL operator string
// =============================================================================

[[nodiscard]] constexpr const char* opToSql(Op op) noexcept {
    switch (op) {
        case Op::EQ: return "=";
        case Op::NE: return "!=";
        case Op::GT: return ">";
        case Op::GE: return ">=";
        case Op::LT: return "<";
        case Op::LE: return "<=";
    }
    return "=";
}

// =============================================================================
// Value conversion for DB queries
// =============================================================================

namespace detail {

/// Convert filter value for DB query and add to params
template<typename FilterType, typename T>
void addParamForDb(pqcoro::PgParams& params, const T& value) {
    if constexpr (std::is_same_v<typename FilterType::converter, AsString>) {
        using std::to_string;
        params.params.push_back(pqcoro::PgParam::text(toString(value)));
    } else if constexpr (std::is_integral_v<std::remove_cvref_t<T>>) {
        if constexpr (sizeof(T) <= 4) {
            params.params.push_back(pqcoro::PgParam::integer(static_cast<int32_t>(value)));
        } else {
            params.params.push_back(pqcoro::PgParam::bigint(static_cast<int64_t>(value)));
        }
    } else if constexpr (std::is_enum_v<std::remove_cvref_t<T>>) {
        using U = std::underlying_type_t<std::remove_cvref_t<T>>;
        params.params.push_back(pqcoro::PgParam::bigint(static_cast<int64_t>(static_cast<U>(value))));
    } else if constexpr (std::is_same_v<std::remove_cvref_t<T>, bool>) {
        params.params.push_back(pqcoro::PgParam::boolean(value));
    } else {
        // String-like types
        params.params.push_back(pqcoro::PgParam::text(std::string(value)));
    }
}

}  // namespace detail

// =============================================================================
// Build SQL WHERE clause from Filters
// =============================================================================

/// Build a parameterized SQL WHERE clause from filter values.
/// Returns: {where_clause, params, param_offset}
///   where_clause: e.g. "\"guild_id\"=$1 AND \"severity\"=$2" (empty if no active filters)
///   params: PgParams with values for each active filter
///   param_offset: next available parameter index (for appending more params)
template<typename Descriptor>
    requires ValidListDescriptor<Descriptor>
struct WhereClause {
    std::string sql;
    pqcoro::PgParams params;
    size_t next_param{1};
};

template<typename Descriptor>
    requires ValidListDescriptor<Descriptor>
[[nodiscard]] WhereClause<Descriptor> buildWhereClause(
    const Filters<Descriptor>& filters
) noexcept {
    WhereClause<Descriptor> result;

    [&]<size_t... Is>(std::index_sequence<Is...>) {
        ([&] {
            using FilterType = filter_at<Descriptor, Is>;
            const auto& filter_value = filters.template get<Is>();

            if (filter_value.has_value()) {
                if (!result.sql.empty()) result.sql += " AND ";
                result.sql += "\"";
                result.sql += FilterType::column();
                result.sql += "\"";
                result.sql += opToSql(FilterType::op);
                result.sql += "$";
                result.sql += std::to_string(result.next_param++);

                detail::addParamForDb<FilterType>(result.params, *filter_value);
            }
        }(), ...);
    }(std::make_index_sequence<filter_count<Descriptor>>{});

    return result;
}

}  // namespace jcailloux::relais::cache::list::decl

#endif  // CODIBOT_CACHE_LIST_DECL_GENERATEDCRITERIA_H
