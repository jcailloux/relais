#ifndef JCX_RELAIS_LIST_SPEC_GENERATEDCRITERIA_H
#define JCX_RELAIS_LIST_SPEC_GENERATEDCRITERIA_H

#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "jcailloux/relais/io/pg/PgParams.h"

#include "FilterDescriptor.h"
#include "ListDescriptor.h"
#include "GeneratedFilters.h"

namespace jcailloux::relais::list::spec {

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
void addParamForDb(io::PgParams& params, const T& value) {
    using Converter = typename FilterType::converter;
    if constexpr (is_via_v<Converter>) {
        // Equality compares the stored text; an ordering operator binds the
        // underlying value.
        if constexpr (FilterType::op == Op::EQ || FilterType::op == Op::NE) {
            params.params.push_back(io::PgParam::text(Converter::codec::toDb(value)));
        } else {
            params.push(static_cast<int64_t>(std::to_underlying(value)));
        }
    } else if constexpr (std::is_same_v<Converter, AsString>) {
        using std::to_string;
        params.params.push_back(io::PgParam::text(toString(value)));
    } else if constexpr (std::is_enum_v<T>) {
        params.push(static_cast<int64_t>(std::to_underlying(value)));
    } else {
        params.push(value);
    }
}

/// Push exactly ONE PgParam — the PG array literal "{e1,e2,...}" — for an IN
/// filter's set, consumed by SQL `= ANY($n)`. Pushing exactly one param is the
/// critical invariant: 0 or N would shift every subsequent $n ↔ cursor/offset
/// binding. Element escaping is reused from PgParams (numeric unquoted, strings
/// quoted on delimiters). An empty set yields `{}` → `= ANY('{}')` → zero rows.
template<typename FilterType, typename T>
void addArrayParamForDb(io::PgParams& params, const std::vector<T>& values) {
    using Converter = typename FilterType::converter;
    if constexpr (is_via_v<Converter>) {
        std::vector<std::string_view> db;
        db.reserve(values.size());
        for (const auto& v : values) db.push_back(Converter::codec::toDb(v));
        params.params.push_back(io::PgParams::arrayLiteral(db));
    } else {
        params.params.push_back(io::PgParams::arrayLiteral(values));
    }
}

/// A mapped enum under an ordering operator compares its rank, not its text.
template<typename FilterType>
inline constexpr bool compares_rank = is_via_v<typename FilterType::converter>
    && FilterType::op != Op::EQ && FilterType::op != Op::NE && !FilterType::is_set_op;

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
    requires ValidFilterSet<Descriptor>
struct WhereClause {
    std::string sql;
    io::PgParams params;
    size_t next_param{1};
};

template<typename Descriptor>
    requires ValidFilterSet<Descriptor>
[[nodiscard]] WhereClause<Descriptor> buildWhereClause(
    const Filters<Descriptor>& filters
) noexcept {
    WhereClause<Descriptor> result;

    [&]<size_t... Is>(std::index_sequence<Is...>) {
        ([&] {
            using FilterType = filter_at<Descriptor, Is>;
            const auto& filter_value = std::get<Is>(filters.values);

            if (filter_value.has_value()) {
                if (!result.sql.empty()) result.sql += " AND ";
                detail::appendColumnSql<typename FilterType::converter,
                                        detail::compares_rank<FilterType>>(
                    result.sql, FilterType::column());

                if constexpr (FilterType::is_set_op) {
                    // Set op → "col" = ANY($n) (IN) | "col" != ALL($n) (NIN). One
                    // array param either way — the $n numbering invariant holds for
                    // both (0 or N params would shift every later cursor binding).
                    result.sql += (FilterType::op == Op::IN) ? " = ANY($" : " != ALL($";
                    result.sql += std::to_string(result.next_param++);
                    result.sql += ")";
                    detail::addArrayParamForDb<FilterType>(result.params, *filter_value);
                } else {
                    result.sql += opToSql(FilterType::op);
                    result.sql += "$";
                    result.sql += std::to_string(result.next_param++);
                    detail::addParamForDb<FilterType>(result.params, *filter_value);
                }
            }
        }(), ...);
    }(std::make_index_sequence<filter_count<Descriptor>>{});

    return result;
}

}  // namespace jcailloux::relais::list::spec

#endif  // JCX_RELAIS_LIST_SPEC_GENERATEDCRITERIA_H
