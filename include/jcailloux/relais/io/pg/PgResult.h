#ifndef JCX_RELAIS_IO_PG_RESULT_H
#define JCX_RELAIS_IO_PG_RESULT_H

#include <charconv>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>

#include <libpq-fe.h>

#include "jcailloux/relais/io/pg/PgError.h"

namespace jcailloux::relais::io {

// PgResult — RAII wrapper for PGresult with typed column access

class PgResult {
public:
    // Row — lightweight proxy for a single row (no ownership)

    class Row {
    public:
        Row(const PgResult& result, int row) noexcept
            : result_(&result), row_(row) {}

        /// Get a typed value by column index.
        template<typename T>
        [[nodiscard]] T get(int col) const;

        /// Get an optional value (NULL -> nullopt).
        template<typename T>
        [[nodiscard]] std::optional<T> getOpt(int col) const {
            if (isNull(col)) return std::nullopt;
            return get<T>(col);
        }

        /// Check if a column value is NULL.
        [[nodiscard]] bool isNull(int col) const noexcept {
            return PQgetisnull(result_->raw(), row_, col) == 1;
        }

        /// Raw string value of a column.
        [[nodiscard]] std::string_view rawValue(int col) const noexcept {
            const char* v = PQgetvalue(result_->raw(), row_, col);
            int len = PQgetlength(result_->raw(), row_, col);
            return {v, static_cast<size_t>(len)};
        }

        [[nodiscard]] int index() const noexcept { return row_; }

    private:
        const PgResult* result_;
        int row_;
    };

    // Construction / ownership

    PgResult() noexcept = default;

    explicit PgResult(PGresult* result) noexcept
        : result_(result, &PQclear) {}

    [[nodiscard]] bool valid() const noexcept { return result_ != nullptr; }
    [[nodiscard]] bool empty() const noexcept { return rows() == 0; }

    // Dimensions

    [[nodiscard]] int rows() const noexcept {
        return result_ ? PQntuples(result_.get()) : 0;
    }

    // Status

    [[nodiscard]] bool ok() const noexcept {
        if (!result_) return false;
        auto s = PQresultStatus(result_.get());
        return s == PGRES_TUPLES_OK || s == PGRES_COMMAND_OK
            || s == PGRES_SINGLE_TUPLE;
    }

    /// Number of rows affected by INSERT/UPDATE/DELETE.
    [[nodiscard]] int affectedRows() const noexcept {
        if (!result_) return 0;
        const char* s = PQcmdTuples(result_.get());
        return s && *s ? std::atoi(s) : 0;
    }

    // Row access

    [[nodiscard]] Row operator[](int row) const noexcept {
        return Row(*this, row);
    }

    // Raw access (used internally by Row)

    [[nodiscard]] PGresult* raw() const noexcept { return result_.get(); }

private:
    std::unique_ptr<PGresult, decltype(&PQclear)> result_{nullptr, &PQclear};
};

// Type specializations for Row::get<T>

template<>
inline std::string PgResult::Row::get<std::string>(int col) const {
    return std::string(rawValue(col));
}

template<>
inline std::string_view PgResult::Row::get<std::string_view>(int col) const {
    return rawValue(col);
}

template<>
inline int32_t PgResult::Row::get<int32_t>(int col) const {
    auto sv = rawValue(col);
    int32_t val = 0;
    std::from_chars(sv.data(), sv.data() + sv.size(), val);
    return val;
}

template<>
inline int64_t PgResult::Row::get<int64_t>(int col) const {
    auto sv = rawValue(col);
    int64_t val = 0;
    std::from_chars(sv.data(), sv.data() + sv.size(), val);
    return val;
}

template<>
inline double PgResult::Row::get<double>(int col) const {
    auto sv = rawValue(col);
    double val = 0;
    std::from_chars(sv.data(), sv.data() + sv.size(), val);
    return val;
}

template<>
inline bool PgResult::Row::get<bool>(int col) const {
    auto v = rawValue(col);
    return !v.empty() && (v[0] == 't' || v[0] == 'T' || v[0] == '1');
}

} // namespace jcailloux::relais::io

#endif // JCX_RELAIS_IO_PG_RESULT_H
