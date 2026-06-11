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
#include <vector>

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

    /// Construct from a shared_ptr (for pipeline coalescing — multiple waiters
    /// can share the same PGresult without copying).
    explicit PgResult(std::shared_ptr<PGresult> shared) noexcept
        : result_(std::move(shared)) {}

    [[nodiscard]] bool valid() const noexcept { return result_ != nullptr; }
    [[nodiscard]] bool empty() const noexcept { return rows() == 0; }

    // Dimensions

    [[nodiscard]] int rows() const noexcept {
        if (!result_) return 0;
        if (is_slice_) return 1;
        return PQntuples(result_.get());
    }

    // Status

    [[nodiscard]] bool ok() const noexcept {
        if (!result_) return false;
        auto s = PQresultStatus(result_.get());
        return s == PGRES_TUPLES_OK || s == PGRES_COMMAND_OK
            || s == PGRES_SINGLE_TUPLE;
    }

    /// Check if this result represents a pipeline aborted state.
    [[nodiscard]] bool pipelineAborted() const noexcept {
        if (!result_) return false;
        return PQresultStatus(result_.get()) == PGRES_PIPELINE_ABORTED;
    }

    /// Number of rows affected by INSERT/UPDATE/DELETE.
    [[nodiscard]] int affectedRows() const noexcept {
        if (!result_) return 0;
        const char* s = PQcmdTuples(result_.get());
        return s && *s ? std::atoi(s) : 0;
    }

    // Row access

    [[nodiscard]] Row operator[](int row) const noexcept {
        return Row(*this, is_slice_ ? row_offset_ : row);
    }

    // Raw access (used internally by Row)

    [[nodiscard]] PGresult* raw() const noexcept { return result_.get(); }

    /// Get the underlying shared_ptr (for pipeline coalescing).
    [[nodiscard]] const std::shared_ptr<PGresult>& shared() const noexcept {
        return result_;
    }

    /// Number of columns in the result.
    [[nodiscard]] int cols() const noexcept {
        return result_ ? PQnfields(result_.get()) : 0;
    }

    /// Create a single-row view sharing the same PGresult.
    /// The returned PgResult shares ownership of the underlying PGresult
    /// but presents a view offset to the given row index.
    /// This enables zero-copy distribution of ANY-batch results.
    static PgResult sliceRow(const PgResult& batch, int row_index) {
        PgResult r;
        r.result_ = batch.result_;
        r.row_offset_ = row_index;
        r.is_slice_ = true;
        return r;
    }

private:
    std::shared_ptr<PGresult> result_{nullptr, &PQclear};
    int row_offset_ = 0;   // for sliceRow views
    bool is_slice_ = false;
};

// Array parsing helpers — PostgreSQL text-format arrays ({1,2,3}, {}, {"a,b"})

namespace detail {

template<typename>
inline constexpr bool always_false_v = false;

template<typename T> struct is_std_vector : std::false_type {};
template<typename U, typename A> struct is_std_vector<std::vector<U, A>> : std::true_type {};
template<typename T> inline constexpr bool is_std_vector_v = is_std_vector<T>::value;

template<typename T> struct is_optional : std::false_type {};
template<typename U> struct is_optional<std::optional<U>> : std::true_type {};
template<typename T> inline constexpr bool is_optional_v = is_optional<T>::value;

/// Parse one already-unquoted scalar token from a PostgreSQL array element.
template<typename T>
inline T parseArrayElement(std::string_view sv) {
    if constexpr (std::is_same_v<T, std::string>) {
        return std::string(sv);
    } else if constexpr (std::is_same_v<T, bool>) {
        return !sv.empty() && (sv[0] == 't' || sv[0] == 'T' || sv[0] == '1');
    } else if constexpr (std::is_arithmetic_v<T>) {
        T val{};
        auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), val);
        if (ec != std::errc{}) [[unlikely]]
            throw PgError("from_chars failed for array element '" + std::string(sv) + "'");
        return val;
    } else {
        static_assert(always_false_v<T>, "Unsupported array element type");
    }
}

/// Parse a PostgreSQL text-format array literal into a vector. Quoting-aware:
/// elements may be double-quoted with backslash escaping, so commas/braces inside
/// quotes stay literal (correct for text[]). Unquoted NULL is rejected — model the
/// column as a NOT NULL aggregate (array_agg over a real column) so the element
/// type stays non-optional.
template<typename T>
inline std::vector<T> parsePgArray(std::string_view s) {
    static_assert(!is_optional_v<T>,
                  "vector<optional<T>> is unsupported: NULL array elements are rejected");
    static_assert(!std::is_same_v<T, std::string_view>,
                  "vector<string_view> is unsafe: unescaped elements would dangle, use std::string");
    std::vector<T> out;
    if (s.size() < 2 || s.front() != '{' || s.back() != '}')
        return out;                       // NULL column or malformed → empty
    s.remove_prefix(1);
    s.remove_suffix(1);
    if (s.empty()) return out;            // {} → empty

    std::string buf;
    const size_t n = s.size();
    size_t i = 0;
    while (true) {
        if (i < n && s[i] == '"') {
            buf.clear();                  // quoted element: un-escape into buf
            ++i;
            while (i < n) {
                const char c = s[i];
                if (c == '\\' && i + 1 < n) { buf.push_back(s[i + 1]); i += 2; continue; }
                if (c == '"') { ++i; break; }
                buf.push_back(c);
                ++i;
            }
            out.push_back(parseArrayElement<T>(buf));
        } else {                          // unquoted element: view up to next comma
            const size_t start = i;
            while (i < n && s[i] != ',') ++i;
            const std::string_view tok = s.substr(start, i - start);
            if (tok == "NULL" || tok == "null") [[unlikely]]
                throw PgError("NULL array element is unsupported");
            out.push_back(parseArrayElement<T>(tok));
        }
        if (i < n && s[i] == ',') { ++i; continue; }
        break;
    }
    return out;
}

}  // namespace detail

// Type specializations for Row::get<T>

// Primary template: only valid for std::vector<Scalar> (array columns); every
// supported scalar has an explicit specialization below. Any other type fails here
// with a readable message instead of an opaque link error.
template<typename T>
inline T PgResult::Row::get(int col) const {
    if constexpr (detail::is_std_vector_v<T>) {
        return detail::parsePgArray<typename T::value_type>(rawValue(col));
    } else {
        static_assert(detail::always_false_v<T>,
                      "PgResult::Row::get<T>: unsupported type");
    }
}

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
    auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), val);
    if (ec != std::errc{}) [[unlikely]]
        throw PgError("from_chars failed for int32 column " + std::to_string(col));
    return val;
}

template<>
inline int64_t PgResult::Row::get<int64_t>(int col) const {
    auto sv = rawValue(col);
    int64_t val = 0;
    auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), val);
    if (ec != std::errc{}) [[unlikely]]
        throw PgError("from_chars failed for int64 column " + std::to_string(col));
    return val;
}

template<>
inline double PgResult::Row::get<double>(int col) const {
    auto sv = rawValue(col);
    double val = 0;
    auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), val);
    if (ec != std::errc{}) [[unlikely]]
        throw PgError("from_chars failed for double column " + std::to_string(col));
    return val;
}

template<>
inline bool PgResult::Row::get<bool>(int col) const {
    auto v = rawValue(col);
    return !v.empty() && (v[0] == 't' || v[0] == 'T' || v[0] == '1');
}

} // namespace jcailloux::relais::io

#endif // JCX_RELAIS_IO_PG_RESULT_H
