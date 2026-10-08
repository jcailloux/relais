#ifndef JCX_RELAIS_IO_PG_PARAMS_H
#define JCX_RELAIS_IO_PG_PARAMS_H

#include <concepts>
#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <vector>

#include "jcailloux/relais/TypeTraits.h"
#include "jcailloux/relais/detail/NumberText.h"

namespace jcailloux::relais::io {

// PgParam — type-safe PostgreSQL query parameter
//
// All values are stored in text format for simplicity and compatibility.
// libpq's PQsendQueryParams accepts text or binary; we use text format
// (paramFormats=NULL or 0) which is universally supported.

class PgParam {
public:
    // Null parameter
    PgParam() noexcept : null_(true) {}

    // Text value
    explicit PgParam(std::string value) noexcept
        : value_(std::move(value)), null_(false) {}

    [[nodiscard]] bool isNull() const noexcept { return null_; }

    // Text value pointer for libpq (nullptr if null)
    [[nodiscard]] const char* data() const noexcept {
        if (null_) return nullptr;
        return value_.c_str();
    }

    // Length for libpq paramLengths
    [[nodiscard]] int length() const noexcept {
        if (null_) return 0;
        return static_cast<int>(value_.size());
    }

    // Format for libpq paramFormats (0=text)
    [[nodiscard]] int format() const noexcept { return 0; }

    // Factory methods
    static PgParam null() noexcept { return {}; }

    static PgParam text(std::string_view s) {
        return PgParam(std::string(s));
    }

    /// Exact decimal text of any number: the shortest text that reads back to
    /// the same value for floating point, whatever the C locale.
    template<relais::detail::TextNumber T>
    static PgParam number(T v) {
        return PgParam(relais::detail::toText(v));
    }

    static PgParam boolean(bool v) {
        return PgParam(std::string(v ? "t" : "f"));
    }

private:
    std::string value_;
    bool null_ = true;

    friend bool operator==(const PgParam& a, const PgParam& b) noexcept {
        if (a.null_ != b.null_) return false;
        if (a.null_) return true;
        return a.value_ == b.value_;
    }
};

// PgArg — the closed set of types a query argument binds as
//
//   number (any arithmetic type but bool and characters)  exact decimal text
//   bool                                                  t / f
//   std::string, std::string_view, const char*            text
//   std::nullptr_t                                        NULL
//   std::optional<scalar>                                 NULL or the scalar
//   std::vector<scalar or optional<scalar>>               PostgreSQL array literal
//   PgParam                                               as is
//
// Nothing converts implicitly: what is bound is exactly what is written. An enum
// may mean its integer or its database text, so it must say which.

namespace detail {

template<typename T>
concept PgScalarArg = relais::detail::TextNumber<T> || std::same_as<T, bool>
                      || std::same_as<T, std::string> || std::same_as<T, std::string_view>
                      || std::same_as<T, const char*> || std::same_as<T, char*>;

template<typename T>
concept PgElementArg = PgScalarArg<T>
                       || (is_optional_v<T> && PgScalarArg<typename T::value_type>);

template<typename T>
concept PgValueArg = PgElementArg<T> || std::same_as<T, std::nullptr_t>
                     || std::same_as<T, PgParam>
                     || (is_std_vector_v<T> && PgElementArg<typename T::value_type>);

/// The type an argument holds once its optional/vector wrapper is removed.
template<typename T> struct pg_arg_inner { using type = T; };
template<typename T> struct pg_arg_inner<std::optional<T>> : pg_arg_inner<T> {};
template<typename T, typename A> struct pg_arg_inner<std::vector<T, A>> : pg_arg_inner<T> {};

}  // namespace detail

template<typename T>
concept PgArg =
    !std::is_enum_v<typename detail::pg_arg_inner<std::decay_t<T>>::type>  // bind an enum explicitly: std::to_underlying(e) for its integer, or its codec's toDb(e) for its text
    && detail::PgValueArg<std::decay_t<T>>;

// PgParams — helper to build parameter arrays for PQsendQueryParams

struct PgParams {
    std::vector<PgParam> params;

    // Build libpq-compatible arrays (valid as long as PgParams is alive)
    [[nodiscard]] int count() const noexcept {
        return static_cast<int>(params.size());
    }

    // Values array for PQsendQueryParams paramValues
    [[nodiscard]] std::vector<const char*> values() const {
        std::vector<const char*> v;
        v.reserve(params.size());
        for (auto& p : params) v.push_back(p.data());
        return v;
    }

    // Lengths array for PQsendQueryParams paramLengths
    [[nodiscard]] std::vector<int> lengths() const {
        std::vector<int> v;
        v.reserve(params.size());
        for (auto& p : params) v.push_back(p.length());
        return v;
    }

    // Formats array for PQsendQueryParams paramFormats
    [[nodiscard]] std::vector<int> formats() const {
        std::vector<int> v;
        v.reserve(params.size());
        for (auto& p : params) v.push_back(p.format());
        return v;
    }

    // Fill pre-allocated arrays (zero-alloc path)
    void fillArrays(const char** values, int* lengths, int* formats) const noexcept {
        for (size_t i = 0; i < params.size(); ++i) {
            values[i] = params[i].data();
            lengths[i] = params[i].length();
            formats[i] = params[i].format();
        }
    }

    // Variadic construction helper
    template<PgArg... Args>
    static PgParams make(Args&&... args) {
        PgParams result;
        result.params.reserve(sizeof...(args));
        (result.params.push_back(toParam(std::forward<Args>(args))), ...);
        return result;
    }

    // Incremental construction helpers (for complex cases: enums, json)
    template<PgArg T>
    void push(T&& v) { params.push_back(toParam(std::forward<T>(v))); }

    void pushNull() { params.push_back(PgParam::null()); }

    /// Build params from a key (expands tuples into individual params).
    template<typename Key>
    static PgParams fromKey(const Key& key) {
        if constexpr (is_tuple_v<Key>) {
            PgParams r;
            std::apply([&](const auto&... a) {
                r.params.reserve(sizeof...(a));
                (r.params.push_back(toParam(a)), ...);
            }, key);
            return r;
        } else {
            return make(key);
        }
    }

    /// Number of params a key expands to (compile-time).
    template<typename Key>
    static constexpr size_t keyParamCount() {
        if constexpr (is_tuple_v<Key>) {
            return std::tuple_size_v<Key>;
        } else {
            return 1;
        }
    }

    /// Build PG array literals from a vector of PgParams (one per key).
    /// Returns N PgParams objects, one per key column, each containing the
    /// PG array literal: {val1,val2,...}
    ///
    /// For simple keys (1 param each): returns a single PgParams with one array.
    /// For composite keys (N params each): returns a single PgParams with N arrays.
    ///
    /// Example: keys [PgParams({1}), PgParams({2}), PgParams({3})]
    ///   → PgParams with one param: "{1,2,3}"
    ///
    /// Example: composite keys [PgParams({1,"a"}), PgParams({2,"b"})]
    ///   → PgParams with two params: "{1,2}" and "{a,b}"
    static PgParams buildArrayLiteral(const std::vector<PgParams>& keys) {
        if (keys.empty()) return {};

        size_t n_cols = keys[0].params.size();
        PgParams result;
        result.params.reserve(n_cols);

        for (size_t col = 0; col < n_cols; ++col) {
            std::string arr = "{";
            for (size_t i = 0; i < keys.size(); ++i) {
                if (i > 0) arr += ',';
                const PgParam& p = keys[i].params[col];
                if (p.isNull()) {
                    arr += "NULL";
                } else {
                    appendArrayText(arr, {p.data(), static_cast<size_t>(p.length())});
                }
            }
            arr += '}';
            result.params.push_back(PgParam(std::move(arr)));
        }

        return result;
    }

    /// Build a single PG text-format array literal "{e1,e2,...}" from a vector,
    /// reusing the scalar element escaping. Exposes the private array path for
    /// `= ANY($n)` callers (e.g. list IN filters); numeric elements stay unquoted,
    /// strings are quoted/escaped when they contain a delimiter.
    template<typename T>
        requires PgArg<std::vector<T>>
    static PgParam arrayLiteral(const std::vector<T>& v) {
        return toParam(v);
    }

    /// Extract key column values as strings from a PgParams (for result matching).
    [[nodiscard]] std::vector<std::string> keyValues() const {
        std::vector<std::string> vals;
        vals.reserve(params.size());
        for (const auto& p : params) {
            vals.emplace_back(p.isNull() ? "" : std::string(p.data(),
                static_cast<size_t>(p.length())));
        }
        return vals;
    }

private:
    // Scalar binding. Only reached through PgArg-constrained entry points, so
    // the overloads never see an enum or a convertible class.
    static PgParam toParam(PgParam p) { return p; }
    template<relais::detail::TextNumber T>
    static PgParam toParam(T v) { return PgParam::number(v); }
    template<std::same_as<bool> B>
    static PgParam toParam(B v) { return PgParam::boolean(v); }
    static PgParam toParam(const char* v) { return PgParam::text(v); }
    static PgParam toParam(std::string_view v) { return PgParam::text(v); }
    static PgParam toParam(const std::string& v) { return PgParam::text(v); }
    static PgParam toParam(std::nullptr_t) { return PgParam::null(); }

    template<typename T>
    static PgParam toParam(const std::optional<T>& v) {
        if (!v) return PgParam::null();
        return toParam(*v);
    }

    // Array column: serialize a vector into a PostgreSQL text-format array literal
    // {e1,e2,...}, written in place. Numbers and booleans never contain a
    // delimiter and stay unquoted; text is quoted/escaped when it does — the
    // inverse of PgResult::Row::get<std::vector<T>>'s parser.
    template<typename T>
    static PgParam toParam(const std::vector<T>& v) {
        std::string arr = "{";
        bool first = true;
        for (const T& e : v) {
            if (!first) arr += ',';
            first = false;
            appendArrayElement(arr, e);
        }
        arr += '}';
        return PgParam(std::move(arr));
    }

    template<typename T>
    static void appendArrayElement(std::string& arr, const T& e) {
        if constexpr (is_optional_v<T>) {
            if (e) {
                appendArrayElement(arr, *e);
            } else {
                arr += "NULL";
            }
        } else if constexpr (relais::detail::TextNumber<T>) {
            relais::detail::appendNumber(arr, e);
        } else if constexpr (std::same_as<T, bool>) {
            arr += e ? 't' : 'f';
        } else {
            appendArrayText(arr, std::string_view(e));
        }
    }

    // Append one text element, quoting and backslash-escaping it when it is
    // empty or contains a PG array delimiter.
    static void appendArrayText(std::string& arr, std::string_view val) {
        bool needs_quoting = val.empty();
        if (!needs_quoting) {
            for (char c : val) {
                if (c == ',' || c == '{' || c == '}' || c == '"'
                    || c == '\\' || c == ' ') {
                    needs_quoting = true;
                    break;
                }
            }
        }
        if (needs_quoting) {
            arr += '"';
            for (char c : val) {
                if (c == '"' || c == '\\') arr += '\\';
                arr += c;
            }
            arr += '"';
        } else {
            arr += val;
        }
    }

    friend bool operator==(const PgParams& a, const PgParams& b) noexcept {
        return a.params == b.params;
    }
};

} // namespace jcailloux::relais::io

#endif // JCX_RELAIS_IO_PG_PARAMS_H
