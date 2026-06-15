#ifndef JCX_RELAIS_IO_PG_PARAMS_H
#define JCX_RELAIS_IO_PG_PARAMS_H

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#include "jcailloux/relais/TypeTraits.h"

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

    static PgParam integer(int32_t v) {
        return PgParam(std::to_string(v));
    }

    static PgParam bigint(int64_t v) {
        return PgParam(std::to_string(v));
    }

    static PgParam boolean(bool v) {
        return PgParam(std::string(v ? "t" : "f"));
    }

    static PgParam floating(double v) {
        return PgParam(std::to_string(v));
    }

    // Nullable variants
    template<typename T>
    static PgParam fromOptional(const std::optional<T>& opt) {
        if (!opt) return null();
        return fromValue(*opt);
    }

private:
    static PgParam fromValue(int32_t v) { return integer(v); }
    static PgParam fromValue(int64_t v) { return bigint(v); }
    static PgParam fromValue(bool v) { return boolean(v); }
    static PgParam fromValue(const std::string& v) { return text(v); }
    static PgParam fromValue(std::string_view v) { return text(v); }

    std::string value_;
    bool null_ = true;

    friend bool operator==(const PgParam& a, const PgParam& b) noexcept {
        if (a.null_ != b.null_) return false;
        if (a.null_) return true;
        return a.value_ == b.value_;
    }
};

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
    template<typename... Args>
    static PgParams make(Args&&... args) {
        PgParams result;
        result.params.reserve(sizeof...(args));
        (result.params.push_back(toParam(std::forward<Args>(args))), ...);
        return result;
    }

    // Incremental construction helpers (for complex cases: enums, json)
    template<typename T>
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
                appendArrayElement(arr, keys[i].params[col]);
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
    static PgParam toParam(PgParam p) { return p; }
    static PgParam toParam(int32_t v) { return PgParam::integer(v); }
    static PgParam toParam(int64_t v) { return PgParam::bigint(v); }
    static PgParam toParam(double v) { return PgParam::floating(v); }
    static PgParam toParam(bool v) { return PgParam::boolean(v); }
    static PgParam toParam(const char* v) { return PgParam::text(v); }
    static PgParam toParam(std::string_view v) { return PgParam::text(v); }
    static PgParam toParam(const std::string& v) { return PgParam::text(v); }
    static PgParam toParam(std::nullptr_t) { return PgParam::null(); }

    template<typename T>
    static PgParam toParam(const std::optional<T>& v) {
        return PgParam::fromOptional(v);
    }

    // Array column: serialize a vector into a PostgreSQL text-format array literal
    // {e1,e2,...}. Each element reuses the scalar toParam (so numeric elements are
    // never quoted) and is quoted/escaped when it contains a delimiter — the inverse
    // of PgResult::Row::get<std::vector<T>>'s parser.
    template<typename T>
    static PgParam toParam(const std::vector<T>& v) {
        std::string arr = "{";
        for (size_t i = 0; i < v.size(); ++i) {
            if (i > 0) arr += ',';
            appendArrayElement(arr, toParam(v[i]));
        }
        arr += '}';
        return PgParam(std::move(arr));
    }

    // Append one already-serialized scalar param as an array element, quoting and
    // backslash-escaping it when it contains a PG array delimiter.
    static void appendArrayElement(std::string& arr, const PgParam& p) {
        if (p.isNull()) { arr += "NULL"; return; }
        std::string_view val(p.data(), static_cast<size_t>(p.length()));
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
