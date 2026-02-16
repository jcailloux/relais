#ifndef JCX_RELAIS_IO_PG_PARAMS_H
#define JCX_RELAIS_IO_PG_PARAMS_H

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

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
};

} // namespace jcailloux::relais::io

#endif // JCX_RELAIS_IO_PG_PARAMS_H
