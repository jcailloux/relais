#ifndef JCX_RELAIS_IO_REDIS_RESULT_H
#define JCX_RELAIS_IO_REDIS_RESULT_H

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "jcailloux/relais/io/redis/RespParser.h"

namespace jcailloux::relais::io {

// RedisResult — RAII wrapper for parsed RESP2 response with typed accessors.
//
// Two modes:
//   - Owning: holds a shared_ptr<RespParser> (root result from a command)
//   - View: references a parent's parser + value index (for at() array access)
//
// Uses RespParser with arena allocation for zero-copy string access.

class RedisResult {
public:
    RedisResult() noexcept = default;

    // Owning constructor — takes ownership of parsed response
    explicit RedisResult(std::shared_ptr<RespParser> parser) noexcept
        : parser_(std::move(parser)), index_(0) {}

    // Type checks

    [[nodiscard]] bool valid() const noexcept { return parser_ != nullptr; }

    [[nodiscard]] bool isNil() const noexcept {
        if (!parser_) return true;
        return value().type == RespValue::Type::Nil;
    }

    [[nodiscard]] bool isString() const noexcept {
        if (!parser_) return false;
        auto t = value().type;
        return t == RespValue::Type::BulkString
            || t == RespValue::Type::SimpleString;
    }

    [[nodiscard]] bool isInteger() const noexcept {
        if (!parser_) return false;
        return value().type == RespValue::Type::Integer;
    }

    [[nodiscard]] bool isArray() const noexcept {
        if (!parser_) return false;
        return value().type == RespValue::Type::Array;
    }

    [[nodiscard]] bool isError() const noexcept {
        if (!parser_) return false;
        return value().type == RespValue::Type::Error;
    }

    // Value accessors

    [[nodiscard]] std::string_view asStringView() const noexcept {
        if (!isString()) return {};
        return parser_->getString(value());
    }

    [[nodiscard]] std::string asString() const {
        return std::string(asStringView());
    }

    [[nodiscard]] int64_t asInteger() const noexcept {
        if (!parser_) return 0;
        return value().integer;
    }

    [[nodiscard]] std::string errorMessage() const {
        if (!isError()) return {};
        return std::string(parser_->getString(value()));
    }

    // Array access

    [[nodiscard]] size_t arraySize() const noexcept {
        if (!isArray()) return 0;
        return value().array_count;
    }

    [[nodiscard]] RedisResult at(size_t index) const noexcept {
        if (!isArray() || index >= value().array_count) return {};
        return RedisResult(parser_, value().array_offset + static_cast<uint32_t>(index));
    }

    [[nodiscard]] std::vector<std::string> asStringArray() const {
        std::vector<std::string> result;
        if (!isArray()) return result;
        auto& v = value();
        result.reserve(v.array_count);
        for (uint32_t i = 0; i < v.array_count; ++i) {
            auto& elem = parser_->value(v.array_offset + i);
            if (elem.type == RespValue::Type::BulkString
                || elem.type == RespValue::Type::SimpleString)
            {
                auto sv = parser_->getString(elem);
                result.emplace_back(sv);
            } else {
                result.emplace_back();
            }
        }
        return result;
    }

private:
    // View constructor (for array element access)
    RedisResult(std::shared_ptr<RespParser> parser, uint32_t index) noexcept
        : parser_(std::move(parser)), index_(index) {}

    [[nodiscard]] const RespValue& value() const noexcept {
        return parser_->value(index_);
    }

    std::shared_ptr<RespParser> parser_;
    uint32_t index_ = 0;
};

} // namespace jcailloux::relais::io

#endif // JCX_RELAIS_IO_REDIS_RESULT_H
