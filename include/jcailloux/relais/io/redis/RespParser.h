#ifndef JCX_RELAIS_IO_REDIS_RESP_PARSER_H
#define JCX_RELAIS_IO_REDIS_RESP_PARSER_H

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

namespace jcailloux::relais::io {

// RespValue — parsed RESP2 value, stored in a flat tree.
//
// Strings reference offsets into the arena (zero-copy within the parser).
// Arrays are stored as (offset, count) into the flat values_ vector.

struct RespValue {
    enum class Type : uint8_t {
        Nil, SimpleString, Error, Integer, BulkString, Array
    };

    Type type = Type::Nil;
    int64_t integer = 0;
    uint32_t str_offset = 0;
    uint32_t str_len = 0;
    uint32_t array_offset = 0;
    uint32_t array_count = 0;
};

// RespParser — incremental RESP2 parser with arena allocation.
//
// All string data is stored in a single arena_ string.
// All RespValues are stored in a flat values_ vector.
// Supports partial reads (returns 0 = incomplete, >0 = bytes consumed).
//
// Usage:
//   RespParser parser;
//   size_t consumed = parser.parse(data, len);
//   if (consumed > 0) {
//       const auto& root = parser.root();
//       auto sv = parser.getString(root);
//   }

class RespParser {
public:
    // Parse data. Returns number of bytes consumed (0 = need more data).
    // After a successful parse, root() returns the parsed value.
    size_t parse(const char* data, size_t len) {
        arena_.clear();
        values_.clear();
        const char* end = data + len;
        const char* pos = data;
        if (!parseValue(pos, end))
            return 0;
        return static_cast<size_t>(pos - data);
    }

    // Access the root parsed value (valid after parse() returns >0).
    [[nodiscard]] const RespValue& root() const noexcept { return values_[0]; }
    [[nodiscard]] const RespValue& value(uint32_t index) const noexcept { return values_[index]; }

    // Get string_view for a RespValue (zero-copy into arena).
    [[nodiscard]] std::string_view getString(const RespValue& v) const noexcept {
        return {arena_.data() + v.str_offset, v.str_len};
    }

    // Array element access
    [[nodiscard]] const RespValue& arrayElement(const RespValue& v, size_t index) const noexcept {
        return values_[v.array_offset + index];
    }

    // Number of values stored (for testing)
    [[nodiscard]] size_t valueCount() const noexcept { return values_.size(); }

    // Reset state
    void reset() {
        arena_.clear();
        values_.clear();
    }

private:
    // Find \r\n in [pos, end). Returns pointer to \r or nullptr if not found.
    static const char* findCRLF(const char* pos, const char* end) noexcept {
        while (pos + 1 < end) {
            if (pos[0] == '\r' && pos[1] == '\n') return pos;
            ++pos;
        }
        return nullptr;
    }

    // Parse a signed integer from [start, eol).
    static int64_t parseInteger(const char* start, const char* eol) noexcept {
        int64_t val = 0;
        bool neg = false;
        if (start < eol && *start == '-') { neg = true; ++start; }
        while (start < eol) {
            val = val * 10 + (*start - '0');
            ++start;
        }
        return neg ? -val : val;
    }

    // Parse one RESP2 value, advancing pos. Returns false on incomplete data.
    bool parseValue(const char*& pos, const char* end) {
        if (pos >= end) return false;

        char type = *pos++;

        switch (type) {
        case '+': return parseSimpleString(pos, end);
        case '-': return parseError(pos, end);
        case ':': return parseIntegerType(pos, end);
        case '$': return parseBulkString(pos, end);
        case '*': return parseArray(pos, end);
        default:  return false;
        }
    }

    // +<data>\r\n
    bool parseSimpleString(const char*& pos, const char* end) {
        auto* eol = findCRLF(pos, end);
        if (!eol) return false;

        uint32_t offset = static_cast<uint32_t>(arena_.size());
        uint32_t len = static_cast<uint32_t>(eol - pos);
        arena_.append(pos, len);

        RespValue v;
        v.type = RespValue::Type::SimpleString;
        v.str_offset = offset;
        v.str_len = len;
        values_.push_back(v);

        pos = eol + 2;
        return true;
    }

    // -<error>\r\n
    bool parseError(const char*& pos, const char* end) {
        auto* eol = findCRLF(pos, end);
        if (!eol) return false;

        uint32_t offset = static_cast<uint32_t>(arena_.size());
        uint32_t len = static_cast<uint32_t>(eol - pos);
        arena_.append(pos, len);

        RespValue v;
        v.type = RespValue::Type::Error;
        v.str_offset = offset;
        v.str_len = len;
        values_.push_back(v);

        pos = eol + 2;
        return true;
    }

    // :<integer>\r\n
    bool parseIntegerType(const char*& pos, const char* end) {
        auto* eol = findCRLF(pos, end);
        if (!eol) return false;

        RespValue v;
        v.type = RespValue::Type::Integer;
        v.integer = parseInteger(pos, eol);
        values_.push_back(v);

        pos = eol + 2;
        return true;
    }

    // $<len>\r\n<data>\r\n  or  $-1\r\n (nil)
    bool parseBulkString(const char*& pos, const char* end) {
        auto* eol = findCRLF(pos, end);
        if (!eol) return false;

        int64_t len = parseInteger(pos, eol);
        pos = eol + 2;

        if (len < 0) {
            // Nil bulk string
            RespValue v;
            v.type = RespValue::Type::Nil;
            values_.push_back(v);
            return true;
        }

        // Need len bytes + \r\n
        auto ulen = static_cast<size_t>(len);
        if (pos + ulen + 2 > end) return false;

        uint32_t offset = static_cast<uint32_t>(arena_.size());
        arena_.append(pos, ulen);

        RespValue v;
        v.type = RespValue::Type::BulkString;
        v.str_offset = offset;
        v.str_len = static_cast<uint32_t>(ulen);
        values_.push_back(v);

        pos += ulen + 2; // skip data + \r\n
        return true;
    }

    // *<count>\r\n<elements...>  or  *-1\r\n (nil array)
    bool parseArray(const char*& pos, const char* end) {
        auto* eol = findCRLF(pos, end);
        if (!eol) return false;

        int64_t count = parseInteger(pos, eol);
        pos = eol + 2;

        if (count < 0) {
            // Nil array
            RespValue v;
            v.type = RespValue::Type::Nil;
            values_.push_back(v);
            return true;
        }

        // Reserve slot for the array value itself
        auto arrayIdx = static_cast<uint32_t>(values_.size());
        values_.push_back({}); // placeholder

        uint32_t childStart = static_cast<uint32_t>(values_.size());
        auto ucount = static_cast<uint32_t>(count);

        for (uint32_t i = 0; i < ucount; ++i) {
            if (!parseValue(pos, end)) return false;
        }

        // Fill in the array value
        values_[arrayIdx].type = RespValue::Type::Array;
        values_[arrayIdx].array_offset = childStart;
        values_[arrayIdx].array_count = ucount;

        return true;
    }

    std::string arena_;
    std::vector<RespValue> values_;
};

} // namespace jcailloux::relais::io

#endif // JCX_RELAIS_IO_REDIS_RESP_PARSER_H
