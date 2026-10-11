#ifndef JCX_RELAIS_IO_REDIS_SCRIPT_H
#define JCX_RELAIS_IO_REDIS_SCRIPT_H

#include <array>
#include <string_view>

#include "jcailloux/relais/detail/Sha1.h"

namespace jcailloux::relais::io {

/// A Lua script and the SHA1 of its text. Redis names a cached script by that
/// SHA1: EVALSHA sends `sha1()`, EVAL sends `body`.
struct RedisScript {
    std::string_view body;
    std::array<char, 40> sha1_hex;  // lowercase, as Redis prints it

    [[nodiscard]] constexpr std::string_view sha1() const noexcept {
        return {sha1_hex.data(), sha1_hex.size()};
    }
};

/// Builds a script whose digest is computed at compile time. `body` is viewed,
/// not copied: it must have static storage (a string literal or a constexpr
/// buffer).
[[nodiscard]] consteval RedisScript makeRedisScript(std::string_view body) {
    return RedisScript{body, detail::sha1Hex(body)};
}

}  // namespace jcailloux::relais::io

#endif  // JCX_RELAIS_IO_REDIS_SCRIPT_H
