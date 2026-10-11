#ifndef JCX_RELAIS_DETAIL_SHA1_H
#define JCX_RELAIS_DETAIL_SHA1_H

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace jcailloux::relais::detail {

namespace sha1_impl {

// One 64-byte block given as 16 big-endian words. Raw arrays, a 16-word rolling
// schedule and branch-free round loops keep the number of constant-evaluation
// steps low: Clang caps them (-fconstexpr-steps, 2^20 by default) and the largest
// script must hash within that budget.
constexpr void compress(uint32_t (&h)[5], uint32_t (&w)[16]) noexcept {
    uint32_t a = h[0], b = h[1], c = h[2], d = h[3], e = h[4];
    auto round = [&](int t, uint32_t f, uint32_t k) {
        if (t >= 16)
            w[t & 15] = std::rotl(w[(t + 13) & 15] ^ w[(t + 8) & 15] ^ w[(t + 2) & 15] ^ w[t & 15], 1);
        const uint32_t temp = std::rotl(a, 5) + f + e + k + w[t & 15];
        e = d;
        d = c;
        c = std::rotl(b, 30);
        b = a;
        a = temp;
    };
    for (int t = 0; t < 20; ++t)  round(t, (b & c) | (~b & d), 0x5A827999u);
    for (int t = 20; t < 40; ++t) round(t, b ^ c ^ d, 0x6ED9EBA1u);
    for (int t = 40; t < 60; ++t) round(t, (b & c) | (b & d) | (c & d), 0x8F1BBCDCu);
    for (int t = 60; t < 80; ++t) round(t, b ^ c ^ d, 0xCA62C1D6u);
    h[0] += a; h[1] += b; h[2] += c; h[3] += d; h[4] += e;
}

constexpr uint32_t byteAt(std::string_view data, std::size_t i) noexcept {
    return static_cast<unsigned char>(data[i]);
}

}  // namespace sha1_impl

/// SHA-1 (FIPS 180-4) of `data` as 40 lowercase hex digits, the form Redis uses
/// to name a cached script. constexpr so a script's digest is fixed at compile
/// time; callable at run time as well. Not for security: SHA-1 is broken against
/// collisions, Redis only uses it as a content address.
[[nodiscard]] constexpr std::array<char, 40> sha1Hex(std::string_view data) noexcept {
    uint32_t h[5] = {0x67452301u, 0xEFCDAB89u, 0x98BADCFEu, 0x10325476u, 0xC3D2E1F0u};
    uint32_t w[16] = {};

    const std::size_t n = data.size();
    std::size_t pos = 0;
    for (; pos + 64 <= n; pos += 64) {
        for (std::size_t t = 0; t < 16; ++t) {
            const std::size_t i = pos + 4 * t;
            w[t] = sha1_impl::byteAt(data, i) << 24 | sha1_impl::byteAt(data, i + 1) << 16
                 | sha1_impl::byteAt(data, i + 2) << 8 | sha1_impl::byteAt(data, i + 3);
        }
        sha1_impl::compress(h, w);
    }

    // Tail: the remaining bytes, 0x80, zeros, then the bit length as 8 big-endian
    // bytes. When the length no longer fits after 0x80, it goes in one more block.
    const std::size_t rest = n - pos;
    unsigned char block[64] = {};
    for (std::size_t i = 0; i < rest; ++i)
        block[i] = static_cast<unsigned char>(data[pos + i]);
    block[rest] = 0x80;
    auto load = [&] {
        for (std::size_t t = 0; t < 16; ++t)
            w[t] = uint32_t(block[4 * t]) << 24 | uint32_t(block[4 * t + 1]) << 16
                 | uint32_t(block[4 * t + 2]) << 8 | uint32_t(block[4 * t + 3]);
    };
    if (rest >= 56) {
        load();
        sha1_impl::compress(h, w);
        for (auto& byte : block) byte = 0;
    }
    const uint64_t bits = uint64_t(n) * 8;
    for (int i = 0; i < 8; ++i)
        block[63 - i] = static_cast<unsigned char>(bits >> (8 * i));
    load();
    sha1_impl::compress(h, w);

    constexpr char kDigits[] = "0123456789abcdef";
    std::array<char, 40> out{};
    for (std::size_t i = 0; i < 5; ++i)
        for (std::size_t j = 0; j < 8; ++j)
            out[i * 8 + j] = kDigits[(h[i] >> (28 - 4 * j)) & 0xFu];
    return out;
}

}  // namespace jcailloux::relais::detail

#endif  // JCX_RELAIS_DETAIL_SHA1_H
