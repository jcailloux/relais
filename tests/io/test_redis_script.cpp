#include <catch2/catch_test_macros.hpp>
#include <jcailloux/relais/detail/Sha1.h>
#include <jcailloux/relais/io/redis/RedisScript.h>

#include <array>
#include <cstddef>
#include <string>
#include <string_view>

using jcailloux::relais::detail::sha1Hex;
using jcailloux::relais::io::makeRedisScript;
using jcailloux::relais::io::RedisScript;

namespace {

constexpr bool digestIs(std::string_view data, std::string_view expected) {
    const auto hex = sha1Hex(data);
    return std::string_view(hex.data(), hex.size()) == expected;
}

template<std::size_t N>
constexpr std::array<char, N> repeatA() {
    std::array<char, N> out{};
    for (auto& c : out) c = 'a';
    return out;
}

// Static storage, so a string_view over them is a constant expression.
template<std::size_t N>
constexpr std::array<char, N> kA = repeatA<N>();

template<std::size_t N>
constexpr std::string_view as(const std::array<char, N>& a) { return {a.data(), N}; }

struct Vector {
    std::string_view input;
    std::string_view sha1;
};

// Reference digests from Python's hashlib.sha1. The 'a' x N lengths straddle every
// padding boundary: 55 and 56 (the length field fits, or no longer fits, in the
// last block), 63/64/65 (one byte short of, exactly, one byte past a block), 119
// and 120 (same boundary one block later). The last input has a NUL and a high byte.
constexpr std::array kVectors{
    Vector{"", "da39a3ee5e6b4b0d3255bfef95601890afd80709"},
    Vector{"abc", "a9993e364706816aba3e25717850c26c9cd0d89d"},
    Vector{"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq",
           "84983e441c3bd26ebaae4aa1f95129e5e54670f1"},
    Vector{as(kA<1>), "86f7e437faa5a7fce15d1ddcb9eaeaea377667b8"},
    Vector{as(kA<55>), "c1c8bbdc22796e28c0e15163d20899b65621d65a"},
    Vector{as(kA<56>), "c2db330f6083854c99d4b5bfb6e8f29f201be699"},
    Vector{as(kA<63>), "03f09f5b158a7a8cdad920bddc29b81c18a551f5"},
    Vector{as(kA<64>), "0098ba824b5c16427bd7a1122a5a442a25ec644d"},
    Vector{as(kA<65>), "11655326c708d70319be2610e8a57d9a5b959d3b"},
    Vector{as(kA<119>), "ee971065aaa017e0632a8ca6c77bb3bf8b1dfc56"},
    Vector{as(kA<120>), "f34c1488385346a55709ba056ddd08280dd4c6d6"},
    Vector{as(kA<128>), "ad5b3fdbcb526778c2839d2f151ea753995e26a0"},
    Vector{as(kA<1000>), "291e9a6c66994949b57ba5e650361e98fc36b1ba"},
    Vector{std::string_view("a\0b\xff", 4), "63fcb4a9187af3e3e3319584f43e809426e0b000"},
};

constexpr bool allVectorsMatch() {
    for (const auto& v : kVectors)
        if (!digestIs(v.input, v.sha1)) return false;
    return true;
}

static_assert(allVectorsMatch());

// Twice the biggest Lua script relais ships (about 8.6 KB): its compile-time digest
// must fit within the compilers' default constexpr budgets with room to grow
// (Clang's -fconstexpr-steps is the tight one).
inline constexpr RedisScript kLarge = makeRedisScript(as(kA<16384>));
static_assert(kLarge.sha1() == "12ff347b4f27d69e1f328e6f4b5573e3666e122f");

inline constexpr RedisScript kGet = makeRedisScript("return redis.call('GET', KEYS[1])");
static_assert(kGet.body == "return redis.call('GET', KEYS[1])");
static_assert(kGet.sha1() == "d3c21d0c2b9ca22f82737626a27bcaf5d288f99f");

}  // namespace

TEST_CASE("sha1Hex matches the reference vectors at run time", "[redis-script]") {
    for (const auto& v : kVectors) {
        // Copy into a run-time buffer so the digest is not constant-folded.
        const std::string input(v.input);
        const auto hex = sha1Hex(input);
        CAPTURE(input.size());
        REQUIRE(std::string_view(hex.data(), hex.size()) == v.sha1);
    }
}

TEST_CASE("makeRedisScript keeps the body and its digest", "[redis-script]") {
    REQUIRE(kGet.body == "return redis.call('GET', KEYS[1])");
    REQUIRE(kGet.sha1() == "d3c21d0c2b9ca22f82737626a27bcaf5d288f99f");
    REQUIRE(kGet.sha1().size() == 40);
    REQUIRE(kLarge.body.size() == 16384);
    REQUIRE(kLarge.sha1() == "12ff347b4f27d69e1f328e6f4b5573e3666e122f");
}
