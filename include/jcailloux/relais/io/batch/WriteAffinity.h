#ifndef JCX_RELAIS_IO_BATCH_WRITE_AFFINITY_H
#define JCX_RELAIS_IO_BATCH_WRITE_AFFINITY_H

#include <cstddef>
#include <cstdint>
#include <string_view>

#include "jcailloux/relais/io/pg/PgParams.h"

namespace jcailloux::relais::io::batch::detail {

// WriteAffinity — which row a write targets, as far as ordering is concerned.
//
// Two writes on the same row must reach the database in submission order;
// writes on different rows need not. The scheduler may only reorder writes
// whose affinities say they cannot touch the same row:
//
//   kOrdered  unknown dependency (predicate, several rows, raw SQL): the write
//             keeps the order of everything submitted around it
//   kFree     no other write can target this row (database-assigned key)
//   row       the table, and where the primary key sits in the write's
//             parameters
//
// A row is named lazily: only a batch that is split hashes it (rowHash), from
// the key text already bound for PostgreSQL. A write sent alone pays nothing.
// The text is what makes two writes agree: two repositories on the same table
// with different key types (int32_t, int64_t) bind the same text.
struct WriteAffinity {
    uint16_t table = 0;   ///< tableTag of the table (row only)
    uint8_t first = 0;    ///< index of the first key parameter (row only)
    uint8_t count = 0;    ///< key parameters; 0 = ordered, kFreeCount = free

    static constexpr uint8_t kFreeCount = 0xFF;

    [[nodiscard]] constexpr bool ordered() const noexcept { return count == 0; }
    [[nodiscard]] constexpr bool free() const noexcept { return count == kFreeCount; }
    [[nodiscard]] constexpr bool row() const noexcept { return !ordered() && !free(); }

    friend constexpr bool operator==(WriteAffinity, WriteAffinity) noexcept = default;
};

// Fits in the padding of a write entry; passed in a register.
static_assert(sizeof(WriteAffinity) == 4);

inline constexpr WriteAffinity kOrdered{};
inline constexpr WriteAffinity kFree{0, 0, WriteAffinity::kFreeCount};

namespace write_affinity {

inline constexpr uint64_t kFnvOffset = 14695981039346656037ull;
inline constexpr uint64_t kFnvPrime = 1099511628211ull;

constexpr uint64_t mix(uint64_t h, std::string_view bytes) noexcept {
    for (char c : bytes) {
        h ^= static_cast<unsigned char>(c);
        h *= kFnvPrime;
    }
    return h;
}

constexpr uint64_t mixLength(uint64_t h, uint64_t n) noexcept {
    for (int i = 0; i < 8; ++i, n >>= 8) {
        h ^= n & 0xFF;
        h *= kFnvPrime;
    }
    return h;
}

}  // namespace write_affinity

/// Tag of a table: its name folded to 16 bits. Two tables sharing a tag only
/// share orderings between rows whose keys also hash alike.
constexpr uint16_t tableTag(std::string_view table) noexcept {
    uint64_t h = write_affinity::mix(write_affinity::kFnvOffset, table);
    return static_cast<uint16_t>(h ^ (h >> 16) ^ (h >> 32) ^ (h >> 48));
}

/// The row whose key fills parameters [first, first + count) of the write.
/// Positions that do not fit fall back to kOrdered, which is always correct.
constexpr WriteAffinity rowAt(uint16_t table, size_t first, size_t count) noexcept {
    if (count == 0 || count >= WriteAffinity::kFreeCount || first > 0xFF) return kOrdered;
    return {table, static_cast<uint8_t>(first), static_cast<uint8_t>(count)};
}

/// Hash of the row `a` names in `params`: equal rows, equal hashes. Each key
/// component is mixed with its length first, so ("1", "23") and ("12", "3")
/// differ, and NULL differs from the empty text. 0 when `a` is not a row of
/// these parameters: keep the write ordered.
inline uint64_t rowHash(WriteAffinity a, const PgParams& params) noexcept {
    if (!a.row() || size_t{a.first} + a.count > params.params.size()) [[unlikely]]
        return 0;
    uint64_t h = write_affinity::mixLength(write_affinity::kFnvOffset, a.table);
    for (size_t i = a.first; i < size_t{a.first} + a.count; ++i) {
        const io::detail::PgParam& p = params.params[i];
        if (p.isNull()) {
            h = write_affinity::mixLength(h, ~uint64_t{0});
        } else {
            const auto n = static_cast<size_t>(p.length());
            h = write_affinity::mix(write_affinity::mixLength(h, n),
                                    std::string_view(p.data(), n));
        }
    }
    return h != 0 ? h : 1;
}

}  // namespace jcailloux::relais::io::batch::detail

#endif  // JCX_RELAIS_IO_BATCH_WRITE_AFFINITY_H
