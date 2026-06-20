#ifndef JCX_RELAIS_DETAIL_CHUNK_H
#define JCX_RELAIS_DETAIL_CHUNK_H

#include <algorithm>
#include <cstddef>
#include <span>
#include <vector>

namespace jcailloux::relais::detail {

/// Partition `items` into consecutive subspans of at most `chunk` elements.
/// The returned subspans cover `items` exactly: their concatenation equals the
/// input, with no loss and no overlap. Used to bound runtime argv lists to a
/// command-size ceiling (L2 `UNLINK` at K_redis, L3 batch at K_pg) before each
/// awaited round-trip. Pure (no I/O) so the split is unit-testable in isolation.
///
/// `chunk == 0` yields an empty result (degenerate ceiling — no progress
/// possible); callers pass a library constant > 0.
template<typename T>
[[nodiscard]] std::vector<std::span<const T>> chunkSpan(
    std::span<const T> items, std::size_t chunk)
{
    std::vector<std::span<const T>> out;
    if (chunk == 0 || items.empty()) {
        return out;
    }
    out.reserve((items.size() + chunk - 1) / chunk);
    for (std::size_t off = 0; off < items.size(); off += chunk) {
        out.push_back(items.subspan(off, std::min(chunk, items.size() - off)));
    }
    return out;
}

}  // namespace jcailloux::relais::detail

#endif  // JCX_RELAIS_DETAIL_CHUNK_H
