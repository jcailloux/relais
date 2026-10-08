#ifndef JCX_RELAIS_REPOSITORY_KEYDEDUP_H
#define JCX_RELAIS_REPOSITORY_KEYDEDUP_H

#include <cstddef>
#include <span>
#include <vector>

namespace jcailloux::relais::detail {

/// Stable dedup by equality only — first-seen order, O(N²) linear scan (N small,
/// the dominant batch size). Needs no operator< , so it works on composite/
/// partition Keys that only model equality.
/// Used at the public batch entry to collapse duplicate Keys before erase/find.
template<typename K>
[[nodiscard]] std::vector<K> dedupStable(std::span<const K> keys) {
    std::vector<K> out;
    out.reserve(keys.size());
    for (const auto& k : keys) {
        bool seen = false;
        for (const auto& u : out) {
            if (u == k) { seen = true; break; }
        }
        if (!seen) out.push_back(k);
    }
    return out;
}

/// Distinct keys of a batch plus the position → distinct-index map.
template<typename K>
struct DedupSlots {
    std::vector<K> unique;      ///< distinct keys, first-seen order
    std::vector<size_t> slot;   ///< slot[i] = index in unique of keys[i]
};

/// Same dedup as dedupStable, keeping where each input position landed, so a
/// batch read can fetch each distinct key once and fan the result back out to
/// every position (duplicates share one entry).
template<typename K>
[[nodiscard]] DedupSlots<K> dedupWithSlots(std::span<const K> keys) {
    const size_t n = keys.size();
    DedupSlots<K> out;
    out.unique.reserve(n);
    out.slot.resize(n);
    for (size_t i = 0; i < n; ++i) {
        size_t u = out.unique.size();
        for (size_t k = 0; k < out.unique.size(); ++k) {
            if (out.unique[k] == keys[i]) { u = k; break; }
        }
        if (u == out.unique.size()) out.unique.push_back(keys[i]);
        out.slot[i] = u;
    }
    return out;
}

}  // namespace jcailloux::relais::detail

#endif  // JCX_RELAIS_REPOSITORY_KEYDEDUP_H
