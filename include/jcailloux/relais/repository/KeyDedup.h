#ifndef JCX_RELAIS_REPOSITORY_KEYDEDUP_H
#define JCX_RELAIS_REPOSITORY_KEYDEDUP_H

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <type_traits>
#include <unordered_set>
#include <vector>

#include "jcailloux/relais/cache/ChunkMap.h"   // cache::detail::AutoHash
#include "jcailloux/relais/cache/MultiView.h"

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

/// A set of keys, for membership tests: hashed when the key hashes, scanned
/// otherwise (a composite key may only model equality). Built once, then
/// shared (share()). One that counts bytes reports to the L1 ledger what it
/// holds, for as long as it lives, whoever holds it last: its blocks and the
/// shared object count themselves (LedgerAllocator), what its keys own is
/// counted when it is built and when it is destroyed.
template<typename K, bool CountsBytes = false>
class KeySet {
    static constexpr bool kHashed = std::is_invocable_v<cache::detail::AutoHash<K>, const K&>;
    using Alloc = cache::detail::LedgerAllocatorIf<CountsBytes, K>;
    std::conditional_t<kHashed,
                       std::unordered_set<K, cache::detail::AutoHash<K>, std::equal_to<K>, Alloc>,
                       std::vector<K, Alloc>> keys_;

    void countKeys(int64_t sign) const {
        if constexpr (CountsBytes && !heap_bytes::owns_nothing<K>) {
            for (const auto& key : keys_) cache::detail::L1Ledger::add(sign * cache::detail::keyHeap(key));
        }
    }

public:
    explicit KeySet(std::span<const K> keys) : keys_(keys.begin(), keys.end()) { countKeys(+1); }
    ~KeySet() { countKeys(-1); }
    KeySet(const KeySet&) = delete;
    KeySet& operator=(const KeySet&) = delete;

    /// A set every copy of a predicate shares.
    [[nodiscard]] static std::shared_ptr<const KeySet> share(std::span<const K> keys) {
        return std::allocate_shared<KeySet>(cache::detail::LedgerAllocatorIf<CountsBytes, KeySet>{}, keys);
    }

    [[nodiscard]] bool contains(const K& key) const {
        if constexpr (kHashed) return keys_.contains(key);
        else return std::ranges::find(keys_, key) != keys_.end();
    }
};

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

/// Build a self-contained MultiView from a batch fetched without L1: `fetched`
/// is aligned on the distinct keys, `slot` maps each request position to one of
/// them. Each present value is moved once into the view; duplicate positions
/// share its pointer. No epoch guard — every pointer targets the view's own
/// storage.
template<typename E>
[[nodiscard]] cache::MultiView<E> ownedMultiView(std::vector<std::optional<E>> fetched,
                                                 std::span<const size_t> slot) {
    size_t present = 0;
    for (const auto& f : fetched) present += f.has_value();

    cache::MultiView<E> view(slot.size());
    view.reserveOwned(present);
    std::vector<const E*> unique(fetched.size(), nullptr);
    for (size_t k = 0; k < fetched.size(); ++k) {
        if (fetched[k]) unique[k] = view.adoptValue(std::move(*fetched[k]));
    }
    for (size_t i = 0; i < slot.size(); ++i) view.pointAt(i, unique[slot[i]]);
    return view;
}

}  // namespace jcailloux::relais::detail

#endif  // JCX_RELAIS_REPOSITORY_KEYDEDUP_H
