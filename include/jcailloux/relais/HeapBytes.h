#ifndef JCX_RELAIS_HEAP_BYTES_H
#define JCX_RELAIS_HEAP_BYTES_H

#include <algorithm>
#include <array>
#include <climits>
#include <cstddef>
#include <iterator>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include <glaze/glaze.hpp>

namespace jcailloux::relais::detail {

// =============================================================================
// heapBytes(v) — the bytes a value asks the allocator for, beyond sizeof(v)
// =============================================================================
//
// Counted recursively, from the value alone, so the same value always gives
// the same count. Each rule states what the standard library allocates; the
// node sizes are libstdc++'s, the reference toolchain (with another standard
// library they are estimates). The tests check every rule against what a copy
// of the value really allocates, so a change in any of them shows up.
//   - a type with a trivial destructor frees nothing, so it owns nothing: 0
//     (arithmetic types, enums, string_view, chrono types, PODs);
//   - std::string: nothing while the characters fit in the object (short
//     string optimization); on the heap, capacity() characters plus the
//     terminating null, which the allocation includes;
//   - std::vector<T>: one array of exactly capacity() elements, plus what each
//     element owns; std::vector<bool> packs bits in whole words, and capacity()
//     counts those bits;
//   - std::array, std::pair, std::tuple, std::optional, std::variant: their
//     elements live inside them; only what those elements own is added;
//   - glz::raw_json: its string;
//   - a struct: the sum over its members, through glaze's reflection (an
//     aggregate, or a glz::meta object listing its data members);
//   - a type with memoryUsage(), which reports sizeof plus its heap (entities,
//     list pages): memoryUsage() - sizeof;
//   - std::map, std::set (and multi): one allocated node per element;
//   - std::unordered_map, std::unordered_set (and multi): one allocated node per
//     element, plus the bucket array.
// Any other type does not compile: a member is never silently counted as 0.

template<typename T>
size_t heapBytes(const T& v);

namespace heap_bytes {

template<typename T, template<typename...> class Tmpl>
inline constexpr bool is_spec = false;
template<template<typename...> class Tmpl, typename... Args>
inline constexpr bool is_spec<Tmpl<Args...>, Tmpl> = true;

template<typename T> inline constexpr bool is_std_array = false;
template<typename T, size_t N> inline constexpr bool is_std_array<std::array<T, N>> = true;

template<typename T>
inline constexpr bool is_ordered_node_container =
    is_spec<T, std::map> || is_spec<T, std::multimap>
    || is_spec<T, std::set> || is_spec<T, std::multiset>;

template<typename T>
inline constexpr bool is_hashed_node_container =
    is_spec<T, std::unordered_map> || is_spec<T, std::unordered_multimap>
    || is_spec<T, std::unordered_set> || is_spec<T, std::unordered_multiset>;

template<typename T>
inline constexpr bool owns_nothing = std::is_trivially_destructible_v<T>;

constexpr size_t roundUp(size_t n, size_t align) {
    return (n + align - 1) / align * align;
}

/// A red-black tree node (std::_Rb_tree_node): its base holds the color, an
/// int padded to a word, and the parent, left and right links, four words in
/// all; the value follows at its own alignment; the node is rounded to the
/// stricter of the two alignments.
template<typename V>
inline constexpr size_t kTreeNodeBytes = roundUp(
    roundUp(4 * sizeof(void*), alignof(V)) + sizeof(V),
    std::max(alignof(void*), alignof(V)));

/// Whether a hash node keeps its key's hash: libstdc++ does when the hash is
/// not marked fast (strings) or may throw (std::__cache_default). Other
/// libraries: assumed kept, the larger node.
template<typename Key, typename Hash>
inline constexpr bool kCachesHash =
#if defined(__GLIBCXX__)
    std::__cache_default<Key, Hash>::value;
#else
    true;
#endif

/// A hash node (std::__detail::_Hash_node): the link to the next node, the
/// value at its own alignment, then the hash as a size_t when it is kept; the
/// node is rounded to the stricter alignment.
template<typename V, bool CachedHash>
inline constexpr size_t kHashNodeBytes = roundUp(
    CachedHash
        ? roundUp(roundUp(sizeof(void*), alignof(V)) + sizeof(V), alignof(size_t))
              + sizeof(size_t)
        : roundUp(sizeof(void*), alignof(V)) + sizeof(V),
    std::max(alignof(void*), alignof(V)));

template<typename Range>
size_t elementsHeapBytes(const Range& r) {
    using E = std::remove_cvref_t<decltype(*std::begin(r))>;
    if constexpr (owns_nothing<E>) {
        return 0;
    } else {
        size_t n = 0;
        for (const auto& e : r) n += heapBytes(e);
        return n;
    }
}

template<typename T>
inline constexpr bool members_are_fields = [] {
    constexpr auto& values = glz::reflect<T>::values;
    return []<size_t... I>(std::index_sequence<I...>) {
        return (std::is_member_object_pointer_v<
                    std::remove_cvref_t<decltype(glz::get<I>(values))>> && ...);
    }(std::make_index_sequence<glz::reflect<T>::size>{});
}();

template<typename>
inline constexpr bool unsupported = false;

}  // namespace heap_bytes

template<typename T>
size_t heapBytes(const T& v) {
    using namespace heap_bytes;
    using U = std::remove_cvref_t<T>;

    if constexpr (owns_nothing<U>) {
        return 0;
    } else if constexpr (is_spec<U, std::basic_string>) {
        // The characters are on the heap when they are not inside the object;
        // the allocation holds capacity() characters and the terminating null.
        const auto* p = reinterpret_cast<const char*>(v.data());
        const auto* obj = reinterpret_cast<const char*>(&v);
        bool on_heap = p < obj || p >= obj + sizeof(U);
        return on_heap ? (v.capacity() + 1) * sizeof(typename U::value_type) : 0;
    } else if constexpr (is_spec<U, glz::basic_raw_json>) {
        return heapBytes(v.str);
    } else if constexpr (is_spec<U, std::vector>) {
        if constexpr (std::is_same_v<typename U::value_type, bool>) {
            return v.capacity() / CHAR_BIT;  // capacity() bits, in whole words
        } else {
            return v.capacity() * sizeof(typename U::value_type) + elementsHeapBytes(v);
        }
    } else if constexpr (is_std_array<U>) {
        return elementsHeapBytes(v);
    } else if constexpr (is_spec<U, std::optional>) {
        return v ? heapBytes(*v) : 0;
    } else if constexpr (is_spec<U, std::variant>) {
        if (v.valueless_by_exception()) return 0;
        return std::visit([](const auto& alt) { return heapBytes(alt); }, v);
    } else if constexpr (is_spec<U, std::pair>) {
        return heapBytes(v.first) + heapBytes(v.second);
    } else if constexpr (is_spec<U, std::tuple>) {
        return std::apply([](const auto&... e) { return (size_t{0} + ... + heapBytes(e)); }, v);
    } else if constexpr (is_ordered_node_container<U>) {
        return v.size() * kTreeNodeBytes<typename U::value_type> + elementsHeapBytes(v);
    } else if constexpr (is_hashed_node_container<U>) {
        constexpr size_t node = kHashNodeBytes<typename U::value_type,
            kCachesHash<typename U::key_type, typename U::hasher>>;
        // The buckets are an array of bucket_count() pointers; a container with
        // a single bucket keeps it inside itself and allocates no array.
        size_t buckets = v.bucket_count() > 1 ? v.bucket_count() * sizeof(void*) : 0;
        return v.size() * node + buckets + elementsHeapBytes(v);
    } else if constexpr (requires { { v.memoryUsage() } -> std::convertible_to<size_t>; }) {
        return v.memoryUsage() - sizeof(U);
    } else if constexpr (glz::glaze_object_t<U>) {
        static_assert(members_are_fields<U>,
            "heapBytes: a glz::meta object must list data members only");
        constexpr auto& values = glz::reflect<U>::values;
        return [&]<size_t... I>(std::index_sequence<I...>) {
            return (size_t{0} + ... + heapBytes(v.*glz::get<I>(values)));
        }(std::make_index_sequence<glz::reflect<U>::size>{});
    } else if constexpr (glz::reflectable<U>) {
        auto members = glz::to_tie(v);
        return [&]<size_t... I>(std::index_sequence<I...>) {
            return (size_t{0} + ... + heapBytes(glz::get<I>(members)));
        }(std::make_index_sequence<glz::tuple_size_v<decltype(members)>>{});
    } else {
        static_assert(unsupported<U>,
            "heapBytes: no rule counts the heap this type owns");
        return 0;
    }
}

}  // namespace jcailloux::relais::detail

#endif  // JCX_RELAIS_HEAP_BYTES_H
