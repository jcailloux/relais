#ifndef JCX_RELAIS_WRAPPER_LIST_WRAPPER_H
#define JCX_RELAIS_WRAPPER_LIST_WRAPPER_H

#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <glaze/glaze.hpp>

#include "jcailloux/relais/io/pg/PgResult.h"
#include "jcailloux/relais/wrapper/Format.h"

namespace jcailloux::relais::wrapper {

// =============================================================================
// ListWrapper<Item> — Generic list wrapper for any entity type
//
// Provides on-demand serialization (no caching), factory methods, and
// accessors.
//
// Satisfies: HasBinarySerialization, HasJsonSerialization, HasFormat
// =============================================================================

template<typename Item>
class ListWrapper {
public:
    using Format = jcailloux::relais::StructFormat;
    using ItemType = Item;
    using MemoryHook = void(*)(void* ctx, int64_t delta);
    static constexpr bool read_only = true;

    std::vector<Item> items;
    int64_t total_count = 0;
    std::string next_cursor;

    ListWrapper() = default;

    ~ListWrapper() {
        if (memory_hook_) {
            memory_hook_(memory_hook_ctx_,
                -static_cast<int64_t>(memoryUsage() + cache_overhead_));
        }
    }

    // Copy ctor/assignment do NOT transfer the hook (copies are independent).
    ListWrapper(const ListWrapper& o)
        : items(o.items), total_count(o.total_count), next_cursor(o.next_cursor) {}

    ListWrapper(ListWrapper&& o) noexcept
        : items(std::move(o.items)), total_count(o.total_count),
          next_cursor(std::move(o.next_cursor)),
          memory_hook_(o.memory_hook_), memory_hook_ctx_(o.memory_hook_ctx_),
          cache_overhead_(o.cache_overhead_)
    {
        o.memory_hook_ = nullptr;
        o.memory_hook_ctx_ = nullptr;
        o.cache_overhead_ = 0;
    }

    ListWrapper& operator=(const ListWrapper& o) {
        if (this != &o) {
            items = o.items;
            total_count = o.total_count;
            next_cursor = o.next_cursor;
        }
        return *this;
    }

    ListWrapper& operator=(ListWrapper&& o) noexcept {
        if (this != &o) {
            if (memory_hook_) {
                memory_hook_(memory_hook_ctx_,
                    -static_cast<int64_t>(memoryUsage() + cache_overhead_));
            }
            items = std::move(o.items);
            total_count = o.total_count;
            next_cursor = std::move(o.next_cursor);
            memory_hook_ = o.memory_hook_;
            memory_hook_ctx_ = o.memory_hook_ctx_;
            cache_overhead_ = o.cache_overhead_;
            o.memory_hook_ = nullptr;
            o.memory_hook_ctx_ = nullptr;
            o.cache_overhead_ = 0;
        }
        return *this;
    }

    // =========================================================================
    // Accessors
    // =========================================================================

    [[nodiscard]] size_t size() const noexcept { return items.size(); }
    [[nodiscard]] bool empty() const noexcept { return items.empty(); }
    [[nodiscard]] int64_t count() const noexcept { return total_count; }
    [[nodiscard]] std::string_view cursor() const noexcept { return next_cursor; }

    [[nodiscard]] const Item* front() const noexcept {
        return items.empty() ? nullptr : &items.front();
    }

    [[nodiscard]] const Item* back() const noexcept {
        return items.empty() ? nullptr : &items.back();
    }

    // =========================================================================
    // Binary serialization (Glaze BEVE, on-demand)
    // =========================================================================

    [[nodiscard]] std::vector<uint8_t> binary() const {
        std::vector<uint8_t> buf;
        if (glz::write_beve(*this, buf)) buf.clear();
        return buf;
    }

    static std::optional<ListWrapper> fromBinary(std::span<const uint8_t> data) {
        if (data.empty()) return std::nullopt;
        ListWrapper list;
        if (glz::read_beve(list, std::string_view{
            reinterpret_cast<const char*>(data.data()), data.size()}))
            return std::nullopt;
        return list;
    }

    // =========================================================================
    // JSON serialization (Glaze JSON, on-demand)
    // =========================================================================

    [[nodiscard]] std::string json() const {
        std::string buf;
        buf.reserve(items.size() * 200 + 64);
        if (glz::write_json(*this, buf)) buf = R"({"items":[]})";
        return buf;
    }

    static std::optional<ListWrapper> fromJson(std::string_view json) {
        if (json.empty()) return std::nullopt;
        ListWrapper list;
        if (glz::read_json(list, json)) return std::nullopt;
        return list;
    }

    // =========================================================================
    // Memory tracking
    // =========================================================================

    /// Approximate heap memory used by this list (items + cursors).
    [[nodiscard]] size_t memoryUsage() const {
        size_t size = sizeof(*this);
        size += items.capacity() * sizeof(Item);
        for (const auto& item : items) {
            if constexpr (requires(const Item& i) { i.memoryUsage(); }) {
                size += item.memoryUsage() - sizeof(Item);
            }
        }
        size += next_cursor.capacity();
        return size;
    }

    // =========================================================================
    // Factory methods
    // =========================================================================

    static ListWrapper fromRows(const io::PgResult& result) {
        ListWrapper list;
        list.items.reserve(result.rows());
        for (int i = 0; i < result.rows(); ++i) {
            if (auto item = Item::fromRow(result[i]))
                list.items.push_back(std::move(*item));
        }
        list.total_count = static_cast<int64_t>(list.items.size());
        return list;
    }

    template<typename ItemPtr>
    static ListWrapper fromItems(
            const std::vector<ItemPtr>& ptrs,
            std::string_view cursor = "") {
        ListWrapper list;
        list.items.reserve(ptrs.size());
        for (const auto& p : ptrs) {
            if (p) list.items.push_back(*p);
        }
        list.total_count = static_cast<int64_t>(list.items.size());
        list.next_cursor = std::string(cursor);
        return list;
    }

    mutable MemoryHook memory_hook_ = nullptr;
    mutable void* memory_hook_ctx_ = nullptr;
    size_t cache_overhead_{0};
};

}  // namespace jcailloux::relais::wrapper

// =============================================================================
// Glaze metadata for ListWrapper<Item>
// =============================================================================

template<typename Item>
struct glz::meta<jcailloux::relais::wrapper::ListWrapper<Item>> {
    using T = jcailloux::relais::wrapper::ListWrapper<Item>;
    static constexpr auto value = glz::object(
        "items", &T::items,
        "total_count", &T::total_count,
        "next_cursor", &T::next_cursor
    );
};

#endif  // JCX_RELAIS_WRAPPER_LIST_WRAPPER_H
