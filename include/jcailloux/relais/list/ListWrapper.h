#ifndef JCX_RELAIS_LIST_WRAPPER_H
#define JCX_RELAIS_LIST_WRAPPER_H

#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <glaze/glaze.hpp>

#include "jcailloux/relais/io/pg/PgResult.h"
#include "jcailloux/relais/entity/Format.h"

namespace jcailloux::relais::list {

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
    static constexpr bool read_only = true;

    std::vector<Item> items;
    std::string next_cursor;

    // =========================================================================
    // Accessors
    // =========================================================================

    [[nodiscard]] size_t size() const noexcept { return items.size(); }
    [[nodiscard]] bool empty() const noexcept { return items.empty(); }
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
        list.next_cursor = std::string(cursor);
        return list;
    }
};

}  // namespace jcailloux::relais::list

// =============================================================================
// Glaze metadata for ListWrapper<Item>
// =============================================================================

template<typename Item>
struct glz::meta<jcailloux::relais::list::ListWrapper<Item>> {
    using T = jcailloux::relais::list::ListWrapper<Item>;
    static constexpr auto value = glz::object(
        "items", &T::items,
        "next_cursor", &T::next_cursor
    );
};

#endif  // JCX_RELAIS_LIST_WRAPPER_H
