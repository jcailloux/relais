#ifndef JCX_RELAIS_WRAPPER_LIST_WRAPPER_H
#define JCX_RELAIS_WRAPPER_LIST_WRAPPER_H

#include <cstdint>
#include <memory>
#include <mutex>
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
// Provides thread-safe lazy-cached serialization, factory methods, and
// accessors. Uses std::call_once for lock-free fast path.
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
    int64_t total_count = 0;
    std::string next_cursor;

    ListWrapper() = default;

    // std::once_flag is non-copyable/non-movable — caches are transient
    // and will be lazily recomputed after copy/move.
    ListWrapper(const ListWrapper& o)
        : items(o.items), total_count(o.total_count), next_cursor(o.next_cursor) {}
    ListWrapper(ListWrapper&& o) noexcept
        : items(std::move(o.items)), total_count(o.total_count),
          next_cursor(std::move(o.next_cursor)) {}
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
            items = std::move(o.items);
            total_count = o.total_count;
            next_cursor = std::move(o.next_cursor);
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
    // Binary serialization (Glaze BEVE)
    // =========================================================================

    [[nodiscard]] std::shared_ptr<const std::vector<uint8_t>> binary() const {
        std::call_once(beve_flag_, [this] {
            auto buf = std::make_shared<std::vector<uint8_t>>();
            if (glz::write_beve(*this, *buf))
                buf->clear();
            beve_cache_ = std::move(buf);
        });
        return beve_cache_;
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
    // JSON serialization (Glaze JSON)
    // =========================================================================

    [[nodiscard]] std::shared_ptr<const std::string> json() const {
        std::call_once(json_flag_, [this] {
            auto json = std::make_shared<std::string>();
            json->reserve(items.size() * 200 + 64);
            if (glz::write_json(*this, *json))
                json_cache_ = std::make_shared<std::string>(R"({"items":[]})");
            else
                json_cache_ = std::move(json);
        });
        return json_cache_;
    }

    static std::optional<ListWrapper> fromJson(std::string_view json) {
        if (json.empty()) return std::nullopt;
        ListWrapper list;
        if (glz::read_json(list, json)) return std::nullopt;
        return list;
    }

    // =========================================================================
    // Cache management
    // =========================================================================

    /// Release serialization caches. After this call, binary()/json()
    /// return nullptr. Callers who previously obtained shared_ptrs from
    /// binary()/json() retain valid data through reference counting.
    void releaseCaches() const noexcept {
        beve_cache_.reset();
        json_cache_.reset();
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

private:
    mutable std::once_flag beve_flag_;
    mutable std::once_flag json_flag_;
    mutable std::shared_ptr<const std::vector<uint8_t>> beve_cache_;
    mutable std::shared_ptr<const std::string> json_cache_;
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
