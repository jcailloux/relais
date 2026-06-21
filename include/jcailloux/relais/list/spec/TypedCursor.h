#ifndef JCX_RELAIS_LIST_SPEC_TYPEDCURSOR_H
#define JCX_RELAIS_LIST_SPEC_TYPEDCURSOR_H

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include "jcailloux/relais/list/ListQuery.h"  // list::Cursor (opaque byte token)

namespace jcailloux::relais::list::spec {

// =============================================================================
// TypedCursor<Descriptor> — phantom-typed keyset cursor.
//
// Wraps the opaque byte-level list::Cursor and tags it with its owning
// Descriptor. A cursor minted for one list therefore cannot be handed to
// another list's query()/builder: the two are distinct types, so the confusion
// is a COMPILE error, not a runtime mis-decode against the wrong sort/key shape.
//
// The base64 wire token is unchanged — decode()/encode() pass straight through
// to list::Cursor, and the keyset SQL / cache-key machinery reads raw(). Only
// the construction boundary is typed.
//
// Cross-descriptor confusion is the only type-level contract reachable here.
// Intra-descriptor coherence (cursor vs the request's current sort) stays a
// runtime concern by design: the sort varies per request and the token arrives
// off the wire, so it cannot be a compile-time type.
// =============================================================================
template<typename Descriptor>
class TypedCursor {
public:
    TypedCursor() = default;  // empty = first page

    /// Decode a server-minted base64 token into a descriptor-tagged cursor.
    /// nullopt on a malformed token (trust boundary); an empty token decodes to
    /// an empty (first-page) cursor. This is the sole runtime entry point — the
    /// type tag is conferred here, at the trust boundary.
    [[nodiscard]] static std::optional<TypedCursor> decode(std::string_view token) {
        auto raw = list::Cursor::decode(token);
        if (!raw) return std::nullopt;
        return TypedCursor{std::move(*raw)};
    }

    [[nodiscard]] std::string encode() const { return raw_.encode(); }
    [[nodiscard]] bool empty() const noexcept { return raw_.empty(); }
    [[nodiscard]] std::size_t size() const noexcept { return raw_.size(); }

    /// The opaque byte-level cursor — for the keyset SQL / cache-key machinery.
    [[nodiscard]] const list::Cursor& raw() const noexcept { return raw_; }

    bool operator==(const TypedCursor&) const = default;

private:
    explicit TypedCursor(list::Cursor raw) noexcept : raw_(std::move(raw)) {}

    list::Cursor raw_;
};

}  // namespace jcailloux::relais::list::spec

#endif  // JCX_RELAIS_LIST_SPEC_TYPEDCURSOR_H