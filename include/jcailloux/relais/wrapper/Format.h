#ifndef JCX_DROGON_WRAPPER_FORMAT_H
#define JCX_DROGON_WRAPPER_FORMAT_H

namespace jcailloux::drogon::smartrepo {

// =============================================================================
// Format tags - explicit type tags for wrapper format detection
//
// Declared via `using Format = StructFormat;` in entity wrappers.
// =============================================================================

/// Tag type for plain C++ struct wrappers (BEVE/JSON via Glaze)
struct StructFormat {};

}  // namespace jcailloux::drogon::smartrepo

#endif  // JCX_DROGON_WRAPPER_FORMAT_H