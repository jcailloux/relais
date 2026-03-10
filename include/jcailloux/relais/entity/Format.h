#ifndef JCX_RELAIS_ENTITY_FORMAT_H
#define JCX_RELAIS_ENTITY_FORMAT_H

namespace jcailloux::relais {

// =============================================================================
// Format tags - explicit type tags for wrapper format detection
//
// Declared via `using Format = StructFormat;` in entity wrappers.
// =============================================================================

/// Tag type for plain C++ struct wrappers (BEVE/JSON via Glaze)
struct StructFormat {};

}  // namespace jcailloux::relais

#endif  // JCX_RELAIS_ENTITY_FORMAT_H
