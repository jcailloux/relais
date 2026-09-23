#ifndef JCX_RELAIS_REPOSITORY_CONDITIONAL_WRITE_H
#define JCX_RELAIS_REPOSITORY_CONDITIONAL_WRITE_H

#include <cstddef>
#include <cstdint>
#include <optional>
#include <type_traits>
#include <vector>

namespace jcailloux::relais {

// =============================================================================
// Options and results of the predicate-driven conditional writes
//
// Options are NTTPs: the SQL shape is fixed per instantiation. Every field has
// a default, so callers name only what they change with designated
// initializers: Repo::patchWhere<{.returns = Returns::Changes}>(...).
// =============================================================================

/// What a conditional write hands back for the rows it changed.
enum class Returns : uint8_t {
    Count,    ///< the number of rows changed
    After,    ///< each changed row as committed
    Changes,  ///< each changed row before and after the write
};

struct WhereOptions {
    Returns returns = Returns::Count;
};

/// One changed row: the version the write locked, and the committed one.
template<typename E>
struct Change {
    E before;
    E after;
};

/// Result of a conditional write returning `R`. nullopt is a DB error; a zero
/// count or an empty vector means no row matched.
template<typename E, Returns R>
using ResultFor = std::optional<std::conditional_t<
    R == Returns::Count, size_t,
    std::conditional_t<R == Returns::After, std::vector<E>, std::vector<Change<E>>>>>;

}  // namespace jcailloux::relais

#endif  // JCX_RELAIS_REPOSITORY_CONDITIONAL_WRITE_H
