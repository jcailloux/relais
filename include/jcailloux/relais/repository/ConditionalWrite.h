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

/// How many rows a claim must take.
enum class ClaimMode : uint8_t {
    Exact,  ///< n rows or none: with fewer candidates, nothing is written
    UpTo,   ///< as many as available, at most n
};

/// What a claim does with a candidate row locked by a concurrent writer.
enum class Lock : uint8_t {
    SkipLocked,  ///< pass over it and take the next candidate
    Wait,        ///< wait for the writer, then re-check the row
};

struct WhereOptions {
    Returns returns = Returns::Count;
};

struct ClaimOptions {
    ClaimMode mode = ClaimMode::Exact;
    Lock lock = Lock::SkipLocked;
    Returns returns = Returns::After;
};

/// One changed row: the version the write locked, and the committed one.
template<typename E>
struct Change {
    E before;
    E after;
};

/// One row of a conditional write returning `R` (After or Changes).
template<typename E, Returns R>
using RowFor = std::conditional_t<R == Returns::Changes, Change<E>, E>;

/// Rows of a conditional write returning `R`: a count, or one entry per row.
template<typename E, Returns R>
using RowsFor = std::conditional_t<R == Returns::Count, size_t, std::vector<RowFor<E, R>>>;

/// Result of a conditional write returning `R`. nullopt is a DB error; a zero
/// count or an empty vector means no row matched.
template<typename E, Returns R>
using ResultFor = std::optional<RowsFor<E, R>>;

}  // namespace jcailloux::relais

#endif  // JCX_RELAIS_REPOSITORY_CONDITIONAL_WRITE_H
