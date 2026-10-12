// Must not compile: an IS_NULL filter on a member that cannot be null.

#include <cstdint>

#include <jcailloux/relais/list/spec/FilterDescriptor.h>

namespace decl = jcailloux::relais::list::spec;

struct Row {
    int64_t author_id = 0;
};

using Bad = decl::Filter<"author_null", &Row::author_id, "author_id", decl::Op::IS_NULL>;
static_assert(sizeof(Bad) > 0);
