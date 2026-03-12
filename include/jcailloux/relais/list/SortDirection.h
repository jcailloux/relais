#ifndef JCX_RELAIS_LIST_SORTDIRECTION_H
#define JCX_RELAIS_LIST_SORTDIRECTION_H

#include <cstdint>

namespace jcailloux::relais::list {

enum class SortDirection : uint8_t {
    Asc,
    Desc
};

}  // namespace jcailloux::relais::list

#endif  // JCX_RELAIS_LIST_SORTDIRECTION_H