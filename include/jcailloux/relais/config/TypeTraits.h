#ifndef JCX_RELAIS_CONFIG_TYPE_TRAITS_H
#define JCX_RELAIS_CONFIG_TYPE_TRAITS_H

#include <tuple>
#include <type_traits>

namespace jcailloux::relais::config {

template<typename T> struct is_tuple : std::false_type {};
template<typename... Ts> struct is_tuple<std::tuple<Ts...>> : std::true_type {};
template<typename T> inline constexpr bool is_tuple_v = is_tuple<T>::value;

}  // namespace jcailloux::relais::config

#endif  // JCX_RELAIS_CONFIG_TYPE_TRAITS_H
