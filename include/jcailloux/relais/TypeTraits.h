#ifndef JCX_RELAIS_TYPE_TRAITS_H
#define JCX_RELAIS_TYPE_TRAITS_H

#include <optional>
#include <tuple>
#include <type_traits>
#include <vector>

namespace jcailloux::relais {

template<typename T> struct is_tuple : std::false_type {};
template<typename... Ts> struct is_tuple<std::tuple<Ts...>> : std::true_type {};
template<typename T> inline constexpr bool is_tuple_v = is_tuple<T>::value;

template<typename T> struct is_optional : std::false_type {};
template<typename U> struct is_optional<std::optional<U>> : std::true_type {};
template<typename T> inline constexpr bool is_optional_v = is_optional<T>::value;

template<typename T> struct is_std_vector : std::false_type {};
template<typename U, typename A> struct is_std_vector<std::vector<U, A>> : std::true_type {};
template<typename T> inline constexpr bool is_std_vector_v = is_std_vector<T>::value;

}  // namespace jcailloux::relais

#endif  // JCX_RELAIS_TYPE_TRAITS_H
