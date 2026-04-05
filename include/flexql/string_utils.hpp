#ifndef FLEXQL_STRING_UTILS_HPP
#define FLEXQL_STRING_UTILS_HPP

#include <string>
#include <string_view>

namespace flexql {

std::string ToUpper(std::string_view input);
std::string ToLower(std::string_view input);
std::string Trim(std::string_view input);
bool IEquals(std::string_view a, std::string_view b);

}  // namespace flexql

#endif
