#pragma once

#include <cstddef>
#include <string>
#include <type_traits>

template <typename T>
concept Integral = std::is_integral_v<T>;

/**
 * @brief Convert an integral to binary string.
 *
 * @param n The integral to convert.
 * @return A string starting with "0b" representing the binary form of the input.
 */
template <Integral T> [[nodiscard]] auto bin(T n) -> std::string {
  std::string res = "0b";
  for (size_t i = sizeof(T) - 1; i >= 0; i--)
    res += (n & (1 << i)) ? '1' : '0';
  return res;
}
