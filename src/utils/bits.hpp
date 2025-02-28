#pragma once

#include <cstddef>
#include <cstdint>

/**
 * @brief Get the closest power of two that is greater than or equal to x.
 *
 * @param x The number to get the closest power of two for.
 * @return The closest power of two that is greater than or equal to x.
 *
 * @example
 * @code
 * upperpower2(3); // => 4
 * upperpower2(5); // => 8
 * @endcode
 */
constexpr auto upperpower2(uint64_t x) -> uint64_t {
  x--;
  x |= x >> 1;
  x |= x >> 2;
  x |= x >> 4;
  x |= x >> 8;
  x |= x >> 16;
  x |= x >> 32;
  x++;
  return x;
}

/**
 * @brief Generate a bitmask with the n least significant bits set to 1.
 *
 * Warning: `n` must not be greater than 32.
 *
 * @param n A number between 0 and 32 (inclusive).
 * @return A bitmask with the n least significant bits set to 1.
 */
consteval auto LOWER_BITS_MASK_32(const size_t n) -> uint32_t {
  if (n == 0)
    return 0;
  // Avoid overflow
  if (n == 32)
    return ~static_cast<uint32_t>(0);
  return (static_cast<uint32_t>(1) << n) - 1;
}
/**
 * @brief Generate a bitmask with the n least significant bits set to 1.
 *
 * Warning: `n` must not be greater than 64.
 *
 * @param n A number between 0 and 64 (inclusive).
 * @return A bitmask with the n least significant bits set to 1.
 */
consteval auto LOWER_BITS_MASK_64(const size_t n) -> uint64_t {
  if (n == 0)
    return 0;
  // Avoid overflow
  if (n == 64)
    return ~static_cast<uint64_t>(0);
  return (static_cast<uint64_t>(1) << n) - 1;
}
