/*
 * Copyright © 1999 CERN - European Organization for Nuclear Research.
 * Permission to use, copy, modify, distribute and sell this software and its documentation for any
 * purpose is hereby granted without fee, provided that the above copyright notice appear in all
 * copies and that both that copyright notice and this permission notice appear in supporting
 * documentation. CERN makes no representations about the suitability of this software for any
 * purpose. It is provided "as is" without expressed or implied warranty.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace bitmap {

/**
 * Adapted version of Java version of QuickBitVector from CERN Colt library.
 * The original documentation is as follows:
 *
 * Implements quick non polymorphic non bounds checking low level bitvector operations.
 * Includes some operations that interpret sub-bitstrings as long integers.
 *
 * WARNING: Methods of this class do not check preconditions.
 * Provided with invalid parameters these method may return (or set) invalid values without throwing
 any exception.
 * You should only use this class when performance is critical and you are absolutely sure that
 indexes are within bounds.
 *
 * A bitvector is modelled as an uint32 array, i.e. uint32_t[] bits holds bits of a bitvector.
 * Each value holds 32 bits.
 * The i-th bit is stored in bits[i/32] at bit position i % 32 (where bit position 0 refers to the
 least significant bit and 31 refers to the most significant bit).
 *
 * @author wolfgang.hoschek@cern.ch
 * @version 1.0, 09/24/99
 */
class QuickBitVector {
protected:
  static constexpr size_t ADDRESS_BITS_PER_UNIT = 5; // 32=2^5
  static constexpr size_t BITS_PER_UNIT = 32;        // = 1 << ADDRESS_BITS_PER_UNIT
  static constexpr size_t BIT_INDEX_MASK = 31;       // = BITS_PER_UNIT - 1;

  static std::vector<uint32_t> pows;

  QuickBitVector() = default;

public:
  ~QuickBitVector() = default;
  QuickBitVector(const QuickBitVector &) = default;
  auto operator=(const QuickBitVector &) -> QuickBitVector & = default;
  QuickBitVector(QuickBitVector &&) = default;
  auto operator=(QuickBitVector &&) -> QuickBitVector & = default;

  [[nodiscard]] static auto bit_mask_with_bits_set_from_to(size_t from, size_t to) -> uint32_t {
    return pows[to - from + 1] << from;
  }

  static void clear(std::vector<uint32_t> &bits, size_t bit_index) {
    bits[bit_index >> ADDRESS_BITS_PER_UNIT] &= ~(1U << (bit_index & BIT_INDEX_MASK));
  }

  [[nodiscard]] static auto get(const std::vector<uint32_t> &bits, size_t bit_index) -> bool {
    // if (bit_index >= bits.size() * BITS_PER_UNIT) {
    //   std::println("bit_index {} is out of range [0, {})", std::to_string(bit_index),
    //                std::to_string(bits.size() * BITS_PER_UNIT));
    // }
    return (bits[bit_index >> ADDRESS_BITS_PER_UNIT] & (1U << (bit_index & BIT_INDEX_MASK))) != 0;
  }

  [[nodiscard]] static auto get_from_to(const std::vector<uint32_t> &bits, size_t from,
                                        size_t to) -> uint32_t {
    if (from > to)
      return 0U;

    const size_t from_index = from >> ADDRESS_BITS_PER_UNIT;
    const size_t to_index = to >> ADDRESS_BITS_PER_UNIT;
    const size_t from_offset = from & BIT_INDEX_MASK;
    const size_t to_offset = to & BIT_INDEX_MASK;

    uint32_t mask;
    if (from_index == to_index) {
      mask = bit_mask_with_bits_set_from_to(from_offset, to_offset);
      return (bits[from_index] & mask) >> from_offset;
    }

    mask = bit_mask_with_bits_set_from_to(from_offset, BIT_INDEX_MASK);
    uint32_t x1 = (bits[from_index] & mask) >> from_offset;

    mask = bit_mask_with_bits_set_from_to(0, to_offset);
    uint32_t x2 = (bits[to_index] & mask) << (BITS_PER_UNIT - from_offset);

    return x1 | x2;
  }

  [[nodiscard]] static auto make_bit_vector(size_t size,
                                            size_t bits_per_element) -> std::vector<uint32_t> {
    size_t n_bits = size * bits_per_element;
    size_t unit_index = (n_bits - 1) >> ADDRESS_BITS_PER_UNIT;
    return std::vector<uint32_t>(unit_index + 1, 0);
  }

  static void put(std::vector<uint32_t> &bits, size_t bit_index, bool value) {
    if (value)
      set(bits, bit_index);
    else
      clear(bits, bit_index);
  }

  static void put_from_to(std::vector<uint32_t> &bits, uint32_t value, size_t from, size_t to) {
    if (from > to)
      return;

    const size_t from_index = from >> ADDRESS_BITS_PER_UNIT;
    const size_t to_index = to >> ADDRESS_BITS_PER_UNIT;
    const size_t from_offset = from & BIT_INDEX_MASK;
    const size_t to_offset = to & BIT_INDEX_MASK;

    uint32_t mask;
    mask = bit_mask_with_bits_set_from_to(to - from + 1, BIT_INDEX_MASK);
    uint32_t clean_value = value & (~mask);

    uint32_t shifted_value;

    if (from_index == to_index) {
      shifted_value = clean_value << from_offset;
      mask = bit_mask_with_bits_set_from_to(from_offset, to_offset);
      bits[from_index] = (bits[from_index] & (~mask)) | shifted_value;
      return;
    }

    shifted_value = clean_value << from_offset;
    mask = bit_mask_with_bits_set_from_to(from_offset, BIT_INDEX_MASK);
    bits[from_index] = (bits[from_index] & (~mask)) | shifted_value;

    shifted_value = clean_value >> (BITS_PER_UNIT - from_offset);
    mask = bit_mask_with_bits_set_from_to(0, to_offset);
    bits[to_index] = (bits[to_index] & (~mask)) | shifted_value;
  }

  static void set(std::vector<uint32_t> &bits, size_t bit_index) {
    bits[bit_index >> ADDRESS_BITS_PER_UNIT] |= 1U << (bit_index & BIT_INDEX_MASK);
  }

  [[nodiscard]] static auto unit(size_t bit_index) -> size_t {
    return bit_index >> ADDRESS_BITS_PER_UNIT;
  }

  [[nodiscard]] static auto offset(size_t bit_index) -> size_t {
    return bit_index & BIT_INDEX_MASK;
  }

private:
  static auto precompute_pows() -> std::vector<uint32_t> {
    std::vector<uint32_t> pows(BITS_PER_UNIT + 1, 0);
    uint32_t value = ~0U;
    for (size_t i = BITS_PER_UNIT + 1; --i >= 1;)
      pows[i] = value >> (BITS_PER_UNIT - i);
    pows[0] = 0U;
    return pows;
  }
};

} // namespace bitmap
