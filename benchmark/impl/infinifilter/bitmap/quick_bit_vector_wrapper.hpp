#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "bitmap.hpp"
#include "quick_bit_vector.hpp"

namespace bitmap {

class QuickBitVectorWrapper : public Bitmap {
  std::vector<uint32_t> bs_;

public:
  QuickBitVectorWrapper(size_t bits_per_entry, size_t num_entries) {
    bs_ = QuickBitVector::make_bit_vector(num_entries, bits_per_entry);
  }

  ~QuickBitVectorWrapper() override = default;
  QuickBitVectorWrapper(const QuickBitVectorWrapper &) = default;
  auto operator=(const QuickBitVectorWrapper &) -> QuickBitVectorWrapper & = default;
  QuickBitVectorWrapper(QuickBitVectorWrapper &&) = default;
  auto operator=(QuickBitVectorWrapper &&) -> QuickBitVectorWrapper & = default;

  [[nodiscard]] auto size() const -> size_t override { return bs_.size() * sizeof(uint32_t) * 8; }

  void set(size_t bit_index, bool value) override {
    if (value)
      QuickBitVector::set(bs_, bit_index);
    else
      QuickBitVector::clear(bs_, bit_index);
  }

  void set_from_to(size_t from, size_t to, uint32_t value) override {
    QuickBitVector::put_from_to(bs_, value, from, to - 1);
  }

  [[nodiscard]] auto get(size_t bit_index) const -> bool override {
    return QuickBitVector::get(bs_, bit_index);
  }

  [[nodiscard]] auto get_from_to(size_t from, size_t to) const -> uint32_t override {
    return QuickBitVector::get_from_to(bs_, from, to - 1);
  }
};

} // namespace bitmap
