#pragma once

#include <cstddef>
#include <cstdint>

namespace bitmap {

class Bitmap {
public:
  virtual ~Bitmap() = default;

  Bitmap(const Bitmap &) = default;
  auto operator=(const Bitmap &) -> Bitmap & = default;
  Bitmap(Bitmap &&) = default;
  auto operator=(Bitmap &&) -> Bitmap & = default;

  [[nodiscard]] virtual auto size() const -> size_t = 0;
  virtual void set(size_t bit_index, bool value) = 0;
  virtual void set_from_to(size_t from, size_t to, uint32_t value) = 0;
  [[nodiscard]] virtual auto get(size_t bit_index) const -> bool = 0;
  [[nodiscard]] virtual auto get_from_to(size_t from, size_t to) const -> uint32_t = 0;

  static auto get_fingerprint_bit(size_t index, uint32_t fingerprint) -> bool {
    uint32_t mask = 1U << index;
    uint32_t and_result = fingerprint & mask;
    return and_result != 0;
  }

protected:
  Bitmap() = default;
};

} // namespace bitmap
