#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>

namespace bamboofilter {

inline auto get_current_time_in_seconds() -> double {
  const auto now = std::chrono::high_resolution_clock::now();
  const auto duration = now.time_since_epoch();
  return std::chrono::duration_cast<std::chrono::duration<double>>(duration).count();
}

constexpr size_t BUCKETS_PER_SEG_POWER = 12;
constexpr size_t BITS_PER_TAG = 16;

constexpr auto generate_ll_is_mask(const uint32_t bits_per_tag) -> uint64_t {
  uint64_t mask = 0;
  for (uint32_t i = 0; i < 64; i += bits_per_tag)
    mask |= (1ULL << i);
  return mask;
}

constexpr auto ll_isl(uint64_t x, uint32_t bit) -> uint64_t {
  return x & (~(((x & (generate_ll_is_mask(BITS_PER_TAG) << bit)) >> bit) *
                ((1ULL << BITS_PER_TAG) - 1)));
}
constexpr auto ll_isn(uint64_t x, uint32_t bit) -> uint64_t {
  return x & ((((x & (generate_ll_is_mask(BITS_PER_TAG) << bit)) >> bit) *
               ((1ULL << BITS_PER_TAG) - 1)));
}

} // namespace bamboofilter
