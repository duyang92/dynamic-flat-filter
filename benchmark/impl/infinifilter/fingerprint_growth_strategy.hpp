#pragma once

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <numbers>
#include <stdexcept>

namespace infinifilter::FingerprintGrowthStrategy {

enum class FalsePositiveRateExpansion : std::uint8_t { UNIFORM, POLYNOMIAL, TRIANGULAR, GEOMETRIC };

inline auto get_new_fingerprint_size(size_t original_fingerprint_size, size_t num_expansions,
                                     FalsePositiveRateExpansion fpr_style) -> size_t {
  double original_fpr = std::pow(2, -(int)original_fingerprint_size);
  double new_filter_fpr = 0.0;

  switch (fpr_style) {
  case FalsePositiveRateExpansion::GEOMETRIC: {
    double factor = 1.0 / std::pow(2, num_expansions);
    new_filter_fpr = factor * original_fpr;
    break;
  }
  case FalsePositiveRateExpansion::POLYNOMIAL: {
    double factor = 1.0 / std::pow(num_expansions + 1, 2);
    new_filter_fpr = factor * original_fpr;
    break;
  }
  case FalsePositiveRateExpansion::TRIANGULAR: {
    double factor = 1.0 / ((double)num_expansions * ((double)num_expansions + 1));
    new_filter_fpr = factor * original_fpr;
    break;
  }
  case FalsePositiveRateExpansion::UNIFORM: {
    new_filter_fpr = original_fpr;
    break;
  }
  default: {
    throw std::invalid_argument("Invalid FalsePositiveRateExpansion value");
  }
  }

  double fingerprint_size = -std::ceil(std::log(new_filter_fpr) / std::numbers::ln2);
  return static_cast<size_t>(fingerprint_size);
}

} // namespace infinifilter::FingerprintGrowthStrategy
