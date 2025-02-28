#pragma once

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "fingerprint_growth_strategy.hpp"
#include "quotient_filter.hpp"

namespace infinifilter {

class Chaining : public QuotientFilter {
public:
  enum class SizeExpansion : std::uint8_t { LINEAR, GEOMETRIC };

  void set_fpr_style(FingerprintGrowthStrategy::FalsePositiveRateExpansion val) {
    fpr_style_ = val;
  }

  void set_growth_style(SizeExpansion val) { size_style_ = val; }

  Chaining(size_t power_of_two, size_t bits_per_entry)
      : QuotientFilter(power_of_two, bits_per_entry) {
    max_entries_before_expansion =
        static_cast<size_t>(std::pow(2, power_of_two_size) * expansion_threshold);
  }

  [[nodiscard]] auto get_num_entries(bool include_all_internal_filters) const -> size_t override {
    size_t num_entries = QuotientFilter::get_num_entries(false);
    if (!include_all_internal_filters)
      return num_entries;

    for (const auto &q : older_filters_) {
      num_entries += q.get_num_entries(false);
    }
    return num_entries;
  }

  [[nodiscard]] auto get_utilization() const -> double override {
    size_t num_slots = 1UZ << power_of_two_size;
    for (const auto &q : older_filters_)
      num_slots += 1UZ << q.power_of_two_size;
    const size_t num_entries = get_num_entries(true);
    return static_cast<double>(num_entries) / static_cast<double>(num_slots);
  }

  [[nodiscard]] auto measure_num_bits_per_entry() const -> double override {
    std::vector<const QuotientFilter *> other_filters;
    other_filters.reserve(older_filters_.size() + 1);
    for (const auto &q : older_filters_)
      other_filters.emplace_back(&q);
    return QuotientFilter::measure_num_bits_per_entry(*this, other_filters);
  }

  auto expand() -> bool override {
    QuotientFilter placeholder(power_of_two_size, bit_per_entry, filter);
    placeholder.hash_type = this->hash_type;
    older_filters_.push_back(std::move(placeholder));
    older_filters_.back().num_existing_entries = num_existing_entries;
    num_existing_entries = 0;
    power_of_two_size += size_style_ == SizeExpansion::GEOMETRIC ? 1 : 0;

    fingerprint_length = FingerprintGrowthStrategy::get_new_fingerprint_size(
        original_fingerprint_size, num_expansions, fpr_style_);
    bit_per_entry = fingerprint_length + 3;
    size_t init_size = 1UZ << power_of_two_size;
    num_extension_slots += 2;
    filter = make_filter(init_size, bit_per_entry);
    QuotientFilter::update(init_size);
    max_entries_before_expansion =
        static_cast<size_t>(std::pow(2UZ, power_of_two_size) * expansion_threshold);
    return true;
  }

  [[nodiscard]] auto query(uint64_t input) -> bool override {
    if (QuotientFilter::Filter::query(input))
      return true;

    for (auto &qf : older_filters_)
      if (qf.Filter::query(input))
        return true;

    return false;
  }

private:
  SizeExpansion size_style_{SizeExpansion::GEOMETRIC};
  FingerprintGrowthStrategy::FalsePositiveRateExpansion fpr_style_{
      FingerprintGrowthStrategy::FalsePositiveRateExpansion::UNIFORM};
  std::vector<QuotientFilter> older_filters_;
};

} // namespace infinifilter
