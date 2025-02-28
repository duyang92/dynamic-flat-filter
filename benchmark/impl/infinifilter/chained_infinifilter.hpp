#pragma once

#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <ranges>
#include <vector>

#include "basic_infinifilter.hpp"
#include "quotient_filter.hpp"

namespace infinifilter {

inline auto get_current_time_in_seconds() -> double {
  const auto now = std::chrono::high_resolution_clock::now();
  const auto duration = now.time_since_epoch();
  return std::chrono::duration_cast<std::chrono::duration<double>>(duration).count();
}

/*
 * The following example assumes we begin with an InfiniFilter with 2^3, or 8 cells and 4 bits per
 * fingerprint The example assumes decreasing the FPR polynomially, or in other words the
 * fingerprint size for new entries is increasing at a rate of  2(log2(X)), where X is the number of
 * expansions that has taken place. This example shows us how to adjust the capacity of the
 * secondary InfiniFilter in response, and how many bits / entry to assign its fingerprints This is
 * based on the intuition that it takes longer for fingerprints
 *
 * expansions	size	bits / entry	Sec size 	sec bits /entry
 * 0			3		4
 * 1			4		6
 * 2			5		7
 * 3			6		8
 * 4			7		8				3			4
 * 5			8		9
 * 6			9		9
 * 7			10		10				4			6
 * 8			11		10
 * 9			12		10				5			7
 * 10			13		10
 * 11			14		11				6			8
 * 12			15		11				7			8
 * 13			16		11
 * 14			17		11				8			9
 * 15			18		12				9			9
 * 16			19		12
 * 17			20		12				10			10
 * 18			21		12				11			10
 * 19			22		12				12			10
 * 20			23		12				13			10
 * 21			24		12
 * 22			25		13				14			11
 */

class ChainedInfiniFilter : public BasicInfiniFilter {
private:
  size_t slot_mask_ = 0;
  uint32_t fingerprint_mask_ = 0;
  uint32_t unary_mask_ = 0;

public:
  std::vector<BasicInfiniFilter> chain;
  std::shared_ptr<BasicInfiniFilter> secondary_if;

  /* Benchmarking start */
  double total_addressing_time = 0.0;
  double total_expansion_time = 0.0;
  /* Benchmarking end */

  ChainedInfiniFilter(const size_t power_of_two, const size_t bits_per_entry)
      : BasicInfiniFilter(power_of_two, bits_per_entry) {
    chain = std::vector<BasicInfiniFilter>();
  }

  [[nodiscard]] auto is_full() const -> bool override { return false; }

  void prep_masks() {
    if (secondary_if == nullptr)
      return;

    prep_masks(power_of_two_size, secondary_if->power_of_two_size,
               secondary_if->fingerprint_length);
  }

  void prep_masks(size_t active_if_power_of_two, size_t secondary_if_power_of_two,
                  size_t secondary_fp_length) {
    size_t slot_mask = (1UZ << secondary_if_power_of_two) - 1UZ;
    size_t actual_fp_length = active_if_power_of_two - secondary_if_power_of_two;
    size_t fp_mask_num_bits = std::min(secondary_fp_length - 1, actual_fp_length);

    size_t fingerprint_mask = (1UZ << fp_mask_num_bits) - 1UZ;
    size_t num_padding_bits = secondary_fp_length - fp_mask_num_bits;
    size_t unary_mask = 0;
    size_t unary_mask1 = 0;

    if (num_padding_bits > 0) {
      unary_mask1 = (1UZ << (num_padding_bits - 1)) - 1UZ;
      unary_mask = unary_mask1 << (actual_fp_length + 1);
    }

    unary_mask_ = unary_mask;
    slot_mask_ = slot_mask;
    fingerprint_mask_ = fingerprint_mask;
  }

  void handle_empty_fingerprint(size_t bucket_index, QuotientFilter &current) override {
    size_t bucket1 = bucket_index;
    size_t fingerprint = bucket_index >> secondary_if->power_of_two_size;
    size_t slot = bucket1 & slot_mask_;

    size_t adjusted_fingerprint = fingerprint & fingerprint_mask_;
    adjusted_fingerprint |= unary_mask_;

    num_existing_entries--;
    secondary_if->insert(adjusted_fingerprint, slot, false);
  }

  auto _query(uint32_t large_hash) -> bool override {
    const double start = get_current_time_in_seconds();

    if (QuotientFilter::_query(large_hash)) {
      total_addressing_time += get_current_time_in_seconds() - start;
      return true;
    }

    if (secondary_if != nullptr && secondary_if->QuotientFilter::_query(large_hash)) {
      total_addressing_time += get_current_time_in_seconds() - start;
      return true;
    }

    for (auto &qf : chain)
      if (qf.QuotientFilter::_query(large_hash)) {
        total_addressing_time += get_current_time_in_seconds() - start;
        return true;
      }

    total_addressing_time += get_current_time_in_seconds() - start;
    return false;
  }

  void create_secondary(size_t power, size_t fp_size) {
    power = std::max(power, 3UZ);
    secondary_if = std::make_shared<BasicInfiniFilter>(power, fp_size + 3);
    secondary_if->hash_type = this->hash_type;
    secondary_if->fpr_style = fpr_style;
    secondary_if->original_fingerprint_size = original_fingerprint_size;
  }

  auto expand() -> bool override {
    double start;
    start = get_current_time_in_seconds();

    if (secondary_if == nullptr && num_void_entries > 0) {
      auto power = static_cast<size_t>(std::ceil(std::log2(num_void_entries)));
      size_t fp_size = power_of_two_size - power + 1;
      create_secondary(power, fp_size);
    } else if (secondary_if != nullptr && secondary_if->num_void_entries > 0) {
      chain.push_back(*secondary_if);
      size_t orig_fp = secondary_if->fingerprint_length;
      secondary_if = std::make_shared<BasicInfiniFilter>(secondary_if->power_of_two_size + 1,
                                                         secondary_if->fingerprint_length + 3);
      secondary_if->hash_type = this->hash_type;
      secondary_if->original_fingerprint_size = orig_fp;
      secondary_if->fpr_style = fpr_style;
    } else if (secondary_if != nullptr) {
      expand_secondary_if();
    }
    prep_masks();

    const bool res = BasicInfiniFilter::expand();

    total_expansion_time += get_current_time_in_seconds() - start;

    return res;
  }

  void expand_secondary_if() {
    size_t num_entries = secondary_if->num_existing_entries + num_void_entries;
    size_t logical_slots = secondary_if->get_logical_num_slots();
    double secondary_fullness =
        static_cast<double>(num_entries) / static_cast<double>(logical_slots);

    do {
      secondary_if->num_expansions++;
      secondary_if->expand();
      logical_slots = secondary_if->get_logical_num_slots();
      secondary_fullness = static_cast<double>(num_entries) / static_cast<double>(logical_slots);
    } while (secondary_fullness > expansion_threshold / 2.0);
  }

  auto rejuvenate(uint64_t key) -> bool override {
    if (BasicInfiniFilter::rejuvenate(key))
      return true;

    if (secondary_if == nullptr) {
      std::cerr << "Warning: it seems the key to be rejuvenated does not exist. We must only ever "
                   "call rejuvenate on keys that exist."
                << std::endl;
      return false;
    }

    if (secondary_if->Filter::remove(key)) {
      if (!Filter::insert(key, false)) {
        std::cerr << "Failed at rejuvenation" << std::endl;
        std::exit(1);
      }
      return true;
    }

    for (auto &it : std::ranges::reverse_view(chain)) {
      if (it.Filter::remove(key)) {
        if (!Filter::insert(key, false)) {
          std::cerr << "Failed at rejuvenation" << std::endl;
          std::exit(1);
        }
        return true;
      }
    }
    return false;
  }

  auto _remove(uint32_t large_hash) -> bool override {
    size_t slot_index = get_slot_index(large_hash);
    uint32_t fp_long = gen_fingerprint(large_hash);

    if (BasicInfiniFilter::remove(fp_long, slot_index)) {
      num_existing_entries--;
      return true;
    }

    slot_index = secondary_if->get_slot_index(large_hash);
    fp_long = secondary_if->gen_fingerprint(large_hash);
    if (secondary_if->remove(fp_long, slot_index)) {
      num_existing_entries--;
      return true;
    }

    for (auto &it : std::ranges::reverse_view(chain)) {
      slot_index = it.get_slot_index(large_hash);
      fp_long = it.gen_fingerprint(large_hash);
      if (it.remove(fp_long, slot_index))
        return true;
    }
    return false;
  }

  [[nodiscard]] auto measure_num_bits_per_entry() const -> double override {
    std::vector<const QuotientFilter *> other_filters;
    other_filters.reserve(chain.size() + 2);
    for (const auto &q : chain)
      other_filters.emplace_back(&q);
    if (secondary_if != nullptr)
      other_filters.emplace_back(secondary_if.get());
    return QuotientFilter::measure_num_bits_per_entry(*this, other_filters);
  }
};

} // namespace infinifilter
