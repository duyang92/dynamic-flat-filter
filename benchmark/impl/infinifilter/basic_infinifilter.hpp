#pragma once

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <ostream>

#include "fingerprint_growth_strategy.hpp"
#include "iterator.hpp"
#include "quotient_filter.hpp"

namespace infinifilter {

class BasicInfiniFilter : public QuotientFilter {
  friend class ChainedInfiniFilter;

protected:
  uint32_t empty_fingerprint{};
  size_t num_void_entries = 0;
  FingerprintGrowthStrategy::FalsePositiveRateExpansion fpr_style =
      FingerprintGrowthStrategy::FalsePositiveRateExpansion::UNIFORM;
  size_t num_distinct_void_entries = 0;

public:
  BasicInfiniFilter(size_t power_of_two, size_t bits_per_entry)
      : QuotientFilter(power_of_two, bits_per_entry) {
    max_entries_before_expansion =
        static_cast<size_t>(std::pow(2, power_of_two_size) * expansion_threshold);
    set_empty_fingerprint(fingerprint_length);
  }

  [[nodiscard]] static auto prep_unary_mask(size_t prev_fp_size, size_t new_fp_size) -> uint32_t {
    const size_t fingerprint_diff = new_fp_size - prev_fp_size;
    uint32_t unary_mask = 0;
    for (size_t i = 0; i < fingerprint_diff + 1; i++) {
      unary_mask <<= 1;
      unary_mask |= 1;
    }
    unary_mask <<= new_fp_size - 1 - fingerprint_diff;
    return unary_mask;
  }

  void set_fpr_style(FingerprintGrowthStrategy::FalsePositiveRateExpansion val) { fpr_style = val; }

  void set_empty_fingerprint(size_t fp_length) { empty_fingerprint = (1U << fp_length) - 2U; }

  [[nodiscard]] auto compare(size_t index, uint32_t fingerprint) const -> bool override {
    const size_t generation = parse_unary(index);
    const size_t first_fp_bit = index * bit_per_entry + 3;
    const size_t last_fp_bit = index * bit_per_entry + 3 + fingerprint_length - (generation + 1);
    const size_t actual_fp_length = last_fp_bit - first_fp_bit;
    const uint32_t existing_fingerprint = filter->get_from_to(first_fp_bit, last_fp_bit);
    const uint32_t mask = (1U << actual_fp_length) - 1U;
    const uint32_t adjusted_saught_fp = fingerprint & mask;
    return existing_fingerprint == adjusted_saught_fp;
  }

  [[nodiscard]] auto parse_unary(size_t slot_index) const -> size_t {
    const uint32_t f = get_fingerprint(slot_index);
    const uint32_t inverted_fp = ~f;
    const uint32_t mask = (1U << fingerprint_length) - 1U;
    const uint32_t masked = mask & inverted_fp;
    const uint32_t highest = highest_one_bit(masked);
    const size_t leading_zeros = __builtin_ctzll(highest);
    const size_t age = fingerprint_length - leading_zeros - 1;
    return age;
  }

  auto rejuvenate(uint64_t key) -> bool override {
    const uint32_t large_hash = get_hash(key);
    const uint32_t fingerprint = gen_fingerprint(large_hash);
    const size_t ideal_index = get_slot_index(large_hash);

    const bool does_run_exist = is_occupied(ideal_index);
    if (!does_run_exist)
      return false;

    const size_t run_start_index = find_run_start(ideal_index);
    const size_t smallest_index =
        find_largest_matching_fingerprint_in_run(run_start_index, fingerprint);
    if (smallest_index == std::numeric_limits<size_t>::max())
      return false;

    swap_fingerprints(smallest_index, fingerprint);
    return true;
  }

  [[nodiscard]] auto decide_which_fingerprint_to_delete(size_t index, uint32_t fingerprint) const
      -> size_t override {
    return find_largest_matching_fingerprint_in_run(index, fingerprint);
  }

  [[nodiscard]] auto find_largest_matching_fingerprint_in_run(size_t index,
                                                              uint32_t fingerprint) const
      -> size_t {
    assert(!is_continuation(index));
    size_t matching_fingerprint_index = std::numeric_limits<size_t>::max();
    size_t lowest_age = std::numeric_limits<size_t>::max();
    do {
      if (compare(index, fingerprint)) {
        const size_t age = parse_unary(index);
        if (age < lowest_age) {
          lowest_age = age;
          matching_fingerprint_index = index;
        }
      }
      index++;
    } while (is_continuation(index));
    return matching_fingerprint_index;
  }

  [[nodiscard]] auto gen_fingerprint(uint32_t large_hash) const -> uint32_t override {
    uint32_t fingerprint_mask = (1U << fingerprint_length) - 1U;
    fingerprint_mask <<= power_of_two_size;
    const uint32_t fingerprint = (large_hash & fingerprint_mask) >> power_of_two_size;
    const uint32_t unary_mask = ~(1U << (fingerprint_length - 1U));
    const uint32_t updated_fingerprint = fingerprint & unary_mask;
    return updated_fingerprint;
  }

  virtual void handle_empty_fingerprint(size_t bucket_index, QuotientFilter &insertee) {
    std::cout << "called" << std::endl;
  }

  [[nodiscard]] auto get_num_void_entries() const -> size_t {
    size_t num = 0;
    for (size_t i = 0; i < get_physical_num_slots(); i++) {
      const uint32_t fp = get_fingerprint(i);
      if (fp == empty_fingerprint)
        num++;
    }
    return num;
  }

  void report_void_entry_creation(size_t slot) { num_distinct_void_entries++; }

  auto expand() -> bool override {
    if (is_full())
      return false;

    size_t new_fingerprint_size = FingerprintGrowthStrategy::get_new_fingerprint_size(
        original_fingerprint_size, num_expansions, fpr_style);
    new_fingerprint_size = std::max(new_fingerprint_size, fingerprint_length);
    QuotientFilter new_qf(power_of_two_size + 1, new_fingerprint_size + 3);
    Iterator it(*this);
    const uint32_t unary_mask = prep_unary_mask(fingerprint_length, new_fingerprint_size);

    const uint32_t current_empty_fingerprint = empty_fingerprint;
    set_empty_fingerprint(new_fingerprint_size);
    num_void_entries = 0;

    while (it.next()) {
      const size_t bucket = it.bucket_index;
      const int64_t fingerprint = it.fingerprint;
      if (it.fingerprint != current_empty_fingerprint) {
        const uint64_t pivot_bit = (1 & fingerprint);
        const uint64_t bucket_mask = pivot_bit << power_of_two_size;
        const size_t updated_bucket = bucket | bucket_mask;
        const int64_t chopped_fingerprint = fingerprint >> 1;
        const int64_t updated_fingerprint = chopped_fingerprint | unary_mask;
        new_qf.insert(updated_fingerprint, updated_bucket, false);

        num_void_entries += updated_fingerprint == empty_fingerprint ? 1 : 0;
        if (updated_fingerprint == empty_fingerprint)
          report_void_entry_creation(updated_bucket);
      } else {
        handle_empty_fingerprint(it.bucket_index, new_qf);
      }
    }

    empty_fingerprint = (1U << new_fingerprint_size) - 2;
    fingerprint_length = new_fingerprint_size;
    bit_per_entry = new_fingerprint_size + 3;
    filter = new_qf.filter;
    num_existing_entries = new_qf.num_existing_entries;
    power_of_two_size++;
    num_extension_slots += 2;
    max_entries_before_expansion =
        static_cast<size_t>(std::pow(2, power_of_two_size) * expansion_threshold);
    last_empty_slot = new_qf.last_empty_slot;
    last_cluster_start = new_qf.last_cluster_start;
    backward_steps = new_qf.backward_steps;
    return true;
  }

  [[nodiscard]] virtual auto is_full() const -> bool { return num_void_entries > 0; }

private:
  static constexpr auto highest_one_bit(uint32_t value) -> uint32_t {
    if (value == 0)
      return 0;
    uint32_t highest = 1;
    while (value >>= 1)
      highest <<= 1;
    return highest;
  }
};

} // namespace infinifilter
