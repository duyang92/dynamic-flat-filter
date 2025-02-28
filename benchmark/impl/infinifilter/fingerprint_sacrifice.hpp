#pragma once

#include <cstddef>
#include <cstdint>

#include "iterator.hpp"
#include "quotient_filter.hpp"

namespace infinifilter {

class FingerprintSacrifice : public QuotientFilter {
public:
  FingerprintSacrifice(size_t power_of_two, size_t bits_per_entry)
      : QuotientFilter(power_of_two, bits_per_entry) {
    max_entries_before_expansion =
        static_cast<size_t>(std::pow(2, power_of_two_size) * expansion_threshold);
  }

  auto expand() -> bool override {
    if (fingerprint_length == 0) {
      is_full = true;
      return false;
    }

    QuotientFilter new_qf(power_of_two_size + 1, bit_per_entry - 1);
    Iterator it(*this);

    while (it.next()) {
      size_t bucket = it.bucket_index;
      uint32_t fingerprint = it.fingerprint;
      uint32_t pivot_bit = (1 & fingerprint);
      uint32_t bucket_mask = pivot_bit << power_of_two_size;
      uint32_t updated_bucket = bucket | bucket_mask;
      uint32_t updated_fingerprint = fingerprint >> 1;

      new_qf.insert(updated_fingerprint, updated_bucket, false);
    }

    last_empty_slot = new_qf.last_empty_slot;
    last_cluster_start = new_qf.last_cluster_start;
    backward_steps = new_qf.backward_steps;

    filter = new_qf.filter;
    power_of_two_size++;
    num_extension_slots += 2;
    bit_per_entry--;
    fingerprint_length--;
    max_entries_before_expansion =
        static_cast<size_t>(std::pow(2, power_of_two_size) * expansion_threshold);
    return true;
  }
};

} // namespace infinifilter
