#pragma once

#include <cassert>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <set>
#include <utility>
#include <vector>

#include "bitmap/bitmap.hpp"
#include "bitmap/quick_bit_vector_wrapper.hpp"
#include "filter.hpp"

namespace infinifilter {

inline auto get_current_time_in_seconds_qf() -> double {
  const auto now = std::chrono::high_resolution_clock::now();
  const auto duration = now.time_since_epoch();
  return std::chrono::duration_cast<std::chrono::duration<double>>(duration).count();
}

class QuotientFilter : public Filter {
public:
  size_t bit_per_entry;
  size_t fingerprint_length;
  size_t power_of_two_size;
  size_t num_extension_slots;
  size_t num_existing_entries;
  std::shared_ptr<bitmap::Bitmap> filter;

  // These three fields are used to prevent throwing exceptions when the buffer
  // space of the filter is exceeded
  size_t last_empty_slot;
  size_t last_cluster_start;
  size_t backward_steps;

  double expansion_threshold;
  size_t max_entries_before_expansion;
  bool expand_autonomously;
  bool is_full;

  // statistics, computed in the compute_statistics method. method should be
  // called before these are used
  size_t num_runs;
  size_t num_clusters;
  double avg_run_length;
  double avg_cluster_length;

  size_t original_fingerprint_size;
  size_t num_expansions;

  QuotientFilter(size_t power_of_two, size_t bits_per_entry)
      : Filter(HashType::XXH), power_of_two_size(power_of_two), bit_per_entry(bits_per_entry),
        fingerprint_length(bits_per_entry - 3), num_extension_slots(power_of_two * 2),
        num_existing_entries(0), last_empty_slot(0), last_cluster_start(0), backward_steps(0),
        expansion_threshold(0.9), max_entries_before_expansion(0), expand_autonomously(false),
        is_full(false), num_runs(0), num_clusters(0), avg_run_length(0.0), avg_cluster_length(0.0),
        original_fingerprint_size(bits_per_entry - 3), num_expansions(0) {

    size_t init_size = 1UZ << power_of_two;
    filter = make_filter(init_size, bits_per_entry);
    max_entries_before_expansion = static_cast<size_t>((double)init_size * expansion_threshold);
    last_empty_slot = init_size + num_extension_slots - 1;
  }

  [[nodiscard]] auto size() const -> size_t { return 1UZ << power_of_two_size; }

  void update(size_t init_size) {
    last_empty_slot = init_size + num_extension_slots - 1;
    last_cluster_start = 0;
    backward_steps = 0;
  }

  auto rejuvenate(uint64_t key) -> bool override { return false; }

  [[nodiscard]] auto get_num_existing_entries() const -> size_t { return num_existing_entries; }

  [[nodiscard]] auto get_max_entries_before_expansion() const -> size_t {
    return max_entries_before_expansion;
  }

  [[nodiscard]] auto get_expand_autonomously() const -> bool { return expand_autonomously; }

  void set_expand_autonomously(bool val) { expand_autonomously = val; }

  [[nodiscard]] auto make_filter(size_t init_size, size_t bits_per_entry) const
      -> std::shared_ptr<bitmap::Bitmap> {
    return std::make_shared<bitmap::QuickBitVectorWrapper>(bits_per_entry,
                                                           init_size + num_extension_slots);
  }

  [[nodiscard]] auto get_fingerprint_length() const -> size_t { return fingerprint_length; }

  QuotientFilter(size_t power_of_two, size_t bits_per_entry, std::shared_ptr<bitmap::Bitmap> bitmap)
      : Filter(HashType::XXH), power_of_two_size(power_of_two), bit_per_entry(bits_per_entry),
        fingerprint_length(bits_per_entry - 3), filter(std::move(bitmap)),
        num_extension_slots(power_of_two * 2), num_existing_entries(0), last_empty_slot(0),
        last_cluster_start(0), backward_steps(0), expansion_threshold(0.8),
        max_entries_before_expansion(0), expand_autonomously(false), is_full(false), num_runs(0),
        num_clusters(0), avg_run_length(0.0), avg_cluster_length(0.0),
        original_fingerprint_size(bits_per_entry - 3), num_expansions(0) {
    size_t init_size = 1UZ << power_of_two;
    last_empty_slot = init_size + num_extension_slots - 1;
  }

  auto expand() -> bool override {
    is_full = true;
    return false;
  }

  [[nodiscard]] auto measure_num_bits_per_entry() const -> double override {
    return measure_num_bits_per_entry(*this, {});
  }

  static auto measure_num_bits_per_entry(const QuotientFilter &current,
                                         const std::vector<const QuotientFilter *> &other_filters)
      -> double {
    size_t num_entries = current.get_num_entries(false);
    for (const auto &q : other_filters)
      num_entries += q->get_num_entries(false);
    size_t init_size = 1UZ << current.power_of_two_size;
    size_t num_bits =
        current.bit_per_entry * init_size + current.num_extension_slots * current.bit_per_entry;
    for (const auto &q : other_filters) {
      init_size = 1UZ << q->power_of_two_size;
      num_bits += q->bit_per_entry * init_size + q->num_extension_slots * q->bit_per_entry;
    }
    double bits_per_entry = (double)num_bits / (double)num_entries;
    return bits_per_entry;
  }

  [[nodiscard]] auto get_num_entries(bool include_all_internal_filters) const -> size_t override {
    size_t slots = get_physical_num_slots();
    size_t num_entries = 0;
    for (size_t i = 0; i < slots; i++)
      if (is_occupied(i) || is_continuation(i) || is_shifted(i))
        num_entries++;
    return num_entries;
  }

  [[nodiscard]] auto get_utilization() const -> double override {
    size_t num_logical_slots = 1UZ << power_of_two_size;
    size_t num_entries = get_num_entries(false);
    return static_cast<double>(num_entries) / static_cast<double>(num_logical_slots);
  }

  [[nodiscard]] auto get_physical_num_slots() const -> size_t {
    return filter->size() / bit_per_entry;
  }

  [[nodiscard]] auto get_logical_num_slots_plus_extensions() const -> size_t {
    return (1UZ << power_of_two_size) + num_extension_slots;
  }

  [[nodiscard]] auto get_logical_num_slots() const -> size_t { return 1UZ << power_of_two_size; }

  void modify_slot(bool is_occupied, bool is_continuation, bool is_shifted, size_t index) {
    set_occupied(index, is_occupied);
    set_continuation(index, is_continuation);
    set_shifted(index, is_shifted);
  }

  void set_fingerprint(size_t index, uint32_t fingerprint) {
    filter->set_from_to(index * bit_per_entry + 3, index * bit_per_entry + 3 + fingerprint_length,
                        fingerprint);
  }

  [[nodiscard]] auto get_pretty_str(bool vertical) const -> std::string {
    std::string sbr;

    size_t logic_slots = get_logical_num_slots();
    size_t all_slots = get_logical_num_slots_plus_extensions();

    for (size_t i = 0; i < filter->size(); i++) {
      size_t remainder = i % bit_per_entry;
      if (remainder == 0) {
        size_t slot_num = i / bit_per_entry;
        sbr.append(" ");
        if (vertical) {
          if (slot_num == logic_slots || slot_num == all_slots)
            sbr.append("\n ---------");
          sbr.append("\n" + std::to_string(slot_num) + " ");
        }
      }
      if (remainder == 3)
        sbr.append(" ");
      sbr.append(filter->get(i) ? "1" : "0");
    }
    sbr.append("\n");
    return sbr;
  }

  [[nodiscard]] auto get_fingerprint(size_t index) const -> uint32_t {
    return filter->get_from_to(index * bit_per_entry + 3,
                               index * bit_per_entry + 3 + fingerprint_length);
  }

  [[nodiscard]] auto get_slot(size_t index) -> uint32_t {
    return filter->get_from_to(index * bit_per_entry, (index + 1) * bit_per_entry);
  }

  [[nodiscard]] virtual auto compare(size_t index, uint32_t fingerprint) const -> bool {
    return get_fingerprint(index) == fingerprint;
  }

  void modify_slot(bool is_occupied, bool is_continuation, bool is_shifted, size_t index,
                   uint32_t fingerprint) {
    modify_slot(is_occupied, is_continuation, is_shifted, index);
    set_fingerprint(index, fingerprint);
  }

  [[nodiscard]] auto is_occupied(size_t index) const -> bool {
    return filter->get(index * bit_per_entry);
  }

  [[nodiscard]] auto is_continuation(size_t index) const -> bool {
    return filter->get(index * bit_per_entry + 1);
  }

  [[nodiscard]] auto is_shifted(size_t index) const -> bool {
    return filter->get(index * bit_per_entry + 2);
  }

  void set_occupied(size_t index, bool val) { filter->set(index * bit_per_entry, val); }

  void set_continuation(size_t index, bool val) { filter->set(index * bit_per_entry + 1, val); }

  void set_shifted(size_t index, bool val) { filter->set(index * bit_per_entry + 2, val); }

  [[nodiscard]] auto is_slot_empty(size_t index) const -> bool {
    return !is_occupied(index) && !is_continuation(index) && !is_shifted(index);
  }

  [[nodiscard]] auto find_cluster_start(size_t index) const -> size_t {
    size_t current_index = index;
    while (is_shifted(current_index))
      current_index--;
    return current_index;
  }

  [[nodiscard]] auto find_run_start(size_t index) -> size_t {
    size_t current_index = index;
    size_t runs_to_skip_counter = 1;
    while (is_shifted(current_index)) {
      if (is_occupied(current_index))
        runs_to_skip_counter++;
      current_index--;
    }
    // Note: this can be SIZE_T_MAX due to underflow
    last_cluster_start = current_index - 1;
    while (true) {
      if (!is_continuation(current_index)) {
        runs_to_skip_counter--;
        if (runs_to_skip_counter == 0)
          return current_index;
      }
      current_index++;
    }
  }

  [[nodiscard]] auto find_first_fingerprint_in_run(size_t index, uint32_t fingerprint) const
      -> size_t {
    assert(!is_continuation(index));
    do {
      if (compare(index, fingerprint))
        return index;
      index++;
    } while (index < get_logical_num_slots_plus_extensions() && is_continuation(index));
    return static_cast<size_t>(-1);
  }

  [[nodiscard]] virtual auto decide_which_fingerprint_to_delete(size_t index,
                                                                uint32_t fingerprint) const
      -> size_t {
    assert(!is_continuation(index));
    auto matching_fingerprint_index = static_cast<uint32_t>(-1);
    do {
      if (compare(index, fingerprint))
        matching_fingerprint_index = index;
      index++;
    } while (index < get_logical_num_slots_plus_extensions() && is_continuation(index));
    return matching_fingerprint_index;
  }

  [[nodiscard]] auto find_run_end(size_t index) const -> size_t {
    while (index < get_logical_num_slots_plus_extensions() - 1 && is_continuation(index + 1))
      index++;
    return index;
  }

  [[nodiscard]] auto query(uint32_t fingerprint, size_t index) -> bool {
    bool does_run_exist = is_occupied(index);
    if (!does_run_exist)
      return false;
    size_t run_start_index = find_run_start(index);
    uint32_t found_index = find_first_fingerprint_in_run(run_start_index, fingerprint);
    return found_index != static_cast<uint32_t>(-1);
  }

  [[nodiscard]] auto get_all_fingerprints(size_t bucket_index) -> std::set<uint32_t> {
    bool does_run_exist = is_occupied(bucket_index);
    std::set<uint32_t> set;
    if (!does_run_exist)
      return set;
    size_t run_index = find_run_start(bucket_index);
    do {
      set.insert(get_fingerprint(run_index));
      run_index++;
    } while (is_continuation(run_index));
    return set;
  }

  auto swap_fingerprints(size_t index, uint32_t new_fingerprint) -> uint32_t {
    uint32_t existing = get_fingerprint(index);
    set_fingerprint(index, new_fingerprint);
    return existing;
  }

  [[nodiscard]] auto find_first_empty_slot(size_t index) const -> size_t {
    while (!is_slot_empty(index))
      index++;
    return index;
  }

  [[nodiscard]] auto find_backward_empty_slot(size_t index) -> size_t {
    while (index != -1UZ && !is_slot_empty(index)) {
      backward_steps++;
      index--;
    }
    return index;
  }

  [[nodiscard]] auto find_new_run_location(size_t index) const -> size_t {
    if (!is_slot_empty(index))
      index++;
    while (is_continuation(index))
      index++;
    return index;
  }

  auto insert_new_run(size_t canonical_slot, uint32_t long_fp) -> bool {
    size_t first_empty_slot = find_first_empty_slot(canonical_slot);
    size_t preexisting_run_start_index = find_run_start(canonical_slot);
    size_t start_of_this_new_run = find_new_run_location(preexisting_run_start_index);
    bool slot_initially_empty = is_slot_empty(start_of_this_new_run);
    set_occupied(canonical_slot, true);
    if (first_empty_slot != canonical_slot)
      set_shifted(start_of_this_new_run, true);
    set_continuation(start_of_this_new_run, false);

    if (slot_initially_empty) {
      set_fingerprint(start_of_this_new_run, long_fp);
      if (start_of_this_new_run == last_empty_slot)
        last_empty_slot = find_backward_empty_slot(last_cluster_start);
      num_existing_entries++;
      return true;
    }

    size_t current_index = start_of_this_new_run;
    bool is_this_slot_empty;
    bool temp_continuation = false;
    do {
      if (current_index >= get_logical_num_slots_plus_extensions())
        return false;

      is_this_slot_empty = is_slot_empty(current_index);
      long_fp = swap_fingerprints(current_index, long_fp);

      if (current_index > start_of_this_new_run)
        set_shifted(current_index, true);

      if (current_index > start_of_this_new_run) {
        bool current_continuation = is_continuation(current_index);
        set_continuation(current_index, temp_continuation);
        temp_continuation = current_continuation;
      }
      current_index++;
      if (current_index == last_empty_slot)
        // Note: `last_empty_slot` can be SIZE_T_MAX due to underflow
        last_empty_slot = find_backward_empty_slot(last_cluster_start);
    } while (!is_this_slot_empty);
    num_existing_entries++;
    return true;
  }

  auto insert(uint32_t long_fp, size_t index, bool insert_only_if_no_match) -> bool {
    if (index == -1UZ || index > last_empty_slot)
      return false;
    bool does_run_exist = is_occupied(index);
    if (!does_run_exist)
      return insert_new_run(index, long_fp);
    size_t run_start_index = find_run_start(index);
    if (does_run_exist && insert_only_if_no_match) {
      size_t found_index = find_first_fingerprint_in_run(run_start_index, long_fp);
      if (found_index != static_cast<size_t>(-1))
        return false;
    }
    return insert_fingerprint_and_push_all_else(long_fp, run_start_index);
  }

  auto insert_fingerprint_and_push_all_else(uint32_t long_fp, uint32_t run_start_index) -> bool {
    uint32_t current_index = run_start_index;
    bool is_this_slot_empty;
    bool finished_first_run = false;
    bool temp_continuation = false;
    do {
      if (current_index >= get_logical_num_slots_plus_extensions())
        return false;
      is_this_slot_empty = is_slot_empty(current_index);
      if (current_index > run_start_index)
        set_shifted(current_index, true);
      if (current_index > run_start_index && !finished_first_run &&
          !is_continuation(current_index)) {
        finished_first_run = true;
        set_continuation(current_index, true);
        long_fp = swap_fingerprints(current_index, long_fp);
      } else if (finished_first_run) {
        bool current_continuation = is_continuation(current_index);
        set_continuation(current_index, temp_continuation);
        temp_continuation = current_continuation;
        long_fp = swap_fingerprints(current_index, long_fp);
      }
      if (current_index == last_empty_slot)
        // Note: `last_empty_slot` can be SIZE_T_MAX due to underflow
        last_empty_slot = find_backward_empty_slot(last_cluster_start);
      current_index++;
    } while (!is_this_slot_empty);
    num_existing_entries++;
    return true;
  }

  auto _remove(uint32_t large_hash) -> bool override {
    uint32_t slot_index = get_slot_index(large_hash);
    uint32_t fp_long = gen_fingerprint(large_hash);
    bool success = remove(fp_long, slot_index);
    if (success)
      num_existing_entries--;
    return success;
  }

  auto remove(uint32_t fingerprint, size_t canonical_slot, size_t run_start_index,
              size_t matching_fingerprint_index) -> bool {
    size_t run_end = find_run_end(matching_fingerprint_index);
    bool turn_off_occupied = run_start_index == run_end;

    for (size_t i = matching_fingerprint_index; i < run_end; i++) {
      uint32_t f = get_fingerprint(i + 1);
      set_fingerprint(i, f);
    }

    size_t cluster_start = find_cluster_start(canonical_slot);
    size_t num_shifted_count = 0;
    size_t num_non_occupied = 0;
    for (size_t i = cluster_start; i <= run_end; i++) {
      if (is_continuation(i))
        num_shifted_count++;
      if (!is_occupied(i))
        num_non_occupied++;
    }
    set_fingerprint(run_end, 0);
    set_shifted(run_end, false);
    set_continuation(run_end, false);

    do {
      if (run_end >= get_logical_num_slots_plus_extensions() - 1 || is_slot_empty(run_end + 1) ||
          !is_shifted(run_end + 1)) {
        if (turn_off_occupied)
          set_occupied(canonical_slot, false);
        if (last_empty_slot == -1UZ || run_end > last_empty_slot)
          last_empty_slot = run_end;
        return true;
      }

      size_t next_run_start = run_end + 1;
      run_end = find_run_end(next_run_start);

      if (is_occupied(next_run_start - 1) && num_shifted_count - num_non_occupied == 1)
        set_shifted(next_run_start - 1, false);
      else
        set_shifted(next_run_start - 1, true);

      for (size_t i = next_run_start; i <= run_end; i++) {
        uint32_t f = get_fingerprint(i);
        set_fingerprint(i - 1, f);
        if (is_continuation(i))
          set_continuation(i - 1, true);
        if (!is_occupied(i))
          num_non_occupied++;
      }
      num_shifted_count += run_end - next_run_start;
      set_fingerprint(run_end, 0);
      set_shifted(run_end, false);
      set_continuation(run_end, false);
    } while (true);
  }

  auto remove(uint32_t fingerprint, uint32_t canonical_slot) -> bool {
    if (canonical_slot >= get_logical_num_slots())
      return false;
    bool does_run_exist = is_occupied(canonical_slot);
    if (!does_run_exist)
      return false;
    size_t run_start_index = find_run_start(canonical_slot);
    size_t matching_fingerprint_index =
        decide_which_fingerprint_to_delete(run_start_index, fingerprint);

    if (matching_fingerprint_index == static_cast<size_t>(-1))
      return false;

    return remove(fingerprint, canonical_slot, run_start_index, matching_fingerprint_index);
  }

  auto _insert(uint32_t large_hash, bool insert_only_if_no_match) -> bool override {
    if (is_full)
      return false;

    size_t slot_index = get_slot_index(large_hash);
    uint32_t fingerprint = gen_fingerprint(large_hash);
    bool success = insert(fingerprint, slot_index, insert_only_if_no_match);

    if (expand_autonomously && num_existing_entries >= max_entries_before_expansion) {
      bool expanded = expand();
      if (expanded)
        num_expansions++;
    }
    return success;
  }

  auto _query(uint32_t large_hash) -> bool override {
    size_t slot_index = get_slot_index(large_hash);
    uint32_t fingerprint = gen_fingerprint(large_hash);
    return query(fingerprint, slot_index);
  }

  [[nodiscard]] auto get_slot_index(uint32_t large_hash) const -> size_t {
    size_t slot_index_mask = (1UZ << power_of_two_size) - 1;
    return large_hash & slot_index_mask;
  }

  [[nodiscard]] virtual auto gen_fingerprint(uint32_t large_hash) const -> uint32_t {
    uint32_t fingerprint_mask = (1U << fingerprint_length) - 1U;
    fingerprint_mask = fingerprint_mask << power_of_two_size;
    return (large_hash & fingerprint_mask) >> power_of_two_size;
  }

  void set_expansion_threshold(double thresh) {
    expansion_threshold = thresh;
    max_entries_before_expansion =
        static_cast<size_t>(std::pow(2, power_of_two_size) * expansion_threshold);
  }

  [[nodiscard]] auto get_bit_at_offset(size_t offset) const -> bool { return filter->get(offset); }

  void compute_statistics() {
    num_runs = 0;
    num_clusters = 0;
    double sum_run_lengths = 0;
    double sum_cluster_lengths = 0;
    size_t current_run_length = 0;
    size_t current_cluster_length = 0;

    uint32_t num_slots = get_logical_num_slots_plus_extensions();
    for (uint32_t i = 0; i < num_slots; i++) {

      bool occupied = is_occupied(i);
      bool continuation = is_continuation(i);
      bool shifted = is_shifted(i);

      if (!occupied && !continuation && !shifted) {
        sum_cluster_lengths += (double)current_cluster_length;
        current_cluster_length = 0;
        sum_run_lengths += (double)current_run_length;
        current_run_length = 0;
        // NOLINTNEXTLINE(bugprone-branch-clone)
      } else if (!occupied && !continuation && shifted) {
        num_runs++;
        sum_run_lengths += (double)current_run_length;
        current_run_length = 1;
        current_cluster_length++;
        // NOLINTNEXTLINE(bugprone-branch-clone)
      } else if (!occupied && continuation && !shifted) {
        // not used
        // NOLINTNEXTLINE(bugprone-branch-clone)
      } else if (!occupied && continuation && shifted) {
        current_cluster_length++;
        current_run_length++;
      } else if (occupied && !continuation && !shifted) {
        num_runs++;
        num_clusters++;
        sum_cluster_lengths += (double)current_cluster_length;
        sum_run_lengths += (double)current_run_length;
        current_cluster_length = 1;
        current_run_length = 1;
      } else if (occupied && !continuation && shifted) {
        num_runs++;
        sum_run_lengths += (double)current_run_length;
        current_run_length = 1;
        current_cluster_length++;
      } else if (occupied && continuation && !shifted) {
        // not used
      } else if (occupied && continuation && shifted) {
        current_cluster_length++;
        current_run_length++;
      }
    }
    avg_run_length = sum_run_lengths / (double)num_runs;
    avg_cluster_length = sum_cluster_lengths / (double)num_clusters;
  }

  static void ar_sum1(std::vector<size_t> &ar, size_t index) {
    size_t s = ar.size();
    if (s <= index)
      ar.resize(index + 1, 0);
    ar[index] += 1;
  }

  auto measure_cluster_length() -> std::vector<size_t> {
    std::vector<size_t> ar;
    num_runs = 0;
    num_clusters = 0;

    size_t current_run_length = 0;
    size_t current_cluster_length = 0;

    size_t cnt = 0;

    for (size_t i = 0; i < get_logical_num_slots_plus_extensions(); i++) {

      bool occupied = is_occupied(i);
      bool continuation = is_continuation(i);
      bool shifted = is_shifted(i);

      if (!occupied && !continuation && !shifted) {
        if (current_cluster_length != 0)
          ar_sum1(ar, current_cluster_length - 1);
        current_cluster_length = 0;
        current_run_length = 0;
        // NOLINTNEXTLINE(bugprone-branch-clone)
      } else if (!occupied && !continuation && shifted) {
        num_runs++;
        current_run_length = 1;
        current_cluster_length++;
        // NOLINTNEXTLINE(bugprone-branch-clone)
      } else if (!occupied && continuation && shifted) {
        current_cluster_length++;
        current_run_length++;
      } else if (occupied && !continuation && !shifted) {
        if (current_cluster_length != 0)
          ar_sum1(ar, current_cluster_length - 1);
        num_runs++;
        num_clusters++;
        current_cluster_length = 1;
        current_run_length = 1;
      } else if (occupied && !continuation && shifted) {
        num_runs++;
        current_run_length = 1;
        current_cluster_length++;
      } else if (occupied && continuation && shifted) {
        current_cluster_length++;
        current_run_length++;
      }
    }
    if (current_cluster_length != 0)
      ar_sum1(ar, current_cluster_length - 1);
    return ar;
  }
};

} // namespace infinifilter
