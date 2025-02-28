#pragma once

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <numbers>
#include <random>

#include "predefine.hpp"
#include "segment.hpp"
#include "utils/bits.hpp"
#include "utils/hash.hpp"

namespace dff {

inline auto get_current_time_in_seconds() -> double {
  const auto now = std::chrono::high_resolution_clock::now();
  const auto duration = now.time_since_epoch();
  return std::chrono::duration_cast<std::chrono::duration<double>>(duration).count();
}

template <typename T, bool ENABLE_FINGERPRINT_GROWTH = false,
          bool BENCHMARK_TRACK_EXPANSION_TIME = false, bool BENCHMARK_TRACK_ADDRESSING_TIME = false>
class DFF {
  static constexpr uint32_t LOWER_32_BIT_MASK = LOWER_BITS_MASK_64(32);

  size_t k_initial_bits_per_item;
  uint64_t k_hash_seed = generate_hash_seed();

  /**
   * @brief Generate a random seed for hash functions.
   *
   * @return The generated seed.
   */
  [[nodiscard]] static auto generate_hash_seed() -> uint64_t {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint64_t> dis(0, std::numeric_limits<uint64_t>::max());
    return dis(gen);
  }

  [[nodiscard]] static auto hash(const T &item, const uint64_t seed) -> uint64_t {
    if constexpr (std::is_integral_v<T> || std::is_enum_v<T>)
      return murmur_hash2_x64_a(&item, sizeof(T), seed);
    else if constexpr (std::is_same_v<T, std::string>)
      return murmur_hash2_x64_a(item.c_str(), item.size(), seed);
    else if constexpr (std::is_same_v<T, const char *>)
      return murmur_hash2_x64_a(item, std::strlen(item), seed);
    else
      return std::hash<T>{}(item);
  }

  /**
   * @brief Calculate the segment index for a given hash (see the formula in the
   * "Constant-time addressing" section of the paper).
   *
   * @param hash The hash (the full hash, not the tag) to calculate the index for.
   * @return The calculated index.
   */
  [[nodiscard]] auto segment_index(const uint32_t hash) const -> size_t {
    // size_t generateTableIndex_Tag(const size_t& index, const uint32_t& tag) {
    //   size_t index_e = max_expansion[index / l];
    //   size_t res =
    //       ((index >> k_l_log) << k_l_log) + (tag >> (bits_per_item - index_e)) * (l >> index_e);
    //   return res;
    // }

    // The index if no expansion happens
    const size_t initial_index = hash & TABLE_MASK;
    // Correct the index if expansion happens
    const size_t index_e = max_expansion[initial_index / INITIAL_LOOKUP_TABLE_ENTRIES_PER_SEG];
    const size_t res = ((initial_index >> k_l_log) << k_l_log) +
                       (static_cast<uint64_t>(hash) >> (32 - index_e)) *
                           (INITIAL_LOOKUP_TABLE_ENTRIES_PER_SEG >> index_e);
    return res;
  }

  /**
   * @brief Generate the bucket index for a given hash.
   *
   * @param hash The hash to generate the index for.
   * @return The index.
   */
  [[nodiscard]] auto bucket_index_hash(const uint32_t hash) const -> size_t {
    // NOTE: Assume that BUCKETS_PER_SEG is a power of 2
    return hash & (BUCKETS_PER_SEG - 1);
  }

  /**
   * @brief Generate the bucket index and hash for a given item.
   *
   * @param item The item to generate the bucket index and tag for.
   * @param bucket_idx The generated index.
   * @param hash The generated hash.
   */
  constexpr void generate_bucket_index_and_hash(const T &item, uint32_t *bucket_idx,
                                                uint32_t *hash) const {
    const uint64_t full_hash = DFF::hash(item, k_hash_seed);

    *bucket_idx = bucket_index_hash(full_hash >> 32);
    *hash = full_hash & LOWER_32_BIT_MASK;
  }

public:
  Segment<T, ENABLE_FINGERPRINT_GROWTH> *head = nullptr;
  Segment<T, ENABLE_FINGERPRINT_GROWTH> *tail = nullptr;

  Segment<T, ENABLE_FINGERPRINT_GROWTH> *lookup_table[LOOKUP_TABLE_SIZE];
  size_t expansion_times[LOOKUP_TABLE_SIZE] = {0};
  size_t max_expansion[INITIAL_SEG_COUNT] = {0};
  size_t k_l_log =
      static_cast<size_t>(std::log(INITIAL_LOOKUP_TABLE_ENTRIES_PER_SEG) / std::numbers::ln2);

  // The number of segments in the filter.
  size_t num_seg = INITIAL_SEG_COUNT;

  /* Benchmarking */
  // Only calculated when `BENCHMARK_TRACK_EXPANSION_TIME` is true
  double total_expansion_time = 0.0;
  // Only calculated when `BENCHMARK_TRACK_ADDRESSING_TIME` is true
  double total_addressing_time = 0.0;

  DFF(const DFF &) = default;
  DFF(DFF &&) = default;
  auto operator=(const DFF &) -> DFF & = default;
  auto operator=(DFF &&) -> DFF & = default;

  explicit DFF(const size_t initial_bits_per_item)
      : k_initial_bits_per_item(initial_bits_per_item) {
    // Initialize lookup table
    size_t counter = 0;
    auto *cur_seg = new Segment<T, ENABLE_FINGERPRINT_GROWTH>(
        BUCKETS_PER_SEG, k_initial_bits_per_item, k_initial_bits_per_item);
    for (size_t i = 0; i < LOOKUP_TABLE_SIZE; i++) {
      if (head == nullptr) {
        head = cur_seg;
        tail = head;
        head->next = nullptr;
        lookup_table[i] = tail;
        cur_seg->lut_slots[cur_seg->lut_slots_count] = i;
        cur_seg->lut_slots_count++;
      } else {
        lookup_table[i] = tail;
        cur_seg->lut_slots[cur_seg->lut_slots_count] = i;
        cur_seg->lut_slots_count++;
      }
      counter++;
      if (counter == INITIAL_LOOKUP_TABLE_ENTRIES_PER_SEG && i != LOOKUP_TABLE_SIZE - 1) {
        cur_seg = new Segment<T, ENABLE_FINGERPRINT_GROWTH>(
            BUCKETS_PER_SEG, k_initial_bits_per_item, k_initial_bits_per_item);
        tail->next = cur_seg;
        tail = cur_seg;
        counter = 0;
      }
    }
  }

  ~DFF() {
    auto current = head;
    while (current != nullptr) {
      auto next = current->next;
      delete current;
      current = nullptr;
      current = next;
    }
    head = tail = nullptr;
  }

  /**
   * @brief Insert an item into the filter.
   *
   * Warning: If this does not return `Ok`, you should stop inserting items anymore, otherwise
   * some inserted items may be lost, causing false negatives.
   *
   * @param item The item to insert.
   * @return The status of the operation.
   */
  auto insert(const T &item) -> Status {
    uint32_t bucket_idx;
    uint32_t hash;
    generate_bucket_index_and_hash(item, &bucket_idx, &hash);

    const size_t seg_idx = segment_index(hash);

    Segment<T, ENABLE_FINGERPRINT_GROWTH> *seg = lookup_table[seg_idx];
    Status res = seg->insert(bucket_idx, hash);

    if (seg->num_items > seg->capacity)
      expand(seg_idx, seg);

    return res;
  }

  /**
   * @brief Query if an item is in the filter, with false positive rate.
   *
   * @param item The item to query.
   * @return The status of the operation.
   */
  auto query(const T &item) -> Status {
    if constexpr (BENCHMARK_TRACK_ADDRESSING_TIME) {
      uint32_t bucket_idx;
      uint32_t hash;
      const double start = get_current_time_in_seconds();
      generate_bucket_index_and_hash(item, &bucket_idx, &hash);
      const size_t seg_idx = segment_index(hash);
      total_addressing_time += get_current_time_in_seconds() - start;

      return lookup_table[seg_idx]->query(bucket_idx, hash);
    } else {
      uint32_t bucket_idx;
      uint32_t hash;
      generate_bucket_index_and_hash(item, &bucket_idx, &hash);

      return lookup_table[segment_index(hash)]->query(bucket_idx, hash);
    }
  }

  /**
   * @brief Remove an item from the filter.
   *
   * @param item The item to remove.
   * @return Status of the operation.
   */
  auto remove(const T &item) -> Status {
    uint32_t bucket_idx;
    uint32_t hash;
    generate_bucket_index_and_hash(item, &bucket_idx, &hash);

    return lookup_table[segment_index(hash)]->remove(bucket_idx, hash);
  }

  /**
   * @brief Expand a segment.
   *
   * @param seg_idx The index of the segment to expand.
   * @param seg The segment to expand.
   * @return The status of the operation.
   */
  auto expand(const size_t seg_idx, Segment<T, ENABLE_FINGERPRINT_GROWTH> *seg) -> Status {
    double start;
    if constexpr (BENCHMARK_TRACK_EXPANSION_TIME)
      start = get_current_time_in_seconds();

    const size_t seg_bits_per_item = seg->k_bits_per_item;
    auto *new_seg = new Segment<T, ENABLE_FINGERPRINT_GROWTH>(
        BUCKETS_PER_SEG,
        ENABLE_FINGERPRINT_GROWTH ? seg_bits_per_item + 1 : k_initial_bits_per_item,
        k_initial_bits_per_item);
    num_seg++;
    if (seg->lut_slots_count < 2)
      return Status::NotSupported;
    tail->next = new_seg;
    tail = new_seg;
    const uint32_t index1 = (seg->lut_slots_count) >> 1;
    const uint32_t index2 = seg->lut_slots_count;
    const size_t expansion_time = expansion_times[seg_idx];

    // spdlog::info("Segment expand triggered. #segs: {} -> {}; seg.cap: {}/{},
    // #fp: {} -> {}, "
    //              "expansion_time: {}",
    //              num_seg - 1, num_seg, seg->num_items, seg->capacity,
    //              seg_bits_per_item, seg_bits_per_item + 1, expansion_time);

    // Move half of the items to the new segment
    for (size_t bucket = 0; bucket < BUCKETS_PER_SEG; bucket++)
      for (size_t slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
        const uint32_t tag = seg->table->read_tag(bucket, slot);
        if (tag != 0) {
          bool should_move;
          bool should_remove;
          // `should_remove` is usually true if `should_move` is true, but false
          // when fingerprint length is exhausted, and in such case the tag will
          // be kept in both segments
          if constexpr (ENABLE_FINGERPRINT_GROWTH) {
            if (expansion_time >= seg_bits_per_item - __builtin_ctz(tag)) {
              // spdlog::warn("Fingerprint length exhausted. tag: {}, expansion_time: {}", tag,
              //              expansion_time);
              should_remove = false;
              should_move = true;
            } else {
              should_remove = should_move =
                  ((tag >> (seg_bits_per_item - expansion_time)) & 1) == 1;
            }
          } else {
            if (expansion_time + 1 >= k_initial_bits_per_item) {
              should_remove = false;
              should_move = true;
            } else {
              should_remove = should_move =
                  ((tag >> (k_initial_bits_per_item - 1 - expansion_time)) & 1) == 1;
            }
          }
          if (should_remove) {
            seg->table->remove_tag(bucket, slot);
            seg->num_items--;
          }
          if (should_move) {
            if constexpr (ENABLE_FINGERPRINT_GROWTH)
              new_seg->table->write_tag(bucket, slot, tag << 1);
            else
              new_seg->table->write_tag(bucket, slot, tag);
            new_seg->num_items++;
          }
        }
      }

    // Assign half of the lookup table slots to the new segment
    for (size_t i = index1; i < index2; i++) {
      new_seg->lut_slots[new_seg->lut_slots_count] = seg->lut_slots[i];
      new_seg->lut_slots_count++;
      lookup_table[seg->lut_slots[i]] = new_seg;
    }
    for (size_t i = 0; i < index2; i++)
      expansion_times[seg->lut_slots[i]]++;
    max_expansion[seg_idx / INITIAL_LOOKUP_TABLE_ENTRIES_PER_SEG] = std::max(
        expansion_times[seg_idx], max_expansion[seg_idx / INITIAL_LOOKUP_TABLE_ENTRIES_PER_SEG]);
    seg->lut_slots_count = index1;

    if constexpr (BENCHMARK_TRACK_EXPANSION_TIME)
      total_expansion_time += get_current_time_in_seconds() - start;

    return Status::Ok;
  }

  /**
   * @brief Compact the filter.
   *
   * @return The status of the operation.
   */
  auto compact() -> Status {
    // TODO: Implement this
    return Status::Ok;
  }
};

} // namespace dff
