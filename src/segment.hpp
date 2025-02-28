/*
 * Modified from Cuckoo Filter implementation
 * https://github.com/efficient/cuckoofilter/blob/master/src/cuckoofilter.h
 */

#pragma once

#include <cstddef>
#include <cstdint>

#include "predefine.hpp"
#include "singletable.hpp"

namespace dff {

// Status returned by a cuckoo filter operation
enum Status : std::uint8_t {
  Ok = 0,
  NotFound = 1,
  NotEnoughSpace = 2,
  NotSupported = 3,
};

// Maximum number of cuckoo kicks before claiming failure
constexpr size_t K_MAX_KICK_COUNT = 500;

// A cuckoo filter class exposes a Bloomier filter interface,
// providing methods of `insert`, `remove`, `query`. It takes three
// template parameters:
//   T: the type of item you want to insert
//   ENABLE_FINGERPRINT_GROWTH: whether to enable fingerprint growth
template <typename T, bool ENABLE_FINGERPRINT_GROWTH> class Segment {
  static constexpr uint64_t HASH_SEED = 1234;

  // Evicted tag due to maximum number of kicks
  bool victim_used_ = false;
  size_t victim_index_ = 0;
  uint32_t victim_tag_ = 0;

  /**
   * @brief Generate the bucket index for a given hash.
   *
   * @param hash The hash to generate the index for.
   * @return The index.
   */
  [[nodiscard]] auto index_hash(uint32_t hash) const -> size_t {
    // NOTE: Assume that BUCKETS_PER_SEG is a power of 2
    return hash & (BUCKETS_PER_SEG - 1);
  }

  /**
   * @brief Generate an alternative index for a given index.
   *
   * @param index The index to generate an alternative index for.
   * @param tag The tag to use in the alternative index.
   * @return The alternative index.
   */
  [[nodiscard]] auto alt_index(const size_t index, const uint32_t tag) const -> size_t {
    // A quick and dirty way to generate an alternative index
    // 0x5bd1e995 is the hash constant from MurmurHash2
    if constexpr (ENABLE_FINGERPRINT_GROWTH)
      return index_hash(static_cast<uint32_t>(index) ^
                        ((tag >> k_bits_to_shift_used_by_alt_index) * 0x5bd1e995));
    else
      return index_hash(static_cast<uint32_t>(index) ^ (tag * 0x5bd1e995));
  }

public:
  size_t k_bits_per_item;
  // Used only when fingerprint growth is enabled
  size_t k_high_bits_used_by_alt_index;
  // Used only when fingerprint growth is enabled
  size_t k_bits_to_shift_used_by_alt_index;

  // Number of items stored
  size_t num_items = 0;
  SingleTable<ENABLE_FINGERPRINT_GROWTH> *table;
  Segment<T, ENABLE_FINGERPRINT_GROWTH> *next;

  size_t capacity;

  // Corresponding lookup table slots occupied by this segment
  uint32_t lut_slots[LOOKUP_TABLE_SIZE]{};
  // Number of lookup table slots occupied by this segment
  uint32_t lut_slots_count = 0;

  Segment(const Segment &) = default;
  Segment(Segment &&) = default;
  auto operator=(const Segment &) -> Segment & = default;
  auto operator=(Segment &&) -> Segment & = default;

  explicit Segment(const size_t num_buckets, const size_t bits_per_item,
                   const size_t high_bits_used_by_alt_index)
      : k_bits_per_item(bits_per_item), k_high_bits_used_by_alt_index(high_bits_used_by_alt_index),
        k_bits_to_shift_used_by_alt_index(k_bits_per_item - high_bits_used_by_alt_index + 1),
        table(new SingleTable<ENABLE_FINGERPRINT_GROWTH>(num_buckets, bits_per_item)),
        next(nullptr),
        capacity(static_cast<size_t>(static_cast<double>(num_buckets) * SLOTS_PER_BUCKET * 0.9)) {}

  ~Segment() { delete table; }

  /**
   * @brief Try to insert a hash into a bucket at a given index. If the bucket
   * is full, try another bucket with an alternative index and enable kickout,
   * and repeat until a bucket is found or the maximum number of cuckoo kicks is
   * reached.
   *
   * Warning: If this does not return `Ok`, you should stop inserting items anymore, otherwise
   * some inserted items may be lost, causing false negatives.
   *
   * @param index The preferred index to insert the tag at.
   * @param hash The hash to insert.
   * @return The status of the operation.
   */
  auto insert(const size_t &index, const uint32_t &hash) -> Status {
    size_t cur_index = index;
    uint32_t cur_tag = table->gen_tag(hash);
    uint32_t old_tag;

    if (table->insert_tag_to_bucket(cur_index, cur_tag, false, old_tag)) {
      num_items++;
      return Ok;
    }
    cur_index = alt_index(cur_index, cur_tag);

    for (size_t count = 0; count < K_MAX_KICK_COUNT; count++) {
      old_tag = 0;
      if (table->insert_tag_to_bucket(cur_index, cur_tag, true, old_tag)) {
        num_items++;
        return Ok;
      }
      cur_tag = old_tag;
      cur_index = alt_index(cur_index, cur_tag);
    }

    victim_used_ = true;
    victim_index_ = cur_index;
    victim_tag_ = cur_tag;

    return NotEnoughSpace;
  }

  /**
   * @brief Query if a hash is in the filter at a given index, with false
   * positive rate.
   *
   * @param index The index to query.
   * @param tag The hash to query.
   * @return The status of the operation.
   */
  [[nodiscard]] auto query(const size_t &index, const uint32_t &hash) const -> Status {
    const uint32_t tag = table->gen_tag(hash);
    const size_t index2 = alt_index(index, tag);

    if (victim_used_ && (victim_index_ == index || victim_index_ == index2) && victim_tag_ == tag)
      return Ok;

    if constexpr (ENABLE_FINGERPRINT_GROWTH) {
      if (table->match_hash_in_buckets(index, index2, hash))
        return Ok;
    } else {
      const uint32_t tag = table->gen_tag(hash);
      if (table->find_tag_in_buckets(index, index2, tag))
        return Ok;
    }

    return NotFound;
  }

  /**
   * @brief Remove a hash from the filter at a given index.
   *
   * @param index The index to remove the tag from.
   * @param hash The hash to remove.
   * @return The status of the operation.
   */
  auto remove(const size_t &index, const uint32_t &hash) -> Status {
    const uint32_t tag = table->gen_tag(hash);
    const size_t index2 = alt_index(index, tag);

    if constexpr (ENABLE_FINGERPRINT_GROWTH) {
      if (table->remove_hash_from_buckets(index, index2, hash)) {
        num_items--;
        // NOLINTNEXTLINE(cppcoreguidelines-avoid-goto)
        goto try_eliminate_victim;
      }
    } else {
      if (table->remove_tag_from_bucket(index, tag) || table->remove_tag_from_bucket(index2, tag)) {
        num_items--;
        // NOLINTNEXTLINE(cppcoreguidelines-avoid-goto)
        goto try_eliminate_victim;
      }
    }

    if (victim_used_ && (victim_index_ == index || victim_index_ == index2) && victim_tag_ == tag) {
      victim_used_ = false;
      return Ok;
    }

    return NotFound;

  try_eliminate_victim:
    if (victim_used_) {
      victim_used_ = false;
      insert(victim_index_, victim_tag_);
    }
    return Ok;
  }
};

} // namespace dff
