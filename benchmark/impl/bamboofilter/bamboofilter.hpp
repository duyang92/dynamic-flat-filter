#pragma once

#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <string>
#include <type_traits>

#include "../../../src/utils/hash.hpp"
#include "predefine.hpp"
#include "segment.hpp"

namespace bamboofilter {

template <typename T> class BambooFilter {
private:
  /* Modified start: Benchmarking */
  double total_addressing_time_ = 0.0;
  /* Modified end: Benchmarking */

  /* Modified start */
  static constexpr uint32_t HASH_SEED = 1234;

  [[nodiscard]] static inline auto GetHash(const T &item, const uint32_t seed) -> uint32_t {
    if constexpr (std::is_integral_v<T> || std::is_enum_v<T>)
      return murmur_hash2_a(&item, sizeof(T), seed);
    else if constexpr (std::is_same_v<T, std::string>)
      return murmur_hash2_a(item.c_str(), item.size(), seed);
    else if constexpr (std::is_same_v<T, const char *>)
      return murmur_hash2_a(item, std::strlen(item), seed);
    else
      return std::hash<T>{}(item);
  }
  /* Modified end */

  static constexpr uint32_t FINGERPRINT_MASK = LOWER_BITS_MASK_32(BITS_PER_TAG);

  uint32_t k_init_table_bits;
  uint32_t num_seg_bits_;

  uint32_t num_table_bits_;

  std::vector<Segment *> hash_table_;

  uint32_t split_condition_;

  uint32_t next_split_idx_;
  uint32_t num_items_;

  [[nodiscard]] inline auto BucketIndexHash(uint32_t hash) const -> uint32_t {
    return hash & ((1 << BUCKETS_PER_SEG_POWER) - 1);
  }

  [[nodiscard]] inline auto SegIndexHash(uint32_t hash) const -> uint32_t {
    return hash & ((1 << num_seg_bits_) - 1);
  }

  [[nodiscard]] inline auto TagHash(uint32_t tag) const -> uint32_t {
    return tag & FINGERPRINT_MASK;
  }

  inline void GenerateIndexTagHash(const T &item, uint32_t &seg_index, uint32_t &bucket_index,
                                   uint32_t &tag) const {
    const uint32_t hash = GetHash(item, HASH_SEED);

    bucket_index = BucketIndexHash(hash);
    seg_index = SegIndexHash(hash >> BUCKETS_PER_SEG_POWER);
    tag = TagHash(hash >> k_init_table_bits);

    if (!(tag)) {
      if (num_table_bits_ > k_init_table_bits) {
        seg_index |= (1 << (k_init_table_bits - BUCKETS_PER_SEG_POWER));
      }
      tag++;
    }

    if (seg_index >= hash_table_.size()) {
      seg_index = seg_index - (1 << (num_seg_bits_ - 1));
    }
  }

public:
  /* Modified start: Benchmarking */
  double total_expansion_time = 0.0;
  /* Modified end: Benchmarking */

  BambooFilter(const BambooFilter &) = default;
  BambooFilter(BambooFilter &&) = default;
  auto operator=(const BambooFilter &) -> BambooFilter & = default;
  auto operator=(BambooFilter &&) -> BambooFilter & = default;

  BambooFilter(uint32_t capacity, uint32_t split_condition_param)
      : k_init_table_bits(
            static_cast<uint32_t>(std::ceil(std::log2(static_cast<double>(capacity) / 4)))),
        num_table_bits_(k_init_table_bits),
        num_seg_bits_(k_init_table_bits - BUCKETS_PER_SEG_POWER) {
    for (int num_segment = 0; num_segment < (1 << num_seg_bits_); num_segment++) {
      hash_table_.push_back(new Segment(1 << BUCKETS_PER_SEG_POWER));
    }

    split_condition_ = uint32_t(split_condition_param * 4 * (1 << BUCKETS_PER_SEG_POWER)) - 1;
    next_split_idx_ = 0;
    num_items_ = 0;
  }

  ~BambooFilter() {
    for (auto &segment : hash_table_) {
      delete segment;
    }
  }

  auto Insert(const T &item) -> bool {
    uint32_t seg_index, bucket_index, tag;

    GenerateIndexTagHash(item, seg_index, bucket_index, tag);

    hash_table_[seg_index]->Insert(bucket_index, tag);

    num_items_++;

    if (!(num_items_ & split_condition_)) {
      Extend();
    }

    return true;
  }

  auto Lookup(const T &item) -> bool {
    uint32_t seg_index, bucket_index, tag;

    /* Modified start: Benchmarking */
    const double start = get_current_time_in_seconds();
    /* Modified end: Benchmarking */
    GenerateIndexTagHash(item, seg_index, bucket_index, tag);
    /* Modified start: Benchmarking */
    total_addressing_time_ += get_current_time_in_seconds() - start;
    /* Modified end: Benchmarking */
    return hash_table_[seg_index]->Lookup(bucket_index, tag);
  }

  auto Delete(const T &key) -> bool {
    uint32_t seg_index, bucket_index, tag;
    GenerateIndexTagHash(key, seg_index, bucket_index, tag);

    if (hash_table_[seg_index]->Delete(bucket_index, tag)) {
      num_items_--;
      /* Modified start: Disable compress for benchmark, since we do not implement it in DFF */
      // if (!(num_items_ & split_condition_)) {
      //   Compress();
      // }
      /* Modified end: Disable compress for benchmark, since we do not implement it in DFF */
      return true;
    }

    return false;
  }

  void Extend() {
    /* Modified start: Benchmarking */
    const double start = get_current_time_in_seconds();
    /* Modified end: Benchmarking */

    Segment *src = hash_table_[next_split_idx_];
    Segment *dst = new Segment(*src);
    hash_table_.push_back(dst);

    num_seg_bits_ = (uint32_t)ceil(log2((double)hash_table_.size()));
    num_table_bits_ = num_seg_bits_ + BUCKETS_PER_SEG_POWER;

    const uint32_t actv_tag_bit = num_table_bits_ - k_init_table_bits;
    src->EraseEle(true, actv_tag_bit - 1);
    dst->EraseEle(false, actv_tag_bit - 1);

    next_split_idx_++;
    if (next_split_idx_ == (1UL << (num_seg_bits_ - 1))) {
      next_split_idx_ = 0;
    }

    /* Modified start: Benchmarking */
    total_expansion_time += get_current_time_in_seconds() - start;
    /* Modified end: Benchmarking */
  }

  void Compress() {
    num_seg_bits_ = (uint32_t)ceil(log2((double)(hash_table_.size() - 1)));
    num_table_bits_ = num_seg_bits_ + BUCKETS_PER_SEG_POWER;
    if (!next_split_idx_) {
      next_split_idx_ = (1UL << (num_seg_bits_ - 1));
    }
    next_split_idx_--;

    Segment *src = hash_table_[next_split_idx_];
    Segment *dst = hash_table_.back();
    src->Absorb(dst);
    delete dst;
    hash_table_.pop_back();
  }

  /* Modified start */
  [[nodiscard]] auto size() const -> size_t {
    size_t num = 0;
    for (const auto &i : hash_table_) {
      num += i->size();
    }
    return num;
  }

  [[nodiscard]] auto average_chain_size() const -> double {
    double total = 0;
    for (const auto &segment : hash_table_)
      total += static_cast<double>(segment->chain_size());
    return total / hash_table_.size();
  }

  [[nodiscard]] auto total_addressing_time() const -> double {
    double total = total_addressing_time_;
    for (auto *segment : hash_table_)
      total += segment->total_addressing_time;
    return total;
  }
  /* Modified end */
};

} // namespace bamboofilter
