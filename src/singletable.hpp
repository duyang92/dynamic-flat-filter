/*
 * Modified from Cuckoo Filter implementation
 * https://github.com/efficient/cuckoofilter/blob/master/src/singletable.h
 */

#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>

#include "predefine.hpp"

namespace dff {

template <bool ENABLE_FINGERPRINT_GROWTH> class SingleTable {
  /**
   * @brief Bits per tag. When `ENABLE_FINGERPRINT_GROWTH` is true, the actual bits per tag is
   * `k_bits_per_tag + 1`.
   */
  size_t k_bits_per_tag_;
  uint32_t k_bits_to_shift_used_by_gen_tag_;

  uint8_t *data_;
  size_t num_buckets_;

  /**
   * @brief Whether the hash (must be a 32-bit uint hash) matches the tag (fingerprint of several
   * high bits of the hash).
   *
   * @param hash The hash to check (must be a 32-bit uint hash).
   * @param tag The tag to check (fingerprint of several high bits of the hash).
   * @return True if the hash matches the tag.
   */
  [[nodiscard]] auto matches_tag(const uint32_t hash, const uint32_t tag) const -> bool {
    if constexpr (ENABLE_FINGERPRINT_GROWTH) {
      const auto to_shift = __builtin_ctz(tag) + 1;
      const auto remain = k_bits_per_tag_ + 1 - to_shift;
      return (hash >> (32 - remain)) == (tag >> to_shift);
    } else {
      return gen_tag(hash) == tag;
    }
  }

  [[nodiscard]] auto read_bits(const size_t from, const size_t length) const -> uint32_t {
    const size_t from_byte = from >> 3;
    const size_t from_bit = from & 7;
    const size_t total_bits = from_bit + length;
    const size_t num_bytes = (total_bits + 7) >> 3;

    uint64_t buffer = 0;
    if (num_bytes == 1)
      buffer = data_[from_byte];
    else if (num_bytes == 2)
      buffer = ((uint64_t)data_[from_byte]) | ((uint64_t)data_[from_byte + 1] << 8);
    else if (num_bytes == 3)
      buffer = ((uint64_t)data_[from_byte]) | ((uint64_t)data_[from_byte + 1] << 8) |
               ((uint64_t)data_[from_byte + 2] << 16);
    else if (num_bytes == 4)
      buffer = ((uint64_t)data_[from_byte]) | ((uint64_t)data_[from_byte + 1] << 8) |
               ((uint64_t)data_[from_byte + 2] << 16) | ((uint64_t)data_[from_byte + 3] << 24);
    else
      buffer = ((uint64_t)data_[from_byte]) | ((uint64_t)data_[from_byte + 1] << 8) |
               ((uint64_t)data_[from_byte + 2] << 16) | ((uint64_t)data_[from_byte + 3] << 24) |
               ((uint64_t)data_[from_byte + 4] << 32);

    buffer >>= from_bit;
    return buffer & ((1ULL << length) - 1ULL);
  }

  void write_bits(const size_t from, const size_t length, const uint32_t bits) {
    const size_t from_byte = from >> 3;
    const size_t from_bit = from & 7;
    const size_t total_bits = from_bit + length;
    const size_t num_bytes = (total_bits + 7) >> 3;

    uint64_t buffer = 0;
    if (num_bytes == 1) {
      buffer = data_[from_byte];
    } else if (num_bytes == 2) {
      buffer = ((uint64_t)data_[from_byte]) | ((uint64_t)data_[from_byte + 1] << 8);
    } else if (num_bytes == 3) {
      buffer = ((uint64_t)data_[from_byte]) | ((uint64_t)data_[from_byte + 1] << 8) |
               ((uint64_t)data_[from_byte + 2] << 16);
    } else if (num_bytes == 4) {
      buffer = ((uint64_t)data_[from_byte]) | ((uint64_t)data_[from_byte + 1] << 8) |
               ((uint64_t)data_[from_byte + 2] << 16) | ((uint64_t)data_[from_byte + 3] << 24);
    } else {
      buffer = ((uint64_t)data_[from_byte]) | ((uint64_t)data_[from_byte + 1] << 8) |
               ((uint64_t)data_[from_byte + 2] << 16) | ((uint64_t)data_[from_byte + 3] << 24) |
               ((uint64_t)data_[from_byte + 4] << 32);
    }

    const uint64_t mask = ((1ULL << length) - 1ULL) << from_bit;
    buffer = (buffer & ~mask) | (((uint64_t)bits << from_bit) & mask);

    if (num_bytes == 1) {
      data_[from_byte] = buffer & 0xff;
    } else if (num_bytes == 2) {
      data_[from_byte] = buffer & 0xff;
      data_[from_byte + 1] = (buffer >> 8) & 0xff;
    } else if (num_bytes == 3) {
      data_[from_byte] = buffer & 0xff;
      data_[from_byte + 1] = (buffer >> 8) & 0xff;
      data_[from_byte + 2] = (buffer >> 16) & 0xff;
    } else if (num_bytes == 4) {
      data_[from_byte] = buffer & 0xff;
      data_[from_byte + 1] = (buffer >> 8) & 0xff;
      data_[from_byte + 2] = (buffer >> 16) & 0xff;
      data_[from_byte + 3] = (buffer >> 24) & 0xff;
    } else {
      data_[from_byte] = buffer & 0xff;
      data_[from_byte + 1] = (buffer >> 8) & 0xff;
      data_[from_byte + 2] = (buffer >> 16) & 0xff;
      data_[from_byte + 3] = (buffer >> 24) & 0xff;
      data_[from_byte + 4] = (buffer >> 32) & 0xff;
    }
  }

public:
  SingleTable(const SingleTable &) = default;
  SingleTable(SingleTable &&) = default;
  auto operator=(const SingleTable &) -> SingleTable & = default;
  auto operator=(SingleTable &&) -> SingleTable & = default;

  /**
   * @brief Create a new single table.
   *
   * @param num_buckets Bucket count.
   * @param bits_per_tag Bits per tag. When `ENABLE_FINGERPRINT_GROWTH` is true, the actual bits per
   * tag is `bits_per_tag + 1`.
   */
  explicit SingleTable(const size_t num_buckets, const size_t bits_per_tag)
      : num_buckets_(num_buckets), k_bits_per_tag_(bits_per_tag),
        k_bits_to_shift_used_by_gen_tag_(32UZ - bits_per_tag) {
    size_t total_size;
    if constexpr (ENABLE_FINGERPRINT_GROWTH)
      total_size = (num_buckets * SLOTS_PER_BUCKET * (bits_per_tag + 1) + 7) >> 3;
    else
      total_size = (num_buckets * SLOTS_PER_BUCKET * (bits_per_tag) + 7) >> 3;
    total_size = (total_size + 7) & ~7; // Add padding for 8-byte alignment
    data_ = new uint8_t[total_size];
    memset(data_, 0, total_size);
  }

  ~SingleTable() {
    delete[] data_;
    data_ = nullptr;
  }

  [[nodiscard]] auto gen_tag(const uint32_t hash) const -> uint32_t {
    if constexpr (ENABLE_FINGERPRINT_GROWTH) {
      return ((hash >> k_bits_to_shift_used_by_gen_tag_) << 1) | 1;
    } else {
      uint32_t tag = hash >> k_bits_to_shift_used_by_gen_tag_;
      // Avoid tag 0
      if (tag == 0)
        tag = 1;
      return tag;
    }
  }

  /**
   * @brief Read tag from a bucket slot. Does not handle unary mask (i.e., just read the raw tag).
   *
   * @param bucket The index of the bucket.
   * @param slot The index of the tag in the bucket (slot index).
   * @return The tag.
   */
  [[nodiscard]] auto read_tag(const size_t bucket, const size_t slot) const -> uint32_t {
    if constexpr (ENABLE_FINGERPRINT_GROWTH) {
      const size_t from = (bucket * SLOTS_PER_BUCKET + slot) * (k_bits_per_tag_ + 1);
      return read_bits(from, k_bits_per_tag_ + 1);
    } else {
      const size_t from = (bucket * SLOTS_PER_BUCKET + slot) * k_bits_per_tag_;
      return read_bits(from, k_bits_per_tag_);
    }
  }

  /**
   * @brief Write tag to a bucket slot. Does not handle unary mask (i.e., just write the raw tag).
   *
   * @param bucket The index of the bucket.
   * @param slot The index of the tag in the bucket (slot index).
   * @param tag The tag to write.
   */
  void write_tag(const size_t bucket, const size_t slot, const uint32_t tag) {
    if constexpr (ENABLE_FINGERPRINT_GROWTH) {
      const size_t from = (bucket * SLOTS_PER_BUCKET + slot) * (k_bits_per_tag_ + 1);
      write_bits(from, k_bits_per_tag_ + 1, tag);
    } else {
      const size_t from = (bucket * SLOTS_PER_BUCKET + slot) * k_bits_per_tag_;
      write_bits(from, k_bits_per_tag_, tag);
    }
  }

  /**
   * @brief Remove the tag from the bucket slot (write 0 to the slot).
   *
   * @param bucket The index of the bucket.
   * @param slot The index of the tag in the bucket (slot index).
   */
  void remove_tag(const size_t bucket, const size_t slot) { write_tag(bucket, slot, 0); }

  /**
   * @brief Find if any slot in the two buckets contains the tag that matches the hash.
   *
   * @param bucket1 The index of the first bucket.
   * @param bucket2 The index of the second bucket.
   * @param hash The hash to match (must be a 32-bit uint hash).
   * @return True if find a tag in any slot of one of the two buckets that
   * matches the hash.
   */
  [[nodiscard]] auto match_hash_in_buckets(const size_t bucket1, const size_t bucket2,
                                           const uint32_t hash) const -> bool {
    for (size_t slot = 0; slot < SLOTS_PER_BUCKET; slot++)
      if (matches_tag(hash, read_tag(bucket1, slot)) || matches_tag(hash, read_tag(bucket2, slot)))
        return true;
    return false;
  }
  /**
   * @brief Find if the tag exists in any slot of the two buckets.
   *
   * @param bucket1 The index of the first bucket.
   * @param bucket2 The index of the second bucket.
   * @param tag The tag to find.
   * @return True if the tag exists in any slot of one of the two buckets.
   */
  [[nodiscard]] auto find_tag_in_buckets(const size_t bucket1, const size_t bucket2,
                                         const uint32_t tag) const -> bool {
    for (size_t slot = 0; slot < SLOTS_PER_BUCKET; slot++)
      if (read_tag(bucket1, slot) == tag || read_tag(bucket2, slot) == tag)
        return true;
    return false;
  }

  /**
   * @brief Find if any slot in the bucket contains the tag that matches the hash.
   *
   * @param bucket The index of the bucket.
   * @param hash The hash to match (must be a 32-bit uint hash).
   * @return True if find a tag in any slot of the bucket that matches the hash.
   */
  [[nodiscard]] auto match_hash_in_bucket(const size_t bucket, const uint32_t hash) const -> bool {
    for (size_t slot = 0; slot < SLOTS_PER_BUCKET; slot++)
      if (matches_tag(hash, read_tag(bucket, slot)))
        return true;
    return false;
  }
  /**
   * @brief Find if exists a tag in any slot of the bucket.
   *
   * @param bucket The index of the bucket.
   * @param tag The tag to find.
   * @return True if find a tag in any slot of the bucket.
   */
  [[nodiscard]] auto find_tag_in_bucket(const size_t bucket, const uint32_t tag) const -> bool {
    for (size_t slot = 0; slot < SLOTS_PER_BUCKET; slot++)
      if (read_tag(bucket, slot) == tag)
        return true;
    return false;
  }

  /**
   * @brief Remove the hash from the bucket (write 0 to the slot).
   *
   * @param bucket The index of the bucket.
   * @param hash The hash to remove (must be a 32-bit uint hash).
   * @return True if the hash is removed successfully.
   */
  auto remove_hash_from_buckets(const size_t bucket1, size_t bucket2, const uint32_t hash) -> bool {
    if constexpr (ENABLE_FINGERPRINT_GROWTH) {
      // Should always remove the tag with longest fingerprint, to avoid false
      // negative. This is a very rare case, but it is necessary to handle it,
      // and consequently, we have to check all slots in the bucket, which is
      // not quite efficient, but unavoidable.
      uint32_t matched_tags_bucket1[SLOTS_PER_BUCKET] = {};
      uint32_t matched_tags_bucket2[SLOTS_PER_BUCKET] = {};
      // These 3 variables are used to speed up the removal process when 0 or 1
      // tag is matched.
      size_t matched_count = 0;
      size_t last_matched_bucket = 0;
      size_t last_matched_slot = 0;
      // Match the hash in both buckets
      for (size_t slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
        uint32_t tag = read_tag(bucket1, slot);
        if (tag != 0 && matches_tag(hash, tag)) {
          assert(match_hash_in_bucket(bucket1, hash));
          matched_tags_bucket1[slot] = tag;
          matched_count++;
          last_matched_bucket = bucket1;
          last_matched_slot = slot;
        }
        tag = read_tag(bucket2, slot);
        if (tag != 0 && matches_tag(hash, tag)) {
          assert(match_hash_in_bucket(bucket2, hash));
          matched_tags_bucket2[slot] = tag;
          matched_count++;
          last_matched_bucket = bucket2;
          last_matched_slot = slot;
        }
      }

      // If no tag is matched, return false
      if (matched_count == 0)
        return false;
      // If only one tag is matched, remove it
      if (matched_count == 1) {
        remove_tag(last_matched_bucket, last_matched_slot);
        return true;
      }

      // If multiple tags are matched, remove the tag with longest fingerprint
      size_t longest_fingerprint_bucket = 0;
      size_t longest_fingerprint_slot = 0;
      size_t lowest_tz = -1UZ;
      // Check bucket1
      for (size_t slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
        const uint32_t tag = matched_tags_bucket1[slot];
        if (tag == 0)
          continue;
        const size_t tz = __builtin_ctz(tag);
        if (tz < lowest_tz) {
          lowest_tz = tz;
          longest_fingerprint_bucket = bucket1;
          longest_fingerprint_slot = slot;
        }
      }
      // Check bucket2
      for (size_t slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
        const uint32_t tag = matched_tags_bucket2[slot];
        if (tag == 0)
          continue;
        const size_t tz = __builtin_ctz(tag);
        if (tz < lowest_tz) {
          lowest_tz = tz;
          longest_fingerprint_bucket = bucket2;
          longest_fingerprint_slot = slot;
        }
      }
      // spdlog::info("Hash 0b{:032b} matched multiple ({}) tags in bucket {}
      // and {}", hash,
      //              matched_count, bucket1, bucket2);
      // for (size_t slot = 0; slot < SLOTS_PER_BUCKET; slot++)
      //   if (matched_tags_bucket1[slot] != 0)
      //     spdlog::info("[Bucket {}] Matched tag at slot {}: 0b{:032b} (tz:
      //     {})", bucket1, slot,
      //                  matched_tags_bucket1[slot],
      //                  __builtin_ctz(matched_tags_bucket1[slot]));
      // for (size_t slot = 0; slot < SLOTS_PER_BUCKET; slot++)
      //   if (matched_tags_bucket2[slot] != 0)
      //     spdlog::info("[Bucket {}] Matched tag at slot {}: 0b{:032b} (tz:
      //     {})", bucket2, slot,
      //                  matched_tags_bucket2[slot],
      //                  __builtin_ctz(matched_tags_bucket2[slot]));
      // spdlog::info("Remove the tag with longest fingerprint in slot {} in
      // bucket {}",
      //              longest_fingerprint_slot, longest_fingerprint_bucket);
      // Remove the tag with longest fingerprint
      remove_tag(longest_fingerprint_bucket, longest_fingerprint_slot);
      return true;
    } else {
      for (size_t slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
        if (matches_tag(hash, read_tag(bucket1, slot))) {
          assert(match_hash_in_bucket(bucket1, hash));
          remove_tag(bucket1, slot);
          return true;
        }
        if (matches_tag(hash, read_tag(bucket2, slot))) {
          assert(match_hash_in_bucket(bucket2, hash));
          remove_tag(bucket2, slot);
          return true;
        }
      }
      return false;
    }
  }
  /**
   * @brief Remove the tag from the bucket (write 0 to the slot).
   *
   * @param bucket The index of the bucket.
   * @param hash The tag to remove.
   * @return True if the tag is removed successfully.
   */
  auto remove_tag_from_bucket(const size_t bucket, const uint32_t tag) -> bool {
    for (size_t slot = 0; slot < SLOTS_PER_BUCKET; slot++)
      if (read_tag(bucket, slot) == tag) {
        assert(find_tag_in_bucket(bucket, tag));
        write_tag(bucket, slot, 0);
        return true;
      }
    return false;
  }

  /**
   * @brief Insert the tag to the bucket. If `kickout` is true, replace a random tag in the bucket
   * if the bucket is full, and `old_tag` is the tag evicted.
   *
   * @param bucket The index of the bucket.
   * @param tag The tag to insert.
   * @param kickout If true, replace a random tag in the bucket if the bucket is
   * full.
   * @param old_tag The tag evicted if the bucket is full.
   * @return True if the tag is inserted successfully.
   */
  auto insert_tag_to_bucket(const size_t bucket, const uint32_t tag, const bool kickout,
                            uint32_t &old_tag) -> bool {
    for (size_t slot = 0; slot < SLOTS_PER_BUCKET; slot++)
      if (read_tag(bucket, slot) == 0) {
        write_tag(bucket, slot, tag);
        return true;
      }

    if (kickout) {
      const size_t slot = std::rand() % SLOTS_PER_BUCKET;
      old_tag = read_tag(bucket, slot);
      write_tag(bucket, slot, tag);
    }

    return false;
  }

  /**
   * @brief Count the number of tags in a bucket.
   *
   * @param bucket The index of the bucket.
   * @return The number of tags in the bucket.
   */
  [[nodiscard]] auto count_tags_in_bucket(const size_t bucket) const -> size_t {
    size_t count = 0;
    for (size_t slot = 0; slot < SLOTS_PER_BUCKET; slot++)
      if (read_tag(bucket, slot) != 0)
        count++;
    return count;
  }
};
} // namespace dff
