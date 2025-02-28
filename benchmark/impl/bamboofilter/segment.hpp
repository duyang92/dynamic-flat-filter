#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>

#include "../../../src/utils/bits.hpp"
#include "predefine.hpp"

namespace bamboofilter {

class Segment {
private:
  static constexpr size_t K_MAX_KICK_COUNT = 500;

  static const uint32_t kTagsPerBucket = 4;
  static const uint32_t kBytesPerBucket = (BITS_PER_TAG * kTagsPerBucket + 7) >> 3;

  static const uint32_t kTagMask = (1ULL << BITS_PER_TAG) - 1;
  static const uint64_t kBucketMask = LOWER_BITS_MASK_64(BITS_PER_TAG * kTagsPerBucket);
  static const uint64_t kBucketClearMask = ~kBucketMask;

  static const uint32_t bucket_size = (BITS_PER_TAG * kTagsPerBucket + 7) / 8; // kBytesPerBucket
  static const uint32_t safe_pad = sizeof(uint64_t) - bucket_size;
  static const uint32_t safe_pad_simd = 4; // 4B for avx2

private:
  char *temp;
  const uint32_t chain_num;
  uint32_t chain_capacity;
  uint32_t total_size;
  uint32_t insert_cur;
  char *data_base;

  static uint32_t IndexHash(uint32_t index) { return index & ((1 << BUCKETS_PER_SEG_POWER) - 1); }
  static uint32_t AltIndex(size_t index, uint32_t tag) {
    return IndexHash((uint32_t)(index ^ tag));
  }

  static void WriteTag(char *p, uint32_t idx, uint32_t tag) {
    uint32_t t = tag & kTagMask;
    if constexpr (BITS_PER_TAG == 8) {
      // 8 bits: each tag occupies 1 byte
      p += idx;
      ((uint8_t *)p)[0] = t;
    } else if constexpr (BITS_PER_TAG == 12) {
      // 12 bits: each tag occupies 1.5 bytes, handle even and odd index
      p += (idx + (idx >> 1));        // idx + (idx / 2)
      if ((idx & 1) == 0) {           // Even index
        ((uint16_t *)p)[0] &= 0xf000; // Clear the lower 12 bits
        ((uint16_t *)p)[0] |= t;
      } else {                        // Odd index
        ((uint16_t *)p)[0] &= 0x000f; // Clear the upper 12 bits
        ((uint16_t *)p)[0] |= (t << 4);
      }
    } else if constexpr (BITS_PER_TAG == 16) {
      // 16 bits: each tag occupies 2 bytes
      p += (idx << 1); // idx * 2
      ((uint16_t *)p)[0] = t;
    } else if constexpr (BITS_PER_TAG == 20) {
      // 20 bits: each tag occupies 2.5 bytes, handle even and odd index
      p += (idx + (idx << 1));            // idx * 2 + (idx / 2) equivalent to idx + (idx << 1)
      if ((idx & 1) == 0) {               // Even index
        ((uint32_t *)p)[0] &= 0xfff00000; // Clear the lower 20 bits
        ((uint32_t *)p)[0] |= t;
      } else {                            // Odd index
        ((uint32_t *)p)[0] &= 0x000fffff; // Clear the upper 20 bits
        ((uint32_t *)p)[0] |= (t << 20);
      }
    } else if constexpr (BITS_PER_TAG == 24) {
      // 24 bits: each tag occupies 3 bytes
      p += (idx + (idx << 1)); // idx * 3
      ((uint8_t *)p)[0] = t & 0xFF;
      ((uint8_t *)p)[1] = (t >> 8) & 0xFF;
      ((uint8_t *)p)[2] = (t >> 16) & 0xFF;
    } else if constexpr (BITS_PER_TAG == 28) {
      // 28 bits: each tag occupies 3.5 bytes, handle even and odd index
      p += (idx * 3 + (idx >> 1));        // idx * 3 + (idx / 2)
      if ((idx & 1) == 0) {               // Even index
        ((uint32_t *)p)[0] &= 0xf0000000; // Clear the lower 28 bits
        ((uint32_t *)p)[0] |= t;
      } else {                            // Odd index
        ((uint32_t *)p)[0] &= 0x0fffffff; // Clear the upper 28 bits
        ((uint32_t *)p)[0] |= (t << 28);
      }
    } else if constexpr (BITS_PER_TAG == 32) {
      // 32 bits: each tag occupies 4 bytes
      p += (idx << 2); // idx * 4
      ((uint32_t *)p)[0] = t;
    }
  }

  static uint32_t ReadTag(const char *p, uint32_t idx) {
    uint32_t tag;
    if constexpr (BITS_PER_TAG == 8) {
      // 8 bits: each tag occupies 1 byte
      p += idx;
      tag = *((uint8_t *)p);
    } else if constexpr (BITS_PER_TAG == 12) {
      // 12 bits: each tag occupies 1.5 bytes, handle even and odd index
      p += idx + (idx >> 1);                      // idx + (idx / 2)
      tag = *((uint16_t *)p) >> ((idx & 1) << 2); // Shift right by 4 bits for odd index
    } else if constexpr (BITS_PER_TAG == 16) {
      // 16 bits: each tag occupies 2 bytes
      p += (idx << 1); // idx * 2
      tag = *((uint16_t *)p);
    } else if constexpr (BITS_PER_TAG == 20) {
      // 20 bits: each tag occupies 2.5 bytes, handle even and odd index
      p += (idx + (idx << 1)); // idx * 2 + (idx / 2) equivalent to idx + (idx << 1)
      tag = *((uint32_t *)p);
      if (idx & 1)    // Odd index
        tag >>= 20;   // Shift right by 20 bits
      tag &= 0xFFFFF; // Mask to get the lower 20 bits
    } else if constexpr (BITS_PER_TAG == 24) {
      // 24 bits: each tag occupies 3 bytes
      p += (idx + (idx << 1)); // idx * 3
      tag = (((uint32_t)((uint8_t *)p)[0]) | ((uint32_t)((uint8_t *)p)[1] << 8) |
             ((uint32_t)((uint8_t *)p)[2] << 16));
    } else if constexpr (BITS_PER_TAG == 28) {
      // 28 bits: each tag occupies 3.5 bytes, handle even and odd index
      p += (idx * 3 + (idx >> 1)); // idx * 3 + (idx / 2)
      tag = *((uint32_t *)p);
      if (idx & 1) { // Odd index
        tag >>= 28;  // Shift right by 28 bits
      }
      tag &= 0xFFFFFFF; // Mask to get the lower 28 bits
    } else if constexpr (BITS_PER_TAG == 32) {
      // 32 bits: each tag occupies 4 bytes
      p += (idx << 2); // idx * 4
      tag = *((uint32_t *)p);
    }
    return tag & kTagMask;
  }

  static bool RemoveOnCondition(const char *p, uint32_t idx, uint32_t old_tag) {
    uint32_t tag;
    if constexpr (BITS_PER_TAG == 8) {
      // 8 bits: each tag occupies 1 byte
      p += idx;
      tag = *((uint8_t *)p) & kTagMask;
      if (old_tag != tag) {
        return false;
      }
      *((uint8_t *)p) &= ~kTagMask;
    } else if constexpr (BITS_PER_TAG == 12) {
      // 12 bits: each tag occupies 1.5 bytes, handle even and odd index
      p += idx + (idx >> 1); // idx + (idx / 2)
      tag =
          (*((uint16_t *)p) >> ((idx & 1) << 2)) & kTagMask; // Shift right by 4 bits for odd index
      if (old_tag != tag) {
        return false;
      }
      if ((idx & 1) == 0) {           // Even index
        ((uint16_t *)p)[0] &= 0xf000; // Clear the lower 12 bits
      } else {                        // Odd index
        ((uint16_t *)p)[0] &= 0x000f; // Clear the upper 12 bits
      }
    } else if constexpr (BITS_PER_TAG == 16) {
      // 16 bits: each tag occupies 2 bytes
      p += (idx << 1); // idx * 2
      tag = *((uint16_t *)p) & kTagMask;
      if (old_tag != tag) {
        return false;
      }
      *((uint16_t *)p) &= ~kTagMask;
    } else if constexpr (BITS_PER_TAG == 20) {
      // 20 bits: each tag occupies 2.5 bytes, handle even and odd index
      p += (idx + (idx << 1)); // idx * 2 + (idx / 2) equivalent to idx + (idx << 1)
      tag =
          (*((uint32_t *)p) >> ((idx & 1) * 20)) & 0xFFFFF; // Shift right by 20 bits for odd index
      if (old_tag != tag) {
        return false;
      }
      if (idx & 1) {                      // Odd index
        ((uint32_t *)p)[0] &= 0x000FFFFF; // Clear the upper 20 bits
      } else {                            // Even index
        ((uint32_t *)p)[0] &= 0xFFF00000; // Clear the lower 20 bits
      }
    } else if constexpr (BITS_PER_TAG == 24) {
      // 24 bits: each tag occupies 3 bytes
      p += (idx + (idx << 1)); // idx * 3
      tag = (((uint32_t)((uint8_t *)p)[0]) | ((uint32_t)((uint8_t *)p)[1] << 8) |
             ((uint32_t)((uint8_t *)p)[2] << 16)) &
            kTagMask;
      if (old_tag != tag) {
        return false;
      }
      ((uint8_t *)p)[0] = 0;
      ((uint8_t *)p)[1] = 0;
      ((uint8_t *)p)[2] = 0;
    } else if constexpr (BITS_PER_TAG == 28) {
      // 28 bits: each tag occupies 3.5 bytes, handle even and odd index
      p += (idx * 3 + (idx >> 1)); // idx * 3 + (idx / 2)
      tag = (*((uint32_t *)p) >> ((idx & 1) * 28)) &
            0xFFFFFFF; // Shift right by 28 bits for odd index
      if (old_tag != tag) {
        return false;
      }
      if (idx & 1) {                      // Odd index
        ((uint32_t *)p)[0] &= 0x0FFFFFFF; // Clear the upper 28 bits
      } else {                            // Even index
        ((uint32_t *)p)[0] &= 0xF0000000; // Clear the lower 28 bits
      }
    } else if constexpr (BITS_PER_TAG == 32) {
      // 32 bits: each tag occupies 4 bytes
      p += (idx << 2); // idx * 4
      tag = *((uint32_t *)p) & kTagMask;
      if (old_tag != tag) {
        return false;
      }
      *((uint32_t *)p) &= ~kTagMask;
    }
    return true;
  }

  static bool DeleteTag(char *p, uint32_t tag) {
    for (size_t tag_idx = 0; tag_idx < kTagsPerBucket; tag_idx++) {
      if (RemoveOnCondition(p, tag_idx, tag)) {
        return true;
      }
    }
    return false;
  }

  static void doErase(char *p, bool is_src, uint32_t actv_bit) {
    uint64_t v = (*(uint64_t *)p) & kBucketMask;

    ((uint64_t *)p)[0] &= kBucketClearMask;
    ((uint64_t *)p)[0] |= is_src ? ll_isl(v, actv_bit) : ll_isn(v, actv_bit);
  }

public:
  /* Modified start: Benchmarking */
  mutable double total_addressing_time = 0.0;
  /* Modified end: Benchmarking */

  Segment(const uint32_t chain_num) : chain_num(chain_num), chain_capacity(1), insert_cur(0) {
    total_size = chain_num * chain_capacity * bucket_size + safe_pad;
    data_base = new char[total_size];
    memset(data_base, 0, (chain_num * chain_capacity * bucket_size));
    temp =
        new char[safe_pad_simd + (2 * chain_capacity * bucket_size + 23) / 24 * 24 + safe_pad_simd];
  }

  Segment(const Segment &s)
      : chain_num(s.chain_num), chain_capacity(s.chain_capacity), total_size(s.total_size),
        insert_cur(0) {
    data_base = new char[total_size];
    temp =
        new char[safe_pad_simd + (2 * chain_capacity * bucket_size + 23) / 24 * 24 + safe_pad_simd];
    memcpy(data_base, s.data_base, total_size);
  }

  ~Segment() {
    delete[] data_base;
    delete[] temp;
  };

  bool Insert(uint32_t chain_idx, uint32_t curtag) {
    char *bucket_p;
    for (uint32_t count = 0; count < K_MAX_KICK_COUNT; count++) {
      bucket_p = data_base + (chain_idx * chain_capacity + insert_cur) * bucket_size;
      bool kickout = count > 0;

      for (size_t tag_idx = 0; tag_idx < kTagsPerBucket; tag_idx++) {
        if (0 == ReadTag(bucket_p, tag_idx)) {
          WriteTag(bucket_p, tag_idx, curtag);
          return true;
        }
      }

      if (kickout) {
        size_t tag_idx = rand() % kTagsPerBucket;
        uint32_t oldtag = ReadTag(bucket_p, tag_idx);
        WriteTag(bucket_p, tag_idx, curtag);
        curtag = oldtag;
      }
      chain_idx = AltIndex(chain_idx, curtag);
      bucket_p = data_base + (chain_idx * chain_capacity + insert_cur) * bucket_size;
    }

    insert_cur++;
    if (insert_cur >= chain_capacity) {
      char *old_data_base = data_base;
      uint32_t old_chain_len = chain_capacity * bucket_size;
      chain_capacity++;
      uint32_t new_chain_len = chain_capacity * bucket_size;
      delete[] temp;
      temp = new char[safe_pad_simd + (2 * chain_capacity * bucket_size + 23) / 24 * 24 +
                      safe_pad_simd];

      total_size = chain_num * chain_capacity * bucket_size + safe_pad;
      data_base = new char[total_size];
      memset(data_base, 0, total_size);
      for (int i = 0; i < chain_num; i++) {
        memcpy(data_base + i * new_chain_len, old_data_base + i * old_chain_len, old_chain_len);
      }
      delete[] old_data_base;
    }
    return Insert(chain_idx, curtag);
  }

  bool Lookup(uint32_t chain_idx, uint16_t tag) const {
    memcpy(temp + safe_pad_simd, data_base + chain_idx * chain_capacity * bucket_size,
           chain_capacity * bucket_size);
    memcpy(temp + safe_pad_simd + chain_capacity * bucket_size,
           data_base + AltIndex(chain_idx, tag) * chain_capacity * bucket_size,
           chain_capacity * bucket_size);
    char *p = temp + safe_pad_simd;
    char *end = p + 2 * chain_capacity * bucket_size;

    const double start = get_current_time_in_seconds();

    /* Modified start: Lookup without AVX */
    while (p < end) {
      for (size_t i = 0; i < kTagsPerBucket; i++) {
        if (ReadTag(p, i) == tag) {
          total_addressing_time += get_current_time_in_seconds() - start;
          return true;
        }
      }
      p += kBytesPerBucket;
    }
    /* Modified end: Lookup without AVX */

    return false;
  }

  bool Delete(uint32_t chain_idx, uint32_t tag) {
    uint32_t chain_idx2 = AltIndex(chain_idx, tag);
    for (int i = 0; i < chain_capacity; i++) {
      char *p = data_base + (chain_idx * chain_capacity + i) * bucket_size;
      if (DeleteTag(p, tag)) {
        return true;
      }
    }
    for (int i = 0; i < chain_capacity; i++) {
      char *p = data_base + (chain_idx2 * chain_capacity + i) * bucket_size;
      if (DeleteTag(p, tag)) {
        return true;
      }
    }
    return false;
  }

  void EraseEle(bool is_src, uint32_t actv_bit) {
    for (int i = 0; i < chain_num * chain_capacity; i++) {
      char *p = data_base + i * bucket_size;
      doErase(p, is_src, actv_bit);
    }
    insert_cur = 0;
  }

  void Absorb(const Segment *segment) {
    char *p1 = data_base;
    uint32_t len1 = (chain_capacity * bucket_size);
    char *p2 = segment->data_base;
    uint32_t len2 = (segment->chain_capacity * segment->bucket_size);

    chain_capacity += segment->chain_capacity;
    insert_cur = 0;

    total_size = chain_num * chain_capacity * bucket_size + safe_pad;
    data_base = new char[total_size];

    for (int i = 0; i < chain_num; i++) {
      memcpy(data_base + i * (len1 + len2), p1 + i * len1, len1);
      memcpy(data_base + i * (len1 + len2) + len1, p2 + i * len2, len2);
    }
    delete[] p1;
    delete[] temp;
    temp =
        new char[safe_pad_simd + (2 * chain_capacity * bucket_size + 23) / 24 * 24 + safe_pad_simd];
  }

  /* Modified start */
  [[nodiscard]] auto size() const -> size_t { return chain_num * chain_capacity; }
  [[nodiscard]] auto chain_size() const -> size_t { return chain_capacity; }
  /* Modified end */
};

} // namespace bamboofilter
