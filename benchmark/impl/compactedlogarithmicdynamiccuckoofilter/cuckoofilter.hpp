#pragma once

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <iostream>
#include <string>
#include <type_traits>

#include "../../../src/utils/hash.hpp"

namespace compactedlogarithmicdynamiccuckoofilter {

constexpr size_t K_MAX_KICK_COUNT = 500;

using Victim_L = struct Victim_L {
  size_t index;
  uint32_t fingerprint;
  bool used;
};

using Bucket_L = struct Bucket_L {
  char *bit_array;
};

template <typename T> class CuckooFilterL {
public:
  /* Modified start */
  static constexpr uint64_t FP_SEED = 123456;
  static constexpr uint64_t INDEX_SEED = 345678;

  [[nodiscard]] static inline auto hash(const T &item, const uint64_t seed) -> uint64_t {
    if constexpr (std::is_integral_v<T> || std::is_enum_v<T>)
      return murmur_hash2_x64_a(&item, sizeof(T), seed);
    else if constexpr (std::is_same_v<T, std::string>)
      return murmur_hash2_x64_a(item.c_str(), item.size(), seed);
    else if constexpr (std::is_same_v<T, const char *>)
      return murmur_hash2_x64_a(item, std::strlen(item), seed);
    else
      return std::hash<T>{}(item);
  }
  /* Modified end */

  size_t single_table_length;

  size_t bits_per_bucket;
  size_t bytes_per_bucket;

  uint32_t mask;

  void generateFP(const T &item, uint32_t &fingerprint) {
    fingerprint = hash(item, FP_SEED);
    fingerprint = fingerprint & mask;
    if (fingerprint == 0)
      fingerprint += 1;
  }

  void generateIndex(const T &item, size_t &index) {
    index = hash(item, INDEX_SEED) % this->single_table_length;
  }

  void generateA(size_t index, uint32_t fingerprint, size_t &alt_index) {
    alt_index = (index ^ (fingerprint * 0x5bd1e995)) % this->single_table_length;
  }

public:
  size_t capacity;
  Bucket_L *bucket;
  size_t fingerprint_size;
  size_t exact_fingerprint_size;
  bool is_full;
  bool is_empty;
  bool is_null;

  int counter;
  int number;
  int level;
  Victim_L victim;

  CuckooFilterL *lchild;
  CuckooFilterL *rchild;

  /*****************************************
   * construction and destruction function *
   *****************************************/
  CuckooFilterL(const size_t single_table_length, const size_t fingerprint_size,
                const size_t curlevel, const size_t number) {
    this->exact_fingerprint_size = fingerprint_size - curlevel;
    this->fingerprint_size = fingerprint_size;
    this->number = number;
    bits_per_bucket = fingerprint_size * 4;
    bytes_per_bucket = (fingerprint_size * 4 + 7) >> 3;
    this->single_table_length = single_table_length;
    this->counter = 0;
    is_full = false;
    is_empty = true;
    level = curlevel;
    lchild = NULL;
    rchild = NULL;
    mask = (1ULL << this->fingerprint_size) - 1;
    victim.used = false;
    is_null = false;

    bucket = new Bucket_L[this->single_table_length];
    this->capacity = single_table_length * 4 * 0.9;
    for (size_t i = 0; i < this->single_table_length; i++) {
      bucket[i].bit_array = new char[bytes_per_bucket];
      memset(bucket[i].bit_array, 0, bytes_per_bucket);
    }
  }

  CuckooFilterL(CuckooFilterL *old_CF, const size_t number) {
    this->fingerprint_size = old_CF->fingerprint_size - 1;
    this->number = number;
    bits_per_bucket = fingerprint_size * 4;
    bytes_per_bucket = (fingerprint_size * 4 + 7) >> 3;
    this->single_table_length = old_CF->single_table_length;
    counter = 0;
    is_full = false;
    is_empty = true;
    level = old_CF->level + 1;
    mask = (1ULL << fingerprint_size) - 1;
    victim.used = false;
    bucket = new Bucket_L[this->single_table_length];
    for (size_t i = 0; i < this->single_table_length; i++) {
      bucket[i].bit_array = new char[bytes_per_bucket];
      memcpy(bucket[i].bit_array, old_CF->bucket[i].bit_array, bytes_per_bucket);
    }
  }

  ~CuckooFilterL() {
    if (bucket)
      for (size_t i = 0; i < this->single_table_length; i++)
        if (bucket[i].bit_array) {
          delete[] bucket[i].bit_array;
          bucket[i].bit_array = nullptr;
        }

    if (bucket) {
      delete[] bucket;
      bucket = nullptr;
    }
    if (lchild) {
      delete lchild;
      lchild = nullptr;
    }
    if (rchild) {
      delete rchild;
      rchild = nullptr;
    }
  }

  /************************************
   * insert & query & delete function *
   ************************************/
  bool insertItem(const T &item) {
    size_t index, alt_index;
    uint32_t fingerprint;
    generateFP(item, fingerprint);
    generateIndex(item, index);
    for (size_t count = 0; count < K_MAX_KICK_COUNT; count++) {
      bool kickout = (count != 0);
      if (insertImpl(index, fingerprint, kickout, victim)) {
        return true;
      }

      if (kickout) {
        index = victim.index;
        fingerprint = victim.fingerprint;
        generateA(index, fingerprint, alt_index);
        index = alt_index;
      } else {
        generateA(index, fingerprint, alt_index);
        index = alt_index;
      }
    }

    return false;
  }

  bool insertItem(size_t index, uint32_t fingerprint, Victim_L &victim) {
    size_t alt_index;
    uint32_t fp = fingerprint & mask;
    for (size_t count = 0; count < K_MAX_KICK_COUNT; count++) {
      bool kickout = (count != 0);
      if (insertImpl(index, fp, kickout, victim)) {
        // counter++;
        return true;
      }

      if (kickout) {
        index = victim.index;
        fp = victim.fingerprint;
        generateA(index, fp, alt_index);
        index = alt_index;
      } else {
        generateA(index, fp, alt_index);
        index = alt_index;
      }
    }
    return false;
  }

  bool queryItem(const T &item) {
    size_t index, alt_index;
    uint32_t fingerprint;
    generateFP(item, fingerprint);
    uint32_t fp = fingerprint & ((1ULL << this->exact_fingerprint_size) - 1);
    generateIndex(item, index);

    if (queryImpl(index, fp)) {
      return true;
    }
    generateA(index, fingerprint, alt_index);
    if (queryImpl(alt_index, fp)) {
      return true;
    }
    return false;
  }

  bool queryItem(size_t index, uint32_t fingerprint) {
    size_t alt_index;
    // uint32_t fingerprint;
    // generateFP(item, fingerprint);
    uint32_t fp = fingerprint & ((1ULL << this->exact_fingerprint_size) - 1);
    // generateIndex(item, index);

    if (queryImpl(index, fp)) {
      return true;
    }
    generateA(index, fingerprint, alt_index);
    if (queryImpl(alt_index, fp)) {
      return true;
    }
    return false;
  }

  bool deleteItem(const T &item) {
    size_t index, alt_index;
    uint32_t fingerprint;
    generateFP(item, fingerprint);
    generateIndex(item, index);

    if (deleteImpl(index, fingerprint)) {
      return true;
    }
    generateA(index, fingerprint, alt_index);
    if (deleteImpl(alt_index, fingerprint)) {
      return true;
    }

    return false;
  }

  bool deleteItem(size_t index, uint32_t fingerprint) {
    size_t alt_index;

    if (deleteImpl(index, fingerprint)) {
      return true;
    }
    generateA(index, fingerprint, alt_index);
    if (deleteImpl(alt_index, fingerprint)) {
      return true;
    }

    return false;
  }

  bool insertImpl(const size_t index, const uint32_t fingerprint, const bool kickout,
                  Victim_L &victim) {
    for (size_t pos = 0; pos < 4; pos++) {
      if (read(index, pos) == 0) {
        write(index, pos, fingerprint);
        if (this->counter == capacity) {
          this->is_full = true;
        }

        if (this->counter > 0) {
          this->is_empty = false;
        }
        this->counter++;
        return true;
      }
    }
    if (kickout) {
      int j = rand() % 4;
      victim.index = index;
      victim.fingerprint = read(index, j);
      victim.used = true;
      write(index, j, fingerprint);
    }
    return false;
  }

  bool queryImpl(const size_t index, const uint32_t fingerprint) {
    /*
    if (fingerprint_size <= 4) {
      const char *p = bucket[index].bit_array;
      uint64_t bits = *(uint64_t *)p;
      return hasvalue4(bits, fingerprint);
    } else if (fingerprint_size <= 8) {
      const char *p = bucket[index].bit_array;
      uint64_t bits = *(uint64_t *)p;
      return hasvalue8(bits, fingerprint);
    } else if (fingerprint_size <= 12) {
      const char *p = bucket[index].bit_array;
      uint64_t bits = *(uint64_t *)p;
      return hasvalue12(bits, fingerprint);
    } else if (fingerprint_size <= 16) {
      const char *p = bucket[index].bit_array;
      uint64_t bits = *(uint64_t *)p;
      return hasvalue16(bits, fingerprint);
    } else {
      return false;
    }
    */

    for (size_t pos = 0; pos < 4; pos++) {
      uint32_t fp = read(index, pos) & ((1ULL << this->exact_fingerprint_size) - 1);
      if (fp == fingerprint) {
        return true;
      }
    }
    return false;
  }

  bool deleteImpl(const size_t index, const uint32_t fingerprint) {
    for (size_t pos = 0; pos < 4; pos++) {
      if (read(index, pos) == fingerprint) {
        write(index, pos, 0);
        counter--;
        if (counter < this->capacity) {
          this->is_full = false;
        }
        if (counter == 0) {
          this->is_empty = true;
        }
        return true;
      }
    }
    return false;
  }

  /****************************************
   * read from bucket & write into bucket *
   ****************************************/
  uint32_t read(const size_t index, const size_t pos) {
    const char *p = bucket[index].bit_array;
    uint32_t fingerprint;

    if (fingerprint_size <= 4) {
      p += (pos >> 1);
      uint8_t bits_8 = *(uint8_t *)p;
      if ((pos & 1) == 0) {
        fingerprint = (bits_8 >> 4) & 0xf;
      } else {
        fingerprint = bits_8 & 0xf;
      }
    } else if (fingerprint_size <= 8) {
      p += pos;
      uint8_t bits_8 = *(uint8_t *)p;
      fingerprint = bits_8 & 0xff;
    } else if (fingerprint_size == 12) {
      p += pos + (pos >> 1);
      uint16_t bits_16 = *(uint16_t *)p;
      if ((pos & 1) == 0) {
        fingerprint = bits_16 & 0xfff;
      } else {
        fingerprint = (bits_16 >> 4) & 0xfff;
      }
    } else if (fingerprint_size <= 16) {
      p += (pos << 1);
      uint16_t bits_16 = *(uint16_t *)p;
      fingerprint = bits_16 & 0xffff;
    } else if (fingerprint_size <= 24) {
      p += pos + (pos << 1);
      uint32_t bits_32 = *(uint32_t *)p;
      fingerprint = (bits_32 >> 4);
    } else if (fingerprint_size <= 32) {
      p += (pos << 2);
      uint32_t bits_32 = *(uint32_t *)p;
      fingerprint = bits_32 & 0xffffffff;
    } else {
      fingerprint = 0;
    }
    return fingerprint & mask;
  }

  void write(const size_t index, const size_t pos, const uint32_t fingerprint) {
    char *p = bucket[index].bit_array;
    uint32_t fp = fingerprint & mask;
    if (fingerprint_size <= 4) {
      p += (pos >> 1);
      if ((pos & 1) == 0) {
        *((uint8_t *)p) &= 0x0f;
        *((uint8_t *)p) |= (fp << 4);
      } else {
        *((uint8_t *)p) &= 0xf0;
        *((uint8_t *)p) |= fp;
      }
    } else if (fingerprint_size <= 8) {
      ((uint8_t *)p)[pos] = fp;
    } else if (fingerprint_size <= 12) {
      p += (pos + (pos >> 1));
      if ((pos & 1) == 0) {
        *((uint16_t *)p) &= 0xf000; // Little-Endian
        *((uint16_t *)p) |= fp;
      } else {
        *((uint16_t *)p) &= 0x000f;
        *((uint16_t *)p) |= fp << 4;
      }
    } else if (fingerprint_size <= 16) {
      ((uint16_t *)p)[pos] = fp;
    } else if (fingerprint_size <= 24) {
      p += (pos + (pos << 1));
      *((uint32_t *)p) &= 0xff000000; // Little-Endian
      *((uint32_t *)p) |= fp;
    } else if (fingerprint_size <= 32) {
      ((uint32_t *)p)[pos] = fp;
    }
  }

  [[nodiscard]] auto size() const -> size_t { return this->single_table_length; }
};

} // namespace compactedlogarithmicdynamiccuckoofilter
