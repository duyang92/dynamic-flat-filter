#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <type_traits>

#include "../../../src/utils/hash.hpp"
#include "bithack.hpp"

namespace dynamiccuckoofilter {

constexpr size_t K_MAX_KICK_COUT = 500uz;

using Victim = struct Victim {
  size_t index;
  uint32_t fingerprint;
};

using Bucket = struct Bucket {
  char *bit_array;
};

template <typename T> class CuckooFilter {
private:
  /* Modified start */
  static constexpr uint64_t HASH_SEED = 1234;

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

  size_t capacity;
  size_t single_table_length;
  size_t fingerprint_size;
  size_t bits_per_bucket;
  size_t bytes_per_bucket;

  Bucket *bucket;

  uint32_t mask;

public:
  bool is_full;
  bool is_empty;
  int counter;
  CuckooFilter *next;
  CuckooFilter *front;

  /*****************************************
   * construction and destruction function *
   *****************************************/
  CuckooFilter(const size_t single_table_length, const size_t fingerprint_size,
               const size_t capacity) {
    this->fingerprint_size = fingerprint_size;
    bits_per_bucket = fingerprint_size * 4;
    bytes_per_bucket = (fingerprint_size * 4 + 7) >> 3;
    this->single_table_length = single_table_length;
    counter = 0;
    this->capacity = capacity;
    is_full = false;
    is_empty = true;
    next = NULL;
    front = NULL;
    mask = (1ULL << fingerprint_size) - 1;

    bucket = new Bucket[single_table_length];
    for (size_t i = 0; i < single_table_length; i++) {
      bucket[i].bit_array = new char[bytes_per_bucket];
      memset(bucket[i].bit_array, 0, bytes_per_bucket);
    }
  }

  ~CuckooFilter() {
    delete[] bucket;
    if (next)
      delete next;
  }

  /************************************
   * insert & query & delete function *
   ************************************/
  bool insertItem(const T &item, Victim &victim) {
    size_t index, alt_index;
    uint32_t fingerprint;
    generateIF(item, index, fingerprint, fingerprint_size, single_table_length);

    // edit 3-17
    for (size_t count = 0; count < K_MAX_KICK_COUT; count++) {
      bool kickout = (count != 0);
      if (insertImpl(index, fingerprint, kickout, victim)) {
        return true;
      }

      if (kickout) {
        index = victim.index;
        fingerprint = victim.fingerprint;
        generateA(index, fingerprint, alt_index, single_table_length);
        index = alt_index;
      } else {
        generateA(index, fingerprint, alt_index, single_table_length);
        index = alt_index;
      }
    }

    return false;
  }

  bool insertItem(size_t index, uint32_t fingerprint, bool kickout, Victim &victim) {
    size_t alt_index;

    for (size_t count = 0; count < K_MAX_KICK_COUT; count++) {
      bool kickout = (count != 0);
      if (insertImpl(index, fingerprint, kickout, victim)) {
        return true;
      }

      if (kickout) {
        index = victim.index;
        fingerprint = victim.fingerprint;
        generateA(index, fingerprint, alt_index, single_table_length);
        index = alt_index;
      } else {
        generateA(index, fingerprint, alt_index, single_table_length);
        index = alt_index;
      }
    }
    return false;
  }

  bool queryItem(const T &item) {
    size_t index, alt_index;
    uint32_t fingerprint;
    generateIF(item, index, fingerprint, fingerprint_size, single_table_length);

    if (queryImpl(index, fingerprint)) {
      return true;
    }
    generateA(index, fingerprint, alt_index, single_table_length);
    if (queryImpl(alt_index, fingerprint)) {
      return true;
    }
    return false;
  }

  bool queryItem(size_t index, const uint32_t fingerprint) {
    size_t alt_index;
    if (queryImpl(index, fingerprint)) {
      return true;
    }
    generateA(index, fingerprint, alt_index, single_table_length);
    if (queryImpl(alt_index, fingerprint)) {
      return true;
    }
    return false;
  }

  bool deleteItem(const T &item) {
    size_t index, alt_index;
    uint32_t fingerprint;
    generateIF(item, index, fingerprint, fingerprint_size, single_table_length);

    if (deleteImpl(index, fingerprint)) {
      return true;
    }
    generateA(index, fingerprint, alt_index, single_table_length);
    if (deleteImpl(alt_index, fingerprint)) {
      return true;
    }

    return false;
  }

  bool deleteItem(size_t index, const uint32_t fingerprint) {
    size_t alt_index;
    if (deleteImpl(index, fingerprint)) {
      return true;
    }
    generateA(index, fingerprint, alt_index, single_table_length);
    if (deleteImpl(alt_index, fingerprint)) {
      return true;
    }
    return false;
  }

  bool insertImpl(const size_t index, const uint32_t fingerprint, const bool kickout,
                  Victim &victim) {
    for (size_t pos = 0; pos < 4; pos++) {
      if (read(index, pos) == 0) {
        write(index, pos, fingerprint);
        counter++;
        if (this->counter == capacity)
          this->is_full = true;
        if (this->counter > 0) {
          this->is_empty = false;
        }
        return true;
      }
    }
    if (kickout) {
      int j = rand() % 4;
      victim.index = index;
      victim.fingerprint = read(index, j);
      write(index, j, fingerprint);
    }
    return false;
  }

  bool queryImpl(const size_t index, const uint32_t fingerprint) {
    if (fingerprint_size == 4) {
      const char *p = bucket[index].bit_array;
      uint64_t bits = *(uint64_t *)p;
      return hasvalue4(bits, fingerprint);
    } else if (fingerprint_size == 8) {
      const char *p = bucket[index].bit_array;
      uint64_t bits = *(uint64_t *)p;
      return hasvalue8(bits, fingerprint);
    } else if (fingerprint_size == 12) {
      const char *p = bucket[index].bit_array;
      uint64_t bits = *(uint64_t *)p;
      return hasvalue12(bits, fingerprint);
    } else if (fingerprint_size == 16) {
      const char *p = bucket[index].bit_array;
      uint64_t bits = *(uint64_t *)p;
      return hasvalue16(bits, fingerprint);
    } else {
      return false;
    }
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

  /*******************************************
   * generate two candidate bucket addresses *
   *******************************************/
  void generateIF(const T &item, size_t &index, uint32_t &fingerprint, size_t fingerprint_size,
                  size_t single_table_length) {
    const uint64_t hv = hash(item, HASH_SEED);

    index = static_cast<uint32_t>(hv >> 32) % single_table_length;
    fingerprint = static_cast<uint32_t>(hv & 0xFFFFFFFF);
    fingerprint &= ((0x1ULL << fingerprint_size) - 1);
    fingerprint += (fingerprint == 0);
  }

  void generateA(size_t index, uint32_t fingerprint, size_t &alt_index,
                 size_t single_table_length) {
    alt_index = (index ^ (fingerprint * 0x5bd1e995)) % single_table_length;
  }

  /****************************************
   * read from bucket & write into bucket *
   ****************************************/
  uint32_t read(const size_t index, const size_t pos) {
    const char *p = bucket[index].bit_array;
    uint32_t fingerprint;

    if (fingerprint_size == 4) {
      p += (pos >> 1);
      uint8_t bits_8 = *(uint8_t *)p;
      if ((pos & 1) == 0) {
        fingerprint = (bits_8 >> 4) & 0xf;
      } else {
        fingerprint = bits_8 & 0xf;
      }
    } else if (fingerprint_size == 8) {
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
    } else if (fingerprint_size == 16) {
      p += (pos << 1);
      uint16_t bits_16 = *(uint16_t *)p;
      fingerprint = bits_16 & 0xffff;
    } else if (fingerprint_size == 24) {
      p += pos + (pos << 1);
      uint32_t bits_32 = *(uint32_t *)p;
      fingerprint = (bits_32 >> 4);
    } else if (fingerprint_size == 32) {
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

    if (fingerprint_size == 4) {
      p += (pos >> 1);
      if ((pos & 1) == 0) {
        *((uint8_t *)p) &= 0x0f;
        *((uint8_t *)p) |= (fingerprint << 4);
      } else {
        *((uint8_t *)p) &= 0xf0;
        *((uint8_t *)p) |= fingerprint;
      }
    } else if (fingerprint_size == 8) {
      ((uint8_t *)p)[pos] = fingerprint;
    } else if (fingerprint_size == 12) {
      p += (pos + (pos >> 1));
      if ((pos & 1) == 0) {
        *((uint16_t *)p) &= 0xf000; // Little-Endian
        *((uint16_t *)p) |= fingerprint;
      } else {
        *((uint16_t *)p) &= 0x000f;
        *((uint16_t *)p) |= fingerprint << 4;
      }
    } else if (fingerprint_size == 16) {
      ((uint16_t *)p)[pos] = fingerprint;
    } else if (fingerprint_size == 24) {
      p += (pos + (pos << 1));
      *((uint32_t *)p) &= 0xff000000; // Little-Endian
      *((uint32_t *)p) |= fingerprint;
    } else if (fingerprint_size == 32) {
      ((uint32_t *)p)[pos] = fingerprint;
    }
  }

  /*************************************************
   * move corresponding fingerprints to sparser CF *
   *************************************************/
  bool transfer(CuckooFilter *tarCF) {
    uint32_t fingerprint = 0;

    for (size_t i = 0; i < single_table_length; i++) {
      for (int j = 0; j < 4; j++) {
        fingerprint = read(i, j);
        if (fingerprint != 0) {
          if (tarCF->is_full == true) {
            return false;
          }
          if (this->is_empty == true) {
            return false;
          }
          Victim victim = {0, 0};
          if (tarCF->insertImpl(i, fingerprint, false, victim)) {
            this->write(i, j, 0);
            this->counter--;

            if (this->counter < capacity) {
              this->is_full = false;
            }
            if (this->counter == 0) {
              this->is_empty = true;
            }

            if (tarCF->counter == capacity) {
              tarCF->is_full = true;
            }

            if (tarCF->counter > 0) {
              tarCF->is_empty = false;
            }
          }
        }
      }
    }
    return true;
  }
};

} // namespace dynamiccuckoofilter
