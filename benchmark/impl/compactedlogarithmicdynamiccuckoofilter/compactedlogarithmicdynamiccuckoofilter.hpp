#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <string>
#include <type_traits>

#include "../../../src/utils/hash.hpp"
#include "cuckoofilter.hpp"

namespace compactedlogarithmicdynamiccuckoofilter {

inline auto get_current_time_in_seconds() -> double {
  auto now = std::chrono::high_resolution_clock::now();
  auto duration = now.time_since_epoch();
  return std::chrono::duration_cast<std::chrono::duration<double>>(duration).count();
}

template <typename T> class CompactedLogarithmicDynamicCuckooFilter {
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

  size_t single_capacity;

  size_t fingerprint_size;

  Victim_L victim;

  CuckooFilterL<T> *root;
  uint32_t mask;

  void generateIF(const T &item, size_t &index, uint32_t &fingerprint) {
    const uint64_t hv = hash(item, HASH_SEED);

    index = static_cast<uint32_t>(hv >> 32) % (this->single_capacity);
    fingerprint = static_cast<uint32_t>(hv & 0xFFFFFFFF);
    fingerprint &= ((0x1ULL << fingerprint_size) - 1);
    fingerprint += (fingerprint == 0);
  }

  CuckooFilterL<T> *getCur(uint32_t fingerprint) {
    unsigned int curlevel = 1;
    CuckooFilterL<T> *curCF = root;
    while (curCF->is_null) {
      if (((fingerprint >> (this->fingerprint_size - curlevel)) & 1) == 0) {
        curCF = curCF->lchild;
      } else if (((fingerprint >> (this->fingerprint_size - curlevel)) & 1) == 1) {
        curCF = curCF->rchild;
      }

      curlevel += 1;
    }
    return curCF;
  }

public:
  /* Modified start: Benchmarking */
  double total_expansion_time = 0.0;
  double total_addressing_time = 0.0;
  /* Modified end: Benchmarking */

  // record the items inside DCF
  size_t counter;

  size_t listNum;

  /*****************************************
   * construction and destruction function *
   *****************************************/
  CompactedLogarithmicDynamicCuckooFilter(const size_t capacity, const size_t fingerprint_size) {
    this->single_capacity = capacity / 4;

    this->fingerprint_size = fingerprint_size;
    counter = 0;
    victim.used = false;
    root = new CuckooFilterL<T>(this->single_capacity, fingerprint_size, 0, 0);
    this->mask = (1ULL << this->fingerprint_size) - 1;
    listNum = 1;
  }

  ~CompactedLogarithmicDynamicCuckooFilter() { delete root; }

  /************************************
   * insert & query & delete function *
   ************************************/
  bool insertItem(const T &item) {
    size_t index;
    uint32_t fingerprint;

    generateIF(item, index, fingerprint);
    Victim_L v1;
    CuckooFilterL<T> *curCF = getCur(fingerprint);
    if (curCF->insertItem(index, fingerprint, v1)) {
      this->counter += 1;
    }
    if (curCF->counter >= curCF->capacity) {
      const double start = get_current_time_in_seconds();
      bool res = false;
      Victim_L victimRes = v1;
      CuckooFilterL<T> *leftChild = new CuckooFilterL<T>(
          this->single_capacity, this->fingerprint_size, curCF->level + 1, curCF->number * 2);
      CuckooFilterL<T> *rightChild = new CuckooFilterL<T>(
          this->single_capacity, this->fingerprint_size, curCF->level + 1, curCF->number * 2 + 1);
      curCF->lchild = leftChild;
      curCF->rchild = rightChild;
      listNum += 1;
      for (unsigned int i = 0; i < curCF->size(); i++) {
        for (unsigned int j = 0; j < 4; j++) {
          uint32_t fp = curCF->read(i, j);
          if (((fp >> (curCF->fingerprint_size - curCF->level - 1)) & 1) == 0) {
            // res = leftChild->insertItem(i, fp, victim);
            leftChild->write(i, j, fp);
            leftChild->counter++;
          } else if (((fp >> (curCF->fingerprint_size - curCF->level - 1)) & 1) == 1) {
            // res = rightChild->insertItem(i, fp, victim);
            rightChild->write(i, j, fp);
            rightChild->counter++;
          }
        }
      }
      delete[] curCF->bucket;
      curCF->bucket = NULL;
      curCF->is_null = true;
      total_expansion_time += get_current_time_in_seconds() - start;

      return true;
    }
    return true;
  }

  bool queryItem(const T &item) {
    size_t index;
    uint32_t fingerprint;

    generateIF(item, index, fingerprint);

    /* Modified start: Benchmarking */
    const double start = get_current_time_in_seconds();
    CuckooFilterL<T> *curCF = getCur(fingerprint);
    /* Modified start: Benchmarking */
    total_addressing_time += get_current_time_in_seconds() - start;
    /* Modified end: Benchmarking */
    return curCF->queryItem(index, fingerprint);

    /*
    for (unsigned int CF_index = 0; CF_index < CF_tree.size(); CF_index++) {
      if (fingerprint >> (this->CF_tree[CF_index]->fingerprint_size) ==
          this->CF_tree[CF_index]->number) {
        return this->CF_tree[CF_index]->queryItem(item);
      }
    }
    */
  }

  bool deleteItem(const T &item) {
    size_t index;
    uint32_t fingerprint;

    generateIF(item, index, fingerprint);
    CuckooFilterL<T> *curCF = getCur(fingerprint);
    if (curCF->deleteItem(index, fingerprint)) {
      this->counter -= 1;
      return true;
    }
    return false;

    /*
    for (unsigned int CF_index = 0; CF_index < CF_tree.size(); CF_index++) {
      if (fingerprint >> (this->CF_tree[CF_index]->fingerprint_size) ==
          this->CF_tree[CF_index]->number) {
        if (this->CF_tree[CF_index]->queryItem(item)) {
          this->counter -= 1;
          return true;
        }
      }
    }
    return false;
    */
  }

  /******************************************************************
   * extra function to make sure the table length is the power of 2 *
   ******************************************************************/
  auto upperpower2(uint64_t x) -> uint64_t {
    x--;
    x |= x >> 1;
    x |= x >> 2;
    x |= x >> 4;
    x |= x >> 8;
    x |= x >> 16;
    x |= x >> 32;
    x++;
    return x;
  }
};

} // namespace compactedlogarithmicdynamiccuckoofilter
