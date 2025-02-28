#pragma once

#include <chrono>
#include <cstddef>
#include <cstring>
#include <string>
#include <type_traits>

#include "../../../src/utils/hash.hpp"
#include "countingbloomfilter.hpp"

namespace dynamicbloomfilter {

inline auto get_current_time_in_seconds() -> double {
  const auto now = std::chrono::high_resolution_clock::now();
  const auto duration = now.time_since_epoch();
  return std::chrono::duration_cast<std::chrono::duration<double>>(duration).count();
}

class LinkList {
public:
  inline static int num = 0;

  CountingBloomFilter *cf_pt;
  CountingBloomFilter *tail_pt;

  LinkList(int single_capacity, double single_false_positive) {
    cf_pt = new CountingBloomFilter(single_capacity, single_false_positive);
    tail_pt = new CountingBloomFilter(single_capacity, single_false_positive);
  }

  LinkList(int n, int m) {
    cf_pt = new CountingBloomFilter(n, m);
    tail_pt = new CountingBloomFilter(n, m);
  }

  ~LinkList() {
    delete cf_pt;
    delete tail_pt;
  }
};

template <typename T> class DynamicBloomFilter {
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

  double false_positive;
  double single_false_positive;
  int capacity;
  int single_capacity;
  int counter;

  int hash_num;

  CountingBloomFilter *curSBF;
  CountingBloomFilter *nextSBF;

public:
  /* Modified start */
  double total_addressing_time = 0.0;
  int bits_num;
  /* Modified end */

  // the link list of building blocks
  LinkList *sbf_list;

  /*****************************************
   * construction and destruction function *
   *****************************************/
  DynamicBloomFilter(const size_t capacity, const double false_positive,
                     const size_t exp_block_num = 6) {
    this->capacity = capacity;

    single_capacity = capacity / exp_block_num; // s=6 7680 s=12 3840 s=24 1920 s=48 960 s=96 480

    this->false_positive = false_positive;
    single_false_positive = 1 - pow((1 - false_positive), (double)single_capacity / capacity);

    counter = 0;
    bits_num =
        (int)ceil(single_capacity * (1 / log(2.0)) * log(1 / single_false_positive) / log(2.0)) * 4;
    //		this->bits_num=(int)ceil(single_capacity*log(single_false_positive)/log(0.61285))*4;
    hash_num = (int)ceil((bits_num / 4) / single_capacity * log(2.0));

    curSBF = new CountingBloomFilter(single_capacity, single_false_positive);
    nextSBF = NULL;
    sbf_list = new LinkList(single_capacity, single_false_positive);
    sbf_list->cf_pt = curSBF;
    sbf_list->tail_pt = curSBF;
    sbf_list->num = 1;
  }

  DynamicBloomFilter(int n, int m) {
    false_positive = 0; // not used
    capacity = n;
    single_capacity = 1024;
    single_false_positive = 0; // not used
    counter = 0;
    bits_num = m;
    hash_num = 7;

    curSBF = new CountingBloomFilter(1024, m);
    nextSBF = NULL;
    sbf_list = new LinkList(1024, m);
    sbf_list->cf_pt = curSBF;
    sbf_list->tail_pt = curSBF;
    sbf_list->num = 1;
  }

  ~DynamicBloomFilter() { delete sbf_list; }

  /************************************
   * insert & query & delete function *
   ************************************/
  bool insertItem(const T &item) {
    unsigned long int *hash_val = generateHashVal(item);

    if (curSBF->item_num >= curSBF->capacity) {
      curSBF = getNextSBF(curSBF);
    }

    if (curSBF->insertItem(hash_val)) {
      this->counter += 1;
    }

    delete[] hash_val;
    return true;
  }

  CountingBloomFilter *getNextSBF(CountingBloomFilter *curSBF) {
    if (curSBF == this->sbf_list->tail_pt) {
      nextSBF = new CountingBloomFilter(single_capacity, single_false_positive);
      curSBF->next = nextSBF;
      nextSBF->front = curSBF;
      sbf_list->tail_pt = nextSBF;
      sbf_list->num++;
    } else {
      nextSBF = curSBF->next;
      if (nextSBF->item_num >= nextSBF->capacity) {
        nextSBF = getNextSBF(nextSBF);
      }
    }
    return nextSBF;
  }

  bool queryItem(const T &item) {
    /* Modified start: Benchmarking */
    const double start = get_current_time_in_seconds();
    /* Modified end: Benchmarking */
    unsigned long int *hash_val = generateHashVal(item);

    CountingBloomFilter *query_pt = sbf_list->cf_pt;
    for (int i = 0; i < sbf_list->num; i++) {
      if (query_pt->queryItem(hash_val)) {
        /* Modified start: Benchmarking */
        total_addressing_time += get_current_time_in_seconds() - start;
        /* Modified end: Benchmarking */
        delete[] hash_val;
        return true;
      }
      query_pt = query_pt->next;
    }

    /* Modified start: Benchmarking */
    total_addressing_time += get_current_time_in_seconds() - start;
    /* Modified end: Benchmarking */
    delete[] hash_val;
    return false;
  }

  bool deleteItem(const T &item) {
    unsigned long int *hash_val = generateHashVal(item);
    CountingBloomFilter *query_pt = sbf_list->cf_pt;
    CountingBloomFilter *delete_pt = sbf_list->cf_pt;
    int counter = 0;

    for (int i = 0; i < sbf_list->num; i++) {
      if (query_pt->queryItem(hash_val)) {
        counter += 1;
      }
      query_pt = query_pt->next;
    }

    if (counter == 1) {
      for (int i = 0; i < sbf_list->num; i++) {
        if (delete_pt->queryItem(hash_val)) {
          delete_pt->deleteItem(hash_val);
          this->counter -= 1;
          delete[] hash_val;
          return true;
        }
        delete_pt = delete_pt->next;
      }
    }

    delete[] hash_val;
    return false;
  }

  unsigned long int *generateHashVal(const T &item) {
    unsigned long int *hash_value = new unsigned long int[hash_num];

    const uint64_t hv = hash(item, HASH_SEED);

    hash_value[0] = static_cast<uint32_t>(hv >> 32) % (bits_num / 4);
    hash_value[1] = static_cast<uint32_t>(hv & 0xFFFFFFFF) % (bits_num / 4);

    for (int i = 2; i < hash_num; i++) {
      hash_value[i] = (hash_value[0] + i * hash_value[1] + i * i) % (bits_num / 4);
    }

    return hash_value;
  }
};

} // namespace dynamicbloomfilter
