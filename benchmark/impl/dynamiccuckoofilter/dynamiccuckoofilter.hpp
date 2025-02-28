#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>

#include "../../../src/utils/hash.hpp"
#include "cuckoofilter.hpp"

namespace dynamiccuckoofilter {

inline auto get_current_time_in_seconds() -> double {
  auto now = std::chrono::high_resolution_clock::now();
  auto duration = now.time_since_epoch();
  return std::chrono::duration_cast<std::chrono::duration<double>>(duration).count();
}

template <typename T> class DynamicCuckooFilter {
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

  size_t single_table_length;
  size_t single_capacity;

  double false_positive;

  size_t fingerprint_size;

  Victim victim;

public:
  /* Modified start: Benchmarking */
  double total_addressing_time = 0.0;
  /* Modified end: Benchmarking */

  // record the items inside DCF
  size_t counter;

  size_t listNum;
  CuckooFilter<T> *firstNode;

  /****************************************
   * construction & destruction functions *
   ****************************************/
  DynamicCuckooFilter(const size_t single_table_length, size_t fingerprint_size) {
    this->single_table_length =
        single_table_length; // 2048 1024 512 256 128 ---!!!---must be the power of 2---!!!---

    this->fingerprint_size = fingerprint_size;
    this->single_capacity = this->single_table_length * 4 * 0.9;

    this->counter = 0;

    this->firstNode = new CuckooFilter<T>(this->single_table_length, this->fingerprint_size,
                                          this->single_capacity);

    this->listNum = 1;
  }

  DynamicCuckooFilter(const size_t single_table_length, size_t fingerprint_size, size_t nums) {
    this->single_table_length =
        single_table_length /
        nums; // 2048 1024 512 256 128 ---!!!---must be the power of 2---!!!---

    this->fingerprint_size = fingerprint_size;

    this->single_capacity = this->single_table_length * 4 * 0.90;

    this->counter = 0;

    this->firstNode = new CuckooFilter<T>(this->single_table_length, this->fingerprint_size,
                                          this->single_capacity);

    CuckooFilter<T> *cur = this->firstNode;
    for (unsigned int i = 0; i < nums - 1; i++) {
      CuckooFilter<T> *nextCF = new CuckooFilter<T>(this->single_table_length,
                                                    this->fingerprint_size, this->single_capacity);
      cur->next = nextCF;
      nextCF->front = cur;
      cur = nextCF;
    }

    this->listNum = nums;
  }

  ~DynamicCuckooFilter() { delete firstNode; }

  /*************************************
   * insert & query & delete functions *
   *************************************/
  auto insertItem(const T &item) -> bool {
    CuckooFilter<T> *curCF = firstNode;
    if (curCF->is_full == true) {
      curCF = getNextCF(curCF);
    }

    if (curCF->insertItem(item, victim)) {
      counter++;
    } else {
      // curCF->is_full = true;
      failureHandle(victim);
      counter++;
    }

    return true;
  }

  CuckooFilter<T> *getNextCF(CuckooFilter<T> *curCF) {
    CuckooFilter<T> *nextCF = firstNode;
    while (nextCF->is_full) {
      if (nextCF->next) {
        nextCF = nextCF->next;
      } else {
        CuckooFilter<T> *cur = new CuckooFilter<T>(this->single_table_length,
                                                   this->fingerprint_size, this->single_capacity);
        this->listNum += 1;
        nextCF->next = cur;
        cur->front = nextCF;
        nextCF = cur;
        return nextCF;
      }
    }
    return nextCF;
  }

  auto failureHandle(Victim &victim) -> bool {
    CuckooFilter<T> *nextCF = getNextCF(firstNode);
    if (nextCF->insertItem(victim.index, victim.fingerprint, true, victim) == false) {
      nextCF->is_full = true;
      nextCF = getNextCF(firstNode);
      failureHandle(victim);
    }
    return true;
  }

  auto queryItem(const T &item) -> bool {
    size_t index, alt_index;
    uint32_t fingerprint;
    generateIF(item, index, fingerprint, fingerprint_size, single_table_length);
    CuckooFilter<T> *query_pt = firstNode;
    const double start = get_current_time_in_seconds();
    while (query_pt) {
      if (query_pt->queryItem(index, fingerprint)) {
        /* Modified start: Benchmarking */
        total_addressing_time += get_current_time_in_seconds() - start;
        return true;
      } else {
        query_pt = query_pt->next;
      }
    }
    /* Modified start: Benchmarking */
    total_addressing_time += get_current_time_in_seconds() - start;
    /* Modified end: Benchmarking */
    return false;
  }

  auto deleteItem(const T &item) -> bool {
    size_t index, alt_index;
    uint32_t fingerprint;
    generateIF(item, index, fingerprint, fingerprint_size, single_table_length);
    CuckooFilter<T> *delete_pt = firstNode;
    while (delete_pt) {
      if (delete_pt->deleteItem(index, fingerprint)) {
        counter--;
        return true;
      } else {
        delete_pt = delete_pt->next;
      }
    }
    return false;

    /*
    size_t index, alt_index;
    uint32_t fingerprint;

    generateIF(item, index, fingerprint, fingerprint_size, single_table_length);
    generateA(index, fingerprint, alt_index, single_table_length);
    CuckooFilter *delete_pt = firstNode;
    for (int count = 0; count < cf_list->num; count++) {
      if (delete_pt->queryImpl(index, fingerprint)) {
        if (delete_pt->deleteImpl(index, fingerprint)) {
          counter--;
          return true;
        }
      } else if (delete_pt->queryImpl(alt_index, fingerprint)) {
        if (delete_pt->deleteImpl(alt_index, fingerprint)) {
          counter--;
          return true;
        }
      } else {
        delete_pt = delete_pt->next;
      }
    }
    return false;
    */
  }

  /**************
   * compaction *
   **************/
  auto compact() -> bool {
    int queue_length = 0;
    CuckooFilter<T> *temp = this->firstNode;
    for (int count = 0; count < this->listNum; count++) {
      if (!temp->is_full) {
        queue_length++;
      }
      temp = temp->next;
    }
    if (queue_length == 0) {
      return true;
    }

    CuckooFilter<T> **cfq = new CuckooFilter<T> *[queue_length];
    int pos = 0;
    temp = this->firstNode;
    for (int count = 0; count < this->listNum; count++) {
      if (!temp->is_full) {
        cfq[pos] = temp;
        pos++;
      }
      temp = temp->next;
    }

    sort(cfq, queue_length);
    for (int i = 0; i < queue_length - 1; i++) {
      for (int j = queue_length - 1; j > i; j--) {
        cfq[i]->transfer(cfq[j]);
        if (cfq[i]->is_empty == true) {
          this->remove(cfq[i]);
          break;
        }
      }
    }
    if (cfq[queue_length - 1]->is_empty == true) {
      this->remove(cfq[queue_length - 1]);
    }

    return true;
  }

  void sort(CuckooFilter<T> **cfq, int queue_length) {
    CuckooFilter<T> *temp;
    for (int i = 0; i < queue_length - 1; i++) {
      for (int j = 0; j < queue_length - 1 - i; j++) {
        if (cfq[j]->counter > cfq[j + 1]->counter) {
          temp = cfq[j];
          cfq[j] = cfq[j + 1];
          cfq[j + 1] = temp;
        }
      }
    }
  }

  auto remove(CuckooFilter<T> *cf_remove) -> bool {
    CuckooFilter<T> *frontCF = cf_remove->front;
    if (frontCF == NULL) {
      // this->cf_pt = cf_remove->next;
      firstNode = cf_remove->next;
    } else {
      frontCF->next = cf_remove->next;
      cf_remove->next->front = frontCF;
    }
    cf_remove = NULL;
    this->listNum -= 1;
    return true;
  }

  /*******************************
   * generate 2 bucket addresses *
   *******************************/
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

} // namespace dynamiccuckoofilter
