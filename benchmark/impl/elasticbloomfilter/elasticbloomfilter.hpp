#pragma once

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <thread>

#include "../../../src/utils/hash.hpp"
#include "param.hpp"

namespace elasticbloomfilter {

#define FINGER_LENGTH finger_length
#define POSMASK posmask
#define SIZELOG sizelog
// #define FINGER_LENGTH 8
// #define POSMASK 0x00FFFFFF
// #define SIZELOG 24

inline auto get_current_time_in_seconds() -> double {
  const auto now = std::chrono::high_resolution_clock::now();
  const auto duration = now.time_since_epoch();
  return std::chrono::duration_cast<std::chrono::duration<double>>(duration).count();
}

template <typename T> class ElasticBloomFilter {
public:
  /* Modified start */
  [[nodiscard]] static constexpr auto hash(const T &item, const uint32_t seed) -> uint32_t {
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

  /* Modified start: Benchmarking */
  double total_expansion_time = 0;
  double total_addressing_time = 0;
  /* Modified end: Benchmarking */

  alignas(64) uint16_t
      finger_buckets[1 << MAX_BLOOM_SIZE][BUCKET_SIZE]; // to simulate finger buckets in slow memory
  alignas(64) uint8_t bucket_size[1 << MAX_BLOOM_SIZE];
  alignas(64) uint8_t bucket_fplen[1 << MAX_BLOOM_SIZE];

  bool expandOrNot;
  uint32_t size; // the length of the bit map
  int hash_num;
  int sizelog;
  uint32_t posmask;
  uint32_t ExpandBitNum;

  int compression; // to record the expanding times(= -compression)

  int _1_num = 0; // the number of those set bits

  /* Modified start: Change from `omp_lock_t` to `std::mutex` */
  std::mutex lock[LOCK_NUM];
  /* Modified end: Change from stack to heap */
  uint32_t finger_length; // the length of the fingerprint

  char *bloom_arr; // the standard Bloom filter

  ElasticBloomFilter(uint32_t sz, int hash_num, bool expand = true) {
    assert(hash_num > 0);
    assert(sz < MAX_SIZE && sz > MIN_SIZE);

    this->hash_num = hash_num;

    expandOrNot = expand;

    finger_length = MAX_SIZE - sz;

    /* to allocate the bit map and the hash function */
    size = 1 << sz;
    sizelog = sz;
    posmask = (1 << sizelog) - 1;
    ExpandBitNum = std::min(size, 1u << SAMPLEBITNUM) * EXPAND_THRESHOLD;
    // std::cout << "hash_num\t" << hash_num << "\t"
    //           << "ExpandBitNum\t" << ExpandBitNum << "\t" << std::endl;
    bloom_arr = new char[size >> 3];
    memset(bloom_arr, 0, size >> 3);
  }

  ~ElasticBloomFilter() {
    if (bloom_arr != NULL)
      delete[] bloom_arr;
  }

  /**
   * Clear the EBF.
   */
  void clear() {
    _1_num = 0;
    memset(bloom_arr, 0, size >> 3);
    memset(&finger_buckets[0][0], 0, size * (BUCKET_SIZE) * sizeof(finger_buckets[0][0]));
    memset(&bucket_size[0], 0, size * sizeof(bucket_size[0]));
    memset(&bucket_fplen[0], 0, size * sizeof(bucket_fplen[0]));
  }

  void lazy_update(int layer, uint32_t bid) {
    if (((bloom_arr[bid >> SHIFT] >> (bid & MASK)) & 1) && bucket_size[bid] &&
        bucket_fplen[bid] > FINGER_LENGTH) {

      uint32_t bs = bucket_size[bid], dlen = (bucket_fplen[bid] - FINGER_LENGTH),
               t = (1 << dlen) - 1, cnt = 0;

      for (int j = 1; j <= bs; j++)
        if ((finger_buckets[bid][j] & t) == (bid >> (sizelog - dlen)))
          finger_buckets[bid][++cnt] = finger_buckets[bid][j] >> dlen;
      bucket_size[bid] = cnt;
      bucket_fplen[bid] = (cnt ? FINGER_LENGTH : 0);

      if (!cnt) {
        bloom_arr[bid >> SHIFT] &= ~(1u << (bid & MASK));
      } else {
        bloom_arr[bid >> SHIFT] |= (1u << (bid & MASK));
        if ((bid >> SAMPLEBITNUM) == 0)
          _1_num++;
      }
    }
  }

  /**
   * Insert a key.
   */
  auto insert(const T &item) -> bool {
    for (int seed = 0; seed < hash_num; ++seed) {
      const double start = get_current_time_in_seconds();
      uint32_t pos = hash(item, seed);
      uint32_t bid = pos & POSMASK;
      total_addressing_time += get_current_time_in_seconds() - start;
      lazy_update(0, bid);

      bucket_fplen[bid] = FINGER_LENGTH;
      finger_buckets[bid][++bucket_size[bid]] = pos >> SIZELOG;

      if (bucket_size[bid] == BUCKET_SIZE + 1) {
        bucket_size[bid]--;
        std::cout << "bucket overflows" << std::endl;
        return false;
      }
      if ((bid >> SAMPLEBITNUM) == 0)
        if (!((bloom_arr[bid >> SHIFT] >> (bid & MASK)) & 1))
          _1_num++;
      bloom_arr[bid >> SHIFT] |= (1 << (bid & MASK));

      lock[pos & (LOCK_NUM - 1)].unlock();

      if (((pos & 0x3FF) == 0) && seed == 0)
        if (_1_num >= ExpandBitNum && expandOrNot)
          expand();
    }

    return true;
  }

  void insert_with_duplicates(const T &item) {
    const double start = get_current_time_in_seconds();
    uint32_t pos[hash_num];
    for (int seed = 0; seed < hash_num; ++seed)
      pos[seed] = hash(item, seed);

    bool inBF = true;
    for (int i = 0; i < hash_num; ++i) {
      uint32_t bid = pos[i] & POSMASK;

      // lock[bid & (LOCK_NUM - 1)].lock();
      bool s = (bloom_arr[bid >> SHIFT] >> (bid & MASK)) & 1;

      // lock[bid & (LOCK_NUM - 1)].unlock();
      if (s == 0) {
        inBF = false;
        break;
      }
    }
    total_addressing_time += get_current_time_in_seconds() - start;

    if (inBF == true) {
      int i;
      for (i = std::min(32 / (32 - sizelog), hash_num) - 1; i >= 0; --i) {
        uint32_t bid = pos[i] & POSMASK;
        lock[bid & (LOCK_NUM - 1)].lock();

        lazy_update(0, bid);

        int j;
        for (j = bucket_size[bid]; j >= 1; --j)
          if (finger_buckets[bid][j] == (pos[i] >> SIZELOG))
            break;

        lock[bid & (LOCK_NUM - 1)].unlock();
        if (j == 0)
          break;
      }
      if (i == -1)
        return;
    }

    for (int i = 0; i < hash_num; ++i) {
      uint32_t bid = pos[i] & POSMASK;

      lock[bid & (LOCK_NUM - 1)].lock();

      lazy_update(0, bid);

      bucket_fplen[bid] = FINGER_LENGTH;
      finger_buckets[bid][++bucket_size[bid]] = pos[i] >> SIZELOG;
      // std::cout << "INS " << bid << std::endl;
      if (bucket_size[bid] == BUCKET_SIZE + 1) {
        bucket_size[bid]--;
        std::cout << "bucket overflows";
      }

      // set Bloom filter
      if ((bid >> SAMPLEBITNUM) == 0)
        if (!((bloom_arr[bid >> SHIFT] >> (bid & MASK)) & 1))
          _1_num++;
      bloom_arr[bid >> SHIFT] |= (1 << (bid & MASK));
      lock[bid & (LOCK_NUM - 1)].unlock();
    }

    if (((pos[0] & 0x3FF) == 0))
      if (_1_num >= ExpandBitNum && expandOrNot)
        expand();

    return;
  }

  /**
   * Query the existence of a key.
   */
  auto query(const T &item) -> bool {
    for (int seed = 0; seed < hash_num; ++seed) {
      const double start = get_current_time_in_seconds();
      uint32_t pos = hash(item, seed);
      uint32_t bid = pos & POSMASK;
      total_addressing_time += get_current_time_in_seconds() - start;

      // lock[pos & (LOCK_NUM - 1)].lock();
      bool result = bloom_arr[bid >> SHIFT] & (1 << (bid & MASK));
      // lock[pos & (LOCK_NUM - 1)].unlock();

      if (!result)
        return false;
    }
    return true;
  }

  void deleteEle(const T &item) {
    int i;
    for (int seed = 0; seed < hash_num; seed++) {
      const double start = get_current_time_in_seconds();
      uint32_t pos = hash(item, seed);
      uint32_t bid = pos & POSMASK;
      total_addressing_time += get_current_time_in_seconds() - start;
      // lock[bid & (LOCK_NUM - 1)].lock();
      lazy_update(0, bid);
      int j;
      for (j = bucket_size[bid]; j >= 1; --j)
        if (finger_buckets[bid][j] == (pos >> SIZELOG)) {
          std::swap(finger_buckets[bid][j], finger_buckets[bid][bucket_size[bid]]);
          bucket_size[bid]--;
          break;
        }
      // if (j == 0)
      //   std::cout << "Deletion Error in bf!" << std::endl;
      if (j != 0)
        if (bucket_size[bid] == 0) {
          if (((bloom_arr[bid >> SHIFT] >> (bid & MASK)) & 1) == 0) {
            // printf("ERROR2\n");
          } else {
            bloom_arr[bid >> SHIFT] &= ~(1u << (bid & MASK));
            if ((bid >> SAMPLEBITNUM) == 0)
              _1_num--;
          }
        }
      // lock[bid & (LOCK_NUM - 1)].unlock();
    }
    return;
  }

  /**
   * Expand the buckets.
   */
  void expand() {
    if (_1_num >= ExpandBitNum && expandOrNot) {
      // std::cout << "EBF expansion." << std::endl;
      // std::cout << "\t Bloom sizelog" << sizelog << "\t _1_num:" << _1_num
      //           << "\tExpandBitNum : " << ExpandBitNum << std::endl;

      if (sizelog == MAX_BLOOM_SIZE) {
        std::cout << "EBF expansion ERROR. No enough bucket space." << std::endl;
      }
      assert((MAX_SIZE - finger_length + 1) > MIN_SIZE && (finger_length - 1) > 0);

      const double start = get_current_time_in_seconds();

      for (int i = LOCK_NUM - 1; i >= 0; i--)
        lock[i & (LOCK_NUM - 1)].lock();

      uint32_t oldsize = size;
      char *old_bloom_arr = bloom_arr;

      char *new_arr = new char[(size << 1) >> 3];
      memcpy(new_arr, bloom_arr, size >> 3);
      memcpy(new_arr + (size >> 3), bloom_arr, size >> 3);
      memcpy(&finger_buckets[size][0], &finger_buckets[0][0],
             size * (BUCKET_SIZE) * sizeof(finger_buckets[0][0]));
      memcpy(&bucket_size[size], &bucket_size[0], size * sizeof(bucket_size[0]));
      memcpy(&bucket_fplen[size], &bucket_fplen[0], size * sizeof(bucket_fplen[0]));

      /* reallocate the bit map */
      bloom_arr = new_arr;

      /* change the parameters */
      compression--;
      finger_length--;
      _1_num = 0;

      /* expand the buckets */
      size <<= 1;
      sizelog++;
      posmask = (1 << sizelog) - 1;
      ExpandBitNum = std::min(size, 1u << SAMPLEBITNUM) * EXPAND_THRESHOLD;

      // for (int i = 0; i < std::min(size, 1u << SAMPLEBITNUM); i++) {
      //   lazy_update(0, i);
      // }
      // std::cout << "expand end" << std::endl;

      // for (int i = (1u << SAMPLEBITNUM); i < size; i++) {
      //   // if (((bloom_arr[i >> SHIFT] >> (i & MASK)) & 1)) {
      //   // lock[i & (LOCK_NUM - 1)].lock();
      //   lazy_update(0, i);
      //   // lock[i & (LOCK_NUM - 1)].unlock();
      //   // }
      // }
      std::thread *th[50];
      for (int i = 0; i < THREAD_NUM; i++) {
        th[i] = new std::thread(lazy_update_range, size / THREAD_NUM * i,
                                size / THREAD_NUM * (i + 1), this);
      }
      for (int i = 0; i < THREAD_NUM; i++) {
        th[i]->join();
      }

      for (int i = LOCK_NUM - 1; i >= 0; i--)
        lock[i & (LOCK_NUM - 1)].unlock();

      delete[] old_bloom_arr;

      total_expansion_time += get_current_time_in_seconds() - start;

      // uint32_t local_1_num = 0;
      // for (int i = oldsize - 1; i >= 0; i--) {
      // 	// // std::cout << i << std::endl;
      // 	// lock[i & (LOCK_NUM - 1)].lock();

      // 	// if (sizelog == 22) std::cout << i << std::endl;
      // 	int BucketNum = finger_buckets[i]=;
      // 	// if(sizelog==22)//std::cout<<i<<" "<<BucketNum<<std::endl;
      // 	finger_buckets[i] = 0;
      // 	finger_buckets[i + oldsize] = 0;
      // 	// if(sizelog==22)//std::cout<<i<<" "<<BucketNum<<std::endl;
      // 	for (int j = 1; j <= BucketNum; j++){
      // 		// if(i==2097105)
      // 			// //std::cout<<j<<std::endl;
      // 		if (finger_buckets[i][j] & 1) {
      // 			finger_buckets[i + oldsize][++finger_buckets[i +
      // oldsize]] = finger_buckets[i][j]>>1;
      // 		}
      // 		else
      // 		{
      // 			finger_buckets[i][++finger_buckets[i]] =
      // finger_buckets[i][j]>>1;
      // 		}
      // 	}
      // 	if (finger_buckets[i])
      // 	{
      // 		bloom_arr[i >> SHIFT] |= (1 << (i & MASK));
      // #pragma opm atomic// 		local
      // _1_num++;
      // // 	}
      // // 	if (finger_buckets[i + oldsize])
      // // 	{
      // // 		bloom_arr[(i  + oldsize) >> SHIFT] |= (1 << ((i  +
      // oldsize) & MASK)); #pragma opm atomic// 		local _1_num++;
      // 	}
      // 	// lock[i & (LOCK_NUM - 1)].unlock();
      // }

      // _1_num = local_1_num;
      // // // std::cout << "\t Bloom sizelog" << sizelog << std::endl;
    }
  }

  /**
   * Compress the buckets.
   */
  void compress() {
    // if (_1_num <= ExpandBitNum / 4) {
    // std::cout << "EBF compress." << std::endl;
    // std::cout << "\t Bloom sizelog" << sizelog << "\t _1_num:" << _1_num
    //           << "\tExpandBitNum:" << ExpandBitNum << std::endl;

    for (int i = LOCK_NUM - 1; i >= 0; i--)
      lock[i & (LOCK_NUM - 1)].lock();

    uint32_t oldsize = size;
    char *old_bloom_arr = bloom_arr;

    char *new_arr = new char[(size >> 1) >> 3];

    for (int pos = 0; pos < size; pos++) {
      lazy_update(0, pos);
    }

    for (int i = ((size >> 3) >> 1) - 1; i >= 0; i--) {
      new_arr[i] = old_bloom_arr[i] | old_bloom_arr[i + ((size >> 3) >> 1)];
    }
    _1_num = 0;
    for (int pos = 0; pos < (size >> 1); pos++) {
      if (bucket_size[pos] + bucket_size[pos + (size >> 1)] > BUCKET_SIZE)
        std::cout << "Compression error!" << std::endl;
      for (int j = bucket_size[pos]; j >= 1; j--)
        finger_buckets[pos][j] <<= 1;
      for (int j = bucket_size[pos + (size >> 1)]; j >= 1; j--)
        finger_buckets[pos][j + bucket_size[pos]] = (finger_buckets[pos + (size >> 1)][j] << 1) | 1;
      bucket_size[pos] += bucket_size[pos + (size >> 1)];
      if (bucket_size[pos]) {
        if (bucket_fplen[pos] == 0)
          std::cout << "ERROR3\n";
        bucket_fplen[pos]++;
      }
      if ((pos >> SAMPLEBITNUM) == 0)
        if (bucket_size[pos]) {
          std::cout << pos << "AAAAAAAAAAAAAAAAA" << std::endl;
          _1_num++;
        }
    }

    /* reallocate the bit map */
    bloom_arr = new_arr;

    /* change the parameters */
    compression++;
    finger_length++;

    /* compress the buckets */
    size >>= 1;
    sizelog--;
    posmask = (1 << sizelog) - 1;
    ExpandBitNum = std::min(size, 1u << SAMPLEBITNUM) * EXPAND_THRESHOLD;

    for (int i = 0; i < std::min(size, 1u << SAMPLEBITNUM); i++) {
      lazy_update(0, i);
    }
    for (int i = LOCK_NUM - 1; i >= 0; i--)
      lock[i & (LOCK_NUM - 1)].unlock();

    delete[] old_bloom_arr;
    for (int i = (1u << SAMPLEBITNUM); i < size; i++) {
      if (((bloom_arr[i >> SHIFT] >> (i & MASK)) & 1)) {
        // lock[i & (LOCK_NUM - 1)].lock();
        lazy_update(0, i);
        // lock[i & (LOCK_NUM - 1)].unlock();
      }
    }
    // }
  }

  /**
   * get the number of 0 bits
   */
  inline auto get_0_num() -> uint32_t {
    uint32_t ans = 0;
    for (int i = 0; i < size; ++i) {
      if (!getbit(i))
        ++ans;
    }
    return ans;
  }
  /**
   * get the number of 1 bits
   */
  inline auto get_1_num() -> int { return _1_num; }
  /**
   * get the size of bitmap
   */
  inline auto get_size() -> uint32_t { return size; }

  void print() {
    std::cout << "myprint::::::::::::::::::\n";
    std::cout << "size " << size << std::endl;
    std::cout << "sizelog " << sizelog << std::endl;
    std::cout << "posmask " << posmask << std::endl;
    std::cout << "ExpandBitNum " << ExpandBitNum << std::endl;
    std::cout << "compression " << compression << std::endl;
    std::cout << "_1_num " << _1_num << std::endl;
    std::cout << "myprint------------------\n";
  }

private:
  /**
   * Get a bit in the bit map.
   */
  inline auto getbit(uint32_t pos) -> bool {
    if (bloom_arr[pos >> SHIFT] & (1 << (pos & MASK)))
      return true;
    return false;
  }
  /**
   * Set a bit in the bit map.
   */
  inline void setbit(uint32_t pos) { bloom_arr[pos >> SHIFT] |= (1 << (pos & MASK)); }
  /**
   * Reset a bit in the bit map.
   */
  inline void resetbit(uint32_t pos) { bloom_arr[pos >> SHIFT] &= ~(1 << (pos & MASK)); }

  static void lazy_update_range(uint32_t bidl, uint32_t bidr, ElasticBloomFilter *ebf) {
    for (int bid = bidl; bid < bidr; bid++) {

      if (((ebf->bloom_arr[bid >> SHIFT] >> (bid & MASK)) & 1) && ebf->bucket_size[bid] &&
          ebf->bucket_fplen[bid] > ebf->finger_length) {

        uint32_t bs = ebf->bucket_size[bid], dlen = (ebf->bucket_fplen[bid] - ebf->finger_length),
                 t = (1 << dlen) - 1, cnt = 0;

        for (int j = 1; j <= bs; j++)
          if ((ebf->finger_buckets[bid][j] & t) == (bid >> (ebf->sizelog - dlen)))
            ebf->finger_buckets[bid][++cnt] = ebf->finger_buckets[bid][j] >> dlen;
        ebf->bucket_size[bid] = cnt;
        ebf->bucket_fplen[bid] = (cnt ? ebf->finger_length : 0);

        if (!cnt) {
          ebf->bloom_arr[bid >> SHIFT] &= ~(1u << (bid & MASK));
        } else {
          ebf->bloom_arr[bid >> SHIFT] |= (1u << (bid & MASK));
          if ((bid >> SAMPLEBITNUM) == 0)
            ebf->_1_num++;
        }
      }
    }
  }
};

} // namespace elasticbloomfilter
