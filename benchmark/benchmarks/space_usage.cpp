#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <unordered_set>

#include <fmt/core.h>

#include "../../src/DFF.hpp"
#include "../../src/predefine.hpp"
#include "../impl/bamboofilter/bamboofilter.hpp"
#include "../impl/compactedlogarithmicdynamiccuckoofilter/compactedlogarithmicdynamiccuckoofilter.hpp"
#include "../impl/dynamicbloomfilter/dynamicbloomfilter.hpp"
#include "../impl/dynamiccuckoofilter/dynamiccuckoofilter.hpp"
#include "../impl/elasticbloomfilter/elasticbloomfilter.hpp"
#include "../impl/infinifilter/chained_infinifilter.hpp"
#include "../predefine.hpp"
#include "benchmark_utils.hpp"

REGISTER_BENCHMARK_TASK(DFF) {
  dff::DFF<uint64_t, false> filter(16);

  for (size_t i = 0; i < n; i++) {
    if (filter.insert(nums[i]) != dff::Ok) {
      const std::string msg =
          fmt::format("Insertion failed: Unable to insert {} at index {}/{}", nums[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }

  size_t bits_used = 0UZ;
  std::unordered_set<const dff::Segment<uint64_t, false> *> visited_segs;
  for (const dff::Segment<uint64_t, false> *seg : filter.lookup_table) {
    if (seg == nullptr)
      continue;
    if (visited_segs.contains(seg))
      continue;
    visited_segs.insert(seg);
    bits_used += dff::BUCKETS_PER_SEG * dff::SLOTS_PER_BUCKET * (seg->k_bits_per_item);
  }
  return static_cast<double>(bits_used);
}

REGISTER_BENCHMARK_TASK(DFF_FG) {
  dff::DFF<uint64_t, true> filter(16);

  for (size_t i = 0; i < n; i++) {
    if (filter.insert(nums[i]) != dff::Ok) {
      const std::string msg =
          fmt::format("Insertion failed: Unable to insert {} at index {}/{}", nums[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }

  size_t bits_used = 0UZ;
  std::unordered_set<const dff::Segment<uint64_t, true> *> visited_segs;
  for (const dff::Segment<uint64_t, true> *seg : filter.lookup_table) {
    if (seg == nullptr)
      continue;
    if (visited_segs.contains(seg))
      continue;
    visited_segs.insert(seg);
    bits_used += dff::BUCKETS_PER_SEG * dff::SLOTS_PER_BUCKET * (seg->k_bits_per_item + 1);
  }
  return static_cast<double>(bits_used);
}

REGISTER_BENCHMARK_TASK(IFF) {
  infinifilter::ChainedInfiniFilter filter(6, 16 + /* flag bits */ 3);

  filter.set_expand_autonomously(true);

  for (size_t i = 0; i < n; i++) {
    if (!filter.Filter::insert(nums[i])) {
      const std::string msg =
          fmt::format("Insertion failed: Unable to insert {} at index {}/{}", nums[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }

  size_t bits_used = filter.size() * filter.bit_per_entry;
  if (filter.secondary_if != nullptr)
    bits_used += filter.secondary_if->size() * filter.secondary_if->bit_per_entry;
  for (const auto &qf : filter.chain)
    bits_used += qf.size() * qf.bit_per_entry;
  return static_cast<double>(bits_used);
}

REGISTER_BENCHMARK_TASK(BBF) {
  bamboofilter::BambooFilter<uint64_t> filter(initial_capacity, 2);

  for (size_t i = 0; i < n; i++) {
    if (!filter.Insert(nums[i])) {
      const std::string msg =
          fmt::format("Insertion failed: Unable to insert {} at index {}/{}", nums[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }

  return static_cast<double>(filter.size() * 4 * 16);
}

REGISTER_BENCHMARK_TASK(EBF) {
  auto *filter =
      new elasticbloomfilter::ElasticBloomFilter<uint64_t>(16 /* 32 - 16fp_len */, 4, true);

  for (size_t i = 0; i < n; i++) {
    if (!filter->insert(nums[i])) {
      const std::string msg =
          fmt::format("Insertion failed: Unable to insert {} at index {}/{}", nums[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }

  const size_t res =
      /* finger_buckets */ static_cast<size_t>(filter->size) * BUCKET_SIZE * sizeof(uint16_t) * 8 +
      /* bucket_size */ static_cast<size_t>(filter->size) * sizeof(uint8_t) * 8 +
      /* bucket_fplen */ static_cast<size_t>(filter->size) * sizeof(uint8_t) * 8 +
      /* bloom_arr */ filter->size;

  delete filter;

  return static_cast<double>(res);
}

REGISTER_BENCHMARK_TASK(LDCF) {
  compactedlogarithmicdynamiccuckoofilter::CompactedLogarithmicDynamicCuckooFilter<uint64_t> filter(
      initial_capacity, 16);

  for (size_t i = 0; i < n; i++) {
    if (!filter.insertItem(nums[i])) {
      const std::string msg =
          fmt::format("Insertion failed: Unable to insert {} at index {}/{}", nums[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }

  return static_cast<double>(filter.listNum * initial_capacity * 16);
}

REGISTER_BENCHMARK_TASK(DCF) {
  dynamiccuckoofilter::DynamicCuckooFilter<uint64_t> filter(
      initial_capacity /* 4 slots/bucket */ >> 2, 16);

  for (size_t i = 0; i < n; i++) {
    if (!filter.insertItem(nums[i])) {
      const std::string msg =
          fmt::format("Insertion failed: Unable to insert {} at index {}/{}", nums[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }

  return static_cast<double>(filter.listNum * initial_capacity * 16);
}

REGISTER_BENCHMARK_TASK(DBF) {
  dynamicbloomfilter::DynamicBloomFilter<uint64_t> filter(initial_capacity, LINK_FP, 1);

  for (size_t i = 0; i < n; i++) {
    if (!filter.insertItem(nums[i])) {
      const std::string msg =
          fmt::format("Insertion failed: Unable to insert {} at index {}/{}", nums[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }

  return dynamicbloomfilter::LinkList::num * filter.bits_num;
}

BENCHMARK_TASK_MAIN
