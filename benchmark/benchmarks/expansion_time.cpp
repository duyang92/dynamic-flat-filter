#include <cstddef>
#include <cstdint>
#include <stdexcept>

#include <fmt/core.h>

#include "../../src/DFF.hpp"
#include "../impl/bamboofilter/bamboofilter.hpp"
#include "../impl/compactedlogarithmicdynamiccuckoofilter/compactedlogarithmicdynamiccuckoofilter.hpp"
#include "../impl/elasticbloomfilter/elasticbloomfilter.hpp"
#include "../impl/infinifilter/chained_infinifilter.hpp"
#include "benchmark_utils.hpp"

REGISTER_BENCHMARK_TASK(DFF) {
  dff::DFF<uint64_t, false, true> filter(16);

  for (size_t i = 0; i < n; i++) {
    if (filter.insert(nums[i]) != dff::Ok) {
      const std::string msg =
          fmt::format("Insertion failed: Unable to insert {} at index {}/{}", nums[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }

  return filter.total_expansion_time;
}

REGISTER_BENCHMARK_TASK(DFF_FG) {
  dff::DFF<uint64_t, true, true> filter(16);

  for (size_t i = 0; i < n; i++) {
    if (filter.insert(nums[i]) != dff::Ok) {
      const std::string msg =
          fmt::format("Insertion failed: Unable to insert {} at index {}/{}", nums[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }

  return filter.total_expansion_time;
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

  return filter.total_expansion_time;
}

REGISTER_BENCHMARK_TASK(BBF) {
  bamboofilter::BambooFilter<uint64_t> filter(initial_capacity, 4);

  for (size_t i = 0; i < n; i++) {
    if (!filter.Insert(nums[i])) {
      const std::string msg =
          fmt::format("Insertion failed: Unable to insert {} at index {}/{}", nums[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }

  return filter.total_expansion_time;
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

  const double res = filter->total_expansion_time;

  delete filter;

  return res;
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

  return filter.total_expansion_time;
}

BENCHMARK_TASK_MAIN
