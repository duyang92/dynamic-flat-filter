#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <stdexcept>

#include <fmt/core.h>

#include "../../src/DFF.hpp"
#include "../impl/bamboofilter/bamboofilter.hpp"
#include "../impl/compactedlogarithmicdynamiccuckoofilter/compactedlogarithmicdynamiccuckoofilter.hpp"
#include "../impl/dynamicbloomfilter/dynamicbloomfilter.hpp"
#include "../impl/dynamiccuckoofilter/dynamiccuckoofilter.hpp"
#include "../impl/elasticbloomfilter/elasticbloomfilter.hpp"
#include "../impl/infinifilter/chained_infinifilter.hpp"
#include "../predefine.hpp"
#include "benchmark_utils.hpp"

REGISTER_BENCHMARK_TASK(DFF) {
  auto *filter = new dff::DFF<uint64_t, false, false, true>(16);

  // Insert
  for (size_t i = 0; i < n; i++) {
    if (filter->insert(nums[i]) != dff::Ok) {
      const std::string msg = fmt::format(
          "Insertion failed: Unable to insert element {} at index {}/{}", nums[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }

  // Test positive query
  for (size_t i = 0; i < n; i++) {
    if (filter->query(nums[i]) != dff::Ok) {
      const std::string msg =
          fmt::format("Query failed (false negative): Unable to find element {} at index {}/{}",
                      nums[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }

  return filter->total_addressing_time;
}

REGISTER_BENCHMARK_TASK(DFF_FG) {
  auto *filter = new dff::DFF<uint64_t, true, false, true>(16);

  // Insert
  for (size_t i = 0; i < n; i++) {
    if (filter->insert(nums[i]) != dff::Ok) {
      const std::string msg = fmt::format(
          "Insertion failed: Unable to insert element {} at index {}/{}", nums[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }

  // Test positive query
  for (size_t i = 0; i < n; i++) {
    if (filter->query(nums[i]) != dff::Ok) {
      const std::string msg =
          fmt::format("Query failed (false negative): Unable to find element {} at index {}/{}",
                      nums[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }

  return filter->total_addressing_time;
}

REGISTER_BENCHMARK_TASK(IFF) {
  infinifilter::ChainedInfiniFilter filter(6, 16 + /* flag bits */ 3);

  filter.set_expand_autonomously(true);

  // Insert
  for (size_t i = 0; i < n; i++) {
    if (!filter.Filter::insert(nums[i])) {
      const std::string msg =
          fmt::format("Insertion failed: Unable to insert {} at index {}/{}", nums[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }

  // Test positive query
  for (size_t i = 0; i < n; i++) {
    if (!filter.Filter::query(nums[i])) {
      const std::string msg = fmt::format(
          "Query failed (false negative): Unable to find {} at index {}/{}", nums[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }

  return filter.total_addressing_time;
}

REGISTER_BENCHMARK_TASK(BBF) {
  bamboofilter::BambooFilter<uint64_t> filter(initial_capacity, 4);

  // Insert
  for (size_t i = 0; i < n; i++) {
    if (!filter.Insert(nums[i])) {
      const std::string msg =
          fmt::format("Insertion failed: Unable to insert {} at index {}/{}", nums[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }

  // Test positive query
  for (size_t i = 0; i < n; i++) {
    if (!filter.Lookup(nums[i])) {
      const std::string msg = fmt::format(
          "Query failed (false negative): Unable to find {} at index {}/{}", nums[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }

  return filter.total_addressing_time();
}

REGISTER_BENCHMARK_TASK(EBF) {
  auto *filter =
      new elasticbloomfilter::ElasticBloomFilter<uint64_t>(16 /* 32 - 16fp_len */, 4, true);

  // Insert
  for (size_t i = 0; i < n; i++) {
    if (!filter->insert(nums[i])) {
      const std::string msg =
          fmt::format("Insertion failed: Unable to insert {} at index {}/{}", nums[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }

  // Test positive query
  for (size_t i = 0; i < n; i++) {
    if (!filter->query(nums[i])) {
      const std::string msg = fmt::format(
          "Query failed (false negative): Unable to find {} at index {}/{}", nums[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }

  const double res = filter->total_addressing_time;

  delete filter;

  return res;
}

REGISTER_BENCHMARK_TASK(LDCF) {
  compactedlogarithmicdynamiccuckoofilter::CompactedLogarithmicDynamicCuckooFilter<uint64_t> filter(
      initial_capacity, 16);

  // Insert
  for (size_t i = 0; i < n; i++) {
    if (!filter.insertItem(nums[i])) {
      const std::string msg =
          fmt::format("Insertion failed: Unable to insert {} at index {}/{}", nums[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }

  // Test positive query
  for (size_t i = 0; i < n; i++) {
    if (!filter.queryItem(nums[i])) {
      const std::string msg = fmt::format(
          "Query failed (false negative): Unable to find {} at index {}/{}", nums[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }

  return filter.total_addressing_time;
}

REGISTER_BENCHMARK_TASK(DCF) {
  dynamiccuckoofilter::DynamicCuckooFilter<uint64_t> filter(
      initial_capacity /* 4 slots/bucket */ >> 2, 16);

  // Insert
  for (size_t i = 0; i < n; i++) {
    if (!filter.insertItem(nums[i])) {
      const std::string msg =
          fmt::format("Insertion failed: Unable to insert {} at index {}/{}", nums[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }

  // Test positive query
  for (size_t i = 0; i < n; i++) {
    if (!filter.queryItem(nums[i])) {
      const std::string msg = fmt::format(
          "Query failed (false negative): Unable to find {} at index {}/{}", nums[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }

  return filter.total_addressing_time;
}

REGISTER_BENCHMARK_TASK(DBF) {
  dynamicbloomfilter::DynamicBloomFilter<uint64_t> filter(initial_capacity, LINK_FP, 1);

  // Insert
  for (size_t i = 0; i < n; i++) {
    if (!filter.insertItem(nums[i])) {
      const std::string msg =
          fmt::format("Insertion failed: Unable to insert {} at index {}/{}", nums[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }

  // Test positive query
  for (size_t i = 0; i < n; i++) {
    if (!filter.queryItem(nums[i])) {
      const std::string msg = fmt::format(
          "Query failed (false negative): Unable to find {} at index {}/{}", nums[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }

  return filter.total_addressing_time;
}

BENCHMARK_TASK_MAIN
