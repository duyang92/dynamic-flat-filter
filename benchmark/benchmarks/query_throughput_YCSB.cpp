#include <cstddef>
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
#include "benchmark_utils_YCSB.hpp"

REGISTER_BENCHMARK_TASK(DFF) {
  dff::DFF<std::string, false> filter(16);

  for (size_t i = 0; i < n; i++) {
    if (filter.insert(lines[i]) != dff::Ok) {
      const std::string msg = fmt::format(
          "Insertion failed: Unable to insert line {} at index {}/{}", lines[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }

  const double start = get_current_time_in_seconds();
  for (size_t i = 0; i < n; i++) {
    if (filter.query(lines[i]) != dff::Ok) {
      const std::string msg = fmt::format(
          "Query failed (false negative): Unable to find {} at index {}/{}", lines[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }
  const double end = get_current_time_in_seconds();

  return end - start;
}

REGISTER_BENCHMARK_TASK(DFF_FG) {
  dff::DFF<std::string, true> filter(16);

  for (size_t i = 0; i < n; i++) {
    if (filter.insert(lines[i]) != dff::Ok) {
      const std::string msg = fmt::format(
          "Insertion failed: Unable to insert line {} at index {}/{}", lines[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }

  const double start = get_current_time_in_seconds();
  for (size_t i = 0; i < n; i++) {
    if (filter.query(lines[i]) != dff::Ok) {
      const std::string msg = fmt::format(
          "Query failed (false negative): Unable to find {} at index {}/{}", lines[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }
  const double end = get_current_time_in_seconds();

  return end - start;
}

REGISTER_BENCHMARK_TASK(IFF) {
  infinifilter::ChainedInfiniFilter filter(6, 16 + /* flag bits */ 3);

  filter.set_expand_autonomously(true);

  for (size_t i = 0; i < n; i++) {
    if (!filter.Filter::insert(lines[i])) {
      const std::string msg = fmt::format(
          "Insertion failed: Unable to insert line {} at index {}/{}", lines[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }

  const double start = get_current_time_in_seconds();
  for (size_t i = 0; i < n; i++) {
    if (!filter.Filter::query(lines[i])) {
      const std::string msg = fmt::format(
          "Query failed (false negative): Unable to find {} at index {}/{}", lines[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }
  const double end = get_current_time_in_seconds();

  return end - start;
}

REGISTER_BENCHMARK_TASK(BBF) {
  bamboofilter::BambooFilter<std::string> filter(initial_capacity, 4);

  for (size_t i = 0; i < n; i++) {
    if (!filter.Insert(lines[i])) {
      const std::string msg = fmt::format(
          "Insertion failed: Unable to insert line {} at index {}/{}", lines[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }

  const double start = get_current_time_in_seconds();
  for (size_t i = 0; i < n; i++) {
    if (!filter.Lookup(lines[i])) {
      const std::string msg = fmt::format(
          "Query failed (false negative): Unable to find {} at index {}/{}", lines[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }
  const double end = get_current_time_in_seconds();

  return end - start;
}

REGISTER_BENCHMARK_TASK(EBF) {
  auto *filter =
      new elasticbloomfilter::ElasticBloomFilter<std::string>(16 /* 32 - 16fp_len */, 4, true);

  for (size_t i = 0; i < n; i++) {
    if (!filter->insert(lines[i])) {
      const std::string msg = fmt::format(
          "Insertion failed: Unable to insert line {} at index {}/{}", lines[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }

  const double start = get_current_time_in_seconds();
  for (size_t i = 0; i < n; i++) {
    if (!filter->query(lines[i])) {
      const std::string msg = fmt::format(
          "Query failed (false negative): Unable to find {} at index {}/{}", lines[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }
  const double end = get_current_time_in_seconds();

  delete filter;

  return end - start;
}

REGISTER_BENCHMARK_TASK(LDCF) {
  compactedlogarithmicdynamiccuckoofilter::CompactedLogarithmicDynamicCuckooFilter<std::string>
      filter(initial_capacity, 16);

  for (size_t i = 0; i < n; i++) {
    if (!filter.insertItem(lines[i])) {
      const std::string msg = fmt::format(
          "Insertion failed: Unable to insert line {} at index {}/{}", lines[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }

  const double start = get_current_time_in_seconds();
  for (size_t i = 0; i < n; i++) {
    if (!filter.queryItem(lines[i])) {
      const std::string msg = fmt::format(
          "Query failed (false negative): Unable to find {} at index {}/{}", lines[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }
  const double end = get_current_time_in_seconds();

  return end - start;
}

REGISTER_BENCHMARK_TASK(DCF) {
  dynamiccuckoofilter::DynamicCuckooFilter<std::string> filter(
      initial_capacity /* 4 slots/bucket */ >> 2, 16);

  for (size_t i = 0; i < n; i++) {
    if (!filter.insertItem(lines[i])) {
      const std::string msg = fmt::format(
          "Insertion failed: Unable to insert line {} at index {}/{}", lines[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }

  const double start = get_current_time_in_seconds();
  for (size_t i = 0; i < n; i++) {
    if (!filter.queryItem(lines[i])) {
      const std::string msg = fmt::format(
          "Query failed (false negative): Unable to find {} at index {}/{}", lines[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }
  const double end = get_current_time_in_seconds();

  return end - start;
}

REGISTER_BENCHMARK_TASK(DBF) {
  dynamicbloomfilter::DynamicBloomFilter<std::string> filter(initial_capacity, LINK_FP, 1);

  for (size_t i = 0; i < n; i++) {
    if (!filter.insertItem(lines[i])) {
      const std::string msg = fmt::format(
          "Insertion failed: Unable to insert line {} at index {}/{}", lines[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }

  const double start = get_current_time_in_seconds();
  for (size_t i = 0; i < n; i++) {
    if (!filter.queryItem(lines[i])) {
      const std::string msg = fmt::format(
          "Query failed (false negative): Unable to find {} at index {}/{}", lines[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }
  const double end = get_current_time_in_seconds();

  return end - start;
}

BENCHMARK_TASK_MAIN
