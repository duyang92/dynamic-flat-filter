#include <cstddef>
#include <cstdint>
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
  dff::DFF<uint64_t, false> filter(16);

  const double start = get_current_time_in_seconds();

  for (size_t i = 0; i < n; i++) {
    if (filter.insert(nums[i]) != dff::Ok) {
      const std::string msg =
          fmt::format("Insertion failed: Unable to insert {} at index {}/{}", nums[i], i, n - 1);
      throw std::runtime_error(msg);
    }
    if (i > 3) {
      if (filter.query(nums[i]) != dff::Ok) {
        const std::string msg =
            fmt::format("Query failed (false negative): Unable to find element {} at index {}/{}",
                        nums[i], i, n - 1);
        throw std::runtime_error(msg);
      }
      if (filter.query(nums[i - 1]) != dff::Ok) {
        const std::string msg =
            fmt::format("Query failed (false negative): Unable to find element {} at index {}/{}",
                        nums[i - 1], i - 1, n - 1);
        throw std::runtime_error(msg);
      }
      if (filter.query(nums[i - 2]) != dff::Ok) {
        const std::string msg =
            fmt::format("Query failed (false negative): Unable to find element {} at index {}/{}",
                        nums[i - 2], i - 2, n - 1);
        throw std::runtime_error(msg);
      }
    }
  }

  for (size_t i = 0; i < n / 10; i++) {
    if (filter.remove(nums[i]) != dff::Ok) {
      const std::string msg =
          fmt::format("Deletion failed: Unable to delete {} at index {}/{}", nums[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }

  const double end = get_current_time_in_seconds();
  return end - start;
}

REGISTER_BENCHMARK_TASK(DFF_FG) {
  dff::DFF<uint64_t, true> filter(16);

  const double start = get_current_time_in_seconds();

  for (size_t i = 0; i < n; i++) {
    if (filter.insert(nums[i]) != dff::Ok) {
      const std::string msg =
          fmt::format("Insertion failed: Unable to insert {} at index {}/{}", nums[i], i, n - 1);
      throw std::runtime_error(msg);
    }
    if (i > 3) {
      if (filter.query(nums[i]) != dff::Ok) {
        const std::string msg =
            fmt::format("Query failed (false negative): Unable to find element {} at index {}/{}",
                        nums[i], i, n - 1);
        throw std::runtime_error(msg);
      }
      if (filter.query(nums[i - 1]) != dff::Ok) {
        const std::string msg =
            fmt::format("Query failed (false negative): Unable to find element {} at index {}/{}",
                        nums[i - 1], i - 1, n - 1);
        throw std::runtime_error(msg);
      }
      if (filter.query(nums[i - 2]) != dff::Ok) {
        const std::string msg =
            fmt::format("Query failed (false negative): Unable to find element {} at index {}/{}",
                        nums[i - 2], i - 2, n - 1);
        throw std::runtime_error(msg);
      }
    }
  }

  for (size_t i = 0; i < n / 10; i++) {
    if (filter.remove(nums[i]) != dff::Ok) {
      const std::string msg =
          fmt::format("Deletion failed: Unable to delete {} at index {}/{}", nums[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }

  const double end = get_current_time_in_seconds();
  return end - start;
}

REGISTER_BENCHMARK_TASK(IFF) {
  infinifilter::ChainedInfiniFilter filter(6, 16 + /* flag bits */ 3);

  filter.set_expand_autonomously(true);

  const double start = get_current_time_in_seconds();

  for (size_t i = 0; i < n; i++) {
    if (!filter.Filter::insert(nums[i])) {
      const std::string msg =
          fmt::format("Insertion failed: Unable to insert {} at index {}/{}", nums[i], i, n - 1);
      throw std::runtime_error(msg);
    }
    if (i > 3) {
      if (!filter.Filter::query(nums[i])) {
        const std::string msg =
            fmt::format("Query failed (false negative): Unable to find element {} at index {}/{}",
                        nums[i], nums[i], i, n - 1);
        throw std::runtime_error(msg);
      }
      if (!filter.Filter::query(nums[i - 1])) {
        const std::string msg =
            fmt::format("Query failed (false negative): Unable to find element {} at index {}/{}",
                        nums[i - 1], i - 1, n - 1);
        throw std::runtime_error(msg);
      }
      if (!filter.Filter::query(nums[i - 2])) {
        const std::string msg =
            fmt::format("Query failed (false negative): Unable to find element {} at index {}/{}",
                        nums[i - 2], i - 2, n - 1);
        throw std::runtime_error(msg);
      }
    }
  }

  for (size_t i = 0; i < n / 10; i++) {
    if (!filter.Filter::remove(nums[i])) {
      const std::string msg =
          fmt::format("Deletion failed: Unable to delete {} at index {}/{}", nums[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }

  const double end = get_current_time_in_seconds();
  return end - start;
}

REGISTER_BENCHMARK_TASK(BBF) {
  bamboofilter::BambooFilter<uint64_t> filter(initial_capacity, 4);

  const double start = get_current_time_in_seconds();

  for (size_t i = 0; i < n; i++) {
    if (!filter.Insert(nums[i])) {
      const std::string msg =
          fmt::format("Insertion failed: Unable to insert {} at index {}/{}", nums[i], i, n - 1);
      throw std::runtime_error(msg);
    }
    if (i > 3) {
      if (!filter.Lookup(nums[i])) {
        const std::string msg =
            fmt::format("Query failed (false negative): Unable to find element {} at index {}/{}",
                        nums[i], i, n - 1);
        throw std::runtime_error(msg);
      }
      if (!filter.Lookup(nums[i - 1])) {
        const std::string msg =
            fmt::format("Query failed (false negative): Unable to find element {} at index {}/{}",
                        nums[i - 1], i - 1, n - 1);
        throw std::runtime_error(msg);
      }
      if (!filter.Lookup(nums[i - 2])) {
        const std::string msg =
            fmt::format("Query failed (false negative): Unable to find element {} at index {}/{}",
                        nums[i - 2], i - 2, n - 1);
        throw std::runtime_error(msg);
      }
    }
  }

  for (size_t i = 0; i < n / 10; i++) {
    if (!filter.Delete(nums[i])) {
      const std::string msg =
          fmt::format("Deletion failed: Unable to delete {} at index {}/{}", nums[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }

  const double end = get_current_time_in_seconds();
  return end - start;
}

REGISTER_BENCHMARK_TASK(EBF) {
  auto *filter =
      new elasticbloomfilter::ElasticBloomFilter<uint64_t>(16 /* 32 - 16fp_len */, 4, true);

  const double start = get_current_time_in_seconds();

  for (size_t i = 0; i < n; i++) {
    if (!filter->insert(nums[i])) {
      const std::string msg =
          fmt::format("Insertion failed: Unable to insert {} at index {}/{}", nums[i], i, n - 1);
      throw std::runtime_error(msg);
    }
    if (i > 3) {
      if (!filter->query(nums[i])) {
        const std::string msg =
            fmt::format("Query failed (false negative): Unable to find element {} at index {}/{}",
                        nums[i], i, n - 1);
        throw std::runtime_error(msg);
      }
      if (!filter->query(nums[i - 1])) {
        const std::string msg =
            fmt::format("Query failed (false negative): Unable to find element {} at index {}/{}",
                        nums[i - 1], i - 1, n - 1);
        throw std::runtime_error(msg);
      }
      if (!filter->query(nums[i - 2])) {
        const std::string msg =
            fmt::format("Query failed (false negative): Unable to find element {} at index {}/{}",
                        nums[i - 2], i - 2, n - 1);
        throw std::runtime_error(msg);
      }
    }
  }

  for (size_t i = 0; i < n / 10; i++) {
    filter->deleteEle(nums[i]);
  }

  const double end = get_current_time_in_seconds();

  delete filter;

  return end - start;
}

REGISTER_BENCHMARK_TASK(LDCF) {
  compactedlogarithmicdynamiccuckoofilter::CompactedLogarithmicDynamicCuckooFilter<uint64_t> filter(
      initial_capacity, 16);

  const double start = get_current_time_in_seconds();

  for (size_t i = 0; i < n; i++) {
    if (!filter.insertItem(nums[i])) {
      const std::string msg =
          fmt::format("Insertion failed: Unable to insert {} at index {}/{}", nums[i], i, n - 1);
      throw std::runtime_error(msg);
    }

    if (i > 3) {
      if (!filter.queryItem(nums[i])) {
        const std::string msg =
            fmt::format("Query failed (false negative): Unable to find element {} at index {}/{}",
                        nums[i], i, n - 1);
        throw std::runtime_error(msg);
      }
      if (!filter.queryItem(nums[i - 1])) {
        const std::string msg =
            fmt::format("Query failed (false negative): Unable to find element {} at index {}/{}",
                        nums[i - 1], i - 1, n - 1);
        throw std::runtime_error(msg);
      }
      if (!filter.queryItem(nums[i - 2])) {
        const std::string msg =
            fmt::format("Query failed (false negative): Unable to find element {} at index {}/{}",
                        nums[i - 2], i - 2, n - 1);
        throw std::runtime_error(msg);
      }
    }
  }

  for (size_t i = 0; i < n / 10; i++) {
    if (!filter.deleteItem(nums[i])) {
      const std::string msg =
          fmt::format("Deletion failed: Unable to delete {} at index {}/{}", nums[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }

  const double end = get_current_time_in_seconds();
  return end - start;
}

REGISTER_BENCHMARK_TASK(DCF) {
  dynamiccuckoofilter::DynamicCuckooFilter<uint64_t> filter(
      initial_capacity /* 4 slots/bucket */ >> 2, 16);

  const double start = get_current_time_in_seconds();

  for (size_t i = 0; i < n; i++) {
    if (!filter.insertItem(nums[i])) {
      const std::string msg =
          fmt::format("Insertion failed: Unable to insert {} at index {}/{}", nums[i], i, n - 1);
      throw std::runtime_error(msg);
    }
    if (i > 3) {
      if (!filter.queryItem(nums[i])) {
        const std::string msg =
            fmt::format("Query failed (false negative): Unable to find element {} at index {}/{}",
                        nums[i], i, n - 1);
        throw std::runtime_error(msg);
      }
      if (!filter.queryItem(nums[i - 1])) {
        const std::string msg =
            fmt::format("Query failed (false negative): Unable to find element {} at index {}/{}",
                        nums[i - 1], i - 1, n - 1);
        throw std::runtime_error(msg);
      }
      if (!filter.queryItem(nums[i - 2])) {
        const std::string msg =
            fmt::format("Query failed (false negative): Unable to find element {} at index {}/{}",
                        nums[i - 2], i - 2, n - 1);
        throw std::runtime_error(msg);
      }
    }
  }

  for (size_t i = 0; i < n / 10; i++) {
    if (!filter.deleteItem(nums[i])) {
      const std::string msg =
          fmt::format("Deletion failed: Unable to delete {} at index {}/{}", nums[i], i, n - 1);
      throw std::runtime_error(msg);
    }
  }

  const double end = get_current_time_in_seconds();
  return end - start;
}

REGISTER_BENCHMARK_TASK(DBF) {
  dynamicbloomfilter::DynamicBloomFilter<uint64_t> filter(initial_capacity, LINK_FP, 1);

  const double start = get_current_time_in_seconds();

  for (size_t i = 0; i < n; i++) {
    if (!filter.insertItem(nums[i])) {
      const std::string msg =
          fmt::format("Insertion failed: Unable to insert {} at index {}/{}", nums[i], i, n - 1);
      throw std::runtime_error(msg);
    }
    if (i > 3) {
      if (!filter.queryItem(nums[i])) {
        const std::string msg =
            fmt::format("Query failed (false negative): Unable to find element {} at index {}/{}",
                        nums[i], i, n - 1);
        throw std::runtime_error(msg);
      }
      if (!filter.queryItem(nums[i - 1])) {
        const std::string msg =
            fmt::format("Query failed (false negative): Unable to find element {} at index {}/{}",
                        nums[i - 1], i - 1, n - 1);
        throw std::runtime_error(msg);
      }
      if (!filter.queryItem(nums[i - 2])) {
        const std::string msg =
            fmt::format("Query failed (false negative): Unable to find element {} at index {}/{}",
                        nums[i - 2], i - 2, n - 1);
        throw std::runtime_error(msg);
      }
    }
  }

  for (size_t i = 0; i < n / 10; i++) {
    if (!filter.deleteItem(nums[i])) {
      // DBF does not support reliable deletion, so no check here
    }
  }

  const double end = get_current_time_in_seconds();
  return end - start;
}

BENCHMARK_TASK_MAIN
