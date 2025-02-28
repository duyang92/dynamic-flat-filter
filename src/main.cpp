/*
 * An example of how to use DFF.
 *
 * For benchmarks, see the `benchmark/` directory.
 */

#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <random>

#include <spdlog/spdlog.h>

#include "DFF.hpp"
#include "segment.hpp"

using namespace std::literals;

using namespace dff;

constexpr size_t INSERT_CAP = 1 << 22;
constexpr size_t GENERATE_NUM = 1 << 23;

uint64_t test_nums[GENERATE_NUM];

void random_gen(size_t n, uint64_t *store) {
  std::mt19937 rd(12821);
  const auto rand_range = static_cast<uint64_t>(std::pow(2, 64) / static_cast<double>(n));
  for (size_t i = 0; i < n; i++) {
    const uint64_t rand = rand_range * i + rd() % rand_range;
    store[i] = rand;
  }
}

[[nodiscard]] constexpr auto stringify_status(const Status status) -> const char * {
  return status == Ok               ? "Ok"
         : status == NotFound       ? "NotFound"
         : status == NotEnoughSpace ? "NotEnoughSpace"
                                    : "NotSupported";
}

auto main() -> int {
  dff::DFF<uint64_t> filter(16);
  random_gen(GENERATE_NUM, test_nums);

  spdlog::info("Insert cap: {}", INSERT_CAP);

  // Insert
  size_t insert_count = 0;
  const auto insert_start = std::chrono::high_resolution_clock::now();
  for (size_t i = 0; i < INSERT_CAP; i++) {
    const uint64_t item = test_nums[i];
    if (filter.insert(item) != Ok) {
      spdlog::warn("Failed to insert item {}", item);
      break;
    }
    insert_count++;
  }
  const auto insert_end = std::chrono::high_resolution_clock::now();

  std::chrono::duration<double> insert_duration = insert_end - insert_start;

  spdlog::info("Inserted {} items in {:.4f} seconds", insert_count, insert_duration.count());
  spdlog::info("Insert success rate: {:.2f}%",
               static_cast<double>(insert_count) * 100.0 / INSERT_CAP);
  spdlog::info("Insert throughput: {:.2f} Mops/s",
               static_cast<double>(insert_count) / insert_duration.count() / 1'000'000.0);

  // Query
  size_t query_count = 0;
  const auto query_start = std::chrono::high_resolution_clock::now();
  for (size_t i = 0; i < insert_count; i++) {
    const uint64_t item = test_nums[i];
    const Status res = filter.query(item);
    if (res != Ok) {
      // Should always be Ok, because we cannot tolerate false negatives
      spdlog::error("Failed to query item {} at i={} ({})", test_nums[i], i, stringify_status(res));
      continue;
    }
    query_count++;
  }
  const auto query_end = std::chrono::high_resolution_clock::now();

  std::chrono::duration<double> query_duration = query_end - query_start;

  spdlog::info("Queried {} items in {:.4f} seconds", insert_count, query_duration.count());
  spdlog::info("Query success rate: {:.2f}%",
               static_cast<double>(query_count) * 100.0 / static_cast<double>(insert_count));
  spdlog::info("Query throughput: {:.2f} Mops/s",
               static_cast<double>(query_count) / query_duration.count() / 1'000'000.0);

  // False positive rate
  size_t false_positive_query = 0;
  for (size_t i = INSERT_CAP; i < GENERATE_NUM; i++) {
    const uint64_t item = test_nums[i];
    if (filter.query(item) == Ok)
      false_positive_query++;
  }
  spdlog::info("False positive rate: {}%",
               static_cast<double>(false_positive_query) * 100.0 / (GENERATE_NUM - INSERT_CAP));

  // Remove
  size_t remove_count = 0;
  const auto remove_start = std::chrono::high_resolution_clock::now();
  for (size_t i = 0; i < insert_count; i++) {
    const uint64_t item = test_nums[i];
    if (filter.remove(item) != Ok) {
      spdlog::error("Failed to remove item {}", item);
      continue;
    }
    remove_count++;
  }
  const auto remove_end = std::chrono::high_resolution_clock::now();

  std::chrono::duration<double> remove_duration = remove_end - remove_start;

  spdlog::info("Removed {} items in {:.4f} seconds", remove_count, remove_duration.count());
  spdlog::info("Remove success rate: {:.2f}%",
               static_cast<double>(remove_count) * 100.0 / static_cast<double>(insert_count));
  spdlog::info("Remove throughput: {:.2f} Mops/s",
               static_cast<double>(remove_count) / remove_duration.count() / 1'000'000.0);

  return 0;
}
