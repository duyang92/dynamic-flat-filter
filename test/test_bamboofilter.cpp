#include <cmath>
#include <cstddef>
#include <cstdint>
#include <random>

#include <catch2/catch_test_macros.hpp>

#include "../benchmark/impl/bamboofilter/bamboofilter.hpp"

using bamboofilter::BambooFilter;

constexpr size_t INSERT_NUM = 300'000;
constexpr size_t INITIAL_CAPACITY = 1 << 16;

// generate the integers
inline void random_gen(size_t n, uint64_t *store) {
  std::mt19937 rd(12821);
  const auto rand_range = static_cast<uint64_t>(std::pow(2, 64) / static_cast<double>(n));
  for (size_t i = 0; i < n; i++) {
    uint64_t rand = rand_range * i + rd() % rand_range;
    store[i] = rand;
  }
}

TEST_CASE("BambooFilter should perform insertions/query/deletion correctly", "[bamboofilter]") {
  constexpr size_t GENERATE_NUM = INSERT_NUM * 2;
  auto *nums = new uint64_t[GENERATE_NUM];
  random_gen(GENERATE_NUM, nums);

  // Insert
  bamboofilter::BambooFilter<uint64_t> filter(INITIAL_CAPACITY, 4);
  for (size_t i = 0; i < INSERT_NUM; i++)
    REQUIRE(filter.Insert(nums[i]));

  SECTION("No false negative should be found after insertion") {
    for (size_t i = 0; i < INSERT_NUM; i++)
      REQUIRE(filter.Lookup(nums[i]));
  }

  SECTION("Should have some false positive, but not too many") {
    size_t false_positive = 0;
    for (size_t i = 0; i < INSERT_NUM; i++)
      if (filter.Lookup(nums[INSERT_NUM + i]))
        false_positive++;
    REQUIRE(false_positive > 0);
    REQUIRE(static_cast<double>(false_positive) / static_cast<double>(INSERT_NUM) < 0.1);
  }

  SECTION("Deletion should work correctly") {
    for (size_t i = 0; i < INSERT_NUM; i++)
      REQUIRE(filter.Delete(nums[i]));

    // Should have 0 false positive since we deleted all the inserted elements
    size_t false_positive = 0;
    for (size_t i = 0; i < INSERT_NUM; i++)
      if (filter.Lookup(nums[i]))
        false_positive++;
    REQUIRE(false_positive == 0);

    // Then insert back some elements
    constexpr size_t INSERT_BACK_NUM = INSERT_NUM * 0.5;
    for (size_t i = 0; i < INSERT_BACK_NUM; i++)
      REQUIRE(filter.Insert(nums[INSERT_NUM + i]));

    // Should have some false positive, but not too many
    false_positive = 0;
    for (size_t i = 0; i < INSERT_NUM; i++)
      if (filter.Lookup(nums[i]))
        false_positive++;
    REQUIRE(false_positive > 0);
    REQUIRE(static_cast<double>(false_positive) / static_cast<double>(INSERT_NUM) < 0.1);
  }

  delete[] nums;
}
