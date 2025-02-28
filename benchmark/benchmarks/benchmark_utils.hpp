#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <functional>
#include <iostream>
#include <random>
#include <string>

#include <fmt/core.h>
#include <fplus/fplus.hpp>

inline size_t initial_capacity_log2;
inline size_t initial_capacity;

inline std::vector<std::string> benchmark_task_names;
inline std::vector<std::function<double(const uint64_t *, size_t)>> benchmark_task_functions;

#define REGISTER_BENCHMARK_TASK(name)                                                              \
  auto benchmark_task_##name(const uint64_t *nums, size_t n)->double;                              \
  static bool _benchmark_task_##name##_registered = [] {                                           \
    benchmark_task_names.emplace_back(#name);                                                      \
    benchmark_task_functions.emplace_back(benchmark_task_##name);                                  \
    return true;                                                                                   \
  }();                                                                                             \
  auto benchmark_task_##name(const uint64_t *nums, size_t n)->double

inline auto get_current_time_in_seconds() -> double {
  const auto now = std::chrono::high_resolution_clock::now();
  const auto duration = now.time_since_epoch();
  return std::chrono::duration_cast<std::chrono::duration<double>>(duration).count();
}

inline void random_gen(uint64_t *nums, const size_t n) {
  std::mt19937_64 gen(std::random_device{}());
  std::uniform_int_distribution<uint64_t> dis(0, std::numeric_limits<uint64_t>::max());
  for (size_t i = 0; i < n; ++i)
    nums[i] = dis(gen);
}

inline auto benchmark_task_main(int argc, char **argv) -> int {
  if (argc < 4) {
    std::cerr << "Usage: " << argv[0] << " {" << fplus::join_elem('|', benchmark_task_names)
              << "} <initial_capacity_log2> <element_count>" << std::endl;
    return 1;
  }

  try {
    const std::string name = argv[1];

    const auto it = std::ranges::find(benchmark_task_names, name);
    if (it == benchmark_task_names.end()) {
      std::cerr << "Unknown benchmark name: " << name << std::endl;
      return 1;
    }

    initial_capacity_log2 = std::stoul(argv[2]);
    initial_capacity = 1UZ << initial_capacity_log2;

    const size_t n = std::stoul(argv[3]);
    auto *nums = new uint64_t[n * 2];
    random_gen(nums, n * 2);

    const size_t index = std::distance(benchmark_task_names.begin(), it);
    std::cout << benchmark_task_functions[index](nums, n) << std::endl;

    delete[] nums;
  } catch (const std::exception &e) {
    std::cerr << e.what() << std::endl;
    return 1;
  }

  return 0;
}

#define BENCHMARK_TASK_MAIN                                                                        \
  int main(int argc, char **argv) { return benchmark_task_main(argc, argv); }
