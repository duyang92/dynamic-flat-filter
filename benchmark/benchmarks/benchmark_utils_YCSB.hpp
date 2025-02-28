#pragma once

#include <algorithm>
#include <chrono>
#include <fstream>
#include <functional>
#include <iostream>
#include <string>

#include <fmt/core.h>
#include <fplus/fplus.hpp>

inline size_t initial_capacity_log2;
inline size_t initial_capacity;

inline std::vector<std::string> benchmark_task_names;
inline std::vector<std::function<double(const std::string *, size_t)>> benchmark_task_functions;

#define REGISTER_BENCHMARK_TASK(name)                                                              \
  inline double benchmark_task_##name(const std::string *lines, size_t n);                         \
  inline static bool _benchmark_task_##name##_registered = [] {                                    \
    benchmark_task_names.emplace_back(#name);                                                      \
    benchmark_task_functions.emplace_back(benchmark_task_##name);                                  \
    return true;                                                                                   \
  }();                                                                                             \
  inline double benchmark_task_##name(const std::string *lines, size_t n)

inline auto get_current_time_in_seconds() -> double {
  const auto now = std::chrono::high_resolution_clock::now();
  const auto duration = now.time_since_epoch();
  return std::chrono::duration_cast<std::chrono::duration<double>>(duration).count();
}

inline auto read_data(const std::string &pathname) -> std::vector<std::string> {
  std::vector<std::string> lines;

  std::ifstream file(pathname);
  if (!file.is_open()) {
    const std::string msg = fmt::format("Failed to open file: {}", pathname);
    throw std::runtime_error(msg);
  }

  std::string line;
  while (std::getline(file, line))
    lines.push_back(line);

  file.close();

  return lines;
}

inline auto benchmark_task_main(int argc, char **argv) -> int {
  if (argc < 4) {
    std::cerr << "Usage: " << argv[0] << " {" << fplus::join_elem('|', benchmark_task_names)
              << "} <initial_capacity_log2> <dataset_path>" << std::endl;
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

    const std::vector<std::string> lines = read_data(argv[3]);

    const size_t index = std::distance(benchmark_task_names.begin(), it);
    std::cout << benchmark_task_functions[index](lines.data(), lines.size()) << std::endl;
  } catch (const std::exception &e) {
    std::cerr << "Error: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}

#define BENCHMARK_TASK_MAIN                                                                        \
  int main(int argc, char **argv) { return benchmark_task_main(argc, argv); }
