#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <functional>
#include <iostream>
#include <string>

#include <fmt/core.h>
#include <fplus/fplus.hpp>

inline size_t initial_capacity_log2;
inline size_t initial_capacity;

inline std::vector<std::string> benchmark_task_names;
inline std::vector<std::function<double(const uint64_t *, size_t)>> benchmark_task_functions;

#define REGISTER_BENCHMARK_TASK(name)                                                              \
  inline double benchmark_task_##name(const uint64_t *addrs, size_t n);                            \
  inline static bool _benchmark_task_##name##_registered = [] {                                    \
    benchmark_task_names.emplace_back(#name);                                                      \
    benchmark_task_functions.emplace_back(benchmark_task_##name);                                  \
    return true;                                                                                   \
  }();                                                                                             \
  inline double benchmark_task_##name(const uint64_t *addrs, size_t n)

inline auto get_current_time_in_seconds() -> double {
  const auto now = std::chrono::high_resolution_clock::now();
  const auto duration = now.time_since_epoch();
  return std::chrono::duration_cast<std::chrono::duration<double>>(duration).count();
}

/**
 * @brief Transform a string representation of an IPv4 address to a 32-bit unsigned integer.
 *
 * @param ipv4 The string representation of the IPv4 address.
 * @return The 32-bit unsigned integer representation of the IPv4 address.
 *
 * @example
 * @code
 * ipv4_to_uint32("127.0.0.1"); // => 2130706433
 * ipv4_to_uint32("10.10.64.1"); // => 168443905
 * @endcode
 */
inline auto ipv4_to_uint32(const std::string &ipv4) -> uint32_t {
  std::stringstream ss(ipv4);
  std::vector<uint32_t> bytes;
  std::string byte;

  while (std::getline(ss, byte, '.')) {
    unsigned int num = std::stoi(byte);
    if (num > 255)
      throw std::invalid_argument("Invalid IP address: " + ipv4);
    bytes.push_back(num);
  }

  if (bytes.size() != 4)
    throw std::invalid_argument("Invalid IP address: " + ipv4);

  return (bytes[0] << 24) | (bytes[1] << 16) | (bytes[2] << 8) | bytes[3];
}

/**
 * @brief Transform a string of 2 IPv4 addresses separated by a space to a 64-bit unsigned
 * integer.
 *
 * @param line The string representation of the 2 IPv4 addresses separated by a space.
 * @return The 64-bit unsigned integer representation of the 2 IPv4 addresses.
 *
 * @example
 * @code
 * caida_line_transformer("10.10.64.1 10.10.64.2"); // => 723461063353974786
 * @endcode
 */
inline auto caida_line_transformer(const std::string &line) -> uint64_t {
  std::stringstream ss(line);
  std::string src;
  std::string dst;

  if (!std::getline(ss, src, ' ') || !std::getline(ss, dst, ' '))
    throw std::invalid_argument("Invalid CAIDA line: " + line);

  return (static_cast<uint64_t>(ipv4_to_uint32(src)) << 32) | ipv4_to_uint32(dst);
};

inline auto stringify_ipv4_pair(uint64_t pair) -> std::string {
  return fmt::format("{}.{}.{}.{} -> {}.{}.{}.{}", (pair >> 56) & 0xff, (pair >> 48) & 0xff,
                     (pair >> 40) & 0xff, (pair >> 32) & 0xff, (pair >> 24) & 0xff,
                     (pair >> 16) & 0xff, (pair >> 8) & 0xff, pair & 0xff);
}

inline auto read_data(const std::string &pathname) -> std::vector<uint64_t> {
  std::vector<uint64_t> lines;

  std::ifstream file(pathname);
  if (!file.is_open()) {
    const std::string msg = fmt::format("Failed to open file: {}", pathname);
    throw std::runtime_error(msg);
  }

  std::string line;
  while (std::getline(file, line))
    lines.push_back(caida_line_transformer(line));

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

    const std::vector<uint64_t> addrs = read_data(argv[3]);

    const size_t index = std::distance(benchmark_task_names.begin(), it);
    std::cout << benchmark_task_functions[index](addrs.data(), addrs.size()) << std::endl;
  } catch (const std::exception &e) {
    std::cerr << "Error: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}

#define BENCHMARK_TASK_MAIN                                                                        \
  int main(int argc, char **argv) { return benchmark_task_main(argc, argv); }
