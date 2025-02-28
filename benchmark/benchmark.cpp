#include <cmath>
#include <cstddef>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

#include <fmt/core.h>
#include <spdlog/spdlog.h>

#include "benchmark_utils.hpp"

// Change to 20 for reproduction of the paper results
// NOTE: This may take a long time to run, for a quick test, use 14 or 16
constexpr size_t INITIAL_CAPACITY_LOG2 = 16;
constexpr size_t INITIAL_CAPACITY = 1UZ << INITIAL_CAPACITY_LOG2;

const std::vector<size_t> MULTIPLIERS = {10, 20, 30, 40, 50, 60, 70, 80};

/***********
 * Helpers *
 ***********/
auto index_formatter(const std::vector<std::string> &arguments) -> std::string {
  const size_t initial_capacity_log2 = std::stoul(arguments[0]);
  const size_t n = std::stoul(arguments[1]);
  if (n % INITIAL_CAPACITY != 0)
    return std::to_string(n);
  const size_t multiplier = n / INITIAL_CAPACITY;
  return fmt::format("2^{} * {} ({})", initial_capacity_log2, multiplier,
                     arguments[1]);
};

auto throughput_formatter(const double value,
                          const std::vector<std::string> &arguments)
    -> std::string {
  return fmt::format("{:.3f}", static_cast<double>(std::stoul(arguments[1])) /
                                   value / 1'000'000.0);
}

auto multiply_formatter(const double multiplier, const size_t fixed = 3)
    -> std::function<std::string(const double,
                                 const std::vector<std::string> &)> {
  return [multiplier, fixed](const double value,
                             const std::vector<std::string> &) {
    return fmt::format("{:.{}f}", value * multiplier, fixed);
  };
}

auto unit_multiply_formatter(const double multiplier, const size_t fixed = 3)
    -> std::function<std::string(const double value,
                                 const std::vector<std::string> &)> {
  return [multiplier, fixed](const double value,
                             const std::vector<std::string> &arguments) {
    return fmt::format(
        "{:.{}f}",
        static_cast<double>(value /
                            static_cast<double>(std::stoul(arguments[1]))) *
            1'000'000,
        fixed);
  };
}

template <ConvertibleToString T> auto constant_formatter(const T &value) {
  return [value](auto &&...) { return convert_to_string(value); };
}

auto throughput_formatter_n(const size_t n)
    -> std::function<std::string(const double,
                                 const std::vector<std::string> &)> {
  return [n](const double value, const std::vector<std::string> &) {
    return fmt::format("{:.3f}", static_cast<double>(n) / value / 1'000'000.0);
  };
}

/**************
 * Throughput *
 **************/
BENCHMARK("insertion throughput") {
  spdlog::info("Benchmarking {}...", name);
  for (const size_t multiplier : MULTIPLIERS) {
    spdlog::info("Testing {} with 2^{} * {} ({}) elements", name,
                 INITIAL_CAPACITY_LOG2, multiplier,
                 INITIAL_CAPACITY * multiplier);
    benchmark_all(INITIAL_CAPACITY_LOG2, INITIAL_CAPACITY * multiplier);
  }
  spdlog::info("Benchmarking {} done.\n", name);

  spdlog::info("Insertion throughput (Mops):");
  summarize(index_formatter, throughput_formatter);
  spdlog::info("Insertion total time (ms):");
  summarize(index_formatter, multiply_formatter(1'000));
}

BENCHMARK("positive query throughput") {
  spdlog::info("Benchmarking {}...", name);
  for (const size_t multiplier : MULTIPLIERS) {
    spdlog::info("Testing {} with 2^{} * {} ({}) elements", name,
                 INITIAL_CAPACITY_LOG2, multiplier,
                 INITIAL_CAPACITY * multiplier);
    benchmark_all(INITIAL_CAPACITY_LOG2, INITIAL_CAPACITY * multiplier);
  }
  spdlog::info("Benchmarking {} done.\n", name);

  spdlog::info("Positive query throughput (Mops):");
  summarize(index_formatter, throughput_formatter);
  spdlog::info("Positive query total time (ms):");
  summarize(index_formatter, multiply_formatter(1'000));
}

BENCHMARK("negative query throughput") {
  spdlog::info("Benchmarking {}...", name);
  for (const size_t multiplier : MULTIPLIERS) {
    spdlog::info("Testing {} with 2^{} * {} ({}) elements", name,
                 INITIAL_CAPACITY_LOG2, multiplier,
                 INITIAL_CAPACITY * multiplier);
    benchmark_all(INITIAL_CAPACITY_LOG2, INITIAL_CAPACITY * multiplier);
  }
  spdlog::info("Benchmarking {} done.\n", name);

  spdlog::info("Negative query throughput (Mops):");
  summarize(index_formatter, throughput_formatter);
  spdlog::info("Negative query total time (ms):");
  summarize(index_formatter, multiply_formatter(1'000));
}

BENCHMARK("deletion throughput") {
  spdlog::info("Benchmarking {}...", name);
  for (const size_t multiplier : MULTIPLIERS) {
    spdlog::info("Testing {} with 2^{} * {} ({}) elements", name,
                 INITIAL_CAPACITY_LOG2, multiplier,
                 INITIAL_CAPACITY * multiplier);
    benchmark_all(INITIAL_CAPACITY_LOG2, INITIAL_CAPACITY * multiplier);
  }
  spdlog::info("Benchmarking {} done.\n", name);

  spdlog::info("Deletion throughput (Mops):");
  summarize(index_formatter, throughput_formatter);
  spdlog::info("Deletion total time (ms):");
  summarize(index_formatter, multiply_formatter(1'000));
}

/*******************
 * Addressing time *
 *******************/
BENCHMARK("positive query addressing time") {
  /* All baselines with linear dataset scale growth */
  spdlog::info("Benchmarking {}...", name);
  for (const size_t multiplier : MULTIPLIERS) {
    spdlog::info("Testing {} with 2^{} * {} ({}) elements", name,
                 INITIAL_CAPACITY_LOG2, multiplier,
                 INITIAL_CAPACITY * multiplier);
    benchmark_all(INITIAL_CAPACITY_LOG2, INITIAL_CAPACITY * multiplier);
  }
  spdlog::info("Benchmarking {} done.\n", name);

  spdlog::info("Positive query addressing time (μs):");
  summarize(index_formatter, unit_multiply_formatter(1'000'000, 6));
  std::cout << std::endl;

  /* Only DFF and BBF with exponential² dataset scale growth */
  reset_benchmark({"DFF", "DFF_FG", "BBF"});
  spdlog::info("Benchmarking {} for DFF and BBF...", name);
  for (const size_t multiplier : {1, 2, 4, 8, 16}) {
    spdlog::info(
        "Testing {} with 2^{} * 2^{} ({}) elements", name,
        INITIAL_CAPACITY_LOG2, multiplier,
        static_cast<size_t>(INITIAL_CAPACITY * std::pow(2, multiplier)));
    benchmark_all(
        INITIAL_CAPACITY_LOG2,
        static_cast<size_t>(INITIAL_CAPACITY * std::pow(2, multiplier)));
  }
  spdlog::info("Benchmarking {} for DFF and BBF done.\n", name);

  spdlog::info("Positive query addressing time (μs) for DFF and BBF:");
  summarize(index_formatter, unit_multiply_formatter(1'000'000, 6));
}

BENCHMARK("negative query addressing time") {
  /* All baselines with linear dataset scale growth */
  spdlog::info("Benchmarking {}...", name);
  for (const size_t multiplier : MULTIPLIERS) {
    spdlog::info("Testing {} with 2^{} * {} ({}) elements", name,
                 INITIAL_CAPACITY_LOG2, multiplier,
                 INITIAL_CAPACITY * multiplier);
    benchmark_all(INITIAL_CAPACITY_LOG2, INITIAL_CAPACITY * multiplier);
  }
  spdlog::info("Benchmarking {} done.\n", name);

  spdlog::info("Negative query addressing time (μs):");
  summarize(index_formatter, unit_multiply_formatter(1'000'000, 6));
  std::cout << std::endl;

  /* Only DFF and BBF with exponential² dataset scale growth */
  reset_benchmark({"DFF", "DFF_FG", "BBF"});
  spdlog::info("Benchmarking {} for DFF and BBF...", name);
  for (const size_t multiplier : {1, 2, 4, 8, 16}) {
    spdlog::info(
        "Testing {} with 2^{} * 2^{} ({}) elements", name,
        INITIAL_CAPACITY_LOG2, multiplier,
        static_cast<size_t>(INITIAL_CAPACITY * std::pow(2, multiplier)));
    benchmark_all(
        INITIAL_CAPACITY_LOG2,
        static_cast<size_t>(INITIAL_CAPACITY * std::pow(2, multiplier)));
  }
  spdlog::info("Benchmarking {} for DFF and BBF done.\n", name);

  spdlog::info("Negative query addressing time (μs) for DFF and BBF:");
  summarize(index_formatter, unit_multiply_formatter(1'000'000, 6));
}

/***********************************
 * Construction and execution time *
 ***********************************/
BENCHMARK("construction time (i:d=10:1)") {
  spdlog::info("Benchmarking {}...", name);
  for (const size_t multiplier : MULTIPLIERS) {
    spdlog::info("Testing {} with 2^{} * {} ({}) elements", name,
                 INITIAL_CAPACITY_LOG2, multiplier,
                 INITIAL_CAPACITY * multiplier);
    benchmark_all(INITIAL_CAPACITY_LOG2, INITIAL_CAPACITY * multiplier);
  }
  spdlog::info("Benchmarking {} done.\n", name);

  spdlog::info("Construction total time (i:d=10:1) (ms):");
  summarize(index_formatter, multiply_formatter(1'000));
}

BENCHMARK("construction time (i:d=10:5)") {
  spdlog::info("Benchmarking {}...", name);
  for (const size_t multiplier : MULTIPLIERS) {
    spdlog::info("Testing {} with 2^{} * {} ({}) elements", name,
                 INITIAL_CAPACITY_LOG2, multiplier,
                 INITIAL_CAPACITY * multiplier);
    benchmark_all(INITIAL_CAPACITY_LOG2, INITIAL_CAPACITY * multiplier);
  }
  spdlog::info("Benchmarking {} done.\n", name);

  spdlog::info("Construction total time (i:d=10:5) (ms):");
  summarize(index_formatter, multiply_formatter(1'000));
}

BENCHMARK("execution time (i:l:d=3:9:1)") {
  spdlog::info("Benchmarking {}...", name);
  for (const size_t multiplier : MULTIPLIERS) {
    spdlog::info("Testing {} with 2^{} * {} ({}) elements", name,
                 INITIAL_CAPACITY_LOG2, multiplier,
                 INITIAL_CAPACITY * multiplier);
    benchmark_all(INITIAL_CAPACITY_LOG2, INITIAL_CAPACITY * multiplier);
  }
  spdlog::info("Benchmarking {} done.\n", name);

  spdlog::info("Execution total time (i:l:d=3:9:1) (ms):");
  summarize(index_formatter, multiply_formatter(1'000));
}

BENCHMARK("execution time (i:l:d=9:3:1)") {
  spdlog::info("Benchmarking {}...", name);
  for (const size_t multiplier : MULTIPLIERS) {
    spdlog::info("Testing {} with 2^{} * {} ({}) elements", name,
                 INITIAL_CAPACITY_LOG2, multiplier,
                 INITIAL_CAPACITY * multiplier);
    benchmark_all(INITIAL_CAPACITY_LOG2, INITIAL_CAPACITY * multiplier);
  }
  spdlog::info("Benchmarking {} done.\n", name);

  spdlog::info("Execution total time (i:l:d=9:3:1) (ms):");
  summarize(index_formatter, multiply_formatter(1'000));
}

/*****************************************************
 * Expansion time (Query blocking duration in paper) *
 *****************************************************/
BENCHMARK("expansion time") {
  spdlog::info("Benchmarking {}...", name);
  for (const size_t multiplier : MULTIPLIERS) {
    spdlog::info("Testing {} with 2^{} * {} ({}) elements", name,
                 INITIAL_CAPACITY_LOG2, multiplier,
                 INITIAL_CAPACITY * multiplier);
    benchmark_all(INITIAL_CAPACITY_LOG2, INITIAL_CAPACITY * multiplier);
  }
  spdlog::info("Benchmarking {} done.\n", name);

  spdlog::info("Expansion total time (ms):");
  summarize(index_formatter, multiply_formatter(1'000));
}

/***********************
 * False positive rate *
 ***********************/
BENCHMARK("false positive rate") {
  spdlog::info("Benchmarking {}...", name);
  for (const size_t multiplier : MULTIPLIERS) {
    spdlog::info("Testing {} with 2^{} * {} ({}) elements", name,
                 INITIAL_CAPACITY_LOG2, multiplier,
                 INITIAL_CAPACITY * multiplier);
    benchmark_all(INITIAL_CAPACITY_LOG2, INITIAL_CAPACITY * multiplier);
  }
  spdlog::info("Benchmarking {} done.\n", name);

  spdlog::info("False positive rate (%):");
  summarize(index_formatter, multiply_formatter(100));
}

/********************************************
 * Space usage (Memory efficiency in paper) *
 ********************************************/
BENCHMARK("space usage") {
  spdlog::info("Benchmarking {}...", name);
  for (const size_t n : {1'000'000, 2'000'000}) {
    spdlog::info("Testing {} with {} elements", name, n);
    benchmark_all(INITIAL_CAPACITY_LOG2, n);
  }
  spdlog::info("Benchmarking {} done.\n", name);

  spdlog::info("Space usage (MB):");
  summarize(index_formatter, multiply_formatter(1 / (8.0 * 1024 * 1024)));
}

/********************************************
 * Throughput on real-world dataset (CAIDA) *
 ********************************************/
constexpr std::string_view CAIDA_PATH = "../data/CAIDA.txt";

BENCHMARK("insertion throughput on CAIDA") {
  spdlog::info("Benchmarking {}...", name);
  benchmark_all(INITIAL_CAPACITY_LOG2, CAIDA_PATH);
  spdlog::info("Benchmarking {} done.\n", name);

  spdlog::info("Insertion throughput on CAIDA (Mops):");
  summarize(constant_formatter(1'000'000), throughput_formatter_n(1'000'000));
}

BENCHMARK("query throughput on CAIDA") {
  spdlog::info("Benchmarking {}...", name);
  benchmark_all(INITIAL_CAPACITY_LOG2, CAIDA_PATH);
  spdlog::info("Benchmarking {} done.\n", name);

  spdlog::info("Query throughput on CAIDA (Mops):");
  summarize(constant_formatter(1'000'000), throughput_formatter_n(1'000'000));
}

/********************************************
 * Throughput on real-world dataset (YCSB) *
 ********************************************/
constexpr std::string_view YCSB_PATH = "../data/YCSB.txt";

BENCHMARK("insertion throughput on YCSB") {
  spdlog::info("Benchmarking {}...", name);
  benchmark_all(INITIAL_CAPACITY_LOG2, CAIDA_PATH);
  spdlog::info("Benchmarking {} done.\n", name);

  spdlog::info("Insertion throughput on YCSB (Mops):");
  summarize(constant_formatter(1'000'000), throughput_formatter_n(1'000'000));
}

BENCHMARK("query throughput on YCSB") {
  spdlog::info("Benchmarking {}...", name);
  benchmark_all(INITIAL_CAPACITY_LOG2, CAIDA_PATH);
  spdlog::info("Benchmarking {} done.\n", name);

  spdlog::info("Query throughput on YCSB (Mops):");
  summarize(constant_formatter(1'000'000), throughput_formatter_n(1'000'000));
}

/********
 * Main *
 ********/
BENCHMARK_MAIN {
  // Change "info" to "debug" to see more detailed logs
  spdlog::set_level(spdlog::level::info);

  return 0;
}
