#pragma once

#include <algorithm>
#include <initializer_list>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <regex>
#include <sstream>
#include <string>
#include <system_error>
#include <tuple>
#include <type_traits>

#include <fplus/fplus.hpp>
#include <reproc++/run.hpp>
#include <spdlog/spdlog.h>

namespace fwd = fplus::fwd;

inline constexpr size_t MIN_BENCHMARK_SECONDS = 10UZ;
inline constexpr size_t MIN_BENCHMARK_TIMES = 10UZ;
inline constexpr size_t TIMEOUT_MILLISECONDS = 900'000UZ;

inline auto get_benchmark_filename(const std::string &name) -> std::string {
  auto match_ending = [](const std::string &input) -> std::string {
    const std::regex pattern(R"(\(([\w:]+)=([\d.:]+)\))");
    std::smatch matches;

    if (std::regex_match(input, matches, pattern)) {
      const std::string left = matches[1].str();
      const std::string right = matches[2].str();

      std::vector<std::string> left_tokens;
      std::istringstream left_iss(left);
      std::string token;
      while (std::getline(left_iss, token, ':'))
        left_tokens.push_back(token);

      std::vector<std::string> right_tokens;
      std::istringstream right_iss(right);
      while (std::getline(right_iss, token, ':'))
        right_tokens.push_back(token);

      if (left_tokens.size() != right_tokens.size())
        return "";

      std::string res;
      for (size_t i = 0; i < left_tokens.size(); i++)
        res += left_tokens[i] + right_tokens[i];
      return res;
    }

    return "";
  };

  std::istringstream iss(name);
  std::string token;
  std::string filename;
  while (std::getline(iss, token, ' ')) {
    if (token == "on")
      continue;
    if (std::string ending = match_ending(token); !ending.empty()) {
      filename += ending + "_";
      continue;
    }
    filename += token + "_";
  }
  filename.pop_back();

  return filename;
}

inline auto get_current_time_in_seconds() -> double {
  const auto now = std::chrono::high_resolution_clock::now();
  const auto duration = now.time_since_epoch();
  return std::chrono::duration_cast<std::chrono::duration<double>>(duration).count();
}

template <typename T>
concept ConvertibleToString = std::is_integral_v<std::remove_cvref_t<T>> ||
                              std::is_same_v<std::remove_cvref_t<T>, std::string> ||
                              std::is_same_v<std::remove_cvref_t<T>, std::string_view> ||
                              std::is_same_v<std::remove_cvref_t<T>, const char *>;

auto convert_to_string(ConvertibleToString auto &&value) -> std::string {
  using T = std::decay_t<decltype(value)>;
  if constexpr (std::is_integral_v<T>) {
    return std::to_string(value); // Handle integral types
  } else if constexpr (std::is_same_v<T, std::string> || std::is_same_v<T, std::string_view> ||
                       std::is_same_v<T, const char *>) {
    return std::string(value); // Handle std::string, std::string_view, const char*
  } else {
    static_assert(!std::is_same_v<T, T>, "Unsupported type for conversion to string");
  }
}

inline std::vector<std::string> benchmark_names;
inline std::vector<std::function<double(const uint64_t *, size_t)>> benchmark_functions;

class BenchmarkBase {
public:
  BenchmarkBase(const BenchmarkBase &) = default;
  BenchmarkBase(BenchmarkBase &&) = delete;
  BenchmarkBase &operator=(const BenchmarkBase &) = default;
  BenchmarkBase &operator=(BenchmarkBase &&) = delete;
  explicit BenchmarkBase(const std::string &&name)
      : name(name), filename_(get_benchmark_filename(name)),
        all_available_benchmark_names_(get_available_benchmarks()),
        enabled_benchmark_names_(all_available_benchmark_names_) {}
  virtual ~BenchmarkBase() = default;

  virtual void run() = 0;

  template <ConvertibleToString... Args> void benchmark_all(Args &&...args) {
    const std::vector<std::string> arguments{convert_to_string(std::forward<Args>(args))...};

    std::vector<double> results;

    for (const std::string &benchmark : enabled_benchmark_names_) {
      std::vector<double> each_results;

      const double start = get_current_time_in_seconds();

      bool command_logged = false;

      size_t times = 0;
      for (; times < MIN_BENCHMARK_TIMES ||
             get_current_time_in_seconds() - start < MIN_BENCHMARK_SECONDS;
           times++) {
        reproc::process process;

        reproc::options options;
        options.redirect.out.type = reproc::redirect::pipe;
        options.redirect.err.type = reproc::redirect::pipe;

        std::vector<std::string> process_args{"./BM_" + filename_, benchmark};
        for (const std::string &argument : arguments)
          process_args.push_back(argument);

        if (!command_logged) {
          command_logged = true;
          std::string command;
          for (const std::string &argument : process_args)
            command += argument + " ";
          command.pop_back();
          spdlog::debug("[{}] Running benchmark with command: {}", benchmark, command);
        }

        std::error_code ec = process.start(process_args, options);
        if (ec) {
          spdlog::error("[{}] Benchmark failed to start: {}", benchmark,
                        fplus::trim_whitespace(ec.message()));
          break;
        }

        std::string output;

        reproc::sink::string sink(output);

        ec = reproc::drain(process, sink, sink);
        if (ec) {
          spdlog::error("[{}] Failed to read process output: {}", benchmark,
                        fplus::trim_whitespace(ec.message()));
          break;
        }

        int status = 0;
        try {
          std::tie(status, ec) = process.wait(reproc::milliseconds{TIMEOUT_MILLISECONDS});
        } catch (const std::exception &e) {
          spdlog::error("[{}] Failed to wait for process: {}", benchmark,
                        fplus::trim_whitespace(std::string(e.what())));
          break;
        } catch (...) {
          spdlog::error("[{}] Failed to wait for process: unknown error");
          break;
        }
        if (ec) {
          spdlog::error("[{}] Failed to wait for process: {}", benchmark,
                        fplus::trim_whitespace(ec.message()));
          break;
        }
        if (status) {
          spdlog::error("[{}/{}] {}", benchmark, times + 1, fplus::trim_whitespace(output));
          spdlog::error("[{}/{}] Process exited with status: {}", benchmark, times + 1, status);
          if (benchmark == "EBF")
            break;
          continue;
        }

        double result = std::numeric_limits<double>::infinity();
        std::istringstream iss(output);
        iss >> result;
        each_results.push_back(result);
      }

      const double mean = each_results.empty()
                              ? std::numeric_limits<double>::infinity()
                              : std::accumulate(each_results.begin(), each_results.end(), 0.0) /
                                    static_cast<double>(each_results.size());

      spdlog::debug("[{}] Benchmark ran {} times. Mean: {}", benchmark, times, mean);

      results.push_back(mean);
    }

    std::string line = "Results: ";
    for (size_t i = 0; i < results.size(); i++)
      line += "(" + enabled_benchmark_names_[i] + ")" + std::to_string(results[i]) + ", ";
    line.pop_back();
    line.pop_back();
    spdlog::debug(line);

    results_.emplace_back(arguments, results);
  }

  void reset_benchmark(const std::initializer_list<std::string> &enabled_benchmarks = {}) {
    // Check if all enabled benchmarks are available
    for (const std::string &enabled_benchmark : enabled_benchmarks)
      if (std::ranges::find(all_available_benchmark_names_, enabled_benchmark) ==
          all_available_benchmark_names_.end())
        throw std::runtime_error("Unknown benchmark: " + enabled_benchmark);
    if (enabled_benchmarks.size() == 0)
      enabled_benchmark_names_ = all_available_benchmark_names_;
    else
      enabled_benchmark_names_ = enabled_benchmarks;
    results_.clear();
  }

  void summarize(
      // NOLINTNEXTLINE(cppcoreguidelines-rvalue-reference-param-not-moved)
      std::function<std::string(const std::vector<std::string> &)> &&index_formatter,
      std::function<std::string(const double, const std::vector<std::string> &)>
          // NOLINTNEXTLINE(cppcoreguidelines-rvalue-reference-param-not-moved)
          &&value_formatter) {
    std::vector<std::vector<std::string>> outputs;
    for (const auto &[arguments, results] : results_) {
      std::vector<std::string> output;
      output.push_back(index_formatter(arguments));
      for (const double mean : results)
        output.push_back(value_formatter(mean, arguments));
      outputs.push_back(output);
    }

    const std::vector<int> candidate_index_column_lengths = {10, 15, 20, 25, 30};
    int index_column_length = 0;
    for (const std::vector<std::string> &output : outputs)
      index_column_length = std::max(index_column_length, static_cast<int>(output[0].size()));
    for (const int len : candidate_index_column_lengths)
      if (index_column_length + 1 < len) {
        index_column_length = len;
        break;
      }

    const std::vector<int> candidate_value_column_lengths = {8, 10, 12, 14, 16};
    int value_column_length = 0;
    for (const std::vector<std::string> &output : outputs)
      for (size_t i = 1; i < output.size(); i++)
        value_column_length = std::max(value_column_length, static_cast<int>(output[i].size()));
    for (const int len : candidate_value_column_lengths)
      if (value_column_length + 1 < len) {
        value_column_length = len;
        break;
      }

    std::ostringstream oss;
    oss << std::left << std::setw(index_column_length) << "Elements";
    for (const std::string &benchmark : enabled_benchmark_names_)
      oss << std::left << std::setw(value_column_length) << benchmark;
    spdlog::info(oss.str());

    for (const std::vector<std::string> &output : outputs) {
      std::ostringstream oss;
      oss << std::left << std::setw(index_column_length) << output[0];
      for (size_t i = 1; i < output.size(); i++)
        oss << std::left << std::setw(value_column_length) << output[i];
      spdlog::info(oss.str());
    }
  }

  void start() {
    run();
    std::cout << std::endl;
  }

protected:
  // NOLINTNEXTLINE
  std::string name;
  // NOLINTNEXTLINE
  std::vector<std::pair<std::vector<std::string>, std::vector<double>>> results_;

  [[nodiscard]] static auto capitalize(const std::string &input) -> std::string {
    std::string res = input;
    if (!res.empty())
      res[0] = static_cast<char>(std::toupper(res[0]));
    return res;
  }

private:
  std::string filename_;
  std::vector<std::string> all_available_benchmark_names_;
  std::vector<std::string> enabled_benchmark_names_;

  auto get_available_benchmarks() -> std::vector<std::string> {
    reproc::process process;

    reproc::options options;
    options.redirect.out.type = reproc::redirect::pipe;
    options.redirect.err.type = reproc::redirect::pipe;

    std::vector<std::string> arguments{"./BM_" + filename_};
    std::error_code ec = process.start(arguments, options);
    if (ec)
      throw std::runtime_error("Failed to start process: " + fplus::trim_whitespace(ec.message()));

    std::string output;

    reproc::sink::string sink(output);

    ec = reproc::drain(process, sink, sink);
    if (ec)
      throw std::runtime_error("Failed to read process output: " +
                               fplus::trim_whitespace(ec.message()));

    process.wait(reproc::infinite);

    if (!output.starts_with("Usage: "))
      throw std::runtime_error("Unexpected output from process: " + output);

    return fwd::apply(output, fwd::trim_whitespace(), fwd::split(' ', false), fwd::elem_at_idx(2),
                      fwd::drop(1), fwd::drop_last(1), fwd::split('|', false));
  }
};

inline std::vector<std::unique_ptr<BenchmarkBase>> benchmarks;

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define CONCAT(a, b) CONCAT_INNER(a, b)
#define CONCAT_INNER(a, b) a##b

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define BENCHMARK(name)                                                                            \
  class CONCAT(Benchmark, __LINE__) : public BenchmarkBase {                                       \
  public:                                                                                          \
    CONCAT(Benchmark, __LINE__)() : BenchmarkBase(name) {}                                         \
    void run() override;                                                                           \
  };                                                                                               \
  static bool CONCAT(CONCAT(_benchmark, __LINE__), _registered) = [] {                             \
    benchmarks.emplace_back(std::make_unique<CONCAT(Benchmark, __LINE__)>());                      \
    return true;                                                                                   \
  }();                                                                                             \
  void CONCAT(Benchmark, __LINE__)::run()

inline auto benchmark_main() -> int {
  for (const auto &benchmark : benchmarks)
    benchmark->start();

  return 0;
}

#define BENCHMARK_MAIN                                                                             \
  auto benchmark_main_impl() -> int;                                                               \
  auto main() -> int {                                                                             \
    const int res = benchmark_main_impl();                                                         \
    if (res != 0)                                                                                  \
      return res;                                                                                  \
    return benchmark_main();                                                                       \
  }                                                                                                \
  auto benchmark_main_impl() -> int
