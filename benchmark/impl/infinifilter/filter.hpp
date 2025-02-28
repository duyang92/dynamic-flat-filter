#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

#include "bitmap/bitmap.hpp"
#include "hash_functions.hpp"
#include "hash_type.hpp"

namespace infinifilter {

class Filter {
public:
  Filter(HashType ht) : hash_type(ht) {}
  Filter(const Filter &) = default;
  Filter(Filter &&) = default;
  auto operator=(const Filter &) -> Filter & = default;
  auto operator=(Filter &&) -> Filter & = default;

  HashType hash_type;

  virtual ~Filter() = default;

  // Pure virtual functions
  virtual auto rejuvenate(uint64_t key) -> bool = 0;
  virtual auto expand() -> bool = 0;

protected:
  virtual auto _remove(uint32_t large_hash) -> bool = 0;
  virtual auto _insert(uint32_t large_hash, bool insert_only_if_no_match) -> bool = 0;
  virtual auto _query(uint32_t large_hash) -> bool = 0;

public:
  virtual auto remove(uint64_t input) -> bool { return _remove(get_hash(input)); }

  virtual auto remove(const std::string &input) -> bool {
    auto hash = HashFunctions::xxhash(std::vector<unsigned char>(input.begin(), input.end()));
    return _remove(hash);
  }

  virtual auto remove(const char *input) -> bool {
    auto hash =
        HashFunctions::xxhash(std::vector<unsigned char>(input, input + std::strlen(input)));
    return _remove(hash);
  }

  virtual auto remove(const std::vector<unsigned char> &input) -> bool {
    auto hash = HashFunctions::xxhash(input);
    return _remove(hash);
  }

  virtual auto insert(uint64_t input, bool insert_only_if_no_match = false) -> bool {
    auto hash = get_hash(input);
    return _insert(hash, insert_only_if_no_match);
  }

  virtual auto insert(const std::string &input, bool insert_only_if_no_match = false) -> bool {
    auto hash = HashFunctions::xxhash(std::vector<unsigned char>(input.begin(), input.end()));
    return _insert(hash, insert_only_if_no_match);
  }

  virtual auto insert(const char *input, bool insert_only_if_no_match = false) -> bool {
    auto hash =
        HashFunctions::xxhash(std::vector<unsigned char>(input, input + std::strlen(input)));
    return _insert(hash, insert_only_if_no_match);
  }

  virtual auto insert(const std::vector<unsigned char> &input, bool insert_only_if_no_match = false)
      -> bool {
    auto hash = HashFunctions::xxhash(input);
    return _insert(hash, insert_only_if_no_match);
  }

  virtual auto query(uint64_t input) -> bool { return _query(get_hash(input)); }

  virtual auto query(const std::string &input) -> bool {
    auto hash = HashFunctions::xxhash(std::vector<unsigned char>(input.begin(), input.end()));
    return _query(hash);
  }

  virtual auto query(const char *input) -> bool {
    auto hash =
        HashFunctions::xxhash(std::vector<unsigned char>(input, input + std::strlen(input)));
    return _query(hash);
  }

  virtual auto query(const std::vector<unsigned char> &input) -> bool {
    auto hash = HashFunctions::xxhash(input);
    return _query(hash);
  }

  [[nodiscard]] constexpr auto get_hash(uint64_t input) const -> uint32_t {
    if (hash_type == HashType::ARBITRARY)
      return HashFunctions::normal_hash((uint32_t)input);
    if (hash_type == HashType::XXH)
      return HashFunctions::xxhash(input);
    throw std::runtime_error("Invalid hash type");
  }

  [[nodiscard]] virtual auto get_num_entries(bool include_all_internal_filters) const -> size_t = 0;

  [[nodiscard]] virtual auto get_utilization() const -> double { return 0.0; }

  [[nodiscard]] virtual auto measure_num_bits_per_entry() const -> double { return 0.0; }

  virtual auto get_fingerprint_str(uint32_t fp, size_t length) -> std::string {
    std::string str;
    for (size_t i = 0; i < length; ++i)
      str += bitmap::Bitmap::get_fingerprint_bit(i, fp) ? "1" : "0";
    return str;
  }
};

} // namespace infinifilter
