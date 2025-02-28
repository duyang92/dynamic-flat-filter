#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <xxhash.h>

#include "hash_type.hpp"

namespace infinifilter {

class HashFunctions {
public:
  // Hash function
  static auto hash(const std::string &key, HashType ht) -> uint32_t {
    return normal_hash(static_cast<uint32_t>(std::stoul(key)));
  }

  // Normal hash function
  static auto normal_hash(uint32_t x) -> uint32_t {
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = (x >> 16) ^ x;
    return x;
  }

  // xxHash functions
  static auto xxhash(const std::vector<unsigned char> &buffer) -> uint32_t {
    return XXH32(buffer.data(), buffer.size(), 0);
  }

  static auto xxhash(uint64_t input) -> uint32_t { return XXH32(&input, sizeof(input), 0); }

  static auto xxhash(uint64_t input, uint32_t seed) -> uint32_t {
    return XXH32(&input, sizeof(input), seed);
  }
};

} // namespace infinifilter
