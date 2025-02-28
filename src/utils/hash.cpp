#include <cstdint>

#include "hash.hpp"

/*-----------------------------------------------------------------------------
// MurmurHash2A, by Austin Appleby
//
// This is a variant of MurmurHash2 modified to use the Merkle-Damgard
// construction. Bulk speed should be identical to Murmur2, small-key speed
// will be 10%-20% slower due to the added overhead at the end of the hash.
//
// This variant fixes a minor issue where null keys were more likely to
// collide with each other than expected, and also makes the function
// more amenable to incremental implementations.
*/

#define mmix(h, k)                                                                                 \
  {                                                                                                \
    k *= m;                                                                                        \
    k ^= k >> r;                                                                                   \
    k *= m;                                                                                        \
    h *= m;                                                                                        \
    h ^= k;                                                                                        \
  }

auto murmur_hash2_a(const void *key, int len, uint32_t seed) -> uint32_t {
  const uint32_t m = 0x5bd1e995;
  const int r = 24;
  uint32_t l = len;

  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-cstyle-cast)
  const auto *data = (const unsigned char *)key;

  uint32_t h = seed;

  while (len >= 4) {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-cstyle-cast)
    uint32_t k = *(uint32_t *)data;

    mmix(h, k);

    data += 4;
    len -= 4;
  }

  uint32_t t = 0;

  switch (len) {
  case 3:
    t ^= data[2] << 16;
  case 2:
    t ^= data[1] << 8;
  case 1:
    t ^= data[0];
  };

  mmix(h, t);
  mmix(h, l);

  h ^= h >> 13;
  h *= m;
  h ^= h >> 15;

  return h;
}

/*-----------------------------------------------------------------------------
// MurmurHash2, 64-bit versions, by Austin Appleby
//
// The same caveats as 32-bit MurmurHash2 apply here - beware of alignment
// and endian-ness issues if used across multiple platforms.
//
// 64-bit hash for 64-bit platforms
*/

auto murmur_hash2_x64_a(const void *key, int len, uint64_t seed) -> uint64_t {
  const uint64_t m = BIG_CONSTANT(0xc6a4a7935bd1e995);
  const int r = 47;

  uint64_t h = seed ^ (len * m);

  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-cstyle-cast)
  const auto *data = (const uint64_t *)key;
  const uint64_t *end = data + (len / 8);

  while (data != end) {
    uint64_t k = *data++;

    k *= m;
    k ^= k >> r;
    k *= m;

    h ^= k;
    h *= m;
  }

  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-cstyle-cast)
  const auto *data2 = (const unsigned char *)data;

  switch (len & 7) {
  case 7:
    h ^= ((uint64_t)data2[6]) << 48;
  case 6:
    h ^= ((uint64_t)data2[5]) << 40;
  case 5:
    h ^= ((uint64_t)data2[4]) << 32;
  case 4:
    h ^= ((uint64_t)data2[3]) << 24;
  case 3:
    h ^= ((uint64_t)data2[2]) << 16;
  case 2:
    h ^= ((uint64_t)data2[1]) << 8;
  case 1:
    h ^= ((uint64_t)data2[0]);
    h *= m;
  };

  h ^= h >> r;
  h *= m;
  h ^= h >> r;

  return h;
}
