#pragma once

#include <cstdint>

#define BIG_CONSTANT(x) (x##LLU)

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
auto murmur_hash2_a(const void *key, int len, uint32_t seed) -> uint32_t;

/*-----------------------------------------------------------------------------
// MurmurHash2, 64-bit versions, by Austin Appleby
//
// The same caveats as 32-bit MurmurHash2 apply here - beware of alignment
// and endian-ness issues if used across multiple platforms.
//
// 64-bit hash for 64-bit platforms
*/
auto murmur_hash2_x64_a(const void *key, int len, uint64_t seed) -> uint64_t;
