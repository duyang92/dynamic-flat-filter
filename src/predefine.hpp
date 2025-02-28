#pragma once

#include <cstddef>

namespace dff {

// Must be a power of 2
constexpr size_t LOOKUP_TABLE_SIZE = 4096UZ;

constexpr size_t BUCKETS_PER_SEG_POWER = 12UZ;
// Segments per bucket (**MUST BE A POWER OF 2**)
constexpr size_t BUCKETS_PER_SEG = 1UZ << BUCKETS_PER_SEG_POWER;
constexpr size_t SLOTS_PER_BUCKET = 4UZ;

constexpr size_t INITIAL_FILTER_CAPACITY = 1UZ << 16;
// Initial number of segments
constexpr size_t INITIAL_SEG_COUNT = INITIAL_FILTER_CAPACITY / SLOTS_PER_BUCKET / BUCKETS_PER_SEG;
// Initial number of lookup table entries per segment
constexpr size_t INITIAL_LOOKUP_TABLE_ENTRIES_PER_SEG = LOOKUP_TABLE_SIZE / INITIAL_SEG_COUNT;

constexpr size_t TABLE_MASK = LOOKUP_TABLE_SIZE - 1;

} // namespace dff
