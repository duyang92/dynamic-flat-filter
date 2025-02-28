#pragma once

namespace elasticbloomfilter {

#define SHIFT 3     // a shift
#define MASK 0x7    // a mask
#define MAX_SIZE 32 // maximum length of the bits array(2^MAX_SIZE)
#define MIN_SIZE 3  // minimum length of the bits array(2^MIN_SIZE)

#define BUCKET_SIZE 8        // number of elements in one bucket
#define EXPAND_THRESHOLD 0.2 // expand when the _1_rate reaches it

#define MAX_BLOOM_SIZE 30 // length of the bits array
#define SAMPLEBITNUM 8

#define THREAD_NUM 4
#define LOCK_NUM (1 << 24)

} // namespace elasticbloomfilter
