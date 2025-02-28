#include <cstdint>

#include "quick_bit_vector.hpp"

namespace bitmap {

std::vector<uint32_t> QuickBitVector::pows = QuickBitVector::precompute_pows();

} // namespace bitmap
