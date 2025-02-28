#pragma once

#include <cstddef>
#include <cstdint>
#include <iostream>
#include <queue>

#include "quotient_filter.hpp"

namespace infinifilter {

class Iterator {
public:
  QuotientFilter qf;
  size_t index{0};
  size_t bucket_index{SIZE_MAX};
  uint32_t fingerprint{UINT32_MAX};
  std::queue<size_t> s;

  Iterator(QuotientFilter &new_qf) : qf(new_qf) {}

  void clear() {
    while (!s.empty())
      s.pop();
    index = 0;
    bucket_index = SIZE_MAX;
    fingerprint = UINT32_MAX;
  }

  auto next() -> bool {
    if (index == qf.get_logical_num_slots_plus_extensions())
      return false;

    uint32_t slot = qf.get_slot(index);
    bool occupied = (slot & 1) != 0;
    bool continuation = (slot & 2) != 0;
    bool shifted = (slot & 4) != 0;

    while (!occupied && !continuation && !shifted &&
           index < qf.get_logical_num_slots_plus_extensions()) {
      index++;
      if (index == qf.get_logical_num_slots_plus_extensions())
        return false;
      slot = qf.get_slot(index);
      occupied = (slot & 1) != 0;
      continuation = (slot & 2) != 0;
      shifted = (slot & 4) != 0;
    }

    if (occupied && !continuation && !shifted) {
      while (!s.empty())
        s.pop();
      s.push(index);
      bucket_index = index;
    } else if (occupied && continuation && shifted) {
      s.push(index);
    } else if (!occupied && !continuation && shifted) {
      s.pop();
      bucket_index = s.front();
    } else if (!occupied && continuation && shifted) {
      // do nothing
    } else if (occupied && !continuation && shifted) {
      s.push(index);
      s.pop();
      bucket_index = s.front();
    }
    fingerprint = slot >> 3;
    index++;
    return true;
  }

  void print() const {
    std::cout << "original slot: " << index << "  " << bucket_index << std::endl;
  }
};

} // namespace infinifilter
