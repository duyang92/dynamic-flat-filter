#pragma once

#include <cmath>

namespace dynamicbloomfilter {

class CountingBloomFilter {
private:
  char *bits;
  int byte_num;
  int bits_num;
  int hash_num;

public:
  CountingBloomFilter *next;
  CountingBloomFilter *front;
  int capacity;
  int item_num;

  /*****************************************
   * construction and destruction function *
   *****************************************/
  CountingBloomFilter(int capacity, double false_positive) {
    this->bits_num = (int)ceil(capacity * (1 / log(2.0)) * log(1 / false_positive) / log(2.0)) * 4;
    this->byte_num = (int)ceil((double)bits_num / 8);
    this->hash_num = (int)ceil((bits_num / 4) / capacity * log(2.0));
    this->capacity = capacity;
    this->item_num = 0;
    this->next = 0;
    this->front = 0;

    this->bits = new char[byte_num];
    for (int i = 0; i < byte_num; i++) {
      bits[i] = 0;
    }
  }

  CountingBloomFilter(int n, int m) {
    this->bits_num = m;
    this->byte_num = (int)ceil((double)bits_num / 8);
    this->hash_num = 7;
    this->capacity = n;
    this->item_num = 0;
    this->next = 0;
    this->front = 0;

    this->bits = new char[byte_num];
    for (int i = 0; i < byte_num; i++) {
      bits[i] = 0;
    }
  }

  ~CountingBloomFilter() { delete[] bits; }

  /************************************
   * insert & query & delete function *
   ************************************/
  bool insertItem(unsigned long int *hash_value) {
    int counter = 0;
    for (int i = 0; i < hash_num; i++) {
      counter = read(hash_value[i]) + 1;
      write(hash_value[i], counter);
    }

    item_num += 1;

    return true;
  }

  bool queryItem(unsigned long int *hash_value) {
    for (int i = 0; i < hash_num; i++) {
      if (read(hash_value[i]) == 0) {
        return false;
      }
    }
    return true;
  }

  bool deleteItem(unsigned long int *hash_value) {
    int counter = 0;

    for (int i = 0; i < hash_num; i++) {
      counter = read(hash_value[i]);
      if (counter != 0) {
        counter -= 1;
        write(hash_value[i], counter);
      } else {
        return false;
      }
    }
    item_num -= 1;
    return true;
  }

  /****************
   * read & write *
   ****************/
  bool write(unsigned long int hash_value, int counter) {
    if (counter > 15) {
      counter = 15;
    }
    if (hash_value % 2 == 0) {
      bits[hash_value >> 1] &= 0x0f;
      bits[hash_value >> 1] |= (counter << 4);
    } else {
      bits[hash_value >> 1] &= 0xf0;
      bits[hash_value >> 1] |= counter;
    }

    return true;
  }

  int read(unsigned long int hash_value) {
    int counter = 0;

    if (hash_value % 2 == 0) {
      counter = (bits[hash_value >> 1] >> 4);
    } else {
      counter = (bits[hash_value >> 1] & 0x0f);
    }

    return counter;
  }
};

} // namespace dynamicbloomfilter
