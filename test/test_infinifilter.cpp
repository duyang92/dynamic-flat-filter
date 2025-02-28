/*
 * Modified from tests for Java implementation of InfiniFilter
 * https://github.com/nivdayan/FilterLibrary/blob/main/filters/Tests.java
 */

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <map>
#include <ostream>
#include <random>
#include <unordered_set>

#include <catch2/catch_test_macros.hpp>

#include "../benchmark/impl/infinifilter/basic_infinifilter.hpp"
#include "../benchmark/impl/infinifilter/chained_infinifilter.hpp"
#include "../benchmark/impl/infinifilter/chaining.hpp"
#include "../benchmark/impl/infinifilter/fingerprint_sacrifice.hpp"
#include "../benchmark/impl/infinifilter/iterator.hpp"
#include "../benchmark/impl/infinifilter/quotient_filter.hpp"

using namespace infinifilter;

/**
 * @brief Returns the greatest element in this set less than or equal to the given element, or
 * `std::nullopt` if there is no such element.
 *
 * @param s The set to search.
 * @param e The element to search for.
 * @return The greatest element in this set less than or equal to the given element, or
 * `std::nullopt`.
 */
template <typename T> auto get_set_floor(const std::set<T> &s, const T &e) -> std::optional<T> {
  auto it = s.upper_bound(e);
  if (it == s.begin())
    return std::nullopt; // No element less than or equal to key
  --it;
  return *it;
};

auto set_slot_in_test(std::vector<bool> &result, size_t bits_per_entry, size_t slot,
                      bool is_occupied, bool is_continuation, bool is_shifted, uint64_t fingerprint)
    -> std::vector<bool> {
  size_t index = bits_per_entry * slot;
  result[index++] = is_occupied;
  result[index++] = is_continuation;
  result[index++] = is_shifted;
  for (size_t i = 0; i < bits_per_entry - 3; i++)
    result[index++] = bitmap::Bitmap::get_fingerprint_bit(i, fingerprint);
  return result;
}

auto set_slot_in_test(std::vector<bool> &result, size_t bits_per_entry, size_t slot,
                      bool is_occupied, bool is_continuation, bool is_shifted,
                      std::string fingerprint) -> std::vector<bool> {
  uint64_t l_fingerprint = 0;
  for (size_t i = 0; i < fingerprint.length(); i++) {
    char c = fingerprint[i];
    if (c == '1')
      l_fingerprint |= (1 << i);
  }
  return set_slot_in_test(result, bits_per_entry, slot, is_occupied, is_continuation, is_shifted,
                          l_fingerprint);
}

inline void print_qf_bits(const QuotientFilter &qf) {
  const std::string purple = "\033[1;35m";
  const std::string blue = "\033[1;34m";
  const std::string reset = "\033[0m";

  const size_t bits_per_entry = qf.bit_per_entry;

  size_t i = 0;
  bool age = true;
  for (; i < qf.size() * bits_per_entry; ++i) {
    bool qf_bit = qf.get_bit_at_offset(i);
    if ((i + 1) % bits_per_entry > 0 && (i + 1) % bits_per_entry <= 3)
      std::cout << purple << qf_bit << reset;
    else if (age)
      std::cout << blue << qf_bit << reset;
    else
      std::cout << qf_bit;
    if ((i + 1) % bits_per_entry > 3 && !qf_bit)
      age = false;
    if ((i + 1) % bits_per_entry == 0) {
      std::cout << " ";
      age = true;
    }
    if ((i + 1) % (bits_per_entry * 4) == 0)
      std::cout << std::endl;
  }
  if (i % bits_per_entry * 4 != 0)
    std::cout << std::endl;
}

// For debugging purposes
inline void print_bits_diff(const QuotientFilter &qf, const std::vector<bool> &bs) {
  const std::string purple = "\033[1;35m";
  const std::string blue = "\033[1;34m";
  const std::string red = "\033[1;31m";
  const std::string reset = "\033[0m";

  const size_t bits_per_entry = qf.bit_per_entry;

  std::cout << "QuotientFilter bits:" << std::endl;
  size_t i = 0;
  bool age = true;
  for (; i < bs.size(); ++i) {
    bool qf_bit = qf.get_bit_at_offset(i);
    if (qf_bit != bs[i])
      std::cout << red << qf_bit << reset;
    else if ((i + 1) % bits_per_entry > 0 && (i + 1) % bits_per_entry <= 3)
      std::cout << purple << qf_bit << reset;
    else if (age)
      std::cout << blue << qf_bit << reset;
    else
      std::cout << qf_bit;
    if ((i + 1) % bits_per_entry > 3 && !qf_bit)
      age = false;
    if ((i + 1) % bits_per_entry == 0) {
      std::cout << " ";
      age = true;
    }
    if ((i + 1) % (bits_per_entry * 4) == 0)
      std::cout << std::endl;
  }
  if (i % bits_per_entry * 4 != 0)
    std::cout << std::endl;

  std::cout << "Bitset bits:" << std::endl;
  for (i = 0, age = true; i < bs.size(); ++i) {
    if (qf.get_bit_at_offset(i) != bs[i])
      std::cout << red << bs[i] << reset;
    else if ((i + 1) % bits_per_entry > 0 && (i + 1) % bits_per_entry <= 3)
      std::cout << purple << bs[i] << reset;
    else if (age)
      std::cout << blue << bs[i] << reset;
    else
      std::cout << bs[i];
    if ((i + 1) % bits_per_entry > 3 && !bs[i])
      age = false;
    if ((i + 1) % bits_per_entry == 0) {
      std::cout << " ";
      age = true;
    }
    if ((i + 1) % (bits_per_entry * 4) == 0)
      std::cout << std::endl;
  }
  if (i % bits_per_entry * 4 != 0)
    std::cout << std::endl;
}

inline void CHECK_EQUALITY(const QuotientFilter &qf, const std::vector<bool> &bs,
                           bool check_also_fingerprints) {
  for (size_t i = 0; i < bs.size(); i++) {
    if (check_also_fingerprints ||
        (i % qf.bit_per_entry == 0 || i % qf.bit_per_entry == 1 || i % qf.bit_per_entry == 2)) {
      if (qf.get_bit_at_offset(i) != bs[i]) {
        std::cout << "Mismatch at bit " << i << ": QuotientFilter=" << qf.get_bit_at_offset(i)
                  << ", Vector=" << bs[i] << std::endl;
        print_bits_diff(qf, bs);
      }
      REQUIRE(qf.get_bit_at_offset(i) == bs[i]);
    }
  }
}

// insert some entries and make sure we get (true) positives for all entries we had inserted.
// This is to verify we do not get any false negatives.
// We then also check the false positive rate
template <typename F> inline void CHECK_NO_FALSE_NEGATIVES(F filter, size_t num_entries) {
  std::unordered_set<uint64_t> added;
  const uint64_t seed = 5;
  std::mt19937_64 rand(seed);
  std::uniform_int_distribution<uint64_t> dist;

  for (size_t i = 0; i < num_entries; ++i) {
    const uint64_t rand_num = dist(rand);
    REQUIRE(dynamic_cast<Filter &>(filter).insert(rand_num, false));
    added.insert(rand_num);
  }

  for (const uint64_t &i : added)
    REQUIRE(dynamic_cast<Filter &>(filter).query(i));
}

// Here we're going to create a largish filter, and then perform deletes and insertions.
// We want to make sure we indeed get a positive for every entry that we inserted and still not
// deleted for every 2 insertions, we make one deletes, in order to still allow the filter to expand
inline void CHECK_INSERTIONS_AND_REMOVES(BasicInfiniFilter &qf) {
  size_t num_entries_power = qf.power_of_two_size;
  const unsigned seed = 2;
  std::set<uint64_t> added;
  std::mt19937 rand(seed);
  const auto num_entries_to_insert =
      static_cast<size_t>(std::pow(2, num_entries_power + 10)); // we'll expand 3-4 times

  for (size_t i = 0; i < num_entries_to_insert; ++i) {
    const uint64_t rand_num = rand();
    if (added.find(rand_num) == added.end()) {
      bool success = qf.Filter::insert(rand_num, false);
      if (success) {
        added.insert(rand_num);
        REQUIRE(qf.Filter::query(rand_num));
      }
    }

    if (i % 4 == 0 && i > static_cast<size_t>(std::pow(2, num_entries_power))) {
      const uint64_t rand_num = rand();
      const auto to_remove = get_set_floor(added, rand_num).value_or(UINT64_MAX);
      if (to_remove != UINT64_MAX) {
        added.erase(to_remove);
        REQUIRE(qf.Filter::remove(to_remove));
      }
    }

    uint64_t key = rand();
    const auto to_query = get_set_floor(added, key).value_or(UINT64_MAX);
    if (to_query != UINT64_MAX)
      REQUIRE(qf.Filter::query(to_query));
  }

  for (const auto &i : added)
    REQUIRE(qf.Filter::query(i));
}

// This test is based on the example from
// https://en.wikipedia.org/wiki/Quotient_filter
// it performs the same insertions and query as the example and verifies that it gets the same
// results.
TEST_CASE("QuotientFilter Test based on Wikipedia example", "[quotientfilter][test1]") {
  const size_t bits_per_entry = 8;
  const size_t num_entries_power = 3;
  const size_t num_entries = 1 << num_entries_power;
  QuotientFilter qf(num_entries_power, bits_per_entry);

  const uint64_t fingerprint0 = 0;
  const uint64_t fingerprint1 = (1 << bits_per_entry) - 1;

  qf.insert(fingerprint0, 1, false);
  qf.insert(fingerprint1, 4, false);
  qf.insert(fingerprint0, 7, false);
  qf.insert(fingerprint0, 1, false);
  qf.insert(fingerprint0, 2, false);
  qf.insert(fingerprint0, 1, false);

  std::vector<bool> result(num_entries * bits_per_entry);
  result = set_slot_in_test(result, bits_per_entry, 0, false, false, false, fingerprint0);
  result = set_slot_in_test(result, bits_per_entry, 1, true, false, false, fingerprint0);
  result = set_slot_in_test(result, bits_per_entry, 2, true, true, true, fingerprint0);
  result = set_slot_in_test(result, bits_per_entry, 3, false, true, true, fingerprint0);
  result = set_slot_in_test(result, bits_per_entry, 4, true, false, true, fingerprint0);
  result = set_slot_in_test(result, bits_per_entry, 5, false, false, true, fingerprint1);
  result = set_slot_in_test(result, bits_per_entry, 6, false, false, false, fingerprint0);
  result = set_slot_in_test(result, bits_per_entry, 7, true, false, false, fingerprint0);

  CHECK_EQUALITY(qf, result, true);

  REQUIRE(qf.num_existing_entries == 6);
}

// This test is based on the example from the quotient filter paper
// it performs the same insertions as in Figure 2 and checks for the same result
TEST_CASE("QuotientFilter Test based on quotient filter paper example", "[quotientfilter][test2]") {
  const size_t bits_per_entry = 8;
  const size_t num_entries_power = 4;
  const size_t num_entries = 1 << num_entries_power;
  QuotientFilter qf(num_entries_power, bits_per_entry);

  qf.insert(0, 1, false);
  qf.insert(0, 1, false);
  qf.insert(0, 3, false);
  qf.insert(0, 3, false);
  qf.insert(0, 3, false);
  qf.insert(0, 4, false);
  qf.insert(0, 6, false);
  qf.insert(0, 6, false);

  std::vector<bool> result(num_entries * bits_per_entry);
  result = set_slot_in_test(result, bits_per_entry, 0, false, false, false, 0);
  result = set_slot_in_test(result, bits_per_entry, 1, true, false, false, 0);
  result = set_slot_in_test(result, bits_per_entry, 2, false, true, true, 0);
  result = set_slot_in_test(result, bits_per_entry, 3, true, false, false, 0);
  result = set_slot_in_test(result, bits_per_entry, 4, true, true, true, 0);
  result = set_slot_in_test(result, bits_per_entry, 5, false, true, true, 0);
  result = set_slot_in_test(result, bits_per_entry, 6, true, false, true, 0);
  result = set_slot_in_test(result, bits_per_entry, 7, false, false, true, 0);
  result = set_slot_in_test(result, bits_per_entry, 8, false, true, true, 0);

  CHECK_EQUALITY(qf, result, false);
}

// test we don't get any false negatives for quotient filter
TEST_CASE("QuotientFilter Test for No False Negatives", "[quotientfilter][test3]") {
  const size_t bits_per_entry = 10;
  const size_t num_entries_power = 10;
  const auto num_entries = static_cast<size_t>(std::pow(2, num_entries_power) * 0.9);
  QuotientFilter filter(num_entries_power, bits_per_entry);

  CHECK_NO_FALSE_NEGATIVES(filter, num_entries);
}

// adds two entries to the end of the filter, causing an overflow
// checks this can be handled
TEST_CASE("QuotientFilter Overflow Test", "[quotientfilter][test4]") {
  const size_t bits_per_entry = 8;
  const size_t num_entries_power = 3;
  const size_t num_entries = 1 << num_entries_power;
  const size_t fingerprint_size = bits_per_entry - 3;
  QuotientFilter qf(num_entries_power, bits_per_entry);

  const uint64_t fp2 = 1 << (fingerprint_size - 1);

  qf.insert(fp2, num_entries - 1, false);
  qf.insert(fp2, num_entries - 1, false);

  qf.remove(fp2, num_entries - 1);
  REQUIRE(qf.query(fp2, num_entries - 1));
}

// This is a test for deleting items. We insert many keys into one slot to create an overflow.
// We then remove them and check that the other keys are back to their canonical slots.
TEST_CASE("QuotientFilter Delete Overflow Test", "[quotientfilter][test5]") {
  const size_t bits_per_entry = 8;
  const size_t num_entries_power = 3;
  const size_t num_entries = 1 << num_entries_power;
  QuotientFilter qf(num_entries_power, bits_per_entry);

  const uint64_t fp1 = 1 << 4;
  const uint64_t fp2 = 1 << 3;
  const uint64_t fp3 = 1 << 2;
  const uint64_t fp4 = 31;

  qf.insert(fp4, 1, false);
  qf.insert(fp1, 1, false);
  qf.insert(fp1, 1, false);
  qf.insert(fp2, 2, false);
  qf.insert(fp1, 1, false);
  qf.insert(fp1, 1, false);
  qf.insert(fp3, 4, false);

  qf.remove(fp4, 1);
  qf.remove(fp1, 1);
  qf.remove(fp1, 1);
  qf.remove(fp1, 1);
  qf.remove(fp1, 1);

  std::vector<bool> result(num_entries * bits_per_entry);
  result = set_slot_in_test(result, bits_per_entry, 2, true, false, false, fp2);
  result = set_slot_in_test(result, bits_per_entry, 4, true, false, false, fp3);

  CHECK_EQUALITY(qf, result, true);
}

// delete testing
TEST_CASE("QuotientFilter Delete Testing", "[quotientfilter][test16]") {
  const size_t bits_per_entry = 8;
  const size_t num_entries_power = 3;
  const size_t num_entries = 1 << num_entries_power;
  QuotientFilter qf(num_entries_power, bits_per_entry);

  qf.insert(0, 1, false);
  qf.insert(0, 1, false);
  qf.insert(0, 2, false);
  qf.insert(0, 2, false);
  qf.insert(0, 3, false);
  qf.insert(0, 3, false);
  qf.insert(0, 3, false);
  qf.insert(0, 6, false);
  qf.insert(0, 6, false);
  qf.insert(0, 6, false);
  qf.insert(0, 7, false);

  qf.remove(0, 2);
  qf.remove(0, 3);

  std::vector<bool> result(num_entries * bits_per_entry);
  result = set_slot_in_test(result, bits_per_entry, 1, true, false, false, 0);
  result = set_slot_in_test(result, bits_per_entry, 2, true, true, true, 0);
  result = set_slot_in_test(result, bits_per_entry, 3, true, false, true, 0);
  result = set_slot_in_test(result, bits_per_entry, 4, false, false, true, 0);
  result = set_slot_in_test(result, bits_per_entry, 5, false, true, true, 0);
  result = set_slot_in_test(result, bits_per_entry, 6, true, false, false, 0);
  result = set_slot_in_test(result, bits_per_entry, 7, true, true, true, 0);

  CHECK_EQUALITY(qf, result, true);
}

// This is a test for deleting items. We insert many keys into one slot to create an overflow.
// We then remove them and check that the other keys are back to their canonical slots.
TEST_CASE("QuotientFilter Test for Deleting Items", "[quotientfilter][test17]") {
  const size_t bits_per_entry = 8;
  const size_t num_entries_power = 3;
  const size_t num_entries = 1 << num_entries_power;
  QuotientFilter qf(num_entries_power, bits_per_entry);

  qf.insert(0, 1, false);
  qf.insert(0, 1, false);
  qf.insert(0, 2, false);
  qf.insert(0, 2, false);
  qf.insert(0, 3, false);
  qf.insert(0, 4, false);
  qf.insert(0, 4, false);
  qf.insert(0, 5, false);

  qf.remove(0, 3);

  std::vector<bool> result(num_entries * bits_per_entry, false);
  result = set_slot_in_test(result, bits_per_entry, 1, true, false, false, 0);
  result = set_slot_in_test(result, bits_per_entry, 2, true, true, true, 0);
  result = set_slot_in_test(result, bits_per_entry, 3, false, false, true, 0);
  result = set_slot_in_test(result, bits_per_entry, 4, true, true, true, 0);
  result = set_slot_in_test(result, bits_per_entry, 5, true, false, true, 0);
  result = set_slot_in_test(result, bits_per_entry, 6, false, true, true, 0);
  result = set_slot_in_test(result, bits_per_entry, 7, false, false, true, 0);

  CHECK_EQUALITY(qf, result, true);
}

TEST_CASE("QuotientFilter Iteration with Overflow", "[quotientfilter][test6]") {
  const size_t bits_per_entry = 8;
  const size_t num_entries_power = 4;
  QuotientFilter qf(num_entries_power, bits_per_entry);

  qf.insert(0, 2, false);
  qf.insert(0, 3, false);
  qf.insert(0, 3, false);
  qf.insert(0, 4, false);
  qf.insert(0, 23, false);
  qf.insert(0, 24, false);

  std::vector<size_t> expected = {2, 3, 3, 4, 23};
  size_t arr_index = 0;

  Iterator it(qf);
  while (it.next()) {
    REQUIRE(arr_index < expected.size());
    REQUIRE(expected[arr_index++] == it.bucket_index);
  }
  REQUIRE(arr_index == expected.size());
}

TEST_CASE("QuotientFilter Another Iteration with Overflow", "[quotientfilter][test7]") {
  const size_t bits_per_entry = 8;
  const size_t num_entries_power = 4;
  QuotientFilter qf(num_entries_power, bits_per_entry);

  qf.insert(0, 1, false);
  qf.insert(0, 4, false);
  qf.insert(0, 7, false);
  qf.insert(0, 1, false);
  qf.insert(0, 2, false);
  qf.insert(0, 1, false);
  qf.insert(0, 15, false);

  std::vector<size_t> expected = {1, 1, 1, 2, 4, 7, 15};
  size_t arr_index = 0;

  Iterator it(qf);
  while (it.next()) {
    REQUIRE(arr_index < expected.size());
    REQUIRE(expected[arr_index++] == it.bucket_index);
  }
  REQUIRE(arr_index == expected.size());
}

TEST_CASE("QuotientFilter Expansion Test", "[quotientfilter][test8]") {
  const size_t bits_per_entry = 10;
  const size_t num_entries_power = 4;
  FingerprintSacrifice qf(num_entries_power, bits_per_entry);
  qf.max_entries_before_expansion = UINT64_MAX; // disable automatic expansion

  for (uint64_t i = 0; i < 12; i++)
    qf.Filter::insert(i, false);

  qf.expand();

  QuotientFilter qf2(num_entries_power + 1, bits_per_entry - 1);

  for (uint64_t i = 0; i < 12; i++)
    qf2.Filter::insert(i, false);

  REQUIRE(qf.filter->size() == qf2.filter->size());

  for (size_t i = 0; i < qf.get_logical_num_slots(); i++) {
    auto set1 = qf.get_all_fingerprints(i);
    auto set2 = qf2.get_all_fingerprints(i);
    REQUIRE(set1 == set2);
  }
}

// insert entries across two phases of expansion, and then check we can still find all of them
TEST_CASE("Insert entries across two phases of expansion and check we can still find all of them",
          "[quotientfilter][test9]") {
  const size_t bits_per_entry = 10;
  const size_t num_entries_power = 3;
  Chaining qf(num_entries_power, bits_per_entry);
  qf.max_entries_before_expansion = UINT64_MAX; // disable automatic expansion

  size_t i = 0;
  while (i < static_cast<size_t>(std::pow(2, num_entries_power)) - 2) {
    REQUIRE(qf.Filter::insert(i, false));
    i++;
  }
  qf.expand();

  while (i < static_cast<size_t>(std::pow(2, num_entries_power + 1)) - 2) {
    REQUIRE(qf.Filter::insert(i, false));
    i++;
  }

  for (size_t j = 0; j < i; j++)
    REQUIRE(qf.query(j));
}

TEST_CASE("BasicInfiniFilter Expansion and Search", "[infinifilter][test10]") {
  const size_t bits_per_entry = 10;
  const size_t num_entries_power = 3;
  BasicInfiniFilter qf(num_entries_power, bits_per_entry);
  qf.hash_type = HashType::ARBITRARY;

  uint64_t i = 1;
  while (i < static_cast<uint64_t>(std::pow(2, num_entries_power)) - 1) {
    REQUIRE(qf.Filter::insert(i, false));
    i++;
  }

  qf.expand();

  const size_t num_entries = 1 << (num_entries_power + 1);
  std::vector<bool> result(num_entries * bits_per_entry, false);
  result = set_slot_in_test(result, bits_per_entry, 0, false, false, false, "0000000");
  result = set_slot_in_test(result, bits_per_entry, 1, true, false, false, "1100101");
  result = set_slot_in_test(result, bits_per_entry, 2, true, false, false, "1010101");
  result = set_slot_in_test(result, bits_per_entry, 3, false, false, false, "0000000");
  result = set_slot_in_test(result, bits_per_entry, 4, false, false, false, "0000000");
  result = set_slot_in_test(result, bits_per_entry, 5, true, false, false, "0010001");
  result = set_slot_in_test(result, bits_per_entry, 6, false, false, false, "0000000");
  result = set_slot_in_test(result, bits_per_entry, 7, true, false, false, "0101101");
  result = set_slot_in_test(result, bits_per_entry, 8, true, false, false, "1001001");
  result = set_slot_in_test(result, bits_per_entry, 9, false, true, true, "0111001");

  CHECK_EQUALITY(qf, result, true);

  uint64_t j = 1;
  while (j < static_cast<uint64_t>(std::pow(2, num_entries_power)) - 1) {
    REQUIRE(qf.Filter::query(j));
    j++;
  }
}

// This test ensures we issue enough insertions until the fingerprints of at least some of the
// first entries inserted run out. This means that for these entries, we are going to try the
// chaining technique to avoid false negatives.
TEST_CASE("ChainedInfiniFilter Insertion and Chaining", "[infinifilter][test12]") {
  const size_t bits_per_entry = 7;
  const size_t num_entries_power = 3;
  infinifilter::ChainedInfiniFilter qf(num_entries_power, bits_per_entry);
  qf.set_expand_autonomously(true);
  qf.set_fpr_style(FingerprintGrowthStrategy::FalsePositiveRateExpansion::POLYNOMIAL);

  const auto max_key =
      static_cast<uint64_t>(std::pow(2, num_entries_power + qf.fingerprint_length * 3 + 7));
  for (uint64_t i = 0; i < max_key; ++i) {
    REQUIRE(qf.Filter::insert(i, false));
    REQUIRE(qf.Filter::query(i));
  }

  for (uint64_t i = 0; i < max_key; ++i)
    REQUIRE(qf.Filter::query(i));

  size_t false_positives = 0;
  for (uint64_t i = max_key; i < max_key + 10000; ++i) {
    if (qf.Filter::query(i))
      false_positives++;
  }
  // Should have had a few false positives
  REQUIRE(false_positives != 0);
}

// Here we test the rejuvenation operation of InfiniFilter
TEST_CASE("InfiniFilter Rejuvenation", "[infinifilter][test13]") {
  const size_t bits_per_entry = 7;
  const size_t num_entries_power = 2;
  BasicInfiniFilter qf(num_entries_power, bits_per_entry);
  qf.hash_type = HashType::ARBITRARY;
  qf.expand_autonomously = false;

  qf.Filter::insert(2, false);
  qf.expand();
  qf.rejuvenate(2);

  std::vector<bool> result(qf.get_logical_num_slots() * bits_per_entry, false);
  result = set_slot_in_test(result, bits_per_entry, 0, true, false, false, 3);

  CHECK_EQUALITY(qf, result, true);
}

// Testing the capability of InfiniFilter to delete the longest matching fingerprint
TEST_CASE("InfiniFilter Delete Longest Matching Fingerprint", "[infinifilter][test14]") {
  const size_t bits_per_entry = 8;
  const size_t num_entries_power = 2;
  const auto num_entries = static_cast<size_t>(std::pow(2, num_entries_power)) + 3;
  BasicInfiniFilter qf(num_entries_power, bits_per_entry);

  const uint32_t fp1 = 1;
  const uint32_t fp2 = 2;
  const uint32_t fp3 = 0;

  qf.insert(fp1, 1, false);

  qf.expand();

  qf.insert(fp3, 5, false);

  qf.insert(fp2, 5, false);

  qf.remove(fp3, 5); // We must delete the longest matching fingerprint

  std::vector<bool> result(num_entries * bits_per_entry, false);
  result = set_slot_in_test(result, bits_per_entry, 5, true, false, false, 16);
  result = set_slot_in_test(result, bits_per_entry, 6, false, true, true, fp2);

  CHECK_EQUALITY(qf, result, true);
}

TEST_CASE("InfiniFilter Insertion and Deletion", "[infinifilter][test15]") {
  const size_t bits_per_entry = 10;
  const size_t num_entries_power = 3;
  infinifilter::ChainedInfiniFilter qf(num_entries_power, bits_per_entry);
  qf.set_expand_autonomously(true);

  CHECK_INSERTIONS_AND_REMOVES(qf);
}

// Here we're going to create a largish filter, and then perform insertions and rejuvenation
// operations. We'll test correctness by ensuring all keys we have inserted indeed still give
// positives.
TEST_CASE("InfiniFilter Insertion and Rejuvenation", "[infinifilter][test18]") {
  const size_t bits_per_entry = 16;
  const size_t num_entries_power = 3;
  const unsigned seed = 5;
  infinifilter::ChainedInfiniFilter qf(num_entries_power, bits_per_entry);
  qf.expand_autonomously = true;
  std::set<uint64_t> added;
  std::mt19937 rand(seed);
  const auto num_entries_to_insert =
      static_cast<size_t>(std::pow(2, num_entries_power + 15)); // will expand 3-4 times

  for (size_t i = 0; i < num_entries_to_insert; ++i) {
    uint64_t rand_num = rand();
    if (added.find(rand_num) == added.end()) {
      bool success = qf.Filter::insert(rand_num, false);
      if (success) {
        added.insert(rand_num);
        REQUIRE(qf.Filter::query(rand_num));
      }
    }

    if (i % 4 == 0 && i > static_cast<size_t>(std::pow(2, num_entries_power))) {
      const uint64_t rand_num = rand();
      const auto to_remove = get_set_floor(added, rand_num).value_or(UINT64_MAX);
      if (to_remove != UINT64_MAX) {
        added.erase(to_remove);
        REQUIRE(qf.Filter::remove(to_remove));
      }
    }

    if (i % 2 == 0 && i > static_cast<size_t>(std::pow(2, num_entries_power))) {
      const uint64_t rand_num = rand();
      const auto to_rejuv = get_set_floor(added, rand_num).value_or(UINT64_MAX);
      if (to_rejuv != UINT64_MAX) {
        REQUIRE(qf.rejuvenate(to_rejuv));
        REQUIRE(qf.Filter::query(to_rejuv));
      }

      const uint64_t key = rand();
      const auto to_query = get_set_floor(added, key).value_or(UINT64_MAX);
      if (to_query != UINT64_MAX)
        REQUIRE(qf.Filter::query(to_query));
    }
  }

  for (const auto &i : added)
    REQUIRE(qf.Filter::query(i));
}

TEST_CASE("Hash Function Equality", "[hash_functions][test20]") {
  constexpr size_t TRIALS = 1000;

  std::random_device rd;
  std::mt19937_64 rnd(rd());

  for (size_t i = 0; i < TRIALS; ++i) {
    const uint64_t input = rnd();
    const auto hash1 = HashFunctions::xxhash(input);
    const auto hash2 = HashFunctions::xxhash(input);
    REQUIRE(hash1 == hash2);
  }
}

TEST_CASE("QuotientFilter Multi-Type Insertion and Deletion", "[quotientfilter][test21]") {
  const size_t TRIALS = 1000;

  std::random_device rd;
  std::mt19937 rnd(rd());

  std::string input_string;
  std::vector<uint8_t> input_bytes(16);

  const size_t bits_per_entry = 16;
  const size_t num_entries_power = 27;
  QuotientFilter qf(num_entries_power, bits_per_entry);

  for (int i = 0; i < TRIALS; ++i) {
    // Test with integer input
    const uint64_t input_int = rnd();
    REQUIRE(qf.Filter::insert(input_int, false));
    REQUIRE(qf.Filter::query(input_int));
    REQUIRE(qf.Filter::remove(input_int));
    REQUIRE(!qf.Filter::query(input_int));

    // Test with long input
    const uint64_t input_long = rnd();
    REQUIRE(qf.Filter::insert(input_long, false));
    REQUIRE(qf.Filter::query(input_long));
    REQUIRE(qf.Filter::remove(input_long));
    REQUIRE(!qf.Filter::query(input_long));

    // Test with string input
    std::ranges::generate(input_bytes, [&] { return rnd() % 256; });
    input_string = std::string(input_bytes.begin(), input_bytes.end());
    REQUIRE(qf.Filter::insert(input_string, false));
    REQUIRE(qf.Filter::query(input_string));
    REQUIRE(qf.Filter::remove(input_string));
    REQUIRE(!qf.Filter::query(input_string));

    // Test with byte array input
    std::ranges::generate(input_bytes, [&] { return rnd() % 256; });
    REQUIRE(qf.Filter::insert(input_bytes, false));
    REQUIRE(qf.Filter::query(input_bytes));
    REQUIRE(qf.Filter::remove(input_bytes));
    REQUIRE(!qf.Filter::query(input_bytes));
  }
}

namespace infinifilter::Experiment {

class Baseline {
public:
  std::map<std::string, std::vector<double>> metrics;

  Baseline() {
    metrics["num_entries"] = std::vector<double>();
    metrics["insertion_time"] = std::vector<double>();
    metrics["query_time"] = std::vector<double>();
    metrics["FPR"] = std::vector<double>();
    metrics["memory"] = std::vector<double>();
    metrics["avg_run_length"] = std::vector<double>();
    metrics["avg_cluster_length"] = std::vector<double>();
  }
};

inline void scalability_experiment(Filter &qf, uint64_t initial_key, uint64_t end_key,
                                   Baseline &results) {
  constexpr size_t NUM_QUERIES = 1000000;

  size_t query_index = std::numeric_limits<size_t>::max();
  size_t num_false_positives = 0;

  const size_t initial_num_entries = initial_key;
  size_t insertion_index = initial_key;
  const auto start_insertions = std::chrono::high_resolution_clock::now();

  bool successful_insert = false;
  do {
    successful_insert = qf.insert(insertion_index, false);
    insertion_index++;
  } while (insertion_index < end_key && successful_insert);

  REQUIRE(successful_insert);

  const auto end_insertions = std::chrono::high_resolution_clock::now();
  const auto start_queries = std::chrono::high_resolution_clock::now();

  for (size_t i = 0; i < NUM_QUERIES || num_false_positives < 10; ++i) {
    const bool found = qf.query(query_index--);
    if (found)
      num_false_positives++;
    if (i > NUM_QUERIES * 10)
      break;
  }

  const size_t actual_num_queries = std::numeric_limits<size_t>::max() - query_index;

  const auto end_queries = std::chrono::high_resolution_clock::now();
  const double avg_insertions =
      std::chrono::duration<double>(end_insertions - start_insertions).count() /
      static_cast<double>(insertion_index - initial_num_entries);
  const double avg_queries = std::chrono::duration<double>(end_queries - start_queries).count() /
                             static_cast<double>(actual_num_queries);
  const double FPR =
      static_cast<double>(num_false_positives) / static_cast<double>(actual_num_queries);

  const size_t num_entries = qf.get_num_entries(true);
  const double bits_per_entry = qf.measure_num_bits_per_entry();

  results.metrics["num_entries"].push_back(static_cast<double>(num_entries));
  results.metrics["insertion_time"].push_back(avg_insertions);
  results.metrics["query_time"].push_back(avg_queries);
  results.metrics["FPR"].push_back(FPR);
  results.metrics["memory"].push_back(bits_per_entry);
}

} // namespace infinifilter::Experiment

void CHECK_FPR(Filter &f, double model_FPR, size_t insertions) {
  Experiment::Baseline results;
  Experiment::scalability_experiment(f, 0, insertions, results);
  const double FPR = results.metrics["FPR"][0];

  REQUIRE(FPR <= model_FPR * 1.1);
  REQUIRE(FPR >= model_FPR / 2);
}

// testing the false positive rate is as expected
TEST_CASE("Testing false positive rate for QuotientFilter", "[quotientfilter][test24]") {
  const size_t num_entries_power = 15;
  const auto num_entries = static_cast<size_t>(std::pow(2, num_entries_power) * 0.9);

  for (size_t i = 5; i <= 16; ++i) {
    const size_t bits_per_entry = i;
    QuotientFilter qf(num_entries_power, bits_per_entry);
    const double model_FPR = std::pow(2, -static_cast<int>(bits_per_entry) + 3);
    CHECK_FPR(qf, model_FPR, num_entries);
  }
}

// this test ensures the basic infinifilter stops expanding after F expansions, where F is the
// original fingerprint size
TEST_CASE("Ensure BasicInfiniFilter stops expanding after F expansions", "[infinifilter][test27]") {
  const size_t bits_per_entry = 10;
  const size_t num_entries_power = 3;
  BasicInfiniFilter qf(num_entries_power, bits_per_entry);
  qf.expand_autonomously = true;
  qf.set_fpr_style(FingerprintGrowthStrategy::FalsePositiveRateExpansion::UNIFORM);

  const auto max_key =
      static_cast<size_t>(std::pow(2, num_entries_power + qf.get_fingerprint_length() * 4 + 1));
  for (size_t i = 0; i < max_key; ++i)
    if (!qf.Filter::insert(i, false))
      break;

  REQUIRE(qf.num_expansions <= qf.original_fingerprint_size);
  REQUIRE(qf.get_num_void_entries() != 0);
}
