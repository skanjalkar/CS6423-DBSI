#include <gtest/gtest.h>
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <exception>
#include <random>
#include <utility>
#include <vector>

#include "external_sort/external_sort.h"
#include "storage/test_file.h"

namespace {

static_assert(sizeof(uint64_t) == 8, "sizeof(uint64_t) must be 8");

constexpr size_t MEM_1KiB = 1ul << 10;
constexpr size_t MEM_1MiB = 1ul << 20;

TEST(ExternalSortTest, EmptyFile) {
  buzzdb::TestFile input{buzzdb::File::READ};
  buzzdb::TestFile output;
  buzzdb::external_sort(input, 0, output, MEM_1MiB);
  ASSERT_EQ(0, output.size());
}

TEST(ExternalSortTest, OneValue) {
  std::vector<char> file_content(8);
  uint64_t input_value = 0xabab42f00f00;
  std::memcpy(file_content.data(), &input_value, 8);
  buzzdb::TestFile input{std::move(file_content)};
  buzzdb::TestFile output;

  buzzdb::external_sort(input, 1, output, MEM_1MiB);

  ASSERT_EQ(8, output.size());
  uint64_t output_value = 0;
  std::memcpy(&output_value, output.get_content().data(), 8);
  ASSERT_EQ(input_value, output_value);
}

std::vector<uint64_t> get_file_values(buzzdb::TestFile file) {
  auto& content = file.get_content();
  std::vector<uint64_t> values(content.size() / 8);
  std::memcpy(values.data(), content.data(), content.size());
  return values;
}

class ExternalSortParametrizedTest
    : public ::testing::TestWithParam<std::pair<size_t, size_t>> {};

TEST_P(ExternalSortParametrizedTest, SortDescendingNumbers) {
  auto [mem_size, num_values] = GetParam();
  std::vector<uint64_t> expected_values(num_values);
  std::vector<char> file_content(num_values * 8);
  {
    std::vector<uint64_t> file_values(num_values);
    for (size_t i = 0; i < num_values; ++i) {
      expected_values[i] = i + 1;
      file_values[i] = num_values - i;
    }
    std::memcpy(file_content.data(), file_values.data(), num_values * 8);
  }
  buzzdb::TestFile input{std::move(file_content)};
  buzzdb::TestFile output;

  buzzdb::external_sort(input, num_values, output, mem_size);

  ASSERT_EQ(num_values * 8, output.size());
  auto output_values = get_file_values(output);
  ASSERT_EQ(expected_values, output_values);
}

INSTANTIATE_TEST_CASE_P(
    ExternalSortTest, ExternalSortParametrizedTest,
    ::testing::Values(
        // All values fit in memory:
        std::make_pair(MEM_1KiB, 3), std::make_pair(MEM_1KiB, 40),
        std::make_pair(MEM_1KiB, 128), std::make_pair(MEM_1MiB, 100000),
        // n-way merge required:
        std::make_pair(MEM_1KiB, 129), std::make_pair(MEM_1KiB, 997),
        std::make_pair(MEM_1KiB, 1024), std::make_pair(MEM_1MiB, 200000)));
}
int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
