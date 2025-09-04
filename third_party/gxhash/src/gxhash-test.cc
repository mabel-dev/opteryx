#include "gxhash.h"

#include <cstdint>
#include <cstring>
#include <gtest/gtest.h>

TEST(gxhash, is_stable) {
  std::vector<uint8_t> arr;

  arr.resize(0);
  ASSERT_EQ(2533353535, gxhash::gxhash32(arr.data(), arr.size(), 0));

  arr.resize(1);
  std::fill(arr.begin(), arr.end(), 0);
  ASSERT_EQ(4243413987, gxhash::gxhash32(arr.data(), arr.size(), 0));

  arr.resize(1000);
  std::fill(arr.begin(), arr.end(), 0);
  ASSERT_EQ(2401749549, gxhash::gxhash32(arr.data(), arr.size(), 0));

  arr.resize(4242);
  std::fill(arr.begin(), arr.end(), 42);
  ASSERT_EQ(4156851105, gxhash::gxhash32(arr.data(), arr.size(), 42));

  ASSERT_EQ(1156095992, gxhash::gxhash32((const uint8_t *)"Hello World", 11,
                                         std::numeric_limits<int64_t>::max()));

  ASSERT_EQ(540827083, gxhash::gxhash32((const uint8_t *)"Hello World", 11,
                                        std::numeric_limits<int64_t>::min()));
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
