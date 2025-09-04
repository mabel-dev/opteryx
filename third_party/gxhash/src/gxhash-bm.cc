#include "gxhash.h"

#include <benchmark/benchmark.h>
#include <cstddef>
#include <cstdint>

std::string gen_random_string(const size_t len) {
  static const char alphanum[] = "0123456789"
                                 "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                 "abcdefghijklmnopqrstuvwxyz";
  std::string tmp_s;
  tmp_s.reserve(len);

  for (size_t i = 0; i < len; ++i) {
    tmp_s += alphanum[rand() % (sizeof(alphanum) - 1)];
  }

  return tmp_s;
}

std::string test_string = gen_random_string(1000000);

constexpr int ITER = 1000;

void BM_gxhash(benchmark::State &state) {
  size_t len = state.range(0);
  size_t bytes = 0;

  uint32_t hash = 0;

  for (auto s : state) {
    for (int i = 0; i < ITER; i++) {
      hash |= gxhash::gxhash32(
          reinterpret_cast<const uint8_t *>(test_string.data()), len, hash);
      bytes += len;
    }
    benchmark::DoNotOptimize(hash);
  }
  state.SetBytesProcessed(bytes);
}

BENCHMARK(BM_gxhash)
    ->Arg(5)
    ->Arg(7)
    ->Arg(17)
    ->Arg(29)
    ->Arg(47)
    ->Arg(77)
    ->Arg(120)
    ->Arg(147)
    ->Arg(349)
    ->Arg(679)
    ->Arg(1024)
    ->Arg(1440)
    ->Arg(2048)
    ->Arg(4096)
    ->Arg(16384)
    ->Arg(65536)
    ->Arg(262144)
    ->Arg(test_string.length());

BENCHMARK_MAIN();
