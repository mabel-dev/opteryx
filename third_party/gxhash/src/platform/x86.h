#pragma once

#include "../compiler.h"

#include <cstddef>
#include <cstdint>
#include <cstring>
#if defined(_MSC_VER)
#include <intrin.h>
#else
#include <x86intrin.h>
#endif

namespace gxhash {

namespace impl {

using state = __m128i;

constexpr size_t VECTOR_SIZE = sizeof(state);

GXHASH_ALWAYS_INLINE state create_empty() { return _mm_setzero_si128(); }

GXHASH_ALWAYS_INLINE state create_seed(int64_t seed) {
  return _mm_set1_epi64x(seed);
}

GXHASH_ALWAYS_INLINE state load_unaligned(const state *&p) {
  auto tmp = p;
  p++;
  return _mm_loadu_si128(tmp);
}

GXHASH_ALWAYS_INLINE __m256i load_unaligned_x2(const __m256i *&p) {
  auto tmp = p;
  p += 2;
  return _mm256_loadu_si256(tmp);
}

GXHASH_ALWAYS_INLINE state get_partial_safe(const state *data, size_t len) {
  // Temporary buffer filled with zeros
  uint8_t buffer[VECTOR_SIZE] = {0};
  // Copy data into the buffer
  memcpy(buffer, data, len);
  // Load the buffer into a __m256i vector
  state partial_vector = _mm_loadu_si128(reinterpret_cast<state *>(buffer));
  return _mm_add_epi8(partial_vector, _mm_set1_epi8(len));
}

GXHASH_ALWAYS_INLINE state get_partial_unsafe(const state *data, size_t len) {
  state indices =
      _mm_set_epi8(15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0);
  state mask = _mm_cmpgt_epi8(_mm_set1_epi8(len), indices);
  state partial_vector = _mm_and_si128(_mm_loadu_si128(data), mask);
  return _mm_add_epi8(partial_vector, _mm_set1_epi8(len));
}

GXHASH_ALWAYS_INLINE state aes_encrypt(state data, state keys) {
  return _mm_aesenc_si128(data, keys);
}

GXHASH_ALWAYS_INLINE state aes_encrypt_last(state data, state keys) {
  return _mm_aesenclast_si128(data, keys);
}

GXHASH_ALWAYS_INLINE state ld(const uint32_t *array) {
  return _mm_loadu_si128(reinterpret_cast<const state *>(array));
}

// #define GXHASH_HYBRID 1

#ifndef GXHASH_HYBRID

GXHASH_ALWAYS_INLINE state compress_8(const state *ptr, const state *end,
                                      state hash_vector, size_t len) {

  // Disambiguation vectors
  state t1 = create_empty();
  state t2 = create_empty();

  // Hash is processed in two separate 128-bit parallel lanes
  // This allows the same processing to be applied using 256-bit V-AES
  // intrinsics so that hashes are stable in both cases.
  state lane1 = hash_vector;
  state lane2 = hash_vector;

  while (ptr < end) {
    auto v0 = load_unaligned(ptr);
    auto v1 = load_unaligned(ptr);
    auto v2 = load_unaligned(ptr);
    auto v3 = load_unaligned(ptr);
    auto v4 = load_unaligned(ptr);
    auto v5 = load_unaligned(ptr);
    auto v6 = load_unaligned(ptr);
    auto v7 = load_unaligned(ptr);

    auto tmp1 = aes_encrypt(v0, v2);
    auto tmp2 = aes_encrypt(v1, v3);

    tmp1 = aes_encrypt(tmp1, v4);
    tmp2 = aes_encrypt(tmp2, v5);

    tmp1 = aes_encrypt(tmp1, v6);
    tmp2 = aes_encrypt(tmp2, v7);

    t1 = _mm_add_epi8(t1, ld(KEYS));
    t2 = _mm_add_epi8(t2, ld(KEYS + 4));

    lane1 = aes_encrypt_last(aes_encrypt(tmp1, t1), lane1);
    lane2 = aes_encrypt_last(aes_encrypt(tmp2, t2), lane2);
  }
  // For 'Zeroes' test
  auto len_vec = _mm_set1_epi32(len);
  lane1 = _mm_add_epi8(lane1, len_vec);
  lane2 = _mm_add_epi8(lane2, len_vec);
  // Merge lanes
  return aes_encrypt(lane1, lane2);
}

#else

GXHASH_ALWAYS_INLINE state compress_8(const state *ptr, const state *end,
                                      state hash_vector, size_t len) {
  const __m256i *ptr256 = reinterpret_cast<const __m256i *>(ptr);
  auto t = _mm256_setzero_si256();
  auto lane = _mm256_set_m128i(hash_vector, hash_vector);
  while (ptr < end) {
    auto v0 = load_unaligned_x2(ptr256);
    auto v1 = load_unaligned_x2(ptr256);
    auto v2 = load_unaligned_x2(ptr256);
    auto v3 = load_unaligned_x2(ptr256);

    ptr += 8;

    auto tmp = _mm256_aesenc_epi128(v0, v1);
    tmp = _mm256_aesenc_epi128(tmp, v2);
    tmp = _mm256_aesenc_epi128(tmp, v3);

    t = _mm256_add_epi8(
        t, _mm256_loadu_si256(reinterpret_cast<const __m256i *>(KEYS)));

    lane = _mm256_aesenclast_epi128(_mm256_aesenc_epi128(tmp, t), lane);
  }
  // Extract the two 128-bit lanes
  auto lane1 = _mm256_castsi256_si128(lane);
  auto lane2 = _mm256_extracti128_si256(lane, 1);
  // For 'Zeroes' test
  auto len_vec = _mm_set1_epi32(len);
  lane1 = _mm_add_epi8(lane1, len_vec);
  lane2 = _mm_add_epi8(lane2, len_vec);
  // Merge lanes
  return aes_encrypt(lane1, lane2);
}

#endif

GXHASH_ALWAYS_INLINE state load_u8(uint8_t x) { return _mm_set1_epi8(x); }

GXHASH_ALWAYS_INLINE state load_u16(uint16_t x) { return _mm_set1_epi16(x); }

GXHASH_ALWAYS_INLINE state load_u32(uint32_t x) { return _mm_set1_epi32(x); }

GXHASH_ALWAYS_INLINE state load_u64(uint64_t x) { return _mm_set1_epi64x(x); }

#if !defined(_MSC_VER)
GXHASH_ALWAYS_INLINE state load_u128(__uint128_t x) {
  auto ptr = (const __m128i *)&x;
  return _mm_loadu_si128(ptr);
}
#endif

GXHASH_ALWAYS_INLINE state load_i8(int8_t x) { return _mm_set1_epi8(x); }

GXHASH_ALWAYS_INLINE state load_i16(int16_t x) { return _mm_set1_epi16(x); }

GXHASH_ALWAYS_INLINE state load_i32(int32_t x) { return _mm_set1_epi32(x); }

GXHASH_ALWAYS_INLINE state load_i64(int64_t x) { return _mm_set1_epi64x(x); }

#if !defined(_MSC_VER)
GXHASH_ALWAYS_INLINE state load_i128(__int128_t x) {
  auto ptr = (const __m128i *)&x;
  return _mm_loadu_si128(ptr);
}
#endif

} // namespace impl

} // namespace gxhash
