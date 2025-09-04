#pragma once

#include "../compiler.h"

#include <arm_neon.h>
#include <cstddef>
#include <cstdint>

namespace gxhash {

namespace impl {

using state = int8x16_t;

static constexpr size_t VECTOR_SIZE = sizeof(state);

GXHASH_ALWAYS_INLINE state create_empty() { return vdupq_n_s8(0); }

GXHASH_ALWAYS_INLINE state create_seed(int64_t seed) {
  return vreinterpretq_s8_s64(vdupq_n_s64(seed));
}

GXHASH_ALWAYS_INLINE state load_unaligned(const state *&p) {
  auto tmp = p;
  p++;
  return vld1q_s8(reinterpret_cast<const int8_t *>(tmp));
}

GXHASH_ALWAYS_INLINE state get_partial_safe(const state *data, size_t len) {
  // Temporary buffer filled with zeros
  int8_t buffer[VECTOR_SIZE] = {0};
  int8_t indices_array[] = {0, 1, 2,  3,  4,  5,  6,  7,
                            8, 9, 10, 11, 12, 13, 14, 15};
  state indices = vld1q_s8(indices_array);
  uint8x16_t mask = vcgtq_s8(vdupq_n_s8(len), indices);
  state partial_vector =
      vandq_s8(load_unaligned(data), vreinterpretq_s8_u8(mask));
  return vaddq_s8(partial_vector, vdupq_n_s8(len));
}

GXHASH_ALWAYS_INLINE state get_partial_unsafe(const state *data, size_t len) {
  int8_t indices_array[] = {0, 1, 2,  3,  4,  5,  6,  7,
                            8, 9, 10, 11, 12, 13, 14, 15};
  state indices = vld1q_s8(indices_array);
  uint8x16_t mask = vcgtq_s8(vdupq_n_s8(len), indices);
  state partial_vector =
      vandq_s8(load_unaligned(data), vreinterpretq_s8_u8(mask));
  return vaddq_s8(partial_vector, vdupq_n_s8(len));
}

// See
// https://blog.michaelbrase.com/2018/05/08/emulating-x86-aes-intrinsics-on-armv8-a
GXHASH_ALWAYS_INLINE state aes_encrypt(state data, state keys) {
  // Encrypt
  auto encrypted = vaeseq_u8(vreinterpretq_u8_s8(data), vdupq_n_u8(0));
  // Mix columns
  auto mixed = vaesmcq_u8(encrypted);
  // Xor keys
  return vreinterpretq_s8_u8(veorq_u8(mixed, vreinterpretq_u8_s8(keys)));
}

// See
// https://blog.michaelbrase.com/2018/05/08/emulating-x86-aes-intrinsics-on-armv8-a
GXHASH_ALWAYS_INLINE state aes_encrypt_last(state data, state keys) {
  // Encrypt
  auto encrypted = vaeseq_u8(vreinterpretq_u8_s8(data), vdupq_n_u8(0));
  // Xor keys
  return vreinterpretq_s8_u8(veorq_u8(encrypted, vreinterpretq_u8_s8(keys)));
}

GXHASH_ALWAYS_INLINE state ld(const uint32_t *array) {
  return vreinterpretq_s8_u32(vld1q_u32(array));
}

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
    state v0 = load_unaligned(ptr);
    state v1 = load_unaligned(ptr);
    state v2 = load_unaligned(ptr);
    state v3 = load_unaligned(ptr);
    state v4 = load_unaligned(ptr);
    state v5 = load_unaligned(ptr);
    state v6 = load_unaligned(ptr);
    state v7 = load_unaligned(ptr);

    state tmp1 = aes_encrypt(v0, v2);
    state tmp2 = aes_encrypt(v1, v3);

    tmp1 = aes_encrypt(tmp1, v4);
    tmp2 = aes_encrypt(tmp2, v5);

    tmp1 = aes_encrypt(tmp1, v6);
    tmp2 = aes_encrypt(tmp2, v7);

    t1 = vaddq_s8(t1, ld(KEYS));
    t2 = vaddq_s8(t2, ld(KEYS + 4));

    lane1 = aes_encrypt_last(aes_encrypt(tmp1, t1), lane1);
    lane2 = aes_encrypt_last(aes_encrypt(tmp2, t2), lane2);
  }

  // For 'Zeroes' test
  state len_vec = vreinterpretq_s8_u32(vdupq_n_u32(len));
  lane1 = vaddq_s8(lane1, len_vec);
  lane2 = vaddq_s8(lane2, len_vec);
  // Merge lanes
  return aes_encrypt(lane1, lane2);
}

GXHASH_ALWAYS_INLINE state load_u8(uint8_t x) {
  return vreinterpretq_s8_u8(vdupq_n_u8(x));
}

GXHASH_ALWAYS_INLINE state load_u16(uint16_t x) {
  return vreinterpretq_s8_u16(vdupq_n_u16(x));
}

GXHASH_ALWAYS_INLINE state load_u32(uint32_t x) {
  return vreinterpretq_s8_u32(vdupq_n_u32(x));
}

GXHASH_ALWAYS_INLINE state load_u64(uint64_t x) {
  return vreinterpretq_s8_u64(vdupq_n_u64(x));
}

GXHASH_ALWAYS_INLINE state load_u128(__uint128_t x) {
  const uint8_t *ptr = reinterpret_cast<const uint8_t *>(&x);
  return vreinterpretq_s8_u8(vld1q_u8(ptr));
}

GXHASH_ALWAYS_INLINE state load_i8(int8_t x) { return vdupq_n_s8(x); }

GXHASH_ALWAYS_INLINE state load_i16(int16_t x) {
  return vreinterpretq_s8_s16(vdupq_n_s16(x));
}

GXHASH_ALWAYS_INLINE state load_i32(int32_t x) {
  return vreinterpretq_s8_s32(vdupq_n_s32(x));
}

GXHASH_ALWAYS_INLINE state load_i64(int64_t x) {
  return vreinterpretq_s8_s64(vdupq_n_s64(x));
}

GXHASH_ALWAYS_INLINE state load_i128(__int128 x) {
  const int8_t *ptr = reinterpret_cast<const int8_t *>(&x);
  return vld1q_s8(ptr);
}

} // namespace impl

} // namespace gxhash
