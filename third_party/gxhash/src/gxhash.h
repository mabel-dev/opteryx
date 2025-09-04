#pragma once

#include "compiler.h"

#include <cstddef>
#include <cstdint>

namespace gxhash {

namespace impl {

static constexpr uint32_t KEYS[12] = {
    0xF2784542, 0xB09D3E21, 0x89C222E5, 0xFC3BC28E, 0x03FCE279, 0xCB6B2E9B,
    0xB361DC58, 0x39132BD9, 0xD0012E32, 0x689D2B7D, 0x5544B1B7, 0xC78B122B};

}

} // namespace gxhash

#if defined(__x86_64__) || defined(_M_X64)
#include "platform/x86.h"
#elif __aarch64__
#include "platform/arm.h"
#endif

namespace gxhash {

namespace impl {

constexpr size_t PAGE_SIZE = 0x1000;

GXHASH_ALWAYS_INLINE bool check_same_page(const state *ptr) {
  uintptr_t address = reinterpret_cast<uintptr_t>(ptr);
  // Mask to keep only the last 12 bits
  size_t offset_within_page = address & (PAGE_SIZE - 1);
  // Check if the 16th byte from the current offset exceeds the page boundary
  return offset_within_page < PAGE_SIZE - VECTOR_SIZE;
}

GXHASH_ALWAYS_INLINE state get_partial(const state *p, size_t len) {
  // Safety check
  if (check_same_page(p)) {
    return get_partial_unsafe(p, len);
  } else {
    return get_partial_safe(p, len);
  }
}

static inline state finalize(state hash) {
  hash = aes_encrypt(hash, ld(KEYS));
  hash = aes_encrypt(hash, ld(KEYS + 4));
  hash = aes_encrypt_last(hash, ld(KEYS + 8));

  return hash;
}

GXHASH_ALWAYS_INLINE state load_unaligned(const uint8_t *&ptr) {
  auto tmp = reinterpret_cast<const state *>(ptr);
  auto s = load_unaligned(tmp);
  ptr = reinterpret_cast<const uint8_t *>(tmp);
  return s;
}

GXHASH_ALWAYS_INLINE state compress_many(const state *ptr, const state *end,
                                         state hash_vector, size_t len) {

  constexpr size_t UNROLL_FACTOR = 8;

  size_t remaining_blocks = end - ptr;

  size_t unrollable_blocks_count =
      remaining_blocks / UNROLL_FACTOR * UNROLL_FACTOR;

  remaining_blocks -= unrollable_blocks_count;
  const state *end_address = ptr + remaining_blocks;

  // Process first individual blocks until we have a whole number of 8 blocks
  while (ptr < end_address) {
    auto v0 = load_unaligned(ptr);
    hash_vector = aes_encrypt(hash_vector, v0);
  }

  // Process the remaining n * 8 blocks
  // This part may use 128-bit or 256-bit
  return compress_8(ptr, end, hash_vector, len);
}

GXHASH_ALWAYS_INLINE state compress_all(const uint8_t *input, size_t len) {
  if (len == 0) {
    return create_empty();
  }

  const uint8_t *ptr = input;

  if (len <= VECTOR_SIZE) {
    // Input fits on a single SIMD vector, however we might read beyond the
    // input message Thus we need this safe method that checks if it can
    // safely read beyond or must copy
    return get_partial(reinterpret_cast<const state *>(ptr), len);
  }

  state hash_vector;
  const uint8_t *end = ptr + len;

  size_t extra_bytes_count = len % VECTOR_SIZE;
  if (extra_bytes_count == 0) {
    auto v0 = load_unaligned(ptr);
    hash_vector = v0;
  } else {
    // If the input length does not match the length of a whole number of SIMD
    // vectors, it means we'll need to read a partial vector. We can start with
    // the partial vector first, so that we can safely read beyond since we
    // expect the following bytes to still be part of the input
    hash_vector = get_partial_unsafe(reinterpret_cast<const state *>(ptr),
                                     extra_bytes_count);
    ptr += extra_bytes_count;
  }

  auto v0 = load_unaligned(ptr);

  if (len > VECTOR_SIZE * 2) {
    // Fast path when input length > 32 and <= 48
    auto v = load_unaligned(ptr);
    v0 = aes_encrypt(v0, v);

    if (len > VECTOR_SIZE * 3) {
      // Fast path when input length > 48 and <= 64
      auto v = load_unaligned(ptr);
      v0 = aes_encrypt(v0, v);

      if (len > VECTOR_SIZE * 4) {
        // Input message is large and we can use the high ILP loop
        hash_vector = compress_many(reinterpret_cast<const state *>(ptr),
                                    reinterpret_cast<const state *>(end),
                                    hash_vector, len);
      }
    }
  }

  return aes_encrypt_last(hash_vector,
                          aes_encrypt(aes_encrypt(v0, ld(KEYS)), ld(KEYS + 4)));
}

GXHASH_ALWAYS_INLINE state gxhash(const uint8_t *in, size_t len, state seed) {
  return finalize(aes_encrypt(compress_all(in, len), seed));
}

} // namespace impl

GXHASH_ALWAYS_INLINE uint32_t gxhash32(const uint8_t *in, size_t len,
                                       uint64_t seed) {
  impl::state hash = impl::gxhash(in, len, impl::create_seed(seed));
  return *(reinterpret_cast<uint32_t *>(&hash));
}

GXHASH_ALWAYS_INLINE uint64_t gxhash64(const uint8_t *in, size_t len,
                                       uint64_t seed) {
  impl::state hash = impl::gxhash(in, len, impl::create_seed(seed));
  return *(reinterpret_cast<uint64_t *>(&hash));
}

#if !defined(_MSC_VER)
GXHASH_ALWAYS_INLINE __int128 gxhash128(const uint8_t *in, size_t len,
                                        uint64_t seed) {
  impl::state hash = impl::gxhash(in, len, impl::create_seed(seed));
  return *(reinterpret_cast<__int128 *>(&hash));
}
#endif

} // namespace gxhash



extern "C" {
    uint32_t gx_hash_32(const void* data, size_t length);
}