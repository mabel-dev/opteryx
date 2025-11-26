#ifndef SIMD_BITOPS_H
#define SIMD_BITOPS_H

#include <cstddef>
#include <cstdint>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * SIMD-accelerated bitwise AND operation on byte arrays.
 * Processes 32 bytes at a time on AVX2, 16 on NEON.
 * 
 * @param dest Destination buffer (can be same as a or b for in-place)
 * @param a First input buffer
 * @param b Second input buffer
 * @param n Number of bytes to process
 */
void simd_and_mask(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n);

/**
 * SIMD-accelerated bitwise OR operation on byte arrays.
 * Processes 32 bytes at a time on AVX2, 16 on NEON.
 * 
 * @param dest Destination buffer (can be same as a or b for in-place)
 * @param a First input buffer
 * @param b Second input buffer
 * @param n Number of bytes to process
 */
void simd_or_mask(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n);

/**
 * SIMD-accelerated bitwise XOR operation on byte arrays.
 * Processes 32 bytes at a time on AVX2, 16 on NEON.
 * 
 * @param dest Destination buffer (can be same as a or b for in-place)
 * @param a First input buffer
 * @param b Second input buffer
 * @param n Number of bytes to process
 */
void simd_xor_mask(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n);

/**
 * SIMD-accelerated bitwise NOT operation on byte array.
 * Processes 32 bytes at a time on AVX2, 16 on NEON.
 * 
 * @param dest Destination buffer (can be same as src for in-place)
 * @param src Source buffer
 * @param n Number of bytes to process
 */
void simd_not_mask(uint8_t* dest, const uint8_t* src, size_t n);

/**
 * SIMD-accelerated bit population count (count of 1 bits).
 * Uses POPCNT instruction when available, falls back to lookup table.
 * 
 * @param data Input buffer
 * @param n Number of bytes to process
 * @return Total number of set bits
 */
size_t simd_popcount(const uint8_t* data, size_t n);

/**
 * SIMD-accelerated selection: dest[i] = mask[i] ? a[i] : b[i]
 * Useful for implementing WHERE clauses and conditional operations.
 * 
 * @param dest Destination buffer
 * @param mask Boolean mask (0 or non-zero per byte)
 * @param a Values to select when mask is true
 * @param b Values to select when mask is false
 * @param n Number of bytes to process
 */
void simd_select_bytes(uint8_t* dest, const uint8_t* mask, 
                       const uint8_t* a, const uint8_t* b, size_t n);

#ifdef __cplusplus
}
#endif

#endif // SIMD_BITOPS_H