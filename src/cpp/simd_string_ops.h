#ifndef SIMD_STRING_OPS_HPP
#define SIMD_STRING_OPS_HPP

#include <cstddef>

#ifdef __cplusplus
extern "C++" {
#endif

/**
 * Convert ASCII characters to uppercase in-place using SIMD.
 * Non-ASCII bytes (>127) are left unchanged.
 * 
 * On AVX2: processes 32 bytes per iteration
 * Fallback: scalar byte-by-byte conversion
 * 
 * @param data Pointer to character data (modified in-place)
 * @param length Number of bytes to process
 */
void simd_to_upper(char* data, size_t length);

/**
 * Convert ASCII characters to lowercase in-place using SIMD.
 * Non-ASCII bytes (>127) are left unchanged.
 * 
 * On AVX2: processes 32 bytes per iteration
 * Fallback: scalar byte-by-byte conversion
 * 
 * @param data Pointer to character data (modified in-place)
 * @param length Number of bytes to process
 */
void simd_to_lower(char* data, size_t length);

#ifdef __cplusplus
}
#endif

#endif // SIMD_STRING_OPS_HPP