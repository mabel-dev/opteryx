#ifndef SIMD_SEARCH_HPP
#define SIMD_SEARCH_HPP

#include <cstddef>

#ifdef __cplusplus
extern "C++" {
#endif

/**
 * Search for target in data using NEON.
 * Returns the index of the first occurrence or -1 if not found.
 */
int neon_search(const char* data, size_t length, char target);

/**
 * Search for target in data using AVX2.
 * Returns the index of the first occurrence or -1 if not found.
 */
int avx_search(const char* data, size_t length, char target);

#ifdef __cplusplus
}
#endif

#endif // SIMD_SEARCH_HPP