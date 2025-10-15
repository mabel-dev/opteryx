#ifndef SIMD_SEARCH_HPP
#define SIMD_SEARCH_HPP

#include <cstddef>
#include <vector>

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

/**
 * Find all occurrences of target in data using NEON.
 * Returns a vector containing the offsets of all occurrences.
 */
std::vector<size_t> neon_find_all(const char* data, size_t length, char target = '\n');

/**
 * Find all occurrences of target in data using AVX2.
 * Returns a vector containing the offsets of all occurrences.
 */
std::vector<size_t> avx_find_all(const char* data, size_t length, char target = '\n');

#ifdef __cplusplus
}
#endif

#endif // SIMD_SEARCH_HPP