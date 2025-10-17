#ifndef SIMD_SEARCH_HPP
#define SIMD_SEARCH_HPP

#include <cstddef>
#include <vector>

#ifdef __cplusplus
extern "C++" {
#endif

/**
 * SIMD substring search for a fixed pattern (up to 16 bytes).
 * Returns the index of the first occurrence or -1 if not found.
 * Pattern length must be <= 16.
 */
int simd_search_substring(const char* data, size_t length, const char* pattern, size_t pattern_len);

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

/**
 * Count occurrences of target in data using NEON.
 * Returns the number of occurrences.
 */
size_t neon_count(const char* data, size_t length, char target);

/**
 * Count occurrences of target in data using AVX2.
 * Returns the number of occurrences.
 */
size_t avx_count(const char* data, size_t length, char target);

/**
 * Find the first occurrence of any JSON delimiter using NEON.
 * Delimiters: space (32), comma (44), '}' (125), ']' (93), tab (9), newline (10).
 * Returns the index of the first delimiter or -1 if not found.
 */
int neon_find_delimiter(const char* data, size_t length);

/**
 * Find the first occurrence of any JSON delimiter using AVX2.
 * Delimiters: space (32), comma (44), '}' (125), ']' (93), tab (9), newline (10).
 * Returns the index of the first delimiter or -1 if not found.
 */
int avx_find_delimiter(const char* data, size_t length);

#ifdef __cplusplus
}
#endif

#endif // SIMD_SEARCH_HPP