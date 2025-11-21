#include "simd_search.h"
#include <cstdint>
#include <vector>
#include <cstring>

// Estimated match ratio for vector pre-allocation (1%)
static const size_t EXPECTED_MATCH_RATIO = 100;

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>
#endif

#if defined(__AVX512F__) && defined(__AVX512BW__) || defined(__AVX2__)
#include <immintrin.h>
#endif

// SIMD substring search (up to 16 bytes pattern)
// Returns index of first occurrence or -1 if not found
int simd_search_substring(const char* data, size_t length, const char* pattern, size_t pattern_len) {
    if (pattern_len == 0 || pattern_len > 16) return -1;
#if defined(__AVX2__)
    // AVX2 implementation
    __m128i pat = _mm_loadu_si128(reinterpret_cast<const __m128i*>(pattern));
    size_t i = 0;
    for (; i + 16 <= length; ++i) {
        __m128i chunk = _mm_loadu_si128(reinterpret_cast<const __m128i*>(data + i));
        __m128i cmp = _mm_cmpeq_epi8(chunk, pat);
        int mask = _mm_movemask_epi8(cmp);
        // Check if all bytes match (for pattern_len)
        if (mask == (1 << pattern_len) - 1) {
            // Confirm full match (avoid false positive for <16 pattern)
            if (memcmp(data + i, pattern, pattern_len) == 0) {
                return static_cast<int>(i);
            }
        }
    }
    // Scalar fallback for tail
    for (; i + pattern_len <= length; ++i) {
        if (memcmp(data + i, pattern, pattern_len) == 0) {
            return static_cast<int>(i);
        }
    }
    return -1;
#elif defined(__ARM_NEON) || defined(__ARM_NEON__)
    // NEON implementation (simple, not fully vectorized for substring)
    // For now, fallback to scalar for substring
    for (size_t i = 0; i + pattern_len <= length; ++i) {
        if (memcmp(data + i, pattern, pattern_len) == 0) {
            return static_cast<int>(i);
        }
    }
    return -1;
#else
    // Scalar fallback
    for (size_t i = 0; i + pattern_len <= length; ++i) {
        if (memcmp(data + i, pattern, pattern_len) == 0) {
            return static_cast<int>(i);
        }
    }
    return -1;
#endif
}

#ifdef __AVX2__
#include <immintrin.h>
#endif

// NEON implementation for ARM
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
int neon_search(const char* data, size_t length, char target) {
    size_t i = 0;
    // Create a vector with the target repeated.
    uint8x16_t target_vec = vdupq_n_u8(static_cast<uint8_t>(target));
    for (; i + 16 <= length; i += 16) {
        // Load 16 bytes.
        uint8x16_t chunk = vld1q_u8(reinterpret_cast<const uint8_t*>(data + i));
        // Compare each byte to the target.
        uint8x16_t cmp = vceqq_u8(chunk, target_vec);
        // Store the comparison results into an array.
        uint8_t mask[16];
        vst1q_u8(mask, cmp);
        for (int j = 0; j < 16; j++) {
            if (mask[j] == 0xFF) {
                return static_cast<int>(i + j);
            }
        }
    }
    // Process any leftover bytes.
    for (; i < length; i++) {
        if (data[i] == target) {
            return static_cast<int>(i);
        }
    }
    return -1;
}
#else
// Fallback scalar implementation if NEON is not available.
int neon_search(const char* data, size_t length, char target) {
    for (size_t i = 0; i < length; i++) {
        if (data[i] == target)
            return static_cast<int>(i);
    }
    return -1;
}
#endif

// NEON find_all implementation for ARM
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
std::vector<size_t> neon_find_all(const char* data, size_t length, char target) {
    std::vector<size_t> results;
    results.reserve(length / EXPECTED_MATCH_RATIO);  // Reserve space for ~1% matches as a reasonable estimate
    
    size_t i = 0;
    // Create a vector with the target repeated.
    uint8x16_t target_vec = vdupq_n_u8(static_cast<uint8_t>(target));
    
    for (; i + 16 <= length; i += 16) {
        // Load 16 bytes.
        uint8x16_t chunk = vld1q_u8(reinterpret_cast<const uint8_t*>(data + i));
        // Compare each byte to the target.
        uint8x16_t cmp = vceqq_u8(chunk, target_vec);
        // Store the comparison results into an array.
        uint8_t mask[16];
        vst1q_u8(mask, cmp);
        for (int j = 0; j < 16; j++) {
            if (mask[j] == 0xFF) {
                results.push_back(i + j);
            }
        }
    }
    
    // Process any leftover bytes.
    for (; i < length; i++) {
        if (data[i] == target) {
            results.push_back(i);
        }
    }
    
    return results;
}
#else
// Fallback scalar implementation if NEON is not available.
std::vector<size_t> neon_find_all(const char* data, size_t length, char target) {
    std::vector<size_t> results;
    for (size_t i = 0; i < length; i++) {
        if (data[i] == target) {
            results.push_back(i);
        }
    }
    return results;
}
#endif

// NEON count implementation for ARM
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
size_t neon_count(const char* data, size_t length, char target) {
    size_t count = 0;
    size_t i = 0;
    // Create a vector with the target repeated.
    uint8x16_t target_vec = vdupq_n_u8(static_cast<uint8_t>(target));
    
    for (; i + 16 <= length; i += 16) {
        // Load 16 bytes.
        uint8x16_t chunk = vld1q_u8(reinterpret_cast<const uint8_t*>(data + i));
        // Compare each byte to the target.
        uint8x16_t cmp = vceqq_u8(chunk, target_vec);
        // Store the comparison results into an array.
        uint8_t mask[16];
        vst1q_u8(mask, cmp);
        for (int j = 0; j < 16; j++) {
            if (mask[j] == 0xFF) {
                count++;
            }
        }
    }
    
    // Process any leftover bytes.
    for (; i < length; i++) {
        if (data[i] == target) {
            count++;
        }
    }
    
    return count;
}
#else
// Fallback scalar implementation if NEON is not available.
size_t neon_count(const char* data, size_t length, char target) {
    size_t count = 0;
    for (size_t i = 0; i < length; i++) {
        if (data[i] == target) {
            count++;
        }
    }
    return count;
}
#endif

// AVX512 implementation for x86 (if available)
#if defined(__AVX512F__) && defined(__AVX512BW__)
int avx_search(const char* data, size_t length, char target) {
    size_t i = 0;
    __m512i target_vec = _mm512_set1_epi8(target);
    for (; i + 64 <= length; i += 64) {
        __m512i chunk = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(data + i));
        __mmask64 mask = _mm512_cmpeq_epi8_mask(chunk, target_vec);
        if (mask != 0) {
            // Use __builtin_ctzll to find the lowest set bit (64-bit mask)
            int offset = __builtin_ctzll(mask);
            return static_cast<int>(i + offset);
        }
    }
    // Process remaining bytes with scalar fallback
    for (; i < length; i++) {
        if (data[i] == target)
            return static_cast<int>(i);
    }
    return -1;
}
// AVX2 implementation for x86 (fallback when AVX512 not available)
#elif defined(__AVX2__)
int avx_search(const char* data, size_t length, char target) {
    size_t i = 0;
    __m256i target_vec = _mm256_set1_epi8(target);
    for (; i + 32 <= length; i += 32) {
        __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + i));
        __m256i cmp = _mm256_cmpeq_epi8(chunk, target_vec);
        int mask = _mm256_movemask_epi8(cmp);
        if (mask != 0) {
            // Use __builtin_ctz to find the lowest set bit.
            int offset = __builtin_ctz(mask);
            return static_cast<int>(i + offset);
        }
    }
    // Process remaining bytes.
    for (; i < length; i++) {
        if (data[i] == target)
            return static_cast<int>(i);
    }
    return -1;
}
#else
// Fallback scalar implementation if AVX2 is not available.
int avx_search(const char* data, size_t length, char target) {
    for (size_t i = 0; i < length; i++) {
        if (data[i] == target)
            return static_cast<int>(i);
    }
    return -1;
}
#endif

// AVX512 find_all implementation for x86
#if defined(__AVX512F__) && defined(__AVX512BW__)
std::vector<size_t> avx_find_all(const char* data, size_t length, char target) {
    std::vector<size_t> results;
    results.reserve(length / EXPECTED_MATCH_RATIO);  // Reserve space for ~1% matches as a reasonable estimate
    
    size_t i = 0;
    __m512i target_vec = _mm512_set1_epi8(target);
    
    for (; i + 64 <= length; i += 64) {
        __m512i chunk = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(data + i));
        __mmask64 mask = _mm512_cmpeq_epi8_mask(chunk, target_vec);
        
        // Process all matches in this chunk
        while (mask != 0) {
            int offset = __builtin_ctzll(mask);
            results.push_back(i + offset);
            mask &= (mask - 1);  // Clear the lowest set bit
        }
    }
    
    // Process remaining bytes.
    for (; i < length; i++) {
        if (data[i] == target) {
            results.push_back(i);
        }
    }
    
    return results;
}
// AVX2 find_all implementation for x86 (fallback)
#elif defined(__AVX2__)
std::vector<size_t> avx_find_all(const char* data, size_t length, char target) {
    std::vector<size_t> results;
    results.reserve(length / EXPECTED_MATCH_RATIO);  // Reserve space for ~1% matches as a reasonable estimate
    
    size_t i = 0;
    __m256i target_vec = _mm256_set1_epi8(target);
    
    for (; i + 32 <= length; i += 32) {
        __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + i));
        __m256i cmp = _mm256_cmpeq_epi8(chunk, target_vec);
        int mask = _mm256_movemask_epi8(cmp);
        
        // Process all matches in this chunk
        while (mask != 0) {
            int offset = __builtin_ctz(mask);
            results.push_back(i + offset);
            mask &= (mask - 1);  // Clear the lowest set bit
        }
    }
    
    // Process remaining bytes.
    for (; i < length; i++) {
        if (data[i] == target) {
            results.push_back(i);
        }
    }
    
    return results;
}
#else
// Fallback scalar implementation if AVX2 is not available.
std::vector<size_t> avx_find_all(const char* data, size_t length, char target) {
    std::vector<size_t> results;
    for (size_t i = 0; i < length; i++) {
        if (data[i] == target) {
            results.push_back(i);
        }
    }
    return results;
}
#endif

// AVX512 count implementation for x86
#if defined(__AVX512F__) && defined(__AVX512BW__)
size_t avx_count(const char* data, size_t length, char target) {
    size_t count = 0;
    size_t i = 0;
    __m512i target_vec = _mm512_set1_epi8(target);
    
    for (; i + 64 <= length; i += 64) {
        __m512i chunk = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(data + i));
        __mmask64 mask = _mm512_cmpeq_epi8_mask(chunk, target_vec);
        
        // Count the number of set bits in the mask using popcount
        count += __builtin_popcountll(mask);
    }
    
    // Process remaining bytes.
    for (; i < length; i++) {
        if (data[i] == target) {
            count++;
        }
    }
    
    return count;
}
// AVX2 count implementation for x86 (fallback)
#elif defined(__AVX2__)
size_t avx_count(const char* data, size_t length, char target) {
    size_t count = 0;
    size_t i = 0;
    __m256i target_vec = _mm256_set1_epi8(target);
    
    for (; i + 32 <= length; i += 32) {
        __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + i));
        __m256i cmp = _mm256_cmpeq_epi8(chunk, target_vec);
        int mask = _mm256_movemask_epi8(cmp);
        
        // Count the number of set bits in the mask
        count += __builtin_popcount(mask);
    }
    
    // Process remaining bytes.
    for (; i < length; i++) {
        if (data[i] == target) {
            count++;
        }
    }
    
    return count;
}
#else
// Fallback scalar implementation if AVX2 is not available.
size_t avx_count(const char* data, size_t length, char target) {
    size_t count = 0;
    for (size_t i = 0; i < length; i++) {
        if (data[i] == target) {
            count++;
        }
    }
    return count;
}
#endif

// NEON delimiter search for ARM
// Delimiters: space (32), comma (44), '}' (125), tab (9)
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
int neon_find_delimiter(const char* data, size_t length) {
    size_t i = 0;
    
    // Create comparison vectors for all delimiters
    uint8x16_t space_vec = vdupq_n_u8(32);   // ' '
    uint8x16_t comma_vec = vdupq_n_u8(44);   // ','
    uint8x16_t brace_vec = vdupq_n_u8(125);  // '}'
    uint8x16_t tab_vec = vdupq_n_u8(9);      // '\t'
    
    for (; i + 16 <= length; i += 16) {
        // Load 16 bytes
        uint8x16_t chunk = vld1q_u8(reinterpret_cast<const uint8_t*>(data + i));
        
        // Compare with all delimiters
        uint8x16_t cmp_space = vceqq_u8(chunk, space_vec);
        uint8x16_t cmp_comma = vceqq_u8(chunk, comma_vec);
        uint8x16_t cmp_brace = vceqq_u8(chunk, brace_vec);
        uint8x16_t cmp_tab = vceqq_u8(chunk, tab_vec);
        
        // OR all comparisons together
        uint8x16_t result = vorrq_u8(cmp_space, cmp_comma);
        result = vorrq_u8(result, cmp_brace);
        result = vorrq_u8(result, cmp_tab);
        
        // Check if any match found
        uint8_t mask[16];
        vst1q_u8(mask, result);
        for (int j = 0; j < 16; j++) {
            if (mask[j] == 0xFF) {
                return static_cast<int>(i + j);
            }
        }
    }
    
    // Process any leftover bytes
    for (; i < length; i++) {
        char c = data[i];
        if (c == 32 || c == 44 || c == 125 || c == 9) {
            return static_cast<int>(i);
        }
    }
    
    return -1;
}
#else
// Fallback scalar implementation if NEON is not available
int neon_find_delimiter(const char* data, size_t length) {
    for (size_t i = 0; i < length; i++) {
        char c = data[i];
        if (c == 32 || c == 44 || c == 125 || c == 9) {
            return static_cast<int>(i);
        }
    }
    return -1;
}
#endif

// AVX512 delimiter search for x86
// Delimiters: space (32), comma (44), '}' (125), tab (9)
#if defined(__AVX512F__) && defined(__AVX512BW__)
int avx_find_delimiter(const char* data, size_t length) {
    size_t i = 0;
    
    // Create comparison vectors for all delimiters
    __m512i space_vec = _mm512_set1_epi8(32);   // ' '
    __m512i comma_vec = _mm512_set1_epi8(44);   // ','
    __m512i brace_vec = _mm512_set1_epi8(125);  // '}'
    __m512i tab_vec = _mm512_set1_epi8(9);      // '\t'
    
    for (; i + 64 <= length; i += 64) {
        // Load 64 bytes
        __m512i chunk = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(data + i));
        
        // Compare with all delimiters
        __mmask64 cmp_space = _mm512_cmpeq_epi8_mask(chunk, space_vec);
        __mmask64 cmp_comma = _mm512_cmpeq_epi8_mask(chunk, comma_vec);
        __mmask64 cmp_brace = _mm512_cmpeq_epi8_mask(chunk, brace_vec);
        __mmask64 cmp_tab = _mm512_cmpeq_epi8_mask(chunk, tab_vec);
        
        // OR all comparisons together
        __mmask64 result = cmp_space | cmp_comma | cmp_brace | cmp_tab;
        
        if (result != 0) {
            // Find the first set bit
            int offset = __builtin_ctzll(result);
            return static_cast<int>(i + offset);
        }
    }
    
    // Process remaining bytes
    for (; i < length; i++) {
        char c = data[i];
        if (c == 32 || c == 44 || c == 125 || c == 9) {
            return static_cast<int>(i);
        }
    }
    
    return -1;
}
// AVX2 delimiter search for x86 (fallback)
// Delimiters: space (32), comma (44), '}' (125), tab (9)
#elif defined(__AVX2__)
int avx_find_delimiter(const char* data, size_t length) {
    size_t i = 0;
    
    // Create comparison vectors for all delimiters
    __m256i space_vec = _mm256_set1_epi8(32);   // ' '
    __m256i comma_vec = _mm256_set1_epi8(44);   // ','
    __m256i brace_vec = _mm256_set1_epi8(125);  // '}'
    __m256i tab_vec = _mm256_set1_epi8(9);      // '\t'
    
    for (; i + 32 <= length; i += 32) {
        // Load 32 bytes
        __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + i));
        
        // Compare with all delimiters
        __m256i cmp_space = _mm256_cmpeq_epi8(chunk, space_vec);
        __m256i cmp_comma = _mm256_cmpeq_epi8(chunk, comma_vec);
        __m256i cmp_brace = _mm256_cmpeq_epi8(chunk, brace_vec);
        __m256i cmp_tab = _mm256_cmpeq_epi8(chunk, tab_vec);
        
        // OR all comparisons together
        __m256i result = _mm256_or_si256(cmp_space, cmp_comma);
        result = _mm256_or_si256(result, cmp_brace);
        result = _mm256_or_si256(result, cmp_tab);
        
        // Get mask of matching bytes
        int mask = _mm256_movemask_epi8(result);
        if (mask != 0) {
            // Find the first set bit
            int offset = __builtin_ctz(mask);
            return static_cast<int>(i + offset);
        }
    }
    
    // Process remaining bytes
    for (; i < length; i++) {
        char c = data[i];
        if (c == 32 || c == 44 || c == 125 || c == 9) {
            return static_cast<int>(i);
        }
    }
    
    return -1;
}
#else
// Fallback scalar implementation if AVX2 is not available
int avx_find_delimiter(const char* data, size_t length) {
    for (size_t i = 0; i < length; i++) {
        char c = data[i];
        if (c == 32 || c == 44 || c == 125 || c == 9) {
            return static_cast<int>(i);
        }
    }
    return -1;
}
#endif