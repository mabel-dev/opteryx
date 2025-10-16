#include "simd_search.h"
#include <cstdint>
#include <vector>
#include <cstring>

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>
#elif defined(__AVX2__)
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
    results.reserve(length / 100);  // Reserve space for ~1% matches as a reasonable estimate
    
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

// AVX2 implementation for x86
#ifdef __AVX2__
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

// AVX2 find_all implementation for x86
#ifdef __AVX2__
std::vector<size_t> avx_find_all(const char* data, size_t length, char target) {
    std::vector<size_t> results;
    results.reserve(length / 100);  // Reserve space for ~1% matches as a reasonable estimate
    
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

// AVX2 count implementation for x86
#ifdef __AVX2__
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