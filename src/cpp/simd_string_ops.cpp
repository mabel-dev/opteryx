#include "simd_string_ops.h"
#include <cstdint>
#include <cstring>

#if (defined(__AVX512F__) && defined(__AVX512BW__)) || defined(__AVX2__)
#include <immintrin.h>
#endif

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>
#endif

// SIMD-accelerated ASCII case conversion
// Handles only ASCII characters (0-127); non-ASCII bytes are left unchanged

// Constants for case conversion
static const uint8_t LOWER_A = 'a';
static const uint8_t LOWER_Z = 'z';
static const uint8_t UPPER_A = 'A';
static const uint8_t UPPER_Z = 'Z';
static const uint8_t CASE_DIFF = 'a' - 'A';  // 32

// AVX512 implementation for to_upper
#if defined(__AVX512F__) && defined(__AVX512BW__)
void simd_to_upper(char* data, size_t length) {
    size_t i = 0;
    
    // Create vectors for comparison and conversion
    __m512i lower_a_vec = _mm512_set1_epi8(LOWER_A);
    __m512i lower_z_vec = _mm512_set1_epi8(LOWER_Z);
    __m512i case_diff_vec = _mm512_set1_epi8(CASE_DIFF);
    
    // Process 64 bytes at a time
    for (; i + 64 <= length; i += 64) {
        __m512i chunk = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(data + i));
        
        // Check if each byte is in range [a, z]
        __mmask64 is_ge_a = _mm512_cmpge_epu8_mask(chunk, lower_a_vec);
        __mmask64 is_le_z = _mm512_cmple_epu8_mask(chunk, lower_z_vec);
        __mmask64 is_lower = is_ge_a & is_le_z;
        
        // Subtract CASE_DIFF from lowercase letters to make them uppercase
        __m512i converted = _mm512_mask_sub_epi8(chunk, is_lower, chunk, case_diff_vec);
        
        _mm512_storeu_si512(reinterpret_cast<__m512i*>(data + i), converted);
    }
    
    // Process remaining bytes with scalar fallback
    for (; i < length; i++) {
        if (data[i] >= LOWER_A && data[i] <= LOWER_Z) {
            data[i] -= CASE_DIFF;
        }
    }
}
// AVX2 implementation for to_upper
#elif defined(__AVX2__)
void simd_to_upper(char* data, size_t length) {
    size_t i = 0;
    
    __m256i lower_a_vec = _mm256_set1_epi8(LOWER_A);
    __m256i lower_z_vec = _mm256_set1_epi8(LOWER_Z);
    __m256i case_diff_vec = _mm256_set1_epi8(CASE_DIFF);
    
    for (; i + 32 <= length; i += 32) {
        __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + i));
        
        // Check if in range [a, z]
        __m256i is_ge_a = _mm256_cmpgt_epi8(chunk, _mm256_sub_epi8(lower_a_vec, _mm256_set1_epi8(1)));
        __m256i is_le_z = _mm256_cmpgt_epi8(_mm256_add_epi8(lower_z_vec, _mm256_set1_epi8(1)), chunk);
        __m256i is_lower = _mm256_and_si256(is_ge_a, is_le_z);
        
        // Subtract CASE_DIFF where mask is true
        __m256i to_subtract = _mm256_and_si256(is_lower, case_diff_vec);
        __m256i converted = _mm256_sub_epi8(chunk, to_subtract);
        
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(data + i), converted);
    }
    
    for (; i < length; i++) {
        if (data[i] >= LOWER_A && data[i] <= LOWER_Z) {
            data[i] -= CASE_DIFF;
        }
    }
}
#else
// Scalar fallback
void simd_to_upper(char* data, size_t length) {
    for (size_t i = 0; i < length; i++) {
        if (data[i] >= LOWER_A && data[i] <= LOWER_Z) {
            data[i] -= CASE_DIFF;
        }
    }
}
#endif

// AVX512 implementation for to_lower
#if defined(__AVX512F__) && defined(__AVX512BW__)
void simd_to_lower(char* data, size_t length) {
    size_t i = 0;
    
    __m512i upper_a_vec = _mm512_set1_epi8(UPPER_A);
    __m512i upper_z_vec = _mm512_set1_epi8(UPPER_Z);
    __m512i case_diff_vec = _mm512_set1_epi8(CASE_DIFF);
    
    for (; i + 64 <= length; i += 64) {
        __m512i chunk = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(data + i));
        
        // Check if each byte is in range [A, Z]
        __mmask64 is_ge_a = _mm512_cmpge_epu8_mask(chunk, upper_a_vec);
        __mmask64 is_le_z = _mm512_cmple_epu8_mask(chunk, upper_z_vec);
        __mmask64 is_upper = is_ge_a & is_le_z;
        
        // Add CASE_DIFF to uppercase letters to make them lowercase
        __m512i converted = _mm512_mask_add_epi8(chunk, is_upper, chunk, case_diff_vec);
        
        _mm512_storeu_si512(reinterpret_cast<__m512i*>(data + i), converted);
    }
    
    for (; i < length; i++) {
        if (data[i] >= UPPER_A && data[i] <= UPPER_Z) {
            data[i] += CASE_DIFF;
        }
    }
}
// AVX2 implementation for to_lower
#elif defined(__AVX2__)
void simd_to_lower(char* data, size_t length) {
    size_t i = 0;
    
    __m256i upper_a_vec = _mm256_set1_epi8(UPPER_A);
    __m256i upper_z_vec = _mm256_set1_epi8(UPPER_Z);
    __m256i case_diff_vec = _mm256_set1_epi8(CASE_DIFF);
    
    for (; i + 32 <= length; i += 32) {
        __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + i));
        
        // Check if in range [A, Z]
        __m256i is_ge_a = _mm256_cmpgt_epi8(chunk, _mm256_sub_epi8(upper_a_vec, _mm256_set1_epi8(1)));
        __m256i is_le_z = _mm256_cmpgt_epi8(_mm256_add_epi8(upper_z_vec, _mm256_set1_epi8(1)), chunk);
        __m256i is_upper = _mm256_and_si256(is_ge_a, is_le_z);
        
        // Add CASE_DIFF where mask is true
        __m256i to_add = _mm256_and_si256(is_upper, case_diff_vec);
        __m256i converted = _mm256_add_epi8(chunk, to_add);
        
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(data + i), converted);
    }
    
    for (; i < length; i++) {
        if (data[i] >= UPPER_A && data[i] <= UPPER_Z) {
            data[i] += CASE_DIFF;
        }
    }
}
#else
// Scalar fallback
void simd_to_lower(char* data, size_t length) {
    for (size_t i = 0; i < length; i++) {
        if (data[i] >= UPPER_A && data[i] <= UPPER_Z) {
            data[i] += CASE_DIFF;
        }
    }
}
#endif
