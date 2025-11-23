#include "simd_bitops.h"
#include <cstring>

#if (defined(__AVX512F__) && defined(__AVX512BW__)) || defined(__AVX2__)
#include <immintrin.h>
#endif

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>
#endif

// ============================================================================
// Bitwise AND
// ============================================================================

#if defined(__AVX512F__) && defined(__AVX512BW__)
void simd_and_mask(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n) {
    size_t i = 0;
    // Process 64 bytes at a time with AVX512
    for (; i + 64 <= n; i += 64) {
        __m512i va = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(a + i));
        __m512i vb = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(b + i));
        __m512i result = _mm512_and_si512(va, vb);
        _mm512_storeu_si512(reinterpret_cast<__m512i*>(dest + i), result);
    }
    // Scalar tail
    for (; i < n; i++) {
        dest[i] = a[i] & b[i];
    }
}
#elif defined(__AVX2__)
void simd_and_mask(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n) {
    size_t i = 0;
    for (; i + 32 <= n; i += 32) {
        __m256i va = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(a + i));
        __m256i vb = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(b + i));
        __m256i result = _mm256_and_si256(va, vb);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest + i), result);
    }
    for (; i < n; i++) {
        dest[i] = a[i] & b[i];
    }
}
#elif defined(__ARM_NEON) || defined(__ARM_NEON__)
void simd_and_mask(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n) {
    size_t i = 0;
    for (; i + 16 <= n; i += 16) {
        uint8x16_t va = vld1q_u8(a + i);
        uint8x16_t vb = vld1q_u8(b + i);
        uint8x16_t result = vandq_u8(va, vb);
        vst1q_u8(dest + i, result);
    }
    for (; i < n; i++) {
        dest[i] = a[i] & b[i];
    }
}
#else
void simd_and_mask(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n) {
    for (size_t i = 0; i < n; i++) {
        dest[i] = a[i] & b[i];
    }
}
#endif

// ============================================================================
// Bitwise OR
// ============================================================================

#if defined(__AVX512F__) && defined(__AVX512BW__)
void simd_or_mask(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n) {
    size_t i = 0;
    for (; i + 64 <= n; i += 64) {
        __m512i va = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(a + i));
        __m512i vb = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(b + i));
        __m512i result = _mm512_or_si512(va, vb);
        _mm512_storeu_si512(reinterpret_cast<__m512i*>(dest + i), result);
    }
    for (; i < n; i++) {
        dest[i] = a[i] | b[i];
    }
}
#elif defined(__AVX2__)
void simd_or_mask(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n) {
    size_t i = 0;
    for (; i + 32 <= n; i += 32) {
        __m256i va = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(a + i));
        __m256i vb = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(b + i));
        __m256i result = _mm256_or_si256(va, vb);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest + i), result);
    }
    for (; i < n; i++) {
        dest[i] = a[i] | b[i];
    }
}
#elif defined(__ARM_NEON) || defined(__ARM_NEON__)
void simd_or_mask(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n) {
    size_t i = 0;
    for (; i + 16 <= n; i += 16) {
        uint8x16_t va = vld1q_u8(a + i);
        uint8x16_t vb = vld1q_u8(b + i);
        uint8x16_t result = vorrq_u8(va, vb);
        vst1q_u8(dest + i, result);
    }
    for (; i < n; i++) {
        dest[i] = a[i] | b[i];
    }
}
#else
void simd_or_mask(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n) {
    for (size_t i = 0; i < n; i++) {
        dest[i] = a[i] | b[i];
    }
}
#endif

// ============================================================================
// Bitwise XOR
// ============================================================================

#if defined(__AVX512F__) && defined(__AVX512BW__)
void simd_xor_mask(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n) {
    size_t i = 0;
    for (; i + 64 <= n; i += 64) {
        __m512i va = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(a + i));
        __m512i vb = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(b + i));
        __m512i result = _mm512_xor_si512(va, vb);
        _mm512_storeu_si512(reinterpret_cast<__m512i*>(dest + i), result);
    }
    for (; i < n; i++) {
        dest[i] = a[i] ^ b[i];
    }
}
#elif defined(__AVX2__)
void simd_xor_mask(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n) {
    size_t i = 0;
    for (; i + 32 <= n; i += 32) {
        __m256i va = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(a + i));
        __m256i vb = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(b + i));
        __m256i result = _mm256_xor_si256(va, vb);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest + i), result);
    }
    for (; i < n; i++) {
        dest[i] = a[i] ^ b[i];
    }
}
#elif defined(__ARM_NEON) || defined(__ARM_NEON__)
void simd_xor_mask(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n) {
    size_t i = 0;
    for (; i + 16 <= n; i += 16) {
        uint8x16_t va = vld1q_u8(a + i);
        uint8x16_t vb = vld1q_u8(b + i);
        uint8x16_t result = veorq_u8(va, vb);
        vst1q_u8(dest + i, result);
    }
    for (; i < n; i++) {
        dest[i] = a[i] ^ b[i];
    }
}
#else
void simd_xor_mask(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n) {
    for (size_t i = 0; i < n; i++) {
        dest[i] = a[i] ^ b[i];
    }
}
#endif

// ============================================================================
// Bitwise NOT
// ============================================================================

#if defined(__AVX512F__) && defined(__AVX512BW__)
void simd_not_mask(uint8_t* dest, const uint8_t* src, size_t n) {
    size_t i = 0;
    __m512i all_ones = _mm512_set1_epi8(static_cast<char>(0xFF));
    for (; i + 64 <= n; i += 64) {
        __m512i v = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(src + i));
        __m512i result = _mm512_xor_si512(v, all_ones);
        _mm512_storeu_si512(reinterpret_cast<__m512i*>(dest + i), result);
    }
    for (; i < n; i++) {
        dest[i] = ~src[i];
    }
}
#elif defined(__AVX2__)
void simd_not_mask(uint8_t* dest, const uint8_t* src, size_t n) {
    size_t i = 0;
    __m256i all_ones = _mm256_set1_epi8(static_cast<char>(0xFF));
    for (; i + 32 <= n; i += 32) {
        __m256i v = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + i));
        __m256i result = _mm256_xor_si256(v, all_ones);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest + i), result);
    }
    for (; i < n; i++) {
        dest[i] = ~src[i];
    }
}
#elif defined(__ARM_NEON) || defined(__ARM_NEON__)
void simd_not_mask(uint8_t* dest, const uint8_t* src, size_t n) {
    size_t i = 0;
    for (; i + 16 <= n; i += 16) {
        uint8x16_t v = vld1q_u8(src + i);
        uint8x16_t result = vmvnq_u8(v);
        vst1q_u8(dest + i, result);
    }
    for (; i < n; i++) {
        dest[i] = ~src[i];
    }
}
#else
void simd_not_mask(uint8_t* dest, const uint8_t* src, size_t n) {
    for (size_t i = 0; i < n; i++) {
        dest[i] = ~src[i];
    }
}
#endif

// ============================================================================
// Population Count (number of set bits)
// ============================================================================

// Lookup table for 8-bit popcount (used in fallback)
static const uint8_t popcount_table[256] = {
    0,1,1,2,1,2,2,3,1,2,2,3,2,3,3,4,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,
    1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,
    1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,
    2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,
    1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,
    2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,
    2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,
    3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,4,5,5,6,5,6,6,7,5,6,6,7,6,7,7,8
};

#if defined(__AVX2__) && defined(__POPCNT__)
// AVX2 with POPCNT instruction (also works for AVX512)
size_t simd_popcount(const uint8_t* data, size_t n) {
    size_t count = 0;
    size_t i = 0;
    
    // Process 8 bytes at a time using POPCNT
    for (; i + 8 <= n; i += 8) {
        uint64_t val;
        memcpy(&val, data + i, 8);
        count += __builtin_popcountll(val);
    }
    
    // Scalar tail
    for (; i < n; i++) {
        count += popcount_table[data[i]];
    }
    
    return count;
}
#else
// Fallback using lookup table
size_t simd_popcount(const uint8_t* data, size_t n) {
    size_t count = 0;
    for (size_t i = 0; i < n; i++) {
        count += popcount_table[data[i]];
    }
    return count;
}
#endif

// ============================================================================
// Conditional Selection: dest[i] = mask[i] ? a[i] : b[i]
// ============================================================================

#if defined(__AVX512F__) && defined(__AVX512BW__)
void simd_select_bytes(uint8_t* dest, const uint8_t* mask, 
                       const uint8_t* a, const uint8_t* b, size_t n) {
    size_t i = 0;
    __m512i zero = _mm512_setzero_si512();
    
    for (; i + 64 <= n; i += 64) {
        __m512i vm = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(mask + i));
        __m512i va = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(a + i));
        __m512i vb = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(b + i));
        
        // Create mask from non-zero bytes
        __mmask64 m = _mm512_cmpneq_epu8_mask(vm, zero);
        
        // Use mask to select between a and b
        __m512i result = _mm512_mask_blend_epi8(m, vb, va);
        _mm512_storeu_si512(reinterpret_cast<__m512i*>(dest + i), result);
    }
    
    for (; i < n; i++) {
        dest[i] = mask[i] ? a[i] : b[i];
    }
}
#elif defined(__AVX2__)
void simd_select_bytes(uint8_t* dest, const uint8_t* mask, 
                       const uint8_t* a, const uint8_t* b, size_t n) {
    size_t i = 0;
    __m256i zero = _mm256_setzero_si256();
    
    for (; i + 32 <= n; i += 32) {
        __m256i vm = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(mask + i));
        __m256i va = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(a + i));
        __m256i vb = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(b + i));
        
        // Create mask from non-zero bytes
        __m256i m = _mm256_cmpeq_epi8(vm, zero);
        
        // Select: where mask is zero (m is true), use b; otherwise use a
        __m256i result = _mm256_blendv_epi8(va, vb, m);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest + i), result);
    }
    
    for (; i < n; i++) {
        dest[i] = mask[i] ? a[i] : b[i];
    }
}
#else
void simd_select_bytes(uint8_t* dest, const uint8_t* mask, 
                       const uint8_t* a, const uint8_t* b, size_t n) {
    for (size_t i = 0; i < n; i++) {
        dest[i] = mask[i] ? a[i] : b[i];
    }
}
#endif