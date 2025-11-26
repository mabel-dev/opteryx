#include "simd_bitops.h"
#include <cstring>
#include <atomic>

#include "simd_dispatch.h"
#include "cpu_features.h"

#if defined(__AVX2__)
#include <immintrin.h>
#endif

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>
#endif

// ============================================================================
// Bitwise AND
// ============================================================================

// Scalar fallback for and
static void simd_and_mask_scalar(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n) {
    for (size_t i = 0; i < n; i++) {
        dest[i] = a[i] & b[i];
    }
}

#if defined(__AVX2__)
static void simd_and_mask_avx2(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n) {
    size_t i = 0;
    for (; i + 32 <= n; i += 32) {
        __m256i va = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(a + i));
        __m256i vb = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(b + i));
        __m256i result = _mm256_and_si256(va, vb);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest + i), result);
    }
    for (; i < n; i++) dest[i] = a[i] & b[i];
}
#endif

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
static void simd_and_mask_neon(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n) {
    size_t i = 0;
    for (; i + 16 <= n; i += 16) {
        uint8x16_t va = vld1q_u8(a + i);
        uint8x16_t vb = vld1q_u8(b + i);
        uint8x16_t result = vandq_u8(va, vb);
        vst1q_u8(dest + i, result);
    }
    for (; i < n; i++) dest[i] = a[i] & b[i];
}
#endif

void simd_and_mask(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n) {
    using fn_t = void(*)(uint8_t*, const uint8_t*, const uint8_t*, size_t);
    static std::atomic<fn_t> cache{nullptr};

    #if defined(__AVX2__)
        fn_t fn = simd::select_dispatch<fn_t>(cache, {
#if defined(__AVX2__)
        { &cpu_supports_avx2, simd_and_mask_avx2 },
#endif
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
        { &cpu_supports_neon, simd_and_mask_neon },
#endif
    }, simd_and_mask_scalar);

    return fn(dest, a, b, n);
}

// ============================================================================
// Bitwise OR
// ============================================================================

// Scalar fallback for or
static void simd_or_mask_scalar(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n) {
    for (size_t i = 0; i < n; i++) dest[i] = a[i] | b[i];
}

#if defined(__AVX2__)
static void simd_or_mask_avx2(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n) {
    size_t i = 0;
    for (; i + 32 <= n; i += 32) {
        __m256i va = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(a + i));
        __m256i vb = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(b + i));
        __m256i result = _mm256_or_si256(va, vb);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest + i), result);
    }
    for (; i < n; i++) dest[i] = a[i] | b[i];
}
#endif

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
static void simd_or_mask_neon(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n) {
    size_t i = 0;
    for (; i + 16 <= n; i += 16) {
        uint8x16_t va = vld1q_u8(a + i);
        uint8x16_t vb = vld1q_u8(b + i);
        uint8x16_t result = vorrq_u8(va, vb);
        vst1q_u8(dest + i, result);
    }
    for (; i < n; i++) dest[i] = a[i] | b[i];
}
#endif

void simd_or_mask(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n) {
    using fn_t = void(*)(uint8_t*, const uint8_t*, const uint8_t*, size_t);
    static std::atomic<fn_t> cache{nullptr};

#if defined(__AVX2__)
    fn_t fn = simd::select_dispatch<fn_t>(cache, {
#if defined(__AVX2__)
        { &cpu_supports_avx2, simd_or_mask_avx2 },
#endif
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
        { &cpu_supports_neon, simd_or_mask_neon },
#endif
    }, simd_or_mask_scalar);

    return fn(dest, a, b, n);
}

// ============================================================================
// Bitwise XOR
// ============================================================================

// Scalar fallback for xor
static void simd_xor_mask_scalar(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n) {
    for (size_t i = 0; i < n; i++) dest[i] = a[i] ^ b[i];
}

#if defined(__AVX2__)
static void simd_xor_mask_avx2(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n) {
    size_t i = 0;
    for (; i + 32 <= n; i += 32) {
        __m256i va = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(a + i));
        __m256i vb = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(b + i));
        __m256i result = _mm256_xor_si256(va, vb);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest + i), result);
    }
    for (; i < n; i++) dest[i] = a[i] ^ b[i];
}
#endif

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
static void simd_xor_mask_neon(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n) {
    size_t i = 0;
    for (; i + 16 <= n; i += 16) {
        uint8x16_t va = vld1q_u8(a + i);
        uint8x16_t vb = vld1q_u8(b + i);
        uint8x16_t result = veorq_u8(va, vb);
        vst1q_u8(dest + i, result);
    }
    for (; i < n; i++) dest[i] = a[i] ^ b[i];
}
#endif

void simd_xor_mask(uint8_t* dest, const uint8_t* a, const uint8_t* b, size_t n) {
    using fn_t = void(*)(uint8_t*, const uint8_t*, const uint8_t*, size_t);
    static std::atomic<fn_t> cache{nullptr};

#if defined(__AVX2__)
    fn_t fn = simd::select_dispatch<fn_t>(cache, {
#if defined(__AVX2__)
        { &cpu_supports_avx2, simd_xor_mask_avx2 },
#endif
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
        { &cpu_supports_neon, simd_xor_mask_neon },
#endif
    }, simd_xor_mask_scalar);

    return fn(dest, a, b, n);
}

// ============================================================================
// Bitwise NOT
// ============================================================================

// Scalar fallback for not
static void simd_not_mask_scalar(uint8_t* dest, const uint8_t* src, size_t n) {
    for (size_t i = 0; i < n; i++) dest[i] = ~src[i];
}

#if defined(__AVX2__)
static void simd_not_mask_avx2(uint8_t* dest, const uint8_t* src, size_t n) {
    size_t i = 0;
    __m256i all_ones = _mm256_set1_epi8(static_cast<char>(0xFF));
    for (; i + 32 <= n; i += 32) {
        __m256i v = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + i));
        __m256i result = _mm256_xor_si256(v, all_ones);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest + i), result);
    }
    for (; i < n; i++) dest[i] = ~src[i];
}
#endif

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
static void simd_not_mask_neon(uint8_t* dest, const uint8_t* src, size_t n) {
    size_t i = 0;
    for (; i + 16 <= n; i += 16) {
        uint8x16_t v = vld1q_u8(src + i);
        uint8x16_t result = vmvnq_u8(v);
        vst1q_u8(dest + i, result);
    }
    for (; i < n; i++) dest[i] = ~src[i];
}
#endif

void simd_not_mask(uint8_t* dest, const uint8_t* src, size_t n) {
    using fn_t = void(*)(uint8_t*, const uint8_t*, size_t);
    static std::atomic<fn_t> cache{nullptr};

#if defined(__AVX2__)
    fn_t fn = simd::select_dispatch<fn_t>(cache, {
#if defined(__AVX2__)
        { &cpu_supports_avx2, simd_not_mask_avx2 },
#endif
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
        { &cpu_supports_neon, simd_not_mask_neon },
#endif
    }, simd_not_mask_scalar);

    return fn(dest, src, n);
}

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

// Keep popcount as-is: its implementations are safe fallbacks and do not execute
// wide SIMD intrinsics that would cause illegal-instruction faults on older CPUs.
#if defined(__AVX2__) && defined(__POPCNT__)
size_t simd_popcount(const uint8_t* data, size_t n) {
    size_t count = 0;
    size_t i = 0;
    for (; i + 8 <= n; i += 8) {
        uint64_t val;
        memcpy(&val, data + i, 8);
        count += __builtin_popcountll(val);
    }
    for (; i < n; i++) count += popcount_table[data[i]];
    return count;
}
#else
size_t simd_popcount(const uint8_t* data, size_t n) {
    size_t count = 0;
    for (size_t i = 0; i < n; i++) count += popcount_table[data[i]];
    return count;
}
#endif

// ============================================================================
// Conditional Selection: dest[i] = mask[i] ? a[i] : b[i]
// ============================================================================

// Scalar fallback for select
static void simd_select_bytes_scalar(uint8_t* dest, const uint8_t* mask,
                                     const uint8_t* a, const uint8_t* b, size_t n) {
    for (size_t i = 0; i < n; i++) dest[i] = mask[i] ? a[i] : b[i];
}

#if defined(__AVX2__)
static void simd_select_bytes_avx2(uint8_t* dest, const uint8_t* mask,
                                   const uint8_t* a, const uint8_t* b, size_t n) {
    size_t i = 0;
    __m256i zero = _mm256_setzero_si256();

    for (; i + 32 <= n; i += 32) {
        __m256i vm = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(mask + i));
        __m256i va = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(a + i));
        __m256i vb = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(b + i));
        __m256i m = _mm256_cmpeq_epi8(vm, zero);
        __m256i result = _mm256_blendv_epi8(va, vb, m);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest + i), result);
    }
    for (; i < n; i++) dest[i] = mask[i] ? a[i] : b[i];
}
#endif

void simd_select_bytes(uint8_t* dest, const uint8_t* mask,
                       const uint8_t* a, const uint8_t* b, size_t n) {
    using fn_t = void(*)(uint8_t*, const uint8_t*, const uint8_t*, const uint8_t*, size_t);
    static std::atomic<fn_t> cache{nullptr};

#if defined(__AVX2__)
    fn_t fn = simd::select_dispatch<fn_t>(cache, {
#if defined(__AVX2__)
        { &cpu_supports_avx2, simd_select_bytes_avx2 },
#endif
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
        { &cpu_supports_neon, simd_select_bytes_scalar },
#endif
    }, simd_select_bytes_scalar);

    return fn(dest, mask, a, b, n);
}