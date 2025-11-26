#include "simd_string_ops.h"
#include <cstdint>
#include <cstring>
#include <atomic>

#include "simd_dispatch.h"
#include "cpu_features.h"

#if (defined(__AVX512F__) && defined(__AVX512BW__)) || defined(__AVX2__)
#include <immintrin.h>
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
// We always provide a scalar fallback implementation. SIMD variants are compiled when
// the compiler supports them, and we select the best implementation at runtime via
// `simd::select_dispatch` so SIMD code is not executed on CPUs that lack support.

// Scalar fallback for to_upper
static void simd_to_upper_scalar(char* data, size_t length) {
    for (size_t i = 0; i < length; i++) {
        if (data[i] >= LOWER_A && data[i] <= LOWER_Z) {
            data[i] -= CASE_DIFF;
        }
    }
}

#if defined(__AVX512F__) && defined(__AVX512BW__)
static void simd_to_upper_avx512(char* data, size_t length) {
    size_t i = 0;
    __m512i lower_a_vec = _mm512_set1_epi8(LOWER_A);
    __m512i lower_z_vec = _mm512_set1_epi8(LOWER_Z);
    __m512i case_diff_vec = _mm512_set1_epi8(CASE_DIFF);

    for (; i + 64 <= length; i += 64) {
        __m512i chunk = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(data + i));
        __mmask64 is_ge_a = _mm512_cmpge_epu8_mask(chunk, lower_a_vec);
        __mmask64 is_le_z = _mm512_cmple_epu8_mask(chunk, lower_z_vec);
        __mmask64 is_lower = is_ge_a & is_le_z;
        __m512i converted = _mm512_mask_sub_epi8(chunk, is_lower, chunk, case_diff_vec);
        _mm512_storeu_si512(reinterpret_cast<__m512i*>(data + i), converted);
    }

    for (; i < length; i++) {
        if (data[i] >= LOWER_A && data[i] <= LOWER_Z) {
            data[i] -= CASE_DIFF;
        }
    }
}
#endif

#if defined(__AVX2__)
static void simd_to_upper_avx2(char* data, size_t length) {
    size_t i = 0;
    __m256i lower_a_vec = _mm256_set1_epi8(LOWER_A);
    __m256i lower_z_vec = _mm256_set1_epi8(LOWER_Z);
    __m256i case_diff_vec = _mm256_set1_epi8(CASE_DIFF);

    for (; i + 32 <= length; i += 32) {
        __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + i));
        __m256i is_ge_a = _mm256_cmpgt_epi8(chunk, _mm256_sub_epi8(lower_a_vec, _mm256_set1_epi8(1)));
        __m256i is_le_z = _mm256_cmpgt_epi8(_mm256_add_epi8(lower_z_vec, _mm256_set1_epi8(1)), chunk);
        __m256i is_lower = _mm256_and_si256(is_ge_a, is_le_z);
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
#endif

// Public wrapper that dispatches at runtime
void simd_to_upper(char* data, size_t length) {
    using fn_t = void(*)(char*, size_t);
    static std::atomic<fn_t> cache{nullptr};

    fn_t fn = simd::select_dispatch<fn_t>(cache, {
#if defined(__AVX512F__) && defined(__AVX512BW__)
        { &cpu_supports_avx512, simd_to_upper_avx512 },
#endif
#if defined(__AVX2__)
        { &cpu_supports_avx2, simd_to_upper_avx2 },
#endif
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
        { &cpu_supports_neon, simd_to_upper_scalar }, // NEON variant not implemented; fall back to scalar
#endif
    }, simd_to_upper_scalar);

    return fn(data, length);
}

// AVX512 implementation for to_lower
// Scalar fallback for to_lower
static void simd_to_lower_scalar(char* data, size_t length) {
    for (size_t i = 0; i < length; i++) {
        if (data[i] >= UPPER_A && data[i] <= UPPER_Z) {
            data[i] += CASE_DIFF;
        }
    }
}

#if defined(__AVX512F__) && defined(__AVX512BW__)
static void simd_to_lower_avx512(char* data, size_t length) {
    size_t i = 0;
    __m512i upper_a_vec = _mm512_set1_epi8(UPPER_A);
    __m512i upper_z_vec = _mm512_set1_epi8(UPPER_Z);
    __m512i case_diff_vec = _mm512_set1_epi8(CASE_DIFF);

    for (; i + 64 <= length; i += 64) {
        __m512i chunk = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(data + i));
        __mmask64 is_ge_a = _mm512_cmpge_epu8_mask(chunk, upper_a_vec);
        __mmask64 is_le_z = _mm512_cmple_epu8_mask(chunk, upper_z_vec);
        __mmask64 is_upper = is_ge_a & is_le_z;
        __m512i converted = _mm512_mask_add_epi8(chunk, is_upper, chunk, case_diff_vec);
        _mm512_storeu_si512(reinterpret_cast<__m512i*>(data + i), converted);
    }

    for (; i < length; i++) {
        if (data[i] >= UPPER_A && data[i] <= UPPER_Z) {
            data[i] += CASE_DIFF;
        }
    }
}
#endif

#if defined(__AVX2__)
static void simd_to_lower_avx2(char* data, size_t length) {
    size_t i = 0;
    __m256i upper_a_vec = _mm256_set1_epi8(UPPER_A);
    __m256i upper_z_vec = _mm256_set1_epi8(UPPER_Z);
    __m256i case_diff_vec = _mm256_set1_epi8(CASE_DIFF);

    for (; i + 32 <= length; i += 32) {
        __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + i));
        __m256i is_ge_a = _mm256_cmpgt_epi8(chunk, _mm256_sub_epi8(upper_a_vec, _mm256_set1_epi8(1)));
        __m256i is_le_z = _mm256_cmpgt_epi8(_mm256_add_epi8(upper_z_vec, _mm256_set1_epi8(1)), chunk);
        __m256i is_upper = _mm256_and_si256(is_ge_a, is_le_z);
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
#endif

// Public wrapper that dispatches at runtime
void simd_to_lower(char* data, size_t length) {
    using fn_t = void(*)(char*, size_t);
    static std::atomic<fn_t> cache{nullptr};

    fn_t fn = simd::select_dispatch<fn_t>(cache, {
#if defined(__AVX512F__) && defined(__AVX512BW__)
        { &cpu_supports_avx512, simd_to_lower_avx512 },
#endif
#if defined(__AVX2__)
        { &cpu_supports_avx2, simd_to_lower_avx2 },
#endif
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
        { &cpu_supports_neon, simd_to_lower_scalar }, // NEON variant not implemented; fall back to scalar
#endif
    }, simd_to_lower_scalar);

    return fn(data, length);
}