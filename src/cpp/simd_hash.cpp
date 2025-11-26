#include "simd_hash.h"

#include <cstddef>
#include <cstdint>
#include <atomic>

#include "simd_dispatch.h"
#include "cpu_features.h"

#if defined(__AVX2__)
#include <immintrin.h>
#elif defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>
#endif

namespace {

inline void scalar_mix(uint64_t* dest, const uint64_t* values, std::size_t count) {
    for (std::size_t i = 0; i < count; ++i) {
        uint64_t mixed = dest[i] ^ values[i];
        mixed = mixed * MIX_HASH_CONSTANT + 1;
        mixed ^= mixed >> 32;
        dest[i] = mixed;
    }
}

// Provide architecture-specific mullo_u64 overloads.

#if defined(__AVX2__)
inline __m256i mullo_u64(__m256i a, __m256i b) {
    // AVX2 lacks a direct 64-bit integer multiply, so combine 32-bit partials per lane.
    const __m256i mask = _mm256_set1_epi64x(0xFFFFFFFFULL);
    __m256i a_lo = _mm256_and_si256(a, mask);
    __m256i b_lo = _mm256_and_si256(b, mask);
    __m256i a_hi = _mm256_srli_epi64(a, 32);
    __m256i b_hi = _mm256_srli_epi64(b, 32);

    __m256i prod_ll = _mm256_mul_epu32(a_lo, b_lo);
    __m256i prod_lh = _mm256_mul_epu32(a_lo, b_hi);
    __m256i prod_hl = _mm256_mul_epu32(a_hi, b_lo);

    __m256i cross = _mm256_add_epi64(prod_lh, prod_hl);
    cross = _mm256_slli_epi64(cross, 32);

    return _mm256_add_epi64(prod_ll, cross);
}
#endif

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
inline uint64x2_t mullo_u64(uint64x2_t a, uint64x2_t b) {
    alignas(16) uint64_t a_vals[2];
    alignas(16) uint64_t b_vals[2];
    alignas(16) uint64_t res_vals[2];
    vst1q_u64(a_vals, a);
    vst1q_u64(b_vals, b);
    res_vals[0] = a_vals[0] * b_vals[0];
    res_vals[1] = a_vals[1] * b_vals[1];
    return vld1q_u64(res_vals);
}
#endif

}  // namespace

static void simd_mix_hash_scalar(uint64_t* dest, const uint64_t* values, std::size_t count) {
    if (dest == nullptr || values == nullptr || count == 0) {
        return;
    }

    scalar_mix(dest, values, count);
}

#if defined(__AVX2__)
static void simd_mix_hash_avx2(uint64_t* dest, const uint64_t* values, std::size_t count) {
    if (dest == nullptr || values == nullptr || count == 0) {
        return;
    }

    const std::size_t stride = 4;
    const __m256i const_vec = _mm256_set1_epi64x(static_cast<long long>(MIX_HASH_CONSTANT));
    std::size_t i = 0;
    for (; i + stride <= count; i += stride) {
        __m256i dst_vec = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(dest + i));
        __m256i val_vec = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(values + i));
        __m256i mixed = _mm256_xor_si256(dst_vec, val_vec);
        __m256i product = mullo_u64(mixed, const_vec);
        product = _mm256_add_epi64(product, _mm256_set1_epi64x(1));
        __m256i shifted = _mm256_srli_epi64(product, 32);
        __m256i combined = _mm256_xor_si256(product, shifted);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest + i), combined);
    }
    if (i < count) {
        scalar_mix(dest + i, values + i, count - i);
    }
}
#endif

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
static void simd_mix_hash_neon(uint64_t* dest, const uint64_t* values, std::size_t count) {
    if (dest == nullptr || values == nullptr || count == 0) {
        return;
    }

    const std::size_t stride = 2;
    const uint64x2_t const_vec = vdupq_n_u64(MIX_HASH_CONSTANT);
    std::size_t i = 0;
    for (; i + stride <= count; i += stride) {
        uint64x2_t dst_vec = vld1q_u64(dest + i);
        uint64x2_t val_vec = vld1q_u64(values + i);
        uint64x2_t mixed = veorq_u64(dst_vec, val_vec);
        uint64x2_t product = mullo_u64(mixed, const_vec);
        product = vaddq_u64(product, vdupq_n_u64(1));
        uint64x2_t shifted = vshrq_n_u64(product, 32);
        uint64x2_t combined = veorq_u64(product, shifted);
        vst1q_u64(dest + i, combined);
    }
    if (i < count) {
        scalar_mix(dest + i, values + i, count - i);
    }
}
#endif

void simd_mix_hash(uint64_t* dest, const uint64_t* values, std::size_t count) {
    using fn_t = void(*)(uint64_t*, const uint64_t*, std::size_t);
    static std::atomic<fn_t> cache{nullptr};

#if defined(__AVX2__)
    // noop - AVX2 candidate included below
#endif
    fn_t fn = simd::select_dispatch<fn_t>(cache, {
#if defined(__AVX2__)
    { &cpu_supports_avx2, simd_mix_hash_avx2 },
#endif
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
        { &cpu_supports_neon, simd_mix_hash_neon },
#endif
    }, simd_mix_hash_scalar);

    return fn(dest, values, count);
}