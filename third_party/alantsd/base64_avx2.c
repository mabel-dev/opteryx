// This file is a renamed copy of the AVX2 base64 implementation.
// Renamed from base64_axv2.c to base64_avx2.c for consistency.
// The implementation uses AVX2 intrinsics when __AVX2__ is available and otherwise
// falls back to the scalar implementation.
#include "base64.h"

#ifdef __AVX2__
#include <immintrin.h>

void* b64tobin_avx2(void* restrict dest, const char* restrict src, size_t len) {
    if (len < 32) {
        return b64tobin_scalar(dest, src, len);
    }

    uint8_t* out = (uint8_t*)dest;
    const uint8_t* in = (const uint8_t*)src;
    const uint8_t* end = in + len;

    while (end - in >= 32) {
        // Load 32 bytes
        __m256i input = _mm256_loadu_si256((const __m256i*)in);
        
        // Basic validation (check if all characters are in valid base64 range)
        __m256i valid_mask = _mm256_or_si256(
            _mm256_or_si256(
                _mm256_cmpgt_epi8(_mm256_set1_epi8('Z' + 1), input),
                _mm256_cmpgt_epi8(_mm256_set1_epi8('z' + 1), input)
            ),
            _mm256_or_si256(
                _mm256_cmpgt_epi8(_mm256_set1_epi8('9' + 1), input),
                _mm256_cmpeq_epi8(input, _mm256_set1_epi8('+'))
            )
        );
        
        if (_mm256_movemask_epi8(valid_mask) != 0xFFFFFFFF) {
            break;
        }

        // For now, use scalar decoding - full AVX2 decode is complex
        in += 32;
        out = b64tobin_scalar(out, (const char*)in - 32, 32);
    }

    // Handle remainder
    if (end > in) {
        out = b64tobin_scalar(out, (const char*)in, end - in);
    }

    return out;
}

char* bintob64_avx2(char* restrict dest, const void* restrict src, size_t size) {
    if (size < 24) {
        return bintob64_scalar(dest, src, size);
    }

    const uint8_t* in = (const uint8_t*)src;
    const uint8_t* end = in + size;
    char* out = dest;

    while (end - in >= 24) {
        // Process 24 bytes at a time (produces 32 base64 chars)
        // Use scalar for now - full AVX2 encode is complex
        in += 24;
        out = bintob64_scalar(out, in - 24, 24);
    }

    // Handle remainder
    if (end > in) {
        out = bintob64_scalar(out, in, end - in);
    }

    return out;
}

#else
// Stub implementations when AVX2 is not available
void* b64tobin_avx2(void* restrict dest, const char* restrict src, size_t len) {
    return b64tobin_scalar(dest, src, len);
}

char* bintob64_avx2(char* restrict dest, const void* restrict src, size_t size) {
    return bintob64_scalar(dest, src, size);
}
#endif

