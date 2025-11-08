// Copied from jsonl_src/simd_helpers.hpp
#pragma once

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>

// Platform detection for SIMD support
#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
    #define HAVE_SSE2 1
    #ifdef __SSE4_2__
        #define HAVE_SSE42 1
    #endif
    #ifdef __AVX2__
        #define HAVE_AVX2 1
    #endif
#endif

// ARM/NEON detection
#if defined(__ARM_NEON) || defined(__aarch64__) || defined(_M_ARM64)
    #define HAVE_NEON 1
#endif

#ifdef HAVE_SSE2
#include <emmintrin.h>  // SSE2
#endif

#ifdef HAVE_SSE42
#include <nmmintrin.h>  // SSE4.2
#endif

#ifdef HAVE_AVX2
#include <immintrin.h>  // AVX2
#endif

#ifdef HAVE_NEON
#include <arm_neon.h>  // NEON
#endif

namespace simd {

// Fast newline search using SIMD when available
inline const char* FindNewline(const char* data, size_t size) {
    const char* ptr = data;
    const char* end = data + size;

#ifdef HAVE_AVX2
    // AVX2: Process 32 bytes at a time
    if (size >= 32) {
        __m256i newline_vec = _mm256_set1_epi8('\n');
        const char* avx_end = end - 31;
        
        while (ptr < avx_end) {
            __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(ptr));
            __m256i cmp = _mm256_cmpeq_epi8(chunk, newline_vec);
            int mask = _mm256_movemask_epi8(cmp);
            
            if (mask != 0) {
                // Found a newline - determine exact position
                int offset = __builtin_ctz(mask);
                return ptr + offset;
            }
            ptr += 32;
        }
    }
#elif defined(HAVE_SSE2)
    // SSE2: Process 16 bytes at a time
    if (size >= 16) {
        __m128i newline_vec = _mm_set1_epi8('\n');
        const char* sse_end = end - 15;
        
        while (ptr < sse_end) {
            __m128i chunk = _mm_loadu_si128(reinterpret_cast<const __m128i*>(ptr));
            __m128i cmp = _mm_cmpeq_epi8(chunk, newline_vec);
            int mask = _mm_movemask_epi8(cmp);
            
            if (mask != 0) {
                // Found a newline - determine exact position
                int offset = __builtin_ctz(mask);
                return ptr + offset;
            }
            ptr += 16;
        }
    }
#elif defined(HAVE_NEON)
    // NEON: Process 16 bytes at a time
    if (size >= 16) {
        uint8x16_t newline_vec = vdupq_n_u8('\n');
        const char* neon_end = end - 15;
        
        while (ptr < neon_end) {
            uint8x16_t chunk = vld1q_u8(reinterpret_cast<const uint8_t*>(ptr));
            uint8x16_t cmp = vceqq_u8(chunk, newline_vec);
            
            // Check if any byte matched
            uint64x2_t cmp64 = vreinterpretq_u64_u8(cmp);
            uint64_t low = vgetq_lane_u64(cmp64, 0);
            uint64_t high = vgetq_lane_u64(cmp64, 1);
            
            if (low != 0 || high != 0) {
                // Found a newline - scan to find exact position
                for (int i = 0; i < 16; i++) {
                    if (ptr[i] == '\n') {
                        return ptr + i;
                    }
                }
            }
            ptr += 16;
        }
    }
#endif

    // Scalar fallback for remaining bytes
    while (ptr < end) {
        if (*ptr == '\n') {
            return ptr;
        }
        ptr++;
    }
    
    return nullptr;
}

// Fast whitespace skipping using SIMD
inline const char* SkipWhitespace(const char* data, size_t size) {
    const char* ptr = data;
    const char* end = data + size;

#ifdef HAVE_AVX2
    // AVX2: Process 32 bytes at a time
    if (size >= 32) {
        __m256i space_vec = _mm256_set1_epi8(' ');
        __m256i tab_vec = _mm256_set1_epi8('\t');
        __m256i cr_vec = _mm256_set1_epi8('\r');
        const char* avx_end = end - 31;
        
        while (ptr < avx_end) {
            __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(ptr));
            
            // Check which bytes are whitespace
            __m256i is_space = _mm256_cmpeq_epi8(chunk, space_vec);
            __m256i is_tab = _mm256_cmpeq_epi8(chunk, tab_vec);
            __m256i is_cr = _mm256_cmpeq_epi8(chunk, cr_vec);
            
            // Combine all whitespace checks
            __m256i is_ws = _mm256_or_si256(_mm256_or_si256(is_space, is_tab), is_cr);
            int mask = _mm256_movemask_epi8(is_ws);
            
            if (mask != 0xFFFFFFFF) {
                // Found a non-whitespace character
                int offset = __builtin_ctz(~mask);
                return ptr + offset;
            }
            ptr += 32;
        }
    }
#elif defined(HAVE_SSE2)
    // SSE2: Process 16 bytes at a time
    if (size >= 16) {
        __m128i space_vec = _mm_set1_epi8(' ');
        __m128i tab_vec = _mm_set1_epi8('\t');
        __m128i cr_vec = _mm_set1_epi8('\r');
        const char* sse_end = end - 15;
        
        while (ptr < sse_end) {
            __m128i chunk = _mm_loadu_si128(reinterpret_cast<const __m128i*>(ptr));
            
            // Check which bytes are whitespace
            __m128i is_space = _mm_cmpeq_epi8(chunk, space_vec);
            __m128i is_tab = _mm_cmpeq_epi8(chunk, tab_vec);
            __m128i is_cr = _mm_cmpeq_epi8(chunk, cr_vec);
            
            // Combine all whitespace checks
            __m128i is_ws = _mm_or_si128(_mm_or_si128(is_space, is_tab), is_cr);
            int mask = _mm_movemask_epi8(is_ws);
            
            if (mask != 0xFFFF) {
                // Found a non-whitespace character
                int offset = __builtin_ctz(~mask & 0xFFFF);
                return ptr + offset;
            }
            ptr += 16;
        }
    }
#elif defined(HAVE_NEON)
    // NEON: Process 16 bytes at a time
    if (size >= 16) {
        uint8x16_t space_vec = vdupq_n_u8(' ');
        uint8x16_t tab_vec = vdupq_n_u8('\t');
        uint8x16_t cr_vec = vdupq_n_u8('\r');
        const char* neon_end = end - 15;
        
        while (ptr < neon_end) {
            uint8x16_t chunk = vld1q_u8(reinterpret_cast<const uint8_t*>(ptr));
            
            // Check which bytes are whitespace
            uint8x16_t is_space = vceqq_u8(chunk, space_vec);
            uint8x16_t is_tab = vceqq_u8(chunk, tab_vec);
            uint8x16_t is_cr = vceqq_u8(chunk, cr_vec);
            
            // Combine all whitespace checks
            uint8x16_t is_ws = vorrq_u8(vorrq_u8(is_space, is_tab), is_cr);
            
            // Check if all bytes are whitespace
            uint64x2_t ws64 = vreinterpretq_u64_u8(is_ws);
            uint64_t low = vgetq_lane_u64(ws64, 0);
            uint64_t high = vgetq_lane_u64(ws64, 1);
            
            if (low != 0xFFFFFFFFFFFFFFFFULL || high != 0xFFFFFFFFFFFFFFFFULL) {
                // Found a non-whitespace character - scan to find exact position
                for (int i = 0; i < 16; i++) {
                    if (ptr[i] != ' ' && ptr[i] != '\t' && ptr[i] != '\r') {
                        return ptr + i;
                    }
                }
            }
            ptr += 16;
        }
    }
#endif

    // Scalar fallback for remaining bytes
    while (ptr < end && (*ptr == ' ' || *ptr == '\t' || *ptr == '\r')) {
        ptr++;
    }
    
    return ptr;
}

// Fast quote detection for string parsing
inline const char* FindQuote(const char* data, size_t size) {
    const char* ptr = data;
    const char* end = data + size;

#ifdef HAVE_AVX2
    // AVX2: Process 32 bytes at a time
    if (size >= 32) {
        __m256i quote_vec = _mm256_set1_epi8('"');
        __m256i backslash_vec = _mm256_set1_epi8('\\');
        const char* avx_end = end - 31;
        bool escaped = false;
        
        while (ptr < avx_end) {
            __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(ptr));
            __m256i is_quote = _mm256_cmpeq_epi8(chunk, quote_vec);
            __m256i is_backslash = _mm256_cmpeq_epi8(chunk, backslash_vec);
            
            int quote_mask = _mm256_movemask_epi8(is_quote);
            int backslash_mask = _mm256_movemask_epi8(is_backslash);
            
            if (quote_mask != 0 || backslash_mask != 0) {
                // Found a quote or backslash, need to handle escapes carefully
                // Fall back to scalar processing for this chunk
                for (int i = 0; i < 32 && ptr < end; i++, ptr++) {
                    if (escaped) {
                        escaped = false;
                    } else if (*ptr == '\\') {
                        escaped = true;
                    } else if (*ptr == '"') {
                        return ptr;
                    }
                }
                continue;
            }
            ptr += 32;
        }
    }
#elif defined(HAVE_SSE2)
    // SSE2: Process 16 bytes at a time
    if (size >= 16) {
        __m128i quote_vec = _mm_set1_epi8('"');
        __m128i backslash_vec = _mm_set1_epi8('\\');
        const char* sse_end = end - 15;
        bool escaped = false;
        
        while (ptr < sse_end) {
            __m128i chunk = _mm_loadu_si128(reinterpret_cast<const __m128i*>(ptr));
            __m128i is_quote = _mm_cmpeq_epi8(chunk, quote_vec);
            __m128i is_backslash = _mm_cmpeq_epi8(chunk, backslash_vec);
            
            int quote_mask = _mm_movemask_epi8(is_quote);
            int backslash_mask = _mm_movemask_epi8(is_backslash);
            
            if (quote_mask != 0 || backslash_mask != 0) {
                // Found a quote or backslash, need to handle escapes carefully
                // Fall back to scalar processing for this chunk
                for (int i = 0; i < 16 && ptr < end; i++, ptr++) {
                    if (escaped) {
                        escaped = false;
                    } else if (*ptr == '\\') {
                        escaped = true;
                    } else if (*ptr == '"') {
                        return ptr;
                    }
                }
                continue;
            }
            ptr += 16;
        }
    }
#elif defined(HAVE_NEON)
    // NEON: Process 16 bytes at a time
    if (size >= 16) {
        uint8x16_t quote_vec = vdupq_n_u8('"');
        uint8x16_t backslash_vec = vdupq_n_u8('\\');
        const char* neon_end = end - 15;
        bool escaped = false;
        
        while (ptr < neon_end) {
            uint8x16_t chunk = vld1q_u8(reinterpret_cast<const uint8_t*>(ptr));
            uint8x16_t is_quote = vceqq_u8(chunk, quote_vec);
            uint8x16_t is_backslash = vceqq_u8(chunk, backslash_vec);
            
            // Check if any quote or backslash is present
            uint64x2_t quote64 = vreinterpretq_u64_u8(is_quote);
            uint64x2_t backslash64 = vreinterpretq_u64_u8(is_backslash);
            
            uint64_t quote_low = vgetq_lane_u64(quote64, 0);
            uint64_t quote_high = vgetq_lane_u64(quote64, 1);
            uint64_t backslash_low = vgetq_lane_u64(backslash64, 0);
            uint64_t backslash_high = vgetq_lane_u64(backslash64, 1);
            
            if (quote_low != 0 || quote_high != 0 || backslash_low != 0 || backslash_high != 0) {
                // Found a quote or backslash, need to handle escapes carefully
                // Fall back to scalar processing for this chunk
                for (int i = 0; i < 16 && ptr < end; i++, ptr++) {
                    if (escaped) {
                        escaped = false;
                    } else if (*ptr == '\\') {
                        escaped = true;
                    } else if (*ptr == '"') {
                        return ptr;
                    }
                }
                continue;
            }
            ptr += 16;
        }
    }
#endif

    // Scalar fallback for remaining bytes
    bool escaped = false;
    while (ptr < end) {
        if (escaped) {
            escaped = false;
        } else if (*ptr == '\\') {
            escaped = true;
        } else if (*ptr == '"') {
            return ptr;
        }
        ptr++;
    }
    
    return nullptr;
}

// Fast character search (for delimiters like ':', ',', '}', etc.)
inline const char* FindChar(const char* data, size_t size, char target) {
    const char* ptr = data;
    const char* end = data + size;

#ifdef HAVE_AVX2
    // AVX2: Process 32 bytes at a time
    if (size >= 32) {
        __m256i target_vec = _mm256_set1_epi8(target);
        const char* avx_end = end - 31;
        
        while (ptr < avx_end) {
            __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(ptr));
            __m256i cmp = _mm256_cmpeq_epi8(chunk, target_vec);
            int mask = _mm256_movemask_epi8(cmp);
            
            if (mask != 0) {
                int offset = __builtin_ctz(mask);
                return ptr + offset;
            }
            ptr += 32;
        }
    }
#elif defined(HAVE_SSE2)
    // SSE2: Process 16 bytes at a time
    if (size >= 16) {
        __m128i target_vec = _mm_set1_epi8(target);
        const char* sse_end = end - 15;
        
        while (ptr < sse_end) {
            __m128i chunk = _mm_loadu_si128(reinterpret_cast<const __m128i*>(ptr));
            __m128i cmp = _mm_cmpeq_epi8(chunk, target_vec);
            int mask = _mm_movemask_epi8(cmp);
            
            if (mask != 0) {
                int offset = __builtin_ctz(mask);
                return ptr + offset;
            }
            ptr += 16;
        }
    }
#elif defined(HAVE_NEON)
    // NEON: Process 16 bytes at a time
    if (size >= 16) {
        uint8x16_t target_vec = vdupq_n_u8(target);
        const char* neon_end = end - 15;
        
        while (ptr < neon_end) {
            uint8x16_t chunk = vld1q_u8(reinterpret_cast<const uint8_t*>(ptr));
            uint8x16_t cmp = vceqq_u8(chunk, target_vec);
            
            // Check if any byte matched
            uint64x2_t cmp64 = vreinterpretq_u64_u8(cmp);
            uint64_t low = vgetq_lane_u64(cmp64, 0);
            uint64_t high = vgetq_lane_u64(cmp64, 1);
            
            if (low != 0 || high != 0) {
                // Found the target character - scan to find exact position
                for (int i = 0; i < 16; i++) {
                    if (ptr[i] == target) {
                        return ptr + i;
                    }
                }
            }
            ptr += 16;
        }
    }
#endif

    // Scalar fallback for remaining bytes
    while (ptr < end) {
        if (*ptr == target) {
            return ptr;
        }
        ptr++;
    }
    
    return nullptr;
}

// Fast newline counting using SIMD for memory pre-allocation
inline size_t CountNewlines(const char* data, size_t size) {
    const char* ptr = data;
    const char* end = data + size;
    size_t count = 0;

#ifdef HAVE_AVX2
    // AVX2: Process 32 bytes at a time
    if (size >= 32) {
        __m256i newline_vec = _mm256_set1_epi8('\n');
        const char* avx_end = end - 31;
        
        while (ptr < avx_end) {
            __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(ptr));
            __m256i cmp = _mm256_cmpeq_epi8(chunk, newline_vec);
            int mask = _mm256_movemask_epi8(cmp);
            
            // Count set bits in mask
            count += __builtin_popcount(mask);
            ptr += 32;
        }
    }
#elif defined(HAVE_SSE2)
    // SSE2: Process 16 bytes at a time
    if (size >= 16) {
        __m128i newline_vec = _mm_set1_epi8('\n');
        const char* sse_end = end - 15;
        
        while (ptr < sse_end) {
            __m128i chunk = _mm_loadu_si128(reinterpret_cast<const __m128i*>(ptr));
            __m128i cmp = _mm_cmpeq_epi8(chunk, newline_vec);
            int mask = _mm_movemask_epi8(cmp);
            
            // Count set bits in mask
            count += __builtin_popcount(mask);
            ptr += 16;
        }
    }
#elif defined(HAVE_NEON)
    // NEON: Process 16 bytes at a time
    if (size >= 16) {
        uint8x16_t newline_vec = vdupq_n_u8('\n');
        const char* neon_end = end - 15;
        
        while (ptr < neon_end) {
            uint8x16_t chunk = vld1q_u8(reinterpret_cast<const uint8_t*>(ptr));
            uint8x16_t cmp = vceqq_u8(chunk, newline_vec);
            
            // Count matching bytes
            // Sum up the comparison results (0xFF for match, 0x00 for no match)
            uint64x2_t cmp64 = vpaddlq_u32(vpaddlq_u16(vpaddlq_u8(cmp)));
            count += (vgetq_lane_u64(cmp64, 0) + vgetq_lane_u64(cmp64, 1)) / 255;
            
            ptr += 16;
        }
    }
#endif

    // Scalar fallback for remaining bytes
    while (ptr < end) {
        if (*ptr == '\n') {
            count++;
        }
        ptr++;
    }
    
    return count;
}

} // namespace simd
