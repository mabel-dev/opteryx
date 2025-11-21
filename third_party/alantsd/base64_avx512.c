#include "base64.h"

#if defined(__AVX512F__) && defined(__AVX512BW__)
#include <immintrin.h>

// AVX512 implementation for base64 decoding (optimized for 64-byte chunks)
void* b64tobin_avx512(void* restrict dest, const char* restrict src, size_t len) {
    uint8_t* d = (uint8_t*)dest;
    const char* s = src;
    size_t i = 0;
    
    // Process full 64-byte blocks (64 base64 chars -> 48 binary bytes)
    while (i + 64 <= len) {
        // For now, use scalar decoding - full AVX512 decode is complex
        // This is a placeholder for future optimization
        for (size_t j = 0; j < 64 && i + j < len; j += 4) {
            if (s[i + j] == '=' || s[i + j + 1] == '=') break;
            
            uint8_t b1 = B64_DECODE_LUT[(uint8_t)s[i + j]];
            uint8_t b2 = B64_DECODE_LUT[(uint8_t)s[i + j + 1]];
            uint8_t b3 = B64_DECODE_LUT[(uint8_t)s[i + j + 2]];
            uint8_t b4 = B64_DECODE_LUT[(uint8_t)s[i + j + 3]];
            
            *d++ = (b1 << 2) | (b2 >> 4);
            if (s[i + j + 2] != '=') {
                *d++ = (b2 << 4) | (b3 >> 2);
                if (s[i + j + 3] != '=') {
                    *d++ = (b3 << 6) | b4;
                }
            }
        }
        i += 64;
    }
    
    // Process remaining bytes with scalar implementation
    while (i < len && s[i] != '=' && i + 3 < len) {
        uint8_t b1 = B64_DECODE_LUT[(uint8_t)s[i]];
        uint8_t b2 = B64_DECODE_LUT[(uint8_t)s[i + 1]];
        uint8_t b3 = B64_DECODE_LUT[(uint8_t)s[i + 2]];
        uint8_t b4 = B64_DECODE_LUT[(uint8_t)s[i + 3]];
        
        *d++ = (b1 << 2) | (b2 >> 4);
        if (s[i + 2] != '=') {
            *d++ = (b2 << 4) | (b3 >> 2);
            if (s[i + 3] != '=') {
                *d++ = (b3 << 6) | b4;
            }
        }
        i += 4;
    }
    
    return dest;
}

// AVX512 implementation for base64 encoding (optimized for 48-byte chunks)
char* bintob64_avx512(char* restrict dest, const void* restrict src, size_t size) {
    const uint8_t* s = (const uint8_t*)src;
    char* d = dest;
    size_t i = 0;
    
    // Process full 48-byte blocks (48 binary bytes -> 64 base64 chars)
    while (i + 48 <= size) {
        // Use scalar for now - full AVX512 encode is complex
        // This is a placeholder for future optimization
        for (size_t j = 0; j < 48; j += 3) {
            uint32_t val = ((uint32_t)s[i + j] << 16) | 
                          ((uint32_t)s[i + j + 1] << 8) | 
                          s[i + j + 2];
            
            *d++ = B64_ENCODE_LUT[(val >> 18) & 0x3F];
            *d++ = B64_ENCODE_LUT[(val >> 12) & 0x3F];
            *d++ = B64_ENCODE_LUT[(val >> 6) & 0x3F];
            *d++ = B64_ENCODE_LUT[val & 0x3F];
        }
        i += 48;
    }
    
    // Handle remaining bytes with scalar implementation
    while (i + 2 < size) {
        uint32_t val = ((uint32_t)s[i] << 16) | 
                      ((uint32_t)s[i + 1] << 8) | 
                      s[i + 2];
        
        *d++ = B64_ENCODE_LUT[(val >> 18) & 0x3F];
        *d++ = B64_ENCODE_LUT[(val >> 12) & 0x3F];
        *d++ = B64_ENCODE_LUT[(val >> 6) & 0x3F];
        *d++ = B64_ENCODE_LUT[val & 0x3F];
        i += 3;
    }
    
    // Handle last 1-2 bytes
    if (i < size) {
        uint32_t val = (uint32_t)s[i] << 16;
        if (i + 1 < size) {
            val |= (uint32_t)s[i + 1] << 8;
        }
        
        *d++ = B64_ENCODE_LUT[(val >> 18) & 0x3F];
        *d++ = B64_ENCODE_LUT[(val >> 12) & 0x3F];
        if (i + 1 < size) {
            *d++ = B64_ENCODE_LUT[(val >> 6) & 0x3F];
        } else {
            *d++ = '=';
        }
        *d++ = '=';
    }
    
    *d = '\0';
    return dest;
}

#else
// Stub implementations when AVX512 is not available
void* b64tobin_avx512(void* restrict dest, const char* restrict src, size_t len) {
    return b64tobin_scalar(dest, src, len);
}

char* bintob64_avx512(char* restrict dest, const void* restrict src, size_t size) {
    return bintob64_scalar(dest, src, size);
}
#endif
