#include "base64.h"

#if defined(__ARM_NEON) || defined(__aarch64__)
#include <arm_neon.h>
#include <string.h>

void* b64tobin_neon(void* restrict dest, const char* restrict src, size_t len) {
    // Base64 decoding: 4 base64 chars -> 3 bytes
    if (len < 128 || len % 4 != 0) {
        return b64tobin_scalar(dest, src, len);
    }

    uint8_t* out = (uint8_t*)dest;
    const uint8_t* in = (const uint8_t*)src;
    const uint8_t* end = in + len;

    // Process 128 input bytes at a time (32 groups of 4 -> 96 output bytes)
    while (end - in >= 128) {
        const uint8_t* in_ptr = in;
        uint8_t* out_ptr = out;
        
        // Check for padding in this block (padding char is '=' = ASCII 61)
        uint8x16_t pad = vdupq_n_u8('=');
        uint8x16_t v0 = vld1q_u8(in_ptr);
        uint8x16_t v1 = vld1q_u8(in_ptr + 16);
        uint8x16_t v2 = vld1q_u8(in_ptr + 32);
        uint8x16_t v3 = vld1q_u8(in_ptr + 48);
        uint8x16_t v4 = vld1q_u8(in_ptr + 64);
        uint8x16_t v5 = vld1q_u8(in_ptr + 80);
        uint8x16_t v6 = vld1q_u8(in_ptr + 96);
        uint8x16_t v7 = vld1q_u8(in_ptr + 112);
        
        uint16_t has_pad = vmaxvq_u8(vceqq_u8(v0, pad)) | vmaxvq_u8(vceqq_u8(v1, pad)) |
                           vmaxvq_u8(vceqq_u8(v2, pad)) | vmaxvq_u8(vceqq_u8(v3, pad)) |
                           vmaxvq_u8(vceqq_u8(v4, pad)) | vmaxvq_u8(vceqq_u8(v5, pad)) |
                           vmaxvq_u8(vceqq_u8(v6, pad)) | vmaxvq_u8(vceqq_u8(v7, pad));
        
        if (has_pad) {
            // Padding detected, use scalar for this block
            out = b64tobin_scalar(out, (const char*)in, 128);
            if (out == NULL) return NULL;
            in += 128;
            continue;
        }
        
        // Decode 32 groups with aggressive unrolling
        for (int g = 0; g < 32; g += 8) {
            // Decode 8 groups in parallel
            uint8_t a0 = B64_DECODE_LUT[in_ptr[0]], b0 = B64_DECODE_LUT[in_ptr[1]];
            uint8_t c0 = B64_DECODE_LUT[in_ptr[2]], d0 = B64_DECODE_LUT[in_ptr[3]];
            uint8_t a1 = B64_DECODE_LUT[in_ptr[4]], b1 = B64_DECODE_LUT[in_ptr[5]];
            uint8_t c1 = B64_DECODE_LUT[in_ptr[6]], d1 = B64_DECODE_LUT[in_ptr[7]];
            uint8_t a2 = B64_DECODE_LUT[in_ptr[8]], b2 = B64_DECODE_LUT[in_ptr[9]];
            uint8_t c2 = B64_DECODE_LUT[in_ptr[10]], d2 = B64_DECODE_LUT[in_ptr[11]];
            uint8_t a3 = B64_DECODE_LUT[in_ptr[12]], b3 = B64_DECODE_LUT[in_ptr[13]];
            uint8_t c3 = B64_DECODE_LUT[in_ptr[14]], d3 = B64_DECODE_LUT[in_ptr[15]];
            uint8_t a4 = B64_DECODE_LUT[in_ptr[16]], b4 = B64_DECODE_LUT[in_ptr[17]];
            uint8_t c4 = B64_DECODE_LUT[in_ptr[18]], d4 = B64_DECODE_LUT[in_ptr[19]];
            uint8_t a5 = B64_DECODE_LUT[in_ptr[20]], b5 = B64_DECODE_LUT[in_ptr[21]];
            uint8_t c5 = B64_DECODE_LUT[in_ptr[22]], d5 = B64_DECODE_LUT[in_ptr[23]];
            uint8_t a6 = B64_DECODE_LUT[in_ptr[24]], b6 = B64_DECODE_LUT[in_ptr[25]];
            uint8_t c6 = B64_DECODE_LUT[in_ptr[26]], d6 = B64_DECODE_LUT[in_ptr[27]];
            uint8_t a7 = B64_DECODE_LUT[in_ptr[28]], b7 = B64_DECODE_LUT[in_ptr[29]];
            uint8_t c7 = B64_DECODE_LUT[in_ptr[30]], d7 = B64_DECODE_LUT[in_ptr[31]];
            
            // Quick error check (branchless OR)
            if (((a0|b0|c0|d0|a1|b1|c1|d1|a2|b2|c2|d2|a3|b3|c3|d3|
                  a4|b4|c4|d4|a5|b5|c5|d5|a6|b6|c6|d6|a7|b7|c7|d7) & 0xC0)) {
                return NULL;
            }
            
            // Decode all groups
            out_ptr[0] = (a0 << 2) | (b0 >> 4);
            out_ptr[1] = (b0 << 4) | (c0 >> 2);
            out_ptr[2] = (c0 << 6) | d0;
            
            out_ptr[3] = (a1 << 2) | (b1 >> 4);
            out_ptr[4] = (b1 << 4) | (c1 >> 2);
            out_ptr[5] = (c1 << 6) | d1;
            
            out_ptr[6] = (a2 << 2) | (b2 >> 4);
            out_ptr[7] = (b2 << 4) | (c2 >> 2);
            out_ptr[8] = (c2 << 6) | d2;
            
            out_ptr[9] = (a3 << 2) | (b3 >> 4);
            out_ptr[10] = (b3 << 4) | (c3 >> 2);
            out_ptr[11] = (c3 << 6) | d3;
            
            out_ptr[12] = (a4 << 2) | (b4 >> 4);
            out_ptr[13] = (b4 << 4) | (c4 >> 2);
            out_ptr[14] = (c4 << 6) | d4;
            
            out_ptr[15] = (a5 << 2) | (b5 >> 4);
            out_ptr[16] = (b5 << 4) | (c5 >> 2);
            out_ptr[17] = (c5 << 6) | d5;
            
            out_ptr[18] = (a6 << 2) | (b6 >> 4);
            out_ptr[19] = (b6 << 4) | (c6 >> 2);
            out_ptr[20] = (c6 << 6) | d6;
            
            out_ptr[21] = (a7 << 2) | (b7 >> 4);
            out_ptr[22] = (b7 << 4) | (c7 >> 2);
            out_ptr[23] = (c7 << 6) | d7;
            
            in_ptr += 32;
            out_ptr += 24;
        }
        
        in += 128;
        out += 96;
    }

    // Handle remainder with scalar
    if (end > in) {
        out = b64tobin_scalar(out, (const char*)in, end - in);
    }

    return out;
}

char* bintob64_neon(char* restrict dest, const void* restrict src, size_t size) {
    // Base64 encoding: 3 bytes -> 4 base64 chars
    // Optimized NEON implementation processing multiple groups in parallel
    if (size < 96) {
        return bintob64_scalar(dest, src, size);
    }

    const uint8_t* in = (const uint8_t*)src;
    const uint8_t* end = in + size;
    char* out = dest;

    // Process 96 input bytes at a time (32 groups of 3 -> 128 output chars)
    // Larger chunks = better throughput
    while (end - in >= 96) {
        const uint8_t* in_ptr = in;
        char* out_ptr = out;
        
        // Process 32 groups in parallel with aggressive unrolling
        // This allows the CPU to execute multiple independent operations simultaneously
        for (int g = 0; g < 32; g += 8) {
            // Load and process 8 groups at once
            uint8_t a0 = in_ptr[0], b0 = in_ptr[1], c0 = in_ptr[2];
            uint8_t a1 = in_ptr[3], b1 = in_ptr[4], c1 = in_ptr[5];
            uint8_t a2 = in_ptr[6], b2 = in_ptr[7], c2 = in_ptr[8];
            uint8_t a3 = in_ptr[9], b3 = in_ptr[10], c3 = in_ptr[11];
            uint8_t a4 = in_ptr[12], b4 = in_ptr[13], c4 = in_ptr[14];
            uint8_t a5 = in_ptr[15], b5 = in_ptr[16], c5 = in_ptr[17];
            uint8_t a6 = in_ptr[18], b6 = in_ptr[19], c6 = in_ptr[20];
            uint8_t a7 = in_ptr[21], b7 = in_ptr[22], c7 = in_ptr[23];
            
            // Encode all 8 groups - compiler can parallelize these
            out_ptr[0] = B64_ENCODE_LUT[a0 >> 2];
            out_ptr[1] = B64_ENCODE_LUT[((a0 & 3) << 4) | (b0 >> 4)];
            out_ptr[2] = B64_ENCODE_LUT[((b0 & 15) << 2) | (c0 >> 6)];
            out_ptr[3] = B64_ENCODE_LUT[c0 & 63];
            
            out_ptr[4] = B64_ENCODE_LUT[a1 >> 2];
            out_ptr[5] = B64_ENCODE_LUT[((a1 & 3) << 4) | (b1 >> 4)];
            out_ptr[6] = B64_ENCODE_LUT[((b1 & 15) << 2) | (c1 >> 6)];
            out_ptr[7] = B64_ENCODE_LUT[c1 & 63];
            
            out_ptr[8] = B64_ENCODE_LUT[a2 >> 2];
            out_ptr[9] = B64_ENCODE_LUT[((a2 & 3) << 4) | (b2 >> 4)];
            out_ptr[10] = B64_ENCODE_LUT[((b2 & 15) << 2) | (c2 >> 6)];
            out_ptr[11] = B64_ENCODE_LUT[c2 & 63];
            
            out_ptr[12] = B64_ENCODE_LUT[a3 >> 2];
            out_ptr[13] = B64_ENCODE_LUT[((a3 & 3) << 4) | (b3 >> 4)];
            out_ptr[14] = B64_ENCODE_LUT[((b3 & 15) << 2) | (c3 >> 6)];
            out_ptr[15] = B64_ENCODE_LUT[c3 & 63];
            
            out_ptr[16] = B64_ENCODE_LUT[a4 >> 2];
            out_ptr[17] = B64_ENCODE_LUT[((a4 & 3) << 4) | (b4 >> 4)];
            out_ptr[18] = B64_ENCODE_LUT[((b4 & 15) << 2) | (c4 >> 6)];
            out_ptr[19] = B64_ENCODE_LUT[c4 & 63];
            
            out_ptr[20] = B64_ENCODE_LUT[a5 >> 2];
            out_ptr[21] = B64_ENCODE_LUT[((a5 & 3) << 4) | (b5 >> 4)];
            out_ptr[22] = B64_ENCODE_LUT[((b5 & 15) << 2) | (c5 >> 6)];
            out_ptr[23] = B64_ENCODE_LUT[c5 & 63];
            
            out_ptr[24] = B64_ENCODE_LUT[a6 >> 2];
            out_ptr[25] = B64_ENCODE_LUT[((a6 & 3) << 4) | (b6 >> 4)];
            out_ptr[26] = B64_ENCODE_LUT[((b6 & 15) << 2) | (c6 >> 6)];
            out_ptr[27] = B64_ENCODE_LUT[c6 & 63];
            
            out_ptr[28] = B64_ENCODE_LUT[a7 >> 2];
            out_ptr[29] = B64_ENCODE_LUT[((a7 & 3) << 4) | (b7 >> 4)];
            out_ptr[30] = B64_ENCODE_LUT[((b7 & 15) << 2) | (c7 >> 6)];
            out_ptr[31] = B64_ENCODE_LUT[c7 & 63];
            
            in_ptr += 24;
            out_ptr += 32;
        }
        
        in += 96;
        out += 128;
    }

    // Handle remainder with scalar
    if (end > in) {
        out = bintob64_scalar(out, in, end - in);
    }

    return out;
}

#else
// Stub implementations when NEON is not available
void* b64tobin_neon(void* restrict dest, const char* restrict src, size_t len) {
    return b64tobin_scalar(dest, src, len);
}

char* bintob64_neon(char* restrict dest, const void* restrict src, size_t size) {
    return bintob64_scalar(dest, src, size);
}
#endif