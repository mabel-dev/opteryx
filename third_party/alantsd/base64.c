#include "base64.h"
#include <string.h>

// Lookup tables - all entries initialized to 64 (invalid marker) except valid base64 chars
const uint8_t B64_DECODE_LUT[256] = {
    [0 ... 255] = 64,  // Initialize all to invalid (NOT_BASE64)
    ['A'] = 0, ['B'] = 1, ['C'] = 2, ['D'] = 3, ['E'] = 4, ['F'] = 5, ['G'] = 6,
    ['H'] = 7, ['I'] = 8, ['J'] = 9, ['K'] = 10, ['L'] = 11, ['M'] = 12, ['N'] = 13,
    ['O'] = 14, ['P'] = 15, ['Q'] = 16, ['R'] = 17, ['S'] = 18, ['T'] = 19, ['U'] = 20,
    ['V'] = 21, ['W'] = 22, ['X'] = 23, ['Y'] = 24, ['Z'] = 25,
    ['a'] = 26, ['b'] = 27, ['c'] = 28, ['d'] = 29, ['e'] = 30, ['f'] = 31, ['g'] = 32,
    ['h'] = 33, ['i'] = 34, ['j'] = 35, ['k'] = 36, ['l'] = 37, ['m'] = 38, ['n'] = 39,
    ['o'] = 40, ['p'] = 41, ['q'] = 42, ['r'] = 43, ['s'] = 44, ['t'] = 45, ['u'] = 46,
    ['v'] = 47, ['w'] = 48, ['x'] = 49, ['y'] = 50, ['z'] = 51,
    ['0'] = 52, ['1'] = 53, ['2'] = 54, ['3'] = 55, ['4'] = 56, ['5'] = 57, ['6'] = 58,
    ['7'] = 59, ['8'] = 60, ['9'] = 61,
    ['+'] = 62, ['/'] = 63,
    ['='] = 65  // PADDING marker
};

const char B64_ENCODE_LUT[64] = 
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz"
    "0123456789"
    "+/";

#define DIGIT(x) B64_DECODE_LUT[(uint8_t)(x)]
#define NOT_BASE64 64
#define PADDING 65

// Utility functions
size_t b64_encoded_size(size_t bin_size) {
    return ((bin_size + 2) / 3) * 4 + 1;
}

size_t b64_decoded_size(size_t b64_len) {
    return (b64_len * 3) / 4;
}

// Scalar implementation
void* b64tobin_scalar(void* restrict dest, const char* restrict src, size_t len) {
    if (len == 0) return dest;
    if (len % 4 != 0) return NULL;

    uint8_t* out = (uint8_t*)dest;
    const uint8_t* in = (const uint8_t*)src;
    const uint8_t* end = in + len;

    while (end - in >= 4) {
        uint8_t a = DIGIT(in[0]);
        uint8_t b = DIGIT(in[1]);
        uint8_t c = DIGIT(in[2]);
        uint8_t d = DIGIT(in[3]);

        if ((a | b) > 63) return NULL;

        *out++ = (a << 2) | (b >> 4);

        if (c == PADDING) break;
        if (c > 63) return NULL;
        *out++ = (b << 4) | (c >> 2);

        if (d == PADDING) break;
        if (d > 63) return NULL;
        *out++ = (c << 6) | d;

        in += 4;
    }

    return out;
}

char* bintob64_scalar(char* restrict dest, const void* restrict src, size_t size) {
    const uint8_t* in = (const uint8_t*)src;
    size_t i = 0;

    while (i + 3 <= size) {
        uint8_t a = in[i++];
        uint8_t b = in[i++];
        uint8_t c = in[i++];

        *dest++ = B64_ENCODE_LUT[a >> 2];
        *dest++ = B64_ENCODE_LUT[((a & 0x03) << 4) | (b >> 4)];
        *dest++ = B64_ENCODE_LUT[((b & 0x0F) << 2) | (c >> 6)];
        *dest++ = B64_ENCODE_LUT[c & 0x3F];
    }

    if (size - i == 1) {
        uint8_t a = in[i++];
        *dest++ = B64_ENCODE_LUT[a >> 2];
        *dest++ = B64_ENCODE_LUT[(a & 0x03) << 4];
        *dest++ = '=';
        *dest++ = '=';
    } else if (size - i == 2) {
        uint8_t a = in[i++];
        uint8_t b = in[i++];
        *dest++ = B64_ENCODE_LUT[a >> 2];
        *dest++ = B64_ENCODE_LUT[((a & 0x03) << 4) | (b >> 4)];
        *dest++ = B64_ENCODE_LUT[(b & 0x0F) << 2];
        *dest++ = '=';
    }

    *dest = '\0';
    return dest;
}

// Scalar implementations are now only used internally by auto-dispatch
// The b64tobin, b64tobin_len, and bintob64 functions are defined in base64_dispatch.c