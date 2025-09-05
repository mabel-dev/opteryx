
/*
<https://github.com/alantsd/base64>

  Licensed under the MIT License <http://opensource.org/licenses/MIT>.
  SPDX-License-Identifier: MIT
  Copyright (c) 2016-2018 Rafa Garcia <rafagarcia77@gmail.com>.
  Copyright (c) 2018-2020 Alan Tong <alantsd@hotmail.com>.
  Copyright (c) 2025 Justin Joyce <justin.joyce@joocer.com>.

  Permission is hereby  granted, free of charge, to any  person obtaining a copy
  of this software and associated  documentation files (the "Software"), to deal
  in the Software  without restriction, including without  limitation the rights
  to  use, copy,  modify, merge,  publish, distribute,  sublicense, and/or  sell
  copies  of  the Software,  and  to  permit persons  to  whom  the Software  is
  furnished to do so, subject to the following conditions:
  The above copyright notice and this permission notice shall be included in all
  copies or substantial portions of the Software.
  THE SOFTWARE  IS PROVIDED "AS  IS", WITHOUT WARRANTY  OF ANY KIND,  EXPRESS OR
  IMPLIED,  INCLUDING BUT  NOT  LIMITED TO  THE  WARRANTIES OF  MERCHANTABILITY,
  FITNESS FOR  A PARTICULAR PURPOSE AND  NONINFRINGEMENT. IN NO EVENT  SHALL THE
  AUTHORS  OR COPYRIGHT  HOLDERS  BE  LIABLE FOR  ANY  CLAIM,  DAMAGES OR  OTHER
  LIABILITY, WHETHER IN AN ACTION OF  CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  OUT OF OR IN CONNECTION WITH THE SOFTWARE  OR THE USE OR OTHER DEALINGS IN THE
  SOFTWARE.

*/

#include "base64.h"

/** Escape values. */
enum special_e {
    notabase64 = 64, /**< Value to return when a non base64 digit is found. */
    terminator = 65, /**< Value to return when the character '=' is found.  */
};
static unsigned char digittobin_map[256] = { [0 ... 255] = 64 };

__attribute__((constructor))  // GCC/Clang only
static void init_base64_map(void) {
    for (int i = 'A'; i <= 'Z'; ++i) digittobin_map[i] = i - 'A';
    for (int i = 'a'; i <= 'z'; ++i) digittobin_map[i] = i - 'a' + 26;
    for (int i = '0'; i <= '9'; ++i) digittobin_map[i] = i - '0' + 52;
    digittobin_map['+'] = 62;
    digittobin_map['/'] = 63;
    digittobin_map['='] = 65;
}
#define DIGIT(x) digittobin_map[(unsigned char)(x)]

/* Convert a base64 null-terminated string to binary format.*/
void* b64tobin(void* restrict dest, char const* restrict src) {
    unsigned char* out = dest;
    unsigned char const* in = (unsigned char const*)src;

    for (;;) {
        unsigned char a = DIGIT(in[0]);
        unsigned char b = DIGIT(in[1]);
        unsigned char c = DIGIT(in[2]);
        unsigned char d = DIGIT(in[3]);

        if (a > 63 || b > 63) break;

        *out++ = (a << 2) | (b >> 4);

        if (c == 65) break; // padding
        if (c > 63) return NULL;
        *out++ = (b << 4) | (c >> 2);

        if (d == 65) break; // padding
        if (d > 63) return NULL;
        *out++ = (c << 6) | d;

        in += 4;
    }

    return out;
}

// when we know the length of the string we can avoid looking for NULL terminator
void* b64tobin_len(void* restrict dest, const char* restrict src, size_t len) {
    unsigned char* out = dest;
    const unsigned char* in = (const unsigned char*)src;

    if (len % 4 != 0) return NULL;  // base64 must be multiple of 4

    size_t i = 0;
    while (i < len) {
        unsigned char a = DIGIT(in[i]);
        unsigned char b = DIGIT(in[i + 1]);
        unsigned char c = DIGIT(in[i + 2]);
        unsigned char d = DIGIT(in[i + 3]);

        if ((a | b) > 63) break;

        *out++ = (a << 2) | (b >> 4);

        if (c == terminator) break;
        if (c > 63) return NULL;

        *out++ = (b << 4) | (c >> 2);

        if (d == terminator) break;
        if (d > 63) return NULL;

        *out++ = (c << 6) | d;
        i += 4;
    }

    return out;
}


/** Lookup table that converts a integer to base64 digit. */
static char const bintodigit[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                 "abcdefghijklmnopqrstuvwxyz"
                                 "0123456789"
                                 "+/";

/* Convert a binary memory block in a base64 null-terminated string. */
char* bintob64( char* dest, void const* src, size_t size ) {
    const unsigned char* in = (const unsigned char*)src;
    size_t i = 0;

    while (i + 3 <= size) {
        unsigned char a = in[i++];
        unsigned char b = in[i++];
        unsigned char c = in[i++];

        *dest++ = bintodigit[a >> 2];
        *dest++ = bintodigit[((a & 0x03) << 4) | (b >> 4)];
        *dest++ = bintodigit[((b & 0x0F) << 2) | (c >> 6)];
        *dest++ = bintodigit[c & 0x3F];
    }

    if (size - i == 1) {
        unsigned char a = in[i++];
        *dest++ = bintodigit[a >> 2];
        *dest++ = bintodigit[(a & 0x03) << 4];
        *dest++ = '=';
        *dest++ = '=';
    } else if (size - i == 2) {
        unsigned char a = in[i++];
        unsigned char b = in[i++];
        *dest++ = bintodigit[a >> 2];
        *dest++ = bintodigit[((a & 0x03) << 4) | (b >> 4)];
        *dest++ = bintodigit[(b & 0x0F) << 2];
        *dest++ = '=';
    }

    *dest = '\0';
    return dest;
}