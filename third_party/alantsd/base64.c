
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

static char digittobin(unsigned char index)
{
	if(122 < index || index < 43)
	{
		return 64;
	}
	return digittobin_map[index - 43];
}
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

/** Lookup table that converts a integer to base64 digit. */
static char const bintodigit[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                 "abcdefghijklmnopqrstuvwxyz"
                                 "0123456789"
                                 "+/";

/** Get the first base 64 digit of a block of 4.
  * @param a The first byte of the source block of 3.
  * @return A base 64 digit. */
static int get0( int a ) {
    int const index = a >> 2u;
    return bintodigit[ index ];
}

/** Get the second base 64 digit of a block of 4.
  * @param a The first byte of the source block of 3.
  * @param b The second byte of the source block of 3.
  * @return A base 64 digit. */
static int get1( int a, int b ) {
    int const indexA = ( a & 3 ) << 4u;
    int const indexB = b >> 4u;
    int const index  = indexA | indexB;
    return bintodigit[ index ];
}

/** Get the third base 64 digit of a block of 4.
  * @param b The second byte of the source block of 3.
  * @param c The third byte of the source block of 3.
  * @return A base 64 digit. */
static unsigned int get2( unsigned int b, unsigned int c ) {
    int const indexB = ( b & 15 ) << 2u;
    int const indexC = c >> 6u;
    int const index  = indexB | indexC;
    return bintodigit[ index ];
}

/** Get the fourth base 64 digit of a block of 4.
  * @param c The third byte of the source block of 3.
  * @return A base 64 digit. */
static int get3( int c ) {
    int const index = c & 0x3f;
    return bintodigit[ index ];
}

/* Convert a binary memory block in a base64 null-terminated string. */
char* bintob64( char* dest, void const* src, size_t size ) {
    typedef struct { unsigned char a; unsigned char b; unsigned char c; } block_t;
    block_t const* block = (block_t*)src;
    for( ; size >= sizeof( block_t ); size -= sizeof( block_t ), ++block ) {
        *dest++ = get0( block->a );
        *dest++ = get1( block->a, block->b );
        *dest++ = get2( block->b, block->c );
        *dest++ = get3( block->c );
    }

    if ( !size ) goto final;

    *dest++ = get0( block->a );
    if ( !--size ) {
        *dest++ = get1( block->a, 0 );
        *dest++ = '=';
        *dest++ = '=';
        goto final;
    }

    *dest++ = get1( block->a, block->b );
    *dest++ = get2( block->b, 0 );
    *dest++ = '=';

  final:
    *dest = '\0';
    return dest;
}