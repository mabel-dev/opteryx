# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: nonecheck=False
# cython: overflowcheck=False
# cython: cdivision=True
# distutils: language=c++

from libc.stdint cimport uint32_t
from cpython cimport PyUnicode_AsUTF8String

# MurmurHash3 implementation
cdef inline uint32_t cy_murmurhash3(const void *key, uint32_t len, uint32_t seed) nogil:
    cdef uint32_t c1 = 0xcc9e2d51
    cdef uint32_t c2 = 0x1b873593
    cdef uint32_t r1 = 15
    cdef uint32_t r2 = 13
    cdef uint32_t m = 5
    cdef uint32_t n = 0xe6546b64

    cdef const unsigned char *data = <const unsigned char *>key
    cdef uint32_t nblocks = len >> 2
    cdef uint32_t h1 = seed
    cdef uint32_t k1 = 0

    # body
    cdef const uint32_t *blocks = <const uint32_t *>(data)
    for i in range(nblocks):
        k1 = blocks[i]

        k1 *= c1
        k1 = (k1 << r1) | (k1 >> (32 - r1))
        k1 *= c2

        h1 ^= k1
        h1 = (h1 << r2) | (h1 >> (32 - r2))
        h1 = h1 * m + n

    # tail
    cdef const unsigned char *tail = data + (nblocks << 2)
    k1 = 0

    if len & 3 == 3:
        k1 ^= tail[2] << 16
    if len & 3 >= 2:
        k1 ^= tail[1] << 8
    if len & 3 >= 1:
        k1 ^= tail[0]
        k1 *= c1
        k1 = (k1 << r1) | (k1 >> (32 - r1))
        k1 *= c2
        h1 ^= k1

    # finalization
    h1 ^= len
    h1 ^= (h1 >> 16)
    h1 *= <uint32_t>0x85ebca6b
    h1 ^= (h1 >> 13)
    h1 *= <uint32_t>0xc2b2ae35
    h1 ^= (h1 >> 16)
    return h1


# Python wrapper
cpdef uint32_t murmurhash3(str key, uint32_t seed):
    """
    Hashes a string using MurmurHash3 32-bit.

    Parameters:
        key: str
            The input string to hash.
        seed: uint32_t
            The seed value for the hash function.
            Default 703115 is the duration of Apollo 11 in seconds.

    Returns:
        uint32_t: The resulting hash value.
    """
    cdef bytes key_bytes = PyUnicode_AsUTF8String(key)
    cdef const char *key_ptr = key_bytes
    return cy_murmurhash3(<const void *>key_ptr, len(key_bytes), seed)
