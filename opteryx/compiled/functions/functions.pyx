# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

import numpy
cimport numpy
numpy.import_array()

from libc.time cimport time

cdef bytes alphabet = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_/"

# Seed for xorshift32 PRNG
cdef unsigned int xorshift32_state = <unsigned int>time(NULL)


def generate_random_strings(int row_count, int width) -> numpy.ndarray:
    """
    Generates a NumPy array of random fixed-width strings, repeated `row_count` times.

    Parameters:
        row_count: int
            The number of random strings to generate.
        width: int
            The length of each random string.

    Returns:
        A NumPy array containing `row_count` random strings of fixed width.
    """

    # Allocate NumPy array with fixed-width strings, dtype='S{width}'
    cdef numpy.ndarray result = numpy.empty((row_count,), dtype=f'S{width}')

    cdef unsigned int total_chars = row_count * width
    cdef unsigned int i
    cdef unsigned char rand_value
    cdef char* ptr = <char*>result.data

    for i from 0 <= i < total_chars:
        rand_value = xorshift32() & 0x3F  # Random value limited to 64 (alphabet size)
        ptr[0] = alphabet[rand_value]
        ptr += 1

    return result


cdef inline unsigned int xorshift32():
    global xorshift32_state  # Declare as global to modify the module-level variable
    cdef unsigned int x = xorshift32_state
    x ^= (x << 13)
    x ^= (x >> 17)
    x ^= (x << 5)
    xorshift32_state = x
    return x
