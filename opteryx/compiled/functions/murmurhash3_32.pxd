# cython: language_level=3

from libc.stdint cimport uint32_t


cdef uint32_t cy_murmurhash3(const void *key, uint32_t len, uint32_t seed) nogil
