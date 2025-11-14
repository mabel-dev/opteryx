# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: nonecheck=False
# cython: overflowcheck=False
# cython: cdivision=True

from libc.stdint cimport uint64_t
from libc.stddef cimport size_t
from cpython.bytes cimport PyBytes_AsStringAndSize

# Import xxHash function signatures from `xxhash.h`
cdef extern from "xxhash.h":
    uint64_t XXH3_64bits(const void* input, size_t length) nogil

cdef inline uint64_t cy_xxhash3_64(const void *key, size_t len) except? 0 nogil:
    return XXH3_64bits(key, len)

cpdef uint64_t hash_bytes(bytes key):
    """ Python-accessible function for hashing bytes. """
    cdef char* data
    cdef Py_ssize_t length

    if PyBytes_AsStringAndSize(key, &data, &length) != 0:
        raise ValueError("Invalid byte string")

    return cy_xxhash3_64(data, length)
