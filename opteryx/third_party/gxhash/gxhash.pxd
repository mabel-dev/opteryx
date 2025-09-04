# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport uint32_t
from libc.stddef cimport size_t

cdef extern from "gxhash.h":
    uint32_t gx_hash_32(const void* data, size_t length)

cdef inline uint32_t gxhash_32_c(const void* data, size_t length):
    return gx_hash_32(data, length)