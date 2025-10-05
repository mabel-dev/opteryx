# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: nonecheck=False
# cython: overflowcheck=False
# cython: lintrule=ignore

from libc.stdint cimport uint64_t
from libc.stddef cimport size_t

cdef uint64_t cy_xxhash3_64(const void *key, size_t len) except? 0 nogil
cpdef uint64_t hash_bytes(bytes key)
