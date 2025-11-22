# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stddef cimport size_t
from libc.stdint cimport uint64_t

from opteryx.draken.interop.arrow import vector_from_arrow

cdef const uint64_t MIX_HASH_CONSTANT
cdef const uint64_t NULL_HASH

cdef extern from "simd_hash.h":
    void simd_mix_hash(uint64_t* dest, const uint64_t* values, size_t count) nogil

cdef inline uint64_t mix_hash(uint64_t current, uint64_t value) nogil:
    cdef uint64_t mixed = current ^ value
    mixed = mixed * MIX_HASH_CONSTANT + 1
    return mixed ^ (mixed >> 32)

cdef class Vector:
    cdef bint here
    cpdef object null_bitmap(self)
    cdef void hash_into(self, uint64_t[::1] out_buf, Py_ssize_t offset=*) except *
