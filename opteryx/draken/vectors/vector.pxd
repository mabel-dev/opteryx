# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport uint64_t

from opteryx.draken.interop.arrow import vector_from_arrow

cdef const uint64_t NULL_HASH

cdef inline uint64_t mix_hash(uint64_t current, uint64_t value, uint64_t mix_constant):
    current ^= value
    current *= mix_constant
    return current ^ (current >> 32)

cdef class Vector:
    cdef bint here
    cpdef object null_bitmap(self)
    cpdef void hash_into(self, uint64_t[::1] out_buf, Py_ssize_t offset=*, uint64_t mix_constant=*)
