# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport uint32_t, uint64_t, int64_t
cimport numpy as cnp

# Declaration of the BloomFilter class
cdef class BloomFilter:
    cdef unsigned char* bit_array
    cdef uint32_t bit_array_size
    cdef uint32_t byte_array_size

    cpdef void add(self, bytes member)
    cdef inline void _add(self, bytes member)
    cpdef bint possibly_contains(self, bytes member)
    cdef inline bint _possibly_contains(self, bytes member)
    cpdef cnp.ndarray[cnp.npy_bool, ndim=1] possibly_contains_many(self, cnp.ndarray keys)
    cpdef memoryview serialize(self)

    cpdef cnp.ndarray[cnp.npy_bool, ndim=1] possibly_contains_many_ints(self, cnp.ndarray[cnp.int64_t] keys)

cpdef BloomFilter deserialize(const unsigned char* data)
cpdef BloomFilter create_bloom_filter(keys)
cpdef BloomFilter create_int_bloom_filter(cnp.ndarray[cnp.int64_t] keys)
