# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport uint8_t, uint32_t, uint64_t, int64_t
cimport numpy

# Declaration of the BloomFilter class
cdef class BloomFilter:
    cdef unsigned char* bit_array
    cdef uint32_t bit_array_size
    cdef uint32_t byte_array_size

    cpdef void add(self, bytes member)
    cdef inline void _add(self, const void *member, size_t length)
    cpdef bint possibly_contains(self, bytes member)
    cdef inline bint _possibly_contains(self, const void *member, size_t length)
    cpdef numpy.ndarray[numpy.npy_bool, ndim=1] possibly_contains_many(self, keys)

cpdef BloomFilter create_bloom_filter(keys)
