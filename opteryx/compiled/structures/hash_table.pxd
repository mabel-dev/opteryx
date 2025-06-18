# distutils: language = c++
# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libcpp.unordered_map cimport unordered_map
from libcpp.vector cimport vector
from libc.stdint cimport int64_t


cdef class HashTable:
    cdef public unordered_map[int64_t, vector[int64_t]] hash_table

    cpdef bint insert(self, int64_t key, int64_t row_id)
    cpdef vector[int64_t] get(self, int64_t key)
