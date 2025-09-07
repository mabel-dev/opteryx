# distutils: language = c++
# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libcpp.unordered_map cimport unordered_map
from libcpp.unordered_set cimport unordered_set
from libcpp.vector cimport vector
from libc.stdint cimport int64_t, uint64_t
from cpython.object cimport PyObject_Hash

import numpy
cimport numpy
numpy.import_array()


cdef class HashTable:
    # Declared in .pxd:
    # cdef public unordered_map[uint64_t, vector[int64_t]] hash_table

    def __init__(self):
        self.hash_table = unordered_map[uint64_t, vector[int64_t]]()
        self.hash_table.reserve(1_048_576)  # try to prevent needing to resize

    cpdef bint insert(self, uint64_t key, int64_t row_id):
        # If the key is already in the hash table, append the row_id to the existing list.
        # Otherwise, create a new list with the row_id.
        self.hash_table[key].push_back(row_id)

    cpdef vector[int64_t] get(self, uint64_t key):
        # Return the list of row IDs for the given key, or an empty list if the key is not found.
        return self.hash_table[key]


cdef class HashSet:
    cdef unordered_set[uint64_t] c_set

    def __cinit__(self):
        self.c_set = unordered_set[uint64_t]()
        self.c_set.reserve(1_048_576)  # try to prevent needing to resize

    cdef inline bint insert(self, uint64_t value):
        cdef unsigned long size_before = self.c_set.size()
        self.c_set.insert(value)
        return self.c_set.size() > size_before

    cdef inline bint contains(self, uint64_t value):
        return self.c_set.find(value) != self.c_set.end()


cpdef tuple list_distinct(numpy.ndarray values, numpy.int64_t[::1] indices, HashSet seen_hashes=None):
    cdef:
        Py_ssize_t i, j = 0
        Py_ssize_t n = values.shape[0]
        uint64_t hash_value
        int64_t[::1] new_indices = numpy.empty(n, dtype=numpy.int64)
        numpy.dtype dtype = values.dtype
        numpy.ndarray new_values = numpy.empty(n, dtype=dtype)

    if seen_hashes is None:
        seen_hashes = HashSet()

    for i in range(n):
        v = values[i]
        hash_value = PyObject_Hash(v)
        if seen_hashes.insert(hash_value):
            new_values[j] = v
            new_indices[j] = indices[i]
            j += 1

    return new_values[:j], new_indices[:j], seen_hashes
