# distutils: language = c++
# cython: language_level=3

from libcpp.unordered_map cimport unordered_map
from libcpp.unordered_set cimport unordered_set
from libcpp.vector cimport vector
from libc.stdint cimport int64_t
from libcpp.pair cimport pair

cimport cython
cimport numpy as cnp

import numpy
import pyarrow

cdef class HashTable:
    cdef public unordered_map[int64_t, vector[int64_t]] hash_table

    def __init__(self):
        self.hash_table = unordered_map[int64_t, vector[int64_t]]()

    def insert(self, int64_t key, int64_t row_id) -> bool:
        # If the key is already in the hash table, append the row_id to the existing list.
        # Otherwise, create a new list with the row_id.
        if self.hash_table.find(key) != self.hash_table.end():
            self.hash_table[key].push_back(row_id)
            return False
        self.hash_table[key] = vector[int64_t](1, row_id)
        return True

    def get(self, int64_t key) -> list:
        # Return the list of row IDs for the given key, or an empty list if the key is not found.
        if self.hash_table.find(key) != self.hash_table.end():
            return self.hash_table[key]
        else:
            return vector[int64_t]()


cdef class HashSet:
    cdef unordered_set[int64_t] c_set

    def __cinit__(self):
        self.c_set = unordered_set[int64_t]()

    cdef inline void insert(self, int64_t value):
        self.c_set.insert(value)

    cdef inline bint contains(self, int64_t value):
        return self.c_set.find(value) != self.c_set.end()


@cython.boundscheck(False)
@cython.wraparound(False)
cdef inline object recast_column(column):
    cdef column_type = column.type

    if pyarrow.types.is_struct(column_type) or pyarrow.types.is_list(column_type):
        return numpy.array([str(a) for a in column.to_pylist()], dtype=numpy.str_)
    return column

@cython.boundscheck(False)
@cython.wraparound(False)
cpdef tuple distinct(table, HashSet seen_hashes=None, list columns=None):
    """
    Perform a distinct operation on the given table using an external SeenHashSet.
    """

    cdef int64_t hashed_value
    cdef int64_t i = 0
    cdef tuple value_tuple
    cdef object value

    if seen_hashes is None:
        seen_hashes = HashSet()

    if columns is None:
        columns_of_interest = table.column_names
    else:
        columns_of_interest = columns

    cdef list keep = []
    cdef cnp.ndarray values = numpy.array([recast_column(column) for column in table.select(columns_of_interest).itercolumns()], dtype=object)

    if len(columns_of_interest) > 1:
        for i in range(values.shape[1]):
            value_tuple = tuple([v if v == v else None for v in values[:, i]])
            hashed_value = hash(value_tuple)
            if not seen_hashes.contains(hashed_value):
                seen_hashes.insert(hashed_value)
                keep.append(i)
    else:
        for i, value in enumerate(values[0]):
            if value != value:
                hashed_value = hash(None)
            else:
                hashed_value = hash(value)
            if not seen_hashes.contains(hashed_value):
                seen_hashes.insert(hashed_value)
                keep.append(i)

    return (keep, seen_hashes)
