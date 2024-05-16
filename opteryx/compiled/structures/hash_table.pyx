# distutils: language = c++
# cython: language_level=3

from libcpp.unordered_map cimport unordered_map
from libcpp.unordered_set cimport unordered_set
from libcpp.vector cimport vector
from libc.stdint cimport int64_t
from libcpp.pair cimport pair

cimport cython

import numpy
import pyarrow

cdef class HashTable:
    cdef unordered_map[int64_t, vector[int64_t]] hash_table

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

    def to_jagged_array(self):
        # Initialize an empty Python list to hold the lists of row IDs
        cdef list jagged_array = []

        # Iterate over the hash table
        cdef pair[int64_t, vector[int64_t]] item
        for item in self.hash_table:
            # Convert each vector<int64_t> to a Python list and append it to the jagged_array
            jagged_array.append([row_id for row_id in item.second])

        return jagged_array


cdef class HashSet:
    cdef unordered_set[int64_t] c_set

    def __cinit__(self):
        self.c_set = unordered_set[int64_t]()

    def insert(self, int64_t value):
        self.c_set.insert(value)

    def contains(self, int64_t value):
        return self.c_set.find(value) != self.c_set.end()


@cython.boundscheck(False)
@cython.wraparound(False)
def recast_column(column):
    cdef:
        Py_ssize_t i, n
        list result_list = []

    if pyarrow.types.is_struct(column.type) or pyarrow.types.is_list(column.type):
        n = len(column)
        # Pre-allocate the list for performance
        result_list = [None] * n
        for i in range(n):
            # Directly convert each element to a Python string
            result_list[i] = str(column[i].as_py())
        return numpy.array(result_list, dtype=numpy.str_)
    else:
        # Use PyArrow's to_numpy() for efficient conversion for other column types
        return column.to_numpy()

cpdef distinct(table, HashSet seen_hashes=None, list columns=None, bint return_seen_hashes=False):
    """
    Perform a distinct operation on the given table using an external SeenHashSet.
    """
    if seen_hashes is None:
        seen_hashes = HashSet()

    if columns is None:
        columns_of_interest = table.column_names
    else:
        columns_of_interest = columns

    cdef list keep = []
    values = [recast_column(c) for c in table.select(columns_of_interest).itercolumns()]

    cdef int64_t hashed_value
    cdef int i = 0

    if len(columns_of_interest) > 1:
        for value_tuple in zip(*values):
            hashed_value = hash(value_tuple)
            if not seen_hashes.contains(hashed_value):
                seen_hashes.insert(hashed_value)
                keep.append(i)
            i += 1
    else:
        for value_tuple in values[0]:
            if value_tuple != value_tuple:
                hashed_value = -14556480 # Apollo 11 Launch, Unix Epoch
            else:
                hashed_value = hash(value_tuple)
            if not seen_hashes.contains(hashed_value):
                seen_hashes.insert(hashed_value)
                keep.append(i)
            i += 1

    if len(keep) > 0:
        distinct_table = table.take(keep)
    else:
        distinct_table = table.slice(0, 0)

    if return_seen_hashes:
        return distinct_table, seen_hashes
    else:
        return distinct_table
