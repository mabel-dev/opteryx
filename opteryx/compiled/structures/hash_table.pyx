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
from libc.stdint cimport int64_t, uint8_t
from cpython.object cimport PyObject_Hash

cimport numpy as cnp

import numpy


cdef class HashTable:
    cdef public unordered_map[int64_t, vector[int64_t]] hash_table

    def __init__(self):
        self.hash_table = unordered_map[int64_t, vector[int64_t]]()
        self.hash_table.reserve(1_048_576)  # try to prevent needing to resize

    cpdef bint insert(self, int64_t key, int64_t row_id):
        # If the key is already in the hash table, append the row_id to the existing list.
        # Otherwise, create a new list with the row_id.
        self.hash_table[key].push_back(row_id)

    cpdef vector[int64_t] get(self, int64_t key):
        # Return the list of row IDs for the given key, or an empty list if the key is not found.
        return self.hash_table[key]


cdef class HashSet:
    cdef unordered_set[int64_t] c_set

    def __cinit__(self):
        self.c_set = unordered_set[int64_t]()
        self.c_set.reserve(1_048_576)  # try to prevent needing to resize

    cdef inline bint insert(self, int64_t value):
        cdef unsigned long size_before = self.c_set.size()
        self.c_set.insert(value)
        return self.c_set.size() > size_before

    cdef inline bint contains(self, int64_t value):
        return self.c_set.find(value) != self.c_set.end()


cpdef tuple list_distinct(cnp.ndarray values, cnp.int64_t[::1] indices, HashSet seen_hashes=None):
    cdef:
        Py_ssize_t i, j = 0
        Py_ssize_t n = values.shape[0]
        int64_t hash_value
        int64_t[::1] new_indices = numpy.empty(n, dtype=numpy.int64)
        cnp.dtype dtype = values.dtype
        cnp.ndarray new_values = numpy.empty(n, dtype=dtype)

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


cpdef HashTable hash_join_map(relation, list join_columns):
    """
    Build a hash table for the join operations.

    Parameters:
        relation: The pyarrow.Table to preprocess.
        join_columns: A list of column names to join on.

    Returns:
        A HashTable where keys are hashes of the join column entries and
        values are lists of row indices corresponding to each hash key.
    """
    cdef HashTable ht = HashTable()

    # Get the dimensions of the dataset we're working with
    cdef int64_t num_rows = relation.num_rows
    cdef int64_t num_columns = len(join_columns)

    # Memory view for combined nulls (used to check for nulls in any column)
    cdef uint8_t[:,] combined_nulls = numpy.full(num_rows, 1, dtype=numpy.uint8)

    # Process each column to update the combined null bitmap
    cdef int64_t i
    cdef uint8_t bit, byte
    cdef uint8_t[::1] bitmap_array

    for column_name in join_columns:
        column = relation.column(column_name)

        if column.null_count > 0:
            # Get the null bitmap for the current column
            bitmap_buffer = column.combine_chunks().buffers()[0]

            if bitmap_buffer is not None:
                # Memory view for the bitmap array
                bitmap_array = numpy.frombuffer(bitmap_buffer, dtype=numpy.uint8)

                # Apply bitwise operations on the bitmap
                for i in range(num_rows):
                    byte = bitmap_array[i // 8]
                    bit = (byte >> (i % 8)) & 1
                    combined_nulls[i] &= bit

    # Get non-null indices using memory views
    cdef cnp.ndarray non_null_indices = numpy.nonzero(combined_nulls)[0]

    # Memory view for the values array (for the join columns)
    cdef object[:, ::1] values_array = numpy.array(list(relation.take(non_null_indices).select(join_columns).itercolumns()), dtype=object)

    cdef int64_t hash_value

    if num_columns == 1:
        col = values_array[0, :]
        for i in range(len(col)):
            hash_value = PyObject_Hash(col[i])
            ht.insert(hash_value, non_null_indices[i])
    else:
        for i in range(values_array.shape[1]):
            # Combine the hashes of each value in the row
            hash_value = 0
            for value in values_array[:, i]:
                hash_value = <int64_t>(hash_value * 31 + PyObject_Hash(value))
            ht.insert(hash_value, non_null_indices[i])

    return ht
