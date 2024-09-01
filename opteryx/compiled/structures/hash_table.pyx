# distutils: language = c++
# cython: language_level=3

from libcpp.unordered_map cimport unordered_map
from libcpp.unordered_set cimport unordered_set
from libcpp.vector cimport vector
from libc.stdint cimport int64_t, uint8_t
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
        return vector[int64_t]()


cdef class HashSet:
    cdef unordered_set[int64_t] c_set

    def __cinit__(self):
        self.c_set = unordered_set[int64_t]()
        self.c_set.reserve(1_048_576)  # try to prevent needing to resize

    cdef inline bint insert(self, int64_t value):
        if self.c_set.find(value) != self.c_set.end():
            return False
        self.c_set.insert(value)
        return True

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
    cdef int64_t null_hash = hash(None)
    cdef int64_t i = 0
    cdef tuple value_tuple
    cdef object value
    cdef int64_t num_columns

    if seen_hashes is None:
        seen_hashes = HashSet()

    if columns is None:
        columns_of_interest = table.column_names
    else:
        columns_of_interest = columns
    num_columns = len(columns_of_interest)

    cdef list keep = []
    cdef cnp.ndarray values = numpy.array([recast_column(column) for column in table.select(columns_of_interest).itercolumns()], dtype=object)

    if num_columns > 1:
        for i in range(values.shape[1]):
            value_tuple = tuple([v if v == v else None for v in [values[j, i] for j in range(num_columns)]])
            hashed_value = hash(value_tuple)
            if seen_hashes.insert(hashed_value):
                keep.append(i)
    else:
        for i, value in enumerate(values[0]):
            if value != value:
                hashed_value = null_hash
            else:
                hashed_value = hash(value)
            if seen_hashes.insert(hashed_value):
                keep.append(i)

    return (keep, seen_hashes)

@cython.boundscheck(False)
@cython.wraparound(False)
cpdef tuple list_distinct(list values, cnp.ndarray indices, HashSet seen_hashes=None):
    new_indices = []
    new_values = []
    for i, v in enumerate(values):
        if seen_hashes.insert(hash(v)):
            new_values.append(v)
            new_indices.append(indices[i])
    return new_values, new_indices, seen_hashes

@cython.boundscheck(False)
@cython.wraparound(False)
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
    cdef cnp.ndarray[uint8_t, ndim=1] bitmap_array

    # Selecting columns
    cdef int64_t num_rows = relation.num_rows
    cdef int64_t num_columns = len(join_columns)

    # Allocate memory for the combined nulls array
    cdef cnp.ndarray[uint8_t, ndim=1] combined_nulls = numpy.full(num_rows, 1, dtype=numpy.uint8)

    # Process each column to update the combined null bitmap
    cdef int64_t i, col_index
    cdef str column_name
    cdef object column, bitmap_buffer
    cdef uint8_t bit, byte

    for column_name in join_columns:
        column = relation.column(column_name)
        if column.null_count > 0:
            # Get the null bitmap for the current column, ensure it's in a single chunk first
            bitmap_buffer = column.combine_chunks().buffers()[0]
            if bitmap_buffer is not None:
                # Convert the bitmap to uint8
                bitmap_array = numpy.frombuffer(bitmap_buffer, dtype=numpy.uint8)
                for i in range(num_rows):
                    byte = bitmap_array[i // 8]
                    bit = 1 if byte & (1 << (i % 8)) else 0
                    combined_nulls[i] &= bit

    # Determine row indices that have nulls in any of the considered columns
    cdef cnp.ndarray non_null_indices = numpy.nonzero(combined_nulls)[0]

    # Convert selected columns to a numpy array of object dtype, skipping null cols
    cdef cnp.ndarray values_array = numpy.array([relation.column(column).take(non_null_indices) for column in join_columns], dtype=object)
    cdef int64_t hash_value
    cdef tuple value_tuple

    if num_columns > 1:
        for i in range(values_array.shape[1]):
            # Create a tuple of values across the columns for the current row
            value_tuple = tuple(values_array[:, i])
            hash_value = <int64_t>hash(value_tuple)
            ht.insert(hash_value, non_null_indices[i])
    else:
        for i, value in enumerate(values_array[0]):
            hash_value = <int64_t>hash(value)
            ht.insert(hash_value, non_null_indices[i])

    return ht