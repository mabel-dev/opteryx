# distutils: language = c++
# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=True
# cython: boundscheck=False

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
        self.hash_table.reserve(1_048_576)  # try to prevent needing to resize

    cpdef bint insert(self, int64_t key, int64_t row_id):
        # If the key is already in the hash table, append the row_id to the existing list.
        # Otherwise, create a new list with the row_id.
        if self.hash_table.find(key) != self.hash_table.end():
            self.hash_table[key].push_back(row_id)
            return False
        self.hash_table[key] = vector[int64_t](1, row_id)
        return True

    cpdef vector[int64_t] get(self, int64_t key):
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

@cython.wraparound(False)
cdef inline object recast_column(column):
    cdef column_type = column.type

    if pyarrow.types.is_struct(column_type) or pyarrow.types.is_list(column_type):
        return numpy.array([str(a) for a in column], dtype=numpy.str_)

    # Otherwise, return the column as-is
    return column

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


@cython.wraparound(False)
cpdef tuple list_distinct(cnp.ndarray values, cnp.ndarray indices, HashSet seen_hashes=None):
    cdef cnp.ndarray[object] new_values = numpy.empty(values.shape[0], dtype=object)
    cdef cnp.ndarray[int64_t] new_indices = numpy.empty(indices.shape[0], dtype=numpy.int64)

    cdef int i, j = 0
    cdef object v
    cdef int64_t hash_value

    if seen_hashes is None:
        seen_hashes = HashSet()

    for i in range(values.shape[0]):
        v = values[i]
        hash_value = <int64_t>hash(v)
        if seen_hashes.insert(hash_value):
            new_values[j] = v
            new_indices[j] = indices[i]
            j += 1
    return new_values[:j], new_indices[:j], seen_hashes

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
            hash_value = <int64_t>hash(col[i])
            ht.insert(hash_value, non_null_indices[i])
    else:
        for i in range(values_array.shape[1]):
            # Combine the hashes of each value in the row
            hash_value = 0
            for value in values_array[:, i]:
                hash_value = <int64_t>(hash_value * 31 + hash(value))
            ht.insert(hash_value, non_null_indices[i])

    return ht


"""
Below here is an incomplete attempt at rewriting the hash table builder to be faster.

The key points to make it faster are:
- specialized hashes for different column types
- more C native structures, relying on less Python

This is competitive but doesn't outright beat the above version and currently doesn't pass all of the tests
"""


import cython
import numpy as np
import pyarrow
from libc.stdint cimport int64_t
from libc.stdlib cimport malloc, free

@cython.boundscheck(False)
@cython.wraparound(False)
cpdef HashTable _hash_join_map(relation, list join_columns):
    """
    Build a hash table for join operations using column-based hashing.
    Each column is hashed separately, and the results are combined efficiently.

    Parameters:
        relation: The pyarrow.Table to preprocess.
        join_columns: A list of column names to join on.

    Returns:
        A HashTable where keys are combined hashes of the join column entries and
        values are lists of row indices corresponding to each hash key.
    """
    cdef HashTable ht = HashTable()
    cdef int64_t num_rows = relation.num_rows
    cdef int64_t num_columns = len(join_columns)

    # Create an array to store column hashes
    cdef int64_t* cell_hashes = <int64_t*>malloc(num_rows * num_columns * sizeof(int64_t))
    if cell_hashes is NULL:
        raise Exception("Unable to allocate memory")

    # Allocate memory for the combined nulls array
    cdef cnp.ndarray[uint8_t, ndim=1] combined_nulls = numpy.full(num_rows, 1, dtype=numpy.uint8)

    # Process each column to update the combined null bitmap
    cdef int64_t i, j, combined_hash
    cdef object column, bitmap_buffer
    cdef uint8_t bit, byte

    for column_name in join_columns:
        column = relation.column(column_name)

        if column.null_count > 0:
            combined_column = column.combine_chunks()
            bitmap_buffer = combined_column.buffers()[0]  # Get the null bitmap buffer

            if bitmap_buffer is not None:
                bitmap_array = np.frombuffer(bitmap_buffer, dtype=np.uint8)

                for i in range(num_rows):
                    byte = bitmap_array[i // 8]
                    bit = (byte >> (i % 8)) & 1
                    combined_nulls[i] &= bit

    # Determine row indices that have no nulls in any considered column
    cdef cnp.ndarray non_null_indices = numpy.nonzero(combined_nulls)[0]

    # Process each column by type
    for j, column_name in enumerate(join_columns):
        column = relation.column(column_name)

        # Handle different PyArrow types
        if pyarrow.types.is_string(column.type):  # String column
            for i in non_null_indices:
                cell_hashes[j * num_rows + i] = hash(column[i].as_buffer().to_pybytes())  # Hash string
        elif pyarrow.types.is_integer(column.type) or pyarrow.types.is_floating(column.type):
            # Access the data buffer directly as a NumPy array
            np_column = numpy.frombuffer(column.combine_chunks().buffers()[1], dtype=np.int64)
            for i in non_null_indices:
                cell_hashes[j * num_rows + i] = np_column[i]  # Directly store as int64
        elif pyarrow.types.is_boolean(column.type):
            bitmap_buffer = column.buffers()[1]  # Boolean values are stored in bitmap
            bitmap_ptr = <uint8_t*>bitmap_buffer.address  # Access the bitmap buffer
            for i in non_null_indices:
                byte_idx = i // 8
                bit_idx = i % 8
                bit_value = (bitmap_ptr[byte_idx] >> bit_idx) & 1
                cell_hashes[j * num_rows + i] = bit_value  # Convert to int64 (True -> 1, False -> 0)
        elif pyarrow.types.is_date(column.type) or pyarrow.types.is_timestamp(column.type):
            np_column = numpy.frombuffer(column.combine_chunks().buffers()[1], dtype=np.int64)
            for i in non_null_indices:
                cell_hashes[j * num_rows + i] = np_column[i]  # Store as int64 timestamp

    # Combine hash values (n * 31 + y pattern)
    if num_columns == 1:
        for i in non_null_indices:
            ht.insert(cell_hashes[i], i)
    else:
        for i in non_null_indices:
            combined_hash = 0
            for j in range(num_columns):
                combined_hash = combined_hash * 31 + cell_hashes[j * num_rows + i]
            ht.insert(combined_hash, i)  # Insert combined hash into the hash table

    free(cell_hashes)
    return ht
