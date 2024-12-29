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
from libc.stdint cimport int64_t, int32_t, uint8_t
from libcpp.pair cimport pair
from libc.math cimport isnan

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


cdef inline object recast_column(column):
    cdef column_type = column.type

    if pyarrow.types.is_struct(column_type) or pyarrow.types.is_list(column_type):
        return numpy.array([str(a) for a in column], dtype=numpy.str_)

    # Otherwise, return the column as-is
    return column


cpdef tuple distinct(table, HashSet seen_hashes=None, list columns=None):
    """
    Perform a distinct operation on the given table using an external HashSet.
    """

    cdef:
        int64_t null_hash = hash(None)
        Py_ssize_t num_columns, num_rows, i
        list columns_of_interest
        list columns_data = []
        list columns_hashes = []
        cnp.ndarray[int64_t] combined_hashes
        HashSet hash_set
        list keep = []
        object column_data
        cnp.ndarray data_array
        cnp.ndarray[int64_t] hashes

    if seen_hashes is None:
        seen_hashes = HashSet()

    columns_of_interest = columns if columns is not None else table.column_names
    num_columns = len(columns_of_interest)
    num_rows = table.num_rows  # Use PyArrow's num_rows attribute

    # Prepare data and compute hashes for each column
    for column_name in columns_of_interest:
        # Get the column from the table
        column = table.column(column_name)
        # Recast column if necessary
        column_data = recast_column(column)
        # Convert PyArrow array to NumPy array without copying
        if isinstance(column_data, pyarrow.ChunkedArray):
            data_array = column_data.combine_chunks().to_numpy(zero_copy_only=False)
        elif isinstance(column_data, pyarrow.Array):
            data_array = column_data.to_numpy(zero_copy_only=False)
        else:
            data_array = column_data  # Already a NumPy array

        columns_data.append(data_array)
        hashes = numpy.empty(num_rows, dtype=numpy.int64)

        # Determine data type and compute hashes accordingly
        if numpy.issubdtype(data_array.dtype, numpy.integer):
            compute_int_hashes(data_array, null_hash, hashes)
        elif numpy.issubdtype(data_array.dtype, numpy.floating):
            compute_float_hashes(data_array, null_hash, hashes)
        elif data_array.dtype == numpy.object_:
            compute_object_hashes(data_array, null_hash, hashes)
        else:
            # For other types (e.g., strings), treat as object
            compute_object_hashes(data_array.astype(numpy.object_), null_hash, hashes)

        columns_hashes.append(hashes)

    # Combine the hashes per row
    combined_hashes = columns_hashes[0]
    for hashes in columns_hashes[1:]:
        combined_hashes = combined_hashes * 31 + hashes

    # Check for duplicates using the HashSet
    for i in range(num_rows):
        if seen_hashes.insert(combined_hashes[i]):
            keep.append(i)

    return keep, seen_hashes

cdef void compute_float_hashes(cnp.ndarray[cnp.float64_t] data, int64_t null_hash, int64_t[:] hashes):
    cdef Py_ssize_t i, n = data.shape[0]
    cdef cnp.float64_t value
    for i in range(n):
        value = data[i]
        if isnan(value):
            hashes[i] = null_hash
        else:
            hashes[i] = hash(value)


cdef void compute_int_hashes(cnp.ndarray[cnp.int64_t] data, int64_t null_hash, int64_t[:] hashes):
    cdef Py_ssize_t i, n = data.shape[0]
    cdef cnp.int64_t value
    for i in range(n):
        value = data[i]
        # Assuming a specific value represents missing data
        # Adjust this condition based on how missing integers are represented
        if value == numpy.iinfo(numpy.int64).min:
            hashes[i] = null_hash
        else:
            hashes[i] = value  # Hash of int is the int itself in Python 3

cdef void compute_object_hashes(cnp.ndarray data, int64_t null_hash, int64_t[:] hashes):
    cdef Py_ssize_t i, n = data.shape[0]
    cdef object value
    for i in range(n):
        value = data[i]
        if value is None:
            hashes[i] = null_hash
        else:
            hashes[i] = hash(value)


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
        hash_value = <int64_t>hash(v)
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


cpdef HashSet filter_join_set(relation, list join_columns, HashSet seen_hashes):
    """
    Build the set for the right of a filter join (ANTI/SEMI)
    """

    cdef int64_t num_columns = len(join_columns)

    if seen_hashes is None:
        seen_hashes = HashSet()

    # Memory view for the values array (for the join columns)
    cdef object[:, ::1] values_array = numpy.array(list(relation.select(join_columns).drop_null().itercolumns()), dtype=object)

    cdef int64_t hash_value, i

    if num_columns == 1:
        col = values_array[0, :]
        for i in range(len(col)):
            hash_value = <int64_t>hash(col[i])
            seen_hashes.insert(hash_value)
    else:
        for i in range(values_array.shape[1]):
            # Combine the hashes of each value in the row
            hash_value = 0
            for value in values_array[:, i]:
                hash_value = <int64_t>(hash_value * 31 + hash(value))
            seen_hashes.insert(hash_value)

    return seen_hashes

cpdef anti_join(relation, list join_columns, HashSet seen_hashes):
    cdef int64_t num_columns = len(join_columns)
    cdef int64_t num_rows = relation.shape[0]
    cdef int64_t hash_value, i
    cdef cnp.ndarray[int64_t, ndim=1] index_buffer = numpy.empty(num_rows, dtype=numpy.int64)
    cdef int64_t idx_count = 0

    cdef object[:, ::1] values_array = numpy.array(list(relation.select(join_columns).drop_null().itercolumns()), dtype=object)

    if num_columns == 1:
        col = values_array[0, :]
        for i in range(len(col)):
            hash_value = <int64_t>hash(col[i])
            if not seen_hashes.contains(hash_value):
                index_buffer[idx_count] = i
                idx_count += 1
    else:
        for i in range(values_array.shape[1]):
            # Combine the hashes of each value in the row
            hash_value = 0
            for value in values_array[:, i]:
                hash_value = <int64_t>(hash_value * 31 + hash(value))
            if not seen_hashes.contains(hash_value):
                index_buffer[idx_count] = i
                idx_count += 1

    if idx_count > 0:
        return relation.take(index_buffer[:idx_count])
    else:
        return relation.slice(0, 0)


cpdef semi_join(relation, list join_columns, HashSet seen_hashes):
    cdef int64_t num_columns = len(join_columns)
    cdef int64_t num_rows = relation.shape[0]
    cdef int64_t hash_value, i
    cdef cnp.ndarray[int64_t, ndim=1] index_buffer = numpy.empty(num_rows, dtype=numpy.int64)
    cdef int64_t idx_count = 0

    cdef object[:, ::1] values_array = numpy.array(list(relation.select(join_columns).drop_null().itercolumns()), dtype=object)

    if num_columns == 1:
        col = values_array[0, :]
        for i in range(len(col)):
            hash_value = <int64_t>hash(col[i])
            if seen_hashes.contains(hash_value):
                index_buffer[idx_count] = i
                idx_count += 1
    else:
        for i in range(values_array.shape[1]):
            # Combine the hashes of each value in the row
            hash_value = 0
            for value in values_array[:, i]:
                hash_value = <int64_t>(hash_value * 31 + hash(value))
            if seen_hashes.contains(hash_value):
                index_buffer[idx_count] = i
                idx_count += 1

    if idx_count > 0:
        return relation.take(index_buffer[:idx_count])
    else:
        return relation.slice(0, 0)