# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

cimport numpy as cnp
import numpy
from libc.stdint cimport uint8_t, int64_t

from opteryx.third_party.abseil.containers cimport FlatHashMap
from cpython.object cimport PyObject_Hash

cpdef FlatHashMap abs_hash_join_map(relation, list join_columns):
    """
    Build a hash table for the join operations.

    Parameters:
        relation: The pyarrow.Table to preprocess.
        join_columns: A list of column names to join on.

    Returns:
        A FlatHashMap where keys are hashes of the join column entries and
        values are lists of row indices corresponding to each hash key.
    """
    cdef FlatHashMap ht = FlatHashMap()

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
