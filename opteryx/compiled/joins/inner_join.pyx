# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport uint8_t, int64_t

from opteryx.third_party.abseil.containers cimport FlatHashMap
from opteryx.compiled.structures.buffers cimport IntBuffer
from cpython.object cimport PyObject_Hash

import numpy
cimport numpy
numpy.import_array()

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
    cdef numpy.ndarray non_null_indices = numpy.nonzero(combined_nulls)[0]

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


cpdef tuple nested_loop_join(left_relation, right_relation, numpy.ndarray left_columns, numpy.ndarray right_columns):
    """
    This performs a nested loop join, this is generally a bad idea but does
    outperform our hash join when the relation sizes are small. This is
    primarily due to avoiding building a hash table.
    """
    cdef IntBuffer left_indexes = IntBuffer()
    cdef IntBuffer right_indexes = IntBuffer()
    cdef int64_t nl = left_relation.shape[0]
    cdef int64_t nr = right_relation.shape[0]
    cdef int64_t left_idx, right_idx
    cdef int64_t left_hash_value, right_hash_value
    cdef object value

    cdef object[:, ::1] left_values_array = numpy.array(list(left_relation.select(left_columns).itercolumns()), dtype=object)
    cdef object[:, ::1] right_values_array = numpy.array(list(right_relation.select(right_columns).itercolumns()), dtype=object)

    cdef int64_t[::1] right_hashes = numpy.empty(nr, dtype=numpy.int64)
    for right_idx in range(nr):
        right_hash_value = 0
        for value in right_values_array[:, right_idx]:
            right_hash_value = <int64_t>(right_hash_value * 31 + PyObject_Hash(value))
        right_hashes[right_idx] = right_hash_value

    for left_idx in range(nl):

        left_hash_value = 0
        for value in left_values_array[:, left_idx]:
            left_hash_value = <int64_t>(left_hash_value * 31 + PyObject_Hash(value))

        for right_idx in range(nr):
            if left_hash_value == right_hashes[right_idx]:
                left_indexes.append(left_idx)
                right_indexes.append(right_idx)

    return (left_indexes.to_numpy(), right_indexes.to_numpy())
