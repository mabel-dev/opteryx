# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
Helper functions for handling PyArrow tables, particularly for hashing values or rows.

This is the "null avoidant" set of these functions, these functions will remove hash values and
remove them from the results.
"""

import pyarrow
import numpy
cimport numpy
numpy.import_array()

from libc.stdint cimport int64_t, uint8_t, uintptr_t


cdef inline numpy.ndarray[int64_t, ndim=1] non_null_row_indices(object relation, list column_names):
    """
    Compute indices of rows where all `column_names` in `relation` are non-null.
    Returns a new numpy array of row indices (int64).
    """
    cdef:
        Py_ssize_t num_rows = relation.num_rows
        numpy.ndarray[uint8_t, ndim=1] combined_nulls_np = numpy.ones(num_rows, dtype=numpy.uint8)
        uint8_t[::1] combined_nulls = combined_nulls_np
        object column, chunk, bitmap_buffer
        const uint8_t* validity
        Py_ssize_t i, j, count = 0
        Py_ssize_t offset, length
        uint8_t byte, bit
        numpy.ndarray[int64_t, ndim=1] indices = numpy.empty(num_rows, dtype=numpy.int64)
        int64_t[::1] indices_view = indices

    for column_name in column_names:
        column = relation.column(column_name)

        if column.null_count > 0:
            offset = 0
            for chunk in column.chunks if isinstance(column, pyarrow.ChunkedArray) else [column]:
                bitmap_buffer = chunk.buffers()[0]
                length = len(chunk)

                if bitmap_buffer is None:
                    # No bitmap -> all values valid, nothing to update
                    offset += length
                    continue

                validity = <const uint8_t*><uintptr_t>bitmap_buffer.address

                if validity == NULL:
                    raise RuntimeError(f"Null validity buffer for column '{column_name}'")

                for j in range(length):
                    byte = validity[j >> 3]
                    bit = (byte >> (j & 7)) & 1
                    combined_nulls[offset + j] &= bit

                offset += length

    for i in range(num_rows):
        if combined_nulls[i]:
            indices_view[count] = i
            count += 1

    return numpy.array(indices_view[:count], copy=True)
