from libc.stdlib cimport malloc, free
import numpy as np
cimport numpy as cnp
cimport cython
from libc.stdint cimport int32_t


@cython.boundscheck(False)
@cython.wraparound(False)
cpdef build_rows_indices_and_column(cnp.ndarray column_data):
    cdef Py_ssize_t i, total_size = 0
    cdef Py_ssize_t length
    cdef list flat_data
    cdef Py_ssize_t row_count = len(column_data)
    cdef int32_t *lengths = <int32_t *>malloc(row_count * sizeof(int32_t))
    if lengths is NULL:
        raise MemoryError("Failed to allocate memory for lengths array.")

    for i in range(row_count):
        length = len(column_data[i])
        lengths[i] = length
        total_size += length

    if total_size == 0:
        free(lengths)
        return (np.array([], dtype=np.int32), np.array([], dtype=object))

    flat_data = [0] * total_size
    cdef int32_t *indices = <int32_t *>malloc(total_size * sizeof(int32_t))
    if indices is NULL:
        raise MemoryError("Failed to allocate memory.")

    cdef int32_t start, end = 0
    cdef int32_t j
    for i in range(row_count):
        end = start + lengths[i]
        for j in range(start, end):
            indices[j] = i 
            flat_data[j] = column_data[i][j - start]
        start = end
    free(lengths)

    cdef cnp.int32_t[:] mv = <cnp.int32_t[:total_size]>indices
    return (np.asarray(mv), flat_data)
