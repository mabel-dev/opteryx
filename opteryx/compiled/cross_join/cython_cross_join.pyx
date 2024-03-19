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
    cdef cnp.ndarray flat_data
    cdef Py_ssize_t row_count = len(column_data)
    cdef Py_ssize_t *lengths = <Py_ssize_t *>malloc(row_count * sizeof(Py_ssize_t))
    if lengths is NULL:
        raise MemoryError("Failed to allocate memory for lengths array.")

    for i in range(row_count):
        length = len(column_data[i])
        lengths[i] = length
        total_size += length

    if total_size == 0:
        free(lengths)
        return (np.array([], dtype=np.int32), np.array([], dtype=object))

    flat_data = np.empty(total_size, dtype=object)
    cdef int32_t *indices = <int32_t *>malloc(total_size * sizeof(int32_t))
    if indices is NULL:
        raise MemoryError("Failed to allocate memory.")

    cdef Py_ssize_t start = 0
    cdef Py_ssize_t end = 0
    cdef Py_ssize_t j = 0

    for i in range(row_count):
        end = start + lengths[i]
        for j in range(start, end):
            indices[j] = i 
            flat_data[j] = column_data[i][j - start]
        start = end
    free(lengths)

    cdef cnp.int32_t[:] mv = <cnp.int32_t[:total_size]>indices
    # Create a NumPy array that is a copy of the memoryview, 
    # which in turn makes it safe to free the original indices memory.
    np_array = np.array(mv, copy=True)
    free(indices)  # Now it's safe to free indices since np_array has its own copy.
    return (np_array, flat_data)
