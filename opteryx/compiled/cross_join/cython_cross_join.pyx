# cython: language_level=3

from libc.stdlib cimport malloc, free
import numpy as np
cimport numpy as cnp
cimport cython
from libc.stdint cimport int32_t

@cython.boundscheck(False)
@cython.wraparound(False)
cpdef build_rows_indices_and_column(cnp.ndarray column_data):
    cdef int32_t i, total_size = 0
    cdef int32_t length
    cdef list flat_data
    cdef int32_t row_count = len(column_data)
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

    flat_data = [''] * total_size
    cdef int32_t *indices = <int32_t *>malloc(total_size * sizeof(int32_t))
    if indices is NULL:
        raise MemoryError("Failed to allocate memory.")

    cdef int32_t start = 0
    cdef int32_t end = 0
    cdef int32_t j = 0

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


from libc.stdlib cimport malloc, realloc, free
import numpy as np
cimport numpy as cnp
cimport cython
from libc.stdint cimport int32_t

@cython.boundscheck(False)
@cython.wraparound(False)
cpdef tuple build_filtered_rows_indices_and_column(cnp.ndarray column_data, set valid_values):
    """
    Build row indices and flattened column data for matching values from a column of array-like elements.

    Parameters:
        column_data: ndarray
            An array of arrays from which to create row indices and flattened data.
        valid_values: set
            A set of values to filter the rows by during the cross join.

    Returns:
        tuple of (ndarray, list)
            Returns a tuple containing an array of indices and a list of flattened data for rows that match the filter.
    """
    cdef int32_t i, index = 0, allocated_size
    cdef int32_t row_count = len(column_data)
    cdef int32_t initial_alloc_size = 500
    allocated_size = initial_alloc_size
    cdef int32_t *indices = <int32_t *>malloc(allocated_size * sizeof(int32_t))
    cdef list flat_data = [None] * allocated_size
    if indices is NULL:
        raise MemoryError("Failed to allocate memory for indices.")

    for i in range(row_count):
        for value in column_data[i]:
            if value in valid_values:
                if index == allocated_size:  # Check if we need to expand the memory allocation
                    allocated_size += initial_alloc_size
                    indices = <int32_t *>realloc(indices, allocated_size * sizeof(int32_t))
                    flat_data.extend([None] * initial_alloc_size)  # Extend flat_data by the same amount
                    if indices is NULL:
                        raise MemoryError("Failed to reallocate memory for indices.")
                flat_data[index] = value
                indices[index] = i
                index += 1

    if index == 0:
        free(indices)
        return (np.array([], dtype=np.int32), [])

    # Slice to actual used size before creating final arrays
    cdef int32_t[:] indices_view = <int32_t[:index]>indices
    cdef cnp.ndarray final_indices = np.array(indices_view, dtype=np.int32)
    free(indices)  # Free the original buffer now

    return (final_indices, flat_data[:index])
