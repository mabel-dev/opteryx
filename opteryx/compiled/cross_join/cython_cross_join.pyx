# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=True
# cython: boundscheck=False

from libc.stdlib cimport malloc, free
import numpy as np
cimport numpy as cnp
cimport cython
from libc.stdint cimport int32_t

cpdef tuple build_rows_indices_and_column(cnp.ndarray column_data):
    cdef int32_t i, total_size = 0
    cdef int32_t length
    cdef int32_t row_count = len(column_data)
    cdef int32_t *lengths = <int32_t *>malloc(row_count * sizeof(int32_t))
    if lengths is NULL:
        raise MemoryError("Failed to allocate memory for lengths array.")

    # Calculate the total size and fill lengths array
    for i in range(row_count):
        length = column_data[i].shape[0]
        lengths[i] = length
        total_size += length

    # If the total size is zero, return empty arrays
    if total_size == 0:
        free(lengths)
        return (np.array([], dtype=np.int32), np.array([], dtype=object))

    # Determine the dtype of the elements in the arrays, handling the case where the first element is None
    element_dtype = object
    for i in range(row_count):
        if column_data[i] is not None:
            element_dtype = column_data[i].dtype
            break

    # Preallocate arrays for indices and flat data
    flat_data = np.empty(total_size, dtype=element_dtype)  # More efficient than list
    cdef int32_t *indices = <int32_t *>malloc(total_size * sizeof(int32_t))
    if indices is NULL:
        free(lengths)
        raise MemoryError("Failed to allocate memory for indices.")

    cdef int32_t start = 0
    cdef int32_t end = 0

    # Flatten the data and fill indices
    for i in range(row_count):
        end = start + lengths[i]
        flat_data[start:end] = column_data[i]  # NumPy handles the slicing and copying
        for j in range(start, end):
            indices[j] = i
        start = end

    free(lengths)  # Free the lengths array

    # Create a NumPy array from indices
    cdef cnp.int32_t[:] mv = <cnp.int32_t[:total_size]>indices
    np_array = np.array(mv, copy=True)  # Copy the memoryview into a NumPy array
    free(indices)  # Free the indices memory now that we've copied it

    return (np_array, flat_data)


from libc.stdlib cimport malloc, realloc, free
import numpy as np
cimport numpy as cnp
cimport cython
from libc.stdint cimport int32_t


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
    cdef int32_t initial_alloc_size = row_count * 2
    allocated_size = initial_alloc_size
    cdef int32_t *indices = <int32_t *>malloc(allocated_size * sizeof(int32_t))
    cdef int32_t *new_indices

    if indices is NULL:
        raise MemoryError("Failed to allocate memory for indices.")

    # Determine the dtype of the elements in the arrays, handling the case where the first element is None
    element_dtype = object
    for i in range(row_count):
        if column_data[i] is not None:
            element_dtype = column_data[i].dtype
            break

    cdef flat_data = np.empty(allocated_size, dtype=element_dtype)

    for i in range(row_count):
        for value in column_data[i]:
            if value in valid_values:
                if index == allocated_size:  # Check if we need to expand the memory allocation
                    allocated_size = allocated_size * 2  # Double the allocation size to reduce reallocations
                    # Handle realloc for indices safely
                    new_indices = <int32_t *>realloc(indices, allocated_size * sizeof(int32_t))
                    if new_indices is NULL:
                        free(indices)  # Free previously allocated memory to avoid memory leak
                        raise MemoryError("Failed to reallocate memory for indices.")
                    indices = new_indices
                    flat_data = np.resize(flat_data, allocated_size)
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
