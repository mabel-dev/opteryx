from libc.stdlib cimport malloc, free
import numpy as np
cimport numpy as cnp
import cython

@cython.boundscheck(False)
@cython.wraparound(False)
cpdef build_rows_indices_and_column(cnp.ndarray column_data):
    cdef Py_ssize_t i, total_size = 0
    cdef Py_ssize_t length
    cdef cnp.ndarray flat_data, indices
    # Use Py_ssize_t for compatibility with Python/Cython sizes and indices
    cdef Py_ssize_t *lengths = <Py_ssize_t *>malloc(len(column_data) * sizeof(Py_ssize_t))
    if lengths is NULL:
        raise MemoryError("Failed to allocate memory for lengths array.")

    # Iterate over the column_data to calculate total_size and lengths of sub-arrays
    for i in range(len(column_data)):
        length = column_data[i].shape[0]
        lengths[i] = length
        total_size += length

    # If total_size is 0, handle the empty case
    if total_size == 0:
        free(lengths)  # Remember to free the allocated memory to avoid memory leaks
        return (np.array([], dtype=np.int64), np.array([], dtype=object))

    # Initialize the flat_data and indices arrays
    flat_data = np.empty(total_size, dtype=object)
    indices = np.empty(total_size, dtype=np.int64)

    # Fill flat_data and indices
    cdef Py_ssize_t start = 0
    for i in range(len(column_data)):
        if column_data[i] is not None:
            end = start + lengths[i]
            flat_data[start:end] = column_data[i]
            indices[start:end] = i
            start = end

    free(lengths)  # Free the lengths array after use

    return (indices, flat_data)
