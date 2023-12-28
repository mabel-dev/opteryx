# cython: language_level=3
import cython
import numpy
cimport numpy as cnp
from cython.parallel import prange
from cython import Py_ssize_t
from libc.stdlib cimport malloc
from libc.stdlib cimport free
from numpy cimport int64_t
from numpy cimport ndarray

cnp.import_array()


@cython.boundscheck(False)
@cython.wraparound(False)
cpdef build_rows_indices_and_column(object column_data):
    cdef int i, j, total_size = 0
    cdef object item

    # Calculate the total size
    for arr in column_data:
        if arr is not None and arr.is_valid:
            total_size += len(arr)

    # Allocate memory for indices
    cdef int* indices = <int*>malloc(total_size * sizeof(int))
    if not indices:
        raise MemoryError("Failed to allocate memory for indices")

    # Prepare new column data list
    new_column_data = []

    # Fill indices and new column data
    cdef int idx = 0
    for i in range(len(column_data)):
        arr = column_data[i]
        if arr is not None and arr.is_valid:
            for item in arr:
                indices[idx] = i
                new_column_data.append(item)
                idx += 1

    if idx == 0:
        return ([], [])

    # Convert indices to a numpy array
    cdef int[:] indices_view = <int[:total_size]>indices
    cdef cnp.ndarray indices_array = numpy.asarray(indices_view)

    # Free the allocated memory
    # free(indices)

    # Return a Python tuple containing the numpy array and the new column data list
    return (indices_array, new_column_data)
