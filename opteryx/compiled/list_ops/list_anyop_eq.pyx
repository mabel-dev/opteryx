# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

import numpy
cimport numpy
numpy.import_array()

from libc.stdint cimport uint8_t

cpdef uint8_t[:] list_anyop_eq(object literal, numpy.ndarray arr):
    """
    Optimized function to check for presence of `literal` in each row.

    Parameters:
        literal: object
            The value to search for in each row.
        arr: numpy.ndarray
            A two-dimensional numpy array where each element is a sub-array (row).

    Returns:
        numpy.ndarray[numpy.npy_bool, ndim=1]
            A boolean array indicating for each row whether `literal` was found.
    """

    cdef Py_ssize_t i, j, num_rows, row_length
    num_rows = arr.shape[0]

    cdef numpy.ndarray[numpy.npy_bool, ndim=1] result = numpy.zeros(num_rows, dtype=bool)
    cdef uint8_t[:] result_view = result
    cdef numpy.ndarray row

    for i in range(num_rows):
        row = arr[i]
        if row is not None:
            row_length = row.shape[0]
            for j in range(row_length):
                if row[j] == literal:
                    result_view[i] = True
                    break

    return result
