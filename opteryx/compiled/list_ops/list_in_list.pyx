# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from cython import Py_ssize_t
from libc.stdint cimport int64_t, uint8_t

import numpy
cimport numpy
numpy.import_array()

cpdef numpy.ndarray[numpy.uint8_t, ndim=1] list_in_list(object[::1] arr, set values):
    """
    Fast membership check for "InList" using Cython.

    Parameters:
        arr: NumPy array of arbitrary type (should be homogeneous).
        values: List of valid values (converted to a Cython set).

    Returns:
        NumPy boolean array indicating membership.
    """
    cdef Py_ssize_t i, size = arr.shape[0]
    cdef numpy.ndarray[numpy.uint8_t, ndim=1] result = numpy.empty(size, dtype=numpy.uint8)
    cdef uint8_t[::1] result_view = result

    for i in range(size):
        result_view[i] = arr[i] in values

    return result

cpdef numpy.ndarray[numpy.uint8_t, ndim=1] list_in_list_int64(const int64_t[::1] arr, set values, Py_ssize_t size):
    """
    Fast membership check for "InList" using Cython.

    Parameters:
        arr: NumPy array of arbitrary type (should be homogeneous).
        values: List of valid values (converted to a Cython set).

    Returns:
        NumPy boolean array indicating membership.
    """
    cdef Py_ssize_t i
    cdef numpy.ndarray[numpy.uint8_t, ndim=1] result = numpy.empty(size, dtype=numpy.uint8)
    cdef uint8_t[::1] result_view = result
    cdef int64_t value

    for i in range(size):
        value = arr[i]
        result_view[i] = value in values

    return result
