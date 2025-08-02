# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=False
# cython: wraparound=False
# cython: boundscheck=False

import numpy
cimport numpy

from libc.stdint cimport uint32_t

cdef extern from "ryu.h":
    int d2fixed_buffered_n(double d, uint32_t precision, char* result)

cpdef numpy.ndarray[object] format_double_array_bytes(numpy.ndarray[numpy.float64_t, ndim=1] arr):
    """
    Convert a NumPy array of float64s to a NumPy object array of bytes.
    """
    cdef:
        Py_ssize_t i, n = arr.shape[0]
        numpy.ndarray[object] result = numpy.empty(n, dtype=object)
        object[:] result_view = result
        char buf[32]
        int length

    for i in range(n):
        length = d2fixed_buffered_n(arr[i], 6, buf)
        # Inline formatting instead of calling format_double()
        result_view[i] = (<bytes>buf[:length])

    return result

cpdef numpy.ndarray[object] format_double_array_ascii(numpy.ndarray[numpy.float64_t, ndim=1] arr):
    """
    Convert a NumPy array of float64s to a NumPy object array of Python strings.
    """
    cdef:
        Py_ssize_t i, n = arr.shape[0]
        numpy.ndarray[object] result = numpy.empty(n, dtype=object)
        object[:] result_view = result
        char buf[32]
        int length

    for i in range(n):
        length = d2fixed_buffered_n(arr[i], 6, buf)
        result_view[i] = (<bytes>buf[:length]).decode("ascii")

    return result
