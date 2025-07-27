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

from libc.stdint cimport uint32_t

cpdef numpy.ndarray[numpy.uint32_t, ndim=1] list_length(numpy.ndarray array):
    cdef Py_ssize_t i, n = array.shape[0]
    cdef numpy.ndarray[numpy.uint32_t, ndim=1] result = numpy.empty(n, dtype=numpy.uint32)
    cdef uint32_t[::1] result_view = result
    cdef object val

    for i in range(n):
        val = array[i]
        if isinstance(val, (str, bytes)):
            result_view[i] = len(val)
        else:
            result_view[i] = 0

    return result
