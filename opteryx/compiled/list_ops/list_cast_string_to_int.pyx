# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: language_level=3

import numpy
cimport numpy
numpy.import_array()

from libc.stdint cimport int64_t
from cpython.bytes cimport PyBytes_AS_STRING

cpdef numpy.ndarray list_cast_ascii_to_int(numpy.ndarray[object, ndim=1] arr):
    cdef Py_ssize_t i, j, n = arr.shape[0]
    cdef numpy.ndarray[int64_t] result = numpy.empty(n, dtype=numpy.int64)
    cdef int64_t[:] result_view = result

    cdef str s
    cdef int64_t value
    cdef int sign
    cdef Py_ssize_t length
    cdef char c

    for i in range(n):
        s = arr[i]
        length = len(s)
        value = 0
        sign = 1

        j = 0
        if length > 0 and s[0] == 45:  # -
            sign = -1
            j = 1

        for j in range(j, length):
            c = ord(s[j]) - 48
            if c < 0 or c > 9:
                raise ValueError(f"Invalid digit: {s[j]!r}")
            value = value * 10 + c

        result_view[i] = sign * value

    return result

cpdef numpy.ndarray list_cast_bytes_to_int(numpy.ndarray[object, ndim=1] arr):
    cdef Py_ssize_t i, j, n = arr.shape[0]
    cdef numpy.ndarray[int64_t] result = numpy.empty(n, dtype=numpy.int64)
    cdef int64_t[:] result_view = result

    cdef const char* c_str
    cdef Py_ssize_t length
    cdef int64_t value
    cdef int sign
    cdef char c

    for i in range(n):
        c_str = PyBytes_AS_STRING(arr[i])
        length = len(arr[i])
        value = 0
        sign = 1

        j = 0
        if length > 0 and c_str[0] == 45:  # -
            sign = -1
            j = 1

        for j in range(j, length):
            c = c_str[j] - 48
            if c < 0 or c > 9:
                raise ValueError(f"Invalid digit: {chr(c_str[j])!r}")
            value = value * 10 + c

        result_view[i] = sign * value

    return result
