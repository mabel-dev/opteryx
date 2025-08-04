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

from libc.stdint cimport int64_t, uint64_t
from cpython.bytes cimport PyBytes_FromStringAndSize

cdef inline char* uint64_to_str_ptr(uint64_t value, char* buf) nogil:
    cdef int64_t val
    cdef int i = 20

    if value == 0:
        buf[19] = 48  # '0'
        return buf + 19

    val = <int64_t>value

    while val != 0:
        i -= 1
        buf[i] = 48 + (val % 10)
        val //= 10

    return buf + i

cpdef numpy.ndarray list_cast_uint64_to_bytes(const uint64_t[:] arr):
    cdef Py_ssize_t i, n = arr.shape[0]
    cdef char buf[21]
    cdef char* ptr
    cdef int length

    cdef numpy.ndarray[object, ndim=1] result = numpy.empty(n, dtype=object)
    cdef object[:] result_view = result

    for i in range(n):
        ptr = uint64_to_str_ptr(arr[i], buf)
        length = buf + 20 - ptr
        result_view[i] = PyBytes_FromStringAndSize(ptr, length)

    return result

cpdef numpy.ndarray list_cast_uint64_to_ascii(const uint64_t[:] arr):
    cdef Py_ssize_t i, n = arr.shape[0]
    cdef char buf[21]
    cdef char* ptr
    cdef int length

    cdef numpy.ndarray[object, ndim=1] result = numpy.empty(n, dtype=object)
    cdef object[:] result_view = result

    for i in range(n):
        ptr = uint64_to_str_ptr(arr[i], buf)
        length = buf + 20 - ptr
        result_view[i] = PyBytes_FromStringAndSize(ptr, length).decode("ascii")

    return result
