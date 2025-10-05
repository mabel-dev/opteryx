# distutils: language = c++
# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

import numpy
cimport numpy

from cpython.bytes cimport PyBytes_AS_STRING, PyBytes_GET_SIZE
from cpython.unicode cimport PyUnicode_AsUTF8String


cdef extern from "fast_float.h" namespace "fast_float":
    cdef cppclass from_chars_result:
        const char* ptr

    from_chars_result from_chars(
        const char* first,
        const char* last,
        double& value
    )

cdef inline double c_parse_fast_float(bytes bts):
    cdef const char* s = bts
    cdef double value = 0.0
    cdef Py_ssize_t n = len(bts)

    cdef from_chars_result res = from_chars(s, s + n, value)

    if res.ptr != NULL:
        return value
    else:
        raise ValueError(f"Could not parse float from {bts!r}")

cpdef double parse_fast_float(bytes bts):
    return c_parse_fast_float(bts)

cpdef numpy.ndarray[double] parse_ascii_array_to_double(numpy.ndarray[object, ndim=1] arr):
    """
    Parse an array of Python strings (str) to NumPy float64 using fast_float.
    Assumes ASCII input.
    """
    cdef Py_ssize_t i, n = arr.shape[0]
    cdef numpy.ndarray[double] out = numpy.empty(n, dtype=numpy.float64)
    cdef double[:] out_view = out
    cdef bytes encoded
    cdef const char* c_str
    cdef Py_ssize_t length
    cdef double val = 0.0
    cdef from_chars_result res
    cdef object item

    for i in range(n):
        item = arr[i]
        if item is None:
            out_view[i] = numpy.nan
            continue

        # Convert str to bytes (UTF-8 encoded, ideally ASCII)
        encoded = PyUnicode_AsUTF8String(item)
        c_str = PyBytes_AS_STRING(encoded)
        length = PyBytes_GET_SIZE(encoded)

        res = from_chars(c_str, c_str + length, val)
        if res.ptr != NULL:
            out_view[i] = val
        else:
            out_view[i] = numpy.nan  # or raise?

    return out


cpdef numpy.ndarray[double] parse_byte_array_to_double(numpy.ndarray[object, ndim=1] arr):
    """
    Parse an array of Python bytes (b"123.45") to NumPy float64 using fast_float.
    """
    cdef Py_ssize_t i, n = arr.shape[0]
    cdef numpy.ndarray[double] out = numpy.empty(n, dtype=numpy.float64)
    cdef double[:] out_view = out
    cdef const char* c_str
    cdef Py_ssize_t length
    cdef double val = 0.0
    cdef from_chars_result res
    cdef object item

    for i in range(n):
        item = arr[i]
        if item is None:
            out_view[i] = numpy.nan
            continue

        c_str = PyBytes_AS_STRING(item)
        length = PyBytes_GET_SIZE(item)

        res = from_chars(c_str, c_str + length, val)
        if res.ptr != NULL:
            out_view[i] = val
        else:
            out_view[i] = numpy.nan

    return out
