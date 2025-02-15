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

from cpython.unicode cimport PyUnicode_AsUTF8String

cpdef numpy.ndarray list_encode_utf8(numpy.ndarray inp):
    """
    Parallel UTF-8 encode all elements of a 1D ndarray of "object" dtype.
    """
    cdef Py_ssize_t n = inp.shape[0]
    cdef numpy.ndarray out = numpy.empty(n, dtype=object)
    cdef object[:] inp_view = inp
    cdef object[:] out_view = out

    for i in range(n):
        out_view[i] = PyUnicode_AsUTF8String(inp_view[i])

    return out
