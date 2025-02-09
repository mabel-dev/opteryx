# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

import numpy
cimport numpy
from libc.stdint cimport int64_t
from cpython.object cimport PyObject_Hash

from opteryx.third_party.abseil.containers cimport FlatHashSet


cpdef FlatHashSet count_distinct(numpy.ndarray[numpy.int64_t, ndim=1] values, FlatHashSet seen_hashes=None):
    cdef:
        int64_t i
        int64_t n = values.shape[0]
        int64_t *values_ptr = &values[0]  # Direct pointer access

    for i in range(n):
        seen_hashes.just_insert(values_ptr[i])

    return seen_hashes

cpdef numpy.ndarray[numpy.int64_t, ndim=1] hash_column(numpy.ndarray values):

    cdef:
        int64_t i
        int64_t n = values.shape[0]
        int64_t hash_value
        #object[:] values_view = values
        numpy.ndarray[numpy.int64_t, ndim=1] result = numpy.empty(n, dtype=numpy.int64)

    for i in range(n):
        hash_value = PyObject_Hash(values[i])
        result[i] = hash_value

    return result

cpdef numpy.ndarray[numpy.int64_t, ndim=1] hash_bytes_column(numpy.ndarray[numpy.bytes] values):
    """
    Computes hash for each byte sequence in an array.

    xxHash and Murmur had too many clashes to be useful.

    Parameters:
        values (ndarray): NumPy array of bytes objects

    Returns:
        ndarray: NumPy array of int64 hashes
    """
    cdef:
        Py_ssize_t i, n = values.shape[0]
        int64_t[::1] result_view = numpy.empty(n, dtype=numpy.int64)
        bytes[::1] values_view = values

    for i in range(n):
        result_view[i] = PyObject_Hash(values_view[i])

    return numpy.asarray(result_view, dtype=numpy.int64)
