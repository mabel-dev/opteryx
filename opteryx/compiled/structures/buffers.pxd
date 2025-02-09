# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport int64_t

import numpy
cimport numpy

cdef class IntBuffer:

    cdef public int64_t[::1] _buffer
    cdef public int64_t size
    cdef public int64_t capacity

    cpdef void append(self, int64_t value)
    cpdef void extend(self, iterable)
    cpdef numpy.ndarray[int64_t, ndim=1] to_numpy(self)
    cpdef buffer(self)