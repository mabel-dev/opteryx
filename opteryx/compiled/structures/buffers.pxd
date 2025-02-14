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
from libcpp.vector cimport vector

cdef extern from "intbuffer.h" namespace "":
    cdef cppclass CIntBuffer:
        CIntBuffer(size_t size_hint)
        void append(int64_t value)
        void extend(const vector[int64_t]& values)
        const int64_t* data() const
        size_t size() const


cdef class IntBuffer:

    cdef CIntBuffer* c_buffer

    cpdef void append(self, int64_t value)
    cpdef void extend(self, iterable)
    cpdef numpy.ndarray[int64_t, ndim=1] to_numpy(self)
    cpdef size_t size(self)
