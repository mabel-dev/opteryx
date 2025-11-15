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
from libc.stddef cimport size_t
from libcpp.vector cimport vector

cdef extern from "intbuffer.h" namespace "" nogil:
    cdef cppclass CIntBuffer:
        CIntBuffer(size_t size_hint) except +
        void append(int64_t value)
        void extend(const vector[int64_t]& values)
        void extend(const int64_t* values, size_t count)
        void reserve(size_t additional_capacity)
        const int64_t* data() const
        size_t size() const
        void append_repeated(int64_t value, size_t count)


cdef class IntBuffer:

    cdef CIntBuffer* c_buffer

    cpdef void append(self, int64_t value)
    cpdef void extend(self, iterable)
    cpdef numpy.ndarray[int64_t, ndim=1] to_numpy(self)
    cpdef const int64_t[::1] get_buffer(self)
    cpdef size_t size(self)
    cpdef void extend_numpy(self, numpy.ndarray[int64_t, ndim=1] arr)
    cpdef void reserve(self, size_t capacity)
    cpdef void append_batch(self, int64_t[::1] values)
