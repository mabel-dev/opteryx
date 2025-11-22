# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

import numpy
cimport numpy

from libc.stdint cimport int64_t, int32_t
from libc.stddef cimport size_t
from libcpp.vector cimport vector

cdef extern from "intbuffer.h" namespace "" nogil:
    cdef cppclass CIntBuffer:
        CIntBuffer(size_t size_hint) except +
        void append(int64_t value)
        void extend(const vector[int64_t]& values)
        void extend(const int64_t* values, size_t count)
        void reserve(size_t additional_capacity)
        void resize(size_t new_size)
        const int64_t* data() const
        int64_t* mutable_data()
        size_t size() const
        void append_repeated(int64_t value, size_t count)

    cdef cppclass CInt32Buffer:
        CInt32Buffer(size_t size_hint) except +
        void append(int32_t value)
        void extend(const vector[int32_t]& values)
        void extend(const int32_t* values, size_t count)
        void reserve(size_t additional_capacity)
        void resize(size_t new_size)
        const int32_t* data() const
        int32_t* mutable_data()
        size_t size() const


cdef class IntBuffer:

    cdef CIntBuffer* c_buffer
    cdef Py_ssize_t _shape[1]
    cdef Py_ssize_t _strides[1]

    cpdef void append(self, int64_t value)
    cpdef void extend(self, iterable)
    cpdef numpy.ndarray[int64_t, ndim=1] to_numpy(self)
    cpdef const int64_t[::1] get_buffer(self)
    cpdef size_t size(self)
    cpdef void extend_numpy(self, numpy.ndarray[int64_t, ndim=1] arr)
    cpdef void reserve(self, size_t capacity)
    cpdef void append_batch(self, int64_t[::1] values)

cdef class Int32Buffer:

    cdef CInt32Buffer* c_buffer
    cdef Py_ssize_t _shape[1]
    cdef Py_ssize_t _strides[1]

    cpdef void append(self, int32_t value)
    cpdef void extend(self, iterable)
    cpdef numpy.ndarray[int32_t, ndim=1] to_numpy(self)
    cpdef size_t size(self)
    cpdef void reserve(self, size_t capacity)
    cpdef void extend_numpy(self, numpy.ndarray[int32_t, ndim=1] arr)
