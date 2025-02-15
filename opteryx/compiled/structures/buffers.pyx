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
numpy.import_array()

from libc.stdint cimport int64_t
from libcpp.vector cimport vector

cdef extern from "intbuffer.h":
    cdef cppclass CIntBuffer:

        CIntBuffer(size_t size_hint)
        inline void append(int64_t value)
        inline void extend(const vector[int64_t]& values)
        inline const int64_t* data() const
        inline size_t size()

cdef class IntBuffer:
    """
    Python wrapper for the C++ IntBuffer class.
    """
    #cdef CIntBuffer* c_buffer

    def __cinit__(self, size_hint: int = 1024):
        self.c_buffer = new CIntBuffer(size_hint)

    def __dealloc__(self):
        del self.c_buffer

    cpdef void append(self, int64_t value):
        """ Append an integer to the buffer. """
        self.c_buffer.append(value)

    cpdef void extend(self, iterable):
        """ Extend the buffer with an iterable of integers. """
        cdef vector[int64_t] values = iterable
        self.c_buffer.extend(values)

    cpdef numpy.ndarray[int64_t, ndim=1] to_numpy(self):
        """ Convert the buffer to a NumPy array by copying data. """
        cdef size_t size = self.c_buffer.size()
        cdef const int64_t* data_ptr = self.c_buffer.data()

        if size == 0:
            return numpy.empty(0, dtype=numpy.int64)  # Handle empty buffer case

        # Allocate a NumPy array and copy data
        arr = numpy.empty(size, dtype=numpy.int64)
        cdef int64_t[::1] arr_view = arr
        for i in range(size):
            arr_view[i] = data_ptr[i]  # Copy values manually

        return arr

    cpdef size_t size(self):
        return self.c_buffer.size()
