# distutils: language = c++
# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport int64_t

import numpy
cimport numpy as cnp

cdef class IntBuffer:
    """
    A fast integer buffer using Cython-managed memory.
    """
    cdef public int64_t[:] _buffer
    cdef public size_t size
    cdef public size_t capacity

    def __cinit__(self, size_hint: int = 1024):
        self.capacity = size_hint
        self._buffer = numpy.zeros(self.capacity, dtype=numpy.int64)
        self.size = 0

    cpdef void append(self, int64_t value):
        """ Append an integer to the buffer. """
        cdef cnp.ndarray[int64_t, ndim=1] new_buffer
        if self.size == self.capacity:
            self.capacity *= 2
            new_buffer = numpy.zeros(self.capacity, dtype=numpy.int64)
            new_buffer[:self.size] = self._buffer
            self._buffer = new_buffer
        self._buffer[self.size] = value
        self.size += 1

    cpdef void extend(self, iterable):
        """ Extend the buffer with an iterable of integers. """
        cdef int64_t i
        for i in range(len(iterable)):
            self.append(iterable[i])

    cpdef cnp.ndarray[int64_t, ndim=1] to_numpy(self):
        """ Convert the buffer to a NumPy array without copying. """
        return numpy.asarray(self._buffer[:self.size])

    cpdef buffer(self):
        """ Convert the buffer to a NumPy array without copying. """
        return self._buffer[:self.size]
