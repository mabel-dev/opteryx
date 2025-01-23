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
cimport numpy

from libc.stdint cimport int64_t
from libc.stdlib cimport malloc, realloc, free
from cpython.object cimport PyObject
from cpython.long cimport PyLong_AsLongLong
from cpython.sequence cimport PySequence_Fast, PySequence_Fast_GET_SIZE, PySequence_Fast_GET_ITEM
from cpython.ref cimport Py_DECREF, Py_INCREF


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
        cdef numpy.ndarray[int64_t, ndim=1] new_buffer
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

    cpdef numpy.ndarray[int64_t, ndim=1] to_numpy(self):
        """ Convert the buffer to a NumPy array without copying. """
        return numpy.asarray(self._buffer[:self.size])

    cpdef buffer(self):
        """ Convert the buffer to a NumPy array without copying. """
        return self._buffer[:self.size]

cdef class CIntBuffer:
    """
    CIntBuffer is roughly twice as fast but intermittently sigfaults, the issue appears to be
    with the ownership and releasing/freeing incorrectly. Likely due to pyarrow threading/slicing
    but nothing I've tried has stopped the segfaulting.

    A fast growable integer buffer backed by raw C memory.
    """
    cdef int64_t* data
    cdef public size_t size
    cdef size_t capacity
    cdef bint sealed  # 🚫 Prevents modifications after `to_numpy()`

    def __cinit__(self, size_hint: int = 1024):
        if size_hint < 1:
            size_hint = 1
        self.size = 0
        self.capacity = size_hint
        self.data = <int64_t*> malloc(self.capacity * sizeof(int64_t))
        if self.data == NULL:
            raise MemoryError("Failed to allocate IntBuffer")
        self.sealed = False

    def __dealloc__(self):
        # Always free if data != NULL:
        if self.data != NULL:
            free(self.data)
            self.data = NULL

    cdef inline void ensure_capacity(self, size_t needed) except *:
        """
        Ensures we have enough space. 🚫 Disabled if sealed.
        """
        if self.sealed:
            raise RuntimeError("Cannot modify buffer after exporting to NumPy")

        cdef size_t new_cap
        cdef int64_t* new_data

        if needed <= self.capacity:
            return

        new_cap = self.capacity
        while new_cap < needed:
            new_cap <<= 1

        new_data = <int64_t*> realloc(self.data, new_cap * sizeof(int64_t))
        if new_data == NULL:
            raise MemoryError("Failed to reallocate IntBuffer")

        self.data = new_data
        self.capacity = new_cap

    cpdef void append(self, int64_t value) except *:
        if self.sealed:
            raise RuntimeError("Cannot append after exporting to NumPy")
        if self.size == self.capacity:
            self.ensure_capacity(self.size + 1)
        self.data[self.size] = value
        self.size += 1

    cpdef void extend(self, object iterable) except *:
        """
        Extend the buffer with a Python iterable of integers.
        """
        if self.sealed:
            raise RuntimeError("Cannot extend after exporting to NumPy")

        cdef object seq = PySequence_Fast(iterable, "extend requires an iterable")
        if seq is None:
            raise TypeError("extend requires an iterable")

        cdef Py_ssize_t length = PySequence_Fast_GET_SIZE(seq)
        if length <= 0:
            Py_DECREF(seq)
            return

        self.ensure_capacity(self.size + length)

        cdef Py_ssize_t i
        cdef PyObject* item
        cdef int64_t value

        for i in range(length):
            item = PySequence_Fast_GET_ITEM(seq, i)
            value = PyLong_AsLongLong(<object> item)
            self.data[self.size + i] = value

        self.size += length
        Py_DECREF(seq)

    cpdef numpy.ndarray[int64_t, ndim=1] to_numpy(self):
        """
        Safely converts the buffer into a NumPy array without causing memory issues.

        Returns:
            A NumPy array backed by the buffer's memory (zero-copy).
        """
        if self.sealed:
            raise RuntimeError("Already exported to NumPy.")
        self.sealed = True  # Prevent further modification

        # ✅ Create a NumPy array directly from the buffer
        cdef numpy.ndarray[int64_t, ndim=1] numpy_array
        numpy_array = numpy.PyArray_SimpleNewFromData(
            1, [self.size], numpy.NPY_INT64, <void*>self.data
        )

        # ✅ Prevent NumPy from freeing the memory
        numpy.PyArray_CLEARFLAGS(numpy_array, numpy.NPY_ARRAY_OWNDATA)

        # ✅ Attach `self` as the BaseObject so NumPy keeps `CIntBuffer` alive
        Py_INCREF(self)  # Ensure Python keeps a reference
        numpy.PyArray_SetBaseObject(numpy_array, <object>self)

        return numpy_array
