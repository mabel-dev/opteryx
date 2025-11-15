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

from libc.stddef cimport size_t
from libc.stdint cimport int64_t
from libcpp.vector cimport vector
from libc.string cimport memcpy

cdef extern from "intbuffer.h":
    cdef cppclass CIntBuffer:
        CIntBuffer(size_t size_hint)
        void append(int64_t value) nogil
        void extend(const vector[int64_t]& values) nogil
        void extend(const int64_t* data, size_t count) nogil
        void reserve(size_t additional_capacity) nogil
        const int64_t* data() nogil
        size_t size() nogil
        void append_repeated(int64_t value, size_t count) nogil

cdef class IntBuffer:

    def __cinit__(self, size_t size_hint = 1024):
        self.c_buffer = new CIntBuffer(size_hint)

    def __dealloc__(self):
        del self.c_buffer

    cpdef void append(self, int64_t value):
        """Append an integer to the buffer."""
        self.c_buffer.append(value)

    cpdef void append_batch(self, int64_t[::1] values):
        """Append a batch of values efficiently."""
        cdef size_t n = values.shape[0]
        if n > 0:
            self.c_buffer.extend(&values[0], n)

    cpdef void extend(self, object iterable):
        """Extend the buffer with an iterable of integers."""
        # Fast path for numpy arrays
        if isinstance(iterable, numpy.ndarray):
            arr = numpy.ascontiguousarray(iterable, dtype=numpy.int64)
            self.extend_numpy(arr)
            return

        # Fast path for lists/tuples - pre-allocate and copy
        cdef size_t estimated_size
        estimated_size = len(iterable)

        cdef vector[int64_t] vec
        if estimated_size > 1000:  # For large iterables, use vector
            vec.reserve(estimated_size)
            for item in iterable:
                vec.push_back(item)
            self.c_buffer.extend(vec)
        else:
            # Small iterables - just append one by one
            for item in iterable:
                self.c_buffer.append(item)

    cpdef void extend_numpy(self, numpy.ndarray[int64_t, ndim=1] arr):
        """Extend with numpy array - fastest method."""
        cdef size_t n = arr.shape[0]
        if n > 0:
            self.c_buffer.extend(&arr[0], n)

    cpdef numpy.ndarray[int64_t, ndim=1] to_numpy(self):
        """Convert the buffer to a NumPy array using memcpy."""
        cdef size_t size = self.c_buffer.size()

        if size == 0:
            return numpy.empty(0, dtype=numpy.int64)

        cdef const int64_t* data_ptr = self.c_buffer.data()
        cdef numpy.ndarray[int64_t, ndim=1] arr = numpy.empty(size, dtype=numpy.int64)

        memcpy(<void*>&arr[0], <const void*>data_ptr, size * sizeof(int64_t))
        return arr

    cpdef const int64_t[::1] get_buffer(self):
        """
        Get a read-only memoryview of the underlying buffer (zero-copy).

        This provides direct access to the C++ buffer without copying.
        The memoryview remains valid as long as the IntBuffer exists
        and no modifications are made to it.

        Returns:
            const int64_t[::1]: Read-only memoryview of the buffer
        """
        cdef size_t size = self.c_buffer.size()
        if size == 0:
            return numpy.empty(0, dtype=numpy.int64)

        cdef const int64_t* data_ptr = self.c_buffer.data()
        return <const int64_t[:size]>data_ptr

    cpdef size_t size(self):
        return self.c_buffer.size()

    cpdef void reserve(self, size_t capacity):
        """Reserve capacity for future appends."""
        if capacity == 0:
            return
        self.c_buffer.reserve(capacity)
