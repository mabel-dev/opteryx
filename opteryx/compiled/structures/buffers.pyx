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
from libc.stdint cimport int64_t, int32_t
from libcpp.vector cimport vector
from libc.string cimport memcpy

cdef extern from "intbuffer.h":
    cdef cppclass CIntBuffer:
        CIntBuffer(size_t size_hint)
        void append(int64_t value) nogil
        void extend(const vector[int64_t]& values) nogil
        void extend(const int64_t* data, size_t count) nogil
        void reserve(size_t additional_capacity) nogil
        void resize(size_t new_size) nogil
        const int64_t* data() nogil
        int64_t* mutable_data() nogil
        size_t size() nogil
        void append_repeated(int64_t value, size_t count) nogil

    cdef cppclass CInt32Buffer:
        CInt32Buffer(size_t size_hint)
        void append(int32_t value) nogil
        void extend(const vector[int32_t]& values) nogil
        void extend(const int32_t* data, size_t count) nogil
        void reserve(size_t additional_capacity) nogil
        void resize(size_t new_size) nogil
        const int32_t* data() nogil
        int32_t* mutable_data() nogil
        size_t size() nogil

cdef class IntBuffer:

    def __cinit__(self, size_t size_hint = 1024):
        self.c_buffer = new CIntBuffer(size_hint)
        self._strides[0] = sizeof(int64_t)

    def __getbuffer__(self, Py_buffer *view, int flags):
        cdef Py_ssize_t itemsize = sizeof(int64_t)
        cdef Py_ssize_t n_items = self.c_buffer.size()

        self._shape[0] = n_items

        view.obj = self
        view.buf = <void*>self.c_buffer.mutable_data()
        view.len = n_items * itemsize
        view.readonly = 0
        view.itemsize = itemsize
        view.format = "q"
        view.ndim = 1
        view.shape = self._shape
        view.strides = self._strides
        view.suboffsets = NULL
        view.internal = NULL

    def __releasebuffer__(self, Py_buffer *view):
        pass

    def __len__(self):
        return self.c_buffer.size()

    def __getitem__(self, Py_ssize_t index):
        cdef size_t size = self.c_buffer.size()
        if index < 0:
            index += size
        if index < 0 or index >= size:
            raise IndexError("IntBuffer index out of range")
        return self.c_buffer.data()[index]

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
            self.c_buffer.extend(<int64_t*>arr.data, n)

    cpdef numpy.ndarray[int64_t, ndim=1] to_numpy(self):
        """Convert to numpy array (copy)."""
        cdef size_t n = self.c_buffer.size()
        cdef numpy.ndarray[int64_t, ndim=1] arr = numpy.empty(n, dtype=numpy.int64)
        if n > 0:
            memcpy(arr.data, self.c_buffer.data(), n * sizeof(int64_t))
        return arr

    cpdef const int64_t[::1] get_buffer(self):
        """Get a read-only memoryview of the buffer."""
        cdef size_t n = self.c_buffer.size()
        if n == 0:
            return None
        return <int64_t[:n]>self.c_buffer.data()

    cpdef size_t size(self):
        return self.c_buffer.size()

    cpdef void reserve(self, size_t capacity):
        self.c_buffer.reserve(capacity)


cdef class Int32Buffer:

    def __cinit__(self, size_t size_hint = 1024):
        self.c_buffer = new CInt32Buffer(size_hint)
        self._strides[0] = sizeof(int32_t)

    def __getbuffer__(self, Py_buffer *view, int flags):
        cdef Py_ssize_t itemsize = sizeof(int32_t)
        cdef Py_ssize_t n_items = self.c_buffer.size()

        self._shape[0] = n_items

        view.obj = self
        view.buf = <void*>self.c_buffer.mutable_data()
        view.len = n_items * itemsize
        view.readonly = 0
        view.itemsize = itemsize
        view.format = "i"  # int
        view.ndim = 1
        view.shape = self._shape
        view.strides = self._strides
        view.suboffsets = NULL
        view.internal = NULL

    def __releasebuffer__(self, Py_buffer *view):
        pass

    def __len__(self):
        return self.c_buffer.size()

    def __getitem__(self, Py_ssize_t index):
        cdef size_t size = self.c_buffer.size()
        if index < 0:
            index += size
        if index < 0 or index >= size:
            raise IndexError("Int32Buffer index out of range")
        return self.c_buffer.data()[index]

    def __dealloc__(self):
        del self.c_buffer

    cpdef void append(self, int32_t value):
        """Append an integer to the buffer."""
        self.c_buffer.append(value)

    cpdef void extend(self, object iterable):
        """Extend the buffer with an iterable of integers."""
        # Fast path for numpy arrays
        if isinstance(iterable, numpy.ndarray):
            if iterable.dtype == numpy.int32:
                self.extend_numpy(iterable)
                return
            # else fall through to generic loop

        # Fast path for lists/tuples - pre-allocate and copy
        cdef size_t estimated_size
        estimated_size = len(iterable)

        cdef vector[int32_t] vec
        if estimated_size > 1000:  # For large iterables, use vector
            vec.reserve(estimated_size)
            for item in iterable:
                vec.push_back(item)
            self.c_buffer.extend(vec)
        else:
            # Small iterables - just append one by one
            for item in iterable:
                self.c_buffer.append(item)

    cpdef void extend_numpy(self, numpy.ndarray[int32_t, ndim=1] arr):
        """Extend with numpy array - fastest method."""
        cdef size_t n = arr.shape[0]
        if n > 0:
            self.c_buffer.extend(<int32_t*>arr.data, n)

    cpdef numpy.ndarray[int32_t, ndim=1] to_numpy(self):
        """Convert to numpy array (copy)."""
        cdef size_t n = self.c_buffer.size()
        cdef numpy.ndarray[int32_t, ndim=1] arr = numpy.empty(n, dtype=numpy.int32)
        if n > 0:
            memcpy(arr.data, self.c_buffer.data(), n * sizeof(int32_t))
        return arr

    cpdef size_t size(self):
        return self.c_buffer.size()

    cpdef void reserve(self, size_t capacity):
        self.c_buffer.reserve(capacity)
