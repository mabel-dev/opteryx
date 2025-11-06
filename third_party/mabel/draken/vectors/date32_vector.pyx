# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
Date32Vector: Cython implementation of a fixed-width date32 column vector for Draken.

This module provides:
- The Date32Vector class for efficient date32 column storage (days since Unix epoch)
- Integration with DrakenFixedBuffer and related C helpers for memory management
- Arrow interoperability for zero-copy conversion
- Fast comparison and null handling for date32 columns

Used for high-performance temporal analytics and columnar data processing in Draken.
"""

from cpython.mem cimport PyMem_Malloc

from libc.stdint cimport int32_t
from libc.stdint cimport int8_t
from libc.stdint cimport intptr_t
from libc.stdint cimport uint64_t
from libc.stdint cimport uint8_t
from libc.stdlib cimport malloc

from draken.core.buffers cimport DrakenFixedBuffer
from draken.core.buffers cimport DRAKEN_DATE32
from draken.core.fixed_vector cimport alloc_fixed_buffer
from draken.core.fixed_vector cimport buf_dtype
from draken.core.fixed_vector cimport buf_itemsize
from draken.core.fixed_vector cimport buf_length
from draken.core.fixed_vector cimport free_fixed_buffer
from draken.vectors.vector cimport Vector
from draken._optional import require_pyarrow

# NULL_HASH constant for null hash entries
cdef uint64_t NULL_HASH = <uint64_t>0x9e3779b97f4a7c15

cdef class Date32Vector(Vector):

    def __cinit__(self, size_t length=0, bint wrap=False):
        """
        length>0, wrap=False  -> allocate new owned buffer
        wrap=True             -> do not allocate; caller will set ptr & metadata
        """
        if wrap:
            self.ptr = NULL
            self.owns_data = False
        else:
            self.ptr = alloc_fixed_buffer(DRAKEN_DATE32, length, 4)
            self.owns_data = True

    def __dealloc__(self):
        # Only free if we own the data and the pointer is not NULL
        if self.owns_data and self.ptr is not NULL:
            free_fixed_buffer(self.ptr, True)
            self.ptr = NULL

    # Python-friendly properties (backed by C getters for kernels)
    @property
    def length(self):
        return buf_length(self.ptr)

    @property
    def itemsize(self):
        return buf_itemsize(self.ptr)

    @property
    def dtype(self):
        return buf_dtype(self.ptr)

    def __getitem__(self, Py_ssize_t i):
        """Return the value at index i, or None if null."""
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int32_t* data = <int32_t*> ptr.data
        if i < 0 or i >= ptr.length:
            raise IndexError("Index out of bounds")
        if ptr.null_bitmap != NULL:
            byte = ptr.null_bitmap[i >> 3]
            bit = (byte >> (i & 7)) & 1
            if not bit:
                return None
        return data[i]

    # -------- Interop (owned -> Arrow) --------
    def to_arrow(self):
        pa = require_pyarrow("Date32Vector.to_arrow()")
        cdef size_t nbytes = buf_length(self.ptr) * buf_itemsize(self.ptr)
        addr = <intptr_t> self.ptr.data
        data_buf = pa.foreign_buffer(addr, nbytes, base=self)

        buffers = []
        if self.ptr.null_bitmap != NULL:
            buffers.append(pa.foreign_buffer(<intptr_t> self.ptr.null_bitmap, (self.ptr.length + 7) // 8, base=self))
        else:
            buffers.append(None)

        buffers.append(data_buf)

        return pa.Array.from_buffers(pa.date32(), buf_length(self.ptr), buffers)

    # -------- Example op --------
    cpdef Date32Vector take(self, int32_t[::1] indices):
        cdef Py_ssize_t i, n = indices.shape[0]
        cdef Date32Vector out = Date32Vector(<size_t>n)
        cdef int32_t* src = <int32_t*> self.ptr.data
        cdef int32_t* dst = <int32_t*> out.ptr.data
        for i in range(n):
            dst[i] = src[indices[i]]
        return out

    cpdef int8_t[::1] equals(self, int32_t value):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int32_t* data = <int32_t*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        cdef int8_t* buf = <int8_t*> PyMem_Malloc(n)
        if buf == NULL:
            raise MemoryError()
        for i in range(n):
            buf[i] = 1 if data[i] == value else 0
        return <int8_t[:n]> buf

    cpdef int8_t[::1] not_equals(self, int32_t value):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int32_t* data = <int32_t*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        cdef int8_t* buf = <int8_t*> PyMem_Malloc(n)
        if buf == NULL:
            raise MemoryError()
        for i in range(n):
            buf[i] = 1 if data[i] != value else 0
        return <int8_t[:n]> buf

    cpdef int8_t[::1] greater_than(self, int32_t value):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int32_t* data = <int32_t*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        cdef int8_t* buf = <int8_t*> PyMem_Malloc(n)
        if buf == NULL:
            raise MemoryError()
        for i in range(n):
            buf[i] = 1 if data[i] > value else 0
        return <int8_t[:n]> buf

    cpdef int8_t[::1] greater_than_or_equals(self, int32_t value):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int32_t* data = <int32_t*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        cdef int8_t* buf = <int8_t*> PyMem_Malloc(n)
        if buf == NULL:
            raise MemoryError()
        for i in range(n):
            buf[i] = 1 if data[i] >= value else 0
        return <int8_t[:n]> buf

    cpdef int8_t[::1] less_than(self, int32_t value):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int32_t* data = <int32_t*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        cdef int8_t* buf = <int8_t*> PyMem_Malloc(n)
        if buf == NULL:
            raise MemoryError()
        for i in range(n):
            buf[i] = 1 if data[i] < value else 0
        return <int8_t[:n]> buf

    cpdef int8_t[::1] less_than_or_equals(self, int32_t value):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int32_t* data = <int32_t*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        cdef int8_t* buf = <int8_t*> PyMem_Malloc(n)
        if buf == NULL:
            raise MemoryError()
        for i in range(n):
            buf[i] = 1 if data[i] <= value else 0
        return <int8_t[:n]> buf

    cpdef int32_t min(self):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int32_t* data = <int32_t*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        if n == 0:
            raise ValueError("Cannot compute min of empty column")

        cdef int32_t m
        cdef bint found = False
        cdef uint8_t byte, bit

        # Find first non-null value
        for i in range(n):
            if ptr.null_bitmap != NULL:
                byte = ptr.null_bitmap[i >> 3]
                bit = (byte >> (i & 7)) & 1
                if not bit:  # null
                    continue
            m = data[i]
            found = True
            break

        if not found:
            raise ValueError("Cannot compute min of all-null column")

        # Find minimum among remaining values
        for i in range(i + 1, n):
            if ptr.null_bitmap != NULL:
                byte = ptr.null_bitmap[i >> 3]
                bit = (byte >> (i & 7)) & 1
                if not bit:  # null
                    continue
            if data[i] < m:
                m = data[i]
        return m

    cpdef int32_t max(self):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int32_t* data = <int32_t*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        if n == 0:
            raise ValueError("Cannot compute max of empty column")

        cdef int32_t m
        cdef bint found = False
        cdef uint8_t byte, bit

        # Find first non-null value
        for i in range(n):
            if ptr.null_bitmap != NULL:
                byte = ptr.null_bitmap[i >> 3]
                bit = (byte >> (i & 7)) & 1
                if not bit:  # null
                    continue
            m = data[i]
            found = True
            break

        if not found:
            raise ValueError("Cannot compute max of all-null column")

        # Find maximum among remaining values
        for i in range(i + 1, n):
            if ptr.null_bitmap != NULL:
                byte = ptr.null_bitmap[i >> 3]
                bit = (byte >> (i & 7)) & 1
                if not bit:  # null
                    continue
            if data[i] > m:
                m = data[i]
        return m

    cpdef int8_t[::1] is_null(self):
        """
        Return a memoryview of int8_t, where each element is 1 if the value is null, 0 otherwise.
        """
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        cdef int8_t* buf = <int8_t*> PyMem_Malloc(n)
        cdef uint8_t byte, bit

        if buf == NULL:
            raise MemoryError()

        if ptr.null_bitmap == NULL:
            # No nulls — fill with 0
            for i in range(n):
                buf[i] = 0
        else:
            # Extract null bits — 1 means valid, so invert for null
            for i in range(n):
                byte = ptr.null_bitmap[i >> 3]
                bit = (byte >> (i & 7)) & 1
                buf[i] = 0 if bit else 1

        return <int8_t[:n]> buf

    @property
    def null_count(self):
        """Return the number of nulls in the vector."""
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        cdef Py_ssize_t count = 0
        cdef uint8_t byte, bit
        if ptr.null_bitmap == NULL:
            return 0
        for i in range(n):
            byte = ptr.null_bitmap[i >> 3]
            bit = (byte >> (i & 7)) & 1
            if not bit:
                count += 1
        return count

    cpdef list to_pylist(self):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int32_t* data = <int32_t*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        cdef list out = []
        cdef uint8_t byte, bit

        if ptr.null_bitmap == NULL:
            for i in range(n):
                out.append(data[i])
        else:
            for i in range(n):
                byte = ptr.null_bitmap[i >> 3]
                bit = (byte >> (i & 7)) & 1
                if bit:
                    out.append(data[i])
                else:
                    out.append(None)

        return out

    cpdef uint64_t[::1] hash(self):
        """
        Produce lightweight 64-bit hashes from int32_t data using a fast XOR mix.
        Null entries are assigned a fixed hash value (NULL_HASH).
        """
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int32_t* data = <int32_t*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        cdef uint64_t* buf = <uint64_t*> PyMem_Malloc(n * sizeof(uint64_t))
        if buf == NULL:
            raise MemoryError()

        cdef uint64_t x
        cdef uint8_t byte, bit
        for i in range(n):
            if ptr.null_bitmap != NULL:
                byte = ptr.null_bitmap[i >> 3]
                bit = (byte >> (i & 7)) & 1
                if not bit:
                    buf[i] = NULL_HASH
                    continue

            x = <uint64_t> data[i]
            buf[i] = (x ^ (x >> 33)) * <uint64_t>0xff51afd7ed558ccdU

        return <uint64_t[:n]> buf

    def __str__(self):
        cdef list vals = []
        cdef Py_ssize_t i, k = min(<Py_ssize_t>buf_length(self.ptr), 10)
        cdef int32_t* data = <int32_t*> self.ptr.data
        for i in range(k):
            vals.append(data[i])
        return f"<Date32Vector len={buf_length(self.ptr)} values={vals}>"


cdef Date32Vector from_arrow(object array):
    cdef Date32Vector vec = Date32Vector(0, True)   # wrap=True: no alloc
    vec.ptr = <DrakenFixedBuffer*> malloc(sizeof(DrakenFixedBuffer))
    if vec.ptr == NULL:
        raise MemoryError()
    vec.owns_data = False

    cdef object bufs = array.buffers()
    cdef intptr_t base_ptr = <intptr_t> bufs[1].address
    cdef size_t itemsize = 4
    cdef Py_ssize_t offset = array.offset
    cdef intptr_t nb_addr

    vec.ptr.type = DRAKEN_DATE32
    vec.ptr.itemsize = itemsize
    vec.ptr.length = <size_t> len(array)

    cdef intptr_t addr = base_ptr + offset * itemsize
    vec.ptr.data = <void*> addr

    if bufs[0] is not None:
        nb_addr = bufs[0].address
        vec.ptr.null_bitmap = <uint8_t*> nb_addr
    else:
        vec.ptr.null_bitmap = NULL

    return vec
