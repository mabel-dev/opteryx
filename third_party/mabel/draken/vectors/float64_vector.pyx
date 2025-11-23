# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
Float64Vector: Cython implementation of a fixed-width float64 column vector for Draken.

This module provides:
- The Float64Vector class for efficient float64 column storage and manipulation
- Integration with DrakenFixedBuffer and related C helpers for memory management
- Arrow interoperability for zero-copy conversion
- Fast hashing, comparison, and null handling for float64 columns

Used for high-performance analytics and columnar data processing in Draken.
"""

from cpython.bytes cimport PyBytes_FromStringAndSize, PyBytes_AS_STRING
from cpython.mem cimport PyMem_Malloc
from libc.string cimport memset

from libc.stdint cimport int32_t, int8_t, intptr_t, uint64_t, uint8_t
from libc.stdlib cimport malloc

from opteryx.draken.core.buffers cimport DrakenFixedBuffer
from opteryx.draken.core.buffers cimport DRAKEN_FLOAT64
from opteryx.draken.core.fixed_vector cimport alloc_fixed_buffer, buf_dtype, buf_itemsize, buf_length, free_fixed_buffer
from opteryx.draken.vectors.vector cimport MIX_HASH_CONSTANT, Vector, NULL_HASH, mix_hash, simd_mix_hash

cdef class Float64Vector(Vector):

    def __cinit__(self, size_t length=0, bint wrap=False):
        if wrap:
            self.ptr = NULL
            self.owns_data = False
        else:
            self.ptr = alloc_fixed_buffer(DRAKEN_FLOAT64, length, 8)
            self.owns_data = True

    def __dealloc__(self):
        if self.owns_data and self.ptr is not NULL:
            free_fixed_buffer(self.ptr, True)
            self.ptr = NULL

    # Python-friendly properties
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
        cdef double* data = <double*> ptr.data
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
        """Convert to a PyArrow array."""
        import pyarrow as pa
        
        cdef size_t nbytes = buf_length(self.ptr) * sizeof(double)
        addr = <intptr_t> self.ptr.data
        data_buf = pa.foreign_buffer(addr, nbytes, base=self)

        buffers = []
        if self.ptr.null_bitmap != NULL:
            buffers.append(pa.foreign_buffer(<intptr_t> self.ptr.null_bitmap, (self.ptr.length + 7) // 8, base=self))
        else:
            buffers.append(None)

        buffers.append(data_buf)

        return pa.Array.from_buffers(pa.float64(), buf_length(self.ptr), buffers)

    # -------- Example op --------
    cpdef Float64Vector take(self, int32_t[::1] indices):
        cdef Py_ssize_t i, n = indices.shape[0]
        cdef Float64Vector out = Float64Vector(<size_t>n)
        cdef double* src = <double*> self.ptr.data
        cdef double* dst = <double*> out.ptr.data
        for i in range(n):
            dst[i] = src[indices[i]]
        return out

    cpdef int8_t[::1] equals(self, double value):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef double* data = <double*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        cdef int8_t* buf = <int8_t*> PyMem_Malloc(n)
        if buf == NULL:
            raise MemoryError()
        for i in range(n):
            buf[i] = 1 if data[i] == value else 0
        return <int8_t[:n]> buf

    cpdef int8_t[::1] equals_vector(self, Float64Vector other):
        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef double* data1 = <double*> ptr1.data
        cdef double* data2 = <double*> ptr2.data
        cdef Py_ssize_t i, n = ptr1.length
        if n != ptr2.length:
            raise ValueError("Vectors must have the same length")
        cdef int8_t* buf = <int8_t*> PyMem_Malloc(n)
        if buf == NULL:
            raise MemoryError()
        for i in range(n):
            buf[i] = 1 if data1[i] == data2[i] else 0
        return <int8_t[:n]> buf

    cpdef int8_t[::1] not_equals(self, double value):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef double* data = <double*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        cdef int8_t* buf = <int8_t*> PyMem_Malloc(n)
        if buf == NULL:
            raise MemoryError()
        for i in range(n):
            buf[i] = 1 if data[i] != value else 0
        return <int8_t[:n]> buf

    cpdef int8_t[::1] not_equals_vector(self, Float64Vector other):
        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef double* data1 = <double*> ptr1.data
        cdef double* data2 = <double*> ptr2.data
        cdef Py_ssize_t i, n = ptr1.length
        if n != ptr2.length:
            raise ValueError("Vectors must have the same length")
        cdef int8_t* buf = <int8_t*> PyMem_Malloc(n)
        if buf == NULL:
            raise MemoryError()
        for i in range(n):
            buf[i] = 1 if data1[i] != data2[i] else 0
        return <int8_t[:n]> buf

    cpdef int8_t[::1] greater_than(self, double value):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef double* data = <double*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        cdef int8_t* buf = <int8_t*> PyMem_Malloc(n)
        if buf == NULL:
            raise MemoryError()
        for i in range(n):
            buf[i] = 1 if data[i] > value else 0
        return <int8_t[:n]> buf

    cpdef int8_t[::1] greater_than_vector(self, Float64Vector other):
        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef double* data1 = <double*> ptr1.data
        cdef double* data2 = <double*> ptr2.data
        cdef Py_ssize_t i, n = ptr1.length
        if n != ptr2.length:
            raise ValueError("Vectors must have the same length")
        cdef int8_t* buf = <int8_t*> PyMem_Malloc(n)
        if buf == NULL:
            raise MemoryError()
        for i in range(n):
            buf[i] = 1 if data1[i] > data2[i] else 0
        return <int8_t[:n]> buf

    cpdef int8_t[::1] greater_than_or_equals(self, double value):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef double* data = <double*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        cdef int8_t* buf = <int8_t*> PyMem_Malloc(n)
        if buf == NULL:
            raise MemoryError()
        for i in range(n):
            buf[i] = 1 if data[i] >= value else 0
        return <int8_t[:n]> buf

    cpdef int8_t[::1] greater_than_or_equals_vector(self, Float64Vector other):
        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef double* data1 = <double*> ptr1.data
        cdef double* data2 = <double*> ptr2.data
        cdef Py_ssize_t i, n = ptr1.length
        if n != ptr2.length:
            raise ValueError("Vectors must have the same length")
        cdef int8_t* buf = <int8_t*> PyMem_Malloc(n)
        if buf == NULL:
            raise MemoryError()
        for i in range(n):
            buf[i] = 1 if data1[i] >= data2[i] else 0
        return <int8_t[:n]> buf

    cpdef int8_t[::1] less_than(self, double value):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef double* data = <double*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        cdef int8_t* buf = <int8_t*> PyMem_Malloc(n)
        if buf == NULL:
            raise MemoryError()
        for i in range(n):
            buf[i] = 1 if data[i] < value else 0
        return <int8_t[:n]> buf

    cpdef int8_t[::1] less_than_vector(self, Float64Vector other):
        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef double* data1 = <double*> ptr1.data
        cdef double* data2 = <double*> ptr2.data
        cdef Py_ssize_t i, n = ptr1.length
        if n != ptr2.length:
            raise ValueError("Vectors must have the same length")
        cdef int8_t* buf = <int8_t*> PyMem_Malloc(n)
        if buf == NULL:
            raise MemoryError()
        for i in range(n):
            buf[i] = 1 if data1[i] < data2[i] else 0
        return <int8_t[:n]> buf

    cpdef int8_t[::1] less_than_or_equals(self, double value):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef double* data = <double*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        cdef int8_t* buf = <int8_t*> PyMem_Malloc(n)
        if buf == NULL:
            raise MemoryError()
        for i in range(n):
            buf[i] = 1 if data[i] <= value else 0
        return <int8_t[:n]> buf

    cpdef int8_t[::1] less_than_or_equals_vector(self, Float64Vector other):
        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef double* data1 = <double*> ptr1.data
        cdef double* data2 = <double*> ptr2.data
        cdef Py_ssize_t i, n = ptr1.length
        if n != ptr2.length:
            raise ValueError("Vectors must have the same length")
        cdef int8_t* buf = <int8_t*> PyMem_Malloc(n)
        if buf == NULL:
            raise MemoryError()
        for i in range(n):
            buf[i] = 1 if data1[i] <= data2[i] else 0
        return <int8_t[:n]> buf

    cpdef double sum(self):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef double* data = <double*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        cdef double total = 0.0
        for i in range(n):
            total += data[i]
        return total

    cpdef double min(self):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef double* data = <double*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        if n == 0:
            raise ValueError("Cannot compute min of empty column")
        cdef double m = data[0]
        for i in range(1, n):
            if data[i] < m:
                m = data[i]
        return m

    cpdef double max(self):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef double* data = <double*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        if n == 0:
            raise ValueError("Cannot compute max of empty column")
        cdef double m = data[0]
        for i in range(1, n):
            if data[i] > m:
                m = data[i]
        return m

    cpdef int8_t[::1] is_null(self):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        cdef int8_t* buf = <int8_t*> PyMem_Malloc(n)
        cdef uint8_t byte, bit

        if buf == NULL:
            raise MemoryError()

        if ptr.null_bitmap == NULL:
            for i in range(n):
                buf[i] = 0
        else:
            for i in range(n):
                byte = ptr.null_bitmap[i >> 3]
                bit = (byte >> (i & 7)) & 1
                buf[i] = 0 if bit else 1

        return <int8_t[:n]> buf

    @property
    def null_count(self):
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
        cdef double* data = <double*> ptr.data
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

    cdef void hash_into(
        self,
        uint64_t[::1] out_buf,
        Py_ssize_t offset=0,
    ) except *:
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef double* data = <double*> ptr.data
        cdef Py_ssize_t n = ptr.length

        if n == 0:
            return
        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("Float64Vector.hash_into: output buffer too small")

        cdef uint64_t* bits = <uint64_t*> data
        cdef uint64_t* dst = &out_buf[offset]
        cdef uint8_t* null_bitmap = ptr.null_bitmap
        cdef bint has_nulls = null_bitmap != NULL
        cdef Py_ssize_t i
        cdef uint8_t byte
        cdef uint64_t value

        # Use shared MIX_HASH_CONSTANT directly; no need to pass it in.
        if has_nulls:
            for i in range(n):
                byte = null_bitmap[i >> 3]
                if byte & (1 << (i & 7)):
                    value = bits[i]
                else:
                    value = NULL_HASH
                dst[i] = mix_hash(dst[i], value)
        else:
            simd_mix_hash(dst, bits, <size_t>n)
            return

    def __str__(self):
        cdef list vals = []
        cdef Py_ssize_t i, k = min(<Py_ssize_t>buf_length(self.ptr), 10)
        cdef double* data = <double*> self.ptr.data
        for i in range(k):
            vals.append(data[i])
        return f"<Float64Vector len={buf_length(self.ptr)} values={vals}>"


cdef Float64Vector from_arrow(object array):
    cdef Float64Vector vec = Float64Vector(0, True)
    vec.ptr = <DrakenFixedBuffer*> malloc(sizeof(DrakenFixedBuffer))
    if vec.ptr == NULL:
        raise MemoryError()
    vec.owns_data = False

    cdef object bufs = array.buffers()
    # Keep references to prevent GC
    vec._arrow_null_buf = bufs[0]
    vec._arrow_data_buf = bufs[1]

    cdef intptr_t base_ptr = <intptr_t> bufs[1].address
    cdef size_t itemsize = 8
    cdef Py_ssize_t offset = array.offset
    cdef intptr_t nb_addr

    vec.ptr.type = DRAKEN_FLOAT64
    vec.ptr.itemsize = itemsize
    vec.ptr.length = <size_t> len(array)

    cdef intptr_t addr = base_ptr + offset * itemsize
    vec.ptr.data = <void*> addr

    # Null bitmap handling with offset support
    cdef Py_ssize_t nb_size
    cdef uint8_t* src_bitmap
    cdef uint8_t* dst_bitmap
    cdef Py_ssize_t i
    cdef object new_bitmap_bytes

    if bufs[0] is not None:
        nb_addr = bufs[0].address
        
        if offset % 8 == 0:
            vec.ptr.null_bitmap = (<uint8_t*> nb_addr) + (offset >> 3)
        else:
            # Unaligned offset: must copy and shift
            nb_size = (len(array) + 7) // 8
            new_bitmap_bytes = PyBytes_FromStringAndSize(NULL, nb_size)
            dst_bitmap = <uint8_t*> PyBytes_AS_STRING(new_bitmap_bytes)
            memset(dst_bitmap, 0, nb_size)
            
            src_bitmap = <uint8_t*> nb_addr
            
            # Copy bits shifting them
            for i in range(len(array)):
                if (src_bitmap[(offset + i) >> 3] >> ((offset + i) & 7)) & 1:
                    dst_bitmap[i >> 3] |= (1 << (i & 7))
            
            vec.ptr.null_bitmap = dst_bitmap
            vec._arrow_null_buf = new_bitmap_bytes
    else:
        vec.ptr.null_bitmap = NULL

    return vec


cdef Float64Vector from_sequence(double[::1] data):
    """
    Create Float64Vector from a typed double memoryview (zero-copy).
    
    Args:
        data: double[::1] memoryview (C-contiguous)
    
    Returns:
        Float64Vector wrapping the memoryview data
    """
    cdef Float64Vector vec = Float64Vector(0, True)
    vec.ptr = <DrakenFixedBuffer*> malloc(sizeof(DrakenFixedBuffer))
    if vec.ptr == NULL:
        raise MemoryError()
    vec.owns_data = False

    # Keep reference to prevent GC
    vec._arrow_data_buf = data.base if data.base is not None else data
    vec._arrow_null_buf = None

    vec.ptr.type = DRAKEN_FLOAT64
    vec.ptr.itemsize = 8
    vec.ptr.length = <size_t> data.shape[0]
    vec.ptr.data = <void*> &data[0]
    vec.ptr.null_bitmap = NULL

    return vec
