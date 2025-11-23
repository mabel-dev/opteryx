# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
Int64Vector: Cython implementation of a fixed-width int64 column vector for Draken.

This module provides:
- The Int64Vector class for efficient int64 column storage and manipulation
- Integration with DrakenFixedBuffer and related C helpers for memory management
- Arrow interoperability for zero-copy conversion
- Fast hashing, comparison, and null handling for int64 columns

Used for high-performance analytics and columnar data processing in Draken.
"""

from cpython.bytes cimport PyBytes_FromStringAndSize, PyBytes_AS_STRING
from cpython.mem cimport PyMem_Malloc
from libc.string cimport memset

from libc.stddef cimport size_t
from libc.stdint cimport int32_t
from libc.stdint cimport int64_t
from libc.stdint cimport int8_t
from libc.stdint cimport intptr_t
from libc.stdint cimport uint64_t
from libc.stdint cimport uint8_t
from libc.stdlib cimport malloc

from opteryx.draken.core.buffers cimport DrakenFixedBuffer
from opteryx.draken.core.buffers cimport DRAKEN_INT64
from opteryx.draken.core.fixed_vector cimport alloc_fixed_buffer
from opteryx.draken.core.fixed_vector cimport buf_dtype
from opteryx.draken.core.fixed_vector cimport buf_itemsize
from opteryx.draken.core.fixed_vector cimport buf_length
from opteryx.draken.core.fixed_vector cimport free_fixed_buffer
from opteryx.draken.vectors.vector cimport (
    MIX_HASH_CONSTANT,
    NULL_HASH,
    Vector,
    mix_hash,
    simd_mix_hash,
)

from opteryx.draken.vectors.bool_vector cimport BoolVector

cdef class Int64Vector(Vector):

    def __cinit__(self, size_t length=0, bint wrap=False):
        """
        length>0, wrap=False  -> allocate new owned buffer
        wrap=True             -> do not allocate; caller will set ptr & metadata
        """
        if wrap:
            self.ptr = NULL
            self.owns_data = False
        else:
            self.ptr = alloc_fixed_buffer(DRAKEN_INT64, length, 8)
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
        cdef int64_t* data = <int64_t*> ptr.data
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

        cdef size_t nbytes = buf_length(self.ptr) * buf_itemsize(self.ptr)
        addr = <intptr_t> self.ptr.data
        data_buf = pa.foreign_buffer(addr, nbytes, base=self)

        buffers = []
        if self.ptr.null_bitmap != NULL:
            buffers.append(
                pa.foreign_buffer(
                    <intptr_t> self.ptr.null_bitmap,
                    (self.ptr.length + 7) // 8,
                    base=self,
                )
            )
        else:
            buffers.append(None)

        buffers.append(data_buf)

        return pa.Array.from_buffers(pa.int64(), buf_length(self.ptr), buffers)

    # -------- Example op --------
    cpdef Int64Vector take(self, int32_t[::1] indices):
        cdef Py_ssize_t i, n = indices.shape[0]
        cdef Int64Vector out = Int64Vector(<size_t>n)
        cdef int64_t* src = <int64_t*> self.ptr.data
        cdef int64_t* dst = <int64_t*> out.ptr.data
        cdef uint8_t* src_null = <uint8_t*> self.ptr.null_bitmap

        # If source has no null bitmap, copy directly
        if src_null == NULL:
            for i in range(n):
                dst[i] = src[indices[i]]
            out.ptr.null_bitmap = NULL
            return out

        # Source has nulls - allocate a null bitmap for the output and preserve nulls
        cdef Py_ssize_t out_nbytes = (n + 7) >> 3
        cdef uint8_t* out_null = <uint8_t*> malloc(out_nbytes)
        if out_null == NULL:
            raise MemoryError()
        # zero-initialize
        for i in range(out_nbytes):
            out_null[i] = 0

        cdef int32_t src_idx
        cdef uint8_t byte
        for i in range(n):
            src_idx = indices[i]
            byte = src_null[src_idx >> 3]
            if byte & (1 << (src_idx & 7)):
                dst[i] = src[src_idx]
                out_null[i >> 3] |= (1 << (i & 7))
            else:
                dst[i] = 0

        out.ptr.null_bitmap = out_null
        return out

    cpdef BoolVector equals(self, int64_t value):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int64_t* data = <int64_t*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t> n)
        cdef uint8_t* dst = <uint8_t*> out.ptr.data

        # zero init
        for i in range(nbytes):
            dst[i] = 0
        for i in range(n):
            if data[i] == value:
                dst[i >> 3] |= (1 << (i & 7))
        out.ptr.null_bitmap = NULL
        return out

    cpdef BoolVector equals_vector(self, Int64Vector other):
        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef int64_t* data1 = <int64_t*> ptr1.data
        cdef int64_t* data2 = <int64_t*> ptr2.data
        cdef Py_ssize_t i, n = ptr1.length
        if n != ptr2.length:
            raise ValueError("Vectors must have the same length")
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t> n)
        cdef uint8_t* dst = <uint8_t*> out.ptr.data
        for i in range(nbytes):
            dst[i] = 0
        for i in range(n):
            if data1[i] == data2[i]:
                dst[i >> 3] |= (1 << (i & 7))
        out.ptr.null_bitmap = NULL
        return out

    cpdef BoolVector not_equals(self, int64_t value):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int64_t* data = <int64_t*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t> n)
        cdef uint8_t* dst = <uint8_t*> out.ptr.data
        for i in range(nbytes):
            dst[i] = 0
        for i in range(n):
            if data[i] != value:
                dst[i >> 3] |= (1 << (i & 7))
        out.ptr.null_bitmap = NULL
        return out

    cpdef BoolVector not_equals_vector(self, Int64Vector other):
        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef int64_t* data1 = <int64_t*> ptr1.data
        cdef int64_t* data2 = <int64_t*> ptr2.data
        cdef Py_ssize_t i, n = ptr1.length
        if n != ptr2.length:
            raise ValueError("Vectors must have the same length")
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t> n)
        cdef uint8_t* dst = <uint8_t*> out.ptr.data
        for i in range(nbytes):
            dst[i] = 0
        for i in range(n):
            if data1[i] != data2[i]:
                dst[i >> 3] |= (1 << (i & 7))
        out.ptr.null_bitmap = NULL
        return out

    cpdef BoolVector greater_than(self, int64_t value):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int64_t* data = <int64_t*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t> n)
        cdef uint8_t* dst = <uint8_t*> out.ptr.data
        for i in range(nbytes):
            dst[i] = 0
        for i in range(n):
            if data[i] > value:
                dst[i >> 3] |= (1 << (i & 7))
        out.ptr.null_bitmap = NULL
        return out

    cpdef BoolVector greater_than_vector(self, Int64Vector other):
        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef int64_t* data1 = <int64_t*> ptr1.data
        cdef int64_t* data2 = <int64_t*> ptr2.data
        cdef Py_ssize_t i, n = ptr1.length
        if n != ptr2.length:
            raise ValueError("Vectors must have the same length")
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t> n)
        cdef uint8_t* dst = <uint8_t*> out.ptr.data
        for i in range(nbytes):
            dst[i] = 0
        for i in range(n):
            if data1[i] > data2[i]:
                dst[i >> 3] |= (1 << (i & 7))
        out.ptr.null_bitmap = NULL
        return out

    cpdef BoolVector greater_than_or_equals(self, int64_t value):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int64_t* data = <int64_t*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t> n)
        cdef uint8_t* dst = <uint8_t*> out.ptr.data
        for i in range(nbytes):
            dst[i] = 0
        for i in range(n):
            if data[i] >= value:
                dst[i >> 3] |= (1 << (i & 7))
        out.ptr.null_bitmap = NULL
        return out

    cpdef BoolVector greater_than_or_equals_vector(self, Int64Vector other):
        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef int64_t* data1 = <int64_t*> ptr1.data
        cdef int64_t* data2 = <int64_t*> ptr2.data
        cdef Py_ssize_t i, n = ptr1.length
        if n != ptr2.length:
            raise ValueError("Vectors must have the same length")
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t> n)
        cdef uint8_t* dst = <uint8_t*> out.ptr.data
        for i in range(nbytes):
            dst[i] = 0
        for i in range(n):
            if data1[i] >= data2[i]:
                dst[i >> 3] |= (1 << (i & 7))
        out.ptr.null_bitmap = NULL
        return out

    cpdef BoolVector less_than(self, int64_t value):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int64_t* data = <int64_t*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t> n)
        cdef uint8_t* dst = <uint8_t*> out.ptr.data
        for i in range(nbytes):
            dst[i] = 0
        for i in range(n):
            if data[i] < value:
                dst[i >> 3] |= (1 << (i & 7))
        out.ptr.null_bitmap = NULL
        return out

    cpdef BoolVector less_than_vector(self, Int64Vector other):
        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef int64_t* data1 = <int64_t*> ptr1.data
        cdef int64_t* data2 = <int64_t*> ptr2.data
        cdef Py_ssize_t i, n = ptr1.length
        if n != ptr2.length:
            raise ValueError("Vectors must have the same length")
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t> n)
        cdef uint8_t* dst = <uint8_t*> out.ptr.data
        for i in range(nbytes):
            dst[i] = 0
        for i in range(n):
            if data1[i] < data2[i]:
                dst[i >> 3] |= (1 << (i & 7))
        out.ptr.null_bitmap = NULL
        return out

    cpdef BoolVector less_than_or_equals(self, int64_t value):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int64_t* data = <int64_t*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t> n)
        cdef uint8_t* dst = <uint8_t*> out.ptr.data
        for i in range(nbytes):
            dst[i] = 0
        for i in range(n):
            if data[i] <= value:
                dst[i >> 3] |= (1 << (i & 7))
        out.ptr.null_bitmap = NULL
        return out

    cpdef BoolVector less_than_or_equals_vector(self, Int64Vector other):
        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef int64_t* data1 = <int64_t*> ptr1.data
        cdef int64_t* data2 = <int64_t*> ptr2.data
        cdef Py_ssize_t i, n = ptr1.length
        if n != ptr2.length:
            raise ValueError("Vectors must have the same length")
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t> n)
        cdef uint8_t* dst = <uint8_t*> out.ptr.data
        for i in range(nbytes):
            dst[i] = 0
        for i in range(n):
            if data1[i] <= data2[i]:
                dst[i >> 3] |= (1 << (i & 7))
        out.ptr.null_bitmap = NULL
        return out

    cpdef int64_t sum(self):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int64_t* data = <int64_t*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        cdef int64_t total = 0
        for i in range(n):
            total += data[i]
        return total

    cpdef int64_t min(self):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int64_t* data = <int64_t*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        if n == 0:
            raise ValueError("Cannot compute min of empty column")
        cdef int64_t m = data[0]
        for i in range(1, n):
            if data[i] < m:
                m = data[i]
        return m

    cpdef int64_t max(self):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int64_t* data = <int64_t*> ptr.data
        cdef Py_ssize_t i, n = ptr.length
        if n == 0:
            raise ValueError("Cannot compute max of empty column")
        cdef int64_t m = data[0]
        for i in range(1, n):
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
        cdef int64_t* data = <int64_t*> ptr.data
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

    cdef inline void hash_into(
        self,
        uint64_t[::1] out_buf,
        Py_ssize_t offset=0
    ) except *:
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int64_t* data = <int64_t*> ptr.data
        cdef Py_ssize_t n = ptr.length
        cdef uint64_t* dst_base

        if n == 0:
            return

        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("Int64Vector.hash_into: output buffer too small")
        dst_base = &out_buf[0]

        cdef Py_ssize_t i
        cdef uint8_t byte
        cdef uint64_t value
        cdef uint64_t* dst = dst_base + offset
        cdef uint64_t* as_uint64 = <uint64_t*> data
        cdef uint8_t* null_bitmap = ptr.null_bitmap
        cdef bint has_nulls = null_bitmap != NULL

        if has_nulls:
            for i in range(n):
                byte = null_bitmap[i >> 3]
                if byte & (1 << (i & 7)):
                    value = <uint64_t> data[i]
                else:
                    value = NULL_HASH
                dst[i] = mix_hash(dst[i], value)
        else:
            simd_mix_hash(dst, as_uint64, <size_t>n)

    def __str__(self):
        cdef list vals = []
        cdef Py_ssize_t i, k = min(<Py_ssize_t>buf_length(self.ptr), 10)
        cdef int64_t* data = <int64_t*> self.ptr.data
        for i in range(k):
            vals.append(data[i])
        return f"<Int64Vector len={buf_length(self.ptr)} values={vals}>"


cdef Int64Vector from_arrow(object array):
    cdef Int64Vector vec = Int64Vector(0, True)   # wrap=True: no alloc
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

    vec.ptr.type = DRAKEN_INT64
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


cdef Int64Vector from_sequence(int64_t[::1] data):
    """
    Create Int64Vector from a typed int64 memoryview (zero-copy).
    
    Args:
        data: int64_t[::1] memoryview (C-contiguous)
    
    Returns:
        Int64Vector wrapping the memoryview data
    """
    cdef Int64Vector vec = Int64Vector(0, True)   # wrap=True: no alloc
    vec.ptr = <DrakenFixedBuffer*> malloc(sizeof(DrakenFixedBuffer))
    if vec.ptr == NULL:
        raise MemoryError()
    vec.owns_data = False

    # Keep reference to prevent GC
    vec._arrow_data_buf = data.base if data.base is not None else data
    vec._arrow_null_buf = None

    vec.ptr.type = DRAKEN_INT64
    vec.ptr.itemsize = 8
    vec.ptr.length = <size_t> data.shape[0]
    vec.ptr.data = <void*> &data[0]
    vec.ptr.null_bitmap = NULL

    return vec
