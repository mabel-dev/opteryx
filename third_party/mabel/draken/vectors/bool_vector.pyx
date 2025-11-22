# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
BoolVector: Cython implementation of a zero-copy, bit-packed boolean column vector for Draken.

This matches Arrow's representation:
- Values are stored bit-packed in data buffer (1 bit per value).
- Nulls are stored in the null_bitmap (same layout).
- Zero-copy interop with Arrow via from_arrow/to_arrow.

"""

from cpython.bytes cimport PyBytes_FromStringAndSize, PyBytes_AS_STRING
from cpython.mem cimport PyMem_Malloc, PyMem_Free
from libc.string cimport memset

from libc.stdint cimport int32_t, int8_t, intptr_t, uint64_t, uint8_t
from libc.stdlib cimport malloc

from opteryx.draken.core.buffers cimport DrakenFixedBuffer
from opteryx.draken.core.buffers cimport DRAKEN_BOOL
from opteryx.draken.core.fixed_vector cimport alloc_fixed_buffer, buf_dtype, buf_length, free_fixed_buffer
from opteryx.draken.vectors.vector cimport MIX_HASH_CONSTANT, Vector, NULL_HASH, mix_hash, simd_mix_hash

cdef const uint64_t TRUE_HASH = <uint64_t>0x4f112caa54efa882
cdef const uint64_t FALSE_HASH = <uint64_t>0xc2fd8b2343f83ce7

DEF BOOL_HASH_CHUNK = 1024

cdef class BoolVector(Vector):

    def __cinit__(self, size_t length=0, bint wrap=False):
        cdef size_t nbytes

        if wrap:
            self.ptr = NULL
            self.owns_data = False
        else:
            # bit-packed, so allocate ceil(length/8) bytes
            nbytes = (length + 7) >> 3
            self.ptr = alloc_fixed_buffer(DRAKEN_BOOL, length, 1)  # itemsize=1 is logical
            if self.ptr != NULL:
                # allocate raw bytes with libc malloc so free_fixed_buffer (which calls free())
                # can safely free the buffer later. Do not mix Python allocator and free().
                self.ptr.data = malloc(nbytes)
                if self.ptr.data == NULL:
                    raise MemoryError()
            self.owns_data = True

    def __dealloc__(self):
        if self.owns_data and self.ptr is not NULL:
            free_fixed_buffer(self.ptr, True)
            self.ptr = NULL

    # Properties
    @property
    def length(self):
        return buf_length(self.ptr)

    @property
    def dtype(self):
        return buf_dtype(self.ptr)

    def __getitem__(self, Py_ssize_t i):
        """Return the value at index i, or None if null."""
        cdef DrakenFixedBuffer* ptr = self.ptr
        if i < 0 or i >= ptr.length:
            raise IndexError("Index out of bounds")
        # null check
        if ptr.null_bitmap != NULL:
            byte = ptr.null_bitmap[i >> 3]
            bit = (byte >> (i & 7)) & 1
            if not bit:
                return None
        # extract bit
        cdef uint8_t val_byte = (<uint8_t*>ptr.data)[i >> 3]
        return bool((val_byte >> (i & 7)) & 1)

    # -------- Interop --------
    def to_arrow(self):
        """Convert to a PyArrow array."""
        import pyarrow as pa
        
        cdef size_t nbytes = (buf_length(self.ptr) + 7) >> 3
        addr = <intptr_t> self.ptr.data
        data_buf = pa.foreign_buffer(addr, nbytes, base=self)

        buffers = []
        if self.ptr.null_bitmap != NULL:
            buffers.append(pa.foreign_buffer(<intptr_t> self.ptr.null_bitmap, (self.ptr.length + 7) // 8, base=self))
        else:
            buffers.append(None)

        buffers.append(data_buf)

        return pa.Array.from_buffers(pa.bool_(), buf_length(self.ptr), buffers)

    cpdef BoolVector and_vector(self, BoolVector other):
        """Element-wise AND between two BoolVector instances. Returns a new BoolVector."""
        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef Py_ssize_t n = ptr1.length
        if n != ptr2.length:
            raise ValueError("Vectors must have the same length")

        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* a = <uint8_t*> ptr1.data
        cdef uint8_t* b = <uint8_t*> ptr2.data
        cdef uint8_t* d = <uint8_t*> out.ptr.data
        cdef Py_ssize_t i
        for i in range(nbytes):
            d[i] = a[i] & b[i]

        # No null bitmap is created for the result (treat nulls as False)
        out.ptr.null_bitmap = NULL
        return out

    cpdef BoolVector or_vector(self, BoolVector other):
        """Element-wise OR between two BoolVector instances. Returns a new BoolVector."""
        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef Py_ssize_t n = ptr1.length
        if n != ptr2.length:
            raise ValueError("Vectors must have the same length")

        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* a = <uint8_t*> ptr1.data
        cdef uint8_t* b = <uint8_t*> ptr2.data
        cdef uint8_t* d = <uint8_t*> out.ptr.data
        cdef Py_ssize_t i
        for i in range(nbytes):
            d[i] = a[i] | b[i]

        out.ptr.null_bitmap = NULL
        return out

    cpdef BoolVector xor_vector(self, BoolVector other):
        """Element-wise XOR between two BoolVector instances. Returns a new BoolVector."""
        cdef DrakenFixedBuffer* ptr1 = self.ptr
        cdef DrakenFixedBuffer* ptr2 = other.ptr
        cdef Py_ssize_t n = ptr1.length
        if n != ptr2.length:
            raise ValueError("Vectors must have the same length")

        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* a = <uint8_t*> ptr1.data
        cdef uint8_t* b = <uint8_t*> ptr2.data
        cdef uint8_t* d = <uint8_t*> out.ptr.data
        cdef Py_ssize_t i
        for i in range(nbytes):
            d[i] = a[i] ^ b[i]

        out.ptr.null_bitmap = NULL
        return out

    # -------- Ops --------
    cpdef BoolVector take(self, int32_t[::1] indices):
        cdef Py_ssize_t i, n = indices.shape[0]
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* src = <uint8_t*> self.ptr.data
        cdef uint8_t* dst = <uint8_t*> out.ptr.data
        # zero init
        cdef Py_ssize_t out_nbytes = (n + 7) >> 3
        for i in range(out_nbytes):
            dst[i] = 0
        for i in range(n):
            idx = indices[i]
            if ((src[idx >> 3] >> (idx & 7)) & 1) != 0:
                dst[i >> 3] |= (1 << (i & 7))
        return out

    cpdef int8_t[::1] equals(self, bint value):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        cdef int8_t* buf = <int8_t*> PyMem_Malloc(n)
        if buf == NULL:
            raise MemoryError()
        cdef int target = 1 if value else 0
        for i in range(n):
            val = ((<uint8_t*>ptr.data)[i >> 3] >> (i & 7)) & 1
            buf[i] = 1 if val == target else 0
        return <int8_t[:n]> buf

    cpdef int8_t[::1] not_equals(self, bint value):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        cdef int8_t* buf = <int8_t*> PyMem_Malloc(n)
        if buf == NULL:
            raise MemoryError()
        cdef int target = 1 if value else 0
        for i in range(n):
            val = ((<uint8_t*>ptr.data)[i >> 3] >> (i & 7)) & 1
            buf[i] = 1 if val != target else 0
        return <int8_t[:n]> buf

    cpdef int8_t any(self):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t nbytes = (ptr.length + 7) >> 3
        cdef Py_ssize_t i
        for i in range(nbytes):
            if (<uint8_t*>ptr.data)[i] != 0:
                return 1
        return 0

    cpdef int8_t all(self):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t i
        for i in range(n):
            if (((<uint8_t*>ptr.data)[i >> 3] >> (i & 7)) & 1) == 0:
                return 0
        return 1

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
        cdef Py_ssize_t i, n = ptr.length
        cdef list out = []
        cdef uint8_t byte, bit
        for i in range(n):
            if ptr.null_bitmap != NULL:
                byte = ptr.null_bitmap[i >> 3]
                bit = (byte >> (i & 7)) & 1
                if not bit:
                    out.append(None)
                    continue
            val = ((<uint8_t*>ptr.data)[i >> 3] >> (i & 7)) & 1
            out.append(bool(val))
        return out

    cdef void hash_into(
        self,
        uint64_t[::1] out_buf,
        Py_ssize_t offset=0
    ) except *:
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        if n == 0:
            return

        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("BoolVector.hash_into: output buffer too small")

        cdef Py_ssize_t i
        cdef Py_ssize_t block = 0
        cdef Py_ssize_t j = 0
        cdef Py_ssize_t idx = 0
        cdef uint8_t byte, bit
        cdef uint64_t value
        cdef uint64_t* dst = &out_buf[offset]
        cdef uint8_t* values = <uint8_t*> ptr.data
        cdef bint has_nulls = ptr.null_bitmap != NULL
        cdef uint64_t[BOOL_HASH_CHUNK] scratch
        cdef uint64_t* scratch_ptr = <uint64_t*> scratch

        if not has_nulls:
            i = 0
            while i < n:
                block = n - i
                if block > BOOL_HASH_CHUNK:
                    block = BOOL_HASH_CHUNK
                for j in range(block):
                    idx = i + j
                    if (values[idx >> 3] >> (idx & 7)) & 1:
                        scratch[j] = TRUE_HASH
                    else:
                        scratch[j] = FALSE_HASH
                simd_mix_hash(dst + i, scratch_ptr, <size_t> block)
                i += block
            return

        for i in range(n):
            byte = ptr.null_bitmap[i >> 3]
            bit = (byte >> (i & 7)) & 1
            if not bit:
                value = NULL_HASH
            else:
                if ((values)[i >> 3] >> (i & 7)) & 1:
                    value = TRUE_HASH
                else:
                    value = FALSE_HASH

            dst[i] = mix_hash(dst[i], value)

    def __str__(self):
        cdef list vals = []
        cdef Py_ssize_t i, k = min(<Py_ssize_t>buf_length(self.ptr), 10)
        for i in range(k):
            vals.append(bool(((<uint8_t*>self.ptr.data)[i >> 3] >> (i & 7)) & 1))
        return f"<BoolVector len={buf_length(self.ptr)} values={vals}>"


cdef BoolVector from_arrow(object array):
    cdef BoolVector vec = BoolVector(0, True)
    vec.ptr = <DrakenFixedBuffer*> malloc(sizeof(DrakenFixedBuffer))
    if vec.ptr == NULL:
        raise MemoryError()
    vec.owns_data = False

    cdef object bufs = array.buffers()
    # Keep references to prevent GC
    vec._arrow_null_buf = bufs[0]
    vec._arrow_data_buf = bufs[1]

    cdef intptr_t base_ptr = <intptr_t> bufs[1].address
    cdef Py_ssize_t offset = array.offset
    cdef intptr_t nb_addr

    vec.ptr.type = DRAKEN_BOOL
    vec.ptr.itemsize = 1
    vec.ptr.length = <size_t> len(array)

    # Data buffer handling
    cdef Py_ssize_t nbytes
    cdef uint8_t* src_data
    cdef uint8_t* dst_data
    cdef object new_data_bytes
    cdef Py_ssize_t i

    if offset % 8 == 0:
        # Aligned offset: zero-copy
        vec.ptr.data = <void*> (base_ptr + (offset >> 3))
    else:
        # Unaligned offset: must copy and shift data
        nbytes = (len(array) + 7) // 8
        new_data_bytes = PyBytes_FromStringAndSize(NULL, nbytes)
        dst_data = <uint8_t*> PyBytes_AS_STRING(new_data_bytes)
        memset(dst_data, 0, nbytes)
        
        src_data = <uint8_t*> base_ptr
        
        # Copy bits shifting them
        for i in range(len(array)):
            if (src_data[(offset + i) >> 3] >> ((offset + i) & 7)) & 1:
                dst_data[i >> 3] |= (1 << (i & 7))
        
        vec.ptr.data = <void*> dst_data
        vec._arrow_data_buf = new_data_bytes

    # Null bitmap handling
    cdef uint8_t* src_bitmap
    cdef uint8_t* dst_bitmap
    cdef object new_bitmap_bytes

    if bufs[0] is not None:
        nb_addr = bufs[0].address
        
        if offset % 8 == 0:
            vec.ptr.null_bitmap = (<uint8_t*> nb_addr) + (offset >> 3)
        else:
            # Unaligned offset: must copy and shift nulls
            nbytes = (len(array) + 7) // 8
            new_bitmap_bytes = PyBytes_FromStringAndSize(NULL, nbytes)
            dst_bitmap = <uint8_t*> PyBytes_AS_STRING(new_bitmap_bytes)
            memset(dst_bitmap, 0, nbytes)
            
            src_bitmap = <uint8_t*> nb_addr
            
            for i in range(len(array)):
                if (src_bitmap[(offset + i) >> 3] >> ((offset + i) & 7)) & 1:
                    dst_bitmap[i >> 3] |= (1 << (i & 7))
            
            vec.ptr.null_bitmap = dst_bitmap
            vec._arrow_null_buf = new_bitmap_bytes
    else:
        vec.ptr.null_bitmap = NULL

    return vec


cdef BoolVector from_sequence(uint8_t[::1] data):
    """
    Create BoolVector from a typed uint8 memoryview (zero-copy, bit-packed).
    
    Args:
        data: uint8_t[::1] memoryview (C-contiguous, bit-packed: 8 bools per byte)
    
    Returns:
        BoolVector wrapping the memoryview data
    
    Note:
        Input data should be bit-packed (8 boolean values per byte).
        The length will be inferred as data.shape[0] * 8.
    """
    cdef BoolVector vec = BoolVector(0, True)
    vec.ptr = <DrakenFixedBuffer*> malloc(sizeof(DrakenFixedBuffer))
    if vec.ptr == NULL:
        raise MemoryError()
    vec.owns_data = False

    # Keep reference to prevent GC
    vec._arrow_data_buf = data.base if data.base is not None else data
    vec._arrow_null_buf = None

    vec.ptr.type = DRAKEN_BOOL
    vec.ptr.itemsize = 1
    # Bit-packed: 8 booleans per byte
    vec.ptr.length = <size_t> (data.shape[0] * 8)
    vec.ptr.data = <void*> &data[0]
    vec.ptr.null_bitmap = NULL

    return vec
