# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
StringVector: Cython implementation of a variable-width byte column for Draken.

This module provides:
- The StringVector class for efficient byte/variable-length storage
- Integration with DrakenVarBuffer and helpers for memory management
- Arrow interoperability (zero-copy wrapping)
- Fast equality, null handling, and hashing
"""

from cpython.buffer cimport PyBUF_READ
from cpython.memoryview cimport PyMemoryView_FromMemory
from cpython.mem cimport PyMem_Malloc, PyMem_Free
from cpython.bytes cimport PyBytes_AS_STRING
from cpython.bytes cimport PyBytes_FromStringAndSize
from libc.stddef cimport size_t
from libc.stdint cimport int32_t, intptr_t, uint8_t, uint64_t
from libc.string cimport memcpy, memset, memcmp
from libc.stdlib cimport malloc, realloc, free

from opteryx.draken.core.buffers cimport DrakenVarBuffer
from opteryx.draken.core.buffers cimport DRAKEN_STRING
from opteryx.draken.core.var_vector cimport alloc_var_buffer, buf_dtype

cdef extern from "xxhash.h":
    uint64_t XXH3_64bits(const void* input, size_t length) nogil

from opteryx.draken.vectors.vector cimport MIX_HASH_CONSTANT, Vector, NULL_HASH, simd_mix_hash

DEF STRING_HASH_CHUNK = 256


cdef class StringVector(Vector):

    def __cinit__(self, size_t length=0, size_t bytes_cap=0, bint wrap=False):
        """
        length>0, wrap=False  -> allocate new owned buffer
        wrap=True             -> no allocation; caller will set ptr
        """
        if wrap:
            self.ptr = NULL
            self.owns_data = False
        else:
            self.ptr = alloc_var_buffer(DRAKEN_STRING, length, bytes_cap)
            self.owns_data = True

    @property
    def length(self):
        """Number of values currently stored in the vector."""
        return self.ptr.length

    def __len__(self):
        return self.ptr.length

    @property
    def dtype(self):
        return buf_dtype(self.ptr)

    def to_arrow(self):
        """
        Zero-copy conversion to Arrow StringArray (bytes-based).
        Keeps a reference to this vector to prevent premature garbage collection.
        """
        import pyarrow as pa
        
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef size_t n = ptr.length

        # Data buffer: all the concatenated string bytes
        # Pass self as base object to keep the vector alive
        # If there are no bytes or the data pointer is NULL, create an empty
        # pyarrow buffer instead of passing a NULL pointer to foreign_buffer
        total_bytes = ptr.offsets[n]
        if total_bytes <= 0 or ptr.data == NULL:
            data_buf = pa.py_buffer(b"")
        else:
            data_buf = pa.foreign_buffer(<intptr_t>ptr.data, total_bytes, base=self)

        # Offsets buffer: (n+1) * int32_t entries
        offs_buf = pa.foreign_buffer(<intptr_t>ptr.offsets, (n + 1) * sizeof(int32_t), base=self)

        # Null bitmap buffer (optional)
        if ptr.null_bitmap != NULL:
            null_buf = pa.foreign_buffer(<intptr_t>ptr.null_bitmap, (n + 7) // 8, base=self)
        else:
            null_buf = None

        return pa.Array.from_buffers(pa.binary(), n, [null_buf, offs_buf, data_buf])

    def __getitem__(self, Py_ssize_t i):
        """
        Return entry i as raw bytes, or None if null.
        """
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef uint8_t byte, bit
        cdef int32_t start, end
        cdef Py_ssize_t nbytes
        cdef char* base

        if i < 0 or i >= ptr.length:
            raise IndexError("Index out of range")

        # Check for null value
        if ptr.null_bitmap != NULL:
            byte = ptr.null_bitmap[i >> 3]
            bit = (byte >> (i & 7)) & 1
            if not bit:
                return None

        start = ptr.offsets[i]
        end = ptr.offsets[i+1]
        nbytes = end - start
        base = <char*>ptr.data
        return PyBytes_FromStringAndSize(base + start, nbytes)

    def __iter__(self):
        return _StringVectorIterator(self)

    def c_iter(self):
        """Return a C-level iterator for high-performance kernel operations."""
        return _StringVectorCIterator._from_ptr(self.ptr)

    cpdef Py_ssize_t byte_length(self, Py_ssize_t i):
        """Return the number of bytes for row ``i`` without materializing the value."""
        cdef DrakenVarBuffer* ptr = self.ptr
        if i < 0 or i >= ptr.length:
            raise IndexError("Index out of range")
        return ptr.offsets[i + 1] - ptr.offsets[i]

    cpdef object buffers(self):
        """Expose data, offsets, and null bitmap buffers as zero-copy views."""
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t total_bytes = ptr.offsets[n]
        cdef object data_view

        if total_bytes <= 0 or ptr.data == NULL:
            data_view = memoryview(b"")
        else:
            data_view = <uint8_t[:total_bytes]> ptr.data

        return (
            data_view,
            <int32_t[:n + 1]> ptr.offsets,
            self.null_bitmap(),
        )

    cpdef object null_bitmap(self):
        """Return the null bitmap as a Python ``memoryview``, or ``None`` if all values are valid."""
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef Py_ssize_t nb_size
        if ptr.null_bitmap == NULL:
            return None
        nb_size = (ptr.length + 7) // 8
        if nb_size == 0:
            nb_size = 1
        return PyMemoryView_FromMemory(<char*>ptr.null_bitmap, nb_size, PyBUF_READ)

    cpdef int32_t[::1] lengths(self):
        """Return a direct view over the offsets buffer for fast length computations."""
        return <int32_t[: self.ptr.length + 1]> self.ptr.offsets

    cpdef object view(self):
        """Return a lightweight pointer/length view for zero-copy consumers."""
        return _StringVectorView(self)

    @property
    def null_count(self):
        """Return the number of nulls in the vector."""
        cdef DrakenVarBuffer* ptr = self.ptr
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

    # Optimized equality check using SIMD-friendly operations
    cpdef int8_t[::1] equals(self, bytes value):
        """
        Return mask: 1 if equal to value, else 0.
        Optimized version with reduced branching and better cache locality.
        """
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef uint8_t* nb_ptr = ptr.null_bitmap
        cdef int8_t* buf = <int8_t*> PyMem_Malloc(n * sizeof(int8_t))
        if buf == NULL:
            raise MemoryError()

        cdef char* val_ptr = PyBytes_AS_STRING(value)
        cdef Py_ssize_t val_len = len(value)
        cdef int32_t start, end, str_len
        cdef Py_ssize_t i
        
        # Process in chunks for better cache performance
        for i in range(n):
            # Check null first (most likely to fail)
            if nb_ptr != NULL and ((nb_ptr[i >> 3] >> (i & 7)) & 1) == 0:
                buf[i] = 0
                continue
            
            start = ptr.offsets[i]
            end = ptr.offsets[i + 1]
            str_len = end - start
            
            # Length check before expensive memcmp
            if str_len != val_len:
                buf[i] = 0
                continue

            buf[i] = 1 if memcmp(<char*>ptr.data + start, val_ptr, str_len) == 0 else 0

        return <int8_t[:n]> buf

    cpdef list to_pylist(self):
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef list out = []
        cdef Py_ssize_t i
        cdef int32_t start, end
        cdef char* data = <char*> ptr.data
        cdef uint8_t byte, bit

        for i in range(n):
            if ptr.null_bitmap != NULL:
                byte = ptr.null_bitmap[i >> 3]
                bit = (byte >> (i & 7)) & 1
                if not bit:
                    out.append(None)
                    continue

            start = ptr.offsets[i]
            end = ptr.offsets[i + 1]
            out.append(PyBytes_FromStringAndSize(data + start, end - start))

        return out

    cdef void hash_into(
        self,
        uint64_t[::1] out_buf,
        Py_ssize_t offset=0,
    ) except *:
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length

        if n == 0:
            return

        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("StringVector.hash_into: output buffer too small")

        cdef const uint8_t* data = <const uint8_t*> ptr.data
        cdef int32_t* offsets = ptr.offsets
        cdef uint8_t* nb_ptr = ptr.null_bitmap
        cdef Py_ssize_t i
        cdef Py_ssize_t j
        cdef uint8_t byte
        cdef size_t str_len
        cdef int32_t start, end
        cdef uint64_t* dst = &out_buf[offset]
        cdef uint64_t[STRING_HASH_CHUNK] scratch
        cdef uint64_t* scratch_ptr = <uint64_t*> scratch
        cdef Py_ssize_t idx

        i = 0
        with nogil:
            while i < n:
                block = n - i
                if block > STRING_HASH_CHUNK:
                    block = STRING_HASH_CHUNK

                if nb_ptr != NULL:
                    for j in range(block):
                        idx = i + j
                        byte = nb_ptr[idx >> 3]
                        if ((byte >> (idx & 7)) & 1) == 0:
                            scratch[j] = NULL_HASH
                            continue
                        start = offsets[idx]
                        end = offsets[idx + 1]
                        str_len = <size_t>(end - start)
                        scratch[j] = XXH3_64bits(data + start, str_len)
                else:
                    for j in range(block):
                        start = offsets[i + j]
                        end = offsets[i + j + 1]
                        str_len = <size_t>(end - start)
                        scratch[j] = XXH3_64bits(data + start, str_len)
                
                simd_mix_hash(dst + i, scratch_ptr, <size_t> block)
                i += block

    cpdef StringVector take(self, int32_t[::1] indices):
        cdef DrakenVarBuffer* src_ptr = self.ptr
        cdef Py_ssize_t n = indices.shape[0]
        cdef size_t total_bytes = 0
        cdef Py_ssize_t i
        cdef int32_t src_idx

        for i in range(n):
            src_idx = indices[i]
            if src_idx < 0 or src_idx >= <Py_ssize_t> src_ptr.length:
                raise IndexError(
                    f"Index {src_idx} out of bounds for length {src_ptr.length}"
                )
            total_bytes += <size_t>(
                src_ptr.offsets[src_idx + 1] - src_ptr.offsets[src_idx]
            )

        cdef StringVector result = StringVector(<size_t> n, total_bytes)
        cdef DrakenVarBuffer* dst_ptr = result.ptr
        cdef char* src_data = <char*> src_ptr.data
        cdef char* dst_data = <char*> dst_ptr.data
        cdef int32_t* dst_offsets = dst_ptr.offsets
        cdef int32_t dst_offset = 0
        cdef bint has_nulls = src_ptr.null_bitmap != NULL
        cdef Py_ssize_t nb_size
        cdef int32_t start, end
        cdef int32_t byte_len
        cdef uint8_t src_bit

        dst_offsets[0] = 0

        if has_nulls and n > 0:
            nb_size = (n + 7) >> 3
            dst_ptr.null_bitmap = <uint8_t*> PyMem_Malloc(nb_size)
            if dst_ptr.null_bitmap == NULL:
                raise MemoryError()
            memset(dst_ptr.null_bitmap, 0xFF, nb_size)
        else:
            dst_ptr.null_bitmap = NULL

        for i in range(n):
            src_idx = indices[i]
            start = src_ptr.offsets[src_idx]
            end = src_ptr.offsets[src_idx + 1]
            byte_len = end - start

            if byte_len > 0:
                memcpy(dst_data + dst_offset, src_data + start, byte_len)

            dst_offset += byte_len
            dst_offsets[i + 1] = dst_offset

            if has_nulls:
                src_bit = (
                    (src_ptr.null_bitmap[src_idx >> 3] >> (src_idx & 7)) & 1
                )
                if not src_bit:
                    dst_ptr.null_bitmap[i >> 3] &= ~(1 << (i & 7))

        return result

    def __str__(self):
        cdef list vals = []
        cdef Py_ssize_t i, k = min(self.ptr.length, 5)
        for i in range(k):
            vals.append(self[i])
        return f"<StringVector len={self.ptr.length} values={vals}>"


# Lightweight struct for C-level iteration over string vector elements
cdef struct StringElement:
    char* ptr
    Py_ssize_t length
    bint is_null


cdef class _StringVectorIterator:
    """Efficient iterator that avoids repeated attribute lookups during scans."""

    cdef DrakenVarBuffer* _ptr
    cdef Py_ssize_t _pos
    cdef Py_ssize_t _length
    cdef char* _base
    cdef int32_t* _offsets
    cdef uint8_t* _nulls
    cdef bint _has_nulls

    def __cinit__(self, StringVector vec):
        self._ptr = vec.ptr
        self._pos = 0
        self._length = self._ptr.length
        self._base = <char*>self._ptr.data
        self._offsets = self._ptr.offsets
        self._nulls = self._ptr.null_bitmap
        self._has_nulls = (self._nulls != NULL)

    def __iter__(self):
        return self

    def __next__(self):
        if self._pos >= self._length:
            raise StopIteration()

        cdef Py_ssize_t i = self._pos
        self._pos += 1

        # Check for null value
        if self._has_nulls and ((self._nulls[i >> 3] >> (i & 7)) & 1) == 0:
            return None

        cdef int32_t start = self._offsets[i]
        cdef int32_t end = self._offsets[i + 1]
        return PyBytes_FromStringAndSize(self._base + start, end - start)


cdef class _StringVectorCIterator:
    """
    Highly optimized C-level iterator with minimal overhead.
    """

    def __cinit__(self):
        # Initialize with NULL; must use _from_ptr factory method
        self._ptr = NULL
        self._pos = 0
        self._length = 0
        self._base = NULL
        self._offsets = NULL
        self._nulls = NULL
        self._has_nulls = False

    @staticmethod
    cdef _StringVectorCIterator _from_ptr(DrakenVarBuffer* ptr):
        """Factory method to create iterator from a buffer pointer."""
        cdef _StringVectorCIterator it = _StringVectorCIterator.__new__(_StringVectorCIterator)
        it._ptr = ptr
        it._pos = 0
        it._length = ptr.length
        it._base = <char*>ptr.data
        it._offsets = ptr.offsets
        it._nulls = ptr.null_bitmap
        it._has_nulls = (it._nulls != NULL)
        return it

    cdef inline bint next(self, StringElement* elem) nogil:
        """
        Ultra-fast inline method for C-level iteration.
        """
        if self._pos >= self._length:
            return False

        cdef Py_ssize_t i = self._pos
        self._pos += 1

        # Check for null
        if self._has_nulls and ((self._nulls[i >> 3] >> (i & 7)) & 1) == 0:
            elem.ptr = NULL
            elem.length = 0
            elem.is_null = True
        else:
            elem.ptr = self._base + self._offsets[i]
            elem.length = self._offsets[i + 1] - self._offsets[i]
            elem.is_null = False

        return True

    cpdef void reset(self):
        """Reset iterator to beginning."""
        self._pos = 0

    @property
    def position(self):
        """Current position in iteration."""
        return self._pos

    cpdef StringElement get_at(self, Py_ssize_t index):
        """
        Get element at specific index without advancing iterator.
        Useful for random access patterns.
        """
        if index < 0 or index >= self._length:
            raise IndexError("Index out of range")

        cdef StringElement elem
        cdef int32_t start, end

        if self._nulls != NULL and ((self._nulls[index >> 3] >> (index & 7)) & 1) == 0:
            elem.ptr = NULL
            elem.length = 0
            elem.is_null = True
        else:
            start = self._ptr.offsets[index]
            end = self._ptr.offsets[index + 1]
            elem.ptr = self._base + start
            elem.length = end - start
            elem.is_null = False

        return elem


cdef class _StringVectorView:
    """Zero-copy helper exposing raw pointer/length access."""

    def __cinit__(self, StringVector vec):
        self._ptr = vec.ptr
        self._data = <char*> self._ptr.data
        self._offsets = self._ptr.offsets
        self._nulls = self._ptr.null_bitmap

    cpdef intptr_t value_ptr(self, Py_ssize_t i):
        if i < 0 or i >= self._ptr.length:
            raise IndexError("Index out of range")
        return <intptr_t> (self._data + self._offsets[i])

    cpdef Py_ssize_t value_len(self, Py_ssize_t i):
        if i < 0 or i >= self._ptr.length:
            raise IndexError("Index out of range")
        return self._offsets[i + 1] - self._offsets[i]

    cpdef bint is_null(self, Py_ssize_t i):
        if i < 0 or i >= self._ptr.length:
            raise IndexError("Index out of range")
        if self._nulls == NULL:
            return False
        return ((self._nulls[i >> 3] >> (i & 7)) & 1) == 0


cdef class StringVectorBuilder:
    """Utility for constructing ``StringVector`` instances with controlled preallocation."""

    def __cinit__(self, Py_ssize_t length, Py_ssize_t bytes_capacity,
                  bint resizable=False, bint strict_capacity=False):
        if length < 0:
            raise ValueError("length must be non-negative")
        if bytes_capacity < 0:
            raise ValueError("bytes_capacity must be non-negative")

        self._vec = StringVector(length, bytes_capacity)
        self._ptr = self._vec.ptr
        self._length = length
        self._next_index = 0
        self._bytes_cap = bytes_capacity
        self._offset = 0
        self._finished = False
        self._resizable = resizable
        self._strict_capacity = strict_capacity
        self._mask_user_provided = False

        # Cache frequently accessed pointers
        self._data = <char*>self._ptr.data
        self._offsets = self._ptr.offsets
        self._nulls = self._ptr.null_bitmap

        if self._offsets != NULL:
            self._offsets[0] = 0

    def __dealloc__(self):
        # Allow the vector to GC naturally; nothing special to do.
        pass

    @classmethod
    def with_counts(cls, Py_ssize_t length, Py_ssize_t total_bytes):
        """Create a builder with an exact byte budget that must be fully consumed."""
        return cls(length, total_bytes, False, True)

    @classmethod
    def with_estimate(cls, Py_ssize_t length, Py_ssize_t est_avg_bytes):
        """Create a resizable builder using an average byte estimate per row."""
        if length < 0:
            raise ValueError("length must be non-negative")
        if est_avg_bytes < 0:
            raise ValueError("est_avg_bytes must be non-negative")
        initial = length * est_avg_bytes
        if initial <= 0:
            initial = max(length, 64)
        return cls(length, initial, True, False)

    def __len__(self):
        return self._length

    property bytes_capacity:
        def __get__(self):
            return self._bytes_cap

    property bytes_used:
        def __get__(self):
            return self._offset

    property remaining_bytes:
        def __get__(self):
            return self._bytes_cap - self._offset

    cpdef void append(self, bytes value):
        """Append a value at the next position, copying bytes into the backing buffer."""
        self._append_with_ptr(self._next_index, PyBytes_AS_STRING(value), len(value))

    cpdef void append_bulk(self, list values):
        """
        Append multiple values at once for better performance.
        """
        cdef Py_ssize_t i, n = len(values)
        cdef bytes value
        cdef char* val_ptr
        cdef Py_ssize_t val_len
        
        for i in range(n):
            if self._next_index >= self._length:
                raise IndexError("Cannot append beyond builder length")
                
            value = values[i]
            if value is None:
                self._set_null(self._next_index)
            else:
                val_ptr = PyBytes_AS_STRING(value)
                val_len = len(value)
                self._append_with_ptr(self._next_index, val_ptr, val_len)

    cdef void append_bytes_bulk(self, const char** ptrs, Py_ssize_t* lengths, Py_ssize_t n):
        """
        Append multiple raw byte sequences at once.
        """
        cdef Py_ssize_t i
        for i in range(n):
            if self._next_index >= self._length:
                raise IndexError("Cannot append beyond builder length")
            self._append_with_ptr(self._next_index, ptrs[i], lengths[i])

    cdef inline void _append_with_ptr(self, Py_ssize_t index, const char* src, Py_ssize_t length) except *:
        self._require_index(index)
        if length < 0:
            raise ValueError("length must be non-negative")

        self._ensure_capacity(length)

        if length > 0 and src != NULL:
            memcpy(self._data + self._offset, src, length)

        if self._nulls != NULL and not self._mask_user_provided:
            self._nulls[index >> 3] |= (1 << (index & 7))

        self._offset += length
        self._next_index += 1
        self._offsets[self._next_index] = <int32_t>self._offset

    cpdef void append_bytes(self, const char* ptr, Py_ssize_t length):
        """
        Append from raw pointer + length without Python bytes intermediary.

        Zero-copy-friendly: avoids creating a Python bytes object, though
        data is still copied into the builder's internal buffer.

        Args:
            ptr: Pointer to byte data (can be NULL if length is 0)
            length: Number of bytes to copy

        Example:
            cdef char* data = get_string_data()
            builder.append_bytes(data, strlen(data))
        """
        self._append_with_ptr(self._next_index, ptr, length)

    cpdef void append_view(self, const uint8_t[::1] value):
        """Append from a read-only memoryview without creating an intermediate bytes object."""
        cdef Py_ssize_t size = value.shape[0]
        cdef const uint8_t* ptr = NULL
        if size == 0:
            self._append_with_ptr(self._next_index, NULL, 0)
        else:
            ptr = &value[0]
            self._append_with_ptr(self._next_index, <const char*>ptr, size)

    cpdef void append_null(self):
        """Append a null entry without advancing the byte offset."""
        self._set_null(self._next_index)

    cpdef void set(self, Py_ssize_t index, bytes value):
        """Set ``index`` to ``value`` (must be the next slot)."""
        self._append_with_ptr(index, PyBytes_AS_STRING(value), len(value))

    cpdef void set_bytes(self, Py_ssize_t index, const char* ptr, Py_ssize_t length):
        """
        Set value at index from raw pointer + length.

        Args:
            index: Index to set (must be the next available slot)
            ptr: Pointer to byte data (can be NULL if length is 0)
            length: Number of bytes to copy
        """
        self._append_with_ptr(index, ptr, length)

    cpdef void set_view(self, Py_ssize_t index, const uint8_t[::1] value):
        cdef Py_ssize_t size = value.shape[0]
        cdef const uint8_t* ptr = NULL
        if size > 0:
            ptr = &value[0]
        self._append_with_ptr(index, <const char*>ptr, size)

    cpdef void set_null(self, Py_ssize_t index):
        self._set_null(index)

    cpdef void set_validity_mask(self, const uint8_t[::1] mask):
        """Install a user-supplied validity bitmap for the entire builder."""
        cdef Py_ssize_t nb_size = (self._length + 7) // 8
        if nb_size == 0:
            nb_size = 1
        if mask.shape[0] < nb_size:
            raise ValueError("validity mask is too small for declared length")
        if self._ptr.null_bitmap == NULL:
            self._ptr.null_bitmap = <uint8_t*> malloc(nb_size)
            if self._ptr.null_bitmap == NULL:
                raise MemoryError()
        memcpy(self._ptr.null_bitmap, &mask[0], nb_size)
        self._mask_user_provided = True

    cpdef StringVector finish(self):
        """Finalize construction and hand off the built vector."""
        if self._finished:
            return self._vec
        if self._next_index != self._length:
            raise ValueError(
                f"builder incomplete: appended {self._next_index} of {self._length} entries"
            )
        if self._offsets[self._length] != self._offset:
            self._offsets[self._length] = <int32_t>self._offset
        if self._strict_capacity and self._offset != self._bytes_cap:
            raise ValueError(
                f"builder consumed {self._offset} bytes but expected {self._bytes_cap}"
            )
        self._finished = True
        return self._vec

    cdef inline void _set_null(self, Py_ssize_t index) except *:
        self._require_index(index)
        self._ensure_capacity(0)
        self._initialize_null_bitmap()
        self._nulls[index >> 3] &= ~(1 << (index & 7))
        self._next_index += 1
        self._offsets[self._next_index] = <int32_t>self._offset

    cdef inline void _ensure_capacity(self, Py_ssize_t to_add) except *:
        if to_add <= 0:
            return
        if self._offset + to_add <= self._bytes_cap:
            return
        if not self._resizable:
            raise ValueError("not enough remaining capacity for value")

        cdef Py_ssize_t new_cap = self._bytes_cap
        if new_cap == 0:
            new_cap = max(to_add, 64)
        else:
            while new_cap < self._offset + to_add:
                new_cap = new_cap * 2
        
        cdef uint8_t* new_data
        if self._data == NULL:
            new_data = <uint8_t*> malloc(new_cap)
        else:
            new_data = <uint8_t*> realloc(self._ptr.data, new_cap)
        if new_data == NULL:
            raise MemoryError()
        
        # Update cached pointers
        self._ptr.data = new_data
        self._data = <char*>new_data
        self._bytes_cap = new_cap

    cdef inline void _initialize_null_bitmap(self) except *:
        cdef Py_ssize_t nb_size
        if self._nulls == NULL:
            nb_size = (self._length + 7) // 8
            if nb_size == 0:
                nb_size = 1
            self._ptr.null_bitmap = <uint8_t*> malloc(nb_size)
            if self._ptr.null_bitmap == NULL:
                raise MemoryError()
            # Update cached pointer
            self._nulls = self._ptr.null_bitmap
            memset(self._nulls, 0xFF, nb_size)
            self._mask_user_provided = False

    cdef inline void _require_index(self, Py_ssize_t index) except *:
        if self._finished:
            raise ValueError("builder already finished")
        if index < 0 or index >= self._length:
            raise IndexError("index out of bounds")
        if index != self._next_index:
            raise IndexError(f"builder expects index {self._next_index}, got {index}")
        if self._offsets == NULL:
            raise ValueError("builder offsets buffer is missing")


cdef StringVector from_arrow(object array):
    """
    Wrap an Arrow StringArray without copying.
    Keeps references to Arrow buffers to prevent GC from freeing memory.
    """
    cdef StringVector vec = StringVector(0, 0, True)
    vec.ptr = <DrakenVarBuffer*> malloc(sizeof(DrakenVarBuffer))
    if vec.ptr == NULL:
        raise MemoryError()
    vec.owns_data = False

    cdef object bufs = array.buffers()
    vec._arrow_null_buf = bufs[0]
    vec._arrow_offs_buf = bufs[1]
    vec._arrow_data_buf = bufs[2]

    vec.ptr.length = <size_t> len(array)
    cdef Py_ssize_t offset = array.offset

    # Data buffer (bytes)
    cdef intptr_t data_addr = bufs[2].address
    vec.ptr.data = <uint8_t*> data_addr

    # Offsets buffer (int32_t[length+1])
    cdef intptr_t offs_addr = bufs[1].address
    vec.ptr.offsets = (<int32_t*> offs_addr) + offset

    # Null bitmap (optional)
    cdef intptr_t nb_addr
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
            # memset(dst_bitmap, 0, nb_size) # Not needed as we overwrite
            
            src_bitmap = <uint8_t*> nb_addr
            
            copy_bitmap_shifted(src_bitmap, dst_bitmap, offset, len(array))
            
            vec.ptr.null_bitmap = dst_bitmap
            vec._arrow_null_buf = new_bitmap_bytes
    else:
        vec.ptr.null_bitmap = NULL

    vec.ptr.type = DRAKEN_STRING
    return vec

cdef void copy_bitmap_shifted(uint8_t* src, uint8_t* dst, Py_ssize_t offset, Py_ssize_t length) noexcept nogil:
    cdef Py_ssize_t i
    cdef int shift = offset & 7
    cdef Py_ssize_t byte_offset = offset >> 3
    cdef Py_ssize_t num_bytes = (length + 7) // 8
    
    if shift == 0:
        memcpy(dst, src + byte_offset, num_bytes)
        return

    # Process all bytes except the last one
    for i in range(num_bytes - 1):
        dst[i] = (src[byte_offset + i] >> shift) | (src[byte_offset + i + 1] << (8 - shift))
        
    # Handle the last byte
    i = num_bytes - 1
    cdef Py_ssize_t last_bit_index = offset + length - 1
    cdef Py_ssize_t last_byte_index = last_bit_index >> 3
    
    if last_byte_index > (byte_offset + i):
        dst[i] = (src[byte_offset + i] >> shift) | (src[byte_offset + i + 1] << (8 - shift))
    else:
        dst[i] = (src[byte_offset + i] >> shift)

cdef inline bint is_null(uint8_t* bitmap, Py_ssize_t i):
    """Check if row i is null, given Arrow-style bitmap (1=valid, 0=null)."""
    if bitmap == NULL:
        return False
    return not ((bitmap[i >> 3] >> (i & 7)) & 1)

cdef StringVector from_arrow_struct(object array):
    """
    Convert an Arrow StructArray into a StringVector of JSON strings.
    Each row becomes {"field": value, ...}
    """
    cdef Py_ssize_t n = len(array)
    cdef list field_names = [f.name for f in array.type]
    cdef int nfields = len(field_names)
    cdef Py_ssize_t nb_size

    # crude capacity guess: 64 bytes per row
    cdef StringVector vec = StringVector(n, n * 64, False)
    vec.owns_data = True
    cdef DrakenVarBuffer* ptr = vec.ptr

    cdef object bufs = array.buffers()
    cdef intptr_t nb_addr
    cdef uint8_t* parent_null_bitmap = NULL
    if bufs[0] is not None:
        nb_addr = bufs[0].address
        parent_null_bitmap = <uint8_t*> nb_addr

        # allocate and copy null bitmap into Draken
        nb_size = (n + 7) // 8
        ptr.null_bitmap = <uint8_t*> malloc(nb_size)
        if ptr.null_bitmap == NULL:
            raise MemoryError()
        memcpy(ptr.null_bitmap, parent_null_bitmap, nb_size)
    else:
        ptr.null_bitmap = NULL

    cdef Py_ssize_t offset = 0
    cdef Py_ssize_t i, j
    cdef bytes json_bytes
    cdef const char* jb_ptr

    ptr.offsets[0] = 0

    for i in range(n):
        if is_null(parent_null_bitmap, i):
            # just carry forward same offset (null row = empty string)
            ptr.offsets[i+1] = offset
            continue

        # build JSON row as Python string for now
        row_items = []
        for j in range(nfields):
            val = array.field(j)[i].as_py()
            if val is None:
                row_items.append(f'"{field_names[j]}": null')
            elif isinstance(val, str):
                # naive escaping
                row_items.append(f'"{field_names[j]}": "{val}"')
            else:
                row_items.append(f'"{field_names[j]}": {val}')
        json_str = "{" + ",".join(row_items) + "}"
        json_bytes = json_str.encode("utf8")

        jb_ptr = PyBytes_AS_STRING(json_bytes)
        memcpy(<char*>ptr.data + offset, jb_ptr, len(json_bytes))

        offset += len(json_bytes)
        ptr.offsets[i+1] = offset

    return vec

#################################

cpdef StringVector uppercase(StringVector input):
    """
    Return a new StringVector with all non-null values uppercased.
    """
    cdef DrakenVarBuffer* in_ptr = input.ptr
    cdef Py_ssize_t i, n = in_ptr.length
    cdef int32_t start, end, length

    # Estimate total bytes (uppercased values won't be longer)
    cdef int32_t total_bytes = in_ptr.offsets[n]

    # Allocate new buffer
    cdef StringVector result = StringVector(n, total_bytes)
    cdef DrakenVarBuffer* out_ptr = result.ptr

    cdef char* in_data = <char*>in_ptr.data
    cdef char* out_data = <char*>out_ptr.data
    cdef int32_t* out_offsets = out_ptr.offsets
    cdef int32_t offset = 0
    out_offsets[0] = 0

    cdef char* src
    cdef char ch
    cdef int j

    for i in range(n):
        if in_ptr.null_bitmap != NULL and ((in_ptr.null_bitmap[i >> 3] >> (i & 7)) & 1) == 0:
            # Set null bit
            if out_ptr.null_bitmap == NULL:
                out_ptr.null_bitmap = <uint8_t*> malloc((n + 7) // 8)
                for j in range((n + 7) // 8):
                    out_ptr.null_bitmap[j] = 0xFF  # Initially mark all as valid

            out_ptr.null_bitmap[i >> 3] &= ~(1 << (i & 7))  # Mark as null
            out_offsets[i + 1] = offset
            continue

        # Get string bounds
        start = in_ptr.offsets[i]
        end = in_ptr.offsets[i + 1]
        length = end - start
        src = in_data + start

        for j in range(length):
            ch = src[j]
            if 97 <= ch <= 122:  # 'a'..'z'
                out_data[offset + j] = ch - 32
            else:
                out_data[offset + j] = ch

        offset += length
        out_offsets[i + 1] = offset

    return result
