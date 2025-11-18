# cython: language_level=3

from libc.stdint cimport int32_t, int8_t, intptr_t, uint64_t, uint8_t
from opteryx.draken.core.buffers cimport DrakenVarBuffer
from opteryx.draken.vectors.vector cimport Vector


# Lightweight struct for C-level iteration over string vector elements
cdef struct StringElement:
    char* ptr
    Py_ssize_t length
    bint is_null


cdef class StringVector(Vector):
    cdef object _arrow_data_buf
    cdef object _arrow_offs_buf
    cdef object _arrow_null_buf

    cdef DrakenVarBuffer* ptr
    cdef bint owns_data

    cpdef int8_t[::1] equals(self, bytes value)
    cdef void hash_into(self, uint64_t[::1] out_buf, Py_ssize_t offset=*) except *
    cpdef StringVector take(self, int32_t[::1] indices)

    cpdef list to_pylist(self)
    cpdef Py_ssize_t byte_length(self, Py_ssize_t i)
    cpdef object buffers(self)
    cpdef object null_bitmap(self)
    cpdef int32_t[::1] lengths(self)
    cpdef object view(self)


cdef class _StringVectorCIterator:
    """C-level iterator for high-performance kernel operations."""
    cdef DrakenVarBuffer* _ptr
    cdef Py_ssize_t _pos
    cdef Py_ssize_t _length
    cdef char* _base
    cdef int32_t* _offsets
    cdef uint8_t* _nulls
    cdef bint _has_nulls

    @staticmethod
    cdef _StringVectorCIterator _from_ptr(DrakenVarBuffer* ptr)
    cdef bint next(self, StringElement* elem) nogil
    cpdef void reset(self)
    cpdef StringElement get_at(self, Py_ssize_t index)


cdef class _StringVectorView:
    cdef DrakenVarBuffer* _ptr
    cdef char* _data
    cdef int32_t* _offsets
    cdef uint8_t* _nulls

    cpdef intptr_t value_ptr(self, Py_ssize_t i)
    cpdef Py_ssize_t value_len(self, Py_ssize_t i)
    cpdef bint is_null(self, Py_ssize_t i)


cdef class StringVectorBuilder:
    """Builder for constructing StringVector instances."""
    cdef StringVector _vec
    cdef DrakenVarBuffer* _ptr
    cdef Py_ssize_t _length
    cdef Py_ssize_t _next_index
    cdef Py_ssize_t _bytes_cap
    cdef Py_ssize_t _offset
    cdef bint _finished
    cdef bint _resizable
    cdef bint _strict_capacity
    cdef bint _mask_user_provided
    cdef char* _data
    cdef int32_t* _offsets
    cdef uint8_t* _nulls

    cpdef void append(self, bytes value)
    cpdef void append_bytes(self, const char* ptr, Py_ssize_t length)
    cpdef void append_view(self, const uint8_t[::1] value)
    cpdef void append_null(self)
    cpdef void append_bulk(self, list values)
    cdef void append_bytes_bulk(self, const char** ptrs, Py_ssize_t* lengths, Py_ssize_t n)
    cpdef void set(self, Py_ssize_t index, bytes value)
    cpdef void set_bytes(self, Py_ssize_t index, const char* ptr, Py_ssize_t length)
    cpdef void set_view(self, Py_ssize_t index, const uint8_t[::1] value)
    cpdef void set_null(self, Py_ssize_t index)
    cpdef void set_validity_mask(self, const uint8_t[::1] mask)
    cpdef StringVector finish(self)
    
    # Private methods
    cdef void _append_with_ptr(self, Py_ssize_t index, const char* src, Py_ssize_t length) except *
    cdef void _set_null(self, Py_ssize_t index) except *
    cdef void _ensure_capacity(self, Py_ssize_t to_add) except *
    cdef void _initialize_null_bitmap(self) except *
    cdef void _require_index(self, Py_ssize_t index) except *


cdef StringVector from_arrow(object array)
cdef StringVector from_arrow_struct(object array)

cpdef StringVector uppercase(StringVector input)
