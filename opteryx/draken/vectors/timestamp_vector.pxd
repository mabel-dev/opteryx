from libc.stdint cimport int32_t
from libc.stdint cimport int64_t
from libc.stdint cimport int8_t
from libc.stdint cimport uint64_t

from opteryx.draken.core.buffers cimport DrakenFixedBuffer
from opteryx.draken.vectors.vector cimport Vector

cdef class TimestampVector(Vector):
    cdef object _arrow_data_buf
    cdef object _arrow_null_buf
    cdef Py_ssize_t null_bit_offset
    cdef DrakenFixedBuffer* ptr
    cdef bint owns_data

    cpdef TimestampVector take(self, int32_t[::1] indices)

    cpdef int8_t[::1] equals(self, int64_t value)
    cpdef int8_t[::1] not_equals(self, int64_t value)
    cpdef int8_t[::1] greater_than(self, int64_t value)
    cpdef int8_t[::1] greater_than_or_equals(self, int64_t value)
    cpdef int8_t[::1] less_than(self, int64_t value)
    cpdef int8_t[::1] less_than_or_equals(self, int64_t value)

    cpdef int8_t[::1] is_null(self)

    cpdef list to_pylist(self)

    cpdef int64_t min(self)
    cpdef int64_t max(self)

    cdef void hash_into(self, uint64_t[::1] out_buf, Py_ssize_t offset=*) except *

cdef TimestampVector from_arrow(object array)
