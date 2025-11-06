from libc.stdint cimport int32_t
from libc.stdint cimport int64_t
from libc.stdint cimport int8_t
from libc.stdint cimport uint64_t

from draken.core.buffers cimport DrakenFixedBuffer
from draken.vectors.vector cimport Vector

cdef class TimeVector(Vector):
    cdef object _arrow_data_buf
    cdef object _arrow_null_buf
    cdef DrakenFixedBuffer* ptr
    cdef bint owns_data
    cdef bint is_time64  # True if time64, False if time32

    cpdef TimeVector take(self, int32_t[::1] indices)

    cpdef int8_t[::1] is_null(self)

    cpdef list to_pylist(self)

    cpdef uint64_t[::1] hash(self)

cdef TimeVector from_arrow(object array)
