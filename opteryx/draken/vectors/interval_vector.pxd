from libc.stdint cimport int32_t
from libc.stdint cimport int64_t
from libc.stdint cimport int8_t
from libc.stdint cimport uint64_t

from opteryx.draken.core.buffers cimport DrakenFixedBuffer
from opteryx.draken.vectors.vector cimport Vector

cdef class IntervalVector(Vector):
    cdef object _arrow_data_buf
    cdef object _arrow_null_buf
    cdef DrakenFixedBuffer* ptr
    cdef bint owns_data

    cpdef IntervalVector take(self, int32_t[::1] indices)

    cpdef int8_t[::1] is_null(self)

    cpdef list to_pylist(self)

    cpdef object to_arrow_interval(self)
    cpdef object to_arrow_binary(self)

    cpdef uint64_t[::1] hash(self)
    cdef void hash_into(self, uint64_t[::1] out_buf, Py_ssize_t offset=*) except *

cdef IntervalVector from_arrow_interval(object array)
cdef IntervalVector from_arrow_binary(object array)
