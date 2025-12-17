from libc.stdint cimport uint64_t

from opteryx.draken.core.buffers cimport DrakenArrayBuffer
from opteryx.draken.vectors.vector cimport Vector


cdef class ArrayVector(Vector):
    cdef DrakenArrayBuffer* ptr
    cdef object _child
    cdef bint owns_offsets
    cdef bint owns_null_bitmap
    cdef object _arrow_parent
    cdef object _arrow_offsets_buf
    cdef object _arrow_null_buf
    cdef object _arrow_child_array
    cdef object _child_arrow_type
    cdef bint _child_decode_utf8

    cdef object _materialize_row(self, Py_ssize_t idx)
    cdef void hash_into(
        self,
        uint64_t[::1] out_buf,
        Py_ssize_t offset=*
    ) except *


cdef ArrayVector from_arrow(object array)
cdef ArrayVector from_sequence(object data)
