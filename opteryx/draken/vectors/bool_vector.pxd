# draken/vectors/bool_vector.pxd

# cython: language_level=3

from libc.stdint cimport int32_t, int8_t, uint8_t, uint64_t
from opteryx.draken.core.buffers cimport DrakenFixedBuffer
from opteryx.draken.vectors.vector cimport Vector

cdef class BoolVector(Vector):
    cdef object _arrow_data_buf
    cdef object _arrow_null_buf
    cdef DrakenFixedBuffer* ptr
    cdef bint owns_data

    # Ops
    cpdef BoolVector take(self, int32_t[::1] indices)
    cpdef int8_t[::1] equals(self, bint value)
    cpdef int8_t[::1] not_equals(self, bint value)
    cpdef int8_t any(self)
    cpdef int8_t all(self)
    cpdef int8_t[::1] is_null(self)
    cpdef list to_pylist(self)
    cdef void hash_into(self, uint64_t[::1] out_buf, Py_ssize_t offset=*) except *
    cpdef BoolVector and_vector(self, BoolVector other)
    cpdef BoolVector or_vector(self, BoolVector other)
    cpdef BoolVector xor_vector(self, BoolVector other)

cdef BoolVector from_arrow(object array)
cdef BoolVector from_sequence(uint8_t[::1] data)
