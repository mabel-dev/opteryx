from libc.stdint cimport int32_t
from libc.stdint cimport int64_t
from libc.stdint cimport int8_t
from libc.stdint cimport uint64_t

from opteryx.draken.core.buffers cimport DrakenFixedBuffer
from opteryx.draken.vectors.vector cimport Vector

cdef class Float64Vector(Vector):
    cdef object _arrow_data_buf
    cdef object _arrow_null_buf
    cdef DrakenFixedBuffer* ptr
    cdef bint owns_data

    cpdef Float64Vector take(self, int32_t[::1] indices)

    cpdef int8_t[::1] equals(self, double value)
    cpdef int8_t[::1] equals_vector(self, Float64Vector other)
    cpdef int8_t[::1] not_equals(self, double value)
    cpdef int8_t[::1] not_equals_vector(self, Float64Vector other)
    cpdef int8_t[::1] greater_than(self, double value)
    cpdef int8_t[::1] greater_than_vector(self, Float64Vector other)
    cpdef int8_t[::1] greater_than_or_equals(self, double value)
    cpdef int8_t[::1] greater_than_or_equals_vector(self, Float64Vector other)
    cpdef int8_t[::1] less_than(self, double value)
    cpdef int8_t[::1] less_than_vector(self, Float64Vector other)
    cpdef int8_t[::1] less_than_or_equals(self, double value)
    cpdef int8_t[::1] less_than_or_equals_vector(self, Float64Vector other)

    cpdef int8_t[::1] is_null(self)

    cpdef list to_pylist(self)
    #cpdef double __getitem__(self, Py_ssize_t i)

    cpdef double sum(self)
    cpdef double min(self)
    cpdef double max(self)

    cdef void hash_into(self, uint64_t[::1] out_buf, Py_ssize_t offset=*) except *

cdef Float64Vector from_arrow(object array)
cdef Float64Vector from_sequence(double[::1] data)
