from libc.stdint cimport uint64_t
from opteryx.draken.vectors.vector cimport Vector
from opteryx.draken.core.buffers cimport DrakenMorsel, DrakenType

cdef class Morsel:
    cdef DrakenMorsel* ptr
    cdef list _encoded_names
    cdef list _columns
    cdef dict _name_to_index
    
    cpdef Vector column(self, bytes name)
    cpdef uint64_t[::1] hash(self, object columns=*)
    cdef void _take_inplace(self, object indices)
    cdef void _empty_inplace(self)
    cdef void _select_inplace(self, object columns)
    cdef Morsel _full_copy(self)
    cdef Vector _empty_vector_like(self, Py_ssize_t column_index, Vector src_vec)
    cdef bint _vector_dtype_matches(self, Vector vector, DrakenType expected)
    cdef object _empty_arrow_array_for_type(self, DrakenType dtype, Vector src_vec)
    cdef object _arrow_type_for_draken(self, DrakenType dtype, Vector src_vec)
    cdef inline void _rebuild_name_to_index(self)
    cdef inline dict _ensure_name_map(self)
    cdef inline Py_ssize_t _column_index_from_name(self, object column)
