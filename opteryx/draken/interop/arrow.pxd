from opteryx.draken.core.buffers cimport DrakenType

cpdef object vector_from_arrow(object array)
cpdef object vector_from_sequence(object data, object dtype=*)
cpdef DrakenType arrow_type_to_draken(object dtype)