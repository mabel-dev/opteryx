# distutils: language = c++
# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

cdef extern from "fast_float.h" namespace "fast_float":
    cdef cppclass from_chars_result:
        const char* ptr

    from_chars_result from_chars(
        const char* first,
        const char* last,
        double& value
    )

cdef double c_parse_fast_float(bytes bts)
cpdef double parse_fast_float(bytes bts)