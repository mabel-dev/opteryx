# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

import numpy
cimport numpy
numpy.import_array()


cpdef numpy.ndarray list_replace(numpy.ndarray data, numpy.ndarray search, numpy.ndarray replace):
    """
    Vectorized string replace implementation without relying on PyArrow or NumPy helpers.
    """
    cdef Py_ssize_t length = data.shape[0]
    cdef Py_ssize_t search_length = search.shape[0]
    cdef Py_ssize_t replace_length = replace.shape[0]

    cdef numpy.ndarray[object, ndim=1] result = numpy.empty(length, dtype=object)

    cdef Py_ssize_t i
    cdef object value
    cdef object search_term
    cdef object replace_term

    cdef bytes value_bytes
    cdef bytes search_bytes
    cdef bytes replace_bytes

    cdef str value_str
    cdef str search_str
    cdef str replace_str

    for i in range(length):
        value = data[i]
        search_term = search[0] if search_length == 1 else search[i]
        replace_term = replace[0] if replace_length == 1 else replace[i]

        if value is None or search_term is None or replace_term is None:
            result[i] = None
            continue

        if isinstance(value, bytes):
            value_bytes = value
            if isinstance(search_term, bytes):
                search_bytes = search_term
            else:
                search_bytes = str(search_term).encode("utf-8")
            if isinstance(replace_term, bytes):
                replace_bytes = replace_term
            else:
                replace_bytes = str(replace_term).encode("utf-8")
            result[i] = value_bytes.replace(search_bytes, replace_bytes)
        else:
            # Handle string types (including numpy.str_)
            try:
                value_str = str(value)
            except (TypeError, ValueError):
                result[i] = None
                continue

            # Convert search_term to str, handling numpy.str_ and other types
            try:
                search_str = str(search_term)
            except (TypeError, ValueError):
                result[i] = None
                continue

            # Convert replace_term to str, handling numpy.str_ and other types
            try:
                replace_str = str(replace_term)
            except (TypeError, ValueError):
                result[i] = None
                continue

            result[i] = value_str.replace(search_str, replace_str)

    return result
