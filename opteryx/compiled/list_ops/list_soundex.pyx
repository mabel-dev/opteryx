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


cpdef numpy.ndarray list_soundex(numpy.ndarray arr):
    """
    Calculate soundex codes for array of strings.

    Parameters:
        arr: Array of strings

    Returns:
        Array of soundex codes
    """
    from opteryx.third_party.fuzzy import soundex

    cdef Py_ssize_t i, n = arr.size
    cdef numpy.ndarray[object, ndim=1] result = numpy.empty(n, dtype=object)
    cdef object string_val

    for i in range(n):
        string_val = arr[i]
        if string_val and string_val is not None:
            result[i] = soundex(string_val)
        else:
            result[i] = None

    return result
